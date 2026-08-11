"""Crew Mode — engineered orchestrator pipeline for multi-topic chat.

Design of record: docs/request-for-change/rfc-orchestrator-chat-sessions.md
(v5, post two adversarial council rounds). The user-selected agent runs only
in continuable sub-sessions ("topics"); this manager is the engineered
control plane: durable ingress queue, single-flight decision agent with
structured I/O, validating executor with idempotent dispatch, and verbatim
summary forwarding with mechanical attribution. The LLM only ever chooses
among legal moves — durability, ordering, attribution, and delivery are
owned by code.

Threading contract: everything here runs on the event loop; the only
blocking work (store writes) is small atomic JSON files, mirroring the
subagent ``state.json`` pattern. The decision LLM call is awaited via
``run_bg_oneliner`` (tool-free, timeout-capped).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
import uuid
from typing import Any

from kiro_crew.config.paths import data_home
from kiro_crew.history import append_if_absent_off_loop
from kiro_crew.llm_helpers import run_bg_oneliner
from kiro_crew.security import redact_credentials, redact_exfiltration_urls
from kiro_crew.subagent_persistence import _agent_dir, read_state

logger = logging.getLogger(__name__)

# Queue entry states (RFC v5 contract):
# pending -> claimed(decision) -> accepted(run_id) -> running -> done|failed
# 'ask' = parked awaiting the user's clarification (returns to routing on
# the next user message).

_DECISION_TIMEOUT = 45.0
_TERMINAL_STATES = ("done", "failed", "steered", "stopped")
# Crew posts that answer the user (as opposed to the templated ack): these
# must never be folded away as intermediate reasoning.
_ANSWER_KINDS = ("crew_result", "crew_meta", "crew_ask")
_QUEUE_TERMINAL_CAP = 200
# topics.json is read inline when a slot's store is first touched, so it must not
# grow without limit. Idle topics past this many are dropped oldest-first; a
# running or held topic is never pruned, however old.
_TOPIC_IDLE_CAP = 200
# A decision pass that settles nothing is retried this many times before the
# entry is failed visibly — silence is the one outcome the user cannot act on.
_DECIDE_MAX_ATTEMPTS = 3
_SUMMARY_RE = re.compile(r"<<<SUMMARY\s*(.*?)\s*>>>", re.DOTALL)
_ACK_TEMPLATES = [
    "On it.",
    "Got it — working on that.",
    "Picking that up now.",
]
_SUB_TASK_SUFFIX = (
    "\n\n---\nDo this work YOURSELF. Do NOT spawn subagents. End your reply "
    "with a summary of the result (<=150 words) wrapped EXACTLY as: "
    "<<<SUMMARY your summary here >>>"
)

_DECISION_PROMPT = """You are the routing decision function for a multi-topic chat. \
Decide what to do with each PENDING message. Reply with ONLY a JSON object, no prose.

Rules:
- A message continuing an existing topic (by meaning) routes to that topic.
- An unrelated new request becomes a new topic (give a short title, <=6 words, in the user's language).
- If genuinely torn between two topics, use "ask" with ONE short casual question.
- A topic with status "running" cannot take a routed message now: use "hold" (it will be dispatched when the topic finishes). Exception: a droppable advisory correction to in-flight work (style/approach preference, "prefer X", "don't touch Y") may use "steer".
- Messages that are meta-questions about the topics themselves ("what's in flight?", "list topics") use "meta".

Output schema:
{"actions": [
  {"do": "route", "msg_id": "<id>", "topic_id": "<id>"},
  {"do": "spawn", "msg_id": "<id>", "title": "<short title>"},
  {"do": "hold",  "msg_id": "<id>", "topic_id": "<id>"},
  {"do": "steer", "msg_id": "<id>", "topic_id": "<id>"},
  {"do": "ask",   "msg_id": "<id>", "question": "<one short line>"},
  {"do": "meta",  "msg_id": "<id>"}
]}

STATE:
%s
"""


def _now() -> float:
    return time.time()


class CrewStore:
    """Durable per-slot queue + topic store (atomic JSON, restart-safe)."""

    def __init__(self, slot_key: str) -> None:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", slot_key)
        self.dir = data_home() / "crew" / safe
        self.dir.mkdir(parents=True, exist_ok=True)
        self.queue: list[dict[str, Any]] = self._load("queue.json")
        self.topics: list[dict[str, Any]] = self._load("topics.json")
        self.forwards: list[dict[str, Any]] = self._load("forwards.json")
        # Off-loop write machinery (see _save).
        # Two-lock split so the event loop NEVER blocks on filesystem I/O:
        #  - _seq_lock guards ONLY the sequence bookkeeping dicts and is never
        #    held across a disk write (both the event-loop seq bump and the
        #    worker's newest-wins check/advance take it for microseconds).
        #  - _io_locks[name] is a per-store lock held BY THE WORKER across the
        #    write+replace to serialize concurrent executor writes to the same
        #    file; the event loop never acquires it, so a slow disk cannot
        #    stall chats/heartbeats waiting on a seq bump.
        self._seq_lock = threading.Lock()
        self._io_locks_guard = threading.Lock()
        self._io_locks: dict[str, threading.Lock] = {}
        self._write_seq: dict[str, int] = {}
        self._written_seq: dict[str, int] = {}
        self._pending_writes: set[Any] = set()

    def _load(self, name: str) -> list[dict[str, Any]]:
        try:
            data = json.loads((self.dir / name).read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return []

    def _save(self, name: str, data: list[dict[str, Any]]) -> Any:
        """Persist one store file without blocking the event loop.

        Serialization happens on the caller (cheap); the disk write is
        offloaded to the default executor when a loop is running (AUTOSDE
        no-blocking-call-on-event-loop). A per-name generation counter
        guarded by ``_seq_lock`` makes newest-wins deterministic even if the
        executor runs writes out of order. The actual write+replace runs
        under a per-name ``_io_locks[name]`` held ONLY by the worker — the
        event loop never acquires it, so a slow disk cannot stall the seq
        bump below. ``_seq_lock`` is never held across the disk I/O. Sync
        callers (tests, reconcile at boot) write inline.
        """
        payload = json.dumps(data, ensure_ascii=False, indent=1)
        with self._seq_lock:
            self._write_seq[name] = seq = self._write_seq.get(name, 0) + 1

        def _write() -> None:
            # Serialize concurrent writes to the SAME store file; worker-only,
            # never acquired on the event loop.
            with self._io_locks_guard:
                io_lock = self._io_locks.setdefault(name, threading.Lock())
            with io_lock:
                with self._seq_lock:
                    if seq <= self._written_seq.get(name, 0):
                        return  # a newer snapshot already landed
                tmp = self.dir / f".{name}.tmp"
                tmp.write_text(payload, encoding="utf-8")
                tmp.replace(self.dir / name)
                # Advance ONLY after the atomic replace succeeded — a failed
                # write must stay retryable, not be recorded as landed. The
                # per-name io_lock guarantees no concurrent writer for this
                # store raced us between the check and this advance.
                with self._seq_lock:
                    if seq > self._written_seq.get(name, 0):
                        self._written_seq[name] = seq

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _write()
            return None       # already on disk: nothing to await
        # Reap only futures that already completed SUCCESSFULLY. The obvious
        # `add_done_callback(discard)` is a hole in the barrier: a write that
        # fails FAST is discarded before `wait_writes()` snapshots the set, so
        # the barrier reports durability for a write that never landed — which
        # is exactly the guarantee every caller here depends on.
        for done in [
            f for f in list(self._pending_writes)
            if f.done() and not f.cancelled() and f.exception() is None
        ]:
            self._pending_writes.discard(done)
        fut = loop.run_in_executor(None, _write)
        self._pending_writes.add(fut)
        return fut

    @staticmethod
    async def wait_for(futures: list[Any]) -> None:
        """Await EXACTLY these writes.

        `wait_writes` awaits whatever is in the pending set when it looks, which
        is not the same guarantee: a caller that needs "the row I just wrote is
        on disk" must name that write, or a reap/replace/race on the shared set
        can let the barrier return early.
        """
        if not futures:
            return
        for r in await asyncio.gather(*futures, return_exceptions=True):
            if isinstance(r, BaseException):
                raise r

    async def wait_writes(self) -> None:
        """Await all in-flight store writes; PROPAGATES the first failure so
        durability-dependent callers (the ingest ack) fail loudly instead of
        acknowledging a write that never landed."""
        pending = list(self._pending_writes)
        if not pending:
            return
        results = await asyncio.gather(*pending, return_exceptions=True)
        # This batch has now been observed, so drop it either way: a failure is
        # raised once, to the caller whose durability depended on it, rather
        # than re-raised on every later barrier.
        for f in pending:
            self._pending_writes.discard(f)
        for r in results:
            if isinstance(r, BaseException):
                raise r

    def save(self) -> list[Any]:
        # Keep queue.json bounded: terminal entries are only needed for quote
        # attribution of recent completions — prune the oldest beyond a cap.
        # Live states (pending/ask/held/claimed/accepted) are never pruned.
        terminal = [e for e in self.queue if e.get("state") in _TERMINAL_STATES]
        if len(terminal) > _QUEUE_TERMINAL_CAP:
            drop = {id(e) for e in terminal[: len(terminal) - _QUEUE_TERMINAL_CAP]}
            self.queue = [e for e in self.queue if id(e) not in drop]
        # topics.json is read INLINE when a slot's store is first touched, so an
        # unbounded file would put a growing parse on the event loop. Same policy
        # as the queue above: prune the oldest IDLE topics past a cap; a running
        # topic, or one still holding queued messages, is never pruned.
        idle = [
            t for t in self.topics
            if t.get("status") != "running" and not (t.get("held") or [])
        ]
        if len(idle) > _TOPIC_IDLE_CAP:
            idle.sort(key=lambda t: float(t.get("last_activity") or 0.0))
            drop = {id(t) for t in idle[: len(idle) - _TOPIC_IDLE_CAP]}
            self.topics = [t for t in self.topics if id(t) not in drop]
        return [f for f in (
            self._save("queue.json", self.queue),
            self._save("topics.json", self.topics),
            self._save("forwards.json", self.forwards),
        ) if f is not None]

    # -- pending-forward helpers (crash-safe delivery) --
    def add_forward(self, body: str) -> str:
        fid = uuid.uuid4().hex[:8]
        self.forwards.append({"fid": fid, "body": body, "ts": _now()})
        self._save("forwards.json", self.forwards)
        return fid

    def remove_forwards(self, fids: set[str]) -> None:
        self.forwards = [f for f in self.forwards if f.get("fid") not in fids]
        self._save("forwards.json", self.forwards)

    # -- queue helpers --
    def add_msg(self, text: str) -> dict[str, Any]:
        return self.add_msg_awaitable(text)[0]

    def add_msg_awaitable(self, text: str) -> tuple[dict[str, Any], list[Any]]:
        """Enqueue, and hand back the entry PLUS the writes it scheduled.

        The futures are returned alongside rather than stored on the entry: the
        entry is the thing that gets serialized, so anything parked on it ends up
        in the JSON payload.
        """
        entry = {"msg_id": uuid.uuid4().hex[:8], "text": text, "ts": _now(), "state": "pending"}
        self.queue.append(entry)
        return entry, self.save()

    def entry(self, msg_id: str) -> dict[str, Any] | None:
        return next((e for e in self.queue if e.get("msg_id") == msg_id), None)

    def pending(self) -> list[dict[str, Any]]:
        return [e for e in self.queue if e.get("state") in ("pending", "ask")]

    # -- topic helpers --
    def topic(self, topic_id: str) -> dict[str, Any] | None:
        return next((t for t in self.topics if t.get("topic_id") == topic_id), None)

    def topic_by_run(self, run_id: str) -> dict[str, Any] | None:
        return next((t for t in self.topics if t.get("active_run_id") == run_id), None)

    def add_topic(self, topic_id: str, run_id: str, title: str, origin_msg: str) -> dict[str, Any]:
        t = {
            "topic_id": topic_id, "active_run_id": run_id, "title": title,
            "digest": "", "status": "running", "last_activity": _now(),
            "origin_msg_id": origin_msg, "held": [],
        }
        self.topics.append(t)
        self.save()
        return t


class CrewOrchestrator:
    """The control plane for crew-mode slots (one instance, all slots)."""

    def __init__(self, state: Any, sessions: Any, subagents: Any, cfg: Any = None) -> None:
        self._state = state
        self._sessions = sessions
        self._subagents = subagents
        self._cfg = cfg
        self._decide_attempts: dict[str, int] = {}
        self._last_transcript_write: Any = None
        self._stores: dict[str, CrewStore] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._drain_locks: dict[str, asyncio.Lock] = {}
        self._rerun: dict[str, bool] = {}
        self._owned: dict[str, str] = {}  # run_id -> slot_key
        self._ack_i = 0
        self._decision_model = getattr(
            getattr(cfg, "dashboard", None), "crew_decision_model", None
        ) or None

    # ---- wiring ----

    def owns(self, run_id: str) -> bool:
        return run_id in self._owned

    async def has_live_work(self, slot_key: str) -> bool:
        """Is any crew work still in flight for this slot?

        `slot.running` cannot answer this: crew work executes in SUBAGENTS, so
        the slot itself is idle the whole time. A caller that means "is this
        session busy" (the mode switch, for one) has to ask here as well, or two
        execution models end up interleaved in one session.
        """
        st = await self._store_async(slot_key)
        if any(t.get("status") == "running" for t in st.topics):
            return True
        # "ask" belongs here: an entry waiting on the user's clarification is
        # unfinished work, and letting the mode switch out from under it
        # abandons the original request with no trace.
        return any(
            e.get("state") in ("pending", "claimed", "accepted", "held", "ask")
            for e in st.queue
        )

    def resume_persisted_slots(self) -> None:
        """Schedule restoration of persisted slots. Returns IMMEDIATELY.

        Constructing the orchestrator is NOT resumption. `_store` only
        reconciles on FIRST touch, and nothing touches it until a new message
        arrives — so after a restart an acknowledged request sat pending with
        nothing scheduled to look at it, and the interrupted-run notices
        `_reconcile` writes were never delivered either. The user's evidence was
        an ack and then silence, indefinitely.

        The work is directory enumeration plus a JSON load and reconcile per
        slot, which scales with the profile, and the gateway calls this on the
        BOOT path — doing it inline delayed readiness and stalled every other
        loop activity. So this only schedules it; the scan runs in the executor
        and the per-slot pass yields between slots.
        """
        try:
            asyncio.get_running_loop().create_task(self._resume_all())
        except RuntimeError:
            logger.warning("crew: no running loop; persisted slots not resumed")

    @staticmethod
    def _list_store_dirs() -> list[str]:
        root = data_home() / "crew"
        if not root.is_dir():
            return []
        return sorted(d.name for d in root.iterdir() if d.is_dir())

    async def _resume_all(self) -> int:
        loop = asyncio.get_running_loop()
        try:
            names = await loop.run_in_executor(None, self._list_store_dirs)
        except Exception:
            logger.warning("crew: could not enumerate crew stores", exc_info=True)
            return 0
        resumed = 0
        for slot_key in names:
            await asyncio.sleep(0)      # yield between slots; never hog the loop
            try:
                st = await self._store_async(slot_key)          # reconciles on first touch
            except Exception:
                logger.warning("crew: could not load store for %s", slot_key, exc_info=True)
                continue
            slot = self._state.get_slot(slot_key) if self._state else None
            if slot is None:
                continue        # not an open slot; its forwards wait on disk
            work = any(e.get("state") in ("pending", "ask") for e in st.queue)
            if not (work or st.forwards):
                continue
            resumed += 1
            await self._resume_slot(slot)
        if resumed:
            logger.info("crew: resumed %d slot(s) with unfinished work", resumed)
        return resumed

    async def _resume_slot(self, slot: Any) -> None:
        st = await self._store_async(slot.key)
        if st.forwards:
            await self._drain_forwards(slot)
        if any(e.get("state") in ("pending", "ask") for e in st.queue):
            await self._decide(slot)

    async def _store_async(self, slot_key: str) -> CrewStore:
        """`_store` for callers running on the event loop.

        Building a store means a mkdir plus three JSON parses, and the queue
        grows with the session — so a cold build on a busy slot blocks the loop
        exactly the way `_save` is careful not to. Only the BUILD is offloaded;
        `_reconcile` still runs on the loop because it posts and schedules.
        """
        cached = self._stores.get(slot_key)
        if cached is not None:
            return cached
        loop = asyncio.get_running_loop()
        built = await loop.run_in_executor(None, CrewStore, slot_key)  # noqa: E501
        # Two concurrent first messages both miss the cache above and both build
        # a store; publishing with `self._stores[k] = built` would let the loser
        # keep writing through its own object to the SAME files, so one queue
        # snapshot overwrites the other (or trips over the other's temp file).
        # `setdefault` picks exactly one winner, and only the winner is
        # reconciled — reconciling twice would re-deliver its forwards.
        st = self._stores.setdefault(slot_key, built)
        if st is built:
            # `_reconcile` must stay ON the loop — it posts and schedules — but
            # its per-entry `state.json` reads scale with the queue, so a restart
            # with many accepted entries stalled the loop one stat() at a time.
            # Gather that evidence in the executor first and hand it over.
            evidence = await loop.run_in_executor(
                None, self._durable_evidence, self._rids_needing_evidence(st))
            self._reconcile(slot_key, st, evidence)
        return st

    @staticmethod
    def _rids_needing_evidence(st: CrewStore) -> list[str]:
        rids = [
            str(e.get("run_id") or e.get("dispatch_id") or "")
            for e in st.queue if e.get("state") in ("claimed", "accepted")
        ]
        rids += [str(t.get("active_run_id") or "") for t in st.topics
                 if t.get("status") == "running"]
        return [r for r in dict.fromkeys(rids) if r]

    @staticmethod
    def _durable_evidence(rids: list[str]) -> dict[str, bool]:
        """``{rid: did-this-run-ever-start}`` — FILESYSTEM reads, executor only.

        Same fail-closed rule as `_run_started`: an answer we could not read is
        treated as "it started", because re-executing on an unknown is worse
        than a duplicate.
        """
        out: dict[str, bool] = {}
        for rid in rids:
            try:
                out[rid] = read_state(rid) is not None or _agent_dir(rid).exists()
            except Exception:
                out[rid] = True
        return out

    def _store(self, slot_key: str) -> CrewStore:
        st = self._stores.get(slot_key)
        if st is None:
            st = CrewStore(slot_key)
            self._stores[slot_key] = st
            self._reconcile(slot_key, st)
        return st

    def _run_alive(self, rid: str) -> bool:
        """Is this run still executing, i.e. will a completion still arrive?

        Only the live registry can answer this, and after a restart the answer
        is legitimately "no" for every run — the processes died with the gateway.
        """
        if not rid or self._subagents is None:
            return False
        info = self._subagents.get(rid)
        return info is not None and not getattr(info, "done", False)

    def _run_started(self, rid: str, evidence: dict[str, bool] | None = None) -> bool:
        """Did this run ever actually start? Answered from DURABLE state.

        This is NOT the same question as `_run_alive`, and conflating the two is
        what stranded entries forever: `state.json` is written when a run is
        CREATED and never updated to a terminal status, so its presence proves
        the dispatch took effect and nothing more. That is exactly the fact
        needed to decide whether re-executing is safe — the in-process registry
        cannot supply it, because a restart empties the registry while the task
        may well have run (and mutated something).
        """
        if not rid:
            return False
        if self._subagents is not None and self._subagents.get(rid) is not None:
            return True
        if evidence is not None and rid in evidence:
            return evidence[rid]      # already read, off the loop
        try:
            if read_state(rid) is not None:
                return True
            # `read_state` returns None for a MISSING file and for one it could
            # not parse, so its None alone cannot be read as "never started" —
            # a corrupt or transiently unreadable state.json would re-dispatch a
            # task that may already have mutated something. Only a positively
            # absent directory is evidence of a dispatch that never took effect.
            return _agent_dir(rid).exists()
        except Exception:
            logger.warning("crew: durable run lookup failed for %s", rid, exc_info=True)
            return True      # fail closed: never re-execute on an unknown answer

    def _reconcile(self, slot_key: str, st: CrewStore,
                   evidence: dict[str, bool] | None = None) -> None:
        """Restart reconciliation: re-own live runs; re-open interrupted
        dispatches (claimed/accepted whose run is unknown -> pending)."""
        for t in st.topics:
            rid = t.get("active_run_id") or ""
            if t.get("status") == "running" and rid:
                info = self._subagents.get(rid) if self._subagents else None
                if info is not None and not info.done:
                    self._owned[rid] = slot_key
                else:
                    t["status"] = "idle"  # completion may have been lost
        for e in st.queue:
            state = e.get("state")
            if state not in ("claimed", "accepted", "held"):
                continue
            if state == "held":
                # A held entry is queued behind a running topic; it never
                # spawned a run of its own, so reopening it cannot re-execute.
                e["state"] = "pending"
                continue
            # claimed/accepted may have already STARTED a run before the crash.
            # Adopt it by its stable dispatch id instead of blindly reopening to
            # pending, which would re-execute a possibly-mutating task (GPT
            # finding on 84dfff5b). ``_apply`` persists dispatch_id (== the run's
            # preassigned id) BEFORE spawning, so this lookup is reliable.
            rid = e.get("run_id") or e.get("dispatch_id") or ""
            if self._run_alive(rid):
                # Genuinely still executing — re-own it and make sure a topic
                # tracks it. A completion IS still coming, so do not re-dispatch.
                self._owned[rid] = slot_key
                e["state"] = "accepted"
                e["run_id"] = rid
                if not st.topic(e.get("topic_id") or rid):
                    st.add_topic(rid, rid, (e.get("text", "")[:24] or rid), e.get("msg_id", ""))
                e.setdefault("topic_id", rid)
            elif self._run_started(rid, evidence):
                # Started, but its process is gone: NO completion will ever
                # arrive. Re-owning it (what this branch used to do for anything
                # with a state.json) left the entry `accepted` forever, silent —
                # and re-opening it would re-execute a task that may already have
                # mutated something. Neither is acceptable, so settle it as
                # interrupted and TELL the user, who can then decide to resend.
                e["state"] = "stopped"
                e["run_id"] = rid
                topic = st.topic(e.get("topic_id") or rid)
                if topic is not None and topic.get("active_run_id") == rid:
                    topic["status"] = "idle"
                st.add_forward(
                    f"⚠ A task was interrupted before it finished: "
                    f"“{(e.get('text') or '')[:120]}”. It may have partially run, "
                    f"so it was not retried automatically — resend it if you still need it."
                )
            else:
                # DURABLE state has no record of this id, so the dispatch never
                # took effect and re-opening cannot double-execute — for either
                # state. `accepted` used to be left alone here, which stranded a
                # capacity-queued spawn forever once the volatile queue died
                # with the process (GPT finding on 20dd06514).
                e["state"] = "pending"
        # Held msg_ids were just reopened to pending; drop them from every
        # topic's held list so a later completion cannot double-dispatch.
        for t in st.topics:
            t["held"] = [
                m for m in t.get("held", []) if (st.entry(m) or {}).get("state") == "held"
            ]
        st.save()
        # Re-deliver forwards that were persisted but never posted because the
        # previous process died between the two (at-least-once).
        if st.forwards:
            slot = self._state.get_slot(slot_key) if self._state else None
            if slot is not None:
                # `_reconcile` is sync (the sync `_store` path calls it), so the
                # replay is scheduled rather than awaited — nothing here holds a
                # result whose only copy would be dropped on scheduling.
                asyncio.get_running_loop().create_task(self._drain_forwards(slot))

    # ---- transcript posting (workflow_inject shape) ----

    @staticmethod
    def _safe_for_log(raw: str) -> str:
        """Redact before raw model output reaches persistent logs.

        `_post` is the delivery chokepoint, but the log is a SECOND egress for
        the same untrusted text: a malformed decision that happens to carry a
        credential would otherwise be written verbatim to the gateway log.
        """
        try:
            out, _ = redact_exfiltration_urls(raw)
            out, _ = redact_credentials(out)
            return out
        except Exception:
            return "<redaction failed; raw output withheld>"

    async def _post_durable(self, slot: Any, content: str, kind: str = "crew") -> bool:
        """`_post`, then wait for the durable transcript row to actually land.

        `_post` schedules that append off-loop, so its True means "delivered and
        scheduled" — not "on disk". A caller holding the only durable copy of a
        result must not drop it on the weaker promise: under history-lock
        contention the removal can land first, and a crash then loses the result.

        The append alone is NOT sufficient proof. `append_if_absent` skips a
        message whose (role, content) is already persisted for this session — it
        exists precisely so the periodic slot flush and this durable copy cannot
        double-write the same row. That dedupe is by CONTENT, so a repeated
        completion body (two topics answering identically, a retried forward)
        makes the second append a successful NO-OP: the future resolves, yet no
        new row reached disk and the new message lives only in the in-memory
        slot until the periodic flush. The authoritative barrier is therefore a
        forced save of the SLOT, which writes full state and so cannot be
        deduplicated away; `best_effort=False` makes it raise instead of
        silently re-arming the periodic retry, so a failure keeps the forward.
        """
        if not self._post(slot, content, kind=kind):
            return False
        fut, self._last_transcript_write = self._last_transcript_write, None
        if fut is not None:
            try:
                await fut
            except Exception:
                # A mirror copy, not the proof: the slot save below is what
                # decides durability, so a lock timeout here is not fatal.
                logger.warning("crew: durable transcript append failed", exc_info=True)
        # Imported here rather than at module scope: `dashboard/__init__` is
        # deliberately lazy (PEP 562) and `dashboard.chat_folders` imports this
        # module, so a top-level import would add weight to every process that
        # merely imports crew_chat.
        from kiro_crew.dashboard.chat_persistence import save_slot_off_loop
        try:
            await save_slot_off_loop(
                self._state, slot, force=True, best_effort=False
            )
        except Exception:
            logger.warning("crew: durable slot save failed", exc_info=True)
            return False
        return True

    def _post(self, slot: Any, content: str, kind: str = "crew") -> bool:
        """Deliver one message. Returns whether it actually reached the user —
        callers that hold the only durable copy MUST NOT drop it on False."""
        # Never trust LLM output: every _post payload is LLM-authored on some
        # path (forwarded summaries, decision-agent questions, meta renders).
        # Redact once at the sole delivery chokepoint, mirroring
        # workflow_inject._redact and gateway._subagent_done.
        try:
            content, _ = redact_exfiltration_urls(content)
            content, _ = redact_credentials(content)
        except Exception:
            logger.warning("crew: redaction failed; refusing to post raw", exc_info=True)
            return False
        try:
            # broadcast=False: the explicit chat_message frame below is the
            # single broadcast (append's implicit _on_message would duplicate
            # it — GPT review finding on 120fd95e; persistence is unaffected).
            # The answer kinds carry a marker class so the transcript can keep
            # them out of the "Worked through N steps" collapse: in crew mode
            # EVERY forward is a final answer for a different topic, so the
            # "last assistant message is the conclusion" model would hide real
            # answers. It goes in cls (persisted) rather than the ws-only
            # `kind`, so the distinction survives a reload.
            is_answer = kind in _ANSWER_KINDS
            cls = "msg msg-a crew-reply" if is_answer else "msg msg-a"
            # The marker lives in META, not just the class: the periodic slot
            # flush (chat_persistence._build_message_entry) keeps `cls` only for
            # role == "system" and drops it for assistant, while it keeps `meta`
            # for every role. A class-only marker was therefore erased by the
            # main persistence path — which is why patching one channel at a time
            # (ws frame, ConversationLog) kept leaving another.
            meta = {"crew_reply": True} if is_answer else None
            slot.append("assistant", content, cls, broadcast=False, meta=meta)
            self._state.broadcast_ws(
                "chat_message",
                {"slot": slot.key, "role": "assistant", "content": content,
                 # Both fields ride the frame: the store reducer reads each off
                 # the payload, and `meta` is the one that survives every
                 # persistence path (see the comment above).
                 "cls": cls, "meta": meta, "kind": kind},
            )
        except Exception:
            logger.warning("crew: transcript post failed for %s", slot.key, exc_info=True)
            return False
        try:
            self._last_transcript_write = append_if_absent_off_loop(
                self._state.conversation_log,
                getattr(slot, "linked_session_key", "") or f"dashboard:{slot.key}",
                "assistant",
                content,
                # The SAME class as the in-memory copy: this log is what a restart
                # replays from, and it had no field for the marker at all.
                cls=cls,
            )
        except Exception:
            logger.debug("crew: conversation_log append failed", exc_info=True)
        # The user HAS seen it by now: the transcript append and broadcast above
        # both succeeded. The conversation_log is a secondary copy, so failing to
        # mirror it there is not a delivery failure and must not make the caller
        # keep (and re-deliver) the durable forward.
        return True

    # ---- ingress ----

    async def ingest(self, slot: Any, message: str, *,
                     user_meta: dict[str, Any] | None = None) -> None:
        """Called from api_chat for crew slots. Enqueue DURABLY, then show the
        user's message, ack, and schedule the decision pass.

        The append lives here rather than in the caller so that nothing is
        VISIBLE before it is DURABLE: the caller used to append first and then
        await this, and on a cold slot that await builds the store — a process
        exit in that window left the user looking at their own message with no
        queue entry behind it, a request that could never be resumed.
        """
        # App-governance boundary: Crew orchestration dispatches work through
        # spawn / continue_conversation, and continue_conversation carries no
        # ``app`` — so an app-owned slot entering Crew would run its subagents
        # (and their host-permitted tools) OUTSIDE the app's profile. Until the
        # whole dispatch path preserves ``slot._app``, refuse Crew for app-owned
        # slots rather than silently drop the app identity (GPT finding on
        # 84dfff5b). Dashboard-created Crew slots have no _app and are unaffected.
        # isinstance guard (not truthiness): test doubles are MagicMock, whose
        # auto-created ._app attribute is truthy — only a real, non-empty str
        # marks an app-owned slot (mirrors the CrewOrchestrator isinstance
        # check in gateway._subagent_done).
        _slot_app = getattr(slot, "_app", "")
        if isinstance(_slot_app, str) and _slot_app:
            self._post(
                slot,
                "Crew mode isn't available in an app-owned session — start a "
                "regular chat for multi-topic orchestration.",
                kind="crew_ask",
            )
            return
        st = await self._store_async(slot.key)
        # Re-deliver forwards persisted while the tab was closed (see
        # on_subagent_done's closed-slot branch): within one process the store
        # is cached, so a slot reopen does NOT pass through _reconcile — flush
        # the durable copies here. Duplicate-vs-loss: the flush removes them
        # only after posting, and a duplicate beats a silently lost result.
        if st.forwards:
            await self._drain_forwards(slot)
        _entry, writes = st.add_msg_awaitable(message)
        # Durability BEFORE anything the user can see: both the echoed message
        # and "on it" are promises that it survives a crash. Await THIS write by
        # name — `wait_writes()` only covers whatever is pending when it looks,
        # which let the ack go out with an empty queue on disk (RFC C1: the
        # queue is the durable record).
        await CrewStore.wait_for(writes)
        await st.wait_writes()
        slot.append("user", message, "msg msg-u", meta=user_meta)
        ack = _ACK_TEMPLATES[self._ack_i % len(_ACK_TEMPLATES)]
        self._ack_i += 1
        # `_post_durable`, not `_post`: the echoed user message and the ack are
        # the visible half of the promise the durable queue entry above makes.
        # `slot.append` only mutates memory, so with a plain `_post` a crash
        # before the periodic flush left the queue holding work whose QUESTION
        # was gone from the transcript — the user comes back to an answer to
        # nothing. The forced slot save inside `_post_durable` writes full slot
        # state, so it covers the user row and the ack in one write. The user
        # has already SEEN both (the broadcast happens inside `_post`), so this
        # await costs the handler's return, not perceived latency.
        await self._post_durable(slot, ack, kind="crew_ack")
        asyncio.create_task(self._decide(slot))

    # ---- single-flight decision loop ----

    async def _decide(self, slot: Any) -> None:
        lock = self._locks.setdefault(slot.key, asyncio.Lock())
        if lock.locked():
            self._rerun[slot.key] = True  # fold into next snapshot
            return
        async with lock:
            while True:
                self._rerun[slot.key] = False
                try:
                    await self._decide_once(slot)
                except Exception:
                    logger.warning("crew: decision pass failed for %s", slot.key, exc_info=True)
                if self._rerun.get(slot.key):
                    continue
                # A pass can return valid JSON that settles NOTHING (empty
                # actions, or every action rejected by the executor). The entry
                # then stays pending with nothing scheduled to look at it again,
                # so the user is left holding only the acknowledgement — forever.
                # Retry a bounded number of times, then fail it VISIBLY. This
                # runs INSIDE the loop and the lock on purpose: recursing into
                # `_decide` would be swallowed by its own single-flight check.
                if self._settle_stragglers(slot):
                    continue
                break
            self._decide_attempts.pop(slot.key, None)

    def _settle_stragglers(self, slot: Any) -> bool:
        """Returns True to run another decision pass, False when done."""
        st = self._store(slot.key)
        stuck = [e for e in st.queue if e.get("state") == "pending"]
        if not stuck:
            return False
        tries = self._decide_attempts.get(slot.key, 0) + 1
        self._decide_attempts[slot.key] = tries
        if tries < _DECIDE_MAX_ATTEMPTS:
            logger.info("crew: %d entr(ies) unsettled for %s, retry %d/%d",
                        len(stuck), slot.key, tries, _DECIDE_MAX_ATTEMPTS)
            return True
        for e in stuck:
            e["state"] = "failed"
        st.save()
        self._post(
            slot,
            "I could not work out how to route "
            + ("this request" if len(stuck) == 1 else f"{len(stuck)} of your requests")
            + " after several attempts, so nothing was started. Please rephrase and send again.",
            kind="crew_meta",
        )
        return False

    def _snapshot(self, st: CrewStore) -> str:
        return json.dumps(
            {
                "queue": [
                    {"msg_id": e["msg_id"], "text": e["text"][:400], "state": e["state"]}
                    for e in st.pending()
                ],
                "topics": [
                    {
                        "topic_id": t["topic_id"], "title": t["title"],
                        "digest": t.get("digest", "")[:200], "status": t["status"],
                    }
                    for t in st.topics if t.get("status") != "released"
                ],
            },
            ensure_ascii=False,
        )

    async def _decide_once(self, slot: Any) -> None:
        st = await self._store_async(slot.key)
        if not st.pending():
            return
        prompt = _DECISION_PROMPT % self._snapshot(st)
        raw = ""
        for attempt in (1, 2):
            try:
                raw = await run_bg_oneliner(
                    self._sessions, prompt, model=self._decision_model,
                    sel_source="crew_decision", timeout=_DECISION_TIMEOUT,
                )
                m = re.search(r"\{.*\}", raw, re.DOTALL)
                actions = json.loads(m.group(0))["actions"] if m else []
                break
            except Exception:
                if attempt == 2:
                    logger.warning("crew: unparseable decision, deferring: %r",
                                   self._safe_for_log(raw[:200]))
                    return
        for a in actions:
            try:
                await self._apply(slot, st, a)
            except Exception:
                # `a` is LLM-authored too — its field values are model output,
                # so it gets the same treatment as the raw decision above.
                logger.warning("crew: action failed: %s",
                               self._safe_for_log(repr(a)), exc_info=True)
        st.save()

    # ---- executor (validates every action; LLM only picks legal moves) ----

    async def _apply(self, slot: Any, st: CrewStore, a: dict[str, Any]) -> None:
        do = a.get("do")
        e = st.entry(str(a.get("msg_id", "")))
        if e is None or e.get("state") not in ("pending", "ask"):
            return  # unknown/settled msg — reject silently
        if do == "spawn":
            # Persist a STABLE dispatch identity before spawning so a crash in
            # the window between spawn() starting the run and the accepted-state
            # write is recoverable WITHOUT re-executing the task (GPT finding on
            # 84dfff5b). ``_preassigned_id`` makes the subagent's id equal this
            # token, so _reconcile can look it up and adopt the already-started
            # run instead of blindly reopening the entry to pending.
            dispatch_id = uuid.uuid4().hex[:8]
            e["state"] = "claimed"
            e["dispatch_id"] = dispatch_id
            st.save()
            await st.wait_writes()  # durable BEFORE the side-effecting spawn
            # A temporary session blocks memory-context injection, and crew
            # dispatch is not an exception to that boundary — chat_runner passes
            # the same flag on the main path. Without it, a temporary crew slot
            # leaked stored memory and lessons into every subagent it spawned.
            no_reads = bool(getattr(slot, "blocks_reads", False))
            info = self._subagents.spawn(
                (e["text"] + _SUB_TASK_SUFFIX),
                parent_session_key=f"dashboard:{slot.key}",
                agent=getattr(slot, "agent", "") or "",
                keep=True,
                _preassigned_id=dispatch_id,
                include_memory=not no_reads,
                include_lessons=not no_reads,
            )
            if info is None or (getattr(info, "done", False) and getattr(info, "error", "")):
                e["state"] = "pending"
                self._post(
                    slot,
                    "Couldn't start that one — say the word and I'll retry.",
                    kind="crew_ask",
                )
                return
            title = str(a.get("title") or e["text"][:24])
            st.add_topic(info.id, info.id, title, e["msg_id"])
            self._owned[info.id] = slot.key
            e["state"] = "accepted"
            e["run_id"] = info.id
            e["topic_id"] = info.id
        elif do == "route":
            t = st.topic(str(a.get("topic_id", "")))
            if t is None or t.get("status") == "released":
                return
            if t.get("status") == "running":
                t.setdefault("held", []).append(e["msg_id"])
                e["state"] = "held"
                return
            await self._dispatch_continue(slot, st, t, e)
        elif do == "hold":
            t = st.topic(str(a.get("topic_id", "")))
            if t is None:
                return
            if t.get("status") != "running":
                # Stale hold: the topic completed while the decision was in
                # flight. A held entry would strand forever (no future
                # completion dispatches it) — continue the topic instead.
                if t.get("status") == "released":
                    return
                await self._dispatch_continue(slot, st, t, e)
                return
            t.setdefault("held", []).append(e["msg_id"])
            e["state"] = "held"
        elif do == "steer":
            t = st.topic(str(a.get("topic_id", "")))
            if t is None or t.get("status") != "running":
                return
            ok, _detail = await self._subagents.steer_run(t["active_run_id"], e["text"])
            if ok:
                e["state"] = "steered"
                return
            # Steer failed — the run may have completed DURING the await
            # (GPT finding on 85f8fbe2): holding now would strand the message
            # forever since no future completion dispatches it. Recheck.
            if t.get("status") == "running":
                t.setdefault("held", []).append(e["msg_id"])
                e["state"] = "held"
            else:
                await self._dispatch_continue(slot, st, t, e)
        elif do == "ask":
            e["state"] = "ask"
            self._post(slot, str(a.get("question") or "Quick check — is that about an existing topic, or something new?"), kind="crew_ask")
        elif do == "meta":
            e["state"] = "done"
            self._post(slot, self._render_topics(st), kind="crew_meta")

    async def _dispatch_continue(self, slot: Any, st: CrewStore, t: dict[str, Any], e: dict[str, Any]) -> None:
        dispatch_id = uuid.uuid4().hex[:8]
        e["state"] = "claimed"
        e["dispatch_id"] = dispatch_id
        # Record WHICH topic this belongs to. A continuation runs under a new run
        # id while staying on the existing topic, so without this the entry has
        # no topic_id and reconciliation invents a second topic keyed by the run.
        e["topic_id"] = t["topic_id"]
        st.save()
        await st.wait_writes()   # durable BEFORE the side-effecting continue
        # Same contract as the spawn path: the dispatch identity is durable
        # BEFORE the side effect, so a crash between the two is recoverable by
        # id rather than a guess. Without this the continuation minted its id
        # inside the call, and a restart could neither adopt the started run nor
        # safely reopen the entry.
        info = self._subagents.continue_conversation(
            t["topic_id"], e["text"] + _SUB_TASK_SUFFIX,
            parent_session_key=f"dashboard:{slot.key}",
            agent=getattr(slot, "agent", "") or "",
            _preassigned_id=dispatch_id,
        )
        if info is None:
            e["state"] = "pending"
            return
        if getattr(info, "done", False) and getattr(info, "error", ""):
            err = str(info.error)
            if err.startswith("conversation_busy"):
                t.setdefault("held", []).append(e["msg_id"])
                e["state"] = "held"
            else:
                # conversation_gone / resume_failed: respawn with digest +
                # original payload — the user never re-types (RFC v5 D-gone).
                seed = f"Context digest of a prior thread: {t.get('digest', '(none)')}\n\nTask: {e['text']}"
                # This respawn is a dispatch like any other, so it carries the
                # same durable identity: without it a crash between the spawn
                # and the acceptance write leaves an id nothing can match, and
                # reconciliation re-executes the task.
                respawn_id = uuid.uuid4().hex[:8]
                e["dispatch_id"] = respawn_id
                st.save()
                await st.wait_writes()   # durable BEFORE the side-effecting spawn
                no_reads = bool(getattr(slot, "blocks_reads", False))
                fresh = self._subagents.spawn(
                    seed + _SUB_TASK_SUFFIX,
                    parent_session_key=f"dashboard:{slot.key}",
                    agent=getattr(slot, "agent", "") or "", keep=True,
                    _preassigned_id=respawn_id,
                    include_memory=not no_reads,
                    include_lessons=not no_reads,
                )
                if fresh is not None and not (
                    getattr(fresh, "done", False) and getattr(fresh, "error", "")
                ):
                    t["topic_id"] = fresh.id
                    t["active_run_id"] = fresh.id
                    t["status"] = "running"
                    self._owned[fresh.id] = slot.key
                    e["state"] = "accepted"
                    e["run_id"] = fresh.id
                else:
                    # Refused respawn (SubagentInfo(done=True, error=...)):
                    # recording it as live would wedge the topic forever.
                    e["state"] = "pending"
                    self._post(
                        slot,
                        "Couldn't pick that one back up — say the word and I'll retry.",
                        kind="crew_ask",
                    )
            return
        t["active_run_id"] = info.id
        t["status"] = "running"
        t["last_activity"] = _now()
        self._owned[info.id] = slot.key
        e["state"] = "accepted"
        e["run_id"] = info.id

    def _render_topics(self, st: CrewStore) -> str:
        live = [t for t in st.topics if t.get("status") != "released"]
        if not live:
            return "Nothing in flight right now — everything's wrapped up."
        lines = ["Here's what's in flight:"]
        for t in live:
            n = len(t.get("held", []))
            extra = f" (+{n} queued)" if n else ""
            lines.append(f"- **{t['title']}** — {t['status']}{extra}: {t.get('digest') or 'just started'}")
        return "\n".join(lines)

    # ---- completion delivery ----

    async def on_subagent_done(self, info: Any) -> None:
        """Called from gateway._subagent_done for owned runs (default
        injection suppressed). Forward the summary with attribution, then
        dispatch the topic's held queue."""
        slot_key = self._owned.pop(info.id, "")
        if not slot_key:
            return
        st = await self._store_async(slot_key)
        t = st.topic_by_run(info.id)
        if t is None:
            logger.info("crew: stale completion %s (no topic) — ignored", info.id)
            return
        # Extract the contracted summary; fall back to truncated result.
        raw = str(getattr(info, "result", "") or "")
        m = _SUMMARY_RE.search(raw)
        summary = (m.group(1).strip() if m else raw.strip()[:800]) or "(no output)"
        # Canonical three-way outcome (SubagentInfo.outcome docstring: consumers
        # MUST use it): a user-stopped run is neither success nor failure.
        outcome = str(getattr(info, "outcome", "") or ("failed" if getattr(info, "error", "") else "completed"))
        if outcome == "stopped":
            summary = "Stopped at your request."
        elif outcome == "failed":
            summary = f"Hit a problem: {getattr(info, 'error', '') or 'unknown error'}"
        origin = st.entry(t.get("origin_msg_id", ""))
        for e in st.queue:
            if e.get("run_id") == info.id and e.get("state") == "accepted":
                e["state"] = {"completed": "done", "stopped": "stopped"}.get(outcome, "failed")
                origin = e
        quote = (origin or {}).get("text", "")[:80]
        t["status"] = "idle"
        t["digest"] = summary[:200]
        t["last_activity"] = _now()
        # Settle + persist BEFORE the slot check: a slot closed mid-run must
        # not leave the topic wedged in "running" or the entry in "accepted"
        # (GPT review finding on 7d6f4d7a). Delivery below is best-effort.
        st.save()
        body = f"↩ re: “{quote}”\n\n{summary}" if quote else summary
        slot = self._state.get_slot(slot_key)
        if slot is None:
            # Tab closed mid-run (GPT finding on 9eb28ee4). Two obligations:
            # (1) the RESULT must stay reachable — persist it as a durable
            #     forward so _reconcile re-delivers it when the slot reopens;
            # (2) HELD follow-ups must not strand — with the topic now idle no
            #     future completion will dispatch them, so reopen them to
            #     pending for the reopened slot's decision pass.
            # Plus a best-effort dashboard notification so the user learns the
            # result even if they never reopen the tab.
            st.add_forward(body)
            for mid in t.get("held", []):
                he = st.entry(mid)
                if he is not None and he.get("state") == "held":
                    he["state"] = "pending"
            t["held"] = []
            st.save()
            # Await the write before returning. `_save` offloads to the executor,
            # so returning early acknowledges a completion whose forward may not
            # be on disk yet — and the callback's own return is what tells the
            # subagent layer this result was handled, so a crash here loses it.
            await st.wait_writes()
            try:
                self._state.notify(
                    "crew",
                    f"Crew result: {t.get('title') or 'topic'}",
                    summary[:500],
                )
            except Exception:
                logger.debug("crew: closed-slot notification failed", exc_info=True)
            logger.info("crew: completion %s settled for closed slot %s (forward persisted)", info.id, slot_key)
            return
        try:
            await self._queue_forward(slot, body)
        finally:
            # The held queue must drain even if forwarding raised: the topic is
            # idle now, so no future completion would ever dispatch these and
            # they would sit behind it forever. Delivering the result and
            # dispatching the queue are independent obligations of one completion.
            held = t.get("held", [])
            if held:
                head = st.entry(held.pop(0))
                if head is not None:
                    head["state"] = "pending"
                    await self._dispatch_continue(slot, st, t, head)
            st.save()

    async def _queue_forward(self, slot: Any, body: str) -> None:
        """Deliver ONE completion as ONE message, immediately.

        Every forward is the final answer for its own topic, so results are
        never merged: grouping two topics into one bubble (and stalling a lone
        result behind a coalesce window) is exactly what the code-driven forward
        path exists to avoid.

        The body is persisted AND the write is awaited before the post, then
        cleared only after it. `_save` offloads the disk write to the executor,
        so `add_forward` returning is not durability — without the await, a
        completion followed immediately by process exit loses the result
        outright. Same contract as the spawn path (`durable BEFORE the
        side-effecting` call). A crash between the awaited write and the removal
        re-delivers on reconcile: at-least-once, a duplicate beats a lost result.
        """
        if slot is None:
            return
        st = await self._store_async(slot.key)
        fid = st.add_forward(body)
        await st.wait_writes()          # durable BEFORE the post, not just queued
        # Clear ONLY on a delivery that happened. `_post` has handled failure
        # paths (redaction refusal, transcript error); dropping the persisted
        # copy on those turned a failed delivery into a permanently lost result.
        if await self._post_durable(slot, body, kind="crew_result"):
            st.remove_forwards({fid})

    async def _drain_forwards(self, slot: Any) -> None:
        """Post every durable forward that outlived its process, one message
        each — same shape as a live delivery, so a restart is indistinguishable
        from having been online.

        SERIALIZED per slot, and the pending set is re-read INSIDE the lock.
        Three call sites reach this (`_reconcile`'s scheduled replay, boot
        `_resume_slot`, and a slot reopen in `ingest`) and two of them fire on
        the same restart: `resume_persisted_slots` touches the store — which
        reconciles, scheduling a drain — and then calls `_resume_slot`, which
        drains again. Concurrent drains each snapshot the pending list before
        either removal lands, so every persisted forward was posted TWICE on
        every restart. At-least-once tolerates a duplicate after a crash; it
        does not excuse one by construction. Holding the lock and re-reading
        makes the second caller a no-op instead of a second delivery, and keeps
        any future fourth call site correct without it having to know.
        """
        lock = self._drain_locks.setdefault(slot.key, asyncio.Lock())
        async with lock:
            st = self._store(slot.key)
            for f in list(st.forwards):
                if await self._post_durable(slot, str(f.get("body", "")), kind="crew_result"):
                    st.remove_forwards({str(f.get("fid", ""))})
