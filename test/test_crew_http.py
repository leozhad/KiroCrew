"""HTTP-level integration tests for Crew Mode.

Drives the REAL api_chat handler with a crew-mode slot and a CrewOrchestrator
whose decision LLM is stubbed (deterministic actions) and whose subagent
manager is mocked at the spawn/continue boundary. Proves the full pipeline:
create crew slot via HTTP → interleaved messages → instant acks in the
transcript → topics spawned/routed → completion → forwarded result with
attribution → held message auto-dispatch. The pieces NOT covered here (real
LLM routing quality, real sub-session execution) are exercised by the live
manual protocol in the PR description.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp.test_utils import TestClient, TestServer
from chat_test_helpers import _make_app, _make_state

import kiro_crew.crew_chat as crew_mod
from kiro_crew.crew_chat import CrewOrchestrator


@pytest.fixture(autouse=True)
def _isolate_crew_dir(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(crew_mod, "data_home", lambda: tmp_path / "crewdata")


def _spawn_info(run_id: str, done: bool = False, error: str = "") -> MagicMock:
    info = MagicMock()
    info.id = run_id
    info.done = done
    info.error = error
    return info


def _crew_state(tmp_path):  # type: ignore[no-untyped-def]
    state = _make_state(tmp_path)
    subagents = MagicMock()
    subagents.spawn = MagicMock(side_effect=[_spawn_info("rA"), _spawn_info("rB")])
    subagents.continue_conversation = MagicMock(return_value=_spawn_info("rC"))
    state.subagents = subagents
    state.crew = CrewOrchestrator(state=state, sessions=state.sessions, subagents=subagents)
    state.broadcast_ws = MagicMock()
    return state


def _mode_app(state):  # type: ignore[no-untyped-def]
    """`_make_app` is a trimmed test app that omits the mode route (the real one
    is registered at server.py:2538). Add just that route."""
    from kiro_crew.dashboard.chat_folders import api_chat_slot_mode
    app = _make_app(state)
    app.router.add_patch("/api/chat/slots/{slot}/mode", api_chat_slot_mode)
    return app


class TestCrewHttpFlow:
    @pytest.mark.asyncio
    async def test_interleaved_messages_full_flow(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        state = _crew_state(tmp_path)
        slot = state.get_or_create_slot("crew1", mode="crew")
        assert slot.mode == "crew"

        # Deterministic decision LLM: msg1 -> new topic; msg2 -> new topic;
        # msg3 -> route to topic rA (idle at that point).
        decisions = [
            '{"actions": [{"do": "spawn", "msg_id": "%s", "title": "task A"}]}',
            '{"actions": [{"do": "spawn", "msg_id": "%s", "title": "task B"}]}',
            '{"actions": [{"do": "route", "msg_id": "%s", "topic_id": "rA"}]}',
        ]
        seen: list[str] = []

        async def fake_oneliner(sessions, prompt, **kw):  # type: ignore[no-untyped-def]
            import json as _j
            state_part = prompt.split("STATE:", 1)[1]
            snap = _j.loads(state_part[state_part.index("{"):state_part.rindex("}") + 1])
            pending = [e["msg_id"] for e in snap.get("queue", [])]
            tmpl = decisions[len(seen)]
            seen.append(pending[0])
            return tmpl % pending[0]

        async with TestClient(TestServer(_make_app(state))) as client:
            with patch.object(crew_mod, "run_bg_oneliner", side_effect=fake_oneliner):
                r1 = await client.post("/api/chat", json={"slot": "crew1", "message": "do task A"})
                assert (await r1.json()).get("crew") is True
                await asyncio.sleep(0.05)  # let the decision task run
                r2 = await client.post("/api/chat", json={"slot": "crew1", "message": "do task B"})
                assert r2.status == 200
                await asyncio.sleep(0.05)

        st = state.crew._store("crew1")
        # Two topics spawned, owned, running
        assert state.crew.owns("rA") and state.crew.owns("rB")
        assert {t["title"] for t in st.topics} == {"task A", "task B"}
        # Transcript got: 2 user messages + 2 acks (assistant)
        roles = [m.get("role") for m in slot.messages]
        assert roles.count("user") == 2
        assert roles.count("assistant") >= 2
        # Both messages were spawned with keep=True and the summary contract
        for call in state.subagents.spawn.call_args_list:
            assert call.kwargs["keep"] is True
            assert "<<<SUMMARY" in call.args[0]

    @pytest.mark.asyncio
    async def test_completion_forwards_and_dispatches_held(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        state = _crew_state(tmp_path)
        slot = state.get_or_create_slot("crew1", mode="crew")
        crew = state.crew
        st = crew._store("crew1")
        e = st.add_msg("original ask")
        e["state"] = "accepted"
        e["run_id"] = "rA"
        t = st.add_topic("rA", "rA", "task A", e["msg_id"])
        held = st.add_msg("follow up")
        held["state"] = "held"
        t["held"] = [held["msg_id"]]
        crew._owned["rA"] = "crew1"

        info = _spawn_info("rA", done=True)
        info.result = "work work <<<SUMMARY task A finished: everything green >>>"
        await crew.on_subagent_done(info)   # delivered synchronously, one message

        # Forward landed in the transcript with attribution to the origin msg
        bodies = [m.get("content", "") for m in slot.messages if m.get("role") == "assistant"]
        fwd = next(b for b in bodies if "task A finished" in b)
        assert "↩ re:" in fwd and "original ask" in fwd
        # Held follow-up auto-dispatched via continue on the same conversation
        state.subagents.continue_conversation.assert_called_once()
        assert t["active_run_id"] == "rC"
        assert crew.owns("rC")

    @pytest.mark.asyncio
    async def test_non_crew_slot_unaffected(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        """A default slot must not route through the crew pipeline."""
        state = _crew_state(tmp_path)
        state.get_or_create_slot("plain", mode="")
        ingest = AsyncMock()
        with patch.object(state.crew, "ingest", ingest):
            async with TestClient(TestServer(_make_app(state))) as client:
                with patch("kiro_crew.dashboard.chat_runner._run_chat", new=AsyncMock()):
                    await client.post("/api/chat", json={"slot": "plain", "message": "hi"})
        ingest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_endpoint_rejects_bad_mode(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        from kiro_crew.dashboard.chat_handlers import api_chat_slot_create

        state = _crew_state(tmp_path)
        app = _make_app(state)
        app.router.add_post("/api/chat/slots", api_chat_slot_create)
        async with TestClient(TestServer(app)) as client:
            r = await client.post("/api/chat/slots", json={"mode": "bogus"})
            assert r.status == 400
            r2 = await client.post("/api/chat/slots", json={"mode": "crew"})
            assert r2.status in (200, 201)


class TestModeSwitchGuard:
    """Mode must not flip while crew work is in flight.

    `slot.running` stays false for the whole life of a crew session, because the
    work executes in SUBAGENTS — so the pre-existing guard alone let the mode
    change mid-flight and interleave two execution models in one session.
    """

    @pytest.mark.asyncio
    async def test_refuses_while_crew_has_live_work(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        state = _crew_state(tmp_path)
        state.subagents.has_pending_work_for = MagicMock(return_value=False)
        slot = state.get_or_create_slot("crew1", mode="crew")
        # The premise: crew work lives in subagents, so the slot itself never
        # looks busy — which is exactly why slot.running cannot be the guard.
        assert not slot.running
        state.crew.has_live_work = AsyncMock(return_value=True)
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/crew1/mode", json={"mode": ""})
            assert r.status == 409
        assert slot.mode == "crew"                # unchanged

    @pytest.mark.asyncio
    async def test_allows_when_crew_is_idle(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        state = _crew_state(tmp_path)
        state.subagents.has_pending_work_for = MagicMock(return_value=False)
        slot = state.get_or_create_slot("crew1", mode="crew")
        assert not slot.running
        state.crew.has_live_work = AsyncMock(return_value=False)
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/crew1/mode", json={"mode": ""})
            assert r.status == 200
        assert slot.mode == ""

    @pytest.mark.asyncio
    async def test_a_non_crew_slot_is_not_gated_on_crew_work(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # Regression: the guard first asked the orchestrator on EVERY switch, so
        # sessions that cannot have crew work (default / orchestrator mode) were
        # refused too. Only a slot already IN crew mode can have such work — one
        # entering it has none yet by construction.
        state = _crew_state(tmp_path)
        state.subagents.has_pending_work_for = MagicMock(return_value=False)
        slot = state.get_or_create_slot("plain1", mode="")
        state.crew.has_live_work = AsyncMock(return_value=True)
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/plain1/mode", json={"mode": "crew"})
            assert r.status == 200
        assert slot.mode == "crew"
        state.crew.has_live_work.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stand_in_crew_attribute_does_not_refuse(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # `state.crew` is gated by isinstance, matching gateway.py's own check on
        # this attribute. An identity check (`is not None`) would accept any
        # stand-in object, whose `has_live_work` then answers with something
        # truthy and refuses a switch that is perfectly fine — the same
        # truthiness-on-a-double mistake an earlier round of this PR already hit.
        state = _crew_state(tmp_path)
        state.subagents.has_pending_work_for = MagicMock(return_value=False)
        slot = state.get_or_create_slot("crew1", mode="crew")
        state.crew = MagicMock()          # not a CrewOrchestrator
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/crew1/mode", json={"mode": ""})
            assert r.status == 200
        assert slot.mode == ""

    @pytest.mark.asyncio
    async def test_entering_crew_is_refused_while_a_plain_subagent_runs(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # The asymmetry the previous guard missed: it gated the whole check on
        # `slot.mode == "crew"`, so ENTERING crew mode skipped it — while a
        # plain-chat subagent was running on that very slot. Its completion
        # follows the default `_run_chat` path, so the session would end up
        # mixing main-agent and crew execution.
        state = _crew_state(tmp_path)
        slot = state.get_or_create_slot("plain2", mode="")
        state.subagents.has_pending_work_for = MagicMock(return_value=True)
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/plain2/mode", json={"mode": "crew"})
            assert r.status == 409
        assert slot.mode == ""
        state.subagents.has_pending_work_for.assert_called_with("dashboard:plain2")

    @pytest.mark.asyncio
    async def test_background_work_check_fails_closed(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        state = _crew_state(tmp_path)
        state.get_or_create_slot("plain3", mode="")
        state.subagents.has_pending_work_for = MagicMock(side_effect=RuntimeError("down"))
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/plain3/mode", json={"mode": "crew"})
            assert r.status == 409

    @pytest.mark.asyncio
    async def test_fails_closed_when_the_orchestrator_raises(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        # An orchestrator that cannot answer is not permission to flip the mode.
        state = _crew_state(tmp_path)
        state.subagents.has_pending_work_for = MagicMock(return_value=False)
        slot = state.get_or_create_slot("crew1", mode="crew")
        assert not slot.running
        state.crew.has_live_work = AsyncMock(side_effect=RuntimeError("store unreadable"))
        async with TestClient(TestServer(_mode_app(state))) as client:
            r = await client.patch("/api/chat/slots/crew1/mode", json={"mode": ""})
            assert r.status == 409
        assert slot.mode == "crew"
