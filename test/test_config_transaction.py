"""Concurrent config updates no longer lose each other (issue #2147).

The load-bearing property is that **the call sites did not change**. Every test here
drives the plain read-then-write pattern the repo already uses::

    data = read_config_for_update(path)
    data["k"] = v
    write_config_atomically(path, data)

and asserts that a concurrent writer's change survives. Before this change that pattern
lost one of the two updates; the fix is inside the two functions, so all 12 sites that
use it are covered without being touched.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

import pytest

from kiro_crew.config.loader import (
    ConfigBusyError,
    _apply_delta,
    config_fingerprint,
    config_transaction,
    read_config_for_update,
    write_config_atomically,
)


@pytest.fixture()
def cfg(tmp_path, monkeypatch) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("KIROCREW_HOME", str(home))
    path = home / "config.json"
    path.write_text(json.dumps({"timezone": "UTC"}))
    return path


def _read(path: Path) -> dict:
    return json.loads(path.read_text())


def _rmw(path: Path, key: str, value, *, hold: float = 0.0) -> None:
    """Exactly the pattern the repo's 12 read-modify-write sites use, unchanged."""
    data = read_config_for_update(path)
    if hold:
        time.sleep(hold)  # widen the window that used to cause the clobber
    data[key] = value
    write_config_atomically(path, data)


class TestTheUnchangedPatternIsNowSafe:
    def test_two_concurrent_updates_both_survive(self, cfg) -> None:
        """The exact interleaving that used to destroy one of the two updates."""
        both_read = threading.Barrier(2)

        def updater(key: str) -> None:
            data = read_config_for_update(cfg)
            both_read.wait(timeout=5)  # force both to hold the SAME snapshot
            data[key] = "set"
            write_config_atomically(cfg, data)

        a = threading.Thread(target=updater, args=("from_a",))
        b = threading.Thread(target=updater, args=("from_b",))
        a.start()
        b.start()
        a.join(15)
        b.join(15)

        final = _read(cfg)
        assert final.get("from_a") == "set", "thread A's update was lost"
        assert final.get("from_b") == "set", "thread B's update was lost"
        assert final["timezone"] == "UTC"

    def test_twelve_concurrent_updaters_all_land(self, cfg) -> None:
        n = 12
        ready = threading.Barrier(n)

        def updater(i: int) -> None:
            ready.wait(timeout=15)
            _rmw(cfg, f"k{i}", i, hold=0.01)

        threads = [threading.Thread(target=updater, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)

        final = _read(cfg)
        missing = [f"k{i}" for i in range(n) if f"k{i}" not in final]
        assert not missing, f"lost updates from {missing}"

    def test_a_nested_section_merges_key_by_key(self, cfg) -> None:
        """Two writers inside the same section must not replace each other's subtree."""
        cfg.write_text(json.dumps({"dashboard": {"url": "http://x"}}))
        both_read = threading.Barrier(2)

        def updater(key: str, value: str) -> None:
            data = read_config_for_update(cfg)
            both_read.wait(timeout=5)
            data.setdefault("dashboard", {})[key] = value
            write_config_atomically(cfg, data)

        a = threading.Thread(target=updater, args=("theme", "dark"))
        b = threading.Thread(target=updater, args=("locale", "ja"))
        a.start()
        b.start()
        a.join(15)
        b.join(15)

        dash = _read(cfg)["dashboard"]
        assert dash.get("theme") == "dark", "A's nested key was lost"
        assert dash.get("locale") == "ja", "B's nested key was lost"
        assert dash["url"] == "http://x", "the untouched nested key was dropped"


class TestTheDeltaSemantics:
    def test_an_untouched_key_keeps_the_newer_value(self) -> None:
        """The property the whole merge rests on."""
        snapshot = {"a": 1, "b": 2}
        desired = {"a": 1, "b": 99}  # caller changed only b
        base = {"a": 42, "b": 2}  # someone else changed a meanwhile
        assert _apply_delta(base, snapshot, desired) == {"a": 42, "b": 99}

    def test_a_deletion_is_replayed_as_a_deletion(self) -> None:
        assert _apply_delta({"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1}) == {"a": 1}

    def test_the_same_leaf_goes_to_the_caller(self) -> None:
        """Unavoidable, and identical to what two serialised writes would produce."""
        assert _apply_delta({"k": "theirs"}, {"k": "orig"}, {"k": "mine"}) == {"k": "mine"}

    def test_nesting_is_recursive_not_wholesale(self) -> None:
        snapshot = {"s": {"x": 1, "y": 2}}
        desired = {"s": {"x": 1, "y": 3}}
        base = {"s": {"x": 9, "y": 2, "z": 7}}
        assert _apply_delta(base, snapshot, desired) == {"s": {"x": 9, "y": 3, "z": 7}}

    def test_a_scalar_replacing_a_dict_is_taken_verbatim(self) -> None:
        assert _apply_delta({"s": {"x": 1}}, {"s": {"x": 1}}, {"s": 5}) == {"s": 5}


class TestAWriteWithNoMatchingRead:
    def test_a_full_payload_is_written_as_given(self, cfg) -> None:
        """`KiroCrewConfig.save()` dumps the whole model; there is no delta to replay."""
        write_config_atomically(cfg, {"only": "this"})
        assert _read(cfg) == {"only": "this"}

    def test_it_still_serialises_against_a_transaction(self, cfg) -> None:
        holding = threading.Event()
        release = threading.Event()

        def holder() -> None:
            with config_transaction(cfg):
                holding.set()
                release.wait(timeout=10)

        t = threading.Thread(target=holder)
        t.start()
        assert holding.wait(timeout=10)
        done = threading.Event()

        def writer() -> None:
            write_config_atomically(cfg, {"late": True})
            done.set()

        w = threading.Thread(target=writer)
        w.start()
        assert not done.wait(timeout=0.3), "the write did not wait for the lock"
        release.set()
        w.join(15)
        t.join(15)
        assert done.is_set()


class TestACorruptFileDoesNotStrandTheCaller:
    def test_an_unreadable_file_is_overwritten_rather_than_merged(self, cfg) -> None:
        """Replaying a delta onto unparseable bytes is impossible; refusing would strand.

        This is the pre-existing behaviour, kept deliberately: the caller already read a
        good snapshot, so writing their payload is the best available outcome.
        """
        data = read_config_for_update(cfg)
        data["mine"] = 1
        cfg.write_text("{ not json")
        write_config_atomically(cfg, data)
        assert _read(cfg)["mine"] == 1


class TestTheLockIsASidecarBesideTheTarget:
    def test_the_lock_file_is_not_the_config(self, cfg) -> None:
        _rmw(cfg, "x", 1)
        assert cfg.with_name(".config.json.lock").exists()

    def test_it_follows_a_symlinked_config(self, tmp_path, monkeypatch) -> None:
        """Symlinking config into a dotfiles repo must not silently disable locking.

        The directory holding the LINK can be read-only while the target is writable, so
        a sidecar beside the link would be uncreatable even though the write succeeds --
        every writer would then run unlocked.
        """
        real_dir = tmp_path / "dotfiles"
        real_dir.mkdir()
        target = real_dir / "config.json"
        target.write_text(json.dumps({"timezone": "UTC"}))
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("KIROCREW_HOME", str(home))
        link = home / "config.json"
        link.symlink_to(target)

        _rmw(link, "added", 1)

        assert (real_dir / ".config.json.lock").exists(), "lock did not follow the symlink"
        assert not (home / ".config.json.lock").exists(), "lock was placed beside the link"
        assert json.loads(target.read_text())["added"] == 1
        assert link.is_symlink(), "the symlink was replaced instead of followed"

    def test_two_writers_through_the_symlink_both_survive(self, tmp_path, monkeypatch):
        real_dir = tmp_path / "dotfiles"
        real_dir.mkdir()
        target = real_dir / "config.json"
        target.write_text(json.dumps({"timezone": "UTC"}))
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("KIROCREW_HOME", str(home))
        link = home / "config.json"
        link.symlink_to(target)

        ready = threading.Barrier(2)

        def updater(key: str) -> None:
            ready.wait(timeout=10)
            _rmw(link, key, "set", hold=0.02)

        a = threading.Thread(target=updater, args=("from_a",))
        b = threading.Thread(target=updater, args=("from_b",))
        a.start()
        b.start()
        a.join(30)
        b.join(30)

        final = json.loads(target.read_text())
        assert final.get("from_a") == "set" and final.get("from_b") == "set"


class TestTheFingerprintIsContentBased:
    def test_an_equal_length_write_is_detected(self, cfg) -> None:
        """`(mtime, size)` cannot see this; a content hash must."""
        cfg.write_text(json.dumps({"v": "aaa"}))
        before = config_fingerprint(cfg)
        cfg.write_text(json.dumps({"v": "bbb"}))
        assert len(cfg.read_text()) == len(json.dumps({"v": "aaa"}))
        assert config_fingerprint(cfg) != before

    def test_absence_is_not_an_error(self, tmp_path) -> None:
        assert config_fingerprint(tmp_path / "nope.json") is None


class TestTheExplicitTransactionStillWorks:
    """`config_transaction` remains available for code that wants to refuse rather than
    merge -- a caller whose new value depends on the old one in a way a key-level merge
    cannot express."""

    def test_a_conflicting_transaction_refuses(self, cfg) -> None:
        with config_transaction(cfg, required=False) as txn:
            data = txn.read()
            data["mine"] = 1
            cfg.write_text(json.dumps({"theirs": 1}))
            with pytest.raises(ConfigBusyError):
                txn.write(data)
        assert _read(cfg) == {"theirs": 1}

    def test_writing_without_reading_is_refused(self, cfg) -> None:
        with config_transaction(cfg) as txn:
            with pytest.raises(RuntimeError, match="before read"):
                txn.write({"anything": 1})

    def test_busy_is_an_oserror(self) -> None:
        assert issubclass(ConfigBusyError, OSError)


class TestTheEventLoopIsNotStalledMeaningfully:
    def test_a_contended_write_from_the_loop_costs_about_one_write(self, cfg) -> None:
        """No async migration is needed because the wait is the length of one write.

        The lock covers a re-read, a dict merge and a rename. This measures the loop
        being blocked while another thread holds the lock through exactly one write, and
        asserts the stall stays in the tens of milliseconds rather than seconds -- the
        threshold that would justify restructuring 12 handlers.
        """

        async def scenario() -> float:
            done = threading.Event()

            def other_writer() -> None:
                for _ in range(20):
                    _rmw(cfg, "other", time.time())
                done.set()

            t = threading.Thread(target=other_writer)
            t.start()
            worst = 0.0
            while not done.is_set():
                t0 = time.perf_counter()
                try:
                    _rmw(cfg, "mine", time.time())  # inline, as today's handlers do
                except ConfigBusyError:
                    # Expected under contention, and the point of this test is that the
                    # refusal is CHEAP: a refusal that took a second would stall the loop
                    # just as badly as the wait it replaced.
                    pass
                worst = max(worst, time.perf_counter() - t0)
                await asyncio.sleep(0)
            t.join(30)
            return worst

        worst_ms = asyncio.run(scenario()) * 1000
        assert worst_ms < 500, f"a contended inline write blocked the loop for {worst_ms:.0f}ms"


class TestTheEventLoopIsNeverSlept:
    """No `time.sleep` poll may run on the event loop.

    The repo's own `no-blocking-call-on-event-loop` rule. An on-loop caller therefore gets
    one non-blocking attempt and no wait.

    It must not then proceed unlocked, which is what an earlier revision of this change did
    on the theory that the delta merge made the lock optional. It does not: the merge base is
    the re-read, and the holder can land its own rename between that re-read and ours, so the
    bytes we merged from are already stale and the holder's update is overwritten. The
    contended on-loop write therefore RAISES rather than silently reverting a setting.
    """

    def test_a_contended_on_loop_write_does_not_sleep(self, cfg, monkeypatch) -> None:
        import kiro_crew.config.loader as loader

        slept: list[float] = []
        real_sleep = time.sleep
        monkeypatch.setattr(
            loader.time, "sleep", lambda d: slept.append(d) or real_sleep(0)
        )

        holding = threading.Event()
        release = threading.Event()

        def holder() -> None:
            with config_transaction(cfg):
                holding.set()
                release.wait(timeout=10)

        t = threading.Thread(target=holder)
        t.start()
        assert holding.wait(timeout=10)

        refused: list[BaseException] = []

        async def scenario() -> None:
            # Inline from a coroutine, exactly as today's handlers call it.
            data = read_config_for_update(cfg)
            data["from_loop"] = 1
            try:
                write_config_atomically(cfg, data)
            except ConfigBusyError as exc:
                refused.append(exc)

        try:
            asyncio.run(scenario())
        finally:
            release.set()
            t.join(10)

        assert slept == [], f"slept on the event loop: {slept}"
        assert refused, "a contended on-loop write must refuse, not proceed unlocked"
        assert "from_loop" not in _read(cfg), "the refused write must not have landed"

    def test_an_off_loop_write_still_waits_for_the_lock(self, cfg) -> None:
        """Off the loop there is no reason to give up the guarantee."""
        holding = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def holder() -> None:
            with config_transaction(cfg):
                holding.set()
                release.wait(timeout=10)

        def waiter() -> None:
            _rmw(cfg, "later", 1)
            finished.set()

        t = threading.Thread(target=holder)
        t.start()
        assert holding.wait(timeout=10)
        w = threading.Thread(target=waiter)
        w.start()
        assert not finished.wait(timeout=0.3), "the off-loop write did not wait"
        release.set()
        w.join(15)
        t.join(15)
        assert finished.is_set()


class TestSaveDoesNotRevertAConcurrentChange:
    """`KiroCrewConfig.save()` dumps the whole model, so it needs its own baseline.

    Without one there is no delta to replay and the dump overwrites whatever landed since
    this instance was loaded -- the exact scenario the review named: a CLI loads config, a
    dashboard toggle commits, the CLI saves, and the toggle is silently reverted.
    """

    def test_a_toggle_committed_after_load_survives_a_save(self, cfg) -> None:
        from kiro_crew.config.loader import KiroCrewConfig

        cfg.write_text(json.dumps({"timezone": "UTC", "auto_update": False}))
        loaded = KiroCrewConfig.load()  # the CLI's snapshot of the world

        # Meanwhile the dashboard commits an unrelated change.
        _rmw(cfg, "auto_update", True)

        loaded.timezone = "Asia/Tokyo"  # the CLI changes something else entirely
        loaded.save()

        final = _read(cfg)
        assert final["timezone"] == "Asia/Tokyo", "the CLI's own change was lost"
        assert final["auto_update"] is True, (
            "save() reverted a change committed after this instance was loaded"
        )

    def test_the_baseline_is_recorded_on_the_instance(self, cfg) -> None:
        """Per-instance, not per-thread: a `load()` elsewhere on this thread must not be
        mistaken for this object's starting point."""
        from kiro_crew.config.loader import KiroCrewConfig

        a = KiroCrewConfig.load()
        _rmw(cfg, "changed_between", 1)
        b = KiroCrewConfig.load()
        assert a._loaded_from != b._loaded_from
        assert b._loaded_from is not None and "changed_between" in b._loaded_from

    def test_an_explicit_baseline_beats_the_thread_local_record(self, cfg) -> None:
        cfg.write_text(json.dumps({"a": 1, "b": 2}))
        read_config_for_update(cfg)  # leaves a thread-local record
        # A caller with its own baseline: it only ever knew about {"a": 1}.
        write_config_atomically(cfg, {"a": 9}, baseline={"a": 1})
        final = _read(cfg)
        assert final["a"] == 9, "the caller's change was not applied"
        assert final["b"] == 2, "a key outside the explicit baseline was dropped"


class TestTheBaselineIsShapeMatched:
    """A baseline only works if it is in the same shape as the payload.

    Two shapes exist in this repo: the raw parsed file, and the canonical `to_dict()` dump
    with a default filled in for every key the file omits. Diffing a canonical payload
    against a raw baseline makes each defaulted key look caller-modified, so it is replayed
    over whatever is on disk -- reintroducing the lost update this change exists to remove,
    on the majority of a partial config's keys.
    """

    def test_load_records_a_baseline_in_the_payload_shape(self, cfg) -> None:
        """The fixture's config has only `timezone`, so `auto_update` is a pure default."""
        from kiro_crew.config.loader import KiroCrewConfig

        loaded = KiroCrewConfig.load()
        baseline = loaded.baseline_for(cfg)

        assert baseline is not None
        assert "auto_update" in baseline, (
            "the baseline is missing a key the payload will carry, so that key would be "
            "replayed as though the caller had set it"
        )
        assert baseline["auto_update"] is True

    def test_save_keeps_a_concurrent_change_to_a_key_it_never_touched(self, cfg) -> None:
        """The reported chain, end to end.

        A partial config lacks `auto_update`; something else turns it off after this
        instance was loaded; this instance saves an unrelated edit. The concurrent value
        has to survive -- with a raw baseline the defaulted `True` was replayed over it.
        """
        from kiro_crew.config.loader import KiroCrewConfig

        loaded = KiroCrewConfig.load()

        # A concurrent writer -- a dashboard toggle -- lands after the load.
        data = _read(cfg)
        data["auto_update"] = False
        write_config_atomically(cfg, data)

        loaded.timezone = "Asia/Shanghai"
        loaded.save()

        on_disk = _read(cfg)
        assert on_disk["auto_update"] is False, "the save reverted a key it never touched"
        assert on_disk["timezone"] == "Asia/Shanghai", "the save's own edit did not land"

    def test_a_hand_built_canonical_payload_can_carry_the_same_baseline(self, cfg) -> None:
        """`kirocrew config set KEY VALUE` builds `save()`'s payload without calling it.

        It loads, sets one key on `to_dict()`, and writes. `baseline_for` is public so
        that site gets the same protection instead of falling back to a raw-shaped snapshot.
        """
        from kiro_crew.config.loader import KiroCrewConfig, stamp_config_meta

        loaded = KiroCrewConfig.load()
        payload = loaded.to_dict()
        payload["timezone"] = "Europe/Berlin"

        data = _read(cfg)
        data["auto_update"] = False
        write_config_atomically(cfg, data)

        write_config_atomically(
            cfg, stamp_config_meta(payload), baseline=loaded.baseline_for(cfg)
        )

        on_disk = _read(cfg)
        assert on_disk["auto_update"] is False
        assert on_disk["timezone"] == "Europe/Berlin"

    def test_a_raw_payload_against_a_canonical_baseline_would_delete_keys(self, cfg) -> None:
        """Why the baseline is per-instance instead of seeded into the read record.

        Seeding the thread-local with the canonical shape at load time would pair it with
        the NEXT write on that thread -- including the writers that build a raw dict
        (`config set --local`, `config set --file`). A key present in the canonical
        baseline and absent from a raw payload reads as "the caller deleted it", so the
        merge drops it from the file. This asserts that failure mode exists, which is the
        reason the pairing is explicit rather than ambient.
        """
        from kiro_crew.config.loader import _apply_delta

        canonical_baseline = {"timezone": "UTC", "auto_update": True}
        raw_payload = {"timezone": "Europe/Berlin"}  # a raw writer's whole dict
        on_disk = {"timezone": "UTC", "auto_update": False}

        merged = _apply_delta(on_disk, canonical_baseline, raw_payload)

        assert "auto_update" not in merged, (
            "if this ever stops deleting the key, the shape-mismatch hazard is gone and "
            "an ambient baseline could be reconsidered"
        )


class TestAContendedWriteRefuses:
    def test_an_off_loop_contended_write_raises_after_its_wait(
        self, cfg, monkeypatch
    ) -> None:
        """Off the loop the wait still happens; a holder that never lets go still refuses.

        `ConfigBusyError` subclasses `OSError`, so the 10 write sites that already guard
        their write with `except OSError` degrade without a change.
        """
        holding = threading.Event()
        release = threading.Event()

        def holder() -> None:
            with config_transaction(cfg):
                holding.set()
                release.wait(timeout=10)

        import kiro_crew.config.loader as loader

        monkeypatch.setattr(loader, "_WRITE_LOCK_TIMEOUT", 0.05)

        t = threading.Thread(target=holder)
        t.start()
        assert holding.wait(timeout=10)
        try:
            data = read_config_for_update(cfg)
            data["mine"] = 1
            with pytest.raises(ConfigBusyError):
                write_config_atomically(cfg, data)
        finally:
            release.set()
            t.join(10)

        assert "mine" not in _read(cfg), "a refused write must leave the file alone"


class TestABaselineOnlyAppliesToItsOwnFile:
    """Found by two round-trip tests, and it has a production form.

    The delta's premise is "a key the caller did not change is already correct on disk". That
    holds only for the file the baseline was captured from, while it still exists. Write to a
    different path -- or to one that has since been deleted -- and every unchanged key is
    skipped against nothing, so it silently disappears from the result.
    """

    def test_a_missing_target_is_written_whole(self, cfg) -> None:
        from kiro_crew.config.loader import KiroCrewConfig

        loaded = KiroCrewConfig.load()
        cfg.unlink()  # the file goes away after the load

        loaded.save()

        on_disk = _read(cfg)
        assert on_disk["timezone"] == "UTC", "the recreated config lost a key it had loaded"
        assert "auto_update" in on_disk, "an unchanged key vanished instead of being written"

    def test_a_baseline_is_refused_for_a_different_path(self, cfg, tmp_path) -> None:
        from kiro_crew.config.loader import KiroCrewConfig

        loaded = KiroCrewConfig.load()

        assert loaded.baseline_for(cfg) is not None, "its own path must still be accepted"
        assert loaded.baseline_for(tmp_path / "other.json") is None, (
            "a baseline from another file would skip every key it reads as unchanged"
        )
