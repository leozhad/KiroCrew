"""Tests for the cycle-8 review findings.

Both are cases where a guard admitted the worst input as if it were safe: a component
root that resolves to the data home ITSELF passed the containment check, and a selective
bundle was indistinguishable to an older restore from a complete one.
"""

from __future__ import annotations

import json
import os
import tarfile

import pytest
from test_snapshot import _setup_fake_kirocrew

from kiro_crew import snapshot as snap


@pytest.fixture
def home(tmp_path, monkeypatch):
    d = tmp_path / "home"
    d.mkdir()
    monkeypatch.setenv("KIROCREW_HOME", str(d))
    _setup_fake_kirocrew(d)
    return d


class TestARootResolvingToTheHomeItselfIsRefused:
    """`workspace/memory -> ..` resolves to the data home. The old predicate allowed
    `resolved == base`, so the "component tree" became the whole home and staging swept
    `.env`, `config.json` and `sel_hmac.key` into an archive meant to carry memory."""

    @pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
    def test_a_link_to_the_home_is_refused(self, home):
        target = home / "workspace" / "memory"
        import shutil

        shutil.rmtree(target)
        target.symlink_to("..", target_is_directory=True)
        assert snap.safe_tree_root(target, what="component root") is None

    @pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
    def test_the_home_itself_is_refused_directly(self, home):
        assert snap.safe_tree_root(home, what="component root") is None

    @pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
    def test_secrets_do_not_reach_the_archive_through_such_a_link(self, home, tmp_path):
        """The consequence, asserted end to end rather than at the predicate."""
        (home / ".env").write_text("TELEGRAM_TOKEN=should-never-be-archived\n")
        import shutil

        target = home / "workspace" / "memory"
        shutil.rmtree(target)
        target.symlink_to("..", target_is_directory=True)

        out = tmp_path / "out"
        snap.snapshot_main([str(out), "--components", "memory"])
        bundles = list(out.glob("*.tar.gz"))
        if not bundles:
            return  # refused outright, which is also acceptable
        with tarfile.open(bundles[0]) as tf:
            names = tf.getnames()
        assert not any(n.endswith("/.env") for n in names), (
            "the data home's .env reached the archive through a link root"
        )

    @pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
    def test_a_contained_link_is_still_allowed(self, home):
        """The predicate is containment, not "is this a link" -- a link pointing
        somewhere else INSIDE the home stays legitimate."""
        other = home / "workspace" / "elsewhere"
        other.mkdir(parents=True)
        import shutil

        target = home / "workspace" / "knowledge"
        shutil.rmtree(target)
        target.symlink_to(other, target_is_directory=True)
        assert snap.safe_tree_root(target, what="component root") == target


class TestASelectiveBundleIsRefusedByOlderRestores:
    """A released restore never reads the manifest's component map and moves each live
    core file out before checking the archive has a replacement. It DOES require the
    extracted root to start with `kirocrew-snapshot-`, so a different name turns silent
    data relocation into a clean refusal on versions already in the wild."""

    def _root_of(self, bundle, tmp_path, tag):
        work = tmp_path / f"x-{tag}"
        work.mkdir()
        with tarfile.open(bundle) as tf:
            tf.extractall(work)
        return next(d for d in work.iterdir() if d.is_dir()).name

    def test_a_selective_bundle_root_is_named_partial(self, home, tmp_path):
        out = tmp_path / "out"
        assert snap.snapshot_main([str(out), "--components", "memory"]) == 0
        bundle = next(out.glob("*.tar.gz"))
        root = self._root_of(bundle, tmp_path, "sel")
        assert root.startswith("kirocrew-partial-"), root
        assert not root.startswith("kirocrew-snapshot-")

    def test_a_complete_bundle_keeps_the_original_root(self, home, tmp_path):
        out = tmp_path / "out"
        assert snap.snapshot_main([str(out)]) == 0
        bundle = next(out.glob("*.tar.gz"))
        root = self._root_of(bundle, tmp_path, "full")
        assert root.startswith("kirocrew-snapshot-"), root

    def test_the_tarball_name_is_unchanged_so_listing_and_pruning_still_work(
        self, home, tmp_path
    ):
        """Only the inner root carries the marker; rotation globs the tarball name."""
        out = tmp_path / "out"
        assert snap.snapshot_main([str(out), "--components", "memory"]) == 0
        assert list(out.glob("kirocrew-snapshot-*.tar.gz")), (
            "a partial bundle became invisible to --list and pruning"
        )

    def test_this_version_still_restores_a_partial_bundle(self, home, tmp_path):
        md = home / "workspace" / "memory"
        (md / "preferences.md").write_text("original")
        out = tmp_path / "out"
        assert snap.snapshot_main([str(out), "--components", "memory"]) == 0
        bundle = next(out.glob("*.tar.gz"))
        (md / "preferences.md").write_text("changed")
        rc = snap.restore_main(
            [str(bundle), "--mode", "replace", "--force", "--components", "memory"]
        )
        assert rc == 0
        assert (md / "preferences.md").read_text() == "original"

    def test_the_manifest_still_records_the_component_map(self, home, tmp_path):
        out = tmp_path / "out"
        assert snap.snapshot_main([str(out), "--components", "memory"]) == 0
        bundle = next(out.glob("*.tar.gz"))
        work = tmp_path / "man"
        work.mkdir()
        with tarfile.open(bundle) as tf:
            tf.extractall(work)
        root = next(d for d in work.iterdir() if d.is_dir())
        man = json.loads((root / "MANIFEST.json").read_text())
        assert man["version"] == 3
        assert "memory" in man["components"]
