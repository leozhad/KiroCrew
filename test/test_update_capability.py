"""One derivation of install shape → update capability, and its anchoring.

The detection this module replaces was ``os.path.exists(".git")`` in three
places. That accepted any ``.git`` entry, and — worse for anything that then runs
``git reset`` — a check anchored only by exit status accepts a directory whose
ANCESTOR is a repository. A venv nested in a project tree, or a home directory
that is itself a dotfiles repo, would classify as a git install.
"""

from __future__ import annotations

import subprocess

import pytest

from kiro_crew.platform import update_capability
from kiro_crew.platform.update_capability import derive_capability, is_git_worktree


def _init_repo(path) -> None:
    subprocess.run(
        ["git", "init", "-q"], cwd=str(path), check=True, capture_output=True, timeout=30
    )


def _commit(path) -> None:
    """A commit, so ``git worktree add`` has something to check out."""
    (path / "seed.txt").write_text("seed\n", encoding="utf-8")
    env = [
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=Test",
        "-c",
        "commit.gpgsign=false",
    ]
    subprocess.run(
        ["git", *env, "add", "seed.txt"], cwd=str(path), check=True, capture_output=True, timeout=30
    )
    subprocess.run(
        ["git", *env, "commit", "-qm", "seed"],
        cwd=str(path),
        check=True,
        capture_output=True,
        timeout=30,
    )


class TestIsGitWorktree:
    def test_accepts_a_repository_root(self, tmp_path):
        _init_repo(tmp_path)
        assert is_git_worktree(str(tmp_path)) is True

    def test_rejects_a_directory_whose_ancestor_is_a_repository(self, tmp_path):
        # The hazard the anchor exists for: a wheel install living inside someone
        # else's checkout must not be treated as a git install, or the git apply
        # path would reset a tree that has nothing to do with this install.
        _init_repo(tmp_path)
        nested = tmp_path / "venvs" / "crew-venv"
        nested.mkdir(parents=True)
        assert is_git_worktree(str(nested)) is False

    def test_rejects_a_stray_git_entry(self, tmp_path):
        # git refuses to answer for a junk `.git`, which is indeterminate rather
        # than "no", so the fallback decides — and the fallback asks for a real
        # repository's markers, not merely the entry's presence. Both paths refuse
        # it, so the apply path never runs `git reset --hard` here.
        (tmp_path / ".git").write_text("not a gitlink\n", encoding="utf-8")
        assert is_git_worktree(str(tmp_path)) is False

    def test_the_fallback_rejects_a_git_directory_with_no_HEAD(self, tmp_path, monkeypatch):
        # A directory named `.git` is not a repository. Reading it as one would
        # hand the git apply path a tree with nothing to reset.
        (tmp_path / ".git").mkdir()

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is False

    def test_the_fallback_accepts_a_linked_worktree_pointer_file(self, tmp_path, monkeypatch):
        # A linked worktree and a submodule keep a `gitdir:` pointer FILE at their
        # root; the fallback has to recognise it or those shapes lose their update
        # path whenever git cannot answer.
        gitdir = tmp_path / "store" / ".git" / "worktrees" / "x"
        gitdir.mkdir(parents=True)
        linked = tmp_path / "linked"
        linked.mkdir()
        (linked / ".git").write_text(f"gitdir: {gitdir}\n", encoding="utf-8")

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(linked)) is True

    def test_the_fallback_rejects_a_pointer_that_points_nowhere(self, tmp_path, monkeypatch):
        # A file that merely BEGINS with the marker is as stray as one that does
        # not: git cannot operate on it, so the apply path must not be handed it.
        (tmp_path / ".git").write_text("gitdir: /definitely/not/here\n", encoding="utf-8")

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is False

    def test_the_fallback_requires_the_marker_at_the_start(self, tmp_path, monkeypatch):
        # git requires the pointer to start the file. Tolerating a leading newline
        # would let a crafted or corrupted file read as a checkout.
        gitdir = tmp_path / "store"
        gitdir.mkdir()
        (tmp_path / ".git").write_text(f"\ngitdir: {gitdir}\n", encoding="utf-8")

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is False

    def test_the_fallback_tolerates_a_byte_order_mark(self, tmp_path, monkeypatch):
        # Some Windows git builds write a BOM, and str.strip() does not remove it —
        # refusing it would take the update path away from a real worktree.
        gitdir = tmp_path / "store"
        gitdir.mkdir()
        (tmp_path / ".git").write_text(f"\ufeffgitdir: {gitdir}\r\n", encoding="utf-8")

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is True

    def test_the_fallback_accepts_a_relative_pointer(self, tmp_path, monkeypatch):
        # `git init --separate-git-dir` and submodules can write a relative target,
        # resolved against the worktree root.
        (tmp_path / "store").mkdir()
        (tmp_path / ".git").write_text("gitdir: store\n", encoding="utf-8")

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is True

    def test_rejects_a_directory_that_is_not_a_repository(self, tmp_path):
        assert is_git_worktree(str(tmp_path)) is False

    def test_accepts_a_linked_worktree(self, tmp_path):
        # A linked worktree stores ``.git`` as a FILE holding a gitdir pointer.
        # ``--show-toplevel`` answers with the worktree's own root, so it needs no
        # special case — and this is the case a naive is_dir() check broke.
        main = tmp_path / "main"
        main.mkdir()
        _init_repo(main)
        _commit(main)
        linked = tmp_path / "linked"
        subprocess.run(
            ["git", "worktree", "add", "-q", str(linked)],
            cwd=str(main),
            check=True,
            capture_output=True,
            timeout=60,
        )
        assert (linked / ".git").is_file()
        assert is_git_worktree(str(linked)) is True

    def test_empty_root_is_not_a_worktree(self):
        assert is_git_worktree("") is False

    def test_nonexistent_root_is_not_a_worktree(self, tmp_path):
        assert is_git_worktree(str(tmp_path / "nope")) is False

    def test_a_missing_git_binary_does_not_take_away_a_real_checkout(
        self, tmp_path, monkeypatch
    ):
        # The regression this guards: a wheel-shaped answer for a real checkout
        # would refuse `POST /api/update` with 409 and leave the boot-time apply
        # unreachable — an install that could update before could not after.
        _init_repo(tmp_path)

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(tmp_path)) is True

    def test_a_timed_out_probe_does_not_take_away_a_real_checkout(
        self, tmp_path, monkeypatch
    ):
        # A stale network mount can hang `rev-parse`. Reading the timeout as "not
        # a checkout" would misclassify the install shape.
        _init_repo(tmp_path)

        def _hang(*_a, **_k):
            raise subprocess.TimeoutExpired(cmd="git", timeout=10)

        monkeypatch.setattr(update_capability.subprocess, "run", _hang)
        assert is_git_worktree(str(tmp_path)) is True

    def test_a_nul_byte_in_the_root_is_refused_not_raised(self, tmp_path):
        # subprocess raises ValueError rather than OSError for an embedded NUL;
        # letting it escape would propagate out of the whole derivation.
        assert is_git_worktree(str(tmp_path) + "\x00evil") is False

    def test_an_unresolvable_answer_does_not_take_away_a_real_checkout(
        self, tmp_path, monkeypatch
    ):
        # Octal-escaped or MSYS-form output cannot be resolved to a directory by
        # this process; that is indeterminate, not a negative answer.
        _init_repo(tmp_path)

        class _Proc:
            returncode = 0
            stdout = "/c/nonexistent/msys/form/path\n"

        monkeypatch.setattr(update_capability.subprocess, "run", lambda *a, **k: _Proc())
        assert is_git_worktree(str(tmp_path)) is True

    def test_ancestor_capture_is_still_rejected_through_the_fallback(
        self, tmp_path, monkeypatch
    ):
        # The fallback stays ANCHORED at the root, so the nested case it exists to
        # reject is rejected on that path too.
        _init_repo(tmp_path)
        nested = tmp_path / "venvs" / "crew-venv"
        nested.mkdir(parents=True)

        def _missing(*_a, **_k):
            raise FileNotFoundError("git")

        monkeypatch.setattr(update_capability.subprocess, "run", _missing)
        assert is_git_worktree(str(nested)) is False

    def test_a_git_location_env_var_cannot_redirect_the_answer(
        self, tmp_path, monkeypatch
    ):
        # GIT_DIR alone makes rev-parse answer for another repository. Honouring
        # it would classify a non-checkout as a checkout, and the apply path would
        # then run `git reset --hard` against a tree that is not this install.
        real = tmp_path / "real"
        real.mkdir()
        _init_repo(real)
        not_a_checkout = tmp_path / "elsewhere"
        not_a_checkout.mkdir()
        monkeypatch.setenv("GIT_DIR", str(real / ".git"))
        monkeypatch.setenv("GIT_WORK_TREE", str(real))
        assert is_git_worktree(str(not_a_checkout)) is False

    def test_a_relative_root_is_not_resolved_against_the_process_cwd(
        self, tmp_path, monkeypatch
    ):
        # `git -C foo` would resolve against the gateway's working directory.
        _init_repo(tmp_path)
        monkeypatch.chdir(tmp_path)
        (tmp_path / "sub").mkdir()
        assert is_git_worktree("sub") is False

    def test_a_root_that_looks_like_an_option_is_refused(self, tmp_path):
        # Passed to `-C`, a leading dash would be read as an option.
        assert is_git_worktree("--upload-pack=touch /tmp/x") is False


class TestDeriveCapability:
    @pytest.mark.parametrize("dist", ["dmg", "appimage"])
    def test_desktop_defers_to_its_own_updater(self, dist):
        capability = derive_capability(install_root="", dist=dist)
        assert capability.managed_by == "electron"
        assert capability.unavailable_reason == "managed_by_app"
        assert capability.defers is True
        # The gateway's own apply endpoint is git-only, so it must not claim the
        # capability just because the surrounding app has it.
        assert capability.can_apply is False

    def test_container_is_supported_but_cannot_apply(self):
        capability = derive_capability(install_root="", dist="docker")
        assert capability.managed_by == "container"
        # supported, so the UI may still show version drift.
        assert capability.supported is True
        assert capability.can_apply is False
        assert capability.remediation is not None
        assert capability.remediation["kind"] == "image_pull"

    def test_a_checkout_can_apply_in_process(self, tmp_path):
        _init_repo(tmp_path)
        capability = derive_capability(install_root=str(tmp_path), dist="source")
        assert capability.managed_by == "git"
        assert capability.can_apply is True
        assert capability.remediation is not None
        assert capability.remediation["command"] == "kirocrew update"

    @pytest.mark.parametrize("dist", ["wheel", "source"])
    def test_feed_checkable_shapes_share_one_capability(self, dist, tmp_path):
        # `source` is what an unstamped wheel reports, so both must answer alike
        # or every already-released CLI install falls outside the contract.
        capability = derive_capability(install_root=str(tmp_path), dist=dist)
        assert capability.managed_by == "kirocrew"
        assert capability.can_apply is False
        assert capability.remediation is not None
        command = capability.remediation["command"]
        assert "--proto '=https'" in command
        assert "--channel " in command

    def test_a_desktop_stamp_wins_over_a_checkout(self, tmp_path):
        # A bundle ships this backend inside itself; being pointed at a checkout
        # does not move ownership of its bytes to that checkout.
        _init_repo(tmp_path)
        capability = derive_capability(install_root=str(tmp_path), dist="dmg")
        assert capability.managed_by == "electron"

    def test_install_root_defaults_to_the_project_env(self, tmp_path, monkeypatch):
        _init_repo(tmp_path)
        monkeypatch.setenv("KIROCREW_PROJECT_DIR", str(tmp_path))
        assert derive_capability(dist="source").managed_by == "git"

    def test_to_dict_carries_the_whole_contract_half(self, tmp_path):
        contract = derive_capability(install_root=str(tmp_path), dist="wheel").to_dict()
        assert set(contract) == {
            "supported",
            "managed_by",
            "mode",
            "can_download",
            "can_apply",
            "requires_restart",
            "unavailable_reason",
            "remediation",
        }
        # state/progress describe a drain lifecycle that does not exist yet, and
        # shipping them as constants would advertise transitions nothing emits.
        assert "state" not in contract
        assert "progress" not in contract
