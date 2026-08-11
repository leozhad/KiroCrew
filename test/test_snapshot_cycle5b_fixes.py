"""Tests for the cycle-5 review findings.

The important one is structural: the authorization used to live in the CLI wrapper, so
anything able to call the library directly could provision and record a destination with
no authorization at all. That is the claim "an agent cannot redirect the backup" failing
a fourth time by a fourth route -- after the registry file, the command that writes it,
and the forgeable TTY gate.
"""

from __future__ import annotations

import json
import os
import sys

import pytest
from test_snapshot_remote import FakeAws

from kiro_crew import platform_compat
from kiro_crew import snapshot_remote as remote
from kiro_crew.deploy import engine

ACCOUNT = "123456789012"
OTHER = "111122223333"
NO_POLICY = (255, "", "An error occurred (NoSuchBucketPolicy) when calling GetBucketPolicy")


def _fake() -> FakeAws:
    return FakeAws({
        "sts get-caller-identity": (0, ACCOUNT + "\n", ""),
        "s3api head-bucket": (1, "", "Not Found"),
        "s3api get-bucket-policy": NO_POLICY,
        "s3api get-bucket-tagging": (0, json.dumps(
            {"TagSet": [{"Key": "kirocrew:backup", "Value": "true"}]}), ""),
        "s3api get-bucket-encryption": (0, json.dumps({
            "ServerSideEncryptionConfiguration": {"Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}}), ""),
        "s3api get-bucket-versioning": (0, json.dumps({"Status": "Enabled"}), ""),
        "s3api get-public-access-block": (0, json.dumps({
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": True, "RestrictPublicBuckets": True}}), ""),
        "s3api get-bucket-lifecycle-configuration": (
            255, "", "NoSuchLifecycleConfiguration"),
    })


def _token(account=ACCOUNT, region="us-west-2", bucket=None):
    t = remote.authorization_token_path()
    t.parent.mkdir(parents=True, exist_ok=True)
    body = {"account": account, "region": region}
    if bucket:
        body["bucket"] = bucket
    t.write_text(json.dumps(body))
    return t


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("KIROCREW_HOME", str(tmp_path))
    return tmp_path


class TestTheLibraryItselfRefusesWithoutAuthorization:
    """The guard has to sit where the mutation is. A check in `backup_main` only
    protects callers who go through `backup_main`."""

    def test_calling_setup_directly_without_a_token_refuses(self, home, monkeypatch):
        fake = _fake()
        monkeypatch.setattr(engine, "run_aws", fake)
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "not authorized" in str(e.value)
        # And nothing was created or recorded.
        assert fake.argv_for("s3api create-bucket") == []
        assert not remote._registry_path().exists()

    def test_it_refuses_before_any_aws_mutation(self, home, monkeypatch):
        """`caller_account` is a read; the refusal must land before the first write."""
        fake = _fake()
        monkeypatch.setattr(engine, "run_aws", fake)
        with pytest.raises(remote.DestinationError):
            remote.setup_destination("p", "us-west-2")
        mutating = [c for c in fake.calls if any(
            v.startswith("put-") or v == "create-bucket" for v in c[:2])]
        assert mutating == [], f"a mutation ran before the refusal: {mutating}"

    def test_a_token_for_another_account_does_not_authorize_this_one(self, home, monkeypatch):
        _token(account=OTHER)
        monkeypatch.setattr(engine, "run_aws", _fake())
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "does not match" in str(e.value)

    def test_a_token_for_another_region_does_not_authorize_this_one(self, home, monkeypatch):
        _token(region="eu-west-1")
        monkeypatch.setattr(engine, "run_aws", _fake())
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "does not match" in str(e.value)

    def test_a_token_pinned_to_another_bucket_refuses(self, home, monkeypatch):
        _token(bucket="somebody-elses-bucket")
        monkeypatch.setattr(engine, "run_aws", _fake())
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "does not match" in str(e.value)

    def test_a_refused_attempt_does_not_burn_the_authorization(self, home, monkeypatch):
        """A mismatch must not consume it: the operator fixes the file and retries."""
        t = _token(account=OTHER)
        monkeypatch.setattr(engine, "run_aws", _fake())
        with pytest.raises(remote.DestinationError):
            remote.setup_destination("p", "us-west-2")
        assert t.exists(), "a refused attempt consumed the authorization"

    def test_an_unreadable_token_refuses(self, home, monkeypatch):
        t = remote.authorization_token_path()
        t.parent.mkdir(parents=True, exist_ok=True)
        t.write_text("{not json")
        monkeypatch.setattr(engine, "run_aws", _fake())
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "could not be read as JSON" in str(e.value)

    def test_a_valid_token_is_consumed_exactly_once(self, home, monkeypatch):
        t = _token()
        monkeypatch.setattr(engine, "run_aws", _fake())
        monkeypatch.setattr(engine, "harden_bucket", lambda *a, **k: None)
        remote.setup_destination("p", "us-west-2")
        assert not t.exists(), "the authorization survived its use"
        with pytest.raises(remote.DestinationError) as e:
            remote.setup_destination("p", "us-west-2")
        assert "not authorized" in str(e.value)


@pytest.mark.skipif(sys.platform == "win32", reason="asserts a POSIX mode")
class TestTheTrustAnchorIsOwnerOnly:
    """`os.chmod` is a POSIX-only guarantee. On Windows it leaves the DACL alone, so
    the record another user could replace is the one deciding where memory is sent."""

    def test_the_record_and_its_directory_are_owner_only(self, home, monkeypatch):
        _token()
        monkeypatch.setattr(engine, "run_aws", _fake())
        monkeypatch.setattr(engine, "harden_bucket", lambda *a, **k: None)
        remote.setup_destination("p", "us-west-2")
        reg = remote._registry_path()
        assert reg.is_file()
        assert oct(reg.stat().st_mode)[-3:] == "600"
        # The directory needs the execute bit or the file inside is unreachable.
        assert oct(reg.parent.stat().st_mode)[-3:] == "700"

    def test_the_directory_helper_is_used_not_the_file_one(self):
        """`restrict_to_owner` applies 0600, which on a DIRECTORY strips traversal and
        breaks the very write it is meant to protect. The first version of this fix did
        exactly that."""
        import inspect

        src = inspect.getsource(remote._save_destination)
        assert "make_owner_only_dir" in src
        assert "restrict_to_owner(str(path.parent))" not in src

    def test_the_temporary_file_is_locked_down_before_the_rename(self):
        import inspect

        src = inspect.getsource(remote._save_destination)
        lock_at = src.index("restrict_to_owner(str(tmp))")
        rename_at = src.index("os.replace(str(tmp)")
        assert lock_at < rename_at, (
            "the record is briefly readable under inherited permissions"
        )

    def test_the_helper_exists_and_creates_a_traversable_dir(self, tmp_path):
        d = tmp_path / "anchor"
        platform_compat.make_owner_only_dir(d)
        assert d.is_dir()
        assert oct(d.stat().st_mode)[-3:] == "700"
        (d / "x").write_text("reachable")
        assert (d / "x").read_text() == "reachable"


class TestTheImportsAreAtModuleScope:
    """`backup_cli`'s `sel` import is at module scope: hoisting it imports cleanly with
    no circular import, and cost nothing measurable.

    `cli.py`'s `backup_main` import is deliberately NOT hoisted. Two review rounds asked
    for opposite things here -- one to hoist it, the next to keep it inside the branch
    because `kirocrew gateway` goes through this module and would pay for
    `backup_cli` -> `snapshot_remote` -> `deploy.engine` on every boot. The second
    position matches the file's own convention (all ten dispatch imports are local) and
    the gateway's interest, so that is the one encoded here.
    """

    def test_backup_cli_imports_sel_at_module_scope(self):
        import inspect

        from kiro_crew import backup_cli

        src = inspect.getsource(backup_cli)
        assert "from kiro_crew.sel import SecurityEvent, sel" in src.split("def ")[0]
        assert "    from kiro_crew.sel import" not in src

    def test_cli_resolves_backup_main_inside_its_branch(self):
        import inspect

        from kiro_crew import cli

        src = inspect.getsource(cli)
        assert "from kiro_crew.backup_cli import backup_main" in src
        # Not at module scope: the gateway boot path must not import it.
        assert "\nfrom kiro_crew.backup_cli import backup_main" not in src

    def test_it_matches_every_sibling_in_that_dispatch_block(self):
        """The convention is the argument, so pin the convention rather than the line."""
        import inspect
        import re

        from kiro_crew import cli

        src = inspect.getsource(cli)
        local_dispatch = re.findall(r"\n        from kiro_crew\.[\w.]+ import ", src)
        assert len(local_dispatch) >= 10, (
            f"expected the lazy-dispatch convention, found {len(local_dispatch)} imports"
        )

    def test_os_is_still_available_where_the_audit_uses_it(self):
        assert callable(os.urandom)
