"""KiroCrew snapshot and restore — portable state management."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import tarfile
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath

from kiro_crew import platform_compat
from kiro_crew import snapshot_remote as remote
from kiro_crew.deploy import profiles as _profiles

try:
    import pysqlite3 as sqlite3
except ImportError:
    import sqlite3

try:
    from kiro_crew.config.loader import DASHBOARD_PORT as _DASHBOARD_PORT
except Exception:  # pragma: no cover - optional during early/standalone import
    _DASHBOARD_PORT = int(os.environ.get("KIROCREW_PORT", 5476))


# Files that must always have 0o600 permissions in snapshots and on restore.
SECURITY_SENSITIVE_FILES: frozenset = frozenset({"sel_hmac.key", "telemetry_salt"})

# Files that must NEVER ride a snapshot: sel_hmac.key is regenerated on restore
# so audit-log HMACs stay bound to the host that wrote them.
#
# This set is matched by BASENAME inside `_data_filter`, which runs over the
# ENTIRE tar — including the staged workspace/, plan_memory/ and skills/ trees.
# So any name added here also silently drops a USER file that happens to share
# it. Keep the set minimal for that reason.
#
# The beacon's per-install identity (beacon_install_id / beacon_last_sent) is
# deliberately NOT here: snapshot staging copies an explicit per-component file
# list (CORE_FILES) plus those three directories, and no component lists a beacon
# file, so a root beacon file is never staged in the first place. The
# id-cloning hazard is closed by that non-selection, not by a basename filter.
NEVER_SNAPSHOT_FILES: frozenset = frozenset({"sel_hmac.key"})


def _data_filter(info: tarfile.TarInfo, _dest: str = "") -> tarfile.TarInfo | None:
    """Equivalent to tarfile ``"data"`` filter (Python 3.12+), with 3.10 fallback.

    Also rejects path traversal, symlinks, and hardlinks to eliminate TOCTOU
    race between pre-scan and extraction.
    Excludes sel_hmac.key (must be regenerated on restore, not shipped).
    Security-sensitive files get 0o600 permissions.
    """
    # Reject path traversal. POSIX checks apply everywhere; the Windows-syntax
    # checks (backslash separators, drive letters — incl. the drive-RELATIVE
    # `C:foo` form is_absolute() misses, which resolves against the drive CWD
    # at extraction) apply ONLY when extracting on Windows, where tarfile
    # honors '\' as a native separator. They must NOT run on POSIX: ':' and
    # '\' are legal characters in Linux/macOS filenames, so a workspace file
    # named `a:1` or `notes..\old` would be silently dropped from a
    # Linux-to-Linux restore.
    name = info.name
    traversal = (
        name.startswith("/")
        or ".." in PurePosixPath(name).parts
        or PurePosixPath(name).is_absolute()
    )
    if not traversal and platform_compat.IS_WINDOWS:
        traversal = (
            name.startswith("\\")
            or ".." in PureWindowsPath(name).parts
            or PureWindowsPath(name).is_absolute()
            or bool(PureWindowsPath(name).drive)
        )
    if traversal:
        print(f"⚠️  Rejecting path traversal entry: {info.name}")
        return None
    # Reject symlinks and hardlinks
    if info.issym() or info.islnk():
        print(f"⚠️  Rejecting symlink/hardlink entry: {info.name}")
        return None
    # Never ship these — each must be regenerated on the restoring host.
    basename = PurePosixPath(info.name).name
    if basename in NEVER_SNAPSHOT_FILES:
        return None
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    # Security-sensitive files get restricted permissions
    if not info.isdir() and basename in SECURITY_SENSITIVE_FILES:
        info.mode = 0o600
    else:
        info.mode = 0o755 if info.isdir() else 0o644
    return info


def _default_snapshot_dir() -> str:
    """Return snapshot directory from config, falling back to <config_dir>/snapshots."""
    try:
        from kiro_crew.config.loader import KiroCrewConfig

        d = KiroCrewConfig.load().snapshot_dir
        if d:
            return str(Path(d).expanduser())
    except Exception:
        pass
    try:
        from kiro_crew.config.paths import config_dir

        return str(config_dir() / "snapshots")
    except Exception:
        return str(Path.home() / ".kiro" / "crew" / "snapshots")


def _audit(event_type: str, resources: str) -> None:
    """Emit a SEL audit event for snapshot/restore operations."""
    try:
        from kiro_crew.sel import SecurityEvent, sel

        sel().log(
            SecurityEvent(
                event_id=os.urandom(8).hex(),
                timestamp=datetime.now(timezone.utc).isoformat(),
                event_type=event_type,
                caller_identity=os.environ.get("USER", "unknown"),
                agent="kirocrew",
                source="cli",
                operation=event_type,
                outcome="completed",
                resources=resources,
            )
        )
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning("SEL audit event '%s' failed: %s", event_type, e)


class Purpose(str, Enum):
    """Why a bundle exists. Decides which components may ride in it.

    A bundle's purpose is not cosmetic: ``BACKUP`` restores onto a replacement host
    the operator already controls, so it wants the credentials that make recovery
    turnkey. ``SHARE`` leaves the operator's control, so a component that carries
    credential material must not ride in one. Recording the purpose in the manifest
    is what lets a reader of a bundle know which of the two they are holding.
    """

    BACKUP = "backup"
    SHARE = "share"


class SecretPolicy(str, Enum):
    """A component's declaration about the credential material it carries.

    Every component must declare one. There is deliberately no default: a component
    added without a declaration is refused at staging (see :func:`resolve_components`)
    rather than inheriting whichever value happens to be permissive.

    ``UNRESOLVED`` means nobody has established that the component is safe to hand to
    another person. It rides a ``BACKUP`` bundle unchanged and is refused outright in
    a ``SHARE`` bundle.

    ``SHARE_SAFE`` means someone has, and **no component claims it today**. That is
    not an oversight. Whether a component is safe to share is a question about
    CONTENT, not structure: a workspace file, a skill, a cron's ``env`` map, a
    notification body or a pasted lesson can each contain a token, and staging cannot
    tell. Two components were flipped from a guessed-safe value to ``UNRESOLVED``
    during review of this change, one at a time, before the pattern was obvious. The
    value is kept so the seam has both sides and the gate stays exercised; the first
    genuinely certified component will arrive with the redaction work that earns it.
    """

    SHARE_SAFE = "share-safe"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class ComponentSpec:
    """What one component stages, and its credential declaration.

    ``files`` are data-home-relative files copied individually; ``trees`` are
    data-home-relative directories copied wholesale. A ``.db`` file in ``files`` is
    copied through the SQLite backup API rather than the filesystem, so a live
    gateway holding the database open still yields a consistent copy.
    """

    policy: SecretPolicy
    help: str
    files: tuple[str, ...] = ()
    trees: tuple[str, ...] = ()


# The single source of truth for what a bundle can contain. Both the staging path
# and the restore path read this, so a component cannot be stageable but
# unrestorable (or the reverse) without the mismatch being visible here.
COMPONENTS: dict[str, ComponentSpec] = {
    # Self-contained on purpose: lessons and semantic/episodic recall live in the two
    # databases, but the markdown half of memory lives under workspace/. Naming those
    # trees here means restoring memory does not require restoring the whole
    # workspace, which on a real install is two orders of magnitude larger.
    "memory": ComponentSpec(
        # UNRESOLVED like every other component: a lesson or a note can contain a
        # token somebody pasted, and staging cannot tell. Memory is NOT redacted in a
        # backup -- that is the whole point of backing it up -- this declaration only
        # governs whether it may ride a bundle that leaves the operator's control.
        policy=SecretPolicy.UNRESOLVED,
        help=(
            "memory.db, memory_index.db (semantic, episodic, lessons), "
            "workspace/memory/ (preferences, projects, history), workspace/knowledge/"
        ),
        files=("memory.db", "memory_index.db"),
        trees=("workspace/memory", "workspace/knowledge"),
    ),
    "crons": ComponentSpec(
        # `CronJob.env` is a persisted dict of per-job environment variables
        # (cron.py), so a job passing an API token carries it in crons.json.
        policy=SecretPolicy.UNRESOLVED,
        help="crons.json (scheduled jobs)",
        files=("crons.json",),
    ),
    "config": ComponentSpec(
        policy=SecretPolicy.UNRESOLVED,
        help="config.json, session_map.json, hooks.json, project_dir, workspace_dir",
        files=("config.json", "session_map.json", "hooks.json", "project_dir", "workspace_dir"),
    ),
    "skills": ComponentSpec(
        policy=SecretPolicy.UNRESOLVED,
        help="skills/ directory",
        trees=("skills",),
    ),
    "workspace": ComponentSpec(
        policy=SecretPolicy.UNRESOLVED,
        help="workspace/, plan_memory/ directories",
        trees=("workspace", "plan_memory"),
    ),
    "notifications": ComponentSpec(
        policy=SecretPolicy.UNRESOLVED,
        help="notifications.jsonl (notification history)",
        files=("notifications.jsonl",),
    ),
    "security": ComponentSpec(
        policy=SecretPolicy.UNRESOLVED,
        help="telemetry_salt (sel_hmac.key excluded — regenerated on restore)",
        files=("telemetry_salt",),
    ),
}


class ComponentRefused(Exception):
    """A requested component cannot ride a bundle of the requested purpose."""


def resolve_components(requested: list[str] | None, purpose: Purpose) -> list[str]:
    """Return the component names to stage, or raise :class:`ComponentRefused`.

    ``None`` means every component. The two refusals are the seam's whole point:
    an unknown name never silently stages nothing, and an ``UNRESOLVED`` component
    never rides a ``SHARE`` bundle just because nobody wrote the policy down.
    """
    names = list(COMPONENTS) if requested is None else requested
    unknown = [c for c in names if c not in COMPONENTS]
    if unknown:
        raise ComponentRefused(
            f"unknown component(s): {', '.join(sorted(unknown))} "
            f"(known: {', '.join(sorted(COMPONENTS))})"
        )
    if purpose is Purpose.SHARE:
        blocked = [c for c in names if COMPONENTS[c].policy is SecretPolicy.UNRESOLVED]
        if blocked:
            raise ComponentRefused(
                f"component(s) {', '.join(sorted(blocked))} have no share-safe policy, "
                f"so they cannot ride a '{Purpose.SHARE.value}' bundle. Whether a "
                f"component is safe to hand to someone else is a question about its "
                f"CONTENT — a workspace file, a skill, a cron's env map or a pasted "
                f"lesson can each hold a token — and no component is certified yet. "
                f"Use --purpose {Purpose.BACKUP.value} to back up onto a host you "
                f"control."
            )
    return names


# Derived views, kept because callers and tests read them as the component tables.
CORE_FILES: dict[str, tuple[str, ...]] = {
    name: spec.files for name, spec in COMPONENTS.items() if spec.files
}

COMPONENT_HELP = {name: spec.help for name, spec in COMPONENTS.items()}

VALID_COMPONENTS: tuple[str, ...] = tuple(COMPONENTS)


def _mc_dir() -> Path:
    # Use the shared resolver so snapshot/restore honor the documented
    # KIROCREW_HOME override (and the same ~/.kiro/crew default) as every other
    # module — not an undocumented KIROCREW_DIR, which would make snapshots
    # silently target the real home even when state was relocated.
    from kiro_crew.config.loader import config_dir

    return config_dir()


# SQLite sidecars are excluded from every staged tree. They describe the SOURCE
# database's in-flight transaction state; shipping them next to a consistent backup
# copy would invite the restoring host to replay a journal that does not match it.
#
# Not redundant with _restage_databases, though it looks that way: re-opening a
# staged database makes SQLite discard the copied sidecars as a side effect, so for a
# real database either mechanism alone appears to work. This glob is what covers the
# case _restage_databases SKIPS — a file named .db that SQLite cannot open, whose
# stray sidecars would otherwise ride.
_DB_SIDECAR_GLOBS = ("*.db-wal", "*.db-shm", "*.db-journal", "*.sqlite3-wal", "*.sqlite3-shm")

# Suffixes treated as SQLite databases when found inside a staged tree.
_DB_SUFFIXES = (".db", ".sqlite", ".sqlite3")


def _restage_databases(src_dir: Path, dst_dir: Path) -> None:
    """Re-copy every SQLite database under *src_dir* through the backup API.

    The plain tree copy already placed a byte copy there; this replaces it with a
    consistent one. Done as a second pass rather than by filtering the tree walk, so
    the copy logic stays in one place and a database newly appearing in a tree is
    covered without anyone remembering to register it.

    A file whose suffix says database but which SQLite cannot open is left as the byte
    copy already made: a non-database that happens to be named ``.db`` is still the
    operator's file and must ride the bundle.
    """
    for src in sorted(src_dir.rglob("*")):
        if not src.is_file() or src.is_symlink() or src.suffix not in _DB_SUFFIXES:
            continue
        dst = dst_dir / src.relative_to(src_dir)
        if not dst.parent.is_dir():
            continue
        try:
            with (
                # `as_uri()` percent-escapes the path. Interpolating it raw meant a
                # POSIX filename containing `?` or `#` was parsed as the start of the
                # URI's query or fragment, truncating the path — so the copy would open
                # a DIFFERENT database and store it under the requested name.
                closing(
                    sqlite3.connect(f"{src.resolve().as_uri()}?mode=ro", uri=True)
                ) as src_conn,
                closing(sqlite3.connect(str(dst))) as dst_conn,
            ):
                src_conn.backup(dst_conn)
        except sqlite3.Error:
            print(f"⚠️  {src.name} is not a readable SQLite database — copied as a plain file")


def safe_tree_root(root: Path, *, what: str, home: Path | None = None) -> Path | None:
    """Return *root* if reading from or writing to it stays inside the data home.

    THE chokepoint for component tree roots. Three separate sites touch them — the
    staging walk, the replace pass and the merge pass — and each was found to
    dereference a link independently, so the check lives here once.

    The predicate is **containment of the fully resolved path**, not "is this node a
    link". Checking the node was tried first and was not enough: a link nested under
    the root, or an ancestor of it, escapes the same way while every individual node
    the loop inspects looks ordinary. ``Path.resolve()`` follows every link in the
    path, so comparing the result against the resolved home answers the question that
    actually matters — can this write land outside the directory we are allowed to
    touch — and covers roots, ancestors, descendants and Windows junctions in one
    predicate.

    ``home`` defaults to the resolved data home. A root that does not exist yet is
    fine: its resolved parent is what gets checked, because that is where a create
    would land.
    """
    base = (home or _mc_dir()).resolve()
    try:
        resolved = root.resolve()
    except OSError as e:  # broken link, ELOOP, permission on an ancestor
        print(f"⚠️  Skipping unresolvable {what} ({e}): {root}")
        return None
    # STRICT descendant. The earlier form allowed `resolved == base`, and that was the
    # worst case rather than a safe one: a link like `workspace/memory -> ..` resolves to
    # the data home itself, so the "component tree" became the whole home and staging
    # copied `.env`, `config.json` and `sel_hmac.key` into an archive that is meant to
    # carry memory. No declared component tree is ever the home — every one of them is a
    # strict descendant — so equality has no legitimate case to serve.
    if base not in resolved.parents:
        print(f"⚠️  Skipping {what} that resolves outside {base}: {root} -> {resolved}")
        return None
    return root


def _fsize(p: Path) -> int:
    try:
        return p.stat().st_size
    except OSError:
        return 0


def _want(components: list[str] | None, name: str) -> bool:
    return components is None or name in components


def _list_components() -> None:
    print("Available components:")
    for k, v in COMPONENT_HELP.items():
        print(f"  {k:16s} {v}")
    print("\nCombine with commas: --components memory,crons,skills")


def _copytree_safe(src: Path, dst: Path, **kwargs) -> None:
    """copytree that skips links to prevent sensitive file leakage.

    Uses :func:`platform_compat.is_link_or_junction`, not ``os.path.islink``, because
    ``islink`` returns False for a Windows directory junction: a junction nested inside
    a component tree would be treated as a real directory and copied THROUGH, pulling
    whatever it points at (a credential directory, say) into the bundle and then to S3.
    ``safe_tree_root`` guards the tree's ROOT; this guards every node below it, and the
    two must agree on what counts as a link or the weaker one decides.
    """
    outer_ignore = kwargs.pop("ignore", None)

    def _ignore_links(directory, contents):
        skipped = {
            name
            for name in contents
            if platform_compat.is_link_or_junction(os.path.join(directory, name))
        }
        for name in skipped:
            print(f"⚠️  Skipping link in source tree: {os.path.join(directory, name)}")
        if outer_ignore:
            skipped |= set(outer_ignore(directory, contents))
        return skipped

    shutil.copytree(str(src), str(dst), ignore=_ignore_links, **kwargs)


def _clear_tree_root(d: Path) -> None:
    """Remove a live tree root so incoming files can replace it.

    THE chokepoint for this operation, because the naive form is subtly wrong in a way
    that bites mid-restore. ``d.is_dir()`` follows a symlink, so a root that is a link to
    a directory answers True — and ``shutil.rmtree`` then refuses a symlink with OSError.
    By the time this runs, databases have already been replaced, so an exception here
    leaves the operator half-restored: the worst outcome available.

    A link is removed as a link; only a real directory is walked.
    """
    if platform_compat.is_link_or_junction(d):
        platform_compat.unlink_link_or_junction(str(d))
    elif d.is_dir():
        shutil.rmtree(str(d))


def _copy_tree_no_overwrite(src: Path, dst: Path, home: Path | None = None) -> None:
    """Merge *src* into *dst* without overwriting, refusing to write outside *home*.

    ``safe_tree_root`` validates the destination ROOT, but this function walks below it
    and the DESTINATION side can contain links too. A nested link under ``dst`` -- say
    ``workspace/memory/history`` pointing at a directory outside the data home -- would
    otherwise be followed on write, and the merge would deposit restored files wherever
    it aimed. Guarding the source alone is not enough: the write target is the dangerous
    end here.

    Each target is therefore checked by *resolved containment* against the resolved
    home, which is the same predicate ``safe_tree_root`` uses. When *home* is None the
    containment check is skipped, which is only appropriate for staging into a
    freshly-created temporary tree.
    """
    resolved_home = home.resolve() if home is not None else None

    def _inside(target: Path) -> bool:
        if resolved_home is None:
            return True
        # Climb to the nearest existing ancestor, because the target itself usually does
        # not exist yet. `exists()` alone is not enough to decide "not there": it FOLLOWS
        # links, so a BROKEN symlink answers False and the climb would step straight past
        # it — then `mkdir(parents=True)` meets the dangling link and raises
        # FileExistsError, aborting a merge that has already replaced the databases.
        # A link is therefore rejected outright, dangling or not: it is not a directory we
        # are willing to write through.
        probe = target
        while probe != probe.parent:
            if platform_compat.is_link_or_junction(probe):
                return False
            if probe.exists():
                break
            probe = probe.parent
        try:
            probe.resolve().relative_to(resolved_home)
        except (ValueError, OSError):
            return False
        return True

    for item in src.rglob("*"):
        # Same reasoning as _copytree_safe: a junction is not an islink, and this path
        # walks INTO directories, so an unguarded junction would be descended.
        if platform_compat.is_link_or_junction(item):
            continue
        target = dst / item.relative_to(src)
        if not _inside(target):
            print(f"⚠️  Skipping merge target outside the data home: {target}")
            continue
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif item.is_file() and not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(item), str(target))


# ── Snapshot ──────────────────────────────────────────────────────────────────


class UnsafeComponentRoot(Exception):
    """A selected component's tree root does not resolve inside the data home.

    Raised rather than skipped: a bundle whose manifest claims a component it could not
    read is a backup that lies about its contents, which is worse than a refusal.
    """


class DestinationUnresolved(Exception):
    """No usable AWS profile is registered for an off-host destination."""


def _resolve_aws_profile(explicit: str | None) -> tuple[str, str]:
    """Return (profile, region) for the backup destination.

    Reuses the deploy module's profile-name registry rather than introducing a
    second place to configure AWS identity. Only names are stored there — never
    keys — and the CLI resolves the actual credentials.

    An unregistered name resolves to ``None`` there, which is a refusal, not a
    fallback: a backup must run under a profile the operator registered, so the
    caller reports it instead of silently reaching for some other identity.
    """
    resolved = _profiles.resolve_profile(explicit or "")
    if resolved is None:
        raise DestinationUnresolved(
            f"no registered AWS profile for {explicit!r}"
            if explicit
            else "no default AWS profile is registered"
        )
    return resolved


def _upload_bundle(outfile: Path, args: argparse.Namespace) -> int:
    """Send a written bundle to the configured off-host destination.

    Deliberately does NOT accept a bucket from the caller. Provisioning is a separate,
    explicit act (`kirocrew backup setup`) and this path writes only to what that
    recorded — which is what removes "decide whether an arbitrary bucket is safe" from
    every backup run.
    """
    try:
        dest = remote.load_destination()
    except remote.DestinationNotConfigured as e:
        print(f"❌ {e}")
        return 1
    except remote.DestinationError as e:
        print(f"❌ {e}")
        return 1
    try:
        profile, _region = _resolve_aws_profile(getattr(args, "aws_profile", None))
    except (DestinationUnresolved, OSError, ValueError) as e:
        print(f"❌ Could not resolve an AWS profile: {e}")
        return 1

    print(f"☁️  Uploading to {dest.url_for(outfile.name)} (profile {profile})")
    try:
        url = remote.upload(outfile, dest, profile)
    except remote.UPLOAD_FAILURES as e:
        # Every failure the AWS path can raise becomes one controlled message. A
        # traceback here would be indistinguishable from a crash, and the operator still
        # has a usable local bundle either way.
        print(f"❌ {type(e).__name__}: {e}")
        print(f"   The local bundle is intact at {outfile}")
        return 1
    print(f"✅ Uploaded: {url}")
    _audit("snapshot_uploaded", url)
    return 0


def snapshot_main(
    argv: list[str] | None = None, *, parsed: argparse.Namespace | None = None
) -> int:
    if parsed is None:
        p = argparse.ArgumentParser(
            prog="kirocrew-snapshot",
            description="Create a portable .tar.gz snapshot of KiroCrew state.",
        )
        p.add_argument("output_dir", nargs="?", default=_default_snapshot_dir())
        p.add_argument("--keep", type=int, default=7)
        p.add_argument("--list", action="store_true", dest="list_snapshots")
        p.add_argument("--components", default=None)
        p.add_argument("--purpose", default=Purpose.BACKUP.value)
        p.add_argument("--to-s3", action="store_true", dest="to_s3")
        p.add_argument("--to", default=None, help=argparse.SUPPRESS)
        p.add_argument("--aws-profile", default=None, dest="aws_profile")
        parsed = p.parse_args(argv)
    args = parsed

    if args.keep <= 0:
        print(f"❌ --keep value must be a positive integer, got: {args.keep}")
        return 1

    # `--to s3://…` was replaced by a provisioned destination. Fail loudly rather than
    # letting an old invocation write the bundle into a local directory named `s3:`.
    if getattr(args, "to", None):
        print(
            f"❌ --to is no longer accepted (you passed {args.to!r}).\n"
            f"   A backup now writes only to a destination you provision once:\n"
            f"     kirocrew backup setup\n"
            f"     kirocrew snapshot --components memory --to-s3"
        )
        return 1

    out = Path(args.output_dir or _default_snapshot_dir())

    if args.list_snapshots:
        if not out.is_dir():
            print(f"No snapshots found in {out}")
            return 0
        snaps = sorted(
            out.glob("kirocrew-snapshot-*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True
        )
        for s in snaps:
            print(s)
        if not snaps:
            print(f"No snapshots found in {out}")
        return 0

    mc = _mc_dir()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Resolve the seam before doing any work: a refusal here must cost nothing and
    # must not leave a half-written bundle behind.
    try:
        purpose = Purpose(getattr(args, "purpose", None) or Purpose.BACKUP.value)
    except ValueError:
        print(
            f"❌ Unknown --purpose: {args.purpose} "
            f"(known: {', '.join(p.value for p in Purpose)})"
        )
        return 1
    requested = (
        [c.strip() for c in args.components.split(",") if c.strip()]
        if getattr(args, "components", None)
        else None
    )
    try:
        selected = resolve_components(requested, purpose)
    except ComponentRefused as e:
        print(f"❌ {e}")
        return 1

    # A SELECTIVE bundle gets a root directory name that older restores refuse.
    #
    # This is the one guard available against a hazard that cannot be fixed in the
    # consumer, because the consumer has already shipped: a released `kirocrew restore`
    # never reads the manifest's component map, and `_backup_and_copy` moves each live
    # core file out before checking whether the archive has a replacement. Point an old
    # restore at a memory-only bundle and it relocates `crons.json`, `config.json`, the
    # notifications store and the security files -- including `sel_hmac.key` -- into
    # `pre-restore-<ts>/`, then prints a tick for each one.
    #
    # What the released code DOES do is require the extracted root to start with
    # `kirocrew-snapshot-`, and print "Invalid snapshot format" and exit 1 otherwise --
    # before touching anything. So naming a partial bundle's root differently converts
    # silent data relocation into a clean refusal on every version already in the wild.
    #
    # The TARBALL keeps the familiar name: `--list`, pruning and `--keep` all glob
    # `kirocrew-snapshot-*.tar.gz`, and a partial bundle still needs to be found and
    # rotated by them. Only the directory inside it carries the marker.
    complete = set(selected) == set(COMPONENTS)
    name = f"kirocrew-snapshot-{ts}"
    root_name = name if complete else f"kirocrew-partial-{ts}"

    # Pre-flight size estimate
    if mc.is_dir():
        total_bytes = sum(
            f.stat().st_size for f in mc.rglob("*") if f.is_file() and not f.is_symlink()
        )
        total_mb = total_bytes / (1024 * 1024)
        if total_mb > 500:
            print(f"⚠️  {mc} is {total_mb:.0f} MB — snapshot may be large and slow")

    # WAL checkpoint
    if (mc / "memory.db").is_file():
        try:
            with closing(sqlite3.connect(str(mc / "memory.db"))) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            print(
                "⚠️  WAL checkpoint failed (DB may be locked by gateway). "
                "The backup API still produces a consistent copy."
            )

    try:
        with tempfile.TemporaryDirectory() as work:
            stage = Path(work) / root_name
            # Unconditionally, before any component runs. A file-only selection whose
            # files are all absent (a fresh home with `--components crons`) stages
            # nothing, and the manifest write below would then fail on a missing
            # directory — an empty bundle is a valid outcome, a crash is not.
            stage.mkdir(parents=True, exist_ok=True)

            # Files. A `.db` goes through the SQLite backup API so a live gateway holding
            # it open still yields a consistent copy; everything else is a plain copy.
            for comp in selected:
                for f in COMPONENTS[comp].files:
                    src = mc / f
                    if not src.is_file():
                        continue
                    if os.path.islink(src):
                        print(f"⚠️  Skipping symlinked core file: {src}")
                        continue
                    dst = stage / f
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    if f.endswith(".db"):
                        with (
                            closing(sqlite3.connect(str(src))) as src_conn,
                            closing(sqlite3.connect(str(dst))) as dst_conn,
                        ):
                            src_conn.backup(dst_conn)
                    else:
                        shutil.copy2(str(src), str(dst))

            # Trees. Selections overlap by design — `memory` names workspace/memory while
            # `workspace` names the whole tree — so staging is idempotent: dirs_exist_ok
            # plus copy2 means the second write of a path is identical to the first.
            for comp in selected:
                for tree in COMPONENTS[comp].trees:
                    src_dir = mc / tree
                    dst_dir = stage / tree
                    # An unsafe root must FAIL the snapshot, not be skipped. Skipping it
                    # produced the worst possible artefact: a bundle whose manifest declares
                    # `memory` while the markdown trees are silently absent, so the operator
                    # believes they are covered and only discovers otherwise when they try to
                    # recover. A backup that lies about its contents is worse than no backup.
                    #
                    # safe_tree_root returns None only for an unsafe or unresolvable root —
                    # a root that simply does not exist yet is fine — so this cannot fire on
                    # a fresh data home.
                    if safe_tree_root(src_dir, what="component root") is None:
                        raise UnsafeComponentRoot(
                            f"component {comp!r} names the tree {tree!r}, which does not "
                            f"resolve inside the data home. Refusing to write a bundle that "
                            f"would claim to contain {comp!r} without it — inspect that path "
                            f"(it is usually a symlink) and re-run."
                        )
                    if not src_dir.is_dir():
                        continue
                    dst_dir.mkdir(parents=True, exist_ok=True)
                    _copytree_safe(
                        src_dir,
                        dst_dir,
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(
                            "hygiene_data", "insert_facts*.py", *_DB_SIDECAR_GLOBS
                        ),
                    )
                    # A tree can contain a LIVE SQLite database (workspace/knowledge holds
                    # knowledge.db, whose WAL is routinely megabytes). A filesystem copy
                    # reads the db and its sidecars at different instants, so a concurrent
                    # write yields a restored database missing committed rows or corrupt
                    # outright. Re-copy each one through the backup API, which takes a
                    # consistent snapshot, and leave the -wal/-shm out entirely: they
                    # describe the source's transaction state, not the copy's.
                    _restage_databases(src_dir, dst_dir)

            # Manifest
            ws_files = sum(1 for _ in (stage / "workspace").rglob("*") if _.is_file())
            pm_files = sum(1 for _ in (stage / "plan_memory").rglob("*") if _.is_file())
            sk_dir = stage / "skills"
            sk_count = sum(1 for _ in sk_dir.iterdir() if _.is_dir()) if sk_dir.is_dir() else 0
            manifest = {
                "version": 3,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "hostname": socket.gethostname(),
                "user": os.environ.get("USER", "unknown"),
                "kirocrew_dir": str(mc),
                "purpose": purpose.value,
                # Which components rode, and what each declared about credential material.
                # A reader of the bundle can answer "is this safe to hand to someone"
                # from the manifest instead of inferring it from the file list.
                "components": {c: COMPONENTS[c].policy.value for c in selected},
                "contents": {
                    "memory_db": _fsize(stage / "memory.db"),
                    "memory_index_db": _fsize(stage / "memory_index.db"),
                    "crons_json": _fsize(stage / "crons.json"),
                    "config_json": _fsize(stage / "config.json"),
                    "notifications_jsonl": _fsize(stage / "notifications.jsonl"),
                    "workspace_files": ws_files,
                    "plan_memory_files": pm_files,
                    "skill_count": sk_count,
                },
            }
            (stage / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))

            # Tarball — write to temp file and rename atomically to avoid corrupt partials
            out.mkdir(parents=True, exist_ok=True)
            outfile = out / f"{name}.tar.gz"
            tmp_tar = outfile.with_suffix(".tar.gz.tmp")
            try:
                with tarfile.open(str(tmp_tar), "w:gz") as tar:
                    tar.add(str(stage), arcname=root_name, filter=_data_filter)
                tmp_tar.rename(outfile)
            except BaseException:
                tmp_tar.unlink(missing_ok=True)
                raise

    except UnsafeComponentRoot as e:
        # A selected component's tree does not resolve inside the data home. Refuse
        # with a message rather than a traceback: every other refusal on this path
        # already does, and a traceback here reads as a crash — which would invite
        # the operator to retry rather than to look at the path.
        print(f"❌ {e}")
        return 1
    sz = outfile.stat().st_size
    # restrict_to_owner (fail-loud), NOT chmod_safe: this tarball can contain
    # sel_hmac.key (see the warning below). chmod_safe swallows OSError and
    # would let the snapshot land group/world-readable while still printing
    # success. Fail loudly instead — better to abort than ship a
    # secret-bearing archive under-protected. POSIX applies chmod 0o600;
    # Windows applies an owner-only DACL via icacls.
    # Unlink+reraise on failure so the "abort" the comment promises actually
    # removes the exposed artifact — otherwise the tarball would sit on disk
    # with the destination's inherited DACL after a Python traceback.
    try:
        platform_compat.restrict_to_owner(str(outfile))
    except OSError:
        outfile.unlink(missing_ok=True)
        raise
    human = f"{sz // 1024}K" if sz < 1024 * 1024 else f"{sz / 1024 / 1024:.1f}M"
    print(f"✅ Snapshot created: {outfile} ({human})")

    _audit("snapshot_created", f"{outfile} ({human})")

    # Off-host destination. Deliberately after the local bundle exists and is
    # owner-restricted: a failed upload must leave a usable local backup behind
    # rather than nothing.
    upload_rc = 0
    if getattr(args, "to_s3", False):
        upload_rc = _upload_bundle(outfile, args)

    # Prune. This runs even when the upload failed, because --keep is a promise about
    # local disk and a persistently failing destination must not turn a daily backup
    # into an unbounded pile of bundles — the disk fills, and then the snapshot that
    # would have worked cannot be written either.
    snaps = sorted(
        out.glob("kirocrew-snapshot-*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True
    )
    for old in snaps[args.keep :]:
        old.unlink()
        print(f"🗑  Pruned: {old.name}")

    remaining = len(list(out.glob("kirocrew-snapshot-*.tar.gz")))
    print(f"📦 Snapshots in {out}: {remaining} (keep={args.keep})")

    if upload_rc != 0:
        return upload_rc
    return 0


# ── Restore ───────────────────────────────────────────────────────────────────


class ManifestUnreadable(Exception):
    """A bundle's manifest exists but cannot be trusted to say what it carries."""


def _manifest_components(snap: Path) -> list[str] | None:
    """Return the component names a bundle's manifest says it carries.

    ``None`` means "this bundle predates the component map", which is the signal to
    keep the historical all-components behaviour — such a bundle really did hold every
    component. That fallback is reserved for a manifest that is READABLE and simply
    has no map; a manifest that cannot be parsed raises :class:`ManifestUnreadable`
    instead, because "we could not read it" must never resolve to the most destructive
    interpretation available.

    Names not in :data:`COMPONENTS` are dropped: the manifest travels with the bundle,
    so a restore must not act on a name this build cannot resolve. The remaining list
    is returned even when EMPTY — that means "declares components, none understood
    here", which must restore nothing.
    """
    mf = snap / "MANIFEST.json"
    if not mf.is_file():
        return None
    try:
        manifest = json.loads(mf.read_text(encoding="utf-8"))
    except (ValueError, OSError) as e:
        raise ManifestUnreadable(f"MANIFEST.json is present but unreadable: {e}") from e
    if not isinstance(manifest, dict):
        raise ManifestUnreadable("MANIFEST.json is not an object")
    comps = manifest.get("components")
    if comps is None:
        return None
    if not isinstance(comps, dict):
        raise ManifestUnreadable(f"MANIFEST.json 'components' is {type(comps).__name__}, not a map")
    known = [c for c in comps if c in COMPONENTS]
    dropped = sorted(set(comps) - set(known))
    if dropped:
        print(f"⚠️  Manifest names unknown component(s), ignoring: {', '.join(dropped)}")
    return known


def _print_manifest(snap: Path) -> None:
    mf = snap / "MANIFEST.json"
    if not mf.is_file():
        return
    try:
        m = json.loads(mf.read_text())
        print("📋 Snapshot info:")
        print(f"  Created: {m.get('created_at', 'unknown')}")
        print(f"  From: {m.get('user', 'unknown')}@{m.get('hostname', 'unknown')}")
        # Absent in bundles written before the purpose seam existed. Say so rather
        # than printing a default, so an old bundle is never read as a declared one.
        print(f"  Purpose: {m.get('purpose') or 'undeclared (pre-seam bundle)'}")
        comps = m.get("components")
        if isinstance(comps, dict) and comps:
            print(f"  Components: {', '.join(f'{k} [{v}]' for k, v in sorted(comps.items()))}")
        c = m.get("contents", {})
        print(f"  Memory DB: {c.get('memory_db', 0) // 1024} KB")
        print(f"  Crons: {c.get('crons_json', 0) // 1024} KB")
        print(f"  Workspace files: {c.get('workspace_files', 0)}")
        print(f"  Skills: {c.get('skill_count', 0)}")
        print(f"  Notifications: {c.get('notifications_jsonl', 0) // 1024} KB")
        print(f"  Plan memory files: {c.get('plan_memory_files', 0)}")
    except Exception as e:
        print(f"  (Could not read manifest: {e})")


_MERGE_ALLOWED_TABLES = frozenset(
    {
        "semantic_memory",
        "episodic_memories",
        "knowledge_facts",
        "knowledge_edges",
    }
)
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str) -> str:
    """Validate a SQL identifier against allowlist pattern. Raises ValueError if invalid."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _merge_memory(src_db: Path, dst_db: Path) -> None:
    # Integrity check on source DB before ATTACH.
    #
    # `closing`, not a bare `with sqlite3.connect(...)`: a connection used as a context
    # manager commits or rolls back the TRANSACTION and leaves the connection OPEN. The
    # handle it kept on src_db made the caller's extraction temp dir undeletable on
    # Windows, which is how this surfaced.
    try:
        with closing(sqlite3.connect(str(src_db))) as check_conn:
            result = check_conn.execute("PRAGMA integrity_check;").fetchone()[0]
        if result != "ok":
            print(f"  ⚠️  Source DB integrity check failed: {result} — skipping merge")
            return
    except Exception as e:
        print(f"  ⚠️  Source DB unreadable: {e} — skipping merge")
        return

    conn = sqlite3.connect(str(dst_db))
    conn.execute("BEGIN")
    attached = False
    try:
        conn.execute("ATTACH DATABASE ? AS src", (str(src_db),))
        attached = True
        for table, cols, where in [
            (
                "semantic_memory",
                "key, value_json, confidence, source, created_at, updated_at, embedding",
                "WHERE is_deleted=0",
            ),
            (
                "episodic_memories",
                "id, conversation_id, text, embedding, tags, importance, created_at, last_accessed_at",
                "WHERE is_deleted=0",
            ),
            ("knowledge_facts", "subject, predicate, object, episode_id, created_at", ""),
            (
                "knowledge_edges",
                "source_key, target_key, relation, weight, metadata, created_at",
                "",
            ),
        ]:
            if table not in _MERGE_ALLOWED_TABLES:
                raise ValueError(f"Table {table!r} not in merge allowlist")
            for col in cols.split(", "):
                _validate_identifier(col.strip())
            try:
                before = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                conn.execute(
                    f"INSERT OR IGNORE INTO {table} ({cols}) "
                    f"SELECT {cols} FROM src.{table} {where}"
                )
                after = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                label = table.replace("_", " ").title()
                print(f"  {label} imported: {after - before}")
            except sqlite3.OperationalError as e:
                import logging

                logging.getLogger(__name__).warning("Skipping table %s: %s", table, e)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if attached:
            try:
                conn.execute("DETACH DATABASE src")
            except Exception:
                pass
        conn.close()


def _merge_crons(src_path: Path, dst_path: Path) -> None:
    src = json.loads(src_path.read_text())
    dst = json.loads(dst_path.read_text())
    existing = {j.get("name") for j in dst.get("jobs", [])}
    imported = 0
    for job in src.get("jobs", []):
        name = job.get("name")
        if not name or name in existing:
            continue
        job["id"] = hashlib.md5(f"{name}-imported".encode(), usedforsecurity=False).hexdigest()[:8]
        dst.setdefault("jobs", []).append(job)
        imported += 1
    dst_path.write_text(json.dumps(dst, indent=2))
    total = len(src.get("jobs", []))
    print(f"  Cron jobs imported: {imported} (skipped {total - imported} duplicates)")


def _merge_notifications(src_path: Path, dst_path: Path) -> None:
    existing: set[str] = set()
    with open(dst_path) as f:
        for line in f:
            try:
                existing.add(json.loads(line).get("ts") or line.strip())
            except (ValueError, TypeError):
                pass
    imported = 0
    with open(dst_path, "a") as out, open(src_path) as f:
        for line in f:
            try:
                key = json.loads(line).get("ts") or line.strip()
                if key not in existing:
                    out.write(line)
                    existing.add(key)
                    imported += 1
            except (ValueError, TypeError):
                pass
    print(f"  Notifications imported: {imported}")


def _backup_and_copy(mc: Path, backup: Path, snap: Path, component: str) -> None:
    for f in CORE_FILES.get(component, ()):
        if (mc / f).is_file():
            if os.path.islink(mc / f):
                print(f"⚠️  Skipping symlinked core file during backup: {mc / f}")
                continue
            shutil.move(str(mc / f), str(backup / f))
        if (snap / f).is_file():
            if os.path.islink(snap / f):
                print(f"⚠️  Skipping symlinked file from snapshot: {snap / f}")
                continue
            shutil.copy2(str(snap / f), str(mc / f))
            if component == "security":
                # restrict_to_owner (fail-loud), NOT chmod_safe (swallows OSError):
                # security files include sel_hmac.key. Mirrors the create path's
                # deliberate fail-loud lockdown — better to abort than silently
                # land a restored secret group/world-readable. POSIX applies
                # chmod 0o600; Windows applies an owner-only DACL via icacls.
                # Unlink the freshly
                # copied file on failure so the "abort" the comment promises
                # actually removes the exposed artifact — otherwise the
                # restored secret would sit under the destination-inherited
                # DACL after the OSError propagates out of _do_replace.
                try:
                    platform_compat.restrict_to_owner(str(mc / f))
                except OSError:
                    (mc / f).unlink(missing_ok=True)
                    raise


def _refuse_unsafe_destination_roots(mc: Path, components: list[str] | None) -> None:
    """Refuse before touching anything if a selected component's tree root is unsafe.

    Hoisted ahead of every mutation on purpose. Checking inside the per-tree loops was
    too late in the worst way: `_backup_and_copy` has already swapped the databases by
    then, so skipping an unsafe markdown tree left memory split between two versions —
    and the command still reported success. A partial restore reported as complete is
    the same lie as a partial backup reported as complete.

    Both restore modes call this. Merge is additive and destroys nothing, but a merge
    that silently omits a tree is still a merge that claims to have imported it.
    """
    offenders = []
    for comp in COMPONENTS:
        if not _want(components, comp):
            continue
        for tree in COMPONENTS[comp].trees:
            d = mc / tree
            if safe_tree_root(d, what="destination root") is None:
                offenders.append(f"{comp}:{tree}")
    if offenders:
        raise UnsafeComponentRoot(
            "these destination trees do not resolve inside the data home: "
            + ", ".join(offenders)
            + ". Nothing has been changed. Inspect those paths (usually a symlink) "
            "and re-run — restoring past them would leave memory split between the "
            "old and new versions while reporting success."
        )


def _do_replace(snap: Path, mc: Path, components: list[str] | None) -> None:
    _refuse_unsafe_destination_roots(mc, components)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = mc / f"pre-restore-{ts}"
    backup.mkdir(exist_ok=True)
    print("🔄 Replace mode — backing up current state...")

    # The memory trees are saved to the rollback directory HERE, before the loop below
    # swaps any database. An earlier revision computed these roots and copied them after
    # that loop, and the comment on the two-pass split even said so out loud — "the
    # databases were replaced before this block even started" — while only fixing the
    # tree-versus-tree ordering. Saving a tree after the databases have already moved
    # means a failure in this copy (a full disk is enough) leaves the rollback set
    # incomplete for state that is ALREADY replaced: memory half old, half new, with no
    # complete copy of either. A rollback set has to be finished before the first
    # mutation, not alongside it.
    #
    # Scoped by the same guard the replace pass uses: when `workspace` is also selected
    # its own pass owns these paths, and copying them twice would save the incoming tree
    # over the saved original.
    mem_roots: list[tuple[str, Path]] = []
    if _want(components, "memory") and not _want(components, "workspace"):
        for tree in COMPONENTS["memory"].trees:
            d = mc / tree
            if safe_tree_root(d, what="destination root") is None:
                continue
            mem_roots.append((tree, d))
        for tree, d in mem_roots:
            if d.is_dir():
                # No `dirs_exist_ok`: the rollback directory is named to the second, so
                # two restores inside one second resolve to the SAME directory. Merging
                # into it would blend two different pre-restore states into one rollback
                # set — the operator could not tell which files came from which restore,
                # and the set would roll back to neither. Colliding here raises, and
                # because this runs before the databases are swapped, the restore aborts
                # having changed nothing.
                _copytree_safe(d, backup / tree)

    for comp in ("memory", "crons", "config", "notifications", "security"):
        if _want(components, comp):
            _backup_and_copy(mc, backup, snap, comp)
            print(f"  ✅ {comp}")

    if _want(components, "workspace"):
        for dirname in ("workspace", "plan_memory"):
            d = mc / dirname
            if d.is_dir():
                _copytree_safe(d, backup / dirname, dirs_exist_ok=True)
            sd = snap / dirname
            if sd.is_dir():
                _clear_tree_root(d)
                _copytree_safe(sd, d)
        print("  ✅ workspace")

    if _want(components, "skills"):
        sk = mc / "skills"
        if sk.is_dir():
            _copytree_safe(sk, backup / "skills", dirs_exist_ok=True)
        snap_sk = snap / "skills"
        if snap_sk.is_dir():
            _clear_tree_root(sk)
            _copytree_safe(snap_sk, sk)
        print("  ✅ skills")

    if _want(components, "memory") and not _want(components, "workspace"):
        # Scoped to memory's own subtrees: selecting `memory` alone must not disturb
        # the rest of workspace/.
        #
        # Skipped entirely when `workspace` is also selected, and that guard is
        # load-bearing rather than an optimization. The workspace block above has
        # already saved the ORIGINAL tree to the rollback dir and replaced the live
        # one with incoming files, so re-running the copy here would save the
        # INCOMING memory over the saved original and destroy the only copy of what
        # was replaced. Workspace's own pass covers these paths anyway.
        #
        # The rollback copy of these trees was already taken at the top of this
        # function, before any database was swapped, so by the time control reaches
        # here a complete rollback set exists and only the replace pass is left.
        for tree, d in mem_roots:
            sd = snap / tree
            # Cleared UNCONDITIONALLY, then filled only if the archive carries it.
            # Clearing only when the archive had the tree meant a bundle without, say,
            # `workspace/knowledge` left the destination's own knowledge tree in place,
            # so a "replace" produced restored memory mixed with stale notes and still
            # reported success. Replace means the destination ends up matching the
            # archive; a tree the archive does not have is a tree the destination must
            # not keep. The rollback copy was taken before any database was swapped, so
            # the removed state is still recoverable.
            #
            # A root can pass containment and still be a LINK — a symlink pointing
            # somewhere else *inside* the data home resolves within it, so
            # safe_tree_root allows it. shutil.rmtree then raises OSError on that
            # link, and by this point the databases have already been replaced, so
            # the operator is left half-restored. Remove a link as a link and reserve
            # rmtree for real directories.
            _clear_tree_root(d)
            if sd.is_dir():
                d.parent.mkdir(parents=True, exist_ok=True)
                _copytree_safe(sd, d)

    try:
        backup.rmdir()
    except OSError:
        print(f"  Previous state saved to: {backup}/")
    print("✅ Replace complete.")


def _do_merge(snap: Path, mc: Path, components: list[str] | None) -> None:
    _refuse_unsafe_destination_roots(mc, components)
    print("🔀 Merge mode — importing...")

    if _want(components, "memory") and (snap / "memory.db").is_file():
        if not (mc / "memory.db").is_file():
            shutil.copy2(str(snap / "memory.db"), str(mc / "memory.db"))
            if (snap / "memory_index.db").is_file():
                shutil.copy2(str(snap / "memory_index.db"), str(mc / "memory_index.db"))
            print("  Memory: copied (no existing memory.db)")
        else:
            _merge_memory(snap / "memory.db", mc / "memory.db")
        print("  ✅ memory")

    # The markdown half of memory (preferences, projects, history, knowledge). Named
    # by the memory component so restoring memory does not require the whole
    # workspace; no-overwrite so a merge never clobbers newer local files.
    if _want(components, "memory"):
        for tree in COMPONENTS["memory"].trees:
            sd = snap / tree
            if sd.is_dir():
                dd = mc / tree
                if safe_tree_root(dd, what="destination root") is None:
                    continue
                dd.mkdir(parents=True, exist_ok=True)
                _copy_tree_no_overwrite(sd, dd, mc)

    if _want(components, "crons"):
        sc, dc = snap / "crons.json", mc / "crons.json"
        if sc.is_file():
            if dc.is_file():
                _merge_crons(sc, dc)
            else:
                shutil.copy2(str(sc), str(dc))
                print("  Crons: copied (no existing crons)")
        print("  ✅ crons")

    if _want(components, "config"):
        for f in CORE_FILES["config"]:
            s, d = snap / f, mc / f
            if s.is_file() and not d.is_file():
                shutil.copy2(str(s), str(d))
                print(f"  {f}: restored (was missing)")
        print("  ✅ config")

    if _want(components, "notifications"):
        sn, dn = snap / "notifications.jsonl", mc / "notifications.jsonl"
        if sn.is_file():
            if dn.is_file():
                _merge_notifications(sn, dn)
            else:
                shutil.copy2(str(sn), str(dn))
                print("  Notifications: copied")
        print("  ✅ notifications")

    if _want(components, "security"):
        for f in CORE_FILES["security"]:
            s, d = snap / f, mc / f
            if s.is_file() and not d.is_file():
                shutil.copy2(str(s), str(d))
                # restrict_to_owner (fail-loud), NOT chmod_safe — security
                # files include sel_hmac.key; mirror the create path. Windows
                # applies an owner-only DACL via icacls. Unlink the freshly
                # copied file on
                # failure so an icacls error doesn't leave a restored secret
                # under the destination-inherited DACL.
                try:
                    platform_compat.restrict_to_owner(str(d))
                except OSError:
                    d.unlink(missing_ok=True)
                    raise
                print(f"  {f}: restored (was missing)")
        print("  ✅ security")

    if _want(components, "workspace"):
        for dirname in ("workspace", "plan_memory"):
            sd = snap / dirname
            if sd.is_dir():
                dd = mc / dirname
                dd.mkdir(parents=True, exist_ok=True)
                _copy_tree_no_overwrite(sd, dd, mc)
        print("  ✅ workspace")

    if _want(components, "skills"):
        if (snap / "skills").is_dir():
            (mc / "skills").mkdir(parents=True, exist_ok=True)
            _copy_tree_no_overwrite(snap / "skills", mc / "skills", mc)
        print("  ✅ skills")

    print("✅ Merge complete.")


def _is_gateway_running() -> bool:
    """Check if the KiroCrew gateway is listening on its dashboard port."""
    # Deterministic override (used by tests / scripted restores) — avoids a real
    # socket probe whose result is environment-dependent.
    override = os.environ.get("KIROCREW_ASSUME_GATEWAY_RUNNING")
    if override is not None:
        return override.strip().lower() not in ("", "0", "false", "no")
    port = _DASHBOARD_PORT
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def restore_main(argv: list[str] | None = None, *, parsed: argparse.Namespace | None = None) -> int:
    if parsed is None:
        p = argparse.ArgumentParser(
            prog="kirocrew-restore", description="Restore KiroCrew state from a snapshot."
        )
        p.add_argument("snapshot", nargs="?")
        p.add_argument("--mode", choices=("replace", "merge"))
        p.add_argument("--dry-run", action="store_true")
        p.add_argument(
            "--force", action="store_true", help="Allow restore even if gateway is running"
        )
        p.add_argument("--components")
        p.add_argument("--list-components", action="store_true")
        parsed = p.parse_args(argv)
    args = parsed

    if args.list_components:
        _list_components()
        return 0

    if not args.snapshot:
        print("❌ snapshot file is required (unless --list-components is given)")
        return 1

    force = getattr(args, "force", False)
    if not force and _is_gateway_running():
        _audit("state_restore_rejected", "reason=gateway_running")
        print("❌ Gateway is running. Stop it first (kirocrew stop) or use --force.")
        return 1

    # An s3:// argument is fetched first, then treated exactly like a local bundle:
    # the extraction filter and the integrity check below are the validation, and a
    # downloaded object is untrusted input regardless of whose bucket it came from.
    #
    # It lands in the snapshots dir rather than a temp dir on purpose: an operator
    # recovering a dead host usually restores more than once, and keeping the fetched
    # bundle means the second attempt costs no transfer.
    if str(args.snapshot).startswith("s3://"):
        try:
            profile, _region = _resolve_aws_profile(getattr(args, "aws_profile", None))
        except (DestinationUnresolved, OSError, ValueError) as e:
            print(f"❌ Could not resolve an AWS profile: {e}")
            return 1
        into = Path(_default_snapshot_dir())
        print(f"☁️  Downloading {args.snapshot} (profile {profile})")
        try:
            local = remote.download(str(args.snapshot), into, profile)
        except remote.UPLOAD_FAILURES as e:
            print(f"❌ {type(e).__name__}: {e}")
            return 1
        try:
            platform_compat.restrict_to_owner(str(local))
        except OSError as e:
            # The bundle arrived but could not be locked down to the owner. Remove it
            # rather than leaving a world-readable copy of the operator's memory on
            # disk, and report it — a traceback here is indistinguishable from a crash.
            local.unlink(missing_ok=True)
            print(f"❌ Could not restrict {local} to owner-only ({e}); removed the download.")
            return 1
        print(f"  Saved to {local}")
        # A downloaded object is untrusted input even from a bucket we own: the key was
        # named on the command line, versioning means an older object may be corrupt,
        # and a truncated transfer produces a file that only fails when opened. Verify
        # it is a readable archive HERE, where the download can still be removed,
        # rather than letting tarfile raise out of the extract path as a traceback that
        # is indistinguishable from a crash and leaves the bad file behind.
        try:
            with tarfile.open(local) as probe:
                probe.getmembers()
        except (tarfile.TarError, OSError, EOFError) as e:
            local.unlink(missing_ok=True)
            print(
                f"❌ The downloaded object is not a readable snapshot archive ({e}); "
                f"removed it. Check the key, or pick another bundle with "
                f"`kirocrew backup list`."
            )
            return 1
        args.snapshot = str(local)

    snap_path = Path(args.snapshot)
    if not snap_path.is_file():
        print(f"❌ File not found: {snap_path}")
        return 1

    # Parse components
    components: list[str] | None = None
    if args.components:
        requested = [c.strip() for c in args.components.split(",") if c.strip()]
        # Restore reads whatever the bundle holds, so the purpose gate does not apply
        # here — only the unknown-name refusal does.
        try:
            components = resolve_components(requested, Purpose.BACKUP)
        except ComponentRefused as e:
            print(f"❌ {e}\n")
            _list_components()
            return 1

    mc = _mc_dir()
    mode = args.mode or ("merge" if (mc / "memory.db").is_file() else "replace")

    with tempfile.TemporaryDirectory() as work_str:
        work = Path(work_str)

        # Security checks are enforced inside _data_filter (no TOCTOU gap)
        with tarfile.open(str(snap_path), "r:gz") as tar:
            try:
                tar.extractall(work, filter=_data_filter)
            except TypeError:
                # Python < 3.11.4: filter param not supported, apply manually
                members = [m for m in tar.getmembers() if _data_filter(m) is not None]
                tar.extractall(work, members=members)

        # Both roots: `kirocrew-snapshot-` for a complete bundle and
        # `kirocrew-partial-` for a selective one. The second name exists so that
        # released versions, which require the first, refuse a partial bundle instead of
        # relocating the components it does not carry. This version reads the manifest,
        # so it can consume either.
        snap_dirs = [
            d
            for d in work.iterdir()
            if d.is_dir()
            and (
                d.name.startswith("kirocrew-snapshot-")
                or d.name.startswith("kirocrew-partial-")
            )
        ]
        if not snap_dirs:
            print("❌ Invalid snapshot format")
            return 1
        snap = snap_dirs[0]

        _print_manifest(snap)
        try:
            declared = _manifest_components(snap)
        except ManifestUnreadable as e:
            print(f"❌ {e}")
            print("   Refusing to guess what this bundle contains. Pass --components "
                  "explicitly if you know.")
            _audit("state_restore_rejected", f"reason=manifest_unreadable from={snap_path.name}")
            return 1
        if components is None:
            # A selective bundle must not be restored as if it held everything. With
            # components unset, _want() answers True for every component, so a
            # memory-only bundle taken through `--mode replace` would rmtree the live
            # workspace and put back only the memory subtrees it carries — deleting
            # unrelated state the bundle never had.
            #
            # The manifest records what actually rode (v3+), so that is the default,
            # INCLUDING when it resolves to an empty set. A pre-v3 bundle has no map
            # (declared is None) and keeps the old all-components behaviour, which is
            # correct for it — it did hold everything.
            if declared is not None:
                components = declared
                print(
                    "🔧 Components (from bundle manifest): "
                    f"{','.join(components) if components else '(none)'}"
                )
        elif declared is not None:
            # An explicit selection the bundle does not contain is a refusal, not a
            # no-op: replace mode would move the live files of that component out to
            # the rollback dir and have nothing to put back.
            absent = [c for c in components if c not in declared]
            if absent:
                print(
                    f"❌ This bundle does not contain: {', '.join(sorted(absent))}\n"
                    f"   It carries: {', '.join(declared) if declared else '(nothing)'}"
                )
                return 1
        if components:
            print(f"🔧 Components: {','.join(components)}")

        if args.dry_run:
            print(f"\n🔍 Dry run — would restore to {mc} in {mode} mode")
            print("Files in snapshot:")
            for f in sorted(snap.rglob("*")):
                if f.is_file():
                    print(f"  {f.relative_to(snap)}")
            return 0

        mc.mkdir(parents=True, exist_ok=True)
        try:
            if mode == "replace":
                _do_replace(snap, mc, components)
            else:
                _do_merge(snap, mc, components)
        except UnsafeComponentRoot as e:
            # Raised before anything was written, so this is a clean refusal. Report it
            # as one rather than letting a traceback out — the same contract every other
            # refusal on this path already follows.
            print(f"❌ {e}")
            return 1

    # Integrity check
    if _want(components, "memory") and (mc / "memory.db").is_file():
        try:
            with sqlite3.connect(str(mc / "memory.db")) as conn:
                result = conn.execute("PRAGMA integrity_check;").fetchone()[0]
        except Exception as e:
            result = str(e)
        if result == "ok":
            print("🔍 memory.db integrity: OK")
        else:
            print(f"⚠️  memory.db integrity check failed: {result}")
            _audit("state_restore_rejected", f"reason=integrity_check_failed from={snap_path.name}")
            return 1
        if not (mc / "memory_index.db").is_file():
            print(
                "⚠️  memory_index.db is missing — full-text search may not "
                "work until the FTS index is rebuilt."
            )

    comp_str = ",".join(components) if components else "all"
    _audit("state_restored", f"mode={mode} components={comp_str} from={snap_path.name}")

    print("\n⚠️  Restart kirocrew gateway to pick up changes: kirocrew restart")
    return 0
