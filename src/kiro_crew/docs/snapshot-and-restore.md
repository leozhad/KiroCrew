# Backup & Restore

`kirocrew snapshot` packs everything Kiro Crew has learned about you into a single
portable `.tar.gz`, and `kirocrew restore` unpacks it, on this machine or a
different one. Use it before an upgrade you are unsure about, to move your setup
to a new laptop, or to merge the memory from two machines you have been using in
parallel. Snapshots are **not** automatic: nothing takes one for you, so if you
want a routine backup, schedule the command yourself.

## Quick Start

```bash
kirocrew snapshot                                     # write to ~/.kiro/crew/snapshots
kirocrew snapshot ~/my-snapshots --keep 3             # custom dir, prune to 3
kirocrew snapshot --components memory                 # just memory, ~20 MB
kirocrew backup setup                                 # once: provision the S3 destination
kirocrew snapshot --components memory --to-s3         # off-host copy
kirocrew snapshot --list                              # list existing snapshots
kirocrew restore snapshot.tar.gz                      # auto-detects replace vs merge
kirocrew restore s3://my-backups/host1/snapshot.tar.gz # fetch, then restore
kirocrew restore snapshot.tar.gz --components memory,crons
kirocrew restore snapshot.tar.gz --dry-run            # preview, write nothing
kirocrew restore --list-components                    # show component names
```

Stop the gateway before restoring. `kirocrew restore` refuses to run while a
gateway is listening, because a live gateway holds the memory database open and
would write over what was just restored. Pass `--force` only if you know the
gateway on that port is not this instance.

## What a snapshot contains

| Component | Files |
|-----------|-------|
| memory | `memory.db`, `memory_index.db`, `workspace/memory/`, `workspace/knowledge/` |
| crons | `crons.json` |
| config | `config.json`, `session_map.json`, `hooks.json`, `project_dir`, `workspace_dir` |
| skills | `skills/` directory |
| workspace | `workspace/`, `plan_memory/` directories |
| notifications | `notifications.jsonl` |
| security | `telemetry_salt` |

`memory` is self-contained: it names the markdown half of memory (preferences,
projects, history) and the knowledge base explicitly, so `--components memory`
restores your recall without also restoring every unrelated working file in
`workspace/`. Selections may overlap — asking for both `memory` and `workspace`
stages the shared paths once.

`workspace/hygiene_data/` and `workspace/insert_facts*.py` are excluded: they are
large and regenerable.

The security event log's HMAC key (`sel_hmac.key`) is deliberately **excluded**
from every snapshot, and is regenerated on the restoring host. That keeps each
machine's audit-log signatures bound to the machine that wrote them, so a copied
snapshot cannot be used to forge audit entries elsewhere.

## Purpose: backup vs share

Every bundle records why it exists, and each component declares whether it is safe to
hand to another person.

| Purpose | Meaning | Today |
|---------|---------|-------|
| `backup` (default) | Restoring onto a host you control | Everything selected rides, unredacted — that is the point |
| `share` | Leaving your control | **Refused for every component** |

`--purpose share` currently refuses whatever you select, and that is deliberate rather
than unfinished. Whether a component is safe to share is a question about its
**content**, not its shape: a workspace file, a skill, a cron's `env` map, a
notification body or a lesson you pasted a token into can each carry a credential, and
staging cannot tell. Marking components share-safe one at a time was tried during
review and guessed wrong twice, so nothing claims it until the redaction work behind
it exists. The purpose, the per-component declaration and the refusal are all live, so
the first certified component only has to change its own declaration.

For now, use `--purpose backup` — restoring onto a host you control is what this
feature is for. The bundle's manifest records the purpose and each component's
declaration, so a reader of a bundle can tell which they are holding.

A component added without a policy declaration is refused at staging rather than
defaulting to permissive, so a new component cannot inherit a permissive value by
omission.

## Off-host copies (S3)

A snapshot written only to `~/.kiro/crew/snapshots` does not survive losing the machine,
which is the whole point of backing up. Sending it to S3 is two steps, and the split
between them is deliberate.

**Once, with you present:**

```bash
kirocrew backup setup
```

`setup` has to be authorized out of band. It refuses until a file exists at
`<data home>/backup/setup-authorized`, and that file must **name the destination it
authorizes**:

```json
{"account": "123456789012", "region": "us-west-2"}
```

Setup refuses unless the account and region it resolved match what you wrote, and deletes
the file once used, so one authorization is good for exactly one setup. You may also pin
`"bucket"`. There is deliberately no `--yes` or `--force`.

Naming the destination is load-bearing, not paperwork. A blank permission slip would only
prove that *somebody* approved *something*: with several AWS profiles registered, a token
you created intending one account could be spent with `--aws-profile other`, and every
later backup would go somewhere you never approved. The refusal prints the values this
invocation resolved so you can paste them — check them first, because that is exactly the
moment a wrong profile becomes visible.

The reason it is a file and not a terminal prompt is worth stating, because the obvious
design does not work. An earlier revision required a TTY and described that as a
human-presence check. It is not: a pseudo-terminal is something any process can allocate,
so `printf 'yes\n' | script -qec 'kirocrew backup setup …' /dev/null` satisfies both the
terminal test and the prompt. Whatever authorizes this has to be something the caller
cannot produce, and `<data home>/backup/` is a keystone path — the agent's file tools
refuse it and shell commands naming it are refused too. You can create that file; nothing
the agent drives can.

You still get a confirmation prompt naming the AWS account, region and bucket when a
terminal is present, because "wrong profile" is the mistake most worth catching. That
prompt is a convenience, not the security boundary.

The reason any gate is needed: everything else stops the backup path from being *pointed*
somewhere new — it takes no bucket from its caller, and the recorded destination sits
behind the same keystone floor. But `setup` writes that record through this program's own
code, so without a gate an agent could simply run the command: pick a registered profile
belonging to another account, create a bucket there, and every later `--to-s3` would ship
your memory to it.

Once setup has run, `kirocrew snapshot --to-s3` needs no authorization and is safe to
schedule.

This creates a bucket in **your own** AWS account — private (Block Public Access on all
four flags), encrypted at rest (AES256), ownership enforced, **versioned**, with a
lifecycle rule expiring superseded versions after 30 days — then reads those controls
back from the API and refuses to record anything if AWS does not confirm them. The
bucket name, region and owning account are written to
`<data home>/backup/destination.json`.

Be clear about what that lifecycle rule bounds. Every run writes a new timestamped key,
so bundles are current versions and practically never become noncurrent: the rule bounds
the history of a *replaced* bundle (a corrupt re-upload, a same-second re-run), which is
what versioning is there to make recoverable. **It does not bound how many bundles you
accumulate** — that grows by one per run, and nothing deletes them. That is a deliberate
choice, not an oversight: a lifecycle rule can only say "delete older than N days", never
"keep the newest N", so on a machine that stopped backing up — the dead host this whole
feature exists for — an expiry rule would delete the last surviving copy of its memory
exactly when you need it. A few gigabytes a year is the better trade. Add your own
expiry rule if you want remote pruning; `kirocrew backup list` shows what is there.

If a bucket of that name already exists, `setup` refuses it unless **both** hold: it
carries this feature's own `kirocrew:backup` tag, and it has no bucket policy at all.
Either check failing — including a tag or policy that cannot be read — is a refusal, and
nothing is written or replaced first.

Both conditions guard a different harm. The tag answers "did Kiro Crew create this?",
because `setup` **replaces** the bucket's lifecycle configuration: pointed at an unrelated
bucket, it would discard that bucket's own rules and start expiring its old versions —
destroying someone's data in order to set up a backup. The policy check answers "who can
read this?", because the hardening step sets public-access, ownership and encryption
controls and none of those revoke a bucket policy, so a bucket already granting read
elsewhere would publish the memory it is about to receive.

Note what the tag is *not* doing: it is not evidence that a bucket is safe to put secrets
in — anyone who owns a bucket can tag it. It is a necessary condition for reuse, never a
sufficient one. A destination is created, not adopted; the escape hatch is to pick a name
that does not exist yet.

Re-running `setup` on the destination it already recorded is the repair path and stays
available regardless, so a bucket weakened out of band can always be fixed.

It is idempotent: run it again and it re-applies every control to the same bucket, which
is how you repair one that was weakened out of band. Default name is
`kirocrew-backup-<accountid>-<region>`; override with `--bucket`. The name carries no
username on purpose: bucket names live in one global namespace that anyone can probe for
existence, the account ID already makes the name unique, and adding an identity there
would only advertise whose bucket it is. Host identity appears in the *key* prefix
instead, where it is not publicly listable and where it helps you recognise which
machine a backup came from.

**Then, as often as you like:**

```bash
kirocrew snapshot --components memory --to-s3
```

That writes only to the destination `setup` recorded. There is no flag for naming a
bucket, and that absence is the design: a backup job must not be in the business of
deciding whether some arbitrary bucket is safe to write your memory into. Deciding that
automatically is not a job code can finish — a tag says who *intended* a bucket for
backup, a bucket policy has to be parsed to learn who can read it, and Block Public
Access does not neutralise a CloudFront origin grant. Creating the bucket ourselves and
refusing every other one answers the question once, visibly, with you there.

The one check that remains on every write is enforced by S3 rather than by Kiro Crew:
each upload carries `--expected-bucket-owner`, so a bucket that is no longer yours —
deleted and re-created by someone else under the same name — fails the write instead of
receiving your data.

Exposure is also re-checked immediately before each upload, because setup's verification
is only a point-in-time fact: a bucket policy can be added afterwards, and Block Public
Access does not stop a grant to one specific named account. If the bucket has acquired a
policy — **or if the policy cannot be read at all** — the upload is refused and your local
snapshot is untouched.

Refusing on an unreadable answer is deliberate. Warning and proceeding sounds kinder, but
a profile simply lacking `s3:GetBucketPolicy` makes the answer permanently unknown, so
every run would warn, you would learn to ignore the line, and the check would be
decorative exactly when it is load-bearing. What a refusal costs is the off-host copy for
that run, not the backup: the local bundle is already written, and the message names the
permission to grant.

Adding your own lifecycle rule is safe: `setup` reads the bucket's existing configuration
and replaces only the rule it owns, leaving yours in place. If it cannot read the existing
configuration it refuses rather than overwriting rules it cannot see.

Bundles are namespaced per machine (`backups/<hostname>/`), so several hosts can share
one bucket and a restore can name which machine's backup it wants:

```bash
kirocrew backup list          # bundles by host
kirocrew backup status        # what is configured, and whether it still checks out
kirocrew restore s3://my-backups/backups/laptop/kirocrew-snapshot-….tar.gz
```

The upload runs **after** the local bundle is written and locked down, and is retried on
a transient failure — so a flaky network costs you a retry, not the backup. A permission
failure is not retried: it will not fix itself.

AWS credentials are never stored or read by Kiro Crew. The `aws` CLI resolves them from a
profile *name*, taken from the same registry `deploy-web` uses, or from `--aws-profile
NAME`.

Scheduling is a cron, not a flag. The command is deterministic, so it belongs on a
schedule that runs it directly rather than dispatching an agent turn. A `command`-kind
Kiro Crew cron does exactly that and costs no tokens; note that `kirocrew cron add` does
not expose it, so create it from the dashboard's cron surface. A plain system crontab
entry works too:

```
0 3 * * *  kirocrew snapshot --components memory --to-s3
```

On a host with no OS-level sandbox backend (no Linux user namespaces, no macOS
`sandbox-exec`), the `aws` subprocess fails closed with a `SandboxUnavailableError` — the
same constraint `deploy-web` has. Set `agent.sandbox_allow_unsandboxed_exec=true` to
allow it, understanding that the subprocess then runs without credential isolation.

## Restore

### Replace vs merge

| Mode | Chosen when | Behavior |
|------|-------------|----------|
| `replace` | No existing `memory.db` | Overwrite the target with the snapshot, backing up any existing state first |
| `merge` | An existing `memory.db` is found | Import new data without overwriting what is already there |

The mode is auto-detected from whether `~/.kiro/crew/memory.db` exists, so a
restore onto a fresh machine replaces and a restore onto a machine you are
already using merges. Override with `--mode replace` or `--mode merge`.

In `replace` mode the state being overwritten is moved into a
`pre-restore-<timestamp>/` folder inside the data home first, and the path is
printed, so a wrong-snapshot restore is recoverable.

### What merge does per component

- **Memory**: existing entries win, new keys are added
- **Crons**: deduplicated by job name. Existing jobs are kept; new jobs are
  imported with fresh IDs
- **Notifications**: deduplicated by timestamp
- **Config and security**: only files that are missing are restored, never
  overwritten
- **Workspace and skills**: only files that do not exist at the destination are
  copied

So a merge never destroys anything on the receiving machine. If you want the
snapshot to win, use `--mode replace`.

### Options

| Flag | Description |
|------|-------------|
| `--mode replace\|merge` | Force the mode instead of auto-detecting |
| `--components X,Y` | Restore only these components |
| `--dry-run` | List what would be restored and write nothing |
| `--list-components` | Show the component names and what each covers |
| `--force` | Restore even though a gateway is listening |

After a restore, run `kirocrew restart` so the gateway picks up the new state.

### Integrity check

A restore runs an integrity check on the memory database and exits non-zero if it
fails, so a corrupted archive is a loud failure rather than a quietly broken
memory. If the full-text index (`memory_index.db`) is missing you get a warning:
search keeps working, but the index needs to rebuild first.

## Security

Snapshots are handled as untrusted input on the way in and as sensitive data on
the way out.

- Archives containing symlinks or hardlinks are rejected before extraction, so a
  crafted archive cannot be used to write outside the data home
- Entries with `..` or absolute paths are rejected
- Extraction strips ownership and permissions from the archive
- Both snapshot and restore emit security audit events, including a rejected
  restore and the reason it was rejected
- The tarball itself is created owner-only. It still contains your config,
  memory, and workspace, so treat it as private: store it with restrictive
  permissions and do not send it over a channel you would not send your notes
  over

## Scheduling your own backups

There is no built-in backup schedule. To get one, add a cron job that runs the
command, for example by asking the agent to schedule `kirocrew snapshot --keep 7`
daily. Verify it afterwards with `kirocrew snapshot --list`: an unverified backup
job is the same as no backup.
