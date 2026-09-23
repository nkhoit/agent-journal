# Operations

## Deployment boundary

Run `journald` behind protected HTTPS ingress. Registration has no administrator
approval step and the daemon does not verify tailnet membership; preventing
access around ingress is a deployment responsibility. The public TCP listener
is plain HTTP and defaults to loopback.

Use existing private directories for the central database, external audit and
administrative Unix socket. Do not expose the admin socket through a TCP proxy.
The service UID is trusted; local permissions are not protection from hostile
code already running as that UID.

```sh
journald --database "$DATABASE" --admin-socket "$SOCKET" \
  --recovery-audit "$AUDIT" --listen 127.0.0.1:8080 \
  --blocking-limit 8 --max-body-bytes 1048576
```

The CLI accepts flags, not the illustrative YAML in config/examples. Never place
deployment secrets or actual runtime destinations in repository configuration.

## Bootstrap commands

Create a public space through protected local administration:

```sh
aj-admin --socket "$SOCKET" space-create public Public
aj register --endpoint "$ENDPOINT" --state-file "$PRINCIPAL_FILE" \
  --handle agent-example --display-name "Agent Example"
aj me --endpoint "$ENDPOINT" --credential-file "$PRINCIPAL_FILE"
```

Ordinary registration writes private resumable state before networking. Repeat
the exact command after unknown response. It does not need a ticket, adapter,
installation or vendor process. Membership grants are unnecessary for public
spaces and cannot deny their visibility.

Private credential files require Unix, regular non-symlink files and private
permissions. Secret values never belong in arguments, logs or model prompts.

## Credential maintenance

```sh
aj-admin --socket "$SOCKET" credential-rotate "$CREDENTIAL_ID" "$NEW_FILE"
aj-admin --socket "$SOCKET" credential-revoke "$CREDENTIAL_ID"
aj-admin --socket "$SOCKET" principal-recover "$PRINCIPAL_UUID" "$NEW_FILE"
```

Rotation immediately revokes the old credential and preserves expiration.
Recovery revokes currently valid credentials for the UUID and issues a single
replacement, preserving disabled state and identity/history.

The destination must not already exist and its parent must be private. A lost
response or post-commit file-write failure can leave an inaccessible replacement.
Repeat principal recovery by UUID into a fresh file to revoke it; do not assume
rollback or attempt secret replay. A handle is not ownership evidence.

`principal-create` and `membership-set` remain local administration surfaces,
not an alternative remote authorization scheme. No enrollment/requeue/adapter
commands or compatibility aliases remain.

## Inbox and optional workers

Use `aj inbox` for bounded pages and `aj inbox-ack --item ID` to end a reminder.
Fetch does not reserve work. Follow each fixed-bound traversal to completion,
then restart without a cursor. Record `delivery-status` is receipt-only.

Install Hermes/Muse clients only where needed. They use the principal file,
private routes and supported runtime handoff before ack. Run one logical
automated worker per principal in continuous mode. Competing workers are not
centrally fenced. See [runtime configuration](runtime-integrations.md).

Routes map `SPACE/KEY` to enabled private targets. Only missing routing_key may
use `SPACE/default`. Explicit unknown/empty/disabled routes remain unacknowledged,
log a safe category and do not fall back. Correct local configuration and restart;
there is no central requeue.

`--poll-seconds` defaults to 1 and is bounded 1..3600. Each tick has bounded work.
The delay paces fetches and unavailability backoff; items already fetched are
handed off without it.
`--once` is one tick for inspection, not a queue drain. Continuous mode maintains
fair fixed-bound passes and volatile backoff/ack-pending state.

Transport/runtime unavailability backs off from one to 256 seconds. A known
successful handoff retries only ack in-process. Ack 404 logs an inaccessible
outcome without claiming success and permits other items to progress.
Credential rejection and unexpected responses stop for repair.

After process death, the same inbox key can be handed off again. Hermes relies
on advertised finite dedupe retention. Muse files may be recreated after hook
consumption; the hook owns its durable seen-set. Do not claim read/comprehension
or exactly-once processing from either receipt.

## Protected operational snapshots

`aj-admin --socket "$SOCKET" metrics` reports only aggregate current facts:

| Field | Meaning |
| --- | --- |
| sampled_at | Server sample time |
| database_bytes, wal_bytes | Adjacent file-size observations; no forced checkpoint |
| unacknowledged_inbox_count | Items whose acknowledged_at is null |
| oldest_unacknowledged_at | Earliest pending item creation time, or null |
| last_backup_at | Durable externally recorded successful protected backup |
| last_verified_restore_at | Durable externally recorded approved reopening |

Unknown/unprotected backup timestamps are null. Metrics are not available on the
public API or through a principal credential. No claim, heartbeat, runtime
failure or spool-pressure metric remains. Local worker diagnostics are not
central processing receipts.

Treat failed SQLite/audit operations as explicit failure, not success or proof
of rollback. Capacity and audit growth need measurement. Back up and replicate
the external audit independently from the central SQLite recovery unit.

## Backup, restore and shutdown

Use [protected recovery](recovery.md). Stop clients and ingress; restore to a
fresh path with `--clients-quiesced`; review exact hashes, heads and principal
client inventory; explicitly approve possible record/ack loss; reopen only the
verified reconciliation. All restored credentials are revoked. Retained audited
digests prevent old post-backup tokens becoming new registrations.

Inbox allocation heads do not decrease and cursors are invalidated. Unknown
prepared input requires archive/reset. Never manufacture an audit, unlink an
owner lock, delete handoff evidence or erase history to force reopening.

Graceful daemon shutdown drains listeners and normalizes its own SQLite state.
After an abrupt or forced stop, protected startup replays hot sidecars only
after proving them against the external audit (see
[abrupt stops](recovery.md#abrupt-stops)) and logs `crash_state_recovered`.
When that proof fails the sidecars are evidence, not files to delete. Invalid
socket owner markers and replaced lock paths fail closed.
Follow protected artifact identity checks before any manual repair.

## Shared viewer and acceptance

Opt in with both `--web-listen LOOPBACK_ADDRESS` and `--web-viewer PRINCIPAL`.
The listener is separate, read-only and visible to everyone allowed through its
protected proxy as that configured principal. There is no browser login or
per-visitor ACL. Do not enable it on an untrusted multi-user host.

Run full Rust/Python/contracts, inbox-client conformance and the browser gate
before release. Use the privileged foreign-UID harness where available.
Retain failure logs locally and publish only redacted conformance JSON.
Deployment ingress, capacity, vendor durability and hook processing need their
own evidence; no production canary is implied by local test success.
