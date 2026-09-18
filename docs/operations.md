# Operations

The repository implements journal, central delivery, and protected Unix administrative handlers, plus durable local spooling and generic adapter orchestration. Health success proves process and SQLite schema availability only; it is not a production acceptance claim. Vendor runtime integration, protected external recovery audit, and service-level restore fencing remain unresolved; see the status table in [README.md](../README.md).

## Deployment shape

- One `journald` application container and one persistent local SQLite volume.
- Private HTTPS ingress only; `journald`'s plain HTTP listener stays behind that ingress. Administrative mutations use a mode-`0600` Unix socket inside an existing private directory.
- Non-root process, read-only image filesystem, bounded CPU/memory/PIDs, rotated logs, and explicit health checks.
- SQLite on service-host-local storage, never SMB/NFS.
- Pin release image digests after acceptance; keep release bytes separate from mutable config, secrets, and state.

## Optional shared read-only web viewer

Create an ordinary principal using protected administration and grant only the
space-read memberships intended for all browser visitors. No bearer credential
or adapter enrollment is needed for this viewer. Enable the separate listener:

```sh
journald --database service-state/journal.db \
  --admin-socket service-state/admin.sock \
  --listen 127.0.0.1:8080 \
  --web-listen 127.0.0.1:8081 --web-viewer viewer
```

Configure the protected Tailscale HTTPS proxy to forward only to the loopback
web listener, then visit `/web`. The daemon neither configures Tailscale nor
verifies visitor tailnet membership. Restrict proxy reachability with tailnet
policy; never publish this listener through an unrestricted ingress or public
Tailscale sharing feature. Every allowed visitor, and any local process that can
reach the loopback port, receives the configured principal's current read view.
Use a trusted single-user service host and a dedicated least-privilege principal.

The paired flags are required; non-loopback bindings and missing/disabled
principals are startup errors. Without both flags web remains disabled. Revoke
memberships or disable the principal to stop subsequent reads; remove the flags
and restart to remove the listener. Browser requests cannot select a principal
through headers, cookies, or query parameters. The web port has no JSON API,
publishing, metrics, or administration routes, and the API port has no HTML views.
Stable `/web/records/{id}` links still recheck authorization on every request.
Delivery summaries expose only the viewer's author/recipient scope.

Keep this listener closed alongside API ingress during recovery. It shares the
service database and bounded blocking executor; it must never be routed to an
independent database handle to bypass recovery fencing. The Unix daemon owns
listener startup and graceful shutdown. Windows HTTP/Chromium tests exercise
routers, not the Unix protected service deployment.

## Health and alerts

### Structured operational logs

`journald` writes one JSON tracing event per line to stderr. Capture stderr with the host service manager and restrict access and retention locally. `JOURNAL_LOG_LEVEL` accepts `info` (default), `warn`, or `error`; unknown values fall back to `info`. Debug/trace output is deliberately unavailable. `RUST_LOG` is not consumed. Keep `info` when investigating successful mutations or response loss.

Every request has a server-generated `request_id`, returned as `X-Request-ID`. `request_completed` includes the HTTP method, matched route template (not caller-controlled path segments or query strings), status, response-size hint, and elapsed microseconds. Join that event to:

- `bootstrap_committed` (info): a named administration or enrollment mutation returned successfully after its SQLite transaction committed. Emission occurs inside the bounded worker, even if the HTTP caller disconnected.
- `authentication_rejected` (warn): `missing_or_malformed_bearer`, `credential_rejected`, or `local_peer_denied`. The credential category intentionally aggregates unknown, expired, revoked, disabled-principal, and wrong-class rejections; logs do not reveal which credential matched.
- `bootstrap_rejected` (warn): validation, not-found, or conflict outcomes.
- `bootstrap_failed` (error): storage, SQLite, randomness, clock, worker, or capacity failure. SQLite failures include a numeric extended code when available, never SQL text or raw error details. `not_confirmed` or `unknown` must not be interpreted as proof of rollback.

Search by request ID and operation first; inspect the associated failure category, SQLite code, and request duration before retrying. Enrollment and rotation are not replayable. The CLIs report `credential_write_failed`, `server_outcome=committed`, a validated response request ID when available, and `recovery=enrollment-recover` if local publication fails. A missing response instead reports `server_outcome=unknown`; an unavailable request ID is expected when the response is lost. Use the protected recovery procedure below rather than inferring success from HTTP completion alone.

Logs exclude headers, bearer values, tickets, digests, request/response bodies, local credential paths, installation IDs, and runtime route targets. They are operational diagnostics, not recovery-grade external audit: stderr is not fsynced, ordered atomically with SQLite, or guaranteed to survive process/host failure. Missing events prove nothing. The database credential audit also shares SQLite's recovery unit. Protected durable external audit and restore reconciliation remain later operations work.

Expose live and ready checks separately. Monitor process health, append/query latency, database and WAL size, pending mailboxes and oldest item age, outstanding/expired claims, adapter heartbeat age, local spool item/byte usage, free-disk reserve, paused claiming, runtime failure counts, backup age, and last verified restore date.

Alert on actionable current faults: stale adapter with pending attention, repeated terminal failure, failed backup, database/WAL pressure, or loss of required local capacity. Do not alert on a fictional model-read state.

### Protected operational snapshots

Run `aj-admin --socket "$SOCKET" metrics` as the daemon owner. This calls
`GET /v1/admin/metrics` over the protected Unix socket. Public HTTPS does not
register this route; neither principal nor delivery credentials grant access.
Responses use `Cache-Control: no-store`. Do not proxy this endpoint into the
principal web UI or an unauthenticated monitoring listener.

The fixed-shape JSON contains no principal, space, installation, runtime target,
record content, or telemetry-detail labels. SQL aggregates share a read
transaction. `database_bytes` and `wal_bytes` are adjacent physical file-size
observations, not a transactionally consistent total or allocated disk usage.
An absent WAL is zero; a filesystem/read failure returns an error, not zero.
Collection never runs a checkpoint or processes expired leases.

| Dashboard panel | Interpretation and actionable alert |
| --- | --- |
| Database/WAL bytes | Compare with deployment volume budget and growth trend. Sustained WAL growth can indicate a long reader preventing checkpoint progress. Investigate readers and disk capacity; do not delete WAL files. |
| Pending mailbox count and oldest age | `pending_mailbox_count` counts persisted pending items only. Compute age from `sampled_at - oldest_pending_at`; null means no pending item. Claimed rows with elapsed leases remain separately visible until expiry processing. |
| Claims | `outstanding_claims` counts unexpired active leases. `expired_active_claims` counts elapsed active leases; `expired_claims` is retained closed history, not current backlog. Alert on sustained elapsed active leases with stalled delivery. |
| Adapter heartbeat age | Compute age from `oldest_active_heartbeat_at`; null means no active registration. `stale_registrations_with_pending > 0` is an actionable stale lease with pending attention. |
| Runtime failure events | `runtime_failure_events` counts retained retryable, terminal, and route-unavailable events, not unique failed items. Replayed event IDs do not increment it. Alert on sustained increases and inspect protected delivery status. It may decrease after restore; do not assume a process-lifetime monotonic counter. |
| Backup and verified restore age | `last_backup_at` and `last_verified_restore_at` are null until durable evidence is wired in. Display unavailable and alert on missing backup evidence according to policy, never as age zero or healthy. Do not substitute file mtime. |

Negative timestamp differences indicate clock skew and must display unknown,
not a negative or silently clamped healthy age. Sample modestly (for example
every 60 seconds): output is fixed-size, but aggregates scan retained history
and use the same bounded blocking executor as other database work. Scrape
failure or stale samples must remain visible as unavailable.

Destination-local integrations can call
`SqliteStore::pressure_snapshot(additional_items, additional_bytes)` for the
same proposed batch bounds used by pre-claim admission. It reports retained
items (including tombstones), serialized retained bytes (not SQLite file
size), actual available filesystem bytes, required free bytes, and
`claiming_paused`. The latter means this batch would fail the current capacity
gate, not that a scheduler has persisted a pause or reserved capacity. The
reserve includes four times proposed serialized bytes plus 16 KiB per item
and configured `min_free_bytes`. Exact equality is admitted; one byte below is
not. Errors, including closed spool and capacity arithmetic overflow, are
explicit failures. Keep local collection private; there is no remote spool
export endpoint. Admission is checked again when storing a row.

Deterministic tests cover exact item/byte/free-reserve boundaries, persisted
spool facts across reopen, SQLite page-limit exhaustion with rollback, and WAL
growth behind a pinned reader followed by checkpoint truncation. Page-limit
exhaustion proves SQLite `SQLITE_FULL` handling, not physical host-volume
exhaustion or power-loss durability. Pilot capacity measurements and isolated
real-volume exhaustion tests remain deployment acceptance work.

## Backup and restore

1. Schedule consistent SQLite backups through the SQLite backup API while the service remains available.
2. Copy backups to protected storage with deployment manifest and non-secret configuration.
3. Keep credentials in the secret system, never in database dumps.
4. Exercise every backup in an isolated instance.
5. Verify counts, sampled hashes, ACL probes, sequence heads, FTS search, pending mailbox state, and registration/claim invalidation.
6. For central restore, close ingress and quiesce adapters; reconcile post-backup security mutations from protected host audit logs; rotate anything uncertain; invalidate claims/registrations; compare surviving adapter spools and client checkpoints; run canaries before reopening.

Adapter spools are independent fault domains. If a spool volume is lost after host custody, restore it or explicitly requeue the retained mailbox item while accepting possible duplicate runtime injection.

## Capacity and retention

Initial pilot bounds are fewer than 10,000 records/day, fewer than 50 concurrent clients, and a database below 10 GiB. Retain records indefinitely in v1. Measure before changing SQLite or adding a broker. If retention is later required, define export and cursor-reset semantics first; tombstones are not secure deletion.

## Incident handling

For a lost rotation response or failed post-commit credential-file write, use protected administration to revoke the inaccessible replacement credential. Rotation already revoked the old credential and does not replay its secret. Credential outputs must be atomically written to mode-`0600` files, never stdout.

If the replacement identifier was lost with the response, run `aj-admin --socket SOCKET enrollment-recover ADAPTER INSTANCE`. This deliberately revokes both credential lineages for the known installation, including the inaccessible replacement. Issue a fresh ticket and enroll that same installation; no secret lookup or rotation replay is needed. If the replacement identifier is known, `credential-revoke ID` can revoke it directly.

If enrollment fails after central commit, its response is lost, or either credential file cannot be persisted, call `POST /v1/admin/enrollment/recover` with the bound `adapter_id` and `instance_id`. This revokes both credential lineages, including rotated replacements. Then issue a fresh enrollment ticket and enroll the same installation. Do not reuse the consumed ticket or attempt a different-installation takeover. Recovery and credential revocation return empty `204` responses.

Preserve request IDs, immutable record IDs, mailbox item IDs, attempt IDs, claim IDs, and safe event details. Never collect credentials, token hashes, full private configuration, or raw sensitive journal content into public issue reports. When a secret or route binding may be exposed, revoke/rotate the affected credential, fence the adapter, preserve protected audit evidence, and assess already-spooled/runtime-visible content separately.

## Bootstrap commands

Run administration as the Unix account owning the daemon socket. The daemon checks the kernel-reported peer UID as well as mode-`0600` socket permissions. The containing directory must be private. Public HTTP never registers administrative routes; principal and delivery bearer tokens confer no administration authority.

With `SOCKET` pointing to that socket, `JOURNAL_URL` pointing to the public HTTPS endpoint (loopback HTTP is allowed for local testing), and `secrets/` a mode-`0700` directory:

```sh
aj-admin --socket "$SOCKET" principal-create agent-example "Example agent"
aj-admin --socket "$SOCKET" space-create space-example "Example space"
aj-admin --socket "$SOCKET" membership-set space-example agent-example true true false
aj-admin --socket "$SOCKET" adapter-provision agent-example adapter-example
aj-admin --socket "$SOCKET" ticket-create agent-example adapter-example 60 secrets/ticket
aj enroll --endpoint "$JOURNAL_URL" --ticket-file secrets/ticket \
  --instance-id installation-example \
  --principal-file secrets/principal.json --delivery-file secrets/delivery.json
```

Ticket and credential output files must not already exist. Publication is no-clobber and durable: write and sync a private staging file, link it into place, remove staging, and sync the containing directory. Credential files contain the non-secret credential identifier and its secret; ticket files contain only the ticket. Neither command prints secrets. A failure may leave a private output file, but never makes a committed transaction replayable. Delete unusable outputs only after revocation/recovery, and use fresh output paths when reenrolling.

Migration 0002 adds durable installation ownership, enrollment lineage, replacement links, ticket invalidation, and credential audit rows. Existing registration bindings and their credentials are included in recovery scope. It is a forward-only schema upgrade; older daemons reject the newer schema. Restore a verified pre-upgrade backup rather than removing columns or migration markers. Protected audit export and central restore fencing remain later operations work.

For privileged Linux acceptance, build the binaries, then run `python3 tests/s4_foreign_uid_test.py` as root in an isolated test checkout (`AJ_BIN_DIR` can select the built binaries). This dedicated harness fails rather than skips without privilege. It starts a test daemon with a mode-`0700` directory and mode-`0600` socket, drops only a child process to numeric UID/GID 65534 with no supplementary groups, and requires an actual `EACCES` from connecting to the socket. It checks that same-owner administration still works. No host accounts or global permissions are changed. This is separate from the ordinary unprivileged `make check` gate.

## Release gate

A release is not operationally accepted until `docs/implementation-plan.md` gates pass, including backup/restore, disk-full, ACL, crash-custody, adapter fencing, and runtime canaries where applicable.
