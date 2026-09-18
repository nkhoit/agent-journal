# Operations

The repository implements journal, central delivery, and protected Unix administrative handlers, plus durable local spooling and generic adapter orchestration. Health success proves process and SQLite schema availability only; it is not a production acceptance claim. Vendor runtime integration, protected external recovery audit, and service-level restore fencing remain unresolved; see the status table in [README.md](../README.md).

## Deployment shape

- One `journald` application container and one persistent local SQLite volume.
- Private HTTPS ingress only; `journald`'s plain HTTP listener stays behind that ingress. Administrative mutations use a mode-`0600` Unix socket inside an existing private directory.
- Non-root process, read-only image filesystem, bounded CPU/memory/PIDs, rotated logs, and explicit health checks.
- SQLite on service-host-local storage, never SMB/NFS.
- Pin release image digests after acceptance; keep release bytes separate from mutable config, secrets, and state.

## Health and alerts

### Structured operational logs

`journald` writes one JSON tracing event per line to stderr. Capture stderr with the host service manager and restrict access and retention locally. `JOURNAL_LOG_LEVEL` accepts `info` (default), `warn`, or `error`; unknown values fall back to `info`. Debug/trace output is deliberately unavailable. `RUST_LOG` is not consumed. Keep `info` when investigating successful mutations or response loss.

Every request has a server-generated `request_id`, returned as `X-Request-ID`. `request_completed` includes the HTTP method, matched route template (not caller-controlled path segments or query strings), status, response-size hint, and elapsed microseconds. Join that event to:

- `bootstrap_committed` (info): a named administration or enrollment mutation returned successfully after its SQLite transaction committed. Emission occurs inside the bounded worker, even if the HTTP caller disconnected.
- `authentication_rejected` (warn): `missing_or_malformed_bearer`, `credential_rejected`, or `local_peer_denied`. The credential category intentionally aggregates unknown, expired, revoked, disabled-principal, and wrong-class rejections; logs do not reveal which credential matched.
- `bootstrap_rejected` (warn): validation, not-found, or conflict outcomes.
- `bootstrap_failed` (error): storage, SQLite, randomness, clock, worker, or capacity failure. SQLite failures include a numeric extended code when available, never SQL text or raw error details. `not_confirmed` or `unknown` must not be interpreted as proof of rollback.

Search by request ID and operation first; inspect the associated failure category, SQLite code, and request duration before retrying. Enrollment and rotation are not replayable. The CLIs report `credential_write_failed`, `server_outcome=committed`, a validated response request ID when available, and `recovery=enrollment-recover` if local publication fails. A missing response instead reports `server_outcome=unknown`; an unavailable request ID is expected when the response is lost. Use the protected recovery procedure below rather than inferring success from HTTP completion alone.

Logs exclude headers, bearer values, tickets, digests, request/response bodies, local credential paths, installation IDs, and runtime route targets. They are operational diagnostics, not recovery-grade external audit: stderr is not fsynced, ordered atomically with SQLite, or guaranteed to survive process/host failure. Missing events prove nothing. The database credential audit also shares SQLite's recovery unit. Protected durable external audit and restore reconciliation remain later operations work; metrics and dashboards remain S12.

Expose live and ready checks separately. Monitor process health, append/query latency, database and WAL size, pending mailboxes and oldest item age, outstanding/expired claims, adapter heartbeat age, local spool item/byte usage, free-disk reserve, paused claiming, runtime failure counts, backup age, and last verified restore date.

Alert on actionable current faults: stale adapter with pending attention, repeated terminal failure, failed backup, database/WAL pressure, or loss of required local capacity. Do not alert on a fictional model-read state.

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
