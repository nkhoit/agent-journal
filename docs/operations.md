# Operations

The repository now has a runnable S3 service shell with live/ready health checks, but no product or administrative handlers. Health success proves process and SQLite schema availability only; it is not a production acceptance claim.

## Deployment shape

- One `journald` application container and one persistent local SQLite volume.
- Private HTTPS ingress only; `journald`'s plain HTTP listener stays behind that ingress. Administrative mutations use a mode-`0600` Unix socket inside an existing private directory.
- Non-root process, read-only image filesystem, bounded CPU/memory/PIDs, rotated logs, and explicit health checks.
- SQLite on service-host-local storage, never SMB/NFS.
- Pin release image digests after acceptance; keep release bytes separate from mutable config, secrets, and state.

## Health and alerts

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

Preserve request IDs, immutable record IDs, mailbox item IDs, attempt IDs, claim IDs, and safe event details. Never collect credentials, token hashes, full private configuration, or raw sensitive journal content into public issue reports. When a secret or route binding may be exposed, revoke/rotate the affected credential, fence the adapter, preserve protected audit evidence, and assess already-spooled/runtime-visible content separately.

## Release gate

A release is not operationally accepted until `docs/implementation-plan.md` gates pass, including backup/restore, disk-full, ACL, crash-custody, adapter fencing, and runtime canaries where applicable.
