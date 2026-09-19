# Runtime integrations

Runtime integration is intentionally a separate, conditional layer. The central journal protocol must not contain vendor session IDs, chat IDs, hook paths, process handles, or runtime-specific assumptions.

## Hermes — Runs API adapter implemented

The Hermes adapter targets the authenticated Runs API exposed by the supported Hermes release contract. It uses only the following surface:

- `GET /health` for reachability;
- authenticated `GET /v1/capabilities`, requiring `features.run_submission == true` and durable `features.runs_idempotency` with at least the documented 86,400-second retention;
- authenticated `POST /api/sessions` with an explicit local session ID;
- authenticated `POST /v1/runs` with `Idempotency-Key: agent-journal:<attempt_id>` and JSON `{ "input": rendered, "session_id": private_session_id }`.

The runtime client requires HTTP `202` and a bounded visible-ASCII `run_id`; it returns only that non-secret receipt to the generic adapter. Exact retries use the same delivery-attempt key. `429`, `5xx`, connection failures, and timeouts map to `RuntimeUnavailable`. Authentication failures, validation failures, missing sessions, and idempotency conflicts map to `RuntimeRejected`. Raw vendor responses and API keys are never logged or persisted.

`Route.runtime_target` is the private Hermes `session_id`. It is loaded from a local routes JSON file, sent only to Hermes, and does not enter central custody, telemetry, or portable protocol values. The generic adapter preserves spool → central custody → injection-started → runtime acceptance → durable result/outbox ordering. The acceptance receipt proves runtime admission only; it does not prove model observation, understanding, or task completion.

The executable `journal-adapter-hermes` wires the delivery journal, SQLite spool, static routes, system clock, runtime client, and generic adapter. `--once` runs one bounded tick for a canary; loop mode handles `SIGINT`/`SIGTERM`. Credential arguments are file paths only. Secret files must be private regular non-symlink files in private directories.

Focused runtime HTTP tests cover accepted submission, exact replay, request body/session/key shape, authentication rejection, idempotency conflict, retryable `429`/`5xx`, malformed or oversized receipts, and capability preflight. A Unix integration test runs the adapter against a real `journald`, a real SQLite spool, and a fake Hermes HTTP server; it verifies custody-before-injection, accepted telemetry, persisted runtime receipt, and that the session ID stays out of central delivery status.

This repository does not claim a production live canary, model completion, or a reply/read path through Hermes. Those remain deployment and product-level acceptance work.

## Muse — hook drop-point adapter implemented

The Muse personal-agent runtime exposes no authenticated injection API to
local processes. Verified 2026-09-18 against Agent Kit v0.1.6: `muse.py
--help` lists only Hindsight/Zulip client actions (`post`, `reply`, `inbox`,
`ack`, `retain`, `recall`, `get-document`, `memory-status`, …); there is no
chat-injection subcommand, webhook, or socket. The supported handoff is a
private local drop directory watched by a platform hook:

- The adapter validates the drop directory at startup: it must be an existing
  regular non-symlink directory whose Unix mode denies group/other access.
  There is no runtime secret, so `journal-adapter-muse` takes no runtime key
  file; the directory's filesystem permissions are the access control.
- `MuseRuntime::inject` durably writes one JSON drop file per delivery
  attempt under a stable attempt-derived name
  (`muse-<sha256(attempt_id)>.drop.json`), via exclusive create, file fsync,
  atomic rename, and directory fsync.
- The payload carries `version`, `dedupe_key` (the attempt ID), `target_chat`
  (the route's private chat ID), the envelope's correlation IDs, the rendered
  body, and a SHA-256 content hash. It never leaves the local host.
- The operator's hook worker picks the file up and calls
  `chat.send_message` from inside the platform, where the session context
  exists.
- Exact replay of an identical payload returns the same receipt without
  rewriting the file. A conflicting or unreadable file at the stable name is
  an ambiguous binding and fails closed as `RuntimeRejected`; a vanished drop
  directory maps to `RuntimeUnavailable`. Disabled routes, malformed chat IDs,
  and rendered text that does not match the envelope are rejected before any
  write.

`Route.runtime_target` is the private Muse chat ID. It is loaded from a local
routes JSON file, sent only to the drop point, and does not enter central
custody, telemetry, or portable protocol values. The generic adapter preserves
spool → central custody → injection-started → runtime acceptance → durable
result/outbox ordering. The drop receipt proves durable local handoff only; it
does not prove the hook worker ran, the turn was queued, or the model
observed, understood, or completed the delivery.

The platform offers no idempotency key, so duplicate chat turns remain
possible if the hook worker redelivers. This is represented honestly: the
drop payload carries the stable `dedupe_key`, the deployment must run a worker
that keeps a durable seen-set on that key, and the adapter itself never
creates two drop files for one attempt.

The executable `journal-adapter-muse` wires the delivery journal, SQLite
spool, static routes, system clock, runtime client, and generic adapter.
`--once` runs one bounded tick for a canary; loop mode handles
`SIGINT`/`SIGTERM`. Credential arguments are file paths only; the delivery
credential must be a private regular non-symlink file in a private directory.

Focused contract tests cover accepted injection, exact replay, malformed
chat IDs, conflicting/unreadable existing files failing closed, and a
vanished drop directory as retryable. A Unix integration test runs the
adapter against a real `journald`, a real SQLite spool, and the real drop
point; it verifies custody-before-injection, accepted telemetry, persisted
runtime receipt, that the chat ID stays out of central delivery status, and
that a restart does not duplicate the drop file.

This repository does not claim a production live canary, hook-worker
behavior, queued-turn durability, model completion, or a reply/read path
through Muse. Those remain deployment and product-level acceptance work.

## Shared rules

Adapters use delivery-only credentials for inbound custody. Any reply uses a separately provisioned principal-client credential through `aj`. Runtime targets remain local and are never sent as central routing values. Runtime acceptance is telemetry; it is not proof of model observation, understanding, or task completion.
