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

## Muse — unresolved / revalidation required

The intended investigation path is a local durable spool, a narrow supported hook/helper handoff, and a supported `chat.send_message`-like call returning the strongest available queued-turn receipt. The exact external inbound API, hook semantics, supported release behavior, and duplicate/restart guarantees are **not established by this repository**.

Before implementing or claiming the Muse adapter, deployment evidence must revalidate:

- persistent chat selection and local-only route binding;
- hook polling and wake behavior;
- busy-chat queuing;
- queued-turn receipt durability;
- crash after send and before receipt persistence;
- safe helper locking/lease behavior;
- a live canary with a correlated journal reply.

If the runtime offers no idempotency key, duplicate runtime turns remain possible and must be represented honestly. Until this evidence exists, `journal-adapter-muse` remains an explicit not-implemented stub.

## Shared rules

Adapters use delivery-only credentials for inbound custody. Any reply uses a separately provisioned principal-client credential through `aj`. Runtime targets remain local and are never sent as central routing values. Runtime acceptance is telemetry; it is not proof of model observation, understanding, or task completion.
