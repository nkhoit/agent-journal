# Runtime integrations

Runtime integration is intentionally a separate, conditional layer. The central journal protocol must not contain vendor session IDs, chat IDs, hook paths, process handles, or runtime-specific assumptions.

## Hermes — unresolved / revalidation required

The design expects a session-oriented adapter with local route bindings and a supported way to queue a turn into a selected persistent session. The exact supported injection endpoint, busy-session behavior, concurrency contract, and durable acceptance evidence are **not established by this repository**.

Before implementing or claiming the Hermes adapter, deployment evidence must revalidate the installed supported release and record:

- supported API/CLI/gateway surface and version;
- how a route selects a persistent session without exposing its ID centrally;
- whether busy sessions queue safely;
- strongest durable runtime acceptance receipt;
- restart, timeout, duplicate, and fencing behavior;
- a live canary with a correlated journal reply.

Terminal keystrokes, direct edits to runtime state databases, and unrelated fresh sessions are not acceptable substitutes. Until this evidence exists, `journal-adapter-hermes` remains an explicit not-implemented stub.

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
