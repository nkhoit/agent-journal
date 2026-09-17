# Adapter authoring

An adapter is a destination-host process, not a second journal implementation. It transfers one principal's central mailbox obligations into a vendor runtime while keeping runtime identifiers and bindings local.

## Required boundary

```text
principal mailbox → durable local custody → local route → runtime injection
```

Implement the shared ports in `crates/journal-adapter-core` and the durable contract in `crates/journal-adapter-spool`; use the protocol fixtures under `conformance/`. The adapter must:

1. use a delivery-only credential bound to one principal and adapter identity;
2. register a persistent random installation ID and renew its fencing generation with heartbeat;
3. hold a machine-local exclusive process lock;
4. claim bounded batches and stop before local item, byte, or free-disk limits are exhausted;
5. write the complete record, claim ID, instance ID, generation, and attempt ID to a durable spool before host-custody commit;
6. retry the same claim/attempt after a lost commit response;
7. persist an idempotent custody-confirmed transition before any runtime injection;
8. enumerate recoverable spool rows after restart, including pending custody and non-terminal injection states;
9. deduplicate ordinary retries by `attempt_id` and retain `mailbox_item_id` and `record_id`;
10. recheck registration/fencing before every new runtime injection;
11. resolve only configured `(space, routing_key)` keys to private local targets, then pass the resolved `Route` together with the trusted `Envelope` to `Runtime::inject`;
12. hold `route-unavailable` items for explicit operator action instead of silently using the default;
13. render trusted envelope metadata separately from the untrusted body;
14. report bounded non-secret runtime telemetry only after exact host acceptance; detail is compact serialized JSON of at most 4,096 UTF-8 bytes;
15. use a separate principal-client credential for optional correlated replies;
16. stop or back off on expiry, fencing, revocation, runtime failure, or local pressure.

## Local state

A minimal durable spool has `inbound_attempts`, `route_bindings`, and `adapter_meta`. Each inbound row persists `claim_id`, `instance_id`, `generation`, the complete envelope, a custody-confirmed flag, an injection lifecycle state, and any runtime receipt or safe failure detail. `put`, custody confirmation, injection-start, acceptance, and failure transitions are idempotent for the same attempt and reject conflicting claim/generation data. `recoverable` returns unfinished rows after restart; accepted and terminal rows remain as compact tombstones after payload retention so an old attempt cannot be accidentally reinjected. Store runtime targets only locally. Keep secrets in the host secret mechanism, not in the spool.

`journal-adapter-spool` currently exposes this contract and a `NotImplementedStore`; it does not claim durable storage. The future implementation must fsync the full row before host-custody commit and prove recovery with crash tests.

## Configuration shape

See [`../config/examples/adapter.yaml`](../config/examples/adapter.yaml). The example contains no real route target. A production adapter must reject missing credentials, duplicate installation use, unknown route keys, and unsafe spool paths before claiming.

## Conformance sequence

Use a fake runtime before connecting a vendor runtime:

- register and heartbeat fence stale installations;
- claim returns no more than the configured batch maximum;
- crash before spool commit leads to redelivery;
- crash after spool commit retries custody for the same claim and attempt;
- runtime injection is impossible before idempotent custody confirmation;
- restart enumerates and resumes recoverable spool rows;
- lease expiry preserves an attempt ID;
- explicit requeue creates a new attempt ID;
- stale generation is rejected and stops processing;
- default routing applies only when no key is supplied;
- unknown/disabled keys do not fall back and are not injected;
- the runtime receives the resolved private route and the exact envelope;
- envelopes contain stable IDs and untrusted-content warnings;
- telemetry is rejected for pending/claimed attempts or a cross-principal mailbox;
- runtime receipts are reported as acceptance telemetry, never comprehension.

## Runtime-specific integration

Do not infer an injection surface from a vendor name or old local installation. Hermes and Muse are deliberately unresolved in this scaffold. Record the supported version, exact API/CLI/hook behavior, concurrency semantics, receipt strength, restart behavior, and canary evidence in deployment-local evidence before implementing the corresponding adapter.

Never use terminal keystrokes, direct edits to a runtime's internal database, shell interpolation of record content, or a fresh unrelated session per delivery as a substitute for a supported integration.
