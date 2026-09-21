# Fake runtime boundary

`journal-runtime-fake` implements the optional inbox worker's narrow Runtime
boundary. It captures the resolved route, envelope and rendered body, permits
availability changes, and can durably record acceptance before terminating a
disposable child without returning a receipt.

Its append-only fixture ledger demonstrates that ambiguous acceptance may
repeat after restart with the same inbox ID. It intentionally does not promise
dedupe. Hermes tests separately enforce stable native keys and advertised
capabilities; Muse tests exercise stable durable drop-file replay.

The reusable worker cases test no ack before handoff, failures without ack,
unknown-route failure without fallback, fixed-bound traversal/wrap, backoff,
ack-only retries and inaccessible-after-handoff progress. Both real-`journald`
client integration tests kill a child between supported handoff and ack.
Muse publication tests kill before/after stable-name publication and recover
without inventing a new dedupe identity.

Run `make inbox-client-conformance`. Its fixed manifest and runner reject
scenario drift and empty/ignored selectors. Only a completed actual test run
produces the completion manifest. Evidence identifies the executed case and
test, not an invented runtime outcome.

Raw fixture ledgers contain route targets and content. Keep them in disposable
private test directories and never publish them. CI may retain only the
allowlisted per-case JSON and completion manifest under
`target/inbox-client-conformance`.

No fake-runtime result establishes production vendor durability, hook execution,
model observation, comprehension, completion or exactly-once processing.
