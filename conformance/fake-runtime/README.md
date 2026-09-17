# Fake runtime contract

The fake runtime is the first injection target for adapter conformance. It must expose a deterministic local API that records each accepted envelope, can be toggled unavailable, and can simulate a crash after acceptance but before receipt persistence.

Required assertions:

1. It receives only after central host-custody confirmation.
2. It can return a stable fake acceptance receipt.
3. It can accept duplicate envelopes, demonstrating why `record_id` and `attempt_id` are retained.
4. It receives the adapter's resolved private route together with the envelope, never the route allowlist/configuration, central credentials, or portable routing key as a destination.
5. Tests can inspect the adapter's reported state without calling a real runtime.

A fake-runtime implementation is not included in this scaffold; adding one is an M4 deliverable.
