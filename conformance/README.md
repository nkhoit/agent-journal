# Conformance fixtures

This directory contains public-safe, generic fixtures for black-box clients and adapters. The fixtures are intentionally data-only; no live endpoint, credential, runtime target, or private identifier is included.

- `adapter/expected-envelope.txt` defines the stable trusted/untrusted envelope shape.
- `adapter/scenarios.yaml` lists required registration, custody, recovery, routing, telemetry, and failure cases.
- `client/requests.yaml` lists representative protocol operations and bounded limits.
- `fake-runtime/README.md` defines the fake runtime behavior required before vendor canaries.

The client contract fixtures become executable in S0; the fake-runtime and adapter scenarios become executable in S11. A test may use localhost and generated per-test IDs, but must never commit captured production traces or secrets.
