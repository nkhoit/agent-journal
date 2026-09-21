# Conformance fixtures

This directory contains public-safe, generic fixtures for black-box clients and adapters. The fixtures are intentionally data-only; no live endpoint, credential, runtime target, or private identifier is included.

- `adapter/expected-envelope.txt` defines the stable trusted/untrusted envelope shape.
- `adapter/scenarios.yaml` lists required registration, custody, recovery, routing, telemetry, and failure cases.
- `client/requests.yaml` lists representative protocol operations and bounded limits.
- `client/wire-examples.json` contains the public-safe schema examples referenced by OpenAPI and compiled through the S1 Rust DTOs.
- `fake-runtime/README.md` defines the fake runtime behavior required before vendor canaries.

The executable S0 contract gate parses the operation and adapter fixtures and verifies OpenAPI operation coverage. S1 additionally compiles the normative wire examples through the typed Rust DTOs, including required-field deletion checks. Neither gate executes client requests or adapter scenarios. The fake-runtime conformance gate executes adapter behavior with a deterministic runtime; the Hermes vendor adapter has separate HTTP and real-`journald` integration tests. A test may use localhost and generated per-test IDs, but must never commit captured production traces or secrets.

The contract covers 35 paths, 37 operations, and 79 fixture mappings, including principal inbox fetch/ack, independent principal registration, protected principal recovery, operational metrics, credential revocation, and transitional enrollment recovery. Record status exposes only inbox receipts. Rotation examples contain a synthetic one-time replacement secret, never a captured credential. Schema validation does not prove transactional or recovery behavior; the protocol/service/client inbox tests and real HTTP inbox test exercise that behavior without adapter enrollment.

S8 central custody, telemetry transition/replay, status visibility, and requeue
behavior are executed by service and HTTP tests, with the real-process CLI
vertical in `tests/s7_delivery_test.py`. The adapter scenarios include
retryable-to-success recovery and late telemetry after requeue. Parsing these
fixtures still does not prove local spooling or runtime injection.
