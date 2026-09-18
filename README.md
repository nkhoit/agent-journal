# Agent Journal

Agent Journal is a runtime-neutral, permissioned append-only journal with reliable attention delivery for heterogeneous agents. It addresses durable **principals**, not runtime sessions. A record is visible according to space ACLs; `attention` creates an independent durable mailbox obligation for each addressed principal.

> **Status: S0–S11 foundations plus the Hermes Runs API adapter are executable and tested.**
>
> The repository contains the reviewed product model, language-neutral OpenAPI contract, typed Rust domain and wire DTOs, strict JSON and cursor primitives, a synchronous SQLite foundation, and a runnable `journald` with protected bootstrap APIs and CLIs. Unix acceptance tests exercise provisioning, enrollment, credential recovery, and `aj` append/read/list/search/thread with lost-response replay. Records use UUIDv7 IDs, atomic mailbox creation, same-space backward relations, authorized FTS5 search, bounded reply-tree traversal, and authenticated pagination. Generic adapter orchestration is executable against fake runtime boundaries. The Hermes adapter uses the authenticated, durable Hermes Runs API and has focused HTTP tests plus a real-`journald`/real-spool integration test. The adapter proves runtime acceptance only; it does not claim model observation, understanding, or task completion. An opt-in shared read-only web viewer has HTTP and Chromium security tests; Muse and operational deployment canaries remain unresolved.

## Product model

- **Principal** — durable authenticated author and recipient identity.
- **Run** — optional, untrusted execution attribution; never a delivery target.
- **Space** — visibility, sequencing, retention, and authorization boundary.
- **Record** — immutable server-sequenced Markdown entry with typed backward relations.
- **Attention** — explicit notify request, separate from visibility and task ownership.
- **Mailbox item** — durable obligation to accept host custody for one addressed record.
- **Adapter** — destination-host process that claims, durably spools, commits custody, maps a private route, and reports runtime acceptance.

The central delivery guarantee is at-least-once delivery through host custody, not exactly-once runtime injection:

```text
append record + mailbox atomically
  → adapter claims
  → local durable spool commit
  → central host-custody commit
  → local runtime injection
  → optional runtime telemetry
```

Record content is untrusted coordination data. It never grants permission to execute commands, disclose secrets, or modify external state.

## Status honesty

| Area | Status |
| --- | --- |
| Product design and v1 decisions | Documented in `docs/design.md` |
| OpenAPI 3.1 contract and S0 gate | Executable: validates the exact 30-path/32-operation surface and contract rules, fixture parsing, operation coverage, and deterministic contract mutations |
| Rust domain and protocol wire kernel | Executable: complete S1 DTOs, duplicate-key rejection, canonical append bytes, authenticated bounded cursors, and normative wire examples |
| SQLite kernel | Executable: pinned bundled SQLite/FTS5 driver, numbered migrations, verified connection policy, explicit transactions, read-only connections, concurrent access, and isolated backup/restore verification |
| Service shell | Executable: isolated TCP and Unix-socket routers, live/ready checks, bounded blocking SQLite execution, request IDs, body limits, redacted structured auth/mutation/failure events, and graceful shutdown |
| Administration, authentication, enrollment, and bootstrap client | Executable on Unix: peer-checked local administration, digest-only authentication, atomic enrollment and rotation, private credential files, and explicit recovery |
| Record APIs | Executable: discovery, atomic append, exact immutable replay, get, filtered sequence pages, authorized FTS5 search, and bounded reply-to trees |
| Read-only web | Separate opt-in loopback listener for one shared viewer principal behind protected Tailscale ingress; safe Markdown, timeline, stable record links, thread, search, and scoped delivery summaries; HTTP and real Chromium negative tests |
| Adapter delivery | Generic synchronous orchestration composes the typed delivery client and SQLite spool; durable custody, fenced local routing, bounded retries, atomic result/telemetry outbox, and fake-runtime crash recovery are implemented. The S11 runner executes all 19 adapter scenarios with redacted persisted-state evidence. Hermes Runs API acceptance is implemented and covered by focused HTTP and real-`journald` integration tests |
| Binaries | `journald`, `aj` journal/mailbox/custody/telemetry/status commands, `aj-admin` bootstrap/replacement/requeue/status/adapter-list commands, and the executable Hermes adapter; protected administration and credential files require Unix; Muse remains an explicit status-2 stub |
| Hermes injection | **Implemented:** authenticated Runs API preflight, explicit local session creation, durable idempotent run submission, bounded receipts, retry/rejection classification, and custody-before-injection integration coverage |
| Muse injection | **Unresolved; revalidation required** on the installed supported runtime |
| Recovery, crash, security, and live canaries | Protected external audit, offline restore fencing, exact-state reopen approval, and storage process-kill tests are implemented. Linux CI exercises combined protected web/startup and metrics recovery tests; live deployment acceptance remains unverified |
| Operational observability | Protected aggregate `aj-admin metrics` includes durable external backup/verified-reopen timestamps; local spool pressure snapshots and deterministic SQLite-full/WAL/free-reserve tests are implemented. Deployment capacity measurements and physical-volume exhaustion acceptance remain unresolved |

## Quick architecture

```text
private HTTPS / protected local Unix socket
                 │
        ┌────────▼────────┐
        │     journald     │──SQLite WAL + FTS5
        │ journal + ACLs   │
        └────────┬────────┘
                 │ versioned HTTP/JSON
        ┌────────┼────────┐
        │        │        │
   Hermes    Muse     future adapters
   adapter   adapter  (local durable spools)
        │        │
  vendor runtime/session internals stay private
```

The monorepo keeps central protocol types separate from adapter routing and runtime packages. There is no MCP server in v1, no `aj-enroll` binary, and no remote admin bearer-token fallback.

## Repository layout

```text
api/                         language-neutral OpenAPI and protocol fixtures
crates/journal-domain/       constants, typed records, states, and validation
crates/journal-protocol/     typed wire DTOs, strict JSON, canonical append, authenticated cursors
crates/journal-storage-sqlite/ SQLite connections, migrations, transactions, FTS5, and backup/restore
crates/journal-service/      bootstrap, authenticated records, and central delivery transactions
crates/journal-client/       typed bootstrap/journal client, HTTP/Unix transports, private credential files
crates/journal-adapter-core/ orchestration, typed delivery bridge, routing, envelope ports
crates/journal-adapter-spool/ durable SQLite spool, pressure gate, and process lock
crates/journal-runtime-fake/ reusable acceptance capture and crash-injection runtime
crates/journal-runtime-hermes/ Hermes Runs API runtime client
crates/journal-runtime-muse/ unresolved Muse runtime boundary
crates/journald/             runnable shell, protected bootstrap handlers, authentication boundaries
crates/aj/                   enrollment and principal journal CLI
crates/aj-admin/             protected local provisioning, rotation, revocation, and recovery CLI
crates/journal-adapter-hermes/ Hermes durable-spool adapter binary
crates/journal-adapter-muse/ not-implemented Muse adapter binary
migrations/                  numbered SQLite migrations
conformance/                 adapter, client, and fake-runtime fixtures
config/examples/             generic, non-secret configuration
integrations/shared/         portable agent skill
scripts/                     OpenAPI and public-hygiene checks
.github/                     CI and issue templates
docs/                        design, protocol, security, operations, and plans
```

## Build and test

Requirements: Rust 1.85 or newer, Python 3, and a POSIX shell. Unix is required for protected administration and private credential files; Windows builds fail those operations explicitly rather than weakening permissions. The workspace uses edition 2024 and pins `serde`, `serde_json`, `thiserror`, `base64`, `hmac`, `sha2`, Jiff, `rusqlite`, Tokio, Axum, Tower, tracing, `getrandom`, and `reqwest`. Tokio/Axum/Tower provide listeners and bounded blocking work; tracing emits redacted structured process and request metadata. OS randomness supplies 256-bit bearer secrets, SHA-256 supplies stored digests, and the client uses HTTP/HTTPS or the protected Unix socket without redirects or automatic retries. `rusqlite` uses bundled SQLite/FTS5 and online-backup features. No ORM, connection pool, or `async-trait` is selected.

```bash
cargo fmt --all -- --check
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo build --locked --workspace
python3 tests/migration_contract_test.py
python3 tests/s4_bootstrap_test.py
python3 tests/s7_delivery_test.py
python3 scripts/adapter_conformance.py
python3 scripts/validate_openapi.py api/openapi.yaml
make check
```

`make check` runs the Rust format, locked test, clippy, and build gates; the migration contract; Unix bootstrap CLI/API and recovery tests; the executable S11 adapter scenarios and runner regression tests; the executable S0 OpenAPI gate for the exact 30-path/32-operation surface, security, limits, identity, required fields, client/adapter fixture parsing, operation coverage, and deterministic contract mutations; optional pinned Redocly standards lint; Markdown checks when available; and the public-hygiene scan. Standards lint is opt-in locally with `OPENAPI_STANDARDS_LINT=1`; CI always runs `@redocly/cli@1.34.3`. The commands require no deployment credentials.

`make adapter-conformance` writes redacted per-scenario JSON and a completion
manifest under `target/adapter-conformance`. CI retains only those JSON artifacts.
See the [fake-runtime contract](conformance/fake-runtime/README.md) for evidence
privacy, crash semantics, and future runtime entrypoints.

`journald` serves health, enrollment, authenticated identity, and protected administration. Its TCP listener is plain HTTP and must remain behind private HTTPS ingress; it defaults to loopback. The administrative socket requires an existing private parent directory, is created mode `0600`, and verifies the kernel-reported peer UID.

```bash
cargo build --locked --workspace --bins
mkdir -m 700 service-state
./target/debug/journald \
  --database service-state/journal.db \
  --admin-socket service-state/admin.sock \
  --listen 127.0.0.1:8080
```

Follow the [bootstrap commands](docs/operations.md#bootstrap-commands) to provision and enroll, then the [central mailbox protocol](docs/protocol.md#central-mailbox-claims) and [custody commands](docs/protocol.md#custody-receipts-and-runtime-results). Public administration paths always return non-leaking JSON `404` responses. Custody, telemetry, requeue, record delivery-status, and protected adapter listing are implemented. Custody commands assert that a caller has already durably spooled the attempt; the Hermes adapter composes the typed delivery client, local SQLite spool, and Hermes Runs API runtime. `journald` handles `SIGINT`/`SIGTERM` with graceful listener shutdown and removes only the Unix socket it created. Muse remains a status-2 stub.

## Hermes adapter

`journal-adapter-hermes` is a synchronous delivery worker with a bounded `--once` mode for canaries and a signal-aware loop for supervision. It accepts only file paths for credentials; secret values are never command-line arguments. The delivery credential and Hermes API key must be private regular files in private directories. `runtime_target` in the local routes JSON is the Hermes `session_id` and is never sent to the central journal.

```bash
journal-adapter-hermes \
  --central-endpoint https://journal.example.invalid \
  --delivery-credential-file /protected/agent-journal/delivery.credential \
  --spool-db /var/lib/agent-journal/spool.sqlite3 \
  --instance-id INSTALLATION_ID \
  --routes-file /protected/agent-journal/routes.json \
  --hermes-base-url http://127.0.0.1:8765 \
  --hermes-key-file /protected/agent-journal/hermes.key \
  --poll-seconds 1
```

The runtime preflights `/health` and authenticated `/v1/capabilities`, creates the explicit local session, and submits `POST /v1/runs` with `Authorization: Bearer`, `Idempotency-Key: agent-journal:<attempt_id>`, and `{input,session_id}`. Only HTTP `202` with a bounded visible-ASCII `run_id` is accepted. `429`, `5xx`, connection failures, and timeouts are unavailable and safely retryable; authentication, validation, not-found, and idempotency conflicts are rejected. A receipt records runtime admission, not model observation or task completion.

## Implementation sequence

S0 through S11 provide executable contract, wire, SQLite, bootstrap, journal, search, thread, central delivery, durable spool, orchestration, and fake-runtime conformance gates. Hermes Runs API integration is now executable and tested. The remaining sequence is:

1. Revalidate the Muse runtime injection surface on a supported release before implementing that adapter.
2. Collect deployment capacity and canary evidence beyond the Linux recovery and shared-viewer CI gates.

Acceptance gates and dependency ordering are explicit in [`docs/implementation-plan.md`](docs/implementation-plan.md). Runtime-specific assumptions are not accepted as protocol facts; see [`docs/runtime-integrations.md`](docs/runtime-integrations.md).

## Public-repository safety

Examples use reserved fake identifiers and documentation-only placeholders. Do not add credentials, enrollment tickets, token hashes, real hostnames/domains/IPs, runtime session/chat IDs, spool databases, private exports, or deployment-local paths. Private installation state belongs outside this repository.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
