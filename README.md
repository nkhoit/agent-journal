# Agent Journal

Agent Journal is a runtime-neutral, permissioned append-only journal with reliable attention delivery for heterogeneous agents. It addresses durable **principals**, not runtime sessions. A record is visible according to space ACLs; `attention` creates an independent durable mailbox obligation for each addressed principal.

> **Status: S0–S7 foundations, protected bootstrap, journal queries, and central mailbox claims implemented.**
>
> The repository contains the reviewed product model, language-neutral OpenAPI contract, typed Rust domain and wire DTOs, strict JSON and cursor primitives, a synchronous SQLite foundation, and a runnable `journald` with protected bootstrap APIs and CLIs. Unix acceptance tests exercise provisioning, enrollment, credential recovery, and `aj` append/read/list/search/thread with lost-response replay. Records use UUIDv7 IDs, atomic mailbox creation, same-space backward relations, authorized FTS5 search, bounded reply-tree traversal, and authenticated pagination. Web UI, durable adapter delivery, and runtime injection remain unresolved. Hermes and Muse injection surfaces require revalidation on supported releases.

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
| OpenAPI 3.1 contract and S0 gate | Executable: validates the exact 29-path/31-operation surface and contract rules, fixture parsing, operation coverage, and deterministic contract mutations |
| Rust domain and protocol wire kernel | Executable: complete S1 DTOs, duplicate-key rejection, canonical append bytes, authenticated bounded cursors, and normative wire examples |
| SQLite kernel | Executable: pinned bundled SQLite/FTS5 driver, numbered migrations, verified connection policy, explicit transactions, read-only connections, concurrent access, and isolated backup/restore verification |
| Service shell | Executable: isolated TCP and Unix-socket routers, live/ready checks, bounded blocking SQLite execution, request IDs, body limits, redacted structured auth/mutation/failure events, and graceful shutdown |
| Administration, authentication, enrollment, and bootstrap client | Executable on Unix: peer-checked local administration, digest-only authentication, atomic enrollment and rotation, private credential files, and explicit recovery |
| Record APIs | Executable: discovery, atomic append, exact immutable replay, get, filtered sequence pages, authorized FTS5 search, and bounded reply-to trees |
| Adapter delivery | S7 registration, heartbeat, replacement fencing, bounded claims, expiry, and mailbox status implemented; S8 custody/telemetry/requeue and S9+ spool/runtime delivery remain unresolved |
| Binaries | `journald`, `aj` journal and mailbox commands, and `aj-admin` bootstrap/replacement/status commands; protected administration and credential files require Unix; runtime adapters remain explicit status-2 stubs |
| Hermes injection | **Unresolved; revalidation required** on the installed supported runtime |
| Muse injection | **Unresolved; revalidation required** on the installed supported runtime |
| Recovery, crash, security, and live canaries | SQLite online backup and isolated restore verification are executable; service-level restore fencing and later acceptance work remain planned |

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
crates/journal-service/      bootstrap, authenticated record transactions, and future delivery ports
crates/journal-client/       typed bootstrap/journal client, HTTP/Unix transports, private credential files
crates/journal-adapter-core/ registration, heartbeat, custody, routing, envelope ports
crates/journal-adapter-spool/ crash-recovery contract and pending store
crates/journal-runtime-hermes/ unresolved Hermes runtime boundary
crates/journal-runtime-muse/ unresolved Muse runtime boundary
crates/journald/             runnable shell, protected bootstrap handlers, authentication boundaries
crates/aj/                   enrollment and principal journal CLI
crates/aj-admin/             protected local provisioning, rotation, revocation, and recovery CLI
crates/journal-adapter-hermes/ not-implemented Hermes adapter binary
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
python3 scripts/validate_openapi.py api/openapi.yaml
make check
```

`make check` runs the Rust format, locked test, clippy, and build gates; the migration contract; Unix bootstrap CLI/API and recovery tests; the executable S0 OpenAPI gate for the exact 29-path/31-operation surface, security, limits, identity, required fields, client/adapter fixture parsing, operation coverage, and deterministic contract mutations; optional pinned Redocly standards lint; Markdown checks when available; and the public-hygiene scan. Standards lint is opt-in locally with `OPENAPI_STANDARDS_LINT=1`; CI always runs `@redocly/cli@1.34.3`. The commands require no deployment credentials.

`journald` serves health, enrollment, authenticated identity, and protected administration. Its TCP listener is plain HTTP and must remain behind private HTTPS ingress; it defaults to loopback. The administrative socket requires an existing private parent directory, is created mode `0600`, and verifies the kernel-reported peer UID.

```bash
cargo build --locked --workspace --bins
mkdir -m 700 service-state
./target/debug/journald \
  --database service-state/journal.db \
  --admin-socket service-state/admin.sock \
  --listen 127.0.0.1:8080
```

Follow the [bootstrap commands](docs/operations.md#bootstrap-commands) to provision and enroll, then the [central mailbox protocol](docs/protocol.md#central-mailbox-claims). Public administration paths always return non-leaking JSON `404` responses. Custody, telemetry, requeue, record delivery-status, and admin adapter listing remain explicit `501` routes. `journald` handles `SIGINT`/`SIGTERM` with graceful listener shutdown and removes only the Unix socket it created. Runtime-specific adapter binaries remain status-2 stubs.

## Implementation sequence

S0 through S7 provide executable contract, wire, SQLite, bootstrap, journal, search, thread, and central claim gates. The remaining sequence is:

1. Complete custody, telemetry, delivery-status, and requeue operations.
2. Implement the generic adapter core and durable local spool; prove custody semantics with a fake runtime.
3. Revalidate Muse and Hermes runtime injection surfaces on supported releases before implementing either adapter.
4. Add safe read-only web views and complete operational restore fencing and canary evidence.

Acceptance gates and dependency ordering are explicit in [`docs/implementation-plan.md`](docs/implementation-plan.md). Runtime-specific assumptions are not accepted as protocol facts; see [`docs/runtime-integrations.md`](docs/runtime-integrations.md).

## Public-repository safety

Examples use reserved fake identifiers and documentation-only placeholders. Do not add credentials, enrollment tickets, token hashes, real hostnames/domains/IPs, runtime session/chat IDs, spool databases, private exports, or deployment-local paths. Private installation state belongs outside this repository.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
