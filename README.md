# Agent Journal

Agent Journal is a runtime-neutral, permissioned append-only journal with reliable attention delivery for heterogeneous agents. It addresses durable **principals**, not runtime sessions. A record is visible according to space ACLs; `attention` creates an independent durable mailbox obligation for each addressed principal.

> **Status: executable S0 contract gate and S1 typed wire kernel; service implementation pending.**
>
> The repository contains the reviewed product model, language-neutral OpenAPI contract, SQLite migration, typed Rust domain and wire DTOs, strict duplicate-key JSON decoding, canonical append encoding, authenticated opaque cursors, adapter guidance, fixtures, and executable stubs. S0 validates the exact 27-path/29-operation OpenAPI surface and fixture coverage. S1 provides protocol types and codecs but no handlers or persistence. It does **not** provide a functioning server, CLI protocol client, enrollment flow, database driver, web UI, or runtime injection. The binaries intentionally exit with status 2. S2 and later remain unimplemented; Hermes and Muse injection surfaces remain unresolved and require revalidation against supported runtime releases.

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
| OpenAPI 3.1 contract and S0 gate | Executable: validates the exact 27-path/29-operation surface and contract rules, fixture parsing, operation coverage, and deterministic contract mutations |
| Rust domain and protocol wire kernel | Executable: complete S1 DTOs, duplicate-key rejection, canonical append bytes, authenticated bounded cursors, and normative wire examples |
| SQLite schema | Initial migration present; driver and transactional repositories are pending |
| Service, storage, adapter, and client implementation | Scaffolded boundaries only; no network or database implementations |
| `journald`, `aj`, `aj-admin`, runtime adapters | Explicit not-implemented stubs; binaries exit 2 |
| Hermes injection | **Unresolved; revalidation required** on the installed supported runtime |
| Muse injection | **Unresolved; revalidation required** on the installed supported runtime |
| Backup/restore, crash, security, and live canaries | Planned acceptance work; not claimed by this repository |

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
crates/journal-storage-sqlite/ SQLite policy and repository ports
crates/journal-service/      service, authorization, mailbox, and clock ports
crates/journal-client/       authenticated client seam without an HTTP stack
crates/journal-adapter-core/ registration, heartbeat, custody, routing, envelope ports
crates/journal-adapter-spool/ crash-recovery contract and pending store
crates/journal-runtime-hermes/ unresolved Hermes runtime boundary
crates/journal-runtime-muse/ unresolved Muse runtime boundary
crates/journald/             not-implemented service binary
crates/aj/                   not-implemented principal CLI binary
crates/aj-admin/             not-implemented protected-admin CLI binary
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

Requirements: Rust 1.85 or newer, Python 3, and a POSIX shell. The workspace uses edition 2024 and pins `serde`, `serde_json`, `thiserror`, `base64`, `hmac`, and `sha2`. The three cryptographic/encoding crates implement bounded HMAC-SHA-256 cursors; no async runtime, HTTP framework, SQLite driver, or `async-trait` is selected yet because no implemented path exercises one.

```bash
cargo fmt --all -- --check
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo build --locked --workspace
python3 tests/migration_contract_test.py
python3 scripts/validate_openapi.py api/openapi.yaml
make check
```

`make check` runs the Rust format, locked test, clippy, and build gates; the migration contract; the executable S0 OpenAPI gate for the exact 27-path/29-operation surface, security, limits, identity, required fields, client/adapter fixture parsing, operation coverage, and deterministic contract mutations; optional pinned Redocly standards lint; Markdown checks when available; and the public-hygiene scan. Standards lint is opt-in locally with `OPENAPI_STANDARDS_LINT=1`; CI always runs `@redocly/cli@1.34.3`. The commands are safe to run without credentials.

The current binaries are compile checks, not services:

```bash
cargo build --locked --workspace --bins
./target/debug/journald       # prints not implemented; exits 2
./target/debug/aj             # prints not implemented; exits 2
./target/debug/aj-admin       # prints not implemented; exits 2
```

The runtime-specific adapter binaries also exit 2. A non-zero stub is deliberate: no runtime integration is claimed.

## Implementation sequence

S0 and S1 are executable contract and wire gates. The remaining sequence is:

1. Implement the Rust SQLite connection policy, numbered migrations, transactional repositories, and typed authorization.
2. Implement `journald` HTTP handlers, request IDs, bounded pagination/search, idempotency, and the protected admin socket.
3. Implement `aj` and `aj enroll` against the same typed client and fixtures; add crash, ACL, and security tests.
4. Implement the generic adapter core and local spool; prove custody semantics with a fake runtime.
5. Revalidate Muse and Hermes runtime injection surfaces on supported releases; implement adapters only after their canary gates pass.
6. Add safe read-only web views, operations/backup tooling, and migration evidence.

Acceptance gates and dependency ordering are explicit in [`docs/implementation-plan.md`](docs/implementation-plan.md). Runtime-specific assumptions are not accepted as protocol facts; see [`docs/runtime-integrations.md`](docs/runtime-integrations.md).

## Public-repository safety

Examples use reserved fake identifiers and documentation-only placeholders. Do not add credentials, enrollment tickets, token hashes, real hostnames/domains/IPs, runtime session/chat IDs, spool databases, private exports, or deployment-local paths. Private installation state belongs outside this repository.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
