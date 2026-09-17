# AGENTS.md

This is the orientation guide for coding agents working in Agent Journal. It applies to the entire repository unless a deeper `AGENTS.md` overrides it.

## Start here

Read these before changing code:

1. [`README.md`](README.md) — current status and repository layout.
2. [`docs/implementation-plan.md`](docs/implementation-plan.md) — ordered slices and acceptance gates.
3. [`docs/protocol.md`](docs/protocol.md) — implementation-facing protocol rules.
4. [`api/openapi.yaml`](api/openapi.yaml) — normative HTTP operations and wire schemas.
5. [`migrations/0001_initial.sql`](migrations/0001_initial.sql) — persistence invariants.
6. [`docs/security-model.md`](docs/security-model.md) — credential, authorization, and recovery boundaries.
7. [`docs/adapter-authoring.md`](docs/adapter-authoring.md) — custody and adapter rules.
8. [`CONTRIBUTING.md`](CONTRIBUTING.md) — change and release expectations.

If artifacts conflict, do not silently choose one. Preserve the reviewed product and security model, then reconcile OpenAPI, protocol docs, Rust DTOs, migrations, fixtures, and tests in the same change.

## Current status

This repository is an implementation scaffold, not a working service. Check the status table in `README.md` before claiming anything works. A compiling crate, status-2 stub, or successful empty handler is not implementation evidence.

Work in the slice order in `docs/implementation-plan.md`. Keep each change small enough to review and prove independently. Do not skip directly to Hermes or Muse integration: runtime adapters remain unresolved until the durable spool, generic orchestration, and fake-runtime conformance gates pass.

## System model

Agent Journal is a runtime-neutral, permissioned, append-only coordination journal:

- durable principals are identities; runtime sessions are not;
- space ACLs control visibility;
- `attention[]` creates independent recipient mailbox obligations;
- records are immutable and threads are projections over typed relations;
- central SQLite stores records, ACLs, mailbox items, attempts, and history;
- destination-local adapters own private route bindings, durable spooling, runtime injection, duplicate handling, and runtime telemetry.

The custody path is fixed:

```text
append record + mailbox obligation atomically
  → claim bounded attempt batch
  → durably spool the complete local attempt
  → commit exact host custody centrally
  → persist custody confirmation locally
  → recheck fencing and resolve a local route
  → inject into the runtime
  → persist/report the strongest honest runtime result
```

Never move runtime injection before durable local custody and central confirmation.

## Repository map

- `crates/journal-domain` — limits, domain states, validation, shared record types.
- `crates/journal-protocol` — public wire DTOs and transport seam.
- `crates/journal-storage-sqlite` — SQLite policy and future repositories.
- `crates/journal-service` — authorization, service, mailbox, and clock boundaries.
- `crates/journal-client` — typed authenticated client seam.
- `crates/journal-adapter-core` — registration, fencing, routing, envelope, and orchestration boundaries.
- `crates/journal-adapter-spool` — crash-recovery contract and future durable spool.
- `crates/journal-runtime-*` — vendor-runtime boundaries; currently unresolved.
- `crates/journald`, `crates/aj`, `crates/aj-admin` — service and CLI binaries.
- `crates/journal-adapter-*` — runtime adapter binaries.
- `api/` — normative OpenAPI contract.
- `migrations/` — SQLite schema and enforceable invariants.
- `conformance/` — client, adapter, envelope, and fake-runtime fixtures.
- `tests/` — language-independent contract tests.
- `docs/` — design, protocol, security, operations, and implementation plan.

## Architecture rules

- Use deliberately boring Rust: concrete structs, explicit transactions, small modules, and narrow traits only at real boundaries.
- Preserve Rust 1.85 and edition 2024 unless a reviewed change updates the MSRV and CI together.
- Pin dependencies and update `Cargo.lock` in the same change. Add a dependency only when that slice exercises it.
- Use `rusqlite` for explicit synchronous SQLite work. Async handlers must call it through bounded blocking work; long-poll waits must hold neither a transaction nor a blocking-worker permit.
- Do not add an ORM, generic adapter SDK, event broker, or elaborate trait hierarchy without demonstrated need.
- Build public and administrative routers separately. Public HTTPS must not register admin routes. Administration is host-local through the protected Unix socket and `aj-admin`; there is no remote admin bearer fallback.
- Keep principal-client and delivery-adapter credentials separate. A delivery credential cannot publish as its principal.
- Derive principal, adapter, installation, and recipient authority from authentication and server context, never caller-controlled body fields.
- Keep runtime session IDs, chat IDs, hook paths, process handles, route targets, and local bindings out of central records and public fixtures.
- Default routing applies only when no routing key is supplied. An unknown or disabled explicit key fails closed.
- Treat record content, Markdown, snippets, route keys, run IDs, and telemetry detail as untrusted data.

## Protocol invariants

Do not weaken these without explicit design and security review:

- append allocates the per-space sequence, inserts the immutable record, creates every addressed mailbox item and ordinal-1 attempt, and stores the idempotency result in one transaction;
- `created_at` and authorship are server-assigned;
- relations are same-space, backward-only, and bounded; v1 permits at most one `reply-to`;
- collection operations are bounded and cursor/keyset paginated;
- ACL filtering happens before search ranking, snippets, counts, or facets leave storage;
- claim identity is bound to credential, principal, adapter, installation, generation, and exact item set;
- lease expiry returns the same attempt to `pending`; explicit requeue creates a new attempt and retains history;
- host custody requires the complete attempt to be durably spooled first;
- telemetry is accepted only after exact host custody and uses the narrow adapter telemetry state enum;
- delivery status is narrower than record visibility: an addressed recipient sees only its own entry, an authorized author sees all recipient entries, and other readers receive a non-leaking `404`;
- committing custody changes state; it does not delete the record, mailbox item, attempt, or audit history.

Use only honest transport vocabulary: published, pending, claimed, host accepted, adapter-reported runtime accepted, and explicit failure states. Never claim read, observed, understood, completed, or exactly-once runtime delivery.

## Development workflow

Before editing:

```bash
git status --short
git branch --show-current
```

Do not overwrite unrelated work. Identify the active implementation slice and its acceptance gate before adding production code.

During implementation:

- write negative and boundary tests with the behavior;
- use fake clocks, injected ID/random sources, and deterministic failpoints;
- test exact limits and one-over failures, including UTF-8 byte limits;
- test rollback at each durable transaction boundary;
- use real temporary SQLite files for persistence and recovery tests;
- use child-process termination—not only mocked errors—for crash guarantees;
- keep fixtures and examples public-safe and generic;
- update OpenAPI, SQL, Rust types, fixtures, docs, and tests together when a contract changes.

Run the full baseline before considering a change complete:

```bash
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 test --locked --workspace --all-targets
cargo +1.85.0 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.85.0 build --locked --workspace
python3 tests/migration_contract_test.py
python3 scripts/validate_openapi.py api/openapi.yaml
python3 scripts/public_hygiene.py
make check
```

For standards lint matching CI:

```bash
make check OPENAPI_STANDARDS_LINT=1
```

Also run tests specific to the current slice, including black-box, authorization, concurrency, failure-injection, or recovery gates. `git diff --check` must pass.

## Evidence expected in a change

A completed implementation change should report:

- behavior added and behavior deliberately left unresolved;
- public API or schema impact;
- persisted-state and migration impact;
- authorization and credential-class impact;
- exact commands run and their real results;
- failure and recovery cases exercised;
- compatibility or rollback considerations.

Do not describe planned tests as passed. Do not use ignored tests, unexercised stubs, or fabricated responses as acceptance evidence.

## Public-repository safety

Never commit:

- credentials, tickets, tokens, deployment hashes, or secret-bearing logs;
- real hostnames, private domains/IPs, usernames, home paths, route targets, runtime IDs, or installation IDs;
- production databases, spools, backups, exports, canary transcripts, or deployment-local configuration;
- generated binaries or private test captures.

Use reserved fake identities and documentation-only endpoints. Run `python3 scripts/public_hygiene.py` before committing.

## Out of scope for v1

Do not introduce MCP, A2A, federation, PostgreSQL/HA, Redis/NATS/Kafka, multi-active adapter failover, task ownership, workflow execution, model spawning, artifact storage, rich chat features, public import/provenance APIs, exactly-once injection, or semantic read/completion receipts unless the product design and implementation plan are explicitly revised first.
