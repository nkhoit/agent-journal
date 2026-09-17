# Agent Journal

Agent Journal is a runtime-neutral, permissioned append-only journal with reliable attention delivery for heterogeneous agents. It addresses durable **principals**, not runtime sessions. A record is visible according to space ACLs; `attention` creates an independent durable mailbox obligation for each addressed principal.

> **Status: public scaffold / implementation pending.**
>
> This repository contains the reviewed product model, protocol contract, storage migration, package boundaries, adapter guidance, fixtures, and executable stubs. It does **not** yet provide a functioning server, CLI, enrollment flow, database driver, web UI, or runtime injection. Commands that are not implemented exit non-zero and say so explicitly. Hermes and Muse injection surfaces remain unresolved and require revalidation against supported runtime releases.

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
| OpenAPI 3.1 contract | Initial contract present; server/client parity is an implementation gate |
| SQLite schema | Initial migration present; driver and transactional repositories are pending |
| Go package boundaries and domain validation | Scaffolded and tested |
| `journald`, `aj`, `aj-admin`, adapters | Explicit not-implemented stubs |
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

The monorepo intentionally keeps central protocol types separate from adapter routing and runtime packages. There is no MCP server in v1, no `aj-enroll` binary, and no remote admin bearer-token fallback.

## Repository layout

```text
api/                         OpenAPI and protocol fixtures
cmd/                         journald, aj, aj-admin, and adapter entrypoints
docs/                        design, protocol, security, operations, and plans
internal/domain/              runtime-neutral domain types and validation
internal/service/             service ports/use-case boundaries
internal/storage/sqlite/      SQLite policy boundary and migration metadata
internal/adapter/             adapter, spool, routing, and envelope boundaries
internal/adapter/runtime/       unresolved Hermes/Muse runtime surfaces
integrations/shared/           portable agent skill
migrations/                  numbered SQLite migrations
conformance/                 adapter/client/fake-runtime test-plan fixtures
config/examples/              generic, non-secret configuration
.github/                     CI and issue templates
```

## Build and test

Requirements: Go 1.22 or newer. The scaffold has no third-party Go dependencies.

```bash
make fmt-check
make test
make vet
make check
```

`make check` runs formatting verification, Go tests, the stdlib-only SQLite migration contract tests, vet, the deterministic OpenAPI structural/reference check, Markdown checks when available, and repository hygiene scans. It does not claim standards-compliant OpenAPI linting unless `OPENAPI_STANDARDS_LINT=1` is set with the pinned Redocly CLI installed. CI runs that structural check plus `@redocly/cli@1.34.3`. The commands are intentionally safe to run without credentials.

The current binaries are compile checks, not services:

```bash
go build ./...
go build -o /tmp/journald-scaffold ./cmd/journald
/tmp/journald-scaffold        # built binary exits 2: not implemented
go run ./cmd/journald          # go's wrapper exits 1 and reports the program's "exit status 2"
```

## Implementation sequence

1. Freeze the OpenAPI schemas, error vocabulary, limits, and conformance fixtures.
2. Implement SQLite connection policy, numbered migrations, transactional repositories, and typed authorization.
3. Implement `journald` HTTP handlers, request IDs, bounded pagination/search, idempotency, and protected admin socket.
4. Implement `aj` and `aj enroll` against the same typed client and fixtures; add crash/ACL/security tests.
5. Implement the generic adapter core and local spool; prove custody semantics with a fake runtime.
6. Revalidate Muse and Hermes runtime injection surfaces on supported releases; implement adapters only after their canary gates pass.
7. Add safe read-only web views, operations/backup tooling, and migration evidence.

Acceptance gates and dependency ordering are explicit in [`docs/implementation-plan.md`](docs/implementation-plan.md). Runtime-specific assumptions are not accepted as protocol facts; see [`docs/runtime-integrations.md`](docs/runtime-integrations.md).

## Public-repository safety

Examples use reserved fake identifiers and documentation-only placeholders. Do not add credentials, enrollment tickets, token hashes, real hostnames/domains/IPs, runtime session/chat IDs, spool databases, private exports, or deployment-local paths. Private installation state belongs outside this repository.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
