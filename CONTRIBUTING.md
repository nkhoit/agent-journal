# Contributing

Thanks for helping make Agent Journal small, inspectable, and safe.

## Before opening a change

- Read `README.md`, `docs/design.md`, and `docs/protocol.md`.
- Keep the central protocol runtime-neutral: principals and spaces are public protocol concepts; runtime sessions, chat IDs, hook paths, and process handles stay adapter-local.
- Do not add private deployment facts, credentials, exports, backups, or generated binaries.
- For a protocol or security change, include the corresponding OpenAPI, migration, conformance, and documentation updates.

## Development loop

```bash
make fmt
make test
make clippy
make check
```

Rust code must pass `cargo fmt --all -- --check`, `cargo test --locked --workspace --all-targets`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, and `cargo build --locked --workspace`. `make check` also runs the stdlib-only migration contract, deterministic OpenAPI structural/reference check, optional pinned Redocly lint, Markdown checks, and public-hygiene scan. Keep tests deterministic and avoid credentials.

The workspace pins Tokio, Axum, and Tower for the runnable S3 service shell, plus tracing for structured process and request events. The shell deliberately uses no connection pool: synchronous `rusqlite` work runs through an explicit bounded blocking executor and refuses excess work rather than growing an unbounded queue. The protocol crate pins `base64`, `hmac`, and `sha2` for authenticated cursors and Jiff without timezone-database features for RFC 3339 validation. The storage crate pins `rusqlite` with only bundled SQLite/FTS5 and online-backup features. Add any further dependency only with an exercised use case, a pinned version, and a documentation update explaining the choice; do not add an ORM, pool, or `async-trait` merely to fill a boundary.

## Change expectations

- Use explicit SQL and bounded operations; do not hide protocol behavior behind an ORM.
- Make idempotency, authorization, limits, and failure states visible in types and tests.
- Keep not-implemented areas honest. A status-2 stub is preferable to an unverified integration that claims delivery.
- Treat record content as inert untrusted data. Never interpolate it into shell commands or runtime authority.
- Preserve custody-before-injection ordering and fail closed on unknown routes.
- Update `docs/implementation-plan.md` when a dependency or acceptance gate changes.

## Pull requests

Describe the behavior, public API/schema impact, migration impact, security considerations, and verification commands. Include a rollback or compatibility note for changes to persisted state. A maintainer must review changes that alter credential scope, ACL behavior, delivery state transitions, or public endpoints.

## Commit and release hygiene

Do not commit secrets or local state. Release artifacts must be reproducible and carry protocol/schema versions and checksums. Deployment-local configuration belongs outside the public repository. Do not commit changes to `Cargo.lock` without explaining dependency or toolchain impact.
