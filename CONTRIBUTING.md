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

Rust code must pass `cargo fmt --all -- --check`, `cargo test --locked --workspace --all-targets`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, and `cargo build --locked --workspace`. Install the shared Python gate dependencies with `python3 -m pip install -r requirements-ci.txt` before running the Python gates locally. `make check` also runs the stdlib-only migration contract, deterministic OpenAPI structural/reference check, optional pinned Redocly lint, Markdown checks, and public-hygiene scan. Keep tests deterministic and avoid credentials.

The workspace pins Tokio, Axum, and Tower for the runnable S3 service shell, plus tracing for structured process and request events. The shell deliberately uses no connection pool: synchronous `rusqlite` work runs through an explicit bounded blocking executor and refuses excess work rather than growing an unbounded queue. The protocol crate pins `base64`, `hmac`, and `sha2` for authenticated cursors and Jiff without timezone-database features for RFC 3339 validation. The storage crate pins `rusqlite` with only bundled SQLite/FTS5 and online-backup features. Add any further dependency only with an exercised use case, a pinned version, and a documentation update explaining the choice; do not add an ORM, pool, or `async-trait` merely to fill a boundary.

The S5 protocol query codec pins `form_urlencoded` and `percent-encoding` for
shared client/server escaping and strict UTF-8 query validation. Record UUIDv7
generation uses the existing injected secure-random source and server clock.
`make records-test` runs the Unix CLI/API vertical through S4 provisioning.
`make delivery-test` runs the S7/S8 registration, claim, replacement, custody,
telemetry, status, and requeue CLI/API verticals, including lost responses,
service restart, same-attempt expiry, and retryable telemetry recovery.
Both are part of `make check`.

The S9 local spool uses the existing pinned `rusqlite`, `serde_json`, and `sha2`
dependencies, plus pinned `fs2` 0.4.3 for cross-platform exclusive process locking
and filesystem free-space checks. `cargo test --locked -p journal-adapter-spool`
runs real-file recovery, child-process termination at durable boundaries, lock
contention, corruption, and pressure/full-database rollback tests without a runtime.

S10 uses the existing pinned client, Jiff, and SHA-256 dependencies in adapter core
for real delivery transport, adapter timestamps, and stable local event identity.
`cargo test --locked -p journal-adapter-spool` also exercises orchestration against
fake journal/runtime boundaries and kills real child processes across send/report
boundaries. `cargo test --locked -p journald --test adapter_orchestration` composes
real HTTP, the typed client, and the spool; Unix additionally launches `journald`.
These tests are in the ordinary workspace gate. Unix subprocess tests need native
private-directory permissions, not a Windows-mounted WSL directory that ignores chmod.
Run the ordinary parallel gate on Linux. The spool explicitly unlocks its sidecar
after closing SQLite, including when another process inherited the lock descriptor.
The Unix regression holds that descriptor in a live child during close and reopen.

S11 adds `journal-runtime-fake` using existing pinned dependencies, and
`make adapter-conformance` executes every adapter scenario with redacted
persisted-state evidence. The runner also rejects fixture drift and missing
coverage. Publish only the top-level JSON files in `target/adapter-conformance`,
never raw `.private-state` fixtures. See the
[fake-runtime contract](conformance/fake-runtime/README.md) for reuse and crash
semantics. This gate is included in `make check` and CI.

## Change expectations

The read-only HTML renderer pins `pulldown-cmark` 0.13.0 with default features
disabled and emits only its own narrow HTML allowlist. Run
`cargo test --locked -p journald --test web_http` for shared-viewer ACL, disabled
principal, delivery-scope, query, and listener-isolation checks. Unix workspace
tests also bind the actual optional listener and check startup/shutdown isolation.
Install `tests/browser-requirements.txt` and Chromium with
`python -m playwright install chromium`, then run `make browser-security`.
The dedicated CI browser job installs the pinned Playwright browser and executes
real DOM/CSP/request tests. This separate gate is required for rendering changes;
`make check` alone does not run a browser. The ephemeral fixture never prints
credentials or uploads browser captures.

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
