# Contributing

Read README.md, AGENTS.md, the implementation plan, protocol and security model
before changing behavior. Keep Agent Journal small, inspectable and runtime-neutral.

## Development

Rust 1.85 and edition 2024 are the compatibility baseline. Dependencies are pinned;
update Cargo.lock with any exercised manifest change. Use explicit rusqlite
transactions and bounded blocking service work, not an ORM or generic SDK.

```sh
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 test --locked --workspace --all-targets
cargo +1.85.0 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.85.0 build --locked --workspace
make check CARGO="cargo +1.85.0" OPENAPI_STANDARDS_LINT=1
make browser-security CARGO="cargo +1.85.0"
```

The Python gate dependencies are pinned in requirements-ci.txt; browser tooling
is pinned in tests/browser-requirements.txt. Install missing dependencies from
those manifests when the selected gate requires them. CI runs pinned Redocly
1.34.3 and Chromium security acceptance. Unix subprocess tests need a native
filesystem with private directory permissions.

`make check` covers Rust, migration/retirement contracts, OpenAPI mutation
coverage, independent registration and protected credential recovery, record
CLI replay, inbox fetch/ack, offline recovery, inbox-client conformance and
public hygiene. Browser and privileged foreign-UID acceptance are separate
targets; an unprivileged skip is not evidence.

## Required coverage

Write tests before implementing new behavior or reproducing a bug. Include exact
limits and one-over failures, UTF-8 byte limits, malformed/unknown/duplicate JSON,
authorization and transaction rollback. Use fake clocks, injected randomness,
real temporary files and child-process kills at durable boundaries.

Preserve independent registration, raw append replay, immutable records,
recipient allocation, first acknowledgment time and receipt-status privacy.
Do not make runtime dependencies necessary for register/post/fetch/ack.
Unknown explicit routes fail closed; automatic ack always follows the documented
handoff. Stable inbox ID is the retry identity, not a random per-retry key.

The fake runtime and `make inbox-client-conformance` exercise actual worker and
vendor-boundary tests. Missing/duplicate scenarios and empty test selectors fail.
Publish only top-level JSON under `target/inbox-client-conformance`, never raw
fixture state. Optional-client crash ambiguity is not exactly-once delivery.

Recovery changes must preserve exact approval, revoked audited credential
bindings, inbox allocation heads, epoch invalidation, acknowledged-state loss
approval and archive/reset for uncertain input. Do not reintroduce adapter/spool
inventories or silently restore authority.

## Change and release expectations

Update OpenAPI, SQL admission, DTOs, client/CLI, fixtures, operational docs and CI
together when a contract changes. Preserve meaningful negative tests rather
than removing them with obsolete fixtures. Do not leave successful stubs or
obsolete required gates.

Describe behavior, deliberate limits, API/schema/credential impact, recovery
and compatibility, exact validation commands and their real results. Include
failed attempts, skips and remaining blockers. Deployment capacity, hook
processing and live canaries require separate evidence.

Keep configuration and runtime destinations local. Never commit secrets,
production state, raw private captures, real hostnames/user paths or binaries.
Run the public-hygiene scan before committing. Release artifacts must identify
their protocol/schema versions and checksums.
