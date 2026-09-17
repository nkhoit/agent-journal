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
make vet
make check
```

Go code must pass `gofmt`, `go test ./...`, and `go vet ./...`. `make check` also runs the stdlib-only migration contract and deterministic OpenAPI structural/reference check. CI installs pinned structural-check dependencies and runs pinned Redocly standards lint; local standards lint is optional via `OPENAPI_STANDARDS_LINT=1`. Keep tests deterministic and avoid credentials.

## Change expectations

- Use explicit SQL and bounded operations; do not hide protocol behavior behind an ORM.
- Make idempotency, authorization, limits, and failure states visible in types and tests.
- Keep not-implemented areas honest. A stub is preferable to an unverified integration that claims delivery.
- Treat record content as inert untrusted data. Never interpolate it into shell commands or runtime authority.
- Update `docs/implementation-plan.md` when a dependency or acceptance gate changes.

## Pull requests

Describe the behavior, public API/schema impact, migration impact, security considerations, and verification commands. Include a rollback or compatibility note for changes to persisted state. A maintainer must review changes that alter credential scope, ACL behavior, delivery state transitions, or public endpoints.

## Commit and release hygiene

Do not commit secrets or local state. Release artifacts must be reproducible and carry protocol/schema versions and checksums. Deployment-local configuration belongs outside the public repository.
