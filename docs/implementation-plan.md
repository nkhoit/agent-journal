# Incremental implementation plan

This plan turns the reviewed Rust scaffold into a working Agent Journal through small, independently testable vertical slices. A slice is complete only when its behavior, negative cases, and recovery boundaries are exercised. Compilation, an HTTP `200`, or a stub test is never feature evidence.

## Working rules

- Keep the system boring: concrete structs, explicit SQLite transactions, narrow traits at real process or storage boundaries, and no generic framework layer.
- Add a dependency only in the slice that exercises it. Pin it, update `Cargo.lock`, and document why it exists.
- Keep synchronous `rusqlite` repositories explicit. From async handlers, run database work in bounded blocking tasks. Long-poll waits must not occupy a database transaction or blocking-worker permit.
- Build public and administrative routers separately. The private HTTPS listener must not register administrative routes; `aj-admin` uses only the protected Unix socket.
- Derive principal, adapter, and installation authority from authenticated server context. Never accept those identities from ordinary JSON bodies.
- Use deterministic failpoints and fake clocks. Do not make tests depend on long sleeps or timing luck.
- Land the typed client method and CLI command with the endpoint family it consumes; do not defer all client work to the end.
- Keep runtime-specific adapters unresolved until the durable spool and fake-runtime conformance suite pass.
- Preserve the current non-goals: no MCP, broker, PostgreSQL, federation, task engine, artifact store, or exactly-once runtime claim in v1.

## Dependency graph

```text
S0 contract gate
 └─ S1 domain + wire kernel
     └─ S2 SQLite kernel
         └─ S3 runnable service shell
             └─ S4 admin + auth + enrollment
                 └─ S5 append/read/list vertical
                     ├─ S6 relations/search/thread
                     └─ S7 registration/claim/lease
                         └─ S8 custody/telemetry/requeue
                             ├─ S9 durable adapter spool
                             │   └─ S10 adapter orchestration
                             │       └─ S11 fake-runtime conformance
                             └─ S12 operations + read-only web

Hermes and Muse are separate conditional canaries after S11.
```

Every slice should be one reviewable PR unless its acceptance gate cannot be demonstrated without two tightly coupled changes.

## S0 — Executable contract gate

**Goal:** make protocol drift fail before implementation code lands.

### Build

- Parse the OpenAPI document and every client/adapter conformance fixture.
- Map fixtures to OpenAPI operation IDs and report uncovered operations explicitly.
- Assert hard limits, security class, idempotency requirements, request identity sources, and required response fields.
- Assert that public routes contain no remote-admin authorization scheme and ordinary append contains no import provenance.
- Preserve the public-hygiene scanner and fake identifiers.

### Test

- Existing structural OpenAPI check and pinned Redocly lint.
- Fixture parser and operation-coverage tests.
- Mutation tests proving failure for a removed path, broken `$ref`, missing security declaration, public admin route, or private identifier.

### Accept when

- CI reports the expected 29 paths and 31 operations plus fixture coverage.
- Failure mutations fail deterministically.
- All binaries remain honest not-implemented stubs.

## S1 — Domain, wire, canonicalization, and cursors

**Goal:** create one typed interpretation of the public protocol before handlers or repositories duplicate it.

### Build

- Complete request/response DTOs for append, record reads, pages, search, errors, claims, commits, telemetry, and enrollment.
- Keep path values, transport headers, authentication context, and JSON body fields in separate types.
- Implement duplicate-JSON-key rejection before deserialization.
- Implement canonical append encoding for idempotency comparison. Normalize only fields explicitly defined as sets; do not normalize identifiers, tokens, content, or ordered relations.
- Implement signed or MACed opaque cursor payloads containing route, filter fingerprint, ordering mode, and last key. Cursor parsing must be bounded and versioned.
- Preserve required response fields even when arrays are empty or values are null.

### Test

- Table-driven serialization tests against OpenAPI examples.
- Required-field deletion tests for every response DTO.
- Unknown-field, duplicate-key, null-versus-omitted, UTF-8 byte-limit, enum, and one-over-limit tests.
- Canonicalization tests for equivalent and conflicting idempotency payloads.
- Cursor tamper, truncation, wrong-route, wrong-filter, wrong-order, and valid-continuation tests.

### Accept when

- Handlers and repositories never need to parse ad hoc JSON.
- One canonical byte representation determines idempotency equality.
- Every required wire field has positive and missing-field tests.

## S2 — SQLite kernel and recovery primitives

**Goal:** replace the storage placeholder with a real, synchronous SQLite foundation.

### Build

- Add pinned `rusqlite` with only exercised features, including bundled SQLite/FTS5 and online backup support.
- Implement database open, numbered migration application, schema-version checks, and explicit transaction helpers.
- Apply and verify `foreign_keys=ON`, WAL, `synchronous=FULL`, and a bounded busy timeout on every connection.
- Add a connection factory suitable for bounded `spawn_blocking` calls. Do not add a pool until measurements justify one.
- Implement backup creation and isolated restore/open verification; service-level restore fencing comes later.

### Test

- Keep the stdlib Python migration contract as an independent implementation.
- Add Rust integration tests using temporary on-disk databases.
- Probe actual pragmas, tables, indexes, triggers, immutable-record guards, and FTS5 search.
- Test migration rollback/retry, busy timeout, read-only paths, backup failure, truncated backup, and schema mismatch.
- Run concurrent reader/writer smoke tests.

### Accept when

- Rust can create, migrate, query, back up, restore, and FTS-search a real database.
- A failed migration leaves no partially advanced schema version.
- No tested storage path uses `NotImplementedStore` or a fake success.

## S3 — Runnable `journald` shell and listener isolation

**Goal:** produce the first real process without pretending the product API is implemented.

### Build

- Add pinned `tokio`, `axum`, and `tower` only now.
- Construct two independent routers: a private application router and a protected local Unix-socket administrative router.
- Implement graceful startup/shutdown, structured tracing, request IDs, bounded JSON bodies, common error envelopes, `/health/live`, and `/health/ready`.
- Readiness checks database availability and schema compatibility.
- Run every SQLite operation through a bounded blocking executor; waiting requests fail with explicit capacity errors rather than creating unbounded tasks.

### Test

- Router tests without a network where practical.
- Black-box subprocess tests over loopback and a temporary Unix socket.
- Assert the public router returns non-leaking `404` for every admin path.
- Test unavailable database, incompatible schema, oversized body, malformed JSON, occupied/inaccessible socket, client disconnect, and graceful shutdown.

### Accept when

- `journald` starts, reports liveness/readiness honestly, and exits cleanly.
- Admin routes are absent—not merely middleware-protected—on the public listener.
- Long-running or blocked database work cannot grow without a configured bound.

## S4 — Protected administration, authentication, and enrollment

**Status:** implemented and verified on Unix. `tests/s4_bootstrap_test.py` bootstraps a fresh daemon entirely through the CLIs, exercises the credential-class matrix, inspects secret outputs and persisted digests, and forces lost-response and post-commit file failures through deterministic forwarding proxies. Rust integration tests cover transaction rollback, subprocess termination, concurrent exchange, expiry, revocation, and installation ownership. Delivery and runtime integrations remain unresolved.

**Goal:** create the security bootstrap needed to test every later endpoint honestly.

### Build

- Add high-entropy token generation and SHA-256 token digests; these are random bearer tokens, not human passwords.
- Implement host-local admin operations for principals, spaces, memberships, adapter provisioning, credential rotation/revocation, and enrollment tickets.
- Implement principal-client and delivery-adapter authentication as distinct credential classes.
- Implement one-use enrollment exchange in one transaction: hash lookup and expiry check, ticket consume, adapter/principal binding, separate credential issuance, and hash-only persistence.
- Implement the corresponding `aj-admin` Unix-socket methods and `aj enroll --ticket-file`.
- Write credentials atomically to separate mode-`0600` destinations and never print their values.
- Rotate by immediate atomic revoke-and-replace, returning the replacement secret once through the protected Unix socket.
- Provide empty-`204` credential revocation and enrollment recovery operations. Recovery revokes both credential lineages, including rotations, before fresh-ticket enrollment for the same installation; consumed tickets never replay and another installation cannot take over.

### Test

- Full credential-class matrix for public, delivery, and admin operations.
- Expired, consumed, revoked, wrong-class, wrong-principal, and malformed credentials.
- Failpoints after ticket lookup, consume, credential creation, and file write.
- Assert failed enrollment cannot leave a consumed ticket with missing credentials.
- Subprocess tests inspect argv, stdout, stderr, traces, and SQLite rows for secrets.
- Unix-socket permissions and peer-access failure tests.
- Lost enrollment responses and post-commit file-write failures require explicit administrator recovery; lost rotation output requires revoking the inaccessible replacement.

### Accept when

- A fresh database can be bootstrapped entirely through `aj-admin` and the protected socket.
- Enrollment returns each secret exactly once and stores only digests centrally.
- Delivery credentials cannot append; principal credentials cannot claim delivery.

## S5 — First product vertical: append, read, and list

**Goal:** deliver the first useful end-to-end journal behavior.

**Implemented:** UUIDv7 records; principal/space discovery; authenticated atomic
append, get, and filtered sequence listing; typed client methods; and
`aj me/spaces/post/get/list`. Migration 3 preserves relation order and persists
the cursor MAC secret. S5 includes same-space backward relation validation and
bounded route/filter/principal-bound pagination because append and list cannot
safely defer these invariants to S6. Search and thread projections remain S6.

Acceptance evidence is exercised by `journal-service/tests/records.rs`,
`journald/tests/records_http.rs`, `journal-client/tests/records.rs`, and
`make records-test` (Unix, bootstrapped through S4). Tests include concurrent
sequences, UTF-8 and collection boundaries, rollback failpoints, child-process
termination before/after commit, restart pagination, and lost HTTP responses.

### Build

- Add UUIDv7 generation when record IDs become real.
- Implement `/v1/me`, relevant principal/space discovery, append, single-record read, and deterministic record listing.
- Implement append as one explicit transaction: authorize; validate/canonicalize; allocate sequence; insert record and relations; validate recipients; create mailbox items and ordinal-1 attempts; persist idempotency result.
- Derive the author from authentication and the timestamp from the server.
- Implement `aj me`, `aj spaces`, `aj post`, `aj get`, and `aj list` through typed `journal-client` methods.

### Test

- Repository transaction and service authorization tests.
- Black-box API and CLI tests bootstrapped through S4.
- Concurrent append tests proving unique monotonic per-space sequence numbers.
- Replay tests after a lost response; conflicting payloads return `409`.
- Fail after each append step and assert complete commit or complete rollback.
- ACL/non-leaking `404` tests for record reads and lists.

### Accept when

- An enrolled principal can append, read, and list using only `aj`.
- Every successful addressed append has exactly one initial mailbox obligation per recipient.
- A lost response replays the original immutable result.

## S6 — Relations, search, threads, and bounded pagination

**Goal:** make retained history useful without leaking inaccessible content.

Implemented: authorized FTS5 search with document-local matching-span rank,
sequence pagination, bounded whole-tree `reply-to` projections, typed clients,
and `aj search/thread`. Rank does not use global BM25 statistics. Migration 4
adds the reverse reply index. Protocol and OpenAPI specify traversal budgets
and fail-closed exhaustion behavior. Portable tests exercise authorization,
query/cursor rejection, concurrent append pagination, Unicode/NUL scoring,
cycles, and independent depth/node/edge limits. The Unix `records-test` gate
also exercises search/thread CLI behavior; it must run on a Unix host.

### Build

- Enforce same-space, backward-only relation rules in service and storage.
- Implement deterministic sequence pagination and bounded thread projection.
- Implement FTS5 search with ACL predicates inside SQL before rows, snippets, ranking, or counts leave storage.
- Support deterministic `order=seq`; label ranked search best-effort under concurrent writes.
- Enforce cursor route/filter/order fingerprints and separate traversal depth/node/edge budgets.
- Add typed `aj search` and `aj thread` commands.

### Test

- Missing, self, forward, cross-space, and inaccessible relation targets.
- Search for a token appearing only in unauthorized content; verify no result, snippet, count, or relation leak.
- Cursor reuse with changed routes/filters/order and concurrent writes between pages.
- Thread cycles and traversal-budget exhaustion.

### Accept when

- Sequence pages are deterministic and resumable.
- Unauthorized records do not influence observable search output.
- Ranked search never claims snapshot completeness.

## S7 — Adapter registration, heartbeat, claims, and lease expiry

**Goal:** make central mailbox custody claimable without yet injecting a runtime.

Implemented: installation-bound registration and heartbeat, protected CAS replacement,
credential-bound bounded claims, lazy same-attempt expiry, current-membership suppression,
and recipient/admin mailbox status. Migration 5 binds new claims to credentials and
cancels unbound legacy claims without replacing attempts. Empty selections wait on
notifications outside SQLite and blocking-worker permits; retries also observe expiry
and out-of-process mutations. Typed clients and `aj`/`aj-admin` commands accompany
the endpoints. S8 custody, telemetry, requeue, record delivery-status, and admin adapter
listing remain explicit `501`; no spool or runtime injection is implemented.

Acceptance gates: `journal-service/tests/delivery.rs`, `journald/tests/delivery_http.rs`,
`journal-client/tests/delivery.rs`, the migration contract, and `make delivery-test`
(real Unix service/CLI provisioning, lost claim response, expiry, replacement,
fresh enrollment, and membership suppression).

### Build

- Implement self-registration, heartbeat, generation fencing, bounded claim, and mailbox status.
- Make restart with the same credential/installation idempotent; require admin compare-and-swap replacement for a different installation.
- Claim only the authenticated principal's mailbox, rechecking current space membership before exposing content.
- Suppress revoked pending items without returning their bodies.
- Allow one active claim per adapter generation and at most 20 items.
- Wait for long-poll notification outside transactions and blocking-worker permits, then retry the bounded claim transaction.

### Test

- Fake-clock state-machine tests and competing claims.
- Registration replacement races and stale-generation heartbeat/claim rejection.
- Revocation immediately before selection.
- Lease expiry and lost claim response; the same attempt ID returns to pending.
- Long-poll tests assert no SQL transaction remains open while waiting.

### Accept when

- Claims expose only the authenticated recipient's currently authorized records.
- Stale installations cannot resume by heartbeat.
- Lease expiry never fabricates a new attempt.

## S8 — Host custody, telemetry, status, and requeue

**Goal:** complete and prove the central delivery state machine.

**Implemented:** service transactions, public/protected routes, typed clients,
and CLI commands. Migration 6 retains exact custody receipts and immutable
telemetry history. Retryable telemetry can recover to runtime acceptance on the
same attempt; runtime acceptance, route-unavailable, and terminal failure are
final except for exact replay. Requeue rejects pending/claimed obligations and
unreadable recipients, creates a fresh ordinal/ID, and retains every prior
attempt. Old-attempt telemetry cannot change the new mailbox projection.
Service tests cover partial/idempotent commits, expiry, credential and generation
binding, authorization, telemetry transitions/replay, and transaction failpoints.
HTTP/client tests and `tests/s7_delivery_test.py` exercise the S8 surfaces.
Local spool durability and runtime injection remain S9–S11, not S8 guarantees.

### Build

- Implement generation-bound batch/partial custody commit with per-attempt idempotent results.
- Advance to `host-accepted` only for an exact active claim/item/attempt binding.
- Accept telemetry only after host custody, from the authenticated recipient adapter, with the four narrow telemetry states.
- Make `event_id` replay idempotent and conflicting reuse an error.
- Implement admin requeue as a retained new ordinal/new attempt, never history deletion.
- Implement delivery-status visibility: authorized author sees all recipients; addressed recipient sees only itself; other readers get the same `404` as unauthorized record access.

### Test

- Partial commits, lost responses, repeated commits, stale generations, expired leases, wrong claim/item/attempt, and cross-principal attempts.
- Telemetry before custody, invalid transitions, exact replay, and conflicting event replay.
- Requeue failpoints around ordinal allocation and attempt creation.
- Complete delivery-status authorization matrix.

### Accept when

- The central service never reports host custody without a valid commit.
- Telemetry cannot advance pending or claimed attempts.
- Lease retry preserves attempt identity; explicit requeue changes it and retains history.

## S9 — Durable adapter spool and process lock

**Goal:** make local custody crash-safe before any vendor runtime is touched.

**Implemented:** `SqliteStore` persists complete attempts in local schema version 1
using SQLite rollback journals with `synchronous=EXTRA`. A nonblocking `fs2`
machine-local lock lives for the connection's lifetime. The pinned `fs2` dependency
also provides cross-platform filesystem free-space checks. Admission checks count
retained rows/serialized bytes and reserve disk headroom; callers can check the
maximum proposed batch before claiming. Transitions remain available under logical
admission pressure and fail atomically on actual SQLite exhaustion.

Recovery is keyset-paginated (1–100 rows), includes unconfirmed custody, and honors
persisted retry times. Accepted, route-unavailable, and terminal outcomes are final.
Explicit compaction drops terminal body payloads but preserves bindings, receipts,
failure detail, and an original SHA-256 fingerprint for duplicate-put validation.
Real-file tests kill a child before and after each put/custody/start/result/retry/
compaction commit, and cover process contention, corruption, SQLite full rollback,
and exact byte/item/reserve limits. No central schema, HTTP, or credential changes.
S10 orchestration and S11 runtime conformance remain separate gates.

An explicit `reconcile_expired_claim` transition handles restart after durable
put but before custody when lease expiry reissues the same attempt. It requires
an exact authoritative `lease-expired` custody result and changes only the claim
binding of an untouched unconfirmed row, atomically with its fingerprint.
Tests cover restart, rejection of changed bindings or uncertain custody, confirmed
and completed rows, capacity/SQLite rollback, and process death on both sides of
reconciliation commit. Authenticated central reconciliation remains S10.

### Build

- Replace `NotImplementedStore` with a local SQLite spool.
- Persist the complete envelope, claim/mailbox/record/attempt IDs, installation, generation, custody confirmation, injection state, receipt/failure detail, and next-attempt time.
- Implement idempotent transitions for put, custody confirmation, injection start, acceptance, retryable failure, route unavailable, and terminal failure.
- Retain compact accepted/terminal tombstones to prevent accidental reinjection.
- Implement bounded recoverable-row enumeration, item/byte/free-disk pressure, and an exclusive machine-local process lock.

### Test

- Reopen/recovery and conflicting-binding tests.
- Child-process kill tests before commit, after commit, and during every state transition.
- Two-process lock contention.
- Corruption, disk full/reserve, item limit, and byte limit tests.

### Accept when

- A complete attempt survives process death.
- Accepted/terminal tombstones never appear as injectable recovery work.
- Pressure stops new claims before durable storage is exhausted.

## S10 — Generic adapter orchestration

**Goal:** implement custody-before-injection against fake boundaries.

### Build

- Implement register → heartbeat → claim → durable spool → custody commit → persist confirmation → fence recheck → route resolution → envelope rendering → injection → result persistence → telemetry.
- Add bounded exponential retry with fake-clock scheduling.
- Apply the default route only when `routing_key` is absent. Unknown or disabled explicit routes become `route-unavailable` and never fall back.
- Keep runtime targets and route bindings local.
- Keep correlated replies on the principal credential, never the delivery credential.

### Test

- Fake Journal, Spool, RouteResolver, and Runtime unit tests.
- End-to-end temporary `journald` plus real spool tests.
- Every crash boundary in `docs/protocol.md`, especially runtime acceptance followed by process death before receipt persistence.
- Stale generation immediately before injection, route removal during recovery, revocation, and duplicate runtime send.
- Exact envelope snapshot and resolved-route assertions.

### Accept when

- Runtime injection is structurally impossible before persisted custody confirmation.
- The runtime receives the resolved private route and exact trusted/untrusted envelope.
- Reported acceptance matches only the strongest receipt returned by the runtime boundary.

## S11 — Executable fake-runtime conformance

**Goal:** turn adapter semantics into a reusable gate for every runtime implementation.

### Build

- Implement the fake runtime described in `conformance/fake-runtime/README.md`.
- Make `conformance/adapter/scenarios.yaml` executable rather than documentary.
- Support availability toggles, duplicate acceptance, recorded envelopes, resolved-route inspection, and accept-then-crash behavior.
- Run the same suite against the generic adapter and every future runtime adapter.

### Test

- Fencing, spool-before-custody, lost commit, restart, lease expiry, requeue, stale generation, route failure, telemetry ordering, revocation, and duplicate injection.
- Use real child-process crashes for recovery claims, not only mocked errors.
- Emit redacted persisted-state evidence keyed by scenario ID.

### Accept when

- Every scenario ID passes with durable-state evidence.
- The suite demonstrates at-least-once behavior and visible duplicate risk rather than hiding it.
- Hermes and Muse still identify as unresolved.

## S12 — Operations, recovery, and safe read-only web

**Goal:** make the runtime-neutral core operable and inspectable.

### Build

- Add stable authorized record URLs, timeline, thread, search, and delivery summaries.
- Render untrusted Markdown with no raw HTML, safe schemes only, no external images, and a restrictive CSP.
- Add transport-fact metrics: database/WAL size, pending age/count, claims, heartbeats, spool pressure, runtime failures, and backup age.
- Complete restore fencing: close ingress, quiesce adapters, restore, invalidate claims/registrations, reconcile spools/checkpoints, run ACL/search/sequence/mailbox probes, then reopen.

### Test

- Browser/API tests for raw HTML, `javascript:`, `data:`, external images, forged envelopes, malicious snippets, and CSP.
- Backup under write load and isolated restore verification of counts, hashes, ACLs, sequence heads, FTS, mailbox attempts, and registration invalidation.
- Disk, WAL, and free-space pressure tests.

### Accept when

- No active content or unauthorized information is exposed.
- A restore remains closed to traffic until every recovery probe passes.
- Dashboards describe custody and runtime telemetry without inventing model-read states.

## Conditional runtime adapters

Hermes and Muse are separate, non-blocking workstreams after S11. Each adapter gets its own PR and release gate:

1. record the supported runtime version;
2. identify a supported injection surface;
3. document busy-session and restart behavior;
4. define the strongest honest acceptance receipt;
5. keep opaque runtime targets in destination-local configuration;
6. pass all S11 fake-runtime scenarios;
7. pass a private Alpha → Journal → Beta → correlated-reply canary.

If a surface cannot be revalidated, that adapter stays an explicit status-2 stub. One runtime's evidence never substitutes for the other's.

## Test architecture

Use progressively wider tests; do not replace lower layers with a giant end-to-end suite.

| Layer | Purpose | Runs |
| --- | --- | --- |
| Domain/protocol unit | Validation, serialization, canonicalization, cursors, state enums | Every PR |
| SQLite integration | Real migrations, transactions, FTS5, ACL predicates, backup | Every PR after S2 |
| Router/service | Authorization and response semantics without process/network noise | Every PR after S3 |
| Black-box process | Real listeners, Unix socket, CLI, shutdown, file permissions | Every PR after S3/S4 |
| Security-negative | Wrong credentials, cross-principal access, non-leakage, secret scans | Every PR after S4 |
| Crash/recovery | Kill at named durable boundaries and reopen real files | Every PR after S9 |
| Adapter conformance | Shared scenario IDs and fake runtime | Every PR after S11 |
| Private canary | Installed runtime version and correlated reply | Manual release gate |

### Determinism requirements

- Inject clocks, UUID sources, and random token sources at service boundaries.
- Use test-only named failpoints around durable transitions.
- Wait on explicit events or bounded conditions, never arbitrary multi-second sleeps.
- Give every concurrency test a hard timeout and include state diagnostics on failure.
- Keep production code free of test behavior unless behind a narrow injected interface.

## CI evolution

Preserve the current Rust 1.85 locked baseline on every PR:

```bash
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 test --locked --workspace --all-targets
cargo +1.85.0 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.85.0 build --locked --workspace
python3 tests/migration_contract_test.py
python3 scripts/validate_openapi.py api/openapi.yaml
python3 scripts/public_hygiene.py
```

Add jobs only when their slice lands:

1. `contract`: OpenAPI, Redocly, fixture coverage, Markdown, hygiene.
2. `sqlite`: Rust integration, FTS5, migration rollback, backup/restore; Linux plus a macOS smoke job.
3. `http`: temporary service, public/Unix listener isolation, auth, body limits, readiness.
4. `security-negative`: credential matrix, revocation, search non-leakage, secret scans.
5. `cli`: subprocess JSON fixtures, exit classes, file modes, no-secret output.
6. `delivery-recovery`: deterministic failpoints and process crash/restart tests.
7. `adapter-conformance`: every scenario ID against every implemented adapter.
8. `browser-security`: only after S12.
9. Manual/nightly: backup under load, isolated restore, and private runtime canaries.

Do not allow `#[ignore]`, a stub return, or a successful empty handler to satisfy a slice gate.

## First three PRs

### PR 1 — Contract execution and fixture coverage

- Extend the existing validator with fixture parsing and operation/security/limit coverage.
- Add deterministic contract mutations.
- Add no production dependencies.
- Preserve status-2 stubs.

**Exit:** explicit coverage of all 29 paths/31 operations; every mutation fails for the intended reason.

### PR 2 — Domain/protocol kernel

- Complete append/read/list/search/common-error DTOs.
- Add duplicate-key rejection, canonical append encoding, and cursor primitives.
- Pin only the base64/HMAC/SHA-256 cursor dependencies and timezone-database-free RFC 3339 parser exercised by this slice.

**Exit:** stable wire fixtures and no need for downstream ad hoc JSON parsing.

### PR 3 — SQLite kernel

- Add pinned `rusqlite` and implement connection policy, migration runner, transaction helpers, FTS5 probe, and backup/restore primitives.
- Keep HTTP handlers, credentials, and adapter behavior out.

**Exit:** a real Rust process creates, migrates, queries, backs up, restores, and validates the existing schema.

## Explicitly deferred

- MCP, A2A, federation, brokers, PostgreSQL, HA, or multi-active adapters.
- Task ownership, workflow execution, model spawning, or action authorization.
- Exactly-once injection or `read`/`understood`/`completed` receipts.
- Central runtime session/chat/hook identifiers.
- Public import/provenance APIs or ordinary-client historical impersonation.
- Artifact/blob storage, retention compaction, deletion semantics, or rich chat UI.
- Automatic URL fetching, raw HTML, command execution, or authority inferred from record content.
