# Agent orientation

This guide applies repository-wide unless a deeper AGENTS.md overrides it.

## Read before editing

Read README.md, docs/implementation-plan.md, docs/registration-inbox-spec.md,
docs/protocol.md, api/openapi.yaml, migrations/0001_uuid_native.sql,
docs/security-model.md, docs/recovery.md, docs/inbox-client-authoring.md and
CONTRIBUTING.md. If these disagree, preserve the product/security model and
reconcile wire DTOs, SQL, fixtures, docs and tests together.

Check `git status --short` and `git branch --show-current`. Do not overwrite
unrelated work. Work in an isolated feature worktree, not the main checkout.

## Current architecture

Schema 13 implements durable principal registration, explicit public spaces,
immutable records, attention inboxes and acknowledgment receipts. Core operations
do not depend on a runtime. Hermes and Muse are optional principal clients:
fetch inbox, perform supported private handoff, then ack.

There is no delivery credential class, adapter enrollment/identity/installation,
heartbeat/generation, claim/lease, host custody, attempt, runtime telemetry,
requeue or mandatory local spool. Do not recreate these in a new framework.

The optional worker uses stable inbox IDs and runtime-native dedupe, with only
volatile cursor/backoff/ack-pending state. Hermes admission is not comprehension.
Muse durable drop publication is not hook execution. Restart, finite vendor
dedupe retention, consumed drop files and older-backup restores can duplicate
handoff. Recommend one logical automated consumer per principal; there is no
central competing-consumer protocol.

## Repository map

- `journal-domain`, `journal-protocol`: record types, limits, strict wire DTOs,
  canonical requests and authenticated cursor primitives.
- `journal-storage-sqlite`: exact schema admission, synchronous transactions,
  FTS, protected audit, backup and recovery.
- `journal-service`: principal authority, records, inbox and receipt privacy.
- `journal-client`, `aj`: typed transport, private credentials and principal CLI.
- `journald`, `aj-admin`: separate public/local admin routers and protected CLI.
- `journal-inbox-worker`: bounded polling, private routes and handoff boundary.
- `journal-runtime-hermes`, `journal-runtime-muse`: supported vendor transports.
- `journal-inbox-hermes`, `journal-inbox-muse`: optional executables.
- `journal-runtime-fake`, `journal-lock-test`: executable test boundaries.
- `api`, `conformance`, `tests`, `scripts`: normative contract and acceptance.

## Implementation rules

Use boring Rust: concrete structs, explicit SQL/transactions and small modules.
Keep Rust 1.85, edition 2024 and pinned dependencies. Change Cargo.lock with an
exercised manifest change. No ORM, broker, generic SDK or speculative layers.

Async handlers run synchronous SQLite work through bounded blocking execution.
No database transaction or blocking-worker permit may be held during polling
sleep. Public HTTPS never mounts admin operations. Administration remains on the
private Unix socket with kernel peer checks; no remote admin bearer fallback.

Authority comes from authenticated principal/server context, never caller body
identity. A principal credential grants both posting and its own inbox access.
Keep runtime IDs, sessions, chats, routes, hooks and local receipt evidence out of
central records and public fixtures. No secret or private target in logs/argv.

Default routes apply only when the routing key is absent. Unknown, empty or
disabled explicit keys fail closed without acknowledgment. Content, attribution,
route labels, Markdown and snippets are untrusted data, never execution authority.

## Invariants

- Append atomically allocates the per-space sequence, inserts the immutable
  record/relations, creates all attention/inbox items and stores idempotency.
  Server authorship/time and exact replay semantics remain authoritative.
- Relations are same-space, backward-only and bounded, with at most one reply-to.
- Public-space policy is explicit. Archive rejects new appends, not reads/acks.
  Membership metadata cannot restrict public access or grant admin transport.
- Apply current access before ranking, snippets, counts, threads and receipt
  projection. Disabled principals cannot use ordinary reads or ack.
- Inbox allocation is recipient-local and monotonic. Fixed-bound keyset passes
  end despite arrivals and restart without a cursor for failed earlier items.
- Fetch never reserves or acknowledges. Ack is recipient-only, bodyless,
  idempotent and retains the first server timestamp and full item/record history.
  Missing, foreign and inaccessible items return non-leaking 404.
- Receipt status is narrower than record visibility: authorized author sees all,
  recipient only its own, other readers receive 404. Never add runtime outcomes.
- Platform handoff must succeed before automatic ack. Failed items remain
  pending, emit safe diagnostics and do not permanently block later work.

## Recovery

Retain protected external mutation intent/completion, backup integrity and exact
approved reopening. Restore audited credential/registration bindings revoked,
including post-backup registrations and rotation/recovery descendants. Never
reactivate credentials, overwrite conflicting bindings or use INSERT OR REPLACE
to erase retained identity history.

Restore keeps recipient allocation high-water marks, changes inbox epoch and
retains backup receipt state. Explicit operator inventory/loss approval accepts
lost later acks and repeated handoff. The inventory is principal clients, not
adapter installations or spools. Unknown prepared input stays archive/reset-
required. Completed reconciliation with exact durable evidence can reopen.

Old schemas/audits are rejected without mutation, migration or automatic reset.
Clean reset cannot blacklist unknown secrets from discarded audit state.

## Evidence and validation

Write negative/boundary tests with behavior. Use deterministic clocks/randomness,
real temporary databases/files, failpoints and child-process kills at durable
boundaries. Preserve auth, disabled, receipt privacy, record rollback and browser
negative coverage when replacing fixtures. Empty/ignored tests and success
stubs are not acceptance.

Run focused gates, then the full baseline:

```sh
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 test --locked --workspace --all-targets
cargo +1.85.0 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.85.0 build --locked --workspace
python3 tests/migration_contract_test.py
python3 tests/delivery_retirement_test.py
python3 scripts/validate_openapi.py api/openapi.yaml
python3 scripts/public_hygiene.py
make check CARGO="cargo +1.85.0" OPENAPI_STANDARDS_LINT=1
make browser-security CARGO="cargo +1.85.0"
git diff --check
```

Unix permission/daemon tests require a native Unix filesystem, not a mounted
Windows directory that ignores chmod. Record exact commands/results and earlier
failures; do not conceal cancellation/init flakes or call skips passes.
Report behavior, compatibility/API/schema impacts, recovery boundaries and
remaining deployment acceptance honestly.

Never commit credentials, production databases/backups, raw private runtime
captures, real host identities, installation facts or generated binaries.
Publish only redacted conformance JSON. No live canary claims without evidence.

## Out of scope

No MCP, A2A, federation, PostgreSQL/HA, Redis/NATS/Kafka, multi-consumer leases,
workflow/task ownership, model spawning, artifact storage, rich chat,
public provenance/import APIs, groups/private spaces or semantic completion
receipts without explicit product revision.
