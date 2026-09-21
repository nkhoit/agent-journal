# Implementation and acceptance gates

The governing product specification is
[registration and durable inbox](registration-inbox-spec.md).
Its four slices are identity, public policy, principal inbox, and optional
clients with central delivery retirement. Schema 12 is the coherent target;
the core does not depend on runtime integration.

## Dependency order

```text
strict wire/domain contract
  -> exact SQLite admission and protected audit
  -> independent principal registration/recovery
  -> public-space immutable records and append replay
  -> recipient inbox allocation, fetch and first ack
  -> optional bounded handoff clients
  -> operational, browser and deployment acceptance
```

Use concrete Rust, explicit transactions, bounded work and narrow real boundaries.
Changes to API/schema/credentials update types, SQL, fixtures, clients, docs and
tests together. An empty test, ignored test or successful stub is not acceptance.

## Core gates

| Surface | Required evidence |
| --- | --- |
| Wire contract | Exact 22 paths/23 operations, references/security/body shapes, required nullable fields, duplicate-key and unknown-field rejection, mutation tests |
| Schema admission | Fresh schema 12, exact objects/constraints, older schema/audit rejection without reset, sidecar/hardlink safeguards |
| Registration | Durable client preparation, exact replay, concurrent retries, body/handle conflict, no revoked/expired/disabled token reuse, process interruption |
| Principal recovery | Atomic revoke/replace, UUID/profile/inbox retention, disabled state, response loss and private-write failures recoverable by UUID |
| Records | UUIDv7 IDs, immutable content, atomic record/attention/inbox/idempotency, rollback and process-kill boundaries, exact replay after mutable policy/profile changes |
| Reads | Bounded keyset list/search/thread, policy before ranking/snippets/counts, malformed filters/cursors, depth/node/edge limits |
| Inbox | Recipient-local allocation, fixed-bound passes under arrivals, first-ack retention, bodyless 204, failed/foreign/disabled/inaccessible outcomes, receipt privacy |
| Listeners | Public/admin/viewer separation, kernel Unix peer checks, body/shutdown bounds, private socket lifetime and crash recovery |

The sole baseline SQL filename is historical; it is not an automatic migration
script. No ticket, delivery credential, adapter identity, heartbeat, claim,
custody, attempt, runtime telemetry or requeue operation/table remains.

## Optional client gates

The shared worker and fake runtime must prove handoff-before-ack, stable inbox
dedupe identity, exact envelope separation, default-versus-explicit routing,
runtime failure without ack, and progress past failed items. More than 100 items
and continuous arrivals must finish a pass and revisit early failures.

Backoff uses injected tick time and bounded caches. Lost ack responses retry only
ack while success remains in memory; inaccessible ack outcomes never become
success or permanent head-of-line blocks. Restart may repeat a stable-key
handoff. Capacity must not silently evict known success.

Hermes retains supported Runs API transport and capability preflight. Tests
enforce advertised durable idempotency/retention and exact key/body mapping,
not actual vendor durability beyond the advertisement.

Muse retains private durable drop publication. Tests use real files for replay,
conflict, unsafe paths, unavailable directories and process death around
publication. A file receipt does not establish hook processing; consumed files
and hook retry need a durable hook-side seen-set.

Both clients have actual `journald` integrations covering handoff then ack,
runtime/route failure without ack and killed clients between handoff and ack.
The fixed [scenario manifest](../conformance/inbox-client/scenarios.yaml) and
runner reject missing/duplicate cases and zero matching tests.
Only redacted per-case JSON/completion manifests may be published.

## Recovery gates

Keep external prepare/commit audit and exact-state approval. Real-file/process
tests cover missing/rolled-back audit, initialization publication, locks,
uncertain input rejection and completed reconciliation reopening.

Older-backup restore preserves audited identities/profile aliases and revoked
credential/registration bindings, including post-backup registration, rotation
and recovery. Conflicting bindings fail closed. It preserves recipient allocator
heads, invalidates inbox epochs and accepts lost newer acks only under explicit
client inventory/loss approval. No adapter/spool inventory or receipt-delta audit
is required or fabricated.

## Validation commands

Run focused tests before the full baseline. Native Unix filesystem/permissions
are required for administration, browser daemon and runtime integration gates.

```sh
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 test --locked --workspace --all-targets
cargo +1.85.0 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.85.0 build --locked --workspace
python3 tests/migration_contract_test.py
python3 tests/delivery_retirement_test.py
python3 -m unittest tests/contract_gate_test.py
python3 scripts/validate_openapi.py api/openapi.yaml
python3 scripts/public_hygiene.py
make check CARGO="cargo +1.85.0" OPENAPI_STANDARDS_LINT=1
make browser-security CARGO="cargo +1.85.0"
git diff --check
```

CI pins Redocly 1.34.3 and browser dependencies. The separate privileged
foreign-UID harness must not count a skip as success. Preserve actual command
output, failures and cancellation/initialization flakes; report them accurately.

## Remaining deployment acceptance and non-goals

Local tests do not establish production ingress policy, audit capacity, disk
exhaustion under real deployment load, live vendor guarantees, hook processing,
model observation or completion. Those require protected deployment evidence.

No groups/private-space release, broker, streaming, federation, task/workflow
engine, competing-consumer leases, model spawning, automatic migrations or
exactly-once runtime semantics are part of these gates.
