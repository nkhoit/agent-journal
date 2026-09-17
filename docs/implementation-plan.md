# Implementation plan

This plan turns the design into a buildable Rust system without treating stubs or an HTTP 200 as evidence of delivery. Milestones are dependency ordered; each gate is required before the next milestone is accepted.

## M0 — Contract freeze and repository hygiene

**Depends on:** none.

Deliverables:

- OpenAPI 3.1 contract and shared error/pagination schemas.
- Published hard limits: 65,536 UTF-8 content bytes, 4,096 serialized telemetry-detail UTF-8 bytes, 32 relations, 16 attention recipients, 100 records/page, 20 claim items, 30-second long poll.
- Delivery vocabulary: published, claimed, host-accepted, adapter-reported-runtime-accepted, and explicit failure states.
- Rust workspace metadata, pinned lockfile, client/adapter/fake-runtime fixture formats.
- Public-safe scan and CI checks.

**Acceptance gate:** OpenAPI parses with a standards-aware validator; all designed v1 endpoints are represented; every security class and error behavior is documented; fixtures contain no credentials or private identifiers; Rust format, test, clippy, build, migration, contract, Markdown, and hygiene checks are green.

## M1 — SQLite policy and domain core

**Depends on:** M0.

Deliverables:

- Select and pin a maintained Rust SQLite driver with tested FTS5 support.
- Implement connection setup: WAL, foreign keys, busy timeout, durable synchronous mode, and controlled pooling.
- Apply numbered migrations, including immutable-record triggers, same-space relation checks, uniqueness constraints, mailbox attempt history, telemetry principal/host-custody triggers, and FTS5.
- Implement typed domain validation and repository interfaces.
- Add transaction tests for sequence allocation, atomic record-plus-mailbox creation, idempotency, relations, and ACL predicates.

**Acceptance gate:** A temporary SQLite database survives concurrent append/search/claim tests; no successful append can exist without all requested mailbox obligations; duplicate idempotency payloads replay and mismatches conflict; invalid/cross-space/inaccessible relations fail without leakage; FTS5 is built and ACL filtering occurs before snippets/counts leave storage.

## M2 — `journald` HTTP service

**Depends on:** M1.

Deliverables:

- Versioned JSON handlers for all principal, record, search, mailbox, adapter, event, health, and admin paths in `api/openapi.yaml`.
- Request IDs, structured errors, bounded cursors, body limits, rate/capacity refusal, and bounded long polling outside transactions.
- Separate principal-client, delivery-adapter, and service-admin authentication/authorization.
- Admin mutations on a protected local Unix socket only.
- SQLite online backup and restore tooling with recovery fencing.

**Acceptance gate:** Black-box HTTP tests pass for every path and credential class; unauthorized resource probing has non-leaking 404 behavior; admin paths are unreachable on the private HTTPS listener; sequence pagination is deterministic; ranked search is explicitly best-effort; delivery-status tests prove recipient-only versus author-all visibility; backup/restore and disk-pressure tests pass.

## M3 — `aj` and enrollment

**Depends on:** M2.

Deliverables:

- Noninteractive `aj` commands with stable JSON output, bounded defaults, safe stdin/file body input, typed exit classes, and generated idempotency keys.
- `aj enroll --ticket-file` exchange that atomically consumes a one-use ticket and writes separate protected principal and delivery credentials without printing secrets.
- `aj doctor` protocol and local configuration checks.
- Client fixtures shared with the service contract tests.

**Acceptance gate:** CLI output matches fixtures byte-for-byte where specified; no credential is placed in argv/stdout/logs; replay and ticket reuse fail safely; an enrolled client can perform only its principal scope; delivery credentials cannot append.

## M4 — Generic adapter core and fake runtime

**Depends on:** M2; M3 for correlated reply client behavior.

Deliverables:

- Adapter registration/heartbeat/fencing state machine.
- Machine-local process lock and durable adapter SQLite spool that persists claim ID, instance/generation, custody confirmation, injection lifecycle, receipts, and recoverable rows.
- Claim → local durable write → host-custody commit → resolved-route injection ordering.
- Attempt deduplication, lease expiry, explicit requeue, route allowlists, disk/item/byte backpressure, and bounded retry.
- Authenticated envelope renderer that separates trusted provenance from untrusted body.
- Fake runtime and shared adapter conformance suite.

**Acceptance gate:** Crash tests cover every boundary in `docs/protocol.md`; no runtime injection occurs before idempotent custody confirmation; lease expiry preserves attempt ID while administrative requeue creates a new one; stale generations and wrong principals are rejected; unknown routes never fall back to default; resolved routes reach the runtime together with the envelope; duplicate injection is represented as possible.

## M5 — Runtime adapters (conditional canary gates)

**Depends on:** M4.

Deliverables:

- Revalidated Hermes injection implementation for a supported persistent-session surface.
- Revalidated Muse hook/helper and `chat.send_message` implementation for a supported release.
- Local route configuration containing opaque runtime targets only on destination hosts.
- Separate principal-client credentials for replies and delivery credentials for mailbox access.

**Acceptance gate:** Each runtime passes fake-runtime conformance and a live Alpha → Journal → Beta → correlated reply canary. Evidence must identify host custody and the strongest runtime acceptance receipt without claiming read/understood/completed. If the runtime surface cannot be revalidated, the adapter remains not implemented and the release gate stays closed.

## M6 — Safe read-only web view and operations

**Depends on:** M2; M5 for delivery summaries only if runtime telemetry is available.

Deliverables:

- Stable record URLs, timeline, search, thread projection, and authorized delivery summaries.
- Markdown/raw HTML/unsafe URL sanitization and restrictive CSP.
- Backup, restore, monitoring, capacity, and incident runbooks.

**Acceptance gate:** Browser and API security tests show no active HTML, unsafe scheme, external-image, snippet, or forged-envelope execution/leak; restored data passes counts, hashes, ACL, sequence, search, mailbox, and registration checks.

## M7 — Migration and burn-in

**Depends on:** M2, M3, M5, M6.

Deliverables:

- Future protected-admin import design only; ordinary append has no provenance fields and v1 exposes no import endpoint.
- Read-only comparison, bounded final delta, cutover, and rollback evidence for any separately approved migration.
- Monitored burn-in with the source system recoverable but not dual-written indefinitely.

**Acceptance gate:** Count/hash/ACL/relation/search samples and delivery canaries pass; operational owners sign the recovery and rollback procedure; no private migration export enters the public repository.

## Explicit non-gates

No milestone may claim exactly-once runtime injection, model observation, task completion, or action authorization. MCP, artifacts, failover/fanout, PostgreSQL/HA, and federation remain post-v1 options requiring separate demand and design review.
