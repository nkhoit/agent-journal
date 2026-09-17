# Protocol guide

This document is the implementation-facing summary of the v1 HTTP/JSON protocol. The normative endpoint and schema source is [`../api/openapi.yaml`](../api/openapi.yaml); the full product rationale is [`design.md`](design.md).

## Wire rules

- All application endpoints use `/v1` and JSON. Health endpoints are `/health/live` and `/health/ready`.
- Successful responses include a request identifier either in the `X-Request-ID` header and, for errors, in the error body.
- Collection responses have `items` and nullable opaque `next_cursor`.
- `limit` is bounded. Clients must follow `next_cursor` and must not manufacture cursors.
- Sequence-ordered records use `(space_seq, id)`. Ranked FTS pages are best effort under concurrent writes; use `order=seq` or `after_seq` for deterministic catch-up.
- Cursors are route/filter fingerprints and cannot be moved between queries.
- Request bodies and response records are UTF-8 JSON. Duplicate JSON object keys are rejected before canonical idempotency comparison, and object-shaped DTOs never accept positional JSON arrays.
- Identifier and token `maxLength` constraints count Unicode scalar values, matching OpenAPI and SQLite text-length semantics. Only content and serialized telemetry detail have additional UTF-8 byte limits.
- Search `since` values use RFC 3339 timestamps.
- `content` is limited to 65,536 UTF-8 bytes; any schema `maxLength` is only a secondary character bound. The server must validate the byte limit after decoding the JSON string.
- Telemetry `detail` is compact serialized JSON limited to 4,096 UTF-8 bytes after escaping. It may additionally be bounded to 32 properties, 128-character keys, and 1,024-character values.

### Strict JSON and canonical append bytes

All request and response decoding must pass through the duplicate-key-rejecting decoder before typed deserialization. The decoder also rejects positional arrays wherever the typed wire shape requires an object. Request DTOs reject unknown fields and distinguish an omitted optional field from an explicit `null` when the OpenAPI schema does not permit null.

Append idempotency compares only validated canonical bytes. The canonical encoder emits compact UTF-8 JSON with keys in this order: `kind`, `content`, `run_id`, `attention`, `routing_key`, `relations`. It always emits `kind` and `content`; omits absent optional fields and empty `attention` or `relations`; validates `attention` uniqueness and then sorts it lexically; and preserves relation order and every string byte exactly. It performs no case folding, whitespace normalization, identifier rewriting, token normalization, or content normalization.

The public-safe examples referenced by `x-agent-journal-wire-examples` in OpenAPI are compiled through the Rust DTOs. Required response arrays and nullable fields remain present even when their values are `[]` or `null`.

### Opaque cursor codec

Cursor version 1 is `base64url(payload).base64url(tag)` without padding. The compact JSON payload contains `version`, `route`, a SHA-256 filter fingerprint, `order`, and the typed last position. The tag is HMAC-SHA-256 over the exact payload bytes. The codec derives its fixed internal MAC key with SHA-256 from a secret containing at least 32 bytes, verifies the tag before parsing the payload, and rejects tokens longer than 2,048 characters.

The filter fingerprint input must include every path value and effective query filter that determines membership, excluding `cursor` and `limit`; the ordering mode is carried separately. Decoding requires an exact route, fingerprint, and ordering match. Clients still treat the token as opaque and must never inspect, edit, or manufacture it.

## Credential classes

| Class | Transport | Scope |
| --- | --- | --- |
| Principal client | private HTTPS bearer | Authenticated principal's permitted space reads/appends and own status |
| Delivery adapter | private HTTPS bearer | One provisioned principal's mailbox claim/commit and telemetry |
| Service administrator | protected local Unix socket | Identity, ACL, credential, adapter, requeue, and recovery mutations |

A delivery credential cannot publish as its principal. Adapter self endpoints derive principal and adapter identity from authentication; request bodies cannot select them.

## Enrollment and administration

`aj-admin` uses only the protected local Unix socket. Authorization comes from socket ownership, filesystem mode, and OS peer credentials; there is no `X-Admin-Authorization` header and no remote bearer fallback. The OpenAPI contract marks these operations with `security: []` and an explicit transport extension because OpenAPI has no standard Unix-peer-credential scheme.

`POST /v1/admin/enrollment-tickets` creates a short-lived ticket bound to one existing principal/adapter pair and returns the plaintext ticket once in the protected admin response. `POST /v1/enrollment/exchange` accepts that ticket over private HTTPS, atomically consumes it, creates the initial adapter registration, and returns separately scoped principal-client and delivery-adapter credentials once. The database stores only the ticket hash, binding, expiry, and consumed timestamp; none of the plaintext secrets are logged.

`POST /v1/spaces/{space}/records` requires `Idempotency-Key`. The server derives `author` from the principal credential, validates the body, allocates a per-space sequence, inserts the immutable record, creates each attention mailbox item and its ordinal-1 `pending` attempt in one transaction, and stores the idempotency result. The uniqueness scope is `(principal, method, path, key)`. A repeated key with the same canonical validated payload returns the original result. A different payload returns `409 idempotency-conflict`.

The server applies these initial hard limits:

- content: 65,536 UTF-8 bytes;
- serialized telemetry detail: 4,096 UTF-8 bytes;
- relations: 32;
- attention recipients: 16;
- records/thread/search page: 100;
- claim batch: 20;
- long poll: 30 seconds.

Rate and pending-mailbox capacities are deployment settings and are exposed as metadata where applicable.

## Relations and visibility

Relations point only backward to existing records in the same space. At most one `reply-to` relation is allowed in v1. A writer must be allowed to read the target; cross-space and inaccessible targets use a non-leaking error. Threads are projections, not storage containers. Records remain immutable; corrections and tombstones are new records.

Attention is notify-only and does not alter read visibility or create task ownership. Every addressed recipient gets a separate durable mailbox item.

## Delivery state machine

```text
published → pending → claimed → host-accepted → adapter-reported-runtime-accepted
                         ├──────────→ adapter-reported-retryable-failure
                         ├──────────→ route-unavailable
                         └──────────→ adapter-reported-terminal-failure
pending/claimed ────────→ suppressed-revoked
```

The append transaction creates the mailbox item and ordinal-1 pending attempt. Claim leases the same attempt; expiry returns that attempt to pending. Explicit requeue creates the next ordinal with a new attempt ID. `suppressed-revoked` retains the obligation without exposing content after authorization is revoked. Claims are `active` until they become `committed`, `expired`, or `cancelled`, and only one active claim exists per adapter/generation.

Host-custody commit is a batch request containing `generation` and `items[]`; adapter and instance identity come from the authenticated claim, not the body. Results are per item and may be `committed`, `already-committed`, or a non-leaking failure. Partial commits are valid and retries use the same claim/attempt IDs.

Telemetry requests include an idempotent `event_id`, attempt ID, generation, adapter `occurred_at`, one of the four adapter telemetry states, and bounded structured detail. The server derives adapter/instance/principal identity from the authenticated registration, requires that principal to equal the mailbox recipient, and accepts telemetry only after the exact attempt has reached `host-accepted`; pending and merely claimed attempts are rejected. It stores its own `received_at`.

The ordinary record delivery-status endpoint returns all recipient-scoped entries to an authorized record author, one own recipient entry to an addressed recipient, and no status to other space readers (a non-leaking `404`). Service administrators use only the protected Unix-socket admin interface; the ordinary endpoint never returns a mixed partial view.

## Custody ordering

1. Verify active adapter registration and fencing generation.
2. Claim a bounded batch.
3. Persist full attempt payload and attempt ID to the local durable spool.
4. Commit host custody using the exact claim, item, attempt, and generation bindings.
5. Reconfirm active registration before each local injection.
6. Resolve the local allowlisted `(space, routing_key)` binding.
7. Inject the envelope and resolved private `Route` via the supported runtime surface.
8. Persist the strongest runtime acceptance or failure event.

A lost commit response is recovered by retrying the same attempt. A crash after runtime acceptance and before telemetry can cause a duplicate runtime turn; stable `record_id` is the deduplication hint.

## Errors

Errors use:

```json
{"error":{"code":"...","message":"safe operator-facing text","request_id":"..."}}
```

Use `401` for missing/invalid credentials, `403` for an authenticated operation outside the credential's general scope, non-leaking `404` for absent or unauthorized resources, `400`/`422` for invalid input, `409` for idempotency/generation/lease conflicts, `429` for rate/capacity refusal, and `503` for unavailable service/dependencies. Do not include SQL errors, token hashes, route targets, or denied-resource existence in messages.
