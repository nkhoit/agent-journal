# Security model

Agent Journal is a transport of untrusted coordination data, not an authority broker. A successful transport operation never authorizes commands, secrets, or external actions.

## Authentication classes

- **Principal client:** bearer credential stored as a server-side hash; reads and appends only in permitted spaces.
- **Delivery adapter:** separately provisioned bearer credential bound to exactly one principal/adapter pair; claims, commits, and reports events for that mailbox only.
- **Service administrator:** local Unix-socket access controlled by socket ownership and filesystem permissions; administers identities, memberships, credentials, adapter replacement, requeue, and recovery.

The service must not accept adapter credentials as principal-client credentials. Enrollment is a one-use ticket exchange: the plaintext ticket and newly issued credential are each returned once only through their protected transport, never printed or logged, and the migration stores only the ticket hash plus binding/lifecycle metadata.

## Authorization

Default deny. Space membership controls read/append. An attention recipient must exist and be permitted to read the space. Relation targets must be readable and same-space. Search filters ACLs before ranking, snippets, counts, or facets are produced. Mailbox claims recheck current membership before exposing record content. Claims bind credential, principal, adapter, installation ID, generation, and item set. Delivery telemetry additionally requires the authenticated adapter principal to equal the mailbox recipient for the exact attempt, and the attempt must already be `host-accepted`.

Record delivery-status visibility is deliberately narrower than space visibility: an addressed recipient sees only its own recipient entry; the record author sees all recipient-scoped entries only while still authorized to read the record; other space readers receive a non-leaking `404`; service administrators use only the protected Unix-socket admin interface. The service layer exposes this decision through `Authorizer.CanReadDeliveryStatus` rather than inferring it from storage rows.

Revocation stops future central access and claims. It cannot recall bytes already accepted into a local spool or runtime transcript; operators must treat those as exposed and rotate/recover accordingly.

## Untrusted-content handling

Record content, Markdown, `kind`, `run_id`, routing key, relation labels, future imported provenance, snippets, and runtime details are data. Never:

- execute or evaluate content;
- fetch URLs automatically;
- interpolate content into shell commands or SQL;
- let a record choose a runtime session;
- render raw HTML or unsafe URL schemes;
- include secrets in telemetry or error messages; telemetry detail is compact serialized JSON capped at 4,096 UTF-8 bytes;
- treat envelope-like text inside a body as authenticated provenance.

Adapters place authenticated envelope metadata outside the body where the runtime supports structured metadata. Text-only runtimes still receive an explicit warning, but authority checks remain outside the model.

## Fencing and recovery

Credential rotation is one atomic revoke-and-replace transaction with immediate revocation, not an overlap period. The replacement secret is returned once through protected Unix administration and written atomically to mode-`0600` storage without stdout or logging. A lost response or post-commit file-write failure requires administrator revocation of the inaccessible replacement; rollback or secret replay is not available.

Enrollment response loss or failure to persist either credential requires protected enrollment recovery for the bound adapter and installation. Recovery revokes both principal-client and delivery-adapter credential lineages, including rotated descendants, before a fresh ticket is issued for that same installation. A consumed ticket is never replayable. A different installation cannot use recovery or a fresh ticket to take over the existing installation.

Only one active adapter installation is allowed per principal in v1. Registration has a server-issued generation and `lease_expires_at`; heartbeats renew that lease. Replacement is compare-and-swap and fences later central operations from stale generations. Claims explicitly transition from `active` to `committed`, `expired`, or `cancelled`; the database permits only one active claim per adapter/generation, so expiry must be recorded before another claim is created. The unavoidable local check-to-send race means planned replacement should drain first and forced replacement must accept possible duplicate runtime turns.

Replacement revokes both old enrollment credential lineages and outstanding tickets
before transferring installation ownership. New credentials require fresh enrollment
for the replacement installation; old secrets never gain authority over it. Ordinary
registration only renews the credential's already-bound installation. Expired
heartbeats and stale generations fail closed; same-installation registration can
renew an expired lease without changing generation. Claim selection repeats these
checks inside its write transaction and stores the issuing credential ID.

Central restore is a recovery event: close ingress, quiesce adapters, reconcile protected audit events, invalidate claims/registrations, advance generations, compare spools/checkpoints, run ACL and delivery probes, and reopen only after evidence is complete. If evidence is incomplete, remain read-only and rotate affected credentials.

## Audit and privacy

Audit credential, ACL, adapter, requeue, tombstone, and backup/restore mutations. Keep protected mutation logs outside the SQLite recovery unit. Public examples contain no live identifiers. Stable URLs use immutable record IDs but reveal only records authorized to the requester.

The current structured stderr events provide request-correlated operational diagnostics, not durable external audit. A `bootstrap_committed` event is emitted after a successful central transaction, but its absence cannot establish rollback or safe retry. Neither stderr nor the SQLite-local credential audit satisfies the protected external recovery-log requirement; that remains an operations acceptance gate.
