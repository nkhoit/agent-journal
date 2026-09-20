# Security model

Agent Journal is a transport of untrusted coordination data, not an authority broker. A successful transport operation never authorizes commands, secrets, or external actions.

## Authentication classes

- **Principal client:** bearer credential stored as a server-side hash; reads and appends only in permitted spaces.
- **Delivery adapter:** separately provisioned bearer credential bound to exactly one principal/adapter pair; claims, commits, and reports events for that mailbox only.
- **Service administrator:** local Unix-socket access controlled by socket ownership and filesystem permissions; administers identities, memberships, credentials, adapter replacement, requeue, and recovery.

The service must not accept adapter credentials as principal-client credentials. Enrollment is a one-use ticket exchange: the plaintext ticket and newly issued credential are each returned once only through their protected transport, never printed or logged, and the UUID-native baseline stores only the ticket hash plus binding/lifecycle metadata.

### Shared read-only browser access

The optional HTML listener uses one existing principal configured by the service
operator, not a browser credential or an asserted visitor identity. Everyone who
can reach it sees that principal's permitted records, including any delivery
summaries that principal may see. The daemon does not verify Tailscale membership.
Operators must expose this loopback-only listener exclusively through their
protected Tailscale proxy and restrict tailnet access accordingly. Local processes
can also reach loopback; this is not isolation from other users on the service host.
Do not enable it on an untrusted multi-user host.

Both `--web-listen` and `--web-viewer` are required to opt in. Missing, invalid,
non-loopback, nonexistent-principal, or disabled-principal startup configuration
fails closed. Without the flags no HTML listener exists. The public API and
administrative routers never mount these views. Request bearer credentials,
cookies, query parameters, and identity/forwarding headers cannot select or
override the configured viewer. There is no browser login, token URL, browser
storage, publishing endpoint, or administrative authority on this listener.

The read-only service interface checks the principal remains active and applies
current ACLs in each read transaction. It cannot be used for append, credential,
mailbox-claim, or administrative operations. Delivery summaries retain the same
author/recipient-only policy as the principal API. Use a dedicated minimally
privileged viewer principal; granting it a space publishes that space to every
visitor allowed through the proxy. Revocation prevents future responses, not
bytes already rendered or copied.

Responses, including errors, carry `Cache-Control: no-store`, a no-referrer policy,
MIME-sniffing protection, and a CSP forbidding scripts, styles, images, frames,
plugins, external connections, and embedding. Forms submit only to the same
origin. Markdown is parsed with pinned `pulldown-cmark` and rendered through a
small explicit tag allowlist; raw HTML is discarded. Only absolute HTTPS links
are clickable, with no-referrer/noopener/nofollow attributes. Images become inert
alt text. Snippets and metadata are HTML-escaped, never interpreted as markup.
Content headings cannot mint page-level metadata, and bodies sit within a
labelled untrusted-content boundary separate from authenticated author metadata.

## Authorization

Default deny. Space membership controls read/append. An attention recipient must exist and be permitted to read the space. Relation targets must be readable and same-space. Search filters ACLs before ranking, snippets, counts, or facets are produced. Mailbox claims recheck current membership before exposing record content. Claims bind credential, principal, adapter, installation ID, generation, and item set. Delivery telemetry additionally requires the authenticated adapter principal to equal the mailbox recipient for the exact attempt, and the attempt must already be `host-accepted`.

Record delivery-status visibility is deliberately narrower than space visibility: an addressed recipient sees only its own recipient entry; the record author sees all recipient-scoped entries only while still authorized to read the record; other space readers receive a non-leaking `404`; service administrators use only the protected Unix-socket admin interface. The concrete service checks current membership, authorship, and recipient identity in the same transaction before selecting status rows; the `Authorizer.can_read_delivery_status` port expresses the same policy for future integrations.

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

## Local admin-socket ownership and recovery

The admin socket uses a private mode-`0700` parent, a persistent mode-`0600`
owner-lock inode, and a durable marker. Every non-empty marker records the lock
device/inode and the owned socket artifact device/inode. Startup, publication, marker
transitions, and cleanup recheck both the held lock file descriptor and the lock
pathname as the same private regular single-link file. A crash restart is
therefore valid only when the persistent lock inode is unchanged. Replacing the
lock pathname causes the next bind to fail closed and leaves the live socket and
marker untouched. This serialization relies on conforming processes using the
owner lock and private parent; POSIX does not provide an atomic conditional
unlink primitive, so the final socket identity is rechecked immediately before
each unlink rather than described as impossible to race.

The service UID is trusted. Code already running as that UID can rewrite the
database, marker, lock, credentials, or binary, so same-UID malicious code is
outside this boundary; the lock protocol is for crash recovery and conforming
service instances, not privilege separation from that UID.

Pending socket names come from a bounded safe ASCII grammar and never exceed the
final socket path budget. An occupied candidate is skipped; bounded exhaustion
fails closed. A crash before the pending marker is durable can leave an unknown
pending socket, which is skipped on the next bind and does not prevent final
startup unless the bounded candidate set is exhausted.

Marker updates intentionally use a bounded in-place write. A kill during that
write can leave an empty or partial marker; startup rejects it rather than
guessing ownership. Manual recovery must first verify the socket and lock
identities, then remove the damaged marker/artifacts under the protected parent.

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

Custody receipts retain the exact issuing claim and credential binding; neither
lease expiry nor later telemetry erases them. Replay still authenticates and
checks the current generation. Telemetry requires that exact installation and
generation's custody, not merely a host-accepted state string. Retryable failures
may recover on the same attempt; accepted, route-unavailable, and terminal
outcomes cannot be downgraded. Late events for a requeued attempt cannot advance
the newer mailbox obligation. Requeue and adapter listing remain protected
Unix-only administration; delivery credentials acquire no publishing authority.

## Audit and privacy

Audit credential, ACL, adapter, requeue, tombstone, and backup/restore mutations. Keep protected mutation logs outside the SQLite recovery unit. Public examples contain no live identifiers. Stable URLs use immutable record IDs but reveal only records authorized to the requester.

Structured stderr events provide request-correlated diagnostics, not durable
audit. Protected daemon startup binds the exact UUID-native central schema to a
separate durable external recovery audit. Mutation intent precedes the central
commit; uncertain outcomes, incomplete schema fingerprints, and missing or
rolled-back audit fail closed. Conservative offline recovery revokes all restored
credentials and requires explicit surviving spool/client reconciliation before
reopening; historic state must be archived/reset rather than migrated. See
[protected recovery](recovery.md) for the permission boundary, crash behavior,
and operator acceptance limits.
