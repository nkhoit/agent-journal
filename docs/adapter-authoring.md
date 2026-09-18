# Adapter authoring

An adapter is a destination-host process, not a second journal implementation. It transfers one principal's central mailbox obligations into a vendor runtime while keeping runtime identifiers and bindings local.

## Required boundary

```text
principal mailbox → durable local custody → local route → runtime injection
```

Implement the shared ports in `crates/journal-adapter-core` and the durable contract in `crates/journal-adapter-spool`; use the protocol fixtures under `conformance/`. The adapter must:

1. use a delivery-only credential bound to one principal and adapter identity;
2. register a persistent random installation ID and renew its fencing generation with heartbeat;
3. hold a machine-local exclusive process lock;
4. claim bounded batches and stop before local item, byte, or free-disk limits are exhausted;
5. write the complete record, claim ID, instance ID, generation, and attempt ID to a durable spool before host-custody commit;
6. retry the same claim/attempt after a lost commit response;
7. persist an idempotent custody-confirmed transition before any runtime injection;
8. enumerate recoverable spool rows after restart, including pending custody and non-terminal injection states;
9. deduplicate ordinary retries by `attempt_id` and retain `mailbox_item_id` and `record_id`;
10. recheck registration/fencing before every new runtime injection;
11. resolve only configured `(space, routing_key)` keys to private local targets, then pass the resolved `Route` together with the trusted `Envelope` to `Runtime::inject`;
12. retain `route-unavailable` as final for that attempt; fixing a route requires explicit central requeue rather than resuming the old attempt;
13. render trusted envelope metadata separately from the untrusted body;
14. report bounded non-secret runtime telemetry only after exact host acceptance; detail is compact serialized JSON of at most 4,096 UTF-8 bytes;
15. use a separate principal-client credential for optional correlated replies;
16. stop or back off on expiry, fencing, revocation, runtime failure, or local pressure.

## Local state

Enrollment credentials are separate principal-client and delivery-adapter secrets. Persist each atomically in mode-`0600` storage without printing it. Lost enrollment responses or failed credential writes require protected administrator recovery that revokes both credential lineages, followed by a fresh ticket for the same installation. Never replay a consumed ticket or use recovery to claim another installation's registration.

A durable spool persists `claim_id`, `instance_id`, `generation`, the complete envelope, a custody-confirmed flag, an injection lifecycle state, and any runtime receipt or safe failure detail. `put`, custody confirmation, injection-start, acceptance, and failure transitions are idempotent for the same attempt and reject conflicting claim/generation data. `recoverable` returns unfinished rows after restart; accepted, route-unavailable, and terminal rows remain as compact tombstones after payload retention so an old attempt cannot be accidentally reinjected. Store runtime targets only locally. Keep secrets in the host secret mechanism, not in the spool.

`journal-adapter-spool::SqliteStore::open(path, limits)` implements local schema
version 2 with SQLite `journal_mode=DELETE` and `synchronous=EXTRA`. Use one fixed
database path in an existing private directory on a machine-local filesystem;
network filesystems and hard-link aliases are unsupported. Symlink database and
lock paths are rejected. The persistent `.lock` sidecar is exclusively locked
until close/drop, including across process termination. Never unlink it while
an adapter may be running. Directory ownership/permissions (or Windows ACLs)
must prevent other users from replacing database or lock files.

Before claiming, call `check_capacity` with the proposed batch's maximum item
count and serialized byte size. This is an admission check, not a reservation;
`put` repeats it atomically with insertion. Limits include retained tombstones.
The disk check reserves four times incoming bytes plus 16 KiB per incoming row,
in addition to the configured free-space reserve (64 MiB by default). Logical
limits stop new puts, not custody/result persistence. Other processes can still
consume disk after a check; SQLite errors roll back and must stop the adapter,
never be treated as custody. The byte cap measures retained serialized rows,
not database file size. Compaction frees reusable SQLite pages, not necessarily
filesystem bytes. Exhausted tombstone capacity needs operator sizing, not deletion
of deduplication history.

Only fresh pending, unconfirmed attempts may be put. Exact original puts remain
idempotent after transitions and compaction through a retained SHA-256 fingerprint.
If the process dies after `put` but before central custody, expiry may return the
same attempt under a new claim. Do not overwrite it with `put`. Retry the exact
old custody request through the authenticated central client first. Only an
exact `lease-expired` item result permits `reconcile_expired_claim(result, replacement)`;
timeouts, local lease calculations, missing claims, and stale generations do not.
Obtain the replacement from a fresh claim using the same credential, installation,
and generation. The spool checks the result's claim/generation/item/attempt and
requires every replacement field except `claim_id` to match the pending,
unconfirmed row. Confirmed, in-flight, completed, and compacted rows cannot rebind.
Reconciliation atomically updates the row, byte accounting, and put fingerprint;
old puts then fail, and exact new puts/reconciliation retries are idempotent while
unconfirmed. Growth remains subject to admission limits. It does not confirm
custody: commit the new claim centrally and persist its confirmation before injection.
The caller is responsible for authenticated result provenance and fresh-claim
validation; the generic orchestrator performs those authenticated client operations.
`mark_retryable` atomically stores failure detail and next retry time; the shared
port's `mark_injection_failed` retryable variant has no delay. Receipt and failure
strings are capped at 4,096 UTF-8 bytes. `recoverable_after` offers keyset pagination
over 1–100 unfinished rows including pending custody; injection still requires
`ready_for_injection()` and an external registration/fencing recheck. In-flight
recovery may duplicate a runtime turn, never imply exactly-once delivery.

Call `compact` only after reporting a final outcome and deciding its body retention
period is over. It drops the body but retains IDs, claim/installation/generation,
metadata, custody, outcome and receipt/detail. A tombstone returned by `get` is not
a complete injectable envelope. Unknown schema versions and malformed databases
fail closed; never delete or recreate a spool to recover accepted custody. This
schema upgrades version 1 atomically, retaining its rows and original put fingerprints.
Legacy rows retain their original envelopes; no missing full record or already-reported
terminal event is invented. Version 1 binaries reject version 2. There is no downgrade:
back up while the owner is stopped and restore only with compatible software and
central fencing reconciliation. Route configuration and credential persistence remain
outside this store's schema.

## Generic orchestration

Compose `journal-adapter-core::Adapter` with `DeliveryJournal`, an open
`SqliteStore`, a `RouteResolver`, a `Runtime`, and a `Clock`. `DeliveryJournal`
owns only the delivery credential and delegates to `journal-client::Client`;
there is no reply/publish operation on this port. Enroll and persist the installation
and credentials through the existing protected bootstrap before constructing it.
Runtime-specific binaries still exit with status 2.

Call `tick` from one synchronous owner, outside an async executor thread. Each
tick performs at most one recovery row or one single-item claim. A caller owns
shutdown and idle polling cadence. `Progress::Backoff` means no network/runtime
work occurred. A transient `JournalUnavailable` is returned, not hidden, after
persisting its next eligible time. Other errors stop processing and require
caller/operator handling. In particular, fencing, authorization, unexpected
responses, and spool failures must not be treated as runtime acceptance.

Before each claim, admission checks one item and a conservative 1 MiB serialized
bound for the retained complete record plus envelope, including worst-case JSON
escaping. The bound trades throughput for predictable admission. `put` checks
the actual bytes again. A single-item batch preserves the exact original custody
request across every crash without a second batch journal. A lost claim response
has no replay key; claim conflicts back off until central expiry. Reconciliation
never substitutes a new claim for an uncertain old custody response.

Recovery uses a one-row keyset cursor and wraps after a pass. Delayed runtime rows
do not block later rows. Custody confirmation is reread from durable storage before
routing. The active registration is checked again after injection-start persistence
and immediately before the runtime call. A forced replacement can still race that
last check; no local design can eliminate the check-to-send window.

Only an absent key selects the default route. An explicitly empty, unknown, or
disabled key produces a final `route-unavailable` outcome. Route configuration
changes are observed on recovery, but never revive final attempts. The runtime
receives the resolved private `Route`, the structured `Envelope`, and its rendered
text. Metadata values are JSON-quoted, so newlines cannot forge metadata headers.
Run attribution, route labels, and the body remain data, never authority. Structured
metadata/body separation is authoritative; textual body delimiters are not a parser
or security boundary. See the executable
[rendered snapshot](../conformance/adapter/orchestrated-envelope.txt).

`RuntimeUnavailable` schedules a retry; `RuntimeRejected` records a final failure.
A successful runtime return asserts runtime acceptance only. Implementations must
return a bounded, non-secret acceptance reference, not a raw vendor response.
It is stored locally and never copied to central telemetry, since references may
identify private runtime targets. Arbitrary runtime error text is discarded.
Central detail is empty; the state itself is
the strongest honest result. Unexpected runtime errors propagate without inventing
a receipt. Death after runtime acceptance but before result persistence leaves an
in-flight row that may send again.

`AdapterSpool::finish` atomically stores the result, retry count/time, and exact
pending event. A SHA-256 event ID derives from attempt ID and a durable event
sequence. Its timestamp and payload are immutable while pending. Outbox enumeration
includes terminal rows independently of ordinary injection recovery; an event
must be acknowledged before another injection for that attempt. Compaction rejects
pending events. Retryable runtime delay doubles from 1 to 256 seconds; transport
delay uses the same cap and a durable scheduler row, including across restart.
No retry loop sleeps inside a transaction. Telemetry outages conservatively pause
network processing rather than allowing an unbounded local event backlog.

## Configuration shape

See [`../config/examples/adapter.yaml`](../config/examples/adapter.yaml). The example contains no real route target. A production adapter must reject missing credentials, duplicate installation use, unknown route keys, and unsafe spool paths before claiming.

## Conformance sequence

S7 central registration, heartbeat, claims, expiry, replacement, and mailbox status
are available through typed `journal-client` methods. A lost claim response is not
replayed: another claim conflicts until its lease expires. Re-registration with the
same valid installation credential keeps the generation; replacement requires protected
administration and fresh enrollment. See the [claim protocol](protocol.md#central-mailbox-claims).

A heartbeat conflict permits one same-installation registration retry per fence
check. The adapter retains its previous authority and accepts renewal only if the
principal, adapter, installation, and generation are unchanged and active.
Transport outages can therefore outlast the registration lease without stranding
durable custody or telemetry work; replacement and revocation still fail closed.
S8 custody commits, post-custody telemetry, status, and protected requeue are
available through typed clients and CLIs. S9 local durable spooling is implemented;
the generic S10 composition connects it to this sequence.
Central commit is an assertion of existing local durability, not proof that a
CLI user has spooled the payload. Retry the exact claim/item/attempt after a lost
commit response. Retryable runtime failures may later report acceptance on the
same attempt with a new event ID; accepted, route-unavailable, and terminal
outcomes are final. Retry a lost event response with its unchanged event ID and
payload. See [custody receipts and runtime results](protocol.md#custody-receipts-and-runtime-results).

Use a fake runtime before connecting a vendor runtime:

- register and heartbeat fence stale installations;
- claim returns no more than the configured batch maximum;
- crash before spool commit leads to redelivery;
- crash after spool commit retries custody for the same claim and attempt;
- runtime injection is impossible before idempotent custody confirmation;
- restart enumerates and resumes recoverable spool rows;
- lease expiry preserves an attempt ID;
- explicit requeue creates a new attempt ID;
- stale generation is rejected and stops processing;
- default routing applies only when no key is supplied;
- unknown/disabled keys do not fall back and are not injected;
- the runtime receives the resolved private route and the exact envelope;
- envelopes contain stable IDs and untrusted-content warnings;
- telemetry is rejected for pending/claimed attempts or a cross-principal mailbox;
- runtime receipts are reported as acceptance telemetry, never comprehension.

## Runtime-specific integration

Do not infer an injection surface from a vendor name or old local installation. Hermes and Muse are deliberately unresolved in this scaffold. Record the supported version, exact API/CLI/hook behavior, concurrency semantics, receipt strength, restart behavior, and canary evidence in deployment-local evidence before implementing the corresponding adapter.

Never use terminal keystrokes, direct edits to a runtime's internal database, shell interpolation of record content, or a fresh unrelated session per delivery as a substitute for a supported integration.
