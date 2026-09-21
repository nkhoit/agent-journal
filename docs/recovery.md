# Protected recovery

Central restore is an offline operation. Close external ingress, stop `journald`,
and stop every adapter before restoring. The audit's lifetime exclusive lock
prevents `journal-recover` from running while the daemon or its database workers
still own that audit. There is no public recovery endpoint, remote administrator
bearer, or Windows permission fallback.

## UUID-native compatibility boundary

Only the UUID-native current schema is supported. Before any protected startup,
archive/reset every pre-UUID central database (including empty schema-7 files)
and every non-current local spool. There is no in-place migration, automatic
audit adoption for legacy state, or schema downgrade. See
[`uuid-native-clean-break.md`](uuid-native-clean-break.md) before invoking a
recovery command.

## Recovery units and current-schema operation

The current UUID-native schema includes `recovery_anchor`, containing a random journal identity, a
monotonic audit revision, and an audit-required marker. First protected startup
creates a security baseline only for a fresh, unanchored UUID-native database. For an existing deployment,
stop and inspect the deployment before that first protected initialization: this baseline cannot
reconstruct security changes lost before audit adoption. Schema downgrade is unsupported; rollback requires a compatible UUID-native
backup and protected recovery review, never deletion, replay, or editing of historical state.

`journald --recovery-audit "$AUDIT"` selects the external audit. By default,
`journal.db` uses sibling `journal.recovery.db`. Its parent must be an existing
private directory. Audit and lock files are mode `0600` on Unix; symlink files,
hard-linked audit files, and non-private parents are rejected. Keep the persistent
lock file; never unlink it to bypass a running owner.

First initialization builds the complete audit in a private sibling named
`<audit>.initializing`, syncs it, renames it to the final audit path, and syncs
the directory before committing central adoption. Reserve that sibling name and
its SQLite `-journal` sidecar for initialization. Under the lifetime lock, an
interrupted staging file can be discarded and rebuilt only when the final audit
is absent and the central anchor is not audit-required and remains at revision
zero. If publication was durable but the initializer crashed before that anchor
update, protected startup read-validates the complete matching revision-zero
lineage under the audit lock, atomically sets `audit_required`, and reuses the
same audit bytes. It never creates a replacement or adopts an absent, malformed,
foreign, mismatched, or closed audit. Required audits are never replaced by this
retry path.

The audit is a separate SQLite database using rollback journaling and
`synchronous=EXTRA`. A sibling file is a separate recovery unit, not a separate
disk fault domain. Protect and replicate it independently. Never overwrite it
with a central backup. Missing required audit, an older audit head, a different
journal identity, unresolved intent, or a closed gate refuses startup. Loss of
the authoritative external audit is not an automatically recoverable condition.
Keep ingress closed and obtain protected authoritative evidence; do not clear
the anchor or manufacture a replacement audit.

Every mutating service transaction durably records its proposed security snapshot
and next revision before committing the central transaction. The central revision
commits with the mutation; only then is external completion recorded. Read-only
operations do not advance the revision. Failure or process death between those
commits is deliberately uncertain, not reported as rollback. Subsequent service
operations fail closed; unresolved prepared input intents require explicit
archive/reset rather than reconciliation. Protected read, status,
and verification gate checks serialize with in-flight audit writers, waiting for
completion rather than treating transient prepared intents as recovery failures;
abandoned intents still fail closed. Snapshots include principal
disablement, spaces, ACLs, installation ownership, registration generations,
credential and ticket metadata, audit history, relations, and space sequence
heads. They contain credential digests, not plaintext bearer secrets, and must
never be published.

Snapshots prioritize a straightforward inspectable recovery representation over
compactness. Audit growth and serialized mutation overhead must be included in
deployment capacity measurements. There is no audit pruning or rotation command.

## Backup

The storage online-backup primitive supports concurrent writers. The protected
operator command additionally records durable backup evidence after all probes;
it requires the daemon to be stopped because it takes the same lifetime lock.

```sh
journal-recover backup "$DATABASE" "$AUDIT" "$BACKUP"
```

The destination must not exist and its parent must be private. Backup evidence
records counts and SHA-256 hashes for every ordinary table, ACL state, per-space
heads, full SQLite/foreign-key/FTS integrity, exact FTS content, and attention,
mailbox, and attempt consistency. A timestamp is recorded only after these
checks succeed. Raw `Database::backup_to` calls have no external audit timestamp.

## Restore and reconcile

1. Close external ingress, stop the daemon, and quiesce every known adapter,
   including replaced, stale, and offline installations. Preserve surviving
   spools, client checkpoints, and audit evidence. The CLI cannot stop a runtime;
   `--adapters-quiesced` is an explicit operator attestation, not an RPC.
2. Restore into a new private path, never over a live database or its WAL files:

   ```sh
   journal-recover restore "$DATABASE" "$AUDIT" "$BACKUP" "$RESTORED" "$APPROVAL" --adapters-quiesced
   ```

   The gate closes before backup validation. Full pre/post-copy hashes must
   agree. Security snapshots restore current principal, space, ACL, and
   installation state. All restored credentials are revoked, all tickets
   invalidated, active claims cancelled, and claimed attempts returned to pending
   without changing their IDs. Registrations advance beyond the surviving audited
   generation and become revoked. Historical records, custody receipts, attempts,
   and telemetry are retained. No custody or runtime result is fabricated.
3. Review the mode-`0600` approval template. Its prior audited and restored space
   heads expose rollback. Its installation inventory includes every installation
   retained in the audit, not merely currently active adapters. Compare every
   surviving spool, including accepted/compacted tombstones and pending telemetry,
   against restored records, attempts, receipts, and generations. Compare every
   client checkpoint and retained append input against restored sequence heads.
   Record missing records, lost acknowledgements, stale custody, and potential
   duplicate runtime turns in protected operator evidence.
4. Do not delete a spool to make reconciliation appear successful. Quarantine
   old-generation work that cannot authenticate after re-enrollment. Only use
   explicit protected requeue for a retained eligible mailbox obligation after
   reviewing duplicate-injection consequences. Reset/replay client checkpoints
   only after deciding which lost records require republishing. The tool does
   not automatically rewrite destination-local spools or client checkpoints.
5. Only after the inventory is demonstrably complete and the rollback and
   duplicate-delivery consequences are accepted, set `inventory_complete` and
   `accepted_record_loss` to `true`. Do not edit the hashes, heads, revision, or
   inventory. Unknown, missing, or unexamined surviving evidence means stay closed.
6. Re-run probes and reopen the exact reconciled database:

   ```sh
   journal-recover reopen "$RESTORED" "$AUDIT" "$APPROVAL"
   ```

   A changed database, incomplete inventory, stale approval, active restored
   authority, or failed probe refuses reopening. Reconciliation advances the audit
   revision so the former central file cannot reopen accidentally. Restart
   `journald` using both the restored database and the same surviving audit path.
   Re-enroll both credential classes through protected Unix administration for
   each retained installation. Reopen external ingress only after deployment ACL,
   search, and delivery canaries.

For a closed, resolved input snapshot, stop/quiesce as above and use:

```sh
journal-recover reconcile "$DATABASE" "$AUDIT" "$NEW_APPROVAL" --adapters-quiesced
```

This is not a success override. It revokes credentials/tickets, advances fencing,
runs probes, and generates a new unapproved template. An uncertain prepared input
intent is archive/reset-required: restore and reconcile reject it before durable
recovery mutation or destination publication, preserve audit evidence, and leave
service admission closed. Repeated attempts and an approval cannot turn an
uncertain input into an approved reconciliation. Denying memberships or disabling
existing principals would not protect public spaces from a newly registered
principal after reopening, so neither is a recovery fallback.

A successfully completed reconciliation has a prepared output head until
`reopen`; its matching durable reconciliation evidence and exact approved
inventory still allow reopening. Do not confuse this with an uncertain input
intent. A crash before that durable evidence exists remains archive/reset-required.
Ordinary verified committed-snapshot restore remains supported. There is no
automatic reset, deletion, or migration. An audit older
than the central revision is rejected rather than replayed. A damaged or failed
restore stays closed; preserve the failed destination and retry to a fresh path.

## Evidence and limits

Storage tests use real files, deterministic in-flight writes during backup,
corrupt FTS/attention cases, incomplete approvals, missing and rolled-back audit,
exclusive-lock contention, ACL/ownership replay, and retained custody. Child
processes are killed after external prepare, after central commit, after closure,
and after reconciliation. Restart remains closed until a fresh verified approval.
The cross-platform storage tests do not establish Unix filesystem authorization
or actual daemon/CLI acceptance on Windows; Unix gates require a native Unix
runner. Deployment spool/client review and private runtime canaries remain
operator acceptance work, not an automated guarantee.
