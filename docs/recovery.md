# Protected recovery

Recovery is offline and host-local. Close external ingress, stop `journald`,
and quiesce principal clients and optional inbox workers. The audit's lifetime
exclusive lock excludes the operator command while the daemon or its database
workers still own that audit. There is no public recovery endpoint, remote
administrator bearer or Windows permission fallback.

## Compatibility and audit ownership

Only exact schema 13 central state and its current external snapshot format are
supported. Older databases, audit formats and legacy spools require explicit
operator archive/reset. There is no in-place migration, automatic reset or
adoption of incompatible state. Preserve evidence before an operator reset.

Fresh central initialization exclusively creates a `<database>.initializing`
marker before opening SQLite and removes it only after the initializing
connection closes. Concurrent initializers wait for that publication boundary,
then perform ordinary exact-schema admission. Normal error unwinding closes the
connection and removes its marker too; any incomplete database still fails
exact-schema admission. Fresh initialization never reuses a marker abandoned by
process death. Reopening that incomplete input fails closed without changing
the marker, database or sidecars. Preserve that evidence for operator
archive/reset; do not remove the marker to bypass an active initializer.

The central `recovery_anchor` holds a random journal identity, monotonic audit
revision, audit-required marker and inbox cursor epoch. `journald --recovery-audit
PATH` selects the external audit; the default is a sibling recovery database.
Its parent must already be private. Audit/lock files are private regular files;
symlinks, hard-linked audits and unsafe parents are rejected. Never unlink the
persistent owner lock to bypass a live owner.

Initial protected startup creates an audit only for fresh, unanchored current
state. It writes a complete private `<audit>.initializing` sibling, syncs,
publishes and syncs its directory before central adoption. Reserve that name
and its rollback-journal sidecar. Under the owner lock, an interrupted staging
file can be rebuilt only when the final audit is absent and the central anchor
is unrequired at revision zero. A durably published matching revision-zero
lineage can be adopted after a crash before the central marker commits.
Required, malformed, foreign, mismatched or closed audits are never replaced.

The external audit uses rollback journaling and `synchronous=EXTRA`. A sibling
is a separate recovery unit, not a separate disk fault domain. Protect and
replicate it independently; never overwrite it with a central backup.
Missing required audit, older audit head, foreign journal, unresolved intent or
closed gate refuses service admission.

## Mutation and retained security state

Every mutating service transaction durably prepares its proposed security
snapshot and next revision before committing centrally. The central revision
commits with the mutation; external completion follows. A crash between these
commits is uncertain, not rollback. Later operations fail closed.

Read/status/verification checks serialize with in-flight audit writers; they
wait for active completion rather than misclassifying a live prepared intent.
Abandoned prepared input remains archive/reset-required. Restoring or repeatedly
approving it cannot manufacture certainty.

Snapshots retain current principal/profile/name, public-space and membership
metadata, credential/digest/registration bindings, security history, space heads
and recipient allocation high-water marks. They never contain plaintext bearer
secrets. They do not snapshot record relations or all inbox receipts.

The audit keeps every revision's number, outcome and timestamp, but only the
head revision keeps its snapshot body. Resolving a revision (committed or
reconciled) clears earlier bodies in the same audit transaction; an unresolved
intent keeps its predecessor's body as evidence. Admission rejects any other
body placement. The audit format is recorded in SQLite `user_version`; audits
written by earlier formats require operator archive/reset.

## Abrupt stops

A daemon killed without graceful shutdown (SIGKILL, OOM, power loss, forced
drain timeout) leaves hot SQLite state: `-wal`, `-shm` or `-journal` sidecars.
Unprotected cold admission still refuses any sidecar. Protected startup instead
proves the hot state before replaying it:

1. Read-only, with sidecars ignored, the main file must have the exact current
   schema and the audit must be open with a resolved head. Legacy input and
   unresolved intents are refused before any lock or copy exists.
2. It takes the audit owner lock, so a live daemon's state is never touched.
3. It copies the database and its `-wal`/`-journal` into a private
   `<database>.crash-recovery` directory and lets SQLite replay only the copy.
4. The replayed copy must pass exact-schema admission, the audit lineage check
   (open audit, contiguous resolved revisions, matching journal ID, revision
   and security snapshot) and the full integrity, foreign-key, FTS, sequence,
   attention and allocation probes. Its audit revision and verification are
   written, synced, to `verified.json` in the recovery directory.
5. Only then is the original replayed in place and normalized. It must pass
   the identical probes, and its table hashes and space heads must equal the
   verified copy's.

Success records one `crash-recovered` recovery event with the audit revision
and verification, `journald` logs `crash_state_recovered`, and the recovery
directory is removed last. While `verified.json` exists, every protected start
resumes this procedure before normal admission, even when no sidecar remains,
so a crash at any later step repeats verification and records the event once.

Before `verified.json` exists, symlinked or hard-linked sidecars, an
unresolved prepared intent, an older replayed state than the audit head (for
example a lost WAL) or any failed probe refuse startup with the central files
untouched; follow the archive/reset or protected-restore procedures. After it
exists the original may already be replayed, so a divergence keeps refusing
every start; preserve the recovery directory and archive/reset. The copy needs
free space equal to the database and WAL. A recovery directory without
`verified.json` is replaced only if it is empty or carries this procedure's
provenance tag, and holds nothing but the copy's files; any other directory at
that path refuses startup untouched. The marker is published by synced rename,
so it is either absent or complete, and each recovery gets its own
`recovery_id` so separate incidents are recorded separately.

Restoration reconciles audited principal and profile state and revokes all
restored credentials. Audited post-backup credentials and registration receipts
are retained as revoked, including rotation/recovery descendants. Existing
identity/digest/receipt bindings must match; conflicting bindings fail closed
before destination publication. Rotation references may be restored in either
row order using deferred foreign keys, verified before preparing recovery output.
Immutable receipt guards are not disabled and history is not replaced/deleted.

This retention matters: an old token must not become a new registration because
its digest was absent from the backup. A clean reset that discards the audit is
different; unknown token bytes cannot be globally blacklisted without evidence.

## Inbox consequences

Ordinary crashes/retries retain committed first acknowledgment timestamps.
An intentional older-backup restore retains that backup's receipt state, so
later acknowledgments can be lost and downstream handoffs can repeat.
`accepted_record_loss` and complete client reconciliation explicitly approve
this consequence, not an exactly-once guarantee.

Restore keeps the greater of backup and audited recipient allocation heads.
Lost records may leave gaps, but recipient sequences are not reused. Restore
changes the inbox cursor epoch; clients restart without a saved continuation.
There is no receipt-delta audit or automatic reconstruction of acknowledgments.

## Backup

The storage online-backup primitive supports concurrent writers. The protected
operator command records durable evidence after probes and requires the daemon
stopped because it takes the same lifetime lock.

```sh
journal-recover backup "$DATABASE" "$AUDIT" "$BACKUP"
```

The destination must not exist and its parent must be private. Verification
includes ordinary-table counts/SHA-256 hashes, public-space/membership state,
space heads, SQLite/foreign-key/FTS integrity, exact FTS content, attention/inbox
agreement and inbox allocation heads. No attempt/custody inventory exists.
A durable timestamp follows successful probes; raw `Database::backup_to` alone
does not create external backup evidence.

## Restore, approve and reopen

1. Close ingress, stop the daemon, and stop principal clients and optional
   workers. Preserve append inputs/checkpoints, runtime-native dedupe evidence,
   published Muse files and hook seen-sets. `--clients-quiesced` is an operator
   attestation, not an RPC or central consumer registry.
2. Restore to a new private path, never over live files:

   ```sh
   journal-recover restore "$DATABASE" "$AUDIT" "$BACKUP" "$RESTORED" "$APPROVAL" --clients-quiesced
   ```

   The gate closes before validating a resolved backup. Copy hashes must match.
   Current audited identity/security bindings are reconciled and credentials
   revoked. Inbox receipt state is retained from the backup.
3. Review the private approval template's exact hashes, revision, space heads
   and `reconciled_clients` principal-ID inventory. Account for all client
   checkpoints, missing posts, lost acks and possible duplicate handoffs. This is
   operator acknowledgment of restored state, not installation/spool inventory.
4. Set only `inventory_complete` and `accepted_record_loss` to true when the
   review is demonstrably complete. Unknown evidence means stay closed. Do not
   change hashes, heads, revision or client identities to force acceptance.
5. Re-probe and reopen the exact reconciled database:

   ```sh
   journal-recover reopen "$RESTORED" "$AUDIT" "$APPROVAL"
   ```

   Changed state, stale/incomplete approval, live credentials or failed probes
   refuse reopening. The advanced revision prevents reopening the former file.
   Restart with the restored path and surviving audit, recover credentials by
   principal UUID through protected administration, reset volatile cursors and
   reopen ingress only after deployment checks.

For a closed resolved input without a backup copy, use:

```sh
journal-recover reconcile "$DATABASE" "$AUDIT" "$APPROVAL" --clients-quiesced
```

It is not a success override. Uncertain prepared input is rejected before
recovery mutation or destination publication. A completed reconciliation has
prepared output until reopen; its matching durable reconciliation evidence and
exact approved client review permit reopening. A crash before that evidence
exists remains archive/reset-required.

No adapter fencing, generation bump, enrollment ticket, spool reconciliation or
runtime requeue remains. Never delete local handoff evidence to make an approval
appear complete. Runtime/hook duplicate consequences remain operator concerns.

## Evidence and limits

Real-file tests cover older backups, post-backup credential lineage, conflicting
bindings, lost acks, allocation nonreuse, exact approvals, absent/rolled-back
audit, FTS/attention corruption, locks and process death around prepare/commit
and reconciliation. Killed-owner tests cover verified replay of WAL-only commits,
refusal while the owner lives, unresolved intents, lost WALs, aliased sidecars,
unrecognized recovery directories, recovery killed after its marker and after
in-place replay, sticky refusal after divergence, and a SIGKILLed daemon
restarting through protected startup. Unix CLI
tests exercise the protected path. Windows unit tests do not prove Unix
permissions or deployment ingress.

Snapshot size and serialized mutation cost scale with principal, credential and
security-history state, not record volume, and still require deployment capacity
measurement. There is no audit rotation command. Loss of authoritative
audit requires protected authoritative review, not clearing the anchor or
manufacturing a replacement. Live runtime and deployment canaries are not claimed.
