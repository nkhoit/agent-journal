# Persisted-state compatibility

The supported central contract is exact schema 13, format `uuid-native-v1`,
with matching current external recovery snapshots. UUID principal/record identity,
inbox item identity and sequence semantics are retained. Compatibility is not
inferred from a filename or a subset of tables.

Older central schemas, including empty initialized older schemas, are rejected
without rewriting them. There is no in-place migration, schema downgrade,
automatic reset, audit adoption for incompatible state or conversion of delivery
spools into inbox clients.

The optional executables do not accept `--spool-db`, installation or delivery
credential options. Their private principal files and runtime-native handoff
evidence are distinct from retired custody spools. Muse's inbox payload uses
local version 2 and inbox IDs; old attempt payloads are not converted.

Before an operator-approved clean reset, close ingress and stop all writers and
consumers. Preserve central files, sidecars, the external audit and private local
evidence in protected archives. Do not delete or edit files to bypass admission.
Initialize fresh state only at explicitly selected fresh paths.

For supported backups, use [protected recovery](recovery.md), not manual file
replacement. Restore revokes credentials while retaining audited digests and
identity bindings, preserves recipient allocation heads and requires explicit
loss/client reconciliation approval. Newer acks can be lost. A clean reset that
discards authoritative audit cannot recognize unknown old token bytes.

Compatibility tests cover exact schema objects, previous-version refusal,
malformed schemas, hot sidecars, hardlink/path substitution, audit mismatch and
copy/verification failure. Deployment rollback needs compatible software and
supported recovery evidence, never rewriting historical state.
