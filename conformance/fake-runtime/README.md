# Fake runtime contract

The fake runtime is the first injection target for adapter conformance. It must expose a deterministic local API that records each accepted envelope, can be toggled unavailable, and can simulate a crash after acceptance but before receipt persistence.

Required assertions:

1. It receives only after central host-custody confirmation.
2. It can return a stable fake acceptance receipt.
3. It can accept duplicate envelopes, demonstrating why `record_id` and `attempt_id` are retained.
4. It receives the adapter's resolved private route together with the envelope, never the route allowlist/configuration, central credentials, or portable routing key as a destination.
5. Tests can inspect the adapter's reported state without calling a real runtime.

## Executable implementation

`journal-runtime-fake::FakeRuntime` implements the shared `Runtime` boundary.
It records the resolved route, structured envelope, and rendered text, accepts
duplicates rather than hiding them, and supports unavailable/available toggles.
Its durable mode persists acceptance before the accept-then-crash exit, so a real
child-process restart can demonstrate the gap between runtime acceptance and
adapter receipt persistence. These are acceptance receipts, never read or
completion receipts.

Run from the repository root:

```sh
python3 scripts/test_adapter_conformance.py
python3 scripts/adapter_conformance.py \
  --scenarios conformance/adapter/scenarios.yaml \
  --output target/adapter-conformance
```

`make adapter-conformance` runs both commands, and `make check` includes the gate.
The runner executes exact assertions from the existing service delivery, spool,
and HTTP orchestration tests. It rejects unknown, missing, duplicate, or changed
scenario definitions, zero-test matches, and missing persisted-state evidence.
The real HTTP crash case terminates after durable fake acceptance and restarts
against the same central database and spool, proving duplicate acceptance with
stable record and attempt identity. The S10 child-process checkpoints additionally
exercise custody, injection-start, result, and telemetry recovery.

## Evidence and privacy

The output contains `<scenario-id>.json` for all 17 cases and `manifest.json`
only after the whole suite passes. Artifacts declare `schema_version: 2` and
`redacted: true`. Evidence entries carry `kind`, `state`, `execution`, `fixture`, and
`assertion`; database entries also identify their checkpoint. Observations come
from persisted central/spool rows and runtime captures, not static pass labels.
Each fixture directory receives a safe token, shared by its central/spool snapshots,
runtime captures, and crash checkpoint. Identity tokens and custody validation are
scoped to `(execution, fixture)`, never across independent fixture directories or
executions. Fixture paths are not published.

Publish only the top-level JSON artifacts. `.private-state` contains raw fixture
state during execution and must never be uploaded or committed. Normal completion
and handled failures clean it up; external termination can leave it behind.
Each assertion has a 180-second timeout. On timeout the runner terminates its
owned process tree before cleaning private state, using a Windows Job Object or
a Unix process group. Descendants cannot keep captured output handles open.
CI uploads only `target/adapter-conformance/*.json`.

## Future runtimes

`--runtime-package` and `--runtime-test` select compatible Rust integration-test
entrypoints in place of the default `journald` orchestration target. A future
target must implement the same entrypoint and persisted-evidence contract;
central-policy and generic-spool assertions remain shared. Merely selecting a
package does not certify its runtime, and zero matching tests fail.

The default gate covers generic orchestration with the fake runtime only.
Hermes and Muse remain explicitly unresolved status-2 stubs. Revalidate their
supported injection surfaces before implementing or claiming conformance.
