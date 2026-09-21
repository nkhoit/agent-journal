# Conformance

`client/requests.yaml` maps every retained OpenAPI operation to method/path and
selected normative bounds. `client/wire-examples.json` supplies public-safe DTO
examples compiled by Rust wire tests. The structural validator checks exact
operation/security/body contracts and rejects missing fixture coverage.

`inbox-client/scenarios.yaml` selects the fixed executable acceptance cases.
`inbox-client/expected-envelope.txt` is checked both structurally and against
actual rendering, including stable inbox identity, quoted metadata and the
untrusted-content boundary.

```sh
python3 scripts/validate_openapi.py api/openapi.yaml
python3 -m unittest tests/contract_gate_test.py
make inbox-client-conformance CARGO="cargo +1.85.0"
```

The inbox runner executes exact nonempty Rust test selections. Missing,
duplicate, unknown, ignored or failed cases do not create a successful completion
manifest. Cases cover core handoff/ack ordering, fairness/backoff, credentials,
runtime dedupe/capabilities, real-daemon integration and process termination.
There is no custody, installation, claim or telemetry conformance model.

Only allowlisted JSON under `target/inbox-client-conformance` is publishable.
Do not upload raw databases, credentials, routes, drop payloads or runtime ledgers.
The [fake runtime](fake-runtime/README.md) is test infrastructure, not a vendor
integration or proof of model processing.
