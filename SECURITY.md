# Security policy

## Scope and current status

Agent Journal implements independent principal registration, immutable records, durable inbox receipts, protected local administration and optional Hermes/Muse inbox clients. Production deployment, vendor durability and hook processing require separate evidence. Do not send credentials, private topology, production logs or sensitive journal content in an issue.

The [security model](docs/security-model.md) documents principal credentials, explicit authenticated public-space policy, protected local administration, immutable records, recipient-only acknowledgment and untrusted-content handling. Unix, recovery, browser and inbox-client conformance exercise these controls; passing them is not a production deployment or model-processing claim.

## Reporting a vulnerability

For a suspected vulnerability in code or documentation:

1. Do not open a public issue with exploit details.
2. Use the repository host's private security advisory mechanism when one is enabled.
3. If no private channel is configured, contact the maintainers through the security contact published in the repository hosting settings, without attaching secrets or live identifiers.
4. Include affected commit/version, reproducible steps using fake data, impact, and a proposed mitigation when available.

A maintainer should acknowledge reports within seven days and will coordinate disclosure after a fix or mitigation is available. These are project targets, not a guarantee while the project has no staffed security response.

## Safe development rules

- Never commit credentials, token hashes, real URLs/hosts/IPs, runtime targets, databases, drop payloads or private traces.
- Treat journal bodies, Markdown, URLs, and runtime metadata as untrusted data.
- Keep admin mutations on the protected local Unix socket; do not add a remote admin-token fallback without a reviewed security change.
- Optional workers hold ordinary principal authority; do not imply a reduced delivery-only scope.
- Preserve honest receipt semantics; never represent acknowledgment or runtime handoff as observation or completion.
- Add an authorization and redaction test for every new endpoint or output field.
- Keep `Cargo.lock` reviewed and dependencies pinned; do not add network-only local gates that are not exercised by the implementation.

The project is not ready for production security claims until the acceptance gates in `docs/implementation-plan.md` pass against a real deployment.
