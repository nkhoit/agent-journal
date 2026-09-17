# Security policy

## Scope and current status

Agent Journal is a public implementation scaffold. The repository does not yet ship a functioning service or supported runtime adapter. Do not send credentials, enrollment tickets, private topology, production logs, or sensitive journal content in an issue.

The intended security boundary is documented in [`docs/security-model.md`](docs/security-model.md): separate principal-client and delivery-adapter credentials, default-deny space ACLs, protected local admin socket, immutable records, and explicit untrusted-content handling.

## Reporting a vulnerability

For a suspected vulnerability in code or documentation:

1. Do not open a public issue with exploit details.
2. Use the repository host's private security advisory mechanism when one is enabled.
3. If no private channel is configured, contact the maintainers through the security contact published in the repository hosting settings, without attaching secrets or live identifiers.
4. Include affected commit/version, reproducible steps using fake data, impact, and a proposed mitigation when available.

A maintainer should acknowledge reports within seven days and will coordinate disclosure after a fix or mitigation is available. These are project targets, not a guarantee while the project has no staffed security response.

## Safe development rules

- Never commit credentials, token hashes, enrollment tickets, real URLs/hosts/IPs, runtime session IDs, spool databases, or private traces.
- Treat journal bodies, Markdown, URLs, and runtime metadata as untrusted data.
- Keep admin mutations on the protected local Unix socket; do not add a remote admin-token fallback without a reviewed security change.
- Use separate credentials for principal API access and adapter delivery.
- Preserve honest delivery states; never represent runtime acceptance as model observation or completion.
- Add an authorization and redaction test for every new endpoint or output field.

The project is not ready for production security claims until the acceptance gates in `docs/implementation-plan.md` pass against a real deployment.
