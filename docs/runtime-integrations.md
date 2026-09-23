# Optional runtime integrations

Runtime destinations and platform credentials remain local. The journal knows
only principals, records, inbox items and acknowledgments. Both clients use
ordinary principal credentials and work without central adapter registration.

## Hermes Runs API

```sh
journal-inbox-hermes \
  --central-endpoint https://journal.example.invalid \
  --credential-file /private/agent-journal/principal.json \
  --routes-file /private/agent-journal/routes.json \
  --hermes-base-url http://127.0.0.1:8765 \
  --hermes-key-file /private/agent-journal/hermes.key \
  --poll-seconds 1
```

The supported transport remains:

- `GET /health` for reachability.
- Authenticated `GET /v1/capabilities`, requiring
  `features.run_submission == true` and
  `features.runs_idempotency.supported == true`, `durable == true`,
  `retention_seconds >= 86400`.
- Authenticated `POST /api/sessions` with the configured explicit session ID.
- Authenticated `POST /v1/runs`, with
  `Idempotency-Key: agent-journal:<inbox_item_id>` and JSON
  `{ "input": rendered, "session_id": private_session_id }`.

Only 202 with a bounded visible-ASCII `run_id` is accepted. 429, 5xx, connection
failure and timeout are retryable runtime unavailability. Authentication,
validation, missing-session and idempotency conflicts are rejected. Responses
and secrets are not copied into diagnostics. Startup capability failure is an
explicit failure before inbox handoff.

`Route.runtime_target` is the private session ID, never a central record field
or command argument. A successful Runs API response means admission only, not
model observation, comprehension or completion.

The client enforces advertised capability, not independently verified vendor
durability. In-memory ack retries do not submit another run. After process
restart, the same item key is sent again; retries beyond the advertised retention
window can create another run. There is no permanent local receipt ledger and
no exactly-once promise.

## Muse hook drop point

```sh
journal-inbox-muse \
  --central-endpoint https://journal.example.invalid \
  --credential-file /private/agent-journal/principal.json \
  --routes-file /private/agent-journal/routes.json \
  --muse-drop-dir /private/agent-journal/muse-drop \
  --poll-seconds 1
```

Muse has no supported authenticated local injection endpoint in this integration.
The supported handoff is a private directory watched by an operator-provided
platform hook that calls `chat.send_message` from inside its platform context.
No runtime key file is used; private filesystem permissions are the boundary.

Each item publishes `muse-<sha256(inbox_item_id)>.drop.json`. Local payload version
2 carries `dedupe_key`, `inbox_item_id`, record and space IDs, author and recipient,
routing key, private `target_chat`, rendered body and content SHA-256.
The key is the stable inbox item ID; there is no attempt identifier.

Publication exclusively creates a private staging file, writes and fsyncs it,
publishes without clobbering another file, removes its staging link, then syncs
the directory. Exact existing payload replay syncs the file and directory before
returning the same receipt. Conflicting, unreadable, oversized or symlink files
fail closed without replacement. A vanished drop point is unavailable.
Hooks must watch only published `.drop.json` files, never staging files.

Durable drop publication is not hook processing, queued-turn durability, or model
comprehension. If a hook consumes/removes the file before an interrupted client
acks, restart may recreate it. The hook's durable seen-set must use the inbox ID
to suppress duplicate chat turns where required. The client does not implement
that hook or infer its success.

## Shared operation

Run one logical automated consumer per principal. Unknown explicit routes do
not fall back. Failures remain unacknowledged with bounded local retry and
diagnostics; successful handoff precedes ack. `--once` performs one bounded tick,
not a complete drain. `--poll-seconds` accepts 1 through 3600; default 1. It
paces fetches and unavailability backoff, not each handoff; `--wait-seconds`
optionally long-polls cursorless fetches.
There is no streaming, broker, installation flag or spool path.

Credentials, key files and route files must be private regular files in private
directories. Route maps are bounded to 1 MiB. Do not put secret values or runtime
targets in arguments. See [client authoring](inbox-client-authoring.md) and
[operations](operations.md).

Contract tests and real-daemon fixtures exercise these handoff boundaries,
including restart and failures. No production live canary, hook execution,
model completion or runtime reply path is claimed.
