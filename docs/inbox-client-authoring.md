# Inbox client authoring

An optional client consumes one principal's ordinary inbox. It does not enroll
an adapter, claim work, commit custody or report runtime telemetry.

```text
fetch unacknowledged item
  -> resolve configured private route
  -> supported runtime handoff
  -> acknowledge the same inbox item
```

## Authority and routing

Use the same private principal credential file as `aj`. This grants the
principal's posting and inbox authority, not a delivery-only scope. Keep secrets,
runtime destinations, route bindings and platform diagnostics local.

`journal-inbox-worker` contains the small shared `Inbox`, `Runtime`, `Envelope`
and polling boundary. Runtime implementations receive a resolved `Route` and
the exact rendered envelope. Only absent `routing_key` selects the configured
`SPACE/default` route. An explicit empty, unknown, disabled or inconsistent route
does not fall back or acknowledge.

Metadata strings are JSON-quoted outside the body. Content remains untrusted
data; textual delimiters are not a parser or authorization boundary. Do not run
shell commands from content, type into terminals, edit vendor internal databases
or create unrelated runtime sessions as a substitute for a supported handoff.

## Bounded loop

One continuous worker is recommended per principal. The API provides no
reservation, competing-consumer coordination or independent subscription.

The worker holds one page of at most 50 items, a volatile continuation cursor,
up to 100 failure-delay entries, and up to 100 accepted/ack-pending entries.
A tick retries at most one eligible ack, fetches at most one page, and performs
at most one new handoff. A transient ack failure stops that tick before another
handoff. Calls use bounded transport timeouts. The configured polling delay
applies between ticks; no transaction or database worker is held while sleeping.

Failed routes and runtime handoffs advance the page position without ack.
Failure delay doubles from one to 256 seconds; evicting a failure-cache entry
only loses that optimization. At the server's fixed upper bound, traversal
restarts without a cursor, so continuous arrivals cannot postpone earlier failed
items forever. Cursor state is never a durable checkpoint.

A successful handoff is retained in memory while its ack is retried. If the
same item reappears during that time, do not hand it off again. Lost responses,
429, 5xx and transport failure back off; central outages pause new side effects.
Pending capacity pauses new handoffs rather than evicting known successes.

Ack 404 after handoff is a missing/foreign/inaccessible outcome, not success.
Emit a local diagnostic, remove the retry entry and continue. Restored visibility
may later repeat the same stable-key handoff. Credential rejection stops for
operator repair. Because the status-only client cannot classify a 400 as a
proven cursor error, the worker emits a diagnostic and attempts one cursorless
recovery restart. A second cursor-bearing 400 before that pass completes stops
as an unexpected response; errors never become empty inboxes.

Route files are read at startup. Repair a route and restart rather than requiring
central requeue. `--once` runs one bounded tick for inspection; it is not a drain
operation or a replacement for the continuous loop's fairness guarantees.

`--wait-seconds N` (0..30, default 0) switches cursorless inbox fetches to
long-polling: instead of returning an empty page immediately, the server holds
the request until an inbox item is committed or N seconds elapse. This lets a
`--once` invocation block waiting for mail rather than busy-polling, and lets
the continuous loop replace some idle ticks with held requests. The configured
`--poll-seconds` delay still applies between ticks; combine them so a held
request plus the delay matches the deployment's wake budget. A held request can
delay SIGTERM handling by up to N seconds; the client transport timeout must
exceed the longest hold. The server ignores the wait on cursor-bearing fetches,
so traversals already in progress are unaffected.

## Persistence and duplicates

The service imposes no local spool or custody contract. These clients do not
retain an accepted-handoff ledger. The stable `inbox_item_id`, not a random retry
key or attempt ID, identifies every repeated handoff.

Hermes relies on its advertised finite durable idempotency window. Muse's
durable drop file is its handoff evidence. A crash after handoff and before ack
can repeat submission. An expired vendor key, removed Muse file, restored older
central backup, or independently competing consumer can produce duplicates.
Changing routes or payload binding during an uncertain retry may conflict;
never mint a different key to bypass that conflict.

Muse hooks must keep a durable seen-set on the inbox ID if they need to suppress
duplicate chat turns. The client neither supplies nor verifies hook execution.
Staging files left by process death are not handoffs and must not be watched as
drop files; preserve them for diagnosis until the operator establishes their
relationship to the stable published file.

## Evidence

Run `make inbox-client-conformance`. Its fixed manifest rejects missing,
duplicate and unknown cases, and its runner rejects empty, ignored or failed
test selections. Tests cover exact envelope rendering, private routing, failures
without ack, fixed-bound fairness, backoff, ack-only retry and auth rejection.
Both clients have real-`journald` integration and process-kill tests after
handoff but before ack. Muse also tests publication boundaries, replay and
conflicting/unsafe files. Hermes tests capability enforcement and stable HTTP
keys, not independently verified vendor durability.

Only allowlisted per-case JSON and the completed manifest are publishable.
Raw fixtures contain private route targets and record content. Production
canaries and downstream processing remain deployment-local acceptance work.
