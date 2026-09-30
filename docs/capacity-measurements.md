# Synthetic capacity measurements

Measurement work for [issue 44](https://github.com/nkhoit/agent-journal/issues/44)
and [issue 45](https://github.com/nkhoit/agent-journal/issues/45), following Kuro's
measurement-first reviews. Both issues remain open. This adds a disposable
fixture and repeatable load driver; it does not implement daemon backup routes,
quotas, SQL cancellation, production telemetry or change defaults.

## Repeat the measurements

Use Unix, Rust 1.85.0 and Python 3 (standard library only). From the repository:

```sh
CARGO_BUILD_JOBS=2 cargo +1.85.0 build --locked --release -p journald --example capacity_fixture
python3 scripts/measure_capacity.py > target/capacity-measurements.json
```

For a quick validation or another modest corpus:

```sh
python3 scripts/measure_capacity.py --sizes 100
python3 scripts/measure_capacity.py --sizes 1000 5000 10000
```

The driver accepts at most three sizes, each 100..20000. The fixture accepts only
a record count: no production database, audit, credential or endpoint input.
It exclusively creates a private temporary root and separate central/audit/copy
subdirectories, holds the existing audit owner, and removes its own fixture on
normal completion/error unwinding. All principals, credentials and content are
synthetic. The public router binds only an ephemeral IPv4 loopback port; admin
operations are not mounted. The fixture's intermediate endpoint stays inside
the driver. Published JSON contains only aggregate measurements/configuration,
not credentials, record bodies, IDs, table hashes or database/audit files.

Work is bounded by operation counts and corpus size, not a hard wall deadline.
The driver requests graceful drain on error and never kills an audited write.
A 120-second cleanup wait failure reports an error without forcibly terminating
the fixture. Avoid interrupting it mid-mutation. A killed process can leave its
private disposable directory; it is never automatically adopted or opened.
Allow about 0.5 GiB of temporary fixture headroom at the maximum count, plus
Rust build space. Run one measurement driver at a time without concurrent builds.

## Workload and measurement boundaries

Three registered principals share one public space. Seed appends go through the
real protected service with ordinary transactions, FTS, idempotency and audit
prepare/commit/completion. Every tenth record addresses the recipient. Bodies
are roughly 0.5 KiB and contain a repeated common term matching every record.
The corpus has no relations, large bodies or growing principal/history counts;
those require separate measurements. Three backup probes add one addressed
append each. The later HTTP phases add only confirmed successful appends.

Each size runs the following fixed work:

- One warmup and seven first-page searches per order, rank and sequence, limit 50.
  These measure complete synchronous service calls, including protected read
  admission, authentication, SQL ranking, snippets and page construction.
- Three separate raw `Database::backup_to` calls, each including copy and basic
  verification, followed by separately timed full recovery verification. These
  are component observations, not a pure copy timer or phases of the later call.
- Three complete `RecoveryAudit::backup` calls through the existing lifetime
  owner. Each includes admission, protected copying/basic probes, synchronization,
  full verification, normalization and durable evidence. The source backup
  primitive uses 128-page steps with 5 ms pauses, without changing that policy.
- During each protected backup, destination reservation is observed while the
  backup thread is running. Reservation occurs under the audit mutex. A new
  protected read and an actual audited append then compete with the backup; an
  already-admitted read connection performs a count concurrently. Probe timings
  include their own work, thread scheduling and audit admission, so they are
  request latency observations rather than an isolated mutex-hold timer. Backup
  totals include thread startup. The reservation/running flag records whether
  overlap was observed; there is no assertion about a minimum measured pause.
- Every protected backup's returned verification must equal full re-verification
  and the separate pre-mutation copy, and durable backup time must be present.
  This checks coherent copy evidence before the competing append commits.
- An isolated actual `BlockingExecutor` test uses eight permits, 12 read workers
  and 20 attempts each, with 1 ms spacing after each response. It counts closure
  entry/exit, peak concurrency, capacity rejection and summed active time. The
  mean active count includes mutex waits inside closures; it excludes scheduling
  before closure entry and the short tail before permit release. It is not an
  exact semaphore gauge and is not instrumentation of the HTTP phase.
- A one-permit executor probe aborts only a started read's async waiter. It
  reports whether capacity remains occupied afterward, whether the read finishes
  after abort, and whether the executor subsequently drains. There is no injected
  slow query or read/write interruption mechanism.
- Actual loopback HTTP first runs 30 append/inbox/ack control rounds. Loaded HTTP
  uses 12 ranked-search clients with 24 attempts each (1 ms response spacing),
  24 real TCP read disconnects (2 ms spacing), and another principal's 30
  append/inbox/ack rounds (2 ms round spacing). Requests use fresh connections
  and 35-second transport deadlines. Overload 503s are counted, not retried or
  treated as empty pages. Search pages must contain 50 results. An empty pending
  inbox can legitimately produce no ack attempt at the smallest fixture size.
  Progress before the last search worker finishes is reported separately from
  whole-phase success. FIN/send completion does not prove a disconnected request
  reached SQL; the separate started-executor probe supplies continuation evidence.
- After graceful HTTP drain, full integrity/FTS/attention/allocation/table probes
  must pass and the verified record count must equal seed + three backup appends
  plus all HTTP appends with confirmed 201 responses. No audited write is cancelled.

JSON retains seven individual search samples and three individual backup samples.
Percentiles use the nearest-rank order statistic; with seven samples, p95 is the
maximum, not a dependable deployment-tail estimate. HTTP latency summaries include
successful requests only, with rejection counts reported separately.

## Observed baseline and repeat

Recorded 2026-09-30 against production code at `789267c8611dd00f5055606bdf4897684efc827d`.
The pending bug PRs 46/47 are not included in this baseline. No live deployment,
vendor runtime or production state was accessed. Evidence files:

- [Initial baseline JSON](measurements/capacity-2026-09-30-baseline.json)
- [Repeated JSON](measurements/capacity-2026-09-30-repeat.json)

Both runs used Rust 1.85.0 release optimization, bundled SQLite 3.53.2, Linux
x86_64, AMD EPYC 9V74, five visible CPUs with a four-CPU cgroup quota, and a
16 GiB memory cap. Measurements used disposable temporary storage with warm
OS caches; no physical-disk performance or independent audit fault domain is
established. Central SQLite used WAL, 4096-byte pages, `synchronous=2`,
`cache_size=-2000` and a 5000 ms busy timeout. The protected audit keeps its
existing rollback/EXTRA policy. The router used eight permits, two Tokio workers
and the existing 1 MiB body bound. No defaults changed. Build jobs were capped
at two, and builds did not run during measurements. The repeat adds an overlap
breakdown to the driver; source hashes are included there. A subsequent
non-Unix refusal guard does not alter this measured Unix execution path.

The repeated run's medians in milliseconds:

| Seed records | DB + WAL MiB | Rank | Sequence | Copy + basic probes | Separate full probes | Protected total | New read | Audited append | Pre-admitted count |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2.38 | 13.69 | 4.68 | 28.07 | 13.81 | 44.22 | 44.64 | 46.50 | 0.04 |
| 5,000 | 9.80 | 57.39 | 10.95 | 123.06 | 76.09 | 215.06 | 216.22 | 219.31 | 0.12 |
| 10,000 | 19.06 | 110.13 | 16.37 | 247.01 | 156.63 | 403.88 | 404.67 | 408.28 | 0.24 |

The initial run measured rank/sequence medians of 14.05/4.78, 58.25/10.61 and
109.09/17.68 ms; protected-backup medians were 54.30, 195.07 and 390.61 ms.
The trend repeats, while individual timings and scheduling vary. All nine
protected backups per run observed concurrent reservation and passed coherent
verification/evidence checks. New-read and competing-append latency tracked the
protected backup duration, consistent with their shared audit gate. The
pre-admitted count reads showed no comparable pause.

Under isolated executor search load, peak active closures reached eight at every
size. Mean active closure counts were 6.76, 7.64 and 7.66 in the repeat (7.47,
7.70 and 7.61 initially). Every started-read waiter-abort probe finished after
abort, rejected the immediately following admission at capacity, then drained.
Read closure durations after abort were 13.38, 57.01 and 117.80 ms in the repeat.
This demonstrates continued work for these started reads, not the admission or
lifetime of every real TCP-disconnected request.

The repeated HTTP burst results:

| Seed records | Search 200 / 503 (288 attempts) | Other append 201 / 503 (30 attempts) | Inbox 200 / 503 (30 attempts) | Ack 204 | Control ack 204 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 92 / 196 | 10 / 20 | 9 / 21 | 9 | 30 |
| 5,000 | 166 / 122 | 0 / 30 | 0 / 30 | 0 | 30 |
| 10,000 | 192 / 96 | 0 / 30 | 0 / 30 | 0 | 30 |

All listed loaded progress responses occurred before the last search worker
finished. At 1,000 records, successful append/inbox/ack medians were
30.69/23.76/27.53 ms under load, compared with 3.71/3.72/3.13 ms in the control.
The initial 1,000-record run completed 27 appends and 28 acks, demonstrating
scheduling sensitivity. Initial 5,000/10,000-record runs also completed zero
loaded append/inbox attempts. Their unloaded controls completed all 30 rounds;
repeat control append/ack medians were 4.37/3.84 and 5.00/4.45 ms.

This is an intentional early burst overload: 30 short-spaced progress attempts
can all exhaust their finite attempt budget while longer searches remain active.
Zero success in that window is not evidence of indefinite starvation. It does
show the current shared admission pool gives no reservation to useful recipient
work in this workload. The harness does not define or validate a client backoff
policy, sustained arrival rate, deployment fairness bound or production quota.

## Implications and deferred work

For issue 44, direct reuse of protected backup creates an observable serialized
pause for new reads and writes. Copy pacing and full probes both contribute.
Keeping the daemon running would not by itself remove that pause. The fixture
does not expose an online admin operation, measure real operator backup cadence,
exercise disk-full/kill/lost-response scenarios, or validate restore/reopen for a
new operation. Those feature acceptance tasks remain open.

For issue 45, common-term ranking costs more than sequence order in these modest
corpora, and concurrent searches can fill shared execution capacity and reject
other principals. Dropping a started read's waiter did not cancel its closure.
The results justify a separate narrowly scoped admission/cancellation experiment;
they do not select safe production thresholds. First repeat with deployment-like
storage, principal/security-history sizes, body sizes, query distributions and
legitimate burst/backoff patterns. Measure long-poll idle waiting and shared
viewer load separately; neither is represented by this bearer-search workload.
Preserve audited writes once admitted. No quota, cancellation, admission partition
or production telemetry implementation is included here.

## Validation

A 100-record smoke run passed full final verification and fixture cleanup;
invalid counts are rejected before fixture creation. The existing focused
storage/service/daemon library baseline passed before the load runs.
Validation on Rust 1.85.0:

- `cargo +1.85.0 test --locked -p journal-storage-sqlite -p journal-service -p journald --lib`:
  46 passed before measurements.
- `make check CARGO="cargo +1.85.0" OPENAPI_STANDARDS_LINT=1`: passed, including
  formatting, 310 workspace tests (none ignored), Clippy with warnings denied,
  build, migration/retirement, 37 contract tests, bootstrap/records/inbox/recovery,
  17 executable conformance cases, structural OpenAPI, pinned Redocly, Markdown
  and public hygiene. Build jobs were capped at two. Pinned Python gate
  dependencies were installed outside the repository for the completed run.
- Release fixture build, final 100-record driver smoke, invalid-count rejection
  in both entrypoints, aggregate-only evidence checks, zero remaining fixture
  roots and `git diff --check`: passed.
- The first `make check` reached Redocly but failed because npm's default cache
  was unwritable. A writable external cache resolved it and the full rerun passed.
  Markdown lint initially interpreted a wrapped `+` as a list marker; that line
  was corrected and the final full run includes a passing Markdown gate.
- `make browser-security CARGO="cargo +1.85.0"`: fixture build passed; the browser
  gate remains blocked because Playwright's pinned Chromium download received
  HTTP 403. It was not counted as passing. A supplemental run of the unchanged
  browser test using Playwright 1.61.0 and installed Chromium 151.0.7922.173 passed
  rendering/CSP/navigation/policy/receipt/listener checks. This used an external
  launch override, not a repository or gate change. Pinned Chromium acceptance
  still needs CI or another environment with download access.
