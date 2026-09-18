use super::*;
use journal_adapter_core::*;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    time::Duration,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::var_os("AJ_CONFORMANCE_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap().join("target"))
            .join(format!(
                "orchestration-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> SqliteStore {
        SqliteStore::open(self.0.join("spool.db"), Limits::default()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            return;
        }
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct FakeClock(Cell<SystemTime>);
impl FakeClock {
    fn new() -> Self {
        Self(Cell::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        ))
    }
    fn advance(&self, seconds: u64) {
        self.0.set(self.now() + Duration::from_secs(seconds));
    }
}
impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        self.0.get()
    }
}

fn registration() -> Registration {
    Registration {
        adapter_id: "adapter".into(),
        principal_id: "destination".into(),
        instance_id: "installation".into(),
        generation: 1,
        status: RegistrationStatus::Active,
        lease_expires_at: "2030-01-01T00:00:00Z".into(),
        heartbeat_after_seconds: 20,
    }
}

fn claim(id: &str, key: Option<&str>) -> ClaimBatch {
    serde_json::from_value(serde_json::json!({
        "claim_id": format!("claim-{id}"), "state": "active",
        "lease_expires_at": "2030-01-01T00:00:00Z", "items": [{
            "mailbox_item_id": format!("item-{id}"), "attempt_id": format!("attempt-{id}"),
            "record": { "id": format!("record-{id}"), "space_id": "space", "seq": 1,
                "author": "source", "kind": "note", "content": "untrusted\nbody",
                "created_at": "2023-11-14T22:13:20Z", "attention": ["destination"],
                "routing_key": key, "relations": [] }
        }]
    }))
    .unwrap()
}

struct FakeJournal<'a> {
    spool: &'a SqliteStore,
    claims: RefCell<VecDeque<ClaimBatch>>,
    commits: RefCell<Vec<CustodyRequest>>,
    events: RefCell<Vec<EventRequest>>,
    heartbeats: Cell<usize>,
    fence_at: Cell<usize>,
    lose_custody: Cell<bool>,
    lose_event: Cell<bool>,
    expired: Cell<bool>,
    lease: Cell<Option<SystemTime>>,
    now: Cell<SystemTime>,
    offline: Cell<bool>,
    registrations: Cell<usize>,
    renewal_generation: Cell<i64>,
}
impl<'a> FakeJournal<'a> {
    fn new(spool: &'a SqliteStore, claims: Vec<ClaimBatch>) -> Self {
        Self {
            spool,
            claims: RefCell::new(claims.into()),
            commits: RefCell::new(vec![]),
            events: RefCell::new(vec![]),
            heartbeats: Cell::new(0),
            fence_at: Cell::new(usize::MAX),
            lose_custody: Cell::new(false),
            lose_event: Cell::new(false),
            expired: Cell::new(false),
            lease: Cell::new(None),
            now: Cell::new(SystemTime::UNIX_EPOCH),
            offline: Cell::new(false),
            registrations: Cell::new(0),
            renewal_generation: Cell::new(1),
        }
    }
}
impl Journal for FakeJournal<'_> {
    fn register(&self, _: RegisterRequest) -> CoreResult<Registration> {
        self.registrations.set(self.registrations.get() + 1);
        self.lease
            .set(Some(self.now.get() + Duration::from_secs(60)));
        let mut result = registration();
        result.generation = self.renewal_generation.get();
        Ok(result)
    }
    fn heartbeat(&self, _: HeartbeatRequest) -> CoreResult<Registration> {
        self.heartbeats.set(self.heartbeats.get() + 1);
        if self.offline.get() {
            return Err(CoreError::JournalUnavailable);
        }
        if self
            .lease
            .get()
            .is_some_and(|lease| lease <= self.now.get())
        {
            return Err(CoreError::JournalRejected(409));
        }
        if self.heartbeats.get() >= self.fence_at.get() {
            return Err(CoreError::Fenced);
        }

        Ok(registration())
    }
    fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch> {
        assert_eq!(request.limit, 1);
        let result = self.claims.borrow_mut().pop_front().unwrap_or(ClaimBatch {
            claim_id: "empty".into(),
            state: ClaimState::Committed,
            lease_expires_at: "2030-01-01T00:00:00Z".into(),
            items: vec![],
        });
        pause("claimed");
        Ok(result)
    }
    fn commit_host_custody(&self, request: CustodyRequest) -> CoreResult<CustodyResult> {
        self.commit(request)
    }
    fn record_event(&self, attempt: &str, event: EventRequest) -> CoreResult<()> {
        self.event(attempt, event)
    }
}

#[test]
fn expired_registration_renews_custody_and_durable_outbox_without_reinjection() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let store = f.open();
    let journal = FakeJournal::new(&store, vec![claim("a", None)]);
    journal.now.set(clock.now());
    journal.lose_custody.set(true);
    let runtime = FakeRuntime::new(&store);
    let routes = routes();
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    journal.offline.set(true);
    for _ in 0..9 {
        clock.advance(256);
        journal.now.set(clock.now());
        assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    }
    journal.offline.set(false);
    journal.lose_event.set(true);
    clock.advance(256);
    journal.now.set(clock.now());
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    assert_eq!(journal.registrations.get(), 2);
    assert!(store.get("attempt-a").unwrap().pending_event.is_some());
    clock.advance(256);
    journal.now.set(clock.now());
    adapter.tick().unwrap();
    assert_eq!(journal.registrations.get(), 3);
    assert!(store.get("attempt-a").unwrap().pending_event.is_none());
    assert_eq!(runtime.calls.borrow().len(), 1);
}

#[test]
fn changed_generation_renewal_preserves_old_authority_and_fails_closed() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let store = f.open();
    let journal = FakeJournal::new(&store, vec![claim("a", None)]);
    journal.now.set(clock.now());
    journal.lose_custody.set(true);
    let runtime = FakeRuntime::new(&store);
    let routes = routes();
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    clock.advance(256);
    journal.now.set(clock.now());
    journal.renewal_generation.set(2);
    assert_eq!(adapter.tick(), Err(CoreError::Fenced));
    assert_eq!(journal.registrations.get(), 2);
    assert!(runtime.calls.borrow().is_empty());
    assert!(!store.get("attempt-a").unwrap().custody_confirmed);
    journal.lease.set(Some(SystemTime::UNIX_EPOCH));
    assert_eq!(adapter.tick(), Err(CoreError::Fenced));
    assert_eq!(journal.registrations.get(), 3);
}
impl FakeJournal<'_> {
    fn commit(&self, request: CustodyRequest) -> CoreResult<CustodyResult> {
        assert_eq!(request.items.len(), 1);
        let item = self.spool.get(&request.items[0].attempt_id)?;
        assert_eq!(item.claim_id, request.claim_id);
        assert!(item.record.is_some());
        pause("spooled");
        self.commits.borrow_mut().push(request.clone());
        pause("central-custody");
        if self.lose_custody.replace(false) {
            return Err(CoreError::JournalUnavailable);
        }
        let expired = self.expired.get() && request.claim_id == "claim-a";
        Ok(CustodyResult {
            claim_id: request.claim_id,
            generation: request.generation,
            items: request
                .items
                .into_iter()
                .map(|item| CustodyItemResult {
                    mailbox_item_id: item.mailbox_item_id,
                    attempt_id: item.attempt_id,
                    result: if expired {
                        CustodyResultState::LeaseExpired
                    } else {
                        CustodyResultState::AlreadyCommitted
                    },
                })
                .collect(),
        })
    }
    fn event(&self, _: &str, event: EventRequest) -> CoreResult<()> {
        let item = self.spool.get(&event.attempt_id)?;
        assert!(item.custody_confirmed);
        assert_eq!(item.pending_event.as_ref(), Some(&event));
        pause("result-persisted");
        self.events.borrow_mut().push(event);
        pause("telemetry-accepted");
        if self.lose_event.replace(false) {
            return Err(CoreError::JournalUnavailable);
        }
        Ok(())
    }
}

struct FakeRuntime<'a> {
    spool: &'a SqliteStore,
    runtime: journal_runtime_fake::FakeRuntime,
    calls: RefCell<Vec<(Route, Envelope, String)>>,
    unavailable: Cell<bool>,
}
impl<'a> FakeRuntime<'a> {
    fn new(spool: &'a SqliteStore) -> Self {
        Self {
            spool,
            runtime: match std::env::var_os("ORCHESTRATION_CHILD_PATH") {
                Some(path) => journal_runtime_fake::FakeRuntime::durable(
                    PathBuf::from(path).join("acceptances.jsonl"),
                )
                .unwrap(),
                None => journal_runtime_fake::FakeRuntime::default(),
            },
            calls: RefCell::new(vec![]),
            unavailable: Cell::new(false),
        }
    }
}
impl Runtime for FakeRuntime<'_> {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> CoreResult<String> {
        let item = self.spool.get(&envelope.attempt_id)?;
        assert!(item.custody_confirmed);
        assert_eq!(item.injection_state, InjectionState::InFlight);
        pause("injection-started");
        self.calls
            .borrow_mut()
            .push((route.clone(), envelope.clone(), rendered.into()));
        self.runtime.set_available(!self.unavailable.get());
        let receipt = self.runtime.inject(route, envelope, rendered)?;
        if let Some(path) = std::env::var_os("ORCHESTRATION_CHILD_PATH") {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(PathBuf::from(path).join("sends"))
                .unwrap();
            writeln!(file, "{}", envelope.attempt_id).unwrap();
            file.sync_all().unwrap();
        }
        pause("runtime-accepted");
        Ok(receipt)
    }
}

#[derive(Default)]
struct OversizedSuccessRuntime {
    calls: Cell<usize>,
}

impl Runtime for OversizedSuccessRuntime {
    fn inject(&self, _: &Route, _: &Envelope, _: &str) -> CoreResult<String> {
        self.calls.set(self.calls.get() + 1);
        Ok("x".repeat(4097))
    }
}

#[derive(Default)]
struct AmbiguousRuntime {
    calls: Cell<usize>,
}

impl Runtime for AmbiguousRuntime {
    fn inject(&self, _: &Route, _: &Envelope, _: &str) -> CoreResult<String> {
        self.calls.set(self.calls.get() + 1);
        Err(CoreError::InvalidResponse)
    }
}

fn routes() -> StaticRoutes {
    [(
        "space/default".into(),
        Route {
            key: "default".into(),
            enabled: true,
            runtime_target: "private-local-target".into(),
        },
    )]
    .into_iter()
    .collect()
}

#[test]
fn oversized_success_does_not_reinject_after_runtime_call() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let store = f.open();
    let journal = FakeJournal::new(&store, vec![claim("a", None)]);
    let runtime = OversizedSuccessRuntime::default();
    let routes = routes();
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();

    assert_eq!(adapter.tick(), Ok(Progress::Worked));
    assert_eq!(adapter.tick(), Ok(Progress::Idle));

    assert_eq!(runtime.calls.get(), 1);
    let item = store.get("attempt-a").unwrap();
    assert_eq!(item.injection_state, InjectionState::Accepted);
    assert!(item.runtime_receipt.is_empty());
    assert!(item.pending_event.is_none());
    let events = journal.events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].state,
        OutcomeState::AdapterReportedRuntimeAccepted
    );
    assert!(events[0].detail.is_empty());
}

#[test]
fn ambiguous_runtime_error_does_not_reinject_after_runtime_call() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let store = f.open();
    let journal = FakeJournal::new(&store, vec![claim("a", None)]);
    let runtime = AmbiguousRuntime::default();
    let routes = routes();
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();

    assert_eq!(adapter.tick(), Ok(Progress::Worked));
    assert_eq!(adapter.tick(), Ok(Progress::Idle));

    assert_eq!(runtime.calls.get(), 1);
    let item = store.get("attempt-a").unwrap();
    assert_eq!(item.injection_state, InjectionState::TerminalFailure);
    assert_eq!(item.failure_detail, "runtime outcome ambiguous");
    assert!(item.pending_event.is_none());
    let events = journal.events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].state,
        OutcomeState::AdapterReportedTerminalFailure
    );
    assert!(events[0].detail.is_empty());
}

#[test]
fn custody_precedes_injection_and_terminal_event_survives_lost_response() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let store = f.open();
    let j = FakeJournal::new(&store, vec![claim("a", None)]);
    j.lose_custody.set(true);
    let runtime = FakeRuntime::new(&store);
    let routes = routes();
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    assert!(runtime.calls.borrow().is_empty());
    assert_eq!(adapter.tick().unwrap(), Progress::Backoff);
    clock.advance(1);
    j.lose_event.set(true);
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    let item = store.get("attempt-a").unwrap();
    assert_eq!(item.injection_state, InjectionState::Accepted);
    assert!(item.pending_event.is_some());
    assert!(store.recoverable(clock.now(), 1).unwrap().is_empty());
    assert!(store.compact("attempt-a").is_err());
    assert_eq!(j.commits.borrow()[0], j.commits.borrow()[1]);
    drop(adapter);
    let first_event = j.events.borrow()[0].clone();
    let (route, envelope, rendered) = runtime.calls.borrow()[0].clone();
    assert_eq!(route.runtime_target, "private-local-target");
    assert_eq!(rendered, envelope.render());
    assert_eq!(
        rendered,
        include_str!("../../../conformance/adapter/orchestrated-envelope.txt")
            .replace("\r\n", "\n")
    );
    drop(j);
    drop(runtime);
    drop(store);
    let store = f.open();
    let j = FakeJournal::new(&store, vec![]);
    let runtime = FakeRuntime::new(&store);
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    assert_eq!(adapter.tick().unwrap(), Progress::Backoff);
    clock.advance(2);
    adapter.tick().unwrap();
    assert_eq!(j.events.borrow()[0], first_event);
    assert!(runtime.calls.borrow().is_empty());
    assert!(store.get("attempt-a").unwrap().pending_event.is_none());
    store.compact("attempt-a").unwrap();
}

#[test]
fn retry_schedule_is_durable_and_recovery_is_fair() {
    let f = Fixture::new();
    let clock = FakeClock::new();
    let routes = routes();
    let store = f.open();
    let j = FakeJournal::new(&store, vec![claim("a", None), claim("b", None)]);
    let runtime = FakeRuntime::new(&store);
    runtime.unavailable.set(true);
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    adapter.tick().unwrap();
    assert_eq!(
        store.get("attempt-a").unwrap().next_runtime_try_at,
        Some(clock.now() + Duration::from_secs(1))
    );
    adapter.tick().unwrap();
    assert_eq!(runtime.calls.borrow().len(), 2);
    drop(adapter);
    drop(runtime);
    drop(j);
    drop(store);
    let store = f.open();
    let j = FakeJournal::new(&store, vec![]);
    let runtime = FakeRuntime::new(&store);
    runtime.unavailable.set(true);
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    adapter.tick().unwrap();
    assert!(runtime.calls.borrow().is_empty());
    clock.advance(1);
    adapter.tick().unwrap();
    adapter.tick().unwrap();
    assert_eq!(runtime.calls.borrow().len(), 2);
    assert_eq!(store.get("attempt-a").unwrap().runtime_failures, 2);
    assert_eq!(
        store.get("attempt-a").unwrap().next_runtime_try_at,
        Some(clock.now() + Duration::from_secs(2))
    );
    assert_eq!(retry_delay(u64::MAX), Duration::from_secs(256));
    runtime.unavailable.set(false);
    clock.advance(2);
    assert_eq!(adapter.tick().unwrap(), Progress::Idle);
    adapter.tick().unwrap();
    adapter.tick().unwrap();
    for attempt in ["attempt-a", "attempt-b"] {
        let saved = store.get(attempt).unwrap();
        assert_eq!(saved.injection_state, InjectionState::Accepted);
        assert!(saved.pending_event.is_none());
    }
    assert_eq!(runtime.runtime.acceptances().len(), 2);
}

#[test]
fn explicit_unknown_empty_disabled_and_removed_routes_never_fall_back() {
    for key in [Some("unknown"), Some(""), Some("disabled")] {
        let f = Fixture::new();
        let store = f.open();
        let clock = FakeClock::new();
        let j = FakeJournal::new(&store, vec![claim("a", key)]);
        let runtime = FakeRuntime::new(&store);
        let mut routes = routes();
        routes.insert(
            "space/disabled".into(),
            Route {
                key: "disabled".into(),
                enabled: false,
                runtime_target: "private".into(),
            },
        );
        let mut adapter =
            Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
        adapter.tick().unwrap();
        assert!(runtime.calls.borrow().is_empty());
        assert_eq!(
            store.get("attempt-a").unwrap().injection_state,
            InjectionState::RouteUnavailable
        );
    }
    let f = Fixture::new();
    let store = f.open();
    let clock = FakeClock::new();
    let j = FakeJournal::new(&store, vec![claim("a", None)]);
    let runtime = FakeRuntime::new(&store);
    runtime.unavailable.set(true);
    let routes = routes();
    Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into())
        .unwrap()
        .tick()
        .unwrap();
    clock.advance(1);
    let removed = StaticRoutes::new();
    Adapter::new(
        &j,
        &store,
        &removed,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap()
    .tick()
    .unwrap();
    assert_eq!(runtime.calls.borrow().len(), 1);
    assert_eq!(
        store.get("attempt-a").unwrap().injection_state,
        InjectionState::RouteUnavailable
    );
}

#[test]
fn fence_immediately_before_send_and_pressure_before_claim() {
    let f = Fixture::new();
    let store = f.open();
    let clock = FakeClock::new();
    let j = FakeJournal::new(&store, vec![claim("a", None)]);
    j.fence_at.set(2);
    let runtime = FakeRuntime::new(&store);
    let routes = routes();
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    assert_eq!(adapter.tick(), Err(CoreError::Fenced));
    assert!(runtime.calls.borrow().is_empty());
    assert!(store.get("attempt-a").unwrap().custody_confirmed);
    drop(adapter);
    drop(runtime);
    drop(j);
    drop(store);
    let store = SqliteStore::open(
        f.0.join("spool.db"),
        Limits {
            max_items: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let j = FakeJournal::new(&store, vec![claim("b", None)]);
    let runtime = FakeRuntime::new(&store);
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    adapter.tick().unwrap();
    assert!(matches!(
        adapter.tick(),
        Err(CoreError::SpoolUnavailable(_))
    ));
    assert_eq!(j.claims.borrow().len(), 1);
}

#[test]
fn authoritative_expiry_reclaims_same_attempt_without_overwriting_payload() {
    let f = Fixture::new();
    let store = f.open();
    let clock = FakeClock::new();
    let first = claim("a", None);
    let mut replacement = first.clone();
    replacement.claim_id = "replacement".into();
    let j = FakeJournal::new(&store, vec![first, replacement]);
    j.expired.set(true);
    let runtime = FakeRuntime::new(&store);
    let routes = routes();
    let mut adapter =
        Adapter::new(&j, &store, &routes, &runtime, &clock, "installation".into()).unwrap();
    adapter.tick().unwrap();
    assert!(!store.get("attempt-a").unwrap().custody_confirmed);
    adapter.tick().unwrap();
    adapter.tick().unwrap();
    assert_eq!(runtime.calls.borrow().len(), 1);
    let saved = store.get("attempt-a").unwrap();
    assert_eq!(saved.claim_id, "replacement");
    assert_eq!(saved.attempt_id, "attempt-a");
    assert_eq!(saved.record.unwrap().content, "untrusted\nbody");
}

fn pause(phase: &str) {
    if std::env::var("ORCHESTRATION_CRASH").is_ok_and(|value| value == phase) {
        let path =
            PathBuf::from(std::env::var_os("ORCHESTRATION_CHILD_PATH").unwrap()).join("checkpoint");
        std::fs::write(path, phase).unwrap();
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

#[test]
fn orchestration_child() {
    let Some(path) = std::env::var_os("ORCHESTRATION_CHILD_PATH") else {
        return;
    };
    let store = SqliteStore::open(PathBuf::from(path).join("spool.db"), Limits::default()).unwrap();
    let clock = FakeClock::new();
    let j = FakeJournal::new(&store, vec![claim("a", None)]);
    let runtime = FakeRuntime::new(&store);
    Adapter::new(
        &j,
        &store,
        &routes(),
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap()
    .tick()
    .unwrap();
}

#[test]
fn process_death_at_orchestration_boundaries_exposes_duplicate_risk() {
    for phase in [
        "claimed",
        "spooled",
        "central-custody",
        "injection-started",
        "runtime-accepted",
        "result-persisted",
        "telemetry-accepted",
    ] {
        let f = Fixture::new();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "orchestration_tests::orchestration_child",
                "--nocapture",
            ])
            .env("ORCHESTRATION_CHILD_PATH", &f.0)
            .env("ORCHESTRATION_CRASH", phase)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !f.0.join("checkpoint").exists() {
            if std::time::Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child failed to reach {phase}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let accepted_before_crash = std::fs::read_to_string(f.0.join("acceptances.jsonl"))
            .unwrap_or_default()
            .lines()
            .count();
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            std::fs::copy(f.0.join("spool.db"), f.0.join("before-recovery.db")).unwrap();
        }
        let store = f.open();
        let clock = FakeClock::new();
        let j = FakeJournal::new(&store, vec![claim("a", None)]);
        let runtime = FakeRuntime::new(&store);
        Adapter::new(
            &j,
            &store,
            &routes(),
            &runtime,
            &clock,
            "installation".into(),
        )
        .unwrap()
        .tick()
        .unwrap();
        let count = runtime.calls.borrow().len();
        assert_eq!(
            count,
            usize::from(!matches!(phase, "result-persisted" | "telemetry-accepted")),
            "{phase}"
        );
        if phase == "runtime-accepted" {
            assert_eq!(
                std::fs::read_to_string(f.0.join("sends"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(count, 1, "accepted send is visibly duplicated after crash");
        }
        assert_eq!(
            store.get("attempt-a").unwrap().injection_state,
            InjectionState::Accepted
        );
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            std::fs::write(
                f.0.join("crash-evidence.json"),
                serde_json::to_vec(&serde_json::json!({
                    "phase": phase,
                    "child_terminated": true,
                    "recovery_injections": count,
                    "accepted_before_crash": accepted_before_crash,
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }
}
