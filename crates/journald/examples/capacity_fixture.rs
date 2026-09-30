//! Disposable measurement fixture; no production input paths or configuration changes.
use journal_protocol::{domain::SpaceAccess, *};
use journal_service::BootstrapService;
use journal_storage_sqlite::{Database, StorageError};
use journald::{BlockingError, BlockingExecutor, ServiceState, public_router};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct Fixture(PathBuf);
impl Fixture {
    #[cfg(not(unix))]
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Err("protected measurement fixtures require Unix".into())
    }

    #[cfg(unix)]
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        use std::os::unix::fs::DirBuilderExt;
        let root = std::env::temp_dir().join(format!("journal-capacity-{}", std::process::id()));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let fixture = Self(root);
        for name in ["central", "audit", "copies"] {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(fixture.0.join(name))?;
        }
        Ok(fixture)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
fn samples(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    json!({"n": values.len(), "min_ms": values[0], "median_ms": values[values.len()/2],
        "p95_ms": values[((values.len()*95).div_ceil(100)-1).min(values.len()-1)],
        "max_ms": values[values.len()-1], "samples_ms": values})
}
fn token(index: usize) -> String {
    format!("{index:064x}")
}
fn input(index: usize, addressed: bool) -> AppendRecordRequest {
    AppendRecordRequest {
        kind: "note".into(),
        content: format!(
            "synthetic common journal record {index} {}",
            "common bounded payload ".repeat(20)
        ),
        run_id: None,
        routing_key: None,
        title: None,
        relations: vec![],
        attention: if addressed {
            vec!["recipient".into()]
        } else {
            vec![]
        },
    }
}
fn query(order: SearchOrder) -> SearchRecordsQuery {
    SearchRecordsQuery {
        q: "common".into(),
        page: PageQuery::new(None, Some(50)),
        author: None,
        attention: None,
        since: None,
        order,
    }
}
async fn executor_measurement(db: &Database) -> Result<Value, Box<dyn std::error::Error>> {
    // Closure entry/exit counts observe actual work, excluding spawn scheduling/permit release tails.
    let executor = BlockingExecutor::new(db.clone(), 8)?;
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    let start = Instant::now();
    for _ in 0..12 {
        let (executor, active, peak, rejected) = (
            executor.clone(),
            active.clone(),
            peak.clone(),
            rejected.clone(),
        );
        tasks.spawn(async move {
            let mut durations = Vec::new();
            for _ in 0..20 {
                let (active, peak) = (active.clone(), peak.clone());
                match executor
                    .execute(move |db| {
                        let started = Instant::now();
                        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(count, Ordering::SeqCst);
                        let result = BootstrapService::new(db.clone()).search_records(
                            &token(1),
                            "bench",
                            &query(SearchOrder::Rank),
                        );
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok((ms(started), result.map(|p| p.items.len())))
                    })
                    .await
                {
                    Ok((duration, Ok(50))) => durations.push(duration),
                    Err(BlockingError::AtCapacity) => {
                        rejected.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => return Err("unexpected executor/search outcome"),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(durations)
        });
    }
    let mut durations = Vec::new();
    while let Some(result) = tasks.join_next().await {
        durations.extend(result??);
    }
    let elapsed_ms = ms(start);
    let work_ms = durations.iter().sum::<f64>();
    // Abort only the async waiter for a read. Writes are never cancelled.
    let executor = BlockingExecutor::new(db.clone(), 1)?;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let copy = executor.clone();
    let task = tokio::spawn(async move {
        copy.execute(move |db| {
            let started = Instant::now();
            let _ = started_tx.send(());
            let result = BootstrapService::new(db.clone()).search_records(
                &token(1),
                "bench",
                &query(SearchOrder::Rank),
            );
            let _ = finished_tx.send((Instant::now(), ms(started), result.is_ok()));
            Ok(())
        })
        .await
    });
    started_rx.await?;
    let aborted = Instant::now();
    task.abort();
    let _ = task.await;
    let capacity_after_abort = matches!(
        executor.execute(|_| Ok(())).await,
        Err(BlockingError::AtCapacity)
    );
    let (finished, duration, success) = finished_rx.await?;
    if !success {
        return Err("detached search failed".into());
    }
    // Receiving the closure's message precedes permit drop; let the closure finish naturally.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match executor.execute(|_| Ok(())).await {
            Ok(()) => break,
            Err(BlockingError::AtCapacity) if Instant::now() < deadline => {
                tokio::task::yield_now().await
            }
            _ => return Err("detached executor did not drain".into()),
        }
    }
    Ok(
        json!({"limit":8,"workers":12,"attempted":240,"completed":durations.len(),
        "rejected_at_capacity":rejected.load(Ordering::SeqCst),"wall_ms":elapsed_ms,
        "peak_active_closures":peak.load(Ordering::SeqCst),"closure_work_ms":work_ms,
        "mean_active_closures":work_ms/elapsed_ms,"search_closure":samples(durations),
        "aborted_read_waiter":{"read_completed":success,"finished_after_abort":finished>aborted,
            "capacity_rejected_after_abort":capacity_after_abort,"read_closure_ms":duration,"drained":true}}),
    )
}
fn backup_measurement(
    db: &Database,
    fixture: &Fixture,
    round: usize,
) -> Result<Value, Box<dyn std::error::Error>> {
    backup_measurement_inner(
        db,
        fixture,
        round,
        #[cfg(all(test, unix))]
        None,
    )
}

fn backup_measurement_inner(
    db: &Database,
    fixture: &Fixture,
    round: usize,
    #[cfg(all(test, unix))] hooks: Option<&tests::DrainHooks>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let raw = fixture.0.join("copies").join(format!("raw-{round}.db"));
    let start = Instant::now();
    let raw_result = db.backup_to(&raw)?;
    let copy_basic_ms = ms(start);
    let copied = Database::open(&raw)?;
    let start = Instant::now();
    let expected = copied.recovery_verification()?;
    let verification_ms = ms(start);
    drop(copied);
    let existing = db.connect_read_only()?;
    let destination = fixture
        .0
        .join("copies")
        .join(format!("protected-{round}.db"));
    let (
        result,
        protected_total_ms,
        probe_start_offset_ms,
        observed_during_backup,
        preadmitted_count,
        preadmitted_read_ms,
        new_read_ms,
        append_ms,
    ) = std::thread::scope(|scope| -> Result<_, Box<dyn std::error::Error>> {
        let copy = db.clone();
        let dest = destination.clone();
        let started = Instant::now();
        let backup = scope.spawn(move || {
            #[cfg(all(test, unix))]
            if let Some(hooks) = hooks {
                match hooks.fault {
                    tests::Fault::BackupError => {
                        return (
                            Instant::now(),
                            Err(StorageError::BackupIntegrity(
                                "injected backup failure".into(),
                            )),
                        );
                    }
                    tests::Fault::BackupPanic => panic!("injected backup worker panic"),
                    _ => {}
                }
            }
            let result = copy.recovery_audit().unwrap().backup(&copy, &dest);
            (Instant::now(), result)
        });
        // Destination reservation occurs under the audit mutex. Observe it before probing.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !destination.exists() && !backup.is_finished() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let probe_start_offset_ms = ms(started);
        let observed_during_backup = destination.exists() && !backup.is_finished();
        let read_db = db.clone();
        let reader = scope.spawn(move || {
            let start = Instant::now();
            #[cfg(all(test, unix))]
            if let Some(hooks) = hooks {
                match hooks.fault {
                    tests::Fault::ReaderError => {
                        return (
                            ms(start),
                            Err(StorageError::BackupIntegrity(
                                "injected reader failure".into(),
                            )),
                        );
                    }
                    tests::Fault::ReaderPanic => panic!("injected reader worker panic"),
                    _ => {}
                }
            }
            let result = read_db.connect_read_only().and_then(|c| {
                c.query_row("SELECT count(*) FROM records", [], |r| r.get::<_, i64>(0))
                    .map_err(StorageError::from)
            });
            (ms(start), result)
        });
        let write_db = db.clone();
        let writer = scope.spawn(move || {
            let start = Instant::now();
            let service = BootstrapService::new(write_db);
            #[cfg(all(test, unix))]
            let service = hooks.map(|hooks| hooks.writer.clone()).unwrap_or(service);
            let result = service.append_record(
                &token(3),
                "bench",
                &format!("backup-probe-{round}"),
                &input(round, true),
            );
            #[cfg(all(test, unix))]
            if let Some(hooks) = hooks {
                let verified = hooks
                    .database
                    .recovery_verification()
                    .is_ok_and(|verification| {
                        ["records", "mailbox_items"].into_iter().all(|name| {
                            verification
                                .tables
                                .iter()
                                .any(|table| table.name == name && table.rows == 1)
                        })
                    });
                let files_present = hooks.root.join("central/journal.db").exists()
                    && hooks.root.join("audit/recovery.db").exists();
                let _ = hooks
                    .writer_done
                    .send((result.is_ok(), verified, files_present));
            }
            (ms(start), result)
        });
        #[cfg(all(test, unix))]
        if let Some(hooks) = hooks {
            // The injected clock stops the append inside its protected transaction.
            hooks
                .writer_entered
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))?;
            hooks.at_error_boundary.send(())?;
        }
        let start = Instant::now();
        #[cfg(all(test, unix))]
        let read_sql =
            if hooks.is_some_and(|hooks| matches!(hooks.fault, tests::Fault::PreadmittedRead)) {
                "SELECT count(*) FROM missing_capacity_fixture_table"
            } else {
                "SELECT count(*) FROM records"
            };
        #[cfg(not(all(test, unix)))]
        let read_sql = "SELECT count(*) FROM records";
        let preadmitted_result = existing.query_row(read_sql, [], |r| r.get::<_, i64>(0));
        let preadmitted_read_ms = ms(start);
        // Drain all workers before inspecting any result. The scope also drains on
        // unwinding or an error before these explicit joins (including spawn failure).
        let backup_result = backup.join();
        let reader_result = reader.join();
        let writer_result = writer.join();
        let preadmitted_count = preadmitted_result?;
        let (finished, result) = backup_result.map_err(|_| "backup thread panicked")?;
        let result = result?;
        let (new_read_ms, read_result) = reader_result.map_err(|_| "reader panicked")?;
        read_result?;
        let (append_ms, append_result) = writer_result.map_err(|_| "writer panicked")?;
        append_result?;
        Ok((
            result,
            finished.duration_since(started).as_secs_f64() * 1000.0,
            probe_start_offset_ms,
            observed_during_backup,
            preadmitted_count,
            preadmitted_read_ms,
            new_read_ms,
            append_ms,
        ))
    })?;
    let backup_db = Database::open(&destination)?;
    let same = backup_db.recovery_verification()? == result && result == expected;
    drop(backup_db);
    if !same || db.recovery_status()?.last_backup_at.is_none() {
        return Err("backup verification/evidence mismatch".into());
    }
    Ok(json!({"copy_plus_basic_verification_ms":copy_basic_ms,
        "separate_full_verification_ms":verification_ms,"protected_total_ms":protected_total_ms,
        "probe_start_offset_ms":probe_start_offset_ms,"observed_destination_while_backup_running":observed_during_backup,
        "new_read_ms":new_read_ms,"audited_append_ms":append_ms,"preadmitted_read_ms":preadmitted_read_ms,
        "preadmitted_record_count":preadmitted_count,"backup_records":raw_result.record_count,
        "verification_equal":same,"durable_backup_timestamp_present":true}))
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    // Deliberately accept only a bounded count, never a database/audit/endpoint path.
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 {
        return Err("usage: capacity_fixture RECORDS (100..=20000)".into());
    }
    let records: usize = args[0].parse()?;
    if !(100..=20_000).contains(&records) {
        return Err("record count must be 100..=20000".into());
    }
    let fixture = Fixture::new()?;
    let db = Database::open_protected(
        fixture.0.join("central/journal.db"),
        fixture.0.join("audit/recovery.db"),
    )?;
    let service = BootstrapService::new(db.clone());
    for (index, handle) in [(1, "searcher"), (2, "recipient"), (3, "writer")] {
        service.register(
            &token(index),
            &RegistrationRequest {
                handle: handle.into(),
                display_name: handle.into(),
            },
        )?;
    }
    service.create_space(&SpaceCreateRequest {
        id: "bench".into(),
        name: "Synthetic measurement".into(),
        access: SpaceAccess::Public,
    })?;
    let seeded = Instant::now();
    for index in 0..records {
        service.append_record(
            &token(3),
            "bench",
            &format!("seed-{index}"),
            &input(index, index % 10 == 0),
        )?;
    }
    let seed_ms = ms(seeded);
    let mut searches = serde_json::Map::new();
    for (label, order) in [("seq", SearchOrder::Seq), ("rank", SearchOrder::Rank)] {
        service.search_records(&token(1), "bench", &query(order))?; // warm cursor and pages
        let mut times = Vec::new();
        for _ in 0..7 {
            let start = Instant::now();
            let page = service.search_records(&token(1), "bench", &query(order))?;
            if page.items.len() != 50 {
                return Err("unexpected search page size".into());
            }
            times.push(ms(start));
        }
        searches.insert(label.into(), samples(times));
    }
    let mut backups = Vec::new();
    for round in 0..3 {
        backups.push(backup_measurement(&db, &fixture, round)?);
    }
    let execution = executor_measurement(&db).await?;
    let connection = db.connect_read_only()?;
    let sqlite_config = json!({"sqlite_version":rusqlite::version(),
        "page_size":connection.query_row("PRAGMA page_size",[],|r|r.get::<_,i64>(0))?,
        "journal_mode":connection.query_row("PRAGMA journal_mode",[],|r|r.get::<_,String>(0))?,
        "synchronous":connection.query_row("PRAGMA synchronous",[],|r|r.get::<_,i64>(0))?,
        "cache_size":connection.query_row("PRAGMA cache_size",[],|r|r.get::<_,i64>(0))?,
        "busy_timeout_ms":connection.query_row("PRAGMA busy_timeout",[],|r|r.get::<_,i64>(0))?});
    drop(connection);
    let storage = db.operational_snapshot()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let router = public_router(ServiceState::new(db.clone(), 8)?, 1_048_576);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    println!(
        "{}",
        json!({"endpoint":endpoint,"baseline":{"records":records,"principals":3,
        "content_bytes":input(0,false).content.len(),"addressed_every":10,"seed_ms":seed_ms,
        "sqlite":sqlite_config,"database_bytes":storage.database_bytes,"wal_bytes":storage.wal_bytes,
        "search":searches,"backup":backups,"executor":execution}})
    );
    std::io::stdout().flush()?;
    // Parent ends the bounded HTTP phase by writing one line. EOF also cleans up.
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)
    })
    .await??;
    let _ = shutdown_tx.send(());
    server.await??;
    let verification = db.recovery_verification()?;
    let final_count = verification
        .tables
        .iter()
        .find(|table| table.name == "records")
        .ok_or("record verification missing")?
        .rows;
    println!(
        "{}",
        json!({"final_records":final_count,"full_verification_passed":true})
    );
    db.normalize_for_clean_shutdown()?;
    drop(service);
    drop(db);
    drop(fixture);
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use journal_service::{Clock, OsSecretSource};
    use std::sync::{Mutex, atomic::AtomicBool, mpsc};
    use std::time::SystemTime;

    #[derive(Clone, Copy, Debug)]
    pub enum Fault {
        PreadmittedRead,
        BackupError,
        BackupPanic,
        ReaderError,
        ReaderPanic,
    }
    pub struct DrainHooks {
        pub fault: Fault,
        pub writer: BootstrapService,
        pub database: Database,
        pub root: PathBuf,
        pub writer_entered: Mutex<mpsc::Receiver<()>>,
        pub at_error_boundary: mpsc::Sender<()>,
        pub writer_done: mpsc::Sender<(bool, bool, bool)>,
    }
    struct HeldWriterClock {
        first: AtomicBool,
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Clock for HeldWriterClock {
        fn now(&self) -> SystemTime {
            if self.first.swap(false, Ordering::SeqCst) {
                let _ = self.entered.send(());
                // Dropping the controller's sender also releases the test writer.
                let _ = self.release.lock().unwrap().recv();
            }
            SystemTime::now()
        }
    }
    struct ReleaseOnDrop(Option<mpsc::Sender<()>>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn backup_failures_drain_audited_writer_before_fixture_cleanup() {
        for fault in [
            Fault::PreadmittedRead,
            Fault::BackupError,
            Fault::BackupPanic,
            Fault::ReaderError,
            Fault::ReaderPanic,
        ] {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (boundary_tx, boundary_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let (writer_done_tx, writer_done_rx) = mpsc::channel();
            let (returned_tx, returned_rx) = mpsc::channel();
            let (root_tx, root_rx) = mpsc::channel();
            let clock = Arc::new(HeldWriterClock {
                first: AtomicBool::new(true),
                entered: entered_tx,
                release: Mutex::new(release_rx),
            });
            std::thread::scope(|scope| {
                let release = ReleaseOnDrop(Some(release_tx));
                let parent = scope.spawn(move || {
                    // Reproduce main's ownership/unwinding: the error drops Fixture.
                    let operation = || -> Result<(), Box<dyn std::error::Error>> {
                        let fixture = Fixture::new()?;
                        root_tx.send(fixture.0.clone())?;
                        let db = Database::open_protected(
                            fixture.0.join("central/journal.db"),
                            fixture.0.join("audit/recovery.db"),
                        )?;
                        let service = BootstrapService::new(db.clone());
                        for (index, handle) in [(2, "recipient"), (3, "writer")] {
                            service.register(
                                &token(index),
                                &RegistrationRequest {
                                    handle: handle.into(),
                                    display_name: handle.into(),
                                },
                            )?;
                        }
                        service.create_space(&SpaceCreateRequest {
                            id: "bench".into(),
                            name: "Drain regression".into(),
                            access: SpaceAccess::Public,
                        })?;
                        let hooks = DrainHooks {
                            fault,
                            writer: BootstrapService::with_sources(
                                db.clone(),
                                clock,
                                Arc::new(OsSecretSource),
                            ),
                            database: db.clone(),
                            root: fixture.0.clone(),
                            writer_entered: Mutex::new(entered_rx),
                            at_error_boundary: boundary_tx,
                            writer_done: writer_done_tx,
                        };
                        backup_measurement_inner(&db, &fixture, 0, Some(&hooks))?;
                        Ok(())
                    };
                    let _ = returned_tx.send(operation().is_err());
                });
                let root = root_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                boundary_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let early = returned_rx.recv_timeout(Duration::from_millis(100));
                let retained_while_writer_held = root.join("central/journal.db").exists()
                    && root.join("audit/recovery.db").exists();
                drop(release);
                let writer = writer_done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let returned_error = match early {
                    Ok(value) => value,
                    Err(_) => returned_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                };
                parent.join().unwrap();
                // Assert after release/join so even a failing regression test drains.
                assert!(
                    matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
                    "{fault:?}: parent returned before writer drained"
                );
                assert!(
                    retained_while_writer_held,
                    "{fault:?}: files removed during audited append"
                );
                assert_eq!(
                    writer,
                    (true, true, true),
                    "{fault:?}: append, verification or file retention failed"
                );
                assert!(
                    returned_error,
                    "{fault:?}: injected failure did not propagate"
                );
                assert!(
                    !root.exists(),
                    "{fault:?}: fixture was not cleaned after drain"
                );
            });
        }
    }
}
