use super::*;
use std::cell::RefCell;
use std::sync::mpsc;

thread_local! {
    static AFTER_PROBE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    static BEFORE_WAIT: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    static AFTER_LOCK_OPEN: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    static AFTER_CONNECTION: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

pub(super) fn after_connection_open() {
    let hook = AFTER_CONNECTION.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[test]
fn killed_initializer_is_rejected_without_changing_abandoned_state() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};

    const CHILD_ROOT: &str = "JOURNAL_TEST_INITIALIZER_ROOT";
    const CHILD_PHASE: &str = "JOURNAL_TEST_INITIALIZER_PHASE";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let pause = || {
            println!("INITIALIZER_PAUSED");
            std::io::stdout().flush().unwrap();
            loop {
                thread::park();
            }
        };
        if std::env::var(CHILD_PHASE).unwrap() == "marker" {
            AFTER_LOCK_OPEN.with(|slot| *slot.borrow_mut() = Some(Box::new(pause)));
        } else {
            AFTER_CONNECTION.with(|slot| *slot.borrow_mut() = Some(Box::new(pause)));
        }
        Database::open(PathBuf::from(root).join("central.db")).unwrap();
        panic!("initializer should have been killed");
    }

    for phase in ["marker", "connection"] {
        let directory =
            std::env::temp_dir().join(format!("journal-init-kill-{phase}-{}", std::process::id()));
        fs::create_dir(&directory).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "initialization_tests::killed_initializer_is_rejected_without_changing_abandoned_state",
                "--nocapture",
            ])
            .env(CHILD_ROOT, &directory)
            .env(CHILD_PHASE, phase)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (paused_tx, paused_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in std::io::BufReader::new(output).lines() {
                if line.unwrap().contains("INITIALIZER_PAUSED") {
                    paused_tx.send(()).unwrap();
                    break;
                }
            }
        });
        let paused = paused_rx.recv_timeout(Duration::from_secs(10));
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        paused.expect("child reached the selected initialization boundary");

        let snapshot = || {
            let mut files = fs::read_dir(&directory)
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (entry.file_name(), fs::read(entry.path()).unwrap())
                })
                .collect::<Vec<_>>();
            files.sort();
            files
        };
        let before = snapshot();
        assert!(matches!(
            Database::open(directory.join("central.db")),
            Err(StorageError::ResetRequired { .. })
        ));
        assert_eq!(snapshot(), before, "abandoned input must remain untouched");
        fs::remove_dir_all(directory).unwrap();
    }
}

pub(super) fn after_lock_open() {
    let hook = AFTER_LOCK_OPEN.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[test]
fn waiter_cannot_own_unlinked_marker_or_remove_its_successor() {
    let directory = std::env::temp_dir().join(format!("journal-init-inode-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("central.db");
    let owner = try_acquire_initialization_lock(&path).unwrap().unwrap();
    let (opened_tx, opened_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let waiter_path = path.clone();
    let waiter = thread::spawn(move || {
        AFTER_LOCK_OPEN.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                opened_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            }));
        });
        try_acquire_initialization_lock(&waiter_path).unwrap()
    });
    opened_rx.recv().unwrap();
    drop(owner);
    let successor = try_acquire_initialization_lock(&path).unwrap().unwrap();
    resume_tx.send(()).unwrap();
    let stale_owner = waiter.join().unwrap();
    let refused = stale_owner.is_none();
    drop(stale_owner);
    let successor_preserved = initialization_lock_path(&path).exists();
    drop(successor);
    fs::remove_dir_all(directory).unwrap();
    assert!(refused, "a waiter must not lock an unlinked marker inode");
    assert!(
        successor_preserved,
        "a waiter must not unlink another owner"
    );
}

pub(super) fn before_initialization_wait() {
    let hook = BEFORE_WAIT.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn after_admission_probe() {
    let hook = AFTER_PROBE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[test]
fn initializer_starting_after_admission_probe_is_not_cold_input() {
    let directory = std::env::temp_dir().join(format!("journal-init-probe-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("central.db");
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let initializer_path = path.clone();
    let initializer = thread::spawn(move || {
        started_rx.recv().unwrap();
        let _lock = try_acquire_initialization_lock(&initializer_path)
            .unwrap()
            .unwrap();
        let factory = ConnectionFactory::new(&initializer_path);
        let mut connection = factory.connect_unchecked().unwrap();
        finish_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        apply_uuid_native_baseline(&mut connection).unwrap();
        drop(connection);
    });
    AFTER_PROBE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            started_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
        }));
    });
    let release_wait = release_tx.clone();
    BEFORE_WAIT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            release_wait.send(()).unwrap();
        }));
    });
    let result = Database::open(&path);
    let _ = release_tx.send(());
    initializer.join().unwrap();
    BEFORE_WAIT.with(|slot| slot.borrow_mut().take());
    result.expect("concurrent initializer is not malformed cold input");
    Database::open_existing(&path).unwrap();
    fs::remove_dir_all(directory).unwrap();
}
