use crate::{Database, StorageError};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn protected_reads_wait_for_writers_but_reject_abandoned_intents() {
    let directory = std::env::temp_dir().join(format!("journal-read-gate-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let database =
        Database::open_protected(directory.join("central.db"), directory.join("audit.db")).unwrap();
    let audit = database.recovery_audit().unwrap();
    type Read = fn(&Database) -> Result<(), StorageError>;
    let readers: [Read; 3] = [
        |db| db.connect_read_only().map(|_| ()),
        |db| db.recovery_status().map(|_| ()),
        |db| db.recovery_verification().map(|_| ()),
    ];
    for read in readers {
        let guard = audit.lock().unwrap();
        let mut connection = database.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        let revision = audit.prepare(&transaction).unwrap();
        let reader_db = database.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx.send(read(&reader_db)).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let early = result_rx.recv_timeout(Duration::from_millis(100));
        transaction.commit().unwrap();
        audit.committed(revision).unwrap();
        drop(guard);
        assert!(
            matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
            "read must wait for the healthy writer, got {early:?}"
        );
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        reader.join().unwrap();
    }
    // Backup and verification already hold the gate internally; neither may relock it.
    audit
        .backup(&database, &directory.join("backup.db"))
        .unwrap();
    database
        .backup_to(directory.join("second-backup.db"))
        .unwrap();
    let guard = audit.lock().unwrap();
    let mut connection = database.connect().unwrap();
    let transaction = connection.transaction().unwrap();
    audit.prepare(&transaction).unwrap();
    transaction.rollback().unwrap();
    drop(guard);
    for read in readers {
        assert!(matches!(
            read(&database),
            Err(StorageError::RecoveryClosed(_))
        ));
    }
    drop(connection);
    drop(database);
    std::fs::remove_dir_all(directory).unwrap();
}
