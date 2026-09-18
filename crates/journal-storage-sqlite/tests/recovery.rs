use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use journal_storage_sqlite::Database;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "journal-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn database(&self) -> Database {
        let database = Database::open(self.0.join("journal.db")).unwrap();
        database.connect().unwrap().execute_batch(
            "INSERT INTO principals VALUES ('p','Principal','2026-01-01T00:00:00Z',NULL);
             INSERT INTO spaces VALUES ('s','Space','2026-01-01T00:00:00Z',NULL);
             INSERT INTO memberships VALUES ('s','p',1,1,0,'2026-01-01T00:00:00Z');
             INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
             VALUES ('r','s',1,'p','note','recovery probe','2026-01-01T00:00:00Z');
             INSERT INTO attention VALUES ('r','p','2026-01-01T00:00:00Z');
             INSERT INTO mailbox_items VALUES ('m','r','p','pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        ).unwrap();
        database
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn restore_probes_cover_hashes_acls_heads_and_mailbox_history() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let expected = database.recovery_verification().unwrap();
    database.backup_to(fixture.0.join("backup.db")).unwrap();
    Database::restore_backup(fixture.0.join("backup.db"), fixture.0.join("restored.db")).unwrap();
    let restored = Database::open(fixture.0.join("restored.db")).unwrap();
    assert_eq!(restored.recovery_verification().unwrap(), expected);
    restored
        .connect()
        .unwrap()
        .execute(
            "UPDATE memberships SET can_read=0 WHERE space_id='s' AND principal_id='p'",
            [],
        )
        .unwrap();
    assert_ne!(restored.recovery_verification().unwrap(), expected);
}

#[test]
fn syntactically_valid_but_incomplete_fts_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM records_fts", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[test]
fn corrupted_fts_postings_fail_even_when_stored_content_remains() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let connection = database.connect().unwrap();
    connection
        .execute("DELETE FROM records_fts_data WHERE id>10", [])
        .unwrap();
    let content: String = connection
        .query_row(
            "SELECT content FROM records_fts WHERE record_id='r'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content, "recovery probe");
    assert!(database.recovery_verification().is_err());
}

#[test]
fn missing_attempt_history_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM delivery_attempts", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[cfg(not(unix))]
#[test]
fn protected_recovery_has_no_non_unix_permission_fallback() {
    let fixture = Fixture::new();
    assert!(
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db")).is_err()
    );
    assert!(!fixture.0.join("central.db").exists());
    assert!(!fixture.0.join("audit.db").exists());
}

#[test]
fn missing_attention_obligation_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM attention", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[test]
fn backup_under_writes_has_consistent_heads_and_hashes() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let writer = database.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut connection = writer.connect().unwrap();
        for sequence in 2..=100 {
            let transaction = connection.transaction().unwrap();
            transaction.execute(
                "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                 VALUES (?,'s',?,'p','note','concurrent recovery token','2026-01-01T00:00:00Z')",
                rusqlite::params![format!("r{sequence}"), sequence],
            ).unwrap();
            if sequence == 100 {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
            }
            transaction.commit().unwrap();
        }
    });
    started_rx.recv().unwrap();
    database.backup_to(fixture.0.join("backup.db")).unwrap();
    finish_tx.send(()).unwrap();
    thread.join().unwrap();
    let backup = Database::open(fixture.0.join("backup.db")).unwrap();
    let expected = backup.recovery_verification().unwrap();
    assert_eq!(expected.space_heads, vec![("s".to_owned(), 99)]);
    Database::restore_backup(fixture.0.join("backup.db"), fixture.0.join("restored.db")).unwrap();
    assert_eq!(
        Database::open(fixture.0.join("restored.db"))
            .unwrap()
            .recovery_verification()
            .unwrap(),
        expected
    );
}
