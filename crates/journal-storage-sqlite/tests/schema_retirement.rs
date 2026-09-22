use journal_storage_sqlite::{Database, StorageError};

#[test]
fn prior_schema_is_rejected_without_reset_or_sidecar_creation() {
    let path = std::env::temp_dir().join(format!(
        "retired-schema-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let sql = include_str!("../../../migrations/0001_uuid_native.sql")
        .replace("version = 13", "version = 12")
        .replace("VALUES (1, 13,", "VALUES (1, 12,");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch(&sql).unwrap();
    drop(connection);
    let original = std::fs::read(&path).unwrap();
    let result = Database::open(&path);
    let after = std::fs::read(&path).unwrap();
    let sidecars = ["-wal", "-shm", "-journal"]
        .map(|suffix| std::path::Path::new(&format!("{}{suffix}", path.display())).exists());
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(result, Err(StorageError::ResetRequired { .. })));
    assert_eq!(original, after);
    assert_eq!(sidecars, [false; 3]);
}
