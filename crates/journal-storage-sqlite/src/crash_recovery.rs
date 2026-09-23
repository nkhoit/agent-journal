//! Verified recovery of hot SQLite state left by an abrupt stop of a protected
//! daemon. The live files are replayed only after a private copy of them has
//! replayed into exactly the state the external audit last committed, and a
//! durable marker keeps an interrupted or rejected replay from being admitted.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

use crate::recovery_audit::RecoveryAudit;
use crate::{Database, RecoveryVerification, StorageError, sqlite_sidecar_entries_exist};

/// Sidecars whose contents SQLite replays. The shared-memory index is derived
/// from the WAL and rebuilt, so it is validated but never copied.
const REPLAYED_SIDECARS: [&str; 2] = ["-wal", "-journal"];
/// Written first into every scratch directory this procedure creates.
const PROVENANCE: &str = "agent-journal-crash-recovery-v1";
/// Written, synced, before the original is replayed in place. While present,
/// every protected start must finish this recovery or keep refusing.
const VERIFIED: &str = "verified.json";
/// Staged marker contents; renamed onto `VERIFIED` only once fully synced.
const VERIFIED_STAGING: &str = "verified.json.tmp";
const COPY: &str = "central.db";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Verified {
    /// Distinguishes separate crash incidents that verify identical state, so
    /// only retries of one marker share an audit event.
    pub recovery_id: String,
    pub audit_revision: i64,
    pub verification: RecoveryVerification,
}

/// A verified in-place replay whose scratch directory, and therefore its
/// resume obligation, remains until `complete` after the audit event commits.
#[derive(Debug)]
pub(crate) struct CrashRecovery {
    pub verified: Verified,
    scratch: PathBuf,
}

impl CrashRecovery {
    pub(crate) fn complete(self) -> Result<(), StorageError> {
        fs::remove_dir_all(&self.scratch).map_err(|source| {
            io(
                "remove completed crash-recovery copy",
                &self.scratch,
                source,
            )
        })
    }
}

/// Whether an interrupted or rejected in-place replay is pending.
pub(crate) fn pending(path: &Path) -> Result<bool, StorageError> {
    entry_exists(&scratch_path(path).join(VERIFIED))
}

/// The caller holds the audit owner lock, so no protected daemon or recovery
/// tool is writing these files. Failures before the marker is written leave the
/// central database and its sidecars byte-for-byte untouched; later failures
/// leave the marker, so no start admits the database until this completes.
pub(crate) fn recover(path: &Path, audit_path: &Path) -> Result<CrashRecovery, StorageError> {
    for suffix in ["-journal", "-wal", "-shm"] {
        validate_sidecar(&sidecar(path, suffix))?;
    }
    let scratch = scratch_path(path);
    let marker = scratch.join(VERIFIED);
    let verified = if entry_exists(&marker)? {
        let bytes = fs::read(&marker)
            .map_err(|source| io("read crash-recovery marker", &marker, source))?;
        serde_json::from_slice(&bytes)?
    } else {
        let guard = Scratch::create(&scratch)?;
        let verified = verify_copy(path, audit_path, &scratch)?;
        publish(
            &scratch.join(VERIFIED_STAGING),
            &marker,
            &serde_json::to_vec(&verified)?,
        )?;
        guard.retain();
        verified
    };
    #[cfg(test)]
    tests::boundary("verified");
    normalize(path)?;
    #[cfg(test)]
    tests::boundary("normalized");
    // The identical probe set, including FTS5's internal integrity check.
    let original = Database::open_existing(path)?.recovery_verification_unguarded()?;
    normalize(path)?;
    if original != verified.verification {
        return Err(StorageError::RecoveryClosed(
            "in-place crash recovery diverged from its verified copy; preserve the crash-recovery directory and archive/reset",
        ));
    }
    Ok(CrashRecovery { verified, scratch })
}

fn verify_copy(path: &Path, audit_path: &Path, scratch: &Path) -> Result<Verified, StorageError> {
    let copy = scratch.join(COPY);
    copy_file(path, &copy)?;
    for suffix in REPLAYED_SIDECARS {
        let source = sidecar(path, suffix);
        if entry_exists(&source)? {
            copy_file(&source, &sidecar(&copy, suffix))?;
        }
    }
    normalize(&copy)?;
    let replayed = Database::open_existing(&copy)?;
    let audit_revision = RecoveryAudit::preflight_existing(&replayed, audit_path)?;
    let verification = replayed.recovery_verification_unguarded()?;
    drop(replayed);
    normalize(&copy)?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    Ok(Verified {
        recovery_id: format!("{nanos:032x}-{:08x}", std::process::id()),
        audit_revision,
        verification,
    })
}

/// Let SQLite roll back a hot journal or replay a WAL, then leave a standalone
/// rollback-journal database with no sidecars.
fn normalize(path: &Path) -> Result<(), StorageError> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(crate::BUSY_TIMEOUT)?;
    connection.query_row("SELECT count(*) FROM sqlite_schema", [], |row| {
        row.get::<_, i64>(0)
    })?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(connection);
    if sqlite_sidecar_entries_exist(path)? {
        return Err(StorageError::RecoveryClosed(
            "crash recovery could not normalize SQLite sidecars",
        ));
    }
    Ok(())
}

/// Removes a scratch directory whose copy was never used to replay the
/// original, unless `retain` records that the marker now protects it.
struct Scratch {
    path: PathBuf,
    retained: bool,
}

impl Scratch {
    fn create(path: &Path) -> Result<Self, StorageError> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => {
                if !stale_copy(path)? {
                    return Err(StorageError::RecoveryClosed(
                        "unrecognized crash-recovery directory; move it aside",
                    ));
                }
                fs::remove_dir_all(path)
                    .map_err(|source| io("remove stale crash-recovery copy", path, source))?;
            }
            Ok(_) => {
                return Err(StorageError::RecoveryClosed(
                    "crash-recovery scratch path is not a directory",
                ));
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io("inspect crash-recovery copy", path, source)),
        }
        #[cfg(unix)]
        let created = {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(path)
        };
        #[cfg(not(unix))]
        let created = fs::create_dir(path);
        created.map_err(|source| io("create crash-recovery copy", path, source))?;
        let scratch = Self {
            path: path.to_owned(),
            retained: false,
        };
        write_synced(&path.join(PROVENANCE), PROVENANCE.as_bytes())?;
        sync_dir(path)?;
        sync_dir(parent(path))?;
        Ok(scratch)
    }

    fn retain(mut self) {
        self.retained = true;
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if !self.retained {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Only a directory this procedure created and never used to replay the
/// original is disposable: it has no marker and nothing but the copy's files,
/// and it is either tagged or empty (interrupted before the tag was written).
fn stale_copy(path: &Path) -> Result<bool, StorageError> {
    let allowed = [
        PROVENANCE.to_owned(),
        VERIFIED_STAGING.to_owned(),
        COPY.to_owned(),
        format!("{COPY}-wal"),
        format!("{COPY}-journal"),
        format!("{COPY}-shm"),
    ];
    let mut tagged = false;
    let mut empty = true;
    for entry in
        fs::read_dir(path).map_err(|source| io("list crash-recovery copy", path, source))?
    {
        let entry = entry.map_err(|source| io("list crash-recovery copy", path, source))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let regular = entry
            .file_type()
            .map_err(|source| io("inspect crash-recovery copy", path, source))?
            .is_file();
        if !regular || !allowed.contains(&name) {
            return Ok(false);
        }
        empty = false;
        tagged |= name == PROVENANCE;
    }
    Ok(tagged || empty)
}

/// Publish complete, synced contents at `path` by rename, so the name is
/// either absent or fully written after any crash.
fn publish(staging: &Path, path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    if entry_exists(staging)? {
        fs::remove_file(staging)
            .map_err(|source| io("remove staged crash-recovery marker", staging, source))?;
    }
    write_synced(staging, bytes)?;
    fs::rename(staging, path)
        .map_err(|source| io("publish crash-recovery marker", path, source))?;
    sync_dir(parent(path))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io("create crash-recovery evidence", path, source))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| io("write crash-recovery evidence", path, source))
}

/// A bare relative path's parent is empty; that directory is `.`.
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn sync_dir(directory: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync crash-recovery directory", directory, source))?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

/// Sidecars follow the central file's mode, so only aliasing is checked: a
/// symlink or extra hardlink could point replay at some other file's state.
fn validate_sidecar(path: &Path) -> Result<(), StorageError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io("inspect SQLite sidecar", path, source)),
    };
    if !metadata.is_file() {
        return Err(StorageError::RecoveryClosed(
            "SQLite sidecar is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(StorageError::RecoveryClosed(
                "SQLite sidecar has multiple links",
            ));
        }
    }
    Ok(())
}

fn scratch_path(path: &Path) -> PathBuf {
    sidecar(path, ".crash-recovery")
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), StorageError> {
    fs::copy(source, destination)
        .map(drop)
        .map_err(|error| io("copy hot SQLite state", source, error))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn entry_exists(path: &Path) -> Result<bool, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io("inspect crash-recovery state", path, source)),
    }
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> StorageError {
    StorageError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::process::{Child, Command};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    /// Parks a recovering child process at `stage` so the parent can kill it.
    pub(crate) fn boundary(stage: &str) {
        if std::env::var("JOURNAL_CRASH_RECOVERY_BOUNDARY").as_deref() == Ok(stage) {
            let root = PathBuf::from(std::env::var_os("JOURNAL_CRASH_RECOVERY_ROOT").unwrap());
            fs::write(root.join("ready"), stage).unwrap();
            loop {
                std::thread::park();
            }
        }
    }

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::current_dir().unwrap().join(format!(
                "journal-crash-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            }
            Self(path)
        }

        fn central(&self) -> PathBuf {
            self.0.join("central.db")
        }

        fn open(&self) -> Result<Database, StorageError> {
            Database::open_protected(self.central(), self.0.join("audit.db"))
        }

        fn scratch(&self) -> PathBuf {
            scratch_path(&self.central())
        }

        fn crash_events(&self) -> i64 {
            Connection::open(self.0.join("audit.db"))
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM recovery_events WHERE kind='crash-recovered'",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        }

        fn retained(&self, database: &Database) -> bool {
            database
                .connect_read_only()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM spaces WHERE id='retained')",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        }

        /// Every durable top-level artifact except the owner lock, which
        /// carries only a kernel lock rather than content.
        fn evidence(&self) -> Vec<(String, u64, String)> {
            use sha2::{Digest, Sha256};
            let mut files = fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.is_file())
                .filter(|path| path.extension().is_none_or(|ext| ext != "recovery-lock"))
                .map(|path| {
                    let bytes = fs::read(&path).unwrap();
                    (
                        path.file_name().unwrap().to_string_lossy().into_owned(),
                        bytes.len() as u64,
                        format!("{:x}", Sha256::digest(&bytes)),
                    )
                })
                .collect::<Vec<_>>();
            files.sort();
            files
        }

        fn child(&self, test: &str, boundary: Option<&str>) -> Owner {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", test, "--nocapture"])
                .env("JOURNAL_CRASH_RECOVERY_ROOT", &self.0);
            match boundary {
                Some(stage) => command.env("JOURNAL_CRASH_RECOVERY_BOUNDARY", stage),
                None => command.env_remove("JOURNAL_CRASH_RECOVERY_BOUNDARY"),
            };
            let mut owner = Owner(Some(command.spawn().unwrap()));
            let deadline = Instant::now() + Duration::from_secs(20);
            while !self.0.join("ready").exists() {
                let child = owner.0.as_mut().unwrap();
                assert!(child.try_wait().unwrap().is_none(), "{test} exited early");
                assert!(
                    Instant::now() < deadline,
                    "{test} did not reach its boundary"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            fs::remove_file(self.0.join("ready")).unwrap();
            owner
        }

        /// A daemon that commits audited writes, then stops at `stage`.
        fn crashing_owner(&self, stage: &str) -> Owner {
            self.child(
                "crash_recovery::tests::crash_recovery_child",
                Some(&format!("owner-{stage}")),
            )
        }

        /// Protected startup recovering a crash, killed at `stage`.
        fn interrupted_recovery(&self, stage: &str) {
            self.child(
                "crash_recovery::tests::interrupted_recovery_child",
                Some(stage),
            )
            .kill();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Kills its process on drop so a failed assertion cannot orphan it.
    struct Owner(Option<Child>);

    impl Owner {
        fn kill(mut self) {
            let mut child = self.0.take().unwrap();
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            if let Some(child) = self.0.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    #[test]
    fn crash_recovery_child() {
        let Some(root) = std::env::var_os("JOURNAL_CRASH_RECOVERY_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let stage = std::env::var("JOURNAL_CRASH_RECOVERY_BOUNDARY").unwrap();
        let database =
            Database::open_protected(root.join("central.db"), root.join("audit.db")).unwrap();
        let create_space = |id: &str| {
            database
                .with_transaction(|transaction| {
                    transaction.execute(
                        "INSERT INTO spaces(id,name,access,created_at)
                         VALUES(?1,?1,'public','2026-01-01T00:00:00Z')",
                        [id],
                    )?;
                    Ok::<_, StorageError>(())
                })
                .unwrap()
        };
        // The first writer switches the file to WAL mode. A reader opened after
        // that keeps later commits in the WAL, as concurrent requests do.
        create_space("checkpointed");
        let _reader = database.connect_read_only().unwrap();
        create_space("retained");
        if stage == "owner-prepared" {
            let audit = database.recovery_audit().unwrap();
            let _guard = audit.lock().unwrap();
            let mut connection = database.connect_unchecked().unwrap();
            let transaction = connection.transaction().unwrap();
            transaction
                .execute(
                    "INSERT INTO spaces(id,name,access,created_at)
                     VALUES('uncertain','Uncertain','public','2026-01-01T00:00:00Z')",
                    [],
                )
                .unwrap();
            audit.prepare(&transaction).unwrap();
            fs::write(root.join("ready"), &stage).unwrap();
            loop {
                std::thread::park();
            }
        }
        fs::write(root.join("ready"), &stage).unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn interrupted_recovery_child() {
        let Some(root) = std::env::var_os("JOURNAL_CRASH_RECOVERY_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let _ = Database::open_protected(root.join("central.db"), root.join("audit.db"));
        panic!("crash recovery boundary was not reached");
    }

    #[test]
    fn abrupt_stop_is_replayed_only_after_audit_verification() {
        let fixture = Fixture::new();
        let owner = fixture.crashing_owner("committed");
        let live = fixture.evidence();
        assert!(
            fixture.open().is_err(),
            "a live owner's state is not a crash"
        );
        assert_eq!(fixture.evidence(), live);
        owner.kill();

        let wal = sidecar(&fixture.central(), "-wal");
        assert!(
            fs::metadata(&wal).unwrap().len() > 0,
            "committed frames stay in the WAL"
        );
        assert!(matches!(
            Database::open(fixture.central()),
            Err(StorageError::ResetRequired { .. })
        ));
        // A copy this procedure made and abandoned before touching the original.
        fs::create_dir(fixture.scratch()).unwrap();
        fs::write(fixture.scratch().join(PROVENANCE), PROVENANCE).unwrap();
        fs::write(fixture.scratch().join(COPY), b"partial copy").unwrap();

        let database = fixture.open().unwrap();
        let revision: i64 = database
            .connect_read_only()
            .unwrap()
            .query_row("SELECT revision FROM recovery_anchor", [], |row| row.get(0))
            .unwrap();
        assert_eq!(database.crash_recovered_revision(), Some(revision));
        assert!(
            fixture.retained(&database),
            "the commit held only in the WAL must survive"
        );
        let evidence: String = Connection::open(fixture.0.join("audit.db"))
            .unwrap()
            .query_row(
                "SELECT detail FROM recovery_events WHERE kind='crash-recovered'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(evidence.contains(&format!("\"audit_revision\":{revision}")));
        assert!(evidence.contains("\"recovery_id\":"));
        assert!(!fixture.scratch().exists());

        database.normalize_for_clean_shutdown().unwrap();
        drop(database);
        assert_eq!(fixture.open().unwrap().crash_recovered_revision(), None);
    }

    #[test]
    fn interrupted_in_place_recovery_resumes_before_admission() {
        for stage in ["verified", "normalized"] {
            let fixture = Fixture::new();
            fixture.crashing_owner("committed").kill();
            fixture.interrupted_recovery(stage);
            assert!(pending(&fixture.central()).unwrap(), "{stage}");
            if stage == "normalized" {
                assert!(
                    !sqlite_sidecar_entries_exist(&fixture.central()).unwrap(),
                    "the original was replayed; only the marker demands completion"
                );
            }
            let database = fixture.open().unwrap();
            assert!(database.crash_recovered_revision().is_some(), "{stage}");
            assert!(fixture.retained(&database), "{stage}");
            assert_eq!(fixture.crash_events(), 1, "{stage}");
            assert!(!fixture.scratch().exists(), "{stage}");
        }
    }

    #[test]
    fn diverged_in_place_recovery_stays_refused() {
        let fixture = Fixture::new();
        fixture.crashing_owner("committed").kill();
        fixture.interrupted_recovery("verified");
        let marker = fixture.scratch().join(VERIFIED);
        let mut verified: Verified = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
        verified.verification.tables[0].sha256 = "0".repeat(64);
        fs::write(&marker, serde_json::to_vec(&verified).unwrap()).unwrap();
        for _ in 0..2 {
            assert!(matches!(
                fixture.open(),
                Err(StorageError::RecoveryClosed(_))
            ));
            assert!(marker.exists());
            assert!(fixture.scratch().join(COPY).exists());
        }
        assert_eq!(fixture.crash_events(), 0);
    }

    #[test]
    fn interrupted_recovery_setup_is_disposable() {
        for setup in ["empty", "staged-marker"] {
            let fixture = Fixture::new();
            fixture.crashing_owner("committed").kill();
            fs::create_dir(fixture.scratch()).unwrap();
            if setup == "staged-marker" {
                fs::write(fixture.scratch().join(PROVENANCE), b"").unwrap();
                fs::write(fixture.scratch().join(VERIFIED_STAGING), b"{\"trunc").unwrap();
            }
            let database = fixture.open().unwrap();
            assert!(database.crash_recovered_revision().is_some(), "{setup}");
            assert!(fixture.retained(&database), "{setup}");
            assert!(!fixture.scratch().exists(), "{setup}");
        }
    }

    #[test]
    fn bare_relative_paths_sync_the_current_directory() {
        assert_eq!(parent(Path::new("journal.db")), Path::new("."));
        assert_eq!(
            parent(Path::new("journal.db.crash-recovery")),
            Path::new(".")
        );
        sync_dir(parent(Path::new("journal.db"))).unwrap();
    }

    #[test]
    fn unrecognized_scratch_directory_is_refused_untouched() {
        let fixture = Fixture::new();
        fixture.crashing_owner("committed").kill();
        fs::create_dir(fixture.scratch()).unwrap();
        fs::write(fixture.scratch().join("operator-notes"), b"keep me").unwrap();
        let before = fixture.evidence();
        assert!(matches!(
            fixture.open(),
            Err(StorageError::RecoveryClosed(_))
        ));
        assert_eq!(fixture.evidence(), before);
        assert_eq!(
            fs::read(fixture.scratch().join("operator-notes")).unwrap(),
            b"keep me"
        );
    }

    #[test]
    fn uncertain_intent_and_lost_wal_are_refused_without_mutation() {
        let uncertain = Fixture::new();
        uncertain.crashing_owner("prepared").kill();
        let before = uncertain.evidence();
        assert!(matches!(
            uncertain.open(),
            Err(StorageError::RecoveryClosed(_))
        ));
        assert_eq!(uncertain.evidence(), before);
        assert!(!uncertain.scratch().exists());

        let rolled_back = Fixture::new();
        rolled_back.crashing_owner("committed").kill();
        fs::remove_file(sidecar(&rolled_back.central(), "-wal")).unwrap();
        let before = rolled_back.evidence();
        assert!(matches!(
            rolled_back.open(),
            Err(StorageError::RecoveryClosed(_))
        ));
        assert_eq!(rolled_back.evidence(), before);
        assert!(!rolled_back.scratch().exists());
    }

    #[cfg(unix)]
    #[test]
    fn aliased_sidecars_are_refused() {
        let fixture = Fixture::new();
        fixture.crashing_owner("committed").kill();
        let wal = sidecar(&fixture.central(), "-wal");
        let elsewhere = fixture.0.join("elsewhere-wal");
        fs::rename(&wal, &elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &wal).unwrap();
        assert!(matches!(
            fixture.open(),
            Err(StorageError::RecoveryClosed(_))
        ));
        assert!(fs::symlink_metadata(&wal).unwrap().file_type().is_symlink());
        fs::remove_file(&wal).unwrap();
        fs::hard_link(&elsewhere, &wal).unwrap();
        assert!(matches!(
            fixture.open(),
            Err(StorageError::RecoveryClosed(_))
        ));
        assert!(!fixture.scratch().exists());
    }
}
