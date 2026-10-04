//! File-spool runtime: durable JSON envelope handoff for a local reader.
//!
//! This is the supported path for a principal whose runtime cannot accept an
//! authenticated injection call (Hermes) and is not a Muse hook. A successful
//! `inject` means one private envelope file is durably published under a name
//! derived from the inbox item ID. It does not mean a model observed the file,
//! a chat turn started, or a downstream reader finished.
//!
//! The worker acknowledges only after `inject` returns. Publication fsyncs the
//! file and its directory before that return. An identical existing file is
//! replay, not a second handoff. A conflicting, unreadable, oversized or
//! symlink file fails closed and is not acknowledged. Staging names are not
//! published envelopes; readers must ignore them.
//!
//! There is no central spool and no exactly-once promise. If a reader removes
//! the published file before a crashed client acknowledges, restart publishes
//! it again. Readers that must suppress duplicate turns keep their own durable
//! seen-set on `dedupe_key` (the inbox item ID).

use journal_domain::{MAX_CONTENT_BYTES, MAX_IDENTIFIER_CHARS};
use journal_inbox_worker::{Envelope, Route, Runtime, RuntimeError, RuntimeResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub const STATUS: &str = "supported";

const MAX_LABEL_BYTES: usize = 255;
const MAX_SPOOL_FRAMING_BYTES: usize = 1024;
// `rendered` contains the raw body plus singly quoted metadata. The spool
// JSON quotes `body` once and `rendered` once more, so content is escaped at
// most twice and each rendered metadata scalar at most twice on top of the
// quote already inside `rendered`.
const MAX_SPOOL_BYTES: usize = 6 * MAX_CONTENT_BYTES
    + 6 * (MAX_CONTENT_BYTES + 8 * (6 * MAX_IDENTIFIER_CHARS + 2) + 512)
    + 9 * (6 * MAX_IDENTIFIER_CHARS + 2)
    + 6 * MAX_LABEL_BYTES
    + MAX_SPOOL_FRAMING_BYTES;
const STAGING_CREATE_ATTEMPTS: usize = 128;
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const SPOOL_EXTENSION: &str = ".envelope.json";
const SPOOL_FILE_PREFIX: &str = "aj-";

/// Synchronous publisher for one private envelope directory.
pub struct FileRuntime {
    spool_dir: PathBuf,
}

impl fmt::Debug for FileRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileRuntime")
            .field("spool_configured", &true)
            .finish()
    }
}

impl FileRuntime {
    /// `spool_dir` must already exist as a private directory. The path is not
    /// a route target and is never written into a journal record.
    pub fn new(spool_dir: &str) -> RuntimeResult<Self> {
        Ok(Self {
            spool_dir: validate_spool_dir(spool_dir)?,
        })
    }
}

impl Runtime for FileRuntime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> RuntimeResult<String> {
        if !route.enabled {
            return Err(RuntimeError::RuntimeRejected);
        }
        validate_label(&route.runtime_target)?;
        if rendered != envelope.render() {
            return Err(RuntimeError::RuntimeRejected);
        }
        let payload = SpoolEnvelope::new(&route.runtime_target, envelope, rendered);
        let bytes = serde_json::to_vec(&payload).map_err(|_| RuntimeError::RuntimeRejected)?;
        if bytes.len() > MAX_SPOOL_BYTES {
            return Err(RuntimeError::RuntimeRejected);
        }
        let file_name = spool_file_name(&envelope.inbox_item_id);
        let path = self.spool_dir.join(&file_name);
        match read_spool_file(&path) {
            Ok(existing) if existing == bytes => {
                sync_spool_file(&path)?;
                Ok(file_name)
            }
            Ok(_) => Err(RuntimeError::RuntimeRejected),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_spool_file(&path, &bytes).map(|()| file_name)
            }
            Err(_) => Err(RuntimeError::RuntimeRejected),
        }
    }
}

/// On-disk contract. Field order is fixed so exact-byte replay is stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SpoolEnvelope {
    version: u8,
    dedupe_key: String,
    runtime_target: String,
    inbox_item_id: String,
    record_id: String,
    space_id: String,
    from_principal: String,
    source_run: Option<String>,
    reply_to: Option<String>,
    addressed_to: String,
    routing_key: Option<String>,
    body: String,
    rendered: String,
    content_sha256: String,
}

impl SpoolEnvelope {
    fn new(runtime_target: &str, envelope: &Envelope, rendered: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(rendered.as_bytes());
        Self {
            version: 1,
            dedupe_key: envelope.inbox_item_id.clone(),
            runtime_target: runtime_target.to_owned(),
            inbox_item_id: envelope.inbox_item_id.clone(),
            record_id: envelope.record_id.clone(),
            space_id: envelope.space_id.clone(),
            from_principal: envelope.from_principal.clone(),
            source_run: envelope.source_run.clone(),
            reply_to: envelope.reply_to.clone(),
            addressed_to: envelope.addressed_to.clone(),
            routing_key: envelope.routing_key.clone(),
            body: envelope.body.clone(),
            rendered: rendered.to_owned(),
            content_sha256: hex_bytes(&hasher.finalize()),
        }
    }
}

fn spool_file_name(inbox_item_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(inbox_item_id.as_bytes());
    format!(
        "{SPOOL_FILE_PREFIX}{}{SPOOL_EXTENSION}",
        hex_bytes(&hasher.finalize())
    )
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn read_spool_file(path: &Path) -> std::io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_SPOOL_BYTES as u64
    {
        return Err(std::io::Error::other("invalid spool file"));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(MAX_SPOOL_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SPOOL_BYTES {
        return Err(std::io::Error::other("oversized spool file"));
    }
    Ok(bytes)
}

fn sync_spool_file(path: &Path) -> RuntimeResult<()> {
    let result = (|| -> std::io::Result<()> {
        fs::File::open(path)?.sync_all()?;
        fs::File::open(
            path.parent()
                .ok_or_else(|| std::io::Error::other("invalid spool path"))?,
        )?
        .sync_all()
    })();
    result.map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))
}

fn validate_spool_dir(spool_dir: &str) -> RuntimeResult<PathBuf> {
    if spool_dir.is_empty() || spool_dir.len() > 4096 {
        return Err(RuntimeError::RuntimeRejected);
    }
    let path = Path::new(spool_dir);
    if path.components().count() == 0 {
        return Err(RuntimeError::RuntimeRejected);
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| RuntimeError::RuntimeRejected)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(RuntimeError::RuntimeRejected);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RuntimeError::RuntimeRejected);
        }
    }
    Ok(path.to_path_buf())
}

/// Route labels are recorded inside the envelope. They are never path segments.
fn validate_label(label: &str) -> RuntimeResult<()> {
    if label.is_empty()
        || label.len() > MAX_LABEL_BYTES
        || label.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || label.contains("..")
        || label.contains(['/', '\\'])
        || (label.len() >= 2
            && label.as_bytes()[0].is_ascii_alphabetic()
            && label.as_bytes()[1] == b':')
    {
        return Err(RuntimeError::RuntimeRejected);
    }
    Ok(())
}

fn create_staging_file(path: &Path) -> std::io::Result<(PathBuf, fs::File)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = STAGING_SEQUENCE.fetch_add(STAGING_CREATE_ATTEMPTS as u64, Ordering::Relaxed);
    create_staging_file_at(path, timestamp, sequence)
}

fn create_staging_file_at(
    path: &Path,
    timestamp: u128,
    sequence: u64,
) -> std::io::Result<(PathBuf, fs::File)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| std::io::Error::other("invalid spool path"))?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    for offset in 0..STAGING_CREATE_ATTEMPTS {
        let sequence = sequence.wrapping_add(offset as u64);
        let staging = path.with_file_name(format!(
            ".{file_name}.tmp.{}.{timestamp}.{sequence}",
            std::process::id()
        ));
        match options.open(&staging) {
            Ok(file) => return Ok((staging, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other("staging candidates exhausted"))
}

fn write_spool_file(path: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let (staging, mut file) = create_staging_file(path)
        .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
        file.sync_all()
            .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
        #[cfg(test)]
        spool_boundary("staged");
        drop(file);
        match fs::hard_link(&staging, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_spool_file(path).map_err(|_| RuntimeError::RuntimeRejected)? != bytes {
                    return Err(RuntimeError::RuntimeRejected);
                }
                sync_spool_file(path)?;
            }
            Err(_) => {
                return Err(RuntimeError::RuntimeUnavailable(
                    "file spool unavailable".into(),
                ));
            }
        }
        #[cfg(test)]
        spool_boundary("published");
        fs::remove_file(&staging)
            .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
        let dir = fs::File::open(path.parent().unwrap_or(Path::new(".")))
            .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
        dir.sync_all()
            .map_err(|_| RuntimeError::RuntimeUnavailable("file spool unavailable".into()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(test)]
fn spool_boundary(stage: &str) {
    if std::env::var("FILE_SPOOL_STAGE").as_deref() != Ok(stage) {
        return;
    }
    let root = std::env::var("FILE_SPOOL_CRASH_ROOT").unwrap();
    let mut marker = fs::File::create(Path::new(&root).join("boundary")).unwrap();
    marker.write_all(stage.as_bytes()).unwrap();
    marker.sync_all().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn private_dir(name: &str) -> PathBuf {
        use std::os::unix::fs::DirBuilderExt;
        let dir = std::env::temp_dir().join(format!(
            "file-spool-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        dir
    }

    fn envelope(inbox_item_id: &str) -> Envelope {
        Envelope {
            record_id: "record-1".into(),
            inbox_item_id: inbox_item_id.into(),
            space_id: "space".into(),
            from_principal: "source".into(),
            source_run: None,
            reply_to: None,
            addressed_to: "destination".into(),
            routing_key: Some("default".into()),
            body: "body".into(),
        }
    }

    fn route() -> Route {
        Route {
            key: "default".into(),
            runtime_target: "totoro".into(),
            enabled: true,
        }
    }

    #[test]
    fn fixed_spool_framing_fits_the_derived_bound() {
        let item = Envelope {
            inbox_item_id: String::new(),
            record_id: String::new(),
            space_id: String::new(),
            from_principal: String::new(),
            source_run: None,
            reply_to: None,
            addressed_to: String::new(),
            routing_key: None,
            body: String::new(),
        };
        let bytes = serde_json::to_vec(&SpoolEnvelope::new("", &item, &item.render())).unwrap();
        assert!(
            bytes.len() <= MAX_SPOOL_FRAMING_BYTES,
            "framing {} exceeds {}",
            bytes.len(),
            MAX_SPOOL_FRAMING_BYTES
        );
    }

    #[test]
    fn spool_file_name_is_stable_and_hides_the_item_id() {
        let first = spool_file_name("item-42");
        assert_eq!(first, spool_file_name("item-42"));
        assert!(first.starts_with(SPOOL_FILE_PREFIX));
        assert!(first.ends_with(SPOOL_EXTENSION));
        assert_ne!(first, spool_file_name("item-43"));
        let hostile = spool_file_name("../../etc/passwd");
        assert!(!hostile.contains('/'));
        assert!(!hostile.contains(".."));
        assert!(!first.contains("item-42"));
    }

    #[test]
    fn labels_are_explicit_and_not_paths() {
        assert!(validate_label("totoro").is_ok());
        assert!(validate_label("").is_err());
        assert!(validate_label("../escape").is_err());
        assert!(validate_label("chat\n1").is_err());
        assert!(validate_label("a:/b").is_err());
        assert!(validate_label("chat/1").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn world_writable_spool_is_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let dir = private_dir("open");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(FileRuntime::new(dir.to_str().unwrap()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn maximum_content_and_metadata_fit_without_truncation() {
        let dir = private_dir("metadata-boundary");
        let metadata = "\u{0001}".repeat(MAX_IDENTIFIER_CHARS);
        let item = Envelope {
            inbox_item_id: format!("0{}", "\u{0001}".repeat(MAX_IDENTIFIER_CHARS - 1)),
            record_id: metadata.clone(),
            space_id: metadata.clone(),
            from_principal: metadata.clone(),
            source_run: Some(metadata.clone()),
            reply_to: Some(metadata.clone()),
            addressed_to: metadata.clone(),
            routing_key: Some(metadata),
            body: "\u{0001}".repeat(MAX_CONTENT_BYTES),
        };
        let mut target = route();
        target.runtime_target = "\"".repeat(MAX_LABEL_BYTES);
        let bytes = serde_json::to_vec(&SpoolEnvelope::new(
            &target.runtime_target,
            &item,
            &item.render(),
        ))
        .unwrap();
        assert!(bytes.len() <= MAX_SPOOL_BYTES, "{}", bytes.len());
        let runtime = FileRuntime::new(dir.to_str().unwrap()).unwrap();
        let receipt = runtime.inject(&target, &item, &item.render()).unwrap();
        let written = fs::read(dir.join(&receipt)).unwrap();
        assert_eq!(written, bytes);
        let decoded: SpoolEnvelope = serde_json::from_slice(&written).unwrap();
        assert_eq!(decoded.body.len(), MAX_CONTENT_BYTES);
        assert_eq!(decoded.rendered, item.render());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spool_crash_child() {
        let Ok(root) = std::env::var("FILE_SPOOL_CRASH_ROOT") else {
            return;
        };
        let runtime = FileRuntime::new(Path::new(&root).join("spool").to_str().unwrap()).unwrap();
        let item = envelope("item-crash");
        runtime.inject(&route(), &item, &item.render()).unwrap();
        panic!("child missed the durable boundary");
    }

    #[cfg(unix)]
    #[test]
    fn killed_publication_replays_only_the_stable_durable_file() {
        use std::{
            os::unix::fs::PermissionsExt,
            process::Command,
            time::{Duration, Instant},
        };
        for stage in ["staged", "published"] {
            let root = std::env::temp_dir().join(format!(
                "file-spool-crash-{}-{stage}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let spool = root.join("spool");
            fs::create_dir_all(&spool).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&spool, fs::Permissions::from_mode(0o700)).unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "tests::spool_crash_child", "--nocapture"])
                .env("FILE_SPOOL_STAGE", stage)
                .env("FILE_SPOOL_CRASH_ROOT", &root)
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !root.join("boundary").exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before publication boundary"
                );
                if Instant::now() > deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("publication boundary deadline exceeded");
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            let path = spool.join(spool_file_name("item-crash"));
            assert_eq!(path.exists(), stage == "published");
            let orphans: Vec<_> = fs::read_dir(&spool)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|entry| entry != &path)
                .map(|entry| (entry.clone(), fs::read(&entry).unwrap()))
                .collect();
            assert_eq!(orphans.len(), 1, "killed attempt leaves its staging file");
            let runtime = FileRuntime::new(spool.to_str().unwrap()).unwrap();
            let item = envelope("item-crash");
            let receipt = runtime.inject(&route(), &item, &item.render()).unwrap();
            let bytes = fs::read(&path).unwrap();
            assert_eq!(
                runtime.inject(&route(), &item, &item.render()).unwrap(),
                receipt
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
            for (orphan, evidence) in orphans {
                assert_eq!(fs::read(orphan).unwrap(), evidence);
            }
            fs::remove_dir_all(root).unwrap();
        }
    }
}
