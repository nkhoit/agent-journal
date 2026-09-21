//! Muse runtime boundary: hook drop-point handoff.
//!
//! The Muse personal-agent runtime exposes no authenticated injection API to
//! local processes. Verified 2026-09-18 against Agent Kit v0.1.6: `muse.py
//! --help` lists only Hindsight/Zulip client actions (`post`, `reply`,
//! `inbox`, `ack`, `retain`, `recall`, `get-document`, `memory-status`, …);
//! there is no chat-injection subcommand, webhook, or socket. The supported
//! handoff is a private local drop directory watched by a platform hook: the
//! client durably writes one file per inbox item, and the operator's
//! hook worker picks the file up and calls `chat.send_message`.
//!
//! A successful `inject` means the drop file is durably on disk in the watched
//! directory under a stable item-derived name. It does NOT mean the model
//! observed, understood, or completed the delivery. The platform offers no
//! idempotency key, so duplicate chat turns remain possible if the hook worker
//! redelivers; the drop payload therefore carries a stable `dedupe_key`
//! (the inbox item ID) and the deployment must run a worker that keeps a durable
//! seen-set on that key. Existing identical files replay without rewriting;
//! consumed files may be recreated after a crash before acknowledgment.

use journal_inbox_worker::{Envelope, Route, Runtime, RuntimeError, RuntimeResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub const STATUS: &str = "supported";

/// Maximum serialized drop payload: 64 KiB record body plus envelope metadata
/// and JSON framing, with headroom. Larger payloads are rejected, not
/// truncated.
const MAX_DROP_BYTES: usize = 256 * 1024;
const DROP_EXTENSION: &str = ".drop.json";
const DROP_FILE_PREFIX: &str = "muse-";

/// A synchronous, narrow client for the Muse hook drop-point.
///
/// Construction validates the drop directory. There is no API key: the
/// private directory's filesystem permissions are the access control, and the
/// directory path itself is the only retained configuration.
pub struct MuseRuntime {
    drop_dir: PathBuf,
}

impl fmt::Debug for MuseRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MuseRuntime")
            .field("drop_point_configured", &true)
            .finish()
    }
}

impl MuseRuntime {
    /// Build a runtime client over an existing private drop directory.
    pub fn new(drop_dir: &str) -> RuntimeResult<Self> {
        let drop_dir = validate_drop_dir(drop_dir)?;
        Ok(Self { drop_dir })
    }
}

impl Runtime for MuseRuntime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> RuntimeResult<String> {
        if !route.enabled {
            return Err(RuntimeError::RuntimeRejected);
        }
        validate_chat_id(&route.runtime_target)?;
        // The route target is the private Muse chat id. It is never placed in
        // central records; it is sent only to the local
        // drop point for the hook worker.
        if rendered != envelope.render() {
            return Err(RuntimeError::RuntimeRejected);
        }
        let payload = DropPayload::new(&route.runtime_target, envelope, rendered);
        let bytes = serde_json::to_vec(&payload).map_err(|_| RuntimeError::RuntimeRejected)?;
        if bytes.len() > MAX_DROP_BYTES {
            return Err(RuntimeError::RuntimeRejected);
        }
        let file_name = drop_file_name(&envelope.inbox_item_id);
        let path = self.drop_dir.join(&file_name);
        match read_drop_file(&path) {
            Ok(existing) => {
                // Exact replay: the same inbox item already dropped this exact
                // payload. Return the same receipt without touching the file.
                if existing == bytes {
                    sync_drop_file(&path)?;
                    Ok(file_name)
                } else {
                    // A different payload already occupies this inbox item's
                    // stable name. Fail closed rather than overwrite it.
                    Err(RuntimeError::RuntimeRejected)
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_drop_file(&path, &bytes).map(|()| file_name)
            }
            Err(_) => {
                // The file exists but cannot be read: its binding is
                // ambiguous. Never blindly overwrite; fail closed.
                Err(RuntimeError::RuntimeRejected)
            }
        }
    }
}

/// The exact on-disk contract consumed by the hook worker. Field order is
/// fixed by struct serialization so replay byte-comparison is stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DropPayload {
    version: u8,
    dedupe_key: String,
    target_chat: String,
    record_id: String,
    inbox_item_id: String,
    space_id: String,
    from_principal: String,
    addressed_to: String,
    routing_key: Option<String>,
    body: String,
    content_sha256: String,
}

impl DropPayload {
    fn new(target_chat: &str, envelope: &Envelope, rendered: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(rendered.as_bytes());
        let content_sha256 = hex_bytes(&hasher.finalize());
        Self {
            version: 2,
            dedupe_key: envelope.inbox_item_id.clone(),
            target_chat: target_chat.to_owned(),
            record_id: envelope.record_id.clone(),
            inbox_item_id: envelope.inbox_item_id.clone(),
            space_id: envelope.space_id.clone(),
            from_principal: envelope.from_principal.clone(),
            addressed_to: envelope.addressed_to.clone(),
            routing_key: envelope.routing_key.clone(),
            body: rendered.to_owned(),
            content_sha256,
        }
    }
}

fn drop_file_name(inbox_item_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(inbox_item_id.as_bytes());
    format!(
        "{DROP_FILE_PREFIX}{}{DROP_EXTENSION}",
        hex_bytes(&hasher.finalize())
    )
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn read_drop_file(path: &Path) -> std::io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_DROP_BYTES as u64
    {
        return Err(std::io::Error::other("invalid drop file"));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(MAX_DROP_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_DROP_BYTES {
        return Err(std::io::Error::other("oversized drop file"));
    }
    Ok(bytes)
}

fn sync_drop_file(path: &Path) -> RuntimeResult<()> {
    let result = (|| -> std::io::Result<()> {
        fs::File::open(path)?.sync_all()?;
        fs::File::open(
            path.parent()
                .ok_or_else(|| std::io::Error::other("invalid drop path"))?,
        )?
        .sync_all()
    })();
    result.map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))
}

fn validate_drop_dir(drop_dir: &str) -> RuntimeResult<PathBuf> {
    if drop_dir.is_empty() || drop_dir.len() > 4096 {
        return Err(RuntimeError::RuntimeRejected);
    }
    let path = Path::new(drop_dir);
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

/// Chat IDs are the platform's opaque identifiers (`main` or a side-chat id).
/// They must be path-safe because they are embedded in a local JSON payload
/// consumed by a shell-adjacent worker; the worker must still treat the value
/// as data.
fn validate_chat_id(chat_id: &str) -> RuntimeResult<()> {
    if chat_id.is_empty()
        || chat_id.len() > 255
        || chat_id.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || chat_id.contains("..")
        || chat_id.contains(['/', '\\'])
        || (chat_id.len() >= 2
            && chat_id.as_bytes()[0].is_ascii_alphabetic()
            && chat_id.as_bytes()[1] == b':')
    {
        return Err(RuntimeError::RuntimeRejected);
    }
    Ok(())
}

/// Durably create the drop file: exclusive create, write, fsync the file,
/// no-clobber publication, fsync the directory.
fn write_drop_file(path: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(RuntimeError::RuntimeRejected)?;
    let staging = path.with_file_name(format!(".{file_name}.tmp.{}", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&staging)
        .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        file.sync_all()
            .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        #[cfg(test)]
        drop_boundary("staged");
        drop(file);
        match fs::hard_link(&staging, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_drop_file(path).map_err(|_| RuntimeError::RuntimeRejected)? != bytes {
                    return Err(RuntimeError::RuntimeRejected);
                }
                sync_drop_file(path)?;
            }
            Err(_) => {
                return Err(RuntimeError::RuntimeUnavailable(
                    "muse drop point unavailable".into(),
                ));
            }
        }
        #[cfg(test)]
        drop_boundary("published");
        fs::remove_file(&staging)
            .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        let dir = fs::File::open(path.parent().unwrap_or(Path::new(".")))
            .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        dir.sync_all()
            .map_err(|_| RuntimeError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(test)]
fn drop_boundary(stage: &str) {
    if std::env::var("MUSE_DROP_STAGE").as_deref() != Ok(stage) {
        return;
    }
    let root = std::env::var("MUSE_CRASH_ROOT").unwrap();
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
    #[test]
    fn drop_crash_child() {
        let Ok(root) = std::env::var("MUSE_CRASH_ROOT") else {
            return;
        };
        let runtime = MuseRuntime::new(Path::new(&root).join("drop").to_str().unwrap()).unwrap();
        let item = envelope("item-crash");
        runtime.inject(&route(), &item, &item.render()).unwrap();
        panic!("child missed the durable boundary");
    }

    #[cfg(unix)]
    #[test]
    fn killed_publication_replays_only_the_stable_durable_drop() {
        use std::{
            os::unix::fs::PermissionsExt,
            process::Command,
            time::{Duration, Instant},
        };
        for stage in ["staged", "published"] {
            let root = std::env::temp_dir().join(format!(
                "muse-crash-{}-{stage}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let drop_dir = root.join("drop");
            fs::create_dir_all(&drop_dir).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&drop_dir, fs::Permissions::from_mode(0o700)).unwrap();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "tests::drop_crash_child", "--nocapture"])
                .env("MUSE_DROP_STAGE", stage)
                .env("MUSE_CRASH_ROOT", &root)
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
            let path = drop_dir.join(drop_file_name("item-crash"));
            assert_eq!(path.exists(), stage == "published");
            let runtime = MuseRuntime::new(drop_dir.to_str().unwrap()).unwrap();
            let item = envelope("item-crash");
            let receipt = runtime.inject(&route(), &item, &item.render()).unwrap();
            let bytes = fs::read(&path).unwrap();
            assert_eq!(
                runtime.inject(&route(), &item, &item.render()).unwrap(),
                receipt
            );
            assert_eq!(fs::read(path).unwrap(), bytes);
            fs::remove_dir_all(root).unwrap();
        }
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
            runtime_target: "main".into(),
            enabled: true,
        }
    }

    #[test]
    fn drop_file_name_is_stable_and_filesystem_safe() {
        let first = drop_file_name("item-42");
        let second = drop_file_name("item-42");
        assert_eq!(first, second);
        assert!(first.starts_with(DROP_FILE_PREFIX));
        assert!(first.ends_with(DROP_EXTENSION));
        assert_ne!(first, drop_file_name("item-43"));
        // Attempt IDs are never embedded raw: path-unsafe input still yields a
        // safe name.
        let hostile = drop_file_name("../../etc/passwd");
        assert!(!hostile.contains('/'));
        assert!(!hostile.contains(".."));
    }

    #[test]
    fn receipts_are_bounded_and_non_secret() {
        let name = drop_file_name("item-1");
        assert!(name.len() < 4096);
        assert!(!name.contains("item-1"));
    }

    #[test]
    fn chat_ids_are_explicit_and_path_safe() {
        assert!(validate_chat_id("main").is_ok());
        assert!(validate_chat_id("01J9Z8X7Y6W5V4U3T2S1R").is_ok());
        assert!(validate_chat_id("").is_err());
        assert!(validate_chat_id("../escape").is_err());
        assert!(validate_chat_id("chat\n1").is_err());
        assert!(validate_chat_id("a:/b").is_err());
    }

    #[test]
    fn debug_output_carries_no_payload() {
        let dir = std::env::temp_dir().join(format!("muse-runtime-debug-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = MuseRuntime::new(dir.to_str().unwrap()).unwrap();
        let debug = format!("{runtime:?}");
        assert!(debug.contains("MuseRuntime"));
        assert!(!debug.contains("secret"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn drop_path_helper_matches_inject_target() {
        let runtime_dir =
            std::env::temp_dir().join(format!("muse-runtime-path-{}", std::process::id()));
        std::fs::create_dir_all(&runtime_dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = MuseRuntime::new(runtime_dir.to_str().unwrap()).unwrap();
        let body = envelope("item-9");
        let rendered = body.render();
        let receipt = runtime.inject(&route(), &body, &rendered).expect("inject");
        assert_eq!(receipt, drop_file_name("item-9"));
        std::fs::remove_dir_all(&runtime_dir).ok();
    }

    #[test]
    fn drop_payload_round_trips_through_documented_json() {
        let body = envelope("item-rt");
        let rendered = body.render();
        let payload = DropPayload::new("main", &body, &rendered);
        let bytes = serde_json::to_vec(&payload).expect("serialize");
        let decoded: DropPayload = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(payload, decoded);
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.dedupe_key, "item-rt");
        assert_eq!(decoded.body, rendered);
    }
}
