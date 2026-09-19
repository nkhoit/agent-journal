//! Muse runtime boundary: hook drop-point handoff.
//!
//! The Muse personal-agent runtime exposes no authenticated injection API to
//! local processes. Verified 2026-09-18 against Agent Kit v0.1.6: `muse.py
//! --help` lists only Hindsight/Zulip client actions (`post`, `reply`,
//! `inbox`, `ack`, `retain`, `recall`, `get-document`, `memory-status`, …);
//! there is no chat-injection subcommand, webhook, or socket. The supported
//! handoff is a private local drop directory watched by a platform hook: the
//! adapter durably writes one file per delivery attempt, and the operator's
//! hook worker picks the file up and calls `chat.send_message`.
//!
//! A successful `inject` means the drop file is durably on disk in the watched
//! directory under a stable attempt-derived name. It does NOT mean the model
//! observed, understood, or completed the delivery. The platform offers no
//! idempotency key, so duplicate chat turns remain possible if the hook worker
//! redelivers; the drop payload therefore carries a stable `dedupe_key`
//! (the attempt ID) and the deployment must run a worker that keeps a durable
//! seen-set on that key. The adapter itself never creates two drop files for
//! one attempt.

use journal_adapter_core::{CoreError, CoreResult, Envelope, Route, Runtime};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
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
            .field("drop_dir", &self.drop_dir)
            .finish()
    }
}

impl MuseRuntime {
    /// Build a runtime client over an existing private drop directory.
    pub fn new(drop_dir: &str) -> CoreResult<Self> {
        let drop_dir = validate_drop_dir(drop_dir)?;
        Ok(Self { drop_dir })
    }
}

impl Runtime for MuseRuntime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> CoreResult<String> {
        if !route.enabled {
            return Err(CoreError::RuntimeRejected);
        }
        validate_chat_id(&route.runtime_target)?;
        // The route target is the private Muse chat id. It is never placed in
        // the central envelope or telemetry; it is sent only to the local
        // drop point for the hook worker.
        if rendered != envelope.render() {
            return Err(CoreError::RuntimeRejected);
        }
        let payload = DropPayload::new(&route.runtime_target, envelope, rendered);
        let bytes = serde_json::to_vec(&payload).map_err(|_| CoreError::RuntimeRejected)?;
        if bytes.len() > MAX_DROP_BYTES {
            return Err(CoreError::RuntimeRejected);
        }
        let file_name = drop_file_name(&envelope.attempt_id);
        let path = self.drop_dir.join(&file_name);
        match fs::read(&path) {
            Ok(existing) => {
                // Exact replay: the same attempt already dropped this exact
                // payload. Return the same receipt without touching the file.
                if existing == bytes {
                    Ok(file_name)
                } else {
                    // A different payload already occupies this attempt's
                    // stable name. Fail closed rather than overwrite it.
                    Err(CoreError::RuntimeRejected)
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_drop_file(&path, &bytes).map(|()| file_name)
            }
            Err(_) => {
                // The file exists but cannot be read: its binding is
                // ambiguous. Never blindly overwrite; fail closed.
                Err(CoreError::RuntimeRejected)
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
    mailbox_item_id: String,
    attempt_id: String,
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
            version: 1,
            dedupe_key: envelope.attempt_id.clone(),
            target_chat: target_chat.to_owned(),
            record_id: envelope.record_id.clone(),
            mailbox_item_id: envelope.mailbox_item_id.clone(),
            attempt_id: envelope.attempt_id.clone(),
            space_id: envelope.space_id.clone(),
            from_principal: envelope.from_principal.clone(),
            addressed_to: envelope.addressed_to.clone(),
            routing_key: envelope.routing_key.clone(),
            body: rendered.to_owned(),
            content_sha256,
        }
    }
}

fn drop_file_name(attempt_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(attempt_id.as_bytes());
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

fn validate_drop_dir(drop_dir: &str) -> CoreResult<PathBuf> {
    if drop_dir.is_empty() || drop_dir.len() > 4096 {
        return Err(CoreError::RuntimeRejected);
    }
    let path = Path::new(drop_dir);
    if path.components().count() == 0 {
        return Err(CoreError::RuntimeRejected);
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| CoreError::RuntimeRejected)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(CoreError::RuntimeRejected);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(CoreError::RuntimeRejected);
        }
    }
    Ok(path.to_path_buf())
}

/// Chat IDs are the platform's opaque identifiers (`main` or a side-chat id).
/// They must be path-safe because they are embedded in a local JSON payload
/// consumed by a shell-adjacent worker; the worker must still treat the value
/// as data.
fn validate_chat_id(chat_id: &str) -> CoreResult<()> {
    if chat_id.is_empty()
        || chat_id.len() > 255
        || chat_id.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || chat_id.contains("..")
        || chat_id.contains(['/', '\\'])
        || (chat_id.len() >= 2
            && chat_id.as_bytes()[0].is_ascii_alphabetic()
            && chat_id.as_bytes()[1] == b':')
    {
        return Err(CoreError::RuntimeRejected);
    }
    Ok(())
}

/// Durably create the drop file: exclusive create, write, fsync the file,
/// atomic rename into place, fsync the directory.
fn write_drop_file(path: &Path, bytes: &[u8]) -> CoreResult<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(CoreError::RuntimeRejected)?;
    let staging = path.with_file_name(format!(".{file_name}.tmp.{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        file.sync_all()
            .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        drop(file);
        fs::rename(&staging, path)
            .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        let dir = fs::File::open(path.parent().unwrap_or(Path::new(".")))
            .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        dir.sync_all()
            .map_err(|_| CoreError::RuntimeUnavailable("muse drop point unavailable".into()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(attempt_id: &str) -> Envelope {
        Envelope {
            record_id: "record-1".into(),
            mailbox_item_id: "item-1".into(),
            attempt_id: attempt_id.into(),
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
        let first = drop_file_name("attempt-42");
        let second = drop_file_name("attempt-42");
        assert_eq!(first, second);
        assert!(first.starts_with(DROP_FILE_PREFIX));
        assert!(first.ends_with(DROP_EXTENSION));
        assert_ne!(first, drop_file_name("attempt-43"));
        // Attempt IDs are never embedded raw: path-unsafe input still yields a
        // safe name.
        let hostile = drop_file_name("../../etc/passwd");
        assert!(!hostile.contains('/'));
        assert!(!hostile.contains(".."));
    }

    #[test]
    fn receipts_are_bounded_and_non_secret() {
        let name = drop_file_name("attempt-1");
        assert!(name.len() < 4096);
        assert!(!name.contains("attempt-1"));
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
        let body = envelope("attempt-9");
        let rendered = body.render();
        let receipt = runtime.inject(&route(), &body, &rendered).expect("inject");
        assert_eq!(receipt, drop_file_name("attempt-9"));
        std::fs::remove_dir_all(&runtime_dir).ok();
    }

    #[test]
    fn drop_payload_round_trips_through_documented_json() {
        let body = envelope("attempt-rt");
        let rendered = body.render();
        let payload = DropPayload::new("main", &body, &rendered);
        let bytes = serde_json::to_vec(&payload).expect("serialize");
        let decoded: DropPayload = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(payload, decoded);
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.dedupe_key, "attempt-rt");
        assert_eq!(decoded.body, rendered);
    }
}
