//! Drop-point contract tests: the exact on-disk behavior the hook worker
//! relies on. These run against real temporary directories, not mocks.

use journal_adapter_core::{CoreError, Envelope, Route, Runtime};
use journal_runtime_muse::{MuseRuntime, STATUS};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;

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
        body: "hello from the journal".into(),
    }
}

fn route(target: &str) -> Route {
    Route {
        key: "default".into(),
        runtime_target: target.into(),
        enabled: true,
    }
}

fn private_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "muse-drop-contract-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create drop dir");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .expect("private drop dir");
    dir
}

#[test]
fn status_is_supported_and_constructor_validates_the_drop_point() {
    assert_eq!(STATUS, "supported");
    let dir = private_dir("ok");
    assert!(MuseRuntime::new(dir.to_str().unwrap()).is_ok());

    // Missing directory.
    let missing = dir.join("nope");
    assert!(matches!(
        MuseRuntime::new(missing.to_str().unwrap()),
        Err(CoreError::RuntimeRejected)
    ));

    // Regular file instead of directory.
    let file = dir.join("file");
    std::fs::write(&file, b"x").unwrap();
    assert!(matches!(
        MuseRuntime::new(file.to_str().unwrap()),
        Err(CoreError::RuntimeRejected)
    ));

    // Symlink is rejected.
    let link = dir.join("link");
    std::os::unix::fs::symlink(&dir, &link).unwrap();
    assert!(matches!(
        MuseRuntime::new(link.to_str().unwrap()),
        Err(CoreError::RuntimeRejected)
    ));

    // Non-private directory is rejected.
    let open = private_dir("open");
    std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(matches!(
        MuseRuntime::new(open.to_str().unwrap()),
        Err(CoreError::RuntimeRejected)
    ));

    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&open).ok();
}

#[test]
fn accepted_injection_writes_the_documented_payload() {
    let dir = private_dir("accept");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    let body = envelope("attempt-1");
    let rendered = body.render();
    let receipt = runtime
        .inject(&route("main"), &body, &rendered)
        .expect("inject");

    assert!(receipt.starts_with("muse-"));
    assert!(receipt.ends_with(".drop.json"));
    let path = dir.join(&receipt);
    let bytes = std::fs::read(&path).expect("drop file");
    let value: Value = serde_json::from_slice(&bytes).expect("drop JSON");
    assert_eq!(value["version"], 1);
    assert_eq!(value["dedupe_key"], "attempt-1");
    assert_eq!(value["target_chat"], "main");
    assert_eq!(value["record_id"], "record-1");
    assert_eq!(value["mailbox_item_id"], "item-1");
    assert_eq!(value["attempt_id"], "attempt-1");
    assert_eq!(value["space_id"], "space");
    assert_eq!(value["from_principal"], "source");
    assert_eq!(value["addressed_to"], "destination");
    assert_eq!(value["routing_key"], "default");
    assert_eq!(value["body"], rendered);
    assert!(value["content_sha256"].as_str().is_some());
    // No staging files leak into the watched directory.
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(entries.len(), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn exact_replay_returns_the_same_receipt_without_rewriting() {
    let dir = private_dir("replay");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    let body = envelope("attempt-replay");
    let rendered = body.render();
    let first = runtime
        .inject(&route("chat-abc"), &body, &rendered)
        .expect("first inject");
    let path = dir.join(&first);
    let before = std::fs::read(&path).expect("drop bytes");
    let modified = std::fs::metadata(&path)
        .expect("metadata")
        .modified()
        .unwrap();

    let second = runtime
        .inject(&route("chat-abc"), &body, &rendered)
        .expect("replay inject");
    assert_eq!(first, second);
    assert_eq!(std::fs::read(&path).expect("drop bytes again"), before);
    assert_eq!(
        std::fs::metadata(&path)
            .expect("metadata again")
            .modified()
            .unwrap(),
        modified,
        "replay must not rewrite the drop file"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn conflicting_payload_at_the_stable_name_fails_closed() {
    let dir = private_dir("conflict");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    let body = envelope("attempt-conflict");
    let rendered = body.render();
    let receipt = runtime
        .inject(&route("main"), &body, &rendered)
        .expect("inject");
    // A different payload now occupies the stable name (simulates a binding
    // collision, never a legitimate state).
    std::fs::write(dir.join(&receipt), b"{\"version\":1,\"tampered\":true}").unwrap();

    assert_eq!(
        runtime.inject(&route("main"), &body, &rendered),
        Err(CoreError::RuntimeRejected)
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn unreadable_existing_file_is_ambiguous_and_fails_closed() {
    let dir = private_dir("ambiguous");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    let body = envelope("attempt-ambiguous");
    let rendered = body.render();
    let receipt = runtime
        .inject(&route("main"), &body, &rendered)
        .expect("inject");
    // Garbage bytes: the file exists but its binding cannot be established.
    // The adapter must not blindly overwrite it.
    std::fs::write(dir.join(&receipt), b"\x00\x01not-json").unwrap();

    assert_eq!(
        runtime.inject(&route("main"), &body, &rendered),
        Err(CoreError::RuntimeRejected)
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn vanished_drop_dir_is_retryable_not_terminal() {
    let dir = private_dir("vanished");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    std::fs::remove_dir_all(&dir).expect("remove drop dir");
    let body = envelope("attempt-gone");
    let rendered = body.render();

    assert!(matches!(
        runtime.inject(&route("main"), &body, &rendered),
        Err(CoreError::RuntimeUnavailable(_))
    ));
}

#[test]
fn malformed_routes_are_rejected_before_any_write() {
    let dir = private_dir("routes");
    let runtime = MuseRuntime::new(dir.to_str().unwrap()).expect("runtime");
    let body = envelope("attempt-routes");
    let rendered = body.render();

    let mut disabled = route("main");
    disabled.enabled = false;
    assert_eq!(
        runtime.inject(&disabled, &body, &rendered),
        Err(CoreError::RuntimeRejected)
    );
    for bad in ["", "../escape", "chat/1", "a:/b", "chat\n1"] {
        assert_eq!(
            runtime.inject(&route(bad), &body, &rendered),
            Err(CoreError::RuntimeRejected),
            "chat id rejected: {bad:?}"
        );
    }
    // Mismatched rendered text is never injected.
    assert_eq!(
        runtime.inject(&route("main"), &body, "different text"),
        Err(CoreError::RuntimeRejected)
    );
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        0,
        "no drop file may exist after rejections"
    );

    std::fs::remove_dir_all(&dir).ok();
}
