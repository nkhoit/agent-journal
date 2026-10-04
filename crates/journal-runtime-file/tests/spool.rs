use journal_inbox_worker::{Envelope, Route, Runtime, RuntimeError};
use journal_runtime_file::FileRuntime;
use std::path::PathBuf;

fn private_dir(name: &str) -> PathBuf {
    use std::os::unix::fs::DirBuilderExt;
    let dir = std::env::temp_dir().join(format!(
        "file-spool-test-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
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

fn route(label: &str) -> Route {
    Route {
        key: "default".into(),
        runtime_target: label.into(),
        enabled: true,
    }
}

#[test]
fn exact_replay_returns_the_same_receipt_without_rewriting() {
    let dir = private_dir("replay");
    let runtime = FileRuntime::new(dir.to_str().unwrap()).unwrap();
    let body = envelope("item-replay");
    let rendered = body.render();
    let first = runtime
        .inject(&route("totoro"), &body, &rendered)
        .expect("first inject");
    let path = dir.join(&first);
    let before = std::fs::read(&path).unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let second = runtime
        .inject(&route("totoro"), &body, &rendered)
        .expect("replay");
    assert_eq!(first, second);
    assert!(first.starts_with("aj-") && first.ends_with(".envelope.json"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        modified
    );
    let value: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(value["version"], 1);
    assert_eq!(value["dedupe_key"], "item-replay");
    assert_eq!(value["runtime_target"], "totoro");
    assert_eq!(value["body"], "body");
    assert!(value["rendered"].as_str().unwrap().contains("body"));
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn conflicting_payload_at_the_stable_name_fails_closed() {
    let dir = private_dir("conflict");
    let runtime = FileRuntime::new(dir.to_str().unwrap()).unwrap();
    let body = envelope("item-conflict");
    let rendered = body.render();
    let receipt = runtime.inject(&route("totoro"), &body, &rendered).unwrap();
    std::fs::write(dir.join(&receipt), b"{\"version\":1,\"tampered\":true}").unwrap();
    assert_eq!(
        runtime.inject(&route("totoro"), &body, &rendered),
        Err(RuntimeError::RuntimeRejected)
    );
    assert_eq!(
        std::fs::read(dir.join(&receipt)).unwrap(),
        b"{\"version\":1,\"tampered\":true}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn identical_payload_through_a_symlink_is_not_handoff_evidence() {
    let dir = private_dir("symlink");
    let runtime = FileRuntime::new(dir.to_str().unwrap()).unwrap();
    let body = envelope("item-symlink");
    let receipt = runtime
        .inject(&route("totoro"), &body, &body.render())
        .unwrap();
    let published = dir.join(&receipt);
    let moved = dir.join("elsewhere.json");
    std::fs::rename(&published, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &published).unwrap();
    let result = runtime.inject(&route("totoro"), &body, &body.render());
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(result, Err(RuntimeError::RuntimeRejected));
}

#[test]
fn rejected_routes_write_nothing() {
    let dir = private_dir("reject");
    let runtime = FileRuntime::new(dir.to_str().unwrap()).unwrap();
    let body = envelope("item-reject");
    let rendered = body.render();
    let mut disabled = route("totoro");
    disabled.enabled = false;
    assert_eq!(
        runtime.inject(&disabled, &body, &rendered),
        Err(RuntimeError::RuntimeRejected)
    );
    assert_eq!(
        runtime.inject(&route("../nope"), &body, &rendered),
        Err(RuntimeError::RuntimeRejected)
    );
    assert_eq!(
        runtime.inject(&route("totoro"), &body, "not the rendered envelope"),
        Err(RuntimeError::RuntimeRejected)
    );
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(dir).unwrap();
}
