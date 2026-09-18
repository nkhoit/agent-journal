use journal_adapter_core::{Envelope, Route, Runtime};
use journal_runtime_fake::FakeRuntime;

fn delivery() -> (Route, Envelope) {
    let route = Route {
        key: "default".into(),
        enabled: true,
        runtime_target: "local-test-route".into(),
    };
    let envelope = serde_json::from_value(serde_json::json!({
        "record_id": "record", "space_id": "space", "from_principal": "source",
        "addressed_to": "destination", "mailbox_item_id": "item", "attempt_id": "attempt",
        "body": "untrusted\nbody", "source_run": null
    }))
    .unwrap();
    (route, envelope)
}

#[test]
fn availability_duplicates_and_exact_capture() {
    let runtime = FakeRuntime::default();
    let (route, envelope) = delivery();
    let rendered = envelope.render();
    runtime.set_available(false);
    assert!(runtime.inject(&route, &envelope, &rendered).is_err());
    assert!(runtime.acceptances().is_empty());
    runtime.set_available(true);
    let first = runtime.inject(&route, &envelope, &rendered).unwrap();
    assert_eq!(runtime.inject(&route, &envelope, &rendered).unwrap(), first);
    let accepted = runtime.acceptances();
    assert_eq!(accepted.len(), 2);
    assert_eq!(accepted[0], accepted[1]);
    assert_eq!(accepted[0].route, route);
    assert_eq!(accepted[0].envelope, envelope);
    assert_eq!(accepted[0].rendered, rendered);
}

#[test]
fn rejects_unresolved_routes_and_changed_rendering() {
    let runtime = FakeRuntime::default();
    let (mut route, envelope) = delivery();
    assert!(runtime.inject(&route, &envelope, "changed").is_err());
    route.enabled = false;
    assert!(
        runtime
            .inject(&route, &envelope, &envelope.render())
            .is_err()
    );
    route.enabled = true;
    route.runtime_target.clear();
    assert!(
        runtime
            .inject(&route, &envelope, &envelope.render())
            .is_err()
    );
    assert!(runtime.acceptances().is_empty());
}

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("FAKE_RUNTIME_CRASH_PATH") else {
        return;
    };
    let runtime = FakeRuntime::durable(path).unwrap();
    runtime.set_accept_then_crash(true);
    let (route, envelope) = delivery();
    runtime
        .inject(&route, &envelope, &envelope.render())
        .unwrap();
    panic!("accept-then-crash returned");
}

#[test]
fn acceptance_survives_child_termination_and_duplicate_replay() {
    let directory = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("fake-runtime-crash-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("acceptances.jsonl");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child"])
        .env("FAKE_RUNTIME_CRASH_PATH", &path)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("fake runtime child timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    assert_eq!(
        status.code(),
        Some(journal_runtime_fake::ACCEPT_THEN_CRASH_EXIT)
    );
    let runtime = FakeRuntime::durable(&path).unwrap();
    assert_eq!(runtime.acceptances().len(), 1);
    let (route, envelope) = delivery();
    runtime
        .inject(&route, &envelope, &envelope.render())
        .unwrap();
    assert_eq!(runtime.acceptances().len(), 2);
    assert_eq!(runtime.acceptances()[0], runtime.acceptances()[1]);
    std::fs::remove_dir_all(directory).unwrap();
}
