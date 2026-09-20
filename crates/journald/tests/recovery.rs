#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use journal_protocol::SpaceCreateRequest;
use journal_service::BootstrapService;
use journal_storage_sqlite::{Database, RecoveryAudit};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "journal-protected-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }

    fn command(&self, verb: &str, extra: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_journal-recover"))
            .arg(verb)
            .arg(self.0.join("central.db"))
            .arg(self.0.join("audit.db"))
            .args(extra)
            .current_dir(&self.0)
            .output()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn service_and_operator_binary_share_lock_and_persistent_gate() {
    let fixture = Fixture::new();
    let database =
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db")).unwrap();
    let service = BootstrapService::new(database.clone());
    assert!(!fixture.command("close", &[]).status.success());
    database.recovery_audit().unwrap().close().unwrap();
    assert!(database.connect_read_only().is_err());
    assert!(
        service
            .create_space(&SpaceCreateRequest {
                id: "space-example".to_owned(),
                name: "Example".to_owned(),
            })
            .is_err()
    );
    drop(service);
    drop(database);
    assert!(
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db"),)
            .is_err()
    );
    assert!(
        !fixture
            .command("reconcile", &["approval.json"])
            .status
            .success()
    );
    let output = fixture.command("reconcile", &["approval.json", "--adapters-quiesced"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !fixture
            .command("reopen", &["approval.json"])
            .status
            .success()
    );
    let mut approval = RecoveryAudit::read_approval(&fixture.0.join("approval.json")).unwrap();
    approval.inventory_complete = true;
    approval.accepted_record_loss = true;
    RecoveryAudit::write_approval(&fixture.0.join("reviewed.json"), &approval).unwrap();
    let output = fixture.command("reopen", &["reviewed.json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let database =
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db")).unwrap();
    BootstrapService::new(database.clone())
        .create_space(&SpaceCreateRequest {
            id: "space-example".to_owned(),
            name: "Example".to_owned(),
        })
        .unwrap();
    assert!(
        database
            .recovery_status()
            .unwrap()
            .last_verified_restore_at
            .is_some()
    );
}
