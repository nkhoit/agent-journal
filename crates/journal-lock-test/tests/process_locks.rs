#![cfg(unix)]

use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "journal-lock-process-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn run(&self, mode: &str) {
        let status = Command::new(env!("CARGO_BIN_EXE_journal-lock-fixture"))
            .arg(mode)
            .arg(&self.0)
            .status()
            .unwrap();
        assert!(status.success(), "{mode} fixture failed: {status}");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn recovery_fork_child_drop_never_unlocks_parent_lock() {
    Fixture::new().run("recovery-child-drop");
}

#[test]
fn recovery_parent_drop_releases_with_live_inherited_child() {
    Fixture::new().run("recovery-parent-drop");
}

#[test]
fn recovery_clone_exec_and_constructor_error_lifecycles_are_safe() {
    let fixture = Fixture::new();
    fixture.run("recovery-clone-exec");
    fixture.run("recovery-constructor-error");
}

#[test]
fn central_hardlink_alias_is_refused_in_library_and_child_process() {
    Fixture::new().run("central-hardlink-alias");
}
