//! Deterministic runtime acceptance fixture, not a vendor runtime or a delivery guarantee.

use journal_adapter_core::{CoreError, CoreResult, Envelope, Route, Runtime};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

pub const ACCEPT_THEN_CRASH_EXIT: i32 = 86;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acceptance {
    pub route: Route,
    pub envelope: Envelope,
    pub rendered: String,
}

pub struct FakeRuntime {
    available: Cell<bool>,
    accept_then_crash: Cell<bool>,
    accepted: RefCell<Vec<Acceptance>>,
    ledger: RefCell<Option<File>>,
}

impl Default for FakeRuntime {
    fn default() -> Self {
        Self {
            available: Cell::new(true),
            accept_then_crash: Cell::new(false),
            accepted: RefCell::new(vec![]),
            ledger: RefCell::new(None),
        }
    }
}

fn unavailable(_: impl std::fmt::Display) -> CoreError {
    CoreError::RuntimeUnavailable("fake runtime ledger unavailable".into())
}

impl FakeRuntime {
    /// The caller owns the private fixture directory and its cleanup. The ledger
    /// contains local routes and record content; it must never be published.
    pub fn durable(path: impl AsRef<Path>) -> CoreResult<Self> {
        let path = path.as_ref();
        let mut options = OpenOptions::new();
        options.append(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let ledger = options.open(path).map_err(unavailable)?;
        let contents = std::fs::read_to_string(path).map_err(unavailable)?;
        let accepted = contents
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Acceptance>, _>>()
            .map_err(unavailable)?;
        Ok(Self {
            accepted: RefCell::new(accepted),
            ledger: RefCell::new(Some(ledger)),
            ..Self::default()
        })
    }

    pub fn set_available(&self, available: bool) {
        self.available.set(available);
    }

    /// Only enable in a disposable child process. Exit deliberately skips Rust
    /// destructors after a synced acceptance, before the caller sees a receipt.
    pub fn set_accept_then_crash(&self, enabled: bool) {
        self.accept_then_crash.set(enabled);
    }

    pub fn acceptances(&self) -> Vec<Acceptance> {
        self.accepted.borrow().clone()
    }
}

impl Runtime for FakeRuntime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> CoreResult<String> {
        if !self.available.get() {
            return Err(CoreError::RuntimeUnavailable("fake runtime offline".into()));
        }
        if !route.enabled || route.runtime_target.is_empty() || rendered != envelope.render() {
            return Err(CoreError::RuntimeRejected);
        }
        let acceptance = Acceptance {
            route: route.clone(),
            envelope: envelope.clone(),
            rendered: rendered.into(),
        };
        if let Some(ledger) = self.ledger.borrow_mut().as_mut() {
            let mut bytes = serde_json::to_vec(&acceptance).map_err(unavailable)?;
            bytes.push(b'\n');
            ledger.write_all(&bytes).map_err(unavailable)?;
            ledger.sync_all().map_err(unavailable)?;
        }
        self.accepted.borrow_mut().push(acceptance);
        if self.accept_then_crash.get() {
            std::process::exit(ACCEPT_THEN_CRASH_EXIT);
        }
        Ok("fake runtime acceptance".into())
    }
}
