//! Hermes runtime boundary. The supported injection surface is unresolved.

use journal_adapter_core::{CoreError, CoreResult, Envelope, Route, Runtime};

pub const STATUS: &str = "unresolved";

#[derive(Debug, Default)]
pub struct Adapter;

impl Adapter {
    pub const fn status(&self) -> &'static str {
        STATUS
    }

    pub fn inject(&self) -> CoreResult<String> {
        Err(CoreError::RuntimeUnavailable(
            "Hermes injection surface requires revalidation".into(),
        ))
    }
}

impl Runtime for Adapter {
    fn inject(&self, _route: &Route, _envelope: &Envelope) -> CoreResult<String> {
        self.inject()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_remains_honest() {
        let adapter = Adapter;
        assert_eq!(adapter.status(), STATUS);
        assert!(matches!(
            adapter.inject(),
            Err(CoreError::RuntimeUnavailable(message)) if message.contains("revalidation")
        ));
    }
}
