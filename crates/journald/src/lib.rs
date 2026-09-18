//! Runnable Agent Journal service shell.

mod config;
mod executor;
mod http;
mod server;

pub use config::{
    Config, ConfigError, DEFAULT_BLOCKING_LIMIT, DEFAULT_BODY_READ_TIMEOUT, DEFAULT_MAX_BODY_BYTES,
    DEFAULT_SHUTDOWN_TIMEOUT,
};
pub use executor::{BlockingError, BlockingExecutor};
pub use http::{ServiceState, admin_router, public_router};
pub use server::{Server, ServerError};

pub fn init_tracing() {
    let level = match std::env::var("JOURNAL_LOG_LEVEL").as_deref() {
        Ok("warn") => tracing::Level::WARN,
        Ok("error") => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(level)
        .with_target(false)
        .with_writer(std::io::stderr)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}
