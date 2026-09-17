use std::process::ExitCode;

use journald::{Config, ConfigError, Server};

fn main() -> ExitCode {
    journald::init_tracing();
    let config = match Config::parse(std::env::args_os().skip(1)) {
        Ok(config) => config,
        Err(ConfigError::HelpRequested) => {
            println!("{}", Config::usage());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("journald: {error}\n\n{}", Config::usage());
            return ExitCode::from(2);
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("journald: cannot start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        let shutdown = match ShutdownSignal::install() {
            Ok(shutdown) => shutdown,
            Err(error) => {
                tracing::error!(event = "signal_registration_failed", error = %error);
                return ExitCode::FAILURE;
            }
        };
        let server = match Server::bind(config).await {
            Ok(server) => server,
            Err(error) => {
                tracing::error!(event = "startup_failed", error = %error);
                return ExitCode::FAILURE;
            }
        };
        match server.serve(shutdown.wait()).await {
            Ok(()) => {
                tracing::info!(event = "service_stopped");
                ExitCode::SUCCESS
            }
            Err(error) => {
                tracing::error!(event = "service_failed", error = %error);
                ExitCode::FAILURE
            }
        }
    })
}

#[cfg(unix)]
struct ShutdownSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignal {
    fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignal;

#[cfg(not(unix))]
impl ShutdownSignal {
    fn install() -> std::io::Result<Self> {
        Ok(Self)
    }

    async fn wait(self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}
