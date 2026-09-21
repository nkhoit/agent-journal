use journal_inbox_worker::cli;
use journal_runtime_hermes::HermesRuntime;
use std::ffi::OsString;

pub use cli::ConfigError;

pub struct Config {
    worker: cli::Config,
    endpoint: String,
    key_file: String,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, ConfigError> {
        let values = cli::options(args, &["--hermes-base-url", "--hermes-key-file"])?;
        Ok(Self {
            worker: cli::Config::from_options(&values)?,
            endpoint: cli::required(&values, "--hermes-base-url")?,
            key_file: cli::required(&values, "--hermes-key-file")?,
        })
    }

    pub fn usage() -> &'static str {
        "Usage: journal-inbox-hermes --central-endpoint URL --credential-file PATH --routes-file PATH --hermes-base-url URL --hermes-key-file PATH [--once] [--poll-seconds N]"
    }
}

pub fn run(config: Config) -> Result<(), &'static str> {
    let key = cli::read_private(&config.key_file)?;
    let runtime = HermesRuntime::new(&config.endpoint, key)
        .map_err(|_| "Hermes capability preflight failed; no inbox items acknowledged")?;
    config.worker.run(&runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_options_are_required_and_legacy_options_are_rejected() {
        let common = [
            "--central-endpoint",
            "https://journal.example.invalid",
            "--credential-file",
            "credential.json",
            "--routes-file",
            "routes.json",
        ];
        assert!(Config::parse(common.map(OsString::from)).is_err());
        assert!(
            Config::parse(
                common
                    .into_iter()
                    .chain([
                        "--hermes-base-url",
                        "http://127.0.0.1:8765",
                        "--hermes-key-file",
                        "hermes.key",
                    ])
                    .map(OsString::from)
            )
            .is_ok()
        );
        assert!(matches!(
            Config::parse(
                common
                    .into_iter()
                    .chain(["--spool-db", "spool.db"])
                    .map(OsString::from)
            ),
            Err(ConfigError::Invalid(
                "unknown option; legacy delivery/spool options are not supported"
            ))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn startup_preflight_failure_is_sanitized() {
        use std::os::unix::fs::PermissionsExt;

        let directory =
            std::env::temp_dir().join(format!("inbox-hermes-startup-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let key = directory.join("hermes.key");
        std::fs::write(&key, "private-hermes-key").unwrap();
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = Config::parse(
            [
                "--central-endpoint",
                "https://journal.example.invalid",
                "--credential-file",
                "unused",
                "--routes-file",
                "unused",
                "--hermes-base-url",
                "http://127.0.0.1:1",
                "--hermes-key-file",
                key.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(
            run(config),
            Err("Hermes capability preflight failed; no inbox items acknowledged")
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
