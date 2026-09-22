use journal_inbox_worker::cli;
use journal_runtime_muse::MuseRuntime;
use std::ffi::OsString;

pub use cli::ConfigError;

pub struct Config {
    worker: cli::Config,
    drop_directory: String,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, ConfigError> {
        let values = cli::options(args, &["--muse-drop-dir"])?;
        Ok(Self {
            worker: cli::Config::from_options(&values)?,
            drop_directory: cli::required(&values, "--muse-drop-dir")?,
        })
    }

    pub fn usage() -> &'static str {
        "Usage: journal-inbox-muse --central-endpoint URL --credential-file PATH --routes-file PATH --muse-drop-dir PATH [--once] [--poll-seconds N] [--wait-seconds N]"
    }
}

pub fn run(config: Config) -> Result<(), &'static str> {
    let runtime = MuseRuntime::new(&config.drop_directory)
        .map_err(|_| "Muse drop directory unavailable or unsafe")?;
    config.worker.run(&runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_option_is_required_and_legacy_options_are_rejected() {
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
                    .chain(["--muse-drop-dir", "drop"])
                    .map(OsString::from)
            )
            .is_ok()
        );
        assert!(matches!(
            Config::parse(
                common
                    .into_iter()
                    .chain(["--instance-id", "legacy"])
                    .map(OsString::from)
            ),
            Err(ConfigError::Invalid(
                "unknown option; legacy delivery/spool options are not supported"
            ))
        ));
    }

    #[test]
    fn startup_drop_failure_is_sanitized() {
        let missing =
            std::env::temp_dir().join(format!("missing-inbox-muse-drop-{}", std::process::id()));
        let config = Config::parse(
            [
                "--central-endpoint",
                "https://journal.example.invalid",
                "--credential-file",
                "unused",
                "--routes-file",
                "unused",
                "--muse-drop-dir",
                missing.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(
            run(config),
            Err("Muse drop directory unavailable or unsafe")
        );
    }
}
