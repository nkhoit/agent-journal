use journal_inbox_worker::cli;
use journal_runtime_file::FileRuntime;
use std::ffi::OsString;

pub use cli::ConfigError;

pub struct Config {
    worker: cli::Config,
    spool_dir: String,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, ConfigError> {
        let values = cli::options(args, &["--spool-dir"])?;
        Ok(Self {
            worker: cli::Config::from_options(&values)?,
            spool_dir: cli::required(&values, "--spool-dir")?,
        })
    }

    pub fn usage() -> &'static str {
        "Usage: journal-inbox-file --central-endpoint URL --credential-file PATH --routes-file PATH --spool-dir PATH [--once] [--poll-seconds N] [--wait-seconds N]"
    }
}

pub fn run(config: Config) -> Result<(), &'static str> {
    let runtime = FileRuntime::new(&config.spool_dir)
        .map_err(|_| "file spool directory unavailable or unsafe")?;
    config.worker.run(&runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_dir_is_required_and_legacy_options_are_rejected() {
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
                    .chain(["--spool-dir", "spool"])
                    .map(OsString::from)
            )
            .is_ok()
        );
        assert!(matches!(
            Config::parse(
                common
                    .into_iter()
                    .chain(["--spool-db", "legacy.db"])
                    .map(OsString::from)
            ),
            Err(ConfigError::Invalid(
                "unknown option; legacy delivery/spool options are not supported"
            ))
        ));
    }

    #[test]
    fn startup_spool_failure_is_sanitized() {
        let missing =
            std::env::temp_dir().join(format!("missing-inbox-file-spool-{}", std::process::id()));
        let config = Config::parse(
            [
                "--central-endpoint",
                "https://journal.example.invalid",
                "--credential-file",
                "unused",
                "--routes-file",
                "unused",
                "--spool-dir",
                missing.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(
            run(config),
            Err("file spool directory unavailable or unsafe")
        );
    }
}
