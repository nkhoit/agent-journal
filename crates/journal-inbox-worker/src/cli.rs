use crate::{PrincipalInbox, Runtime, StaticRoutes, Worker};
use journal_client::{
    Client, HttpTransport,
    journal_protocol::{OneTimePrincipalClientSecret, decode_json},
    private_file,
};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::Path,
    time::{Duration, Instant},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("help requested")]
    HelpRequested,
    #[error("{0}")]
    Invalid(&'static str),
}

pub fn options(
    args: impl IntoIterator<Item = OsString>,
    runtime_flags: &[&str],
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut values = BTreeMap::new();
    let mut args = args.into_iter();
    while let Some(raw) = args.next() {
        let flag = raw
            .to_str()
            .ok_or(ConfigError::Invalid("arguments must be UTF-8"))?;
        if matches!(flag, "--help" | "-h") {
            return Err(ConfigError::HelpRequested);
        }
        let flag = if flag == "--routes-json" {
            "--routes-file"
        } else {
            flag
        };
        let value = if flag == "--once" {
            String::new()
        } else if [
            "--central-endpoint",
            "--credential-file",
            "--routes-file",
            "--poll-seconds",
            "--wait-seconds",
        ]
        .contains(&flag)
            || runtime_flags.contains(&flag)
        {
            args.next()
                .and_then(|value| value.into_string().ok())
                .ok_or(ConfigError::Invalid("missing or invalid option value"))?
        } else {
            return Err(ConfigError::Invalid(
                "unknown option; legacy delivery/spool options are not supported",
            ));
        };
        if values.insert(flag.into(), value).is_some() {
            return Err(ConfigError::Invalid("duplicate option"));
        }
    }
    Ok(values)
}

pub fn required(
    values: &BTreeMap<String, String>,
    flag: &'static str,
) -> Result<String, ConfigError> {
    values
        .get(flag)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or(ConfigError::Invalid(flag))
}

pub struct Config {
    endpoint: String,
    credential_file: String,
    routes_file: String,
    once: bool,
    poll: Duration,
    wait_seconds: u64,
}

impl Config {
    pub fn from_options(values: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let poll = values
            .get("--poll-seconds")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| ConfigError::Invalid("poll seconds must be 1..3600"))?
            .unwrap_or(1);
        if !(1..=3600).contains(&poll) {
            return Err(ConfigError::Invalid("poll seconds must be 1..3600"));
        }
        let wait_seconds = values
            .get("--wait-seconds")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| ConfigError::Invalid("wait seconds must be 0..30"))?
            .unwrap_or(0);
        if wait_seconds > journal_client::journal_protocol::MAX_INBOX_WAIT_SECONDS {
            return Err(ConfigError::Invalid("wait seconds must be 0..30"));
        }
        Ok(Self {
            endpoint: required(values, "--central-endpoint")?,
            credential_file: required(values, "--credential-file")?,
            routes_file: required(values, "--routes-file")?,
            once: values.contains_key("--once"),
            poll: Duration::from_secs(poll),
            wait_seconds,
        })
    }

    pub fn run(&self, runtime: &impl Runtime) -> Result<(), &'static str> {
        let credential: OneTimePrincipalClientSecret =
            decode_json(read_private(&self.credential_file)?.as_bytes())
                .map_err(|_| "invalid principal credential file")?;
        let routes: StaticRoutes =
            decode_json(read_private_with_limit(&self.routes_file, 1_048_576)?.as_bytes())
                .map_err(|_| "invalid private routes file")?;
        let transport =
            HttpTransport::new(&self.endpoint).map_err(|_| "invalid central endpoint")?;
        let inbox = PrincipalInbox::new(Client::new(transport), credential.secret)
            .with_wait_seconds(self.wait_seconds);
        let mut worker = Worker::new(&inbox, runtime, &routes);
        let clock = Instant::now();
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "cannot start signal handler")?;
        #[cfg(unix)]
        let mut terminate = executor
            .block_on(async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            })
            .map_err(|_| "cannot install termination handler")?;
        loop {
            let events = worker.tick(clock.elapsed()).map_err(|error| match error {
                crate::WorkerError::Unauthorized => {
                    "principal credential rejected; repair before restarting"
                }
                crate::WorkerError::InvalidResponse => {
                    "unexpected journal response; stopped without acknowledgment"
                }
            })?;
            for event in events {
                eprintln!(
                    "inbox-client event={:?} item={:?}",
                    event.kind,
                    event.item_id.as_deref().unwrap_or("none")
                );
            }
            if self.once {
                return Ok(());
            }
            let stop = executor
                .block_on(async {
                    #[cfg(unix)]
                    {
                        tokio::select! {
                            result = tokio::signal::ctrl_c() => result.map(|()| true),
                            _ = terminate.recv() => Ok(true),
                            _ = tokio::time::sleep(self.poll) => Ok(false),
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        tokio::select! {
                            result = tokio::signal::ctrl_c() => result.map(|()| true),
                            _ = tokio::time::sleep(self.poll) => Ok(false),
                        }
                    }
                })
                .map_err(|_| "shutdown signal handler failed")?;
            if stop {
                return Ok(());
            }
        }
    }
}

pub fn read_private(path: &str) -> Result<String, &'static str> {
    read_private_with_limit(path, 16_384)
}

fn read_private_with_limit(path: &str, max_bytes: u64) -> Result<String, &'static str> {
    let path = Path::new(path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let metadata =
            std::fs::symlink_metadata(parent).map_err(|_| "private directory unavailable")?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err("file parent must be a private regular directory");
        }
    }
    private_file::read_with_limit(path, max_bytes)
        .map_err(|_| "private input file unavailable or unsafe")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args<'a>(values: &'a [&'a str]) -> impl Iterator<Item = OsString> + 'a {
        values.iter().map(OsString::from)
    }

    #[test]
    fn legacy_options_and_duplicate_aliases_fail_closed() {
        for legacy in ["--delivery-credential-file", "--spool-db", "--instance-id"] {
            assert!(matches!(
                options(args(&[legacy, "value"]), &[]),
                Err(ConfigError::Invalid(
                    "unknown option; legacy delivery/spool options are not supported"
                ))
            ));
        }
        assert!(matches!(
            options(
                args(&[
                    "--routes-file",
                    "routes.json",
                    "--routes-json",
                    "other.json"
                ]),
                &[]
            ),
            Err(ConfigError::Invalid("duplicate option"))
        ));
    }

    #[test]
    fn poll_bounds_and_required_options_are_exact() {
        for invalid in ["0", "3601", "not-a-number"] {
            let values = options(
                args(&[
                    "--central-endpoint",
                    "https://journal.example.invalid",
                    "--credential-file",
                    "credential.json",
                    "--routes-file",
                    "routes.json",
                    "--poll-seconds",
                    invalid,
                ]),
                &[],
            )
            .unwrap();
            assert!(matches!(
                Config::from_options(&values),
                Err(ConfigError::Invalid("poll seconds must be 1..3600"))
            ));
        }
        for valid in ["1", "3600"] {
            let values = options(
                args(&[
                    "--central-endpoint",
                    "https://journal.example.invalid",
                    "--credential-file",
                    "credential.json",
                    "--routes-json",
                    "routes.json",
                    "--poll-seconds",
                    valid,
                    "--once",
                ]),
                &[],
            )
            .unwrap();
            assert!(Config::from_options(&values).is_ok());
        }
    }

    #[test]
    fn wait_seconds_bounds_are_exact() {
        for invalid in ["-1", "31", "3600", "not-a-number"] {
            let values = options(
                args(&[
                    "--central-endpoint",
                    "https://journal.example.invalid",
                    "--credential-file",
                    "credential.json",
                    "--routes-file",
                    "routes.json",
                    "--wait-seconds",
                    invalid,
                ]),
                &[],
            )
            .unwrap();
            assert!(
                matches!(
                    Config::from_options(&values),
                    Err(ConfigError::Invalid("wait seconds must be 0..30"))
                ),
                "{invalid}"
            );
        }
        for valid in ["0", "1", "30"] {
            let values = options(
                args(&[
                    "--central-endpoint",
                    "https://journal.example.invalid",
                    "--credential-file",
                    "credential.json",
                    "--routes-file",
                    "routes.json",
                    "--wait-seconds",
                    valid,
                    "--once",
                ]),
                &[],
            )
            .unwrap();
            assert!(Config::from_options(&values).is_ok(), "{valid}");
        }
        // Omitted --wait-seconds defaults to immediate return.
        let values = options(
            args(&[
                "--central-endpoint",
                "https://journal.example.invalid",
                "--credential-file",
                "credential.json",
                "--routes-file",
                "routes.json",
                "--once",
            ]),
            &[],
        )
        .unwrap();
        assert_eq!(Config::from_options(&values).unwrap().wait_seconds, 0);
    }

    #[cfg(unix)]
    #[test]
    fn private_input_limit_is_exact_and_symlinks_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        let directory =
            std::env::temp_dir().join(format!("inbox-client-private-input-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let input = directory.join("input");
        std::fs::write(&input, b"1234").unwrap();
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_private_with_limit(input.to_str().unwrap(), 4),
            Ok("1234".into())
        );
        assert_eq!(
            read_private_with_limit(input.to_str().unwrap(), 3),
            Err("private input file unavailable or unsafe")
        );
        let link = directory.join("link");
        std::os::unix::fs::symlink(&input, &link).unwrap();
        assert_eq!(
            read_private_with_limit(link.to_str().unwrap(), 4),
            Err("private input file unavailable or unsafe")
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
