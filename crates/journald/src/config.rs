use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

pub const DEFAULT_BLOCKING_LIMIT: usize = 8;
pub const DEFAULT_MAX_BODY_BYTES: usize = 1_048_576;
pub const DEFAULT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_PUBLIC_PORT: u16 = 8080;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub database_path: PathBuf,
    pub recovery_audit_path: Option<PathBuf>,
    pub public_address: SocketAddr,
    pub admin_socket_path: PathBuf,
    pub blocking_limit: usize,
    pub max_body_bytes: usize,
    pub body_read_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub web: Option<WebConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebConfig {
    pub address: SocketAddr,
    pub viewer: String,
}

impl WebConfig {
    pub fn validate(&self) -> bool {
        self.address.ip().is_loopback()
            && journal_protocol::domain::validate_identifier("viewer", &self.viewer).is_ok()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("help requested")]
    HelpRequested,
    #[error("missing required option {0}")]
    Missing(&'static str),
    #[error("option {0} requires a value")]
    MissingValue(String),
    #[error("option {0} was provided more than once")]
    Duplicate(String),
    #[error("unknown option {0}")]
    Unknown(String),
    #[error("option {option} has invalid value {value}")]
    Invalid { option: String, value: String },
}

impl Config {
    pub fn parse<I, S>(arguments: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        let mut database_path = None;
        let mut recovery_audit_path = None;
        let mut public_address = None;
        let mut admin_socket_path = None;
        let mut blocking_limit = None;
        let mut max_body_bytes = None;
        let mut web_address = None;
        let mut web_viewer = None;
        let mut arguments = arguments.into_iter().map(Into::into);

        while let Some(argument) = arguments.next() {
            let option = argument.to_string_lossy().into_owned();
            if option == "--help" || option == "-h" {
                return Err(ConfigError::HelpRequested);
            }
            let value = arguments
                .next()
                .ok_or_else(|| ConfigError::MissingValue(option.clone()))?;
            match option.as_str() {
                "--database" => set_once(&mut database_path, PathBuf::from(value), &option)?,
                "--recovery-audit" => {
                    set_once(&mut recovery_audit_path, PathBuf::from(value), &option)?
                }
                "--listen" => {
                    let text = value.to_string_lossy().into_owned();
                    let parsed = text.parse().map_err(|_| ConfigError::Invalid {
                        option: option.clone(),
                        value: text,
                    })?;
                    set_once(&mut public_address, parsed, &option)?;
                }
                "--admin-socket" => {
                    set_once(&mut admin_socket_path, PathBuf::from(value), &option)?;
                }
                "--web-listen" => {
                    let text = value.to_string_lossy().into_owned();
                    let parsed = text
                        .parse::<SocketAddr>()
                        .map_err(|_| ConfigError::Invalid {
                            option: option.clone(),
                            value: text.clone(),
                        })?;
                    if !parsed.ip().is_loopback() {
                        return Err(ConfigError::Invalid {
                            option,
                            value: text,
                        });
                    }
                    set_once(&mut web_address, parsed, &option)?;
                }
                "--web-viewer" => {
                    let text = value.to_string_lossy().into_owned();
                    journal_protocol::domain::validate_identifier("viewer", &text).map_err(
                        |_| ConfigError::Invalid {
                            option: option.clone(),
                            value: text.clone(),
                        },
                    )?;
                    set_once(&mut web_viewer, text, &option)?;
                }
                "--blocking-limit" => {
                    let text = value.to_string_lossy().into_owned();
                    let parsed = parse_blocking_limit(&option, &text)?;
                    set_once(&mut blocking_limit, parsed, &option)?;
                }
                "--max-body-bytes" => {
                    let text = value.to_string_lossy().into_owned();
                    let parsed = parse_positive_usize(&option, &text)?;
                    set_once(&mut max_body_bytes, parsed, &option)?;
                }
                _ => return Err(ConfigError::Unknown(option)),
            }
        }

        let web = match (web_address, web_viewer) {
            (None, None) => None,
            (Some(address), Some(viewer)) => Some(WebConfig { address, viewer }),
            (Some(_), None) => return Err(ConfigError::Missing("--web-viewer")),
            (None, Some(_)) => return Err(ConfigError::Missing("--web-listen")),
        };
        Ok(Self {
            database_path: database_path.ok_or(ConfigError::Missing("--database"))?,
            recovery_audit_path,
            public_address: public_address.unwrap_or_else(|| {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PUBLIC_PORT)
            }),
            admin_socket_path: admin_socket_path.ok_or(ConfigError::Missing("--admin-socket"))?,
            blocking_limit: blocking_limit.unwrap_or(DEFAULT_BLOCKING_LIMIT),
            max_body_bytes: max_body_bytes.unwrap_or(DEFAULT_MAX_BODY_BYTES),
            body_read_timeout: DEFAULT_BODY_READ_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            web,
        })
    }

    pub const fn usage() -> &'static str {
        "Usage: journald --database PATH --admin-socket PATH [--recovery-audit PATH] [--listen ADDRESS] \
         [--blocking-limit COUNT] [--max-body-bytes BYTES] \
         [--web-listen LOOPBACK_ADDRESS --web-viewer PRINCIPAL]"
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, option: &str) -> Result<(), ConfigError> {
    if slot.replace(value).is_some() {
        return Err(ConfigError::Duplicate(option.to_owned()));
    }
    Ok(())
}

fn parse_positive_usize(option: &str, value: &str) -> Result<usize, ConfigError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ConfigError::Invalid {
            option: option.to_owned(),
            value: value.to_owned(),
        })
}

fn parse_blocking_limit(option: &str, value: &str) -> Result<usize, ConfigError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(value))
        .ok_or_else(|| ConfigError::Invalid {
            option: option.to_owned(),
            value: value.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_web_requires_explicit_loopback_and_principal_pair() {
        let base = ["--database", "journal.db", "--admin-socket", "admin.sock"];
        assert!(Config::parse(base).unwrap().web.is_none());
        for extra in [
            vec!["--web-listen", "127.0.0.1:8081"],
            vec!["--web-viewer", "viewer"],
            vec!["--web-listen", "0.0.0.0:8081", "--web-viewer", "viewer"],
            vec!["--web-listen", "[::]:8081", "--web-viewer", "viewer"],
            vec!["--web-listen", "127.0.0.1:8081", "--web-viewer", ""],
        ] {
            assert!(Config::parse(base.into_iter().chain(extra)).is_err());
        }
        let config = Config::parse(base.into_iter().chain([
            "--web-listen",
            "127.0.0.1:8081",
            "--web-viewer",
            "viewer",
        ]))
        .unwrap();
        assert!(config.web.unwrap().validate());
    }

    #[test]
    fn parses_required_and_bounded_options() {
        let config = Config::parse([
            "--database",
            "journal.db",
            "--recovery-audit",
            "audit.db",
            "--admin-socket",
            "admin.sock",
            "--listen",
            "127.0.0.1:9000",
            "--blocking-limit",
            "4",
            "--max-body-bytes",
            "2048",
        ])
        .expect("parse configuration");

        assert_eq!(config.database_path, PathBuf::from("journal.db"));
        assert_eq!(config.recovery_audit_path, Some(PathBuf::from("audit.db")));
        assert_eq!(config.public_address, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(config.admin_socket_path, PathBuf::from("admin.sock"));
        assert_eq!(config.blocking_limit, 4);
        assert_eq!(config.max_body_bytes, 2048);
        assert_eq!(config.body_read_timeout, DEFAULT_BODY_READ_TIMEOUT);
        assert_eq!(config.shutdown_timeout, DEFAULT_SHUTDOWN_TIMEOUT);
    }

    #[test]
    fn enforces_the_blocking_limit_semaphore_boundary() {
        let maximum = tokio::sync::Semaphore::MAX_PERMITS.to_string();
        let config = Config::parse([
            "--database",
            "journal.db",
            "--admin-socket",
            "admin.sock",
            "--blocking-limit",
            maximum.as_str(),
        ])
        .expect("accept maximum semaphore capacity");
        assert_eq!(config.blocking_limit, tokio::sync::Semaphore::MAX_PERMITS);

        let excessive = (tokio::sync::Semaphore::MAX_PERMITS + 1).to_string();
        assert!(matches!(
            Config::parse([
                "--database",
                "journal.db",
                "--admin-socket",
                "admin.sock",
                "--blocking-limit",
                excessive.as_str()
            ]),
            Err(ConfigError::Invalid { option, .. }) if option == "--blocking-limit"
        ));
    }

    #[test]
    fn rejects_missing_duplicate_unknown_and_zero_options() {
        assert!(matches!(
            Config::parse(["--database", "journal.db"]),
            Err(ConfigError::Missing("--admin-socket"))
        ));
        assert!(matches!(
            Config::parse([
                "--database",
                "one.db",
                "--database",
                "two.db",
                "--admin-socket",
                "admin.sock"
            ]),
            Err(ConfigError::Duplicate(option)) if option == "--database"
        ));
        assert!(matches!(
            Config::parse([
                "--database",
                "journal.db",
                "--admin-socket",
                "admin.sock",
                "--blocking-limit",
                "0"
            ]),
            Err(ConfigError::Invalid { option, .. }) if option == "--blocking-limit"
        ));
        assert!(matches!(
            Config::parse(["--wat", "value"]),
            Err(ConfigError::Unknown(option)) if option == "--wat"
        ));
    }
}
