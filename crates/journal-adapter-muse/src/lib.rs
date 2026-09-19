use journal_adapter_core::DeliveryJournal;
use journal_adapter_core::{
    Adapter, CoreError, MAX_LONG_POLL_SECONDS, Progress, StaticRoutes, SystemClock,
};
use journal_adapter_spool::{Limits, SqliteStore};
use journal_client::{Client, HttpTransport, private_file};
use journal_runtime_muse::MuseRuntime;
use std::{
    ffi::OsString,
    fmt, fs,
    io::{self, Read},
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_POLL_SECONDS: u64 = 1;
const MAX_ROUTES_BYTES: u64 = 1024 * 1024;

pub const STATUS: &str = "supported";

#[derive(Debug)]
pub enum ConfigError {
    HelpRequested,
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HelpRequested => formatter.write_str("help requested"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    central_endpoint: String,
    delivery_credential_file: PathBuf,
    spool_db: PathBuf,
    instance_id: String,
    routes_file: PathBuf,
    muse_drop_dir: String,
    once: bool,
    poll_seconds: u64,
    wait_seconds: u64,
}

impl Config {
    pub fn parse<I>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut central_endpoint = None;
        let mut delivery_credential_file = None;
        let mut spool_db = None;
        let mut instance_id = None;
        let mut routes_file = None;
        let mut muse_drop_dir = None;
        let mut once = false;
        let mut poll_seconds = DEFAULT_POLL_SECONDS;
        let mut wait_seconds = 0u64;
        let mut args = args.into_iter();

        while let Some(raw) = args.next() {
            let flag = raw
                .to_str()
                .ok_or_else(|| ConfigError::Invalid("arguments must be UTF-8".into()))?;
            match flag {
                "-h" | "--help" => return Err(ConfigError::HelpRequested),
                "--once" => once = true,
                "--central-endpoint" => {
                    central_endpoint = Some(required_value(&mut args, flag)?);
                }
                "--delivery-credential-file" => {
                    delivery_credential_file = Some(required_path(&mut args, flag)?);
                }
                "--spool-db" => spool_db = Some(required_path(&mut args, flag)?),
                "--instance-id" => instance_id = Some(required_value(&mut args, flag)?),
                "--routes-file" | "--routes-json" => {
                    routes_file = Some(required_path(&mut args, flag)?);
                }
                "--muse-drop-dir" => {
                    muse_drop_dir = Some(required_value(&mut args, flag)?);
                }
                "--poll-seconds" => {
                    let value = required_value(&mut args, flag)?;
                    poll_seconds = value.parse().map_err(|_| {
                        ConfigError::Invalid(
                            "--poll-seconds must be an integer from 1 to 3600".into(),
                        )
                    })?;
                    if !(1..=3600).contains(&poll_seconds) {
                        return Err(ConfigError::Invalid(
                            "--poll-seconds must be an integer from 1 to 3600".into(),
                        ));
                    }
                }
                "--wait-seconds" => {
                    let value = required_value(&mut args, flag)?;
                    wait_seconds = value.parse().map_err(|_| {
                        ConfigError::Invalid(format!(
                            "--wait-seconds must be an integer from 0 to {MAX_LONG_POLL_SECONDS}"
                        ))
                    })?;
                    if wait_seconds > MAX_LONG_POLL_SECONDS {
                        return Err(ConfigError::Invalid(format!(
                            "--wait-seconds must be an integer from 0 to {MAX_LONG_POLL_SECONDS}"
                        )));
                    }
                }
                _ => return Err(ConfigError::Invalid(format!("unknown argument: {flag}"))),
            }
        }

        Ok(Self {
            central_endpoint: required_option(central_endpoint, "--central-endpoint")?,
            delivery_credential_file: required_option(
                delivery_credential_file,
                "--delivery-credential-file",
            )?,
            spool_db: required_option(spool_db, "--spool-db")?,
            instance_id: required_option(instance_id, "--instance-id")?,
            routes_file: required_option(routes_file, "--routes-file")?,
            muse_drop_dir: required_option(muse_drop_dir, "--muse-drop-dir")?,
            once,
            poll_seconds,
            wait_seconds,
        })
    }

    pub fn usage() -> &'static str {
        "Usage: journal-adapter-muse --central-endpoint URL --delivery-credential-file PATH --spool-db PATH --instance-id ID --routes-file PATH --muse-drop-dir DIR [--once] [--poll-seconds N] [--wait-seconds N]"
    }
}

fn required_value<I>(args: &mut I, flag: &str) -> Result<String, ConfigError>
where
    I: Iterator<Item = OsString>,
{
    args.next()
        .ok_or_else(|| ConfigError::Invalid(format!("{flag} requires a value")))?
        .into_string()
        .map_err(|_| ConfigError::Invalid(format!("{flag} value must be UTF-8")))
}

fn required_path<I>(args: &mut I, flag: &str) -> Result<PathBuf, ConfigError>
where
    I: Iterator<Item = OsString>,
{
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| ConfigError::Invalid(format!("{flag} requires a value")))
}

fn required_option<T>(value: Option<T>, flag: &str) -> Result<T, ConfigError> {
    value.ok_or_else(|| ConfigError::Invalid(format!("missing required argument: {flag}")))
}

#[derive(Debug)]
pub enum RunError {
    Config(String),
    Io(String),
    Core(CoreError),
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Io(message) => write!(formatter, "I/O error: {message}"),
            Self::Core(error) => error.fmt(formatter),
        }
    }
}

impl From<CoreError> for RunError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

/// Execute the configured adapter. Exit-code translation is kept in `main` so
/// library tests can exercise the exact same construction and once path.
///
/// There is deliberately no `--muse-key-file`: the Muse runtime exposes no
/// authenticated API to local processes, so there is no runtime secret to
/// load. The private drop directory's filesystem permissions are the access
/// control for the handoff.
pub fn run(config: Config) -> Result<(), RunError> {
    let delivery_credential = read_delivery_credential(&config.delivery_credential_file)?;
    let routes = read_routes(&config.routes_file)?;
    let transport = HttpTransport::new(&config.central_endpoint)
        .map_err(|error| RunError::Config(format!("invalid central endpoint: {error}")))?;
    let journal = DeliveryJournal::new(Client::new(transport), delivery_credential);
    let spool = SqliteStore::open(&config.spool_db, Limits::default())?;
    let runtime = MuseRuntime::new(&config.muse_drop_dir)?;
    let clock = SystemClock;
    let mut adapter = build_adapter(
        &journal,
        &spool,
        &routes,
        &runtime,
        &clock,
        config.instance_id.clone(),
        config.wait_seconds,
    )?;

    if config.once {
        adapter.tick().map(|_| ()).map_err(RunError::from)
    } else {
        run_loop(&mut adapter, config.poll_seconds)
    }
}

/// Construct the adapter from its parts, applying the configured long-poll
/// wait. Kept as a separate generic helper so tests exercise the exact
/// construction `run` uses, including the `wait_seconds` wiring.
fn build_adapter<'a, J, S, R, T, C>(
    journal: &'a J,
    spool: &'a S,
    routes: &'a R,
    runtime: &'a T,
    clock: &'a C,
    instance_id: String,
    wait_seconds: u64,
) -> Result<Adapter<'a, J, S, R, T, C>, CoreError>
where
    J: journal_adapter_core::Journal,
    S: journal_adapter_core::AdapterSpool,
    R: journal_adapter_core::RouteResolver,
    T: journal_adapter_core::Runtime,
    C: journal_adapter_core::Clock,
{
    Adapter::new(journal, spool, routes, runtime, clock, instance_id)?
        .with_wait_seconds(wait_seconds)
}

/// Signal-aware poll loop. The shutdown signal is only observed between
/// ticks: a tick whose claim long-polls blocks synchronously, so shutdown can
/// take up to `--wait-seconds` after SIGTERM before the loop exits. No
/// delivery is lost — an interrupted run leaves a server-side lease that
/// expires, and at-least-once redelivery plus the attempt dedupe key keep a
/// repeated claim safe.
fn run_loop<J, S, R, T, C>(
    adapter: &mut Adapter<'_, J, S, R, T, C>,
    poll_seconds: u64,
) -> Result<(), RunError>
where
    J: journal_adapter_core::Journal,
    S: journal_adapter_core::AdapterSpool,
    R: journal_adapter_core::RouteResolver,
    T: journal_adapter_core::Runtime,
    C: journal_adapter_core::Clock,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| RunError::Config(format!("cannot start runtime: {error}")))?;
    let mut shutdown = runtime
        .block_on(async { ShutdownSignal::install() })
        .map_err(|error| RunError::Config(format!("cannot install shutdown handler: {error}")))?;
    loop {
        // `tick` performs synchronous journal and filesystem work. Keep it
        // outside Tokio's context.
        match adapter.tick() {
            Ok(Progress::Idle | Progress::Worked | Progress::Backoff)
            | Err(CoreError::JournalUnavailable) => {}
            Err(error) => return Err(error.into()),
        }
        let should_shutdown = runtime.block_on(async {
            tokio::select! {
                _ = shutdown.wait() => true,
                _ = tokio::time::sleep(Duration::from_secs(poll_seconds)) => false,
            }
        });
        if should_shutdown {
            return Ok(());
        }
    }
}

fn read_secret(path: &Path) -> Result<String, RunError> {
    ensure_private_parent(path)?;
    private_file::read(path).map_err(|error| RunError::Io(format!("{}: {error}", path.display())))
}

fn read_delivery_credential(path: &Path) -> Result<String, RunError> {
    let encoded = read_secret(path)?;
    let credential: journal_client::journal_protocol::OneTimeDeliveryAdapterSecret =
        journal_client::journal_protocol::decode_json(encoded.as_bytes())
            .map_err(|_| RunError::Config("delivery credential file is not valid JSON".into()))?;
    Ok(credential.secret)
}

fn read_routes(path: &Path) -> Result<StaticRoutes, RunError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| RunError::Io(format!("{}: {error}", path.display())))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(RunError::Io(format!(
            "{}: routes file must be a regular non-symlink file",
            path.display()
        )));
    }
    if metadata.len() > MAX_ROUTES_BYTES {
        return Err(RunError::Config("routes file is too large".into()));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    fs::File::open(path)
        .map_err(|error| RunError::Io(format!("{}: {error}", path.display())))?
        .take(MAX_ROUTES_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| RunError::Io(format!("{}: {error}", path.display())))?;
    if bytes.len() as u64 > MAX_ROUTES_BYTES {
        return Err(RunError::Config("routes file is too large".into()));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| RunError::Config("routes file is not valid JSON".into()))
}

#[cfg(unix)]
fn ensure_private_parent(path: &Path) -> Result<(), RunError> {
    use std::os::unix::fs::PermissionsExt;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let metadata = fs::symlink_metadata(parent)
        .map_err(|error| RunError::Io(format!("{}: {error}", parent.display())))?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(RunError::Io(format!(
            "{}: secret parent directory must be private",
            parent.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_private_parent(_path: &Path) -> Result<(), RunError> {
    Err(RunError::Io("private secret files require Unix".into()))
}

#[cfg(unix)]
struct ShutdownSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignal {
    fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) {
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
    fn install() -> io::Result<Self> {
        Ok(Self)
    }

    async fn wait(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_requires_all_non_secret_paths_and_supports_once() {
        let config = Config::parse(
            [
                "--central-endpoint",
                "http://127.0.0.1:1",
                "--delivery-credential-file",
                "delivery",
                "--spool-db",
                "spool.db",
                "--instance-id",
                "installation",
                "--routes-file",
                "routes.json",
                "--muse-drop-dir",
                "drop",
                "--once",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert!(config.once);
        assert_eq!(config.poll_seconds, DEFAULT_POLL_SECONDS);
        assert_eq!(config.wait_seconds, 0);
        assert_eq!(config.muse_drop_dir, "drop");
    }

    #[test]
    fn parser_accepts_bounded_wait_seconds() {
        let config = Config::parse(
            [
                "--central-endpoint",
                "http://127.0.0.1:1",
                "--delivery-credential-file",
                "delivery",
                "--spool-db",
                "spool.db",
                "--instance-id",
                "installation",
                "--routes-file",
                "routes.json",
                "--muse-drop-dir",
                "drop",
                "--wait-seconds",
                "25",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(config.wait_seconds, 25);
        // The server bound is accepted exactly.
        let config = Config::parse(
            [
                "--central-endpoint",
                "http://127.0.0.1:1",
                "--delivery-credential-file",
                "delivery",
                "--spool-db",
                "spool.db",
                "--instance-id",
                "installation",
                "--routes-file",
                "routes.json",
                "--muse-drop-dir",
                "drop",
                "--wait-seconds",
                &MAX_LONG_POLL_SECONDS.to_string(),
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(config.wait_seconds, MAX_LONG_POLL_SECONDS);
    }

    #[test]
    fn parser_rejects_out_of_range_wait_seconds() {
        let base = [
            "--central-endpoint",
            "http://127.0.0.1:1",
            "--delivery-credential-file",
            "delivery",
            "--spool-db",
            "spool.db",
            "--instance-id",
            "installation",
            "--routes-file",
            "routes.json",
            "--muse-drop-dir",
            "drop",
            "--wait-seconds",
        ];
        for value in [
            (MAX_LONG_POLL_SECONDS + 1).to_string(),
            "3600".to_string(),
            "-1".to_string(),
            "abc".to_string(),
            String::new(),
        ] {
            let mut args: Vec<OsString> = base.iter().map(OsString::from).collect();
            args.push(OsString::from(value.clone()));
            assert!(
                Config::parse(args.into_iter()).is_err(),
                "wait-seconds value {value:?} must be rejected"
            );
        }
    }

    #[test]
    fn parser_rejects_unknown_and_unbounded_polling() {
        assert!(Config::parse(["--unknown"].into_iter().map(OsString::from)).is_err());
        assert!(Config::parse(["--poll-seconds", "0"].into_iter().map(OsString::from)).is_err());
        // The Hermes flags do not exist on the Muse adapter.
        assert!(
            Config::parse(
                ["--hermes-base-url", "http://127.0.0.1:2"]
                    .into_iter()
                    .map(OsString::from)
            )
            .is_err()
        );
    }

    #[test]
    fn routes_file_is_rejected_before_an_unbounded_read() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agent-journal-oversized-routes-{}-{nonce}",
            std::process::id()
        ));
        let file = fs::File::create(&path).expect("create sparse routes file");
        file.set_len(MAX_ROUTES_BYTES + 1)
            .expect("size sparse routes file");
        assert!(matches!(
            read_routes(&path),
            Err(RunError::Config(message)) if message == "routes file is too large"
        ));
        fs::remove_file(path).expect("remove sparse routes file");
    }

    #[test]
    fn adapter_reports_supported_status() {
        assert_eq!(STATUS, "supported");
        assert_eq!(journal_runtime_muse::STATUS, "supported");
    }

    // Trivial fakes exercising the `build_adapter` glue `run` uses, asserting
    // the configured wait lands on the outgoing `ClaimRequest`.
    use journal_adapter_core::{
        AdapterSpool, Backoff, ClaimBatch, ClaimRequest, ClaimState, CoreResult, CustodyRequest,
        CustodyResult, Envelope, EventRequest, HeartbeatRequest, InjectionState, Journal,
        RegisterRequest, Registration, RegistrationStatus, Route, RouteResolver, Runtime, Spool,
        SpoolItem,
    };
    use std::cell::RefCell;
    use std::time::SystemTime;

    struct RecordingJournal {
        claims: RefCell<Vec<ClaimRequest>>,
    }

    impl RecordingJournal {
        fn registration() -> Registration {
            Registration {
                adapter_id: "adapter".into(),
                principal_id: "principal".into(),
                instance_id: "instance-1".into(),
                generation: 1,
                status: RegistrationStatus::Active,
                lease_expires_at: "2030-01-01T00:00:00Z".into(),
                heartbeat_after_seconds: 20,
            }
        }
    }

    impl Journal for RecordingJournal {
        fn register(&self, _: RegisterRequest) -> CoreResult<Registration> {
            Ok(Self::registration())
        }
        fn heartbeat(&self, _: HeartbeatRequest) -> CoreResult<Registration> {
            Ok(Self::registration())
        }
        fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch> {
            self.claims.borrow_mut().push(request);
            Ok(ClaimBatch {
                claim_id: "claim-1".into(),
                state: ClaimState::Active,
                lease_expires_at: "2030-01-01T00:00:00Z".into(),
                items: Vec::new(),
            })
        }
        fn commit_host_custody(&self, _: CustodyRequest) -> CoreResult<CustodyResult> {
            panic!("unexpected custody commit")
        }
        fn record_event(&self, _: &str, _: EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
    }

    struct IdleSpool;

    impl Spool for IdleSpool {
        fn put(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected put")
        }
        fn get(&self, _: &str) -> CoreResult<SpoolItem> {
            panic!("unexpected get")
        }
        fn reconcile_expired_claim(&self, _: &CustodyResult, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected reconcile")
        }
        fn confirm_custody(&self, _: &str, _: &str, _: &str, _: i64) -> CoreResult<()> {
            panic!("unexpected confirm")
        }
        fn mark_injection_started(&self, _: &str, _: &str, _: i64) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn mark_injected(&self, _: &str, _: &str, _: i64, _: &str) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn mark_injection_failed(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: InjectionState,
            _: &str,
        ) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn recoverable(&self, _: SystemTime, _: usize) -> CoreResult<Vec<SpoolItem>> {
            Ok(Vec::new())
        }
    }

    impl AdapterSpool for IdleSpool {
        fn check_capacity(&self, _: u64, _: u64) -> CoreResult<()> {
            Ok(())
        }
        fn find(&self, _: &str) -> CoreResult<Option<SpoolItem>> {
            Ok(None)
        }
        fn work_after(&self, _: SystemTime, _: Option<&str>) -> CoreResult<Option<SpoolItem>> {
            Ok(None)
        }
        fn finish(&self, _: &SpoolItem, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected finish")
        }
        fn acknowledge_event(&self, _: &str, _: &EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
        fn suppress(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected suppression")
        }
        fn backoff(&self) -> CoreResult<Backoff> {
            Ok(Backoff::default())
        }
        fn set_backoff(&self, _: &Backoff) -> CoreResult<()> {
            Ok(())
        }
    }

    struct PanicResolver;

    impl RouteResolver for PanicResolver {
        fn resolve(&self, _: &str, _: &str) -> CoreResult<Route> {
            panic!("unexpected resolve")
        }
    }

    struct PanicRuntime;

    impl Runtime for PanicRuntime {
        fn inject(&self, _: &Route, _: &Envelope, _: &str) -> CoreResult<String> {
            panic!("unexpected inject")
        }
    }

    #[test]
    fn build_adapter_applies_configured_wait_seconds() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = IdleSpool;
        let clock = SystemClock;
        let mut adapter = build_adapter(
            &journal,
            &spool,
            &PanicResolver,
            &PanicRuntime,
            &clock,
            "instance-1".into(),
            25,
        )
        .unwrap();
        assert!(matches!(adapter.tick(), Ok(Progress::Idle)));
        let claims = journal.claims.borrow();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].wait_seconds, 25);
    }

    #[test]
    fn build_adapter_defaults_to_immediate_claim() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = IdleSpool;
        let clock = SystemClock;
        let mut adapter = build_adapter(
            &journal,
            &spool,
            &PanicResolver,
            &PanicRuntime,
            &clock,
            "instance-1".into(),
            0,
        )
        .unwrap();
        assert!(matches!(adapter.tick(), Ok(Progress::Idle)));
        assert_eq!(journal.claims.borrow()[0].wait_seconds, 0);
    }

    #[test]
    fn build_adapter_rejects_wait_above_server_bound() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = IdleSpool;
        let clock = SystemClock;
        assert!(
            build_adapter(
                &journal,
                &spool,
                &PanicResolver,
                &PanicRuntime,
                &clock,
                "instance-1".into(),
                MAX_LONG_POLL_SECONDS + 1,
            )
            .is_err()
        );
    }
}
