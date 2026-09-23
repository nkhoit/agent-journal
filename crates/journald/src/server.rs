use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::pin::Pin;
#[cfg(unix)]
use std::task::{Context, Poll};
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use fs2::FileExt;
#[cfg(unix)]
use journal_storage_sqlite::Database;
use journal_storage_sqlite::StorageError;
use thiserror::Error;
#[cfg(unix)]
use tokio::sync::watch;
use tokio::task::JoinError;

use crate::config::Config;
use crate::executor::BlockingError;
#[cfg(unix)]
use crate::http::{
    ServiceState, admin_router_with_timeout, public_router_with_timeout, web_router_with_timeout,
};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("database initialization failed: {0}")]
    Database(#[from] StorageError),
    #[error("database initialization task failed: {0}")]
    DatabaseTask(#[from] JoinError),
    #[error("database clean shutdown failed: {0}")]
    DatabaseShutdown(#[source] BlockingError),
    #[error("cannot bind public listener at {address}: {source}")]
    BindPublic {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("cannot bind administrative socket at {path}: {source}")]
    BindAdmin {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot protect administrative socket at {path}: {source}")]
    ProtectAdmin {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("administrative socket parent is not a private directory: {0}")]
    InsecureAdminDirectory(PathBuf),
    #[error("public listener failed: {0}")]
    ServePublic(io::Error),
    #[error("web listener failed: {0}")]
    ServeWeb(io::Error),
    #[error("cannot initialize web listener: {0}")]
    WebInitialization(&'static str),
    #[error("administrative listener failed: {0}")]
    ServeAdmin(io::Error),
    #[error("listener task failed: {0}")]
    ListenerTask(JoinError),
    #[error("a listener stopped before shutdown")]
    ListenerStopped,
    #[error("graceful shutdown timed out; hot database state was left fail-closed")]
    ShutdownTimeout,

    #[error("administrative Unix sockets are unsupported on this platform")]
    AdminSocketUnsupported,
}

#[cfg(unix)]
pub struct Server {
    public_listener: tokio::net::TcpListener,
    public_address: SocketAddr,
    admin: AdminSocket,
    state: ServiceState,
    max_body_bytes: usize,
    body_read_timeout: Duration,
    shutdown_timeout: Duration,
    web_listener: Option<(tokio::net::TcpListener, String)>,
}

#[cfg(unix)]
impl Server {
    pub async fn bind(config: Config) -> Result<Self, ServerError> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&config.blocking_limit) {
            return Err(ServerError::InvalidConfig(
                "blocking limit exceeds the supported semaphore capacity",
            ));
        }
        if config.max_body_bytes == 0 {
            return Err(ServerError::InvalidConfig(
                "maximum body size must be positive",
            ));
        }
        if config.body_read_timeout.is_zero() {
            return Err(ServerError::InvalidConfig(
                "body read timeout must be positive",
            ));
        }
        if config.shutdown_timeout.is_zero() {
            return Err(ServerError::InvalidConfig(
                "shutdown timeout must be positive",
            ));
        }
        validate_admin_socket_parent(&config.admin_socket_path)?;
        if config.web.as_ref().is_some_and(|web| !web.validate()) {
            return Err(ServerError::InvalidConfig(
                "web requires a loopback address and valid viewer",
            ));
        }

        // Acquire the admin ownership lock before opening any other service
        // resource. A second conforming server must fail at this lock, not at
        // an incidental database or listener dependency.
        let admin = bind_admin_listener(&config.admin_socket_path)
            .await
            .map_err(|source| ServerError::BindAdmin {
                path: config.admin_socket_path.clone(),
                source,
            })?;

        let database_path = config.database_path.clone();
        let audit_path = config
            .recovery_audit_path
            .clone()
            .unwrap_or_else(|| database_path.with_extension("recovery.db"));
        let database = tokio::task::spawn_blocking(move || {
            Database::open_protected(database_path, audit_path)
        })
        .await??;
        let state = ServiceState::new(database, config.blocking_limit)
            .map_err(|_| ServerError::InvalidConfig("blocking limit must be positive"))?;
        let web_listener = if let Some(web) = config.web {
            let viewer = web.viewer.clone();
            state
                .blocking()
                .execute(move |database| {
                    Ok(journal_service::BootstrapService::new(database.clone())
                        .shared_viewer(&viewer)
                        .verify())
                })
                .await
                .map_err(|_| ServerError::WebInitialization("viewer check unavailable"))?
                .map_err(|_| {
                    ServerError::WebInitialization("viewer must be an existing active principal")
                })?;
            let listener = tokio::net::TcpListener::bind(web.address)
                .await
                .map_err(ServerError::ServeWeb)?;
            Some((listener, web.viewer))
        } else {
            None
        };

        let public_listener = tokio::net::TcpListener::bind(config.public_address)
            .await
            .map_err(|source| ServerError::BindPublic {
                address: config.public_address,
                source,
            })?;
        let public_address =
            public_listener
                .local_addr()
                .map_err(|source| ServerError::BindPublic {
                    address: config.public_address,
                    source,
                })?;

        Ok(Self {
            public_listener,
            public_address,
            admin,
            state,
            max_body_bytes: config.max_body_bytes,
            body_read_timeout: config.body_read_timeout,
            shutdown_timeout: config.shutdown_timeout,
            web_listener,
        })
    }

    pub const fn public_address(&self) -> SocketAddr {
        self.public_address
    }

    pub fn admin_socket_path(&self) -> &Path {
        self.admin.path()
    }

    pub fn web_address(&self) -> io::Result<Option<SocketAddr>> {
        self.web_listener
            .as_ref()
            .map(|(listener, _)| listener.local_addr())
            .transpose()
    }

    pub async fn serve<S>(self, shutdown: S) -> Result<(), ServerError>
    where
        S: Future<Output = ()> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Retain a database handle only for final normalization. It owns no
        // connection; axum's graceful joins below close every request first.
        let shutdown_state = self.state.clone();
        let public_router = public_router_with_timeout(
            self.state.clone(),
            self.max_body_bytes,
            self.body_read_timeout,
            shutdown_rx.clone(),
        );
        let admin_router = admin_router_with_timeout(
            self.state.clone(),
            self.max_body_bytes,
            self.body_read_timeout,
            shutdown_rx.clone(),
        );
        use std::os::unix::fs::MetadataExt;
        let admin_owner = std::fs::metadata(self.admin.path())
            .map_err(ServerError::ServeAdmin)?
            .uid();
        let admin_router =
            admin_router.layer(axum::Extension(crate::http::AdminOwner(admin_owner)));
        let shutdown_timeout = self.shutdown_timeout;
        let (admin_listener, admin_guard) =
            self.admin.into_parts().map_err(ServerError::ServeAdmin)?;
        let web_enabled = self.web_listener.is_some();
        let web_shutdown = shutdown_rx.clone();
        let web_serving = async move {
            if let Some((listener, viewer)) = self.web_listener {
                let router = web_router_with_timeout(
                    self.state,
                    viewer,
                    self.max_body_bytes,
                    self.body_read_timeout,
                    web_shutdown.clone(),
                );
                axum::serve(listener, router)
                    .with_graceful_shutdown(wait_for_shutdown(web_shutdown))
                    .await
                    .map_err(ServerError::ServeWeb)
            } else {
                Ok(())
            }
        };
        let public_shutdown = shutdown_rx.clone();
        let public_serving = async move {
            axum::serve(self.public_listener, public_router)
                .with_graceful_shutdown(wait_for_shutdown(public_shutdown))
                .await
                .map_err(ServerError::ServePublic)
        };
        let admin_shutdown = shutdown_rx;
        let admin_serving = async move {
            axum::serve(
                admin_listener,
                admin_router.into_make_service_with_connect_info::<crate::http::AdminPeer>(),
            )
            .with_graceful_shutdown(wait_for_shutdown(admin_shutdown))
            .await
            .map_err(ServerError::ServeAdmin)
        };
        // Own listener futures directly: dropping serve must finish listener
        // cleanup, not merely schedule cancellation of detached child tasks.
        let admin_serving = AdminServing {
            serving: Box::pin(admin_serving),
            _guard: admin_guard,
        };
        tokio::pin!(public_serving, admin_serving, web_serving);
        tracing::info!(
            event = "service_ready",
            version = env!("CARGO_PKG_VERSION"),
            public_address = %self.public_address
        );

        let mut public_done = false;
        let mut admin_done = false;
        let mut web_done = !web_enabled;
        let first = tokio::select! {
            () = shutdown => None,
            result = &mut public_serving => {
                public_done = true;
                Some(result)
            },
            result = &mut admin_serving => {
                admin_done = true;
                Some(result)
            },
            result = &mut web_serving, if web_enabled => {
                web_done = true;
                Some(result)
            },
        };
        let _ = shutdown_tx.send(true);

        let mut outcome = match first {
            None => Ok(()),
            Some(Ok(())) => Err(ServerError::ListenerStopped),
            Some(Err(error)) => Err(error),
        };
        let drained = tokio::time::timeout(shutdown_timeout, async {
            tokio::join!(
                async {
                    if public_done {
                        Ok(())
                    } else {
                        public_serving.await
                    }
                },
                async {
                    if admin_done {
                        Ok(())
                    } else {
                        admin_serving.await
                    }
                },
                async { if web_done { Ok(()) } else { web_serving.await } },
            )
        })
        .await;
        match drained {
            Ok((public, admin, web)) => {
                for result in [public, admin, web] {
                    if outcome.is_ok() {
                        outcome = result;
                    }
                }
            }
            Err(_) => {
                tracing::warn!(
                    event = "shutdown_forced",
                    timeout_millis = shutdown_timeout.as_millis() as u64
                );
                // Forced cancellation is not a clean checkpoint boundary.
                if outcome.is_ok() {
                    outcome = Err(ServerError::ShutdownTimeout);
                }
            }
        }
        if outcome.is_ok() {
            outcome = shutdown_state
                .blocking()
                .execute(|database| database.normalize_for_clean_shutdown())
                .await
                .map_err(ServerError::DatabaseShutdown);
        }
        outcome
    }
}
#[cfg(unix)]
fn validate_admin_socket_parent(path: &Path) -> Result<(), ServerError> {
    use std::os::unix::fs::PermissionsExt;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let metadata = std::fs::symlink_metadata(parent).map_err(|source| ServerError::BindAdmin {
        path: path.to_owned(),
        source,
    })?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(ServerError::InsecureAdminDirectory(parent.to_owned()));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent).and_then(|file| file.sync_all())
}

#[cfg(unix)]
async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

#[cfg(unix)]
const ADMIN_ARTIFACT_MAX_BYTES: u64 = 4096;
#[cfg(unix)]
const ADMIN_MARKER_VERSION: &str = "2";
#[cfg(unix)]
const ADMIN_LOCK_SUFFIX: &str = ".lock";
#[cfg(unix)]
const ADMIN_MARKER_SUFFIX: &str = ".marker";
#[cfg(unix)]
const ADMIN_PENDING_ALPHABET: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
#[cfg(unix)]
const ADMIN_PENDING_MAX_CANDIDATES: u64 = 4096;
#[cfg(unix)]
const ADMIN_PENDING_MAX_NAME_BYTES: usize = 256;

#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArtifactIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArtifactKind {
    Socket,
    Other,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Artifact {
    identity: ArtifactIdentity,
    kind: ArtifactKind,
}

#[cfg(unix)]
struct OwnerLock {
    path: PathBuf,
    file: File,
    identity: ArtifactIdentity,
}

#[cfg(unix)]
impl OwnerLock {
    fn verify(&self) -> io::Result<()> {
        if validate_open_private_file(&self.path, &self.file)? != self.identity {
            return Err(invalid_data("administrative owner lock changed"));
        }
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Debug)]
enum MarkerState {
    Empty,
    Pending {
        owner_lock: ArtifactIdentity,
        identity: ArtifactIdentity,
        pending_name: OsString,
    },
    Published {
        owner_lock: ArtifactIdentity,
        identity: ArtifactIdentity,
        pending_name: Option<OsString>,
    },
}

#[cfg(unix)]
struct MarkerFile {
    path: PathBuf,
    file: File,
    identity: ArtifactIdentity,
}

#[cfg(unix)]
struct AdminSocketGuard {
    path: PathBuf,
    marker: MarkerFile,
    owner_lock: OwnerLock,
    identity: ArtifactIdentity,
}

#[cfg(unix)]
struct AdminSocket {
    // Struct fields drop in declaration order. Keep the listener before the
    // guard so an unserved Server closes the listener before removing the
    // pathname, clearing the marker, and releasing the owner lock.
    listener: Option<tokio::net::UnixListener>,
    guard: AdminSocketGuard,
}

#[cfg(unix)]
struct AdminServing<F> {
    serving: Pin<Box<F>>,
    _guard: AdminSocketGuard,
}

#[cfg(unix)]
impl<F> Future for AdminServing<F>
where
    F: Future<Output = Result<(), ServerError>> + Send,
{
    type Output = Result<(), ServerError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // AdminServing is pinned as a whole. Poll only the serving future in
        // place; its field is declared before _guard so cancellation drops
        // the listener before the owner lock.
        self.get_mut().serving.as_mut().poll(context)
    }
}

#[cfg(unix)]
impl AdminSocket {
    fn path(&self) -> &Path {
        self.guard.path()
    }

    fn into_parts(self) -> io::Result<(tokio::net::UnixListener, AdminSocketGuard)> {
        let Self { listener, guard } = self;
        listener
            .ok_or_else(|| invalid_data("administrative listener is unavailable"))
            .map(|listener| (listener, guard))
    }
}

#[cfg(unix)]
fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

#[cfg(unix)]
fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(unix)]
fn artifact(path: &Path) -> io::Result<Option<Artifact>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let identity = ArtifactIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    let kind = if metadata.file_type().is_socket() {
        ArtifactKind::Socket
    } else {
        ArtifactKind::Other
    };
    Ok(Some(Artifact { identity, kind }))
}

#[cfg(unix)]
fn validate_private_file_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    max_bytes: u64,
) -> io::Result<ArtifactIdentity> {
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(invalid_data("administrative sidecar is not a regular file"));
    }
    if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
        return Err(invalid_data("administrative sidecar is not private"));
    }
    if metadata.len() > max_bytes {
        return Err(invalid_data("administrative sidecar is oversized"));
    }
    let identity = ArtifactIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    let path_metadata = fs::symlink_metadata(path)?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
        || path_metadata.dev() != identity.device
        || path_metadata.ino() != identity.inode
    {
        return Err(invalid_data("administrative sidecar changed while opening"));
    }
    Ok(identity)
}

#[cfg(unix)]
fn validate_open_private_file(path: &Path, file: &File) -> io::Result<ArtifactIdentity> {
    let identity =
        validate_private_file_metadata(path, &file.metadata()?, ADMIN_ARTIFACT_MAX_BYTES)?;
    let file_metadata = file.metadata()?;
    if file_metadata.dev() != identity.device || file_metadata.ino() != identity.inode {
        return Err(invalid_data("administrative sidecar changed while opening"));
    }
    Ok(identity)
}

#[cfg(unix)]
fn open_owner_lock(path: &Path) -> io::Result<OwnerLock> {
    for _ in 0..2 {
        let exists = match fs::symlink_metadata(path) {
            Ok(metadata) => {
                validate_private_file_metadata(path, &metadata, ADMIN_ARTIFACT_MAX_BYTES)?;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        options.mode(0o600);
        if exists {
            options.create(false);
        } else {
            options.create_new(true);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if !exists && error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let identity = validate_open_private_file(path, &file)?;
        file.try_lock_exclusive()?;
        let owner_lock = OwnerLock {
            path: path.to_owned(),
            file,
            identity,
        };
        owner_lock.verify()?;
        return Ok(owner_lock);
    }
    Err(invalid_data("administrative owner lock creation raced"))
}

#[cfg(unix)]
fn marker_name(name: &OsStr) -> io::Result<String> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > ADMIN_PENDING_MAX_NAME_BYTES
        || !bytes
            .iter()
            .all(|byte| ADMIN_PENDING_ALPHABET.contains(byte))
    {
        return Err(invalid_data(
            "administrative marker has an invalid pending path",
        ));
    }
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

#[cfg(unix)]
fn decode_marker_name(encoded: &str) -> io::Result<OsString> {
    if encoded.is_empty() || encoded.len() > 512 || encoded.len() % 2 != 0 {
        return Err(invalid_data(
            "administrative marker has an invalid pending path",
        ));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let chars = encoded.as_bytes();
    for pair in chars.chunks_exact(2) {
        let high = hex_digit(pair[0]).ok_or_else(|| invalid_data("invalid marker hex"))?;
        let low = hex_digit(pair[1]).ok_or_else(|| invalid_data("invalid marker hex"))?;
        bytes.push((high << 4) | low);
    }
    let name = OsString::from_vec(bytes);
    marker_name(&name)?;
    Ok(name)
}

#[cfg(unix)]
fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(unix)]
fn marker_number(text: &str) -> io::Result<u64> {
    if text.is_empty() || text.len() > 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_data(
            "administrative marker has an invalid identity",
        ));
    }
    let number = text
        .parse::<u64>()
        .map_err(|_| invalid_data("administrative marker has an invalid identity"))?;
    if number.to_string() != text {
        return Err(invalid_data(
            "administrative marker has an invalid identity",
        ));
    }
    Ok(number)
}

#[cfg(unix)]
fn marker_field<'a>(line: &'a str, prefix: &str) -> io::Result<&'a str> {
    line.strip_prefix(prefix)
        .ok_or_else(|| invalid_data("administrative marker format is invalid"))
}

#[cfg(unix)]
fn parse_marker(bytes: &[u8]) -> io::Result<MarkerState> {
    if bytes.len() as u64 > ADMIN_ARTIFACT_MAX_BYTES {
        return Err(invalid_data("administrative marker is oversized"));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_data("administrative marker is not ASCII"))?;
    let lines: Vec<&str> = text
        .strip_suffix('\n')
        .map_or_else(Vec::new, |text| text.split('\n').collect());
    if lines == ["version=2", "state=empty"] {
        return Ok(MarkerState::Empty);
    }
    let identity = |dev: &str, ino: &str| -> io::Result<ArtifactIdentity> {
        Ok(ArtifactIdentity {
            device: marker_number(marker_field(dev, "dev=")?)?,
            inode: marker_number(marker_field(ino, "ino=")?)?,
        })
    };
    let owner_lock = |dev: &str, ino: &str| -> io::Result<ArtifactIdentity> {
        Ok(ArtifactIdentity {
            device: marker_number(marker_field(dev, "lock-dev=")?)?,
            inode: marker_number(marker_field(ino, "lock-ino=")?)?,
        })
    };
    match lines.as_slice() {
        [
            "version=2",
            "state=pending",
            "type=socket",
            lock_dev,
            lock_ino,
            dev,
            ino,
            pending,
        ] => {
            let encoded = marker_field(pending, "pending=")?;
            Ok(MarkerState::Pending {
                owner_lock: owner_lock(lock_dev, lock_ino)?,
                identity: identity(dev, ino)?,
                pending_name: decode_marker_name(encoded)?,
            })
        }
        [
            "version=2",
            "state=published",
            "type=socket",
            lock_dev,
            lock_ino,
            dev,
            ino,
            pending,
        ] => {
            let encoded = marker_field(pending, "pending=")?;
            Ok(MarkerState::Published {
                owner_lock: owner_lock(lock_dev, lock_ino)?,
                identity: identity(dev, ino)?,
                pending_name: Some(decode_marker_name(encoded)?),
            })
        }
        [
            "version=2",
            "state=published",
            "type=socket",
            lock_dev,
            lock_ino,
            dev,
            ino,
        ] => Ok(MarkerState::Published {
            owner_lock: owner_lock(lock_dev, lock_ino)?,
            identity: identity(dev, ino)?,
            pending_name: None,
        }),
        _ => Err(invalid_data("administrative marker format is invalid")),
    }
}

#[cfg(unix)]
fn marker_bytes(state: &MarkerState) -> io::Result<Vec<u8>> {
    let text = match state {
        MarkerState::Empty => format!("version={ADMIN_MARKER_VERSION}\nstate=empty\n"),
        MarkerState::Pending {
            owner_lock,
            identity,
            pending_name,
        } => format!(
            "version={ADMIN_MARKER_VERSION}\nstate=pending\ntype=socket\nlock-dev={}\nlock-ino={}\ndev={}\nino={}\npending={}\n",
            owner_lock.device,
            owner_lock.inode,
            identity.device,
            identity.inode,
            marker_name(pending_name)?
        ),
        MarkerState::Published {
            owner_lock,
            identity,
            pending_name,
        } => {
            let mut text = format!(
                "version={ADMIN_MARKER_VERSION}\nstate=published\ntype=socket\nlock-dev={}\nlock-ino={}\ndev={}\nino={}\n",
                owner_lock.device, owner_lock.inode, identity.device, identity.inode
            );
            if let Some(pending_name) = pending_name {
                text.push_str("pending=");
                text.push_str(&marker_name(pending_name)?);
                text.push('\n');
            }
            text
        }
    };
    Ok(text.into_bytes())
}

#[cfg(unix)]
impl MarkerFile {
    fn write_state(&mut self, state: &MarkerState, owner_lock: &OwnerLock) -> io::Result<()> {
        let bytes = marker_bytes(state)?;
        if bytes.len() as u64 > ADMIN_ARTIFACT_MAX_BYTES {
            return Err(invalid_data("administrative marker is oversized"));
        }
        owner_lock.verify()?;
        if validate_open_private_file(&self.path, &self.file)? != self.identity {
            return Err(invalid_data("administrative marker changed"));
        }
        // The lock check is deliberately immediately before the in-place
        // marker transition. A later pathname replacement is fail-closed.
        owner_lock.verify()?;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        sync_parent(&self.path)?;
        owner_lock.verify()?;
        if validate_open_private_file(&self.path, &self.file)? != self.identity {
            return Err(invalid_data("administrative marker changed"));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn open_marker(path: &Path, owner_lock: &OwnerLock) -> io::Result<(MarkerFile, MarkerState)> {
    for _ in 0..2 {
        let exists = match fs::symlink_metadata(path) {
            Ok(metadata) => {
                validate_private_file_metadata(path, &metadata, ADMIN_ARTIFACT_MAX_BYTES)?;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        options.mode(0o600);
        if exists {
            options.create(false);
        } else {
            options.create_new(true);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if !exists && error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let identity = validate_open_private_file(path, &file)?;
        let mut marker = MarkerFile {
            path: path.to_owned(),
            file,
            identity,
        };
        let state = if exists {
            let mut bytes = Vec::new();
            marker.file.seek(SeekFrom::Start(0))?;
            (&mut marker.file)
                .take(ADMIN_ARTIFACT_MAX_BYTES.saturating_add(1))
                .read_to_end(&mut bytes)?;
            parse_marker(&bytes)?
        } else {
            marker.write_state(&MarkerState::Empty, owner_lock)?;
            MarkerState::Empty
        };
        return Ok((marker, state));
    }
    Err(invalid_data("administrative marker creation raced"))
}

#[cfg(unix)]
fn remove_owned(
    owner_lock: &OwnerLock,
    path: &Path,
    expected: ArtifactIdentity,
) -> io::Result<bool> {
    owner_lock.verify()?;
    let Some(actual) = artifact(path)? else {
        return Ok(false);
    };
    if actual.kind != ArtifactKind::Socket || actual.identity != expected {
        return Err(invalid_data("administrative socket identity changed"));
    }
    // POSIX has no atomic conditional unlink by dev/inode. Recheck both
    // identities immediately before unlink and rely on the owner lock plus
    // the private parent directory for conforming-process serialization.
    owner_lock.verify()?;
    let Some(actual) = artifact(path)? else {
        return Ok(false);
    };
    if actual.kind != ArtifactKind::Socket || actual.identity != expected {
        return Err(invalid_data("administrative socket identity changed"));
    }
    fs::remove_file(path)?;
    Ok(true)
}

#[cfg(unix)]
fn recover_marker(
    owner_lock: &OwnerLock,
    marker: &mut MarkerFile,
    state: MarkerState,
    final_path: &Path,
    parent: Option<&Path>,
) -> io::Result<()> {
    owner_lock.verify()?;
    let (marker_owner_lock, identity, pending_name) = match state {
        MarkerState::Empty => {
            if artifact(final_path)?.is_some() {
                return Err(invalid_data("unmarked administrative socket is preserved"));
            }
            return Ok(());
        }
        MarkerState::Pending {
            owner_lock,
            identity,
            pending_name,
        }
        | MarkerState::Published {
            owner_lock,
            identity,
            pending_name: Some(pending_name),
        } => (owner_lock, identity, Some(pending_name)),
        MarkerState::Published {
            owner_lock,
            identity,
            pending_name: None,
        } => (owner_lock, identity, None),
    };
    if marker_owner_lock != owner_lock.identity {
        return Err(invalid_data("administrative marker owner lock changed"));
    }
    let pending_path = pending_name
        .map(|name| parent.map_or_else(|| PathBuf::from(&name), |parent| parent.join(&name)));
    if pending_path
        .as_deref()
        .is_some_and(|path| path == final_path)
    {
        return Err(invalid_data(
            "administrative marker pending path is final path",
        ));
    }
    if pending_path.as_deref().is_some_and(|path| {
        path.as_os_str().as_bytes().len() > final_path.as_os_str().as_bytes().len()
    }) {
        return Err(invalid_data(
            "administrative marker pending path exceeds final path budget",
        ));
    }
    for path in [Some(final_path), pending_path.as_deref()]
        .into_iter()
        .flatten()
    {
        if let Some(actual) = artifact(path)? {
            if actual.kind != ArtifactKind::Socket || actual.identity != identity {
                return Err(invalid_data("administrative socket identity changed"));
            }
        }
    }
    remove_owned(owner_lock, final_path, identity)?;
    if let Some(path) = pending_path {
        remove_owned(owner_lock, &path, identity)?;
    }
    sync_parent(final_path)?;
    marker.write_state(&MarkerState::Empty, owner_lock)
}

#[cfg(unix)]
fn pending_candidate(index: u64, max_bytes: usize) -> Option<OsString> {
    let base = ADMIN_PENDING_ALPHABET.len() as u64;
    let mut value = index;
    let mut bytes = Vec::new();
    loop {
        bytes.push(ADMIN_PENDING_ALPHABET[(value % base) as usize]);
        value /= base;
        if value == 0 {
            break;
        }
    }
    bytes.reverse();
    if bytes.len() > max_bytes || bytes.len() > ADMIN_PENDING_MAX_NAME_BYTES {
        None
    } else {
        Some(OsString::from_vec(bytes))
    }
}

#[cfg(unix)]
async fn bind_admin_listener(path: &Path) -> io::Result<AdminSocket> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let final_name = path
        .file_name()
        .ok_or_else(|| invalid_data("administrative socket has no final name"))?;
    let final_name_bytes = final_name.as_bytes();
    if final_name_bytes.is_empty() {
        return Err(invalid_data("administrative socket has no final name"));
    }
    let lock_path = sidecar_path(path, ADMIN_LOCK_SUFFIX);
    let owner_lock = open_owner_lock(&lock_path)?;
    let marker_path = sidecar_path(path, ADMIN_MARKER_SUFFIX);
    let (mut marker, marker_state) = open_marker(&marker_path, &owner_lock)?;
    recover_marker(&owner_lock, &mut marker, marker_state, path, parent)?;

    let (listener, pending_path, identity) = {
        let mut selected = None;
        for candidate_index in 0..ADMIN_PENDING_MAX_CANDIDATES {
            let Some(pending_name) = pending_candidate(candidate_index, final_name_bytes.len())
            else {
                break;
            };
            if pending_name.as_os_str().as_bytes() == final_name_bytes {
                continue;
            }
            let pending_path = parent.map_or_else(
                || PathBuf::from(&pending_name),
                |parent| parent.join(&pending_name),
            );
            if pending_path.as_os_str().as_bytes().len() > path.as_os_str().as_bytes().len() {
                continue;
            }
            if artifact(&pending_path)?.is_some() {
                continue;
            }
            match tokio::net::UnixListener::bind(&pending_path) {
                Ok(listener) => {
                    let Some(artifact) = artifact(&pending_path)? else {
                        return Err(invalid_data("pending administrative socket disappeared"));
                    };
                    if artifact.kind != ArtifactKind::Socket {
                        return Err(invalid_data("pending administrative path is not a socket"));
                    }
                    selected = Some((listener, pending_path, artifact.identity));
                    break;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::AlreadyExists | io::ErrorKind::AddrInUse
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        selected.ok_or_else(|| {
            invalid_data("administrative pending socket name candidates exhausted")
        })?
    };
    owner_lock.verify()?;
    fs::set_permissions(&pending_path, fs::Permissions::from_mode(0o600))?;
    let pending_name = pending_path
        .file_name()
        .ok_or_else(|| invalid_data("pending administrative socket has no name"))?
        .to_os_string();
    marker.write_state(
        &MarkerState::Pending {
            owner_lock: owner_lock.identity,
            identity,
            pending_name: pending_name.clone(),
        },
        &owner_lock,
    )?;
    // Publication is protected by the same pinned lock identity as marker
    // transitions. The final unlink/link checks remain identity-based.
    owner_lock.verify()?;
    fs::hard_link(&pending_path, path)?;
    sync_parent(path)?;
    marker.write_state(
        &MarkerState::Published {
            owner_lock: owner_lock.identity,
            identity,
            pending_name: Some(pending_name),
        },
        &owner_lock,
    )?;
    remove_owned(&owner_lock, &pending_path, identity)?;
    sync_parent(path)?;
    marker.write_state(
        &MarkerState::Published {
            owner_lock: owner_lock.identity,
            identity,
            pending_name: None,
        },
        &owner_lock,
    )?;
    Ok(AdminSocket {
        listener: Some(listener),
        guard: AdminSocketGuard {
            path: path.to_owned(),
            marker,
            owner_lock,
            identity,
        },
    })
}

#[cfg(unix)]
impl AdminSocketGuard {
    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
impl Drop for AdminSocketGuard {
    fn drop(&mut self) {
        // The owning AdminSocket closes its listener before dropping this
        // guard. Only an identity-matching socket may be removed. A replaced
        // path deliberately leaves the published marker behind for fail-closed
        // recovery on the next startup.
        if self.owner_lock.verify().is_ok()
            && remove_owned(&self.owner_lock, &self.path, self.identity).unwrap_or(false)
        {
            let _ = sync_parent(&self.path);
            let _ = self
                .marker
                .write_state(&MarkerState::Empty, &self.owner_lock);
        }
        let _ = FileExt::unlock(&self.owner_lock.file);
    }
}

#[cfg(not(unix))]
pub struct Server;

#[cfg(not(unix))]
impl Server {
    pub async fn bind(_config: Config) -> Result<Self, ServerError> {
        Err(ServerError::AdminSocketUnsupported)
    }

    pub const fn public_address(&self) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    }

    pub fn admin_socket_path(&self) -> &Path {
        Path::new("")
    }

    pub async fn serve<S>(self, _shutdown: S) -> Result<(), ServerError>
    where
        S: Future<Output = ()> + Send + 'static,
    {
        Err(ServerError::AdminSocketUnsupported)
    }
}
