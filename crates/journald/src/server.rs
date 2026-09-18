use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use journal_storage_sqlite::Database;
use journal_storage_sqlite::StorageError;
use thiserror::Error;
#[cfg(unix)]
use tokio::sync::watch;
use tokio::task::JoinError;
#[cfg(unix)]
use tokio::task::JoinSet;

use crate::config::Config;
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
    #[error("administrative Unix sockets are unsupported on this platform")]
    AdminSocketUnsupported,
}

#[cfg(unix)]
pub struct Server {
    public_listener: tokio::net::TcpListener,
    public_address: SocketAddr,
    admin_listener: tokio::net::UnixListener,
    admin_socket: AdminSocketGuard,
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

        let database_path = config.database_path.clone();
        let database = tokio::task::spawn_blocking(move || Database::open(database_path)).await??;
        let state = ServiceState::new(database, config.blocking_limit)
            .map_err(|_| ServerError::InvalidConfig("blocking limit must be positive"))?;
        let web_listener = if let Some(web) = config.web {
            let viewer = web.viewer.clone();
            state
                .blocking()
                .execute(move |database| {
                    Ok(journal_service::BootstrapService::new(database.clone())
                        .shared_viewer(&viewer)
                        .spaces(&journal_protocol::PageQuery::new(None, Some(1))))
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

        let admin_listener =
            tokio::net::UnixListener::bind(&config.admin_socket_path).map_err(|source| {
                ServerError::BindAdmin {
                    path: config.admin_socket_path.clone(),
                    source,
                }
            })?;
        let admin_socket = AdminSocketGuard::new(config.admin_socket_path.clone())?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            &config.admin_socket_path,
            std::fs::Permissions::from_mode(0o600),
        )
        .map_err(|source| ServerError::ProtectAdmin {
            path: config.admin_socket_path,
            source,
        })?;

        Ok(Self {
            public_listener,
            public_address,
            admin_listener,
            admin_socket,
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
        self.admin_socket.path()
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
        let admin_owner = std::fs::metadata(self.admin_socket.path())
            .map_err(ServerError::ServeAdmin)?
            .uid();
        let admin_router =
            admin_router.layer(axum::Extension(crate::http::AdminOwner(admin_owner)));
        let shutdown_timeout = self.shutdown_timeout;
        let mut listeners = JoinSet::new();
        if let Some((listener, viewer)) = self.web_listener {
            let router = web_router_with_timeout(
                self.state,
                viewer,
                self.max_body_bytes,
                self.body_read_timeout,
                shutdown_rx.clone(),
            );
            let web_shutdown = shutdown_rx.clone();
            listeners.spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(wait_for_shutdown(web_shutdown))
                    .await
                    .map_err(ServerError::ServeWeb)
            });
        }
        let public_shutdown = shutdown_rx.clone();
        listeners.spawn(async move {
            axum::serve(self.public_listener, public_router)
                .with_graceful_shutdown(wait_for_shutdown(public_shutdown))
                .await
                .map_err(ServerError::ServePublic)
        });
        let admin_shutdown = shutdown_rx;
        listeners.spawn(async move {
            axum::serve(
                self.admin_listener,
                admin_router.into_make_service_with_connect_info::<crate::http::AdminPeer>(),
            )
            .with_graceful_shutdown(wait_for_shutdown(admin_shutdown))
            .await
            .map_err(ServerError::ServeAdmin)
        });
        tracing::info!(
            event = "service_ready",
            version = env!("CARGO_PKG_VERSION"),
            public_address = %self.public_address
        );

        let first = tokio::select! {
            () = shutdown => None,
            result = listeners.join_next() => result,
        };
        let _ = shutdown_tx.send(true);

        let mut outcome = match first {
            None => Ok(()),
            Some(Ok(Ok(()))) => Err(ServerError::ListenerStopped),
            Some(Ok(Err(error))) => Err(error),
            Some(Err(error)) => Err(ServerError::ListenerTask(error)),
        };
        let deadline = tokio::time::Instant::now() + shutdown_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, listeners.join_next()).await {
                Ok(Some(result)) => {
                    if outcome.is_ok() {
                        outcome = match result {
                            Ok(result) => result,
                            Err(error) => Err(ServerError::ListenerTask(error)),
                        };
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!(
                        event = "shutdown_forced",
                        timeout_millis = shutdown_timeout.as_millis() as u64
                    );
                    listeners.abort_all();
                    while listeners.join_next().await.is_some() {}
                    break;
                }
            }
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
    let metadata = std::fs::metadata(parent).map_err(|source| ServerError::BindAdmin {
        path: path.to_owned(),
        source,
    })?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(ServerError::InsecureAdminDirectory(parent.to_owned()));
    }
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

#[cfg(unix)]
struct AdminSocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl AdminSocketGuard {
    fn new(path: PathBuf) -> Result<Self, ServerError> {
        use std::os::unix::fs::MetadataExt;
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|source| ServerError::ProtectAdmin {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
impl Drop for AdminSocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.dev() == self.device && metadata.ino() == self.inode {
                let _ = std::fs::remove_file(&self.path);
            }
        }
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
