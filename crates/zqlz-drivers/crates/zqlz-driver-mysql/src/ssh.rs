//! MySQL SSH Tunnel Support
//!
//! Provides SSH tunnel functionality for secure MySQL connections
//! through bastion/jump hosts. This module handles SSH session management,
//! authentication, and port forwarding.
//!
//! This implementation shares the core SSH tunneling logic with the PostgreSQL
//! module but is tailored for MySQL's default port and connection patterns.

use anyhow::{Context, Result};
use ssh2::Session;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use zqlz_core::security::{SshAuthMethod, SshTunnelConfig};

/// Error types for MySQL SSH tunnel operations
#[derive(Debug, thiserror::Error)]
pub enum MysqlSshTunnelError {
    /// Failed to connect to SSH server
    #[error("Failed to connect to SSH server {host}:{port}: {source}")]
    ConnectionFailed {
        host: String,
        port: u16,
        source: std::io::Error,
    },

    /// SSH handshake failed
    #[error("SSH handshake failed: {0}")]
    HandshakeFailed(String),

    /// Authentication failed
    #[error("SSH authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Failed to establish port forwarding
    #[error("Failed to establish port forwarding: {0}")]
    PortForwardingFailed(String),

    /// SSH agent not available
    #[error("SSH agent not available: {0}")]
    AgentNotAvailable(String),

    /// Private key file not found
    #[error("Private key file not found: {path}")]
    PrivateKeyNotFound { path: String },

    /// Invalid private key format
    #[error("Invalid private key format: {0}")]
    InvalidPrivateKey(String),

    /// Tunnel is closed
    #[error("SSH tunnel is closed")]
    TunnelClosed,
}

/// An SSH tunnel for MySQL connections
///
/// This struct manages an SSH session and provides local port forwarding
/// to a remote MySQL server. When dropped, all resources are cleaned up.
///
/// # Example
///
/// ```ignore
/// use zqlz_drivers::mysql::MysqlSshTunnel;
/// use zqlz_core::security::SshTunnelConfig;
///
/// let config = SshTunnelConfig::with_password("bastion.example.com", "user", "password");
/// let tunnel = MysqlSshTunnel::new(&config, "db.internal", 3306)?;
///
/// // Connect to the tunnel's local port
/// let url = format!("mysql://user:pass@127.0.0.1:{}/mydb", tunnel.local_port());
/// ```
pub struct MysqlSshTunnel {
    session: Session,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
    is_running: Arc<AtomicBool>,
    forward_thread: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for MysqlSshTunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSshTunnel")
            .field("local_port", &self.local_port)
            .field("remote_host", &self.remote_host)
            .field("remote_port", &self.remote_port)
            .field("is_running", &self.is_running.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl MysqlSshTunnel {
    /// Default MySQL port
    pub const DEFAULT_MYSQL_PORT: u16 = 3306;

    /// Create a new SSH tunnel for MySQL
    ///
    /// Establishes an SSH connection to the configured host and sets up
    /// local port forwarding to the remote MySQL server.
    ///
    /// # Arguments
    ///
    /// * `config` - SSH tunnel configuration
    /// * `remote_host` - The MySQL server hostname (as seen from the SSH server)
    /// * `remote_port` - The MySQL server port (typically 3306)
    ///
    /// # Returns
    ///
    /// A new `MysqlSshTunnel` with a local port ready for connections.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = SshTunnelConfig::with_password("bastion.example.com", "user", "password");
    /// let tunnel = MysqlSshTunnel::new(&config, "mysql.internal", 3306)?;
    /// println!("Connect to: 127.0.0.1:{}", tunnel.local_port());
    /// ```
    pub fn new(config: &SshTunnelConfig, remote_host: &str, remote_port: u16) -> Result<Self> {
        config.validate().context("Invalid SSH configuration")?;

        info!(
            ssh_host = %config.host,
            ssh_port = config.port,
            remote_host = %remote_host,
            remote_port = remote_port,
            "Establishing MySQL SSH tunnel"
        );

        let tcp = TcpStream::connect_timeout(
            &format!("{}:{}", config.host, config.port).parse()?,
            Duration::from_secs(config.timeout_seconds as u64),
        )
        .map_err(|e| MysqlSshTunnelError::ConnectionFailed {
            host: config.host.clone(),
            port: config.port,
            source: e,
        })?;

        tcp.set_read_timeout(Some(Duration::from_secs(config.timeout_seconds as u64)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(config.timeout_seconds as u64)))?;

        let mut session = Session::new()?;
        session.set_tcp_stream(tcp);
        session.handshake().map_err(|e| {
            MysqlSshTunnelError::HandshakeFailed(format!("SSH handshake failed: {}", e))
        })?;

        authenticate(&mut session, &config.username, &config.auth)?;

        if config.keepalive_seconds > 0 {
            session.set_keepalive(true, config.keepalive_seconds);
        }

        let local_port = find_available_port()?;

        let is_running = Arc::new(AtomicBool::new(true));
        let forward_thread = start_forwarding_thread(
            session.clone(),
            local_port,
            remote_host.to_string(),
            remote_port,
            is_running.clone(),
        );

        info!(
            local_port = local_port,
            remote = format!("{}:{}", remote_host, remote_port),
            "MySQL SSH tunnel established"
        );

        Ok(Self {
            session,
            local_port,
            remote_host: remote_host.to_string(),
            remote_port,
            is_running,
            forward_thread: Some(forward_thread),
        })
    }

    /// Create a new SSH tunnel using the default MySQL port (3306)
    ///
    /// This is a convenience method that calls `new()` with port 3306.
    pub fn with_default_port(config: &SshTunnelConfig, remote_host: &str) -> Result<Self> {
        Self::new(config, remote_host, Self::DEFAULT_MYSQL_PORT)
    }

    /// Get the local port number
    ///
    /// Use this port to connect to the tunneled MySQL server.
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    /// Get the remote host
    pub fn remote_host(&self) -> &str {
        &self.remote_host
    }

    /// Get the remote port
    pub fn remote_port(&self) -> u16 {
        self.remote_port
    }

    /// Check if the tunnel is still active
    pub fn is_active(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get a MySQL connection URL for this tunnel
    ///
    /// Returns a connection URL suitable for mysql_async.
    ///
    /// # Arguments
    ///
    /// * `user` - MySQL username
    /// * `password` - MySQL password (will be URL-encoded if needed)
    /// * `database` - Database name
    ///
    /// # Example
    ///
    /// ```ignore
    /// let url = tunnel.mysql_url("root", "secret", "myapp");
    /// // Returns: mysql://root:secret@127.0.0.1:12345/myapp
    /// ```
    pub fn mysql_url(&self, user: &str, password: &str, database: &str) -> String {
        format!(
            "mysql://{}:{}@127.0.0.1:{}/{}",
            user, password, self.local_port, database
        )
    }

    /// Get a MySQL connection URL without credentials
    ///
    /// Returns a connection URL template without embedded credentials.
    /// Useful when credentials are provided separately.
    pub fn mysql_url_template(&self, database: &str) -> String {
        format!("mysql://127.0.0.1:{}/{}", self.local_port, database)
    }
}

impl Drop for MysqlSshTunnel {
    fn drop(&mut self) {
        info!(local_port = self.local_port, "Closing MySQL SSH tunnel");

        self.is_running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.forward_thread.take() {
            let _ = handle.join();
        }

        if let Err(e) = self.session.disconnect(None, "Tunnel closed", None) {
            warn!("Error disconnecting SSH session: {}", e);
        }

        debug!("MySQL SSH tunnel closed");
    }
}

/// Authenticate to the SSH server using the configured method
fn authenticate(session: &mut Session, username: &str, auth: &SshAuthMethod) -> Result<()> {
    match auth {
        SshAuthMethod::Password { password } => {
            debug!("Authenticating with password");
            session
                .userauth_password(username, password)
                .map_err(|e| MysqlSshTunnelError::AuthenticationFailed(e.to_string()))?;
        }
        SshAuthMethod::PrivateKey { path, passphrase } => {
            debug!(path = %path.display(), "Authenticating with private key");
            if !path.exists() {
                return Err(MysqlSshTunnelError::PrivateKeyNotFound {
                    path: path.display().to_string(),
                }
                .into());
            }
            session
                .userauth_pubkey_file(username, None, path, passphrase.as_deref())
                .map_err(|e| MysqlSshTunnelError::InvalidPrivateKey(e.to_string()))?;
        }
        SshAuthMethod::Agent => {
            debug!("Authenticating with SSH agent");
            authenticate_with_agent(session, username)?;
        }
    }

    if !session.authenticated() {
        return Err(MysqlSshTunnelError::AuthenticationFailed(
            "Authentication not confirmed".to_string(),
        )
        .into());
    }

    debug!("SSH authentication successful");
    Ok(())
}

/// Authenticate using SSH agent
fn authenticate_with_agent(session: &mut Session, username: &str) -> Result<()> {
    let mut agent = session
        .agent()
        .map_err(|e| MysqlSshTunnelError::AgentNotAvailable(e.to_string()))?;

    agent
        .connect()
        .map_err(|e| MysqlSshTunnelError::AgentNotAvailable(e.to_string()))?;

    agent.list_identities().map_err(|e| {
        MysqlSshTunnelError::AgentNotAvailable(format!("Failed to list identities: {}", e))
    })?;

    let identities = agent.identities()?;

    if identities.is_empty() {
        return Err(
            MysqlSshTunnelError::AgentNotAvailable("No identities in agent".to_string()).into(),
        );
    }

    for identity in identities {
        if agent.userauth(username, &identity).is_ok() && session.authenticated() {
            debug!("Authenticated with agent identity");
            return Ok(());
        }
    }

    Err(MysqlSshTunnelError::AuthenticationFailed("No agent identity worked".to_string()).into())
}

/// Find an available local port for the tunnel
fn find_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Start the port forwarding thread
fn start_forwarding_thread(
    session: Session,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
    is_running: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = match TcpListener::bind(format!("127.0.0.1:{}", local_port)) {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind local port {}: {}", local_port, e);
                return;
            }
        };

        listener.set_nonblocking(true).ok();

        while is_running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((local_stream, _)) => {
                    let session_clone = session.clone();
                    let remote_host_clone = remote_host.clone();
                    let is_running_clone = is_running.clone();

                    thread::spawn(move || {
                        if let Err(e) = handle_connection(
                            local_stream,
                            &session_clone,
                            &remote_host_clone,
                            remote_port,
                            &is_running_clone,
                        ) {
                            debug!("Connection handler error: {}", e);
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    if is_running.load(Ordering::SeqCst) {
                        warn!("Error accepting connection: {}", e);
                    }
                    break;
                }
            }
        }

        debug!("Port forwarding thread exiting");
    })
}

/// Handle a single forwarded connection
fn handle_connection(
    mut local_stream: TcpStream,
    session: &Session,
    remote_host: &str,
    remote_port: u16,
    is_running: &Arc<AtomicBool>,
) -> Result<()> {
    local_stream.set_nonblocking(false)?;
    local_stream.set_read_timeout(Some(Duration::from_millis(100)))?;

    let mut channel = session
        .channel_direct_tcpip(remote_host, remote_port, None)
        .map_err(|e| MysqlSshTunnelError::PortForwardingFailed(e.to_string()))?;

    session.set_blocking(false);

    let mut local_buf = [0u8; 8192];
    let mut remote_buf = [0u8; 8192];

    while is_running.load(Ordering::SeqCst) {
        let mut activity = false;

        match local_stream.read(&mut local_buf) {
            Ok(0) => break,
            Ok(n) => {
                session.set_blocking(true);
                channel.write_all(&local_buf[..n])?;
                session.set_blocking(false);
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e.into()),
        }

        match channel.read(&mut remote_buf) {
            Ok(0) => {
                if channel.eof() {
                    break;
                }
            }
            Ok(n) => {
                local_stream.write_all(&remote_buf[..n])?;
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }

        if !activity {
            thread::sleep(Duration::from_millis(1));
        }
    }

    session.set_blocking(true);
    let _ = channel.send_eof();
    let _ = channel.wait_close();

    Ok(())
}

#[cfg(test)]
mod tests;
