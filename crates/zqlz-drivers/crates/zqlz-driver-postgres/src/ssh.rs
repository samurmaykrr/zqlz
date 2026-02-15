//! PostgreSQL SSH Tunnel Support
//!
//! Provides SSH tunnel functionality for secure PostgreSQL connections
//! through bastion/jump hosts. This module handles SSH session management,
//! authentication, and port forwarding.

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

/// Error types for SSH tunnel operations
#[derive(Debug, thiserror::Error)]
pub enum SshTunnelError {
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

/// An SSH tunnel for PostgreSQL connections
///
/// This struct manages an SSH session and provides local port forwarding
/// to a remote PostgreSQL server. When dropped, all resources are cleaned up.
///
/// # Example
///
/// ```ignore
/// use zqlz_drivers::postgres::PostgresSshTunnel;
/// use zqlz_core::security::SshTunnelConfig;
///
/// let config = SshTunnelConfig::with_password("bastion.example.com", "user", "password");
/// let tunnel = PostgresSshTunnel::new(&config, "db.internal", 5432)?;
///
/// // Connect to the tunnel's local port
/// let connection_string = format!("host=127.0.0.1 port={}", tunnel.local_port());
/// ```
pub struct PostgresSshTunnel {
    session: Session,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
    is_running: Arc<AtomicBool>,
    forward_thread: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for PostgresSshTunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSshTunnel")
            .field("local_port", &self.local_port)
            .field("remote_host", &self.remote_host)
            .field("remote_port", &self.remote_port)
            .field("is_running", &self.is_running.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl PostgresSshTunnel {
    /// Create a new SSH tunnel
    ///
    /// Establishes an SSH connection to the configured host and sets up
    /// local port forwarding to the remote database server.
    ///
    /// # Arguments
    ///
    /// * `config` - SSH tunnel configuration
    /// * `remote_host` - The database server hostname (as seen from the SSH server)
    /// * `remote_port` - The database server port (typically 5432 for PostgreSQL)
    ///
    /// # Returns
    ///
    /// A new `PostgresSshTunnel` with a local port ready for connections.
    pub fn new(config: &SshTunnelConfig, remote_host: &str, remote_port: u16) -> Result<Self> {
        config.validate().context("Invalid SSH configuration")?;

        info!(
            ssh_host = %config.host,
            ssh_port = config.port,
            remote_host = %remote_host,
            remote_port = remote_port,
            "Establishing SSH tunnel"
        );

        let tcp = TcpStream::connect_timeout(
            &format!("{}:{}", config.host, config.port).parse()?,
            Duration::from_secs(config.timeout_seconds as u64),
        )
        .map_err(|e| SshTunnelError::ConnectionFailed {
            host: config.host.clone(),
            port: config.port,
            source: e,
        })?;

        tcp.set_read_timeout(Some(Duration::from_secs(config.timeout_seconds as u64)))?;
        tcp.set_write_timeout(Some(Duration::from_secs(config.timeout_seconds as u64)))?;

        let mut session = Session::new()?;
        session.set_tcp_stream(tcp);
        session
            .handshake()
            .map_err(|e| SshTunnelError::HandshakeFailed(format!("SSH handshake failed: {}", e)))?;

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
            "SSH tunnel established"
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

    /// Get the local port number
    ///
    /// Use this port to connect to the tunneled PostgreSQL server.
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

    /// Get the local connection string for PostgreSQL
    ///
    /// Returns a connection string suitable for use with tokio-postgres.
    pub fn local_connection_string(&self) -> String {
        format!("host=127.0.0.1 port={}", self.local_port)
    }
}

impl Drop for PostgresSshTunnel {
    fn drop(&mut self) {
        info!(local_port = self.local_port, "Closing SSH tunnel");

        self.is_running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.forward_thread.take() {
            let _ = handle.join();
        }

        if let Err(e) = self.session.disconnect(None, "Tunnel closed", None) {
            warn!("Error disconnecting SSH session: {}", e);
        }

        debug!("SSH tunnel closed");
    }
}

/// Authenticate to the SSH server using the configured method
fn authenticate(session: &mut Session, username: &str, auth: &SshAuthMethod) -> Result<()> {
    match auth {
        SshAuthMethod::Password { password } => {
            debug!("Authenticating with password");
            session
                .userauth_password(username, password)
                .map_err(|e| SshTunnelError::AuthenticationFailed(e.to_string()))?;
        }
        SshAuthMethod::PrivateKey { path, passphrase } => {
            debug!(path = %path.display(), "Authenticating with private key");
            if !path.exists() {
                return Err(SshTunnelError::PrivateKeyNotFound {
                    path: path.display().to_string(),
                }
                .into());
            }
            session
                .userauth_pubkey_file(username, None, path, passphrase.as_deref())
                .map_err(|e| SshTunnelError::InvalidPrivateKey(e.to_string()))?;
        }
        SshAuthMethod::Agent => {
            debug!("Authenticating with SSH agent");
            authenticate_with_agent(session, username)?;
        }
    }

    if !session.authenticated() {
        return Err(SshTunnelError::AuthenticationFailed(
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
        .map_err(|e| SshTunnelError::AgentNotAvailable(e.to_string()))?;

    agent
        .connect()
        .map_err(|e| SshTunnelError::AgentNotAvailable(e.to_string()))?;

    agent.list_identities().map_err(|e| {
        SshTunnelError::AgentNotAvailable(format!("Failed to list identities: {}", e))
    })?;

    let identities = agent.identities()?;

    if identities.is_empty() {
        return Err(SshTunnelError::AgentNotAvailable("No identities in agent".to_string()).into());
    }

    for identity in identities {
        if agent.userauth(username, &identity).is_ok() && session.authenticated() {
            debug!("Authenticated with agent identity");
            return Ok(());
        }
    }

    Err(SshTunnelError::AuthenticationFailed("No agent identity worked".to_string()).into())
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
        .map_err(|e| SshTunnelError::PortForwardingFailed(e.to_string()))?;

    // Set session to non-blocking for the channel operations
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
