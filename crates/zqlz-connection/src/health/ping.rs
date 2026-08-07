//! Database ping implementation
//!
//! Provides lightweight health checking by executing a minimal query
//! and measuring response time.

use futures::future::{Either, select};
use gpui::BackgroundExecutor;
use std::time::{Duration, Instant};
use zqlz_core::Connection;

/// Result of a ping operation
pub type PingResult = Result<Duration, PingError>;

/// Error that can occur during a ping operation
#[derive(Debug, Clone)]
pub enum PingError {
    /// The connection is closed
    ConnectionClosed,
    /// Query execution failed
    QueryFailed(String),
    /// Ping timed out
    Timeout,
}

impl std::fmt::Display for PingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PingError::ConnectionClosed => write!(f, "Connection is closed"),
            PingError::QueryFailed(msg) => write!(f, "Ping query failed: {}", msg),
            PingError::Timeout => write!(f, "Ping timed out"),
        }
    }
}

impl std::error::Error for PingError {}

/// Ping a database connection to check if it's alive.
///
/// Executes a minimal query (SELECT 1) and returns the round-trip time.
/// This is useful for connection pool health checks and monitoring.
///
/// # Arguments
///
/// * `conn` - The database connection to ping
///
/// # Returns
///
/// * `Ok(Duration)` - The round-trip time if the ping succeeded
/// * `Err(PingError)` - If the ping failed
///
/// # Example
///
/// ```ignore
/// use zqlz_connection::health::ping_database;
///
/// let latency = ping_database(&connection).await?;
/// println!("Database latency: {:?}", latency);
/// ```
pub async fn ping_database(conn: &dyn Connection) -> PingResult {
    // Check if connection is already closed
    if conn.is_closed() {
        return Err(PingError::ConnectionClosed);
    }

    // Execute a minimal query and time it
    let start = Instant::now();

    // Use the connection-provided ping query
    let ping_query = conn.ping_query_sql();

    match conn.query(ping_query, &[]).await {
        Ok(_) => Ok(start.elapsed()),
        Err(e) => Err(PingError::QueryFailed(e.to_string())),
    }
}

/// Ping a database connection, giving up after `timeout`.
///
/// A socket that died without a FIN leaves the ping waiting on TCP
/// retransmission, which takes minutes. Heartbeats ping connections in sequence,
/// so without a deadline one wedged connection stalls every other connection
/// behind it.
pub async fn ping_database_with_timeout(
    conn: &dyn Connection,
    executor: &BackgroundExecutor,
    timeout: Duration,
) -> PingResult {
    let ping = Box::pin(ping_database(conn));
    let deadline = Box::pin(executor.timer(timeout));

    match select(ping, deadline).await {
        Either::Left((result, _)) => result,
        Either::Right(((), _)) => Err(PingError::Timeout),
    }
}
