//! Database ping implementation
//!
//! Provides lightweight health checking by executing a minimal query
//! and measuring response time.

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

    // Use driver-appropriate ping query
    let ping_query = get_ping_query(conn.driver_name());

    match conn.query(ping_query, &[]).await {
        Ok(_) => Ok(start.elapsed()),
        Err(e) => Err(PingError::QueryFailed(e.to_string())),
    }
}

/// Get the appropriate ping query for a given driver.
///
/// Different databases have different optimal ping queries:
/// - PostgreSQL: `SELECT 1` or `;` (empty statement)
/// - MySQL: `SELECT 1` or `DO 1`
/// - SQLite: `SELECT 1`
/// - MS SQL: `SELECT 1`
pub(super) fn get_ping_query(driver_name: &str) -> &'static str {
    match driver_name {
        "mysql" => "SELECT 1",
        "postgresql" | "postgres" => "SELECT 1",
        "sqlite" => "SELECT 1",
        "mssql" => "SELECT 1",
        _ => "SELECT 1", // Generic fallback
    }
}
