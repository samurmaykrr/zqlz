//! Health check functionality for database connections
//!
//! This module provides health checking, ping, and status classification
//! for database connections.
//!
//! # Example
//!
//! ```ignore
//! use zqlz_connection::health::{ping_database, HealthStatus, HealthChecker, HealthCheckConfig};
//!
//! // One-time status check
//! let latency = ping_database(&connection).await?;
//! let status = HealthStatus::from_latency(latency);
//!
//! // Periodic health checking
//! let checker = HealthChecker::with_defaults();
//! let result = checker.check_connection(&connection).await;
//! println!("Status: {:?}, Latency: {:?}", result.status, result.latency);
//! ```

mod checker;
mod ping;
mod status;

#[cfg(test)]
mod tests;

pub use checker::{HealthCheckConfig, HealthCheckResult, HealthChecker, create_shared_checker};
pub use ping::{PingError, PingResult, ping_database};
pub use status::{HealthStatus, HealthThresholds};
