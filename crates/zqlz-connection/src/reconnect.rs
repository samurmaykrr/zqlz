//! Auto-reconnect functionality for database connections
//!
//! This module provides automatic reconnection with exponential backoff
//! and configurable retry strategies.
//!
//! # Example
//!
//! ```ignore
//! use zqlz_connection::reconnect::{BackoffStrategy, ReconnectConfig, ReconnectingConnection};
//!
//! // Create a backoff strategy
//! let backoff = BackoffStrategy::new(100, 30_000);
//! let config = ReconnectConfig::new(3, backoff);
//!
//! // Create a reconnecting connection
//! let conn = ReconnectingConnection::new(factory, config).await?;
//!
//! // Operations will automatically retry on connection failures
//! let result = conn.query("SELECT 1", &[]).await?;
//! ```

mod backoff;
mod wrapper;

#[cfg(test)]
mod tests;

pub use backoff::BackoffStrategy;
pub use wrapper::{ConnectionFactory, ReconnectConfig, ReconnectEvent, ReconnectingConnection};
