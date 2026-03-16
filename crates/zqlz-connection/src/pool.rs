//! Connection pooling for database connections
//!
//! This module provides connection pooling functionality with configurable
//! pool sizes, timeouts, and statistics tracking.
//!
//! # Example
//!
//! ```ignore
//! use zqlz_connection::pool::{ConnectionPool, PoolConfig};
//!
//! let config = PoolConfig::new(5, 20)
//!     .with_acquire_timeout_ms(5000)
//!     .with_idle_timeout_ms(300000);
//!
//! let pool = ConnectionPool::new(config, connection_factory);
//! let conn = pool.get().await?;
//! // Use connection...
//! // Connection returned to pool on drop
//! ```

mod config;
#[path = "pool/pool.rs"]
mod connection_pool;
mod stats;

#[cfg(test)]
mod tests;

pub use config::PoolConfig;
pub use connection_pool::{ConnectionPool, PooledConnection};
pub use stats::PoolStats;
