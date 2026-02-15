//! ZQLZ Connection - Connection management and pooling
//!
//! This crate handles connection lifecycle, pooling, and secure credential storage.

mod config;
pub mod health;
mod manager;
pub mod pool;
pub mod reconnect;
mod storage;
pub mod widgets;

pub use config::SavedConnection;
pub use health::{
    HealthCheckConfig, HealthCheckResult, HealthChecker, HealthStatus, HealthThresholds, PingError,
    PingResult, create_shared_checker, ping_database,
};
pub use manager::ConnectionManager;
pub use pool::{ConnectionPool, PoolConfig, PoolStats, PooledConnection};
pub use reconnect::{
    BackoffStrategy, ConnectionFactory, ReconnectConfig, ReconnectEvent, ReconnectingConnection,
};
pub use storage::SecureStorage;
pub use widgets::{ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent, SavedQueryInfo};
