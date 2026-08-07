//! ZQLZ Connection - Connection management and pooling

mod config;
pub mod health;
mod manager;
pub mod pool;
pub mod reconnect;
pub mod widgets;

pub use config::SavedConnection;
pub use health::{
    HealthCheckConfig, HealthCheckResult, HealthChecker, HealthStatus, HealthThresholds, PingError,
    PingResult, create_shared_checker, ping_database, ping_database_with_timeout,
};
pub use manager::{
    ConnectionManager, DEFAULT_HEARTBEAT_INTERVAL, HeartbeatOutcome,
};
pub use pool::{ConnectionPool, PoolConfig, PoolStats, PooledConnection};
pub use reconnect::{
    BackoffStrategy, ConnectionFactory, ReconnectConfig, ReconnectEvent, ReconnectingConnection,
};
pub use widgets::{
    ConnectionEntry, ConnectionForm, ConnectionFormEvent, ConnectionPicker, ConnectionPickerEvent,
    ConnectionSidebar, ConnectionSidebarEvent, DatabaseType, SavedQueryInfo, SchemaObjects,
    SidebarObjectCapabilities, SidebarSection, SidebarTableDetailsData, SidebarTableKey,
};
