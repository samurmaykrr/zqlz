//! Server status monitoring
//!
//! Provides types and functions for retrieving database server status.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use zqlz_core::{Connection, Result, ZqlzError};

/// Server health state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServerHealth {
    /// Server is healthy and responding normally
    Healthy,
    /// Server is responding but with degraded performance
    Degraded,
    /// Server is not responding or has critical issues
    Unhealthy,
    /// Server health is unknown (not yet checked)
    #[default]
    Unknown,
}

impl ServerHealth {
    /// Check if the server is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, ServerHealth::Healthy)
    }

    /// Check if the server is operational (healthy or degraded)
    pub fn is_operational(&self) -> bool {
        matches!(self, ServerHealth::Healthy | ServerHealth::Degraded)
    }
}

impl std::fmt::Display for ServerHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerHealth::Healthy => write!(f, "Healthy"),
            ServerHealth::Degraded => write!(f, "Degraded"),
            ServerHealth::Unhealthy => write!(f, "Unhealthy"),
            ServerHealth::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Database server status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatus {
    /// Database server version string
    pub version: String,
    /// Server uptime duration
    pub uptime: Duration,
    /// Number of currently active connections
    pub active_connections: u32,
    /// Maximum allowed connections (if available)
    pub max_connections: Option<u32>,
    /// Current server health state
    pub health: ServerHealth,
    /// When this status was retrieved
    pub retrieved_at: DateTime<Utc>,
    /// Response time for the status query
    pub response_time: Duration,
    /// Server hostname or address
    pub hostname: Option<String>,
    /// Server process ID (if available)
    pub process_id: Option<u32>,
    /// Database name being monitored
    pub database: Option<String>,
}

impl ServerStatus {
    /// Create a new ServerStatus with required fields
    pub fn new(version: String, uptime: Duration, active_connections: u32) -> Self {
        Self {
            version,
            uptime,
            active_connections,
            max_connections: None,
            health: ServerHealth::Healthy,
            retrieved_at: Utc::now(),
            response_time: Duration::ZERO,
            hostname: None,
            process_id: None,
            database: None,
        }
    }

    /// Builder method: set max connections
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Builder method: set health status
    pub fn with_health(mut self, health: ServerHealth) -> Self {
        self.health = health;
        self
    }

    /// Builder method: set response time
    pub fn with_response_time(mut self, response_time: Duration) -> Self {
        self.response_time = response_time;
        self
    }

    /// Builder method: set hostname
    pub fn with_hostname(mut self, hostname: String) -> Self {
        self.hostname = Some(hostname);
        self
    }

    /// Builder method: set process ID
    pub fn with_process_id(mut self, pid: u32) -> Self {
        self.process_id = Some(pid);
        self
    }

    /// Builder method: set database name
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    /// Get connection usage percentage
    pub fn connection_usage_percent(&self) -> Option<f64> {
        self.max_connections.map(|max| {
            if max == 0 {
                0.0
            } else {
                (self.active_connections as f64 / max as f64) * 100.0
            }
        })
    }

    /// Check if connection limit is approaching (over 80%)
    pub fn is_connection_limit_approaching(&self) -> bool {
        self.connection_usage_percent()
            .map(|pct| pct >= 80.0)
            .unwrap_or(false)
    }

    /// Format uptime as a human-readable string
    pub fn uptime_display(&self) -> String {
        let secs = self.uptime.as_secs();
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        let minutes = (secs % 3600) / 60;
        let seconds = secs % 60;

        if days > 0 {
            format!("{}d {}h {}m {}s", days, hours, minutes, seconds)
        } else if hours > 0 {
            format!("{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        }
    }
}

/// Trait for database connections that support server monitoring
#[async_trait]
pub trait MonitorableConnection: Connection {
    /// Get the current server status
    async fn get_server_status(&self) -> Result<ServerStatus>;
}

/// Server monitor for tracking database server health
pub struct ServerMonitor {
    /// Thresholds for health classification
    config: MonitorConfig,
}

/// Configuration for the server monitor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorConfig {
    /// Response time threshold for healthy status (ms)
    pub healthy_response_ms: u64,
    /// Response time threshold for degraded status (ms)
    pub degraded_response_ms: u64,
    /// Connection usage threshold for warning (percentage)
    pub connection_warning_percent: f64,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            healthy_response_ms: 100,
            degraded_response_ms: 500,
            connection_warning_percent: 80.0,
        }
    }
}

impl MonitorConfig {
    /// Create a new MonitorConfig with custom thresholds
    pub fn new(healthy_response_ms: u64, degraded_response_ms: u64) -> Self {
        Self {
            healthy_response_ms,
            degraded_response_ms,
            connection_warning_percent: 80.0,
        }
    }

    /// Builder method: set connection warning percentage
    pub fn with_connection_warning(mut self, percent: f64) -> Self {
        self.connection_warning_percent = percent;
        self
    }
}

impl ServerMonitor {
    /// Create a new server monitor with default configuration
    pub fn new() -> Self {
        Self {
            config: MonitorConfig::default(),
        }
    }

    /// Create a new server monitor with custom configuration
    pub fn with_config(config: MonitorConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration
    pub fn config(&self) -> &MonitorConfig {
        &self.config
    }

    /// Classify health based on response time
    pub fn classify_health_by_response_time(&self, response_time: Duration) -> ServerHealth {
        let ms = response_time.as_millis() as u64;
        if ms <= self.config.healthy_response_ms {
            ServerHealth::Healthy
        } else if ms <= self.config.degraded_response_ms {
            ServerHealth::Degraded
        } else {
            ServerHealth::Unhealthy
        }
    }

    /// Get server status from a connection with health classification
    pub async fn get_status<C: MonitorableConnection>(&self, conn: &C) -> Result<ServerStatus> {
        let start = std::time::Instant::now();
        let mut status = conn.get_server_status().await?;
        let response_time = start.elapsed();

        status.response_time = response_time;
        status.health = self.classify_health_by_response_time(response_time);

        Ok(status)
    }

    /// Check if server is reachable by executing a simple query
    pub async fn ping<C: Connection>(&self, conn: &C) -> Result<Duration> {
        let start = std::time::Instant::now();

        // Try to execute a simple query to verify the connection
        let ping_query = match conn.driver_name() {
            "postgresql" | "postgres" => "SELECT 1",
            "mysql" => "SELECT 1",
            "sqlite" => "SELECT 1",
            "mssql" | "sqlserver" => "SELECT 1",
            "clickhouse" => "SELECT 1",
            "duckdb" => "SELECT 1",
            _ => "SELECT 1",
        };

        conn.query(ping_query, &[]).await?;
        Ok(start.elapsed())
    }

    /// Check server health by pinging
    pub async fn check_health<C: Connection>(&self, conn: &C) -> ServerHealth {
        match self.ping(conn).await {
            Ok(response_time) => self.classify_health_by_response_time(response_time),
            Err(_) => ServerHealth::Unhealthy,
        }
    }
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Query builder for database-specific server status queries
pub struct ServerStatusQuery;

impl ServerStatusQuery {
    /// Get the SQL query for PostgreSQL server status
    pub fn postgres() -> &'static str {
        r#"
        SELECT 
            version() as version,
            EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint as uptime_seconds,
            (SELECT count(*) FROM pg_stat_activity)::integer as active_connections,
            (SELECT setting::integer FROM pg_settings WHERE name = 'max_connections') as max_connections,
            inet_server_addr()::text as hostname,
            pg_backend_pid() as process_id,
            current_database() as database
        "#
    }

    /// Get the SQL query for MySQL server status
    pub fn mysql() -> &'static str {
        r#"
        SELECT 
            @@version as version,
            @@GLOBAL.uptime as uptime_seconds,
            (SELECT COUNT(*) FROM information_schema.processlist) as active_connections,
            @@GLOBAL.max_connections as max_connections,
            @@hostname as hostname,
            CONNECTION_ID() as process_id,
            DATABASE() as database
        "#
    }

    /// Get the SQL query for SQLite (limited status info)
    pub fn sqlite() -> &'static str {
        "SELECT sqlite_version() as version"
    }

    /// Get the SQL query for SQL Server status
    pub fn mssql() -> &'static str {
        r#"
        SELECT 
            @@VERSION as version,
            DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) as uptime_seconds,
            (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1) as active_connections,
            (SELECT value_in_use FROM sys.configurations WHERE name = 'user connections') as max_connections,
            @@SERVERNAME as hostname,
            @@SPID as process_id,
            DB_NAME() as database
        FROM sys.dm_os_sys_info
        "#
    }

    /// Get the SQL query for ClickHouse server status
    pub fn clickhouse() -> &'static str {
        r#"
        SELECT 
            version() as version,
            uptime() as uptime_seconds,
            (SELECT count() FROM system.processes) as active_connections,
            currentDatabase() as database
        "#
    }

    /// Get the SQL query for DuckDB (limited status info)
    pub fn duckdb() -> &'static str {
        "SELECT duckdb_version() as version"
    }

    /// Get the appropriate query for a driver
    pub fn for_driver(driver_name: &str) -> Result<&'static str> {
        match driver_name {
            "postgresql" | "postgres" => Ok(Self::postgres()),
            "mysql" => Ok(Self::mysql()),
            "sqlite" => Ok(Self::sqlite()),
            "mssql" | "sqlserver" => Ok(Self::mssql()),
            "clickhouse" => Ok(Self::clickhouse()),
            "duckdb" => Ok(Self::duckdb()),
            _ => Err(ZqlzError::NotSupported(format!(
                "Server status query not available for driver: {}",
                driver_name
            ))),
        }
    }
}
