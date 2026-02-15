//! Health checker with periodic checking functionality
//!
//! Provides periodic health monitoring for database connections,
//! tracking status changes and emitting events.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::ping::ping_database;
use super::status::{HealthStatus, HealthThresholds};
use zqlz_core::Connection;

/// Configuration for health checking
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Interval between health checks
    pub check_interval: Duration,
    /// Thresholds for classifying health status
    pub thresholds: HealthThresholds,
    /// Timeout for each ping operation
    pub ping_timeout: Duration,
    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: u32,
}

impl HealthCheckConfig {
    /// Create a new health check configuration.
    pub fn new(interval: Duration) -> Self {
        Self {
            check_interval: interval,
            thresholds: HealthThresholds::default(),
            ping_timeout: Duration::from_secs(5),
            failure_threshold: 3,
        }
    }

    /// Set custom health thresholds.
    pub fn with_thresholds(mut self, thresholds: HealthThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Set ping timeout.
    pub fn with_ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = timeout;
        self
    }

    /// Set failure threshold for consecutive failures.
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

/// Result of a single health check
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// The resulting health status
    pub status: HealthStatus,
    /// Latency of the ping, if successful
    pub latency: Option<Duration>,
    /// Error message if the check failed
    pub error: Option<String>,
    /// Timestamp of when the check was performed
    pub checked_at: Instant,
    /// Number of consecutive failures (0 if healthy)
    pub consecutive_failures: u32,
}

impl HealthCheckResult {
    /// Create a successful health check result.
    pub fn success(latency: Duration, thresholds: &HealthThresholds) -> Self {
        Self {
            status: HealthStatus::from_latency_with_thresholds(latency, thresholds),
            latency: Some(latency),
            error: None,
            checked_at: Instant::now(),
            consecutive_failures: 0,
        }
    }

    /// Create a failed health check result.
    pub fn failure(error: String, consecutive_failures: u32) -> Self {
        Self {
            status: HealthStatus::Unhealthy,
            latency: None,
            error: Some(error),
            checked_at: Instant::now(),
            consecutive_failures,
        }
    }
}

/// Health checker for monitoring connection health.
///
/// Performs periodic health checks on a connection and tracks status changes.
pub struct HealthChecker {
    config: HealthCheckConfig,
    consecutive_failures: AtomicU64,
    last_status: parking_lot::Mutex<HealthStatus>,
    is_running: AtomicBool,
}

impl HealthChecker {
    /// Create a new health checker with the given configuration.
    pub fn new(config: HealthCheckConfig) -> Self {
        Self {
            config,
            consecutive_failures: AtomicU64::new(0),
            last_status: parking_lot::Mutex::new(HealthStatus::Healthy),
            is_running: AtomicBool::new(false),
        }
    }

    /// Create a health checker with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(HealthCheckConfig::default())
    }

    /// Get the current configuration.
    pub fn config(&self) -> &HealthCheckConfig {
        &self.config
    }

    /// Get the number of consecutive failures.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::SeqCst) as u32
    }

    /// Get the last known health status.
    pub fn last_status(&self) -> HealthStatus {
        *self.last_status.lock()
    }

    /// Check if the checker is currently running periodic checks.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Perform a single health check on a connection.
    ///
    /// This is useful for on-demand health checks outside of periodic monitoring.
    pub async fn check_connection(&self, conn: &dyn Connection) -> HealthCheckResult {
        let result = ping_database(conn).await;

        match result {
            Ok(latency) => {
                self.consecutive_failures.store(0, Ordering::SeqCst);
                let check_result = HealthCheckResult::success(latency, &self.config.thresholds);
                *self.last_status.lock() = check_result.status;
                check_result
            }
            Err(e) => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) as u32 + 1;
                let check_result = HealthCheckResult::failure(e.to_string(), failures);
                *self.last_status.lock() = check_result.status;
                check_result
            }
        }
    }

    /// Determine if the connection should be considered unhealthy based on failure count.
    ///
    /// Returns true if consecutive failures >= failure_threshold.
    pub fn should_mark_unhealthy(&self) -> bool {
        self.consecutive_failures() >= self.config.failure_threshold
    }

    /// Reset the consecutive failure counter.
    pub fn reset_failures(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        *self.last_status.lock() = HealthStatus::Healthy;
    }

    /// Start periodic health checking (marks as running).
    ///
    /// Note: This only sets the running flag. Actual periodic execution
    /// should be handled by the caller using the check_interval from config.
    pub fn start(&self) {
        self.is_running.store(true, Ordering::SeqCst);
    }

    /// Stop periodic health checking.
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    /// Get the check interval from the configuration.
    pub fn check_interval(&self) -> Duration {
        self.config.check_interval
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Create a health checker that can be shared across threads.
pub fn create_shared_checker(config: HealthCheckConfig) -> Arc<HealthChecker> {
    Arc::new(HealthChecker::new(config))
}
