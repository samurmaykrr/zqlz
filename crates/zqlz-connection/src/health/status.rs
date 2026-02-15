//! Health status classification
//!
//! Classifies connection health based on latency thresholds.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Health status of a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Connection is healthy with good latency
    Healthy,
    /// Connection is working but latency is elevated
    Degraded,
    /// Connection is unhealthy (very high latency or errors)
    Unhealthy,
}

impl HealthStatus {
    /// Classify health status from latency using default thresholds.
    ///
    /// Default thresholds:
    /// - Healthy: < 100ms
    /// - Degraded: 100ms - 500ms
    /// - Unhealthy: > 500ms
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_connection::health::HealthStatus;
    /// use std::time::Duration;
    ///
    /// let status = HealthStatus::from_latency(Duration::from_millis(50));
    /// assert_eq!(status, HealthStatus::Healthy);
    ///
    /// let status = HealthStatus::from_latency(Duration::from_millis(200));
    /// assert_eq!(status, HealthStatus::Degraded);
    ///
    /// let status = HealthStatus::from_latency(Duration::from_millis(1000));
    /// assert_eq!(status, HealthStatus::Unhealthy);
    /// ```
    pub fn from_latency(latency: Duration) -> Self {
        Self::from_latency_with_thresholds(latency, &HealthThresholds::default())
    }

    /// Classify health status from latency using custom thresholds.
    pub fn from_latency_with_thresholds(latency: Duration, thresholds: &HealthThresholds) -> Self {
        if latency <= thresholds.healthy_threshold {
            HealthStatus::Healthy
        } else if latency <= thresholds.degraded_threshold {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        }
    }

    /// Check if status indicates the connection is usable.
    ///
    /// Both `Healthy` and `Degraded` are considered usable.
    pub fn is_usable(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }

    /// Check if status is healthy.
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }
}

impl Default for HealthStatus {
    fn default() -> Self {
        HealthStatus::Healthy
    }
}

/// Thresholds for health status classification
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Maximum latency considered healthy
    pub healthy_threshold: Duration,
    /// Maximum latency considered degraded (above this is unhealthy)
    pub degraded_threshold: Duration,
}

impl HealthThresholds {
    /// Create new thresholds with the given values.
    ///
    /// # Arguments
    ///
    /// * `healthy_ms` - Maximum latency in ms considered healthy
    /// * `degraded_ms` - Maximum latency in ms considered degraded
    pub fn new(healthy_ms: u64, degraded_ms: u64) -> Self {
        Self {
            healthy_threshold: Duration::from_millis(healthy_ms),
            degraded_threshold: Duration::from_millis(degraded_ms.max(healthy_ms)),
        }
    }
}

impl Default for HealthThresholds {
    /// Default thresholds: healthy < 100ms, degraded < 500ms
    fn default() -> Self {
        Self {
            healthy_threshold: Duration::from_millis(100),
            degraded_threshold: Duration::from_millis(500),
        }
    }
}
