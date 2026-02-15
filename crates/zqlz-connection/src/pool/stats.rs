//! Pool statistics types

use serde::{Deserialize, Serialize};

/// Statistics about a connection pool's current state
///
/// Provides insight into pool utilization and health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolStats {
    /// Total number of connections (idle + active)
    total: usize,
    /// Number of idle connections available in the pool
    idle: usize,
    /// Number of connections currently in use
    active: usize,
    /// Number of requests waiting for a connection
    waiting: usize,
}

impl PoolStats {
    /// Create new pool statistics
    pub fn new(total: usize, idle: usize, active: usize, waiting: usize) -> Self {
        Self {
            total,
            idle,
            active,
            waiting,
        }
    }

    /// Get the total number of connections
    pub fn total(&self) -> usize {
        self.total
    }

    /// Get the number of idle connections
    pub fn idle(&self) -> usize {
        self.idle
    }

    /// Get the number of active (in-use) connections
    pub fn active(&self) -> usize {
        self.active
    }

    /// Get the number of waiting requests
    pub fn waiting(&self) -> usize {
        self.waiting
    }

    /// Calculate pool utilization as a percentage (0.0 to 1.0)
    ///
    /// Returns 0.0 if total is 0 to avoid division by zero.
    pub fn utilization(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.active as f64 / self.total as f64
        }
    }

    /// Check if the pool is fully utilized (all connections in use)
    pub fn is_full(&self) -> bool {
        self.idle == 0 && self.total > 0
    }
}

impl Default for PoolStats {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}
