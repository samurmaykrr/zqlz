//! Pool configuration types

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for a connection pool
///
/// Controls pool sizing, timeouts, and connection lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain in the pool
    min_size: usize,
    /// Maximum number of connections allowed in the pool
    max_size: usize,
    /// Timeout in milliseconds when acquiring a connection from the pool
    acquire_timeout_ms: u64,
    /// Timeout in milliseconds before an idle connection is closed
    idle_timeout_ms: u64,
    /// Maximum lifetime of a connection in milliseconds before it's recycled
    max_lifetime_ms: Option<u64>,
}

impl PoolConfig {
    /// Create a new pool configuration with the given min and max sizes
    ///
    /// # Panics
    ///
    /// Panics if `min_size > max_size` or if `max_size` is 0.
    pub fn new(min_size: usize, max_size: usize) -> Self {
        assert!(
            max_size > 0,
            "max_size must be greater than 0, got {}",
            max_size
        );
        assert!(
            min_size <= max_size,
            "min_size ({}) cannot exceed max_size ({})",
            min_size,
            max_size
        );

        Self {
            min_size,
            max_size,
            acquire_timeout_ms: 30_000, // 30 seconds default
            idle_timeout_ms: 600_000,   // 10 minutes default
            max_lifetime_ms: None,
        }
    }

    /// Set the acquire timeout in milliseconds
    pub fn with_acquire_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = timeout_ms;
        self
    }

    /// Set the idle timeout in milliseconds
    pub fn with_idle_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.idle_timeout_ms = timeout_ms;
        self
    }

    /// Set the maximum connection lifetime in milliseconds
    pub fn with_max_lifetime_ms(mut self, lifetime_ms: u64) -> Self {
        self.max_lifetime_ms = Some(lifetime_ms);
        self
    }

    /// Get the minimum pool size
    pub fn min_size(&self) -> usize {
        self.min_size
    }

    /// Get the maximum pool size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Get the acquire timeout as a Duration
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms)
    }

    /// Get the idle timeout as a Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_ms)
    }

    /// Get the maximum lifetime as a Duration if set
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime_ms.map(Duration::from_millis)
    }
}

impl Default for PoolConfig {
    /// Create a default pool configuration
    ///
    /// Defaults:
    /// - min_size: 1
    /// - max_size: 10
    /// - acquire_timeout: 30 seconds
    /// - idle_timeout: 10 minutes
    /// - max_lifetime: None
    fn default() -> Self {
        Self::new(1, 10)
    }
}
