//! Exponential backoff calculator for connection retry
//!
//! Implements exponential backoff with jitter for retry delays,
//! preventing thundering herd problems when many clients reconnect.

use std::time::Duration;

/// Exponential backoff strategy for connection retries.
///
/// Calculates delays that grow exponentially with each attempt,
/// up to a configurable maximum. Includes optional jitter to
/// prevent synchronized retry storms.
///
/// # Example
///
/// ```
/// use zqlz_connection::reconnect::BackoffStrategy;
/// use std::time::Duration;
///
/// let backoff = BackoffStrategy::new(100, 30_000);
///
/// // First attempt: ~100ms
/// assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));
///
/// // Second attempt: ~200ms
/// assert_eq!(backoff.calculate_delay(1), Duration::from_millis(200));
///
/// // Delay grows exponentially but is capped at max
/// let delay = backoff.calculate_delay(20);
/// assert!(delay <= Duration::from_millis(30_000));
/// ```
#[derive(Debug, Clone)]
pub struct BackoffStrategy {
    /// Initial delay in milliseconds for the first retry
    initial_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential growth)
    max_ms: u64,
    /// Multiplier for exponential growth (default: 2.0)
    multiplier: f64,
    /// Whether to add jitter to delays (default: false for predictable testing)
    jitter: bool,
}

impl BackoffStrategy {
    /// Create a new backoff strategy with the given initial and maximum delays.
    ///
    /// # Arguments
    ///
    /// * `initial_ms` - Initial delay in milliseconds for the first retry
    /// * `max_ms` - Maximum delay in milliseconds (cap for exponential growth)
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_connection::reconnect::BackoffStrategy;
    ///
    /// let backoff = BackoffStrategy::new(100, 30_000);
    /// ```
    pub fn new(initial_ms: u64, max_ms: u64) -> Self {
        Self {
            initial_ms: initial_ms.max(1), // Ensure at least 1ms
            max_ms: max_ms.max(initial_ms),
            multiplier: 2.0,
            jitter: false,
        }
    }

    /// Set the multiplier for exponential growth.
    ///
    /// Default is 2.0 (delay doubles each attempt).
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier.max(1.0);
        self
    }

    /// Enable jitter to add randomness to delays.
    ///
    /// Jitter helps prevent thundering herd problems when many
    /// clients retry simultaneously.
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate the delay for a given attempt number.
    ///
    /// Attempt 0 returns the initial delay, with subsequent attempts
    /// growing exponentially up to the maximum.
    ///
    /// # Arguments
    ///
    /// * `attempt` - Zero-based attempt number (0 = first retry)
    ///
    /// # Returns
    ///
    /// Duration to wait before the retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        // Calculate exponential delay: initial * multiplier^attempt
        let delay_ms = (self.initial_ms as f64) * self.multiplier.powi(attempt as i32);

        // Cap at maximum
        let capped_ms = delay_ms.min(self.max_ms as f64) as u64;

        // Optionally add jitter (up to ±25% of the delay)
        let final_ms = if self.jitter {
            let jitter_range = capped_ms / 4;
            let jitter = (rand_simple() * (jitter_range * 2) as f64) as u64;
            capped_ms
                .saturating_sub(jitter_range)
                .saturating_add(jitter)
        } else {
            capped_ms
        };

        Duration::from_millis(final_ms)
    }

    /// Get the initial delay.
    pub fn initial_delay(&self) -> Duration {
        Duration::from_millis(self.initial_ms)
    }

    /// Get the maximum delay.
    pub fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_ms)
    }

    /// Get the multiplier.
    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }

    /// Check if jitter is enabled.
    pub fn has_jitter(&self) -> bool {
        self.jitter
    }

    /// Reset is a no-op for BackoffStrategy (stateless).
    ///
    /// Provided for API consistency with stateful retry trackers.
    pub fn reset(&self) {
        // BackoffStrategy is stateless, nothing to reset
    }
}

impl Default for BackoffStrategy {
    /// Default backoff: 100ms initial, 30 seconds max, 2x multiplier
    fn default() -> Self {
        Self::new(100, 30_000)
    }
}

/// Simple pseudo-random number generator for jitter.
/// Returns a value between 0.0 and 1.0.
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}
