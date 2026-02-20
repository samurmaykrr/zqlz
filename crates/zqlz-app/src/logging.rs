//! Logging and tracing infrastructure for ZQLZ
//!
//! This module provides a comprehensive, production-ready logging system using the `tracing` crate.
//! It supports:
//! - Multiple output formats (pretty console for development, JSON for production/bug reports)
//! - File rotation with timestamps
//! - Environment-based configuration via RUST_LOG
//! - Structured logging with spans for async operations
//! - Performance tracing

use std::path::PathBuf;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

/// Logging configuration
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// Directory where log files should be written
    pub log_dir: PathBuf,

    /// Whether to enable JSON output to files (for bug reports)
    pub enable_json_logs: bool,

    /// Whether to enable pretty console output
    pub enable_console_logs: bool,

    /// Whether to include file/line information in logs
    pub include_location: bool,

    /// Whether to log spans (for performance tracing)
    pub enable_spans: bool,

    /// Default log level filter
    pub default_filter: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        let log_dir = dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("zqlz")
            .join("logs");

        Self {
            log_dir,
            enable_json_logs: true,
            enable_console_logs: true,
            include_location: cfg!(debug_assertions),
            enable_spans: cfg!(debug_assertions),
            default_filter: "debug,zqlz_app=debug,zqlz_ui=debug,zqlz_core=debug,zqlz_drivers=debug,zqlz_connection=debug,zqlz_query=debug,zqlz_schema=debug,zqlz_services=debug".to_string(),
        }
    }
}

impl LoggingConfig {
    /// Create a production configuration (minimal console output, JSON logs for bug reports)
    pub fn production() -> Self {
        let log_dir = dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("zqlz")
            .join("logs");

        Self {
            log_dir,
            enable_json_logs: true,
            enable_console_logs: false,
            include_location: false,
            enable_spans: false,
            default_filter: "warn,zqlz_app=info,zqlz_ui=warn,zqlz_core=info,zqlz_drivers=info,zqlz_connection=info,zqlz_query=info,zqlz_schema=info".to_string(),
        }
    }

    /// Create a development configuration (pretty console output, verbose logging)
    pub fn development() -> Self {
        Self::default()
    }

    /// Create a testing configuration (console only, no files)
    #[allow(dead_code)]
    pub fn testing() -> Self {
        Self {
            log_dir: PathBuf::from("/tmp/zqlz-tests"),
            enable_json_logs: false,
            enable_console_logs: true,
            include_location: true,
            enable_spans: true,
            default_filter: "debug".to_string(),
        }
    }
}

/// Initialize the logging system with the given configuration
///
/// # Panics
/// Panics if logging has already been initialized
pub fn init(config: LoggingConfig) -> anyhow::Result<()> {
    // Create log directory if it doesn't exist
    std::fs::create_dir_all(&config.log_dir)?;

    // Build the environment filter
    // RUST_LOG environment variable takes precedence over default filter
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.default_filter));

    // NEW fires once when the span is created; ENTER would fire on every async
    // re-poll, producing misleading "duplicate" log lines for awaited futures.
    let span_events = if config.enable_spans {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    let mut layers = Vec::new();

    // Console layer (pretty output for development)
    if config.enable_console_logs {
        let console_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_file(config.include_location)
            .with_line_number(config.include_location)
            .with_span_events(span_events.clone())
            .with_ansi(true) // Enable colors
            .pretty()
            .with_filter(env_filter.clone())
            .boxed();

        layers.push(console_layer);
    }

    // JSON file layer (for bug reports and production debugging)
    if config.enable_json_logs {
        // Create a non-blocking writer that won't slow down the application
        let file_appender = tracing_appender::rolling::daily(&config.log_dir, "zqlz.log");
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

        // Store the guard to prevent it from being dropped
        // This is a known limitation - the guard must live for the duration of the program
        std::mem::forget(_guard);

        let json_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(span_events)
            .with_ansi(false) // No colors in JSON
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_writer(non_blocking)
            .with_filter(env_filter)
            .boxed();

        layers.push(json_layer);
    }

    // Initialize the subscriber
    tracing_subscriber::registry().with(layers).init();

    // Log initialization
    tracing::info!(
        log_dir = %config.log_dir.display(),
        json_enabled = config.enable_json_logs,
        console_enabled = config.enable_console_logs,
        "Logging system initialized"
    );

    Ok(())
}

/// Initialize logging with default configuration
pub fn init_default() -> anyhow::Result<()> {
    let config = if cfg!(debug_assertions) {
        LoggingConfig::development()
    } else {
        LoggingConfig::production()
    };

    init(config)
}

/// Get the log directory path
pub fn log_directory() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("zqlz")
        .join("logs")
}

/// Helper macro for logging errors with context
///
/// Usage:
/// ```ignore
/// log_error!(e, "Failed to execute query", query = %sql);
/// ```
#[macro_export]
macro_rules! log_error {
    ($err:expr, $msg:literal $(, $($key:ident = $value:expr),*)?) => {
        tracing::error!(
            error = %$err,
            error_source = ?$err.source(),
            $($($key = $value,)*)?
            $msg
        );
    };
}

/// Helper macro for timing operations
///
/// Usage:
/// ```ignore
/// let _timer = trace_timing!("query_execution", query = %sql);
/// // ... operation ...
/// // Timer automatically logs on drop
/// ```
#[macro_export]
macro_rules! trace_timing {
    ($name:expr $(, $($key:ident = $value:expr),*)?) => {
        {
            let span = tracing::info_span!($name, $($($key = $value,)*)?);
            let _enter = span.enter();
            TimingGuard::new($name)
        }
    };
}

/// Guard that logs timing information when dropped
#[allow(dead_code)]
pub struct TimingGuard {
    name: &'static str,
    start: std::time::Instant,
}

#[allow(dead_code)]
impl TimingGuard {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        tracing::debug!(
            operation = self.name,
            duration_ms = elapsed.as_millis(),
            "Operation completed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert!(config.enable_console_logs);
        assert!(config.enable_json_logs);
    }

    #[test]
    fn test_production_config() {
        let config = LoggingConfig::production();
        assert!(!config.enable_console_logs);
        assert!(config.enable_json_logs);
        assert!(!config.include_location);
    }

    #[test]
    fn test_development_config() {
        let config = LoggingConfig::development();
        assert!(config.enable_console_logs);
        assert!(config.enable_json_logs);
    }
}
