//! Logging and tracing infrastructure for ZQLZ
//!
//! This module provides a comprehensive, production-ready logging system using the `tracing` crate.
//! It supports:
//! - Multiple output formats (pretty console for development, JSON for production/bug reports)
//! - File rotation with timestamps
//! - Environment-based configuration via RUST_LOG
//! - Structured logging with spans for async operations
//! - Performance tracing

use std::io::IsTerminal;
use std::path::PathBuf;
use tracing_subscriber::{
    EnvFilter, Layer,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// EnvFilter directives match by target string prefix (not whole crate name), so `zqlz=info`
/// covers every workspace crate (`zqlz_driver_postgres`, `zqlz_editor`, `zqlz_services`, ...),
/// including future ones; the more specific `zqlz_ui=warn` overrides it, `warn` mutes the rest.
const PRODUCTION_FILTER: &str = "warn,zqlz=info,zqlz_ui=warn";

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
        let log_dir = default_log_directory();

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
        let log_dir = default_log_directory();

        Self {
            log_dir,
            enable_json_logs: true,
            enable_console_logs: false,
            include_location: false,
            enable_spans: false,
            default_filter: PRODUCTION_FILTER.to_string(),
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

/// Attach to the parent process's console.
///
/// The GUI is linked as a Windows-subsystem binary, so it starts with no console
/// of its own. `zqlz --foreground` wants a real terminal session, which needs the
/// console *device* (Ctrl+C, console mode) and not just the inherited handles.
#[cfg(target_os = "windows")]
pub fn attach_parent_console(requested: bool) {
    if !requested {
        return;
    }

    // Failing here just means there is no parent console — the normal case when
    // launched from Explorer, a shortcut, or a file association.
    unsafe {
        let _ = windows_sys::Win32::System::Console::AttachConsole(
            windows_sys::Win32::System::Console::ATTACH_PARENT_PROCESS,
        );
    }
}

#[cfg(not(target_os = "windows"))]
pub fn attach_parent_console(_requested: bool) {}

/// Initialize logging with default configuration
pub fn init_default() -> anyhow::Result<()> {
    let mut config = if cfg!(debug_assertions) {
        LoggingConfig::development()
    } else {
        LoggingConfig::production()
    };

    // `debug_assertions` picks the verbosity; the presence of a terminal picks
    // the sink. A Windows-subsystem build launched from Explorer has no stdout,
    // and the CLI nulls stdio when it launches the GUI in the background.
    config.enable_console_logs = std::io::stdout().is_terminal();

    init(config)
}

/// Get the log directory path
pub fn log_directory() -> PathBuf {
    default_log_directory()
}

fn default_log_directory() -> PathBuf {
    match zqlz_core::paths::logs_dir() {
        Ok(path) => path,
        Err(_) => PathBuf::from("."),
    }
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

    #[test]
    fn production_filter_covers_workspace_crates() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tracing_subscriber::Registry;

        struct Count(Arc<AtomicUsize>);
        impl<S: tracing::Subscriber> Layer<S> for Count {
            fn on_event(
                &self,
                _: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let seen = Arc::new(AtomicUsize::new(0));
        let dispatch = tracing::Dispatch::new(
            Registry::default()
                .with(Count(seen.clone()).with_filter(EnvFilter::new(PRODUCTION_FILTER))),
        );

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "zqlz_driver_postgres::connection", "x");
            tracing::info!(target: "zqlz_services::schema_service", "x");
            tracing::info!(target: "zqlz_editor::main_view", "x");
            tracing::info!(target: "zqlz_query::engine", "x");
            tracing::info!(target: "zqlz_lsp::hover", "x");
        });
        assert_eq!(seen.load(Ordering::SeqCst), 5);

        seen.store(0, Ordering::SeqCst);
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "gpui_macos::text_system", "x");
            tracing::info!(target: "zqlz_ui::fonts", "x");
            tracing::info!(target: "tokio_postgres::query", "x");
            tracing::warn!(target: "gpui_macos::text_system", "x");
        });
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }
}
