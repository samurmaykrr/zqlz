//! Comprehensive panic handling system for ZQLZ
//!
//! This module provides a robust panic handler that:
//! - Logs detailed panic information to a file
//! - Captures full backtraces
//! - Records system information
//! - Provides structured panic data for display in the UI

use chrono::{DateTime, Utc};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::panic::{self, PanicInfo};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Panic data structure for UI display
#[derive(Clone, Debug)]
pub struct PanicData {
    pub timestamp: DateTime<Utc>,
    pub message: String,
    pub location: String,
    pub backtrace: String,
    pub system_info: SystemInfo,
}

/// System information captured during a panic
#[derive(Clone, Debug)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub rust_version: String,
    pub app_version: String,
}

impl SystemInfo {
    fn capture() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            rust_version: "rustc 1.0".to_string(), // Could be captured at compile time
            app_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Global panic handler that stores the last panic for UI display
pub struct PanicHandler {
    last_panic: Arc<Mutex<Option<PanicData>>>,
    log_path: PathBuf,
}

impl PanicHandler {
    /// Create a new panic handler
    pub fn new(log_dir: impl AsRef<Path>) -> Self {
        let log_path = log_dir.as_ref().join("zqlz_crash.log");

        Self {
            last_panic: Arc::new(Mutex::new(None)),
            log_path,
        }
    }

    /// Install this panic handler as the global panic hook
    pub fn install(self) -> Arc<Mutex<Option<PanicData>>> {
        let last_panic = self.last_panic.clone();
        let log_path = self.log_path.clone();

        let default_hook = panic::take_hook();

        panic::set_hook(Box::new(move |panic_info| {
            let panic_data = Self::capture_panic_data(panic_info);

            // Store for UI display
            if let Ok(mut last) = last_panic.lock() {
                *last = Some(panic_data.clone());
            }

            // Log to file
            if let Err(e) = Self::log_to_file(&log_path, &panic_data) {
                // Use tracing if possible, fallback to eprintln for panic handler
                tracing::error!("Failed to write panic log: {}", e);
            }

            // Log to tracing system
            Self::log_to_tracing(&panic_data);

            // Print to stderr for immediate visibility
            Self::print_to_stderr(&panic_data);

            // Call the default hook for debugger attachment
            default_hook(panic_info);
        }));

        self.last_panic
    }

    /// Capture panic information into a structured format
    fn capture_panic_data(panic_info: &PanicInfo) -> PanicData {
        let timestamp = Utc::now();

        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic payload".to_string()
        };

        let location = if let Some(loc) = panic_info.location() {
            format!("{}:{}:{}", loc.file(), loc.line(), loc.column())
        } else {
            "unknown location".to_string()
        };

        let backtrace = format!("{:?}", std::backtrace::Backtrace::force_capture());
        let system_info = SystemInfo::capture();

        PanicData {
            timestamp,
            message,
            location,
            backtrace,
            system_info,
        }
    }

    /// Log panic data to a file
    fn log_to_file(log_path: &Path, panic_data: &PanicData) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;

        writeln!(
            file,
            "\n================================================================================"
        )?;
        writeln!(file, "ZQLZ CRASH REPORT")?;
        writeln!(
            file,
            "================================================================================"
        )?;
        writeln!(file, "Timestamp: {}", panic_data.timestamp.to_rfc3339())?;
        writeln!(file, "\nPanic Message:")?;
        writeln!(file, "  {}", panic_data.message)?;
        writeln!(file, "\nLocation:")?;
        writeln!(file, "  {}", panic_data.location)?;
        writeln!(file, "\nSystem Information:")?;
        writeln!(file, "  OS: {}", panic_data.system_info.os)?;
        writeln!(file, "  Architecture: {}", panic_data.system_info.arch)?;
        writeln!(
            file,
            "  Rust Version: {}",
            panic_data.system_info.rust_version
        )?;
        writeln!(
            file,
            "  App Version: {}",
            panic_data.system_info.app_version
        )?;
        writeln!(file, "\nBacktrace:")?;
        writeln!(file, "{}", panic_data.backtrace)?;
        writeln!(
            file,
            "================================================================================\n"
        )?;

        file.flush()?;

        Ok(())
    }

    /// Log panic to the tracing system
    fn log_to_tracing(panic_data: &PanicData) {
        tracing::error!(
            "================================================================================"
        );
        tracing::error!("ZQLZ APPLICATION PANIC");
        tracing::error!(
            "================================================================================"
        );
        tracing::error!("Timestamp: {}", panic_data.timestamp);
        tracing::error!("Message: {}", panic_data.message);
        tracing::error!("Location: {}", panic_data.location);
        tracing::error!(
            "OS: {} ({})",
            panic_data.system_info.os,
            panic_data.system_info.arch
        );
        tracing::error!("App Version: {}", panic_data.system_info.app_version);
        tracing::error!("Backtrace (first 20 lines):");
        for (i, line) in panic_data.backtrace.lines().take(20).enumerate() {
            tracing::error!("  {}: {}", i, line);
        }
        tracing::error!(
            "================================================================================"
        );
    }

    /// Print panic information to stderr
    ///
    /// Note: This intentionally uses `eprintln!` instead of tracing because:
    /// 1. Stderr output provides immediate visibility during panics
    /// 2. It ensures users see the error even if logging system fails
    /// 3. It's designed for human-readable panic notifications
    fn print_to_stderr(panic_data: &PanicData) {
        eprintln!(
            "\n================================================================================"
        );
        eprintln!("╔═══════════════════════════════════════════════════════════════════════════╗");
        eprintln!("║                        ZQLZ APPLICATION PANIC                             ║");
        eprintln!("╚═══════════════════════════════════════════════════════════════════════════╝");
        eprintln!();
        eprintln!("The application has encountered a critical error and must close.");
        eprintln!();
        eprintln!(
            "Timestamp: {}",
            panic_data.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        );
        eprintln!();
        eprintln!("Error Message:");
        eprintln!("  {}", panic_data.message);
        eprintln!();
        eprintln!("Location:");
        eprintln!("  {}", panic_data.location);
        eprintln!();
        eprintln!("A detailed crash report has been saved to: zqlz_crash.log");
        eprintln!("Please consider reporting this issue to the ZQLZ team.");
        eprintln!();
        eprintln!(
            "================================================================================\n"
        );
    }

    /// Get the directory for storing crash logs
    pub fn log_directory() -> PathBuf {
        // Try to use platform-specific directories, fallback to current directory
        if let Some(data_dir) = dirs::data_local_dir() {
            let zqlz_dir = data_dir.join("zqlz").join("logs");
            if std::fs::create_dir_all(&zqlz_dir).is_ok() {
                return zqlz_dir;
            }
        }

        // Fallback to current directory
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    }
}

/// Safely extract the last panic data from the global handler
pub fn get_last_panic(panic_data: &Arc<Mutex<Option<PanicData>>>) -> Option<PanicData> {
    panic_data.lock().ok().and_then(|guard| guard.clone())
}
