//! Formatting utilities for table handlers.
//!
//! This module provides functions to format various data types into
//! human-readable strings for display in the UI.

/// Format bytes into human-readable string (e.g., "48 B", "1.2 KB")
pub(in crate::main_view) fn format_bytes(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Format TTL seconds into human-readable string
pub(in crate::main_view) fn format_ttl_seconds(seconds: i64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}

/// Format a value for SQL statement
pub(in crate::main_view) fn format_sql_value(value: &str) -> String {
    if value.is_empty() || value == "NULL" {
        "NULL".to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Escape a value for use in Redis commands
pub(in crate::main_view) fn escape_redis_value(value: &str) -> String {
    // If the value contains spaces or special chars, quote it
    if value.contains(|c: char| c.is_whitespace() || c == '"' || c == '\'') {
        // Escape internal quotes and wrap in quotes
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else if value.is_empty() {
        "\"\"".to_string()
    } else {
        value.to_string()
    }
}
