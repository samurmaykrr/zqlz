//! Formatting utilities for table handlers.
//!
//! This module provides functions to format various data types into
//! human-readable strings for display in the UI.

use zqlz_core::Value;

use crate::components::CellValue;

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
pub(in crate::main_view) fn format_sql_value(value: &CellValue) -> String {
    match value {
        CellValue::Null => "NULL".to_string(),
        CellValue::Value(value) => format_sql_value_from_value(value),
    }
}

pub(in crate::main_view) fn format_sql_value_from_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Int8(value) => value.to_string(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Decimal(value) => value.clone(),
        Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Bytes(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("X'{}'", hex)
        }
        Value::Uuid(value) => format!("'{}'", value),
        Value::Date(value) => format!("'{}'", value),
        Value::Time(value) => format!("'{}'", value.format("%H:%M:%S%.f")),
        Value::DateTime(value) => format!("'{}'", value.format("%Y-%m-%d %H:%M:%S%.f")),
        Value::DateTimeUtc(value) => {
            format!("'{}'", value.format("%Y-%m-%d %H:%M:%S%.f UTC"))
        }
        Value::Json(value) => format!("'{}'", value.to_string().replace('\'', "''")),
        Value::Array(values) => {
            let rendered_values: Vec<String> =
                values.iter().map(format_sql_value_from_value).collect();
            format!("ARRAY[{}]", rendered_values.join(", "))
        }
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
