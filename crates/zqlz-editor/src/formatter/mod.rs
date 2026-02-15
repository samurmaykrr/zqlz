//! SQL Formatting Module
//!
//! This module provides SQL formatting capabilities with configurable options
//! for indentation, keyword case, and line breaking.
//!
//! # Quick Start
//!
//! ```
//! use zqlz_editor::formatter::{format_sql, SqlFormatter, FormatterConfig};
//!
//! // Simple formatting with defaults
//! let formatted = format_sql("select * from users where id=1").unwrap();
//!
//! // Custom configuration
//! let config = FormatterConfig::default()
//!     .with_indent_size(4)
//!     .with_uppercase_keywords(true);
//! let formatter = SqlFormatter::new(config);
//! let formatted = formatter.format("select * from users").unwrap();
//! ```

mod config;
mod format;

#[cfg(test)]
mod tests;

pub use config::FormatterConfig;
pub use format::{FormatError, SqlFormatter, format_sql, format_sql_with_config};
