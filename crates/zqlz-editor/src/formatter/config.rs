//! SQL Formatter Configuration
//!
//! Configurable options for SQL formatting including indentation, keyword case,
//! and line breaking behavior.
//!
//! # Example
//!
//! ```
//! use zqlz_editor::formatter::FormatterConfig;
//!
//! let config = FormatterConfig::default()
//!     .with_indent_size(4)
//!     .with_uppercase_keywords(true)
//!     .with_lines_between_queries(2);
//!
//! assert_eq!(config.indent_size(), 4);
//! assert!(config.uppercase_keywords());
//! ```

use serde::{Deserialize, Serialize};

/// Configuration for SQL formatting
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormatterConfig {
    /// Number of spaces for each indentation level
    indent_size: usize,
    /// Whether to use uppercase for SQL keywords (SELECT vs select)
    uppercase_keywords: bool,
    /// Number of blank lines between separate SQL statements
    lines_between_queries: usize,
}

impl Default for FormatterConfig {
    fn default() -> Self {
        Self {
            indent_size: 2,
            uppercase_keywords: true,
            lines_between_queries: 1,
        }
    }
}

impl FormatterConfig {
    /// Creates a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the indentation size (number of spaces)
    pub fn with_indent_size(mut self, size: usize) -> Self {
        self.indent_size = size;
        self
    }

    /// Sets whether to uppercase SQL keywords
    pub fn with_uppercase_keywords(mut self, uppercase: bool) -> Self {
        self.uppercase_keywords = uppercase;
        self
    }

    /// Sets the number of blank lines between queries
    pub fn with_lines_between_queries(mut self, lines: usize) -> Self {
        self.lines_between_queries = lines;
        self
    }

    /// Returns the indentation size
    pub fn indent_size(&self) -> usize {
        self.indent_size
    }

    /// Returns whether keywords should be uppercase
    pub fn uppercase_keywords(&self) -> bool {
        self.uppercase_keywords
    }

    /// Returns the number of blank lines between queries
    pub fn lines_between_queries(&self) -> usize {
        self.lines_between_queries
    }

    /// Creates a compact configuration (minimal whitespace)
    pub fn compact() -> Self {
        Self {
            indent_size: 0,
            uppercase_keywords: true,
            lines_between_queries: 0,
        }
    }

    /// Creates a verbose configuration (maximum readability)
    pub fn verbose() -> Self {
        Self {
            indent_size: 4,
            uppercase_keywords: true,
            lines_between_queries: 2,
        }
    }
}
