//! SQL Formatter - Core Formatting Logic
//!
//! Provides SQL formatting with configurable options for indentation,
//! keyword case, and line breaking.
//!
//! # Example
//!
//! ```
//! use zqlz_editor::formatter::{SqlFormatter, FormatterConfig};
//!
//! let formatter = SqlFormatter::new(FormatterConfig::default());
//! let formatted = formatter.format("select * from users where id=1").unwrap();
//! assert!(formatted.contains("SELECT"));
//! ```

use super::config::FormatterConfig;
use thiserror::Error;

/// Errors that can occur during SQL formatting
#[derive(Debug, Error)]
pub enum FormatError {
    /// The SQL input is empty
    #[error("empty SQL input")]
    EmptyInput,

    /// The SQL syntax is invalid
    #[error("invalid SQL syntax: {0}")]
    InvalidSyntax(String),

    /// Formatting failed for an unknown reason
    #[error("formatting failed: {0}")]
    FormattingFailed(String),
}

/// SQL Formatter with configurable options
#[derive(Debug, Clone)]
pub struct SqlFormatter {
    config: FormatterConfig,
}

impl Default for SqlFormatter {
    fn default() -> Self {
        Self::new(FormatterConfig::default())
    }
}

impl SqlFormatter {
    /// Creates a new formatter with the given configuration
    pub fn new(config: FormatterConfig) -> Self {
        Self { config }
    }

    /// Creates a formatter with default configuration
    pub fn with_defaults() -> Self {
        Self::default()
    }

    /// Returns the current configuration
    pub fn config(&self) -> &FormatterConfig {
        &self.config
    }

    /// Formats the given SQL string
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL string to format
    ///
    /// # Returns
    ///
    /// The formatted SQL string, or an error if formatting failed
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_editor::formatter::SqlFormatter;
    ///
    /// let formatter = SqlFormatter::with_defaults();
    /// let sql = "select id, name from users where active = true";
    /// let formatted = formatter.format(sql).unwrap();
    /// println!("{}", formatted);
    /// ```
    pub fn format(&self, sql: &str) -> Result<String, FormatError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(FormatError::EmptyInput);
        }

        let options = self.build_format_options();
        let formatted = sqlformat::format(trimmed, &Default::default(), &options);

        Ok(self.post_process(&formatted))
    }

    /// Formats multiple SQL statements separated by semicolons
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL string containing multiple statements
    ///
    /// # Returns
    ///
    /// The formatted SQL with statements separated by configured blank lines
    pub fn format_multiple(&self, sql: &str) -> Result<String, FormatError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(FormatError::EmptyInput);
        }

        let options = self.build_format_options();
        let formatted = sqlformat::format(trimmed, &Default::default(), &options);

        Ok(self.post_process(&formatted))
    }

    /// Checks if the SQL can be parsed (basic syntax validation)
    pub fn validate(&self, sql: &str) -> Result<(), FormatError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(FormatError::EmptyInput);
        }

        // Try to parse the SQL using sqlparser for validation
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, trimmed) {
            Ok(_) => Ok(()),
            Err(e) => Err(FormatError::InvalidSyntax(e.to_string())),
        }
    }

    /// Formats SQL with inline comments preserved
    ///
    /// The formatter preserves single-line (--) and block (/* */) comments
    pub fn format_preserving_comments(&self, sql: &str) -> Result<String, FormatError> {
        // sqlformat already preserves comments by default
        self.format(sql)
    }

    fn build_format_options(&self) -> sqlformat::FormatOptions<'static> {
        sqlformat::FormatOptions {
            indent: sqlformat::Indent::Spaces(self.config.indent_size() as u8),
            uppercase: Some(self.config.uppercase_keywords()),
            lines_between_queries: self.config.lines_between_queries() as u8,
            ..Default::default()
        }
    }

    fn post_process(&self, formatted: &str) -> String {
        // Ensure consistent line endings
        let result = formatted.replace("\r\n", "\n");

        // Ensure single trailing newline
        let trimmed = result.trim_end();
        if trimmed.is_empty() {
            return String::new();
        }

        format!("{}\n", trimmed)
    }
}

/// Formats SQL with default settings (convenience function)
///
/// # Example
///
/// ```
/// use zqlz_editor::formatter::format_sql;
///
/// let formatted = format_sql("select * from users").unwrap();
/// assert!(formatted.contains("SELECT"));
/// ```
pub fn format_sql(sql: &str) -> Result<String, FormatError> {
    SqlFormatter::with_defaults().format(sql)
}

/// Formats SQL with custom configuration (convenience function)
pub fn format_sql_with_config(sql: &str, config: FormatterConfig) -> Result<String, FormatError> {
    SqlFormatter::new(config).format(sql)
}
