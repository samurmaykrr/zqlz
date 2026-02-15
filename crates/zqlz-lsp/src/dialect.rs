//! Database-specific SQL dialect support
//!
//! This module provides dialect information by delegating to the driver-provided
//! `DialectInfo` from `zqlz-core`. The `SqlDialect` enum is kept for API compatibility
//! but internally uses the driver's metadata.

use zqlz_core::{DialectConfig, DialectInfo, FunctionCategory};
use zqlz_drivers::{get_dialect_bundle, get_dialect_info};

/// SQL Dialect types
///
/// This enum provides a simple interface for dialect selection while delegating
/// to the comprehensive `DialectInfo` from drivers for actual metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    /// SQLite dialect
    SQLite,
    /// MySQL/MariaDB dialect
    MySQL,
    /// PostgreSQL dialect
    PostgreSQL,
    /// Microsoft SQL Server dialect
    SQLServer,
    /// Redis commands dialect
    Redis,
    /// Generic SQL (common subset)
    Generic,
}

impl SqlDialect {
    /// Get dialect from driver name
    pub fn from_driver(driver: &str) -> Self {
        match driver.to_lowercase().as_str() {
            "sqlite" => Self::SQLite,
            "mysql" | "mariadb" => Self::MySQL,
            "postgres" | "postgresql" => Self::PostgreSQL,
            "sqlserver" | "mssql" => Self::SQLServer,
            "redis" => Self::Redis,
            _ => Self::Generic,
        }
    }

    /// Get the driver name for this dialect
    fn driver_name(&self) -> &'static str {
        match self {
            Self::SQLite => "sqlite",
            Self::MySQL => "mysql",
            Self::PostgreSQL => "postgres",
            Self::SQLServer => "sqlserver",
            Self::Redis => "redis",
            Self::Generic => "generic",
        }
    }

    /// Get the full dialect info from the driver
    pub fn dialect_info(&self) -> DialectInfo {
        get_dialect_info(self.driver_name())
    }

    /// Get the dialect configuration if available
    ///
    /// Returns the declarative dialect config from the driver's TOML files.
    /// This provides additional metadata like language type for determining
    /// whether SQL validation should be skipped.
    pub fn dialect_config(&self) -> Option<&'static DialectConfig> {
        get_dialect_bundle(self.driver_name()).map(|bundle| &bundle.config)
    }

    /// Check if SQL validation should be skipped for this dialect
    ///
    /// Returns true for non-SQL dialects like Redis (command-based)
    /// or MongoDB (document-based).
    pub fn skip_sql_validation(&self) -> bool {
        self.dialect_config()
            .map(|config| config.skip_sql_validation())
            .unwrap_or(false)
    }

    /// Check if tree-sitter error detection should be skipped
    ///
    /// Returns true for dialects that don't have a tree-sitter grammar.
    pub fn skip_tree_sitter_errors(&self) -> bool {
        self.dialect_config()
            .map(|config| config.skip_tree_sitter_errors())
            .unwrap_or(false)
    }

    /// Get all keywords for this dialect
    pub fn keywords(&self) -> Vec<String> {
        self.dialect_info()
            .keyword_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Get all functions for this dialect
    pub fn functions(&self) -> Vec<String> {
        self.dialect_info()
            .function_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Get only scalar functions (non-aggregate) for this dialect
    /// These can be used in WHERE clauses
    pub fn scalar_functions(&self) -> Vec<String> {
        let info = self.dialect_info();
        info.functions
            .iter()
            .filter(|f| f.category != FunctionCategory::Aggregate)
            .map(|f| f.name.to_string())
            .collect()
    }

    /// Check if a function is an aggregate function
    pub fn is_aggregate_function(&self, func_name: &str) -> bool {
        let info = self.dialect_info();
        info.functions.iter().any(|f| {
            f.name.eq_ignore_ascii_case(func_name) && f.category == FunctionCategory::Aggregate
        })
    }

    /// Get data types for this dialect
    pub fn data_types(&self) -> Vec<String> {
        self.dialect_info()
            .data_type_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Check if a keyword is valid for this dialect
    pub fn is_valid_keyword(&self, keyword: &str) -> bool {
        let info = self.dialect_info();
        info.keywords
            .iter()
            .any(|k| k.keyword.eq_ignore_ascii_case(keyword))
    }

    /// Get dialect-specific documentation for a keyword
    pub fn get_keyword_doc(&self, keyword: &str) -> Option<String> {
        let info = self.dialect_info();

        info.keywords
            .iter()
            .find(|k| k.keyword.eq_ignore_ascii_case(keyword))
            .map(|k| {
                let mut doc = format!("**{}**\n\n", k.keyword);
                if let Some(desc) = &k.description {
                    doc.push_str(desc);
                }
                doc
            })
    }

    /// Get function documentation
    pub fn get_function_doc(&self, func_name: &str) -> Option<String> {
        let info = self.dialect_info();

        info.functions
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(func_name))
            .map(|f| {
                let mut doc = format!("**{}**\n\n", f.name);
                if let Some(desc) = &f.description {
                    doc.push_str(desc);
                    doc.push_str("\n\n");
                }
                if !f.signatures.is_empty() {
                    doc.push_str("**Syntax:**\n");
                    for sig in &f.signatures {
                        doc.push_str(&format!("- `{}`\n", sig.signature));
                    }
                }
                doc
            })
    }

    /// Get data type documentation
    pub fn get_data_type_doc(&self, type_name: &str) -> Option<String> {
        let info = self.dialect_info();

        info.data_types
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(type_name))
            .map(|t| {
                let mut doc = format!("**{}**\n\n", t.name);
                doc.push_str(&format!("Category: {:?}\n", t.category));
                if t.accepts_length {
                    if let Some(def) = t.default_length {
                        doc.push_str(&format!("Default length: {}\n", def));
                    }
                    if let Some(max) = t.max_length {
                        doc.push_str(&format!("Max length: {}\n", max));
                    }
                }
                doc
            })
    }
}
