//! SQL Dialect Parser and Formatter Routing
//!
//! This module maps SQL drivers (PostgreSQL, MySQL, SQLite, ClickHouse) to their
//! specific parser dialects, keyword sets, and formatter configurations. It ensures
//! that each SQL dialect uses the correct parsing rules and highlights dialect-specific
//! keywords and functions.
//!
//! # Architecture
//!
//! - Each SQL driver maps to a specific `sqlparser` dialect
//! - Keywords and functions are driver-specific and sourced from dialect info
//! - Formatters use the correct dialect output rules
//! - Non-SQL drivers (Redis, MongoDB) are excluded from SQL parsing

use sqlparser::dialect::{
    Dialect as SqlParserDialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
};
use zqlz_core::{dialects::SqlDialect as CoreSqlDialect, DialectInfo};
use zqlz_drivers::get_dialect_info;

/// SQL dialect parser configuration
///
/// This struct maps a driver to its specific SQL parser dialect, keyword set,
/// and formatting rules. It provides the information needed for accurate SQL
/// parsing, validation, and formatting.
#[derive(Debug, Clone)]
pub struct SqlDialectConfig {
    /// Driver identifier (e.g., "postgres", "mysql", "sqlite", "clickhouse")
    pub driver_id: String,

    /// Human-readable dialect name
    pub display_name: String,

    /// Core SQL dialect enum
    pub core_dialect: CoreSqlDialect,

    /// Dialect information (keywords, functions, data types)
    pub dialect_info: DialectInfo,

    /// Whether this dialect supports RETURNING clause
    pub supports_returning: bool,

    /// Whether this dialect supports UPSERT (ON CONFLICT)
    pub supports_upsert: bool,

    /// Whether this dialect supports CTEs (WITH clause)
    pub supports_cte: bool,

    /// Whether this dialect supports window functions
    pub supports_window_functions: bool,
}

impl SqlDialectConfig {
    /// Create a new SQL dialect configuration
    pub fn new(
        driver_id: impl Into<String>,
        display_name: impl Into<String>,
        core_dialect: CoreSqlDialect,
    ) -> Self {
        let driver_id = driver_id.into();
        let dialect_info = get_dialect_info(&driver_id);

        let (supports_returning, supports_upsert, supports_cte, supports_window_functions) =
            match core_dialect {
                CoreSqlDialect::PostgreSql => (true, true, true, true),
                CoreSqlDialect::MySql => (false, false, true, true),
                CoreSqlDialect::Sqlite => (true, true, true, true),
                CoreSqlDialect::ClickHouse => (false, false, true, true),
                CoreSqlDialect::Ansi => (false, false, true, true),
            };

        Self {
            driver_id,
            display_name: display_name.into(),
            core_dialect,
            dialect_info,
            supports_returning,
            supports_upsert,
            supports_cte,
            supports_window_functions,
        }
    }

    /// Get the sqlparser dialect for this SQL variant
    ///
    /// Returns a trait object that implements sqlparser's Dialect trait,
    /// which is used for parsing SQL statements.
    pub fn sqlparser_dialect(&self) -> Box<dyn SqlParserDialect> {
        match self.core_dialect {
            CoreSqlDialect::PostgreSql => Box::new(PostgreSqlDialect {}),
            CoreSqlDialect::MySql => Box::new(MySqlDialect {}),
            CoreSqlDialect::Sqlite => Box::new(SQLiteDialect {}),
            CoreSqlDialect::ClickHouse => {
                // ClickHouse uses PostgreSQL-like syntax with extensions
                Box::new(PostgreSqlDialect {})
            }
            CoreSqlDialect::Ansi => Box::new(GenericDialect {}),
        }
    }

    /// Get all keywords for this dialect
    pub fn keywords(&self) -> Vec<String> {
        self.dialect_info
            .keyword_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Check if a keyword is valid for this dialect
    pub fn is_valid_keyword(&self, keyword: &str) -> bool {
        self.dialect_info
            .keywords
            .iter()
            .any(|k| k.keyword.eq_ignore_ascii_case(keyword))
    }

    /// Get all functions for this dialect
    pub fn functions(&self) -> Vec<String> {
        self.dialect_info
            .function_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Check if a function is valid for this dialect
    pub fn is_valid_function(&self, func_name: &str) -> bool {
        self.dialect_info
            .functions
            .iter()
            .any(|f| f.name.eq_ignore_ascii_case(func_name))
    }

    /// Get all data types for this dialect
    pub fn data_types(&self) -> Vec<String> {
        self.dialect_info
            .data_type_names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Check if a data type is valid for this dialect
    pub fn is_valid_data_type(&self, type_name: &str) -> bool {
        self.dialect_info
            .data_types
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(type_name))
    }

    /// Get documentation for a keyword
    pub fn get_keyword_doc(&self, keyword: &str) -> Option<String> {
        self.dialect_info
            .keywords
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

    /// Get documentation for a function
    pub fn get_function_doc(&self, func_name: &str) -> Option<String> {
        self.dialect_info
            .functions
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

    /// Get documentation for a data type
    pub fn get_data_type_doc(&self, type_name: &str) -> Option<String> {
        self.dialect_info
            .data_types
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

    /// Get dialect-specific keywords that are not in ANSI SQL
    ///
    /// This is useful for highlighting dialect-specific syntax.
    pub fn dialect_specific_keywords(&self) -> Vec<String> {
        match self.core_dialect {
            CoreSqlDialect::PostgreSql => vec![
                "ILIKE".to_string(),
                "RETURNING".to_string(),
                "ON CONFLICT".to_string(),
                "DO UPDATE".to_string(),
                "DO NOTHING".to_string(),
                "DISTINCT ON".to_string(),
                "LATERAL".to_string(),
                "ANALYZE".to_string(),
                "VACUUM".to_string(),
                "REINDEX".to_string(),
                "SERIAL".to_string(),
                "BIGSERIAL".to_string(),
                "SMALLSERIAL".to_string(),
                "JSONB".to_string(),
                "TIMESTAMPTZ".to_string(),
            ],
            CoreSqlDialect::MySql => vec![
                "UNSIGNED".to_string(),
                "ZEROFILL".to_string(),
                "AUTO_INCREMENT".to_string(),
                "ENGINE".to_string(),
                "CHARSET".to_string(),
                "COLLATE".to_string(),
                "TINYINT".to_string(),
                "MEDIUMINT".to_string(),
                "BIGINT".to_string(),
                "LONGTEXT".to_string(),
                "MEDIUMTEXT".to_string(),
                "TINYTEXT".to_string(),
            ],
            CoreSqlDialect::Sqlite => vec![
                "AUTOINCREMENT".to_string(),
                "WITHOUT ROWID".to_string(),
                "PRAGMA".to_string(),
                "ATTACH".to_string(),
                "DETACH".to_string(),
            ],
            CoreSqlDialect::ClickHouse => vec![
                "ENGINE".to_string(),
                "PARTITION BY".to_string(),
                "ORDER BY".to_string(),
                "SAMPLE BY".to_string(),
                "TTL".to_string(),
                "CODEC".to_string(),
                "MATERIALIZE".to_string(),
                "POPULATE".to_string(),
                "FINAL".to_string(),
            ],
            CoreSqlDialect::Ansi => vec![],
        }
    }

    /// Get dialect-specific functions that are not in ANSI SQL
    pub fn dialect_specific_functions(&self) -> Vec<String> {
        let all_functions = self.functions();
        let ansi_functions = vec![
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "UPPER",
            "LOWER",
            "TRIM",
            "LENGTH",
            "SUBSTRING",
            "CONCAT",
            "COALESCE",
            "NULLIF",
            "CAST",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
        ];

        all_functions
            .into_iter()
            .filter(|f| !ansi_functions.iter().any(|a| a.eq_ignore_ascii_case(f)))
            .collect()
    }
}

/// Get SQL dialect configuration for a driver
///
/// Returns None for non-SQL drivers (Redis, MongoDB, etc.)
pub fn get_sql_dialect_config(driver_id: &str) -> Option<SqlDialectConfig> {
    match driver_id.to_lowercase().as_str() {
        "postgres" | "postgresql" => Some(SqlDialectConfig::new(
            "postgres",
            "PostgreSQL",
            CoreSqlDialect::PostgreSql,
        )),
        "mysql" | "mariadb" => Some(SqlDialectConfig::new(
            "mysql",
            "MySQL",
            CoreSqlDialect::MySql,
        )),
        "sqlite" => Some(SqlDialectConfig::new(
            "sqlite",
            "SQLite",
            CoreSqlDialect::Sqlite,
        )),
        "clickhouse" => Some(SqlDialectConfig::new(
            "clickhouse",
            "ClickHouse",
            CoreSqlDialect::ClickHouse,
        )),
        "mssql" | "sqlserver" => {
            // SQL Server not yet fully supported, but we can provide basic config
            Some(SqlDialectConfig::new(
                "mssql",
                "SQL Server",
                CoreSqlDialect::Ansi,
            ))
        }
        "duckdb" => Some(SqlDialectConfig::new(
            "duckdb",
            "DuckDB",
            CoreSqlDialect::PostgreSql, // DuckDB uses PostgreSQL-compatible syntax
        )),
        // Non-SQL drivers return None
        "redis" | "mongodb" => None,
        _ => None,
    }
}

/// Check if a driver uses SQL
pub fn is_sql_driver(driver_id: &str) -> bool {
    get_sql_dialect_config(driver_id).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config() {
        let config = get_sql_dialect_config("postgres").expect("postgres should be supported");
        assert_eq!(config.driver_id, "postgres");
        assert_eq!(config.display_name, "PostgreSQL");
        assert!(matches!(config.core_dialect, CoreSqlDialect::PostgreSql));
        assert!(config.supports_returning);
        assert!(config.supports_upsert);
        assert!(config.supports_cte);
        assert!(config.supports_window_functions);

        let keywords = config.keywords();
        assert!(!keywords.is_empty());
        assert!(config.is_valid_keyword("SELECT"));
        assert!(config.is_valid_keyword("RETURNING"));

        let functions = config.functions();
        assert!(!functions.is_empty());

        let dialect_specific = config.dialect_specific_keywords();
        assert!(dialect_specific.contains(&"ILIKE".to_string()));
        assert!(dialect_specific.contains(&"RETURNING".to_string()));
    }

    #[test]
    fn test_mysql_config() {
        let config = get_sql_dialect_config("mysql").expect("mysql should be supported");
        assert_eq!(config.driver_id, "mysql");
        assert_eq!(config.display_name, "MySQL");
        assert!(matches!(config.core_dialect, CoreSqlDialect::MySql));
        assert!(!config.supports_returning);
        assert!(!config.supports_upsert);
        assert!(config.supports_cte);
        assert!(config.supports_window_functions);

        let dialect_specific = config.dialect_specific_keywords();
        assert!(dialect_specific.contains(&"UNSIGNED".to_string()));
        assert!(dialect_specific.contains(&"AUTO_INCREMENT".to_string()));
    }

    #[test]
    fn test_sqlite_config() {
        let config = get_sql_dialect_config("sqlite").expect("sqlite should be supported");
        assert_eq!(config.driver_id, "sqlite");
        assert_eq!(config.display_name, "SQLite");
        assert!(matches!(config.core_dialect, CoreSqlDialect::Sqlite));
        assert!(config.supports_returning);
        assert!(config.supports_upsert);

        let dialect_specific = config.dialect_specific_keywords();
        assert!(dialect_specific.contains(&"AUTOINCREMENT".to_string()));
        assert!(dialect_specific.contains(&"WITHOUT ROWID".to_string()));
    }

    #[test]
    fn test_clickhouse_config() {
        let config = get_sql_dialect_config("clickhouse").expect("clickhouse should be supported");
        assert_eq!(config.driver_id, "clickhouse");
        assert_eq!(config.display_name, "ClickHouse");
        assert!(matches!(config.core_dialect, CoreSqlDialect::ClickHouse));

        let dialect_specific = config.dialect_specific_keywords();
        assert!(dialect_specific.contains(&"ENGINE".to_string()));
    }

    #[test]
    fn test_non_sql_drivers() {
        assert!(get_sql_dialect_config("redis").is_none());
        assert!(get_sql_dialect_config("mongodb").is_none());
        assert!(!is_sql_driver("redis"));
        assert!(!is_sql_driver("mongodb"));
    }

    #[test]
    fn test_is_sql_driver() {
        assert!(is_sql_driver("postgres"));
        assert!(is_sql_driver("mysql"));
        assert!(is_sql_driver("sqlite"));
        assert!(is_sql_driver("clickhouse"));
        assert!(!is_sql_driver("redis"));
        assert!(!is_sql_driver("mongodb"));
    }

    #[test]
    fn test_keyword_validation() {
        let config = get_sql_dialect_config("postgres").expect("postgres should be supported");
        assert!(config.is_valid_keyword("SELECT"));
        assert!(config.is_valid_keyword("select")); // Case insensitive
        assert!(config.is_valid_keyword("FROM"));
        assert!(!config.is_valid_keyword("INVALID_KEYWORD_XYZ"));
    }

    #[test]
    fn test_function_validation() {
        let config = get_sql_dialect_config("postgres").expect("postgres should be supported");
        let functions = config.functions();
        assert!(!functions.is_empty());

        // Check that common functions exist
        assert!(
            functions.iter().any(|f| f.eq_ignore_ascii_case("COUNT")),
            "COUNT function should exist"
        );
    }

    #[test]
    fn test_sqlparser_dialect() {
        let postgres_config =
            get_sql_dialect_config("postgres").expect("postgres should be supported");
        let _dialect = postgres_config.sqlparser_dialect();

        let mysql_config = get_sql_dialect_config("mysql").expect("mysql should be supported");
        let _dialect = mysql_config.sqlparser_dialect();
    }

    #[test]
    fn test_documentation() {
        let config = get_sql_dialect_config("postgres").expect("postgres should be supported");

        let keyword_doc = config.get_keyword_doc("SELECT");
        assert!(keyword_doc.is_some());

        let functions = config.functions();
        if !functions.is_empty() {
            let func_doc = config.get_function_doc(&functions[0]);
            // Function may or may not have documentation
            let _ = func_doc;
        }
    }
}
