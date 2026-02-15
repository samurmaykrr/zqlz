//! Dialect Registry v2 - Unified Dialect Profiles
//!
//! This module provides a unified registry of dialect profiles that work for
//! both SQL and non-SQL databases. Each driver maps to a DialectProfile that
//! declares its parsing, validation, highlighting, and formatting capabilities.
//!
//! Key principles:
//! - Dialect metadata is explicit, not inferred
//! - Non-SQL drivers (Redis, MongoDB) have profiles that disable SQL features
//! - SQL drivers specify their specific SQL dialect and parser
//! - All capabilities are declared upfront for clear behavior

use crate::dialect_config::{DialectBundle, LanguageType};
use crate::DialectInfo;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Parsing capabilities for a dialect
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserCapability {
    /// Use SQL parser with specific dialect
    Sql(SqlDialect),
    /// Command-based syntax (Redis, etc.) - no SQL parsing
    Command,
    /// Document-based syntax (MongoDB, etc.) - JSON/BSON validation
    Document,
    /// Custom parser/validator (advanced use cases)
    Custom,
}

/// SQL dialect variants for the SQL parser
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
    /// PostgreSQL SQL dialect
    PostgreSql,
    /// MySQL/MariaDB SQL dialect
    MySql,
    /// SQLite SQL dialect
    Sqlite,
    /// ClickHouse SQL dialect (extends PostgreSQL-like syntax)
    ClickHouse,
    /// Generic ANSI SQL (fallback)
    Ansi,
}

impl SqlDialect {
    /// Get the sqlparser dialect for this SQL variant
    pub fn sqlparser_dialect(&self) -> sqlparser::dialect::GenericDialect {
        // This is a placeholder - actual implementation would return appropriate dialect
        sqlparser::dialect::GenericDialect {}
    }

    /// Get display name for this SQL dialect
    pub fn display_name(&self) -> &'static str {
        match self {
            SqlDialect::PostgreSql => "PostgreSQL",
            SqlDialect::MySql => "MySQL",
            SqlDialect::Sqlite => "SQLite",
            SqlDialect::ClickHouse => "ClickHouse",
            SqlDialect::Ansi => "ANSI SQL",
        }
    }
}

/// Tree-sitter grammar information
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeSitterGrammar {
    /// No tree-sitter grammar (use keyword-based highlighting)
    None,
    /// Standard SQL grammar
    Sql,
    /// Custom grammar with name
    Custom(&'static str),
}

/// Formatter capability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatterCapability {
    /// Use SQL formatter
    Sql,
    /// Use custom formatter
    Custom,
    /// No formatter available
    None,
}

/// Code folding capability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldingCapability {
    /// Use tree-sitter folds.scm queries
    TreeSitter,
    /// Custom folding rules
    Custom,
    /// No folding support
    None,
}

/// Bracket matching capability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BracketCapability {
    /// Use tree-sitter brackets.scm queries
    TreeSitter,
    /// Standard bracket pairs: (), [], {}
    Standard,
    /// No bracket matching
    None,
}

/// Complete dialect profile for a database driver
///
/// This struct unifies all metadata needed for syntax highlighting,
/// parsing, validation, formatting, and editor features.
#[derive(Debug, Clone)]
pub struct DialectProfile {
    /// Unique identifier (matches driver id)
    pub id: &'static str,

    /// Human-readable language name
    pub language_name: &'static str,

    /// Language type (SQL, Command, Document, Custom)
    pub language_type: LanguageType,

    /// Tree-sitter grammar for syntax highlighting
    pub tree_sitter_language: TreeSitterGrammar,

    /// Parser capability for validation
    pub parser: ParserCapability,

    /// SQL dialect if applicable (for sqlparser)
    pub sql_dialect: Option<SqlDialect>,

    /// Custom validator function (for non-SQL languages)
    pub custom_validator: Option<fn(&str) -> Vec<ValidationError>>,

    /// Formatter capability
    pub formatter: FormatterCapability,

    /// Code folding capability
    pub folding: FoldingCapability,

    /// Bracket matching capability
    pub brackets: BracketCapability,

    /// Dialect bundle (declarative TOML config)
    pub bundle: Option<&'static DialectBundle>,

    /// Legacy dialect info (for backwards compatibility)
    pub legacy_info: Option<&'static DialectInfo>,
}

/// Validation error from custom validators
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub line: usize,
    pub column: usize,
    pub message: String,
    pub severity: crate::dialect_config::DiagnosticSeverity,
}

impl DialectProfile {
    /// Create a SQL dialect profile
    pub const fn sql(id: &'static str, language_name: &'static str, dialect: SqlDialect) -> Self {
        Self {
            id,
            language_name,
            language_type: LanguageType::Sql,
            tree_sitter_language: TreeSitterGrammar::Sql,
            parser: ParserCapability::Sql(dialect),
            sql_dialect: Some(dialect),
            custom_validator: None,
            formatter: FormatterCapability::Sql,
            folding: FoldingCapability::TreeSitter,
            brackets: BracketCapability::TreeSitter,
            bundle: None,
            legacy_info: None,
        }
    }

    /// Create a command-based dialect profile (Redis, etc.)
    pub const fn command(id: &'static str, language_name: &'static str) -> Self {
        Self {
            id,
            language_name,
            language_type: LanguageType::Command,
            tree_sitter_language: TreeSitterGrammar::None,
            parser: ParserCapability::Command,
            sql_dialect: None,
            custom_validator: None,
            formatter: FormatterCapability::None,
            folding: FoldingCapability::None,
            brackets: BracketCapability::Standard,
            bundle: None,
            legacy_info: None,
        }
    }

    /// Create a document-based dialect profile (MongoDB, etc.)
    pub const fn document(id: &'static str, language_name: &'static str) -> Self {
        Self {
            id,
            language_name,
            language_type: LanguageType::Document,
            tree_sitter_language: TreeSitterGrammar::Custom("json"),
            parser: ParserCapability::Document,
            sql_dialect: None,
            custom_validator: None,
            formatter: FormatterCapability::Custom,
            folding: FoldingCapability::TreeSitter,
            brackets: BracketCapability::TreeSitter,
            bundle: None,
            legacy_info: None,
        }
    }

    /// Set the dialect bundle
    pub const fn with_bundle(mut self, bundle: &'static DialectBundle) -> Self {
        self.bundle = Some(bundle);
        self
    }

    /// Set the legacy dialect info
    pub const fn with_legacy_info(mut self, info: &'static DialectInfo) -> Self {
        self.legacy_info = Some(info);
        self
    }

    /// Set a custom validator
    pub const fn with_validator(mut self, validator: fn(&str) -> Vec<ValidationError>) -> Self {
        self.custom_validator = Some(validator);
        self
    }

    /// Set tree-sitter grammar
    pub const fn with_tree_sitter(mut self, grammar: TreeSitterGrammar) -> Self {
        self.tree_sitter_language = grammar;
        self
    }

    /// Check if this dialect uses SQL parsing
    pub fn is_sql(&self) -> bool {
        matches!(self.parser, ParserCapability::Sql(_))
    }

    /// Check if SQL validation should be skipped
    pub fn skip_sql_validation(&self) -> bool {
        !self.is_sql()
    }

    /// Get the SQL dialect if applicable
    pub fn get_sql_dialect(&self) -> Option<SqlDialect> {
        self.sql_dialect
    }

    /// Get dialect info (from bundle or legacy)
    pub fn dialect_info(&self) -> Option<DialectInfo> {
        if let Some(bundle) = self.bundle {
            Some(bundle.into())
        } else {
            self.legacy_info.cloned()
        }
    }
}

/// Global dialect registry
///
/// Maps driver IDs to their dialect profiles. This is the central source of truth
/// for how each database driver should be parsed, validated, and highlighted.
pub struct DialectRegistry {
    profiles: HashMap<&'static str, DialectProfile>,
}

impl DialectRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            profiles: HashMap::new(),
        }
    }

    /// Register a dialect profile
    pub fn register(&mut self, profile: DialectProfile) {
        self.profiles.insert(profile.id, profile);
    }

    /// Get a dialect profile by driver ID
    pub fn get(&self, driver_id: &str) -> Option<&DialectProfile> {
        self.profiles.get(driver_id)
    }

    /// Get all registered driver IDs
    pub fn driver_ids(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.profiles.keys().copied()
    }

    /// Get all SQL dialect profiles
    pub fn sql_profiles(&self) -> impl Iterator<Item = &DialectProfile> {
        self.profiles.values().filter(|p| p.is_sql())
    }

    /// Get all non-SQL dialect profiles
    pub fn non_sql_profiles(&self) -> impl Iterator<Item = &DialectProfile> {
        self.profiles.values().filter(|p| !p.is_sql())
    }
}

impl Default for DialectRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global dialect registry instance
///
/// This is initialized with default profiles for all supported drivers.
pub static DIALECT_REGISTRY: LazyLock<Arc<DialectRegistry>> = LazyLock::new(|| {
    let mut registry = DialectRegistry::new();

    // Register SQL dialects
    registry.register(DialectProfile::sql(
        "postgres",
        "PostgreSQL",
        SqlDialect::PostgreSql,
    ));

    registry.register(DialectProfile::sql("mysql", "MySQL", SqlDialect::MySql));

    registry.register(DialectProfile::sql("sqlite", "SQLite", SqlDialect::Sqlite));

    registry.register(DialectProfile::sql(
        "clickhouse",
        "ClickHouse",
        SqlDialect::ClickHouse,
    ));

    // Register non-SQL dialects
    registry.register(DialectProfile::command("redis", "Redis Commands"));

    registry.register(DialectProfile::document("mongodb", "MongoDB Shell"));

    Arc::new(registry)
});

/// Get a dialect profile by driver ID
///
/// This is the main entry point for accessing dialect metadata.
pub fn get_dialect_profile(driver_id: &str) -> Option<&DialectProfile> {
    DIALECT_REGISTRY.get(driver_id)
}

/// Check if a driver uses SQL
pub fn is_sql_driver(driver_id: &str) -> bool {
    get_dialect_profile(driver_id)
        .map(|p| p.is_sql())
        .unwrap_or(false)
}

/// Get SQL dialect for a driver
pub fn get_sql_dialect(driver_id: &str) -> Option<SqlDialect> {
    get_dialect_profile(driver_id).and_then(|p| p.get_sql_dialect())
}

/// Get the tree-sitter language name for syntax highlighting
///
/// This maps driver IDs to their corresponding language names in the LanguageRegistry.
/// Falls back to "sql" for unknown drivers.
pub fn get_highlight_language(driver_id: &str) -> &'static str {
    match driver_id {
        "postgres" | "postgresql" => "postgresql",
        "mysql" | "mariadb" => "mysql",
        "sqlite" => "sqlite",
        "clickhouse" => "clickhouse",
        "redis" => "redis",
        "mongodb" | "mongo" => "mongodb",
        _ => "sql", // Generic SQL fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_profiles() {
        let profile = DialectProfile::sql("postgres", "PostgreSQL", SqlDialect::PostgreSql);
        assert!(profile.is_sql());
        assert!(!profile.skip_sql_validation());
        assert_eq!(profile.get_sql_dialect(), Some(SqlDialect::PostgreSql));
    }

    #[test]
    fn test_command_profiles() {
        let profile = DialectProfile::command("redis", "Redis Commands");
        assert!(!profile.is_sql());
        assert!(profile.skip_sql_validation());
        assert_eq!(profile.get_sql_dialect(), None);
    }

    #[test]
    fn test_document_profiles() {
        let profile = DialectProfile::document("mongodb", "MongoDB Shell");
        assert!(!profile.is_sql());
        assert!(profile.skip_sql_validation());
        assert_eq!(profile.get_sql_dialect(), None);
    }

    #[test]
    fn test_registry_postgres() {
        let profile = get_dialect_profile("postgres").expect("postgres profile should exist");
        assert_eq!(profile.id, "postgres");
        assert_eq!(profile.language_name, "PostgreSQL");
        assert!(profile.is_sql());
    }

    #[test]
    fn test_registry_redis() {
        let profile = get_dialect_profile("redis").expect("redis profile should exist");
        assert_eq!(profile.id, "redis");
        assert_eq!(profile.language_name, "Redis Commands");
        assert!(!profile.is_sql());
    }

    #[test]
    fn test_registry_mongodb() {
        let profile = get_dialect_profile("mongodb").expect("mongodb profile should exist");
        assert_eq!(profile.id, "mongodb");
        assert_eq!(profile.language_name, "MongoDB Shell");
        assert!(!profile.is_sql());
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
    fn test_get_sql_dialect() {
        assert_eq!(get_sql_dialect("postgres"), Some(SqlDialect::PostgreSql));
        assert_eq!(get_sql_dialect("mysql"), Some(SqlDialect::MySql));
        assert_eq!(get_sql_dialect("sqlite"), Some(SqlDialect::Sqlite));
        assert_eq!(get_sql_dialect("clickhouse"), Some(SqlDialect::ClickHouse));
        assert_eq!(get_sql_dialect("redis"), None);
        assert_eq!(get_sql_dialect("mongodb"), None);
    }

    #[test]
    fn test_registry_all_drivers() {
        let driver_ids: Vec<_> = DIALECT_REGISTRY.driver_ids().collect();
        assert!(driver_ids.contains(&"postgres"));
        assert!(driver_ids.contains(&"mysql"));
        assert!(driver_ids.contains(&"sqlite"));
        assert!(driver_ids.contains(&"clickhouse"));
        assert!(driver_ids.contains(&"redis"));
        assert!(driver_ids.contains(&"mongodb"));
    }

    #[test]
    fn test_sql_vs_non_sql_profiles() {
        let sql_count = DIALECT_REGISTRY.sql_profiles().count();
        let non_sql_count = DIALECT_REGISTRY.non_sql_profiles().count();

        assert_eq!(sql_count, 4); // postgres, mysql, sqlite, clickhouse
        assert_eq!(non_sql_count, 2); // redis, mongodb
    }

    #[test]
    fn test_get_highlight_language() {
        // SQL dialects
        assert_eq!(get_highlight_language("postgres"), "postgresql");
        assert_eq!(get_highlight_language("postgresql"), "postgresql");
        assert_eq!(get_highlight_language("mysql"), "mysql");
        assert_eq!(get_highlight_language("mariadb"), "mysql");
        assert_eq!(get_highlight_language("sqlite"), "sqlite");
        assert_eq!(get_highlight_language("clickhouse"), "clickhouse");

        // Non-SQL dialects
        assert_eq!(get_highlight_language("redis"), "redis");
        assert_eq!(get_highlight_language("mongodb"), "mongodb");
        assert_eq!(get_highlight_language("mongo"), "mongodb");

        // Unknown defaults to SQL
        assert_eq!(get_highlight_language("unknown"), "sql");
        assert_eq!(get_highlight_language(""), "sql");
    }
}
