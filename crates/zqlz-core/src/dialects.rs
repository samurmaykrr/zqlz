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

use crate::DialectInfo;
use crate::dialect_config::{
    DialectBundle, LanguageType, SyntaxBracketCapabilityName, SyntaxGrammarName,
    SyntaxHighlightQueryName, SyntaxParameterPlaceholderMode,
};
use crate::dialect_syntax::{SyntaxTermProfile, normalize_syntax_profile, syntax_term_profile};
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
    /// DuckDB SQL dialect
    DuckDb,
    /// Microsoft SQL Server dialect
    MsSql,
    /// Generic ANSI SQL (fallback)
    Ansi,
}

impl SqlDialect {
    /// Get the sqlparser dialect for this SQL variant
    pub fn sqlparser_dialect(&self) -> Box<dyn sqlparser::dialect::Dialect> {
        match self {
            SqlDialect::PostgreSql => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
            SqlDialect::MySql => Box::new(sqlparser::dialect::MySqlDialect {}),
            SqlDialect::Sqlite => Box::new(sqlparser::dialect::SQLiteDialect {}),
            SqlDialect::ClickHouse => Box::new(sqlparser::dialect::ClickHouseDialect {}),
            SqlDialect::DuckDb => Box::new(sqlparser::dialect::DuckDbDialect {}),
            SqlDialect::MsSql => Box::new(sqlparser::dialect::MsSqlDialect {}),
            SqlDialect::Ansi => Box::new(sqlparser::dialect::GenericDialect {}),
        }
    }

    /// Get display name for this SQL dialect
    pub fn display_name(&self) -> &'static str {
        match self {
            SqlDialect::PostgreSql => "PostgreSQL",
            SqlDialect::MySql => "MySQL",
            SqlDialect::Sqlite => "SQLite",
            SqlDialect::ClickHouse => "ClickHouse",
            SqlDialect::DuckDb => "DuckDB",
            SqlDialect::MsSql => "Microsoft SQL Server",
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
    /// JavaScript grammar for shell-style document database queries
    Javascript,
    /// Custom grammar with name
    Custom(&'static str),
}

/// Highlight query asset family used by editor syntax capture queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HighlightQueryLanguage {
    None,
    Sql,
    PostgreSql,
    MySql,
    Sqlite,
    ClickHouse,
    MongoDb,
    Redis,
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

/// SQL bind placeholder behavior used by editor overlays and parameter tooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParameterPlaceholderCapability {
    pub enabled: bool,
    pub question_mark: bool,
}

impl ParameterPlaceholderCapability {
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            question_mark: false,
        }
    }

    pub const fn sql(question_mark: bool) -> Self {
        Self {
            enabled: true,
            question_mark,
        }
    }
}

/// Editor syntax behavior selected by a driver's canonical syntax profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxDriverCapabilities {
    pub profile: &'static str,
    pub tree_sitter_grammar: TreeSitterGrammar,
    pub highlight_query_language: HighlightQueryLanguage,
    pub brackets: BracketCapability,
    pub line_comment_prefix: Option<&'static str>,
    pub block_comment_delimiters: Option<(&'static str, &'static str)>,
    pub parameter_placeholders: ParameterPlaceholderCapability,
    pub command_syntax: bool,
    pub document_syntax: bool,
    pub sql_overlays: bool,
    pub dollar_quoted_strings: bool,
    pub formatter: FormatterCapability,
    pub indent_after_keywords: Vec<&'static str>,
    pub auto_close_pairs: Vec<(char, char)>,
    pub folding: SyntaxFoldingRules,
    pub execution_unit: SyntaxExecutionUnitMode,
    pub document_symbols: SyntaxDocumentSymbolMode,
    pub overlays: SyntaxOverlayMode,
    pub diagnostics: SyntaxDiagnosticRules,
    pub completion_triggers: Vec<char>,
    pub completion_word_chars: Vec<char>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxDiagnosticRules {
    pub select_wildcard: bool,
    pub dml_without_where: bool,
    pub suspicious_drop_chain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxFoldingRules {
    pub begin_end_blocks: bool,
    pub case_blocks: bool,
    pub function_definitions: bool,
    pub parenthesis_blocks: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxExecutionUnitMode {
    SqlStatement,
    CommandLine,
    WholeDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxDocumentSymbolMode {
    SqlStatements,
    CommandCommands,
    MongodbCollections,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntaxOverlayMode {
    Sql,
    Command,
    Document,
    None,
}

pub const SQL_INDENT_AFTER_KEYWORDS: &[&str] = &["BEGIN", "THEN", "ELSE", "LOOP", "AS", "DECLARE"];
pub const SQL_AUTO_CLOSE_PAIRS: &[(char, char)] =
    &[('(', ')'), ('[', ']'), ('{', '}'), ('\'', '\''), ('"', '"')];
pub const COMMAND_AUTO_CLOSE_PAIRS: &[(char, char)] = &[('"', '"')];
pub const SQL_COMPLETION_TRIGGERS: &[char] = &['.', ' ', '(', ','];
/// Postgres additionally triggers on JSON operator characters so `data->` opens
/// the operator menu mid-token.
pub const POSTGRES_COMPLETION_TRIGGERS: &[char] = &['.', ' ', '(', ',', '-', '>', '?', '@', '#'];
pub const SQL_COMPLETION_WORD_CHARS: &[char] = &['$'];
pub const REDIS_COMPLETION_TRIGGERS: &[char] = &[' '];
pub const REDIS_COMPLETION_WORD_CHARS: &[char] = &[':', '-'];
pub const MONGODB_COMPLETION_TRIGGERS: &[char] = &['.', ':', '{', '[', ','];
pub const MONGODB_COMPLETION_WORD_CHARS: &[char] = &['$'];
pub const SQL_BLOCK_COMMENT_DELIMITERS: (&str, &str) = ("/*", "*/");
pub const SQL_FOLDING_RULES: SyntaxFoldingRules = SyntaxFoldingRules {
    begin_end_blocks: true,
    case_blocks: true,
    function_definitions: true,
    parenthesis_blocks: true,
};
pub const DOCUMENT_FOLDING_RULES: SyntaxFoldingRules = SyntaxFoldingRules {
    begin_end_blocks: false,
    case_blocks: false,
    function_definitions: false,
    parenthesis_blocks: true,
};
pub const COMMAND_FOLDING_RULES: SyntaxFoldingRules = SyntaxFoldingRules {
    begin_end_blocks: false,
    case_blocks: false,
    function_definitions: false,
    parenthesis_blocks: false,
};
pub const SQL_DIAGNOSTIC_RULES: SyntaxDiagnosticRules = SyntaxDiagnosticRules {
    select_wildcard: true,
    dml_without_where: true,
    suspicious_drop_chain: true,
};
pub const NO_DIAGNOSTIC_RULES: SyntaxDiagnosticRules = SyntaxDiagnosticRules {
    select_wildcard: false,
    dml_without_where: false,
    suspicious_drop_chain: false,
};

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

    /// Canonical editor syntax profile for highlighting terms and overlays
    pub highlight_language: &'static str,

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

    /// Primary line comment prefix used by editor commands.
    pub line_comment_prefix: Option<&'static str>,

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
            highlight_language: id,
            parser: ParserCapability::Sql(dialect),
            sql_dialect: Some(dialect),
            custom_validator: None,
            formatter: FormatterCapability::Sql,
            folding: FoldingCapability::TreeSitter,
            brackets: BracketCapability::TreeSitter,
            line_comment_prefix: Some("--"),
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
            highlight_language: id,
            parser: ParserCapability::Command,
            sql_dialect: None,
            custom_validator: None,
            formatter: FormatterCapability::Custom,
            folding: FoldingCapability::None,
            brackets: BracketCapability::Standard,
            line_comment_prefix: Some("#"),
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
            tree_sitter_language: TreeSitterGrammar::Javascript,
            highlight_language: id,
            parser: ParserCapability::Document,
            sql_dialect: None,
            custom_validator: None,
            formatter: FormatterCapability::Custom,
            folding: FoldingCapability::TreeSitter,
            brackets: BracketCapability::TreeSitter,
            line_comment_prefix: Some("//"),
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

    /// Set canonical syntax highlighting language for driver aliases
    pub const fn with_highlight_language(mut self, highlight_language: &'static str) -> Self {
        self.highlight_language = highlight_language;
        self
    }

    /// Set the primary line comment prefix for editor commands.
    pub const fn with_line_comment_prefix(
        mut self,
        line_comment_prefix: Option<&'static str>,
    ) -> Self {
        self.line_comment_prefix = line_comment_prefix;
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

    /// Get syntax terms used by editor overlays and command/document highlighters
    pub fn syntax_terms(&self) -> SyntaxTermProfile {
        syntax_term_profile(self.highlight_language)
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
    registry.register(
        DialectProfile::sql("postgres", "PostgreSQL", SqlDialect::PostgreSql)
            .with_highlight_language("postgresql"),
    );
    registry.register(DialectProfile::sql(
        "postgresql",
        "PostgreSQL",
        SqlDialect::PostgreSql,
    ));

    registry.register(DialectProfile::sql("mysql", "MySQL", SqlDialect::MySql));
    registry.register(
        DialectProfile::sql("mariadb", "MariaDB", SqlDialect::MySql)
            .with_highlight_language("mysql"),
    );

    registry.register(DialectProfile::sql("sqlite", "SQLite", SqlDialect::Sqlite));
    registry.register(
        DialectProfile::sql("turso", "Turso", SqlDialect::Sqlite).with_highlight_language("sqlite"),
    );

    registry.register(DialectProfile::sql(
        "clickhouse",
        "ClickHouse",
        SqlDialect::ClickHouse,
    ));
    registry.register(DialectProfile::sql("duckdb", "DuckDB", SqlDialect::DuckDb));
    registry.register(DialectProfile::sql(
        "mssql",
        "Microsoft SQL Server",
        SqlDialect::MsSql,
    ));
    registry.register(
        DialectProfile::sql("sqlserver", "Microsoft SQL Server", SqlDialect::MsSql)
            .with_highlight_language("mssql"),
    );

    // Register non-SQL dialects
    registry.register(DialectProfile::command("redis", "Redis Commands"));

    registry.register(DialectProfile::document("mongodb", "MongoDB Shell"));
    registry.register(
        DialectProfile::document("mongo", "MongoDB Shell").with_highlight_language("mongodb"),
    );

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
    get_dialect_profile(driver_id)
        .map(|profile| profile.highlight_language)
        .unwrap_or("sql")
}

/// Get the human-readable language name for a driver.
///
/// Falls back to the provided driver ID when the registry has no matching profile.
pub fn get_dialect_language_name(driver_id: &str) -> &str {
    get_dialect_profile(driver_id)
        .map(|profile| profile.language_name)
        .unwrap_or(driver_id)
}

/// Get the preferred single-line comment prefix for an editor syntax profile.
pub fn line_comment_prefix_for_profile(driver_id: &str) -> Option<&'static str> {
    get_dialect_profile(normalize_syntax_profile(driver_id))
        .and_then(|profile| profile.line_comment_prefix)
        .or_else(|| get_dialect_profile(driver_id).and_then(|profile| profile.line_comment_prefix))
}

/// Get tree-sitter grammar selected for a driver's editor syntax profile.
pub fn get_tree_sitter_grammar(driver_id: &str) -> TreeSitterGrammar {
    get_dialect_profile(driver_id)
        .map(|profile| profile.tree_sitter_language.clone())
        .unwrap_or(TreeSitterGrammar::Sql)
}

/// Get highlight query asset family selected for a driver's editor syntax profile.
pub fn get_highlight_query_language(driver_id: &str) -> HighlightQueryLanguage {
    match normalize_syntax_profile(driver_id) {
        "postgresql" | "duckdb" => HighlightQueryLanguage::PostgreSql,
        "mysql" => HighlightQueryLanguage::MySql,
        "sqlite" => HighlightQueryLanguage::Sqlite,
        "clickhouse" => HighlightQueryLanguage::ClickHouse,
        "mongodb" => HighlightQueryLanguage::MongoDb,
        "redis" => HighlightQueryLanguage::Redis,
        "mssql" | "sql" => HighlightQueryLanguage::Sql,
        _ => HighlightQueryLanguage::Sql,
    }
}

pub fn uses_command_syntax(driver_id: &str) -> bool {
    get_highlight_query_language(driver_id) == HighlightQueryLanguage::Redis
}

pub fn uses_document_syntax(driver_id: &str) -> bool {
    get_highlight_query_language(driver_id) == HighlightQueryLanguage::MongoDb
}

pub fn uses_sql_syntax_overlays(driver_id: &str) -> bool {
    !matches!(
        get_highlight_query_language(driver_id),
        HighlightQueryLanguage::None
            | HighlightQueryLanguage::MongoDb
            | HighlightQueryLanguage::Redis
    )
}

/// Get all syntax-facing editor capabilities for a driver/profile in one registry call.
pub fn get_syntax_driver_capabilities(driver_id: &str) -> SyntaxDriverCapabilities {
    let profile = normalize_syntax_profile(driver_id);
    let highlight_query_language = get_highlight_query_language(profile);
    SyntaxDriverCapabilities {
        profile,
        tree_sitter_grammar: get_tree_sitter_grammar(profile),
        highlight_query_language,
        brackets: get_dialect_profile(profile)
            .map(|dialect| dialect.brackets)
            .unwrap_or(BracketCapability::TreeSitter),
        line_comment_prefix: line_comment_prefix_for_profile(profile),
        block_comment_delimiters: default_block_comment_delimiters_for_profile(profile),
        parameter_placeholders: get_parameter_placeholder_capability(profile),
        command_syntax: highlight_query_language == HighlightQueryLanguage::Redis,
        document_syntax: highlight_query_language == HighlightQueryLanguage::MongoDb,
        sql_overlays: !matches!(
            highlight_query_language,
            HighlightQueryLanguage::None
                | HighlightQueryLanguage::MongoDb
                | HighlightQueryLanguage::Redis
        ),
        dollar_quoted_strings: profile == "postgresql",
        formatter: formatter_capability_for_profile(profile),
        indent_after_keywords: default_indent_after_keywords_for_profile(profile),
        auto_close_pairs: default_auto_close_pairs_for_profile(profile),
        folding: default_folding_rules_for_profile(profile),
        execution_unit: default_execution_unit_mode_for_profile(profile),
        document_symbols: default_document_symbol_mode_for_profile(profile),
        overlays: default_overlay_mode_for_profile(profile),
        diagnostics: default_diagnostic_rules_for_profile(profile),
        completion_triggers: default_completion_triggers_for_profile(profile),
        completion_word_chars: default_completion_word_chars_for_profile(profile),
    }
}

pub fn markdown_fence_language_for_capabilities(
    capabilities: &SyntaxDriverCapabilities,
) -> &'static str {
    match capabilities.highlight_query_language {
        HighlightQueryLanguage::MongoDb => "javascript",
        HighlightQueryLanguage::Redis => "redis",
        HighlightQueryLanguage::None => "",
        HighlightQueryLanguage::Sql
        | HighlightQueryLanguage::PostgreSql
        | HighlightQueryLanguage::MySql
        | HighlightQueryLanguage::Sqlite
        | HighlightQueryLanguage::ClickHouse => "sql",
    }
}

fn default_indent_after_keywords_for_profile(profile: &'static str) -> Vec<&'static str> {
    match profile {
        "redis" | "mongodb" => Vec::new(),
        _ => SQL_INDENT_AFTER_KEYWORDS.to_vec(),
    }
}

fn default_auto_close_pairs_for_profile(profile: &'static str) -> Vec<(char, char)> {
    match profile {
        "redis" => COMMAND_AUTO_CLOSE_PAIRS.to_vec(),
        _ => SQL_AUTO_CLOSE_PAIRS.to_vec(),
    }
}

fn formatter_capability_for_profile(profile: &'static str) -> FormatterCapability {
    get_dialect_profile(profile)
        .map(|dialect| dialect.formatter)
        .unwrap_or(FormatterCapability::Sql)
}

pub fn default_completion_triggers_for_profile(profile: &'static str) -> Vec<char> {
    match profile {
        "redis" => REDIS_COMPLETION_TRIGGERS.to_vec(),
        "mongodb" => MONGODB_COMPLETION_TRIGGERS.to_vec(),
        "postgresql" => POSTGRES_COMPLETION_TRIGGERS.to_vec(),
        _ => SQL_COMPLETION_TRIGGERS.to_vec(),
    }
}

pub fn default_completion_word_chars_for_profile(profile: &'static str) -> Vec<char> {
    match profile {
        "redis" => REDIS_COMPLETION_WORD_CHARS.to_vec(),
        "mongodb" => MONGODB_COMPLETION_WORD_CHARS.to_vec(),
        _ => SQL_COMPLETION_WORD_CHARS.to_vec(),
    }
}

fn default_block_comment_delimiters_for_profile(
    profile: &'static str,
) -> Option<(&'static str, &'static str)> {
    match profile {
        "redis" => None,
        _ => Some(SQL_BLOCK_COMMENT_DELIMITERS),
    }
}

fn default_folding_rules_for_profile(profile: &'static str) -> SyntaxFoldingRules {
    match profile {
        "redis" => COMMAND_FOLDING_RULES,
        "mongodb" => DOCUMENT_FOLDING_RULES,
        _ => SQL_FOLDING_RULES,
    }
}

fn default_execution_unit_mode_for_profile(profile: &'static str) -> SyntaxExecutionUnitMode {
    match profile {
        "redis" => SyntaxExecutionUnitMode::CommandLine,
        "mongodb" => SyntaxExecutionUnitMode::WholeDocument,
        _ => SyntaxExecutionUnitMode::SqlStatement,
    }
}

fn default_document_symbol_mode_for_profile(profile: &'static str) -> SyntaxDocumentSymbolMode {
    match profile {
        "redis" => SyntaxDocumentSymbolMode::CommandCommands,
        "mongodb" => SyntaxDocumentSymbolMode::MongodbCollections,
        _ => SyntaxDocumentSymbolMode::SqlStatements,
    }
}

fn default_overlay_mode_for_profile(profile: &'static str) -> SyntaxOverlayMode {
    match profile {
        "redis" => SyntaxOverlayMode::Command,
        "mongodb" => SyntaxOverlayMode::Document,
        _ => SyntaxOverlayMode::Sql,
    }
}

fn default_diagnostic_rules_for_profile(profile: &'static str) -> SyntaxDiagnosticRules {
    match profile {
        "redis" | "mongodb" => NO_DIAGNOSTIC_RULES,
        _ => SQL_DIAGNOSTIC_RULES,
    }
}

fn overlay_mode_from_config(
    profile: &'static str,
    syntax: &crate::dialect_config::SyntaxHighlightingConfig,
) -> SyntaxOverlayMode {
    use crate::dialect_config::SyntaxOverlayModeName;

    match syntax.overlays {
        SyntaxOverlayModeName::Sql => {
            if matches!(profile, "redis" | "mongodb") {
                default_overlay_mode_for_profile(profile)
            } else if syntax.sql_overlays {
                SyntaxOverlayMode::Sql
            } else {
                SyntaxOverlayMode::None
            }
        }
        SyntaxOverlayModeName::Command => SyntaxOverlayMode::Command,
        SyntaxOverlayModeName::Document => SyntaxOverlayMode::Document,
        SyntaxOverlayModeName::None => SyntaxOverlayMode::None,
    }
}

fn document_symbol_mode_from_config(
    profile: &'static str,
    syntax: &crate::dialect_config::SyntaxHighlightingConfig,
) -> SyntaxDocumentSymbolMode {
    use crate::dialect_config::SyntaxDocumentSymbolModeName;

    match syntax.document_symbols {
        SyntaxDocumentSymbolModeName::SqlStatements => {
            if matches!(profile, "redis" | "mongodb") {
                default_document_symbol_mode_for_profile(profile)
            } else {
                SyntaxDocumentSymbolMode::SqlStatements
            }
        }
        SyntaxDocumentSymbolModeName::CommandCommands => SyntaxDocumentSymbolMode::CommandCommands,
        SyntaxDocumentSymbolModeName::MongodbCollections => {
            SyntaxDocumentSymbolMode::MongodbCollections
        }
        SyntaxDocumentSymbolModeName::None => SyntaxDocumentSymbolMode::None,
    }
}

fn execution_unit_mode_from_config(
    profile: &'static str,
    syntax: &crate::dialect_config::SyntaxHighlightingConfig,
) -> SyntaxExecutionUnitMode {
    use crate::dialect_config::SyntaxExecutionUnitModeName;

    match syntax.execution_unit {
        SyntaxExecutionUnitModeName::SqlStatement => {
            if matches!(profile, "redis" | "mongodb") {
                default_execution_unit_mode_for_profile(profile)
            } else {
                SyntaxExecutionUnitMode::SqlStatement
            }
        }
        SyntaxExecutionUnitModeName::CommandLine => SyntaxExecutionUnitMode::CommandLine,
        SyntaxExecutionUnitModeName::WholeDocument => SyntaxExecutionUnitMode::WholeDocument,
    }
}

fn folding_rules_from_config(
    profile: &'static str,
    syntax: &crate::dialect_config::SyntaxHighlightingConfig,
) -> SyntaxFoldingRules {
    let defaults = default_folding_rules_for_profile(profile);
    SyntaxFoldingRules {
        begin_end_blocks: syntax
            .folding
            .begin_end_blocks
            .unwrap_or(defaults.begin_end_blocks),
        case_blocks: syntax.folding.case_blocks.unwrap_or(defaults.case_blocks),
        function_definitions: syntax
            .folding
            .function_definitions
            .unwrap_or(defaults.function_definitions),
        parenthesis_blocks: syntax
            .folding
            .parenthesis_blocks
            .unwrap_or(defaults.parenthesis_blocks),
    }
}

fn auto_close_pairs_from_config(pairs: &[String]) -> Vec<(char, char)> {
    pairs
        .iter()
        .filter_map(|pair| {
            let mut chars = pair.chars();
            let opener = chars.next()?;
            let closer = chars.next()?;
            chars.next().is_none().then_some((opener, closer))
        })
        .collect()
}

/// Build editor syntax capabilities from a declarative driver dialect bundle.
pub fn syntax_driver_capabilities_from_bundle(
    bundle: &'static DialectBundle,
) -> SyntaxDriverCapabilities {
    let syntax = &bundle.config.syntax_highlighting;
    let profile = syntax
        .profile
        .as_deref()
        .map(normalize_syntax_profile)
        .unwrap_or_else(|| normalize_syntax_profile(&bundle.config.id));
    let highlight_query_language = match syntax.highlight_query {
        SyntaxHighlightQueryName::None => HighlightQueryLanguage::None,
        SyntaxHighlightQueryName::Sql => HighlightQueryLanguage::Sql,
        SyntaxHighlightQueryName::Postgresql => HighlightQueryLanguage::PostgreSql,
        SyntaxHighlightQueryName::Mysql => HighlightQueryLanguage::MySql,
        SyntaxHighlightQueryName::Sqlite => HighlightQueryLanguage::Sqlite,
        SyntaxHighlightQueryName::Clickhouse => HighlightQueryLanguage::ClickHouse,
        SyntaxHighlightQueryName::Mongodb => HighlightQueryLanguage::MongoDb,
        SyntaxHighlightQueryName::Redis => HighlightQueryLanguage::Redis,
    };

    SyntaxDriverCapabilities {
        profile,
        tree_sitter_grammar: match syntax.grammar {
            SyntaxGrammarName::Sql => TreeSitterGrammar::Sql,
            SyntaxGrammarName::Javascript => TreeSitterGrammar::Javascript,
            SyntaxGrammarName::None => TreeSitterGrammar::None,
        },
        highlight_query_language,
        brackets: match syntax.brackets {
            SyntaxBracketCapabilityName::TreeSitter => BracketCapability::TreeSitter,
            SyntaxBracketCapabilityName::Standard => BracketCapability::Standard,
            SyntaxBracketCapabilityName::None => BracketCapability::None,
        },
        line_comment_prefix: bundle.config.comments.line_comment.as_deref(),
        block_comment_delimiters: bundle
            .config
            .comments
            .block_comment_start
            .as_deref()
            .zip(bundle.config.comments.block_comment_end.as_deref()),
        parameter_placeholders: match syntax.parameter_placeholders {
            SyntaxParameterPlaceholderMode::Disabled => ParameterPlaceholderCapability::disabled(),
            SyntaxParameterPlaceholderMode::SqlNoQuestionMark => {
                ParameterPlaceholderCapability::sql(false)
            }
            SyntaxParameterPlaceholderMode::SqlQuestionMark => {
                ParameterPlaceholderCapability::sql(true)
            }
        },
        command_syntax: syntax.command_syntax,
        document_syntax: syntax.document_syntax,
        sql_overlays: syntax.sql_overlays,
        dollar_quoted_strings: syntax.dollar_quoted_strings,
        formatter: formatter_capability_for_profile(profile),
        indent_after_keywords: if syntax.indent_after_keywords.is_empty() {
            default_indent_after_keywords_for_profile(profile)
        } else {
            syntax
                .indent_after_keywords
                .iter()
                .map(String::as_str)
                .collect()
        },
        auto_close_pairs: if syntax.auto_close_pairs.is_empty() {
            default_auto_close_pairs_for_profile(profile)
        } else {
            auto_close_pairs_from_config(&syntax.auto_close_pairs)
        },
        folding: folding_rules_from_config(profile, syntax),
        execution_unit: execution_unit_mode_from_config(profile, syntax),
        document_symbols: document_symbol_mode_from_config(profile, syntax),
        overlays: overlay_mode_from_config(profile, syntax),
        diagnostics: default_diagnostic_rules_for_profile(profile),
        completion_triggers: if syntax.completion_triggers.is_empty() {
            default_completion_triggers_for_profile(profile)
        } else {
            chars_from_config(&syntax.completion_triggers)
        },
        completion_word_chars: if syntax.completion_word_chars.is_empty() {
            default_completion_word_chars_for_profile(profile)
        } else {
            chars_from_config(&syntax.completion_word_chars)
        },
    }
}

fn chars_from_config(values: &[String]) -> Vec<char> {
    values
        .iter()
        .filter_map(|value| value.chars().next())
        .collect()
}

/// Get parameter placeholder syntax supported by a driver's query editor profile.
pub fn get_parameter_placeholder_capability(driver_id: &str) -> ParameterPlaceholderCapability {
    match normalize_syntax_profile(driver_id) {
        "mongodb" | "redis" => ParameterPlaceholderCapability::disabled(),
        "postgresql" | "mssql" => ParameterPlaceholderCapability::sql(false),
        "clickhouse" => ParameterPlaceholderCapability::disabled(),
        _ => ParameterPlaceholderCapability::sql(true),
    }
}

/// Whether the editor should protect and paint PostgreSQL dollar-quoted strings.
pub fn supports_dollar_quoted_strings(driver_id: &str) -> bool {
    normalize_syntax_profile(driver_id) == "postgresql"
}

/// Whether a quoted identifier range should be painted as an identifier.
///
/// Bracket identifiers are SQL Server-specific in the syntax highlighter because
/// the generic SQL grammar often treats square-bracket text as array/index
/// syntax in other dialects.
pub fn paints_quoted_identifier(driver_id: &str, source: &str) -> bool {
    let profile = normalize_syntax_profile(driver_id);
    if source.starts_with('[') {
        return profile == "mssql" || profile == "sqlite";
    }
    if source.starts_with('`') {
        return profile == "mysql" || profile == "sqlite" || profile == "clickhouse";
    }
    if source.starts_with('"') {
        if source.starts_with(r#""$"#) {
            return false;
        }
        return profile != "mysql";
    }

    true
}

/// Get syntax terms for a driver ID
pub fn get_syntax_term_profile(driver_id: &str) -> SyntaxTermProfile {
    get_dialect_profile(driver_id)
        .map(DialectProfile::syntax_terms)
        .unwrap_or_else(|| syntax_term_profile(driver_id))
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
        assert_eq!(profile.tree_sitter_language, TreeSitterGrammar::Javascript);
    }

    #[test]
    fn test_registry_postgres() {
        let profile = get_dialect_profile("postgres").expect("postgres profile should exist");
        assert_eq!(profile.id, "postgres");
        assert_eq!(profile.language_name, "PostgreSQL");
        assert!(profile.is_sql());
        assert_eq!(profile.highlight_language, "postgresql");

        let alias = get_dialect_profile("postgresql").expect("postgresql profile should exist");
        assert_eq!(alias.id, "postgresql");
        assert_eq!(alias.get_sql_dialect(), Some(SqlDialect::PostgreSql));
        assert_eq!(alias.highlight_language, "postgresql");
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
        assert_eq!(profile.tree_sitter_language, TreeSitterGrammar::Javascript);
    }

    #[test]
    fn line_comment_prefix_matches_syntax_profile() {
        assert_eq!(line_comment_prefix_for_profile("postgres"), Some("--"));
        assert_eq!(line_comment_prefix_for_profile("postgresql"), Some("--"));
        assert_eq!(line_comment_prefix_for_profile("mongo"), Some("//"));
        assert_eq!(line_comment_prefix_for_profile("mongodb"), Some("//"));
        assert_eq!(line_comment_prefix_for_profile("redis"), Some("#"));
    }

    #[test]
    fn test_is_sql_driver() {
        assert!(is_sql_driver("postgres"));
        assert!(is_sql_driver("postgresql"));
        assert!(is_sql_driver("mysql"));
        assert!(is_sql_driver("mariadb"));
        assert!(is_sql_driver("sqlite"));
        assert!(is_sql_driver("turso"));
        assert!(is_sql_driver("clickhouse"));
        assert!(is_sql_driver("duckdb"));
        assert!(is_sql_driver("mssql"));
        assert!(is_sql_driver("sqlserver"));
        assert!(!is_sql_driver("redis"));
        assert!(!is_sql_driver("mongodb"));
    }

    #[test]
    fn test_get_sql_dialect() {
        assert_eq!(get_sql_dialect("postgres"), Some(SqlDialect::PostgreSql));
        assert_eq!(get_sql_dialect("postgresql"), Some(SqlDialect::PostgreSql));
        assert_eq!(get_sql_dialect("mysql"), Some(SqlDialect::MySql));
        assert_eq!(get_sql_dialect("mariadb"), Some(SqlDialect::MySql));
        assert_eq!(get_sql_dialect("sqlite"), Some(SqlDialect::Sqlite));
        assert_eq!(get_sql_dialect("turso"), Some(SqlDialect::Sqlite));
        assert_eq!(get_sql_dialect("clickhouse"), Some(SqlDialect::ClickHouse));
        assert_eq!(get_sql_dialect("duckdb"), Some(SqlDialect::DuckDb));
        assert_eq!(get_sql_dialect("mssql"), Some(SqlDialect::MsSql));
        assert_eq!(get_sql_dialect("sqlserver"), Some(SqlDialect::MsSql));
        assert_eq!(get_sql_dialect("redis"), None);
        assert_eq!(get_sql_dialect("mongodb"), None);
    }

    #[test]
    fn test_sqlparser_dialects_parse_dialect_specific_sql() {
        use sqlparser::parser::Parser;

        assert!(
            Parser::parse_sql(&*SqlDialect::PostgreSql.sqlparser_dialect(), "SELECT $1").is_ok()
        );
        assert!(
            Parser::parse_sql(
                &*SqlDialect::MySql.sqlparser_dialect(),
                "SELECT `users`.`id`"
            )
            .is_ok()
        );
        assert!(
            Parser::parse_sql(
                &*SqlDialect::Sqlite.sqlparser_dialect(),
                "SELECT [users].[id]"
            )
            .is_ok()
        );
        assert!(
            Parser::parse_sql(
                &*SqlDialect::ClickHouse.sqlparser_dialect(),
                "SELECT * FROM events LIMIT 1 BY user_id"
            )
            .is_ok()
        );
        assert!(
            Parser::parse_sql(
                &*SqlDialect::DuckDb.sqlparser_dialect(),
                "SELECT {'name': 'zed', 'score': 10}"
            )
            .is_ok()
        );
        assert!(
            Parser::parse_sql(
                &*SqlDialect::MsSql.sqlparser_dialect(),
                "SELECT TOP 10 [User Name] FROM [dbo].[Events]"
            )
            .is_ok()
        );
        assert!(Parser::parse_sql(&*SqlDialect::Ansi.sqlparser_dialect(), "SELECT 1").is_ok());
    }

    #[test]
    fn test_registry_all_drivers() {
        let driver_ids: Vec<_> = DIALECT_REGISTRY.driver_ids().collect();
        assert!(driver_ids.contains(&"postgres"));
        assert!(driver_ids.contains(&"postgresql"));
        assert!(driver_ids.contains(&"mysql"));
        assert!(driver_ids.contains(&"mariadb"));
        assert!(driver_ids.contains(&"sqlite"));
        assert!(driver_ids.contains(&"turso"));
        assert!(driver_ids.contains(&"clickhouse"));
        assert!(driver_ids.contains(&"duckdb"));
        assert!(driver_ids.contains(&"mssql"));
        assert!(driver_ids.contains(&"sqlserver"));
        assert!(driver_ids.contains(&"redis"));
        assert!(driver_ids.contains(&"mongodb"));
        assert!(driver_ids.contains(&"mongo"));
    }

    #[test]
    fn test_sql_vs_non_sql_profiles() {
        let sql_count = DIALECT_REGISTRY.sql_profiles().count();
        let non_sql_count = DIALECT_REGISTRY.non_sql_profiles().count();

        assert_eq!(sql_count, 10);
        assert_eq!(non_sql_count, 3); // redis, mongodb, mongo alias
    }

    #[test]
    fn test_get_highlight_language() {
        // SQL dialects
        assert_eq!(get_highlight_language("postgres"), "postgresql");
        assert_eq!(get_highlight_language("postgresql"), "postgresql");
        assert_eq!(get_highlight_language("mysql"), "mysql");
        assert_eq!(get_highlight_language("mariadb"), "mysql");
        assert_eq!(get_highlight_language("sqlite"), "sqlite");
        assert_eq!(get_highlight_language("turso"), "sqlite");
        assert_eq!(get_highlight_language("duckdb"), "duckdb");
        assert_eq!(get_highlight_language("mssql"), "mssql");
        assert_eq!(get_highlight_language("sqlserver"), "mssql");
        assert_eq!(get_highlight_language("clickhouse"), "clickhouse");

        // Non-SQL dialects
        assert_eq!(get_highlight_language("redis"), "redis");
        assert_eq!(get_highlight_language("mongodb"), "mongodb");
        assert_eq!(get_highlight_language("mongo"), "mongodb");

        // Unknown defaults to SQL
        assert_eq!(get_highlight_language("unknown"), "sql");
        assert_eq!(get_highlight_language(""), "sql");
    }

    #[test]
    fn test_get_dialect_language_name() {
        assert_eq!(get_dialect_language_name("postgres"), "PostgreSQL");
        assert_eq!(get_dialect_language_name("postgresql"), "PostgreSQL");
        assert_eq!(get_dialect_language_name("turso"), "Turso");
        assert_eq!(
            get_dialect_language_name("sqlserver"),
            "Microsoft SQL Server"
        );
        assert_eq!(get_dialect_language_name("mongo"), "MongoDB Shell");
        assert_eq!(get_dialect_language_name("unknown"), "unknown");
    }

    #[test]
    fn test_get_tree_sitter_grammar() {
        assert_eq!(get_tree_sitter_grammar("postgres"), TreeSitterGrammar::Sql);
        assert_eq!(
            get_tree_sitter_grammar("postgresql"),
            TreeSitterGrammar::Sql
        );
        assert_eq!(get_tree_sitter_grammar("redis"), TreeSitterGrammar::None);
        assert_eq!(
            get_tree_sitter_grammar("mongodb"),
            TreeSitterGrammar::Javascript
        );
        assert_eq!(
            get_tree_sitter_grammar("mongo"),
            TreeSitterGrammar::Javascript
        );
        assert_eq!(get_tree_sitter_grammar("unknown"), TreeSitterGrammar::Sql);
    }

    #[test]
    fn test_get_highlight_query_language() {
        assert_eq!(
            get_highlight_query_language("postgres"),
            HighlightQueryLanguage::PostgreSql
        );
        assert_eq!(
            get_highlight_query_language("postgresql"),
            HighlightQueryLanguage::PostgreSql
        );
        assert_eq!(
            get_highlight_query_language("duckdb"),
            HighlightQueryLanguage::PostgreSql
        );
        assert_eq!(
            get_highlight_query_language("mysql"),
            HighlightQueryLanguage::MySql
        );
        assert_eq!(
            get_highlight_query_language("mariadb"),
            HighlightQueryLanguage::MySql
        );
        assert_eq!(
            get_highlight_query_language("mssql"),
            HighlightQueryLanguage::Sql
        );
        assert_eq!(
            get_highlight_query_language("mongodb"),
            HighlightQueryLanguage::MongoDb
        );
        assert_eq!(
            get_highlight_query_language("redis"),
            HighlightQueryLanguage::Redis
        );
        assert_eq!(
            get_highlight_query_language("unknown"),
            HighlightQueryLanguage::Sql
        );
        assert!(uses_sql_syntax_overlays("postgresql"));
        assert!(uses_sql_syntax_overlays("unknown"));
        assert!(!uses_sql_syntax_overlays("mongodb"));
        assert!(!uses_sql_syntax_overlays("redis"));
        assert!(uses_document_syntax("mongodb"));
        assert!(uses_document_syntax("mongo"));
        assert!(uses_command_syntax("redis"));
        assert!(!uses_command_syntax("mysql"));
    }

    #[test]
    fn test_get_syntax_driver_capabilities() {
        let postgres = get_syntax_driver_capabilities("postgres");
        assert_eq!(postgres.profile, "postgresql");
        assert_eq!(postgres.tree_sitter_grammar, TreeSitterGrammar::Sql);
        assert_eq!(
            postgres.highlight_query_language,
            HighlightQueryLanguage::PostgreSql
        );
        assert_eq!(
            postgres.parameter_placeholders,
            ParameterPlaceholderCapability::sql(false)
        );
        assert_eq!(postgres.brackets, BracketCapability::TreeSitter);
        assert_eq!(postgres.line_comment_prefix, Some("--"));
        assert_eq!(
            postgres.block_comment_delimiters,
            Some(SQL_BLOCK_COMMENT_DELIMITERS)
        );
        assert!(postgres.sql_overlays);
        assert!(postgres.dollar_quoted_strings);
        assert_eq!(postgres.diagnostics, SQL_DIAGNOSTIC_RULES);
        assert!(!postgres.command_syntax);
        assert!(!postgres.document_syntax);
        assert_eq!(postgres.indent_after_keywords, SQL_INDENT_AFTER_KEYWORDS);
        assert_eq!(postgres.auto_close_pairs, SQL_AUTO_CLOSE_PAIRS);

        let redis = get_syntax_driver_capabilities("redis");
        assert_eq!(redis.profile, "redis");
        assert_eq!(redis.tree_sitter_grammar, TreeSitterGrammar::None);
        assert_eq!(redis.brackets, BracketCapability::Standard);
        assert_eq!(redis.line_comment_prefix, Some("#"));
        assert_eq!(redis.block_comment_delimiters, None);
        assert!(redis.command_syntax);
        assert!(!redis.sql_overlays);
        assert_eq!(redis.diagnostics, NO_DIAGNOSTIC_RULES);
        assert_eq!(
            redis.parameter_placeholders,
            ParameterPlaceholderCapability::disabled()
        );
        assert!(redis.indent_after_keywords.is_empty());
        assert_eq!(redis.auto_close_pairs, COMMAND_AUTO_CLOSE_PAIRS);

        let mongo = get_syntax_driver_capabilities("mongo");
        assert_eq!(mongo.profile, "mongodb");
        assert_eq!(mongo.tree_sitter_grammar, TreeSitterGrammar::Javascript);
        assert_eq!(mongo.brackets, BracketCapability::TreeSitter);
        assert_eq!(mongo.line_comment_prefix, Some("//"));
        assert_eq!(
            mongo.block_comment_delimiters,
            Some(SQL_BLOCK_COMMENT_DELIMITERS)
        );
        assert!(mongo.document_syntax);
        assert!(!mongo.sql_overlays);
        assert_eq!(mongo.diagnostics, NO_DIAGNOSTIC_RULES);
        assert!(mongo.indent_after_keywords.is_empty());
        assert_eq!(mongo.auto_close_pairs, SQL_AUTO_CLOSE_PAIRS);
    }

    #[test]
    fn test_markdown_fence_language_comes_from_syntax_capabilities() {
        assert_eq!(
            markdown_fence_language_for_capabilities(&get_syntax_driver_capabilities("postgres")),
            "sql"
        );
        assert_eq!(
            markdown_fence_language_for_capabilities(&get_syntax_driver_capabilities("redis")),
            "redis"
        );
        assert_eq!(
            markdown_fence_language_for_capabilities(&get_syntax_driver_capabilities("mongodb")),
            "javascript"
        );
    }

    #[test]
    fn test_parameter_placeholder_capability() {
        assert_eq!(
            get_parameter_placeholder_capability("postgresql"),
            ParameterPlaceholderCapability::sql(false)
        );
        assert_eq!(
            get_parameter_placeholder_capability("postgres"),
            ParameterPlaceholderCapability::sql(false)
        );
        assert_eq!(
            get_parameter_placeholder_capability("mysql"),
            ParameterPlaceholderCapability::sql(true)
        );
        assert_eq!(
            get_parameter_placeholder_capability("mssql"),
            ParameterPlaceholderCapability::sql(false)
        );
        assert_eq!(
            get_parameter_placeholder_capability("sqlserver"),
            ParameterPlaceholderCapability::sql(false)
        );
        assert_eq!(
            get_parameter_placeholder_capability("clickhouse"),
            ParameterPlaceholderCapability::disabled()
        );
        assert_eq!(
            get_parameter_placeholder_capability("redis"),
            ParameterPlaceholderCapability::disabled()
        );
        assert_eq!(
            get_parameter_placeholder_capability("mongodb"),
            ParameterPlaceholderCapability::disabled()
        );
        assert!(supports_dollar_quoted_strings("postgresql"));
        assert!(supports_dollar_quoted_strings("postgres"));
        assert!(!supports_dollar_quoted_strings("mysql"));
        assert!(!supports_dollar_quoted_strings("mongodb"));
        assert!(paints_quoted_identifier("postgresql", r#""table name""#));
        assert!(paints_quoted_identifier("mysql", "`table name`"));
        assert!(!paints_quoted_identifier("mysql", r#""string value""#));
        assert!(paints_quoted_identifier("sqlite", r#""table name""#));
        assert!(paints_quoted_identifier("sqlite", "[table name]"));
        assert!(paints_quoted_identifier("clickhouse", "`table name`"));
        assert!(paints_quoted_identifier("mssql", "[table name]"));
        assert!(paints_quoted_identifier("sqlserver", "[table name]"));
        assert!(!paints_quoted_identifier("postgresql", "[array_index]"));
        assert!(!paints_quoted_identifier("mysql", "[array_index]"));
    }

    #[test]
    fn test_registry_profiles_own_syntax_terms_for_aliases() {
        assert!(
            get_syntax_term_profile("postgres")
                .dialect_types
                .contains(&"JSONB")
        );
        assert!(
            get_syntax_term_profile("mariadb")
                .dialect_keywords
                .contains(&"FORCE")
        );
        assert!(
            get_syntax_term_profile("turso")
                .dialect_keywords
                .contains(&"INDEXED")
        );
        assert!(
            get_syntax_term_profile("sqlserver")
                .dialect_keywords
                .contains(&"TOP")
        );
        assert!(
            get_syntax_term_profile("mongo")
                .dialect_types
                .contains(&"NUMBERLONG")
        );
    }
}
