//! Declarative Dialect Configuration
//!
//! This module defines the types for loading dialect configuration from TOML files.
//! Each database driver provides a `dialect/` folder with:
//! - `config.toml` - Dialect configuration (language type, syntax features)
//! - `completions.toml` - Keywords, functions, snippets for IntelliSense
//! - `highlights.scm` - Tree-sitter highlighting queries (optional)
//!
//! This replaces the hardcoded `DialectInfo` approach with declarative configuration.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Language type determines how queries are validated and highlighted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LanguageType {
    /// SQL-based languages (PostgreSQL, MySQL, SQLite, etc.)
    /// Uses SQL parser and tree-sitter-sql grammar
    #[default]
    Sql,
    /// Command-based languages (Redis, etc.)
    /// No SQL parsing, simple command syntax
    Command,
    /// Document-based languages (MongoDB, etc.)
    /// JSON/BSON document syntax
    Document,
    /// Custom language with its own grammar
    Custom,
}

/// Grammar type for syntax highlighting
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GrammarType {
    /// Use tree-sitter grammar (specify name)
    TreeSitter(String),
    /// No tree-sitter grammar, use keyword-based highlighting
    #[default]
    None,
    /// Custom grammar loaded from highlights.scm
    Custom,
}

/// Grammar configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GrammarConfig {
    /// Grammar type
    #[serde(rename = "type", default)]
    pub grammar_type: GrammarType,
    /// Grammar name for tree-sitter (e.g., "sql", "json")
    #[serde(default)]
    pub name: Option<String>,
}

/// Parser configuration for diagnostics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParserConfig {
    /// Skip SQL parser validation (for non-SQL languages)
    #[serde(default)]
    pub skip_sql_validation: bool,
    /// Skip tree-sitter error detection
    #[serde(default)]
    pub skip_tree_sitter_errors: bool,
    /// Use custom validation rules from diagnostics.toml
    #[serde(default)]
    pub custom_validator: bool,
}

/// Syntax features configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyntaxConfig {
    /// Character used to quote identifiers (e.g., '"' for SQL standard, '`' for MySQL)
    #[serde(default = "default_identifier_quote")]
    pub identifier_quote: char,
    /// Character used for string literals (usually '\'')
    #[serde(default = "default_string_quote")]
    pub string_quote: char,
    /// Whether identifiers are case-sensitive
    #[serde(default)]
    pub case_sensitive: bool,
    /// Statement terminator character (e.g., ';' for SQL, '\n' for Redis)
    #[serde(default = "default_statement_terminator")]
    pub statement_terminator: char,
    /// Whether comments are supported
    #[serde(default = "default_supports_comments")]
    pub supports_comments: bool,
}

fn default_identifier_quote() -> char {
    '"'
}
fn default_string_quote() -> char {
    '\''
}
fn default_statement_terminator() -> char {
    ';'
}
fn default_supports_comments() -> bool {
    true
}

impl Default for SyntaxConfig {
    fn default() -> Self {
        Self {
            identifier_quote: '"',
            string_quote: '\'',
            case_sensitive: false,
            statement_terminator: ';',
            supports_comments: true,
        }
    }
}

/// Comment styles configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommentsConfig {
    /// Single-line comment prefix (e.g., "--" for SQL)
    #[serde(default)]
    pub line_comment: Option<String>,
    /// Block comment start (e.g., "/*")
    #[serde(default)]
    pub block_comment_start: Option<String>,
    /// Block comment end (e.g., "*/")
    #[serde(default)]
    pub block_comment_end: Option<String>,
}

/// Main dialect configuration loaded from config.toml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DialectConfig {
    /// Dialect identifier (matches driver id, e.g., "redis", "sqlite")
    pub id: String,
    /// Human-readable display name
    pub display_name: String,
    /// Language type determines validation behavior
    #[serde(default)]
    pub language_type: LanguageType,
    /// Grammar configuration
    #[serde(default)]
    pub grammar: GrammarConfig,
    /// Parser/diagnostics configuration
    #[serde(default)]
    pub parser: ParserConfig,
    /// Syntax features
    #[serde(default)]
    pub syntax: SyntaxConfig,
    /// Comment styles
    #[serde(default)]
    pub comments: CommentsConfig,
}

impl Default for DialectConfig {
    fn default() -> Self {
        Self {
            id: "generic".to_string(),
            display_name: "SQL".to_string(),
            language_type: LanguageType::Sql,
            grammar: GrammarConfig::default(),
            parser: ParserConfig::default(),
            syntax: SyntaxConfig::default(),
            comments: CommentsConfig {
                line_comment: Some("--".to_string()),
                block_comment_start: Some("/*".to_string()),
                block_comment_end: Some("*/".to_string()),
            },
        }
    }
}

impl DialectConfig {
    /// Check if this dialect uses SQL parsing
    pub fn is_sql(&self) -> bool {
        self.language_type == LanguageType::Sql
    }

    /// Check if SQL validation should be skipped
    pub fn skip_sql_validation(&self) -> bool {
        self.parser.skip_sql_validation || self.language_type != LanguageType::Sql
    }

    /// Check if tree-sitter errors should be skipped
    pub fn skip_tree_sitter_errors(&self) -> bool {
        self.parser.skip_tree_sitter_errors
            || matches!(self.grammar.grammar_type, GrammarType::None)
    }
}

// ============================================================================
// Completions Configuration (completions.toml)
// ============================================================================

/// Keyword category for grouping in completions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeywordCategory {
    /// Data Query Language (SELECT, FROM, WHERE, etc.)
    Query,
    /// Data Manipulation Language (INSERT, UPDATE, DELETE)
    Mutation,
    /// Data Definition Language (CREATE, ALTER, DROP)
    Definition,
    /// Data Control Language (GRANT, REVOKE)
    Control,
    /// Transaction Control (BEGIN, COMMIT, ROLLBACK)
    Transaction,
    /// Clauses (JOIN, ON, HAVING, GROUP BY, etc.)
    Clause,
    /// Operators (AND, OR, NOT, IN, LIKE, etc.)
    Operator,
    /// Database-specific commands (PRAGMA, SHOW, etc.)
    Server,
    /// Other keywords
    #[default]
    Other,
}

/// Function category for grouping in completions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionCategory {
    /// Aggregate functions (COUNT, SUM, AVG, etc.)
    Aggregate,
    /// Window functions (ROW_NUMBER, RANK, etc.)
    Window,
    /// String functions (CONCAT, SUBSTRING, etc.)
    String,
    /// Numeric/Math functions (ABS, ROUND, etc.)
    Numeric,
    /// Date/Time functions (NOW, DATE, etc.)
    Datetime,
    /// Type conversion (CAST, CONVERT, etc.)
    Conversion,
    /// Conditional (CASE, COALESCE, NULLIF, etc.)
    Conditional,
    /// JSON functions
    Json,
    /// Array functions
    Array,
    /// Database-specific functions
    Database,
    /// Scripting (e.g., Lua for Redis)
    Scripting,
    /// Other functions
    #[default]
    Other,
}

/// Data type category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataTypeCategory {
    /// Integer types
    Integer,
    /// Floating point types
    Float,
    /// Fixed precision (DECIMAL, NUMERIC)
    Decimal,
    /// Character/String types
    String,
    /// Binary data
    Binary,
    /// Boolean
    Boolean,
    /// Date only
    Date,
    /// Time only
    Time,
    /// Date and time
    Datetime,
    /// Interval/Duration
    Interval,
    /// JSON/JSONB
    Json,
    /// Arrays
    Array,
    /// UUID
    Uuid,
    /// Network types
    Network,
    /// Geometric types
    Geometry,
    /// Other types
    #[default]
    Other,
}

/// Keyword definition for completions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeywordDef {
    /// Keyword name (e.g., "SELECT", "GET")
    pub name: String,
    /// Category for grouping
    #[serde(default)]
    pub category: KeywordCategory,
    /// Brief description shown in completion list
    #[serde(default)]
    pub description: Option<String>,
    /// Full documentation (markdown supported)
    #[serde(default)]
    pub documentation: Option<String>,
    /// VSCode-style snippet (e.g., "SELECT ${1:columns} FROM ${2:table}")
    #[serde(default)]
    pub snippet: Option<String>,
}

/// Function definition for completions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDef {
    /// Function name
    pub name: String,
    /// Category for grouping
    #[serde(default)]
    pub category: FunctionCategory,
    /// Function signature (e.g., "COUNT(expression)")
    #[serde(default)]
    pub signature: Option<String>,
    /// Return type description
    #[serde(default)]
    pub return_type: Option<String>,
    /// Brief description
    #[serde(default)]
    pub description: Option<String>,
    /// Full documentation
    #[serde(default)]
    pub documentation: Option<String>,
}

/// Data type definition for completions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataTypeDef {
    /// Type name (e.g., "VARCHAR", "INTEGER")
    pub name: String,
    /// Category for grouping
    #[serde(default)]
    pub category: DataTypeCategory,
    /// Aliases (e.g., ["INT"] for "INTEGER")
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Whether this type accepts a length parameter
    #[serde(default)]
    pub accepts_length: bool,
    /// Whether this type accepts scale (for decimals)
    #[serde(default)]
    pub accepts_scale: bool,
    /// Brief description
    #[serde(default)]
    pub description: Option<String>,
}

/// Snippet definition for common patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnippetDef {
    /// Snippet name/label
    pub name: String,
    /// Trigger prefix (what user types to activate)
    pub prefix: String,
    /// Snippet body (VSCode-style with ${1:placeholder})
    pub body: String,
    /// Brief description
    #[serde(default)]
    pub description: Option<String>,
}

/// Completions configuration loaded from completions.toml
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompletionsConfig {
    /// Keywords (commands for Redis, SQL keywords for SQL databases)
    #[serde(default, rename = "keyword")]
    pub keywords: Vec<KeywordDef>,
    /// Functions
    #[serde(default, rename = "function")]
    pub functions: Vec<FunctionDef>,
    /// Data types
    #[serde(default, rename = "data_type")]
    pub data_types: Vec<DataTypeDef>,
    /// Snippets for common patterns
    #[serde(default, rename = "snippet")]
    pub snippets: Vec<SnippetDef>,
}

// ============================================================================
// Diagnostics Configuration (Custom Validation Rules)
// ============================================================================

/// Severity level for validation rules
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

/// Type of validation rule
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationType {
    /// Check argument count for a command/function
    ArgumentCount,
    /// Check argument type (string, integer, etc.)
    ArgumentType,
    /// Check for deprecated syntax
    Deprecated,
    /// Check for invalid combinations
    InvalidCombination,
    /// Custom pattern-based validation
    Pattern,
}

/// Validation rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    /// Rule ID (unique within dialect)
    pub id: String,
    /// Command or keyword this rule applies to (case-insensitive, * for all)
    pub command: String,
    /// Type of validation
    #[serde(rename = "type")]
    pub validation_type: ValidationType,
    /// Severity level
    pub severity: DiagnosticSeverity,
    /// Error message to display
    pub message: String,
    /// Min argument count (for ArgumentCount type)
    #[serde(default)]
    pub min_args: Option<usize>,
    /// Max argument count (for ArgumentCount type, None = unlimited)
    #[serde(default)]
    pub max_args: Option<usize>,
    /// Expected argument types (for ArgumentType validation)
    #[serde(default)]
    pub arg_types: Vec<String>,
    /// Regex pattern (for Pattern type)
    #[serde(default)]
    pub pattern: Option<String>,
    /// Help text with suggestions
    #[serde(default)]
    pub help: Option<String>,
}

/// Diagnostics configuration loaded from diagnostics.toml
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticsConfig {
    /// Validation rules
    #[serde(default, rename = "rule")]
    pub rules: Vec<ValidationRule>,
}

// ============================================================================
// Full Dialect Bundle (all files combined)
// ============================================================================

/// Complete dialect definition loaded from all files in dialect/ folder
#[derive(Debug, Clone, Default)]
pub struct DialectBundle {
    /// Main configuration from config.toml
    pub config: DialectConfig,
    /// Completions from completions.toml
    pub completions: CompletionsConfig,
    /// Syntax highlighting queries from highlights.scm (optional)
    pub highlights: Option<String>,
    /// Custom validation rules from diagnostics.toml
    pub diagnostics: Option<DiagnosticsConfig>,
}

impl DialectBundle {
    /// Create a new dialect bundle with the given configuration
    pub fn new(config: DialectConfig, completions: CompletionsConfig) -> Self {
        Self {
            config,
            completions,
            highlights: None,
            diagnostics: None,
        }
    }

    /// Set the highlights.scm content
    pub fn with_highlights(mut self, highlights: impl Into<String>) -> Self {
        self.highlights = Some(highlights.into());
        self
    }

    /// Set the diagnostics configuration
    pub fn with_diagnostics(mut self, diagnostics: DiagnosticsConfig) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    /// Get validation rules
    pub fn validation_rules(&self) -> &[ValidationRule] {
        self.diagnostics
            .as_ref()
            .map(|d| d.rules.as_slice())
            .unwrap_or(&[])
    }

    /// Get dialect ID
    pub fn id(&self) -> &str {
        &self.config.id
    }

    /// Get display name
    pub fn display_name(&self) -> &str {
        &self.config.display_name
    }

    /// Check if this is a SQL dialect
    pub fn is_sql(&self) -> bool {
        self.config.is_sql()
    }

    /// Get all keyword names for completion
    pub fn keyword_names(&self) -> impl Iterator<Item = &str> {
        self.completions.keywords.iter().map(|k| k.name.as_str())
    }

    /// Get all function names for completion
    pub fn function_names(&self) -> impl Iterator<Item = &str> {
        self.completions.functions.iter().map(|f| f.name.as_str())
    }

    /// Get all data type names for completion
    pub fn data_type_names(&self) -> impl Iterator<Item = &str> {
        self.completions.data_types.iter().map(|t| t.name.as_str())
    }

    /// Get keywords by category
    pub fn keywords_by_category(
        &self,
        category: KeywordCategory,
    ) -> impl Iterator<Item = &KeywordDef> {
        self.completions
            .keywords
            .iter()
            .filter(move |k| k.category == category)
    }

    /// Get functions by category
    pub fn functions_by_category(
        &self,
        category: FunctionCategory,
    ) -> impl Iterator<Item = &FunctionDef> {
        self.completions
            .functions
            .iter()
            .filter(move |f| f.category == category)
    }

    /// Check if a function is an aggregate function
    pub fn is_aggregate_function(&self, name: &str) -> bool {
        self.completions
            .functions
            .iter()
            .any(|f| f.name.eq_ignore_ascii_case(name) && f.category == FunctionCategory::Aggregate)
    }

    /// Get keyword documentation
    pub fn get_keyword_doc(&self, name: &str) -> Option<String> {
        self.completions
            .keywords
            .iter()
            .find(|k| k.name.eq_ignore_ascii_case(name))
            .map(|k| {
                let mut doc = format!("**{}**\n\n", k.name);
                if let Some(desc) = &k.description {
                    doc.push_str(desc);
                }
                if let Some(full_doc) = &k.documentation {
                    doc.push_str("\n\n");
                    doc.push_str(full_doc);
                }
                doc
            })
    }

    /// Get function documentation
    pub fn get_function_doc(&self, name: &str) -> Option<String> {
        self.completions
            .functions
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
            .map(|f| {
                let mut doc = format!("**{}**\n\n", f.name);
                if let Some(sig) = &f.signature {
                    doc.push_str(&format!("```\n{}\n```\n\n", sig));
                }
                if let Some(desc) = &f.description {
                    doc.push_str(desc);
                }
                if let Some(ret) = &f.return_type {
                    doc.push_str(&format!("\n\n**Returns:** {}", ret));
                }
                if let Some(full_doc) = &f.documentation {
                    doc.push_str("\n\n");
                    doc.push_str(full_doc);
                }
                doc
            })
    }
}

// ============================================================================
// Conversion from DialectBundle to legacy DialectInfo
// ============================================================================

use crate::dialect::{
    CommentStyles, DataTypeCategory as LegacyDataTypeCategory, DataTypeInfo as LegacyDataTypeInfo,
    DialectInfo, ExplainConfig, FunctionCategory as LegacyFunctionCategory,
    KeywordCategory as LegacyKeywordCategory, KeywordInfo, SqlFunctionInfo,
};

impl From<KeywordCategory> for LegacyKeywordCategory {
    fn from(cat: KeywordCategory) -> Self {
        match cat {
            KeywordCategory::Query => LegacyKeywordCategory::Dql,
            KeywordCategory::Mutation => LegacyKeywordCategory::Dml,
            KeywordCategory::Definition => LegacyKeywordCategory::Ddl,
            KeywordCategory::Control => LegacyKeywordCategory::Dcl,
            KeywordCategory::Transaction => LegacyKeywordCategory::Transaction,
            KeywordCategory::Clause => LegacyKeywordCategory::Clause,
            KeywordCategory::Operator => LegacyKeywordCategory::Operator,
            KeywordCategory::Server => LegacyKeywordCategory::DatabaseSpecific,
            KeywordCategory::Other => LegacyKeywordCategory::Other,
        }
    }
}

impl From<FunctionCategory> for LegacyFunctionCategory {
    fn from(cat: FunctionCategory) -> Self {
        match cat {
            FunctionCategory::Aggregate => LegacyFunctionCategory::Aggregate,
            FunctionCategory::Window => LegacyFunctionCategory::Window,
            FunctionCategory::String => LegacyFunctionCategory::String,
            FunctionCategory::Numeric => LegacyFunctionCategory::Numeric,
            FunctionCategory::Datetime => LegacyFunctionCategory::DateTime,
            FunctionCategory::Conversion => LegacyFunctionCategory::Conversion,
            FunctionCategory::Conditional => LegacyFunctionCategory::Conditional,
            FunctionCategory::Json => LegacyFunctionCategory::Json,
            FunctionCategory::Array => LegacyFunctionCategory::Array,
            FunctionCategory::Database | FunctionCategory::Scripting => {
                LegacyFunctionCategory::DatabaseSpecific
            }
            FunctionCategory::Other => LegacyFunctionCategory::Other,
        }
    }
}

impl From<DataTypeCategory> for LegacyDataTypeCategory {
    fn from(cat: DataTypeCategory) -> Self {
        match cat {
            DataTypeCategory::Integer => LegacyDataTypeCategory::Integer,
            DataTypeCategory::Float => LegacyDataTypeCategory::Float,
            DataTypeCategory::Decimal => LegacyDataTypeCategory::Decimal,
            DataTypeCategory::String => LegacyDataTypeCategory::String,
            DataTypeCategory::Binary => LegacyDataTypeCategory::Binary,
            DataTypeCategory::Boolean => LegacyDataTypeCategory::Boolean,
            DataTypeCategory::Date => LegacyDataTypeCategory::Date,
            DataTypeCategory::Time => LegacyDataTypeCategory::Time,
            DataTypeCategory::Datetime => LegacyDataTypeCategory::DateTime,
            DataTypeCategory::Interval => LegacyDataTypeCategory::Interval,
            DataTypeCategory::Json => LegacyDataTypeCategory::Json,
            DataTypeCategory::Array => LegacyDataTypeCategory::Array,
            DataTypeCategory::Uuid => LegacyDataTypeCategory::Uuid,
            DataTypeCategory::Network => LegacyDataTypeCategory::Network,
            DataTypeCategory::Geometry => LegacyDataTypeCategory::Geometry,
            DataTypeCategory::Other => LegacyDataTypeCategory::Other,
        }
    }
}

impl From<&DialectBundle> for DialectInfo {
    fn from(bundle: &DialectBundle) -> Self {
        let config = &bundle.config;
        let completions = &bundle.completions;

        DialectInfo {
            id: Cow::Owned(config.id.clone()),
            display_name: Cow::Owned(config.display_name.clone()),
            keywords: completions
                .keywords
                .iter()
                .map(|k| {
                    let mut info = KeywordInfo::new(
                        Box::leak(k.name.clone().into_boxed_str()),
                        k.category.into(),
                    );
                    if let Some(desc) = &k.description {
                        info.description = Some(Cow::Owned(desc.clone()));
                    }
                    if let Some(doc) = &k.documentation {
                        info.documentation = Some(Cow::Owned(doc.clone()));
                    }
                    info
                })
                .collect(),
            functions: completions
                .functions
                .iter()
                .map(|f| {
                    let mut info = SqlFunctionInfo::new(
                        Box::leak(f.name.clone().into_boxed_str()),
                        f.category.into(),
                    );
                    if let Some(sig) = &f.signature {
                        info = info.with_signature(Box::leak(sig.clone().into_boxed_str()));
                    }
                    if let Some(desc) = &f.description {
                        info.description = Some(Cow::Owned(desc.clone()));
                    }
                    if let Some(ret) = &f.return_type {
                        info.return_type = Some(Cow::Owned(ret.clone()));
                    }
                    info
                })
                .collect(),
            data_types: completions
                .data_types
                .iter()
                .map(|t| {
                    let mut info = LegacyDataTypeInfo::new(
                        Box::leak(t.name.clone().into_boxed_str()),
                        t.category.into(),
                    );
                    info.aliases = t.aliases.iter().map(|a| Cow::Owned(a.clone())).collect();
                    info.accepts_length = t.accepts_length;
                    info.accepts_scale = t.accepts_scale;
                    if let Some(desc) = &t.description {
                        info.description = Some(Cow::Owned(desc.clone()));
                    }
                    info
                })
                .collect(),
            table_options: vec![],
            auto_increment: None,
            identifier_quote: config.syntax.identifier_quote,
            string_quote: config.syntax.string_quote,
            case_sensitive_identifiers: config.syntax.case_sensitive,
            statement_terminator: config.syntax.statement_terminator,
            comment_styles: CommentStyles {
                line_comment: config
                    .comments
                    .line_comment
                    .as_ref()
                    .map(|s| Cow::Owned(s.clone())),
                block_comment_start: config
                    .comments
                    .block_comment_start
                    .as_ref()
                    .map(|s| Cow::Owned(s.clone())),
                block_comment_end: config
                    .comments
                    .block_comment_end
                    .as_ref()
                    .map(|s| Cow::Owned(s.clone())),
            },
            explain_config: ExplainConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dialect_config_defaults() {
        let config = DialectConfig::default();
        assert_eq!(config.id, "generic");
        assert_eq!(config.language_type, LanguageType::Sql);
        assert!(!config.skip_sql_validation());
    }

    #[test]
    fn test_redis_config() {
        let config = DialectConfig {
            id: "redis".to_string(),
            display_name: "Redis Commands".to_string(),
            language_type: LanguageType::Command,
            grammar: GrammarConfig {
                grammar_type: GrammarType::None,
                name: None,
            },
            parser: ParserConfig {
                skip_sql_validation: true,
                skip_tree_sitter_errors: true,
                custom_validator: false,
            },
            syntax: SyntaxConfig {
                identifier_quote: '"',
                string_quote: '"',
                case_sensitive: true,
                statement_terminator: '\n',
                supports_comments: false,
            },
            comments: CommentsConfig::default(),
        };

        assert!(!config.is_sql());
        assert!(config.skip_sql_validation());
        assert!(config.skip_tree_sitter_errors());
    }

    #[test]
    fn test_parse_config_toml() {
        let toml_str = r#"
id = "redis"
display_name = "Redis Commands"
language_type = "command"

[grammar]
type = "none"

[parser]
skip_sql_validation = true
skip_tree_sitter_errors = true

[syntax]
identifier_quote = '"'
string_quote = '"'
case_sensitive = true
statement_terminator = """

"""
supports_comments = false
"#;

        let config: DialectConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.id, "redis");
        assert_eq!(config.language_type, LanguageType::Command);
        assert!(config.skip_sql_validation());
        assert_eq!(config.syntax.statement_terminator, '\n');
    }

    #[test]
    fn test_parse_completions_toml() {
        let toml_str = r#"
[[keyword]]
name = "GET"
category = "query"
description = "Get value of key"

[[keyword]]
name = "SET"
category = "mutation"
description = "Set string value"
snippet = "SET ${1:key} ${2:value}"

[[function]]
name = "redis.call"
category = "scripting"
signature = "redis.call(command, ...)"
return_type = "any"
"#;

        let completions: CompletionsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(completions.keywords.len(), 2);
        assert_eq!(completions.keywords[0].name, "GET");
        assert_eq!(
            completions.keywords[1].snippet,
            Some("SET ${1:key} ${2:value}".to_string())
        );
        assert_eq!(completions.functions.len(), 1);
        assert_eq!(
            completions.functions[0].category,
            FunctionCategory::Scripting
        );
    }
}
