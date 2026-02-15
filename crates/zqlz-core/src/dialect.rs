//! SQL Dialect Metadata
//!
//! This module defines types that drivers provide to describe their SQL dialect,
//! including keywords, functions, data types, and syntax rules.
//!
//! The key principle is that **drivers provide all metadata** - the rest of the
//! codebase consumes this metadata without hardcoding per-driver logic.

use std::borrow::Cow;

/// Information about a SQL keyword
#[derive(Debug, Clone)]
pub struct KeywordInfo {
    /// The keyword (e.g., "SELECT", "PRAGMA")
    pub keyword: Cow<'static, str>,
    /// Category for grouping
    pub category: KeywordCategory,
    /// Brief description
    pub description: Option<Cow<'static, str>>,
    /// Full documentation
    pub documentation: Option<Cow<'static, str>>,
}

impl KeywordInfo {
    pub const fn new(keyword: &'static str, category: KeywordCategory) -> Self {
        Self {
            keyword: Cow::Borrowed(keyword),
            category,
            description: None,
            documentation: None,
        }
    }

    pub const fn with_desc(
        keyword: &'static str,
        category: KeywordCategory,
        description: &'static str,
    ) -> Self {
        Self {
            keyword: Cow::Borrowed(keyword),
            category,
            description: Some(Cow::Borrowed(description)),
            documentation: None,
        }
    }
}

/// Categories of SQL keywords
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeywordCategory {
    /// Data Query Language (SELECT, FROM, WHERE, etc.)
    Dql,
    /// Data Manipulation Language (INSERT, UPDATE, DELETE)
    Dml,
    /// Data Definition Language (CREATE, ALTER, DROP)
    Ddl,
    /// Data Control Language (GRANT, REVOKE)
    Dcl,
    /// Transaction Control (BEGIN, COMMIT, ROLLBACK)
    Transaction,
    /// Clauses (JOIN, ON, HAVING, GROUP BY, etc.)
    Clause,
    /// Operators (AND, OR, NOT, IN, LIKE, etc.)
    Operator,
    /// Database-specific commands (PRAGMA, SHOW, etc.)
    DatabaseSpecific,
    /// Other keywords
    Other,
}

/// Information about a SQL function for completions/LSP
#[derive(Debug, Clone)]
pub struct SqlFunctionInfo {
    /// Function name (e.g., "COUNT", "SUBSTRING")
    pub name: Cow<'static, str>,
    /// Category for grouping
    pub category: FunctionCategory,
    /// Brief description
    pub description: Option<Cow<'static, str>>,
    /// Function signature(s) for display
    pub signatures: Vec<FunctionSignature>,
    /// Return type description
    pub return_type: Option<Cow<'static, str>>,
}

impl SqlFunctionInfo {
    pub const fn new(name: &'static str, category: FunctionCategory) -> Self {
        Self {
            name: Cow::Borrowed(name),
            category,
            description: None,
            signatures: Vec::new(),
            return_type: None,
        }
    }

    pub fn with_signature(mut self, signature: &'static str) -> Self {
        self.signatures.push(FunctionSignature {
            signature: Cow::Borrowed(signature),
            parameters: Vec::new(),
        });
        self
    }
}

/// Function signature with parameter info
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Full signature string (e.g., "SUBSTRING(string, start, length)")
    pub signature: Cow<'static, str>,
    /// Individual parameters
    pub parameters: Vec<SqlParameterInfo>,
}

/// Parameter information for function signatures (dialect)
#[derive(Debug, Clone)]
pub struct SqlParameterInfo {
    /// Parameter name
    pub name: Cow<'static, str>,
    /// Parameter type
    pub param_type: Cow<'static, str>,
    /// Whether optional
    pub optional: bool,
    /// Description
    pub description: Option<Cow<'static, str>>,
}

/// Categories of SQL functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    DateTime,
    /// Type conversion (CAST, CONVERT, etc.)
    Conversion,
    /// Conditional (CASE, COALESCE, NULLIF, etc.)
    Conditional,
    /// JSON functions
    Json,
    /// Array functions
    Array,
    /// Database-specific
    DatabaseSpecific,
    /// Other
    Other,
}

/// Information about a SQL data type
#[derive(Debug, Clone)]
pub struct DataTypeInfo {
    /// Type name as used in DDL (e.g., "VARCHAR", "INTEGER")
    pub name: Cow<'static, str>,
    /// Aliases (e.g., "INT" for "INTEGER")
    pub aliases: Vec<Cow<'static, str>>,
    /// Category for grouping
    pub category: DataTypeCategory,
    /// Whether this type accepts a length/precision parameter
    pub accepts_length: bool,
    /// Whether this type accepts scale (for decimals)
    pub accepts_scale: bool,
    /// Default length if applicable
    pub default_length: Option<u32>,
    /// Maximum length if applicable
    pub max_length: Option<u64>,
    /// Brief description
    pub description: Option<Cow<'static, str>>,
    /// Example usage
    pub example: Option<Cow<'static, str>>,
}

impl DataTypeInfo {
    pub const fn new(name: &'static str, category: DataTypeCategory) -> Self {
        Self {
            name: Cow::Borrowed(name),
            aliases: Vec::new(),
            category,
            accepts_length: false,
            accepts_scale: false,
            default_length: None,
            max_length: None,
            description: None,
            example: None,
        }
    }

    pub fn with_length(mut self, default: Option<u32>, max: Option<u64>) -> Self {
        self.accepts_length = true;
        self.default_length = default;
        self.max_length = max;
        self
    }
}

/// Categories of SQL data types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataTypeCategory {
    /// Integer types (INTEGER, BIGINT, etc.)
    Integer,
    /// Floating point (REAL, DOUBLE, etc.)
    Float,
    /// Fixed precision (DECIMAL, NUMERIC)
    Decimal,
    /// Character/String (VARCHAR, TEXT, etc.)
    String,
    /// Binary data (BLOB, BYTEA, etc.)
    Binary,
    /// Boolean
    Boolean,
    /// Date only
    Date,
    /// Time only
    Time,
    /// Date and time
    DateTime,
    /// Interval/Duration
    Interval,
    /// JSON/JSONB
    Json,
    /// Arrays
    Array,
    /// UUID
    Uuid,
    /// Network types (INET, CIDR, etc.)
    Network,
    /// Geometric types
    Geometry,
    /// Other database-specific
    Other,
}

/// Table option definition for the table designer
#[derive(Debug, Clone)]
pub struct TableOptionDef {
    /// Option key (e.g., "without_rowid", "engine")
    pub key: Cow<'static, str>,
    /// Display label
    pub label: Cow<'static, str>,
    /// Option type
    pub option_type: TableOptionType,
    /// Default value
    pub default_value: Option<Cow<'static, str>>,
    /// Description
    pub description: Option<Cow<'static, str>>,
    /// Possible values (for enums)
    pub choices: Vec<Cow<'static, str>>,
}

/// Types of table options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableOptionType {
    /// Boolean checkbox
    Boolean,
    /// Text input
    Text,
    /// Dropdown/select
    Choice,
    /// Numeric input
    Number,
}

/// Auto-increment configuration
#[derive(Debug, Clone)]
pub struct AutoIncrementInfo {
    /// Keyword used (e.g., "AUTOINCREMENT", "AUTO_INCREMENT", "SERIAL")
    pub keyword: Cow<'static, str>,
    /// Syntax style
    pub style: AutoIncrementStyle,
    /// Description
    pub description: Option<Cow<'static, str>>,
}

/// How auto-increment is specified
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoIncrementStyle {
    /// Keyword after column definition (SQLite AUTOINCREMENT, MySQL AUTO_INCREMENT)
    Suffix,
    /// Special type name (PostgreSQL SERIAL, BIGSERIAL)
    TypeName,
    /// Generated always as identity (SQL standard)
    Generated,
}

/// Complete dialect information provided by a driver
#[derive(Debug, Clone)]
pub struct DialectInfo {
    /// Dialect identifier (e.g., "sqlite", "postgresql")
    pub id: Cow<'static, str>,
    /// Display name
    pub display_name: Cow<'static, str>,
    /// All supported keywords
    pub keywords: Vec<KeywordInfo>,
    /// All supported functions
    pub functions: Vec<SqlFunctionInfo>,
    /// All supported data types
    pub data_types: Vec<DataTypeInfo>,
    /// Table options for the designer
    pub table_options: Vec<TableOptionDef>,
    /// Auto-increment configuration
    pub auto_increment: Option<AutoIncrementInfo>,
    /// Identifier quote character (e.g., '"' for SQL standard, '`' for MySQL)
    pub identifier_quote: char,
    /// String literal quote (usually '\'')
    pub string_quote: char,
    /// Whether identifiers are case-sensitive
    pub case_sensitive_identifiers: bool,
    /// Statement terminator (usually ';')
    pub statement_terminator: char,
    /// Comment styles supported
    pub comment_styles: CommentStyles,
    /// EXPLAIN configuration for query plan analysis
    pub explain_config: ExplainConfig,
}

impl Default for DialectInfo {
    fn default() -> Self {
        Self {
            id: Cow::Borrowed("generic"),
            display_name: Cow::Borrowed("SQL"),
            keywords: Vec::new(),
            functions: Vec::new(),
            data_types: Vec::new(),
            table_options: Vec::new(),
            auto_increment: None,
            identifier_quote: '"',
            string_quote: '\'',
            case_sensitive_identifiers: false,
            statement_terminator: ';',
            comment_styles: CommentStyles::default(),
            explain_config: ExplainConfig::default(),
        }
    }
}

impl DialectInfo {
    /// Get keywords by category
    pub fn keywords_by_category(
        &self,
        category: KeywordCategory,
    ) -> impl Iterator<Item = &KeywordInfo> {
        self.keywords.iter().filter(move |k| k.category == category)
    }

    /// Get functions by category
    pub fn functions_by_category(
        &self,
        category: FunctionCategory,
    ) -> impl Iterator<Item = &SqlFunctionInfo> {
        self.functions
            .iter()
            .filter(move |f| f.category == category)
    }

    /// Get aggregate functions
    pub fn aggregate_functions(&self) -> impl Iterator<Item = &SqlFunctionInfo> {
        self.functions_by_category(FunctionCategory::Aggregate)
    }

    /// Get data types by category
    pub fn data_types_by_category(
        &self,
        category: DataTypeCategory,
    ) -> impl Iterator<Item = &DataTypeInfo> {
        self.data_types
            .iter()
            .filter(move |t| t.category == category)
    }

    /// Check if a name is an aggregate function
    pub fn is_aggregate_function(&self, name: &str) -> bool {
        let name_upper = name.to_uppercase();
        self.functions.iter().any(|f| {
            f.category == FunctionCategory::Aggregate && f.name.to_uppercase() == name_upper
        })
    }

    /// Get all keyword names (for completion)
    pub fn keyword_names(&self) -> impl Iterator<Item = &str> {
        self.keywords.iter().map(|k| k.keyword.as_ref())
    }

    /// Get all function names (for completion)
    pub fn function_names(&self) -> impl Iterator<Item = &str> {
        self.functions.iter().map(|f| f.name.as_ref())
    }

    /// Get all data type names (for completion)
    pub fn data_type_names(&self) -> impl Iterator<Item = &str> {
        self.data_types.iter().map(|t| t.name.as_ref())
    }
}

/// Comment style support
#[derive(Debug, Clone, Default)]
pub struct CommentStyles {
    /// Single-line comment prefix (e.g., "--")
    pub line_comment: Option<Cow<'static, str>>,
    /// Block comment start (e.g., "/*")
    pub block_comment_start: Option<Cow<'static, str>>,
    /// Block comment end (e.g., "*/")
    pub block_comment_end: Option<Cow<'static, str>>,
}

/// Configuration for EXPLAIN functionality
///
/// Different databases have different EXPLAIN syntax and capabilities.
/// This struct captures those differences so the query service can
/// generate the correct EXPLAIN statements for each database.
#[derive(Debug, Clone)]
pub struct ExplainConfig {
    /// Format string for basic EXPLAIN (e.g., "EXPLAIN {sql}")
    /// Use `{sql}` as placeholder for the SQL statement
    pub explain_format: Cow<'static, str>,

    /// Optional format string for query plan (e.g., "EXPLAIN QUERY PLAN {sql}" for SQLite)
    /// Use `{sql}` as placeholder for the SQL statement
    /// If None, only the basic explain is used
    pub query_plan_format: Option<Cow<'static, str>>,

    /// Optional format for EXPLAIN with ANALYZE (executes the query)
    /// Use `{sql}` as placeholder for the SQL statement
    pub analyze_format: Option<Cow<'static, str>>,

    /// Description of what the basic EXPLAIN returns
    pub explain_description: Cow<'static, str>,

    /// Description of what the query plan returns (if available)
    pub query_plan_description: Option<Cow<'static, str>>,

    /// Whether EXPLAIN ANALYZE is safe to run (some databases modify data)
    pub analyze_is_safe: bool,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            explain_format: Cow::Borrowed("EXPLAIN {sql}"),
            query_plan_format: None,
            analyze_format: None,
            explain_description: Cow::Borrowed("Shows query execution plan"),
            query_plan_description: None,
            analyze_is_safe: false,
        }
    }
}

impl ExplainConfig {
    /// Create SQLite-specific EXPLAIN configuration
    pub fn sqlite() -> Self {
        Self {
            explain_format: Cow::Borrowed("EXPLAIN {sql}"),
            query_plan_format: Some(Cow::Borrowed("EXPLAIN QUERY PLAN {sql}")),
            analyze_format: None,
            explain_description: Cow::Borrowed("Shows virtual machine opcodes (bytecode)"),
            query_plan_description: Some(Cow::Borrowed(
                "Shows high-level query plan with scan types",
            )),
            analyze_is_safe: false,
        }
    }

    /// Create PostgreSQL-specific EXPLAIN configuration
    pub fn postgresql() -> Self {
        Self {
            explain_format: Cow::Borrowed("EXPLAIN {sql}"),
            query_plan_format: Some(Cow::Borrowed("EXPLAIN (FORMAT TEXT) {sql}")),
            analyze_format: Some(Cow::Borrowed(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}",
            )),
            explain_description: Cow::Borrowed("Shows query execution plan"),
            query_plan_description: Some(Cow::Borrowed("Shows detailed execution plan with costs")),
            analyze_is_safe: false, // EXPLAIN ANALYZE actually executes the query
        }
    }

    /// Create MySQL-specific EXPLAIN configuration
    pub fn mysql() -> Self {
        Self {
            explain_format: Cow::Borrowed("EXPLAIN {sql}"),
            query_plan_format: Some(Cow::Borrowed("EXPLAIN FORMAT=TREE {sql}")),
            analyze_format: Some(Cow::Borrowed("EXPLAIN ANALYZE {sql}")),
            explain_description: Cow::Borrowed("Shows query execution plan"),
            query_plan_description: Some(Cow::Borrowed("Shows tree-formatted execution plan")),
            analyze_is_safe: false,
        }
    }

    /// Format the basic EXPLAIN SQL
    pub fn format_explain(&self, sql: &str) -> String {
        self.explain_format.replace("{sql}", sql)
    }

    /// Format the query plan SQL (if available)
    pub fn format_query_plan(&self, sql: &str) -> Option<String> {
        self.query_plan_format
            .as_ref()
            .map(|fmt| fmt.replace("{sql}", sql))
    }

    /// Format the EXPLAIN ANALYZE SQL (if available)
    pub fn format_analyze(&self, sql: &str) -> Option<String> {
        self.analyze_format
            .as_ref()
            .map(|fmt| fmt.replace("{sql}", sql))
    }
}

impl CommentStyles {
    pub const fn sql_standard() -> Self {
        Self {
            line_comment: Some(Cow::Borrowed("--")),
            block_comment_start: Some(Cow::Borrowed("/*")),
            block_comment_end: Some(Cow::Borrowed("*/")),
        }
    }
}
