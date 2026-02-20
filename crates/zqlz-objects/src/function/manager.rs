//! Function manager implementation
//!
//! Provides functionality for creating, altering, and dropping user-defined
//! functions across different database dialects.

use serde::{Deserialize, Serialize};

/// Parameter mode for function parameters
///
/// # Examples
///
/// ```
/// use zqlz_objects::FunctionParameterMode;
///
/// let mode = FunctionParameterMode::In;
/// assert!(mode.is_input());
/// assert_eq!(mode.as_sql(), "IN");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionParameterMode {
    /// Input parameter (default)
    In,
    /// Output parameter
    Out,
    /// Input/Output parameter
    InOut,
    /// Variadic parameter (accepts multiple values)
    Variadic,
}

impl FunctionParameterMode {
    /// Check if this parameter accepts input
    pub fn is_input(&self) -> bool {
        matches!(
            self,
            FunctionParameterMode::In
                | FunctionParameterMode::InOut
                | FunctionParameterMode::Variadic
        )
    }

    /// Check if this parameter provides output
    pub fn is_output(&self) -> bool {
        matches!(
            self,
            FunctionParameterMode::Out | FunctionParameterMode::InOut
        )
    }

    /// Convert to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            FunctionParameterMode::In => "IN",
            FunctionParameterMode::Out => "OUT",
            FunctionParameterMode::InOut => "INOUT",
            FunctionParameterMode::Variadic => "VARIADIC",
        }
    }
}

impl Default for FunctionParameterMode {
    fn default() -> Self {
        FunctionParameterMode::In
    }
}

/// Function parameter definition
///
/// # Examples
///
/// ```
/// use zqlz_objects::{FunctionParam, FunctionParameterMode};
///
/// let param = FunctionParam::new("user_id", "INTEGER");
/// assert_eq!(param.name(), "user_id");
/// assert_eq!(param.data_type(), "INTEGER");
/// assert_eq!(param.mode(), FunctionParameterMode::In);
///
/// let param_with_default = FunctionParam::new("active", "BOOLEAN")
///     .with_default("TRUE");
/// assert_eq!(param_with_default.default_value(), Some("TRUE"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionParam {
    name: String,
    data_type: String,
    mode: FunctionParameterMode,
    default_value: Option<String>,
}

impl FunctionParam {
    /// Create a new function parameter
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            mode: FunctionParameterMode::In,
            default_value: None,
        }
    }

    /// Set the parameter mode
    pub fn with_mode(mut self, mode: FunctionParameterMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set a default value for the parameter
    pub fn with_default(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }

    /// Get the parameter name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the data type
    pub fn data_type(&self) -> &str {
        &self.data_type
    }

    /// Get the parameter mode
    pub fn mode(&self) -> FunctionParameterMode {
        self.mode
    }

    /// Get the default value (if set)
    pub fn default_value(&self) -> Option<&str> {
        self.default_value.as_deref()
    }
}

/// Function volatility classification
///
/// Determines how the database optimizer can handle function calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionVolatility {
    /// Function always returns the same result for the same arguments (cacheable)
    Immutable,
    /// Function returns same result within a single table scan
    Stable,
    /// Function can return different results even with same arguments (default)
    Volatile,
}

impl FunctionVolatility {
    /// Convert to SQL keyword
    pub fn as_sql(&self) -> &'static str {
        match self {
            FunctionVolatility::Immutable => "IMMUTABLE",
            FunctionVolatility::Stable => "STABLE",
            FunctionVolatility::Volatile => "VOLATILE",
        }
    }
}

impl Default for FunctionVolatility {
    fn default() -> Self {
        FunctionVolatility::Volatile
    }
}

/// Function behavior with NULL arguments
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullBehavior {
    /// Function is called even when any argument is NULL (default)
    CalledOnNullInput,
    /// Function returns NULL immediately if any argument is NULL
    ReturnsNullOnNullInput,
    /// SQL standard alias for CalledOnNullInput
    Strict,
}

impl NullBehavior {
    /// Convert to SQL keyword (PostgreSQL style)
    pub fn as_sql(&self) -> &'static str {
        match self {
            NullBehavior::CalledOnNullInput => "CALLED ON NULL INPUT",
            NullBehavior::ReturnsNullOnNullInput => "RETURNS NULL ON NULL INPUT",
            NullBehavior::Strict => "STRICT",
        }
    }
}

impl Default for NullBehavior {
    fn default() -> Self {
        NullBehavior::CalledOnNullInput
    }
}

/// Function security mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMode {
    /// Function runs with privileges of the caller (default)
    Invoker,
    /// Function runs with privileges of the definer
    Definer,
}

impl SecurityMode {
    /// Convert to SQL clause (PostgreSQL style)
    pub fn as_sql(&self) -> &'static str {
        match self {
            SecurityMode::Invoker => "SECURITY INVOKER",
            SecurityMode::Definer => "SECURITY DEFINER",
        }
    }
}

impl Default for SecurityMode {
    fn default() -> Self {
        SecurityMode::Invoker
    }
}

/// Function language
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionLanguage {
    /// SQL language
    Sql,
    /// PL/pgSQL (PostgreSQL procedural language)
    PlPgSql,
    /// Python (via plpython)
    Python,
    /// JavaScript (various implementations)
    JavaScript,
    /// Custom language name
    Custom(String),
}

impl FunctionLanguage {
    /// Convert to SQL language name
    pub fn as_sql(&self) -> &str {
        match self {
            FunctionLanguage::Sql => "SQL",
            FunctionLanguage::PlPgSql => "plpgsql",
            FunctionLanguage::Python => "plpython3u",
            FunctionLanguage::JavaScript => "plv8",
            FunctionLanguage::Custom(name) => name,
        }
    }
}

impl Default for FunctionLanguage {
    fn default() -> Self {
        FunctionLanguage::Sql
    }
}

/// Specification for creating a new function
///
/// # Examples
///
/// ```
/// use zqlz_objects::{FunctionSpec, FunctionParam, FunctionLanguage, FunctionVolatility};
///
/// let spec = FunctionSpec::new("add_numbers", "INTEGER")
///     .with_parameter(FunctionParam::new("a", "INTEGER"))
///     .with_parameter(FunctionParam::new("b", "INTEGER"))
///     .with_body("SELECT a + b")
///     .with_language(FunctionLanguage::Sql)
///     .with_volatility(FunctionVolatility::Immutable);
///
/// assert_eq!(spec.name(), "add_numbers");
/// assert_eq!(spec.return_type(), "INTEGER");
/// assert_eq!(spec.parameters().len(), 2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSpec {
    name: String,
    schema: Option<String>,
    parameters: Vec<FunctionParam>,
    return_type: String,
    returns_set: bool,
    returns_table: Option<Vec<FunctionParam>>,
    body: Option<String>,
    language: FunctionLanguage,
    volatility: FunctionVolatility,
    null_behavior: NullBehavior,
    security: SecurityMode,
    parallel_safe: bool,
    cost: Option<u32>,
    rows: Option<u32>,
    comment: Option<String>,
}

impl FunctionSpec {
    /// Create a new function specification
    pub fn new(name: impl Into<String>, return_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            parameters: Vec::new(),
            return_type: return_type.into(),
            returns_set: false,
            returns_table: None,
            body: None,
            language: FunctionLanguage::Sql,
            volatility: FunctionVolatility::Volatile,
            null_behavior: NullBehavior::CalledOnNullInput,
            security: SecurityMode::Invoker,
            parallel_safe: false,
            cost: None,
            rows: None,
            comment: None,
        }
    }

    /// Set the schema for this function
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add a parameter to the function
    pub fn with_parameter(mut self, param: FunctionParam) -> Self {
        self.parameters.push(param);
        self
    }

    /// Add multiple parameters to the function
    pub fn with_parameters(mut self, params: Vec<FunctionParam>) -> Self {
        self.parameters.extend(params);
        self
    }

    /// Set that this function returns a set (SETOF)
    pub fn returns_set(mut self) -> Self {
        self.returns_set = true;
        self
    }

    /// Set that this function returns a table
    pub fn returns_table(mut self, columns: Vec<FunctionParam>) -> Self {
        self.returns_table = Some(columns);
        self.returns_set = true;
        self
    }

    /// Set the function body
    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Set the function language
    pub fn with_language(mut self, language: FunctionLanguage) -> Self {
        self.language = language;
        self
    }

    /// Set the function volatility
    pub fn with_volatility(mut self, volatility: FunctionVolatility) -> Self {
        self.volatility = volatility;
        self
    }

    /// Set the NULL behavior
    pub fn with_null_behavior(mut self, behavior: NullBehavior) -> Self {
        self.null_behavior = behavior;
        self
    }

    /// Set the security mode
    pub fn with_security(mut self, security: SecurityMode) -> Self {
        self.security = security;
        self
    }

    /// Mark the function as parallel safe
    pub fn parallel_safe(mut self) -> Self {
        self.parallel_safe = true;
        self
    }

    /// Set the estimated execution cost
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = Some(cost);
        self
    }

    /// Set the estimated number of rows returned (for set-returning functions)
    pub fn with_rows(mut self, rows: u32) -> Self {
        self.rows = Some(rows);
        self
    }

    /// Set a comment for the function
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Get the function name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the schema (if set)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the parameters
    pub fn parameters(&self) -> &[FunctionParam] {
        &self.parameters
    }

    /// Get the return type
    pub fn return_type(&self) -> &str {
        &self.return_type
    }

    /// Check if this function returns a set
    pub fn is_set_returning(&self) -> bool {
        self.returns_set
    }

    /// Get the table columns (if RETURNS TABLE)
    pub fn table_columns(&self) -> Option<&[FunctionParam]> {
        self.returns_table.as_deref()
    }

    /// Get the function body
    pub fn body(&self) -> Option<&str> {
        self.body.as_deref()
    }

    /// Get the function language
    pub fn language(&self) -> &FunctionLanguage {
        &self.language
    }

    /// Get the function volatility
    pub fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }

    /// Get the NULL behavior
    pub fn null_behavior(&self) -> NullBehavior {
        self.null_behavior
    }

    /// Get the security mode
    pub fn security(&self) -> SecurityMode {
        self.security
    }

    /// Check if the function is parallel safe
    pub fn is_parallel_safe(&self) -> bool {
        self.parallel_safe
    }

    /// Get the estimated cost
    pub fn cost(&self) -> Option<u32> {
        self.cost
    }

    /// Get the estimated rows
    pub fn rows(&self) -> Option<u32> {
        self.rows
    }

    /// Get the comment (if set)
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Get the fully qualified name (schema.name or just name)
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

/// Database dialect for function generation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionDialect {
    /// PostgreSQL syntax
    PostgreSQL,
    /// MySQL/MariaDB syntax
    MySQL,
    /// SQLite syntax (limited function support)
    SQLite,
    /// Microsoft SQL Server syntax
    MsSql,
}

impl FunctionDialect {
    /// Check if this dialect supports user-defined functions
    pub fn supports_functions(&self) -> bool {
        !matches!(self, FunctionDialect::SQLite)
    }

    /// Check if this dialect supports OUT parameters
    pub fn supports_out_parameters(&self) -> bool {
        matches!(
            self,
            FunctionDialect::PostgreSQL | FunctionDialect::MySQL | FunctionDialect::MsSql
        )
    }

    /// Check if this dialect supports RETURNS TABLE
    pub fn supports_returns_table(&self) -> bool {
        matches!(self, FunctionDialect::PostgreSQL | FunctionDialect::MsSql)
    }

    /// Check if this dialect supports volatility annotations
    pub fn supports_volatility(&self) -> bool {
        matches!(self, FunctionDialect::PostgreSQL)
    }

    /// Check if this dialect supports function security modes
    pub fn supports_security_mode(&self) -> bool {
        matches!(
            self,
            FunctionDialect::PostgreSQL | FunctionDialect::MySQL | FunctionDialect::MsSql
        )
    }

    /// Check if this dialect supports parallel safety annotations
    pub fn supports_parallel(&self) -> bool {
        matches!(self, FunctionDialect::PostgreSQL)
    }

    /// Check if this dialect requires BEGIN/END blocks
    pub fn requires_block(&self) -> bool {
        matches!(self, FunctionDialect::MySQL | FunctionDialect::MsSql)
    }

    /// Get the default function language for this dialect
    pub fn default_language(&self) -> FunctionLanguage {
        match self {
            FunctionDialect::PostgreSQL => FunctionLanguage::PlPgSql,
            FunctionDialect::MySQL => FunctionLanguage::Sql,
            FunctionDialect::SQLite => FunctionLanguage::Sql,
            FunctionDialect::MsSql => FunctionLanguage::Sql,
        }
    }
}

/// Error type for function operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionError {
    /// Function name is empty
    EmptyName,
    /// Return type is empty
    EmptyReturnType,
    /// Function body is empty
    EmptyBody,
    /// User-defined functions not supported by this dialect
    FunctionsNotSupported,
    /// OUT parameters not supported by this dialect
    OutParametersNotSupported,
    /// RETURNS TABLE not supported by this dialect
    ReturnsTableNotSupported,
    /// Volatility annotations not supported by this dialect
    VolatilityNotSupported,
    /// Security mode not supported by this dialect
    SecurityModeNotSupported,
    /// Parameter name is empty
    EmptyParameterName,
    /// Parameter type is empty
    EmptyParameterType,
    /// Invalid parameter definition
    InvalidParameter(String),
}

impl std::fmt::Display for FunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionError::EmptyName => write!(f, "Function name cannot be empty"),
            FunctionError::EmptyReturnType => write!(f, "Return type cannot be empty"),
            FunctionError::EmptyBody => write!(f, "Function body cannot be empty"),
            FunctionError::FunctionsNotSupported => {
                write!(
                    f,
                    "User-defined functions are not supported by this dialect"
                )
            }
            FunctionError::OutParametersNotSupported => {
                write!(f, "OUT parameters are not supported by this dialect")
            }
            FunctionError::ReturnsTableNotSupported => {
                write!(f, "RETURNS TABLE is not supported by this dialect")
            }
            FunctionError::VolatilityNotSupported => {
                write!(
                    f,
                    "Volatility annotations are not supported by this dialect"
                )
            }
            FunctionError::SecurityModeNotSupported => {
                write!(f, "Security mode is not supported by this dialect")
            }
            FunctionError::EmptyParameterName => write!(f, "Parameter name cannot be empty"),
            FunctionError::EmptyParameterType => write!(f, "Parameter type cannot be empty"),
            FunctionError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
        }
    }
}

impl std::error::Error for FunctionError {}

/// Function manager for generating function DDL statements
///
/// # Examples
///
/// ```
/// use zqlz_objects::{FunctionManager, FunctionDialect, FunctionSpec, FunctionParam};
///
/// let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
/// let spec = FunctionSpec::new("get_user_name", "VARCHAR")
///     .with_parameter(FunctionParam::new("user_id", "INTEGER"))
///     .with_body("SELECT name FROM users WHERE id = user_id");
///
/// let sql = manager.build_create_function(&spec).unwrap();
/// assert!(sql.contains("CREATE FUNCTION"));
/// assert!(sql.contains("get_user_name"));
/// ```
pub struct FunctionManager {
    dialect: FunctionDialect,
}

impl FunctionManager {
    /// Create a new function manager for the specified dialect
    pub fn new(dialect: FunctionDialect) -> Self {
        Self { dialect }
    }

    /// Get the dialect for this manager
    pub fn dialect(&self) -> FunctionDialect {
        self.dialect
    }

    /// Validate a function specification
    pub fn validate(&self, spec: &FunctionSpec) -> Result<(), FunctionError> {
        if spec.name.trim().is_empty() {
            return Err(FunctionError::EmptyName);
        }
        if spec.return_type.trim().is_empty() && spec.returns_table.is_none() {
            return Err(FunctionError::EmptyReturnType);
        }
        if spec
            .body
            .as_ref()
            .map(|b| b.trim().is_empty())
            .unwrap_or(true)
        {
            return Err(FunctionError::EmptyBody);
        }
        if !self.dialect.supports_functions() {
            return Err(FunctionError::FunctionsNotSupported);
        }

        for param in &spec.parameters {
            if param.name.trim().is_empty() {
                return Err(FunctionError::EmptyParameterName);
            }
            if param.data_type.trim().is_empty() {
                return Err(FunctionError::EmptyParameterType);
            }
            if param.mode.is_output() && !self.dialect.supports_out_parameters() {
                return Err(FunctionError::OutParametersNotSupported);
            }
        }

        if spec.returns_table.is_some() && !self.dialect.supports_returns_table() {
            return Err(FunctionError::ReturnsTableNotSupported);
        }

        Ok(())
    }

    /// Build a CREATE FUNCTION statement
    pub fn build_create_function(&self, spec: &FunctionSpec) -> Result<String, FunctionError> {
        self.validate(spec)?;

        match self.dialect {
            FunctionDialect::PostgreSQL => self.build_postgres_function(spec),
            FunctionDialect::MySQL => self.build_mysql_function(spec),
            FunctionDialect::SQLite => Err(FunctionError::FunctionsNotSupported),
            FunctionDialect::MsSql => self.build_mssql_function(spec),
        }
    }

    fn build_postgres_function(&self, spec: &FunctionSpec) -> Result<String, FunctionError> {
        let qualified_name = self.quote_identifier(&spec.qualified_name());
        let params = self.build_parameters_clause(&spec.parameters);
        let returns = self.build_returns_clause(spec);
        let body = spec.body.as_ref().ok_or(FunctionError::EmptyBody)?;

        let mut attributes = Vec::new();

        attributes.push(format!("LANGUAGE {}", spec.language.as_sql()));

        if spec.volatility != FunctionVolatility::Volatile {
            attributes.push(spec.volatility.as_sql().to_string());
        }

        if spec.null_behavior == NullBehavior::Strict
            || spec.null_behavior == NullBehavior::ReturnsNullOnNullInput
        {
            attributes.push(spec.null_behavior.as_sql().to_string());
        }

        if spec.security == SecurityMode::Definer {
            attributes.push(spec.security.as_sql().to_string());
        }

        if spec.parallel_safe {
            attributes.push("PARALLEL SAFE".to_string());
        }

        if let Some(cost) = spec.cost {
            attributes.push(format!("COST {}", cost));
        }

        if let Some(rows) = spec.rows {
            attributes.push(format!("ROWS {}", rows));
        }

        let attributes_str = if attributes.is_empty() {
            String::new()
        } else {
            format!("\n{}", attributes.join("\n"))
        };

        let body_delimiter = if body.contains("$$") { "$func$" } else { "$$" };

        Ok(format!(
            "CREATE FUNCTION {}({})\nRETURNS {}{}\nAS {}\n{}\n{}",
            qualified_name,
            params,
            returns,
            attributes_str,
            body_delimiter,
            body.trim(),
            body_delimiter
        ))
    }

    fn build_mysql_function(&self, spec: &FunctionSpec) -> Result<String, FunctionError> {
        let qualified_name = self.quote_identifier(&spec.qualified_name());
        let params = self.build_parameters_clause(&spec.parameters);
        let body = spec.body.as_ref().ok_or(FunctionError::EmptyBody)?;

        let mut modifiers = Vec::new();

        if spec.volatility == FunctionVolatility::Immutable {
            modifiers.push("DETERMINISTIC");
        } else {
            modifiers.push("NOT DETERMINISTIC");
        }

        if spec.security == SecurityMode::Definer {
            modifiers.push("SQL SECURITY DEFINER");
        } else {
            modifiers.push("SQL SECURITY INVOKER");
        }

        let modifiers_str = modifiers.join("\n");

        Ok(format!(
            "CREATE FUNCTION {}({})\nRETURNS {}\n{}\nBEGIN\n{}\nEND",
            qualified_name,
            params,
            spec.return_type,
            modifiers_str,
            body.trim()
        ))
    }

    fn build_mssql_function(&self, spec: &FunctionSpec) -> Result<String, FunctionError> {
        let qualified_name = self.quote_identifier(&spec.qualified_name());
        let params = self.build_mssql_parameters_clause(&spec.parameters);
        let body = spec.body.as_ref().ok_or(FunctionError::EmptyBody)?;

        if let Some(table_cols) = &spec.returns_table {
            let table_def = table_cols
                .iter()
                .map(|p| format!("{} {}", self.quote_identifier(p.name()), p.data_type()))
                .collect::<Vec<_>>()
                .join(", ");

            Ok(format!(
                "CREATE FUNCTION {}({})\nRETURNS TABLE ({})\nAS\nRETURN (\n{}\n)",
                qualified_name,
                params,
                table_def,
                body.trim()
            ))
        } else if spec.returns_set {
            let table_def = format!("@result TABLE (value {})", spec.return_type);
            Ok(format!(
                "CREATE FUNCTION {}({})\nRETURNS {}\nAS\nBEGIN\n{}\nRETURN\nEND",
                qualified_name,
                params,
                table_def,
                body.trim()
            ))
        } else {
            Ok(format!(
                "CREATE FUNCTION {}({})\nRETURNS {}\nAS\nBEGIN\n{}\nEND",
                qualified_name,
                params,
                spec.return_type,
                body.trim()
            ))
        }
    }

    fn build_parameters_clause(&self, params: &[FunctionParam]) -> String {
        params
            .iter()
            .map(|p| {
                let mode_prefix = if p.mode != FunctionParameterMode::In {
                    format!("{} ", p.mode.as_sql())
                } else {
                    String::new()
                };

                let default_suffix = p
                    .default_value
                    .as_ref()
                    .map(|d| format!(" DEFAULT {}", d))
                    .unwrap_or_default();

                format!(
                    "{}{} {}{}",
                    mode_prefix, p.name, p.data_type, default_suffix
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn build_mssql_parameters_clause(&self, params: &[FunctionParam]) -> String {
        params
            .iter()
            .map(|p| {
                let output_suffix = if p.mode.is_output() { " OUTPUT" } else { "" };

                let default_suffix = p
                    .default_value
                    .as_ref()
                    .map(|d| format!(" = {}", d))
                    .unwrap_or_default();

                format!(
                    "@{} {}{}{}",
                    p.name, p.data_type, default_suffix, output_suffix
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn build_returns_clause(&self, spec: &FunctionSpec) -> String {
        if let Some(table_cols) = &spec.returns_table {
            let cols = table_cols
                .iter()
                .map(|p| format!("{} {}", p.name, p.data_type))
                .collect::<Vec<_>>()
                .join(", ");
            format!("TABLE ({})", cols)
        } else if spec.returns_set {
            format!("SETOF {}", spec.return_type)
        } else {
            spec.return_type.clone()
        }
    }

    /// Build a CREATE OR REPLACE FUNCTION statement (where supported)
    pub fn build_create_or_replace_function(
        &self,
        spec: &FunctionSpec,
    ) -> Result<String, FunctionError> {
        self.validate(spec)?;

        match self.dialect {
            FunctionDialect::PostgreSQL => {
                let create_sql = self.build_postgres_function(spec)?;
                Ok(create_sql.replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
            }
            FunctionDialect::MySQL => Err(FunctionError::InvalidParameter(
                "MySQL does not support CREATE OR REPLACE for functions".to_string(),
            )),
            FunctionDialect::SQLite => Err(FunctionError::FunctionsNotSupported),
            FunctionDialect::MsSql => {
                let create_sql = self.build_mssql_function(spec)?;
                Ok(create_sql.replacen("CREATE FUNCTION", "CREATE OR ALTER FUNCTION", 1))
            }
        }
    }

    /// Build a DROP FUNCTION statement
    ///
    /// # Arguments
    /// * `name` - Function name (can be schema-qualified)
    /// * `params` - Parameter types (needed for PostgreSQL to identify overloaded functions)
    /// * `if_exists` - Add IF EXISTS clause
    /// * `cascade` - Add CASCADE clause (PostgreSQL)
    pub fn build_drop_function(
        &self,
        name: &str,
        params: Option<&[&str]>,
        if_exists: bool,
        cascade: bool,
    ) -> String {
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let quoted_name = self.quote_identifier(name);

        match self.dialect {
            FunctionDialect::PostgreSQL => {
                let params_clause = params
                    .map(|p| format!("({})", p.join(", ")))
                    .unwrap_or_else(|| "()".to_string());

                let cascade_clause = if cascade { " CASCADE" } else { "" };

                format!(
                    "DROP FUNCTION {}{}{}{}",
                    if_exists_clause, quoted_name, params_clause, cascade_clause
                )
            }
            FunctionDialect::MySQL => {
                format!("DROP FUNCTION {}{}", if_exists_clause, quoted_name)
            }
            FunctionDialect::SQLite => {
                format!("DROP FUNCTION {}{}", if_exists_clause, quoted_name)
            }
            FunctionDialect::MsSql => {
                if if_exists {
                    format!(
                        "IF OBJECT_ID('{}', 'FN') IS NOT NULL DROP FUNCTION {}",
                        name, quoted_name
                    )
                } else {
                    format!("DROP FUNCTION {}", quoted_name)
                }
            }
        }
    }

    /// Build a COMMENT ON FUNCTION statement (PostgreSQL only)
    pub fn build_comment(
        &self,
        name: &str,
        params: Option<&[&str]>,
        comment: Option<&str>,
    ) -> Option<String> {
        if !matches!(self.dialect, FunctionDialect::PostgreSQL) {
            return None;
        }

        let quoted_name = self.quote_identifier(name);
        let params_clause = params
            .map(|p| format!("({})", p.join(", ")))
            .unwrap_or_else(|| "()".to_string());

        let comment_value = match comment {
            Some(c) => format!("'{}'", c.replace('\'', "''")),
            None => "NULL".to_string(),
        };

        Some(format!(
            "COMMENT ON FUNCTION {}{} IS {}",
            quoted_name, params_clause, comment_value
        ))
    }

    /// Build an ALTER FUNCTION statement for changing owner (PostgreSQL only)
    pub fn build_alter_owner(
        &self,
        name: &str,
        params: Option<&[&str]>,
        owner: &str,
    ) -> Option<String> {
        if !matches!(self.dialect, FunctionDialect::PostgreSQL) {
            return None;
        }

        let quoted_name = self.quote_identifier(name);
        let params_clause = params
            .map(|p| format!("({})", p.join(", ")))
            .unwrap_or_else(|| "()".to_string());

        Some(format!(
            "ALTER FUNCTION {}{} OWNER TO {}",
            quoted_name, params_clause, owner
        ))
    }

    /// Quote an identifier based on the dialect
    fn quote_identifier(&self, name: &str) -> String {
        if name.contains('.') {
            name.split('.')
                .map(|part| self.quote_single_identifier(part))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            self.quote_single_identifier(name)
        }
    }

    fn quote_single_identifier(&self, name: &str) -> String {
        match self.dialect {
            FunctionDialect::PostgreSQL | FunctionDialect::SQLite => {
                if Self::needs_quoting(name) {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    name.to_string()
                }
            }
            FunctionDialect::MySQL => {
                if Self::needs_quoting(name) {
                    format!("`{}`", name.replace('`', "``"))
                } else {
                    name.to_string()
                }
            }
            FunctionDialect::MsSql => {
                if Self::needs_quoting(name) {
                    format!("[{}]", name.replace(']', "]]"))
                } else {
                    name.to_string()
                }
            }
        }
    }

    fn needs_quoting(name: &str) -> bool {
        let Some(first) = name.chars().next() else {
            return true;
        };
        if !first.is_ascii_alphabetic() && first != '_' {
            return true;
        }
        name.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
            || RESERVED_KEYWORDS.contains(&name.to_uppercase().as_str())
    }
}

static RESERVED_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "TABLE",
    "VIEW",
    "INDEX",
    "TRIGGER",
    "FUNCTION",
    "PROCEDURE",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "AS",
    "ON",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "FULL",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "ALL",
    "DISTINCT",
    "INTO",
    "VALUES",
    "SET",
    "DEFAULT",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "CONSTRAINT",
    "UNIQUE",
    "CHECK",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IF",
    "EXISTS",
    "IN",
    "BETWEEN",
    "LIKE",
    "IS",
    "BEGIN",
    "RETURN",
    "RETURNS",
    "LANGUAGE",
    "IMMUTABLE",
    "STABLE",
    "VOLATILE",
];
