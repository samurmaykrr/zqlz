//! Connection trait and transaction handling

use crate::{ExplainConfig, QueryResult, Result, SchemaIntrospection, StatementResult, Value};
use async_trait::async_trait;
use std::sync::Arc;

/// Policy for formatting SQL bind placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindPlaceholderPolicy {
    /// Use `?` for every parameter.
    #[default]
    QuestionMark,
    /// Use PostgreSQL style placeholders (`$1`, `$2`, ...).
    DollarNumbered,
    /// Use Oracle/SQLite named index placeholders (`:1`, `:2`, ...).
    ColonNumbered,
}

impl BindPlaceholderPolicy {
    /// Format a placeholder for a 0-based parameter index.
    pub fn format(self, parameter_index: usize) -> String {
        let one_based_index = parameter_index.saturating_add(1);
        match self {
            Self::QuestionMark => "?".to_string(),
            Self::DollarNumbered => format!("${}", one_based_index),
            Self::ColonNumbered => format!(":{}", one_based_index),
        }
    }
}

/// SQL object name with an optional namespace qualifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlObjectName {
    /// Optional namespace/schema/database qualifier.
    pub namespace: Option<String>,
    /// Unqualified object name.
    pub name: String,
}

impl SqlObjectName {
    /// Create an unqualified object name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    /// Create a qualified object name.
    pub fn with_namespace(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }
}

/// Options for DROP VIEW statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropViewOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Options for DROP TABLE statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropTableOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Options for DROP TRIGGER statement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DropTriggerOptions {
    /// Include `IF EXISTS` when supported.
    pub if_exists: bool,
    /// Include `CASCADE` when supported.
    pub cascade: bool,
}

/// Parser strategy for EXPLAIN output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainParserKind {
    /// PostgreSQL EXPLAIN JSON parser.
    PostgreSql,
    /// MySQL/MariaDB EXPLAIN JSON parser.
    MySql,
    /// SQLite EXPLAIN QUERY PLAN text parser.
    Sqlite,
    /// No parser available for this driver.
    None,
}

/// Semantic default values that require driver-specific SQL rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportSemanticDefault {
    CurrentUser,
    GeneratedUuid,
}

/// Support level for CHECK constraint enforcement on the active connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckConstraintEnforcement {
    /// CHECK constraints are expected to be enforced.
    Enforced,
    /// CHECK constraints may be ignored or not fully enforced.
    NotEnforced,
    /// Unknown/depends on server version; caller should warn conservatively.
    Unknown,
}

/// Capability matrix used when mapping source indexes to target SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportIndexCapabilities {
    pub supports_hash: bool,
    pub supports_gin: bool,
    pub supports_gist: bool,
    pub supports_spgist: bool,
    pub supports_brin: bool,
    pub supports_fulltext: bool,
    pub supports_spatial: bool,
    pub supports_partial: bool,
    pub supports_include: bool,
    pub supports_nulls_ordering: bool,
}

impl ImportIndexCapabilities {
    pub const fn standard() -> Self {
        Self {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: false,
            supports_include: false,
            supports_nulls_ordering: false,
        }
    }
}

/// SQL snippets to disable and re-enable FK checks for bulk import workflows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyChecksSql {
    pub disable_sql: String,
    pub enable_sql: String,
}

impl DropViewOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

impl DropTableOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

impl DropTriggerOptions {
    /// Options with `IF EXISTS` enabled.
    pub const fn if_exists() -> Self {
        Self {
            if_exists: true,
            cascade: false,
        }
    }
}

/// Handle for cancelling a running query from any thread.
///
/// This trait allows database drivers to provide a way to interrupt
/// long-running queries. The handle is safe to call from any thread
/// and can be called multiple times (subsequent calls are no-ops).
pub trait QueryCancelHandle: Send + Sync {
    /// Cancel the currently running query on the associated connection.
    ///
    /// This method is safe to call from any thread and is idempotent.
    /// If no query is running, this is a no-op.
    fn cancel(&self);
}

/// Request to update a single cell value
#[derive(Debug, Clone)]
pub struct CellUpdateRequest {
    /// Table name
    pub table_name: String,
    /// Column name to update
    pub column_name: String,
    /// New value (None for NULL)
    pub new_value: Option<Value>,
    /// Row identifier - can be row index, primary key value, or full row data
    pub row_identifier: RowIdentifier,
}

/// Different ways to identify a row for updating
#[derive(Debug, Clone)]
pub enum RowIdentifier {
    /// Use row index/offset (0-based)
    RowIndex(usize),
    /// Use primary key value(s)
    PrimaryKey(Vec<(String, Value)>),
    /// Use all column values to identify the row uniquely
    FullRow(Vec<(String, Value)>),
}

/// A database connection
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get the driver name (e.g., "sqlite", "postgresql", "mysql")
    fn driver_name(&self) -> &str;

    /// Execute a statement that modifies data (INSERT/UPDATE/DELETE)
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;

    /// Execute a query that returns rows (SELECT)
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Get the dialect identifier for this connection (e.g., "sqlite", "postgresql")
    ///
    /// This is used by the query service to look up dialect-specific behavior
    /// like EXPLAIN syntax. Returns None if the dialect is unknown.
    fn dialect_id(&self) -> Option<&'static str> {
        None
    }

    /// Return a lightweight ping SQL statement for this connection.
    fn ping_query_sql(&self) -> &'static str {
        "SELECT 1"
    }

    /// Whether this driver requires one connection per logical database.
    ///
    /// Drivers that can query only within a single selected database (for example
    /// PostgreSQL, SQL Server, Redis logical databases) can override this to true
    /// so connection managers create per-database connection instances.
    fn requires_database_scoped_connection(&self) -> bool {
        false
    }

    /// Normalize a database/scope name used for per-database connection lookup.
    ///
    /// Drivers may override this when UI labels differ from driver-native names.
    fn normalize_database_scope_name(&self, database_name: &str) -> String {
        database_name.to_string()
    }

    /// Return the EXPLAIN configuration for this connection.
    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::default()
    }

    /// Return the parser kind to use for EXPLAIN output.
    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::None
    }

    /// Quote a SQL identifier for this connection's dialect.
    ///
    /// The default uses SQL-standard double quotes and escapes embedded quotes.
    fn quote_identifier(&self, identifier: &str) -> String {
        let escaped_identifier = identifier.replace('"', "\"\"");
        format!("\"{}\"", escaped_identifier)
    }

    /// Render a possibly-qualified SQL object name.
    fn render_qualified_name(&self, object_name: &SqlObjectName) -> String {
        match object_name.namespace.as_deref() {
            Some(namespace) => {
                format!(
                    "{}.{}",
                    self.quote_identifier(namespace),
                    self.quote_identifier(&object_name.name)
                )
            }
            None => self.quote_identifier(&object_name.name),
        }
    }

    /// Placeholder formatting policy used by this connection.
    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    /// Format a bind placeholder for a 0-based parameter index.
    fn format_bind_placeholder(&self, parameter_index: usize) -> String {
        self.bind_placeholder_policy().format(parameter_index)
    }

    /// Maximum number of bind parameters supported in a single statement.
    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    /// Wrap a base `SELECT` statement with pagination syntax for this dialect.
    ///
    /// `base_sql` should be a complete query without trailing limit/offset clauses.
    fn paginated_select_sql(&self, base_sql: &str, limit: u64, offset: u64) -> String {
        format!("{} LIMIT {} OFFSET {}", base_sql, limit, offset)
    }

    /// Generate a SQL expression suitable for text-search comparisons.
    ///
    /// This hook lets drivers cast non-text expressions to a searchable text type.
    /// Default behavior is conservative and returns the input expression unchanged.
    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    /// Normalize imported CHECK expressions for this connection.
    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    /// Keyword used for generated column storage mode.
    fn generated_column_storage_keyword(&self, requested_stored: bool) -> &'static str {
        if requested_stored {
            "STORED"
        } else {
            "VIRTUAL"
        }
    }

    /// Render SQL for semantic defaults that vary by driver.
    fn semantic_default_sql(&self, _kind: ImportSemanticDefault) -> Option<String> {
        None
    }

    /// Whether this connection supports partial indexes (`WHERE` clause).
    fn supports_partial_indexes(&self) -> bool {
        false
    }

    /// Whether this connection supports include/covering columns in indexes.
    fn supports_include_indexes(&self) -> bool {
        false
    }

    /// Whether this connection supports `NULLS FIRST/LAST` in index ordering.
    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        false
    }

    /// Rich index capability matrix for import-time index translation.
    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_partial: self.supports_partial_indexes(),
            supports_include: self.supports_include_indexes(),
            supports_nulls_ordering: self.supports_nulls_ordering_in_indexes(),
            ..ImportIndexCapabilities::standard()
        }
    }

    /// Generate SQL to rename a table.
    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String>;

    /// Generate SQL to drop a table.
    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String>;

    /// Generate SQL to drop a view.
    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String>;

    /// Generate SQL to drop a trigger.
    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String>;

    /// Generate SQL to truncate/clear all rows from a table.
    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL to duplicate one table into another.
    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String>;

    /// Generate SQL to clear all rows from a table without dropping schema.
    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL that checks whether a table contains at least one row.
    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String>;

    /// Generate SQL for selecting rows from a table.
    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String>;

    /// Generate SQL for selecting distinct rows with ordering and a row limit.
    fn select_distinct_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String>;

    /// Generate SQL for inserting a single row.
    fn insert_row_sql(
        &self,
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String>;

    /// Generate SQL to reset table identity/auto-increment state when supported.
    fn reset_table_identity_sql(&self, _table_name: &SqlObjectName) -> Option<String> {
        None
    }

    /// Generate SQL to restore a sequence/identity counter to a value.
    fn restore_sequence_sql(&self, _sequence_name: &str, _current_value: i64) -> Option<String> {
        None
    }

    /// Read the current sequence/identity value for a table column when available.
    async fn export_sequence_current_value(
        &self,
        _table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Export named enum type definitions as `(name, values)` pairs.
    async fn export_named_enum_definitions(&self) -> Result<Vec<(String, Vec<String>)>> {
        Ok(Vec::new())
    }

    /// Whether and how CHECK constraints are enforced on this connection.
    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Enforced
    }

    /// SQL to toggle foreign-key checks for bulk import operations.
    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        None
    }

    /// Whether target uses schema-level enum type creation during import.
    fn supports_import_named_enum_types(&self) -> bool {
        false
    }

    /// Normalize `CREATE VIEW` SQL before execution.
    ///
    /// Drivers can use this to enforce dialect-specific requirements.
    fn normalize_create_view_sql(&self, sql: &str) -> String {
        sql.trim().to_string()
    }

    /// Resolve the current session namespace/schema for this connection.
    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        Ok(None)
    }

    /// Whether this connection supports top-level trigger listing.
    ///
    /// Some databases expose triggers as table-level metadata only.
    fn supports_top_level_triggers(&self) -> bool {
        true
    }

    /// Whether this connection supports materialized views.
    fn supports_materialized_views(&self) -> bool {
        false
    }

    /// Whether this connection has a meaningful session namespace/schema.
    fn has_session_namespace(&self) -> bool {
        true
    }

    /// Whether this connection supports a fast exact row count strategy.
    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    /// Return an estimated row count for an object when available.
    async fn estimated_row_count(&self, _table_name: &SqlObjectName) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Generate the driver-specific SQL used to collect performance metrics.
    fn performance_metrics_query_sql(&self) -> Result<String>;

    /// Update a single cell value
    ///
    /// This is a high-level method that each database driver implements according to
    /// its specific requirements. For example:
    /// - SQLite might use ROWID
    /// - PostgreSQL might use ctid or primary keys
    /// - MySQL might use primary keys
    ///
    /// Returns the number of rows affected.
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value"
        );
        // Default implementation uses the row identifier to build a WHERE clause
        // Drivers can override this for database-specific optimizations
        let (where_clause, mut params) = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                // This is database-specific and may not work for all databases
                return Err(crate::ZqlzError::NotSupported(
                    "Row index-based updates not supported by this driver. Use primary key or full row identifier.".to_string()
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => {
                let conditions: Vec<String> = pk_values
                    .iter()
                    .map(|(col, _)| format!("{} = ?", col))
                    .collect();
                let params: Vec<Value> = pk_values.iter().map(|(_, v)| v.clone()).collect();
                (conditions.join(" AND "), params)
            }
            RowIdentifier::FullRow(row_values) => {
                let conditions: Vec<String> = row_values
                    .iter()
                    .map(|(col, val)| {
                        if val == &Value::Null {
                            format!("{} IS NULL", col)
                        } else {
                            format!("{} = ?", col)
                        }
                    })
                    .collect();
                let params: Vec<Value> = row_values
                    .iter()
                    .filter(|(_, val)| val != &Value::Null)
                    .map(|(_, v)| v.clone())
                    .collect();
                (conditions.join(" AND "), params)
            }
        };

        // Build UPDATE statement
        let sql = if let Some(new_val) = &request.new_value {
            params.insert(0, new_val.clone());
            format!(
                "UPDATE {} SET {} = ? WHERE {}",
                request.table_name, request.column_name, where_clause
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {}",
                request.table_name, request.column_name, where_clause
            )
        };

        let result = self.execute(&sql, &params).await?;
        tracing::debug!(
            affected_rows = result.affected_rows,
            "cell update completed"
        );
        Ok(result.affected_rows)
    }

    /// Begin a transaction
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;

    /// Close the connection
    async fn close(&self) -> Result<()>;

    /// Check if the connection is closed
    fn is_closed(&self) -> bool;

    /// Get schema introspection interface if supported
    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        None
    }

    /// Get a handle that can be used to cancel running queries.
    ///
    /// Returns `None` if the driver does not support query cancellation.
    /// The returned handle is safe to use from any thread.
    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        None
    }
}

/// A database transaction
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// Execute a query within the transaction
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute a statement within the transaction
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;
}

/// A prepared statement
#[async_trait]
pub trait PreparedStatement: Send + Sync {
    /// Execute the prepared statement with parameters
    async fn execute(&self, params: &[Value]) -> Result<StatementResult>;

    /// Query the prepared statement with parameters
    async fn query(&self, params: &[Value]) -> Result<QueryResult>;

    /// Close/deallocate the prepared statement
    async fn close(self: Box<Self>) -> Result<()>;
}
