//! SQLite connection implementation

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::{Connection as RusqliteConnection, InterruptHandle, OpenFlags, params_from_iter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, ColumnMeta, Connection,
    ConnectionScope, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig,
    ExplainParserKind, ForeignKeyChecksSql, ImportIndexCapabilities, QueryCancelHandle, QueryResult,
    ResolvedConnectionScope, Result, Row, RowIdentifier, SchemaIntrospection, SingleRowDmlScope,
    SqlObjectName, StatementResult, TableType, Transaction, Value, ZqlzError,
};
use zqlz_schema_engine::{DefaultDialect, SchemaEngine};

use crate::schema::SqliteCatalog;

fn strip_pg_casts(expr: &str) -> String {
    let mut result = expr.to_owned();
    loop {
        let Some(cast_start) = result.rfind("::") else {
            break;
        };
        let after = &result[cast_start + 2..];
        let ident_len = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ' ')
            .map(char::len_utf8)
            .sum::<usize>();
        if ident_len == 0 {
            break;
        }
        let mut end = cast_start + 2 + ident_len;
        if result[end..].starts_with("[]") {
            end += 2;
        }
        result.replace_range(cast_start..end, "");
    }
    result
}

/// Cancel handle for SQLite queries.
///
/// This wraps the rusqlite `InterruptHandle` and can be called from any thread
/// to interrupt a running query. The interrupted query will return SQLITE_INTERRUPT.
pub struct SqliteCancelHandle {
    interrupt_handle: Arc<InterruptHandle>,
}

impl QueryCancelHandle for SqliteCancelHandle {
    fn cancel(&self) {
        tracing::debug!("Interrupting SQLite query");
        self.interrupt_handle.interrupt();
    }
}

/// SQLite connection wrapper
pub struct SqliteConnection {
    conn: Arc<Mutex<RusqliteConnection>>,
    interrupt_handle: Arc<InterruptHandle>,
    schema_engine: SchemaEngine,
    closed: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteOpenMode {
    Create,
    ReadWrite,
    ReadOnly,
}

impl SqliteOpenMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "read_write_create" => Ok(Self::Create),
            "read_write" => Ok(Self::ReadWrite),
            "read_only" => Ok(Self::ReadOnly),
            _ => Err(ZqlzError::Configuration(format!(
                "Invalid SQLite open mode: {}",
                value
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteOpenOptions {
    pub path: String,
    pub open_mode: SqliteOpenMode,
    pub foreign_keys: bool,
    pub journal_mode: String,
    pub synchronous: String,
    pub busy_timeout_ms: u64,
    pub load_extensions: bool,
    pub extension_paths: Vec<PathBuf>,
}

impl SqliteOpenOptions {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            open_mode: SqliteOpenMode::Create,
            foreign_keys: true,
            journal_mode: "WAL".to_string(),
            synchronous: "NORMAL".to_string(),
            busy_timeout_ms: 5000,
            load_extensions: false,
            extension_paths: Vec::new(),
        }
    }
}

impl SqliteConnection {
    fn sqlite_identifier_literal(identifier: &str) -> String {
        identifier.replace('"', "\"\"")
    }

    fn sqlite_string_literal(value: &str) -> String {
        value.replace('\'', "''")
    }

    /// Open a SQLite database
    pub fn open(path: &str) -> Result<Self> {
        Self::open_with_options(SqliteOpenOptions::new(path))
    }

    pub fn open_with_options(options: SqliteOpenOptions) -> Result<Self> {
        tracing::info!(path = %options.path, open_mode = ?options.open_mode, "opening SQLite database");
        // Expand path to handle ~ and relative paths
        let expanded_path = Self::expand_path(&options.path)?;

        let flags = match options.open_mode {
            SqliteOpenMode::Create => {
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
            }
            SqliteOpenMode::ReadWrite => OpenFlags::SQLITE_OPEN_READ_WRITE,
            SqliteOpenMode::ReadOnly => OpenFlags::SQLITE_OPEN_READ_ONLY,
        } | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let conn = if options.path == ":memory:" {
            RusqliteConnection::open_in_memory().map_err(|e| {
                ZqlzError::Connection(format!("Failed to open in-memory database: {}", e))
            })?
        } else {
            // Validate that parent directory exists for non-URI paths
            if options.open_mode == SqliteOpenMode::Create && !expanded_path.starts_with("file:") {
                let file_path = std::path::Path::new(&expanded_path);
                if let Some(parent) = file_path.parent()
                    && !parent.exists()
                {
                    return Err(ZqlzError::Connection(format!(
                        "Parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }

            RusqliteConnection::open_with_flags(&expanded_path, flags).map_err(|e| {
                ZqlzError::Connection(format!(
                    "Failed to open SQLite database at '{}': {}",
                    expanded_path, e
                ))
            })?
        };

        conn.pragma_update(
            None,
            "foreign_keys",
            if options.foreign_keys { "ON" } else { "OFF" },
        )
        .map_err(|e| ZqlzError::Connection(format!("Failed to set foreign keys: {}", e)))?;

        conn.pragma_update(None, "journal_mode", options.journal_mode.as_str())
            .map_err(|e| ZqlzError::Connection(format!("Failed to set journal mode: {}", e)))?;

        conn.pragma_update(None, "synchronous", options.synchronous.as_str())
            .map_err(|e| ZqlzError::Connection(format!("Failed to set synchronous mode: {}", e)))?;

        conn.busy_timeout(Duration::from_millis(options.busy_timeout_ms))
            .map_err(|e| ZqlzError::Connection(format!("Failed to set busy timeout: {}", e)))?;

        if options.load_extensions {
            unsafe {
                conn.load_extension_enable().map_err(|e| {
                    ZqlzError::Connection(format!("Failed to enable extension loading: {}", e))
                })?;
                for path in &options.extension_paths {
                    conn.load_extension(path, None::<&str>).map_err(|e| {
                        ZqlzError::Connection(format!(
                            "Failed to load SQLite extension '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                }
                conn.load_extension_disable().map_err(|e| {
                    ZqlzError::Connection(format!("Failed to disable extension loading: {}", e))
                })?;
            }
        }

        // Get interrupt handle before wrapping connection in Mutex
        // This handle can be used from any thread to cancel running queries
        let interrupt_handle = Arc::new(conn.get_interrupt_handle());

        tracing::info!(path = %expanded_path, "SQLite database connection established");
        let conn = Arc::new(Mutex::new(conn));
        let schema_engine = SchemaEngine::new(
            Arc::new(SqliteCatalog::new(conn.clone())),
            Arc::new(DefaultDialect),
        );
        Ok(Self {
            conn,
            interrupt_handle,
            schema_engine,
            closed: AtomicBool::new(false),
        })
    }

    /// Expand path to handle ~ (home directory) and relative paths
    fn expand_path(path: &str) -> Result<String> {
        // Handle special cases
        if path == ":memory:" || path.starts_with("file:") {
            return Ok(path.to_string());
        }

        // Expand ~ to home directory
        let expanded = if let Some(rest) = path.strip_prefix("~/") {
            if let Some(home_path) = dirs::home_dir() {
                home_path.join(rest).to_string_lossy().to_string()
            } else {
                return Err(ZqlzError::Configuration(
                    "Unable to determine home directory".into(),
                ));
            }
        } else if path.starts_with('~') {
            return Err(ZqlzError::Configuration(
                "User-specific home directories (~user) are not supported".into(),
            ));
        } else {
            path.to_string()
        };

        // Convert to absolute path if relative
        let path_buf = std::path::PathBuf::from(&expanded);
        let result = if path_buf.is_relative() {
            std::env::current_dir()
                .map_err(ZqlzError::Io)?
                .join(path_buf)
                .to_string_lossy()
                .to_string()
        } else {
            expanded
        };

        Ok(result)
    }

    /// Get database file information
    pub fn get_info(&self) -> Result<DatabaseFileInfo> {
        let conn = self.conn.lock();

        // Get page count and page size
        let page_count: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .map_err(|e| ZqlzError::Query(e.to_string()))?;
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(|e| ZqlzError::Query(e.to_string()))?;
        let file_size = page_count * page_size;

        // Get encoding
        let encoding: String = conn
            .query_row("PRAGMA encoding", [], |row| row.get(0))
            .map_err(|e| ZqlzError::Query(e.to_string()))?;

        // Get journal mode
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|e| ZqlzError::Query(e.to_string()))?;

        // Get foreign keys status
        let foreign_keys: bool = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
            .map_err(|e| ZqlzError::Query(e.to_string()))?
            != 0;

        Ok(DatabaseFileInfo {
            file_size_bytes: file_size,
            page_count: page_count as usize,
            page_size: page_size as usize,
            encoding,
            journal_mode,
            foreign_keys_enabled: foreign_keys,
        })
    }

    /// Execute multiple SQL statements in a batch
    /// This is useful for executing schema changes or running SQL scripts
    pub async fn execute_batch(&self, sql: &str) -> Result<Vec<StatementResult>> {
        tracing::debug!("executing SQL batch");
        let conn = self.conn.lock();

        // Use rusqlite's execute_batch for multiple statements
        conn.execute_batch(sql)
            .map_err(|e| ZqlzError::Query(format!("Failed to execute batch: {}", e)))?;

        // Return a single success result (rusqlite doesn't provide per-statement results for batch)
        Ok(vec![StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0, // Unknown for batch operations
            error: None,
        }])
    }

    /// Execute SQL that may contain multiple statements, automatically detecting if it's a single query or batch
    pub async fn execute_multi(&self, sql: &str, params: &[Value]) -> Result<ExecuteMultiResult> {
        let trimmed = sql.trim();

        // Check if it looks like a single statement
        let statement_count = trimmed.matches(';').count();
        let has_params = !params.is_empty();

        // If it's a single statement or has parameters, use regular execute/query
        if statement_count == 0 || (statement_count == 1 && trimmed.ends_with(';')) || has_params {
            // Try to detect if it's a SELECT query
            let is_select = trimmed.to_uppercase().starts_with("SELECT")
                || trimmed.to_uppercase().starts_with("WITH")
                || trimmed.to_uppercase().starts_with("EXPLAIN");

            if is_select {
                let result = self.query(trimmed, params).await?;
                Ok(ExecuteMultiResult::Query(result))
            } else {
                let result = self.execute(trimmed, params).await?;
                Ok(ExecuteMultiResult::Statement(vec![result]))
            }
        } else {
            // Multiple statements without parameters - use batch execution
            if !params.is_empty() {
                return Err(ZqlzError::Query(
                    "Parameters are not supported with multiple statements".into(),
                ));
            }

            let results = self.execute_batch(sql).await?;
            Ok(ExecuteMultiResult::Statement(results))
        }
    }
}

/// Result of executing multiple statements
#[derive(Debug)]
pub enum ExecuteMultiResult {
    /// A single query result
    Query(QueryResult),
    /// One or more statement results
    Statement(Vec<StatementResult>),
}

/// Information about the SQLite database file
#[derive(Debug, Clone)]
pub struct DatabaseFileInfo {
    pub file_size_bytes: i64,
    pub page_count: usize,
    pub page_size: usize,
    pub encoding: String,
    pub journal_mode: String,
    pub foreign_keys_enabled: bool,
}

#[async_trait]
impl Connection for SqliteConnection {
    fn driver_name(&self) -> &str {
        "sqlite"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("sqlite")
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = Some("main".to_string());
                resolved.effective_namespace = Some("main".to_string());
                resolved.introspection_scope = Some("main".to_string());
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let namespace = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                resolved.effective_database = Some(namespace.clone());
                resolved.effective_namespace = Some(namespace.clone());
                resolved.introspection_scope = Some(namespace);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::sqlite()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::Sqlite
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        format!("\"{}\"", Self::sqlite_identifier_literal(identifier))
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    fn max_bind_parameters(&self) -> usize {
        32_766
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("CAST({} AS TEXT)", expression_sql)
    }

    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        strip_pg_casts(expression_sql)
    }

    fn supports_partial_indexes(&self) -> bool {
        true
    }

    fn supports_include_indexes(&self) -> bool {
        false
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        true
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: true,
            supports_include: false,
            supports_nulls_ordering: true,
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        if table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support schema-qualified RENAME TABLE".to_string(),
            ));
        }

        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.quote_identifier(&table_name.name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String> {
        if table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support schema-qualified DROP TABLE".to_string(),
            ));
        }

        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support DROP TABLE ... CASCADE".to_string(),
            ));
        }

        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&table_name.name));
        Ok(sql)
    }

    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String> {
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support DROP VIEW ... CASCADE".to_string(),
            ));
        }

        if view_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support schema-qualified DROP VIEW".to_string(),
            ));
        }

        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&view_name.name));
        Ok(sql)
    }

    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        if trigger_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support schema-qualified DROP TRIGGER".to_string(),
            ));
        }

        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support DROP TRIGGER ... CASCADE".to_string(),
            ));
        }

        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&trigger_name.name));
        Ok(sql)
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String> {
        if source_table_name.namespace.is_some() || new_table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "SQLite does not support schema-qualified table duplication".to_string(),
            ));
        }

        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.quote_identifier(&new_table_name.name),
            self.quote_identifier(&source_table_name.name)
        ))
    }

    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );

        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }

        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );

        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }

        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    fn reset_table_identity_sql(&self, table_name: &SqlObjectName) -> Option<String> {
        Some(format!(
            "DELETE FROM sqlite_sequence WHERE name = '{}'",
            table_name.name.replace('\'', "''")
        ))
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        let table_name = sequence_name
            .split_once('.')
            .map(|(table, _)| table)
            .unwrap_or(sequence_name);
        Some(format!(
            "INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('{}', {})",
            table_name.replace('\'', "''"),
            current_value
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT seq FROM sqlite_sequence WHERE name = '{}'",
            table_name.replace('\'', "''")
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_i64())
            .filter(|value| *value > 0))
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        Ok(Some("main".to_string()))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some("main".to_string()))
    }

    fn has_session_namespace(&self) -> bool {
        false
    }

    fn supports_fast_exact_count(&self) -> bool {
        true
    }

    fn should_use_schema_only_table_browse_fallback(
        &self,
        table_type: TableType,
        error_message: &str,
    ) -> bool {
        table_type == TableType::VirtualTable
            && error_message
                .to_ascii_lowercase()
                .contains("vtable constructor failed")
    }

    fn should_use_ddl_column_fallback(&self, table_type: TableType, _error_message: &str) -> bool {
        table_type == TableType::VirtualTable
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Unknown
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        Some(ForeignKeyChecksSql {
            disable_sql: "PRAGMA foreign_keys = OFF".to_string(),
            enable_sql: "PRAGMA foreign_keys = ON".to_string(),
        })
    }

    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        let (where_clause, mut params) = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported by SQLite. Use primary key or full row identifier."
                        .to_string(),
                ));
            }
            RowIdentifier::PrimaryKey(primary_key_values) => {
                let conditions = primary_key_values
                    .iter()
                    .enumerate()
                    .map(|(index, (column_name, _))| {
                        format!(
                            "{} = {}",
                            self.quote_identifier(column_name),
                            self.format_bind_placeholder(index + 1)
                        )
                    })
                    .collect::<Vec<_>>();
                let params = primary_key_values
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                (conditions.join(" AND "), params)
            }
            RowIdentifier::FullRow(row_values) => {
                let mut next_param_index = 1usize;
                let conditions = row_values
                    .iter()
                    .map(|(column_name, value)| {
                        if value == &Value::Null {
                            format!("{} IS NULL", self.quote_identifier(column_name))
                        } else {
                            let placeholder = self.format_bind_placeholder(next_param_index);
                            next_param_index += 1;
                            format!("{} = {}", self.quote_identifier(column_name), placeholder)
                        }
                    })
                    .collect::<Vec<_>>();
                let params = row_values
                    .iter()
                    .filter(|(_, value)| value != &Value::Null)
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                (conditions.join(" AND "), params)
            }
        };

        let table_name = self.quote_identifier(&request.table_name);
        let column_name = self.quote_identifier(&request.column_name);
        let scope = match &request.row_identifier {
            RowIdentifier::FullRow(_) => self.single_row_dml_scope(&table_name, &where_clause),
            _ => SingleRowDmlScope::Unsupported,
        };
        let (where_clause, statement_suffix) = scope.apply(&where_clause);
        let sql = if let Some(new_value) = &request.new_value {
            params.insert(0, new_value.clone());
            format!(
                "UPDATE {} SET {} = {} WHERE {}{}",
                table_name,
                column_name,
                self.format_bind_placeholder(0),
                where_clause,
                statement_suffix
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {}{}",
                table_name, column_name, where_clause, statement_suffix
            )
        };

        let result = self.execute(&sql, &params).await?;
        Ok(result.affected_rows)
    }

    /// SQLite tables without a key still have a `rowid`, and the only tables
    /// that lack one (`WITHOUT ROWID`) are required to declare a primary key,
    /// so they never reach a keyless statement.
    fn single_row_dml_scope(
        &self,
        qualified_table: &str,
        where_clause: &str,
    ) -> SingleRowDmlScope {
        SingleRowDmlScope::by_row_identity("rowid", qualified_table, where_clause)
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        if table_name
            .namespace
            .as_deref()
            .is_some_and(|namespace| namespace != "main")
        {
            return Ok(None);
        }

        let sql = format!(
            "SELECT COALESCE(CAST(stat AS INTEGER), 0) FROM sqlite_stat1 WHERE tbl = '{}' LIMIT 1",
            Self::sqlite_string_literal(&table_name.name)
        );
        let result = self.query(&sql, &[]).await?;

        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let conn = self.conn.lock();
        let rusqlite_params = values_to_rusqlite(params);

        let rows_affected = conn
            .execute(sql, params_from_iter(rusqlite_params.iter()))
            .map_err(|e| ZqlzError::Query(format!("Failed to execute statement: {}", e)))?;

        tracing::debug!(affected_rows = rows_affected, "statement executed");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected as u64,
            error: None,
        })
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        run_sqlite_query(&self.conn, sql, params)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        tracing::debug!("beginning SQLite transaction");
        {
            let conn = self.conn.lock();
            // DEFERRED means the write lock is only acquired when the first write occurs,
            // which matches the typical behaviour expected from a default transaction.
            conn.execute_batch("BEGIN DEFERRED")
                .map_err(|e| ZqlzError::Query(format!("Failed to begin transaction: {}", e)))?;
        }
        tracing::debug!("SQLite transaction started");
        Ok(Box::new(SqliteTransaction {
            conn: Arc::clone(&self.conn),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing SQLite connection");
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn requires_heartbeat(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(&self.schema_engine)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(SqliteCancelHandle {
            interrupt_handle: self.interrupt_handle.clone(),
        }))
    }
}

/// SQLite transaction wrapper.
///
/// Issues raw `BEGIN DEFERRED` / `COMMIT` / `ROLLBACK` SQL so that it can share
/// the connection `Arc<Mutex<…>>` without running into rusqlite's borrow-based
/// transaction lifetime requirements.
pub struct SqliteTransaction {
    conn: Arc<Mutex<RusqliteConnection>>,
    committed: bool,
    rolled_back: bool,
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        // If the transaction is abandoned without an explicit commit/rollback, issue a
        // best-effort rollback so the connection is left in a clean state.
        if !self.committed && !self.rolled_back {
            tracing::warn!(
                "SQLite transaction dropped without commit or rollback, issuing automatic rollback"
            );
            let conn = self.conn.lock();
            if let Err(e) = conn.execute_batch("ROLLBACK") {
                tracing::error!(error = %e, "automatic rollback on drop failed");
            }
        }
    }
}

#[async_trait]
impl Transaction for SqliteTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("committing SQLite transaction");

        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }

        let conn = self.conn.lock();
        conn.execute_batch("COMMIT")
            .map_err(|e| ZqlzError::Query(format!("Failed to commit transaction: {}", e)))?;

        self.committed = true;
        tracing::debug!("SQLite transaction committed successfully");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("rolling back SQLite transaction");

        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Ok(());
        }

        let conn = self.conn.lock();
        conn.execute_batch("ROLLBACK")
            .map_err(|e| ZqlzError::Query(format!("Failed to rollback transaction: {}", e)))?;

        self.rolled_back = true;
        tracing::debug!("SQLite transaction rolled back successfully");
        Ok(())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in SQLite transaction");

        let start_time = std::time::Instant::now();
        let conn = self.conn.lock();
        let rusqlite_params = values_to_rusqlite(params);

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| ZqlzError::Query(format!("Failed to prepare query: {}", e)))?;

        let column_count = stmt.column_count();
        let mut column_names: Vec<String> = Vec::with_capacity(column_count);
        let mut columns: Vec<ColumnMeta> = Vec::with_capacity(column_count);

        let stmt_columns = stmt.columns();
        for (idx, col) in stmt_columns.iter().enumerate() {
            let name = col.name().to_string();
            let data_type = col.decl_type().unwrap_or("DYNAMIC").to_string();
            column_names.push(name.clone());
            columns.push(ColumnMeta {
                name,
                data_type,
                nullable: true,
                ordinal: idx,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            });
        }

        let mut rows = Vec::new();
        let mut query_rows = stmt
            .query(params_from_iter(rusqlite_params.iter()))
            .map_err(|e| ZqlzError::Query(format!("Failed to execute query: {}", e)))?;

        while let Some(row) = query_rows
            .next()
            .map_err(|e| ZqlzError::Query(format!("Failed to fetch row: {}", e)))?
        {
            let mut values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let value = rusqlite_to_value(row, i)?;
                values.push(value);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();
        Ok(QueryResult {
            id: uuid::Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(total_rows as u64),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in SQLite transaction");

        let conn = self.conn.lock();
        let rusqlite_params = values_to_rusqlite(params);

        let rows_affected = conn
            .execute(sql, params_from_iter(rusqlite_params.iter()))
            .map_err(|e| ZqlzError::Query(format!("Failed to execute statement: {}", e)))?;

        tracing::debug!(
            affected_rows = rows_affected,
            "statement executed in SQLite transaction"
        );
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected as u64,
            error: None,
        })
    }
}

/// Execute a read query against a shared SQLite connection and collect the
/// result. Shared by [`SqliteConnection`] and the schema-introspection adapter
/// so the rusqlite glue lives in one place.
pub(crate) fn run_sqlite_query(
    conn: &Arc<Mutex<RusqliteConnection>>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    let start_time = std::time::Instant::now();

    let conn = conn.lock();
    let rusqlite_params = values_to_rusqlite(params);

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| ZqlzError::Query(format!("Failed to prepare query: {}", e)))?;

    let column_count = stmt.column_count();
    let mut column_names: Vec<String> = Vec::with_capacity(column_count);
    let mut columns: Vec<ColumnMeta> = Vec::with_capacity(column_count);

    let stmt_columns = stmt.columns();
    for (idx, col) in stmt_columns.iter().enumerate() {
        let name = col.name().to_string();
        let data_type = col.decl_type().unwrap_or("DYNAMIC").to_string();

        column_names.push(name.clone());
        columns.push(ColumnMeta {
            name,
            data_type,
            nullable: true,
            ordinal: idx,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        });
    }

    let mut rows = Vec::new();
    let mut query_rows = stmt
        .query(params_from_iter(rusqlite_params.iter()))
        .map_err(|e| ZqlzError::Query(format!("Failed to execute query: {}", e)))?;

    while let Some(row) = query_rows
        .next()
        .map_err(|e| ZqlzError::Query(format!("Failed to fetch row: {}", e)))?
    {
        let mut values = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let value = rusqlite_to_value(row, i)?;
            values.push(value);
        }
        rows.push(Row::new(column_names.clone(), values));
    }

    let execution_time_ms = start_time.elapsed().as_millis() as u64;
    let total_rows = rows.len();

    Ok(QueryResult {
        id: uuid::Uuid::new_v4(),
        columns,
        rows,
        total_rows: Some(total_rows as u64),
        is_estimated_total: false,
        affected_rows: 0,
        execution_time_ms,
        warnings: Vec::new(),
    })
}

pub(crate) fn object_type_to_sqlite(obj_type: &zqlz_core::ObjectType) -> Result<&'static str> {
    match obj_type {
        zqlz_core::ObjectType::Table => Ok("table"),
        zqlz_core::ObjectType::View => Ok("view"),
        zqlz_core::ObjectType::Index => Ok("index"),
        zqlz_core::ObjectType::Trigger => Ok("trigger"),
        unsupported => Err(ZqlzError::NotImplemented(format!(
            "SQLite DDL generation is not supported for {:?}",
            unsupported
        ))),
    }
}

/// Convert our Value types to rusqlite-compatible types
fn values_to_rusqlite(values: &[Value]) -> Vec<rusqlite::types::Value> {
    values.iter().map(value_to_rusqlite).collect()
}

fn value_to_rusqlite(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Value::Int8(i) => rusqlite::types::Value::Integer(*i as i64),
        Value::Int16(i) => rusqlite::types::Value::Integer(*i as i64),
        Value::Int32(i) => rusqlite::types::Value::Integer(*i as i64),
        Value::Int64(i) => rusqlite::types::Value::Integer(*i),
        Value::Float32(f) => rusqlite::types::Value::Real(*f as f64),
        Value::Float64(f) => rusqlite::types::Value::Real(*f),
        Value::Decimal(d) => rusqlite::types::Value::Text(d.clone()),
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        Value::Bytes(b) => rusqlite::types::Value::Blob(b.clone()),
        Value::Date(d) => rusqlite::types::Value::Text(d.to_string()),
        Value::Time(t) => rusqlite::types::Value::Text(t.to_string()),
        Value::DateTime(dt) => rusqlite::types::Value::Text(dt.to_string()),
        Value::DateTimeUtc(dt) => rusqlite::types::Value::Text(dt.to_rfc3339()),
        Value::Json(j) => rusqlite::types::Value::Text(j.to_string()),
        Value::Uuid(u) => rusqlite::types::Value::Text(u.to_string()),
        Value::Array(values) => rusqlite::types::Value::Text(
            serde_json::Value::Array(values.iter().map(Value::to_json_value).collect()).to_string(),
        ),
    }
}

/// Convert rusqlite row value to our Value type
fn rusqlite_to_value(row: &rusqlite::Row, idx: usize) -> Result<Value> {
    use rusqlite::types::ValueRef;

    let value_ref = row
        .get_ref(idx)
        .map_err(|e| ZqlzError::Query(e.to_string()))?;

    let value = match value_ref {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Int64(i),
        ValueRef::Real(f) => Value::Float64(f),
        ValueRef::Text(s) => Value::String(String::from_utf8_lossy(s).to_string()),
        ValueRef::Blob(b) => {
            // SQLite BLOBs might actually contain text data
            // Try to decode as UTF-8 first - if successful, treat as String
            // This handles cases where text is stored in columns without explicit type
            match std::str::from_utf8(b) {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::Bytes(b.to_vec()),
            }
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{SqliteConnection, object_type_to_sqlite, value_to_rusqlite};
    use zqlz_core::{Connection, ObjectType, Value, ZqlzError};

    #[test]
    fn object_type_to_sqlite_maps_supported_types() {
        assert_eq!(object_type_to_sqlite(&ObjectType::Table).unwrap(), "table");
        assert_eq!(object_type_to_sqlite(&ObjectType::View).unwrap(), "view");
        assert_eq!(object_type_to_sqlite(&ObjectType::Index).unwrap(), "index");
        assert_eq!(
            object_type_to_sqlite(&ObjectType::Trigger).unwrap(),
            "trigger"
        );
    }

    #[test]
    fn object_type_to_sqlite_rejects_unsupported_types() {
        let error = object_type_to_sqlite(&ObjectType::Function).unwrap_err();
        match error {
            ZqlzError::NotImplemented(message) => {
                assert!(message.contains("Function"));
                assert!(message.contains("not supported"));
            }
            other => panic!("Expected NotImplemented, got {:?}", other),
        }
    }

    #[test]
    fn sqlite_array_values_serialize_as_json_text() {
        let value = value_to_rusqlite(&Value::Array(vec![
            Value::String("one".to_string()),
            Value::Int32(2),
            Value::Null,
        ]));

        assert_eq!(
            value,
            rusqlite::types::Value::Text("[\"one\",2,null]".to_string())
        );
    }

    #[tokio::test]
    async fn sqlite_objects_panel_loads_table_counts_without_per_table_fanout() {
        let connection = SqliteConnection::open(":memory:").expect("open sqlite memory db");
        connection
            .execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
                &[],
            )
            .await
            .expect("create users table");
        connection
            .execute("CREATE INDEX users_email_idx ON users(email)", &[])
            .await
            .expect("create users index");
        connection
            .execute(
                "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN SELECT 1; END",
                &[],
            )
            .await
            .expect("create users trigger");
        connection
            .execute(
                "INSERT INTO users (email) VALUES ('a@example.com'), ('b@example.com')",
                &[],
            )
            .await
            .expect("insert users");

        let data = connection
            .as_schema_introspection()
            .expect("sqlite exposes schema introspection")
            .list_tables_extended(None)
            .await
            .expect("list sqlite objects panel data");
        let users = data
            .rows
            .iter()
            .find(|row| row.name == "users")
            .expect("users table row exists");

        assert!(users.values.contains_key("row_count"));
        assert_eq!(
            users.values.get("index_count").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            users.values.get("trigger_count").map(String::as_str),
            Some("1")
        );
    }
}
