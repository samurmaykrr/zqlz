//! SQLite connection implementation

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::{Connection as RusqliteConnection, InterruptHandle, OpenFlags, params_from_iter};
use std::sync::Arc;
use zqlz_core::{
    ColumnInfo, ColumnMeta, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject, Dependency,
    ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, QueryCancelHandle,
    QueryResult, Result, Row, SchemaInfo, SchemaIntrospection, SequenceInfo, StatementResult,
    TableDetails, TableInfo, TableType, Transaction, TriggerInfo, TypeInfo, Value, ViewInfo,
    ZqlzError,
};

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
}

impl SqliteConnection {
    /// Open a SQLite database
    pub fn open(path: &str) -> Result<Self> {
        tracing::info!(path = %path, "opening SQLite database");
        // Expand path to handle ~ and relative paths
        let expanded_path = Self::expand_path(path)?;

        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        let conn = if path == ":memory:" {
            RusqliteConnection::open_in_memory().map_err(|e| {
                ZqlzError::Connection(format!("Failed to open in-memory database: {}", e))
            })?
        } else {
            // Validate that parent directory exists for non-URI paths
            if !expanded_path.starts_with("file:") {
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

        // Enable foreign keys (PRAGMA commands return results, so use pragma_update)
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| ZqlzError::Connection(format!("Failed to enable foreign keys: {}", e)))?;

        // Set other useful pragmas for better performance and safety
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| ZqlzError::Connection(format!("Failed to set journal mode: {}", e)))?;

        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| ZqlzError::Connection(format!("Failed to set synchronous mode: {}", e)))?;

        // Get interrupt handle before wrapping connection in Mutex
        // This handle can be used from any thread to cancel running queries
        let interrupt_handle = Arc::new(conn.get_interrupt_handle());

        tracing::info!(path = %expanded_path, "SQLite database connection established");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            interrupt_handle,
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
            if let Some(home) = std::env::var_os("HOME") {
                let home_path = std::path::PathBuf::from(home);
                home_path.join(rest).to_string_lossy().to_string()
            } else {
                return Err(ZqlzError::Configuration(
                    "Unable to determine HOME directory".into(),
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

    /// Get the row count for a specific table
    /// Returns an error if the table doesn't exist or query fails
    async fn get_table_row_count(&self, table_name: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
        let result = self.query(&sql, &[]).await?;

        if let Some(row) = result.rows.first()
            && let Some(value) = row.get(0)
        {
            return value
                .as_i64()
                .ok_or_else(|| ZqlzError::Query("Row count is not an integer".into()));
        }

        Err(ZqlzError::Query("Failed to get row count".into()))
    }

    /// Get the number of indexes for a specific table
    async fn get_table_index_count(&self, table_name: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM pragma_index_list('{}')", table_name);
        let result = self.query(&sql, &[]).await?;

        if let Some(row) = result.rows.first()
            && let Some(value) = row.get(0)
        {
            return value
                .as_i64()
                .ok_or_else(|| ZqlzError::Query("Index count is not an integer".into()));
        }

        Ok(0)
    }

    /// Get the number of triggers for a specific table
    async fn get_table_trigger_count(&self, table_name: &str) -> Result<i64> {
        let sql = format!(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{}'",
            table_name
        );
        let result = self.query(&sql, &[]).await?;

        if let Some(row) = result.rows.first()
            && let Some(value) = row.get(0)
        {
            return value
                .as_i64()
                .ok_or_else(|| ZqlzError::Query("Trigger count is not an integer".into()));
        }

        Ok(0)
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
        let start_time = std::time::Instant::now();

        let conn = self.conn.lock();
        let rusqlite_params = values_to_rusqlite(params);

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| ZqlzError::Query(format!("Failed to prepare query: {}", e)))?;

        // Get column count and names before executing
        let column_count = stmt.column_count();
        let mut column_names: Vec<String> = Vec::with_capacity(column_count);
        let mut columns: Vec<ColumnMeta> = Vec::with_capacity(column_count);

        // Use stmt.columns() to get column info including declared types
        let stmt_columns = stmt.columns();
        for (idx, col) in stmt_columns.iter().enumerate() {
            let name = col.name().to_string();
            // Get the declared column type from the schema if available
            // This uses sqlite3_column_decltype which returns the type from CREATE TABLE
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

        // Execute query and collect rows
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

        tracing::debug!(
            row_count = total_rows,
            execution_time_ms = execution_time_ms,
            "query executed successfully"
        );
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
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(SqliteCancelHandle {
            interrupt_handle: self.interrupt_handle.clone(),
        }))
    }
}

#[async_trait]
impl SchemaIntrospection for SqliteConnection {
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        Ok(vec![DatabaseInfo {
            name: "main".to_string(),
            owner: None,
            encoding: Some("UTF-8".to_string()),
            size_bytes: None,
            comment: None,
        }])
    }

    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "main".to_string(),
            owner: None,
            comment: None,
        }])
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        tracing::debug!("listing tables from sqlite_master");
        let result = self
            .query(
                "SELECT name, type FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;

        let mut tables = Vec::new();

        for row in &result.rows {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Fetch row count for this table
            let row_count = self.get_table_row_count(&name).await.ok();

            // Fetch index count for this table
            let index_count = self.get_table_index_count(&name).await.ok();

            // Fetch trigger count for this table
            let trigger_count = self.get_table_trigger_count(&name).await.ok();

            tables.push(TableInfo {
                name,
                schema: Some("main".to_string()),
                table_type: TableType::Table,
                owner: None,
                row_count,
                size_bytes: None,
                comment: None,
                index_count,
                trigger_count,
                key_value_info: None,
            });
        }

        tracing::debug!(table_count = tables.len(), "tables listed");
        Ok(tables)
    }

    #[tracing::instrument(skip(self))]
    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let result = self
            .query(
                "SELECT name, sql FROM sqlite_master WHERE type = 'view' ORDER BY name",
                &[],
            )
            .await?;

        let views = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                ViewInfo {
                    name,
                    schema: Some("main".to_string()),
                    is_materialized: false,
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
    async fn get_table(&self, _schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let tables = self.list_tables(None).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        let columns = self.get_columns(None, name).await?;
        let indexes = self.get_indexes(None, name).await?;
        let foreign_keys = self.get_foreign_keys(None, name).await?;
        let primary_key = self.get_primary_key(None, name).await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints: Vec::new(),
            triggers: Vec::new(),
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        tracing::trace!(table = %table, "fetching column information");
        let result = self
            .query(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;

        let columns = result
            .rows
            .iter()
            .map(|row| {
                let ordinal = row.get(0).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("TEXT")
                    .to_string();
                let nullable = row.get(3).and_then(|v| v.as_i64()).unwrap_or(0) == 0;
                let default_value = row.get(4).and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                });
                let is_primary_key = row.get(5).and_then(|v| v.as_i64()).unwrap_or(0) > 0;

                ColumnInfo {
                    name,
                    ordinal,
                    data_type: data_type.clone(),
                    nullable,
                    default_value,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key,
                    is_auto_increment: is_primary_key && data_type.to_uppercase() == "INTEGER",
                    is_unique: false,
                    foreign_key: None,
                    comment: None,
                    ..Default::default()
                }
            })
            .collect();

        Ok(columns)
    }

    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, _schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        tracing::trace!(table = %table, "fetching index information");
        let result = self
            .query(&format!("PRAGMA index_list('{}')", table), &[])
            .await?;

        let mut indexes = Vec::new();
        for row in &result.rows {
            let name = match row.get(1).and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            let is_unique = row.get(2).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

            // Get columns for this index
            let cols_result = self
                .query(&format!("PRAGMA index_info('{}')", name), &[])
                .await?;

            let columns: Vec<String> = cols_result
                .rows
                .iter()
                .filter_map(|r| r.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();

            indexes.push(IndexInfo {
                name,
                columns,
                is_unique,
                is_primary: false,
                index_type: "btree".to_string(),
                comment: None,
                ..Default::default()
            });
        }

        Ok(indexes)
    }

    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        tracing::trace!(table = %table, "fetching foreign key information");
        let result = self
            .query(&format!("PRAGMA foreign_key_list('{}')", table), &[])
            .await?;

        let fks = result
            .rows
            .iter()
            .map(|row| {
                let ref_table = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let from_col = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let to_col = row
                    .get(4)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let on_update_str = row.get(5).and_then(|v| v.as_str()).unwrap_or("NO ACTION");
                let on_delete_str = row.get(6).and_then(|v| v.as_str()).unwrap_or("NO ACTION");

                ForeignKeyInfo {
                    name: format!("fk_{}_{}", table, ref_table),
                    columns: vec![from_col],
                    referenced_table: ref_table,
                    referenced_schema: Some("main".to_string()),
                    referenced_columns: vec![to_col],
                    on_update: parse_fk_action(on_update_str),
                    on_delete: parse_fk_action(on_delete_str),
                    is_deferrable: false,
                    initially_deferred: false,
                }
            })
            .collect();

        Ok(fks)
    }

    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let columns = self.get_columns(None, table).await?;
        let pk_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();

        if pk_columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PrimaryKeyInfo {
                name: None,
                columns: pk_columns,
            }))
        }
    }

    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        Ok(Vec::new())
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = if let Some(tbl) = table {
            format!(
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{}' ORDER BY name",
                tbl
            )
        } else {
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
                .to_string()
        };

        let result = self.query(&sql, &[]).await?;

        let triggers = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                TriggerInfo {
                    name,
                    schema: Some("main".to_string()),
                    table_name,
                    timing: zqlz_core::TriggerTiming::After,
                    events: vec![zqlz_core::TriggerEvent::Insert],
                    for_each: zqlz_core::TriggerForEach::Row,
                    definition,
                    enabled: true,
                    comment: None,
                }
            })
            .collect();

        Ok(triggers)
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        Ok(Vec::new())
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let result = self
            .query(
                "SELECT sql FROM sqlite_master WHERE name = ? AND type = ?",
                &[
                    Value::String(object.name.clone()),
                    Value::String(object_type_to_sqlite(&object.object_type)),
                ],
            )
            .await?;

        result
            .rows
            .first()
            .and_then(|row| row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .ok_or_else(|| ZqlzError::NotFound(format!("DDL not found for '{}'", object.name)))
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables_extended(&self, _schema: Option<&str>) -> Result<ObjectsPanelData> {
        let result = self
            .query(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;

        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(400.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("index_count", "Indexes")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("trigger_count", "Triggers")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
        ];

        let mut rows = Vec::new();
        for row in &result.rows {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let obj_type = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("table")
                .to_string();

            let row_count = self
                .get_table_row_count(&name)
                .await
                .ok()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string());

            let index_count = self
                .get_table_index_count(&name)
                .await
                .ok()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string());

            let trigger_count = self
                .get_table_trigger_count(&name)
                .await
                .ok()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string());

            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), name.clone());
            values.insert("row_count".to_string(), row_count);
            values.insert("index_count".to_string(), index_count);
            values.insert("trigger_count".to_string(), trigger_count);

            rows.push(ObjectsPanelRow {
                name,
                object_type: obj_type,
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        Ok(ObjectsPanelData { columns, rows })
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
            tracing::warn!("SQLite transaction dropped without commit or rollback, issuing automatic rollback");
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

        tracing::debug!(affected_rows = rows_affected, "statement executed in SQLite transaction");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected as u64,
            error: None,
        })
    }
}

fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

fn object_type_to_sqlite(obj_type: &zqlz_core::ObjectType) -> String {
    match obj_type {
        zqlz_core::ObjectType::Table => "table",
        zqlz_core::ObjectType::View => "view",
        zqlz_core::ObjectType::Index => "index",
        zqlz_core::ObjectType::Trigger => "trigger",
        _ => "table",
    }
    .to_string()
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
        Value::Array(_) => rusqlite::types::Value::Null,
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
