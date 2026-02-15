//! MySQL connection implementation

use async_trait::async_trait;
use mysql_async::{
    Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row as MySqlRow,
    consts::ColumnType, prelude::*,
};
use std::sync::Arc;
use std::sync::OnceLock;
use zqlz_core::{
    CellUpdateRequest, ColumnMeta, Connection, QueryCancelHandle, QueryResult, Result, Row,
    RowIdentifier, SchemaIntrospection, StatementResult, Transaction, Value, ZqlzError,
};

/// Global Tokio runtime for MySQL operations.
///
/// mysql_async internally calls `tokio::spawn` for connection pooling and networking,
/// which requires a Tokio runtime context. GPUI uses its own async runtime, so we
/// provide a dedicated Tokio runtime for all MySQL operations.
fn get_mysql_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-mysql-runtime")
            .build()
            .expect("Failed to create Tokio runtime for MySQL driver")
    })
}

/// Cancel handle for MySQL queries.
///
/// MySQL doesn't have native query cancellation like PostgreSQL,
/// so we use a flag to signal cancellation and check it periodically.
pub struct MySqlCancelHandle {
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl QueryCancelHandle for MySqlCancelHandle {
    fn cancel(&self) {
        tracing::debug!("Setting MySQL query cancellation flag");
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// MySQL connection wrapper
pub struct MySqlConnection {
    pool: Pool,
    /// Stored at connect time so schema introspection methods can resolve
    /// `schema: None` to a concrete database name instead of relying on
    /// `DATABASE()` which returns NULL when no database was selected.
    database_name: Option<String>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl MySqlConnection {
    /// Connect to a MySQL database
    pub async fn connect(
        host: &str,
        port: u16,
        database: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self> {
        tracing::info!(host = %host, port = %port, database = ?database, "connecting to MySQL database");

        let mut opts_builder = OptsBuilder::from_opts(Opts::default())
            .ip_or_hostname(host)
            .tcp_port(port);

        if let Some(db) = database {
            opts_builder = opts_builder.db_name(Some(db));
        }
        if let Some(u) = user {
            opts_builder = opts_builder.user(Some(u));
        }
        if let Some(p) = password {
            opts_builder = opts_builder.pass(Some(p));
        }

        let constraints = PoolConstraints::new(1, 1).ok_or_else(|| {
            ZqlzError::Connection("Failed to configure MySQL pool constraints (min=1, max=1)".into())
        })?;

        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_reset_connection(false);
        opts_builder = opts_builder.pool_opts(pool_opts);

        let opts: Opts = opts_builder.into();

        // Pool creation and initial connection test must run on the Tokio runtime
        // because mysql_async internally uses tokio::spawn for pool management.
        let runtime = get_mysql_runtime();
        let pool = runtime
            .spawn(async move {
                let pool = Pool::new(opts);
                // Verify connectivity by acquiring and releasing a connection
                let _conn = pool.get_conn().await.map_err(|e| {
                    ZqlzError::Connection(format!("Failed to connect to MySQL: {}", e))
                })?;
                Ok::<Pool, ZqlzError>(pool)
            })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL connection task failed: {}", e))
            })??;

        // Resolve the active database name so schema introspection can use a
        // concrete value instead of relying on DATABASE() at query time.
        let database_name = if let Some(db) = database {
            Some(db.to_string())
        } else {
            let pool_clone = pool.clone();
            get_mysql_runtime()
                .spawn(async move {
                    let mut conn = pool_clone.get_conn().await.map_err(|e| {
                        ZqlzError::Connection(format!(
                            "Failed to get connection for DATABASE() query: {}",
                            e
                        ))
                    })?;
                    let row: Option<(Option<String>,)> = conn
                        .query_first("SELECT DATABASE()")
                        .await
                        .map_err(|e| {
                            ZqlzError::Query(format!("Failed to query DATABASE(): {}", e))
                        })?;
                    Ok::<Option<String>, ZqlzError>(row.and_then(|(db,)| db))
                })
                .await
                .map_err(|e| {
                    ZqlzError::Connection(format!("MySQL DATABASE() task failed: {}", e))
                })?
                .unwrap_or(None)
        };

        tracing::info!(host = %host, port = %port, database = ?database_name, "MySQL connection established");
        Ok(Self {
            pool,
            database_name,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Get a connection from the pool, dispatched on the MySQL Tokio runtime
    async fn get_conn(&self) -> Result<Conn> {
        let pool = self.pool.clone();
        get_mysql_runtime()
            .spawn(async move { pool.get_conn().await })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL get_conn task failed: {}", e))
            })?
            .map_err(|e| ZqlzError::Connection(format!("Failed to get MySQL connection: {}", e)))
    }

    /// Reset the cancellation flag
    fn reset_cancellation(&self) {
        self.cancelled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns the stored database name for use by schema introspection.
    ///
    /// In MySQL, "schema" and "database" are synonymous. This mirrors
    /// PostgreSQL's `schema.unwrap_or("public")` pattern — callers pass
    /// `schema: None` through the trait, and this method provides the
    /// concrete database name to use in information_schema queries.
    pub fn default_database(&self) -> Option<&str> {
        self.database_name.as_deref()
    }
}

/// Escape a value for SQL literal inclusion (for MySQL)
fn value_to_mysql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::String(v) => format!("'{}'", v.replace("'", "''").replace("\\", "\\\\")),
        Value::Bytes(v) => {
            // Convert bytes to hex string for MySQL
            let hex: String = v.iter().map(|b| format!("{:02x}", b)).collect();
            format!("X'{}'", hex)
        }
        Value::Uuid(v) => format!("'{}'", v),
        Value::Json(v) => format!(
            "'{}'",
            v.to_string().replace("'", "''").replace("\\", "\\\\")
        ),
        Value::DateTimeUtc(v) => format!("'{}'", v.format("%Y-%m-%d %H:%M:%S")),
        Value::Date(v) => format!("'{}'", v),
        Value::Time(v) => format!("'{}'", v),
        Value::DateTime(v) => format!("'{}'", v.format("%Y-%m-%d %H:%M:%S")),
        Value::Decimal(v) => v.to_string(),
        Value::Array(arr) => {
            // MySQL doesn't have native array support, convert to JSON
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", json.replace("'", "''"))
        }
    }
}

/// Convert mysql_async Value to our Value type, using column type metadata
/// to correctly interpret byte strings from the text protocol.
fn mysql_value_to_value(val: mysql_async::Value, col_type: ColumnType) -> Value {
    match val {
        mysql_async::Value::NULL => Value::Null,
        mysql_async::Value::Bytes(bytes) => {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                match col_type {
                    ColumnType::MYSQL_TYPE_TINY
                    | ColumnType::MYSQL_TYPE_SHORT
                    | ColumnType::MYSQL_TYPE_LONG
                    | ColumnType::MYSQL_TYPE_LONGLONG
                    | ColumnType::MYSQL_TYPE_INT24
                    | ColumnType::MYSQL_TYPE_YEAR => {
                        s.parse::<i64>().map(Value::Int64).unwrap_or(Value::String(s))
                    }
                    ColumnType::MYSQL_TYPE_FLOAT => {
                        s.parse::<f32>().map(Value::Float32).unwrap_or(Value::String(s))
                    }
                    ColumnType::MYSQL_TYPE_DOUBLE
                    | ColumnType::MYSQL_TYPE_DECIMAL
                    | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                        s.parse::<f64>().map(Value::Float64).unwrap_or(Value::String(s))
                    }
                    _ => Value::String(s),
                }
            } else {
                Value::Bytes(bytes)
            }
        }
        mysql_async::Value::Int(i) => Value::Int64(i),
        mysql_async::Value::UInt(u) => {
            if u <= i64::MAX as u64 {
                Value::Int64(u as i64)
            } else {
                Value::String(u.to_string())
            }
        }
        mysql_async::Value::Float(f) => Value::Float32(f),
        mysql_async::Value::Double(d) => Value::Float64(d),
        mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
            if hour == 0 && min == 0 && sec == 0 && micro == 0 {
                // Date only
                if let Some(date) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                {
                    Value::Date(date)
                } else {
                    Value::String(format!("{:04}-{:02}-{:02}", year, month, day))
                }
            } else {
                // DateTime
                if let Some(dt) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).and_then(
                        |d| d.and_hms_micro_opt(hour as u32, min as u32, sec as u32, micro),
                    )
                {
                    Value::DateTime(dt)
                } else {
                    Value::String(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, min, sec
                    ))
                }
            }
        }
        mysql_async::Value::Time(negative, days, hours, mins, secs, micros) => {
            let total_hours = (days as u32) * 24 + (hours as u32);
            let sign = if negative { "-" } else { "" };
            Value::String(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                sign, total_hours, mins, secs, micros
            ))
        }
    }
}

#[async_trait]
impl Connection for MySqlConnection {
    fn driver_name(&self) -> &str {
        "mysql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mysql")
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.reset_cancellation();

        let mut conn = self.get_conn().await?;

        let final_sql = if params.is_empty() {
            sql.to_string()
        } else {
            let mut result = sql.to_string();
            for (i, param) in params.iter().enumerate() {
                let placeholder = format!("${}", i + 1);
                let value_str = value_to_mysql_literal(param);
                result = result.replacen(&placeholder, &value_str, 1);
            }
            for param in params.iter() {
                let value_str = value_to_mysql_literal(param);
                result = result.replacen("?", &value_str, 1);
            }
            result
        };

        let affected_rows = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop(&final_sql).await.map_err(|e| {
                    ZqlzError::Query(format!("Failed to execute statement: {}", e))
                })?;
                Ok::<u64, ZqlzError>(conn.affected_rows())
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL execute task failed: {}", e)))??;

        tracing::debug!(affected_rows = affected_rows, "statement executed");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows,
            error: None,
        })
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.reset_cancellation();
        let start_time = std::time::Instant::now();

        let mut conn = self.get_conn().await?;

        let final_sql = if params.is_empty() {
            sql.to_string()
        } else {
            let mut result = sql.to_string();
            for (i, param) in params.iter().enumerate() {
                let placeholder = format!("${}", i + 1);
                let value_str = value_to_mysql_literal(param);
                result = result.replacen(&placeholder, &value_str, 1);
            }
            for param in params.iter() {
                let value_str = value_to_mysql_literal(param);
                result = result.replacen("?", &value_str, 1);
            }
            result
        };

        let cancelled = self.cancelled.clone();
        let (columns, _column_names, rows) = get_mysql_runtime()
            .spawn(async move {
                let mysql_rows: Vec<MySqlRow> = conn.query(&final_sql).await.map_err(|e| {
                    ZqlzError::Query(format!("Failed to execute query: {}", e))
                })?;

                let mut columns = Vec::new();
                let mut column_names = Vec::new();
                let mut column_types = Vec::new();

                if let Some(first_row) = mysql_rows.first() {
                    for (idx, col) in first_row.columns_ref().iter().enumerate() {
                        let name = col.name_str().to_string();
                        column_names.push(name.clone());
                        column_types.push(col.column_type());

                        columns.push(ColumnMeta {
                            name,
                            data_type: format!("{:?}", col.column_type()),
                            nullable: true,
                            ordinal: idx,
                            max_length: Some(col.column_length() as i64),
                            precision: None,
                            scale: None,
                            auto_increment: false,
                            default_value: None,
                            comment: None,
                            enum_values: None,
                        });
                    }
                }

                let mut rows = Vec::new();
                for mysql_row in mysql_rows {
                    if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                        tracing::debug!("Query cancelled by user");
                        break;
                    }

                    let mut values = Vec::new();
                    for idx in 0..columns.len() {
                        let mysql_val: mysql_async::Value =
                            mysql_row.get(idx).unwrap_or(mysql_async::Value::NULL);
                        let col_type = column_types.get(idx).copied().unwrap_or(ColumnType::MYSQL_TYPE_STRING);
                        let value = mysql_value_to_value(mysql_val, col_type);
                        values.push(value);
                    }
                    rows.push(Row::new(column_names.clone(), values));
                }

                Ok::<(Vec<ColumnMeta>, Vec<String>, Vec<Row>), ZqlzError>((
                    columns,
                    column_names,
                    rows,
                ))
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))??;

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
        tracing::debug!("beginning MySQL transaction");
        
        // Get a connection from the pool for the duration of the transaction
        let mut conn = self.get_conn().await?;
        
        // Begin the transaction
        let conn = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop("START TRANSACTION").await.map_err(|e| {
                    ZqlzError::Query(format!("Failed to begin transaction: {}", e))
                })?;
                Ok::<Conn, ZqlzError>(conn)
            })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL begin transaction task failed: {}", e))
            })??;
        
        tracing::debug!("MySQL transaction begun successfully");
        Ok(Box::new(MySqlTransaction {
            conn: Arc::new(tokio::sync::Mutex::new(Some(conn))),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing MySQL connection pool");
        let pool = self.pool.clone();
        get_mysql_runtime()
            .spawn(async move { pool.disconnect().await })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL close task failed: {}", e))
            })?
            .map_err(|e| {
                ZqlzError::Connection(format!("Failed to close MySQL connection: {}", e))
            })?;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(MySqlCancelHandle {
            cancelled: self.cancelled.clone(),
        }))
    }

    /// Override update_cell to use SQL literals instead of parameters
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value (MySQL)"
        );

        // Escape table name (may include schema.table format)
        let table_identifier = escape_table_name_mysql(&request.table_name);

        // Build WHERE clause with literal values
        let where_clause = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported. Use primary key or full row identifier."
                        .to_string(),
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => pk_values
                .iter()
                .map(|(col, val)| {
                    format!(
                        "{} = {}",
                        escape_identifier_mysql(col),
                        value_to_mysql_literal(val)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            RowIdentifier::FullRow(row_values) => row_values
                .iter()
                .map(|(col, val)| {
                    if val == &Value::Null {
                        format!("{} IS NULL", escape_identifier_mysql(col))
                    } else {
                        format!(
                            "{} = {}",
                            escape_identifier_mysql(col),
                            value_to_mysql_literal(val)
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND "),
        };

        let set_value = match &request.new_value {
            Some(val) => value_to_mysql_literal(val),
            None => "NULL".to_string(),
        };

        let sql = format!(
            "UPDATE {} SET {} = {} WHERE {}",
            table_identifier,
            escape_identifier_mysql(&request.column_name),
            set_value,
            where_clause
        );

        tracing::debug!("MySQL update SQL: {}", sql);

        let mut conn = self.get_conn().await?;
        let rows_affected = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop(&sql).await.map_err(|e| {
                    ZqlzError::Query(format!("Failed to update cell: {}", e))
                })?;
                Ok::<u64, ZqlzError>(conn.affected_rows())
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL update task failed: {}", e)))??;

        tracing::debug!(affected_rows = rows_affected, "cell update completed");
        Ok(rows_affected)
    }
}

/// Escape a MySQL identifier (column name, etc.)
fn escape_identifier_mysql(identifier: &str) -> String {
    format!("`{}`", identifier.replace("`", "``"))
}

/// Escape a table name which may include schema (e.g., "schema.table")
fn escape_table_name_mysql(table_name: &str) -> String {
    if table_name.contains('.') {
        // Handle schema.table format
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            format!(
                "{}.{}",
                escape_identifier_mysql(parts[0]),
                escape_identifier_mysql(parts[1])
            )
        } else {
            escape_identifier_mysql(table_name)
        }
    } else {
        escape_identifier_mysql(table_name)
    }
}

/// MySQL transaction implementation
///
/// Manages a MySQL transaction lifecycle using a dedicated connection from the pool.
/// The transaction begins with START TRANSACTION and can be committed or rolled back.
/// The connection is held for the duration of the transaction and returned to the pool when done.
pub struct MySqlTransaction {
    conn: Arc<tokio::sync::Mutex<Option<Conn>>>,
    committed: bool,
    rolled_back: bool,
}

#[async_trait]
impl Transaction for MySqlTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("committing MySQL transaction");
        
        let conn_mutex = self.conn.clone();
        get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(mut conn) = guard.take() {
                    conn.query_drop("COMMIT").await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to commit transaction: {}", e))
                    })?;
                    // Connection returns to pool when dropped
                }
                Ok::<(), ZqlzError>(())
            })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL commit task failed: {}", e))
            })??;

        self.committed = true;
        tracing::debug!("MySQL transaction committed");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("rolling back MySQL transaction");
        
        let conn_mutex = self.conn.clone();
        get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(mut conn) = guard.take() {
                    conn.query_drop("ROLLBACK").await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to rollback transaction: {}", e))
                    })?;
                    // Connection returns to pool when dropped
                }
                Ok::<(), ZqlzError>(())
            })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL rollback task failed: {}", e))
            })??;

        self.rolled_back = true;
        tracing::debug!("MySQL transaction rolled back");
        Ok(())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        if self.committed {
            return Err(ZqlzError::Query("Cannot execute on committed transaction".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Cannot execute on rolled back transaction".into()));
        }

        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in transaction");

        if !params.is_empty() {
            return Err(ZqlzError::NotSupported(
                "Parameterized queries not yet supported for MySQL transactions. Use SQL literals instead.".to_string(),
            ));
        }

        let conn_mutex = self.conn.clone();
        let sql = sql.to_string();
        
        let rows_affected = get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(ref mut conn) = *guard {
                    conn.query_drop(&sql).await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to execute statement: {}", e))
                    })?;
                    Ok::<u64, ZqlzError>(conn.affected_rows())
                } else {
                    Err(ZqlzError::Query("Transaction connection no longer available".into()))
                }
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL execute task failed: {}", e)))??;

        tracing::debug!(affected_rows = rows_affected, "statement executed in transaction");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected,
            error: None,
        })
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        if self.committed {
            return Err(ZqlzError::Query("Cannot query on committed transaction".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Cannot query on rolled back transaction".into()));
        }

        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in transaction");

        if !params.is_empty() {
            return Err(ZqlzError::NotSupported(
                "Parameterized queries not yet supported for MySQL transactions. Use SQL literals instead.".to_string(),
            ));
        }

        let conn_mutex = self.conn.clone();
        let sql = sql.to_string();
        let start_time = std::time::Instant::now();
        
        let (rows_data, column_names, column_types) = get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(ref mut conn) = *guard {
                    let rows: Vec<MySqlRow> = conn.query(&sql).await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to execute query: {}", e))
                    })?;

                    let mut column_names = Vec::new();
                    let mut column_types = Vec::new();
                    
                    if let Some(first_row) = rows.first() {
                        for col in first_row.columns_ref().iter() {
                            column_names.push(col.name_str().to_string());
                            column_types.push(col.column_type());
                        }
                    }

                    Ok::<(Vec<MySqlRow>, Vec<String>, Vec<ColumnType>), ZqlzError>((rows, column_names, column_types))
                } else {
                    Err(ZqlzError::Query("Transaction connection no longer available".into()))
                }
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))??;

        // Convert MySQL rows to ZQLZ rows
        let mut columns = Vec::new();
        for (idx, name) in column_names.iter().enumerate() {
            columns.push(ColumnMeta {
                name: name.clone(),
                data_type: format!("{:?}", column_types.get(idx)),
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
        for mysql_row in &rows_data {
            let mut values = Vec::new();
            for idx in 0..column_names.len() {
                let mysql_val: mysql_async::Value =
                    mysql_row.get(idx).unwrap_or(mysql_async::Value::NULL);
                let col_type = column_types.get(idx).copied().unwrap_or(ColumnType::MYSQL_TYPE_STRING);
                let value = mysql_value_to_value(mysql_val, col_type);
                values.push(value);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();

        tracing::debug!(
            row_count = total_rows,
            execution_time_ms = execution_time_ms,
            "query executed successfully in transaction"
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
}

impl Drop for MySqlTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            tracing::warn!("MySQL transaction dropped without commit or rollback - will auto-rollback");
            let conn_mutex = self.conn.clone();
            std::thread::spawn(move || {
                get_mysql_runtime().block_on(async move {
                    let mut guard = conn_mutex.lock().await;
                    if let Some(ref mut conn) = *guard {
                        if let Err(e) = conn.query_drop("ROLLBACK").await {
                            tracing::error!("Failed to rollback dropped transaction: {}", e);
                        }
                    }
                });
            });
        }
    }
}
