//! DuckDB driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;
use zqlz_core::{
    AutoIncrementInfo, AutoIncrementStyle, ColumnMeta, CommentStyles, Connection, ConnectionConfig,
    ConnectionField, ConnectionFieldSchema, DataTypeCategory, DataTypeInfo, DatabaseDriver,
    DialectInfo, DriverCapabilities, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo,
    QueryResult, Result, Row, SqlFunctionInfo, StatementResult, Transaction, Value, ZqlzError,
};

/// DuckDB database driver
///
/// DuckDB is an embeddable analytical database designed for OLAP workloads.
/// It can run in-memory (`:memory:`) or persist data to a file.
pub struct DuckDbDriver;

impl DuckDbDriver {
    /// Create a new DuckDB driver instance
    pub fn new() -> Self {
        tracing::debug!("DuckDB driver initialized");
        Self
    }
}

impl Default for DuckDbDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for DuckDbDriver {
    fn id(&self) -> &'static str {
        "duckdb"
    }

    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn display_name(&self) -> &'static str {
        "DuckDB"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn default_port(&self) -> Option<u16> {
        // DuckDB is file-based, no network port
        None
    }

    fn icon_name(&self) -> &'static str {
        "duckdb"
    }

    fn dialect_info(&self) -> DialectInfo {
        duckdb_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: true,
            supports_upsert: true, // INSERT OR REPLACE / ON CONFLICT
            supports_window_functions: true,
            supports_cte: true,
            supports_json: true,
            supports_full_text_search: true, // Full-text search extension
            supports_stored_procedures: false, // No stored procedures in DuckDB
            supports_schemas: true,
            supports_multiple_databases: true, // ATTACH DATABASE
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: false, // No triggers in DuckDB
            supports_ssl: false,      // File-based, no SSL
            max_identifier_length: Some(255),
            max_parameters: None, // No hard limit
        }
    }

    #[tracing::instrument(skip(self, config), fields(database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to DuckDB");

        let path = config
            .get_string("database")
            .or_else(|| config.get_string("path"))
            .unwrap_or_else(|| ":memory:".to_string());

        let connection = duckdb::Connection::open(&path)
            .map_err(|e| ZqlzError::Driver(format!("Failed to open DuckDB database: {}", e)))?;

        Ok(Arc::new(DuckDbConnection::new(connection, path)))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing DuckDB connection");
        let conn = self.connect(config).await?;
        conn.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        config
            .get_string("database")
            .or_else(|| config.get_string("path"))
            .unwrap_or_else(|| ":memory:".to_string())
    }

    fn connection_string_help(&self) -> &'static str {
        "Path to database file (e.g., /path/to/db.duckdb) or :memory: for in-memory database"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("DuckDB Connection"),
            fields: vec![
                ConnectionField::file_path("path", "Database File")
                    .placeholder("/path/to/database.duckdb")
                    .with_extensions(vec!["duckdb", "db"])
                    .required()
                    .help_text("Use :memory: for an in-memory database"),
            ],
        }
    }
}

/// DuckDB connection wrapper implementing the Connection trait
pub struct DuckDbConnection {
    connection: std::sync::Mutex<duckdb::Connection>,
    path: String,
    closed: AtomicBool,
}

impl DuckDbConnection {
    /// Create a new DuckDB connection wrapper
    pub fn new(connection: duckdb::Connection, path: String) -> Self {
        Self {
            connection: std::sync::Mutex::new(connection),
            path,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the database path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Check if this is an in-memory database
    pub fn is_memory(&self) -> bool {
        self.path == ":memory:"
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for DuckDbConnection {
    fn driver_name(&self) -> &str {
        "duckdb"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("duckdb")
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let conn = self
            .connection
            .lock()
            .map_err(|e| ZqlzError::Driver(format!("Lock poisoned: {}", e)))?;

        let affected = conn
            .execute(sql, [])
            .map_err(|e| ZqlzError::Driver(format!("Execute failed: {}", e)))?;

        tracing::debug!(
            affected_rows = affected,
            duration_ms = start.elapsed().as_millis() as u64,
            "execute completed"
        );

        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: affected as u64,
            error: None,
        })
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let conn = self
            .connection
            .lock()
            .map_err(|e| ZqlzError::Driver(format!("Lock poisoned: {}", e)))?;

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| ZqlzError::Driver(format!("Prepare failed: {}", e)))?;

        // Execute query first, then get column info
        let mut duckdb_rows = stmt
            .query([])
            .map_err(|e| ZqlzError::Driver(format!("Query failed: {}", e)))?;

        // Get column names from the result set
        let column_names: Vec<String> = duckdb_rows
            .as_ref()
            .map(|r| r.column_names().iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let column_count = column_names.len();

        // Build column metadata
        let columns: Vec<ColumnMeta> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| ColumnMeta {
                name: name.clone(),
                data_type: "TEXT".to_string(), // DuckDB doesn't expose types easily
                nullable: true,
                ordinal: idx,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            })
            .collect();

        // Collect rows
        let mut raw_rows: Vec<Vec<Value>> = Vec::new();
        while let Some(row) = duckdb_rows
            .next()
            .map_err(|e| ZqlzError::Driver(format!("Row fetch failed: {}", e)))?
        {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value = row_to_value(&row, i);
                values.push(value);
            }
            raw_rows.push(values);
        }

        // Convert to Row structs
        let rows: Vec<Row> = raw_rows
            .into_iter()
            .map(|values| Row::new(column_names.clone(), values))
            .collect();

        let execution_time_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            row_count = rows.len(),
            duration_ms = execution_time_ms,
            "query completed"
        );

        Ok(QueryResult {
            id: Uuid::new_v4(),
            columns,
            rows,
            total_rows: None,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.ensure_not_closed()?;
        Err(ZqlzError::NotImplemented(
            "Transactions for DuckDB will be implemented in a future update".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        tracing::debug!("DuckDB connection closed");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

fn row_to_value(row: &duckdb::Row, idx: usize) -> Value {
    // Try i64 first (most common integer type)
    if let Ok(v) = row.get::<_, Option<i64>>(idx) {
        return match v {
            Some(n) => Value::Int64(n),
            None => Value::Null,
        };
    }
    // Try f64
    if let Ok(v) = row.get::<_, Option<f64>>(idx) {
        return match v {
            Some(n) => Value::Float64(n),
            None => Value::Null,
        };
    }
    // Try string
    if let Ok(v) = row.get::<_, Option<String>>(idx) {
        return match v {
            Some(s) => Value::String(s),
            None => Value::Null,
        };
    }
    // Try bool
    if let Ok(v) = row.get::<_, Option<bool>>(idx) {
        return match v {
            Some(b) => Value::Bool(b),
            None => Value::Null,
        };
    }
    // Try bytes
    if let Ok(v) = row.get::<_, Option<Vec<u8>>>(idx) {
        return match v {
            Some(b) => Value::Bytes(b),
            None => Value::Null,
        };
    }
    // Default to null
    Value::Null
}

impl std::fmt::Debug for DuckDbConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbConnection")
            .field("path", &self.path)
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}

/// Create DuckDB dialect information
pub fn duckdb_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("duckdb"),
        display_name: Cow::Borrowed("DuckDB SQL"),
        keywords: duckdb_keywords(),
        functions: duckdb_functions(),
        data_types: duckdb_data_types(),
        table_options: vec![],
        auto_increment: Some(AutoIncrementInfo {
            keyword: Cow::Borrowed(""),
            style: AutoIncrementStyle::Generated, // DuckDB uses GENERATED AS IDENTITY
            description: Some(Cow::Borrowed(
                "Use INTEGER PRIMARY KEY for auto-increment (DuckDB auto-generates)",
            )),
        }),
        identifier_quote: '"',
        string_quote: '\'',
        case_sensitive_identifiers: false,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: duckdb_explain_config(),
    }
}

fn duckdb_explain_config() -> ExplainConfig {
    ExplainConfig {
        explain_format: Cow::Borrowed("EXPLAIN {sql}"),
        query_plan_format: Some(Cow::Borrowed("EXPLAIN ANALYZE {sql}")),
        analyze_format: Some(Cow::Borrowed("EXPLAIN ANALYZE {sql}")),
        explain_description: Cow::Borrowed("Shows query execution plan"),
        query_plan_description: Some(Cow::Borrowed(
            "Shows execution plan with runtime statistics",
        )),
        analyze_is_safe: false,
    }
}

fn duckdb_keywords() -> Vec<KeywordInfo> {
    vec![
        // DQL
        KeywordInfo::new("SELECT", KeywordCategory::Dql),
        KeywordInfo::new("FROM", KeywordCategory::Dql),
        KeywordInfo::new("WHERE", KeywordCategory::Dql),
        KeywordInfo::new("DISTINCT", KeywordCategory::Dql),
        KeywordInfo::new("ALL", KeywordCategory::Dql),
        // DML
        KeywordInfo::new("INSERT", KeywordCategory::Dml),
        KeywordInfo::new("UPDATE", KeywordCategory::Dml),
        KeywordInfo::new("DELETE", KeywordCategory::Dml),
        KeywordInfo::new("COPY", KeywordCategory::Dml),
        // DDL
        KeywordInfo::new("CREATE", KeywordCategory::Ddl),
        KeywordInfo::new("ALTER", KeywordCategory::Ddl),
        KeywordInfo::new("DROP", KeywordCategory::Ddl),
        KeywordInfo::new("TRUNCATE", KeywordCategory::Ddl),
        // Transaction
        KeywordInfo::new("BEGIN", KeywordCategory::Transaction),
        KeywordInfo::new("COMMIT", KeywordCategory::Transaction),
        KeywordInfo::new("ROLLBACK", KeywordCategory::Transaction),
        KeywordInfo::new("SAVEPOINT", KeywordCategory::Transaction),
        KeywordInfo::new("TRANSACTION", KeywordCategory::Transaction),
        // Clauses
        KeywordInfo::new("JOIN", KeywordCategory::Clause),
        KeywordInfo::new("INNER", KeywordCategory::Clause),
        KeywordInfo::new("LEFT", KeywordCategory::Clause),
        KeywordInfo::new("RIGHT", KeywordCategory::Clause),
        KeywordInfo::new("OUTER", KeywordCategory::Clause),
        KeywordInfo::new("CROSS", KeywordCategory::Clause),
        KeywordInfo::new("FULL", KeywordCategory::Clause),
        KeywordInfo::new("ON", KeywordCategory::Clause),
        KeywordInfo::new("USING", KeywordCategory::Clause),
        KeywordInfo::new("GROUP", KeywordCategory::Clause),
        KeywordInfo::new("BY", KeywordCategory::Clause),
        KeywordInfo::new("HAVING", KeywordCategory::Clause),
        KeywordInfo::new("ORDER", KeywordCategory::Clause),
        KeywordInfo::new("LIMIT", KeywordCategory::Clause),
        KeywordInfo::new("OFFSET", KeywordCategory::Clause),
        KeywordInfo::new("WITH", KeywordCategory::Clause),
        KeywordInfo::new("AS", KeywordCategory::Clause),
        KeywordInfo::new("OVER", KeywordCategory::Clause),
        KeywordInfo::new("PARTITION", KeywordCategory::Clause),
        KeywordInfo::new("WINDOW", KeywordCategory::Clause),
        KeywordInfo::new("RETURNING", KeywordCategory::Clause),
        KeywordInfo::new("QUALIFY", KeywordCategory::Clause),
        KeywordInfo::new("SAMPLE", KeywordCategory::Clause),
        // Operators
        KeywordInfo::new("AND", KeywordCategory::Operator),
        KeywordInfo::new("OR", KeywordCategory::Operator),
        KeywordInfo::new("NOT", KeywordCategory::Operator),
        KeywordInfo::new("IN", KeywordCategory::Operator),
        KeywordInfo::new("LIKE", KeywordCategory::Operator),
        KeywordInfo::new("ILIKE", KeywordCategory::Operator),
        KeywordInfo::new("BETWEEN", KeywordCategory::Operator),
        KeywordInfo::new("EXISTS", KeywordCategory::Operator),
        KeywordInfo::new("IS", KeywordCategory::Operator),
        KeywordInfo::new("NULL", KeywordCategory::Operator),
        KeywordInfo::new("GLOB", KeywordCategory::Operator),
        KeywordInfo::new("SIMILAR", KeywordCategory::Operator),
        // DuckDB specific
        KeywordInfo::with_desc(
            "ATTACH",
            KeywordCategory::DatabaseSpecific,
            "Attach external database",
        ),
        KeywordInfo::with_desc(
            "DETACH",
            KeywordCategory::DatabaseSpecific,
            "Detach external database",
        ),
        KeywordInfo::with_desc(
            "EXPORT",
            KeywordCategory::DatabaseSpecific,
            "Export database to directory",
        ),
        KeywordInfo::with_desc(
            "IMPORT",
            KeywordCategory::DatabaseSpecific,
            "Import database from directory",
        ),
        KeywordInfo::with_desc(
            "PIVOT",
            KeywordCategory::DatabaseSpecific,
            "Pivot rows to columns",
        ),
        KeywordInfo::with_desc(
            "UNPIVOT",
            KeywordCategory::DatabaseSpecific,
            "Unpivot columns to rows",
        ),
        KeywordInfo::with_desc(
            "SUMMARIZE",
            KeywordCategory::DatabaseSpecific,
            "Generate table statistics",
        ),
        KeywordInfo::with_desc(
            "DESCRIBE",
            KeywordCategory::DatabaseSpecific,
            "Show table structure",
        ),
        KeywordInfo::with_desc(
            "PRAGMA",
            KeywordCategory::DatabaseSpecific,
            "Database configuration",
        ),
        KeywordInfo::with_desc(
            "INSTALL",
            KeywordCategory::DatabaseSpecific,
            "Install extension",
        ),
        KeywordInfo::with_desc("LOAD", KeywordCategory::DatabaseSpecific, "Load extension"),
        KeywordInfo::with_desc(
            "CALL",
            KeywordCategory::DatabaseSpecific,
            "Call table function",
        ),
        // DCL
        KeywordInfo::new("GRANT", KeywordCategory::Dcl),
        KeywordInfo::new("REVOKE", KeywordCategory::Dcl),
    ]
}

fn duckdb_functions() -> Vec<SqlFunctionInfo> {
    vec![
        // Aggregate functions
        SqlFunctionInfo::new("COUNT", FunctionCategory::Aggregate)
            .with_signature("COUNT(expression)"),
        SqlFunctionInfo::new("SUM", FunctionCategory::Aggregate).with_signature("SUM(expression)"),
        SqlFunctionInfo::new("AVG", FunctionCategory::Aggregate).with_signature("AVG(expression)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Aggregate).with_signature("MIN(expression)"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Aggregate).with_signature("MAX(expression)"),
        SqlFunctionInfo::new("STRING_AGG", FunctionCategory::Aggregate)
            .with_signature("STRING_AGG(expression, separator)"),
        SqlFunctionInfo::new("FIRST", FunctionCategory::Aggregate)
            .with_signature("FIRST(expression)"),
        SqlFunctionInfo::new("LAST", FunctionCategory::Aggregate)
            .with_signature("LAST(expression)"),
        SqlFunctionInfo::new("MEDIAN", FunctionCategory::Aggregate)
            .with_signature("MEDIAN(expression)"),
        SqlFunctionInfo::new("MODE", FunctionCategory::Aggregate)
            .with_signature("MODE(expression)"),
        SqlFunctionInfo::new("STDDEV", FunctionCategory::Aggregate)
            .with_signature("STDDEV(expression)"),
        SqlFunctionInfo::new("VARIANCE", FunctionCategory::Aggregate)
            .with_signature("VARIANCE(expression)"),
        SqlFunctionInfo::new("APPROX_COUNT_DISTINCT", FunctionCategory::Aggregate)
            .with_signature("APPROX_COUNT_DISTINCT(expression)"),
        SqlFunctionInfo::new("LIST", FunctionCategory::Aggregate)
            .with_signature("LIST(expression)"),
        SqlFunctionInfo::new("HISTOGRAM", FunctionCategory::Aggregate)
            .with_signature("HISTOGRAM(expression)"),
        // Window functions
        SqlFunctionInfo::new("ROW_NUMBER", FunctionCategory::Window)
            .with_signature("ROW_NUMBER() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("RANK", FunctionCategory::Window)
            .with_signature("RANK() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("DENSE_RANK", FunctionCategory::Window)
            .with_signature("DENSE_RANK() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("NTILE", FunctionCategory::Window)
            .with_signature("NTILE(n) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("LAG", FunctionCategory::Window)
            .with_signature("LAG(expression, offset, default) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("LEAD", FunctionCategory::Window)
            .with_signature("LEAD(expression, offset, default) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("FIRST_VALUE", FunctionCategory::Window)
            .with_signature("FIRST_VALUE(expression) OVER (...)"),
        SqlFunctionInfo::new("LAST_VALUE", FunctionCategory::Window)
            .with_signature("LAST_VALUE(expression) OVER (...)"),
        SqlFunctionInfo::new("NTH_VALUE", FunctionCategory::Window)
            .with_signature("NTH_VALUE(expression, n) OVER (...)"),
        // String functions
        SqlFunctionInfo::new("LENGTH", FunctionCategory::String).with_signature("LENGTH(string)"),
        SqlFunctionInfo::new("LEFT", FunctionCategory::String)
            .with_signature("LEFT(string, length)"),
        SqlFunctionInfo::new("RIGHT", FunctionCategory::String)
            .with_signature("RIGHT(string, length)"),
        SqlFunctionInfo::new("SUBSTRING", FunctionCategory::String)
            .with_signature("SUBSTRING(string FROM start FOR length)"),
        SqlFunctionInfo::new("SUBSTR", FunctionCategory::String)
            .with_signature("SUBSTR(string, start, length)"),
        SqlFunctionInfo::new("POSITION", FunctionCategory::String)
            .with_signature("POSITION(substring IN string)"),
        SqlFunctionInfo::new("STRPOS", FunctionCategory::String)
            .with_signature("STRPOS(string, substring)"),
        SqlFunctionInfo::new("REPLACE", FunctionCategory::String)
            .with_signature("REPLACE(string, from, to)"),
        SqlFunctionInfo::new("REGEXP_REPLACE", FunctionCategory::String)
            .with_signature("REGEXP_REPLACE(string, pattern, replacement)"),
        SqlFunctionInfo::new("CONCAT", FunctionCategory::String)
            .with_signature("CONCAT(string1, string2, ...)"),
        SqlFunctionInfo::new("CONCAT_WS", FunctionCategory::String)
            .with_signature("CONCAT_WS(separator, string1, string2, ...)"),
        SqlFunctionInfo::new("UPPER", FunctionCategory::String).with_signature("UPPER(string)"),
        SqlFunctionInfo::new("LOWER", FunctionCategory::String).with_signature("LOWER(string)"),
        SqlFunctionInfo::new("TRIM", FunctionCategory::String)
            .with_signature("TRIM([LEADING|TRAILING|BOTH] FROM string)"),
        SqlFunctionInfo::new("LTRIM", FunctionCategory::String).with_signature("LTRIM(string)"),
        SqlFunctionInfo::new("RTRIM", FunctionCategory::String).with_signature("RTRIM(string)"),
        SqlFunctionInfo::new("SPLIT_PART", FunctionCategory::String)
            .with_signature("SPLIT_PART(string, delimiter, index)"),
        SqlFunctionInfo::new("REVERSE", FunctionCategory::String).with_signature("REVERSE(string)"),
        SqlFunctionInfo::new("REPEAT", FunctionCategory::String)
            .with_signature("REPEAT(string, count)"),
        SqlFunctionInfo::new("LPAD", FunctionCategory::String)
            .with_signature("LPAD(string, length, pad)"),
        SqlFunctionInfo::new("RPAD", FunctionCategory::String)
            .with_signature("RPAD(string, length, pad)"),
        SqlFunctionInfo::new("FORMAT", FunctionCategory::String)
            .with_signature("FORMAT(format_string, ...)"),
        SqlFunctionInfo::new("PRINTF", FunctionCategory::String)
            .with_signature("PRINTF(format, ...)"),
        // Numeric functions
        SqlFunctionInfo::new("ABS", FunctionCategory::Numeric).with_signature("ABS(number)"),
        SqlFunctionInfo::new("CEIL", FunctionCategory::Numeric).with_signature("CEIL(number)"),
        SqlFunctionInfo::new("FLOOR", FunctionCategory::Numeric).with_signature("FLOOR(number)"),
        SqlFunctionInfo::new("ROUND", FunctionCategory::Numeric)
            .with_signature("ROUND(number, precision)"),
        SqlFunctionInfo::new("TRUNC", FunctionCategory::Numeric)
            .with_signature("TRUNC(number, precision)"),
        SqlFunctionInfo::new("POWER", FunctionCategory::Numeric)
            .with_signature("POWER(base, exponent)"),
        SqlFunctionInfo::new("SQRT", FunctionCategory::Numeric).with_signature("SQRT(number)"),
        SqlFunctionInfo::new("CBRT", FunctionCategory::Numeric).with_signature("CBRT(number)"),
        SqlFunctionInfo::new("EXP", FunctionCategory::Numeric).with_signature("EXP(number)"),
        SqlFunctionInfo::new("LN", FunctionCategory::Numeric).with_signature("LN(number)"),
        SqlFunctionInfo::new("LOG", FunctionCategory::Numeric).with_signature("LOG(base, number)"),
        SqlFunctionInfo::new("LOG10", FunctionCategory::Numeric).with_signature("LOG10(number)"),
        SqlFunctionInfo::new("MOD", FunctionCategory::Numeric)
            .with_signature("MOD(dividend, divisor)"),
        SqlFunctionInfo::new("SIGN", FunctionCategory::Numeric).with_signature("SIGN(number)"),
        SqlFunctionInfo::new("RANDOM", FunctionCategory::Numeric).with_signature("RANDOM()"),
        // Date/time functions
        SqlFunctionInfo::new("CURRENT_DATE", FunctionCategory::DateTime)
            .with_signature("CURRENT_DATE"),
        SqlFunctionInfo::new("CURRENT_TIME", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIME"),
        SqlFunctionInfo::new("CURRENT_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIMESTAMP"),
        SqlFunctionInfo::new("NOW", FunctionCategory::DateTime).with_signature("NOW()"),
        SqlFunctionInfo::new("DATE_TRUNC", FunctionCategory::DateTime)
            .with_signature("DATE_TRUNC(part, timestamp)"),
        SqlFunctionInfo::new("DATE_PART", FunctionCategory::DateTime)
            .with_signature("DATE_PART(part, timestamp)"),
        SqlFunctionInfo::new("DATE_DIFF", FunctionCategory::DateTime)
            .with_signature("DATE_DIFF(part, start, end)"),
        SqlFunctionInfo::new("DATE_ADD", FunctionCategory::DateTime)
            .with_signature("DATE_ADD(timestamp, interval)"),
        SqlFunctionInfo::new("DATE_SUB", FunctionCategory::DateTime)
            .with_signature("DATE_SUB(timestamp, interval)"),
        SqlFunctionInfo::new("EXTRACT", FunctionCategory::DateTime)
            .with_signature("EXTRACT(part FROM timestamp)"),
        SqlFunctionInfo::new("YEAR", FunctionCategory::DateTime).with_signature("YEAR(timestamp)"),
        SqlFunctionInfo::new("MONTH", FunctionCategory::DateTime)
            .with_signature("MONTH(timestamp)"),
        SqlFunctionInfo::new("DAY", FunctionCategory::DateTime).with_signature("DAY(timestamp)"),
        SqlFunctionInfo::new("HOUR", FunctionCategory::DateTime).with_signature("HOUR(timestamp)"),
        SqlFunctionInfo::new("MINUTE", FunctionCategory::DateTime)
            .with_signature("MINUTE(timestamp)"),
        SqlFunctionInfo::new("SECOND", FunctionCategory::DateTime)
            .with_signature("SECOND(timestamp)"),
        SqlFunctionInfo::new("EPOCH_MS", FunctionCategory::DateTime).with_signature("EPOCH_MS(ms)"),
        SqlFunctionInfo::new("STRFTIME", FunctionCategory::DateTime)
            .with_signature("STRFTIME(format, timestamp)"),
        SqlFunctionInfo::new("STRPTIME", FunctionCategory::DateTime)
            .with_signature("STRPTIME(string, format)"),
        // Conversion functions
        SqlFunctionInfo::new("CAST", FunctionCategory::Conversion)
            .with_signature("CAST(expression AS type)"),
        SqlFunctionInfo::new("TRY_CAST", FunctionCategory::Conversion)
            .with_signature("TRY_CAST(expression AS type)"),
        SqlFunctionInfo::new("TYPEOF", FunctionCategory::Conversion)
            .with_signature("TYPEOF(expression)"),
        // Conditional functions
        SqlFunctionInfo::new("CASE", FunctionCategory::Conditional)
            .with_signature("CASE WHEN condition THEN result ELSE default END"),
        SqlFunctionInfo::new("COALESCE", FunctionCategory::Conditional)
            .with_signature("COALESCE(expression1, expression2, ...)"),
        SqlFunctionInfo::new("NULLIF", FunctionCategory::Conditional)
            .with_signature("NULLIF(expression1, expression2)"),
        SqlFunctionInfo::new("IFNULL", FunctionCategory::Conditional)
            .with_signature("IFNULL(expression, default)"),
        SqlFunctionInfo::new("IF", FunctionCategory::Conditional)
            .with_signature("IF(condition, true_value, false_value)"),
        SqlFunctionInfo::new("GREATEST", FunctionCategory::Conditional)
            .with_signature("GREATEST(value1, value2, ...)"),
        SqlFunctionInfo::new("LEAST", FunctionCategory::Conditional)
            .with_signature("LEAST(value1, value2, ...)"),
        // JSON functions
        SqlFunctionInfo::new("JSON", FunctionCategory::Json).with_signature("JSON(string)"),
        SqlFunctionInfo::new("JSON_EXTRACT", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT(json, path)"),
        SqlFunctionInfo::new("JSON_EXTRACT_STRING", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT_STRING(json, path)"),
        SqlFunctionInfo::new("JSON_KEYS", FunctionCategory::Json).with_signature("JSON_KEYS(json)"),
        SqlFunctionInfo::new("JSON_TYPE", FunctionCategory::Json).with_signature("JSON_TYPE(json)"),
        SqlFunctionInfo::new("JSON_VALID", FunctionCategory::Json)
            .with_signature("JSON_VALID(json)"),
        SqlFunctionInfo::new("JSON_ARRAY_LENGTH", FunctionCategory::Json)
            .with_signature("JSON_ARRAY_LENGTH(json)"),
        SqlFunctionInfo::new("TO_JSON", FunctionCategory::Json)
            .with_signature("TO_JSON(expression)"),
        // List functions
        SqlFunctionInfo::new("ARRAY_LENGTH", FunctionCategory::Other)
            .with_signature("ARRAY_LENGTH(list)"),
        SqlFunctionInfo::new("LIST_VALUE", FunctionCategory::Other)
            .with_signature("LIST_VALUE(value1, value2, ...)"),
        SqlFunctionInfo::new("LIST_EXTRACT", FunctionCategory::Other)
            .with_signature("LIST_EXTRACT(list, index)"),
        SqlFunctionInfo::new("LIST_CONCAT", FunctionCategory::Other)
            .with_signature("LIST_CONCAT(list1, list2)"),
        SqlFunctionInfo::new("UNNEST", FunctionCategory::Other).with_signature("UNNEST(list)"),
        // Table functions
        SqlFunctionInfo::new("READ_CSV", FunctionCategory::Other)
            .with_signature("READ_CSV('path', columns={...})"),
        SqlFunctionInfo::new("READ_PARQUET", FunctionCategory::Other)
            .with_signature("READ_PARQUET('path')"),
        SqlFunctionInfo::new("READ_JSON", FunctionCategory::Other)
            .with_signature("READ_JSON('path')"),
        SqlFunctionInfo::new("GLOB", FunctionCategory::Other).with_signature("GLOB('pattern')"),
        SqlFunctionInfo::new("RANGE", FunctionCategory::Other)
            .with_signature("RANGE(start, end, step)"),
        SqlFunctionInfo::new("GENERATE_SERIES", FunctionCategory::Other)
            .with_signature("GENERATE_SERIES(start, end, step)"),
        // Other functions
        SqlFunctionInfo::new("UUID", FunctionCategory::Other).with_signature("UUID()"),
        SqlFunctionInfo::new("GEN_RANDOM_UUID", FunctionCategory::Other)
            .with_signature("GEN_RANDOM_UUID()"),
        SqlFunctionInfo::new("HASH", FunctionCategory::Other).with_signature("HASH(expression)"),
        SqlFunctionInfo::new("MD5", FunctionCategory::Other).with_signature("MD5(string)"),
        SqlFunctionInfo::new("SHA256", FunctionCategory::Other).with_signature("SHA256(string)"),
    ]
}

fn duckdb_data_types() -> Vec<DataTypeInfo> {
    vec![
        // Boolean
        DataTypeInfo::new("BOOLEAN", DataTypeCategory::Boolean),
        DataTypeInfo::new("BOOL", DataTypeCategory::Boolean),
        // Integer types
        DataTypeInfo::new("TINYINT", DataTypeCategory::Integer),
        DataTypeInfo::new("SMALLINT", DataTypeCategory::Integer),
        DataTypeInfo::new("INTEGER", DataTypeCategory::Integer),
        DataTypeInfo::new("INT", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGINT", DataTypeCategory::Integer),
        DataTypeInfo::new("HUGEINT", DataTypeCategory::Integer),
        DataTypeInfo::new("UTINYINT", DataTypeCategory::Integer),
        DataTypeInfo::new("USMALLINT", DataTypeCategory::Integer),
        DataTypeInfo::new("UINTEGER", DataTypeCategory::Integer),
        DataTypeInfo::new("UBIGINT", DataTypeCategory::Integer),
        // Decimal types
        DataTypeInfo::new("DECIMAL", DataTypeCategory::Decimal).with_length(Some(18), Some(38)),
        DataTypeInfo::new("NUMERIC", DataTypeCategory::Decimal).with_length(Some(18), Some(38)),
        // Float types
        DataTypeInfo::new("REAL", DataTypeCategory::Float),
        DataTypeInfo::new("FLOAT", DataTypeCategory::Float),
        DataTypeInfo::new("DOUBLE", DataTypeCategory::Float),
        // String types
        DataTypeInfo::new("VARCHAR", DataTypeCategory::String),
        DataTypeInfo::new("TEXT", DataTypeCategory::String),
        DataTypeInfo::new("STRING", DataTypeCategory::String),
        DataTypeInfo::new("CHAR", DataTypeCategory::String).with_length(Some(1), Some(1)),
        // Binary
        DataTypeInfo::new("BLOB", DataTypeCategory::Binary),
        DataTypeInfo::new("BYTEA", DataTypeCategory::Binary),
        // Date/time types
        DataTypeInfo::new("DATE", DataTypeCategory::Date),
        DataTypeInfo::new("TIME", DataTypeCategory::Time),
        DataTypeInfo::new("TIMESTAMP", DataTypeCategory::DateTime),
        DataTypeInfo::new("TIMESTAMPTZ", DataTypeCategory::DateTime),
        DataTypeInfo::new("TIMESTAMP WITH TIME ZONE", DataTypeCategory::DateTime),
        DataTypeInfo::new("INTERVAL", DataTypeCategory::DateTime),
        // UUID
        DataTypeInfo::new("UUID", DataTypeCategory::Uuid),
        // JSON
        DataTypeInfo::new("JSON", DataTypeCategory::Json),
        // Complex types
        DataTypeInfo::new("LIST", DataTypeCategory::Array),
        DataTypeInfo::new("ARRAY", DataTypeCategory::Array),
        DataTypeInfo::new("STRUCT", DataTypeCategory::Other),
        DataTypeInfo::new("MAP", DataTypeCategory::Other),
        DataTypeInfo::new("UNION", DataTypeCategory::Other),
        DataTypeInfo::new("ENUM", DataTypeCategory::Other),
    ]
}
