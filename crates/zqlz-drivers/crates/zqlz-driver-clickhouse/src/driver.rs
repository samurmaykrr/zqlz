//! ClickHouse driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, CommentStyles, Connection, ConnectionConfig, ConnectionField,
    ConnectionFieldSchema, DataTypeCategory, DataTypeInfo, DatabaseDriver, DialectInfo,
    DriverCapabilities, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo, QueryResult,
    Result, Row, SqlFunctionInfo, StatementResult, TableOptionDef, TableOptionType, Transaction,
    Value, ZqlzError,
};

/// ClickHouse database driver
///
/// ClickHouse is a column-oriented OLAP database designed for real-time analytics.
/// It excels at aggregating large volumes of data quickly and supports
/// distributed query processing.
pub struct ClickHouseDriver;

impl ClickHouseDriver {
    /// Create a new ClickHouse driver instance
    pub fn new() -> Self {
        tracing::debug!("ClickHouse driver initialized");
        Self
    }
}

impl Default for ClickHouseDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for ClickHouseDriver {
    fn id(&self) -> &'static str {
        "clickhouse"
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn display_name(&self) -> &'static str {
        "ClickHouse"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn default_port(&self) -> Option<u16> {
        Some(8123) // HTTP interface default port
    }

    fn icon_name(&self) -> &'static str {
        "clickhouse"
    }

    fn dialect_info(&self) -> DialectInfo {
        clickhouse_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: false, // ClickHouse has limited transaction support
            supports_savepoints: false,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: false, // No RETURNING clause
            supports_upsert: true,     // Has INSERT...ON CONFLICT syntax with ReplacingMergeTree
            supports_window_functions: true,
            supports_cte: true,
            supports_json: true,
            supports_full_text_search: true, // Full-text search functions
            supports_stored_procedures: false, // No stored procedures
            supports_schemas: true,          // Databases act as schemas
            supports_multiple_databases: true,
            supports_streaming: true, // Streaming inserts
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: false, // No foreign keys
            supports_views: true,         // Regular and materialized views
            supports_triggers: false,     // No triggers
            supports_ssl: true,           // TLS support
            max_identifier_length: Some(255),
            max_parameters: None,
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to ClickHouse");

        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 8123 };
        let database = config
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let username = config
            .username
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let password = config.password.clone().unwrap_or_default();
        let use_ssl = config
            .params
            .get("ssl")
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);

        let url = build_connection_url(&host, port, &database, &username, &password, use_ssl);

        let client = clickhouse::Client::default()
            .with_url(&url)
            .with_user(&username)
            .with_password(&password)
            .with_database(&database);

        // Test the connection
        let test_result: std::result::Result<u8, clickhouse::error::Error> =
            client.query("SELECT 1").fetch_one().await;

        if let Err(e) = test_result {
            return Err(ZqlzError::Driver(format!(
                "Failed to connect to ClickHouse: {}",
                e
            )));
        }

        tracing::debug!("ClickHouse connection established");
        Ok(Arc::new(ClickHouseConnection::new(client, database)))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing ClickHouse connection");
        let conn = self.connect(config).await?;
        conn.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 8123 };
        let database = config
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let username = config
            .username
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let password = config.password.clone().unwrap_or_default();
        let use_ssl = config
            .params
            .get("ssl")
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);

        build_connection_url(&host, port, &database, &username, &password, use_ssl)
    }

    fn connection_string_help(&self) -> &'static str {
        "ClickHouse connection parameters: host, port (default: 8123), database, username, password, ssl (true/false)"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("ClickHouse Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("8123")
                    .default_value("8123")
                    .help_text("HTTP interface port")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database")
                    .placeholder("default")
                    .default_value("default"),
                ConnectionField::text("username", "Username")
                    .placeholder("default")
                    .default_value("default")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::boolean("ssl", "Use SSL/TLS").help_text("Enable HTTPS connection"),
            ],
        }
    }
}

/// Build a ClickHouse HTTP connection URL
fn build_connection_url(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
    use_ssl: bool,
) -> String {
    let protocol = if use_ssl { "https" } else { "http" };

    if password.is_empty() && username == "default" {
        format!("{}://{}:{}/{}", protocol, host, port, database)
    } else if password.is_empty() {
        format!("{}://{}@{}:{}/{}", protocol, username, host, port, database)
    } else {
        format!(
            "{}://{}:{}@{}:{}/{}",
            protocol, username, password, host, port, database
        )
    }
}

/// ClickHouse connection wrapper implementing the Connection trait
pub struct ClickHouseConnection {
    client: clickhouse::Client,
    database: String,
    closed: AtomicBool,
}

impl ClickHouseConnection {
    /// Create a new ClickHouse connection wrapper
    pub fn new(client: clickhouse::Client, database: String) -> Self {
        Self {
            client,
            database,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the database name
    pub fn database(&self) -> &str {
        &self.database
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for ClickHouseConnection {
    fn driver_name(&self) -> &str {
        "clickhouse"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("clickhouse")
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        // ClickHouse doesn't return affected rows for most DDL/DML
        // We just execute and check for errors
        self.client
            .query(sql)
            .execute()
            .await
            .map_err(|e| ZqlzError::Driver(format!("Execute failed: {}", e)))?;

        tracing::debug!(
            duration_ms = start.elapsed().as_millis() as u64,
            "execute completed"
        );

        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0, // ClickHouse doesn't reliably report this
            error: None,
        })
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        // Fetch rows as JSONEachRow format for flexibility with dynamic queries
        let mut cursor = self
            .client
            .query(sql)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| ZqlzError::Driver(format!("Query failed: {}", e)))?;

        // Collect all bytes
        let mut all_bytes = Vec::new();
        while let Some(chunk) = cursor
            .next()
            .await
            .map_err(|e| ZqlzError::Driver(format!("Failed to read query result: {}", e)))?
        {
            all_bytes.extend_from_slice(&chunk);
        }

        // Parse JSONEachRow format (one JSON object per line)
        let content = String::from_utf8_lossy(&all_bytes);
        let mut result: Vec<serde_json::Value> = Vec::new();
        for line in content.lines() {
            if !line.trim().is_empty() {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
                    result.push(value);
                }
            }
        }

        // Extract column names from the first row
        let column_names: Vec<String> = if let Some(first_row) = result.first() {
            if let Some(obj) = first_row.as_object() {
                obj.keys().cloned().collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        // Build column metadata
        let columns: Vec<ColumnMeta> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| ColumnMeta {
                name: name.clone(),
                data_type: "String".to_string(), // Default type
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

        // Convert rows
        let rows: Vec<Row> = result
            .into_iter()
            .map(|row| {
                let values: Vec<Value> = column_names
                    .iter()
                    .map(|col| row.get(col).map(json_to_value).unwrap_or(Value::Null))
                    .collect();
                Row::new(column_names.clone(), values)
            })
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
        Err(ZqlzError::NotSupported(
            "ClickHouse has limited transaction support. Use MergeTree tables with ReplacingMergeTree for data consistency.".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        tracing::debug!("ClickHouse connection closed");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn as_schema_introspection(&self) -> Option<&dyn zqlz_core::SchemaIntrospection> {
        Some(self)
    }
}

/// Convert a JSON value to a zqlz-core Value
#[doc(hidden)]
pub fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            // Convert array to JSON string
            Value::String(serde_json::to_string(arr).unwrap_or_default())
        }
        serde_json::Value::Object(obj) => {
            // Convert object to JSON string
            Value::String(serde_json::to_string(obj).unwrap_or_default())
        }
    }
}

impl std::fmt::Debug for ClickHouseConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConnection")
            .field("database", &self.database)
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}

/// Create ClickHouse dialect information
pub fn clickhouse_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("clickhouse"),
        display_name: Cow::Borrowed("ClickHouse SQL"),
        keywords: clickhouse_keywords(),
        functions: clickhouse_functions(),
        data_types: clickhouse_data_types(),
        table_options: clickhouse_table_options(),
        auto_increment: None, // ClickHouse doesn't have auto-increment in the traditional sense
        identifier_quote: '`', // ClickHouse uses backticks
        string_quote: '\'',
        case_sensitive_identifiers: true,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: clickhouse_explain_config(),
    }
}

fn clickhouse_explain_config() -> ExplainConfig {
    ExplainConfig {
        explain_format: Cow::Borrowed("EXPLAIN {sql}"),
        query_plan_format: Some(Cow::Borrowed("EXPLAIN PLAN {sql}")),
        analyze_format: Some(Cow::Borrowed("EXPLAIN ANALYZE {sql}")),
        explain_description: Cow::Borrowed("Shows query execution plan"),
        query_plan_description: Some(Cow::Borrowed("Shows detailed execution plan")),
        analyze_is_safe: false, // EXPLAIN ANALYZE actually executes the query
    }
}

fn clickhouse_table_options() -> Vec<TableOptionDef> {
    vec![
        TableOptionDef {
            key: Cow::Borrowed("engine"),
            label: Cow::Borrowed("ENGINE"),
            option_type: TableOptionType::Choice,
            default_value: Some(Cow::Borrowed("MergeTree")),
            description: Some(Cow::Borrowed(
                "Table engine determining storage and query semantics",
            )),
            choices: vec![
                Cow::Borrowed("MergeTree"),
                Cow::Borrowed("ReplacingMergeTree"),
                Cow::Borrowed("SummingMergeTree"),
                Cow::Borrowed("AggregatingMergeTree"),
                Cow::Borrowed("CollapsingMergeTree"),
                Cow::Borrowed("VersionedCollapsingMergeTree"),
                Cow::Borrowed("Log"),
                Cow::Borrowed("TinyLog"),
                Cow::Borrowed("StripeLog"),
                Cow::Borrowed("Memory"),
                Cow::Borrowed("Buffer"),
                Cow::Borrowed("Distributed"),
            ],
        },
        TableOptionDef {
            key: Cow::Borrowed("order_by"),
            label: Cow::Borrowed("ORDER BY"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Primary key columns for MergeTree tables")),
            choices: vec![],
        },
        TableOptionDef {
            key: Cow::Borrowed("partition_by"),
            label: Cow::Borrowed("PARTITION BY"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed(
                "Partitioning expression for data organization",
            )),
            choices: vec![],
        },
        TableOptionDef {
            key: Cow::Borrowed("primary_key"),
            label: Cow::Borrowed("PRIMARY KEY"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Primary key (subset of ORDER BY)")),
            choices: vec![],
        },
        TableOptionDef {
            key: Cow::Borrowed("sample_by"),
            label: Cow::Borrowed("SAMPLE BY"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Expression for sampling queries")),
            choices: vec![],
        },
        TableOptionDef {
            key: Cow::Borrowed("ttl"),
            label: Cow::Borrowed("TTL"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed(
                "Time-to-live rule for automatic data deletion",
            )),
            choices: vec![],
        },
        TableOptionDef {
            key: Cow::Borrowed("settings"),
            label: Cow::Borrowed("SETTINGS"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Additional table settings")),
            choices: vec![],
        },
    ]
}

fn clickhouse_keywords() -> Vec<KeywordInfo> {
    vec![
        // DQL
        KeywordInfo::new("SELECT", KeywordCategory::Dql),
        KeywordInfo::new("FROM", KeywordCategory::Dql),
        KeywordInfo::new("WHERE", KeywordCategory::Dql),
        KeywordInfo::new("DISTINCT", KeywordCategory::Dql),
        KeywordInfo::new("ALL", KeywordCategory::Dql),
        KeywordInfo::new("FINAL", KeywordCategory::Dql), // ClickHouse-specific
        KeywordInfo::new("SAMPLE", KeywordCategory::Dql), // ClickHouse-specific
        KeywordInfo::new("PREWHERE", KeywordCategory::Dql), // ClickHouse-specific
        // DML
        KeywordInfo::new("INSERT", KeywordCategory::Dml),
        KeywordInfo::new("UPDATE", KeywordCategory::Dml),
        KeywordInfo::new("DELETE", KeywordCategory::Dml),
        KeywordInfo::new("ALTER", KeywordCategory::Dml),
        // DDL
        KeywordInfo::new("CREATE", KeywordCategory::Ddl),
        KeywordInfo::new("DROP", KeywordCategory::Ddl),
        KeywordInfo::new("TRUNCATE", KeywordCategory::Ddl),
        KeywordInfo::new("RENAME", KeywordCategory::Ddl),
        KeywordInfo::new("ATTACH", KeywordCategory::Ddl),
        KeywordInfo::new("DETACH", KeywordCategory::Ddl),
        KeywordInfo::new("OPTIMIZE", KeywordCategory::Ddl), // ClickHouse-specific
        // Clauses
        KeywordInfo::new("JOIN", KeywordCategory::Clause),
        KeywordInfo::new("INNER", KeywordCategory::Clause),
        KeywordInfo::new("LEFT", KeywordCategory::Clause),
        KeywordInfo::new("RIGHT", KeywordCategory::Clause),
        KeywordInfo::new("OUTER", KeywordCategory::Clause),
        KeywordInfo::new("CROSS", KeywordCategory::Clause),
        KeywordInfo::new("FULL", KeywordCategory::Clause),
        KeywordInfo::new("GLOBAL", KeywordCategory::Clause), // ClickHouse-specific
        KeywordInfo::new("ANY", KeywordCategory::Clause),    // ClickHouse-specific
        KeywordInfo::new("ASOF", KeywordCategory::Clause),   // ClickHouse-specific
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
        KeywordInfo::new("UNION", KeywordCategory::Clause),
        KeywordInfo::new("INTERSECT", KeywordCategory::Clause),
        KeywordInfo::new("EXCEPT", KeywordCategory::Clause),
        KeywordInfo::new("FORMAT", KeywordCategory::Clause), // ClickHouse-specific
        KeywordInfo::new("INTO", KeywordCategory::Clause),
        KeywordInfo::new("OUTFILE", KeywordCategory::Clause), // ClickHouse-specific
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
        KeywordInfo::new("GLOBAL", KeywordCategory::Operator), // ClickHouse GLOBAL IN
        // ClickHouse-specific
        KeywordInfo::with_desc(
            "ENGINE",
            KeywordCategory::DatabaseSpecific,
            "Table engine specification",
        ),
        KeywordInfo::with_desc(
            "MergeTree",
            KeywordCategory::DatabaseSpecific,
            "MergeTree family table engine",
        ),
        KeywordInfo::with_desc(
            "ReplacingMergeTree",
            KeywordCategory::DatabaseSpecific,
            "MergeTree with row deduplication",
        ),
        KeywordInfo::with_desc(
            "SummingMergeTree",
            KeywordCategory::DatabaseSpecific,
            "MergeTree with automatic sum on merge",
        ),
        KeywordInfo::with_desc(
            "AggregatingMergeTree",
            KeywordCategory::DatabaseSpecific,
            "MergeTree with custom aggregation",
        ),
        KeywordInfo::with_desc(
            "CollapsingMergeTree",
            KeywordCategory::DatabaseSpecific,
            "MergeTree with row collapsing",
        ),
        KeywordInfo::with_desc(
            "VersionedCollapsingMergeTree",
            KeywordCategory::DatabaseSpecific,
            "CollapsingMergeTree with versioning",
        ),
        KeywordInfo::with_desc(
            "Distributed",
            KeywordCategory::DatabaseSpecific,
            "Distributed table engine",
        ),
        KeywordInfo::with_desc(
            "MaterializedView",
            KeywordCategory::DatabaseSpecific,
            "Materialized view engine",
        ),
        KeywordInfo::with_desc(
            "Kafka",
            KeywordCategory::DatabaseSpecific,
            "Kafka table engine",
        ),
        KeywordInfo::with_desc(
            "Buffer",
            KeywordCategory::DatabaseSpecific,
            "Buffer table engine",
        ),
        KeywordInfo::with_desc(
            "Memory",
            KeywordCategory::DatabaseSpecific,
            "In-memory table engine",
        ),
        KeywordInfo::with_desc(
            "Log",
            KeywordCategory::DatabaseSpecific,
            "Log family table engine",
        ),
        KeywordInfo::with_desc(
            "SYSTEM",
            KeywordCategory::DatabaseSpecific,
            "System commands",
        ),
        KeywordInfo::with_desc(
            "SETTINGS",
            KeywordCategory::DatabaseSpecific,
            "Query settings",
        ),
        KeywordInfo::with_desc(
            "TTL",
            KeywordCategory::DatabaseSpecific,
            "Time-to-live specification",
        ),
        // DCL
        KeywordInfo::new("GRANT", KeywordCategory::Dcl),
        KeywordInfo::new("REVOKE", KeywordCategory::Dcl),
        KeywordInfo::new("SHOW", KeywordCategory::Dcl),
    ]
}

fn clickhouse_functions() -> Vec<SqlFunctionInfo> {
    vec![
        // Aggregate functions
        SqlFunctionInfo::new("count", FunctionCategory::Aggregate)
            .with_signature("count(expression)"),
        SqlFunctionInfo::new("sum", FunctionCategory::Aggregate).with_signature("sum(expression)"),
        SqlFunctionInfo::new("avg", FunctionCategory::Aggregate).with_signature("avg(expression)"),
        SqlFunctionInfo::new("min", FunctionCategory::Aggregate).with_signature("min(expression)"),
        SqlFunctionInfo::new("max", FunctionCategory::Aggregate).with_signature("max(expression)"),
        SqlFunctionInfo::new("any", FunctionCategory::Aggregate).with_signature("any(expression)"),
        SqlFunctionInfo::new("anyLast", FunctionCategory::Aggregate)
            .with_signature("anyLast(expression)"),
        SqlFunctionInfo::new("argMin", FunctionCategory::Aggregate)
            .with_signature("argMin(arg, val)"),
        SqlFunctionInfo::new("argMax", FunctionCategory::Aggregate)
            .with_signature("argMax(arg, val)"),
        SqlFunctionInfo::new("groupArray", FunctionCategory::Aggregate)
            .with_signature("groupArray(expression)"),
        SqlFunctionInfo::new("groupUniqArray", FunctionCategory::Aggregate)
            .with_signature("groupUniqArray(expression)"),
        SqlFunctionInfo::new("uniq", FunctionCategory::Aggregate)
            .with_signature("uniq(expression)"),
        SqlFunctionInfo::new("uniqExact", FunctionCategory::Aggregate)
            .with_signature("uniqExact(expression)"),
        SqlFunctionInfo::new("uniqCombined", FunctionCategory::Aggregate)
            .with_signature("uniqCombined(expression)"),
        SqlFunctionInfo::new("uniqHLL12", FunctionCategory::Aggregate)
            .with_signature("uniqHLL12(expression)"),
        SqlFunctionInfo::new("quantile", FunctionCategory::Aggregate)
            .with_signature("quantile(level)(expression)"),
        SqlFunctionInfo::new("quantiles", FunctionCategory::Aggregate)
            .with_signature("quantiles(level1, level2, ...)(expression)"),
        SqlFunctionInfo::new("median", FunctionCategory::Aggregate)
            .with_signature("median(expression)"),
        SqlFunctionInfo::new("stddevPop", FunctionCategory::Aggregate)
            .with_signature("stddevPop(expression)"),
        SqlFunctionInfo::new("stddevSamp", FunctionCategory::Aggregate)
            .with_signature("stddevSamp(expression)"),
        SqlFunctionInfo::new("varPop", FunctionCategory::Aggregate)
            .with_signature("varPop(expression)"),
        SqlFunctionInfo::new("varSamp", FunctionCategory::Aggregate)
            .with_signature("varSamp(expression)"),
        SqlFunctionInfo::new("covarPop", FunctionCategory::Aggregate)
            .with_signature("covarPop(x, y)"),
        SqlFunctionInfo::new("covarSamp", FunctionCategory::Aggregate)
            .with_signature("covarSamp(x, y)"),
        SqlFunctionInfo::new("topK", FunctionCategory::Aggregate)
            .with_signature("topK(N)(expression)"),
        SqlFunctionInfo::new("histogram", FunctionCategory::Aggregate)
            .with_signature("histogram(number_of_bins)(expression)"),
        // Window functions
        SqlFunctionInfo::new("row_number", FunctionCategory::Window)
            .with_signature("row_number() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("rank", FunctionCategory::Window)
            .with_signature("rank() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("dense_rank", FunctionCategory::Window)
            .with_signature("dense_rank() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("ntile", FunctionCategory::Window)
            .with_signature("ntile(n) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("lag", FunctionCategory::Window)
            .with_signature("lag(expression, offset, default) OVER (...)"),
        SqlFunctionInfo::new("lead", FunctionCategory::Window)
            .with_signature("lead(expression, offset, default) OVER (...)"),
        SqlFunctionInfo::new("first_value", FunctionCategory::Window)
            .with_signature("first_value(expression) OVER (...)"),
        SqlFunctionInfo::new("last_value", FunctionCategory::Window)
            .with_signature("last_value(expression) OVER (...)"),
        // String functions
        SqlFunctionInfo::new("length", FunctionCategory::String).with_signature("length(string)"),
        SqlFunctionInfo::new("lengthUTF8", FunctionCategory::String)
            .with_signature("lengthUTF8(string)"),
        SqlFunctionInfo::new("lower", FunctionCategory::String).with_signature("lower(string)"),
        SqlFunctionInfo::new("upper", FunctionCategory::String).with_signature("upper(string)"),
        SqlFunctionInfo::new("concat", FunctionCategory::String)
            .with_signature("concat(s1, s2, ...)"),
        SqlFunctionInfo::new("substring", FunctionCategory::String)
            .with_signature("substring(s, offset, length)"),
        SqlFunctionInfo::new("substringUTF8", FunctionCategory::String)
            .with_signature("substringUTF8(s, offset, length)"),
        SqlFunctionInfo::new("trim", FunctionCategory::String).with_signature("trim(string)"),
        SqlFunctionInfo::new("trimLeft", FunctionCategory::String)
            .with_signature("trimLeft(string)"),
        SqlFunctionInfo::new("trimRight", FunctionCategory::String)
            .with_signature("trimRight(string)"),
        SqlFunctionInfo::new("replace", FunctionCategory::String)
            .with_signature("replace(haystack, pattern, replacement)"),
        SqlFunctionInfo::new("replaceAll", FunctionCategory::String)
            .with_signature("replaceAll(haystack, pattern, replacement)"),
        SqlFunctionInfo::new("replaceRegexpAll", FunctionCategory::String)
            .with_signature("replaceRegexpAll(haystack, pattern, replacement)"),
        SqlFunctionInfo::new("splitByChar", FunctionCategory::String)
            .with_signature("splitByChar(separator, s)"),
        SqlFunctionInfo::new("splitByString", FunctionCategory::String)
            .with_signature("splitByString(separator, s)"),
        SqlFunctionInfo::new("format", FunctionCategory::String)
            .with_signature("format(pattern, arg1, arg2, ...)"),
        SqlFunctionInfo::new("reverse", FunctionCategory::String).with_signature("reverse(string)"),
        SqlFunctionInfo::new("position", FunctionCategory::String)
            .with_signature("position(haystack, needle)"),
        SqlFunctionInfo::new("positionUTF8", FunctionCategory::String)
            .with_signature("positionUTF8(haystack, needle)"),
        SqlFunctionInfo::new("match", FunctionCategory::String)
            .with_signature("match(haystack, pattern)"),
        SqlFunctionInfo::new("extract", FunctionCategory::String)
            .with_signature("extract(haystack, pattern)"),
        SqlFunctionInfo::new("extractAll", FunctionCategory::String)
            .with_signature("extractAll(haystack, pattern)"),
        // Numeric functions
        SqlFunctionInfo::new("abs", FunctionCategory::Numeric).with_signature("abs(x)"),
        SqlFunctionInfo::new("ceil", FunctionCategory::Numeric).with_signature("ceil(x)"),
        SqlFunctionInfo::new("floor", FunctionCategory::Numeric).with_signature("floor(x)"),
        SqlFunctionInfo::new("round", FunctionCategory::Numeric).with_signature("round(x, N)"),
        SqlFunctionInfo::new("trunc", FunctionCategory::Numeric).with_signature("trunc(x, N)"),
        SqlFunctionInfo::new("exp", FunctionCategory::Numeric).with_signature("exp(x)"),
        SqlFunctionInfo::new("log", FunctionCategory::Numeric).with_signature("log(x)"),
        SqlFunctionInfo::new("log2", FunctionCategory::Numeric).with_signature("log2(x)"),
        SqlFunctionInfo::new("log10", FunctionCategory::Numeric).with_signature("log10(x)"),
        SqlFunctionInfo::new("sqrt", FunctionCategory::Numeric).with_signature("sqrt(x)"),
        SqlFunctionInfo::new("cbrt", FunctionCategory::Numeric).with_signature("cbrt(x)"),
        SqlFunctionInfo::new("pow", FunctionCategory::Numeric).with_signature("pow(x, y)"),
        SqlFunctionInfo::new("intDiv", FunctionCategory::Numeric).with_signature("intDiv(a, b)"),
        SqlFunctionInfo::new("intDivOrZero", FunctionCategory::Numeric)
            .with_signature("intDivOrZero(a, b)"),
        SqlFunctionInfo::new("modulo", FunctionCategory::Numeric).with_signature("modulo(a, b)"),
        SqlFunctionInfo::new("rand", FunctionCategory::Numeric).with_signature("rand()"),
        SqlFunctionInfo::new("rand64", FunctionCategory::Numeric).with_signature("rand64()"),
        // Date/time functions
        SqlFunctionInfo::new("now", FunctionCategory::DateTime).with_signature("now()"),
        SqlFunctionInfo::new("today", FunctionCategory::DateTime).with_signature("today()"),
        SqlFunctionInfo::new("yesterday", FunctionCategory::DateTime).with_signature("yesterday()"),
        SqlFunctionInfo::new("toYear", FunctionCategory::DateTime).with_signature("toYear(date)"),
        SqlFunctionInfo::new("toMonth", FunctionCategory::DateTime).with_signature("toMonth(date)"),
        SqlFunctionInfo::new("toDayOfMonth", FunctionCategory::DateTime)
            .with_signature("toDayOfMonth(date)"),
        SqlFunctionInfo::new("toDayOfWeek", FunctionCategory::DateTime)
            .with_signature("toDayOfWeek(date)"),
        SqlFunctionInfo::new("toHour", FunctionCategory::DateTime)
            .with_signature("toHour(datetime)"),
        SqlFunctionInfo::new("toMinute", FunctionCategory::DateTime)
            .with_signature("toMinute(datetime)"),
        SqlFunctionInfo::new("toSecond", FunctionCategory::DateTime)
            .with_signature("toSecond(datetime)"),
        SqlFunctionInfo::new("toStartOfYear", FunctionCategory::DateTime)
            .with_signature("toStartOfYear(date)"),
        SqlFunctionInfo::new("toStartOfMonth", FunctionCategory::DateTime)
            .with_signature("toStartOfMonth(date)"),
        SqlFunctionInfo::new("toStartOfWeek", FunctionCategory::DateTime)
            .with_signature("toStartOfWeek(date)"),
        SqlFunctionInfo::new("toStartOfDay", FunctionCategory::DateTime)
            .with_signature("toStartOfDay(datetime)"),
        SqlFunctionInfo::new("toStartOfHour", FunctionCategory::DateTime)
            .with_signature("toStartOfHour(datetime)"),
        SqlFunctionInfo::new("toStartOfMinute", FunctionCategory::DateTime)
            .with_signature("toStartOfMinute(datetime)"),
        SqlFunctionInfo::new("toStartOfInterval", FunctionCategory::DateTime)
            .with_signature("toStartOfInterval(date, INTERVAL x unit)"),
        SqlFunctionInfo::new("dateDiff", FunctionCategory::DateTime)
            .with_signature("dateDiff('unit', start, end)"),
        SqlFunctionInfo::new("dateAdd", FunctionCategory::DateTime)
            .with_signature("dateAdd(unit, value, date)"),
        SqlFunctionInfo::new("dateSub", FunctionCategory::DateTime)
            .with_signature("dateSub(unit, value, date)"),
        SqlFunctionInfo::new("formatDateTime", FunctionCategory::DateTime)
            .with_signature("formatDateTime(datetime, format)"),
        SqlFunctionInfo::new("parseDateTime", FunctionCategory::DateTime)
            .with_signature("parseDateTime(string, format)"),
        SqlFunctionInfo::new("toUnixTimestamp", FunctionCategory::DateTime)
            .with_signature("toUnixTimestamp(datetime)"),
        SqlFunctionInfo::new("fromUnixTimestamp", FunctionCategory::DateTime)
            .with_signature("fromUnixTimestamp(timestamp)"),
        // Type conversion functions
        SqlFunctionInfo::new("toInt8", FunctionCategory::Conversion).with_signature("toInt8(x)"),
        SqlFunctionInfo::new("toInt16", FunctionCategory::Conversion).with_signature("toInt16(x)"),
        SqlFunctionInfo::new("toInt32", FunctionCategory::Conversion).with_signature("toInt32(x)"),
        SqlFunctionInfo::new("toInt64", FunctionCategory::Conversion).with_signature("toInt64(x)"),
        SqlFunctionInfo::new("toUInt8", FunctionCategory::Conversion).with_signature("toUInt8(x)"),
        SqlFunctionInfo::new("toUInt16", FunctionCategory::Conversion)
            .with_signature("toUInt16(x)"),
        SqlFunctionInfo::new("toUInt32", FunctionCategory::Conversion)
            .with_signature("toUInt32(x)"),
        SqlFunctionInfo::new("toUInt64", FunctionCategory::Conversion)
            .with_signature("toUInt64(x)"),
        SqlFunctionInfo::new("toFloat32", FunctionCategory::Conversion)
            .with_signature("toFloat32(x)"),
        SqlFunctionInfo::new("toFloat64", FunctionCategory::Conversion)
            .with_signature("toFloat64(x)"),
        SqlFunctionInfo::new("toDecimal32", FunctionCategory::Conversion)
            .with_signature("toDecimal32(x, scale)"),
        SqlFunctionInfo::new("toDecimal64", FunctionCategory::Conversion)
            .with_signature("toDecimal64(x, scale)"),
        SqlFunctionInfo::new("toDecimal128", FunctionCategory::Conversion)
            .with_signature("toDecimal128(x, scale)"),
        SqlFunctionInfo::new("toString", FunctionCategory::Conversion)
            .with_signature("toString(x)"),
        SqlFunctionInfo::new("toDate", FunctionCategory::Conversion).with_signature("toDate(x)"),
        SqlFunctionInfo::new("toDateTime", FunctionCategory::Conversion)
            .with_signature("toDateTime(x)"),
        SqlFunctionInfo::new("toDateTime64", FunctionCategory::Conversion)
            .with_signature("toDateTime64(x, precision)"),
        SqlFunctionInfo::new("cast", FunctionCategory::Conversion)
            .with_signature("CAST(x AS type)"),
        SqlFunctionInfo::new("accurateCast", FunctionCategory::Conversion)
            .with_signature("accurateCast(x, type)"),
        // Conditional functions
        SqlFunctionInfo::new("if", FunctionCategory::Conditional)
            .with_signature("if(cond, then, else)"),
        SqlFunctionInfo::new("multiIf", FunctionCategory::Conditional)
            .with_signature("multiIf(cond1, then1, cond2, then2, ..., else)"),
        SqlFunctionInfo::new("ifNull", FunctionCategory::Conditional)
            .with_signature("ifNull(x, alt)"),
        SqlFunctionInfo::new("nullIf", FunctionCategory::Conditional)
            .with_signature("nullIf(x, y)"),
        SqlFunctionInfo::new("coalesce", FunctionCategory::Conditional)
            .with_signature("coalesce(x1, x2, ...)"),
        SqlFunctionInfo::new("greatest", FunctionCategory::Conditional)
            .with_signature("greatest(a, b, ...)"),
        SqlFunctionInfo::new("least", FunctionCategory::Conditional)
            .with_signature("least(a, b, ...)"),
        // JSON functions
        SqlFunctionInfo::new("JSONExtract", FunctionCategory::Json)
            .with_signature("JSONExtract(json, path, type)"),
        SqlFunctionInfo::new("JSONExtractString", FunctionCategory::Json)
            .with_signature("JSONExtractString(json, path)"),
        SqlFunctionInfo::new("JSONExtractInt", FunctionCategory::Json)
            .with_signature("JSONExtractInt(json, path)"),
        SqlFunctionInfo::new("JSONExtractFloat", FunctionCategory::Json)
            .with_signature("JSONExtractFloat(json, path)"),
        SqlFunctionInfo::new("JSONExtractBool", FunctionCategory::Json)
            .with_signature("JSONExtractBool(json, path)"),
        SqlFunctionInfo::new("JSONExtractRaw", FunctionCategory::Json)
            .with_signature("JSONExtractRaw(json, path)"),
        SqlFunctionInfo::new("JSONExtractArrayRaw", FunctionCategory::Json)
            .with_signature("JSONExtractArrayRaw(json, path)"),
        SqlFunctionInfo::new("JSONExtractKeysAndValues", FunctionCategory::Json)
            .with_signature("JSONExtractKeysAndValues(json, path, type)"),
        SqlFunctionInfo::new("JSONHas", FunctionCategory::Json)
            .with_signature("JSONHas(json, path)"),
        SqlFunctionInfo::new("JSONLength", FunctionCategory::Json)
            .with_signature("JSONLength(json, path)"),
        SqlFunctionInfo::new("JSONType", FunctionCategory::Json)
            .with_signature("JSONType(json, path)"),
        // Array functions
        SqlFunctionInfo::new("array", FunctionCategory::Other).with_signature("array(x1, x2, ...)"),
        SqlFunctionInfo::new("arrayConcat", FunctionCategory::Other)
            .with_signature("arrayConcat(arr1, arr2, ...)"),
        SqlFunctionInfo::new("arrayElement", FunctionCategory::Other)
            .with_signature("arrayElement(arr, n)"),
        SqlFunctionInfo::new("has", FunctionCategory::Other).with_signature("has(arr, elem)"),
        SqlFunctionInfo::new("hasAll", FunctionCategory::Other).with_signature("hasAll(arr, sub)"),
        SqlFunctionInfo::new("hasAny", FunctionCategory::Other).with_signature("hasAny(arr, sub)"),
        SqlFunctionInfo::new("indexOf", FunctionCategory::Other).with_signature("indexOf(arr, x)"),
        SqlFunctionInfo::new("arrayCount", FunctionCategory::Other)
            .with_signature("arrayCount(func, arr)"),
        SqlFunctionInfo::new("countEqual", FunctionCategory::Other)
            .with_signature("countEqual(arr, x)"),
        SqlFunctionInfo::new("arrayEnumerate", FunctionCategory::Other)
            .with_signature("arrayEnumerate(arr)"),
        SqlFunctionInfo::new("arrayJoin", FunctionCategory::Other).with_signature("arrayJoin(arr)"),
        SqlFunctionInfo::new("arrayMap", FunctionCategory::Other)
            .with_signature("arrayMap(func, arr)"),
        SqlFunctionInfo::new("arrayFilter", FunctionCategory::Other)
            .with_signature("arrayFilter(func, arr)"),
        SqlFunctionInfo::new("arraySort", FunctionCategory::Other).with_signature("arraySort(arr)"),
        SqlFunctionInfo::new("arrayReverse", FunctionCategory::Other)
            .with_signature("arrayReverse(arr)"),
        SqlFunctionInfo::new("arrayUniq", FunctionCategory::Other).with_signature("arrayUniq(arr)"),
        SqlFunctionInfo::new("arrayDistinct", FunctionCategory::Other)
            .with_signature("arrayDistinct(arr)"),
        SqlFunctionInfo::new("arrayReduce", FunctionCategory::Other)
            .with_signature("arrayReduce('agg', arr)"),
        // Hash functions
        SqlFunctionInfo::new("cityHash64", FunctionCategory::Other).with_signature("cityHash64(x)"),
        SqlFunctionInfo::new("sipHash64", FunctionCategory::Other).with_signature("sipHash64(x)"),
        SqlFunctionInfo::new("MD5", FunctionCategory::Other).with_signature("MD5(s)"),
        SqlFunctionInfo::new("SHA1", FunctionCategory::Other).with_signature("SHA1(s)"),
        SqlFunctionInfo::new("SHA256", FunctionCategory::Other).with_signature("SHA256(s)"),
        SqlFunctionInfo::new("xxHash32", FunctionCategory::Other).with_signature("xxHash32(x)"),
        SqlFunctionInfo::new("xxHash64", FunctionCategory::Other).with_signature("xxHash64(x)"),
        // URL functions
        SqlFunctionInfo::new("domain", FunctionCategory::Other).with_signature("domain(url)"),
        SqlFunctionInfo::new("domainWithoutWWW", FunctionCategory::Other)
            .with_signature("domainWithoutWWW(url)"),
        SqlFunctionInfo::new("topLevelDomain", FunctionCategory::Other)
            .with_signature("topLevelDomain(url)"),
        SqlFunctionInfo::new("protocol", FunctionCategory::Other).with_signature("protocol(url)"),
        SqlFunctionInfo::new("path", FunctionCategory::Other).with_signature("path(url)"),
        SqlFunctionInfo::new("queryString", FunctionCategory::Other)
            .with_signature("queryString(url)"),
        SqlFunctionInfo::new("extractURLParameter", FunctionCategory::Other)
            .with_signature("extractURLParameter(url, name)"),
        // Tuple functions
        SqlFunctionInfo::new("tuple", FunctionCategory::Other).with_signature("tuple(x1, x2, ...)"),
        SqlFunctionInfo::new("tupleElement", FunctionCategory::Other)
            .with_signature("tupleElement(tuple, n)"),
        // UUID functions
        SqlFunctionInfo::new("generateUUIDv4", FunctionCategory::Other)
            .with_signature("generateUUIDv4()"),
        SqlFunctionInfo::new("toUUID", FunctionCategory::Other).with_signature("toUUID(string)"),
        // Other useful functions
        SqlFunctionInfo::new("runningDifference", FunctionCategory::Other)
            .with_signature("runningDifference(x)"),
        SqlFunctionInfo::new("neighbor", FunctionCategory::Other)
            .with_signature("neighbor(column, offset, default)"),
        SqlFunctionInfo::new("version", FunctionCategory::Other).with_signature("version()"),
        SqlFunctionInfo::new("hostName", FunctionCategory::Other).with_signature("hostName()"),
        SqlFunctionInfo::new("getMacro", FunctionCategory::Other).with_signature("getMacro(name)"),
    ]
}

fn clickhouse_data_types() -> Vec<DataTypeInfo> {
    vec![
        // Integer types
        DataTypeInfo::new("Int8", DataTypeCategory::Integer),
        DataTypeInfo::new("Int16", DataTypeCategory::Integer),
        DataTypeInfo::new("Int32", DataTypeCategory::Integer),
        DataTypeInfo::new("Int64", DataTypeCategory::Integer),
        DataTypeInfo::new("Int128", DataTypeCategory::Integer),
        DataTypeInfo::new("Int256", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt8", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt16", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt32", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt64", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt128", DataTypeCategory::Integer),
        DataTypeInfo::new("UInt256", DataTypeCategory::Integer),
        // Float types
        DataTypeInfo::new("Float32", DataTypeCategory::Float),
        DataTypeInfo::new("Float64", DataTypeCategory::Float),
        // Decimal types
        DataTypeInfo::new("Decimal", DataTypeCategory::Decimal).with_length(Some(9), Some(76)),
        DataTypeInfo::new("Decimal32", DataTypeCategory::Decimal),
        DataTypeInfo::new("Decimal64", DataTypeCategory::Decimal),
        DataTypeInfo::new("Decimal128", DataTypeCategory::Decimal),
        DataTypeInfo::new("Decimal256", DataTypeCategory::Decimal),
        // Boolean
        DataTypeInfo::new("Bool", DataTypeCategory::Boolean),
        // String types
        DataTypeInfo::new("String", DataTypeCategory::String),
        DataTypeInfo::new("FixedString", DataTypeCategory::String),
        // Date/time types
        DataTypeInfo::new("Date", DataTypeCategory::Date),
        DataTypeInfo::new("Date32", DataTypeCategory::Date),
        DataTypeInfo::new("DateTime", DataTypeCategory::DateTime),
        DataTypeInfo::new("DateTime64", DataTypeCategory::DateTime),
        // UUID
        DataTypeInfo::new("UUID", DataTypeCategory::Uuid),
        // Enum types
        DataTypeInfo::new("Enum8", DataTypeCategory::Other),
        DataTypeInfo::new("Enum16", DataTypeCategory::Other),
        // IP addresses
        DataTypeInfo::new("IPv4", DataTypeCategory::Other),
        DataTypeInfo::new("IPv6", DataTypeCategory::Other),
        // Complex types
        DataTypeInfo::new("Array", DataTypeCategory::Array),
        DataTypeInfo::new("Tuple", DataTypeCategory::Other),
        DataTypeInfo::new("Map", DataTypeCategory::Other),
        DataTypeInfo::new("Nested", DataTypeCategory::Other),
        // JSON type
        DataTypeInfo::new("JSON", DataTypeCategory::Json),
        // Nullable wrapper
        DataTypeInfo::new("Nullable", DataTypeCategory::Other),
        // Low cardinality wrapper
        DataTypeInfo::new("LowCardinality", DataTypeCategory::Other),
        // Geo types
        DataTypeInfo::new("Point", DataTypeCategory::Other),
        DataTypeInfo::new("Ring", DataTypeCategory::Other),
        DataTypeInfo::new("Polygon", DataTypeCategory::Other),
        DataTypeInfo::new("MultiPolygon", DataTypeCategory::Other),
        // Special types
        DataTypeInfo::new("Nothing", DataTypeCategory::Other),
        DataTypeInfo::new("Interval", DataTypeCategory::Other),
        DataTypeInfo::new("SimpleAggregateFunction", DataTypeCategory::Other),
        DataTypeInfo::new("AggregateFunction", DataTypeCategory::Other),
    ]
}
