//! Redis driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, CompletionsConfig, Connection, ConnectionConfig, ConnectionField,
    ConnectionFieldSchema, DatabaseDriver, DiagnosticsConfig, DialectBundle, DialectConfig,
    DialectInfo, DriverCapabilities, QueryResult, Result, Row, SchemaIntrospection,
    StatementResult, Transaction, Value, ZqlzError,
};

/// Embedded dialect configuration files
const CONFIG_TOML: &str = include_str!("../dialect/config.toml");
const COMPLETIONS_TOML: &str = include_str!("../dialect/completions.toml");
const DIAGNOSTICS_TOML: &str = include_str!("../dialect/diagnostics.toml");

/// Cached dialect bundle - loaded once on first access
fn get_dialect_bundle() -> &'static DialectBundle {
    static BUNDLE: OnceLock<DialectBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        let config: DialectConfig =
            toml::from_str(CONFIG_TOML).expect("Failed to parse Redis dialect config.toml");
        let completions: CompletionsConfig = toml::from_str(COMPLETIONS_TOML)
            .expect("Failed to parse Redis dialect completions.toml");
        let diagnostics: DiagnosticsConfig = toml::from_str(DIAGNOSTICS_TOML)
            .expect("Failed to parse Redis dialect diagnostics.toml");

        DialectBundle::new(config, completions).with_diagnostics(diagnostics)
    })
}

/// Global Tokio runtime for Redis operations
/// Redis requires a Tokio runtime for DNS resolution and networking
fn get_redis_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-redis-runtime")
            .build()
            .expect("Failed to create Tokio runtime for Redis driver")
    })
}

/// Redis database driver
///
/// Redis is an in-memory key-value store that can be used as a database,
/// cache, and message broker. This driver provides basic connectivity
/// and command execution capabilities.
pub struct RedisDriver;

impl RedisDriver {
    /// Create a new Redis driver instance
    pub fn new() -> Self {
        tracing::debug!("Redis driver initialized");
        Self
    }
}

impl Default for RedisDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for RedisDriver {
    fn id(&self) -> &'static str {
        "redis"
    }

    fn name(&self) -> &'static str {
        "redis"
    }

    fn display_name(&self) -> &'static str {
        "Redis"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn default_port(&self) -> Option<u16> {
        Some(6379)
    }

    fn icon_name(&self) -> &'static str {
        "redis"
    }

    fn dialect_info(&self) -> DialectInfo {
        // Convert from the declarative DialectBundle to legacy DialectInfo
        get_dialect_bundle().into()
    }

    /// Get the dialect bundle with full configuration
    fn dialect_bundle(&self) -> Option<&'static DialectBundle> {
        Some(get_dialect_bundle())
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,         // MULTI/EXEC
            supports_savepoints: false,          // No savepoints
            supports_prepared_statements: false, // No prepared statements
            supports_multiple_statements: false, // Commands are individual
            supports_returning: false,           // No RETURNING clause
            supports_upsert: true,               // SET with NX/XX
            supports_window_functions: false,    // Not applicable
            supports_cte: false,                 // Not applicable
            supports_json: true,                 // RedisJSON module
            supports_full_text_search: true,     // RediSearch module
            supports_stored_procedures: false,   // Lua scripts instead
            supports_schemas: false,             // No schema concept
            supports_multiple_databases: true,   // SELECT 0-15
            supports_streaming: true,            // Pub/Sub
            supports_cancellation: false,        // No query cancellation
            supports_explain: false,             // No EXPLAIN
            supports_foreign_keys: false,        // No foreign keys
            supports_views: false,               // No views
            supports_triggers: false,            // No triggers
            supports_ssl: true,                  // TLS supported
            max_identifier_length: None,         // Keys can be any length
            max_parameters: None,                // No parameter concept
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to Redis");

        let connection_string = self.build_connection_string(config);
        let config_clone = config.clone();

        // Redis requires a Tokio runtime for DNS resolution and networking
        // We spawn the connection on our dedicated runtime and await the result
        let runtime = get_redis_runtime();

        let mut connection = runtime
            .spawn(async move {
                let client = redis::Client::open(connection_string.as_str()).map_err(|e| {
                    ZqlzError::Driver(format!("Failed to create Redis client: {}", e))
                })?;

                client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|e| ZqlzError::Driver(format!("Failed to connect to Redis: {}", e)))
            })
            .await
            .map_err(|e| ZqlzError::Driver(format!("Redis connection task failed: {}", e)))??;

        // Verify connection actually works by sending PING
        // This catches authentication errors that wouldn't surface until first command
        let ping_result: redis::RedisResult<String> =
            redis::cmd("PING").query_async(&mut connection).await;

        match ping_result {
            Ok(response) => {
                if response != "PONG" {
                    tracing::warn!("Unexpected PING response: {}", response);
                }
                tracing::debug!("Redis connection verified with PING");
            }
            Err(e) => {
                let error_msg = e.to_string();
                // Check for authentication errors
                if error_msg.contains("NOAUTH") || error_msg.contains("Authentication") {
                    return Err(ZqlzError::Driver(format!(
                        "Redis authentication required. Please provide a password in connection settings."
                    )));
                }
                return Err(ZqlzError::Driver(format!(
                    "Redis connection verification failed: {}",
                    e
                )));
            }
        }

        Ok(Arc::new(RedisConnection::new(connection, config_clone)))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing Redis connection");
        let conn = self.connect(config).await?;
        // PING command returns PONG
        conn.execute("PING", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = config
            .get_string("port")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or_else(|| if config.port > 0 { config.port } else { 6379 });
        let database: u16 = config
            .get_string("database")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        // Use get_string to check both params and direct fields
        let username = config.get_string("username").filter(|s| !s.is_empty());
        let password = config.get_string("password").filter(|s| !s.is_empty());
        let use_tls = config
            .get_string("ssl")
            .or_else(|| config.get_string("tls"))
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);

        let scheme = if use_tls { "rediss" } else { "redis" };

        tracing::debug!(
            host = %host,
            port = port,
            database = database,
            has_username = username.is_some(),
            has_password = password.is_some(),
            use_tls = use_tls,
            "building Redis connection string"
        );

        match (username, password) {
            (Some(user), Some(pass)) => {
                format!(
                    "{}://{}:{}@{}:{}/{}",
                    scheme, user, pass, host, port, database
                )
            }
            (None, Some(pass)) => {
                format!("{}://:{}@{}:{}/{}", scheme, pass, host, port, database)
            }
            _ => {
                format!("{}://{}:{}/{}", scheme, host, port, database)
            }
        }
    }

    fn connection_string_help(&self) -> &'static str {
        "Redis URL format: redis://[user:password@]host[:port][/database]\n\
         Examples:\n\
         - redis://localhost:6379/0\n\
         - redis://:password@localhost:6379/0\n\
         - rediss://localhost:6379/0 (TLS)"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("Redis Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("6379")
                    .default_value("6379")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::password("password", "Password")
                    .help_text("Leave empty if no authentication required"),
                ConnectionField::number("database", "Database")
                    .placeholder("0")
                    .default_value("0")
                    .help_text("Database index (0-15)")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::boolean("ssl", "Use TLS/SSL")
                    .help_text("Enable secure connection")
                    .width(0.5)
                    .row_group(2),
            ],
        }
    }
}

/// Redis connection wrapper implementing the Connection trait
pub struct RedisConnection {
    connection: Mutex<redis::aio::MultiplexedConnection>,
    config: ConnectionConfig,
    closed: AtomicBool,
}

impl RedisConnection {
    /// Create a new Redis connection wrapper
    pub fn new(connection: redis::aio::MultiplexedConnection, config: ConnectionConfig) -> Self {
        Self {
            connection: Mutex::new(connection),
            config,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the current database number (0-15)
    pub fn database(&self) -> u16 {
        self.config
            .get_string("database")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for RedisConnection {
    fn driver_name(&self) -> &str {
        "redis"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("redis")
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut conn = self.connection.lock().await;

        // Parse the command - first word is the command, rest are args
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.is_empty() {
            return Err(ZqlzError::Driver("Empty command".to_string()));
        }

        let command = parts[0].to_uppercase();
        let mut cmd = redis::cmd(&command);

        // Add command arguments from SQL string
        for part in parts.iter().skip(1) {
            cmd.arg(*part);
        }

        // Add any additional parameters
        for param in params {
            match param {
                Value::String(s) => cmd.arg(s.as_str()),
                Value::Int64(n) => cmd.arg(*n),
                Value::Float64(f) => cmd.arg(*f),
                Value::Bool(b) => cmd.arg(if *b { "1" } else { "0" }),
                Value::Bytes(b) => cmd.arg(b.as_slice()),
                Value::Null => cmd.arg(""),
                _ => cmd.arg(param.to_string().as_str()),
            };
        }

        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut *conn).await;

        let execution_time = start.elapsed();

        match result {
            Ok(value) => {
                tracing::debug!(
                    command = %command,
                    duration_ms = execution_time.as_millis() as u64,
                    "execute completed"
                );

                // Determine affected count based on return value
                let affected = match &value {
                    redis::Value::Int(n) => *n as u64,
                    redis::Value::Okay => 1,
                    redis::Value::Nil => 0,
                    _ => 1,
                };

                Ok(StatementResult {
                    is_query: false,
                    result: None,
                    affected_rows: affected,
                    error: None,
                })
            }
            Err(e) => Err(ZqlzError::Driver(format!("Redis command failed: {}", e))),
        }
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut conn = self.connection.lock().await;

        // Parse the command
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.is_empty() {
            return Err(ZqlzError::Driver("Empty command".to_string()));
        }

        let command = parts[0].to_uppercase();
        let mut cmd = redis::cmd(&command);

        for part in parts.iter().skip(1) {
            cmd.arg(*part);
        }

        for param in params {
            match param {
                Value::String(s) => cmd.arg(s.as_str()),
                Value::Int64(n) => cmd.arg(*n),
                Value::Float64(f) => cmd.arg(*f),
                Value::Bool(b) => cmd.arg(if *b { "1" } else { "0" }),
                Value::Bytes(b) => cmd.arg(b.as_slice()),
                Value::Null => cmd.arg(""),
                _ => cmd.arg(param.to_string().as_str()),
            };
        }

        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut *conn).await;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(value) => {
                let (columns, rows) = redis_value_to_rows(&value);

                tracing::debug!(
                    command = %command,
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
            Err(e) => Err(ZqlzError::Driver(format!("Redis command failed: {}", e))),
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.ensure_not_closed()?;
        Err(ZqlzError::NotImplemented(
            "Redis transactions (MULTI/EXEC) will be implemented in a future update".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        tracing::debug!("Redis connection closed");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }
}

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnection")
            .field("database", &self.database())
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}

/// Convert Redis value to query result rows
fn redis_value_to_rows(value: &redis::Value) -> (Vec<ColumnMeta>, Vec<Row>) {
        let columns = vec![
            ColumnMeta {
                name: "key".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                ordinal: 0,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            },
            ColumnMeta {
                name: "value".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                ordinal: 1,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            },
        ];

    let column_names = vec!["key".to_string(), "value".to_string()];

    let rows = match value {
        redis::Value::Nil => vec![],
        redis::Value::Int(n) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Int64(*n)],
            )]
        }
        redis::Value::BulkString(data) => {
            let s = String::from_utf8_lossy(data).to_string();
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(s)],
            )]
        }
        redis::Value::Array(arr) => {
            // Flatten nested arrays for commands like SCAN that return [cursor, [keys...]]
            let mut rows = Vec::new();
            for (i, v) in arr.iter().enumerate() {
                match v {
                    // If the element is itself an array, flatten it into individual rows
                    redis::Value::Array(inner_arr) => {
                        for inner_v in inner_arr {
                            let val = redis_value_to_zqlz_value(inner_v);
                            rows.push(Row::new(column_names.clone(), vec![Value::Null, val]));
                        }
                    }
                    // Otherwise, treat as a normal element with index as key
                    _ => {
                        let key = Value::String(i.to_string());
                        let val = redis_value_to_zqlz_value(v);
                        rows.push(Row::new(column_names.clone(), vec![key, val]));
                    }
                }
            }
            rows
        }
        redis::Value::Okay => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String("OK".to_string())],
            )]
        }
        redis::Value::SimpleString(s) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(s.clone())],
            )]
        }
        redis::Value::Map(map) => map
            .iter()
            .map(|(k, v)| {
                let key = redis_value_to_zqlz_value(k);
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
        redis::Value::Set(set) => set
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let key = Value::String(i.to_string());
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
        redis::Value::Double(d) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Float64(*d)],
            )]
        }
        redis::Value::Boolean(b) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Bool(*b)],
            )]
        }
        redis::Value::BigNumber(bn) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(format!("{:?}", bn))],
            )]
        }
        redis::Value::VerbatimString { format: _, text } => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(text.clone())],
            )]
        }
        redis::Value::ServerError(err) => {
            vec![Row::new(
                column_names.clone(),
                vec![
                    Value::String("error".to_string()),
                    Value::String(format!("{:?}", err)),
                ],
            )]
        }
        redis::Value::Attribute {
            data,
            attributes: _,
        } => redis_value_to_rows(data).1,
        redis::Value::Push { kind: _, data } => data
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let key = Value::String(i.to_string());
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
    };

    (columns, rows)
}

/// Convert a single Redis value to ZQLZ Value
fn redis_value_to_zqlz_value(value: &redis::Value) -> Value {
    match value {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(n) => Value::Int64(*n),
        redis::Value::BulkString(data) => Value::String(String::from_utf8_lossy(data).to_string()),
        redis::Value::Okay => Value::String("OK".to_string()),
        redis::Value::SimpleString(s) => Value::String(s.clone()),
        redis::Value::Double(d) => Value::Float64(*d),
        redis::Value::Boolean(b) => Value::Bool(*b),
        redis::Value::Array(arr) => {
            let json_arr: Vec<serde_json::Value> = arr
                .iter()
                .map(|v| match redis_value_to_zqlz_value(v) {
                    Value::String(s) => serde_json::Value::String(s),
                    Value::Int64(n) => serde_json::Value::Number(n.into()),
                    Value::Float64(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Value::Bool(b) => serde_json::Value::Bool(b),
                    Value::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                })
                .collect();
            Value::String(serde_json::to_string(&json_arr).unwrap_or_default())
        }
        redis::Value::Map(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                let key = match redis_value_to_zqlz_value(k) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                let val = match redis_value_to_zqlz_value(v) {
                    Value::String(s) => serde_json::Value::String(s),
                    Value::Int64(n) => serde_json::Value::Number(n.into()),
                    Value::Float64(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Value::Bool(b) => serde_json::Value::Bool(b),
                    Value::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                json_map.insert(key, val);
            }
            Value::String(serde_json::to_string(&json_map).unwrap_or_default())
        }
        redis::Value::BigNumber(bn) => Value::String(format!("{:?}", bn)),
        redis::Value::VerbatimString { format: _, text } => Value::String(text.clone()),
        redis::Value::ServerError(err) => Value::String(format!("ERROR: {:?}", err)),
        _ => Value::Null,
    }
}
