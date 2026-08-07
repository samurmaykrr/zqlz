//! Redis driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, CompletionsConfig, Connection, ConnectionConfig, ConnectionField,
    ConnectionFieldOption, ConnectionFieldSchema, ConnectionScope, ConnectionSecurity,
    DatabaseDriver, DiagnosticsConfig, DialectBundle, DialectConfig, DialectInfo,
    DriverCapabilities, DriverCategory, DropTableOptions, DropTriggerOptions, DropViewOptions,
    ExplainParserKind, KeyValueCellUpdateRequest, KeyValueDatabaseInfo, KeyValueDeleteOutcome,
    KeyValueDeleteRequest, KeyValueEntry, KeyValueKeySummary, KeyValueKind, KeyValueSaveOutcome,
    KeyValueSaveRequest, KeyValueScanRequest, KeyValueScanResult, KeyValueStore, QueryResult,
    ResolvedConnectionScope, Result, Row, SchemaIntrospection, SqlObjectName, StatementResult,
    Transaction, Value, ZqlzError,
    command::{CommandTokenizer, parse_commands},
    parse_redis_database_index,
    security::{SshAuthMethod, SshTunnelConfig, TlsConfig, TlsMode},
};

use crate::RedisSshTunnel;

/// Embedded dialect configuration files
const CONFIG_TOML: &str = include_str!("../dialect/config.toml");
const COMPLETIONS_TOML: &str = include_str!("../dialect/completions.toml");
const DIAGNOSTICS_TOML: &str = include_str!("../dialect/diagnostics.toml");
const HYPERLOGLOG_MAGIC: &[u8; 4] = b"HYLL";

type StreamEntryFields = Vec<(String, String)>;
type StreamEntries = Vec<(String, StreamEntryFields)>;

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
        env!("CARGO_PKG_VERSION")
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
            supports_transactions: false, // Manual MULTI/EXEC works, trait transaction does not.
            supports_savepoints: false,   // No savepoints
            supports_prepared_statements: false, // No prepared statements
            supports_multiple_statements: false, // Commands are individual
            supports_returning: false,    // No RETURNING clause
            supports_upsert: true,        // SET with NX/XX
            supports_window_functions: false, // Not applicable
            supports_cte: false,          // Not applicable
            supports_json: false,         // RedisJSON is optional module-gated behavior
            supports_full_text_search: false, // RediSearch is optional module-gated behavior
            supports_stored_procedures: false, // Lua scripts instead
            supports_schemas: false,      // No schema concept
            supports_multiple_databases: true, // SELECT 0-15
            supports_streaming: false,    // No app-level Pub/Sub streaming surface yet
            supports_cancellation: false, // No query cancellation
            supports_explain: false,      // No EXPLAIN
            supports_foreign_keys: false, // No foreign keys
            supports_views: false,        // No views
            supports_triggers: false,     // No triggers
            supports_ssl: true,           // TLS supported
            max_identifier_length: None,  // Keys can be any length
            max_parameters: None,         // No parameter concept
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to Redis");

        validate_connection_config(config)?;
        let tls_config = build_tls_config(config)?;
        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = redis_port(config);
        let ssh_tunnel = build_ssh_tunnel(config, &host, port)?;
        let (connect_host, connect_port) = if let Some(tunnel) = ssh_tunnel.as_ref() {
            ("127.0.0.1".to_string(), tunnel.local_port())
        } else {
            (host.clone(), port)
        };
        let connection_string =
            build_redis_connection_string(config, &tls_config, &connect_host, connect_port);
        let mut config_clone = config.clone();
        config_clone = config_clone.with_param("database", redis_database(config).to_string());

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
                    return Err(ZqlzError::Driver(
                        "Redis authentication required. Please provide a password in connection settings.".to_string(),
                    ));
                }
                return Err(ZqlzError::Driver(format!(
                    "Redis connection verification failed: {}",
                    e
                )));
            }
        }

        Ok(Arc::new(RedisConnection::new(
            connection,
            config_clone,
            Some(tls_config),
            ssh_tunnel,
        )))
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
        let tls_config = build_tls_config(config).unwrap_or_else(|_| TlsConfig::disabled());
        let host = config
            .get_string("host")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        build_redis_connection_string(config, &tls_config, &host, redis_port(config))
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
                ConnectionField::text("username", "Username")
                    .help_text("Redis ACL username")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .help_text("Leave empty if no authentication required")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::number("database", "Database")
                    .placeholder("0")
                    .default_value("0")
                    .help_text("Database index (0-15)")
                    .width(0.5)
                    .row_group(3),
                ConnectionField::select(
                    "ssl_mode",
                    "SSL Mode",
                    vec![
                        ConnectionFieldOption::new("DISABLED", "Disabled"),
                        ConnectionFieldOption::new("REQUIRED", "Required"),
                        ConnectionFieldOption::new("VERIFY_CA", "Verify CA"),
                        ConnectionFieldOption::new("VERIFY_FULL", "Verify Full"),
                    ],
                )
                .default_value("DISABLED")
                .help_text("Redis TLS mode; legacy ssl/tls booleans still enable Required mode")
                .tab("ssl"),
                ConnectionField::file_path("ssl_ca_cert", "CA Certificate")
                    .placeholder("/path/to/ca-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .help_text("Validated for verify modes; Redis native TLS uses system roots")
                    .tab("ssl"),
                ConnectionField::boolean("ssh_enabled", "Use SSH Tunnel")
                    .default_value("false")
                    .tab("ssh"),
                ConnectionField::text("ssh_host", "SSH Host")
                    .placeholder("bastion.example.com")
                    .width(0.7)
                    .row_group(20)
                    .tab("ssh"),
                ConnectionField::number("ssh_port", "SSH Port")
                    .placeholder("22")
                    .default_value("22")
                    .width(0.3)
                    .row_group(20)
                    .tab("ssh"),
                ConnectionField::text("ssh_username", "SSH Username")
                    .width(0.5)
                    .row_group(21)
                    .tab("ssh"),
                ConnectionField::select(
                    "ssh_auth_method",
                    "SSH Auth Method",
                    vec![
                        ConnectionFieldOption::new("password", "Password"),
                        ConnectionFieldOption::new("private_key", "Private Key"),
                        ConnectionFieldOption::new("agent", "Agent"),
                    ],
                )
                .default_value("password")
                .width(0.5)
                .row_group(21)
                .tab("ssh"),
                ConnectionField::password("ssh_password", "SSH Password").tab("ssh"),
                ConnectionField::file_path("ssh_private_key", "SSH Private Key")
                    .placeholder("~/.ssh/id_rsa")
                    .with_extensions(vec!["pem", "key"])
                    .width(0.5)
                    .row_group(22)
                    .tab("ssh"),
                ConnectionField::password("ssh_private_key_passphrase", "SSH Key Passphrase")
                    .width(0.5)
                    .row_group(22)
                    .tab("ssh"),
                ConnectionField::number("ssh_timeout_seconds", "SSH Timeout (seconds)")
                    .default_value("30")
                    .width(0.5)
                    .row_group(23)
                    .tab("ssh"),
                ConnectionField::number("ssh_keepalive_seconds", "SSH Keepalive (seconds)")
                    .default_value("0")
                    .width(0.5)
                    .row_group(23)
                    .tab("ssh"),
            ],
        }
    }
}

fn validate_connection_config(config: &ConnectionConfig) -> Result<()> {
    let port = config
        .get_string("port")
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u16>()
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| {
                    ZqlzError::Configuration("Redis port must be between 1 and 65535".to_string())
                })
        })
        .transpose()?
        .unwrap_or(if config.port > 0 { config.port } else { 6379 });
    if port == 0 {
        return Err(ZqlzError::Configuration(
            "Redis port must be between 1 and 65535".to_string(),
        ));
    }

    if let Some(database) = config
        .get_string("database")
        .filter(|value| !value.is_empty())
    {
        database.parse::<u16>().map_err(|_| {
            ZqlzError::Configuration("Redis database must be a whole number".to_string())
        })?;
    }

    Ok(())
}

fn redis_port(config: &ConnectionConfig) -> u16 {
    config
        .get_string("port")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(if config.port > 0 { config.port } else { 6379 })
}

fn redis_database(config: &ConnectionConfig) -> u16 {
    config
        .get_string("database")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

fn build_redis_connection_string(
    config: &ConnectionConfig,
    tls_config: &TlsConfig,
    host: &str,
    port: u16,
) -> String {
    let database = redis_database(config);
    let username = config.get_string("username").filter(|s| !s.is_empty());
    let password = config.get_string("password").filter(|s| !s.is_empty());
    let use_tls = tls_config.mode != TlsMode::Disable;
    let scheme = if use_tls { "rediss" } else { "redis" };
    let mut url = String::new();
    url.push_str(scheme);
    url.push_str("://");

    match (username, password) {
        (Some(user), Some(pass)) => {
            url.push_str(&encode_url_component(&user));
            url.push(':');
            url.push_str(&encode_url_component(&pass));
            url.push('@');
        }
        (None, Some(pass)) => {
            url.push(':');
            url.push_str(&encode_url_component(&pass));
            url.push('@');
        }
        _ => {}
    }

    url.push_str(host);
    url.push(':');
    url.push_str(&port.to_string());
    url.push('/');
    url.push_str(&database.to_string());

    if use_tls && !tls_config.verify_server {
        url.push_str("#insecure");
    }

    url
}

fn encode_url_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn build_tls_config(config: &ConnectionConfig) -> Result<TlsConfig> {
    let mode = match config
        .get_string("ssl_mode")
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
    {
        Some(value) => match value.as_str() {
            "DISABLED" | "DISABLE" => TlsMode::Disable,
            "PREFERRED" | "PREFER" | "ALLOW" => TlsMode::Prefer,
            "REQUIRED" | "REQUIRE" => TlsMode::Require,
            "VERIFY_CA" | "VERIFY-CA" => TlsMode::VerifyCa,
            "VERIFY_FULL" | "VERIFY-FULL" | "VERIFY_IDENTITY" => TlsMode::VerifyFull,
            _ => {
                return Err(ZqlzError::Configuration(format!(
                    "Invalid Redis SSL mode: {}",
                    value
                )));
            }
        },
        None if parse_bool(config, "ssl", false) || parse_bool(config, "tls", false) => {
            TlsMode::Require
        }
        None => TlsMode::Disable,
    };

    let mut tls_config = TlsConfig::new(mode);
    if matches!(mode, TlsMode::Require | TlsMode::Prefer) {
        tls_config = tls_config.verify_server(false);
    }
    if let Some(ca_cert) = config
        .get_string("ssl_ca_cert")
        .filter(|value| !value.is_empty())
    {
        tls_config = tls_config.ca_cert(ca_cert);
    }
    tls_config.validate()?;
    Ok(tls_config)
}

fn build_ssh_tunnel(
    config: &ConnectionConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<Option<RedisSshTunnel>> {
    if !parse_bool(config, "ssh_enabled", false) {
        return Ok(None);
    }

    let ssh_config = build_ssh_config(config)?;
    RedisSshTunnel::new(&ssh_config, remote_host, remote_port)
        .map(Some)
        .map_err(|error| {
            ZqlzError::Connection(format!("Failed to establish SSH tunnel: {}", error))
        })
}

fn build_ssh_config(config: &ConnectionConfig) -> Result<SshTunnelConfig> {
    let host = required_param(config, "ssh_host")?;
    let username = required_param(config, "ssh_username")?;
    let port = parse_optional_u16(config, "ssh_port")?.unwrap_or(22);
    let timeout_seconds = parse_optional_u64(config, "ssh_timeout_seconds")?.unwrap_or(30) as u32;
    let keepalive_seconds =
        parse_optional_u64(config, "ssh_keepalive_seconds")?.unwrap_or(0) as u32;

    let auth = match config
        .get_string("ssh_auth_method")
        .unwrap_or_else(|| "password".to_string())
        .as_str()
    {
        "password" => SshAuthMethod::password(required_param(config, "ssh_password")?),
        "private_key" => {
            let key = required_param(config, "ssh_private_key")?;
            let passphrase = config
                .get_string("ssh_private_key_passphrase")
                .filter(|value| !value.is_empty());
            SshAuthMethod::PrivateKey {
                path: key.into(),
                passphrase,
            }
        }
        "agent" => SshAuthMethod::agent(),
        value => {
            return Err(ZqlzError::Configuration(format!(
                "Invalid SSH auth method: {}",
                value
            )));
        }
    };

    let ssh_config = SshTunnelConfig {
        host,
        port,
        username,
        auth,
        timeout_seconds,
        keepalive_seconds,
    };
    ssh_config.validate()?;
    Ok(ssh_config)
}

fn required_param(config: &ConnectionConfig, key: &str) -> Result<String> {
    config
        .get_string(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ZqlzError::Configuration(format!("{} is required", key)))
}

fn parse_bool(config: &ConnectionConfig, key: &str, default: bool) -> bool {
    config
        .get_string(key)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn parse_optional_u16(config: &ConnectionConfig, key: &str) -> Result<Option<u16>> {
    config
        .get_string(key)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u16>()
                .map_err(|_| ZqlzError::Configuration(format!("{} must be a whole number", key)))
        })
        .transpose()
}

fn parse_optional_u64(config: &ConnectionConfig, key: &str) -> Result<Option<u64>> {
    config
        .get_string(key)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ZqlzError::Configuration(format!("{} must be a whole number", key)))
        })
        .transpose()
}

/// Redis connection wrapper implementing the Connection trait
/// Liveness command issued by connection heartbeats.
const REDIS_PING_COMMAND: &str = "PING";

pub struct RedisConnection {
    connection: Mutex<redis::aio::MultiplexedConnection>,
    config: ConnectionConfig,
    tls_config: Option<TlsConfig>,
    ssh_tunnel: Option<RedisSshTunnel>,
    closed: AtomicBool,
}

impl RedisConnection {
    /// Create a new Redis connection wrapper
    pub fn new(
        connection: redis::aio::MultiplexedConnection,
        config: ConnectionConfig,
        tls_config: Option<TlsConfig>,
        ssh_tunnel: Option<RedisSshTunnel>,
    ) -> Self {
        Self {
            connection: Mutex::new(connection),
            config,
            tls_config,
            ssh_tunnel,
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

    async fn run_redis_command(
        &self,
        command: &str,
        configure: impl FnOnce(&mut redis::Cmd),
    ) -> Result<redis::Value> {
        self.ensure_not_closed()?;
        let mut connection = self.connection.lock().await;
        let mut command_builder = redis::cmd(command);
        configure(&mut command_builder);
        command_builder
            .query_async(&mut *connection)
            .await
            .map_err(|error| ZqlzError::Driver(format!("Redis command failed: {}", error)))
    }

    async fn run_redis_int_command(
        &self,
        command: &str,
        configure: impl FnOnce(&mut redis::Cmd),
    ) -> Result<i64> {
        let value = self.run_redis_command(command, configure).await?;
        match value {
            redis::Value::Int(value) => Ok(value),
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(&bytes)
                .parse::<i64>()
                .map_err(|error| ZqlzError::Driver(format!("Invalid Redis integer: {}", error))),
            redis::Value::SimpleString(value) => value
                .parse::<i64>()
                .map_err(|error| ZqlzError::Driver(format!("Invalid Redis integer: {}", error))),
            redis::Value::Nil => Ok(0),
            other => Err(ZqlzError::Driver(format!(
                "Unexpected Redis integer response: {:?}",
                other
            ))),
        }
    }

    fn key_value_kind_from_type(value_type: &str) -> KeyValueKind {
        match value_type.to_ascii_lowercase().as_str() {
            "string" => KeyValueKind::String,
            "list" => KeyValueKind::List,
            "set" => KeyValueKind::Set,
            "zset" => KeyValueKind::ZSet,
            "hash" => KeyValueKind::Hash,
            "stream" => KeyValueKind::Stream,
            "json" | "rejson-rl" => KeyValueKind::Json,
            _ => KeyValueKind::None,
        }
    }

    fn value_to_string(value: &redis::Value) -> String {
        match value {
            redis::Value::Nil => String::new(),
            redis::Value::Int(value) => value.to_string(),
            redis::Value::BulkString(bytes) => bytes_to_display_string(bytes),
            redis::Value::SimpleString(value) => value.clone(),
            redis::Value::Okay => "OK".to_string(),
            redis::Value::Double(value) => value.to_string(),
            redis::Value::Boolean(value) => value.to_string(),
            redis::Value::Array(values) => values
                .iter()
                .map(Self::value_to_string)
                .collect::<Vec<_>>()
                .join(", "),
            other => format!("{:?}", other),
        }
    }

    fn string_column(name: &str, ordinal: usize) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        }
    }

    fn rows_from_values(values: Vec<Value>, key_column_name: &str) -> QueryResult {
        let columns = vec![
            Self::string_column(key_column_name, 0),
            Self::string_column("value", 1),
        ];
        let column_names = vec![key_column_name.to_string(), "value".to_string()];
        let rows = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                Row::new(
                    column_names.clone(),
                    vec![Value::String(index.to_string()), value],
                )
            })
            .collect::<Vec<_>>();
        QueryResult {
            id: Uuid::new_v4(),
            columns,
            total_rows: Some(rows.len() as u64),
            rows,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    }

    fn parse_collection_items(value: &str) -> Vec<String> {
        serde_json::from_str::<Vec<serde_json::Value>>(value)
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(value) => value,
                        other => other.to_string(),
                    })
                    .collect()
            })
            .unwrap_or_else(|_| {
                value
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
    }

    fn parse_hash_fields(value: &str) -> Result<Vec<(String, String)>> {
        let json = serde_json::from_str::<serde_json::Value>(value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid hash payload JSON: {}", error)))?;
        let object = json
            .as_object()
            .ok_or_else(|| ZqlzError::Driver("Hash payload must be a JSON object".to_string()))?;
        Ok(object
            .iter()
            .map(|(key, value)| {
                let value = match value {
                    serde_json::Value::String(value) => value.clone(),
                    other => other.to_string(),
                };
                (key.clone(), value)
            })
            .collect())
    }

    fn parse_zset_items(value: &str) -> Result<Vec<(f64, String)>> {
        let json = serde_json::from_str::<serde_json::Value>(value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid zset payload JSON: {}", error)))?;

        if let Some(object) = json.as_object() {
            return object
                .iter()
                .map(|(member, score)| {
                    let score = score.as_f64().ok_or_else(|| {
                        ZqlzError::Driver(format!("ZSet member '{}' has invalid score", member))
                    })?;
                    Ok((score, member.clone()))
                })
                .collect();
        }

        let array = json.as_array().ok_or_else(|| {
            ZqlzError::Driver("ZSet payload must be a JSON object or array".to_string())
        })?;

        if array.iter().all(|item| item.as_object().is_some()) {
            return array
                .iter()
                .map(|item| {
                    let object = item.as_object().ok_or_else(|| {
                        ZqlzError::Driver("ZSet members must be JSON objects".to_string())
                    })?;
                    let score = object
                        .get("score")
                        .and_then(|value| value.as_f64())
                        .ok_or_else(|| {
                            ZqlzError::Driver("ZSet member missing score".to_string())
                        })?;
                    let member = object
                        .get("member")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            ZqlzError::Driver("ZSet member missing member".to_string())
                        })?;
                    Ok((score, member.to_string()))
                })
                .collect();
        }

        array
            .chunks(2)
            .map(|chunk| {
                if chunk.len() != 2 {
                    return Err(ZqlzError::Driver(
                        "ZSet array payload must contain score/member pairs".to_string(),
                    ));
                }
                let score = chunk[0].as_f64().ok_or_else(|| {
                    ZqlzError::Driver("ZSet array pair missing score".to_string())
                })?;
                let member = chunk[1].as_str().ok_or_else(|| {
                    ZqlzError::Driver("ZSet array pair missing member".to_string())
                })?;
                Ok((score, member.to_string()))
            })
            .collect()
    }

    fn parse_stream_entries(value: &str) -> Result<StreamEntries> {
        parse_stream_entries(value)
    }

    fn ensure_key_value_database(&self, database_index: u16) -> Result<()> {
        let current_database = self.database();
        if database_index != current_database {
            return Err(ZqlzError::Driver(format!(
                "Redis connection is scoped to database {}, but request targeted database {}",
                current_database, database_index
            )));
        }
        Ok(())
    }

    async fn keyspace_database_counts(&self) -> Result<HashMap<u16, i64>> {
        let value = self
            .run_redis_command("INFO", |command| {
                command.arg("keyspace");
            })
            .await?;
        let text = Self::value_to_string(&value);
        let mut counts = HashMap::new();

        for line in text.lines() {
            let Some((database_name, stats)) = line.split_once(':') else {
                continue;
            };
            let Some(index) = parse_redis_database_index(database_name) else {
                continue;
            };
            let key_count = stats
                .split(',')
                .find_map(|part| {
                    let (key, value) = part.split_once('=')?;
                    (key == "keys").then(|| value.parse::<i64>().ok()).flatten()
                })
                .unwrap_or(0);
            counts.insert(index, key_count);
        }

        Ok(counts)
    }

    async fn key_preview(&self, key: &str, kind: KeyValueKind) -> Option<String> {
        match kind {
            KeyValueKind::String => self
                .run_redis_command("GETRANGE", |command| {
                    command.arg(key).arg(0).arg(80);
                })
                .await
                .ok()
                .map(|value| string_key_preview(key, &value)),
            KeyValueKind::Json => self
                .run_redis_command("GETRANGE", |command| {
                    command.arg(key).arg(0).arg(120);
                })
                .await
                .ok()
                .map(|value| truncate_text(&Self::value_to_string(&value), 120)),
            KeyValueKind::List => self
                .run_redis_command("LRANGE", |command| {
                    command.arg(key).arg(0).arg(2);
                })
                .await
                .ok()
                .map(|value| array_preview(value, "[", "]")),
            KeyValueKind::Set => self
                .run_redis_command("SSCAN", |command| {
                    command.arg(key).arg(0).arg("COUNT").arg(3);
                })
                .await
                .ok()
                .and_then(scan_items_preview)
                .map(|items| format!("{{{}}}", items.join(", "))),
            KeyValueKind::ZSet => self
                .run_redis_command("ZRANGE", |command| {
                    command.arg(key).arg(0).arg(2).arg("WITHSCORES");
                })
                .await
                .ok()
                .map(zset_preview),
            KeyValueKind::Hash => self
                .run_redis_command("HSCAN", |command| {
                    command.arg(key).arg(0).arg("COUNT").arg(3);
                })
                .await
                .ok()
                .and_then(hash_scan_preview),
            KeyValueKind::Stream => {
                let length = self
                    .run_redis_int_command("XLEN", |command| {
                        command.arg(key);
                    })
                    .await
                    .ok()
                    .unwrap_or(0);
                self.run_redis_command("XREVRANGE", |command| {
                    command.arg(key).arg("+").arg("-").arg("COUNT").arg(1);
                })
                .await
                .ok()
                .map(|value| stream_preview(value, length))
            }
            KeyValueKind::None => None,
        }
    }
}

fn parse_single_redis_command(sql: &str) -> Result<(String, Vec<String>)> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(ZqlzError::Driver("Empty command".to_string()));
    }
    if has_unclosed_quote(trimmed) {
        return Err(ZqlzError::Driver(
            "Redis command contains an unterminated quoted string".to_string(),
        ));
    }

    let mut tokenizer = CommandTokenizer::new(trimmed, false);
    let tokens = tokenizer.tokenize();
    let commands = parse_commands(&tokens);

    match commands.as_slice() {
        [] => Err(ZqlzError::Driver("Empty command".to_string())),
        [command] => Ok((command.command.clone(), command.args.clone())),
        _ => Err(ZqlzError::Driver(
            "Redis driver executes one command at a time".to_string(),
        )),
    }
}

fn has_unclosed_quote(input: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;

    for character in input.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        match quote {
            Some(open_quote) if character == open_quote => quote = None,
            None if character == '"' || character == '\'' => quote = Some(character),
            _ => {}
        }
    }

    quote.is_some()
}

fn truncate_text(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        value.to_string()
    } else {
        let take_len = max_len.saturating_sub(3);
        let prefix = value.chars().take(take_len).collect::<String>();
        format!("{}...", prefix)
    }
}

fn bytes_to_display_string(bytes: &[u8]) -> String {
    if is_hyperloglog_payload(bytes) {
        return format!("HyperLogLog payload ({} B)", bytes.len());
    }

    if let Ok(value) = std::str::from_utf8(bytes)
        && value.chars().all(is_text_display_character)
    {
        return value.to_string();
    }

    let hex = bytes
        .iter()
        .take(32)
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<_>>()
        .join(" ");
    let suffix = if bytes.len() > 32 { " ..." } else { "" };
    format!("Binary data ({} B): {}{}", bytes.len(), hex, suffix)
}

fn bytes_to_zqlz_value(bytes: &[u8]) -> Value {
    if let Ok(value) = std::str::from_utf8(bytes)
        && value.chars().all(is_text_display_character)
    {
        return Value::String(value.to_string());
    }

    Value::Bytes(bytes.to_vec())
}

fn is_text_display_character(character: char) -> bool {
    !character.is_control() || matches!(character, '\n' | '\r' | '\t')
}

fn is_hyperloglog_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(HYPERLOGLOG_MAGIC)
}

fn string_key_preview(key: &str, value: &redis::Value) -> String {
    match value {
        redis::Value::BulkString(bytes) if is_hyperloglog_payload(bytes) => {
            format!("HyperLogLog key ({} B). Run PFCOUNT {}", bytes.len(), key)
        }
        _ => RedisConnection::value_to_string(value),
    }
}

fn array_preview(value: redis::Value, open: &str, close: &str) -> String {
    let items = match value {
        redis::Value::Array(items) => items
            .iter()
            .map(RedisConnection::value_to_string)
            .map(|value| truncate_text(&value, 24))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    format!("{}{}{}", open, items.join(", "), close)
}

fn scan_items_preview(value: redis::Value) -> Option<Vec<String>> {
    let redis::Value::Array(items) = value else {
        return None;
    };
    let redis::Value::Array(values) = items.get(1)? else {
        return None;
    };
    Some(
        values
            .iter()
            .map(RedisConnection::value_to_string)
            .map(|value| truncate_text(&value, 24))
            .collect(),
    )
}

fn hash_scan_preview(value: redis::Value) -> Option<String> {
    let values = scan_items_preview(value)?;
    let pairs = values
        .chunks(2)
        .filter(|chunk| chunk.len() == 2)
        .map(|chunk| format!("{}: {}", chunk[0], chunk[1]))
        .collect::<Vec<_>>();
    Some(pairs.join(", "))
}

fn zset_preview(value: redis::Value) -> String {
    let redis::Value::Array(items) = value else {
        return String::new();
    };
    items
        .chunks(2)
        .filter(|chunk| chunk.len() == 2)
        .map(|chunk| {
            format!(
                "{}({})",
                truncate_text(&RedisConnection::value_to_string(&chunk[0]), 24),
                RedisConnection::value_to_string(&chunk[1])
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn stream_preview(value: redis::Value, length: i64) -> String {
    let redis::Value::Array(entries) = value else {
        return format!("{} entries", length);
    };
    let Some(redis::Value::Array(parts)) = entries.first() else {
        return format!("{} entries", length);
    };
    let Some(id) = parts.first().map(RedisConnection::value_to_string) else {
        return format!("{} entries", length);
    };
    format!("{} entries [{}]", length, truncate_text(&id, 32))
}

fn stream_entries_to_json(value: &redis::Value) -> Result<String> {
    let redis::Value::Array(entries) = value else {
        return Ok("[]".to_string());
    };

    let mut serialized_entries = Vec::new();
    for entry in entries {
        let redis::Value::Array(parts) = entry else {
            continue;
        };
        if parts.len() != 2 {
            continue;
        }

        let id = RedisConnection::value_to_string(&parts[0]);
        let redis::Value::Array(field_values) = &parts[1] else {
            continue;
        };

        let mut fields = serde_json::Map::new();
        for chunk in field_values.chunks(2) {
            if chunk.len() != 2 {
                continue;
            }
            fields.insert(
                RedisConnection::value_to_string(&chunk[0]),
                serde_json::Value::String(RedisConnection::value_to_string(&chunk[1])),
            );
        }

        let mut entry_object = serde_json::Map::new();
        entry_object.insert("id".to_string(), serde_json::Value::String(id));
        entry_object.insert("fields".to_string(), serde_json::Value::Object(fields));
        serialized_entries.push(serde_json::Value::Object(entry_object));
    }

    serde_json::to_string(&serialized_entries)
        .map_err(|error| ZqlzError::Driver(format!("Invalid stream payload JSON: {}", error)))
}

fn parse_stream_entries(value: &str) -> Result<StreamEntries> {
    let json = serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| ZqlzError::Driver(format!("Invalid stream payload JSON: {}", error)))?;
    let entries = json
        .as_array()
        .ok_or_else(|| ZqlzError::Driver("Stream payload must be a JSON array".to_string()))?;

    entries
        .iter()
        .map(|entry| {
            let object = entry.as_object().ok_or_else(|| {
                ZqlzError::Driver("Stream entries must be JSON objects".to_string())
            })?;
            let id = object
                .get("id")
                .and_then(|value| value.as_str())
                .filter(|id| !id.trim().is_empty())
                .unwrap_or("*")
                .to_string();
            let fields = object
                .get("fields")
                .and_then(|value| value.as_object())
                .ok_or_else(|| {
                    ZqlzError::Driver("Stream entry missing fields object".to_string())
                })?;
            let fields = fields
                .iter()
                .filter(|(field, _)| !field.trim().is_empty())
                .map(|(field, value)| {
                    let value = match value {
                        serde_json::Value::String(value) => value.clone(),
                        other => other.to_string(),
                    };
                    (field.clone(), value)
                })
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err(ZqlzError::Driver(
                    "Stream entries must contain at least one field".to_string(),
                ));
            }
            Ok((id, fields))
        })
        .collect()
}

#[cfg(test)]
mod display_tests {
    use super::*;

    #[test]
    fn bytes_to_display_string_keeps_plain_text() {
        assert_eq!(bytes_to_display_string(b"enabled"), "enabled");
    }

    #[test]
    fn bytes_to_display_string_formats_binary_payloads() {
        let value = bytes_to_display_string(&[0, 159, 146, 150]);
        assert_eq!(value, "Binary data (4 B): 00 9f 92 96");
    }

    #[test]
    fn string_key_preview_identifies_hyperloglog_payloads() {
        let value = redis::Value::BulkString(vec![b'H', b'Y', b'L', b'L', 1, 0, 0, 0]);
        assert_eq!(
            string_key_preview("zqlz:hll:test", &value),
            "HyperLogLog key (8 B). Run PFCOUNT zqlz:hll:test"
        );
    }

    #[test]
    fn bytes_to_zqlz_value_preserves_binary_bytes() {
        assert_eq!(
            bytes_to_zqlz_value(&[0, 159, 146, 150]),
            Value::Bytes(vec![0, 159, 146, 150])
        );
    }
}

#[cfg(test)]
mod zset_payload_tests {
    use super::*;

    #[test]
    fn parse_zset_items_accepts_member_score_object() {
        let items = RedisConnection::parse_zset_items(r#"{"gru":34800,"dfw":92783}"#).unwrap();
        assert_eq!(
            items,
            vec![(92783.0, "dfw".to_string()), (34800.0, "gru".to_string())]
        );
    }

    #[test]
    fn parse_zset_items_accepts_score_member_objects() {
        let items =
            RedisConnection::parse_zset_items(r#"[{"score":34800,"member":"gru"}]"#).unwrap();
        assert_eq!(items, vec![(34800.0, "gru".to_string())]);
    }

    #[test]
    fn parse_zset_items_accepts_score_member_pairs() {
        let items = RedisConnection::parse_zset_items(r#"[34800,"gru",92783,"dfw"]"#).unwrap();
        assert_eq!(
            items,
            vec![(34800.0, "gru".to_string()), (92783.0, "dfw".to_string())]
        );
    }

    #[test]
    fn parse_stream_entries_accepts_entry_objects() {
        let items = RedisConnection::parse_stream_entries(
            r#"[{"id":"1778802803694-1","fields":{"player":"ada","severity":"low"}}]"#,
        )
        .unwrap();

        assert_eq!(
            items,
            vec![(
                "1778802803694-1".to_string(),
                vec![
                    ("player".to_string(), "ada".to_string()),
                    ("severity".to_string(), "low".to_string())
                ]
            )]
        );
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

    fn requires_database_scoped_connection(&self) -> bool {
        true
    }

    fn normalize_database_scope_name(&self, database_name: &str) -> String {
        parse_redis_database_index(database_name)
            .map(|index| index.to_string())
            .unwrap_or_else(|| database_name.trim().to_string())
    }

    fn driver_category(&self) -> DriverCategory {
        DriverCategory::KeyValue
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        let database_index = match scope {
            ConnectionScope::Default => self.database(),
            ConnectionScope::KeyValueDatabase(index) => index,
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                parse_redis_database_index(&database_name).ok_or_else(|| {
                    ZqlzError::Driver(format!("Invalid Redis database scope: {}", database_name))
                })?
            }
        };

        let database_name = database_index.to_string();
        resolved.normalized_scope = ConnectionScope::KeyValueDatabase(database_index);
        resolved.effective_database = Some(database_name.clone());
        resolved.physical_database_key = Some(database_name);
        resolved.requires_dedicated_connection = true;

        Ok(resolved)
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some(self.database().to_string()))
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::None
    }

    fn rename_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _new_table_name: &str,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table rename operations".to_string(),
        ))
    }

    fn drop_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _options: DropTableOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP TABLE operations".to_string(),
        ))
    }

    fn drop_view_sql(
        &self,
        _view_name: &SqlObjectName,
        _options: DropViewOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP VIEW operations".to_string(),
        ))
    }

    fn drop_trigger_sql(
        &self,
        _trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        _options: DropTriggerOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP TRIGGER operations".to_string(),
        ))
    }

    fn truncate_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL TRUNCATE TABLE operations".to_string(),
        ))
    }

    fn duplicate_table_sql(
        &self,
        _source_table_name: &SqlObjectName,
        _new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table duplication operations".to_string(),
        ))
    }

    fn clear_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table clear operations".to_string(),
        ))
    }

    fn table_has_rows_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table row-existence queries".to_string(),
        ))
    }

    fn select_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL row-selection queries".to_string(),
        ))
    }

    fn select_distinct_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
        _order_by_columns: &[String],
        _limit: u64,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL distinct-row queries".to_string(),
        ))
    }

    fn insert_row_sql(
        &self,
        _table_name: &SqlObjectName,
        _column_names: &[String],
        _value_count: usize,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL INSERT-row statement generation".to_string(),
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL performance metrics queries".to_string(),
        ))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut conn = self.connection.lock().await;

        let (command, args) = parse_single_redis_command(sql)?;
        let mut cmd = redis::cmd(&command);

        for arg in args {
            cmd.arg(arg);
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

        let (command, args) = parse_single_redis_command(sql)?;
        let mut cmd = redis::cmd(&command);

        for arg in args {
            cmd.arg(arg);
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

    fn ping_query_sql(&self) -> &'static str {
        REDIS_PING_COMMAND
    }

    fn is_busy(&self) -> bool {
        self.connection.try_lock().is_err()
    }


    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn as_key_value_store(&self) -> Option<&dyn KeyValueStore> {
        Some(self)
    }
}

impl ConnectionSecurity for RedisConnection {
    fn supports_ssh(&self) -> bool {
        true
    }

    fn supports_tls(&self) -> bool {
        true
    }

    fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }

    fn is_encrypted(&self) -> bool {
        self.tls_config
            .as_ref()
            .map(|config| config.mode != TlsMode::Disable)
            .unwrap_or(false)
    }

    fn is_tunneled(&self) -> bool {
        self.ssh_tunnel.is_some()
    }
}

#[async_trait]
impl KeyValueStore for RedisConnection {
    async fn list_key_value_databases(&self) -> Result<Vec<KeyValueDatabaseInfo>> {
        let keyspace_counts = self.keyspace_database_counts().await.unwrap_or_default();
        let databases = self
            .as_schema_introspection()
            .ok_or_else(|| ZqlzError::Driver("Redis schema introspection unavailable".to_string()))?
            .list_databases()
            .await?;
        Ok(databases
            .into_iter()
            .filter_map(|database| {
                parse_redis_database_index(&database.name).map(|index| KeyValueDatabaseInfo {
                    index,
                    size_bytes: keyspace_counts.get(&index).copied().or(database.size_bytes),
                })
            })
            .collect())
    }

    async fn scan_keys(&self, request: KeyValueScanRequest) -> Result<KeyValueScanResult> {
        self.ensure_key_value_database(request.database_index)?;
        let effective_limit = request.limit.max(1);
        let effective_scan_batch_size = request.scan_batch_size.max(1);
        let mut keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let value = self
                .run_redis_command("SCAN", |command| {
                    command
                        .arg(cursor)
                        .arg("MATCH")
                        .arg("*")
                        .arg("COUNT")
                        .arg(effective_scan_batch_size);
                })
                .await?;
            let redis::Value::Array(items) = value else {
                break;
            };
            if items.len() < 2 {
                break;
            }

            cursor = Self::value_to_string(&items[0]).parse::<u64>().unwrap_or(0);
            if let redis::Value::Array(batch) = &items[1] {
                for key in batch {
                    keys.push(Self::value_to_string(key));
                    if keys.len() >= effective_limit {
                        break;
                    }
                }
            }

            if cursor == 0 || keys.len() >= effective_limit {
                break;
            }
        }

        keys.sort();
        Ok(KeyValueScanResult { keys })
    }

    async fn load_key_summaries(&self, database_index: u16) -> Result<Vec<KeyValueKeySummary>> {
        self.ensure_key_value_database(database_index)?;
        let keys = self
            .scan_keys(KeyValueScanRequest {
                database_index,
                limit: 1000,
                scan_batch_size: 100,
            })
            .await?
            .keys;

        let mut summaries = Vec::with_capacity(keys.len());
        for key in keys {
            let key_type = Self::value_to_string(
                &self
                    .run_redis_command("TYPE", |command| {
                        command.arg(&key);
                    })
                    .await?,
            );
            let kind = Self::key_value_kind_from_type(&key_type);
            let ttl_seconds = self
                .run_redis_int_command("TTL", |command| {
                    command.arg(&key);
                })
                .await
                .ok();
            let size_bytes = self
                .run_redis_int_command("MEMORY", |command| {
                    command.arg("USAGE").arg(&key);
                })
                .await
                .ok();
            let preview = self.key_preview(&key, kind).await;
            summaries.push(KeyValueKeySummary {
                key,
                kind,
                ttl_seconds,
                size_bytes,
                preview,
            });
        }

        Ok(summaries)
    }

    async fn read_key(&self, key: &str, limit: usize) -> Result<KeyValueEntry> {
        let key_type = Self::value_to_string(
            &self
                .run_redis_command("TYPE", |command| {
                    command.arg(key);
                })
                .await?,
        );
        let kind = Self::key_value_kind_from_type(&key_type);
        let limit = limit.max(1);
        let ttl_seconds = self
            .run_redis_int_command("TTL", |command| {
                command.arg(key);
            })
            .await
            .ok();

        let data = match kind {
            KeyValueKind::String => {
                let value = self
                    .run_redis_command("GET", |command| {
                        command.arg(key);
                    })
                    .await?;
                Self::rows_from_values(vec![redis_value_to_zqlz_value(&value)], "index")
            }
            KeyValueKind::Json => {
                let value = self
                    .run_redis_command("GET", |command| {
                        command.arg(key);
                    })
                    .await?;
                Self::rows_from_values(vec![Value::String(Self::value_to_string(&value))], "index")
            }
            KeyValueKind::List => {
                let end = limit.saturating_sub(1);
                let value = self
                    .run_redis_command("LRANGE", |command| {
                        command.arg(key).arg(0).arg(end);
                    })
                    .await?;
                let values = match value {
                    redis::Value::Array(items) => items
                        .iter()
                        .map(|item| Value::String(Self::value_to_string(item)))
                        .collect(),
                    _ => Vec::new(),
                };
                Self::rows_from_values(values, "index")
            }
            KeyValueKind::Set => {
                let value = self
                    .run_redis_command("SMEMBERS", |command| {
                        command.arg(key);
                    })
                    .await?;
                let values = match value {
                    redis::Value::Array(items) => items
                        .iter()
                        .take(limit)
                        .map(|item| Value::String(Self::value_to_string(item)))
                        .collect(),
                    _ => Vec::new(),
                };
                Self::rows_from_values(values, "member")
            }
            KeyValueKind::ZSet => {
                let end = limit.saturating_sub(1);
                let value = self
                    .run_redis_command("ZRANGE", |command| {
                        command.arg(key).arg(0).arg(end).arg("WITHSCORES");
                    })
                    .await?;
                let items = match value {
                    redis::Value::Array(items) => items,
                    _ => Vec::new(),
                };
                let columns = vec![
                    Self::string_column("member", 0),
                    Self::string_column("score", 1),
                ];
                let column_names = vec!["member".to_string(), "score".to_string()];
                let rows = items
                    .chunks(2)
                    .filter(|chunk| chunk.len() == 2)
                    .map(|chunk| {
                        Row::new(
                            column_names.clone(),
                            vec![
                                Value::String(Self::value_to_string(&chunk[0])),
                                Value::String(Self::value_to_string(&chunk[1])),
                            ],
                        )
                    })
                    .collect::<Vec<_>>();
                QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    total_rows: Some(rows.len() as u64),
                    rows,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms: 0,
                    warnings: Vec::new(),
                }
            }
            KeyValueKind::Hash => {
                let value = self
                    .run_redis_command("HGETALL", |command| {
                        command.arg(key);
                    })
                    .await?;
                let items = match value {
                    redis::Value::Array(items) => items,
                    _ => Vec::new(),
                };
                let columns = vec![
                    Self::string_column("field", 0),
                    Self::string_column("value", 1),
                ];
                let column_names = vec!["field".to_string(), "value".to_string()];
                let rows = items
                    .chunks(2)
                    .filter(|chunk| chunk.len() == 2)
                    .map(|chunk| {
                        Row::new(
                            column_names.clone(),
                            vec![
                                Value::String(Self::value_to_string(&chunk[0])),
                                Value::String(Self::value_to_string(&chunk[1])),
                            ],
                        )
                    })
                    .collect::<Vec<_>>();
                QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    total_rows: Some(rows.len() as u64),
                    rows,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms: 0,
                    warnings: Vec::new(),
                }
            }
            KeyValueKind::Stream => {
                let value = self
                    .run_redis_command("XRANGE", |command| {
                        command.arg(key).arg("-").arg("+").arg("COUNT").arg(limit);
                    })
                    .await?;
                let serialized = stream_entries_to_json(&value)?;
                Self::rows_from_values(vec![Value::String(serialized)], "entry")
            }
            KeyValueKind::None => QueryResult::empty(),
        };

        Ok(KeyValueEntry {
            key: key.to_string(),
            kind,
            ttl_seconds,
            data,
        })
    }

    async fn save_key(&self, request: KeyValueSaveRequest) -> Result<KeyValueSaveOutcome> {
        let temporary_key = &request.temporary_key;
        let should_protect_target =
            request.original_key.is_empty() || request.original_key != request.new_key;
        let was_renamed =
            !request.original_key.is_empty() && request.original_key != request.new_key;
        if should_protect_target
            && self
                .run_redis_int_command("EXISTS", |command| {
                    command.arg(&request.new_key);
                })
                .await?
                > 0
        {
            return Err(ZqlzError::Driver(format!(
                "Redis key '{}' already exists",
                request.new_key
            )));
        }

        let save_result = async {
            match request.kind {
                KeyValueKind::String => {
                    self.run_redis_command("SET", |command| {
                        command.arg(temporary_key).arg(&request.serialized_value);
                    })
                    .await?;
                }
                KeyValueKind::Json => {
                    self.run_redis_command("SET", |command| {
                        command.arg(temporary_key).arg(&request.serialized_value);
                    })
                    .await?;
                }
                KeyValueKind::List => {
                    let items = Self::parse_collection_items(&request.serialized_value);
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis list".to_string(),
                        ));
                    }
                    self.run_redis_command("RPUSH", |command| {
                        command.arg(temporary_key);
                        for item in items {
                            command.arg(item);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Set => {
                    let items = Self::parse_collection_items(&request.serialized_value);
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis set".to_string(),
                        ));
                    }
                    self.run_redis_command("SADD", |command| {
                        command.arg(temporary_key);
                        for item in items {
                            command.arg(item);
                        }
                    })
                    .await?;
                }
                KeyValueKind::ZSet => {
                    let items = Self::parse_zset_items(&request.serialized_value)?;
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis sorted set".to_string(),
                        ));
                    }
                    self.run_redis_command("ZADD", |command| {
                        command.arg(temporary_key);
                        for (score, member) in items {
                            command.arg(score).arg(member);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Hash => {
                    let fields = Self::parse_hash_fields(&request.serialized_value)?;
                    if fields.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis hash".to_string(),
                        ));
                    }
                    self.run_redis_command("HSET", |command| {
                        command.arg(temporary_key);
                        for (field, value) in fields {
                            command.arg(field).arg(value);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Stream => {
                    let entries = Self::parse_stream_entries(&request.serialized_value)?;
                    if entries.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis stream".to_string(),
                        ));
                    }
                    for (id, fields) in entries {
                        self.run_redis_command("XADD", |command| {
                            command.arg(temporary_key).arg(id);
                            for (field, value) in fields {
                                command.arg(field).arg(value);
                            }
                        })
                        .await?;
                    }
                }
                KeyValueKind::None => {
                    return Err(ZqlzError::Driver(
                        "Cannot save unknown Redis type".to_string(),
                    ));
                }
            }

            if let Some(ttl_seconds) = request.ttl_seconds {
                self.run_redis_command("EXPIRE", |command| {
                    command.arg(temporary_key).arg(ttl_seconds);
                })
                .await?;
            } else if let Err(error) = self
                .run_redis_command("PERSIST", |command| {
                    command.arg(temporary_key);
                })
                .await
            {
                tracing::debug!(
                    key = temporary_key,
                    error = %error,
                    "failed to clear Redis key TTL before rename"
                );
            }

            self.run_redis_command("RENAME", |command| {
                command.arg(temporary_key).arg(&request.new_key);
            })
            .await?;

            if was_renamed {
                self.run_redis_command("DEL", |command| {
                    command.arg(&request.original_key);
                })
                .await?;
            }

            Ok(())
        }
        .await;

        if let Err(error) = save_result {
            if let Err(cleanup_error) = self
                .run_redis_command("DEL", |command| {
                    command.arg(temporary_key);
                })
                .await
            {
                tracing::warn!(
                    key = temporary_key,
                    error = %cleanup_error,
                    "failed to clean up temporary Redis key after save failure"
                );
            }
            return Err(error);
        }

        Ok(KeyValueSaveOutcome { was_renamed })
    }

    async fn update_key_cell(&self, request: KeyValueCellUpdateRequest) -> Result<()> {
        let new_value = request
            .new_value
            .as_ref()
            .map(|value| value.display_for_editor())
            .unwrap_or_default();
        match request.kind {
            KeyValueKind::String | KeyValueKind::Json => {
                self.run_redis_command("SET", |command| {
                    command.arg(&request.key).arg(&new_value);
                })
                .await?;
            }
            KeyValueKind::Hash => {
                let field = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| ZqlzError::Driver("No hash field selected".to_string()))?;
                self.run_redis_command("HSET", |command| {
                    command.arg(&request.key).arg(field).arg(new_value);
                })
                .await?;
            }
            KeyValueKind::List => {
                let index = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| ZqlzError::Driver("No list index selected".to_string()))?;
                self.run_redis_command("LSET", |command| {
                    command.arg(&request.key).arg(index).arg(new_value);
                })
                .await?;
            }
            KeyValueKind::ZSet if request.column_name == "score" => {
                let member = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ZqlzError::Driver("No sorted-set member selected".to_string())
                    })?;
                self.run_redis_command("ZADD", |command| {
                    command.arg(&request.key).arg(new_value).arg(member);
                })
                .await?;
            }
            KeyValueKind::Set => {
                return Err(ZqlzError::Driver(
                    "Set members cannot be updated directly".to_string(),
                ));
            }
            other => {
                return Err(ZqlzError::Driver(format!(
                    "Update not supported for key type {}",
                    other.as_str()
                )));
            }
        }

        Ok(())
    }

    async fn delete_keys(&self, request: KeyValueDeleteRequest) -> Result<KeyValueDeleteOutcome> {
        let mut outcome = KeyValueDeleteOutcome::default();
        for key_name in request.key_names {
            match self
                .run_redis_command("DEL", |command| {
                    command.arg(&key_name);
                })
                .await
            {
                Ok(_) => outcome.deleted_key_names.push(key_name),
                Err(error) => {
                    outcome.errors.push(format!("'{}': {}", key_name, error));
                    if !request.continue_on_error {
                        return Ok(outcome);
                    }
                }
            }
        }
        Ok(outcome)
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
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, bytes_to_zqlz_value(data)],
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
        redis::Value::BulkString(data) => bytes_to_zqlz_value(data),
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

#[cfg(test)]
mod ping_tests {
    use super::*;

    #[test]
    fn ping_statement_parses_as_a_single_redis_command() {
        let (command, args) = parse_single_redis_command(REDIS_PING_COMMAND)
            .expect("heartbeat ping must be a command the driver can run");
        assert_eq!(command, "PING");
        assert!(args.is_empty());
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    use zqlz_core::{
        DatabaseDriver, HighlightQueryLanguage, ParameterPlaceholderCapability, TreeSitterGrammar,
        syntax_driver_capabilities_from_bundle,
    };

    #[test]
    fn redis_dialect_bundle_owns_editor_syntax_capabilities() {
        let capabilities = syntax_driver_capabilities_from_bundle(get_dialect_bundle());

        assert_eq!(capabilities.profile, "redis");
        assert_eq!(capabilities.tree_sitter_grammar, TreeSitterGrammar::None);
        assert_eq!(
            capabilities.highlight_query_language,
            HighlightQueryLanguage::Redis
        );
        assert_eq!(
            capabilities.parameter_placeholders,
            ParameterPlaceholderCapability::disabled()
        );
        assert!(capabilities.command_syntax);
        assert!(!capabilities.document_syntax);
        assert!(!capabilities.sql_overlays);
        assert!(!capabilities.dollar_quoted_strings);
    }

    #[test]
    fn redis_schema_includes_username() {
        let schema = RedisDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert_eq!(field("username").row_group, Some(2));
        assert_eq!(field("database").default_value.as_deref(), Some("0"));
        assert_eq!(
            field("database").help_text.as_deref(),
            Some("Database index (0-15)")
        );
        assert_eq!(field("ssl_mode").tab.as_deref(), Some("ssl"));
        assert_eq!(field("ssl_ca_cert").tab.as_deref(), Some("ssl"));
        assert_eq!(field("ssh_enabled").tab.as_deref(), Some("ssh"));
        assert_eq!(
            field("ssh_auth_method").default_value.as_deref(),
            Some("password")
        );
    }

    #[test]
    fn redis_validation_rejects_invalid_port() {
        let config = ConnectionConfig::new("redis", "test").with_param("port", "0");

        let error = validate_connection_config(&config).unwrap_err();

        assert!(error.to_string().contains("between 1 and 65535"));
    }

    #[test]
    fn redis_validation_accepts_large_database_number() {
        let config = ConnectionConfig::new("redis", "test")
            .with_param("port", "6379")
            .with_param("database", "32");

        validate_connection_config(&config).unwrap();
    }

    #[test]
    fn redis_parser_preserves_quoted_arguments() {
        let (command, args) = parse_single_redis_command(r#"SET "my key" "hello world""#).unwrap();

        assert_eq!(command, "SET");
        assert_eq!(args, vec!["my key", "hello world"]);
    }

    #[test]
    fn redis_parser_rejects_multiple_commands() {
        let error = parse_single_redis_command("PING\nPING").unwrap_err();

        assert!(error.to_string().contains("one command"));
    }

    #[test]
    fn redis_parser_rejects_unclosed_quote() {
        let error = parse_single_redis_command(r#"SET key "unterminated"#).unwrap_err();

        assert!(error.to_string().contains("unterminated"));
    }

    #[test]
    fn redis_verify_ca_requires_ca() {
        let config = ConnectionConfig::new("redis", "test").with_param("ssl_mode", "VERIFY_CA");

        let error = build_tls_config(&config).unwrap_err();

        assert!(error.to_string().contains("CA certificate"));
    }

    #[test]
    fn redis_ssh_enabled_requires_host_and_user() {
        let config = ConnectionConfig::new("redis", "test")
            .with_param("ssh_enabled", "true")
            .with_param("ssh_auth_method", "agent");

        let error = build_ssh_config(&config).unwrap_err();

        assert!(error.to_string().contains("ssh_host"));
    }
}
