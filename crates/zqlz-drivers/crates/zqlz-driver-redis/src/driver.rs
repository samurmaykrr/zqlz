//! Redis driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::OnceLock;
use zqlz_core::{
    CompletionsConfig, Connection, ConnectionConfig, ConnectionField, ConnectionFieldOption,
    ConnectionFieldSchema, DatabaseDriver, DiagnosticsConfig, DialectBundle, DialectConfig,
    DialectInfo, DriverCapabilities, ObjectKindSupport, Result, ZqlzError,
    security::TlsConfig,
};

mod config;
mod connection;
mod parsing;
mod rows;

pub use connection::RedisConnection;

use config::{
    build_redis_connection_string, build_ssh_tunnel, build_tls_config, redis_database,
    redis_port, validate_connection_config,
};

/// Embedded dialect configuration files
const CONFIG_TOML: &str = include_str!("../dialect/config.toml");
const COMPLETIONS_TOML: &str = include_str!("../dialect/completions.toml");
const DIAGNOSTICS_TOML: &str = include_str!("../dialect/diagnostics.toml");

/// Redis surfaces keys as "tables"; it has no views, routines, sequences, or
/// custom types to enumerate.
pub(crate) const OBJECT_KINDS: ObjectKindSupport = ObjectKindSupport {
    tables: true,
    views: false,
    materialized_views: false,
    functions: false,
    procedures: false,
    triggers: false,
    sequences: false,
    types: false,
    events: false,
    domains: false,
    extensions: false,
};

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

    fn object_kinds(&self) -> ObjectKindSupport {
        OBJECT_KINDS
    }

    fn default_schema_name(&self) -> Option<&'static str> {
        None
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

#[cfg(test)]
mod ping_tests {
    use super::connection::REDIS_PING_COMMAND;
    use super::parsing::parse_single_redis_command;

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
    use super::parsing::parse_single_redis_command;
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
}
