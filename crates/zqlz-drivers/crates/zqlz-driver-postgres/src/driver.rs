//! PostgreSQL driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
};

use crate::PostgresConnection;

/// PostgreSQL database driver
pub struct PostgresDriver;

impl PostgresDriver {
    /// Create a new PostgreSQL driver instance
    pub fn new() -> Self {
        tracing::debug!("PostgreSQL driver initialized");
        Self
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn display_name(&self) -> &'static str {
        "PostgreSQL"
    }

    fn dialect_info(&self) -> DialectInfo {
        crate::postgres_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: true,
            supports_upsert: true,
            supports_window_functions: true,
            supports_cte: true,
            supports_json: true,
            supports_full_text_search: true,
            supports_stored_procedures: true,
            supports_schemas: true,
            supports_multiple_databases: true,
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: Some(63),
            max_parameters: Some(65535),
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref(), database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 5432 };
        let database = config
            .get_string("database")
            .unwrap_or_else(|| "postgres".to_string());
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));
        let password = config.get_string("password");
        
        // Extract SSL configuration
        let ssl_mode = config.get_string("ssl_mode").unwrap_or_else(|| "prefer".to_string());
        let ssl_ca_cert = config.get_string("ssl_ca_cert");
        let ssl_client_cert = config.get_string("ssl_client_cert");
        let ssl_client_key = config.get_string("ssl_client_key");

        let conn = PostgresConnection::connect(
            &host,
            port,
            &database,
            user.as_deref(),
            password.as_deref(),
            &ssl_mode,
            ssl_ca_cert.as_deref(),
            ssl_client_cert.as_deref(),
            ssl_client_key.as_deref(),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to connect to PostgreSQL database");
            ZqlzError::Connection(format!("Failed to connect to PostgreSQL database: {}", e))
        })?;

        tracing::info!(host = %host, port = %port, database = %database, ssl = %ssl_mode, "PostgreSQL connection created");
        Ok(Arc::new(conn))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing PostgreSQL connection");
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 5432 };
        let database = config
            .get_string("database")
            .unwrap_or_else(|| "postgres".to_string());
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));

        let mut conn_str = format!("postgresql://");

        if let Some(u) = user {
            conn_str.push_str(&u);
            if let Some(p) = config.get_string("password") {
                conn_str.push(':');
                conn_str.push_str(&p);
            }
            conn_str.push('@');
        }

        conn_str.push_str(&format!("{}:{}/{}", host, port, database));

        conn_str
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        use zqlz_core::ConnectionFieldOption;
        
        ConnectionFieldSchema {
            title: Cow::Borrowed("PostgreSQL Connection"),
            fields: vec![
                // General tab fields
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("5432")
                    .default_value("5432")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database")
                    .placeholder("postgres")
                    .default_value("postgres"),
                ConnectionField::text("user", "Username")
                    .placeholder("postgres")
                    .default_value("postgres")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
                
                // SSL tab fields
                ConnectionField::select(
                    "ssl_mode",
                    "SSL Mode",
                    vec![
                        ConnectionFieldOption::new("disable", "Disable"),
                        ConnectionFieldOption::new("allow", "Allow"),
                        ConnectionFieldOption::new("prefer", "Prefer (Recommended)"),
                        ConnectionFieldOption::new("require", "Require"),
                        ConnectionFieldOption::new("verify-ca", "Verify CA"),
                        ConnectionFieldOption::new("verify-full", "Verify Full"),
                    ],
                )
                .default_value("prefer")
                .help_text("SSL connection mode - Prefer tries SSL first, falls back to unencrypted")
                .tab("ssl"),
                
                ConnectionField::file_path("ssl_ca_cert", "CA Certificate")
                    .placeholder("/path/to/ca-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .help_text("Root certificate for verifying server certificate")
                    .tab("ssl"),
                
                ConnectionField::file_path("ssl_client_cert", "Client Certificate")
                    .placeholder("/path/to/client-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .help_text("Client certificate for mutual TLS authentication")
                    .width(0.5)
                    .row_group(10)
                    .tab("ssl"),
                
                ConnectionField::file_path("ssl_client_key", "Client Key")
                    .placeholder("/path/to/client-key.pem")
                    .with_extensions(vec!["pem", "key"])
                    .help_text("Private key for client certificate")
                    .width(0.5)
                    .row_group(10)
                    .tab("ssl"),
                
                // Advanced tab fields
                ConnectionField::text("connect_timeout", "Connect Timeout (seconds)")
                    .placeholder("10")
                    .default_value("10")
                    .help_text("Maximum time to wait for connection")
                    .tab("advanced"),
                
                ConnectionField::text("application_name", "Application Name")
                    .placeholder("ZQLZ")
                    .default_value("ZQLZ")
                    .help_text("Application name sent to PostgreSQL server")
                    .tab("advanced"),
                
                ConnectionField::text("search_path", "Search Path")
                    .placeholder("public")
                    .help_text("Default schema search path (comma-separated)")
                    .tab("advanced"),
                
                ConnectionField::boolean("keepalive", "Keep Alive")
                    .default_value("true")
                    .help_text("Send TCP keepalive packets to maintain connection")
                    .tab("advanced"),
            ],
        }
    }
}
