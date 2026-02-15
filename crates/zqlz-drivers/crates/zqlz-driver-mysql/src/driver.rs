//! MySQL driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
};

use crate::MySqlConnection;

/// MySQL database driver
pub struct MySqlDriver;

impl MySqlDriver {
    /// Create a new MySQL driver instance
    pub fn new() -> Self {
        tracing::debug!("MySQL driver initialized");
        Self
    }
}

impl Default for MySqlDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn display_name(&self) -> &'static str {
        "MySQL"
    }

    fn default_port(&self) -> Option<u16> {
        Some(3306)
    }

    fn dialect_info(&self) -> DialectInfo {
        crate::mysql_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: false, // MySQL 8.0.21+ has some support, but limited
            supports_upsert: true,     // ON DUPLICATE KEY UPDATE
            supports_window_functions: true, // MySQL 8.0+
            supports_cte: true,        // MySQL 8.0+
            supports_json: true,       // MySQL 5.7+
            supports_full_text_search: true,
            supports_stored_procedures: true,
            supports_schemas: false, // MySQL uses databases instead of schemas
            supports_multiple_databases: true,
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: Some(64),
            max_parameters: Some(65535),
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref(), database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 3306 };
        let database = config.get_string("database");
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));
        let password = config.get_string("password");

        let conn = MySqlConnection::connect(
            &host,
            port,
            database.as_deref(),
            user.as_deref(),
            password.as_deref(),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to connect to MySQL database");
            ZqlzError::Connection(format!("Failed to connect to MySQL database: {}", e))
        })?;

        tracing::info!(host = %host, port = %port, database = ?database, "MySQL connection created");
        Ok(Arc::new(conn))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing MySQL connection");
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 3306 };
        let database = config.get_string("database");
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));

        let mut conn_str = String::from("mysql://");

        if let Some(u) = user {
            conn_str.push_str(&u);
            if let Some(p) = config.get_string("password") {
                conn_str.push(':');
                conn_str.push_str(&p);
            }
            conn_str.push('@');
        }

        conn_str.push_str(&format!("{}:{}", host, port));

        if let Some(db) = database {
            conn_str.push('/');
            conn_str.push_str(&db);
        }

        conn_str
    }

    fn connection_string_help(&self) -> &'static str {
        "mysql://[user[:password]@]host[:port][/database]"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("MySQL Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("3306")
                    .default_value("3306")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database").placeholder("mydb"),
                ConnectionField::text("user", "Username")
                    .placeholder("root")
                    .default_value("root")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
            ],
        }
    }
}
