//! SQLite driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
};

use crate::SqliteConnection;

/// SQLite database driver
pub struct SqliteDriver;

impl SqliteDriver {
    /// Create a new SQLite driver instance
    pub fn new() -> Self {
        tracing::debug!("SQLite driver initialized");
        Self
    }
}

impl Default for SqliteDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for SqliteDriver {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn display_name(&self) -> &'static str {
        "SQLite"
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
            supports_stored_procedures: false,
            supports_schemas: false,
            supports_multiple_databases: false,
            supports_streaming: false,
            supports_cancellation: false,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: false,
            max_identifier_length: None,
            max_parameters: Some(999),
        }
    }

    fn dialect_info(&self) -> DialectInfo {
        crate::sqlite_dialect()
    }

    #[tracing::instrument(skip(self, config), fields(path = config.get_string("path").or_else(|| config.get_string("database")).as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let path = config
            .get_string("path")
            .or_else(|| config.get_string("database"))
            .ok_or_else(|| ZqlzError::Configuration(
                "SQLite requires 'path' or 'database' parameter. Example: { \"path\": \"/path/to/database.db\" }".into()
            ))?;

        let conn = SqliteConnection::open(&path).map_err(|e| {
            tracing::error!(error = %e, "failed to connect to SQLite database");
            ZqlzError::Connection(format!("Failed to connect to SQLite database: {}", e))
        })?;

        tracing::info!(path = %path, "SQLite connection created");
        Ok(Arc::new(conn))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing SQLite connection");
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        config
            .get_string("path")
            .or_else(|| config.get_string("database"))
            .unwrap_or_else(|| ":memory:".to_string())
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("SQLite Connection"),
            fields: vec![
                ConnectionField::file_path("path", "Database File")
                    .placeholder("/path/to/database.db")
                    .with_extensions(vec!["db", "sqlite", "sqlite3"])
                    .required()
                    .help_text("Use :memory: for an in-memory database"),
            ],
        }
    }
}
