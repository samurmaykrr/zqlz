//! Turso driver implementation.

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
};

use crate::TursoConnection;

/// Turso remote database driver.
pub struct TursoDriver;

impl TursoDriver {
    /// Create a new Turso driver instance.
    pub fn new() -> Self {
        tracing::debug!("Turso driver initialized");
        Self
    }
}

impl Default for TursoDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for TursoDriver {
    fn id(&self) -> &'static str {
        "turso"
    }

    fn name(&self) -> &'static str {
        "turso"
    }

    fn display_name(&self) -> &'static str {
        "Turso"
    }

    fn icon_name(&self) -> &'static str {
        "turso"
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
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: None,
            max_parameters: Some(999),
        }
    }

    fn dialect_info(&self) -> DialectInfo {
        zqlz_driver_sqlite::sqlite_dialect()
    }

    #[tracing::instrument(skip(self, config), fields(url = config.get_string("url").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let url = config
            .get_string("url")
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ZqlzError::Configuration("Turso requires a database URL".into()))?;
        let auth_token = config
            .get_string("auth_token")
            .or_else(|| config.get_string("password"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ZqlzError::Configuration("Turso requires an auth token".into()))?;

        let connection = TursoConnection::connect(url, auth_token).await?;
        Ok(Arc::new(connection))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        config.get_string("url").unwrap_or_default()
    }

    fn connection_string_help(&self) -> &'static str {
        "Turso remote URL, for example libsql://database-org.turso.io"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("Turso Connection"),
            fields: vec![
                ConnectionField::text("url", "Database URL")
                    .placeholder("libsql://database-org.turso.io")
                    .required()
                    .help_text("Use the Turso/libSQL remote URL"),
                ConnectionField::password("auth_token", "Auth Token")
                    .required()
                    .help_text("Create with `turso db tokens create <database>`"),
                ConnectionField::boolean("load_schema_on_connect", "Load Schema on Connect")
                    .default_value("true")
                    .help_text("Fetch tables and objects immediately after connecting")
                    .tab("advanced"),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turso_schema_exposes_url_and_auth_token() {
        let schema = TursoDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert!(field("url").required);
        assert!(field("auth_token").required);
        assert!(matches!(
            field("auth_token").field_type,
            zqlz_core::ConnectionFieldType::Password
        ));
    }

    #[test]
    fn turso_reuses_sqlite_dialect_info() {
        let dialect = TursoDriver::new().dialect_info();
        assert!(
            dialect
                .keywords
                .iter()
                .any(|keyword| keyword.keyword == "PRAGMA")
        );
    }
}
