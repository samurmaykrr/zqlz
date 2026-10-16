//! Turso driver implementation.

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectBundle, DialectInfo, DriverCapabilities, ObjectKindSupport, Result, ZqlzError,
};

use crate::TursoConnection;

pub(crate) const OBJECT_KINDS: ObjectKindSupport = ObjectKindSupport {
    tables: true,
    views: true,
    materialized_views: false,
    functions: false,
    procedures: false,
    triggers: true,
    sequences: false,
    types: false,
    events: false,
    domains: false,
    extensions: false,
};

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

    fn object_kinds(&self) -> ObjectKindSupport {
        OBJECT_KINDS
    }

    fn default_schema_name(&self) -> Option<&'static str> {
        Some("main")
    }

    fn dialect_info(&self) -> DialectInfo {
        zqlz_driver_sqlite::sqlite_dialect()
    }

    fn dialect_bundle(&self) -> Option<&'static DialectBundle> {
        zqlz_driver_sqlite::SqliteDriver::new().dialect_bundle()
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
            .filter(|value| !value.trim().is_empty());
        let auth_token = match auth_token {
            Some(token) => token,
            // A local `sqld` / `turso dev` server has no auth; hosted ones always do.
            None if is_loopback_url(&url) => String::new(),
            None => {
                return Err(ZqlzError::Configuration(
                    "Turso requires an auth token".into(),
                ));
            }
        };

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
                ConnectionField::password("auth_token", "Auth Token").help_text(
                    "Create with `turso db tokens create <database>`. Not needed for a local sqld server",
                ),
                ConnectionField::boolean("load_schema_on_connect", "Load Schema on Connect")
                    .default_value("true")
                    .help_text("Fetch tables and objects immediately after connecting")
                    .tab("advanced"),
            ],
        }
    }
}

/// `http://127.0.0.1:8080`, `libsql://localhost`, `http://[::1]:8080`: a server on this machine.
fn is_loopback_url(url: &str) -> bool {
    let host = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = host.split(['/', '?']).next().unwrap_or_default();
    let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
    let host = match host.strip_prefix('[') {
        Some(ipv6) => ipv6.split(']').next().unwrap_or_default(),
        None => host.split(':').next().unwrap_or_default(),
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turso_schema_exposes_url_and_auth_token() {
        let schema = TursoDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert!(field("url").required);
        assert!(!field("auth_token").required);
        assert!(matches!(
            field("auth_token").field_type,
            zqlz_core::ConnectionFieldType::Password
        ));
    }

    #[test]
    fn only_local_servers_may_skip_the_auth_token() {
        for url in [
            "http://127.0.0.1:8081",
            "libsql://localhost",
            "http://[::1]:8080/",
            "http://u@localhost:1",
        ] {
            assert!(is_loopback_url(url), "{url}");
        }
        for url in [
            "libsql://db-org.turso.io",
            "https://localhost.evil.test",
            "http://10.0.0.5:8080",
        ] {
            assert!(!is_loopback_url(url), "{url}");
        }
    }

    #[tokio::test]
    async fn hosted_urls_still_require_an_auth_token() {
        let mut config = ConnectionConfig::new("turso", "t");
        config
            .params
            .insert("url".into(), "libsql://db-org.turso.io".into());
        let error = TursoDriver::new()
            .connect(&config)
            .await
            .err()
            .expect("must fail");
        assert!(error.to_string().contains("auth token"));
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

    #[test]
    fn turso_reuses_sqlite_dialect_bundle() {
        let bundle = TursoDriver::new()
            .dialect_bundle()
            .expect("Turso should reuse SQLite dialect bundle");

        assert_eq!(
            bundle.config.syntax_highlighting.profile.as_deref(),
            Some("sqlite")
        );
    }
}
