//! SQLite driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectBundle, DialectInfo, DriverCapabilities, Result, ZqlzError,
    dialect_bundle_from_legacy_info,
};

use crate::{SqliteConnection, SqliteOpenMode, SqliteOpenOptions};

const CONFIG_TOML: &str = include_str!("../dialect/config.toml");

fn get_dialect_bundle() -> &'static DialectBundle {
    static BUNDLE: OnceLock<DialectBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        dialect_bundle_from_legacy_info(CONFIG_TOML, &crate::sqlite_dialect(), "SQLite")
    })
}

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

    fn dialect_bundle(&self) -> Option<&'static DialectBundle> {
        Some(get_dialect_bundle())
    }

    #[tracing::instrument(skip(self, config), fields(path = config.get_string("path").or_else(|| config.get_string("database")).as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let path = config
            .get_string("path")
            .or_else(|| config.get_string("database"))
            .ok_or_else(|| ZqlzError::Configuration(
                "SQLite requires 'path' or 'database' parameter. Example: { \"path\": \"/path/to/database.db\" }".into()
            ))?;

        let conn = SqliteConnection::open_with_options(build_open_options(config, path.clone())?)
            .map_err(|e| {
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
        use zqlz_core::ConnectionFieldOption;

        ConnectionFieldSchema {
            title: Cow::Borrowed("SQLite Connection"),
            fields: vec![
                ConnectionField::file_path("path", "Database File")
                    .placeholder("/path/to/database.db")
                    .with_extensions(vec!["db", "sqlite", "sqlite3"])
                    .required()
                    .help_text("Use :memory: for an in-memory database"),
                ConnectionField::select(
                    "open_mode",
                    "Open Mode",
                    vec![
                        ConnectionFieldOption::new("read_write_create", "Read/Write/Create"),
                        ConnectionFieldOption::new("read_write", "Read/Write"),
                        ConnectionFieldOption::new("read_only", "Read Only"),
                    ],
                )
                .default_value("read_write_create")
                .tab("advanced"),
                ConnectionField::boolean("foreign_keys", "Foreign Keys")
                    .default_value("true")
                    .tab("advanced"),
                ConnectionField::select(
                    "journal_mode",
                    "Journal Mode",
                    vec![
                        ConnectionFieldOption::new("WAL", "WAL"),
                        ConnectionFieldOption::new("DELETE", "DELETE"),
                        ConnectionFieldOption::new("TRUNCATE", "TRUNCATE"),
                        ConnectionFieldOption::new("PERSIST", "PERSIST"),
                        ConnectionFieldOption::new("MEMORY", "MEMORY"),
                        ConnectionFieldOption::new("OFF", "OFF"),
                    ],
                )
                .default_value("WAL")
                .tab("advanced"),
                ConnectionField::select(
                    "synchronous",
                    "Synchronous",
                    vec![
                        ConnectionFieldOption::new("NORMAL", "NORMAL"),
                        ConnectionFieldOption::new("FULL", "FULL"),
                        ConnectionFieldOption::new("EXTRA", "EXTRA"),
                        ConnectionFieldOption::new("OFF", "OFF"),
                    ],
                )
                .default_value("NORMAL")
                .tab("advanced"),
                ConnectionField::number("busy_timeout_ms", "Busy Timeout (ms)")
                    .default_value("5000")
                    .tab("advanced"),
                ConnectionField::boolean("load_extensions", "Load Extensions")
                    .default_value("false")
                    .tab("advanced"),
                ConnectionField::text("extension_paths", "Extension Paths")
                    .help_text("Newline, comma, or semicolon separated")
                    .tab("advanced"),
            ],
        }
    }
}

fn build_open_options(config: &ConnectionConfig, path: String) -> Result<SqliteOpenOptions> {
    let mut options = SqliteOpenOptions::new(path);

    if let Some(open_mode) = config
        .get_string("open_mode")
        .filter(|value| !value.is_empty())
    {
        options.open_mode = SqliteOpenMode::parse(&open_mode)?;
    }
    options.foreign_keys = parse_bool(config, "foreign_keys", true);
    if let Some(journal_mode) = config
        .get_string("journal_mode")
        .filter(|value| !value.is_empty())
    {
        options.journal_mode = journal_mode;
    }
    if let Some(synchronous) = config
        .get_string("synchronous")
        .filter(|value| !value.is_empty())
    {
        options.synchronous = synchronous;
    }
    if let Some(timeout) = config
        .get_string("busy_timeout_ms")
        .filter(|value| !value.is_empty())
    {
        options.busy_timeout_ms = timeout.parse::<u64>().map_err(|_| {
            ZqlzError::Configuration("SQLite busy_timeout_ms must be a number".to_string())
        })?;
    }
    options.load_extensions = parse_bool(config, "load_extensions", false);
    if options.load_extensions {
        options.extension_paths = parse_path_list(
            config
                .get_string("extension_paths")
                .unwrap_or_default()
                .as_str(),
        );
    }

    Ok(options)
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

fn parse_path_list(value: &str) -> Vec<std::path::PathBuf> {
    value
        .split(['\n', ',', ';'])
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(std::path::PathBuf::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{
        DatabaseDriver, HighlightQueryLanguage, ParameterPlaceholderCapability, TreeSitterGrammar,
        syntax_driver_capabilities_from_bundle,
    };

    #[test]
    fn sqlite_dialect_bundle_owns_editor_syntax_capabilities() {
        let driver = SqliteDriver::new();
        let bundle = driver
            .dialect_bundle()
            .expect("SQLite driver should expose dialect bundle");
        let capabilities = syntax_driver_capabilities_from_bundle(bundle);

        assert_eq!(capabilities.profile, "sqlite");
        assert_eq!(capabilities.tree_sitter_grammar, TreeSitterGrammar::Sql);
        assert_eq!(
            capabilities.highlight_query_language,
            HighlightQueryLanguage::Sqlite
        );
        assert_eq!(
            capabilities.parameter_placeholders,
            ParameterPlaceholderCapability::sql(true)
        );
        assert!(capabilities.sql_overlays);
        assert!(!capabilities.dollar_quoted_strings);
        assert!(!capabilities.command_syntax);
        assert!(!capabilities.document_syntax);
    }

    #[test]
    fn sqlite_schema_exposes_advanced_options() {
        let schema = SqliteDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert!(field("path").required);
        assert_eq!(field("open_mode").tab.as_deref(), Some("advanced"));
        assert_eq!(
            field("open_mode").default_value.as_deref(),
            Some("read_write_create")
        );
        assert_eq!(field("foreign_keys").default_value.as_deref(), Some("true"));
        assert_eq!(field("journal_mode").default_value.as_deref(), Some("WAL"));
        assert_eq!(
            field("synchronous").default_value.as_deref(),
            Some("NORMAL")
        );
        assert_eq!(
            field("busy_timeout_ms").default_value.as_deref(),
            Some("5000")
        );
        assert_eq!(
            field("load_extensions").default_value.as_deref(),
            Some("false")
        );
        assert_eq!(field("extension_paths").tab.as_deref(), Some("advanced"));
    }

    #[test]
    fn sqlite_options_ignore_extension_paths_when_disabled() {
        let config =
            ConnectionConfig::new("sqlite", "test").with_param("extension_paths", "/tmp/a\n/tmp/b");

        let options = build_open_options(&config, ":memory:".to_string()).unwrap();

        assert!(!options.load_extensions);
        assert!(options.extension_paths.is_empty());
    }

    #[test]
    fn sqlite_options_parse_extension_paths_when_enabled() {
        let config = ConnectionConfig::new("sqlite", "test")
            .with_param("load_extensions", "true")
            .with_param("extension_paths", "/tmp/a,/tmp/b;/tmp/c");

        let options = build_open_options(&config, ":memory:".to_string()).unwrap();

        assert!(options.load_extensions);
        assert_eq!(options.extension_paths.len(), 3);
    }

    #[test]
    fn sqlite_read_only_missing_file_does_not_create_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let database_path = tempdir.path().join("missing.sqlite");
        let options = SqliteOpenOptions {
            path: database_path.display().to_string(),
            open_mode: SqliteOpenMode::ReadOnly,
            ..SqliteOpenOptions::new(":memory:")
        };

        let result = SqliteConnection::open_with_options(options);

        assert!(result.is_err());
        assert!(!database_path.exists());
    }
}
