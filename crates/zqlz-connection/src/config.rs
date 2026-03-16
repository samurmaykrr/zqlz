//! Saved connection configuration

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;
use zqlz_core::ConnectionConfig;

/// A saved database connection configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedConnection {
    /// Unique identifier
    pub id: Uuid,

    /// Display name
    pub name: String,

    /// Driver type (sqlite, postgres, mysql, etc.)
    pub driver: String,

    /// Connection parameters (host, port, database, username, password, etc.)
    /// All values including sensitive ones are stored here and persisted in params_json.
    pub params: std::collections::HashMap<String, String>,

    /// Optional folder/group for organization
    pub folder: Option<String>,

    /// Optional color tag
    pub color: Option<String>,

    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Last modified timestamp
    pub modified_at: chrono::DateTime<chrono::Utc>,

    /// Last connected timestamp
    pub last_connected: Option<chrono::DateTime<chrono::Utc>>,
}

impl SavedConnection {
    /// Create a new saved connection
    pub fn new(name: String, driver: String) -> Self {
        tracing::debug!(name = %name, driver = %driver, "creating new saved connection");
        let now = chrono::Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            driver,
            params: std::collections::HashMap::new(),
            folder: None,
            color: None,
            created_at: now,
            modified_at: now,
            last_connected: None,
        }
    }

    /// Set a connection parameter
    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.params.insert(key.to_string(), value.to_string());
        self
    }

    /// Build a driver config from persisted params.
    ///
    /// Saved connections keep their user-entered values in the generic params map.
    /// Hydrating the strongly typed fields here keeps connection creation logic in one place,
    /// including ports for drivers that read `config.port` directly.
    pub fn to_connection_config(&self) -> ConnectionConfig {
        let mut config = ConnectionConfig::new(&self.driver, &self.name);

        for (key, value) in &self.params {
            config = config.with_param(key, value.clone());
        }

        config.host = self.params.get("host").cloned().unwrap_or_default();
        config.port = self
            .params
            .get("port")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0);
        config.database = self
            .params
            .get("database")
            .cloned()
            .or_else(|| self.params.get("path").cloned());
        config.username = self
            .params
            .get("username")
            .cloned()
            .or_else(|| self.params.get("user").cloned());
        config.password = self.params.get("password").cloned();

        config
    }

    /// Create a saved connection from a dropped/imported target.
    ///
    /// This keeps sidebar drop handling simple and ensures every import path
    /// uses the same provider detection and automatic naming rules.
    pub fn from_external_target(target: &str) -> anyhow::Result<Option<Self>> {
        let trimmed = target.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        if has_uri_scheme(trimmed) {
            return Self::from_database_uri(trimmed);
        }

        if trimmed == ":memory:" {
            return Ok(Some(
                SavedConnection::new("SQLite Memory".to_string(), "sqlite".to_string())
                    .with_param("path", ":memory:"),
            ));
        }

        let path = PathBuf::from(trimmed);
        if path.components().next().is_some() {
            return Ok(Self::from_database_path(&path));
        }

        Ok(None)
    }

    fn from_database_uri(target: &str) -> anyhow::Result<Option<Self>> {
        let url = match Url::parse(target) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("invalid connection URI: {target}"));
            }
        };

        match url.scheme() {
            "postgres" | "postgresql" => Ok(Some(Self::from_postgres_uri(&url))),
            "mysql" => Ok(Some(Self::from_mysql_uri(&url))),
            "sqlite" => Ok(Some(Self::from_sqlite_uri(&url)?)),
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("unsupported file URI: {target}"))?;
                Ok(Self::from_database_path(&path))
            }
            _ => Ok(None),
        }
    }

    fn from_database_path(path: &Path) -> Option<Self> {
        if is_duckdb_path(path) {
            return Some(Self::from_duckdb_path(path));
        }

        if is_sqlite_path(path) {
            return Some(Self::from_sqlite_path(path));
        }

        None
    }

    fn from_sqlite_uri(url: &Url) -> anyhow::Result<Self> {
        let path = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("unsupported sqlite URI: {}", url.as_str()))?;

        Self::from_database_path(&path)
            .ok_or_else(|| anyhow::anyhow!("unsupported sqlite database path: {}", path.display()))
    }

    fn from_sqlite_path(path: &Path) -> Self {
        let name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("SQLite Database")
            .to_string();

        SavedConnection::new(name, "sqlite".to_string()).with_param("path", &path.to_string_lossy())
    }

    fn from_duckdb_path(path: &Path) -> Self {
        let name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("DuckDB Database")
            .to_string();

        SavedConnection::new(name, "duckdb".to_string()).with_param("path", &path.to_string_lossy())
    }

    fn from_postgres_uri(url: &Url) -> Self {
        let database = database_name_from_url(url);
        let name = auto_connection_name("PostgreSQL", url.host_str(), database.as_deref());

        let mut connection = SavedConnection::new(name, "postgres".to_string());

        if let Some(host) = url.host_str() {
            connection = connection.with_param("host", host);
        }
        if let Some(port) = url.port() {
            connection = connection.with_param("port", &port.to_string());
        }
        if let Some(database) = database {
            connection = connection.with_param("database", &database);
        }
        if !url.username().is_empty() {
            connection = connection.with_param("user", url.username());
        }
        if let Some(password) = url.password() {
            connection = connection.with_param("password", password);
        }

        for (key, value) in url.query_pairs() {
            let normalized_key = match key.as_ref() {
                "sslmode" => "ssl_mode",
                other => other,
            };
            connection = connection.with_param(normalized_key, value.as_ref());
        }

        connection
    }

    fn from_mysql_uri(url: &Url) -> Self {
        let database = database_name_from_url(url);
        let name = auto_connection_name("MySQL", url.host_str(), database.as_deref());

        let mut connection = SavedConnection::new(name, "mysql".to_string());

        if let Some(host) = url.host_str() {
            connection = connection.with_param("host", host);
        }
        if let Some(port) = url.port() {
            connection = connection.with_param("port", &port.to_string());
        }
        if let Some(database) = database {
            connection = connection.with_param("database", &database);
        }
        if !url.username().is_empty() {
            connection = connection.with_param("user", url.username());
        }
        if let Some(password) = url.password() {
            connection = connection.with_param("password", password);
        }

        for (key, value) in url.query_pairs() {
            connection = connection.with_param(key.as_ref(), value.as_ref());
        }

        connection
    }
}

fn database_name_from_url(url: &Url) -> Option<String> {
    let database = url.path().trim_matches('/');
    if database.is_empty() {
        None
    } else {
        Some(database.to_string())
    }
}

fn auto_connection_name(driver_name: &str, host: Option<&str>, database: Option<&str>) -> String {
    match (
        database.filter(|value| !value.is_empty()),
        host.filter(|value| !value.is_empty()),
    ) {
        (Some(database), Some(host)) => format!("{database} @ {host}"),
        (Some(database), None) => database.to_string(),
        (None, Some(host)) => format!("{driver_name} @ {host}"),
        (None, None) => driver_name.to_string(),
    }
}

fn has_uri_scheme(target: &str) -> bool {
    let Some((scheme, _)) = target.split_once("://") else {
        return false;
    };

    !scheme.is_empty()
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn is_duckdb_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("duckdb"))
        .unwrap_or(false)
}

fn is_sqlite_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "db" | "sqlite" | "sqlite3"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::SavedConnection;

    #[test]
    fn imports_sqlite_path_with_automatic_name() {
        let connection = SavedConnection::from_external_target("/tmp/sample.sqlite")
            .unwrap()
            .expect("sqlite path should import");

        assert_eq!(connection.driver, "sqlite");
        assert_eq!(connection.name, "sample");
        assert_eq!(
            connection.params.get("path").map(String::as_str),
            Some("/tmp/sample.sqlite")
        );
    }

    #[test]
    fn imports_postgres_uri_and_maps_port() {
        let connection = SavedConnection::from_external_target(
            "postgresql://alice:secret@db.example.com:5433/app?sslmode=require",
        )
        .unwrap()
        .expect("postgres uri should import");

        assert_eq!(connection.driver, "postgres");
        assert_eq!(connection.name, "app @ db.example.com");
        assert_eq!(
            connection.params.get("host").map(String::as_str),
            Some("db.example.com")
        );
        assert_eq!(
            connection.params.get("port").map(String::as_str),
            Some("5433")
        );
        assert_eq!(
            connection.params.get("database").map(String::as_str),
            Some("app")
        );
        assert_eq!(
            connection.params.get("user").map(String::as_str),
            Some("alice")
        );
        assert_eq!(
            connection.params.get("password").map(String::as_str),
            Some("secret")
        );
        assert_eq!(
            connection.params.get("ssl_mode").map(String::as_str),
            Some("require")
        );

        let config = connection.to_connection_config();
        assert_eq!(config.port, 5433);
    }

    #[test]
    fn imports_mysql_uri() {
        let connection =
            SavedConnection::from_external_target("mysql://root:secret@localhost/shop")
                .unwrap()
                .expect("mysql uri should import");

        assert_eq!(connection.driver, "mysql");
        assert_eq!(connection.name, "shop @ localhost");
        assert_eq!(
            connection.params.get("database").map(String::as_str),
            Some("shop")
        );
    }

    #[test]
    fn imports_duckdb_path() {
        let connection = SavedConnection::from_external_target("/tmp/warehouse.duckdb")
            .unwrap()
            .expect("duckdb path should import");

        assert_eq!(connection.driver, "duckdb");
        assert_eq!(connection.name, "warehouse");
    }

    #[test]
    fn ignores_unsupported_url_schemes() {
        let connection =
            SavedConnection::from_external_target("https://example.com/database").unwrap();

        assert!(connection.is_none());
    }

    #[test]
    fn ignores_non_database_files() {
        let connection = SavedConnection::from_external_target("/tmp/query.sql").unwrap();

        assert!(connection.is_none());
    }
}
