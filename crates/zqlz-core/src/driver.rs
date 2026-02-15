//! Database driver trait definition

use crate::{Connection, DialectBundle, DialectInfo, Result};
use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

/// Field type for connection dialog UI
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionFieldType {
    /// Single-line text input
    Text,
    /// Password input (masked)
    Password,
    /// Numeric input
    Number,
    /// File path with browse button
    FilePath {
        /// File extension filter (e.g., "db", "sqlite")
        extensions: Vec<&'static str>,
        /// Whether to allow selecting directories
        allow_directories: bool,
    },
    /// Dropdown/select with predefined options
    Select { options: Vec<ConnectionFieldOption> },
    /// Checkbox/toggle
    Boolean,
}

/// Option for select fields
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionFieldOption {
    pub value: Cow<'static, str>,
    pub label: Cow<'static, str>,
}

impl ConnectionFieldOption {
    pub const fn new(value: &'static str, label: &'static str) -> Self {
        Self {
            value: Cow::Borrowed(value),
            label: Cow::Borrowed(label),
        }
    }
}

/// Definition of a connection form field
#[derive(Debug, Clone)]
pub struct ConnectionField {
    /// Field identifier (used as key in params)
    pub id: Cow<'static, str>,
    /// Display label
    pub label: Cow<'static, str>,
    /// Field type
    pub field_type: ConnectionFieldType,
    /// Placeholder text
    pub placeholder: Option<Cow<'static, str>>,
    /// Default value
    pub default_value: Option<Cow<'static, str>>,
    /// Whether the field is required
    pub required: bool,
    /// Help text shown below the field
    pub help_text: Option<Cow<'static, str>>,
    /// Width hint: 1.0 = full width, 0.5 = half width, etc.
    pub width: f32,
    /// Group fields on the same row (fields with same row_group are rendered together)
    pub row_group: Option<u8>,
    /// Tab identifier for organizing fields into tabs (e.g., "general", "ssl", "advanced")
    pub tab: Option<Cow<'static, str>>,
}

impl ConnectionField {
    /// Create a new text field
    pub const fn text(id: &'static str, label: &'static str) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::Text,
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }

    /// Create a new password field
    pub const fn password(id: &'static str, label: &'static str) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::Password,
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }

    /// Create a new number field
    pub const fn number(id: &'static str, label: &'static str) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::Number,
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }

    /// Create a new file path field
    pub const fn file_path(id: &'static str, label: &'static str) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::FilePath {
                extensions: Vec::new(),
                allow_directories: false,
            },
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }

    /// Create a new boolean/checkbox field
    pub const fn boolean(id: &'static str, label: &'static str) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::Boolean,
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }
    
    /// Create a new select field
    pub fn select(id: &'static str, label: &'static str, options: Vec<ConnectionFieldOption>) -> Self {
        Self {
            id: Cow::Borrowed(id),
            label: Cow::Borrowed(label),
            field_type: ConnectionFieldType::Select { options },
            placeholder: None,
            default_value: None,
            required: false,
            help_text: None,
            width: 1.0,
            row_group: None,
            tab: None,
        }
    }

    // Builder methods
    pub fn placeholder(mut self, placeholder: &'static str) -> Self {
        self.placeholder = Some(Cow::Borrowed(placeholder));
        self
    }

    pub fn default_value(mut self, value: &'static str) -> Self {
        self.default_value = Some(Cow::Borrowed(value));
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn help_text(mut self, text: &'static str) -> Self {
        self.help_text = Some(Cow::Borrowed(text));
        self
    }

    pub fn width(mut self, width: f32) -> Self {
        self.width = width;
        self
    }

    pub fn row_group(mut self, group: u8) -> Self {
        self.row_group = Some(group);
        self
    }

    pub fn with_extensions(mut self, extensions: Vec<&'static str>) -> Self {
        if let ConnectionFieldType::FilePath {
            allow_directories, ..
        } = self.field_type
        {
            self.field_type = ConnectionFieldType::FilePath {
                extensions,
                allow_directories,
            };
        }
        self
    }
    
    pub fn tab(mut self, tab: &'static str) -> Self {
        self.tab = Some(Cow::Borrowed(tab));
        self
    }
}

/// Schema defining all fields for a connection dialog
#[derive(Debug, Clone)]
pub struct ConnectionFieldSchema {
    /// Dialog title (e.g., "PostgreSQL Connection")
    pub title: Cow<'static, str>,
    /// Fields to display
    pub fields: Vec<ConnectionField>,
}

/// Capabilities that a driver may support
#[derive(Debug, Clone, Default)]
pub struct DriverCapabilities {
    /// Supports transactions
    pub supports_transactions: bool,
    /// Supports savepoints
    pub supports_savepoints: bool,
    /// Supports prepared statements
    pub supports_prepared_statements: bool,
    /// Supports multiple statements in one query
    pub supports_multiple_statements: bool,
    /// Supports RETURNING clause
    pub supports_returning: bool,
    /// Supports UPSERT/ON CONFLICT
    pub supports_upsert: bool,
    /// Supports window functions
    pub supports_window_functions: bool,
    /// Supports common table expressions (WITH)
    pub supports_cte: bool,
    /// Supports JSON operations
    pub supports_json: bool,
    /// Supports full-text search
    pub supports_full_text_search: bool,
    /// Supports stored procedures
    pub supports_stored_procedures: bool,
    /// Supports schemas (namespaces)
    pub supports_schemas: bool,
    /// Supports multiple databases
    pub supports_multiple_databases: bool,
    /// Supports streaming results
    pub supports_streaming: bool,
    /// Supports query cancellation
    pub supports_cancellation: bool,
    /// Supports EXPLAIN
    pub supports_explain: bool,
    /// Supports foreign keys
    pub supports_foreign_keys: bool,
    /// Supports views
    pub supports_views: bool,
    /// Supports triggers
    pub supports_triggers: bool,
    /// Supports SSL/TLS
    pub supports_ssl: bool,
    /// Maximum identifier length (None = no limit)
    pub max_identifier_length: Option<usize>,
    /// Maximum parameters per query (None = no limit)
    pub max_parameters: Option<usize>,
}

/// Core driver trait that all database drivers must implement
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Unique identifier for this driver (e.g., "postgres", "mysql", "sqlite")
    fn id(&self) -> &'static str {
        self.name()
    }

    /// Human-readable name (e.g., "PostgreSQL", "MySQL", "SQLite")
    fn name(&self) -> &'static str;

    /// Display name for UI
    fn display_name(&self) -> &'static str {
        self.name()
    }

    /// Driver version
    fn version(&self) -> &'static str {
        "0.1.0"
    }

    /// Supported features/capabilities
    fn capabilities(&self) -> DriverCapabilities;

    /// Default connection port (None for file-based databases like SQLite)
    fn default_port(&self) -> Option<u16> {
        None
    }

    /// Connection string format help text
    fn connection_string_help(&self) -> &'static str {
        ""
    }

    /// Icon name for the driver (used in UI)
    fn icon_name(&self) -> &'static str {
        "database"
    }

    /// Get SQL dialect information
    ///
    /// Returns comprehensive metadata about the SQL dialect this driver uses,
    /// including keywords, functions, data types, and syntax rules.
    ///
    /// This is the primary extension point for driver-specific behavior.
    /// The rest of the codebase should use this metadata instead of
    /// hardcoding per-driver logic.
    fn dialect_info(&self) -> DialectInfo {
        DialectInfo::default()
    }

    /// Get the full dialect bundle with configuration
    ///
    /// Returns the declarative dialect configuration loaded from TOML files.
    /// This provides additional metadata like language type (SQL, Command, Document)
    /// and parser configuration for diagnostics.
    ///
    /// Drivers that support the new declarative dialect system should override
    /// this method to return their DialectBundle.
    fn dialect_bundle(&self) -> Option<&'static DialectBundle> {
        None
    }

    /// Get the dialect profile for this driver
    ///
    /// Returns the DialectProfile that defines parsing, validation, highlighting,
    /// and formatting capabilities. This is the primary method for accessing
    /// dialect metadata in the new Dialect Registry v2 system.
    ///
    /// By default, this looks up the profile in the global DIALECT_REGISTRY
    /// using the driver's id(). Drivers can override this to provide custom
    /// profiles or additional metadata.
    fn dialect_profile(&self) -> Option<&crate::DialectProfile> {
        crate::get_dialect_profile(self.id())
    }

    /// Create a new connection
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>>;

    /// Test connection without fully connecting
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()>;

    /// Parse a connection string into a configuration
    fn parse_connection_string(&self, _conn_str: &str) -> Result<ConnectionConfig> {
        // Default implementation that returns an error
        Err(crate::ZqlzError::NotImplemented(
            "Connection string parsing not implemented for this driver".into(),
        ))
    }

    /// Build a connection string from configuration
    fn build_connection_string(&self, config: &ConnectionConfig) -> String;

    /// Get default connection parameters
    fn default_params(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    /// Get the connection field schema for the UI dialog
    ///
    /// This defines what fields are shown in the connection dialog,
    /// their types, defaults, and layout.
    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        // Default schema for server-based databases
        ConnectionFieldSchema {
            title: Cow::Borrowed("Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("5432")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database").placeholder("database_name"),
                ConnectionField::text("user", "Username")
                    .placeholder("username")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
            ],
        }
    }
}

/// Connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Unique identifier
    pub id: uuid::Uuid,
    /// Display name
    pub name: String,
    /// Driver ID (e.g., "postgres", "mysql", "sqlite")
    pub driver: String,
    /// Host address (empty for file-based databases)
    pub host: String,
    /// Port number (0 for default or file-based)
    pub port: u16,
    /// Database name or file path
    pub database: Option<String>,
    /// Username
    pub username: Option<String>,
    /// Password (should be encrypted in storage)
    pub password: Option<String>,
    /// Additional connection parameters
    pub params: HashMap<String, String>,
    /// Connection color (for UI)
    pub color: Option<String>,
    /// Group/folder path
    pub group: Option<String>,
    /// Notes
    pub notes: Option<String>,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last used timestamp
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ConnectionConfig {
    /// Create a new configuration with default values
    pub fn new(driver: &str, name: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            driver: driver.to_string(),
            host: String::new(),
            port: 0,
            database: None,
            username: None,
            password: None,
            params: HashMap::new(),
            color: None,
            group: None,
            notes: None,
            created_at: chrono::Utc::now(),
            last_used_at: None,
        }
    }

    /// Create a SQLite configuration
    pub fn new_sqlite(database_path: &str) -> Self {
        let mut config = Self::new("sqlite", "SQLite Database");
        config.database = Some(database_path.to_string());
        config
    }

    /// Create a PostgreSQL configuration
    pub fn new_postgres(host: &str, port: u16, database: &str, username: &str) -> Self {
        let mut config = Self::new("postgres", "PostgreSQL");
        config.host = host.to_string();
        config.port = port;
        config.database = Some(database.to_string());
        config.username = Some(username.to_string());
        config
    }

    /// Create a MySQL configuration
    pub fn new_mysql(host: &str, port: u16, database: &str, username: &str) -> Self {
        let mut config = Self::new("mysql", "MySQL");
        config.host = host.to_string();
        config.port = port;
        config.database = Some(database.to_string());
        config.username = Some(username.to_string());
        config
    }

    /// Set a connection parameter
    pub fn with_param(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        let val = value.into();
        let str_val = match val {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        };
        self.params.insert(key.to_string(), str_val);
        self
    }

    /// Get a string parameter
    pub fn get_string(&self, key: &str) -> Option<String> {
        // First check params
        if let Some(val) = self.params.get(key) {
            return Some(val.clone());
        }
        // Check known fields
        match key {
            "host" => Some(self.host.clone()),
            "database" | "path" => self.database.clone(),
            "username" | "user" => self.username.clone(),
            "password" => self.password.clone(),
            _ => None,
        }
    }

    /// Get port
    pub fn get_port(&self) -> u16 {
        self.port
    }
}
