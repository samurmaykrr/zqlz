//! Type definitions for sidebar data structures

use std::sync::OnceLock;

use uuid::Uuid;
use zqlz_drivers::DriverRegistry;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SidebarObjectCapabilities {
    pub supports_views: bool,
    pub supports_materialized_views: bool,
    pub supports_triggers: bool,
    pub supports_functions: bool,
    pub supports_procedures: bool,
}

impl SidebarObjectCapabilities {
    pub fn for_driver(driver_name: &str) -> Self {
        let normalized_driver = Self::normalized_driver_name(driver_name);
        let driver_capabilities = sidebar_driver_registry()
            .get(&normalized_driver)
            .map(|driver| driver.capabilities());

        Self {
            supports_views: driver_capabilities
                .as_ref()
                .map_or(true, |capabilities| capabilities.supports_views),
            supports_materialized_views: matches!(
                normalized_driver.as_str(),
                "postgres" | "mssql" | "clickhouse"
            ),
            supports_triggers: driver_capabilities.as_ref().map_or(false, |capabilities| {
                capabilities.supports_triggers && normalized_driver != "postgres"
            }),
            supports_functions: matches!(
                normalized_driver.as_str(),
                "postgres" | "mysql" | "mariadb" | "mssql"
            ),
            supports_procedures: driver_capabilities.as_ref().map_or(false, |capabilities| {
                capabilities.supports_stored_procedures
            }),
        }
    }

    fn normalized_driver_name(driver_name: &str) -> String {
        match driver_name.to_ascii_lowercase().as_str() {
            "postgresql" => "postgres".to_string(),
            "mariadb" => "mysql".to_string(),
            other => other.to_string(),
        }
    }
}

impl Default for SidebarObjectCapabilities {
    fn default() -> Self {
        Self {
            supports_views: true,
            supports_materialized_views: false,
            supports_triggers: false,
            supports_functions: false,
            supports_procedures: false,
        }
    }
}

fn sidebar_driver_registry() -> &'static DriverRegistry {
    static DRIVER_REGISTRY: OnceLock<DriverRegistry> = OnceLock::new();
    DRIVER_REGISTRY.get_or_init(DriverRegistry::with_defaults)
}

/// Information about a saved query for display in the sidebar
#[derive(Clone, Debug)]
pub struct SavedQueryInfo {
    pub id: Uuid,
    pub name: String,
}

/// Information about a Redis database for display in the sidebar
#[derive(Clone, Debug)]
pub struct RedisDatabaseInfo {
    /// Database index (0, 1, 2, ...)
    pub index: u16,
    /// Number of keys in this database (if known)
    pub key_count: Option<i64>,
    /// Keys in this database (loaded on expand)
    pub keys: Vec<String>,
    /// Whether this database is expanded in the tree
    pub is_expanded: bool,
    /// Whether keys are currently loading
    pub is_loading: bool,
}

impl RedisDatabaseInfo {
    pub fn new(index: u16, key_count: Option<i64>) -> Self {
        Self {
            index,
            key_count,
            keys: Vec::new(),
            is_expanded: false,
            is_loading: false,
        }
    }
}

/// Information about a database on the server for display in the sidebar
#[derive(Clone, Debug)]
pub struct SidebarDatabaseInfo {
    pub name: String,
    pub size_bytes: Option<i64>,
    /// Whether this is the currently connected/active database
    pub is_active: bool,
    /// Whether this database node is expanded in the tree
    pub is_expanded: bool,
    /// Whether schema is currently being loaded for this database
    pub is_loading: bool,
    /// Schema data loaded for this database (populated on demand)
    pub schema: Option<DatabaseSchemaData>,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaObjects {
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub schema_name: Option<String>,
    pub schema_names: Vec<String>,
}

/// Schema objects for a single database, used in the sidebar tree
#[derive(Clone, Debug, Default)]
pub struct DatabaseSchemaData {
    pub schema_name: Option<String>,
    pub schema_names: Vec<String>,
    pub schema_expanded: bool,
    /// Expanded schema-group folders for grouped-schema rendering.
    ///
    /// Legacy field name retained for backward compatibility with persisted
    /// state; semantically this now stores expanded groups.
    pub collapsed_schema_groups: Vec<String>,
    /// Expanded section keys for grouped schemas.
    ///
    /// Legacy field name retained for backward compatibility.
    /// Keys are encoded as `<schema>::<section>`.
    pub collapsed_schema_section_keys: Vec<String>,
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub materialized_views_expanded: bool,
    pub triggers_expanded: bool,
    pub functions_expanded: bool,
    pub procedures_expanded: bool,
    /// Whether tables are currently being fetched from the server
    pub tables_loading: bool,
    /// Whether views are currently being fetched from the server
    pub views_loading: bool,
    /// Whether materialized views are currently being fetched from the server
    pub materialized_views_loading: bool,
    /// Whether triggers are currently being fetched from the server
    pub triggers_loading: bool,
    /// Whether functions are currently being fetched from the server
    pub functions_loading: bool,
    /// Whether procedures are currently being fetched from the server
    pub procedures_loading: bool,
}

/// A database connection entry
#[derive(Clone, Debug)]
pub struct ConnectionEntry {
    pub id: Uuid,
    pub name: String,
    pub db_type: String,
    pub object_capabilities: SidebarObjectCapabilities,
    pub is_connected: bool,
    pub is_connecting: bool,
    pub is_expanded: bool,
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub queries: Vec<SavedQueryInfo>,
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub materialized_views_expanded: bool,
    pub triggers_expanded: bool,
    pub functions_expanded: bool,
    pub procedures_expanded: bool,
    pub queries_expanded: bool,
    /// Whether tables are currently being fetched from the server
    pub tables_loading: bool,
    /// Whether views are currently being fetched from the server
    pub views_loading: bool,
    /// Whether materialized views are currently being fetched from the server
    pub materialized_views_loading: bool,
    /// Whether triggers are currently being fetched from the server
    pub triggers_loading: bool,
    /// Whether functions are currently being fetched from the server
    pub functions_loading: bool,
    /// Whether procedures are currently being fetched from the server
    pub procedures_loading: bool,
    /// Redis databases (only used for Redis connections)
    pub redis_databases: Vec<RedisDatabaseInfo>,
    /// Whether Redis databases section is expanded
    pub redis_databases_expanded: bool,
    /// All databases on the server (for drivers that support listing them)
    pub databases: Vec<SidebarDatabaseInfo>,
    /// The schema name for hierarchy display (e.g. "public")
    pub schema_name: Option<String>,
    /// Known schema names for the active database.
    pub schema_names: Vec<String>,
    /// Whether the schema-level node is expanded
    pub schema_expanded: bool,
    /// Expanded schema-group folders for grouped-schema rendering.
    ///
    /// Legacy field name retained for backward compatibility with persisted
    /// state; semantically this now stores expanded groups.
    pub collapsed_schema_groups: Vec<String>,
    /// Expanded section keys for grouped schemas.
    ///
    /// Legacy field name retained for backward compatibility.
    /// Keys are encoded as `<schema>::<section>`.
    pub collapsed_schema_section_keys: Vec<String>,
}

impl ConnectionEntry {
    pub fn new(id: Uuid, name: String, db_type: String) -> Self {
        let object_capabilities = SidebarObjectCapabilities::for_driver(&db_type);
        Self {
            id,
            name,
            db_type,
            object_capabilities,
            is_connected: false,
            is_connecting: false,
            is_expanded: false,
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            triggers: Vec::new(),
            functions: Vec::new(),
            procedures: Vec::new(),
            queries: Vec::new(),
            tables_expanded: false,
            views_expanded: false,
            materialized_views_expanded: false,
            triggers_expanded: false,
            functions_expanded: false,
            procedures_expanded: false,
            queries_expanded: false,
            tables_loading: false,
            views_loading: false,
            materialized_views_loading: false,
            triggers_loading: false,
            functions_loading: false,
            procedures_loading: false,
            redis_databases: Vec::new(),
            redis_databases_expanded: false,
            databases: Vec::new(),
            schema_name: None,
            schema_names: Vec::new(),
            schema_expanded: false,
            collapsed_schema_groups: Vec::new(),
            collapsed_schema_section_keys: Vec::new(),
        }
    }

    /// Check if this is a Redis connection
    pub fn is_redis(&self) -> bool {
        self.db_type == "redis"
    }

    pub fn set_db_type(&mut self, db_type: String) {
        self.object_capabilities = SidebarObjectCapabilities::for_driver(&db_type);
        self.db_type = db_type;
    }
}

#[cfg(test)]
mod tests {
    use super::SidebarObjectCapabilities;

    #[test]
    fn sqlite_hides_unsupported_sidebar_sections() {
        let capabilities = SidebarObjectCapabilities::for_driver("sqlite");

        assert!(capabilities.supports_views);
        assert!(!capabilities.supports_materialized_views);
        assert!(capabilities.supports_triggers);
        assert!(!capabilities.supports_functions);
        assert!(!capabilities.supports_procedures);
    }

    #[test]
    fn postgres_hides_top_level_triggers_but_keeps_other_objects() {
        let capabilities = SidebarObjectCapabilities::for_driver("postgres");

        assert!(capabilities.supports_views);
        assert!(capabilities.supports_materialized_views);
        assert!(!capabilities.supports_triggers);
        assert!(capabilities.supports_functions);
        assert!(capabilities.supports_procedures);
    }

    #[test]
    fn redis_only_keeps_saved_queries_tree() {
        let capabilities = SidebarObjectCapabilities::for_driver("redis");

        assert!(!capabilities.supports_views);
        assert!(!capabilities.supports_materialized_views);
        assert!(!capabilities.supports_triggers);
        assert!(!capabilities.supports_functions);
        assert!(!capabilities.supports_procedures);
    }
}
