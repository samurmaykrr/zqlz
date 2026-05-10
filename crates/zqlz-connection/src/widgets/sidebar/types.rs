//! Type definitions for sidebar data structures

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use uuid::Uuid;
use zqlz_core::ObjectsPanelManifest;
use zqlz_drivers::DriverRegistry;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SidebarObjectCapabilities {
    pub supports_views: bool,
    pub supports_materialized_views: bool,
    pub supports_triggers: bool,
    pub supports_functions: bool,
    pub supports_procedures: bool,
    pub supports_events: bool,
    pub supports_sequences: bool,
    pub supports_domains: bool,
    pub supports_types: bool,
    pub supports_extensions: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SidebarSection {
    Tables,
    Views,
    MaterializedViews,
    Triggers,
    Functions,
    Procedures,
    Events,
    Sequences,
    Domains,
    Types,
    Extensions,
    Queries,
    RedisDatabases,
}

impl SidebarSection {
    pub fn from_key(section: &str) -> Option<Self> {
        match section {
            "tables" => Some(Self::Tables),
            "views" => Some(Self::Views),
            "materialized_views" => Some(Self::MaterializedViews),
            "triggers" => Some(Self::Triggers),
            "functions" => Some(Self::Functions),
            "procedures" => Some(Self::Procedures),
            "events" => Some(Self::Events),
            "sequences" => Some(Self::Sequences),
            "domains" => Some(Self::Domains),
            "types" => Some(Self::Types),
            "extensions" => Some(Self::Extensions),
            "queries" => Some(Self::Queries),
            "redis_databases" => Some(Self::RedisDatabases),
            _ => None,
        }
    }

    pub fn as_key(self) -> &'static str {
        match self {
            Self::Tables => "tables",
            Self::Views => "views",
            Self::MaterializedViews => "materialized_views",
            Self::Triggers => "triggers",
            Self::Functions => "functions",
            Self::Procedures => "procedures",
            Self::Events => "events",
            Self::Sequences => "sequences",
            Self::Domains => "domains",
            Self::Types => "types",
            Self::Extensions => "extensions",
            Self::Queries => "queries",
            Self::RedisDatabases => "redis_databases",
        }
    }
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
                .is_none_or(|capabilities| capabilities.supports_views),
            supports_materialized_views: matches!(
                normalized_driver.as_str(),
                "postgres" | "mssql" | "clickhouse"
            ),
            supports_triggers: driver_capabilities.as_ref().is_some_and(|capabilities| {
                capabilities.supports_triggers && normalized_driver != "postgres"
            }),
            supports_functions: matches!(
                normalized_driver.as_str(),
                "postgres" | "mysql" | "mariadb" | "mssql"
            ),
            supports_procedures: driver_capabilities
                .as_ref()
                .is_some_and(|capabilities| capabilities.supports_stored_procedures),
            supports_events: normalized_driver == "mysql",
            supports_sequences: normalized_driver == "postgres",
            supports_domains: normalized_driver == "postgres",
            supports_types: normalized_driver == "postgres",
            supports_extensions: normalized_driver == "postgres",
        }
    }

    pub fn for_connection(connection: &dyn zqlz_core::Connection) -> Self {
        if let Some(dialect_id) = connection.dialect_id() {
            return Self::for_driver(dialect_id);
        }

        Self::for_driver(connection.driver_name())
    }

    fn normalized_driver_name(driver_name: &str) -> String {
        match driver_name.to_ascii_lowercase().as_str() {
            "postgresql" => "postgres".to_string(),
            "mariadb" => "mysql".to_string(),
            other => other.to_string(),
        }
    }

    pub fn supports_section(&self, section: SidebarSection) -> bool {
        match section {
            SidebarSection::Tables | SidebarSection::Queries | SidebarSection::RedisDatabases => {
                true
            }
            SidebarSection::Views => self.supports_views,
            SidebarSection::MaterializedViews => self.supports_materialized_views,
            SidebarSection::Triggers => self.supports_triggers,
            SidebarSection::Functions => self.supports_functions,
            SidebarSection::Procedures => self.supports_procedures,
            SidebarSection::Events => self.supports_events,
            SidebarSection::Sequences => self.supports_sequences,
            SidebarSection::Domains => self.supports_domains,
            SidebarSection::Types => self.supports_types,
            SidebarSection::Extensions => self.supports_extensions,
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
            supports_events: false,
            supports_sequences: false,
            supports_domains: false,
            supports_types: false,
            supports_extensions: false,
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
    /// Document collections loaded for document-store databases.
    pub collections: Vec<String>,
    pub collections_expanded: bool,
    pub collections_loading: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaObjects {
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub events: Vec<String>,
    pub sequences: Vec<String>,
    pub domains: Vec<String>,
    pub types: Vec<String>,
    pub extensions: Vec<String>,
    pub schema_name: Option<String>,
    pub schema_names: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SidebarTableDetailsData {
    pub fields: Vec<String>,
    pub indexes: Vec<String>,
    pub foreign_keys: Vec<String>,
    pub uniques: Vec<String>,
    pub checks: Vec<String>,
    pub excludes: Vec<String>,
    pub triggers: Vec<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SidebarTableKey {
    pub conn_id: Uuid,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: String,
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
    pub collapsed_schema_groups: HashSet<String>,
    /// Expanded section keys for grouped schemas.
    ///
    /// Legacy field name retained for backward compatibility.
    /// Keys are encoded as `<schema>::<section>`.
    pub collapsed_schema_section_keys: HashSet<String>,
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub events: Vec<String>,
    pub sequences: Vec<String>,
    pub domains: Vec<String>,
    pub types: Vec<String>,
    pub extensions: Vec<String>,
    pub table_details: HashMap<SidebarTableKey, SidebarTableDetailsData>,
    pub expanded_table_keys: HashSet<SidebarTableKey>,
    pub loading_table_keys: HashSet<SidebarTableKey>,
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub materialized_views_expanded: bool,
    pub triggers_expanded: bool,
    pub functions_expanded: bool,
    pub procedures_expanded: bool,
    pub events_expanded: bool,
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
    pub objects_panel_manifest: Option<ObjectsPanelManifest>,
    pub is_connected: bool,
    pub is_connecting: bool,
    pub is_expanded: bool,
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub events: Vec<String>,
    pub sequences: Vec<String>,
    pub domains: Vec<String>,
    pub types: Vec<String>,
    pub extensions: Vec<String>,
    pub queries: Vec<SavedQueryInfo>,
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub materialized_views_expanded: bool,
    pub triggers_expanded: bool,
    pub functions_expanded: bool,
    pub procedures_expanded: bool,
    pub events_expanded: bool,
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
    pub collapsed_schema_groups: HashSet<String>,
    /// Expanded section keys for grouped schemas.
    ///
    /// Legacy field name retained for backward compatibility.
    /// Keys are encoded as `<schema>::<section>`.
    pub collapsed_schema_section_keys: HashSet<String>,
}

impl ConnectionEntry {
    pub fn new(id: Uuid, name: String, db_type: String) -> Self {
        let object_capabilities = SidebarObjectCapabilities::for_driver(&db_type);
        Self {
            id,
            name,
            db_type,
            object_capabilities,
            objects_panel_manifest: None,
            is_connected: false,
            is_connecting: false,
            is_expanded: false,
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            triggers: Vec::new(),
            functions: Vec::new(),
            procedures: Vec::new(),
            events: Vec::new(),
            sequences: Vec::new(),
            domains: Vec::new(),
            types: Vec::new(),
            extensions: Vec::new(),
            queries: Vec::new(),
            tables_expanded: false,
            views_expanded: false,
            materialized_views_expanded: false,
            triggers_expanded: false,
            functions_expanded: false,
            procedures_expanded: false,
            events_expanded: false,
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
            collapsed_schema_groups: HashSet::new(),
            collapsed_schema_section_keys: HashSet::new(),
        }
    }

    /// Check if this is a Redis connection
    pub fn is_redis(&self) -> bool {
        self.db_type == "redis"
    }

    pub fn is_document(&self) -> bool {
        self.db_type == "mongodb"
    }

    pub fn set_db_type(&mut self, db_type: String) {
        self.object_capabilities = SidebarObjectCapabilities::for_driver(&db_type);
        self.db_type = db_type;
    }
}

#[cfg(test)]
mod tests {
    use super::{SidebarObjectCapabilities, SidebarSection};

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
        assert!(!capabilities.supports_events);
    }

    #[test]
    fn mysql_supports_event_sidebar_section() {
        let capabilities = SidebarObjectCapabilities::for_driver("mysql");

        assert!(capabilities.supports_events);
        assert!(capabilities.supports_section(SidebarSection::Events));
    }

    #[test]
    fn redis_only_keeps_saved_queries_tree() {
        let capabilities = SidebarObjectCapabilities::for_driver("redis");

        assert!(!capabilities.supports_views);
        assert!(!capabilities.supports_materialized_views);
        assert!(!capabilities.supports_triggers);
        assert!(!capabilities.supports_functions);
        assert!(!capabilities.supports_procedures);
        assert!(capabilities.supports_section(SidebarSection::Queries));
        assert!(!capabilities.supports_section(SidebarSection::Views));
    }

    #[test]
    fn parses_sidebar_sections_from_keys() {
        assert_eq!(
            SidebarSection::from_key("materialized_views"),
            Some(SidebarSection::MaterializedViews)
        );
        assert_eq!(SidebarSection::from_key("unknown"), None);
        assert_eq!(SidebarSection::Functions.as_key(), "functions");
        assert_eq!(
            SidebarSection::from_key("events"),
            Some(SidebarSection::Events)
        );
    }
}
