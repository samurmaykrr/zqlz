//! Type definitions for sidebar data structures

use std::collections::HashSet;
use std::sync::OnceLock;

use uuid::Uuid;
use zqlz_core::{
    DocumentAdminObjectInfo, DocumentCollectionInfo, DocumentFunctionInfo,
    DocumentGridFsBucketInfo, DocumentIndexInfo, DriverCategory, ObjectFeatureSet,
    ObjectKindSupport, ObjectsPanelManifest, driver_category_from_driver_name,
};
use zqlz_drivers::DriverRegistry;

/// Sidebar-facing projection of what a driver can browse, derived from the
/// driver registry (and refined per connection at runtime) instead of
/// driver-name allowlists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SidebarObjectCapabilities {
    /// Whether a database on this driver contains a separate schema level.
    ///
    /// False for drivers where the database *is* the namespace (MySQL/MariaDB,
    /// ClickHouse) or where there is only one (SQLite, Turso); rendering a
    /// schema folder for those repeats the database node.
    pub supports_schemas: bool,
    /// Schema label assumed for objects with no schema qualifier.
    pub default_schema: Option<&'static str>,
    /// Object kinds the driver can actually enumerate.
    pub kinds: ObjectKindSupport,
    /// Whether triggers render as a top-level section. PostgreSQL renders
    /// triggers under each table instead.
    pub triggers_top_level: bool,
    /// Whether the numbered-databases tree applies (key-value drivers).
    pub supports_redis_databases: bool,
}

/// Declaration order is render order — `Ord` and [`SidebarSection::OBJECT_SECTIONS`]
/// rely on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// The schema-object sections, in render order. Excludes `Queries` and
    /// `RedisDatabases`, which are not schema objects.
    pub const OBJECT_SECTIONS: [Self; 11] = [
        Self::Tables,
        Self::Views,
        Self::MaterializedViews,
        Self::Triggers,
        Self::Functions,
        Self::Procedures,
        Self::Events,
        Self::Sequences,
        Self::Domains,
        Self::Types,
        Self::Extensions,
    ];

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

    pub fn label(self) -> &'static str {
        match self {
            Self::Tables => "Tables",
            Self::Views => "Views",
            Self::MaterializedViews => "Materialized Views",
            Self::Triggers => "Triggers",
            Self::Functions => "Functions",
            Self::Procedures => "Procedures",
            Self::Events => "Events",
            Self::Sequences => "Sequences",
            Self::Domains => "Domains",
            Self::Types => "Types",
            Self::Extensions => "Extensions",
            Self::Queries => "Queries",
            Self::RedisDatabases => "Databases",
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
        // The registry resolves aliases (postgresql, mariadb, sqlserver,
        // mongo) itself; no name normalization needed here.
        let Some(driver) = sidebar_driver_registry().get(driver_name) else {
            return Self::default();
        };

        Self {
            supports_schemas: driver.capabilities().supports_schemas,
            default_schema: driver.default_schema_name(),
            kinds: driver.object_kinds(),
            triggers_top_level: Self::triggers_render_top_level(driver.id()),
            supports_redis_databases: driver_category_from_driver_name(driver.id())
                == DriverCategory::KeyValue,
        }
    }

    pub fn for_connection(connection: &dyn zqlz_core::Connection) -> Self {
        let mut capabilities = match connection.dialect_id() {
            Some(dialect_id) => Self::for_driver(dialect_id),
            None => Self::for_driver(connection.driver_name()),
        };

        if let Some(introspection) = connection.as_schema_introspection()
            && let Some(kinds) = introspection.object_kind_support()
        {
            capabilities.kinds = kinds;
        }
        capabilities.triggers_top_level = connection.supports_top_level_triggers();

        capabilities
    }

    /// Placement policy, not a capability: PostgreSQL shows triggers under
    /// each table, everyone else as a top-level section. Runtime connections
    /// answer this via `Connection::supports_top_level_triggers`.
    fn triggers_render_top_level(driver_id: &str) -> bool {
        driver_id != "postgres"
    }

    pub fn supports_section(&self, section: SidebarSection) -> bool {
        match section {
            SidebarSection::Tables | SidebarSection::Queries => true,
            SidebarSection::RedisDatabases => self.supports_redis_databases,
            SidebarSection::Views => self.kinds.views,
            SidebarSection::MaterializedViews => self.kinds.materialized_views,
            SidebarSection::Triggers => self.kinds.triggers && self.triggers_top_level,
            SidebarSection::Functions => self.kinds.functions,
            SidebarSection::Procedures => self.kinds.procedures,
            SidebarSection::Events => self.kinds.events,
            SidebarSection::Sequences => self.kinds.sequences,
            SidebarSection::Domains => self.kinds.domains,
            SidebarSection::Types => self.kinds.types,
            SidebarSection::Extensions => self.kinds.extensions,
        }
    }
}

impl Default for SidebarObjectCapabilities {
    /// Fail-closed baseline for unknown drivers: tables and saved queries
    /// only, no schema level.
    fn default() -> Self {
        Self {
            supports_schemas: false,
            default_schema: None,
            kinds: ObjectKindSupport {
                tables: true,
                ..ObjectKindSupport::NONE
            },
            triggers_top_level: true,
            supports_redis_databases: false,
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
    pub query_text: String,
    pub folder: Option<String>,
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
    pub collections: Vec<DocumentCollectionInfo>,
    pub indexes: Vec<DocumentIndexInfo>,
    pub functions: Vec<DocumentFunctionInfo>,
    pub gridfs_buckets: Vec<DocumentGridFsBucketInfo>,
    pub users: Vec<DocumentAdminObjectInfo>,
    pub roles: Vec<DocumentAdminObjectInfo>,
    pub search_indexes: Vec<DocumentAdminObjectInfo>,
    pub vector_indexes: Vec<DocumentAdminObjectInfo>,
    pub server: Vec<DocumentAdminObjectInfo>,
    pub sharding: Vec<DocumentAdminObjectInfo>,
    pub collections_expanded: bool,
    pub collections_loading: bool,
    pub document_expanded_sections: HashSet<String>,
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
    pub expanded_schema_groups: HashSet<String>,
    /// Expanded section keys for grouped schemas.
    ///
    /// Legacy field name retained for backward compatibility.
    /// Keys are encoded as `<schema>::<section>`.
    pub expanded_schema_section_keys: HashSet<String>,
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
    /// Which sections are expanded in the tree.
    pub expanded_sections: HashSet<SidebarSection>,
    /// Which sections are currently being fetched from the server.
    pub loading_sections: HashSet<SidebarSection>,
}

impl From<SchemaObjects> for DatabaseSchemaData {
    fn from(schema: SchemaObjects) -> Self {
        let SchemaObjects {
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            events,
            sequences,
            domains,
            types,
            extensions,
            schema_name,
            schema_names,
        } = schema;

        Self {
            schema_name,
            schema_names,
            tables,
            views,
            materialized_views,
            triggers,
            functions,
            procedures,
            events,
            sequences,
            domains,
            types,
            extensions,
            ..Self::default()
        }
    }
}

impl DatabaseSchemaData {
    /// Objects and schema names from `self`, tree expansion state from
    /// `previous`. Loading state always resets — the data just landed.
    pub fn with_expansion_from(mut self, previous: Option<&DatabaseSchemaData>) -> Self {
        self.schema_expanded = previous.is_some_and(|previous| previous.schema_expanded);
        self.expanded_schema_groups = previous
            .map(|previous| previous.expanded_schema_groups.clone())
            .unwrap_or_default();
        self.expanded_schema_section_keys = previous
            .map(|previous| previous.expanded_schema_section_keys.clone())
            .unwrap_or_default();
        self.expanded_sections = previous
            .map(|previous| previous.expanded_sections.clone())
            .unwrap_or_default();
        self.loading_sections.clear();
        self
    }

    /// The schema-object lists paired with their sections in render order —
    /// the input shape for schema grouping.
    pub fn object_sections(&self) -> [(SidebarSection, &[String]); 11] {
        SidebarSection::OBJECT_SECTIONS.map(|section| (section, self.objects(section)))
    }

    pub fn objects(&self, section: SidebarSection) -> &[String] {
        match section {
            SidebarSection::Tables => &self.tables,
            SidebarSection::Views => &self.views,
            SidebarSection::MaterializedViews => &self.materialized_views,
            SidebarSection::Triggers => &self.triggers,
            SidebarSection::Functions => &self.functions,
            SidebarSection::Procedures => &self.procedures,
            SidebarSection::Events => &self.events,
            SidebarSection::Sequences => &self.sequences,
            SidebarSection::Domains => &self.domains,
            SidebarSection::Types => &self.types,
            SidebarSection::Extensions => &self.extensions,
            SidebarSection::Queries | SidebarSection::RedisDatabases => &[],
        }
    }

    pub fn objects_mut(&mut self, section: SidebarSection) -> Option<&mut Vec<String>> {
        match section {
            SidebarSection::Tables => Some(&mut self.tables),
            SidebarSection::Views => Some(&mut self.views),
            SidebarSection::MaterializedViews => Some(&mut self.materialized_views),
            SidebarSection::Triggers => Some(&mut self.triggers),
            SidebarSection::Functions => Some(&mut self.functions),
            SidebarSection::Procedures => Some(&mut self.procedures),
            SidebarSection::Events => Some(&mut self.events),
            SidebarSection::Sequences => Some(&mut self.sequences),
            SidebarSection::Domains => Some(&mut self.domains),
            SidebarSection::Types => Some(&mut self.types),
            SidebarSection::Extensions => Some(&mut self.extensions),
            SidebarSection::Queries | SidebarSection::RedisDatabases => None,
        }
    }
}

/// A database connection entry
#[derive(Clone, Debug)]
pub struct ConnectionEntry {
    pub id: Uuid,
    pub name: String,
    pub db_type: String,
    pub object_capabilities: SidebarObjectCapabilities,
    pub objects_panel_manifest: Option<ObjectsPanelManifest>,
    /// Which object actions the connection advertises, so menus can explain why
    /// an action is unavailable rather than failing after it is clicked.
    pub object_features: Option<ObjectFeatureSet>,
    pub is_connected: bool,
    pub is_connecting: bool,
    pub is_expanded: bool,
    pub queries: Vec<SavedQueryInfo>,
    pub queries_expanded: bool,
    pub redis_databases_expanded: bool,
    /// Redis databases (only used for Redis connections)
    pub redis_databases: Vec<RedisDatabaseInfo>,
    /// All databases on the server (for drivers that support listing them)
    pub databases: Vec<SidebarDatabaseInfo>,
    /// Schema state for the connection scope: what renders while no database
    /// node owns a copy — single-namespace drivers that never list databases,
    /// and the window between connecting and database discovery.
    ///
    /// Never read alongside a database's own [`SidebarDatabaseInfo::schema`];
    /// [`Self::schema`] and [`Self::schema_mut`] pick whichever of the two is
    /// live so the schema state has exactly one home at any moment.
    pub schema: DatabaseSchemaData,
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
            object_features: None,
            is_connected: false,
            is_connecting: false,
            is_expanded: false,
            queries: Vec::new(),
            queries_expanded: false,
            redis_databases_expanded: false,
            redis_databases: Vec::new(),
            databases: Vec::new(),
            schema: DatabaseSchemaData::default(),
        }
    }

    /// Whether this connection renders the key-value (numbered databases) tree.
    pub fn is_redis(&self) -> bool {
        driver_category_from_driver_name(&self.db_type) == DriverCategory::KeyValue
    }

    /// Whether this connection renders the document (collections) tree.
    pub fn is_document(&self) -> bool {
        driver_category_from_driver_name(&self.db_type) == DriverCategory::Document
    }

    pub fn set_db_type(&mut self, db_type: String) {
        self.object_capabilities = SidebarObjectCapabilities::for_driver(&db_type);
        self.db_type = db_type;
    }

    /// The schema state that renders for `database_name`, or for the active
    /// database when `None`.
    ///
    /// Databases without a loaded copy of their own render the connection-level
    /// state, so reads and writes must resolve the scope the same way.
    pub fn schema(&self, database_name: Option<&str>) -> &DatabaseSchemaData {
        self.databases
            .iter()
            .find(|database| Self::is_schema_owner(database, database_name))
            .and_then(|database| database.schema.as_ref())
            .unwrap_or(&self.schema)
    }

    pub fn schema_mut(&mut self, database_name: Option<&str>) -> &mut DatabaseSchemaData {
        let owner = self
            .databases
            .iter()
            .position(|database| Self::is_schema_owner(database, database_name));

        match owner {
            // `is_schema_owner` already required a loaded schema, so this
            // resolves the existing copy rather than inserting a default one.
            Some(owner) => self.databases[owner].schema.get_or_insert_default(),
            None => &mut self.schema,
        }
    }

    fn is_schema_owner(database: &SidebarDatabaseInfo, database_name: Option<&str>) -> bool {
        database.schema.is_some()
            && match database_name {
                Some(database_name) => database.name == database_name,
                None => database.is_active,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::{SidebarObjectCapabilities, SidebarSection};

    #[test]
    fn sqlite_hides_unsupported_sidebar_sections() {
        let capabilities = SidebarObjectCapabilities::for_driver("sqlite");

        assert!(capabilities.supports_section(SidebarSection::Views));
        assert!(!capabilities.supports_section(SidebarSection::MaterializedViews));
        assert!(capabilities.supports_section(SidebarSection::Triggers));
        assert!(!capabilities.supports_section(SidebarSection::Functions));
        assert!(!capabilities.supports_section(SidebarSection::Procedures));
        assert_eq!(capabilities.default_schema, Some("main"));
    }

    #[test]
    fn postgres_hides_top_level_triggers_but_keeps_other_objects() {
        let capabilities = SidebarObjectCapabilities::for_driver("postgres");

        assert!(capabilities.supports_section(SidebarSection::Views));
        assert!(capabilities.supports_section(SidebarSection::MaterializedViews));
        assert!(!capabilities.supports_section(SidebarSection::Triggers));
        assert!(capabilities.supports_section(SidebarSection::Functions));
        assert!(capabilities.supports_section(SidebarSection::Procedures));
        assert!(capabilities.supports_section(SidebarSection::Sequences));
        assert!(capabilities.supports_section(SidebarSection::Domains));
        assert!(capabilities.supports_section(SidebarSection::Extensions));
        assert!(!capabilities.supports_section(SidebarSection::Events));
        assert_eq!(capabilities.default_schema, Some("public"));
    }

    #[test]
    fn mysql_supports_event_sidebar_section() {
        let capabilities = SidebarObjectCapabilities::for_driver("mysql");

        assert!(capabilities.supports_section(SidebarSection::Events));
        assert!(!capabilities.supports_section(SidebarSection::MaterializedViews));
        assert!(capabilities.supports_section(SidebarSection::Triggers));
    }

    #[test]
    fn registry_aliases_resolve_to_the_same_capabilities() {
        assert_eq!(
            SidebarObjectCapabilities::for_driver("postgresql"),
            SidebarObjectCapabilities::for_driver("postgres"),
        );
        assert_eq!(
            SidebarObjectCapabilities::for_driver("mariadb"),
            SidebarObjectCapabilities::for_driver("mysql"),
        );
    }

    #[test]
    fn unknown_driver_fails_closed_to_tables_and_queries() {
        let capabilities = SidebarObjectCapabilities::for_driver("not-a-driver");

        assert!(capabilities.supports_section(SidebarSection::Tables));
        assert!(capabilities.supports_section(SidebarSection::Queries));
        assert!(!capabilities.supports_section(SidebarSection::Views));
        assert!(!capabilities.supports_schemas);
    }

    #[test]
    fn redis_only_keeps_saved_queries_tree() {
        let capabilities = SidebarObjectCapabilities::for_driver("redis");

        assert!(!capabilities.supports_section(SidebarSection::Views));
        assert!(!capabilities.supports_section(SidebarSection::MaterializedViews));
        assert!(!capabilities.supports_section(SidebarSection::Triggers));
        assert!(!capabilities.supports_section(SidebarSection::Functions));
        assert!(!capabilities.supports_section(SidebarSection::Procedures));
        assert!(capabilities.supports_section(SidebarSection::Queries));
        assert!(capabilities.supports_section(SidebarSection::RedisDatabases));
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
