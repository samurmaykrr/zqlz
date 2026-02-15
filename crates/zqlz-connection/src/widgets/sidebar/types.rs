//! Type definitions for sidebar data structures

use uuid::Uuid;

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

/// Schema objects for a single database, used in the sidebar tree
#[derive(Clone, Debug, Default)]
pub struct DatabaseSchemaData {
    pub schema_name: Option<String>,
    pub schema_expanded: bool,
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
}

/// A database connection entry
#[derive(Clone, Debug)]
pub struct ConnectionEntry {
    pub id: Uuid,
    pub name: String,
    pub db_type: String,
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
    /// Redis databases (only used for Redis connections)
    pub redis_databases: Vec<RedisDatabaseInfo>,
    /// Whether Redis databases section is expanded
    pub redis_databases_expanded: bool,
    /// All databases on the server (for drivers that support listing them)
    pub databases: Vec<SidebarDatabaseInfo>,
    /// The schema name for hierarchy display (e.g. "public")
    pub schema_name: Option<String>,
    /// Whether the schema-level node is expanded
    pub schema_expanded: bool,
}

impl ConnectionEntry {
    pub fn new(id: Uuid, name: String, db_type: String) -> Self {
        Self {
            id,
            name,
            db_type,
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
            tables_expanded: true,
            views_expanded: false,
            materialized_views_expanded: false,
            triggers_expanded: false,
            functions_expanded: false,
            procedures_expanded: false,
            queries_expanded: true,
            redis_databases: Vec::new(),
            redis_databases_expanded: true,
            databases: Vec::new(),
            schema_name: None,
            schema_expanded: true,
        }
    }

    /// Check if this is a Redis connection
    pub fn is_redis(&self) -> bool {
        self.db_type == "redis"
    }
}
