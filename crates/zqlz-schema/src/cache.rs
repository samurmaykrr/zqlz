//! Schema cache for improved performance

use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zqlz_core::{
    ColumnInfo, FunctionInfo, IndexInfo, ObjectsPanelData, ProcedureInfo, TableInfo, TriggerInfo,
    ViewInfo,
};

/// Cached schema information
pub struct CachedSchema {
    pub tables: Vec<TableInfo>,
    pub columns: HashMap<String, Vec<ColumnInfo>>,
    pub indexes: HashMap<String, Vec<IndexInfo>>,
    pub views: Vec<ViewInfo>,
    pub materialized_views: Vec<ViewInfo>,
    pub triggers: Vec<TriggerInfo>,
    pub functions: Vec<FunctionInfo>,
    pub procedures: Vec<ProcedureInfo>,
    pub objects_panel_data: Option<ObjectsPanelData>,
    pub cached_at: Instant,
    /// Resolved database name (e.g. `mydb`). Cached so callers avoid repeated
    /// `SELECT DATABASE()` / `SELECT current_database()` round-trips.
    pub database_name: Option<String>,
    /// Resolved schema name (e.g. `public`). Cached for the same reason.
    pub schema_name: Option<String>,
}

impl CachedSchema {
    fn empty() -> Self {
        Self {
            tables: Vec::new(),
            columns: HashMap::new(),
            indexes: HashMap::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            triggers: Vec::new(),
            functions: Vec::new(),
            procedures: Vec::new(),
            objects_panel_data: None,
            cached_at: Instant::now(),
            database_name: None,
            schema_name: None,
        }
    }
}

/// Schema cache for a connection
pub struct SchemaCache {
    /// Cache per connection ID
    cache: RwLock<HashMap<Uuid, CachedSchema>>,

    /// Cache TTL
    ttl: Duration,
}

impl SchemaCache {
    /// Create a new schema cache
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Check if cache is valid for a connection
    pub fn is_valid(&self, connection_id: Uuid) -> bool {
        tracing::trace!(connection_id = %connection_id, "checking cache validity");
        if let Some(cached) = self.cache.read().get(&connection_id) {
            cached.cached_at.elapsed() < self.ttl
        } else {
            false
        }
    }

    /// Get cached tables
    pub fn get_tables(&self, connection_id: Uuid) -> Option<Vec<TableInfo>> {
        let cache = self.cache.read();
        let result = cache.get(&connection_id).map(|c| c.tables.clone());
        if result.is_some() {
            tracing::debug!(connection_id = %connection_id, "cache hit for tables");
        } else {
            tracing::debug!(connection_id = %connection_id, "cache miss for tables");
        }
        result
    }

    /// Get cached columns for a table
    pub fn get_columns(&self, connection_id: Uuid, table: &str) -> Option<Vec<ColumnInfo>> {
        let cache = self.cache.read();
        let result = cache
            .get(&connection_id)
            .and_then(|c| c.columns.get(table).cloned());
        if result.is_some() {
            tracing::debug!(connection_id = %connection_id, table = %table, "cache hit for columns");
        } else {
            tracing::debug!(connection_id = %connection_id, table = %table, "cache miss for columns");
        }
        result
    }

    /// Get all cached views
    pub fn get_views(&self, connection_id: Uuid) -> Option<Vec<ViewInfo>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.views.clone())
    }

    /// Get all cached materialized views
    pub fn get_materialized_views(&self, connection_id: Uuid) -> Option<Vec<ViewInfo>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.materialized_views.clone())
    }

    /// Get all cached triggers
    pub fn get_triggers(&self, connection_id: Uuid) -> Option<Vec<TriggerInfo>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.triggers.clone())
    }

    /// Get all cached functions
    pub fn get_functions(&self, connection_id: Uuid) -> Option<Vec<FunctionInfo>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.functions.clone())
    }

    /// Get all cached procedures
    pub fn get_procedures(&self, connection_id: Uuid) -> Option<Vec<ProcedureInfo>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.procedures.clone())
    }

    /// Get the cached resolved database name for a connection, if any.
    pub fn get_database_name(&self, connection_id: Uuid) -> Option<String> {
        self.cache
            .read()
            .get(&connection_id)
            .and_then(|c| c.database_name.clone())
    }

    /// Get the cached resolved schema name for a connection, if any.
    pub fn get_schema_name(&self, connection_id: Uuid) -> Option<String> {
        self.cache
            .read()
            .get(&connection_id)
            .and_then(|c| c.schema_name.clone())
    }

    /// Store resolved database and schema names. Creates the cache entry if it
    /// does not yet exist so this can be called before `set_tables`.
    pub fn set_connection_names(
        &self,
        connection_id: Uuid,
        database_name: Option<String>,
        schema_name: Option<String>,
    ) {
        let mut cache = self.cache.write();
        let entry = cache
            .entry(connection_id)
            .or_insert_with(|| CachedSchema::empty());
        entry.database_name = database_name;
        entry.schema_name = schema_name;
    }

    /// Store tables in cache. Creates the entry if it does not yet exist.
    pub fn set_tables(&self, connection_id: Uuid, tables: Vec<TableInfo>) {
        tracing::debug!(connection_id = %connection_id, table_count = tables.len(), "caching tables");
        let mut cache = self.cache.write();
        let entry = cache
            .entry(connection_id)
            .or_insert_with(|| CachedSchema::empty());
        entry.tables = tables;
        entry.cached_at = Instant::now();
    }

    /// Store columns in cache. Creates the entry if it does not yet exist so
    /// that callers do not need to call `set_tables` first.
    pub fn set_columns(&self, connection_id: Uuid, table: &str, columns: Vec<ColumnInfo>) {
        tracing::debug!(connection_id = %connection_id, table = %table, column_count = columns.len(), "caching columns");
        let mut cache = self.cache.write();
        let entry = cache
            .entry(connection_id)
            .or_insert_with(|| CachedSchema::empty());
        entry.columns.insert(table.to_string(), columns);
    }

    /// Get all cached indexes (keyed by table name)
    pub fn get_all_indexes(&self, connection_id: Uuid) -> Option<HashMap<String, Vec<IndexInfo>>> {
        self.cache
            .read()
            .get(&connection_id)
            .map(|c| c.indexes.clone())
    }

    /// Store all table indexes in cache.
    pub fn set_all_indexes(&self, connection_id: Uuid, indexes: HashMap<String, Vec<IndexInfo>>) {
        tracing::debug!(connection_id = %connection_id, table_count = indexes.len(), "caching table indexes");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.indexes = indexes;
        }
    }

    /// Store views in cache
    pub fn set_views(&self, connection_id: Uuid, views: Vec<ViewInfo>) {
        tracing::debug!(connection_id = %connection_id, view_count = views.len(), "caching views");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.views = views;
        }
    }

    /// Store materialized views in cache
    pub fn set_materialized_views(&self, connection_id: Uuid, views: Vec<ViewInfo>) {
        tracing::debug!(connection_id = %connection_id, view_count = views.len(), "caching materialized views");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.materialized_views = views;
        }
    }

    /// Store triggers in cache
    pub fn set_triggers(&self, connection_id: Uuid, triggers: Vec<TriggerInfo>) {
        tracing::debug!(connection_id = %connection_id, trigger_count = triggers.len(), "caching triggers");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.triggers = triggers;
        }
    }

    /// Store functions in cache
    pub fn set_functions(&self, connection_id: Uuid, functions: Vec<FunctionInfo>) {
        tracing::debug!(connection_id = %connection_id, function_count = functions.len(), "caching functions");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.functions = functions;
        }
    }

    /// Store procedures in cache
    pub fn set_procedures(&self, connection_id: Uuid, procedures: Vec<ProcedureInfo>) {
        tracing::debug!(connection_id = %connection_id, procedure_count = procedures.len(), "caching procedures");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.procedures = procedures;
        }
    }

    /// Get cached objects panel data
    pub fn get_objects_panel_data(&self, connection_id: Uuid) -> Option<ObjectsPanelData> {
        self.cache
            .read()
            .get(&connection_id)
            .and_then(|c| c.objects_panel_data.clone())
    }

    /// Store objects panel data in cache
    pub fn set_objects_panel_data(&self, connection_id: Uuid, data: ObjectsPanelData) {
        tracing::debug!(connection_id = %connection_id, "caching objects panel data");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.objects_panel_data = Some(data);
        }
    }

    /// Invalidate cache for a connection
    pub fn invalidate(&self, connection_id: Uuid) {
        tracing::info!(connection_id = %connection_id, "invalidating schema cache");
        self.cache.write().remove(&connection_id);
    }

    /// Clear all caches
    pub fn clear(&self) {
        let count = self.cache.read().len();
        tracing::info!(cache_entries = count, "clearing all schema caches");
        self.cache.write().clear();
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(300)) // 5 minute TTL
    }
}
