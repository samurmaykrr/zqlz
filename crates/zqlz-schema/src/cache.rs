//! Schema cache for improved performance

use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zqlz_core::{ColumnInfo, IndexInfo, ObjectsPanelData, TableInfo};

/// Cached schema information
pub struct CachedSchema {
    pub tables: Vec<TableInfo>,
    pub columns: HashMap<String, Vec<ColumnInfo>>,
    #[allow(dead_code)]
    pub indexes: HashMap<String, Vec<IndexInfo>>,
    pub objects_panel_data: Option<ObjectsPanelData>,
    pub cached_at: Instant,
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

    /// Store tables in cache
    pub fn set_tables(&self, connection_id: Uuid, tables: Vec<TableInfo>) {
        tracing::debug!(connection_id = %connection_id, table_count = tables.len(), "caching tables");
        let mut cache = self.cache.write();
        let entry = cache.entry(connection_id).or_insert_with(|| CachedSchema {
            tables: Vec::new(),
            columns: HashMap::new(),
            indexes: HashMap::new(),
            objects_panel_data: None,
            cached_at: Instant::now(),
        });
        entry.tables = tables;
        entry.cached_at = Instant::now();
    }

    /// Store columns in cache
    pub fn set_columns(&self, connection_id: Uuid, table: &str, columns: Vec<ColumnInfo>) {
        tracing::debug!(connection_id = %connection_id, table = %table, column_count = columns.len(), "caching columns");
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&connection_id) {
            entry.columns.insert(table.to_string(), columns);
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
