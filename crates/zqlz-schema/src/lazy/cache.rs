//! Lazy loading cache for large database schemas
//!
//! This module provides a lazy loading cache that only fetches schema data
//! when first accessed, avoiding upfront loading of entire large schemas.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zqlz_core::{ColumnInfo, IndexInfo, TableDetails, TableInfo};

/// Cache entry state for lazy loading
#[derive(Debug, Clone)]
pub enum CacheEntry<T: Clone> {
    /// Data has been loaded and is available
    Loaded(T),
    /// Data is currently being loaded (prevents duplicate loads)
    Loading,
    /// Data has not been loaded yet
    NotLoaded,
}

impl<T: Clone> CacheEntry<T> {
    /// Check if this entry is loaded
    pub fn is_loaded(&self) -> bool {
        matches!(self, CacheEntry::Loaded(_))
    }

    /// Check if this entry is currently loading
    pub fn is_loading(&self) -> bool {
        matches!(self, CacheEntry::Loading)
    }

    /// Check if this entry has not been loaded
    pub fn is_not_loaded(&self) -> bool {
        matches!(self, CacheEntry::NotLoaded)
    }

    /// Get the loaded value if available
    pub fn get(&self) -> Option<&T> {
        match self {
            CacheEntry::Loaded(data) => Some(data),
            _ => None,
        }
    }
}

/// Entry with timestamp tracking for per-item TTL
#[derive(Debug, Clone)]
struct TimestampedEntry<T: Clone> {
    entry: CacheEntry<T>,
    loaded_at: Option<Instant>,
}

impl<T: Clone> TimestampedEntry<T> {
    fn not_loaded() -> Self {
        Self {
            entry: CacheEntry::NotLoaded,
            loaded_at: None,
        }
    }

    fn loading() -> Self {
        Self {
            entry: CacheEntry::Loading,
            loaded_at: None,
        }
    }

    fn loaded(data: T) -> Self {
        Self {
            entry: CacheEntry::Loaded(data),
            loaded_at: Some(Instant::now()),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        match self.loaded_at {
            Some(time) => time.elapsed() >= ttl,
            None => false,
        }
    }
}

/// Configuration for lazy schema cache
#[derive(Debug, Clone)]
pub struct LazyCacheConfig {
    /// TTL for table/view lists (less frequent changes)
    pub list_ttl: Duration,
    /// TTL for column/index details (medium frequency)
    pub detail_ttl: Duration,
    /// TTL for table details with row counts (more frequent updates)
    pub stats_ttl: Duration,
}

impl Default for LazyCacheConfig {
    fn default() -> Self {
        Self {
            list_ttl: Duration::from_secs(300),   // 5 minutes
            detail_ttl: Duration::from_secs(180), // 3 minutes
            stats_ttl: Duration::from_secs(60),   // 1 minute
        }
    }
}

impl LazyCacheConfig {
    /// Create a config with custom TTLs
    pub fn new(list_ttl: Duration, detail_ttl: Duration, stats_ttl: Duration) -> Self {
        Self {
            list_ttl,
            detail_ttl,
            stats_ttl,
        }
    }

    /// Create a config where all TTLs are the same
    pub fn uniform(ttl: Duration) -> Self {
        Self {
            list_ttl: ttl,
            detail_ttl: ttl,
            stats_ttl: ttl,
        }
    }
}

/// Cache data for a single connection
#[derive(Debug)]
struct ConnectionCache {
    /// Cached table list
    tables: TimestampedEntry<Vec<TableInfo>>,
    /// Cached columns per table
    columns: HashMap<String, TimestampedEntry<Vec<ColumnInfo>>>,
    /// Cached indexes per table
    indexes: HashMap<String, TimestampedEntry<Vec<IndexInfo>>>,
    /// Cached full table details
    table_details: HashMap<String, TimestampedEntry<TableDetails>>,
}

impl Default for ConnectionCache {
    fn default() -> Self {
        Self {
            tables: TimestampedEntry::not_loaded(),
            columns: HashMap::new(),
            indexes: HashMap::new(),
            table_details: HashMap::new(),
        }
    }
}

/// Lazy loading schema cache for large database schemas
///
/// This cache provides:
/// - Lazy loading: data is only fetched when first accessed
/// - Concurrent access protection: prevents duplicate loads
/// - Per-item TTL: different data types can have different TTLs
/// - Prefetch support: load multiple schemas in advance
pub struct LazySchemaCache {
    /// Cache per connection ID
    cache: RwLock<HashMap<Uuid, ConnectionCache>>,
    /// Cache configuration
    config: LazyCacheConfig,
}

impl LazySchemaCache {
    /// Create a new lazy schema cache with custom configuration
    pub fn new(config: LazyCacheConfig) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Create a lazy schema cache with default configuration
    pub fn with_defaults() -> Self {
        Self::new(LazyCacheConfig::default())
    }

    /// Get the current configuration
    pub fn config(&self) -> &LazyCacheConfig {
        &self.config
    }

    // ========== Table List Operations ==========

    /// Get the cache state for tables
    pub fn get_tables_state(&self, connection_id: Uuid) -> CacheEntry<Vec<TableInfo>> {
        let cache = self.cache.read();
        if let Some(conn_cache) = cache.get(&connection_id) {
            if conn_cache.tables.is_expired(self.config.list_ttl) {
                tracing::debug!(
                    connection_id = %connection_id,
                    "tables cache expired"
                );
                CacheEntry::NotLoaded
            } else {
                conn_cache.tables.entry.clone()
            }
        } else {
            CacheEntry::NotLoaded
        }
    }

    /// Get cached tables if available and not expired
    pub fn get_tables(&self, connection_id: Uuid) -> Option<Vec<TableInfo>> {
        match self.get_tables_state(connection_id) {
            CacheEntry::Loaded(tables) => Some(tables),
            _ => None,
        }
    }

    /// Mark tables as loading to prevent duplicate loads
    /// Returns true if we successfully set loading state, false if already loading
    pub fn set_tables_loading(&self, connection_id: Uuid) -> bool {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();

        match &conn_cache.tables.entry {
            CacheEntry::Loading => {
                tracing::debug!(
                    connection_id = %connection_id,
                    "tables already loading, skipping"
                );
                false
            }
            _ => {
                conn_cache.tables = TimestampedEntry::loading();
                tracing::debug!(
                    connection_id = %connection_id,
                    "marked tables as loading"
                );
                true
            }
        }
    }

    /// Store loaded tables in cache
    pub fn set_tables(&self, connection_id: Uuid, tables: Vec<TableInfo>) {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();
        tracing::debug!(
            connection_id = %connection_id,
            table_count = tables.len(),
            "cached tables"
        );
        conn_cache.tables = TimestampedEntry::loaded(tables);
    }

    // ========== Column Operations ==========

    /// Get the cache state for columns of a table
    pub fn get_columns_state(
        &self,
        connection_id: Uuid,
        table: &str,
    ) -> CacheEntry<Vec<ColumnInfo>> {
        let cache = self.cache.read();
        if let Some(conn_cache) = cache.get(&connection_id) {
            if let Some(entry) = conn_cache.columns.get(table) {
                if entry.is_expired(self.config.detail_ttl) {
                    CacheEntry::NotLoaded
                } else {
                    entry.entry.clone()
                }
            } else {
                CacheEntry::NotLoaded
            }
        } else {
            CacheEntry::NotLoaded
        }
    }

    /// Get cached columns if available and not expired
    pub fn get_columns(&self, connection_id: Uuid, table: &str) -> Option<Vec<ColumnInfo>> {
        match self.get_columns_state(connection_id, table) {
            CacheEntry::Loaded(columns) => Some(columns),
            _ => None,
        }
    }

    /// Mark columns as loading to prevent duplicate loads
    pub fn set_columns_loading(&self, connection_id: Uuid, table: &str) -> bool {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();

        if let Some(entry) = conn_cache.columns.get(table) {
            if entry.entry.is_loading() {
                return false;
            }
        }

        conn_cache
            .columns
            .insert(table.to_string(), TimestampedEntry::loading());
        true
    }

    /// Store loaded columns in cache
    pub fn set_columns(&self, connection_id: Uuid, table: &str, columns: Vec<ColumnInfo>) {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();
        tracing::debug!(
            connection_id = %connection_id,
            table = %table,
            column_count = columns.len(),
            "cached columns"
        );
        conn_cache
            .columns
            .insert(table.to_string(), TimestampedEntry::loaded(columns));
    }

    // ========== Index Operations ==========

    /// Get cached indexes if available and not expired
    pub fn get_indexes(&self, connection_id: Uuid, table: &str) -> Option<Vec<IndexInfo>> {
        let cache = self.cache.read();
        if let Some(conn_cache) = cache.get(&connection_id) {
            if let Some(entry) = conn_cache.indexes.get(table) {
                if entry.is_expired(self.config.detail_ttl) {
                    return None;
                }
                return entry.entry.get().cloned();
            }
        }
        None
    }

    /// Mark indexes as loading
    pub fn set_indexes_loading(&self, connection_id: Uuid, table: &str) -> bool {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();

        if let Some(entry) = conn_cache.indexes.get(table) {
            if entry.entry.is_loading() {
                return false;
            }
        }

        conn_cache
            .indexes
            .insert(table.to_string(), TimestampedEntry::loading());
        true
    }

    /// Store loaded indexes in cache
    pub fn set_indexes(&self, connection_id: Uuid, table: &str, indexes: Vec<IndexInfo>) {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();
        conn_cache
            .indexes
            .insert(table.to_string(), TimestampedEntry::loaded(indexes));
    }

    // ========== Table Details Operations ==========

    /// Get cached table details if available and not expired
    pub fn get_table_details(&self, connection_id: Uuid, table: &str) -> Option<TableDetails> {
        let cache = self.cache.read();
        if let Some(conn_cache) = cache.get(&connection_id) {
            if let Some(entry) = conn_cache.table_details.get(table) {
                if entry.is_expired(self.config.stats_ttl) {
                    return None;
                }
                return entry.entry.get().cloned();
            }
        }
        None
    }

    /// Mark table details as loading
    pub fn set_table_details_loading(&self, connection_id: Uuid, table: &str) -> bool {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();

        if let Some(entry) = conn_cache.table_details.get(table) {
            if entry.entry.is_loading() {
                return false;
            }
        }

        conn_cache
            .table_details
            .insert(table.to_string(), TimestampedEntry::loading());
        true
    }

    /// Store loaded table details in cache
    pub fn set_table_details(&self, connection_id: Uuid, table: &str, details: TableDetails) {
        let mut cache = self.cache.write();
        let conn_cache = cache.entry(connection_id).or_default();
        conn_cache
            .table_details
            .insert(table.to_string(), TimestampedEntry::loaded(details));
    }

    // ========== Prefetch Operations ==========

    /// Get the list of tables that need loading for prefetch
    pub fn tables_to_prefetch(&self, connection_ids: &[Uuid]) -> Vec<Uuid> {
        let cache = self.cache.read();
        connection_ids
            .iter()
            .copied()
            .filter(|id| {
                cache
                    .get(id)
                    .map(|c| {
                        c.tables.entry.is_not_loaded() || c.tables.is_expired(self.config.list_ttl)
                    })
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Get tables that need columns loaded for a connection
    pub fn tables_needing_columns(&self, connection_id: Uuid, tables: &[String]) -> Vec<String> {
        let cache = self.cache.read();
        if let Some(conn_cache) = cache.get(&connection_id) {
            tables
                .iter()
                .filter(|t| {
                    conn_cache
                        .columns
                        .get(*t)
                        .map(|entry| {
                            entry.entry.is_not_loaded() || entry.is_expired(self.config.detail_ttl)
                        })
                        .unwrap_or(true)
                })
                .cloned()
                .collect()
        } else {
            tables.to_vec()
        }
    }

    // ========== Cache Management ==========

    /// Invalidate all cache entries for a connection
    pub fn invalidate(&self, connection_id: Uuid) {
        tracing::info!(connection_id = %connection_id, "invalidating lazy schema cache");
        self.cache.write().remove(&connection_id);
    }

    /// Invalidate specific table data for a connection
    pub fn invalidate_table(&self, connection_id: Uuid, table: &str) {
        let mut cache = self.cache.write();
        if let Some(conn_cache) = cache.get_mut(&connection_id) {
            conn_cache.columns.remove(table);
            conn_cache.indexes.remove(table);
            conn_cache.table_details.remove(table);
            tracing::debug!(
                connection_id = %connection_id,
                table = %table,
                "invalidated table cache"
            );
        }
    }

    /// Clear all caches
    pub fn clear(&self) {
        let count = self.cache.read().len();
        tracing::info!(cache_entries = count, "clearing all lazy schema caches");
        self.cache.write().clear();
    }

    /// Get cache statistics for monitoring
    pub fn stats(&self) -> LazyCacheStats {
        let cache = self.cache.read();
        let mut stats = LazyCacheStats::default();

        for conn_cache in cache.values() {
            stats.connection_count += 1;

            if conn_cache.tables.entry.is_loaded() {
                stats.tables_loaded += 1;
            }

            stats.columns_loaded += conn_cache
                .columns
                .values()
                .filter(|e| e.entry.is_loaded())
                .count();

            stats.indexes_loaded += conn_cache
                .indexes
                .values()
                .filter(|e| e.entry.is_loaded())
                .count();

            stats.table_details_loaded += conn_cache
                .table_details
                .values()
                .filter(|e| e.entry.is_loaded())
                .count();
        }

        stats
    }
}

impl Default for LazySchemaCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Statistics about the lazy cache state
#[derive(Debug, Clone, Default)]
pub struct LazyCacheStats {
    /// Number of connections in cache
    pub connection_count: usize,
    /// Number of connections with tables loaded
    pub tables_loaded: usize,
    /// Total number of tables with columns loaded
    pub columns_loaded: usize,
    /// Total number of tables with indexes loaded
    pub indexes_loaded: usize,
    /// Total number of tables with full details loaded
    pub table_details_loaded: usize,
}

impl LazyCacheStats {
    /// Total number of cached items
    pub fn total_items(&self) -> usize {
        self.tables_loaded + self.columns_loaded + self.indexes_loaded + self.table_details_loaded
    }
}

/// Thread-safe wrapper for sharing LazySchemaCache
pub type SharedLazySchemaCache = Arc<LazySchemaCache>;

/// Create a new shared lazy schema cache
pub fn new_shared_cache() -> SharedLazySchemaCache {
    Arc::new(LazySchemaCache::default())
}

/// Create a new shared lazy schema cache with custom config
pub fn new_shared_cache_with_config(config: LazyCacheConfig) -> SharedLazySchemaCache {
    Arc::new(LazySchemaCache::new(config))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
