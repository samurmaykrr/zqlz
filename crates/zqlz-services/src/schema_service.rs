//! Schema introspection service with caching
//!
//! Provides centralized schema operations with automatic caching to reduce
//! database round-trips.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;
use zqlz_core::{Connection, DatabaseObject, FunctionInfo, ObjectType, ObjectsPanelData, ProcedureInfo, TableInfo, TriggerInfo, ViewInfo};
use zqlz_schema::SchemaCache;
use futures::future::join_all;

use crate::error::{ServiceError, ServiceResult};
use crate::view_models::{ColumnInfo, DatabaseSchema, TableDetails};

/// Batch size for prefetching table details. Small enough to avoid saturating
/// remote connection poolers (e.g. Neon PgBouncer) while still providing
/// some parallelism.
const PREFETCH_BATCH_SIZE: usize = 2;

/// How long to wait before starting the prefetch background task, giving the
/// user's first browse query a clear path to the database.
const PREFETCH_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// Pause between batches so prefetch never monopolises the connection pool.
const PREFETCH_INTER_BATCH_DELAY: std::time::Duration = std::time::Duration::from_millis(200);

/// Service for schema introspection operations
///
/// This service wraps the `SchemaCache` and provides:
/// - Automatic caching of schema metadata
/// - Batch loading of schema objects
/// - Graceful handling of partial failures
/// - UI-friendly schema models
pub struct SchemaService {
    cache: Arc<SchemaCache>,
    /// Per-connection, per-table cache for full TableDetails. TTL is governed
    /// by `self.cache.is_valid(connection_id)` — entries are considered stale
    /// whenever the main schema cache expires or is invalidated, so they share
    /// one consistent TTL domain rather than having independent timestamps.
    table_details_cache: RwLock<HashMap<(Uuid, String), TableDetails>>,
    /// Per-connection, per-table cache for generated DDL strings. Keyed by
    /// `(connection_id, table_name)` and shares the same TTL domain as
    /// `table_details_cache` — cleared together on invalidation.
    ddl_cache: RwLock<HashMap<(Uuid, String), String>>,
}

impl SchemaService {
    /// Create a new schema service
    pub fn new() -> Self {
        Self {
            cache: Arc::new(SchemaCache::new(std::time::Duration::from_secs(300))), // 5 minutes
            table_details_cache: RwLock::new(HashMap::new()),
            ddl_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Create a schema service with a custom cache
    pub fn with_cache(cache: Arc<SchemaCache>) -> Self {
        Self {
            cache,
            table_details_cache: RwLock::new(HashMap::new()),
            ddl_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Load full database schema with caching
    ///
    /// This method loads all schema objects (tables, views, triggers, etc.) and
    /// caches them for future use. Partial failures are handled gracefully.
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection to introspect
    /// * `connection_id` - UUID for cache key generation
    ///
    /// # Returns
    ///
    /// A `DatabaseSchema` containing all discovered objects
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_database_schema(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<DatabaseSchema> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Check cache validity
        if self.cache.is_valid(connection_id) {
            if let Some(cached_tables) = self.cache.get_tables(connection_id) {
                tracing::debug!("Schema cache hit for connection {}", connection_id);

                let tables: Vec<String> = cached_tables.iter().map(|t| t.name.clone()).collect();
                let objects_panel_data = self.cache.get_objects_panel_data(connection_id);
                let views = self.cache.get_views(connection_id).unwrap_or_default();
                let materialized_views = self.cache.get_materialized_views(connection_id).unwrap_or_default();
                let triggers = self.cache.get_triggers(connection_id).unwrap_or_default();
                let functions = self.cache.get_functions(connection_id).unwrap_or_default();
                let procedures = self.cache.get_procedures(connection_id).unwrap_or_default();
                let table_indexes = self.cache.get_all_indexes(connection_id).unwrap_or_default();
                let database_name = self.cache.get_database_name(connection_id);
                let schema_name = self.cache.get_schema_name(connection_id);

                return Ok(DatabaseSchema {
                    table_infos: cached_tables,
                    objects_panel_data,
                    tables,
                    views: views.into_iter().map(|v| v.name).collect(),
                    materialized_views: materialized_views.into_iter().map(|v| v.name).collect(),
                    triggers: triggers.into_iter().map(|t| t.name).collect(),
                    functions: functions.into_iter().map(|f| f.name).collect(),
                    procedures: procedures.into_iter().map(|p| p.name).collect(),
                    table_indexes,
                    database_name,
                    schema_name,
                });
            }
        }

        tracing::debug!("Schema cache miss, loading from database");

        // Resolve current database and schema names BEFORE introspection so we can
        // pass them as the schema parameter. This is critical for MySQL: when no
        // default database is selected, DATABASE() returns NULL, causing all
        // information_schema queries with `TABLE_SCHEMA = DATABASE()` to return
        // zero rows. By resolving the name first we can pass it explicitly.
        let (db_query, schema_query) = match connection.driver_name() {
            "mysql" => ("SELECT DATABASE()", "SELECT DATABASE()"),
            "mssql" => ("SELECT DB_NAME()", "SELECT SCHEMA_NAME()"),
            "sqlite" => ("SELECT 'main'", "SELECT 'main'"),
            _ => ("SELECT current_database()", "SELECT current_schema()"),
        };
        let database_name = connection
            .query(db_query, &[])
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.get(0).and_then(|v| v.as_str().map(|s| s.to_string()))));
        let schema_name = connection
            .query(schema_query, &[])
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.get(0).and_then(|v| v.as_str().map(|s| s.to_string()))));

        // For MySQL/MSSQL the "schema" parameter to introspection methods is the
        // database name. For PostgreSQL it's the schema name (e.g. "public").
        let introspection_schema = match connection.driver_name() {
            "mysql" | "mssql" => database_name.as_deref(),
            "sqlite" => None, // SQLite always uses the single attached database
            _ => schema_name.as_deref(),
        };

        // Load all schema objects (handle partial failures gracefully)
        let tables_result = schema.list_tables(introspection_schema).await;
        let extended_result = schema.list_tables_extended(introspection_schema).await;
        let views_result = schema.list_views(introspection_schema).await;
        let materialized_views_result = schema.list_materialized_views(introspection_schema).await;
        // PostgreSQL triggers are table-level objects, not top-level sidebar items.
        // Skip loading them for PG to avoid cluttering the sidebar.
        let triggers_result = match connection.driver_name() {
            "postgres" => Ok(Vec::new()),
            _ => schema.list_triggers(introspection_schema, None).await,
        };
        let functions_result = schema.list_functions(introspection_schema).await;
        let procedures_result = schema.list_procedures(introspection_schema).await;

        let tables = tables_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load tables: {}", e);
            Vec::new()
        });

        let objects_panel_data = extended_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load extended objects panel data: {}", e);
            ObjectsPanelData::from_table_infos(tables.clone())
        });

        let views = views_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load views: {}", e);
            Vec::new()
        });

        let materialized_views = materialized_views_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load materialized views: {}", e);
            Vec::new()
        });

        let triggers = triggers_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load triggers: {}", e);
            Vec::new()
        });

        let functions = functions_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load functions: {}", e);
            Vec::new()
        });

        let procedures = procedures_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load procedures: {}", e);
            Vec::new()
        });

        // Load indexes for each table (best effort)
        let mut table_indexes = std::collections::HashMap::new();
        for table in &tables {
            if let Ok(indexes) = schema.get_indexes(introspection_schema, &table.name).await {
                table_indexes.insert(table.name.clone(), indexes);
            }
        }

        // Cache tables and all other schema objects for future use
        if !tables.is_empty() {
            self.cache.set_tables(connection_id, tables.clone());
            self.cache.set_connection_names(connection_id, database_name.clone(), schema_name.clone());
            self.cache
                .set_objects_panel_data(connection_id, objects_panel_data.clone());
            self.cache.set_views(connection_id, views.clone());
            self.cache.set_materialized_views(connection_id, materialized_views.clone());
            self.cache.set_triggers(connection_id, triggers.clone());
            self.cache.set_functions(connection_id, functions.clone());
            self.cache.set_procedures(connection_id, procedures.clone());
            self.cache.set_all_indexes(connection_id, table_indexes.clone());
        }

        let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
        let materialized_view_names: Vec<String> = materialized_views.into_iter().map(|v| v.name).collect();
        let db_schema = DatabaseSchema {
            table_infos: tables,
            objects_panel_data: Some(objects_panel_data),
            tables: table_names,
            views: views.into_iter().map(|v| v.name).collect(),
            materialized_views: materialized_view_names,
            triggers: triggers.into_iter().map(|t| t.name).collect(),
            functions: functions.into_iter().map(|f| f.name).collect(),
            procedures: procedures.into_iter().map(|p| p.name).collect(),
            table_indexes,
            database_name,
            schema_name,
        };

        tracing::info!(
            tables = db_schema.tables.len(),
            views = db_schema.views.len(),
            materialized_views = db_schema.materialized_views.len(),
            triggers = db_schema.triggers.len(),
            functions = db_schema.functions.len(),
            procedures = db_schema.procedures.len(),
            database_name = ?db_schema.database_name,
            schema_name = ?db_schema.schema_name,
            "Schema loaded successfully"
        );

        Ok(db_schema)
    }

    /// Load just the table names quickly (first priority)
    ///
    /// This is the fastest schema query and should be done first to populate
    /// the sidebar with basic table list.
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_tables_only(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<TableInfo>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let tables = schema.list_tables(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load tables: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        // Populate cache so subsequent load_database_schema calls can use cached data.
        // Also purge any stale table-details entries for this connection so that
        // reconnects don't serve details from the previous schema generation.
        self.cache.set_tables(connection_id, tables.clone());
        self.table_details_cache
            .write()
            .retain(|(conn_id, _), _| *conn_id != connection_id);
        tracing::debug!("Cached {} tables for connection {}", tables.len(), connection_id);

        tracing::info!("Loaded {} tables", tables.len());
        Ok(tables)
    }

    /// Load views for a connection
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_views(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<ViewInfo>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let views = schema.list_views(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load views: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} views", views.len());
        Ok(views)
    }

    /// Load materialized views for a connection
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_materialized_views(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<ViewInfo>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let views = schema.list_materialized_views(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load materialized views: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} materialized views", views.len());
        Ok(views)
    }

    /// Load functions for a connection
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_functions(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<FunctionInfo>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let functions = schema.list_functions(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load functions: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} functions", functions.len());
        Ok(functions)
    }

    /// Load procedures for a connection
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_procedures(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<ProcedureInfo>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let procedures = schema.list_procedures(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load procedures: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} procedures", procedures.len());
        Ok(procedures)
    }

    /// Load triggers for a connection
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id))]
    pub async fn load_triggers(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<TriggerInfo>> {
        // Skip triggers for PostgreSQL (they're table-level)
        if connection.driver_name() == "postgres" {
            return Ok(Vec::new());
        }

        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self.get_introspection_schema_cached(&connection, connection_id).await;

        let triggers = schema.list_triggers(introspection_schema.as_deref(), None).await.map_err(|e| {
            tracing::error!("Failed to load triggers: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} triggers", triggers.len());
        Ok(triggers)
    }

    /// Helper to get the introspection schema parameter.
    ///
    /// When a `connection_id` is provided, the resolved names are cached so
    /// subsequent calls for the same connection avoid additional round-trips.
    pub async fn get_introspection_schema(
        &self,
        connection: &Arc<dyn Connection>,
    ) -> Option<String> {
        self.get_introspection_schema_impl(connection, None).await
    }

    /// Like [`get_introspection_schema`] but reads from and writes to the
    /// schema cache, so repeated calls within one connect sequence only query
    /// the database once.
    pub async fn get_introspection_schema_cached(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> Option<String> {
        self.get_introspection_schema_impl(connection, Some(connection_id)).await
    }

    /// Return the current database name (e.g. `"neondb"`, `"mydb"`) for the
    /// connection, using the cache to avoid redundant round-trips.
    ///
    /// Unlike [`get_introspection_schema_cached`], this always returns the
    /// database-level name regardless of driver, making it suitable for
    /// marking which database node is active in the sidebar.
    pub async fn get_database_name_cached(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> Option<String> {
        if connection.driver_name() == "sqlite" {
            return None;
        }

        // Warm the cache if not already done (shared with get_introspection_schema_cached)
        if self.cache.is_valid(connection_id) {
            let cached_db = self.cache.get_database_name(connection_id);
            let cached_schema = self.cache.get_schema_name(connection_id);
            if cached_db.is_some() || cached_schema.is_some() {
                return cached_db;
            }
        }

        // Not in cache yet — fetch and populate via the shared impl, then read
        // back the database name directly from the cache.
        let _ = self.get_introspection_schema_impl(connection, Some(connection_id)).await;
        self.cache.get_database_name(connection_id)
    }

    async fn get_introspection_schema_impl(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Option<Uuid>,
    ) -> Option<String> {
        if connection.driver_name() == "sqlite" {
            return None;
        }

        // Check cache first to avoid redundant queries
        if let Some(conn_id) = connection_id {
            if self.cache.is_valid(conn_id) {
                let cached_db = self.cache.get_database_name(conn_id);
                let cached_schema = self.cache.get_schema_name(conn_id);
                if cached_db.is_some() || cached_schema.is_some() {
                    return match connection.driver_name() {
                        "mysql" | "mssql" => cached_db,
                        _ => cached_schema,
                    };
                }
            }
        }

        let (db_query, schema_query) = match connection.driver_name() {
            "mysql" => ("SELECT DATABASE()", "SELECT DATABASE()"),
            "mssql" => ("SELECT DB_NAME()", "SELECT SCHEMA_NAME()"),
            _ => ("SELECT current_database()", "SELECT current_schema()"),
        };

        let database_name = connection
            .query(db_query, &[])
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.get(0).and_then(|v| v.as_str().map(|s| s.to_string()))));
        let schema_name = connection
            .query(schema_query, &[])
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.get(0).and_then(|v| v.as_str().map(|s| s.to_string()))));

        if let Some(conn_id) = connection_id {
            self.cache.set_connection_names(conn_id, database_name.clone(), schema_name.clone());
        }

        match connection.driver_name() {
            "mysql" | "mssql" => database_name,
            _ => schema_name,
        }
    }

    /// Get detailed table information
    ///
    /// Loads all metadata for a specific table including columns, indexes,
    /// foreign keys, and primary key information.
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `connection_id` - UUID for cache key generation
    /// * `table_name` - Name of the table to introspect
    ///
    /// # Returns
    ///
    /// A `TableDetails` containing all table metadata
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id, table_name = %table_name))]
    pub async fn get_table_details(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        table_name: &str,
        schema: Option<&str>,
    ) -> ServiceResult<TableDetails> {
        // Check full TableDetails cache first.
        // Validity is governed by the main schema cache — if that has expired or
        // been invalidated, table details are treated as stale too, ensuring both
        // caches share a single consistent TTL domain.
        let cache_key = (connection_id, table_name.to_string());
        if self.cache.is_valid(connection_id) {
            let cache = self.table_details_cache.read();
            if let Some(cached) = cache.get(&cache_key) {
                tracing::debug!("TableDetails cache hit for {}", table_name);
                return Ok(cached.clone());
            }
        }

        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Columns may already be warm in the schema cache; only hit the DB if not.
        let columns = match self.cache.get_columns(connection_id, table_name) {
            Some(cached_columns) => {
                tracing::debug!("Table columns cache hit for {}", table_name);
                cached_columns
            }
            None => {
                tracing::debug!("Table columns cache miss, loading from database");
                let cols = schema_introspection
                    .get_columns(schema, table_name)
                    .await
                    .map_err(|e| ServiceError::SchemaLoadFailed(e.to_string()))?;
                self.cache.set_columns(connection_id, table_name, cols.clone());
                cols
            }
        };

        // These three metadata queries share the same underlying connection, which
        // is protected by an async mutex. Running them concurrently via join! causes
        // pathological waker contention on non-Tokio executors (like GPUI's), leading
        // to "connection closed" errors. Sequential execution is semantically equivalent
        // because the mutex serializes the queries regardless.
        let indexes = schema_introspection
            .get_indexes(schema, table_name)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load indexes for {}: {}", table_name, e);
                Vec::new()
            });
        let foreign_keys = schema_introspection
            .get_foreign_keys(schema, table_name)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load foreign keys for {}: {}", table_name, e);
                Vec::new()
            });
        let primary_key = schema_introspection
            .get_primary_key(schema, table_name)
            .await
            .ok()
            .flatten();

        // Extract primary key column names
        let pk_columns: Vec<String> = primary_key
            .as_ref()
            .map(|pk| pk.columns.clone())
            .unwrap_or_default();

        // Transform columns to UI-friendly format
        let column_infos: Vec<ColumnInfo> = columns
            .into_iter()
            .map(|col| {
                let is_pk = pk_columns.contains(&col.name);
                ColumnInfo {
                    name: col.name,
                    data_type: col.data_type,
                    nullable: col.nullable,
                    is_primary_key: is_pk,
                    default_value: col.default_value,
                }
            })
            .collect();

        let details = TableDetails {
            name: table_name.to_string(),
            columns: column_infos,
            indexes,
            foreign_keys,
            primary_key_columns: pk_columns,
            row_count: None,
        };

        tracing::info!(
            table_name = %table_name,
            columns = details.columns.len(),
            indexes = details.indexes.len(),
            foreign_keys = details.foreign_keys.len(),
            "Table details loaded successfully"
        );

        // Cache the full TableDetails
        self.table_details_cache
            .write()
            .insert(cache_key, details.clone());

        Ok(details)
    }

    /// Generate DDL for a database object
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `object_type` - Type of object (Table, View, etc.)
    /// * `schema` - Optional schema name
    /// * `name` - Object name
    ///
    /// # Returns
    ///
    /// DDL string for creating the object
    #[tracing::instrument(skip(self, connection))]
    pub async fn generate_ddl(
        &self,
        connection: Arc<dyn Connection>,
        object_type: ObjectType,
        schema: Option<String>,
        name: String,
    ) -> ServiceResult<String> {
        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let db_object = DatabaseObject {
            object_type,
            schema,
            name: name.clone(),
        };

        let ddl = schema_introspection
            .generate_ddl(&db_object)
            .await
            .map_err(|e| ServiceError::DdlGenerationFailed(e.to_string()))?;

        tracing::info!("Generated DDL for {:?} {}", object_type, name);

        Ok(ddl)
    }

    /// Invalidate all cached schema data for a connection
    ///
    /// This should be called when schema changes are made (CREATE, DROP, ALTER, etc.)
    ///
    /// # Arguments
    ///
    /// * `connection_id` - UUID of the connection whose cache should be invalidated
    pub fn invalidate_connection_cache(&self, connection_id: Uuid) {
        tracing::info!("Invalidating schema cache for connection {}", connection_id);
        self.cache.invalidate(connection_id);
        self.table_details_cache
            .write()
            .retain(|(conn_id, _), _| *conn_id != connection_id);
        self.ddl_cache
            .write()
            .retain(|(conn_id, _), _| *conn_id != connection_id);
    }

    /// Invalidate cached TableDetails for a specific table on a connection.
    ///
    /// Call this after schema-modifying operations (ALTER TABLE, etc.) so the
    /// next `get_table_details` call fetches fresh data.
    pub fn invalidate_table_details(&self, connection_id: Uuid, table_name: &str) {
        let key = (connection_id, table_name.to_string());
        self.table_details_cache.write().remove(&key);
        self.ddl_cache.write().remove(&key);
    }

    /// Synchronous cache-only read of TableDetails — no database round-trip.
    ///
    /// Returns `Some` only when the main schema cache is still valid AND the
    /// details have already been fetched (e.g. by the prefetch task).  Callers
    /// can use this to skip showing a loading spinner when data is already warm.
    pub fn peek_table_details_cache(
        &self,
        connection_id: Uuid,
        table_name: &str,
    ) -> Option<TableDetails> {
        if !self.cache.is_valid(connection_id) {
            return None;
        }
        self.table_details_cache
            .read()
            .get(&(connection_id, table_name.to_string()))
            .cloned()
    }

    /// Return the DDL for `table_name` from cache if available, otherwise
    /// generate it via a database query and cache the result.
    ///
    /// `None` is returned when the connection does not support schema
    /// introspection, or when DDL generation fails.
    pub async fn get_or_generate_ddl(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Uuid,
        table_name: &str,
    ) -> Option<String> {
        let cache_key = (connection_id, table_name.to_string());

        // Fast path: return cached DDL if the main schema cache is still valid
        if self.cache.is_valid(connection_id) {
            if let Some(cached) = self.ddl_cache.read().get(&cache_key).cloned() {
                tracing::debug!("DDL cache hit for {}", table_name);
                return Some(cached);
            }
        }

        let schema_introspection = connection.as_schema_introspection()?;
        let db_object = DatabaseObject {
            object_type: ObjectType::Table,
            schema: None,
            name: table_name.to_string(),
        };

        match schema_introspection.generate_ddl(&db_object).await {
            Ok(ddl) => {
                self.ddl_cache.write().insert(cache_key, ddl.clone());
                tracing::debug!("DDL generated and cached for {}", table_name);
                Some(ddl)
            }
            Err(e) => {
                tracing::warn!("Failed to generate DDL for {}: {}", table_name, e);
                None
            }
        }
    }

    /// Warm `table_details_cache` for every table in `table_names` by fetching
    /// details from the database in concurrent batches.
    ///
    /// This should be called immediately after `load_tables_only` succeeds so
    /// that the LSP (and any other consumer) can read all table details without
    /// hitting the database at all.
    ///
    /// Tables are processed in batches of `PREFETCH_BATCH_SIZE` to avoid
    /// exhausting the connection pool while still providing parallelism.
    pub async fn prefetch_all_table_details(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        table_names: Vec<String>,
        schema: Option<String>,
    ) {
        tracing::info!(
            "Pre-warming table details for {} tables on connection {}",
            table_names.len(),
            connection_id
        );

        // Give the user's first browse query a head-start before we start
        // issuing background queries that compete for the same connection pool.
        smol::Timer::after(PREFETCH_INITIAL_DELAY).await;

        for batch in table_names.chunks(PREFETCH_BATCH_SIZE) {
            let futures: Vec<_> = batch
                .iter()
                .map(|table_name| {
                    let connection = connection.clone();
                    let schema_ref = schema.as_deref();
                    self.get_table_details(connection, connection_id, table_name, schema_ref)
                })
                .collect();

            // Run the batch concurrently; log individual failures but don't abort
            let results = join_all(futures).await;
            for (table_name, result) in batch.iter().zip(results) {
                if let Err(e) = result {
                    tracing::warn!("Failed to pre-warm details for table '{}': {}", table_name, e);
                }
            }

            // Pause between batches to avoid monopolising the connection pool
            smol::Timer::after(PREFETCH_INTER_BATCH_DELAY).await;
        }

        tracing::info!("Table details pre-warm complete for connection {}", connection_id);
    }

    /// Return all currently-cached `TableDetails` for a connection in one map.
    ///
    /// Returns `None` when the main schema cache is expired or not yet
    /// populated, so callers can decide whether to wait or trigger a load.
    pub fn get_all_cached_table_details(
        &self,
        connection_id: Uuid,
    ) -> Option<HashMap<String, TableDetails>> {
        if !self.cache.is_valid(connection_id) {
            return None;
        }

        let cache = self.table_details_cache.read();
        let details: HashMap<String, TableDetails> = cache
            .iter()
            .filter(|((conn_id, _), _)| *conn_id == connection_id)
            .map(|((_, table_name), details)| (table_name.clone(), details.clone()))
            .collect();

        if details.is_empty() {
            None
        } else {
            Some(details)
        }
    }

    /// Get the underlying cache
    ///
    /// This is provided for advanced use cases but should rarely be needed.
    pub fn cache(&self) -> Arc<SchemaCache> {
        self.cache.clone()
    }

    /// Get cached tables for a connection if available
    ///
    /// Returns the cached table list if the cache is valid, None otherwise.
    /// This is useful for updating UI components without making database calls.
    pub fn get_cached_tables(&self, connection_id: Uuid) -> Option<Vec<zqlz_core::TableInfo>> {
        if self.cache.is_valid(connection_id) {
            self.cache.get_tables(connection_id)
        } else {
            None
        }
    }

    /// Get cached view names for a connection if available
    ///
    /// Returns the cached view name list if the cache is valid, None otherwise.
    /// This is useful for updating UI components (e.g. command palette) without
    /// making database calls.
    pub fn get_cached_view_names(&self, connection_id: Uuid) -> Option<Vec<String>> {
        if self.cache.is_valid(connection_id) {
            self.cache
                .get_views(connection_id)
                .map(|views| views.into_iter().map(|v| v.name).collect())
        } else {
            None
        }
    }
}

impl Default for SchemaService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_service_creation() {
        let service = SchemaService::new();
        assert!(Arc::strong_count(&service.cache) >= 1);
    }

    #[test]
    fn test_schema_service_with_custom_cache() {
        let cache = Arc::new(SchemaCache::new(std::time::Duration::from_secs(300)));
        let service = SchemaService::with_cache(cache.clone());
        assert!(Arc::ptr_eq(&service.cache, &cache));
    }
}
