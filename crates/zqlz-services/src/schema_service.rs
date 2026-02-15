//! Schema introspection service with caching
//!
//! Provides centralized schema operations with automatic caching to reduce
//! database round-trips.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;
use zqlz_core::{Connection, DatabaseObject, ObjectType, ObjectsPanelData, ViewInfo, FunctionInfo, ProcedureInfo, TriggerInfo, TableInfo};
use zqlz_schema::SchemaCache;

use crate::error::{ServiceError, ServiceResult};
use crate::view_models::{ColumnInfo, DatabaseSchema, TableDetails};

/// Service for schema introspection operations
///
/// This service wraps the `SchemaCache` and provides:
/// - Automatic caching of schema metadata
/// - Batch loading of schema objects
/// - Graceful handling of partial failures
/// - UI-friendly schema models
pub struct SchemaService {
    cache: Arc<SchemaCache>,
    /// Per-connection, per-table cache for full TableDetails.
    /// Avoids redundant index/FK/PK queries on repeated opens or tab switches.
    table_details_cache: RwLock<HashMap<(Uuid, String), TableDetails>>,
}

impl SchemaService {
    /// Create a new schema service
    pub fn new() -> Self {
        Self {
            cache: Arc::new(SchemaCache::new(std::time::Duration::from_secs(300))), // 5 minutes
            table_details_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Create a schema service with a custom cache
    pub fn with_cache(cache: Arc<SchemaCache>) -> Self {
        Self {
            cache,
            table_details_cache: RwLock::new(HashMap::new()),
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

                return Ok(DatabaseSchema {
                    table_infos: cached_tables,
                    objects_panel_data,
                    tables,
                    views: Vec::new(),
                    materialized_views: Vec::new(),
                    triggers: Vec::new(),
                    functions: Vec::new(),
                    procedures: Vec::new(),
                    table_indexes: std::collections::HashMap::new(),
                    database_name: None,
                    schema_name: None,
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

        // Cache tables and objects panel data for future use
        if !tables.is_empty() {
            self.cache.set_tables(connection_id, tables.clone());
            self.cache
                .set_objects_panel_data(connection_id, objects_panel_data.clone());
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

        let introspection_schema = self.get_introspection_schema(&connection).await;

        let tables = schema.list_tables(introspection_schema.as_deref()).await.map_err(|e| {
            tracing::error!("Failed to load tables: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        // Populate cache so subsequent load_database_schema calls can use cached data
        self.cache.set_tables(connection_id, tables.clone());
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

        let introspection_schema = self.get_introspection_schema(&connection).await;

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

        let introspection_schema = self.get_introspection_schema(&connection).await;

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

        let introspection_schema = self.get_introspection_schema(&connection).await;

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

        let introspection_schema = self.get_introspection_schema(&connection).await;

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

        let introspection_schema = self.get_introspection_schema(&connection).await;

        let triggers = schema.list_triggers(introspection_schema.as_deref(), None).await.map_err(|e| {
            tracing::error!("Failed to load triggers: {}", e);
            ServiceError::SchemaLoadFailed(e.to_string())
        })?;

        tracing::info!("Loaded {} triggers", triggers.len());
        Ok(triggers)
    }

    /// Helper to get the introspection schema parameter
    pub async fn get_introspection_schema(&self, connection: &Arc<dyn Connection>) -> Option<String> {
        let (db_query, schema_query) = match connection.driver_name() {
            "mysql" => ("SELECT DATABASE()", "SELECT DATABASE()"),
            "mssql" => ("SELECT DB_NAME()", "SELECT SCHEMA_NAME()"),
            "sqlite" => return None,
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

        match connection.driver_name() {
            "mysql" | "mssql" => database_name,
            "sqlite" => None,
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
        // Check full TableDetails cache first
        let cache_key = (connection_id, table_name.to_string());
        {
            let cache = self.table_details_cache.read();
            if let Some(cached) = cache.get(&cache_key) {
                tracing::debug!("TableDetails cache hit for {}", table_name);
                return Ok(cached.clone());
            }
        }

        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Check cache for columns
        let columns_from_cache = self.cache.get_columns(connection_id, table_name);

        let columns = if let Some(cached_columns) = columns_from_cache {
            tracing::debug!("Table columns cache hit for {}", table_name);
            cached_columns
        } else {
            tracing::debug!("Table columns cache miss, loading from database");
            let cols = schema_introspection
                .get_columns(schema, table_name)
                .await
                .map_err(|e| ServiceError::SchemaLoadFailed(e.to_string()))?;

            // Cache columns
            self.cache
                .set_columns(connection_id, table_name, cols.clone());
            cols
        };

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
    }

    /// Invalidate cached TableDetails for a specific table on a connection.
    ///
    /// Call this after schema-modifying operations (ALTER TABLE, etc.) so the
    /// next `get_table_details` call fetches fresh data.
    pub fn invalidate_table_details(&self, connection_id: Uuid, table_name: &str) {
        self.table_details_cache
            .write()
            .remove(&(connection_id, table_name.to_string()));
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
