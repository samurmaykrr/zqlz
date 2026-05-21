//! Schema introspection service with caching
//!
//! Provides centralized schema operations with automatic caching to reduce
//! database round-trips.

use futures::future::join_all;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    connection_is_postgres, ColumnInfo as SchemaColumnInfo, Connection, ConnectionScope,
    DatabaseObject, FunctionInfo, ObjectFormDdlRequest, ObjectFormSpec, ObjectFormSpecRequest,
    ObjectType, ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelRow, ProcedureInfo,
    SchemaIntrospection, TableInfo, TableType, TriggerInfo, TypeKind, ViewInfo,
};
use zqlz_schema::SchemaCache;

use crate::error::{ServiceError, ServiceResult};
use crate::view_models::{ColumnInfo, DatabaseSchema, TableDetails};

#[derive(Clone, Debug)]
pub enum SidebarSectionLoadData {
    Views(Vec<String>),
    MaterializedViews(Vec<String>),
    Functions(Vec<String>),
    Procedures(Vec<String>),
    Triggers(Vec<String>),
    Events(Vec<String>),
    Sequences(Vec<String>),
    Domains(Vec<String>),
    Types(Vec<String>),
    Extensions(Vec<String>),
}

type ScopedObjectCacheKey = (Uuid, String, Option<String>);
type TableDetailsCacheMap = HashMap<ScopedObjectCacheKey, TableDetails>;
type DdlCacheMap = HashMap<ScopedObjectCacheKey, String>;
type ObjectsPanelKindCacheKey = (Uuid, Option<String>, String, Option<String>);
type ObjectsPanelKindCacheMap = HashMap<ObjectsPanelKindCacheKey, ObjectsPanelData>;

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
    /// Per-connection, per-table, per-schema cache for full TableDetails. TTL is governed
    /// by `self.cache.is_valid(connection_id)` — entries are considered stale
    /// whenever the main schema cache expires or is invalidated, so they share
    /// one consistent TTL domain rather than having independent timestamps.
    table_details_cache: RwLock<TableDetailsCacheMap>,
    /// Per-connection, per-object, per-schema cache for generated DDL strings.
    /// Shares the same TTL domain as
    /// `table_details_cache` — cleared together on invalidation.
    ddl_cache: RwLock<DdlCacheMap>,
    objects_panel_kind_cache: RwLock<ObjectsPanelKindCacheMap>,
}

impl SchemaService {
    fn emit_objects_panel_manifest_coverage_telemetry(
        connection_id: Uuid,
        has_driver_manifest: bool,
    ) {
        let manifest_source = if has_driver_manifest {
            "driver_manifest"
        } else {
            "fallback_manifest"
        };

        tracing::info!(
            target: "objects_panel.telemetry",
            metric = "objects_panel_driver_manifest_coverage",
            connection_id = %connection_id,
            has_driver_manifest,
            manifest_source,
            "Objects panel driver-manifest coverage telemetry"
        );
    }

    fn build_objects_panel_rows_by_kind(
        rows: &[ObjectsPanelRow],
    ) -> HashMap<String, Vec<ObjectsPanelRow>> {
        let mut rows_by_kind: HashMap<String, Vec<ObjectsPanelRow>> = HashMap::new();
        for row in rows {
            rows_by_kind
                .entry(row.object_kind_id().to_string())
                .or_default()
                .push(row.clone());
        }
        rows_by_kind
    }

    fn object_names_by_kind(objects_panel_data: &ObjectsPanelData, kind: &str) -> Vec<String> {
        objects_panel_data
            .rows
            .iter()
            .filter(|row| row.object_kind_id() == kind)
            .map(|row| row.name.clone())
            .collect()
    }

    fn sidebar_type_names(
        objects_panel_data: &ObjectsPanelData,
        include_domains: bool,
    ) -> Vec<String> {
        objects_panel_data
            .rows
            .iter()
            .filter(|row| row.object_kind_id() == "type")
            .filter(|row| {
                let is_domain = row
                    .values
                    .get("data_type")
                    .or_else(|| row.values.get("table_type"))
                    .is_some_and(|value| value == "Domain");
                is_domain == include_domains
            })
            .map(|row| row.name.clone())
            .collect()
    }

    fn sidebar_introspection_scope(
        connection: &dyn Connection,
        introspection_schema: Option<String>,
    ) -> Option<String> {
        if connection_is_postgres(connection) {
            None
        } else {
            introspection_schema
        }
    }

    async fn get_sidebar_introspection_schema_cached(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> Option<String> {
        let introspection_schema = self
            .get_introspection_schema_cached(connection, connection_id)
            .await;
        Self::sidebar_introspection_scope(connection.as_ref(), introspection_schema)
    }

    fn normalize_scope(scope: Option<&str>) -> Option<String> {
        scope
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    fn scoped_column_cache_key(table_name: &str, schema: Option<&str>) -> String {
        match Self::normalize_scope(schema) {
            Some(schema) => format!("{schema}.{table_name}"),
            None => table_name.to_string(),
        }
    }

    async fn resolve_database_name_for_connection(
        connection: &Arc<dyn Connection>,
    ) -> Option<String> {
        connection.current_database_name().await.ok().flatten()
    }

    /// Create a new schema service
    pub fn new() -> Self {
        Self {
            cache: Arc::new(SchemaCache::new(std::time::Duration::from_secs(300))), // 5 minutes
            table_details_cache: RwLock::new(HashMap::new()),
            ddl_cache: RwLock::new(HashMap::new()),
            objects_panel_kind_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Create a schema service with a custom cache
    pub fn with_cache(cache: Arc<SchemaCache>) -> Self {
        Self {
            cache,
            table_details_cache: RwLock::new(HashMap::new()),
            ddl_cache: RwLock::new(HashMap::new()),
            objects_panel_kind_cache: RwLock::new(HashMap::new()),
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
        self.load_database_schema_for_database(connection, connection_id, None)
            .await
    }

    /// Load full database schema with optional explicit database target.
    ///
    /// `target_database` is primarily used by MySQL/MariaDB multi-database
    /// workflows where sidebar navigation can inspect a database different from
    /// the connection default. When a target is provided, cache reads/writes are
    /// bypassed to avoid mixing per-database data under a connection-only key.
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id, target_database = ?target_database))]
    pub async fn load_database_schema_for_database(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        target_database: Option<&str>,
    ) -> ServiceResult<DatabaseSchema> {
        self.load_database_schema_for_database_and_schema(
            connection,
            connection_id,
            target_database,
            None,
        )
        .await
    }

    /// Load full database schema with optional explicit database and schema targets.
    ///
    /// `target_schema` is used by editor-local schema switching. Targeted loads
    /// bypass the connection-wide cache so one selected schema cannot poison
    /// completions for another selected schema.
    #[tracing::instrument(skip(self, connection), fields(connection_id = %connection_id, target_database = ?target_database, target_schema = ?target_schema))]
    pub async fn load_database_schema_for_database_and_schema(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        target_database: Option<&str>,
        target_schema: Option<&str>,
    ) -> ServiceResult<DatabaseSchema> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let target_database = target_database
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
        let target_schema = target_schema
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
        let bypass_cache = target_database.is_some() || target_schema.is_some();

        if bypass_cache {
            // Targeted database switches only need to replace the connection-
            // wide schema snapshot. Per-object detail caches are already scoped
            // by schema and object identity, so keeping them warm avoids
            // unnecessary reload churn when the active database changes.
            self.invalidate_schema_snapshot(connection_id);
        }

        // Check cache validity
        if !bypass_cache && self.cache.is_valid(connection_id) {
            if let Some(cached_tables) = self.cache.get_tables(connection_id) {
                tracing::debug!("Schema cache hit for connection {}", connection_id);

                let has_driver_manifest = self
                    .cache
                    .get_objects_panel_manifest_has_driver_manifest(connection_id)
                    .unwrap_or(false);
                Self::emit_objects_panel_manifest_coverage_telemetry(
                    connection_id,
                    has_driver_manifest,
                );

                let tables: Vec<String> = cached_tables.iter().map(|t| t.name.clone()).collect();
                let objects_panel_data = self.cache.get_objects_panel_data(connection_id);
                let objects_panel_manifest = self.cache.get_objects_panel_manifest(connection_id);
                let views = self.cache.get_views(connection_id).unwrap_or_default();
                let materialized_views = self
                    .cache
                    .get_materialized_views(connection_id)
                    .unwrap_or_default();
                let triggers = self.cache.get_triggers(connection_id).unwrap_or_default();
                let functions = self.cache.get_functions(connection_id).unwrap_or_default();
                let procedures = self.cache.get_procedures(connection_id).unwrap_or_default();
                let table_indexes = self
                    .cache
                    .get_all_indexes(connection_id)
                    .unwrap_or_default();
                let database_name = self.cache.get_database_name(connection_id);
                let schema_name = self.cache.get_schema_name(connection_id);
                let schema_names = self
                    .cache
                    .get_schema_names(connection_id)
                    .unwrap_or_default();

                return Ok(DatabaseSchema {
                    table_infos: cached_tables,
                    events: objects_panel_data
                        .as_ref()
                        .map(|data| Self::object_names_by_kind(data, "event"))
                        .unwrap_or_default(),
                    sequences: objects_panel_data
                        .as_ref()
                        .map(|data| Self::object_names_by_kind(data, "sequence"))
                        .unwrap_or_default(),
                    domains: objects_panel_data
                        .as_ref()
                        .map(|data| Self::sidebar_type_names(data, true))
                        .unwrap_or_default(),
                    types: objects_panel_data
                        .as_ref()
                        .map(|data| Self::sidebar_type_names(data, false))
                        .unwrap_or_default(),
                    extensions: objects_panel_data
                        .as_ref()
                        .map(|data| Self::object_names_by_kind(data, "extension"))
                        .unwrap_or_default(),
                    objects_panel_data,
                    objects_panel_manifest,
                    tables,
                    views: views.into_iter().map(|v| v.name).collect(),
                    materialized_views: materialized_views.into_iter().map(|v| v.name).collect(),
                    triggers: triggers.into_iter().map(|t| t.name).collect(),
                    functions: functions.into_iter().map(|f| f.name).collect(),
                    procedures: procedures.into_iter().map(|p| p.name).collect(),
                    table_indexes,
                    database_name,
                    schema_name,
                    schema_names,
                });
            }
        }

        tracing::debug!("Schema cache miss, loading from database");

        let requested_scope = target_database
            .as_ref()
            .map(|database_name| ConnectionScope::Database(database_name.clone()))
            .unwrap_or(ConnectionScope::Default);
        let resolved_scope = connection
            .resolve_scope(requested_scope)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?;

        let database_name = resolved_scope
            .effective_database
            .clone()
            .or_else(|| target_database.clone());
        let schema_name = target_schema
            .clone()
            .or_else(|| resolved_scope.effective_namespace.clone());
        let schema_names = schema
            .list_schemas()
            .await
            .map(|schemas| schemas.into_iter().map(|schema| schema.name).collect())
            .unwrap_or_else(|_| Vec::new());

        let introspection_schema = Self::sidebar_introspection_scope(
            connection.as_ref(),
            target_schema
                .clone()
                .or_else(|| resolved_scope.introspection_scope.clone())
                .or_else(|| schema_name.clone()),
        );

        // Load all schema objects (handle partial failures gracefully)
        let tables_result = schema.list_tables(introspection_schema.as_deref()).await;
        let views_result = schema.list_views(introspection_schema.as_deref()).await;
        let materialized_views_result = if connection.supports_materialized_views() {
            schema
                .list_materialized_views(introspection_schema.as_deref())
                .await
        } else {
            Ok(Vec::new())
        };
        let triggers_result = if connection.supports_top_level_triggers() {
            schema
                .list_triggers(introspection_schema.as_deref(), None)
                .await
        } else {
            Ok(Vec::new())
        };
        let functions_result = schema.list_functions(introspection_schema.as_deref()).await;
        let procedures_result = schema
            .list_procedures(introspection_schema.as_deref())
            .await;

        let tables = tables_result.unwrap_or_else(|e| {
            tracing::warn!("Failed to load tables: {}", e);
            Vec::new()
        });

        let objects_panel_data = schema
            .list_tables_extended(introspection_schema.as_deref())
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(
                    connection_id = %connection_id,
                    %error,
                    "Failed to load extended objects panel data, falling back to table metadata"
                );
                ObjectsPanelData::from_table_infos(tables.clone())
            });
        let objects_panel_rows_by_kind =
            Self::build_objects_panel_rows_by_kind(&objects_panel_data.rows);
        let (objects_panel_manifest, has_driver_manifest) = match schema
            .list_objects_panel_manifest(introspection_schema.as_deref())
            .await
        {
            Ok(manifest) => {
                manifest.validate().map_err(|error| {
                    ServiceError::SchemaLoadFailed(format!(
                        "Driver returned invalid objects panel manifest: {}",
                        error
                    ))
                })?;
                (manifest, true)
            }
            Err(error) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    %error,
                    "Failed to load objects panel manifest, deriving fallback from data"
                );
                let fallback_manifest = ObjectsPanelManifest::from_data(&objects_panel_data);
                fallback_manifest.validate().map_err(|validation_error| {
                    ServiceError::SchemaLoadFailed(format!(
                        "Derived fallback objects panel manifest is invalid: {}",
                        validation_error
                    ))
                })?;
                (fallback_manifest, false)
            }
        };

        Self::emit_objects_panel_manifest_coverage_telemetry(connection_id, has_driver_manifest);

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
            if let Ok(indexes) = schema
                .get_indexes(introspection_schema.as_deref(), &table.name)
                .await
            {
                table_indexes.insert(table.name.clone(), indexes);
            }
        }

        // Cache tables and all other schema objects for future use
        if !tables.is_empty() && !bypass_cache {
            self.cache.set_tables(connection_id, tables.clone());
            self.cache.set_connection_names(
                connection_id,
                database_name.clone(),
                schema_name.clone(),
            );
            self.cache
                .set_schema_names(connection_id, schema_names.clone());
            self.cache
                .set_objects_panel_data(connection_id, objects_panel_data.clone());
            self.cache
                .set_objects_panel_rows_by_kind(connection_id, objects_panel_rows_by_kind);
            self.cache.set_objects_panel_manifest(
                connection_id,
                objects_panel_manifest.clone(),
                has_driver_manifest,
            );
            self.cache.set_views(connection_id, views.clone());
            self.cache
                .set_materialized_views(connection_id, materialized_views.clone());
            self.cache.set_triggers(connection_id, triggers.clone());
            self.cache.set_functions(connection_id, functions.clone());
            self.cache.set_procedures(connection_id, procedures.clone());
            self.cache
                .set_all_indexes(connection_id, table_indexes.clone());
        }

        let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
        let materialized_view_names: Vec<String> =
            materialized_views.into_iter().map(|v| v.name).collect();
        let sequences = Self::object_names_by_kind(&objects_panel_data, "sequence");
        let events = Self::object_names_by_kind(&objects_panel_data, "event");
        let domains = Self::sidebar_type_names(&objects_panel_data, true);
        let types = Self::sidebar_type_names(&objects_panel_data, false);
        let extensions = Self::object_names_by_kind(&objects_panel_data, "extension");
        let db_schema = DatabaseSchema {
            table_infos: tables,
            objects_panel_data: Some(objects_panel_data),
            objects_panel_manifest: Some(objects_panel_manifest),
            tables: table_names,
            views: views.into_iter().map(|v| v.name).collect(),
            materialized_views: materialized_view_names,
            triggers: triggers.into_iter().map(|t| t.name).collect(),
            functions: functions.into_iter().map(|f| f.name).collect(),
            procedures: procedures.into_iter().map(|p| p.name).collect(),
            events,
            sequences,
            domains,
            types,
            extensions,
            table_indexes,
            database_name,
            schema_name,
            schema_names,
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
            schema_count = db_schema.schema_names.len(),
            schema_names = ?db_schema.schema_names,
            "Schema loaded successfully"
        );

        Ok(db_schema)
    }

    /// Load Objects Panel rows for one kind while preserving the schema cache.
    ///
    /// This is the service-layer contract for kind-first callers: it returns the
    /// current snapshot filtered to the requested object kind and optional scope,
    /// but it still reuses the same cached schema load underneath so callers do not
    /// need to reimplement filtering or duplicate cache reads.
    #[tracing::instrument(
        skip(self, connection),
        fields(connection_id = %connection_id, target_database = ?target_database, kind_id = %kind_id, scope = ?scope)
    )]
    pub async fn load_objects_panel_data_for_kind(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        target_database: Option<&str>,
        kind_id: &str,
        scope: Option<&str>,
    ) -> ServiceResult<ObjectsPanelData> {
        let cache_key = (
            connection_id,
            target_database.map(ToOwned::to_owned),
            kind_id.to_string(),
            scope.map(ToOwned::to_owned),
        );
        if self.cache.is_valid(connection_id) {
            if let Some(data) = self
                .objects_panel_kind_cache
                .read()
                .get(&cache_key)
                .cloned()
            {
                return Ok(data);
            }
        }

        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;
        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;
        let objects_panel_data = schema
            .list_objects_panel_data_for_kind(introspection_schema.as_deref(), kind_id)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?
            .for_kind_and_scope(kind_id, scope);

        self.objects_panel_kind_cache
            .write()
            .insert(cache_key, objects_panel_data.clone());

        Ok(objects_panel_data)
    }

    pub async fn object_form_spec(
        &self,
        connection: Arc<dyn Connection>,
        request: &ObjectFormSpecRequest,
    ) -> ServiceResult<Option<ObjectFormSpec>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        schema
            .object_form_spec(request)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))
    }

    pub async fn generate_object_form_ddl(
        &self,
        connection: Arc<dyn Connection>,
        request: &ObjectFormDdlRequest,
    ) -> ServiceResult<Vec<String>> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        schema
            .generate_object_form_ddl(request)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))
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

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let tables = schema
            .list_tables(introspection_schema.as_deref())
            .await
            .map_err(|e| {
                tracing::error!("Failed to load tables: {}", e);
                ServiceError::SchemaLoadFailed(e.to_string())
            })?;

        // Populate cache so subsequent load_database_schema calls can use cached data.
        // Also purge any stale table-details entries for this connection so that
        // reconnects don't serve details from the previous schema generation.
        self.cache.set_tables(connection_id, tables.clone());
        self.table_details_cache
            .write()
            .retain(|(conn_id, _, _), _| *conn_id != connection_id);
        tracing::debug!(
            "Cached {} tables for connection {}",
            tables.len(),
            connection_id
        );

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

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let views = schema
            .list_views(introspection_schema.as_deref())
            .await
            .map_err(|e| {
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
        if !connection.supports_materialized_views() {
            return Ok(Vec::new());
        }

        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let views = schema
            .list_materialized_views(introspection_schema.as_deref())
            .await
            .map_err(|e| {
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

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let functions = schema
            .list_functions(introspection_schema.as_deref())
            .await
            .map_err(|e| {
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

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let procedures = schema
            .list_procedures(introspection_schema.as_deref())
            .await
            .map_err(|e| {
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
        if !connection.supports_top_level_triggers() {
            return Ok(Vec::new());
        }

        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let introspection_schema = self
            .get_sidebar_introspection_schema_cached(&connection, connection_id)
            .await;

        let triggers = schema
            .list_triggers(introspection_schema.as_deref(), None)
            .await
            .map_err(|e| {
                tracing::error!("Failed to load triggers: {}", e);
                ServiceError::SchemaLoadFailed(e.to_string())
            })?;

        tracing::info!("Loaded {} triggers", triggers.len());
        Ok(triggers)
    }

    /// Load sidebar section content for lazy section expansion paths.
    pub async fn load_sidebar_section_data(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        section: zqlz_connection::SidebarSection,
    ) -> ServiceResult<Option<SidebarSectionLoadData>> {
        match section {
            zqlz_connection::SidebarSection::Views => {
                let names = self
                    .load_views(connection, connection_id)
                    .await?
                    .into_iter()
                    .map(|view| view.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::Views(names)))
            }
            zqlz_connection::SidebarSection::MaterializedViews => {
                let names = self
                    .load_materialized_views(connection, connection_id)
                    .await?
                    .into_iter()
                    .map(|view| view.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::MaterializedViews(names)))
            }
            zqlz_connection::SidebarSection::Functions => {
                let names = self
                    .load_functions(connection, connection_id)
                    .await?
                    .into_iter()
                    .map(|function| function.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::Functions(names)))
            }
            zqlz_connection::SidebarSection::Procedures => {
                let names = self
                    .load_procedures(connection, connection_id)
                    .await?
                    .into_iter()
                    .map(|procedure| procedure.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::Procedures(names)))
            }
            zqlz_connection::SidebarSection::Triggers => {
                let names = self
                    .load_triggers(connection, connection_id)
                    .await?
                    .into_iter()
                    .map(|trigger| trigger.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::Triggers(names)))
            }
            zqlz_connection::SidebarSection::Events => {
                let schema = self.load_database_schema(connection, connection_id).await?;
                Ok(Some(SidebarSectionLoadData::Events(schema.events)))
            }
            zqlz_connection::SidebarSection::Sequences => {
                let schema = connection
                    .as_schema_introspection()
                    .ok_or(ServiceError::SchemaNotSupported)?;
                let introspection_schema = self
                    .get_sidebar_introspection_schema_cached(&connection, connection_id)
                    .await;
                let names = schema
                    .list_sequences(introspection_schema.as_deref())
                    .await
                    .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?
                    .into_iter()
                    .map(|sequence| sequence.name)
                    .collect();
                Ok(Some(SidebarSectionLoadData::Sequences(names)))
            }
            zqlz_connection::SidebarSection::Domains | zqlz_connection::SidebarSection::Types => {
                let schema = connection
                    .as_schema_introspection()
                    .ok_or(ServiceError::SchemaNotSupported)?;
                let introspection_schema = self
                    .get_sidebar_introspection_schema_cached(&connection, connection_id)
                    .await;
                let include_domains = matches!(section, zqlz_connection::SidebarSection::Domains);
                let names = schema
                    .list_types(introspection_schema.as_deref())
                    .await
                    .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?
                    .into_iter()
                    .filter(|data_type| {
                        matches!(data_type.type_kind, TypeKind::Domain) == include_domains
                    })
                    .map(|data_type| data_type.name)
                    .collect();
                Ok(Some(if include_domains {
                    SidebarSectionLoadData::Domains(names)
                } else {
                    SidebarSectionLoadData::Types(names)
                }))
            }
            zqlz_connection::SidebarSection::Extensions => {
                let schema = self.load_database_schema(connection, connection_id).await?;
                Ok(Some(SidebarSectionLoadData::Extensions(schema.extensions)))
            }
            zqlz_connection::SidebarSection::Tables
            | zqlz_connection::SidebarSection::Queries
            | zqlz_connection::SidebarSection::RedisDatabases => Ok(None),
        }
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
        self.get_introspection_schema_impl(connection, Some(connection_id))
            .await
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
        if !connection.has_session_namespace() {
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
        let _ = self
            .get_introspection_schema_impl(connection, Some(connection_id))
            .await;
        self.cache.get_database_name(connection_id)
    }

    /// Return the current schema name (e.g. `"public"`, `"dbo"`) for the
    /// connection, using the cache to avoid redundant round-trips.
    ///
    /// This is intentionally separate from [`get_introspection_schema_cached`]
    /// because that method returns a driver-specific introspection parameter
    /// (database name for MySQL/MSSQL), while sidebar hierarchy should use the
    /// resolved schema name when available.
    pub async fn get_schema_name_cached(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> Option<String> {
        if !connection.has_session_namespace() {
            return None;
        }

        if self.cache.is_valid(connection_id) {
            let cached_schema = self.cache.get_schema_name(connection_id);
            let cached_db = self.cache.get_database_name(connection_id);
            if cached_schema.is_some() || cached_db.is_some() {
                return cached_schema;
            }
        }

        let _ = self
            .get_introspection_schema_impl(connection, Some(connection_id))
            .await;
        self.cache.get_schema_name(connection_id)
    }

    async fn get_introspection_schema_impl(
        &self,
        connection: &Arc<dyn Connection>,
        connection_id: Option<Uuid>,
    ) -> Option<String> {
        // Check cache first to avoid redundant queries
        if let Some(conn_id) = connection_id {
            if self.cache.is_valid(conn_id) {
                let cached_db = self.cache.get_database_name(conn_id);
                let cached_schema = self.cache.get_schema_name(conn_id);
                if cached_db.is_some() || cached_schema.is_some() {
                    return cached_schema;
                }
            }
        }

        let resolved_scope = connection
            .resolve_scope(ConnectionScope::Default)
            .await
            .ok()
            .unwrap_or_else(zqlz_core::ResolvedConnectionScope::default_scope);
        let schema_name = resolved_scope.effective_namespace;
        let database_name = Self::resolve_database_name_for_connection(connection).await;

        if let Some(conn_id) = connection_id {
            self.cache
                .set_connection_names(conn_id, database_name.clone(), schema_name.clone());
        }

        resolved_scope.introspection_scope.or(schema_name)
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
        let normalized_scope = Self::normalize_scope(schema);
        let cache_key = (
            connection_id,
            table_name.to_string(),
            normalized_scope.clone(),
        );
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

        let table_info = self
            .lookup_relation_info(
                &connection,
                schema_introspection,
                connection_id,
                table_name,
                schema,
            )
            .await
            .unwrap_or_else(|| Self::placeholder_table_info(table_name, schema, TableType::Table));

        // Columns may already be warm in the schema cache; only hit the DB if not.
        let columns = self
            .load_relation_columns(
                &connection,
                schema_introspection,
                connection_id,
                table_name,
                schema,
                table_info.table_type,
            )
            .await?;

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
        let primary_key = if matches!(table_info.table_type, TableType::VirtualTable) {
            None
        } else {
            schema_introspection
                .get_primary_key(schema, table_name)
                .await
                .ok()
                .flatten()
        };
        let constraints = schema_introspection
            .get_constraints(schema, table_name)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load constraints for {}: {}", table_name, e);
                Vec::new()
            });
        let triggers = schema_introspection
            .list_triggers(schema, Some(table_name))
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load triggers for {}: {}", table_name, e);
                Vec::new()
            });

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
                    max_length: col.max_length,
                    precision: col.precision,
                    scale: col.scale,
                    is_auto_increment: col.is_auto_increment,
                    comment: col.comment,
                    enum_values: col.enum_values,
                }
            })
            .collect();

        let details = TableDetails {
            name: table_name.to_string(),
            table_type: table_info.table_type,
            columns: column_infos,
            indexes,
            foreign_keys,
            constraints,
            triggers,
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
            signature: None,
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
        self.invalidate_schema_snapshot(connection_id);
        self.table_details_cache
            .write()
            .retain(|(conn_id, _, _), _| *conn_id != connection_id);
        self.ddl_cache
            .write()
            .retain(|(conn_id, _, _), _| *conn_id != connection_id);
        self.objects_panel_kind_cache
            .write()
            .retain(|(conn_id, _, _, _), _| *conn_id != connection_id);
    }

    /// Invalidate only the connection-wide schema snapshot.
    ///
    /// Targeted database loads need a fresh manifest and row set, but the
    /// schema-scoped table-details and generated-DDL caches can survive that
    /// switch because their keys already carry the object scope.
    fn invalidate_schema_snapshot(&self, connection_id: Uuid) {
        self.cache.invalidate(connection_id);
        self.objects_panel_kind_cache
            .write()
            .retain(|(conn_id, _, _, _), _| *conn_id != connection_id);
    }

    /// Invalidate cached TableDetails for a specific table on a connection.
    ///
    /// Call this after schema-modifying operations (ALTER TABLE, etc.) so the
    /// next `get_table_details` call fetches fresh data.
    pub fn invalidate_table_details(&self, connection_id: Uuid, table_name: &str) {
        self.cache
            .invalidate_columns_for_table(connection_id, table_name);
        self.table_details_cache
            .write()
            .retain(|(conn_id, cached_table_name, _), _| {
                *conn_id != connection_id || cached_table_name != table_name
            });
        self.ddl_cache
            .write()
            .retain(|(conn_id, cache_identifier, _), _| {
                !(*conn_id == connection_id
                    && cache_identifier.ends_with(&format!(":{table_name}")))
            });
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
        schema: Option<&str>,
    ) -> Option<TableDetails> {
        if !self.cache.is_valid(connection_id) {
            return None;
        }
        let normalized_scope = Self::normalize_scope(schema);
        self.table_details_cache
            .read()
            .get(&(connection_id, table_name.to_string(), normalized_scope))
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
        object_name: &str,
        schema: Option<&str>,
        preferred_object_type: Option<ObjectType>,
    ) -> Option<String> {
        let schema_introspection = connection.as_schema_introspection()?;
        let object_type = self
            .resolve_object_type(
                connection,
                schema_introspection,
                connection_id,
                object_name,
                schema,
                preferred_object_type,
            )
            .await;
        let cache_identifier = format!("{:?}:{}", object_type, object_name);
        let normalized_scope = Self::normalize_scope(schema);
        let cache_key = (connection_id, cache_identifier, normalized_scope.clone());

        // Fast path: return cached DDL if the main schema cache is still valid
        if self.cache.is_valid(connection_id) {
            if let Some(cached) = self.ddl_cache.read().get(&cache_key).cloned() {
                tracing::debug!("DDL cache hit for {}", object_name);
                return Some(cached);
            }
        }

        let db_object = DatabaseObject {
            object_type,
            schema: schema.map(ToOwned::to_owned),
            name: object_name.to_string(),
            signature: None,
        };

        match schema_introspection.generate_ddl(&db_object).await {
            Ok(ddl) => {
                self.ddl_cache.write().insert(cache_key, ddl.clone());
                tracing::debug!("DDL generated and cached for {}", object_name);
                Some(ddl)
            }
            Err(e) => {
                tracing::warn!("Failed to generate DDL for {}: {}", object_name, e);
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
                    tracing::warn!(
                        "Failed to pre-warm details for table '{}': {}",
                        table_name,
                        e
                    );
                }
            }

            // Pause between batches to avoid monopolising the connection pool
            smol::Timer::after(PREFETCH_INTER_BATCH_DELAY).await;
        }

        tracing::info!(
            "Table details pre-warm complete for connection {}",
            connection_id
        );
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
            .filter(|((conn_id, _, _), _)| *conn_id == connection_id)
            .map(|((_, table_name, _), details)| (table_name.clone(), details.clone()))
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

    /// Get cached objects panel rows grouped by kind for a connection.
    ///
    /// Returns `None` when the schema cache is stale.
    pub fn get_cached_objects_panel_rows_by_kind(
        &self,
        connection_id: Uuid,
    ) -> Option<HashMap<String, Vec<ObjectsPanelRow>>> {
        if self.cache.is_valid(connection_id) {
            self.cache.get_objects_panel_rows_by_kind(connection_id)
        } else {
            None
        }
    }

    /// Get cached objects panel rows for a specific object kind.
    ///
    /// Returns `None` when the schema cache is stale or the kind is not present.
    pub fn get_cached_objects_panel_rows_for_kind(
        &self,
        connection_id: Uuid,
        kind_id: &str,
    ) -> Option<Vec<ObjectsPanelRow>> {
        self.get_cached_objects_panel_rows_by_kind(connection_id)
            .and_then(|rows_by_kind| rows_by_kind.get(kind_id).cloned())
    }
}

impl SchemaService {
    fn placeholder_table_info(
        table_name: &str,
        schema: Option<&str>,
        table_type: TableType,
    ) -> TableInfo {
        TableInfo {
            schema: schema.map(ToOwned::to_owned),
            name: table_name.to_string(),
            table_type,
            owner: None,
            row_count: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        }
    }

    fn table_type_to_object_type(table_type: TableType) -> ObjectType {
        match table_type {
            TableType::View => ObjectType::View,
            TableType::MaterializedView => ObjectType::MaterializedView,
            TableType::Table
            | TableType::VirtualTable
            | TableType::ForeignTable
            | TableType::PartitionedTable
            | TableType::Temporary
            | TableType::System => ObjectType::Table,
        }
    }

    async fn resolve_object_type(
        &self,
        connection: &Arc<dyn Connection>,
        schema_introspection: &dyn SchemaIntrospection,
        connection_id: Uuid,
        object_name: &str,
        schema: Option<&str>,
        preferred_object_type: Option<ObjectType>,
    ) -> ObjectType {
        if let Some(info) = self
            .lookup_relation_info(
                connection,
                schema_introspection,
                connection_id,
                object_name,
                schema,
            )
            .await
        {
            return Self::table_type_to_object_type(info.table_type);
        }

        preferred_object_type.unwrap_or(ObjectType::Table)
    }

    async fn lookup_relation_info(
        &self,
        connection: &Arc<dyn Connection>,
        schema_introspection: &dyn SchemaIntrospection,
        connection_id: Uuid,
        object_name: &str,
        schema: Option<&str>,
    ) -> Option<TableInfo> {
        if self.cache.is_valid(connection_id) {
            if let Some(info) = self
                .cache
                .get_tables(connection_id)
                .unwrap_or_default()
                .into_iter()
                .find(|info| info.name == object_name)
            {
                return Some(info);
            }

            if self
                .cache
                .get_views(connection_id)
                .unwrap_or_default()
                .iter()
                .any(|view| view.name == object_name)
            {
                return Some(Self::placeholder_table_info(
                    object_name,
                    schema,
                    TableType::View,
                ));
            }

            if self
                .cache
                .get_materialized_views(connection_id)
                .unwrap_or_default()
                .iter()
                .any(|view| view.name == object_name)
            {
                return Some(Self::placeholder_table_info(
                    object_name,
                    schema,
                    TableType::MaterializedView,
                ));
            }
        }

        if let Ok(tables) = schema_introspection.list_tables(schema).await {
            if let Some(info) = tables.iter().find(|info| info.name == object_name).cloned() {
                self.cache.set_tables(connection_id, tables);
                return Some(info);
            }
        }

        if let Ok(views) = schema_introspection.list_views(schema).await {
            if views.iter().any(|view| view.name == object_name) {
                return Some(Self::placeholder_table_info(
                    object_name,
                    schema,
                    TableType::View,
                ));
            }
        }

        if connection.supports_materialized_views() {
            if let Ok(views) = schema_introspection.list_materialized_views(schema).await {
                if views.iter().any(|view| view.name == object_name) {
                    return Some(Self::placeholder_table_info(
                        object_name,
                        schema,
                        TableType::MaterializedView,
                    ));
                }
            }
        }

        None
    }

    async fn load_relation_columns(
        &self,
        connection: &Arc<dyn Connection>,
        schema_introspection: &dyn SchemaIntrospection,
        connection_id: Uuid,
        table_name: &str,
        schema: Option<&str>,
        table_type: TableType,
    ) -> ServiceResult<Vec<SchemaColumnInfo>> {
        let cache_key = Self::scoped_column_cache_key(table_name, schema);
        if let Some(cached_columns) = self.cache.get_columns(connection_id, &cache_key) {
            if cached_columns
                .iter()
                .all(|column| column.data_type.is_empty())
            {
                tracing::warn!(
                    table_name = %table_name,
                    schema = ?schema,
                    "Ignoring cached table columns because all data types are empty"
                );
            } else {
                tracing::debug!("Table columns cache hit for {}", cache_key);
                return Ok(cached_columns);
            }
        }

        tracing::debug!("Table columns cache miss, loading from database");
        match schema_introspection.get_columns(schema, table_name).await {
            Ok(columns) => {
                self.cache
                    .set_columns(connection_id, &cache_key, columns.clone());
                Ok(columns)
            }
            Err(error)
                if connection.should_use_ddl_column_fallback(table_type, &error.to_string()) =>
            {
                tracing::warn!(
                    table_name = %table_name,
                    error = %error,
                    "Falling back to DDL-based virtual table column parsing"
                );

                let columns = self
                    .load_sqlite_virtual_table_columns(schema_introspection, table_name)
                    .await?;
                self.cache
                    .set_columns(connection_id, &cache_key, columns.clone());
                Ok(columns)
            }
            Err(error) => Err(ServiceError::SchemaLoadFailed(error.to_string())),
        }
    }

    async fn load_sqlite_virtual_table_columns(
        &self,
        schema_introspection: &dyn SchemaIntrospection,
        table_name: &str,
    ) -> ServiceResult<Vec<SchemaColumnInfo>> {
        let ddl = schema_introspection
            .generate_ddl(&DatabaseObject {
                object_type: ObjectType::Table,
                schema: None,
                name: table_name.to_string(),
                signature: None,
            })
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?;

        parse_sqlite_virtual_table_columns(&ddl).ok_or_else(|| {
            ServiceError::SchemaLoadFailed(format!(
                "Unable to parse virtual table columns from SQLite DDL for '{}'",
                table_name
            ))
        })
    }
}

fn parse_sqlite_virtual_table_columns(ddl: &str) -> Option<Vec<SchemaColumnInfo>> {
    let arguments = extract_sqlite_virtual_table_arguments(ddl)?;
    let mut columns = Vec::new();

    for (ordinal, argument) in split_top_level_sql_arguments(arguments)
        .into_iter()
        .enumerate()
    {
        let trimmed = argument.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.contains('=') {
            break;
        }

        let Some(name) = parse_virtual_table_column_name(trimmed) else {
            continue;
        };

        columns.push(SchemaColumnInfo {
            name,
            ordinal,
            data_type: "TEXT".to_string(),
            nullable: true,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
            ..Default::default()
        });
    }

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn extract_sqlite_virtual_table_arguments(ddl: &str) -> Option<&str> {
    let using_index = ddl.to_ascii_uppercase().find("USING")?;
    let after_using = &ddl[using_index + "USING".len()..];
    let open_paren = after_using.find('(')?;
    let mut depth = 0usize;

    for (index, character) in after_using[open_paren..].char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(&after_using[open_paren + 1..open_paren + index]);
                }
            }
            _ => {}
        }
    }

    None
}

fn split_top_level_sql_arguments(arguments: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote = None;

    for character in arguments.chars() {
        match quote {
            Some(active_quote) if character == active_quote => {
                quote = None;
                current.push(character);
            }
            Some(_) => current.push(character),
            None => match character {
                '\'' | '"' => {
                    quote = Some(character);
                    current.push(character);
                }
                '(' => {
                    depth += 1;
                    current.push(character);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(character);
                }
                ',' if depth == 0 => {
                    result.push(current.trim().to_string());
                    current.clear();
                }
                _ => current.push(character),
            },
        }
    }

    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

fn parse_virtual_table_column_name(argument: &str) -> Option<String> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(rest) = trimmed.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(rest[..end].to_string());
    }

    if let Some(rest) = trimmed.strip_prefix('`') {
        let end = rest.find('`')?;
        return Some(rest[..end].to_string());
    }

    if let Some(rest) = trimmed.strip_prefix('[') {
        let end = rest.find(']')?;
        return Some(rest[..end].to_string());
    }

    trimmed
        .split_whitespace()
        .next()
        .map(|name| name.trim_matches(',').to_string())
        .filter(|name| !name.is_empty())
}

impl Default for SchemaService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::ObjectsPanelObjectRef;

    fn make_row(kind_id: &str, name: &str) -> ObjectsPanelRow {
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), name.to_string());

        ObjectsPanelRow {
            name: name.to_string(),
            schema: Some("public".to_string()),
            object_type: kind_id.to_string(),
            object_ref: Some(
                ObjectsPanelObjectRef::new(kind_id, name)
                    .with_schema_option(Some("public".to_string())),
            ),
            values,
            redis_database_index: None,
            key_value_info: None,
        }
    }

    fn make_type_row(name: &str, type_kind: &str) -> ObjectsPanelRow {
        let mut row = make_row("type", name);
        row.values
            .insert("data_type".to_string(), type_kind.to_string());
        row
    }

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

    #[test]
    fn groups_objects_panel_rows_by_kind() {
        let rows = vec![
            make_row("table", "users"),
            make_row("view", "active_users"),
            make_row("table", "orders"),
        ];

        let rows_by_kind = SchemaService::build_objects_panel_rows_by_kind(&rows);

        assert_eq!(rows_by_kind.get("table").map(Vec::len), Some(2));
        assert_eq!(rows_by_kind.get("view").map(Vec::len), Some(1));
        assert!(!rows_by_kind.contains_key("function"));
    }

    #[test]
    fn sidebar_metadata_names_split_domains_from_types() {
        let objects_panel_data = ObjectsPanelData {
            columns: Vec::new(),
            rows: vec![
                make_row("sequence", "public.invoice_id_seq"),
                make_type_row("public.status", "Enum"),
                make_type_row("public.email_address", "Domain"),
                make_type_row("public.price_range", "Range"),
                make_row("extension", "postgis"),
            ],
        };

        assert_eq!(
            SchemaService::object_names_by_kind(&objects_panel_data, "sequence"),
            vec!["public.invoice_id_seq".to_string()]
        );
        assert_eq!(
            SchemaService::sidebar_type_names(&objects_panel_data, true),
            vec!["public.email_address".to_string()]
        );
        assert_eq!(
            SchemaService::sidebar_type_names(&objects_panel_data, false),
            vec![
                "public.status".to_string(),
                "public.price_range".to_string()
            ]
        );
        assert_eq!(
            SchemaService::object_names_by_kind(&objects_panel_data, "extension"),
            vec!["postgis".to_string()]
        );
    }

    #[test]
    fn cached_objects_panel_rows_for_kind_respect_cache_validity() {
        let connection_id = Uuid::new_v4();
        let cache = Arc::new(SchemaCache::new(std::time::Duration::from_secs(300)));
        let service = SchemaService::with_cache(cache.clone());

        cache.set_tables(connection_id, Vec::new());
        cache.set_objects_panel_rows_by_kind(
            connection_id,
            HashMap::from([
                ("table".to_string(), vec![make_row("table", "users")]),
                ("view".to_string(), vec![make_row("view", "active_users")]),
            ]),
        );

        let table_rows = service.get_cached_objects_panel_rows_for_kind(connection_id, "table");
        assert_eq!(table_rows.map(|rows| rows.len()), Some(1));

        cache.invalidate(connection_id);
        let stale = service.get_cached_objects_panel_rows_for_kind(connection_id, "table");
        assert!(stale.is_none());
    }
}
