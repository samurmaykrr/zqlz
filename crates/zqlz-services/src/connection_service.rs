//! Connection lifecycle service
//!
//! Orchestrates connection establishment, schema loading, and disconnection.

use std::sync::Arc;
use uuid::Uuid;
use zqlz_connection::{
    ConnectionManager, SavedConnection, SidebarObjectCapabilities, SidebarSection,
};
use zqlz_core::{
    Connection, ConnectionFeatureSet, ConnectionScope, DriverCategory, ObjectFormDdlRequest,
    ObjectFormSpec, ObjectFormSpecRequest, ObjectsPanelData, ObjectsPanelManifest, TableInfo,
};

use crate::error::{ServiceError, ServiceResult};
use crate::key_value_service::KeyValueService;
use crate::schema_service::SchemaService;
use crate::view_models::DatabaseSchema;

/// Service for connection lifecycle management
///
/// This service orchestrates the connection process by:
/// - Establishing connections via ConnectionManager
/// - Automatically loading schema after connection
/// - Handling disconnection and cleanup
/// - Testing connections without activating them
pub struct ConnectionService {
    manager: Arc<ConnectionManager>,
    schema_service: Arc<SchemaService>,
    key_value_service: Arc<KeyValueService>,
}

/// Resolved connection target for workflows that may address a specific logical
/// database or namespace.
pub struct ResolvedConnection {
    /// Connection ID this resolution belongs to.
    pub connection_id: Uuid,
    /// Connection handle to use for downstream operations.
    pub connection: Arc<dyn Connection>,
    /// Driver name from the resolved connection.
    pub driver_name: String,
    /// Driver category for workflow behavior decisions.
    pub driver_category: DriverCategory,
    /// Requested scope after driver-specific normalization.
    pub scope: ConnectionScope,
    /// Effective logical database selected by this resolution.
    pub effective_database: Option<String>,
    /// Backward-compatible effective database name for existing callers.
    pub effective_database_name: Option<String>,
    /// Effective namespace/schema selected by this resolution.
    pub effective_namespace: Option<String>,
    /// Whether this scope required a dedicated physical/session connection.
    pub requires_dedicated_connection: bool,
    /// Whether the resolved connection targets key-value semantics.
    pub is_key_value: bool,
}

/// Backward-compatible alias for older callers while scope-aware call sites are
/// migrated.
pub type DatabaseScopedConnection = ResolvedConnection;

/// Relational sidebar bootstrap payload for a connected data source.
#[derive(Debug, Clone)]
pub struct RelationalSidebarBootstrap {
    /// Full table metadata needed for table-first sidebar rendering.
    pub tables: Vec<TableInfo>,
    /// Table names derived from `tables` for direct sidebar/editor updates.
    pub table_names: Vec<String>,
    /// Resolved schema name for hierarchy-aware table rendering.
    pub resolved_schema_name: Option<String>,
    /// Known schema names for PostgreSQL-style schema selectors.
    pub schema_names: Vec<String>,
    /// Objects panel dataset derived from relational table metadata.
    pub objects_panel_data: ObjectsPanelData,
    /// Objects panel behavior metadata derived from `objects_panel_data`.
    pub objects_panel_manifest: ObjectsPanelManifest,
    /// Driver category used by objects panel rendering behavior.
    pub driver_category: DriverCategory,
    /// Sidebar object capabilities for the connected driver.
    pub object_capabilities: SidebarObjectCapabilities,
}

/// Best-effort database metadata used by multi-database sidebars.
#[derive(Debug, Clone)]
pub struct ConnectionDatabaseEntry {
    /// Database name as reported by introspection.
    pub name: String,
    /// Optional database size in bytes.
    pub size_bytes: Option<i64>,
}

/// Typed database-discovery outcome for sidebar bootstrap orchestration.
#[derive(Debug, Clone)]
pub enum DiscoverDatabasesSidebarOutcome {
    /// Database list is available for sidebar merge.
    Loaded(Vec<ConnectionDatabaseEntry>),
    /// No database list is available for this connection.
    NoData,
    /// Database discovery failed with a warning-worthy reason.
    Failed(String),
}

/// Redis database metadata for sidebar rendering.
#[derive(Debug, Clone)]
pub struct RedisDatabaseEntry {
    /// Redis logical database index.
    pub index: u16,
    /// Optional best-effort key count or size proxy.
    pub size_bytes: Option<i64>,
}

/// Result of loading one sidebar section during connect bootstrap.
#[derive(Debug, Clone)]
pub struct SidebarSectionLoadResult {
    /// Section that was attempted.
    pub section: SidebarSection,
    /// Typed outcome for this section.
    pub outcome: SidebarSectionLoadOutcome,
}

/// Command-palette schema command payload derived from cached schema and
/// connection metadata.
#[derive(Debug, Clone)]
pub struct PaletteSchemaCommandsData {
    pub connection_name: String,
    pub object_capabilities: SidebarObjectCapabilities,
    pub tables: Vec<String>,
    pub views: Vec<String>,
}

/// Outcome for a sidebar section load attempt.
#[derive(Debug, Clone)]
pub enum SidebarSectionLoadOutcome {
    /// Section loaded successfully, including the empty-state case.
    Loaded(Vec<String>),
    /// Section failed to load; caller can clear loading indicators.
    Failed(String),
}

/// Typed lazy-load outcome for one sidebar section request.
#[derive(Debug, Clone)]
pub enum LazySidebarSectionLoadOutcome {
    /// Section names loaded successfully.
    Loaded(Vec<String>),
    /// No section data is available for the current request context.
    ///
    /// This covers both unsupported sections and disconnected/unavailable
    /// connections so app callers can keep lazy-load cleanup behavior stable
    /// without coupling to service error variants.
    NoData,
    /// Section load failed with a warning-worthy reason.
    Failed(String),
}

impl ConnectionService {
    fn get_connection_or_error(&self, connection_id: Uuid) -> ServiceResult<Arc<dyn Connection>> {
        self.manager
            .get(connection_id)
            .ok_or(ServiceError::ConnectionNotFound)
    }

    fn sidebar_names_from_load_data(data: crate::SidebarSectionLoadData) -> Vec<String> {
        match data {
            crate::SidebarSectionLoadData::Views(names)
            | crate::SidebarSectionLoadData::MaterializedViews(names)
            | crate::SidebarSectionLoadData::Functions(names)
            | crate::SidebarSectionLoadData::Procedures(names)
            | crate::SidebarSectionLoadData::Triggers(names)
            | crate::SidebarSectionLoadData::Events(names)
            | crate::SidebarSectionLoadData::Sequences(names)
            | crate::SidebarSectionLoadData::Domains(names)
            | crate::SidebarSectionLoadData::Types(names)
            | crate::SidebarSectionLoadData::Extensions(names) => names,
        }
    }

    /// Create a new connection service
    ///
    /// # Arguments
    ///
    /// * `manager` - Connection manager for low-level connection operations
    /// * `schema_service` - Schema service for loading metadata after connection
    pub fn new(
        manager: Arc<ConnectionManager>,
        schema_service: Arc<SchemaService>,
        key_value_service: Arc<KeyValueService>,
    ) -> Self {
        Self {
            manager,
            schema_service,
            key_value_service,
        }
    }

    /// Build a duplicated saved-connection configuration.
    ///
    /// This preserves all driver parameters while assigning a new identifier
    /// and the same display naming convention used by connection duplication UI
    /// flows.
    pub fn build_duplicate_saved_connection(
        &self,
        saved_connection: &SavedConnection,
    ) -> SavedConnection {
        let mut duplicated_connection = saved_connection.clone();
        duplicated_connection.id = Uuid::new_v4();
        duplicated_connection.name = format!("{} (Copy)", duplicated_connection.name);
        duplicated_connection
    }

    /// Parse an external target (database file or URL) into a saved connection.
    ///
    /// Returning `Ok(None)` allows callers to keep unsupported-target handling as
    /// a non-error UX path while centralizing import parsing policy in the
    /// services layer.
    pub fn import_saved_connection_from_external_target(
        &self,
        target: &str,
    ) -> ServiceResult<Option<SavedConnection>> {
        SavedConnection::from_external_target(target)
            .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))
    }

    /// Persist a saved connection in the active manager snapshot.
    ///
    /// This helper centralizes the manager write path used by app-shell import
    /// and duplication flows.
    pub fn persist_saved_connection(&self, saved_connection: SavedConnection) {
        if self
            .manager
            .saved_connections()
            .iter()
            .any(|existing| existing.id == saved_connection.id)
        {
            self.manager.update_saved(saved_connection);
        } else {
            self.manager.add_saved(saved_connection);
        }
    }

    /// Remove a saved connection from the active manager snapshot.
    pub fn remove_saved_connection(&self, connection_id: Uuid) {
        self.manager.remove_saved(connection_id);
    }

    /// Connect to a database and load initial schema
    ///
    /// This is the primary method for establishing connections. It performs:
    /// 1. Connection establishment via ConnectionManager
    /// 2. Automatic schema loading (best effort, non-blocking)
    /// 3. Returns connection info for UI
    ///
    /// # Arguments
    ///
    /// * `saved_connection` - Connection configuration
    ///
    /// # Returns
    ///
    /// A `ConnectionInfo` containing the connection ID, name, and optional schema
    #[tracing::instrument(skip(self, saved_connection), fields(connection_name = %saved_connection.name))]
    pub async fn connect_and_initialize(
        &self,
        saved_connection: &SavedConnection,
    ) -> ServiceResult<ConnectionInfo> {
        tracing::info!("Connecting to database: {}", saved_connection.name);

        // Step 1: Establish connection
        let connection_id = self.manager.connect(saved_connection).await.map_err(|e| {
            tracing::error!("Connection failed: {}", e);
            ServiceError::ConnectionFailed(e.to_string())
        })?;

        tracing::info!("Connection established with ID: {}", connection_id);

        // Step 2: Get connection handle
        let connection = self.manager.get(connection_id).ok_or_else(|| {
            tracing::error!("Connection {} not found after establishment", connection_id);
            ServiceError::ConnectionNotFound
        })?;

        // Step 3: Load schema (non-blocking, best-effort)
        let is_key_value = connection.driver_category() == DriverCategory::KeyValue;
        let schema = if is_key_value {
            tracing::info!("Skipping schema load for key-value connection");
            None
        } else {
            match self
                .schema_service
                .load_database_schema(connection.clone(), connection_id)
                .await
            {
                Ok(s) => {
                    tracing::info!(
                        tables = s.tables.len(),
                        views = s.views.len(),
                        "Schema loaded successfully"
                    );
                    Some(s)
                }
                Err(e) => {
                    tracing::warn!("Failed to load schema (non-fatal): {}", e);
                    None
                }
            }
        };

        Ok(ConnectionInfo {
            id: connection_id,
            name: saved_connection.name.clone(),
            driver: saved_connection.driver.clone(),
            schema,
        })
    }

    /// Connect to a database and load initial schema for a specific target database.
    ///
    /// This is useful when the caller already knows the intended logical database
    /// differs from the connection's current default and needs deterministic
    /// introspection for that target.
    #[tracing::instrument(skip(self, saved_connection), fields(connection_name = %saved_connection.name, target_database = ?target_database))]
    pub async fn connect_and_initialize_for_database(
        &self,
        saved_connection: &SavedConnection,
        target_database: Option<&str>,
    ) -> ServiceResult<ConnectionInfo> {
        tracing::info!("Connecting to database: {}", saved_connection.name);

        let connection_id = self.manager.connect(saved_connection).await.map_err(|e| {
            tracing::error!("Connection failed: {}", e);
            ServiceError::ConnectionFailed(e.to_string())
        })?;

        tracing::info!("Connection established with ID: {}", connection_id);

        let connection = self.manager.get(connection_id).ok_or_else(|| {
            tracing::error!("Connection {} not found after establishment", connection_id);
            ServiceError::ConnectionNotFound
        })?;

        let is_key_value = connection.driver_category() == DriverCategory::KeyValue;
        let schema = if is_key_value {
            tracing::info!("Skipping schema load for key-value connection");
            None
        } else {
            let scope = target_database
                .map(|database_name| ConnectionScope::Database(database_name.to_string()))
                .unwrap_or(ConnectionScope::Default);
            let resolved_connection = self.resolve_connection(connection_id, scope).await?;
            match self
                .schema_service
                .load_database_schema_for_database(
                    resolved_connection.connection.clone(),
                    connection_id,
                    resolved_connection.effective_database.as_deref(),
                )
                .await
            {
                Ok(s) => {
                    tracing::info!(
                        tables = s.tables.len(),
                        views = s.views.len(),
                        "Schema loaded successfully"
                    );
                    Some(s)
                }
                Err(e) => {
                    tracing::warn!("Failed to load schema (non-fatal): {}", e);
                    None
                }
            }
        };

        Ok(ConnectionInfo {
            id: connection_id,
            name: saved_connection.name.clone(),
            driver: saved_connection.driver.clone(),
            schema,
        })
    }

    /// Connect to a database quickly without loading schema
    ///
    /// This method only establishes the connection and returns immediately.
    /// Schema loading should be done separately in the background.
    ///
    /// # Arguments
    ///
    /// * `saved_connection` - Connection configuration
    ///
    /// # Returns
    ///
    /// A `ConnectionInfo` with the connection ID and name (schema will be None)
    #[tracing::instrument(skip(self, saved_connection), fields(connection_name = %saved_connection.name))]
    pub async fn connect_fast(
        &self,
        saved_connection: &SavedConnection,
    ) -> ServiceResult<ConnectionInfo> {
        tracing::info!("Fast connecting to database: {}", saved_connection.name);

        let connection_id = self.manager.connect(saved_connection).await.map_err(|e| {
            tracing::error!("Connection failed: {}", e);
            ServiceError::ConnectionFailed(e.to_string())
        })?;

        tracing::info!("Connection established with ID: {}", connection_id);

        Ok(ConnectionInfo {
            id: connection_id,
            name: saved_connection.name.clone(),
            driver: saved_connection.driver.clone(),
            schema: None,
        })
    }

    /// Connect quickly and return both metadata and active connection handle.
    ///
    /// App-layer connect flows need the low-latency behavior of `connect_fast`
    /// plus an immediately usable connection for bootstrap orchestration. Keeping
    /// this as one service operation removes repeated "connect, then resolve
    /// handle" branching from UI handlers.
    pub async fn connect_fast_and_resolve(
        &self,
        saved_connection: &SavedConnection,
    ) -> ServiceResult<(ConnectionInfo, Arc<dyn Connection>)> {
        let connection_info = self.connect_fast(saved_connection).await?;
        let connection = self.get_connection_or_error(connection_info.id)?;

        Ok((connection_info, connection))
    }

    /// Load schema for an existing connection
    ///
    /// This method loads schema metadata for an already-connected database.
    /// It should be called in the background after `connect_fast`.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - UUID of the connection to load schema for
    ///
    /// # Returns
    ///
    /// A `DatabaseSchema` containing all discovered objects
    #[tracing::instrument(skip(self), fields(connection_id = %connection_id))]
    pub async fn load_schema(&self, connection_id: Uuid) -> ServiceResult<DatabaseSchema> {
        self.load_schema_for_database(connection_id, None).await
    }

    /// Load schema for an existing connection using an explicit target database.
    #[tracing::instrument(skip(self), fields(connection_id = %connection_id, target_database = ?target_database))]
    pub async fn load_schema_for_database(
        &self,
        connection_id: Uuid,
        target_database: Option<&str>,
    ) -> ServiceResult<DatabaseSchema> {
        tracing::info!("Loading schema for connection: {}", connection_id);

        let connection = self.manager.get(connection_id).ok_or_else(|| {
            tracing::error!("Connection {} not found", connection_id);
            ServiceError::ConnectionNotFound
        })?;

        if connection.driver_category() == DriverCategory::KeyValue {
            tracing::info!("Skipping schema load for key-value connection");
            return Err(ServiceError::SchemaNotSupported);
        }

        let scope = target_database
            .map(|database_name| ConnectionScope::Database(database_name.to_string()))
            .unwrap_or(ConnectionScope::Default);
        let resolved_connection = self.resolve_connection(connection_id, scope).await?;

        self.schema_service
            .load_database_schema_for_database(
                resolved_connection.connection,
                connection_id,
                resolved_connection.effective_database.as_deref(),
            )
            .await
    }

    /// Resolve the physical/session connection for a typed logical scope.
    ///
    /// Most drivers reuse the main connection for scoped work. Drivers that
    /// report `requires_database_scoped_connection` get a cached dedicated
    /// connection for database/Redis scopes. Namespace scopes never create a
    /// dedicated database connection.
    pub async fn resolve_connection(
        &self,
        connection_id: Uuid,
        scope: ConnectionScope,
    ) -> ServiceResult<ResolvedConnection> {
        let main_connection = self.get_connection_or_error(connection_id)?;
        let driver_name = main_connection.driver_name().to_string();
        let driver_category = main_connection.driver_category();
        let is_key_value = driver_category == DriverCategory::KeyValue;
        let resolved_scope = main_connection
            .resolve_scope(scope)
            .await
            .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))?;

        let connection = if resolved_scope.requires_dedicated_connection {
            let database_key =
                resolved_scope
                    .physical_database_key
                    .as_deref()
                    .ok_or_else(|| {
                        ServiceError::ConnectionFailed(
                            "Driver requested dedicated connection without database key"
                                .to_string(),
                        )
                    })?;
            self.manager
                .get_for_database(connection_id, database_key)
                .await
                .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))?
        } else {
            main_connection
        };

        Ok(ResolvedConnection {
            connection_id,
            connection,
            driver_name,
            driver_category,
            scope: resolved_scope.normalized_scope,
            effective_database_name: resolved_scope.effective_database.clone(),
            effective_database: resolved_scope.effective_database,
            effective_namespace: resolved_scope.effective_namespace,
            requires_dedicated_connection: resolved_scope.requires_dedicated_connection,
            is_key_value,
        })
    }

    /// Resolves the connection that should be used when a workflow may target
    /// a specific logical database.
    ///
    /// For PostgreSQL flows, callers can accidentally pass a schema name as a
    /// database target. This method normalizes that case by preferring the
    /// active session database.
    pub async fn resolve_database_scoped_connection(
        &self,
        connection_id: Uuid,
        requested_database: Option<String>,
    ) -> ServiceResult<DatabaseScopedConnection> {
        let scope = match requested_database {
            Some(database_name) => ConnectionScope::Database(database_name),
            None => ConnectionScope::Default,
        };
        self.resolve_connection(connection_id, scope).await
    }

    /// Load table-first relational sidebar bootstrap data for a connected
    /// relational connection.
    pub async fn load_relational_sidebar_bootstrap(
        &self,
        connection_id: Uuid,
    ) -> ServiceResult<RelationalSidebarBootstrap> {
        let connection = self.get_connection_or_error(connection_id)?;
        if connection.driver_category() == DriverCategory::KeyValue {
            return Err(ServiceError::SchemaNotSupported);
        }

        let tables = self
            .schema_service
            .load_tables_only(connection.clone(), connection_id)
            .await?;
        let table_names: Vec<String> = tables.iter().map(|table| table.name.clone()).collect();
        let resolved_schema_name = self
            .schema_service
            .get_schema_name_cached(&connection, connection_id)
            .await;
        let schema_names = if let Some(introspection) = connection.as_schema_introspection() {
            introspection
                .list_schemas()
                .await
                .map(|schemas| schemas.into_iter().map(|schema| schema.name).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let objects_panel_data = if let Some(introspection) = connection.as_schema_introspection() {
            introspection
                .list_tables_extended(None)
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(
                        connection_id = %connection_id,
                        %error,
                        "Failed to load extended sidebar objects panel data, falling back to table metadata"
                    );
                    ObjectsPanelData::from_table_infos(tables.clone())
                })
        } else {
            ObjectsPanelData::from_table_infos(tables.clone())
        };
        let objects_panel_manifest =
            if let Some(introspection) = connection.as_schema_introspection() {
                introspection
                    .list_objects_panel_manifest(None)
                    .await
                    .unwrap_or_else(|_| ObjectsPanelManifest::from_data(&objects_panel_data))
            } else {
                ObjectsPanelManifest::from_data(&objects_panel_data)
            };
        objects_panel_manifest.validate().map_err(|error| {
            ServiceError::SchemaLoadFailed(format!(
                "Derived relational sidebar objects panel manifest is invalid: {}",
                error
            ))
        })?;
        let driver_category = connection.driver_category();
        let object_capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());

        Ok(RelationalSidebarBootstrap {
            tables,
            table_names,
            resolved_schema_name,
            schema_names,
            objects_panel_data,
            objects_panel_manifest,
            driver_category,
            object_capabilities,
        })
    }

    /// Discover databases for a connected data source.
    ///
    /// This is best-effort and intentionally returns `Ok(None)` when database
    /// listing is unsupported or fails, so callers can preserve existing UI
    /// state without surfacing connection-level errors.
    pub async fn discover_connection_databases(
        &self,
        connection_id: Uuid,
    ) -> ServiceResult<Option<Vec<ConnectionDatabaseEntry>>> {
        let connection = self.get_connection_or_error(connection_id)?;
        let Some(introspection) = connection.as_schema_introspection() else {
            return Ok(None);
        };

        match introspection.list_databases().await {
            Ok(databases) => Ok(Some(
                databases
                    .into_iter()
                    .map(|database| ConnectionDatabaseEntry {
                        name: database.name,
                        size_bytes: database.size_bytes,
                    })
                    .collect(),
            )),
            Err(error) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    %error,
                    "Failed to discover databases"
                );
                Ok(None)
            }
        }
    }

    /// Discover databases and classify a sidebar-friendly bootstrap outcome.
    ///
    /// This keeps best-effort discovery policy in the service layer so app
    /// handlers can stay focused on UI orchestration.
    pub async fn discover_databases_sidebar_outcome(
        &self,
        connection_id: Uuid,
    ) -> DiscoverDatabasesSidebarOutcome {
        match self.discover_connection_databases(connection_id).await {
            Ok(Some(databases)) => DiscoverDatabasesSidebarOutcome::Loaded(databases),
            Ok(None) | Err(ServiceError::ConnectionNotFound) => {
                DiscoverDatabasesSidebarOutcome::NoData
            }
            Err(error) => DiscoverDatabasesSidebarOutcome::Failed(error.to_string()),
        }
    }

    /// Load Redis database metadata for a connected Redis source.
    pub async fn load_redis_sidebar_databases(
        &self,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<RedisDatabaseEntry>> {
        let connection = self.get_connection_or_error(connection_id)?;
        self.key_value_service
            .load_databases(connection)
            .await
            .map(|databases| {
                databases
                    .into_iter()
                    .map(|(index, size_bytes)| RedisDatabaseEntry { index, size_bytes })
                    .collect()
            })
    }

    /// Load all supported non-table sidebar sections for a connected source.
    ///
    /// The returned vector only includes sections supported by the active
    /// driver. Each section includes a typed success/failure outcome.
    pub async fn load_supported_sidebar_sections(
        &self,
        connection_id: Uuid,
    ) -> ServiceResult<Vec<SidebarSectionLoadResult>> {
        let connection = self.get_connection_or_error(connection_id)?;
        let capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());
        let mut results = Vec::new();

        for section in [
            SidebarSection::Views,
            SidebarSection::MaterializedViews,
            SidebarSection::Functions,
            SidebarSection::Procedures,
            SidebarSection::Triggers,
            SidebarSection::Events,
            SidebarSection::Sequences,
            SidebarSection::Domains,
            SidebarSection::Types,
            SidebarSection::Extensions,
        ] {
            if !capabilities.supports_section(section) {
                continue;
            }

            let outcome = match self
                .schema_service
                .load_sidebar_section_data(connection.clone(), connection_id, section)
                .await
            {
                Ok(Some(data)) => {
                    SidebarSectionLoadOutcome::Loaded(Self::sidebar_names_from_load_data(data))
                }
                Ok(None) => SidebarSectionLoadOutcome::Loaded(Vec::new()),
                Err(error) => SidebarSectionLoadOutcome::Failed(error.to_string()),
            };

            results.push(SidebarSectionLoadResult { section, outcome });
        }

        Ok(results)
    }

    /// Load one supported sidebar section and return its object names.
    pub async fn load_sidebar_section_names(
        &self,
        connection_id: Uuid,
        section: SidebarSection,
    ) -> ServiceResult<Option<Vec<String>>> {
        let connection = self.get_connection_or_error(connection_id)?;
        let capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());

        if !capabilities.supports_section(section) {
            return Ok(None);
        }

        self.schema_service
            .load_sidebar_section_data(connection.clone(), connection_id, section)
            .await
            .map(|data| data.map(Self::sidebar_names_from_load_data))
    }

    /// Load one supported sidebar section and classify the lazy-load outcome
    /// for app orchestration.
    pub async fn load_sidebar_section_lazy_outcome(
        &self,
        connection_id: Uuid,
        section: SidebarSection,
    ) -> LazySidebarSectionLoadOutcome {
        match self
            .load_sidebar_section_names(connection_id, section)
            .await
        {
            Ok(Some(names)) => LazySidebarSectionLoadOutcome::Loaded(names),
            Ok(None) | Err(ServiceError::ConnectionNotFound) => {
                LazySidebarSectionLoadOutcome::NoData
            }
            Err(error) => LazySidebarSectionLoadOutcome::Failed(error.to_string()),
        }
    }

    pub async fn load_objects_panel_kind_data(
        &self,
        connection_id: Uuid,
        target_database: Option<String>,
        kind_id: &str,
        scope: Option<String>,
    ) -> ServiceResult<ObjectsPanelData> {
        let scope_request = target_database
            .clone()
            .map(ConnectionScope::Database)
            .unwrap_or(ConnectionScope::Default);
        let resolved_connection = self
            .resolve_connection(connection_id, scope_request)
            .await?;

        self.schema_service
            .load_objects_panel_data_for_kind(
                resolved_connection.connection,
                connection_id,
                resolved_connection.effective_database.as_deref(),
                kind_id,
                scope.as_deref(),
            )
            .await
    }

    pub async fn object_form_spec(
        &self,
        connection_id: Uuid,
        target_database: Option<String>,
        request: &ObjectFormSpecRequest,
    ) -> ServiceResult<Option<ObjectFormSpec>> {
        let scope_request = target_database
            .clone()
            .map(ConnectionScope::Database)
            .unwrap_or(ConnectionScope::Default);
        let resolved_connection = self
            .resolve_connection(connection_id, scope_request)
            .await?;

        self.schema_service
            .object_form_spec(resolved_connection.connection, request)
            .await
    }

    pub async fn generate_object_form_ddl(
        &self,
        connection_id: Uuid,
        target_database: Option<String>,
        request: &ObjectFormDdlRequest,
    ) -> ServiceResult<Vec<String>> {
        let scope_request = target_database
            .clone()
            .map(ConnectionScope::Database)
            .unwrap_or(ConnectionScope::Default);
        let resolved_connection = self
            .resolve_connection(connection_id, scope_request)
            .await?;

        self.schema_service
            .generate_object_form_ddl(resolved_connection.connection, request)
            .await
    }

    /// Build command-palette schema command inputs for a connection from cache.
    pub fn build_palette_schema_commands_data(
        &self,
        connection_id: Uuid,
        schema_service: &SchemaService,
    ) -> Option<PaletteSchemaCommandsData> {
        let connection = self.get_connection(connection_id)?;
        let object_capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());
        let connection_name = self
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());
        let tables: Vec<String> = schema_service
            .get_cached_tables(connection_id)
            .unwrap_or_default()
            .into_iter()
            .map(|table| table.name)
            .collect();
        let views: Vec<String> = schema_service
            .get_cached_view_names(connection_id)
            .unwrap_or_default();

        Some(PaletteSchemaCommandsData {
            connection_name,
            object_capabilities,
            tables,
            views,
        })
    }

    /// Pre-warm table detail metadata for a connection's current table set.
    pub async fn prefetch_table_details_for_connection(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
    ) -> ServiceResult<()> {
        let connection = self.get_connection_or_error(connection_id)?;
        let introspection_schema = self
            .schema_service
            .get_introspection_schema_cached(&connection, connection_id)
            .await;
        self.schema_service
            .prefetch_all_table_details(
                connection,
                connection_id,
                table_names,
                introspection_schema,
            )
            .await;
        Ok(())
    }

    /// Disconnect from a database and cleanup
    ///
    /// This method:
    /// 1. Disconnects via ConnectionManager
    /// 2. Invalidates cached schema data
    ///
    /// # Arguments
    ///
    /// * `connection_id` - UUID of the connection to disconnect
    #[tracing::instrument(skip(self), fields(connection_id = %connection_id))]
    pub async fn disconnect(&self, connection_id: Uuid) -> ServiceResult<()> {
        tracing::info!("Disconnecting connection {}", connection_id);

        self.manager.disconnect(connection_id).await.map_err(|e| {
            tracing::error!("Disconnection failed: {}", e);
            ServiceError::DisconnectionFailed(e.to_string())
        })?;

        // Invalidate cached schema
        self.schema_service
            .invalidate_connection_cache(connection_id);

        tracing::info!("Connection {} disconnected successfully", connection_id);

        Ok(())
    }

    /// List databases for an active connection.
    pub async fn list_databases(&self, connection_id: Uuid) -> ServiceResult<Vec<String>> {
        self.manager
            .list_databases(connection_id)
            .await
            .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))
    }

    /// Test a connection without activating it
    ///
    /// This is useful for validating connection parameters before saving.
    ///
    /// # Arguments
    ///
    /// * `saved_connection` - Connection configuration to test
    ///
    /// # Returns
    ///
    /// A `TestResult` indicating success or failure with error details
    #[tracing::instrument(skip(self, saved_connection), fields(connection_name = %saved_connection.name))]
    pub async fn test_connection(
        &self,
        saved_connection: &SavedConnection,
    ) -> ServiceResult<TestResult> {
        tracing::info!("Testing connection: {}", saved_connection.name);

        // First, save temporarily so test_saved can find it
        self.manager.add_saved(saved_connection.clone());

        let result = match self.manager.test_saved(saved_connection.id).await {
            Ok(_) => {
                tracing::info!("Connection test successful");
                TestResult {
                    success: true,
                    message: "Connection successful".to_string(),
                    error: None,
                }
            }
            Err(e) => {
                tracing::warn!("Connection test failed: {}", e);
                TestResult {
                    success: false,
                    message: "Connection failed".to_string(),
                    error: Some(e.to_string()),
                }
            }
        };

        Ok(result)
    }

    /// Get the default active connection by ID.
    ///
    /// Use this only for lifecycle/default-connection operations such as ping,
    /// disconnect, database discovery, or explicitly default-scoped work. Any
    /// operation tied to a selected database, Redis DB, or namespace should use
    /// `resolve_connection` instead.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - UUID of the connection
    ///
    /// # Returns
    ///
    /// `Some(connection)` if found, `None` otherwise
    pub fn get_connection(&self, connection_id: Uuid) -> Option<Arc<dyn Connection>> {
        self.manager.get(connection_id)
    }

    /// Build a capability-driven UI feature set for an active connection.
    ///
    /// App handlers should prefer this over branching on concrete driver names.
    pub async fn connection_feature_set(
        &self,
        connection_id: Uuid,
        target_database: Option<String>,
    ) -> ServiceResult<ConnectionFeatureSet> {
        let resolved_connection = match target_database {
            Some(database_name) => {
                self.resolve_connection(connection_id, ConnectionScope::Database(database_name))
                    .await?
            }
            None => {
                self.resolve_connection(connection_id, ConnectionScope::Default)
                    .await?
            }
        };

        let manifest = if let Some(introspection) =
            resolved_connection.connection.as_schema_introspection()
        {
            let schema = resolved_connection.effective_namespace.as_deref();
            match introspection.list_objects_panel_manifest(schema).await {
                Ok(manifest) => Some(manifest),
                Err(error) => {
                    tracing::warn!(
                        connection_id = %connection_id,
                        %error,
                        "Failed to load objects panel manifest while deriving connection feature set"
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(ConnectionFeatureSet::from_connection(
            resolved_connection.connection.as_ref(),
            manifest.as_ref(),
        ))
    }

    /// Get an active connection for an optional database-scoped cache key.
    pub fn get_connection_for_database_cached(
        &self,
        connection_id: Uuid,
        database_name: Option<&str>,
    ) -> Option<Arc<dyn Connection>> {
        self.manager
            .get_for_database_cached(connection_id, database_name)
    }

    /// Resolve a connection handle for a specific database target.
    pub async fn get_connection_for_database(
        &self,
        connection_id: Uuid,
        database_name: &str,
    ) -> ServiceResult<Arc<dyn Connection>> {
        self.manager
            .get_for_database(connection_id, database_name)
            .await
            .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))
    }

    /// Resolve a saved connection configuration by ID.
    pub fn get_saved_connection(&self, connection_id: Uuid) -> ServiceResult<SavedConnection> {
        self.manager
            .saved_connections()
            .into_iter()
            .find(|saved_connection| saved_connection.id == connection_id)
            .ok_or(ServiceError::ConnectionNotFound)
    }

    /// Resolve a saved-connection display name by ID.
    pub fn get_saved_connection_name(&self, connection_id: Uuid) -> Option<String> {
        self.get_saved_connection(connection_id)
            .ok()
            .map(|saved_connection| saved_connection.name)
    }

    /// Resolve saved connection driver name by ID.
    pub fn get_saved_connection_driver(&self, connection_id: Uuid) -> Option<String> {
        self.get_saved_connection(connection_id)
            .ok()
            .map(|saved_connection| saved_connection.driver)
    }

    /// List all active connection IDs
    ///
    /// # Returns
    ///
    /// Vector of UUIDs for all active connections
    pub fn list_active_connections(&self) -> Vec<Uuid> {
        self.manager
            .saved_connections()
            .into_iter()
            .filter(|saved| self.manager.is_connected(saved.id))
            .map(|saved| saved.id)
            .collect()
    }

    /// Check whether a connection is currently active.
    pub fn is_connection_active(&self, connection_id: Uuid) -> bool {
        self.manager.is_connected(connection_id)
    }

    /// List all saved connection configurations.
    pub fn list_saved_connections(&self) -> Vec<SavedConnection> {
        self.manager.saved_connections()
    }

    /// Get a reference to the underlying connection manager
    pub fn manager(&self) -> Arc<ConnectionManager> {
        self.manager.clone()
    }

    /// Get a reference to the schema service
    pub fn schema_service(&self) -> Arc<SchemaService> {
        self.schema_service.clone()
    }
}

/// Connection information returned after successful connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Unique ID of the connection
    pub id: Uuid,
    /// User-friendly name
    pub name: String,
    /// Driver type (e.g., "sqlite", "postgres")
    pub driver: String,
    /// Optional database schema (None if loading failed)
    pub schema: Option<DatabaseSchema>,
}

/// Result of testing a connection
#[derive(Debug, Clone)]
pub struct TestResult {
    /// Whether the test succeeded
    pub success: bool,
    /// User-friendly message
    pub message: String,
    /// Error details if test failed
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_info_creation() {
        let info = ConnectionInfo {
            id: Uuid::new_v4(),
            name: "Test DB".to_string(),
            driver: "sqlite".to_string(),
            schema: None,
        };

        assert_eq!(info.name, "Test DB");
        assert_eq!(info.driver, "sqlite");
        assert!(info.schema.is_none());
    }

    #[test]
    fn test_test_result_creation() {
        let result = TestResult {
            success: true,
            message: "OK".to_string(),
            error: None,
        };

        assert!(result.success);
        assert_eq!(result.message, "OK");
        assert!(result.error.is_none());
    }
}
