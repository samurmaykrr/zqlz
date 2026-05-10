//! Connection refresh orchestration.
//!
//! This service centralizes the non-UI decisions required to refresh connection-
//! backed surfaces so the application does not need to duplicate cache
//! invalidation, schema reload, and database-list loading logic in multiple UI
//! handlers.

use std::sync::Arc;

use uuid::Uuid;
use zqlz_connection::{ConnectionManager, SidebarObjectCapabilities};
use zqlz_core::{Connection, ConnectionScope, DocumentCollectionInfo, DriverCategory};

use crate::{
    DatabaseSchema, DocumentService, KeyValueService, SchemaService, ServiceError, ServiceResult,
};

/// A refresh request for a connected data source.
#[derive(Clone, Debug)]
pub struct RefreshRequest {
    /// Connection whose metadata-backed UI should be refreshed.
    pub connection_id: Uuid,
    /// Whether schema caches should be invalidated before reloading.
    pub invalidate_schema_cache: bool,
    /// Optional database/schema target for multi-database drivers.
    ///
    /// When present, schema loading is performed against this explicit target
    /// instead of whatever the connection reports as its current default.
    pub target_database: Option<String>,
    /// Whether to refresh the server database list as part of this request.
    ///
    /// Object/schema reloads should leave this false so a targeted schema refresh
    /// cannot cascade into sidebar database-list churn.
    pub refresh_database_list: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshIntent {
    ConnectionsList,
    ActiveConnectionSurfaces,
    ConnectionSurfaces(Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceRefreshKind {
    SidebarAndObjects,
    ConnectionsList,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshPlanStep {
    RefreshConnectionsList,
    RefreshConnectionSurfaces {
        connection_id: Option<Uuid>,
        kind: SurfaceRefreshKind,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshPlan {
    pub steps: Vec<RefreshPlanStep>,
}

impl RefreshPlan {
    pub fn from_intent(intent: RefreshIntent, connected_connection_ids: &[Uuid]) -> Self {
        let steps = match intent {
            RefreshIntent::ConnectionsList => {
                std::iter::once(RefreshPlanStep::RefreshConnectionsList)
                    .chain(
                        connected_connection_ids
                            .iter()
                            .copied()
                            .map(|connection_id| RefreshPlanStep::RefreshConnectionSurfaces {
                                connection_id: Some(connection_id),
                                kind: SurfaceRefreshKind::ConnectionsList,
                            }),
                    )
                    .collect()
            }
            RefreshIntent::ActiveConnectionSurfaces => {
                vec![RefreshPlanStep::RefreshConnectionSurfaces {
                    connection_id: None,
                    kind: SurfaceRefreshKind::SidebarAndObjects,
                }]
            }
            RefreshIntent::ConnectionSurfaces(connection_id) => {
                vec![RefreshPlanStep::RefreshConnectionSurfaces {
                    connection_id: Some(connection_id),
                    kind: SurfaceRefreshKind::SidebarAndObjects,
                }]
            }
        };

        Self { steps }
    }
}

/// The fully refreshed state for a connection.
#[derive(Clone, Debug)]
pub struct ConnectionRefresh {
    /// Connection that was refreshed.
    pub connection_id: Uuid,
    /// Refreshed payload for the connection type.
    pub payload: ConnectionRefreshPayload,
}

/// Typed refresh payload for a connected data source.
#[derive(Clone, Debug)]
pub enum ConnectionRefreshPayload {
    /// Refresh payload for SQL-like drivers.
    Relational(Box<RelationalConnectionRefresh>),
    /// Refresh payload for key-value drivers.
    KeyValue(KeyValueConnectionRefresh),
    /// Refresh payload for document drivers.
    Document(DocumentConnectionRefresh),
}

/// Refreshed metadata for relational-style connections.
#[derive(Clone, Debug)]
pub struct RelationalConnectionRefresh {
    /// Refreshed schema snapshot.
    pub schema: DatabaseSchema,
    /// Best-effort list of available databases. `None` means listing databases
    /// was not possible and existing UI state should be preserved.
    pub databases: Option<Vec<(String, Option<i64>)>>,
    /// Driver category used by UI components that vary their presentation.
    pub driver_category: DriverCategory,
    /// Sidebar object capabilities resolved for this connection.
    pub object_capabilities: SidebarObjectCapabilities,
}

/// Refreshed metadata for key-value connections.
#[derive(Clone, Debug)]
pub struct KeyValueConnectionRefresh {
    /// Logical databases keyed by index with best-effort sizes/counts.
    pub databases: Vec<(u16, Option<i64>)>,
    /// Sidebar object capabilities resolved for this connection.
    pub object_capabilities: SidebarObjectCapabilities,
}

/// Refreshed metadata for document connections.
#[derive(Clone, Debug)]
pub struct DocumentConnectionRefresh {
    /// Document database names with best-effort sizes.
    pub databases: Vec<(String, Option<i64>)>,
    /// Document collections loaded for available databases.
    pub collections: Vec<DocumentCollectionInfo>,
    /// Sidebar object capabilities resolved for this connection.
    pub object_capabilities: SidebarObjectCapabilities,
}

/// Service responsible for refreshing connection-backed metadata.
pub struct RefreshService {
    connection_manager: Arc<ConnectionManager>,
    schema_service: Arc<SchemaService>,
    key_value_service: Arc<KeyValueService>,
    document_service: Arc<DocumentService>,
}

impl RefreshService {
    /// Creates a new refresh service.
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        schema_service: Arc<SchemaService>,
        key_value_service: Arc<KeyValueService>,
        document_service: Arc<DocumentService>,
    ) -> Self {
        Self {
            connection_manager,
            schema_service,
            key_value_service,
            document_service,
        }
    }

    /// Reloads the metadata required by schema-oriented UI surfaces.
    pub async fn refresh_connection(
        &self,
        request: RefreshRequest,
    ) -> ServiceResult<ConnectionRefresh> {
        let connection = self
            .connection_manager
            .get(request.connection_id)
            .ok_or(ServiceError::ConnectionNotFound)?;

        if request.invalidate_schema_cache {
            self.schema_service
                .invalidate_connection_cache(request.connection_id);
        }

        match connection.driver_category() {
            DriverCategory::KeyValue => {
                return self
                    .refresh_key_value(connection, request.connection_id)
                    .await;
            }
            DriverCategory::Document => {
                return self
                    .refresh_document(connection, request.connection_id)
                    .await;
            }
            _ => {}
        }

        self.refresh_relational(
            connection,
            request.connection_id,
            request.target_database.as_deref(),
            request.refresh_database_list,
        )
        .await
    }

    async fn refresh_relational(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        target_database: Option<&str>,
        refresh_database_list: bool,
    ) -> ServiceResult<ConnectionRefresh> {
        let driver_category = connection.driver_category();
        let object_capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());
        let requested_scope = target_database
            .map(|database_name| ConnectionScope::Database(database_name.to_string()))
            .unwrap_or(ConnectionScope::Default);
        let resolved_scope = connection
            .resolve_scope(requested_scope)
            .await
            .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))?;
        let schema_connection = if resolved_scope.requires_dedicated_connection {
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
            self.connection_manager
                .get_for_database(connection_id, database_key)
                .await
                .map_err(|error| ServiceError::ConnectionFailed(error.to_string()))?
        } else {
            connection.clone()
        };
        let schema = self
            .schema_service
            .load_database_schema_for_database(
                schema_connection.clone(),
                connection_id,
                resolved_scope.effective_database.as_deref(),
            )
            .await?;

        let databases = if refresh_database_list {
            if let Some(schema_introspection) = connection.as_schema_introspection() {
                match schema_introspection.list_databases().await {
                    Ok(databases) => Some(
                        databases
                            .into_iter()
                            .map(|database| (database.name, database.size_bytes))
                            .collect(),
                    ),
                    Err(error) => {
                        tracing::warn!(
                            connection_id = %connection_id,
                            %error,
                            "Failed to refresh database list while refreshing connection"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(ConnectionRefresh {
            connection_id,
            payload: ConnectionRefreshPayload::Relational(Box::new(RelationalConnectionRefresh {
                schema,
                databases,
                driver_category,
                object_capabilities,
            })),
        })
    }

    async fn refresh_key_value(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<ConnectionRefresh> {
        let object_capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());
        let databases = self.key_value_service.load_databases(connection).await?;

        Ok(ConnectionRefresh {
            connection_id,
            payload: ConnectionRefreshPayload::KeyValue(KeyValueConnectionRefresh {
                databases,
                object_capabilities,
            }),
        })
    }

    async fn refresh_document(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<ConnectionRefresh> {
        let object_capabilities = SidebarObjectCapabilities::for_connection(connection.as_ref());
        let databases = self
            .document_service
            .list_databases(connection.clone())
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let mut collections = Vec::new();

        for database in &databases {
            match self
                .document_service
                .list_collections(connection.clone(), &database.name)
                .await
            {
                Ok(database_collections) => collections.extend(database_collections),
                Err(error) => {
                    tracing::warn!(
                        connection_id = %connection_id,
                        database_name = %database.name,
                        %error,
                        "Failed to refresh document collections for database"
                    );
                }
            }
        }

        let databases = databases
            .into_iter()
            .map(|database| {
                (
                    database.name,
                    database
                        .size_bytes
                        .and_then(|size| i64::try_from(size).ok()),
                )
            })
            .collect();

        Ok(ConnectionRefresh {
            connection_id,
            payload: ConnectionRefreshPayload::Document(DocumentConnectionRefresh {
                databases,
                collections,
                object_capabilities,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{RefreshIntent, RefreshPlan, RefreshPlanStep, SurfaceRefreshKind};
    use uuid::Uuid;

    #[test]
    fn refresh_plan_expands_connections_list_to_list_and_connected_surfaces() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        let plan = RefreshPlan::from_intent(RefreshIntent::ConnectionsList, &[first, second]);

        assert_eq!(
            plan.steps,
            vec![
                RefreshPlanStep::RefreshConnectionsList,
                RefreshPlanStep::RefreshConnectionSurfaces {
                    connection_id: Some(first),
                    kind: SurfaceRefreshKind::ConnectionsList,
                },
                RefreshPlanStep::RefreshConnectionSurfaces {
                    connection_id: Some(second),
                    kind: SurfaceRefreshKind::ConnectionsList,
                },
            ]
        );
    }

    #[test]
    fn refresh_plan_keeps_active_connection_unresolved_for_app_layer() {
        let plan = RefreshPlan::from_intent(RefreshIntent::ActiveConnectionSurfaces, &[]);

        assert_eq!(
            plan.steps,
            vec![RefreshPlanStep::RefreshConnectionSurfaces {
                connection_id: None,
                kind: SurfaceRefreshKind::SidebarAndObjects,
            }]
        );
    }
}
