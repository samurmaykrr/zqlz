//! Connection refresh orchestration.
//!
//! This service centralizes the non-UI decisions required to refresh connection-
//! backed surfaces so the application does not need to duplicate cache
//! invalidation, schema reload, and database-list loading logic in multiple UI
//! handlers.

use std::sync::Arc;

use uuid::Uuid;
use zqlz_connection::ConnectionManager;
use zqlz_core::{Connection, DriverCategory};

use crate::{DatabaseSchema, SchemaService, ServiceError, ServiceResult};

/// A refresh request for a connected data source.
#[derive(Clone, Copy, Debug)]
pub struct RefreshRequest {
    /// Connection whose metadata-backed UI should be refreshed.
    pub connection_id: Uuid,
    /// Whether schema caches should be invalidated before reloading.
    pub invalidate_schema_cache: bool,
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
    Relational(RelationalConnectionRefresh),
    /// Refresh payload for Redis-like drivers.
    Redis(RedisConnectionRefresh),
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
}

/// Refreshed metadata for Redis-style connections.
#[derive(Clone, Debug)]
pub struct RedisConnectionRefresh {
    /// Redis databases keyed by index with their best-effort key counts.
    pub databases: Vec<(u16, Option<i64>)>,
}

/// Service responsible for refreshing connection-backed metadata.
pub struct RefreshService {
    connection_manager: Arc<ConnectionManager>,
    schema_service: Arc<SchemaService>,
}

impl RefreshService {
    /// Creates a new refresh service.
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        schema_service: Arc<SchemaService>,
    ) -> Self {
        Self {
            connection_manager,
            schema_service,
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

        if connection.driver_name() == "redis" {
            return self.refresh_redis(connection, request.connection_id).await;
        }

        self.refresh_relational(connection, request.connection_id)
            .await
    }

    async fn refresh_relational(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<ConnectionRefresh> {
        let driver_category = driver_category_for(connection.driver_name());
        let schema = self
            .schema_service
            .load_database_schema(connection.clone(), connection_id)
            .await?;

        let databases = if let Some(schema_introspection) = connection.as_schema_introspection() {
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
        };

        Ok(ConnectionRefresh {
            connection_id,
            payload: ConnectionRefreshPayload::Relational(RelationalConnectionRefresh {
                schema,
                databases,
                driver_category,
            }),
        })
    }

    async fn refresh_redis(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
    ) -> ServiceResult<ConnectionRefresh> {
        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let databases = schema_introspection
            .list_databases()
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?
            .into_iter()
            .filter_map(|database| {
                database
                    .name
                    .strip_prefix("db")
                    .and_then(|value| value.parse::<u16>().ok())
                    .map(|index| (index, database.size_bytes))
            })
            .collect();

        Ok(ConnectionRefresh {
            connection_id,
            payload: ConnectionRefreshPayload::Redis(RedisConnectionRefresh { databases }),
        })
    }
}

fn driver_category_for(driver_name: &str) -> DriverCategory {
    match driver_name.to_ascii_lowercase().as_str() {
        "redis" | "memcached" | "valkey" => DriverCategory::KeyValue,
        "mongodb" | "couchdb" => DriverCategory::Document,
        "influxdb" | "timescaledb" => DriverCategory::TimeSeries,
        "neo4j" => DriverCategory::Graph,
        "elasticsearch" => DriverCategory::Search,
        _ => DriverCategory::Relational,
    }
}
