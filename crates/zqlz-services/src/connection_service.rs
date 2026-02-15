//! Connection lifecycle service
//!
//! Orchestrates connection establishment, schema loading, and disconnection.

use std::sync::Arc;
use uuid::Uuid;
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_core::Connection;

use crate::error::{ServiceError, ServiceResult};
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
}

impl ConnectionService {
    /// Create a new connection service
    ///
    /// # Arguments
    ///
    /// * `manager` - Connection manager for low-level connection operations
    /// * `schema_service` - Schema service for loading metadata after connection
    pub fn new(manager: Arc<ConnectionManager>, schema_service: Arc<SchemaService>) -> Self {
        Self {
            manager,
            schema_service,
        }
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
        // Skip schema loading for Redis - it doesn't have traditional SQL schema
        // Redis database/key loading is handled separately in the UI layer
        let is_redis = connection.driver_name() == "redis";
        let schema = if is_redis {
            tracing::info!("Skipping schema load for Redis connection");
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
    pub async fn load_schema(
        &self,
        connection_id: Uuid,
    ) -> ServiceResult<DatabaseSchema> {
        tracing::info!("Loading schema for connection: {}", connection_id);

        let connection = self.manager.get(connection_id).ok_or_else(|| {
            tracing::error!("Connection {} not found", connection_id);
            ServiceError::ConnectionNotFound
        })?;

        // Skip schema loading for Redis
        if connection.driver_name() == "redis" {
            tracing::info!("Skipping schema load for Redis connection");
            return Err(ServiceError::SchemaNotSupported);
        }

        self.schema_service
            .load_database_schema(connection, connection_id)
            .await
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

    /// Get an active connection by ID
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
