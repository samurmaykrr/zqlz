//! Connection manager for handling active connections

use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{Connection, ConnectionConfig, Result, ZqlzError};
use zqlz_drivers::DriverRegistry;

use crate::SavedConnection;

/// Manages database connections
pub struct ConnectionManager {
    /// Driver registry
    drivers: DriverRegistry,

    /// Active connections
    active: RwLock<HashMap<Uuid, Arc<dyn Connection>>>,

    /// Saved connection configurations
    saved: RwLock<Vec<SavedConnection>>,

    /// Path to save connections
    storage_path: Option<PathBuf>,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new() -> Self {
        Self {
            drivers: DriverRegistry::with_defaults(),
            active: RwLock::new(HashMap::new()),
            saved: RwLock::new(Vec::new()),
            storage_path: None,
        }
    }

    /// Create a new connection manager with storage path
    pub fn with_storage_path(path: PathBuf) -> Self {
        Self {
            drivers: DriverRegistry::with_defaults(),
            active: RwLock::new(HashMap::new()),
            saved: RwLock::new(Vec::new()),
            storage_path: Some(path),
        }
    }

    /// Get the driver registry
    pub fn drivers(&self) -> &DriverRegistry {
        &self.drivers
    }

    /// Connect to a saved connection
    #[tracing::instrument(skip(self, saved), fields(connection_id = %saved.id, connection_name = %saved.name, driver = %saved.driver))]
    pub async fn connect(&self, saved: &SavedConnection) -> Result<Uuid> {
        tracing::info!("connecting to saved connection");
        let driver = self
            .drivers
            .get(&saved.driver)
            .ok_or_else(|| ZqlzError::Driver(format!("Unknown driver: {}", saved.driver)))?;

        let mut config = ConnectionConfig::new(&saved.driver, &saved.name);

        // Debug: log which params are being set (without revealing password value)
        let has_password = saved.params.contains_key("password");
        let param_keys: Vec<_> = saved.params.keys().collect();
        tracing::debug!(
            has_password = has_password,
            param_keys = ?param_keys,
            "building connection config from saved params"
        );

        for (key, value) in &saved.params {
            config = config.with_param(key, value.clone());
        }

        let conn = driver.connect(&config).await.map_err(|e| {
            tracing::error!(error = %e, "failed to connect");
            e
        })?;

        let conn_id = saved.id;
        self.active.write().insert(conn_id, conn);

        tracing::info!(connection_id = %conn_id, "connection established");
        Ok(conn_id)
    }

    /// Disconnect a connection
    #[tracing::instrument(skip(self), fields(connection_id = %id))]
    pub async fn disconnect(&self, id: Uuid) -> Result<()> {
        tracing::info!("disconnecting connection");
        let conn = self.active.write().remove(&id);
        if let Some(conn) = conn {
            conn.close().await?;
        }
        Ok(())
    }

    /// Get an active connection
    pub fn get(&self, id: Uuid) -> Option<Arc<dyn Connection>> {
        let conn = self.active.read().get(&id).cloned();
        if conn.is_none() {
            tracing::debug!(connection_id = %id, "connection not found in active pool");
        }
        conn
    }

    /// Check if a connection is active
    pub fn is_connected(&self, id: Uuid) -> bool {
        self.active.read().contains_key(&id)
    }

    /// Get all saved connections
    pub fn saved_connections(&self) -> Vec<SavedConnection> {
        self.saved.read().clone()
    }

    /// Add a saved connection
    pub fn add_saved(&self, connection: SavedConnection) {
        self.saved.write().push(connection);
    }

    /// Remove a saved connection
    pub fn remove_saved(&self, id: Uuid) {
        self.saved.write().retain(|c| c.id != id);
    }

    /// Update a saved connection
    pub fn update_saved(&self, connection: SavedConnection) {
        let mut saved = self.saved.write();
        if let Some(pos) = saved.iter().position(|c| c.id == connection.id) {
            saved[pos] = connection;
        }
    }

    /// Load connections from persistent storage
    #[tracing::instrument(skip(self))]
    pub async fn load_from_storage(&self) -> Result<()> {
        tracing::debug!("loading connections from storage");
        if let Some(ref path) = self.storage_path
            && path.exists()
        {
            let content = tokio::fs::read_to_string(path)
                .await
                .map_err(ZqlzError::Io)?;

            let connections: Vec<SavedConnection> =
                serde_json::from_str(&content).map_err(ZqlzError::Serialization)?;

            tracing::info!(count = connections.len(), "connections loaded from storage");
            *self.saved.write() = connections;
        } else {
            tracing::debug!("no storage path configured or file doesn't exist");
        }
        Ok(())
    }

    /// Save connections to persistent storage
    #[tracing::instrument(skip(self))]
    pub async fn save_to_storage(&self) -> Result<()> {
        tracing::debug!("saving connections to storage");
        if let Some(ref path) = self.storage_path {
            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(ZqlzError::Io)?;
            }

            let connections = self.saved.read().clone();
            let content =
                serde_json::to_string_pretty(&connections).map_err(ZqlzError::Serialization)?;

            tokio::fs::write(path, content)
                .await
                .map_err(ZqlzError::Io)?;

            tracing::info!(count = connections.len(), path = ?path, "connections saved to storage");
        } else {
            tracing::debug!("no storage path configured");
        }
        Ok(())
    }

    /// Get a saved connection by ID
    pub fn get_saved(&self, id: Uuid) -> Option<SavedConnection> {
        self.saved.read().iter().find(|c| c.id == id).cloned()
    }

    /// Test a saved connection without activating it
    #[tracing::instrument(skip(self), fields(connection_id = %id))]
    pub async fn test_saved(&self, id: Uuid) -> Result<()> {
        tracing::debug!("testing saved connection");
        let saved = self
            .get_saved(id)
            .ok_or_else(|| ZqlzError::NotFound("Connection not found".into()))?;

        let driver = self
            .drivers
            .get(&saved.driver)
            .ok_or_else(|| ZqlzError::Driver(format!("Unknown driver: {}", saved.driver)))?;

        let mut config = ConnectionConfig::new(&saved.driver, &saved.name);
        for (key, value) in &saved.params {
            config = config.with_param(key, value.clone());
        }

        driver.test_connection(&config).await
    }

    /// List databases for an active connection
    ///
    /// Returns list of database names available on the connection.
    /// This uses the SchemaIntrospection trait to query database metadata.
    #[tracing::instrument(skip(self), fields(connection_id = %id))]
    pub async fn list_databases(&self, id: Uuid) -> Result<Vec<String>> {
        use zqlz_core::DatabaseInfo;
        
        tracing::debug!("listing databases for connection");
        let conn = self
            .get(id)
            .ok_or_else(|| ZqlzError::NotFound("Connection not found".into()))?;

        let schema_introspection = conn
            .as_schema_introspection()
            .ok_or_else(|| ZqlzError::NotSupported("Schema introspection not supported".into()))?;

        let databases: Vec<DatabaseInfo> = schema_introspection.list_databases().await?;
        let names: Vec<String> = databases.into_iter().map(|db| db.name).collect();

        tracing::debug!(count = ?names.len(), "databases retrieved");
        Ok(names)
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}
