//! Connection manager for handling active connections

use gpui::BackgroundExecutor;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use zqlz_core::{Connection, ConnectionScope, Result, ZqlzError};
use zqlz_drivers::DriverRegistry;

use crate::SavedConnection;
use crate::health::{PingError, PingResult, ping_database_with_timeout};

/// How often active connections should be pinged to keep them alive and to
/// notice a server-side or network-side disconnect before the user runs a query.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// How long a single heartbeat ping may take before the connection is treated as
/// wedged. Connections are pinged in sequence, so this bounds how long one dead
/// socket can delay the rest.
pub const DEFAULT_HEARTBEAT_PING_TIMEOUT: Duration = Duration::from_secs(10);

/// Result of pinging a single connection during a heartbeat pass.
#[derive(Debug, Clone)]
pub enum HeartbeatOutcome {
    /// The connection answered the ping.
    Healthy {
        connection_id: Uuid,
        database: Option<String>,
        latency: Duration,
    },
    /// The connection was dead and a replacement was established.
    Reconnected {
        connection_id: Uuid,
        database: Option<String>,
    },
    /// A query was already occupying the connection, so it was left alone. An
    /// in-flight query is its own proof of liveness.
    Busy {
        connection_id: Uuid,
        database: Option<String>,
    },
    /// The connection was dead and could not be replaced.
    Lost {
        connection_id: Uuid,
        database: Option<String>,
        error: String,
    },
}

/// Whether a failed ping means the connection itself is gone, as opposed to the
/// server rejecting the ping statement.
///
/// A closed transport and a wedged one are both unrecoverable. A query error is
/// not: only rebuild for messages that clearly describe a broken transport, so a
/// transient server complaint never discards a working session.
fn indicates_connection_loss(error: &PingError) -> bool {
    match error {
        PingError::ConnectionClosed | PingError::Timeout => true,
        PingError::QueryFailed(message) => message_indicates_connection_loss(message),
    }
}

fn message_indicates_connection_loss(message: &str) -> bool {
    const TRANSPORT_FAILURES: [&str; 11] = [
        "connection closed",
        "connection reset",
        "connection refused",
        "connection aborted",
        "connection is closed",
        "broken pipe",
        "not connected",
        "server closed the connection",
        "terminating connection",
        "no connection to the server",
        "unexpected end of file",
    ];

    let normalized = message.to_ascii_lowercase();
    TRANSPORT_FAILURES
        .iter()
        .any(|failure| normalized.contains(failure))
}

/// Manages database connections
pub struct ConnectionManager {
    /// Driver registry
    drivers: DriverRegistry,

    /// Active connections
    active: RwLock<HashMap<Uuid, Arc<dyn Connection>>>,

    /// Per-database connections for drivers like PostgreSQL where each connection
    /// is scoped to a single database. Keyed by (connection_id, database_name).
    database_connections: RwLock<HashMap<(Uuid, String), Arc<dyn Connection>>>,

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
            database_connections: RwLock::new(HashMap::new()),
            saved: RwLock::new(Vec::new()),
            storage_path: None,
        }
    }

    /// Create a new connection manager with storage path
    pub fn with_storage_path(path: PathBuf) -> Self {
        Self {
            drivers: DriverRegistry::with_defaults(),
            active: RwLock::new(HashMap::new()),
            database_connections: RwLock::new(HashMap::new()),
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

        let config = saved.to_connection_config();

        // Debug: log which params are being set (without revealing password value)
        let has_password = saved.params.contains_key("password");
        let param_keys: Vec<_> = saved.params.keys().collect();
        tracing::debug!(
            has_password = has_password,
            param_keys = ?param_keys,
            "building connection config from saved params"
        );

        let conn = driver.connect(&config).await.map_err(|e| {
            tracing::error!(error = %e, "failed to connect");
            e
        })?;

        let conn_id = saved.id;
        self.active.write().insert(conn_id, conn);

        tracing::info!(connection_id = %conn_id, "connection established");
        Ok(conn_id)
    }

    /// Disconnect a connection and all its database-specific connections
    #[tracing::instrument(skip(self), fields(connection_id = %id))]
    pub async fn disconnect(&self, id: Uuid) -> Result<()> {
        tracing::info!("disconnecting connection");
        let conn = self.active.write().remove(&id);
        if let Some(conn) = conn {
            conn.close().await?;
        }

        // Close all database-specific connections for this connection_id
        let db_conns: Vec<((Uuid, String), Arc<dyn Connection>)> = {
            let mut guard = self.database_connections.write();
            let keys: Vec<(Uuid, String)> = guard
                .keys()
                .filter(|(conn_id, _)| *conn_id == id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| guard.remove(&key).map(|conn| (key, conn)))
                .collect()
        };

        for ((_, database_name), conn) in db_conns {
            if let Err(e) = conn.close().await {
                tracing::warn!(
                    database = %database_name,
                    error = %e,
                    "failed to close database-specific connection"
                );
            }
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

    /// Get a connection for a specific database, creating one if necessary.
    ///
    /// For drivers like PostgreSQL where each connection is scoped to a single
    /// database, this method returns a cached connection to the target database,
    /// or creates a new one from the saved connection config with the database
    /// parameter overridden.
    ///
    /// For drivers that can query across databases (MySQL, ClickHouse), this
    /// returns the main connection since no separate connection is needed.
    pub async fn get_for_database(
        &self,
        id: Uuid,
        database_name: &str,
    ) -> Result<Arc<dyn Connection>> {
        let main_conn = self
            .get(id)
            .ok_or_else(|| ZqlzError::NotFound("Connection not found".into()))?;

        let resolved_scope = main_conn
            .resolve_scope(ConnectionScope::Database(database_name.to_string()))
            .await?;

        if !resolved_scope.requires_dedicated_connection {
            return Ok(main_conn);
        }

        let normalized_database_name = resolved_scope
            .physical_database_key
            .ok_or_else(|| ZqlzError::Driver("Missing database scope key".to_string()))?;
        let key = (id, normalized_database_name.clone());

        // Check cache first
        if let Some(cached) = self.database_connections.read().get(&key)
            && !cached.is_closed()
        {
            return Ok(cached.clone());
        }
        // Connection is stale, will be replaced below

        // Create a new connection to the target database
        let saved = self
            .get_saved(id)
            .ok_or_else(|| ZqlzError::NotFound("Saved connection config not found".into()))?;

        let driver = self
            .drivers
            .get(&saved.driver)
            .ok_or_else(|| ZqlzError::Driver(format!("Unknown driver: {}", saved.driver)))?;

        let mut config = saved.to_connection_config();
        config = config.with_param("database", normalized_database_name.as_str());

        tracing::info!(
            connection_id = %id,
            database = %normalized_database_name,
            "creating database-specific connection"
        );

        let conn = driver.connect(&config).await?;
        self.database_connections.write().insert(key, conn.clone());

        Ok(conn)
    }

    /// Get a connection appropriate for the given database, using the main
    /// connection when no database-specific connection is needed.
    ///
    /// Unlike `get_for_database`, this method is synchronous and only returns
    /// already-cached database connections. Returns `None` if a database-specific
    /// connection is required but hasn't been created yet.
    pub fn get_for_database_cached(
        &self,
        id: Uuid,
        database_name: Option<&str>,
    ) -> Option<Arc<dyn Connection>> {
        let main_conn = self.get(id)?;

        let Some(database_name) = database_name else {
            return Some(main_conn);
        };

        if !main_conn.requires_database_scoped_connection() {
            return Some(main_conn);
        }

        let normalized_database_name = main_conn.normalize_database_scope_name(database_name);
        let key = (id, normalized_database_name.clone());
        let cached = self.database_connections.read().get(&key).cloned();
        match cached {
            Some(conn) if !conn.is_closed() => Some(conn),
            _ => {
                tracing::warn!(
                    connection_id = %id,
                    database = %normalized_database_name,
                    "database-specific connection not yet cached"
                );
                None
            }
        }
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

        let config = saved.to_connection_config();

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

    /// Ping every live connection once, replacing any that have died.
    ///
    /// This is what keeps sessions alive through idle periods: managed
    /// PostgreSQL services, connection poolers and NAT gateways all drop idle
    /// TCP sessions, and without traffic the first sign of that would be a
    /// failed user query. Call this on a timer (see
    /// [`DEFAULT_HEARTBEAT_INTERVAL`]) from a background task so it runs
    /// whether or not the window is focused.
    #[tracing::instrument(skip(self, executor))]
    pub async fn heartbeat(&self, executor: &BackgroundExecutor) -> Vec<HeartbeatOutcome> {
        let main_connections: Vec<(Uuid, Arc<dyn Connection>)> = self
            .active
            .read()
            .iter()
            .filter(|(_, conn)| conn.requires_heartbeat())
            .map(|(id, conn)| (*id, Arc::clone(conn)))
            .collect();

        let scoped_connections: Vec<((Uuid, String), Arc<dyn Connection>)> = self
            .database_connections
            .read()
            .iter()
            .filter(|(_, conn)| conn.requires_heartbeat())
            .map(|(key, conn)| (key.clone(), Arc::clone(conn)))
            .collect();

        let mut outcomes =
            Vec::with_capacity(main_connections.len() + scoped_connections.len());

        for (connection_id, conn) in main_connections {
            if conn.is_busy() {
                outcomes.push(HeartbeatOutcome::Busy {
                    connection_id,
                    database: None,
                });
                continue;
            }

            match self.ping(conn.as_ref(), executor).await {
                Ok(latency) => outcomes.push(HeartbeatOutcome::Healthy {
                    connection_id,
                    database: None,
                    latency,
                }),
                Err(error) => {
                    let message = error.to_string();
                    if !indicates_connection_loss(&error) {
                        tracing::warn!(
                            %connection_id,
                            error = %message,
                            "heartbeat ping failed but transport looks alive; keeping connection"
                        );
                        outcomes.push(HeartbeatOutcome::Lost {
                            connection_id,
                            database: None,
                            error: message,
                        });
                        continue;
                    }

                    tracing::warn!(
                        %connection_id,
                        error = %message,
                        "heartbeat detected a dead connection; reconnecting"
                    );
                    match self.reconnect(connection_id).await {
                        Ok(()) => outcomes.push(HeartbeatOutcome::Reconnected {
                            connection_id,
                            database: None,
                        }),
                        Err(reconnect_error) => {
                            tracing::error!(
                                %connection_id,
                                error = %reconnect_error,
                                "failed to reconnect after heartbeat failure"
                            );
                            outcomes.push(HeartbeatOutcome::Lost {
                                connection_id,
                                database: None,
                                error: reconnect_error.to_string(),
                            });
                        }
                    }
                }
            }
        }

        for ((connection_id, database), conn) in scoped_connections {
            if conn.is_busy() {
                outcomes.push(HeartbeatOutcome::Busy {
                    connection_id,
                    database: Some(database),
                });
                continue;
            }

            match self.ping(conn.as_ref(), executor).await {
                Ok(latency) => outcomes.push(HeartbeatOutcome::Healthy {
                    connection_id,
                    database: Some(database),
                    latency,
                }),
                Err(error) => {
                    let message = error.to_string();
                    if !indicates_connection_loss(&error) {
                        outcomes.push(HeartbeatOutcome::Lost {
                            connection_id,
                            database: Some(database),
                            error: message,
                        });
                        continue;
                    }

                    // Database-scoped connections are created on demand, so
                    // evicting the dead one is enough - the next lookup rebuilds it.
                    tracing::warn!(
                        %connection_id,
                        %database,
                        error = %message,
                        "evicting dead database-scoped connection"
                    );
                    self.database_connections
                        .write()
                        .remove(&(connection_id, database.clone()));
                    outcomes.push(HeartbeatOutcome::Lost {
                        connection_id,
                        database: Some(database),
                        error: message,
                    });
                }
            }
        }

        outcomes
    }

    async fn ping(
        &self,
        conn: &dyn Connection,
        executor: &BackgroundExecutor,
    ) -> PingResult {
        ping_database_with_timeout(conn, executor, DEFAULT_HEARTBEAT_PING_TIMEOUT).await
    }

    /// Replace an active connection with a freshly established one.
    ///
    /// The saved configuration is reused, and every database-scoped connection
    /// derived from this connection is dropped because they were bound to the
    /// old server session.
    #[tracing::instrument(skip(self), fields(connection_id = %id))]
    pub async fn reconnect(&self, id: Uuid) -> Result<()> {
        let saved = self
            .get_saved(id)
            .ok_or_else(|| ZqlzError::NotFound("Saved connection config not found".into()))?;

        let driver = self
            .drivers
            .get(&saved.driver)
            .ok_or_else(|| ZqlzError::Driver(format!("Unknown driver: {}", saved.driver)))?;

        let conn = driver.connect(&saved.to_connection_config()).await?;

        let previous = self.active.write().insert(id, conn);

        let stale_scoped: Vec<Arc<dyn Connection>> = {
            let mut guard = self.database_connections.write();
            let keys: Vec<(Uuid, String)> = guard
                .keys()
                .filter(|(connection_id, _)| *connection_id == id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| guard.remove(&key))
                .collect()
        };

        // Deliberately not calling `close()` on the connections being replaced.
        // Another task may still be awaiting a query on one of them, and for
        // pool-backed drivers `close()` disconnects the pool, which would abort
        // that query. Dropping our handle lets each connection shut down once
        // its last user is finished.
        drop(previous);
        drop(stale_scoped);

        tracing::info!("connection re-established");
        Ok(())
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use zqlz_core::{
        DropTableOptions, DropTriggerOptions, DropViewOptions, QueryResult, SqlObjectName,
        StatementResult, Transaction, Value,
    };

    struct MockConnection {
        driver_name: &'static str,
        requires_scoped_connection: bool,
        closed: AtomicBool,
        requires_heartbeat: bool,
        busy: bool,
        pinged: AtomicBool,
    }

    impl MockConnection {
        fn new(driver_name: &'static str, requires_scoped_connection: bool) -> Self {
            Self {
                driver_name,
                requires_scoped_connection,
                closed: AtomicBool::new(false),
                requires_heartbeat: true,
                busy: false,
                pinged: AtomicBool::new(false),
            }
        }

        fn busy(driver_name: &'static str) -> Self {
            Self {
                busy: true,
                ..Self::new(driver_name, false)
            }
        }

        fn embedded(driver_name: &'static str) -> Self {
            Self {
                requires_heartbeat: false,
                ..Self::new(driver_name, false)
            }
        }

        fn dead(driver_name: &'static str, requires_scoped_connection: bool) -> Self {
            let connection = Self::new(driver_name, requires_scoped_connection);
            connection.closed.store(true, Ordering::SeqCst);
            connection
        }
    }

    #[async_trait]
    impl Connection for MockConnection {
        fn driver_name(&self) -> &str {
            self.driver_name
        }

        fn requires_database_scoped_connection(&self) -> bool {
            self.requires_scoped_connection
        }

        fn normalize_database_scope_name(&self, database_name: &str) -> String {
            database_name
                .strip_prefix("db")
                .unwrap_or(database_name)
                .to_string()
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<StatementResult> {
            Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 0,
                error: None,
            })
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
            self.pinged.store(true, Ordering::SeqCst);
            Ok(QueryResult::empty())
        }

        fn rename_table_sql(
            &self,
            table_name: &SqlObjectName,
            new_table_name: &str,
        ) -> Result<String> {
            Ok(format!(
                "ALTER TABLE {} RENAME TO {}",
                self.render_qualified_name(table_name),
                self.quote_identifier(new_table_name)
            ))
        }

        fn drop_table_sql(
            &self,
            table_name: &SqlObjectName,
            _options: DropTableOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn drop_view_sql(
            &self,
            view_name: &SqlObjectName,
            _options: DropViewOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP VIEW {}",
                self.render_qualified_name(view_name)
            ))
        }

        fn drop_trigger_sql(
            &self,
            trigger_name: &SqlObjectName,
            _table_name: Option<&SqlObjectName>,
            _options: DropTriggerOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TRIGGER {}",
                self.render_qualified_name(trigger_name)
            ))
        }

        fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "TRUNCATE TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn duplicate_table_sql(
            &self,
            source_table_name: &SqlObjectName,
            new_table_name: &SqlObjectName,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} AS SELECT * FROM {}",
                self.render_qualified_name(new_table_name),
                self.render_qualified_name(source_table_name)
            ))
        }

        fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "DELETE FROM {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
            Ok(format!(
                "SELECT 1 FROM {} LIMIT 1",
                self.render_qualified_name(table_name)
            ))
        }

        fn select_rows_sql(
            &self,
            table_name: &SqlObjectName,
            _projected_columns: &[String],
            _where_clause_sql: Option<&str>,
        ) -> Result<String> {
            Ok(format!(
                "SELECT * FROM {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn select_distinct_rows_sql(
            &self,
            table_name: &SqlObjectName,
            projected_columns: &[String],
            _where_clause_sql: Option<&str>,
            _order_by_columns: &[String],
            limit: u64,
        ) -> Result<String> {
            let columns = projected_columns.join(", ");
            Ok(format!(
                "SELECT DISTINCT {} FROM {} LIMIT {}",
                columns,
                self.render_qualified_name(table_name),
                limit
            ))
        }

        fn insert_row_sql(
            &self,
            table_name: &SqlObjectName,
            column_names: &[String],
            value_count: usize,
        ) -> Result<String> {
            let placeholders = vec!["?"; value_count].join(", ");
            Ok(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.render_qualified_name(table_name),
                column_names.join(", "),
                placeholders
            ))
        }

        fn performance_metrics_query_sql(&self) -> Result<String> {
            Ok("SELECT 0 AS total_queries".to_string())
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported(
                "Transactions not supported in mock".into(),
            ))
        }

        async fn close(&self) -> Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        fn requires_heartbeat(&self) -> bool {
            self.requires_heartbeat
        }

        fn is_busy(&self) -> bool {
            self.busy
        }
    }

    #[test]
    fn cached_scoped_lookup_does_not_fallback_to_main_connection() {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        manager.active.write().insert(
            connection_id,
            Arc::new(MockConnection::new("postgresql", true)),
        );

        assert!(
            manager
                .get_for_database_cached(connection_id, Some("erp_lab"))
                .is_none()
        );
    }

    #[test]
    fn cached_scoped_lookup_reuses_main_for_unscoped_driver() {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        manager
            .active
            .write()
            .insert(connection_id, Arc::new(MockConnection::new("mysql", false)));

        assert!(
            manager
                .get_for_database_cached(connection_id, Some("analytics"))
                .is_some()
        );
    }

    #[test]
    fn transport_failures_are_distinguished_from_server_errors() {
        assert!(indicates_connection_loss(&PingError::QueryFailed(
            "Query error: Failed to prepare query: connection closed".into()
        )));
        assert!(indicates_connection_loss(&PingError::QueryFailed(
            "Broken pipe (os error 32)".into()
        )));
        assert!(indicates_connection_loss(&PingError::ConnectionClosed));
        assert!(indicates_connection_loss(&PingError::Timeout));
        assert!(!indicates_connection_loss(&PingError::QueryFailed(
            "permission denied for table users".into()
        )));
        assert!(!indicates_connection_loss(&PingError::QueryFailed(
            "syntax error at or near \"SELCT\"".into()
        )));
    }

    #[gpui::test]
    async fn heartbeat_reports_live_connections_as_healthy(cx: &mut gpui::TestAppContext) {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        manager
            .active
            .write()
            .insert(connection_id, Arc::new(MockConnection::new("mysql", false)));

        let outcomes = manager.heartbeat(&cx.background_executor).await;

        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            outcomes[0],
            HeartbeatOutcome::Healthy {
                connection_id: id,
                database: None,
                ..
            } if id == connection_id
        ));
    }

    #[gpui::test]
    async fn heartbeat_skips_drivers_with_no_transport_to_keep_warm(
        cx: &mut gpui::TestAppContext,
    ) {
        let manager = ConnectionManager::new();
        manager
            .active
            .write()
            .insert(Uuid::new_v4(), Arc::new(MockConnection::embedded("sqlite")));

        assert!(manager.heartbeat(&cx.background_executor).await.is_empty());
    }

    #[gpui::test]
    async fn heartbeat_leaves_busy_connections_unpinged(cx: &mut gpui::TestAppContext) {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        let connection = Arc::new(MockConnection::busy("postgresql"));
        manager
            .active
            .write()
            .insert(connection_id, connection.clone());

        let outcomes = manager.heartbeat(&cx.background_executor).await;

        assert!(matches!(
            outcomes.as_slice(),
            [HeartbeatOutcome::Busy { database: None, .. }]
        ));
        assert!(
            !connection.pinged.load(Ordering::SeqCst),
            "a busy connection must not be pinged"
        );
    }

    #[gpui::test]
    async fn heartbeat_evicts_dead_database_scoped_connections(cx: &mut gpui::TestAppContext) {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        manager.database_connections.write().insert(
            (connection_id, "erp_lab".to_string()),
            Arc::new(MockConnection::dead("postgresql", true)),
        );

        let outcomes = manager.heartbeat(&cx.background_executor).await;

        assert!(matches!(
            outcomes.as_slice(),
            [HeartbeatOutcome::Lost {
                database: Some(_),
                ..
            }]
        ));
        assert!(manager.database_connections.read().is_empty());
    }

    #[gpui::test]
    async fn heartbeat_reports_loss_when_dead_connection_cannot_be_rebuilt(
        cx: &mut gpui::TestAppContext,
    ) {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        manager
            .active
            .write()
            .insert(connection_id, Arc::new(MockConnection::dead("mysql", false)));

        let outcomes = manager.heartbeat(&cx.background_executor).await;

        assert!(matches!(
            outcomes.as_slice(),
            [HeartbeatOutcome::Lost { database: None, .. }]
        ));
    }

    #[test]
    fn cached_scoped_lookup_normalizes_redis_database_labels() {
        let manager = ConnectionManager::new();
        let connection_id = Uuid::new_v4();
        let scoped_connection: Arc<dyn Connection> = Arc::new(MockConnection::new("redis", true));
        manager
            .active
            .write()
            .insert(connection_id, Arc::new(MockConnection::new("redis", true)));
        manager
            .database_connections
            .write()
            .insert((connection_id, "3".to_string()), scoped_connection);

        assert!(
            manager
                .get_for_database_cached(connection_id, Some("db3"))
                .is_some()
        );
    }
}
