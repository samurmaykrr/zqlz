//! Application-wide state management
//!
//! This module contains the global application state that is shared
//! across the entire application.

use gpui::*;
use parking_lot::RwLock;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_connection::{ConnectionManager, SavedConnection};
use zqlz_query::{QueryHistory, QueryService};
use zqlz_services::{ConnectionService, SchemaService, TableService};
use zqlz_versioning::VersionRepository;

use crate::storage::LocalStorage;

/// Global application state
///
/// This struct holds application-wide state that needs to be shared
/// across multiple windows and components.
pub struct AppState {
    /// Unique identifier for this application instance
    pub instance_id: Uuid,

    /// User preferences and settings
    pub settings: Arc<RwLock<AppSettings>>,

    /// Connection manager for database connections
    pub connections: Arc<ConnectionManager>,

    /// Recent connections for quick access
    pub recent_connections: Arc<RwLock<Vec<RecentConnection>>>,

    /// Currently active connection ID (used across all query editors)
    pub active_connection_id: Arc<RwLock<Option<Uuid>>>,

    /// Currently active database name (for multi-database connections)
    pub active_database: Arc<RwLock<Option<String>>>,

    /// Local SQLite storage for app data
    pub storage: Arc<LocalStorage>,

    // ✅ NEW: Service layer instances
    /// Query execution service (wraps QueryEngine)
    pub query_service: Arc<QueryService>,

    /// Schema introspection service (with caching)
    pub schema_service: Arc<SchemaService>,

    /// Connection lifecycle service
    pub connection_service: Arc<ConnectionService>,

    /// Table operations service
    pub table_service: Arc<TableService>,

    /// Version control repository for database objects
    pub version_repository: Arc<VersionRepository>,

    /// Query execution history
    pub query_history: Arc<RwLock<QueryHistory>>,
}

impl AppState {
    /// Create a new application state
    pub fn new() -> Self {
        let storage = Arc::new(LocalStorage::new().expect("Failed to initialize local storage"));

        // Load connections from storage
        let connections = Arc::new(ConnectionManager::new());
        if let Ok(saved_connections) = storage.load_connections() {
            for conn in saved_connections {
                connections.add_saved(conn);
            }
        }

        // ✅ NEW: Initialize service layer
        let settings = Arc::new(RwLock::new(AppSettings::default()));
        let default_query_limit = settings.read().default_query_limit;

        // Initialize query history first so it can be shared with QueryService
        let query_history = Arc::new(RwLock::new(QueryHistory::default()));

        // Create QueryService with the shared query history instance
        let query_service = Arc::new(QueryService::with_shared_history(query_history.clone()));
        let schema_service = Arc::new(SchemaService::new());
        let table_service = Arc::new(TableService::new(default_query_limit));
        let connection_service = Arc::new(ConnectionService::new(
            connections.clone(),
            schema_service.clone(),
        ));

        // Initialize version control repository
        let version_repository =
            Arc::new(VersionRepository::new().expect("Failed to initialize version repository"));

        Self {
            instance_id: Uuid::new_v4(),
            settings,
            connections,
            recent_connections: Arc::new(RwLock::new(Vec::new())),
            active_connection_id: Arc::new(RwLock::new(None)),
            active_database: Arc::new(RwLock::new(None)),
            storage,
            query_service,
            schema_service,
            connection_service,
            table_service,
            version_repository,
            query_history,
        }
    }

    /// Set the active connection for all query editors
    pub fn set_active_connection(&self, connection_id: Option<Uuid>) {
        *self.active_connection_id.write() = connection_id;

        // Clear active database when connection changes
        if connection_id.is_none() {
            *self.active_database.write() = None;
        }
    }

    /// Get the active connection ID
    pub fn active_connection(&self) -> Option<Uuid> {
        *self.active_connection_id.read()
    }

    /// Set the active database
    pub fn set_active_database(&self, database: Option<String>) {
        *self.active_database.write() = database;
    }

    /// Get the active database name
    pub fn active_database(&self) -> Option<String> {
        self.active_database.read().clone()
    }

    /// Get the active connection name
    pub fn active_connection_name(&self) -> Option<String> {
        let conn_id = self.active_connection()?;
        self.saved_connections()
            .into_iter()
            .find(|c| c.id == conn_id)
            .map(|c| c.name)
    }

    /// Add a connection to recent list
    pub fn add_recent_connection(&self, connection: RecentConnection) {
        let mut recent = self.recent_connections.write();

        // Remove if already exists (we'll re-add at the front)
        recent.retain(|c| c.id != connection.id);

        // Add to front
        recent.insert(0, connection);

        // Keep only last 10
        recent.truncate(10);
    }

    /// Get the connection manager
    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connections
    }

    /// Save or update a connection
    pub fn save_connection(&self, saved: SavedConnection) {
        // Check if connection with this ID already exists
        let exists = self
            .connections
            .saved_connections()
            .iter()
            .any(|c| c.id == saved.id);

        if exists {
            // Update existing connection
            self.connections.update_saved(saved.clone());
        } else {
            // Add new connection
            self.connections.add_saved(saved.clone());
        }

        // Persist to local storage
        if let Err(e) = self.storage.save_connection(&saved) {
            tracing::error!("Failed to save connection to storage: {}", e);
        }
    }

    /// Delete a connection
    pub fn delete_connection(&self, id: Uuid) {
        self.connections.remove_saved(id);

        // Remove from storage
        if let Err(e) = self.storage.delete_connection(id) {
            tracing::error!("Failed to delete connection from storage: {}", e);
        }
    }

    /// Get all saved connections
    pub fn saved_connections(&self) -> Vec<SavedConnection> {
        self.connections.saved_connections()
    }

    /// Check if a connection is active
    pub fn is_connected(&self, id: Uuid) -> bool {
        self.connections.is_connected(id)
    }

    /// Get query history entries
    pub fn query_history_entries(&self) -> Vec<zqlz_query::QueryHistoryEntry> {
        self.query_history.read().entries().cloned().collect()
    }

    /// Add a query to history
    pub fn add_query_history(&self, entry: zqlz_query::QueryHistoryEntry) {
        self.query_history.write().add(entry);
    }

    /// Clear query history
    pub fn clear_query_history(&self) {
        self.query_history.write().clear();
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl Global for AppState {}

/// Application settings
#[derive(Clone, Debug)]
pub struct AppSettings {
    /// Theme name
    pub theme: String,

    /// Font family for editor
    pub editor_font_family: String,

    /// Font size for editor
    pub editor_font_size: f32,

    /// Enable auto-complete
    pub auto_complete: bool,

    /// Auto-format on save
    pub format_on_save: bool,

    /// Show line numbers in editor
    pub show_line_numbers: bool,

    /// Tab size for SQL editor
    pub tab_size: usize,

    /// Use spaces instead of tabs
    pub use_spaces: bool,

    /// Default limit for SELECT queries
    pub default_query_limit: usize,

    /// Confirm before executing destructive queries
    pub confirm_destructive: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            editor_font_family: "JetBrains Mono".to_string(),
            editor_font_size: 14.0,
            auto_complete: true,
            format_on_save: false,
            show_line_numbers: true,
            tab_size: 4,
            use_spaces: true,
            default_query_limit: 1000,
            confirm_destructive: true,
        }
    }
}

/// A recent connection entry
#[derive(Clone, Debug)]
pub struct RecentConnection {
    /// Unique identifier
    pub id: Uuid,

    /// Display name
    pub name: String,

    /// Database type (sqlite, postgres, mysql, etc.)
    pub db_type: String,

    /// Connection string or path (sanitized for display)
    pub display_path: String,

    /// Last connected timestamp
    pub last_connected: chrono::DateTime<chrono::Utc>,
}
