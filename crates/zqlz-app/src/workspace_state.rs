//! Centralized workspace state management
//!
//! This module provides a single source of truth for UI-related state that needs
//! to be shared across multiple panels. Following Zed's architecture pattern,
//! this separates UI state from application services (which remain in AppState).
//!
//! # Architecture
//!
//! ```text
//! AppState (Global)           WorkspaceState (Per-Window Entity)
//! ├── Services                ├── active_connection_id
//! │   ├── query_service       ├── active_database
//! │   ├── schema_service      ├── connected_ids
//! │   ├── table_service       ├── active_editor_id
//! │   └── connection_service  ├── editors
//! ├── connection_manager      ├── running_queries
//! └── storage                 ├── diagnostics
//!                             └── schema_cache
//! ```
//!
//! # Usage
//!
//! Panels subscribe to WorkspaceState events for automatic updates:
//!
//! ```rust,ignore
//! // In panel initialization
//! let subscription = cx.subscribe(&workspace_state, |panel, state, event, cx| {
//!     if let WorkspaceStateEvent::ActiveConnectionChanged(id) = event {
//!         panel.handle_connection_change(*id, cx);
//!     }
//! });
//! ```

use gpui::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;
use zqlz_core::QueryCancelHandle;

/// Unique identifier for a query editor tab
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EditorId(pub usize);

impl std::fmt::Display for EditorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Editor({})", self.0)
    }
}

/// State of a single query editor
#[derive(Clone, Debug)]
pub struct EditorState {
    /// Connection bound to this editor (can be None for unbound editors)
    pub connection_id: Option<Uuid>,
    /// Whether this editor has unsaved changes
    pub is_dirty: bool,
    /// File path if this is a saved query
    pub file_path: Option<String>,
    /// Display name for the tab
    pub display_name: String,
}

/// State of a running query
#[derive(Clone, Debug)]
pub struct QueryExecutionState {
    pub started_at: Instant,
    pub sql: String,
    pub connection_id: Uuid,
}

/// A diagnostic message (error/warning) for an editor
#[derive(Clone, Debug)]
pub struct EditorDiagnostic {
    pub line: usize,
    pub column: usize,
    pub end_line: usize,
    pub end_column: usize,
    pub message: String,
    pub severity: DiagnosticSeverity,
    pub source: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

/// Cached schema information for a connection
#[derive(Clone, Debug, Default)]
pub struct SchemaCache {
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub last_refreshed: Option<Instant>,
    pub is_loading: bool,
}

// =============================================================================
// Schema Object Actions
// =============================================================================

/// Type of database object for schema operations
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SchemaObjectType {
    Table,
    View,
    Function,
    Procedure,
    Trigger,
    Index,
}

impl SchemaObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaObjectType::Table => "table",
            SchemaObjectType::View => "view",
            SchemaObjectType::Function => "function",
            SchemaObjectType::Procedure => "procedure",
            SchemaObjectType::Trigger => "trigger",
            SchemaObjectType::Index => "index",
        }
    }
}

impl std::fmt::Display for SchemaObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Unified action for schema object operations
///
/// This consolidates the various ObjectsPanelEvent and SchemaTreeEvent variants
/// into a single action type that can be dispatched and handled uniformly.
///
/// # Usage
///
/// Instead of subscribing to multiple event types from different panels,
/// handlers can use this unified action:
///
/// ```rust,ignore
/// fn handle_schema_action(action: &SchemaObjectAction, window: &mut Window, cx: &mut Context<Self>) {
///     match action {
///         SchemaObjectAction::Open { object_type, connection_id, name } => {
///             // Open table/view/function/etc
///         }
///         SchemaObjectAction::Design { object_type, connection_id, name } => {
///             // Open in design mode
///         }
///         // ...
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub enum SchemaObjectAction {
    // ===== Open/View Actions =====
    /// Open an object to view its data/definition
    Open {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    /// Open an object in design/edit mode
    Design {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    // ===== Create Actions =====
    /// Create a new object
    Create {
        object_type: SchemaObjectType,
        connection_id: Uuid,
    },

    // ===== Modify Actions =====
    /// Rename an object
    Rename {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    /// Duplicate an object
    Duplicate {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    // ===== Delete Actions =====
    /// Delete an object
    Delete {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    /// Empty a table (DELETE FROM, but keep structure)
    EmptyTable {
        connection_id: Uuid,
        table_name: String,
    },

    // ===== Import/Export Actions =====
    /// Import data into a table
    ImportData {
        connection_id: Uuid,
        table_name: String,
    },

    /// Export data from a table
    ExportData {
        connection_id: Uuid,
        table_name: String,
    },

    /// Dump SQL for an object (CREATE statement and optionally INSERT statements)
    DumpSql {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
        include_data: bool,
    },

    // ===== Clipboard Actions =====
    /// Copy object name to clipboard
    CopyName {
        object_type: SchemaObjectType,
        name: String,
    },

    // ===== History Actions =====
    /// View version history for an object
    ViewHistory {
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: String,
    },

    // ===== Refresh Actions =====
    /// Refresh the schema/objects list
    RefreshSchema { connection_id: Option<Uuid> },
}

impl SchemaObjectAction {
    /// Create an Open action for a table
    pub fn open_table(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Open {
            object_type: SchemaObjectType::Table,
            connection_id,
            name: name.into(),
        }
    }

    /// Create an Open action for a view
    pub fn open_view(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Open {
            object_type: SchemaObjectType::View,
            connection_id,
            name: name.into(),
        }
    }

    /// Create an Open action for a function
    pub fn open_function(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Open {
            object_type: SchemaObjectType::Function,
            connection_id,
            name: name.into(),
        }
    }

    /// Create an Open action for a procedure
    pub fn open_procedure(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Open {
            object_type: SchemaObjectType::Procedure,
            connection_id,
            name: name.into(),
        }
    }

    /// Create an Open action for a trigger
    pub fn open_trigger(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Open {
            object_type: SchemaObjectType::Trigger,
            connection_id,
            name: name.into(),
        }
    }

    /// Create a Design action for a table
    pub fn design_table(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Design {
            object_type: SchemaObjectType::Table,
            connection_id,
            name: name.into(),
        }
    }

    /// Create a Design action for a view
    pub fn design_view(connection_id: Uuid, name: impl Into<String>) -> Self {
        Self::Design {
            object_type: SchemaObjectType::View,
            connection_id,
            name: name.into(),
        }
    }

    /// Create a Delete action
    pub fn delete(
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: impl Into<String>,
    ) -> Self {
        Self::Delete {
            object_type,
            connection_id,
            name: name.into(),
        }
    }

    /// Create a ViewHistory action
    pub fn view_history(
        object_type: SchemaObjectType,
        connection_id: Uuid,
        name: impl Into<String>,
    ) -> Self {
        Self::ViewHistory {
            object_type,
            connection_id,
            name: name.into(),
        }
    }

    /// Get the object type for this action (if applicable)
    pub fn object_type(&self) -> Option<SchemaObjectType> {
        match self {
            Self::Open { object_type, .. } => Some(*object_type),
            Self::Design { object_type, .. } => Some(*object_type),
            Self::Create { object_type, .. } => Some(*object_type),
            Self::Rename { object_type, .. } => Some(*object_type),
            Self::Duplicate { object_type, .. } => Some(*object_type),
            Self::Delete { object_type, .. } => Some(*object_type),
            Self::DumpSql { object_type, .. } => Some(*object_type),
            Self::CopyName { object_type, .. } => Some(*object_type),
            Self::ViewHistory { object_type, .. } => Some(*object_type),
            Self::EmptyTable { .. } => Some(SchemaObjectType::Table),
            Self::ImportData { .. } => Some(SchemaObjectType::Table),
            Self::ExportData { .. } => Some(SchemaObjectType::Table),
            Self::RefreshSchema { .. } => None,
        }
    }

    /// Get the connection ID for this action (if applicable)
    pub fn connection_id(&self) -> Option<Uuid> {
        match self {
            Self::Open { connection_id, .. } => Some(*connection_id),
            Self::Design { connection_id, .. } => Some(*connection_id),
            Self::Create { connection_id, .. } => Some(*connection_id),
            Self::Rename { connection_id, .. } => Some(*connection_id),
            Self::Duplicate { connection_id, .. } => Some(*connection_id),
            Self::Delete { connection_id, .. } => Some(*connection_id),
            Self::EmptyTable { connection_id, .. } => Some(*connection_id),
            Self::ImportData { connection_id, .. } => Some(*connection_id),
            Self::ExportData { connection_id, .. } => Some(*connection_id),
            Self::DumpSql { connection_id, .. } => Some(*connection_id),
            Self::ViewHistory { connection_id, .. } => Some(*connection_id),
            Self::CopyName { .. } => None,
            Self::RefreshSchema { connection_id } => *connection_id,
        }
    }

    /// Get the object name for this action (if applicable)
    pub fn object_name(&self) -> Option<&str> {
        match self {
            Self::Open { name, .. } => Some(name),
            Self::Design { name, .. } => Some(name),
            Self::Rename { name, .. } => Some(name),
            Self::Duplicate { name, .. } => Some(name),
            Self::Delete { name, .. } => Some(name),
            Self::EmptyTable { table_name, .. } => Some(table_name),
            Self::ImportData { table_name, .. } => Some(table_name),
            Self::ExportData { table_name, .. } => Some(table_name),
            Self::DumpSql { name, .. } => Some(name),
            Self::CopyName { name, .. } => Some(name),
            Self::ViewHistory { name, .. } => Some(name),
            Self::Create { .. } => None,
            Self::RefreshSchema { .. } => None,
        }
    }
}

/// Events emitted by WorkspaceState when state changes
///
/// Panels subscribe to these events for automatic UI updates.
#[derive(Clone, Debug)]
pub enum WorkspaceStateEvent {
    // ===== Connection Events =====
    /// The active connection changed (may be None if disconnected)
    ActiveConnectionChanged(Option<Uuid>),
    /// The active database changed (for multi-database connections)
    ActiveDatabaseChanged(Option<String>),
    /// A connection's status changed (connected/disconnected)
    ConnectionStatusChanged { id: Uuid, connected: bool },

    // ===== Editor Events =====
    /// The active (focused) editor changed
    ActiveEditorChanged(Option<EditorId>),
    /// A new editor was added
    EditorAdded(EditorId),
    /// An editor was removed/closed
    EditorRemoved(EditorId),
    /// An editor's state changed (dirty flag, connection, etc.)
    EditorStateChanged(EditorId),

    // ===== Query Execution Events =====
    /// A query started executing
    QueryStarted {
        editor_id: EditorId,
        connection_id: Uuid,
    },
    /// A query finished executing
    QueryCompleted { editor_id: EditorId, success: bool },
    /// A query was cancelled
    QueryCancelled(EditorId),

    // ===== Diagnostics Events =====
    /// Diagnostics changed for an editor
    DiagnosticsChanged(EditorId),

    // ===== Schema Events =====
    /// Schema was refreshed for a connection
    SchemaRefreshed(Uuid),
    /// Schema loading started for a connection
    SchemaLoadStarted(Uuid),
}

/// Central workspace state container
///
/// This is the single source of truth for UI state that needs to be shared
/// across multiple panels. It follows Zed's Workspace/Project pattern.
///
/// Key responsibilities:
/// - Track active connection (replaces scattered `connection_id` fields)
/// - Track active editor (for diagnostics, results panel)
/// - Track running queries (for cancel functionality, status bar)
/// - Cache schema information (for auto-refresh)
pub struct WorkspaceState {
    // ===== Connection State =====
    /// Currently active connection (single source of truth)
    active_connection_id: Option<Uuid>,
    /// Active database for multi-database connections
    active_database: Option<String>,
    /// Set of currently connected connection IDs
    connected_ids: Vec<Uuid>,
    /// Set of connection IDs currently in the process of connecting
    connecting_ids: Vec<Uuid>,

    // ===== Editor State =====
    /// All open editors and their state
    editors: HashMap<EditorId, EditorState>,
    /// Currently active/focused editor
    active_editor_id: Option<EditorId>,
    /// Counter for generating unique editor IDs
    next_editor_id: usize,

    // ===== Query Execution State =====
    /// Currently running queries (keyed by editor)
    running_queries: HashMap<EditorId, QueryExecutionState>,
    /// Cancel handles for running queries
    query_cancel_handles: HashMap<EditorId, Arc<dyn QueryCancelHandle>>,

    // ===== Diagnostics =====
    /// Diagnostics per editor
    diagnostics: HashMap<EditorId, Vec<EditorDiagnostic>>,

    // ===== Schema Cache =====
    /// Cached schema information per connection
    schema_cache: HashMap<Uuid, SchemaCache>,
}

impl EventEmitter<WorkspaceStateEvent> for WorkspaceState {}

impl WorkspaceState {
    /// Create a new workspace state
    pub fn new() -> Self {
        Self {
            active_connection_id: None,
            active_database: None,
            connected_ids: Vec::new(),
            connecting_ids: Vec::new(),
            editors: HashMap::new(),
            active_editor_id: None,
            next_editor_id: 1,
            running_queries: HashMap::new(),
            query_cancel_handles: HashMap::new(),
            diagnostics: HashMap::new(),
            schema_cache: HashMap::new(),
        }
    }

    // =========================================================================
    // Connection Methods
    // =========================================================================

    /// Set the active connection
    ///
    /// Emits `ActiveConnectionChanged` event. All subscribed panels will be notified.
    pub fn set_active_connection(&mut self, connection_id: Option<Uuid>, cx: &mut Context<Self>) {
        if self.active_connection_id != connection_id {
            self.active_connection_id = connection_id;

            // Clear database when connection changes
            if connection_id.is_none() {
                self.active_database = None;
            }

            tracing::debug!(
                "WorkspaceState: active connection changed to {:?}",
                connection_id
            );
            cx.emit(WorkspaceStateEvent::ActiveConnectionChanged(connection_id));
            cx.notify();
        }
    }

    /// Get the active connection ID
    pub fn active_connection_id(&self) -> Option<Uuid> {
        self.active_connection_id
    }

    /// Set the active database (for multi-database connections)
    pub fn set_active_database(&mut self, database: Option<String>, cx: &mut Context<Self>) {
        if self.active_database != database {
            self.active_database = database.clone();
            tracing::debug!("WorkspaceState: active database changed to {:?}", database);
            cx.emit(WorkspaceStateEvent::ActiveDatabaseChanged(database));
            cx.notify();
        }
    }

    /// Get the active database name
    pub fn active_database(&self) -> Option<&str> {
        self.active_database.as_deref()
    }

    /// Update connection status (connected/disconnected)
    pub fn set_connection_status(&mut self, id: Uuid, connected: bool, cx: &mut Context<Self>) {
        let was_connected = self.connected_ids.contains(&id);

        if connected && !was_connected {
            self.connected_ids.push(id);
            tracing::debug!("WorkspaceState: connection {} connected", id);
        } else if !connected && was_connected {
            self.connected_ids.retain(|&x| x != id);
            tracing::debug!("WorkspaceState: connection {} disconnected", id);

            // If this was the active connection, clear it
            if self.active_connection_id == Some(id) {
                self.set_active_connection(None, cx);
            }

            // Clear schema cache for disconnected connection
            self.schema_cache.remove(&id);
        }

        if was_connected != connected {
            cx.emit(WorkspaceStateEvent::ConnectionStatusChanged { id, connected });
            cx.notify();
        }
    }

    /// Check if a connection is currently connected
    pub fn is_connected(&self, id: Uuid) -> bool {
        self.connected_ids.contains(&id)
    }

    /// Get all connected connection IDs
    pub fn connected_ids(&self) -> &[Uuid] {
        &self.connected_ids
    }

    /// Set a connection as currently connecting
    pub fn set_connecting(&mut self, id: Uuid, connecting: bool) {
        let is_connecting = self.connecting_ids.contains(&id);

        if connecting && !is_connecting {
            self.connecting_ids.push(id);
            tracing::debug!("WorkspaceState: connection {} connecting", id);
        } else if !connecting && is_connecting {
            self.connecting_ids.retain(|&x| x != id);
            tracing::debug!("WorkspaceState: connection {} no longer connecting", id);
        }
    }

    /// Check if a connection is currently connecting
    pub fn is_connecting(&self, id: Uuid) -> bool {
        self.connecting_ids.contains(&id)
    }

    // =========================================================================
    // Editor Methods
    // =========================================================================

    /// Register a new editor and get its ID
    pub fn create_editor(
        &mut self,
        connection_id: Option<Uuid>,
        display_name: String,
        cx: &mut Context<Self>,
    ) -> EditorId {
        let id = EditorId(self.next_editor_id);
        self.next_editor_id += 1;

        self.editors.insert(
            id,
            EditorState {
                connection_id,
                is_dirty: false,
                file_path: None,
                display_name,
            },
        );

        tracing::debug!("WorkspaceState: created editor {:?}", id);
        cx.emit(WorkspaceStateEvent::EditorAdded(id));
        cx.notify();

        id
    }

    /// Remove an editor
    pub fn remove_editor(&mut self, id: EditorId, cx: &mut Context<Self>) {
        if self.editors.remove(&id).is_some() {
            // Clean up associated state
            self.running_queries.remove(&id);
            self.query_cancel_handles.remove(&id);
            self.diagnostics.remove(&id);

            // If this was the active editor, clear it
            if self.active_editor_id == Some(id) {
                self.active_editor_id = None;
            }

            tracing::debug!("WorkspaceState: removed editor {:?}", id);
            cx.emit(WorkspaceStateEvent::EditorRemoved(id));
            cx.notify();
        }
    }

    /// Set the active/focused editor
    pub fn set_active_editor(&mut self, editor_id: Option<EditorId>, cx: &mut Context<Self>) {
        if self.active_editor_id != editor_id {
            self.active_editor_id = editor_id;
            tracing::debug!("WorkspaceState: active editor changed to {:?}", editor_id);
            cx.emit(WorkspaceStateEvent::ActiveEditorChanged(editor_id));
            cx.notify();
        }
    }

    /// Get the active editor ID
    pub fn active_editor_id(&self) -> Option<EditorId> {
        self.active_editor_id
    }

    /// Get editor state by ID
    pub fn editor_state(&self, id: EditorId) -> Option<&EditorState> {
        self.editors.get(&id)
    }

    /// Get mutable editor state by ID
    pub fn editor_state_mut(&mut self, id: EditorId) -> Option<&mut EditorState> {
        self.editors.get_mut(&id)
    }

    /// Update editor state and emit event
    pub fn update_editor<F>(&mut self, id: EditorId, f: F, cx: &mut Context<Self>)
    where
        F: FnOnce(&mut EditorState),
    {
        if let Some(state) = self.editors.get_mut(&id) {
            f(state);
            cx.emit(WorkspaceStateEvent::EditorStateChanged(id));
            cx.notify();
        }
    }

    /// Get all editor IDs
    pub fn editor_ids(&self) -> impl Iterator<Item = EditorId> + '_ {
        self.editors.keys().copied()
    }

    // =========================================================================
    // Diagnostics Methods
    // =========================================================================

    /// Set diagnostics for an editor
    pub fn set_diagnostics(
        &mut self,
        editor_id: EditorId,
        diagnostics: Vec<EditorDiagnostic>,
        cx: &mut Context<Self>,
    ) {
        self.diagnostics.insert(editor_id, diagnostics);
        tracing::debug!(
            "WorkspaceState: diagnostics updated for editor {:?}",
            editor_id
        );
        cx.emit(WorkspaceStateEvent::DiagnosticsChanged(editor_id));
        cx.notify();
    }

    /// Clear diagnostics for an editor
    pub fn clear_diagnostics(&mut self, editor_id: EditorId, cx: &mut Context<Self>) {
        if self.diagnostics.remove(&editor_id).is_some() {
            cx.emit(WorkspaceStateEvent::DiagnosticsChanged(editor_id));
            cx.notify();
        }
    }

    /// Get diagnostics for a specific editor
    pub fn diagnostics_for_editor(&self, editor_id: EditorId) -> &[EditorDiagnostic] {
        self.diagnostics
            .get(&editor_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get diagnostics for the currently active editor
    pub fn active_diagnostics(&self) -> &[EditorDiagnostic] {
        self.active_editor_id
            .and_then(|id| self.diagnostics.get(&id))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Check if any editor has errors
    pub fn has_errors(&self) -> bool {
        self.diagnostics.values().any(|diags| {
            diags
                .iter()
                .any(|d| d.severity == DiagnosticSeverity::Error)
        })
    }

    // =========================================================================
    // Query Execution Methods
    // =========================================================================

    /// Start tracking a query execution
    pub fn start_query(
        &mut self,
        editor_id: EditorId,
        sql: String,
        connection_id: Uuid,
        cancel_handle: Arc<dyn QueryCancelHandle>,
        cx: &mut Context<Self>,
    ) {
        self.running_queries.insert(
            editor_id,
            QueryExecutionState {
                started_at: Instant::now(),
                sql,
                connection_id,
            },
        );
        self.query_cancel_handles.insert(editor_id, cancel_handle);

        tracing::debug!(
            "WorkspaceState: query started for editor {:?} on connection {}",
            editor_id,
            connection_id
        );
        cx.emit(WorkspaceStateEvent::QueryStarted {
            editor_id,
            connection_id,
        });
        cx.notify();
    }

    /// Mark a query as completed
    pub fn complete_query(&mut self, editor_id: EditorId, success: bool, cx: &mut Context<Self>) {
        self.running_queries.remove(&editor_id);
        self.query_cancel_handles.remove(&editor_id);

        tracing::debug!(
            "WorkspaceState: query completed for editor {:?}, success={}",
            editor_id,
            success
        );
        cx.emit(WorkspaceStateEvent::QueryCompleted { editor_id, success });
        cx.notify();
    }

    /// Cancel a running query
    pub fn cancel_query(&mut self, editor_id: EditorId, cx: &mut Context<Self>) {
        if let Some(handle) = self.query_cancel_handles.remove(&editor_id) {
            handle.cancel();
            self.running_queries.remove(&editor_id);

            tracing::debug!("WorkspaceState: query cancelled for editor {:?}", editor_id);
            cx.emit(WorkspaceStateEvent::QueryCancelled(editor_id));
            cx.notify();
        }
    }

    /// Check if a query is running for an editor
    pub fn is_query_running(&self, editor_id: EditorId) -> bool {
        self.running_queries.contains_key(&editor_id)
    }

    /// Check if any query is running
    pub fn any_query_running(&self) -> bool {
        !self.running_queries.is_empty()
    }

    /// Get query execution state
    pub fn query_execution_state(&self, editor_id: EditorId) -> Option<&QueryExecutionState> {
        self.running_queries.get(&editor_id)
    }

    /// Get the cancel handle for a running query
    pub fn query_cancel_handle(&self, editor_id: EditorId) -> Option<Arc<dyn QueryCancelHandle>> {
        self.query_cancel_handles.get(&editor_id).cloned()
    }

    // =========================================================================
    // Schema Cache Methods
    // =========================================================================

    /// Start loading schema for a connection
    pub fn start_schema_load(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        let cache = self.schema_cache.entry(connection_id).or_default();
        cache.is_loading = true;

        tracing::debug!(
            "WorkspaceState: schema load started for connection {}",
            connection_id
        );
        cx.emit(WorkspaceStateEvent::SchemaLoadStarted(connection_id));
        cx.notify();
    }

    /// Update schema cache for a connection
    pub fn refresh_schema(
        &mut self,
        connection_id: Uuid,
        tables: Vec<String>,
        views: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        self.schema_cache.insert(
            connection_id,
            SchemaCache {
                tables,
                views,
                last_refreshed: Some(Instant::now()),
                is_loading: false,
            },
        );

        tracing::debug!(
            "WorkspaceState: schema refreshed for connection {}",
            connection_id
        );
        cx.emit(WorkspaceStateEvent::SchemaRefreshed(connection_id));
        cx.notify();
    }

    /// Get schema cache for a connection
    pub fn schema_for_connection(&self, connection_id: Uuid) -> Option<&SchemaCache> {
        self.schema_cache.get(&connection_id)
    }

    /// Check if schema is loading for a connection
    pub fn is_schema_loading(&self, connection_id: Uuid) -> bool {
        self.schema_cache
            .get(&connection_id)
            .map(|c| c.is_loading)
            .unwrap_or(false)
    }

    /// Clear schema cache for a connection
    pub fn clear_schema_cache(&mut self, connection_id: Uuid) {
        self.schema_cache.remove(&connection_id);
    }
}

impl Default for WorkspaceState {
    fn default() -> Self {
        Self::new()
    }
}
