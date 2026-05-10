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
#![allow(dead_code)]
//! ├── Services                ├── active_connection_id
//! │   ├── query_service       ├── active_database
//! │   ├── schema_service      ├── connected_ids
//! │   ├── table_service       ├── active_editor_id
//! │   └── connection_service  ├── editors
//! ├── connection_manager      ├── running_queries
//! └── storage                 └── diagnostics
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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;
use zqlz_core::{ConnectionFeatureSet, QueryCancelHandle};
use zqlz_text_editor::{DocumentContext, DocumentIdentity};

/// Unique identifier for a query editor tab
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EditorId(pub usize);

impl std::fmt::Display for EditorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Editor({})", self.0)
    }
}

/// State of a single query editor
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct EditorState {
    /// Connection bound to this editor (can be None for unbound editors)
    pub connection_id: Option<Uuid>,
    /// Whether this editor has unsaved changes
    pub is_dirty: bool,
    /// File path if this is a saved query
    pub file_path: Option<String>,
    /// Display name for the tab
    pub display_name: String,
    /// Canonical document identity for this editor.
    pub document_identity: Option<DocumentIdentity>,
    /// Cached document context used for workspace-scoped metadata.
    pub document_context: Option<DocumentContext>,
    /// Last known editor buffer text for session restore.
    pub draft_text: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkspaceSession {
    pub active_connection_id: Option<Uuid>,
    pub active_database: Option<String>,
    pub open_query_tabs: Vec<WorkspaceSessionQueryTab>,
    pub open_viewer_tabs: Vec<WorkspaceSessionViewerTab>,
    pub active_editor_id: Option<EditorId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSessionQueryTab {
    pub id: EditorId,
    pub display_name: String,
    pub connection_id: Option<Uuid>,
    pub document_path: Option<String>,
    #[serde(default)]
    pub draft_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSessionViewerTab {
    pub connection_id: Uuid,
    #[serde(flatten)]
    pub kind: WorkspaceSessionViewerKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceSessionViewerKind {
    Table {
        table_name: String,
        database_name: Option<String>,
        is_view: bool,
        #[serde(default)]
        viewer_state: Option<WorkspaceSessionTableViewerState>,
    },
    Collection {
        database_name: String,
        collection_name: String,
        #[serde(default)]
        viewer_state: Option<WorkspaceSessionTableViewerState>,
    },
    RedisDatabase {
        database_index: u16,
    },
    RedisKey {
        database_index: u16,
        key_name: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSessionTableViewerState {
    #[serde(default)]
    pub filters: Vec<WorkspaceSessionFilterCondition>,
    #[serde(default)]
    pub sorts: Vec<WorkspaceSessionSortCriterion>,
    #[serde(default)]
    pub visible_columns: Vec<String>,
    #[serde(default)]
    pub search_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSessionFilterCondition {
    pub id: usize,
    pub enabled: bool,
    pub column: Option<String>,
    pub operator: String,
    pub value: String,
    pub value2: Option<String>,
    pub custom_sql: Option<String>,
    pub logical_operator: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSessionSortCriterion {
    pub id: usize,
    pub column: String,
    pub direction: String,
}

/// State of a running query
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct QueryExecutionState {
    pub execution_id: u64,
    pub started_at: Instant,
    pub sql: String,
    pub connection_id: Uuid,
    pub status: QueryExecutionStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryExecutionStatus {
    Queued,
    Running,
    Cancelled,
    Succeeded,
    Failed,
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

// =============================================================================
// Schema Object Actions
// =============================================================================

/// Type of database object for schema operations
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum SchemaObjectType {
    Table,
    View,
    Function,
    Procedure,
    Trigger,
    Index,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshScope {
    /// Refresh the connection sidebar list, then fan out to connected surfaces.
    ConnectionsList,
    /// Refresh schema/object surfaces for the current active connection.
    ActiveConnectionSurfaces,
    /// Refresh schema/object surfaces for a specific connection.
    ConnectionSurfaces(Uuid),
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum WorkspaceStateEvent {
    // ===== Connection Events =====
    /// The active connection changed (may be None if disconnected)
    ActiveConnectionChanged(Option<Uuid>),
    /// The active database changed (for multi-database connections)
    ActiveDatabaseChanged(Option<String>),
    /// Canonical refresh intent routed through MainView.
    RefreshRequested(RefreshScope),
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
    /// Persisted viewer tab descriptors changed.
    ViewerTabsChanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryCancellationOutcome {
    /// A tracked execution existed and was cancelled.
    CancelledActiveExecution,
    /// No execution was tracked, but a stale cancel handle was still present.
    CancelledStaleHandle,
    /// No execution and no cancel handle were tracked.
    NoTrackedQuery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryCompletionOutcome {
    /// The completion owned the active execution and was applied.
    CompletedActiveExecution,
    /// No execution is tracked for the editor (for example, after cancellation).
    SkippedNoTrackedQuery,
    /// A different execution is active for this editor, so this completion is stale.
    SkippedStaleExecution,
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
    /// Feature sets keyed by connection and optional database/scope.
    connection_feature_sets: HashMap<(Uuid, Option<String>), ConnectionFeatureSet>,

    // ===== Editor State =====
    /// All open editors and their state
    editors: HashMap<EditorId, EditorState>,
    /// Currently active/focused editor
    active_editor_id: Option<EditorId>,
    /// Counter for generating unique editor IDs
    next_editor_id: usize,

    /// Monotonic identifier for query executions.
    ///
    /// This allows completion callbacks to ignore stale results from a previous
    /// execution on the same editor.
    next_query_execution_id: u64,

    // ===== Query Execution State =====
    /// Currently running queries (keyed by editor)
    running_queries: HashMap<EditorId, QueryExecutionState>,
    /// Cancel handles for running queries
    query_cancel_handles: HashMap<EditorId, Arc<dyn QueryCancelHandle>>,
    /// Latest known query status for each editor, including terminal states.
    query_statuses: HashMap<EditorId, QueryExecutionStatus>,
    /// Lightweight descriptors for non-query center tabs that should reopen on restart.
    open_viewer_tabs: Vec<WorkspaceSessionViewerTab>,

    // ===== Diagnostics =====
    /// Diagnostics per editor
    diagnostics: HashMap<EditorId, Vec<EditorDiagnostic>>,
}

impl EventEmitter<WorkspaceStateEvent> for WorkspaceState {}

fn apply_document_metadata(
    state: &mut EditorState,
    document_context: &DocumentContext,
    display_name: String,
    is_dirty: bool,
) {
    state.is_dirty = is_dirty;
    state.document_identity = Some(document_context.identity.clone());
    state.file_path = document_context
        .identity
        .path()
        .map(|path| path.to_string_lossy().into_owned());
    state.display_name = display_name;
    state.document_context = Some(document_context.clone());
}

impl WorkspaceState {
    #[allow(dead_code)]
    /// Create a new workspace state
    pub fn new() -> Self {
        Self {
            active_connection_id: None,
            active_database: None,
            connected_ids: Vec::new(),
            connecting_ids: Vec::new(),
            connection_feature_sets: HashMap::new(),
            editors: HashMap::new(),
            active_editor_id: None,
            next_editor_id: 1,
            next_query_execution_id: 1,
            running_queries: HashMap::new(),
            query_cancel_handles: HashMap::new(),
            query_statuses: HashMap::new(),
            open_viewer_tabs: Vec::new(),
            diagnostics: HashMap::new(),
        }
    }

    pub fn persisted_session(&self) -> WorkspaceSession {
        let mut open_query_tabs = self
            .editors
            .iter()
            .map(|(id, state)| WorkspaceSessionQueryTab {
                id: *id,
                display_name: state.display_name.clone(),
                connection_id: state.connection_id,
                document_path: state.file_path.clone(),
                draft_text: state.draft_text.clone(),
            })
            .collect::<Vec<_>>();

        open_query_tabs.sort_by_key(|tab| tab.id.0);

        WorkspaceSession {
            active_connection_id: self.active_connection_id,
            active_database: self.active_database.clone(),
            open_query_tabs,
            open_viewer_tabs: self.open_viewer_tabs.clone(),
            active_editor_id: self.active_editor_id,
        }
    }

    pub fn from_persisted_session(session: WorkspaceSession) -> Self {
        let next_editor_id = session
            .open_query_tabs
            .iter()
            .map(|tab| tab.id.0)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);

        let editors = session
            .open_query_tabs
            .into_iter()
            .map(|tab| {
                (
                    tab.id,
                    EditorState {
                        connection_id: tab.connection_id,
                        is_dirty: false,
                        file_path: tab.document_path,
                        display_name: tab.display_name,
                        document_identity: None,
                        document_context: None,
                        draft_text: tab.draft_text,
                    },
                )
            })
            .collect();

        Self {
            active_connection_id: session.active_connection_id,
            active_database: session.active_database,
            connected_ids: Vec::new(),
            connecting_ids: Vec::new(),
            connection_feature_sets: HashMap::new(),
            editors,
            active_editor_id: session.active_editor_id,
            next_editor_id,
            next_query_execution_id: 1,
            running_queries: HashMap::new(),
            query_cancel_handles: HashMap::new(),
            query_statuses: HashMap::new(),
            open_viewer_tabs: session.open_viewer_tabs,
            diagnostics: HashMap::new(),
        }
    }

    pub fn record_open_viewer_tab(
        &mut self,
        viewer_tab: WorkspaceSessionViewerTab,
        cx: &mut Context<Self>,
    ) {
        record_open_viewer_tab_in_order(&mut self.open_viewer_tabs, viewer_tab);
        cx.emit(WorkspaceStateEvent::ViewerTabsChanged);
        cx.notify();
    }

    pub fn update_table_viewer_state(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        database_name: Option<&str>,
        viewer_state: WorkspaceSessionTableViewerState,
        cx: &mut Context<Self>,
    ) {
        if let Some(tab) = self.open_viewer_tabs.iter_mut().find(|tab| {
            tab.connection_id == connection_id
                && matches!(
                    &tab.kind,
                    WorkspaceSessionViewerKind::Table {
                        table_name: existing_table_name,
                        database_name: existing_database_name,
                        ..
                    } if existing_table_name == table_name
                        && existing_database_name.as_deref() == database_name
                )
        }) && let WorkspaceSessionViewerKind::Table {
            viewer_state: state,
            ..
        } = &mut tab.kind
        {
            *state = Some(viewer_state);
            cx.emit(WorkspaceStateEvent::ViewerTabsChanged);
            cx.notify();
        }
    }

    fn allocate_query_execution_id(&mut self) -> u64 {
        let execution_id = self.next_query_execution_id;
        self.next_query_execution_id = if execution_id == u64::MAX {
            1
        } else {
            execution_id + 1
        };

        if execution_id == u64::MAX {
            tracing::warn!(
                "WorkspaceState: query execution id wrapped to 1 after reaching u64::MAX"
            );
        }

        execution_id
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
    #[allow(dead_code)]
    pub fn set_active_database(&mut self, database: Option<String>, cx: &mut Context<Self>) {
        if self.active_database != database {
            self.active_database = database.clone();
            tracing::debug!("WorkspaceState: active database changed to {:?}", database);
            cx.emit(WorkspaceStateEvent::ActiveDatabaseChanged(database));
            cx.notify();
        }
    }

    /// Get the active database name
    #[allow(dead_code)]
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

            self.connection_feature_sets
                .retain(|(connection_id, _), _| *connection_id != id);
        }

        if was_connected != connected {
            cx.emit(WorkspaceStateEvent::ConnectionStatusChanged { id, connected });
            cx.notify();
        }
    }

    /// Emit a canonical refresh intent for MainView to handle.
    pub fn request_refresh(&mut self, scope: RefreshScope, cx: &mut Context<Self>) {
        cx.emit(WorkspaceStateEvent::RefreshRequested(scope));
        cx.notify();
    }

    /// Check if a connection is currently connected
    #[allow(dead_code)]
    pub fn is_connected(&self, id: Uuid) -> bool {
        self.connected_ids.contains(&id)
    }

    /// Get all connected connection IDs
    #[allow(dead_code)]
    pub fn connected_ids(&self) -> &[Uuid] {
        &self.connected_ids
    }

    pub fn set_connection_feature_set(
        &mut self,
        connection_id: Uuid,
        database: Option<String>,
        feature_set: ConnectionFeatureSet,
        cx: &mut Context<Self>,
    ) {
        self.connection_feature_sets
            .insert((connection_id, database), feature_set);
        cx.notify();
    }

    pub fn connection_feature_set(
        &self,
        connection_id: Uuid,
        database: Option<&str>,
    ) -> Option<&ConnectionFeatureSet> {
        self.connection_feature_sets
            .get(&(connection_id, database.map(ToOwned::to_owned)))
            .or_else(|| self.connection_feature_sets.get(&(connection_id, None)))
    }

    /// Set a connection as currently connecting
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
                document_identity: None,
                document_context: None,
                draft_text: None,
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
            self.query_statuses.remove(&id);
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

    pub fn update_editor_document(
        &mut self,
        id: EditorId,
        document_context: DocumentContext,
        is_dirty: bool,
        display_name: String,
        draft_text: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.update_editor(
            id,
            move |state| {
                apply_document_metadata(state, &document_context, display_name, is_dirty);
                state.draft_text = draft_text;
            },
            cx,
        );
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
    ) -> u64 {
        let previous_execution = self.running_queries.get(&editor_id).cloned();
        let previous_cancel_handle = self.query_cancel_handles.get(&editor_id).cloned();

        if let Some(previous_state) = previous_execution.as_ref() {
            tracing::warn!(
                "WorkspaceState: start_query superseded active execution {} for editor {:?}; cancelling previous query handle before tracking new execution",
                previous_state.execution_id,
                editor_id
            );
        }

        if let Some(handle) = previous_cancel_handle.as_ref() {
            handle.cancel();
        }

        let execution_id = self.allocate_query_execution_id();
        self.running_queries.insert(
            editor_id,
            QueryExecutionState {
                execution_id,
                started_at: Instant::now(),
                sql,
                connection_id,
                status: QueryExecutionStatus::Running,
            },
        );
        self.query_cancel_handles.insert(editor_id, cancel_handle);
        self.query_statuses
            .insert(editor_id, QueryExecutionStatus::Running);

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

        execution_id
    }

    /// Mark a query as completed.
    ///
    /// Returns a typed completion outcome so callers can keep stale/no-tracked
    /// completion handling explicit at UI boundaries.
    pub fn complete_query(
        &mut self,
        editor_id: EditorId,
        execution_id: u64,
        success: bool,
        cx: &mut Context<Self>,
    ) -> QueryCompletionOutcome {
        let Some(running_query_state) = self.running_queries.get(&editor_id) else {
            tracing::debug!(
                "WorkspaceState: ignoring completion for editor {:?} because no running query is tracked",
                editor_id
            );
            return QueryCompletionOutcome::SkippedNoTrackedQuery;
        };

        if running_query_state.execution_id != execution_id {
            tracing::debug!(
                "WorkspaceState: ignoring stale completion for editor {:?} because execution id {} does not match active execution {}",
                editor_id,
                execution_id,
                running_query_state.execution_id
            );
            return QueryCompletionOutcome::SkippedStaleExecution;
        }

        self.running_queries.remove(&editor_id);
        self.query_cancel_handles.remove(&editor_id);
        self.query_statuses.insert(
            editor_id,
            if success {
                QueryExecutionStatus::Succeeded
            } else {
                QueryExecutionStatus::Failed
            },
        );

        tracing::debug!(
            "WorkspaceState: query completed for editor {:?}, success={}",
            editor_id,
            success
        );
        cx.emit(WorkspaceStateEvent::QueryCompleted { editor_id, success });
        cx.notify();
        QueryCompletionOutcome::CompletedActiveExecution
    }

    /// Cancel a running query.
    ///
    /// Returns a typed outcome so callers can decide whether to apply UI side effects
    /// (for example, cancellation notifications) only when an active execution was
    /// actually cancelled.
    pub fn cancel_query(
        &mut self,
        editor_id: EditorId,
        cx: &mut Context<Self>,
    ) -> QueryCancellationOutcome {
        let cancel_handle = self.query_cancel_handles.remove(&editor_id);
        let had_running_query = self.running_queries.remove(&editor_id).is_some();

        if let Some(handle) = cancel_handle.as_ref() {
            handle.cancel();
        }

        if had_running_query {
            if cancel_handle.is_none() {
                tracing::warn!(
                    "WorkspaceState: cancelled query for editor {:?} without a cancel handle",
                    editor_id
                );
            }

            tracing::debug!("WorkspaceState: query cancelled for editor {:?}", editor_id);
            self.query_statuses
                .insert(editor_id, QueryExecutionStatus::Cancelled);
            cx.emit(WorkspaceStateEvent::QueryCancelled(editor_id));
            cx.notify();
            QueryCancellationOutcome::CancelledActiveExecution
        } else if cancel_handle.is_some() {
            tracing::warn!(
                "WorkspaceState: cancelled stale handle for editor {:?} with no running query tracked",
                editor_id
            );
            QueryCancellationOutcome::CancelledStaleHandle
        } else {
            QueryCancellationOutcome::NoTrackedQuery
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

    /// Get the latest query status for an editor.
    pub fn query_execution_status(&self, editor_id: EditorId) -> Option<QueryExecutionStatus> {
        self.query_statuses.get(&editor_id).copied()
    }

    /// Get the cancel handle for a running query
    pub fn query_cancel_handle(&self, editor_id: EditorId) -> Option<Arc<dyn QueryCancelHandle>> {
        self.query_cancel_handles.get(&editor_id).cloned()
    }
}

fn record_open_viewer_tab_in_order(
    open_viewer_tabs: &mut Vec<WorkspaceSessionViewerTab>,
    viewer_tab: WorkspaceSessionViewerTab,
) {
    if let Some(existing_index) = open_viewer_tabs
        .iter()
        .position(|existing| existing == &viewer_tab)
    {
        open_viewer_tabs.remove(existing_index);
    }
    open_viewer_tabs.push(viewer_tab);
}

impl Default for WorkspaceState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiagnosticSeverity, EditorDiagnostic, EditorId, EditorState, QueryExecutionState,
        QueryExecutionStatus, RefreshScope, WorkspaceSession, WorkspaceSessionQueryTab,
        WorkspaceSessionTableViewerState, WorkspaceSessionViewerKind, WorkspaceSessionViewerTab,
        WorkspaceState, WorkspaceStateEvent, apply_document_metadata,
        record_open_viewer_tab_in_order,
    };
    use std::time::Instant;
    use uuid::Uuid;
    use zqlz_text_editor::{
        DocumentContext, DocumentIdentity, DocumentSettings, LineEnding,
        document::{DocumentCapability, DocumentFileState},
    };

    fn test_document_context(identity: DocumentIdentity, saved_revision: usize) -> DocumentContext {
        DocumentContext {
            id: Uuid::new_v4(),
            identity,
            settings: DocumentSettings::default(),
            saved_revision,
            line_ending: LineEnding::Lf,
            file_state: DocumentFileState::default(),
            is_dirty: false,
            capability: DocumentCapability::ReadWrite,
            lifecycle_events: Vec::new(),
        }
    }

    fn test_editor_state() -> EditorState {
        EditorState {
            connection_id: None,
            is_dirty: false,
            file_path: None,
            display_name: "Query 1".to_string(),
            document_identity: None,
            document_context: None,
            draft_text: None,
        }
    }

    #[test]
    fn apply_document_metadata_uses_explicit_dirty_state() {
        let mut state = test_editor_state();
        let context = test_document_context(
            DocumentIdentity::internal().expect("internal document identity"),
            42,
        );

        apply_document_metadata(&mut state, &context, "Query.sql".to_string(), true);

        assert!(state.is_dirty);
        assert_eq!(state.display_name, "Query.sql");
        assert_eq!(state.document_identity, Some(context.identity.clone()));
        assert_eq!(state.document_context, Some(context));
        assert_eq!(state.file_path, None);
    }

    #[test]
    fn apply_document_metadata_projects_external_path_into_workspace_state() {
        let mut state = test_editor_state();
        let context = test_document_context(
            DocumentIdentity::from_path("/tmp/query.sql").expect("external document identity"),
            0,
        );

        apply_document_metadata(&mut state, &context, "query.sql".to_string(), false);

        assert!(!state.is_dirty);
        assert_eq!(state.display_name, "query.sql");
        assert_eq!(state.file_path.as_deref(), Some("/tmp/query.sql"));
        assert_eq!(state.document_identity, Some(context.identity.clone()));
        assert_eq!(state.document_context, Some(context));
    }

    #[test]
    fn refresh_scope_is_copy_and_equality_comparable() {
        let scope = RefreshScope::ActiveConnectionSurfaces;
        let copied_scope = scope;

        assert_eq!(scope, copied_scope);
    }

    #[test]
    fn request_refresh_constructs_refresh_requested_event_variant() {
        let event = WorkspaceStateEvent::RefreshRequested(RefreshScope::ConnectionsList);
        assert!(matches!(
            event,
            WorkspaceStateEvent::RefreshRequested(RefreshScope::ConnectionsList)
        ));
    }

    #[test]
    fn workspace_session_serde_roundtrips() {
        let connection_id = Uuid::new_v4();
        let session = WorkspaceSession {
            active_connection_id: Some(connection_id),
            active_database: Some("analytics".to_string()),
            open_query_tabs: vec![WorkspaceSessionQueryTab {
                id: EditorId(7),
                display_name: "Revenue.sql".to_string(),
                connection_id: Some(connection_id),
                document_path: Some("/tmp/revenue.sql".to_string()),
                draft_text: Some("select * from revenue".to_string()),
            }],
            open_viewer_tabs: vec![
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::Table {
                        table_name: "revenue".to_string(),
                        database_name: Some("analytics".to_string()),
                        is_view: false,
                        viewer_state: Some(WorkspaceSessionTableViewerState {
                            visible_columns: vec!["id".to_string(), "amount".to_string()],
                            search_text: "north".to_string(),
                            ..WorkspaceSessionTableViewerState::default()
                        }),
                    },
                },
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::Collection {
                        database_name: "analytics".to_string(),
                        collection_name: "events".to_string(),
                        viewer_state: Some(WorkspaceSessionTableViewerState {
                            visible_columns: vec!["_id".to_string(), "type".to_string()],
                            ..WorkspaceSessionTableViewerState::default()
                        }),
                    },
                },
            ],
            active_editor_id: Some(EditorId(7)),
        };

        let json = serde_json::to_string(&session).expect("serialize workspace session");
        let restored: WorkspaceSession =
            serde_json::from_str(&json).expect("deserialize workspace session");

        assert_eq!(restored, session);
    }

    #[test]
    fn workspace_session_deserializes_missing_fields_as_default() {
        let session: WorkspaceSession =
            serde_json::from_str("{}").expect("deserialize empty workspace session");

        assert_eq!(session, WorkspaceSession::default());
    }

    #[test]
    fn persisted_session_ignores_live_workspace_fields() {
        let connection_id = Uuid::new_v4();
        let mut workspace_state = WorkspaceState::from_persisted_session(WorkspaceSession {
            active_connection_id: Some(connection_id),
            active_database: Some("warehouse".to_string()),
            open_query_tabs: vec![WorkspaceSessionQueryTab {
                id: EditorId(3),
                display_name: "Query 3".to_string(),
                connection_id: Some(connection_id),
                document_path: Some("/tmp/query-3.sql".to_string()),
                draft_text: Some("select * from query_3".to_string()),
            }],
            open_viewer_tabs: vec![WorkspaceSessionViewerTab {
                connection_id,
                kind: WorkspaceSessionViewerKind::RedisDatabase { database_index: 2 },
            }],
            active_editor_id: Some(EditorId(3)),
        });

        let editor_state = workspace_state
            .editors
            .get_mut(&EditorId(3))
            .expect("editor exists");
        editor_state.is_dirty = true;
        editor_state.document_identity =
            Some(DocumentIdentity::internal().expect("internal document identity"));
        workspace_state.connected_ids.push(connection_id);
        workspace_state.running_queries.insert(
            EditorId(3),
            QueryExecutionState {
                execution_id: 9,
                started_at: Instant::now(),
                sql: "select secret_live_buffer".to_string(),
                connection_id,
                status: QueryExecutionStatus::Running,
            },
        );
        workspace_state.diagnostics.insert(
            EditorId(3),
            vec![EditorDiagnostic {
                line: 1,
                column: 1,
                end_line: 1,
                end_column: 2,
                message: "live diagnostic".to_string(),
                severity: DiagnosticSeverity::Warning,
                source: Some("test".to_string()),
            }],
        );

        let session = workspace_state.persisted_session();
        let json = serde_json::to_string(&session).expect("serialize persisted session");

        assert!(json.contains("Query 3"));
        assert!(!json.contains("secret_live_buffer"));
        assert!(!json.contains("live diagnostic"));
        assert_eq!(
            session.open_query_tabs,
            vec![WorkspaceSessionQueryTab {
                id: EditorId(3),
                display_name: "Query 3".to_string(),
                connection_id: Some(connection_id),
                document_path: Some("/tmp/query-3.sql".to_string()),
                draft_text: Some("select * from query_3".to_string()),
            }]
        );
        assert_eq!(
            session.open_viewer_tabs,
            vec![WorkspaceSessionViewerTab {
                connection_id,
                kind: WorkspaceSessionViewerKind::RedisDatabase { database_index: 2 },
            }]
        );
    }

    #[test]
    fn record_open_viewer_tab_dedupes_and_orders_by_recent_use() {
        let connection_id = Uuid::new_v4();
        let first = WorkspaceSessionViewerTab {
            connection_id,
            kind: WorkspaceSessionViewerKind::Table {
                table_name: "users".to_string(),
                database_name: None,
                is_view: false,
                viewer_state: None,
            },
        };
        let second = WorkspaceSessionViewerTab {
            connection_id,
            kind: WorkspaceSessionViewerKind::RedisDatabase { database_index: 0 },
        };

        let mut open_viewer_tabs = Vec::new();
        record_open_viewer_tab_in_order(&mut open_viewer_tabs, first.clone());
        record_open_viewer_tab_in_order(&mut open_viewer_tabs, second.clone());
        record_open_viewer_tab_in_order(&mut open_viewer_tabs, first.clone());

        assert_eq!(open_viewer_tabs, vec![second, first]);
    }
}
