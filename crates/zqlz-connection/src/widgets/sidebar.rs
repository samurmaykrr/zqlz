//! Connection sidebar panel
//!
//! Displays a tree view of all database connections and their objects.

mod menus;
#[cfg(test)]
mod render_tests;
mod state;
pub mod types;
mod virtual_rows;

pub use types::*;
use virtual_rows::*;

use gpui::prelude::FluentBuilder;
use gpui::*;
use menus::state::ContextMenuState;
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;
use zqlz_core::{
    DocumentAdminObjectInfo, DocumentCollectionInfo, ObjectFormMode, ObjectsPanelObjectRef,
    TableRef,
};
use zqlz_ui::widgets::{
    ActiveTheme, DatabaseLogo, Icon, IconName, Sizable, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::Scrollbar,
    typography::body_small,
    v_flex,
};

// Keyboard actions for the connection sidebar
actions!(
    connection_sidebar,
    [
        ActivateConnection,
        DeleteSelectedConnection,
        ShowContextMenu
    ]
);

/// Events emitted by the connection sidebar
#[derive(Clone, Debug)]
pub enum ConnectionSidebarEvent {
    /// User wants to connect to a database
    Connect(Uuid),
    /// User wants to disconnect from a database
    Disconnect(Uuid),
    /// User selected a connection
    Selected(Uuid),
    /// User wants to add a new connection
    AddConnection,
    /// User wants to close/disconnect all active connections
    CloseAllConnections,
    /// User wants to create a new connection group
    NewGroup,
    /// User wants to open a table
    OpenTable {
        connection_id: Uuid,
        table: TableRef,
    },
    LoadTableDetails {
        connection_id: Uuid,
        table_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },
    /// User wants to open a view
    OpenView {
        connection_id: Uuid,
        view: TableRef,
    },
    /// User wants to design/edit a view SQL definition
    DesignView {
        connection_id: Uuid,
        view_name: String,
        object_schema: Option<String>,
    },
    /// User wants to create a new view
    NewView {
        connection_id: Uuid,
    },
    /// User wants to delete a view
    DeleteView {
        connection_id: Uuid,
        view_name: String,
    },
    /// User wants to duplicate a view
    DuplicateView {
        connection_id: Uuid,
        view_name: String,
    },
    /// User wants to rename a view
    RenameView {
        connection_id: Uuid,
        view_name: String,
    },
    /// User wants to copy view name to clipboard
    CopyViewName {
        view_name: String,
    },
    /// User wants to open a new query for this connection
    NewQuery(Uuid),
    /// User wants to refresh connections list
    RefreshConnections,
    /// User wants to delete a connection
    DeleteConnection(Uuid),
    /// User wants to duplicate a connection
    DuplicateConnection(Uuid),
    /// User wants to open settings for a connection
    OpenConnectionSettings(Uuid),
    /// User changed what an external AI agent may do on a connection
    SetAgentAccess(Uuid, crate::AgentAccess),
    /// User dropped one or more external paths onto the sidebar
    OpenDroppedPaths(Vec<PathBuf>),

    // Table-specific events
    /// User wants to design/edit a table structure
    DesignTable {
        connection_id: Uuid,
        table: TableRef,
    },
    /// User wants to create a new table
    NewTable {
        connection_id: Uuid,
    },
    /// User wants to delete a table
    DeleteTable {
        connection_id: Uuid,
        table: TableRef,
    },
    /// User wants to empty/truncate a table
    EmptyTable {
        connection_id: Uuid,
        table: TableRef,
    },
    /// User wants to duplicate a table
    DuplicateTable {
        connection_id: Uuid,
        table: TableRef,
    },
    /// User wants to rename a table
    RenameTable {
        connection_id: Uuid,
        table: TableRef,
    },
    /// User wants to import data into a table
    ImportData {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to export data from a table
    ExportData {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to dump table SQL (structure and data)
    DumpTableSql {
        connection_id: Uuid,
        table_name: String,
        include_data: bool,
    },
    /// User wants to copy table name to clipboard
    CopyTableName {
        table_name: String,
    },
    /// User wants to refresh a specific connection's schema
    RefreshSchema {
        connection_id: Uuid,
    },

    // Saved queries events
    /// User wants to open a saved query
    OpenSavedQuery {
        connection_id: Uuid,
        query_id: Uuid,
        query_name: String,
    },
    /// User wants to delete a saved query
    DeleteSavedQuery {
        connection_id: Uuid,
        query_id: Uuid,
        query_name: String,
    },
    /// User wants to rename a saved query
    RenameSavedQuery {
        connection_id: Uuid,
        query_id: Uuid,
        query_name: String,
    },
    ImportSavedQueries {
        connection_id: Uuid,
    },
    ExportSavedQueries {
        connection_id: Uuid,
    },
    MoveSavedQueryToFolder {
        connection_id: Uuid,
        query_id: Uuid,
        query_name: String,
        folder: Option<String>,
    },

    // Version history events
    /// User wants to view version history for a database object
    ViewHistory {
        connection_id: Uuid,
        object_name: String,
        object_schema: Option<String>,
        object_type: String, // "table", "view", "function", "procedure", "trigger"
    },

    // Function events
    /// User wants to open/view a function definition
    OpenFunction {
        connection_id: Uuid,
        function_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },

    // Procedure events
    /// User wants to open/view a procedure definition
    OpenProcedure {
        connection_id: Uuid,
        procedure_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },

    // Trigger events
    /// User wants to design/edit a trigger definition
    DesignTrigger {
        connection_id: Uuid,
        trigger_name: String,
        object_schema: Option<String>,
    },
    /// User wants to create a new trigger
    NewTrigger {
        connection_id: Uuid,
    },
    /// User wants to delete a trigger
    DeleteTrigger {
        connection_id: Uuid,
        trigger_name: String,
    },
    /// User wants to open the visual trigger designer
    OpenTriggerDesigner {
        connection_id: Uuid,
        trigger_name: Option<String>,
        object_schema: Option<String>,
    },
    OpenGenericObjectDefinition {
        connection_id: Uuid,
        object_ref: ObjectsPanelObjectRef,
    },
    OpenObjectDesigner {
        connection_id: Uuid,
        kind_id: String,
        mode: ObjectFormMode,
        object_ref: Option<ObjectsPanelObjectRef>,
    },

    // Redis-specific events
    /// User expanded a Redis database and needs keys loaded
    LoadRedisKeys {
        connection_id: Uuid,
        database_index: u16,
    },
    /// User wants to open/view a Redis key
    OpenRedisKey {
        connection_id: Uuid,
        database_index: u16,
        key_name: String,
    },
    /// User wants to open a Redis database to view all keys in table viewer
    OpenRedisDatabase {
        connection_id: Uuid,
        database_index: u16,
    },
    LoadDocumentCollections {
        connection_id: Uuid,
        database_name: String,
    },
    OpenDocumentCollection {
        connection_id: Uuid,
        database_name: String,
        collection_name: String,
    },

    // Multi-database events
    /// User wants to connect to a different database on the same server
    ConnectToDatabase {
        connection_id: Uuid,
        database_name: String,
    },

    /// User expanded a section that has not been loaded yet (lazy loading)
    LoadSection {
        connection_id: Uuid,
        section: SidebarSection,
    },
}

#[derive(Clone)]
struct DragSavedQuery {
    connection_id: Uuid,
    query_id: Uuid,
    query_name: SharedString,
}

impl Render for DragSavedQuery {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .px_3()
            .py_1()
            .bg(cx.theme().muted)
            .text_color(cx.theme().muted_foreground)
            .border_1()
            .border_color(cx.theme().border)
            .shadow_md()
            .child(self.query_name.clone())
    }
}

/// Connection sidebar showing all connections and their schema objects
/// Where a set of object sections is being rendered, and the context leaves
/// need (owning database, schema qualifier). Drives element ids and click
/// actions so all three tree shapes share one section renderer.
enum SectionScope<'a> {
    /// Sections directly under the connection, for drivers that never list
    /// databases.
    Connection { current_schema: Option<&'a str> },
    /// Sections directly under a database node (flattened, no schema level).
    Database {
        database_name: &'a str,
        current_schema: Option<&'a str>,
    },
    /// Sections nested inside a schema folder of a grouped database.
    SchemaGroup {
        database_name: &'a str,
        schema_name: &'a str,
    },
}

impl SectionScope<'_> {
    /// Unique element-id suffix; each scope has a distinct shape so rows from
    /// different scopes can never collide (a database and a schema with the
    /// same name used to produce duplicate GPUI element ids).
    fn element_suffix(&self, conn_id: Uuid) -> String {
        match self {
            Self::Connection { .. } => format!("{conn_id}"),
            Self::Database { database_name, .. } => format!("{conn_id}-{database_name}"),
            Self::SchemaGroup {
                database_name,
                schema_name,
            } => format!("{conn_id}-{database_name}-schema-{schema_name}"),
        }
    }

    fn action(&self, conn_id: Uuid, section: &'static str) -> SidebarSectionAction {
        match self {
            Self::Connection { .. } => SidebarSectionAction::ConnectionSection { conn_id, section },
            Self::Database { database_name, .. } => SidebarSectionAction::DatabaseSection {
                conn_id,
                database_name: (*database_name).to_string(),
                section,
            },
            Self::SchemaGroup {
                database_name,
                schema_name,
            } => SidebarSectionAction::SchemaGroupSection {
                conn_id,
                database_name: (*database_name).to_string(),
                schema_name: (*schema_name).to_string(),
                section,
            },
        }
    }

    fn database_name(&self) -> Option<&str> {
        match self {
            Self::Connection { .. } => None,
            Self::Database { database_name, .. } | Self::SchemaGroup { database_name, .. } => {
                Some(database_name)
            }
        }
    }
}

pub struct ConnectionSidebar {
    /// Focus handle for keyboard interactions
    focus_handle: FocusHandle,

    /// List of connections
    connections: Vec<ConnectionEntry>,

    /// Currently selected connection
    selected_connection: Option<Uuid>,

    /// Last object row activated from the sidebar tree.
    active_leaf_item_id: Option<String>,

    /// Scroll handle for the virtualized tree list.
    scroll_handle: UniformListScrollHandle,

    /// Flattened rows rendered by the sidebar virtual list.
    virtual_rows: Vec<SidebarVirtualRow>,

    table_details: std::collections::HashMap<SidebarTableKey, SidebarTableDetailsData>,
    expanded_table_keys: std::collections::HashSet<SidebarTableKey>,
    loading_table_keys: std::collections::HashSet<SidebarTableKey>,

    /// Tracks whether virtual rows need rebuilding before render.
    virtual_rows_dirty: bool,

    /// Number of rows rendered in the last visible range.
    rendered_rows_len: usize,

    /// Search query for filtering schema objects
    search_query: String,

    /// Lowercased search query cached for repeated per-item matching.
    search_query_lowercase: String,

    /// Input state for search field (lazily initialized)
    search_input_state: Option<Entity<InputState>>,

    /// Debounce task for expensive search recomputation.
    search_debounce_task: Option<Task<()>>,

    /// Context menu for the sidebar background (New Connection, Close All, New Group, Refresh)
    sidebar_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for connections
    connection_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for tables
    table_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for views
    view_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for saved queries
    query_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for functions
    function_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for procedures
    procedure_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu entity for triggers
    trigger_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu for section headers (Tables, Views, etc.)
    section_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu for materialized view items
    materialized_view_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu for Redis database items
    redis_db_context_menu: Option<Entity<ContextMenuState>>,

    /// Context menu for metadata object items
    metadata_object_context_menu: Option<Entity<ContextMenuState>>,

    /// Subscriptions to keep alive
    _subscriptions: Vec<Subscription>,
}

impl ConnectionSidebar {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            connections: Vec::new(),
            selected_connection: None,
            active_leaf_item_id: None,
            scroll_handle: UniformListScrollHandle::new(),
            virtual_rows: Vec::new(),
            table_details: std::collections::HashMap::new(),
            expanded_table_keys: std::collections::HashSet::new(),
            loading_table_keys: std::collections::HashSet::new(),
            virtual_rows_dirty: true,
            rendered_rows_len: 0,
            search_query: String::new(),
            search_query_lowercase: String::new(),
            search_input_state: None,
            search_debounce_task: None,
            sidebar_context_menu: None,
            connection_context_menu: None,
            table_context_menu: None,
            view_context_menu: None,
            query_context_menu: None,
            function_context_menu: None,
            procedure_context_menu: None,
            trigger_context_menu: None,
            section_context_menu: None,
            materialized_view_context_menu: None,
            redis_db_context_menu: None,
            metadata_object_context_menu: None,
            _subscriptions: Vec::new(),
        }
    }

    fn sidebar_indent(depth: usize) -> Pixels {
        px(10.0 + depth as f32 * 14.0)
    }

    /// Ensure the search input state is initialized
    fn ensure_search_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.search_input_state.is_none() {
            let search_input_state = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("Search objects...")
                    .clean_on_escape()
            });

            self._subscriptions.push(cx.subscribe(
                &search_input_state,
                |this, _, event: &InputEvent, cx| {
                    if let InputEvent::Change = event
                        && let Some(input) = &this.search_input_state
                    {
                        this.search_query = input.read(cx).value().to_string();
                        let next_lowercase = this.search_query.to_lowercase();

                        if this.search_query.is_empty() {
                            this.search_query_lowercase.clear();
                            this.search_debounce_task = None;
                            this.virtual_rows_dirty = true;
                            cx.notify();
                            return;
                        }

                        this.search_debounce_task = Some(cx.spawn({
                            let next_lowercase = next_lowercase.clone();
                            async move |this, cx| {
                                cx.background_executor()
                                    .timer(Duration::from_millis(150))
                                    .await;

                                if let Err(error) = this.update(cx, |sidebar, cx| {
                                    sidebar.search_query_lowercase = next_lowercase.clone();
                                    sidebar.search_debounce_task = None;
                                    sidebar.virtual_rows_dirty = true;
                                    cx.notify();
                                }) {
                                    tracing::debug!(
                                        error = %error,
                                        "Sidebar search debounce update skipped"
                                    );
                                }
                            }
                        }));
                    }
                },
            ));

            self.search_input_state = Some(search_input_state);
        }
    }

    /// Get current connections (for reading state)
    pub fn connections(&self) -> &[ConnectionEntry] {
        &self.connections
    }

    /// Emit a refresh event to reload connections and/or schema
    ///
    /// This is called by MainView when the user presses Cmd+R while the sidebar is focused.
    /// If a connection is selected, it refreshes that connection's schema.
    /// Otherwise, it refreshes the connections list.
    pub fn refresh(&self, cx: &mut Context<Self>) {
        if let Some(connection_id) = self.selected_connection {
            tracing::info!(
                "ConnectionSidebar: Refreshing schema for connection {:?}",
                connection_id
            );
            cx.emit(ConnectionSidebarEvent::RefreshSchema { connection_id });
        } else {
            tracing::info!("ConnectionSidebar: Refreshing connections list");
            cx.emit(ConnectionSidebarEvent::RefreshConnections);
        }
    }

    /// Toggle Redis database expand/collapse and trigger key loading if needed
    #[allow(dead_code)]
    fn toggle_redis_database_expand(
        &mut self,
        conn_id: Uuid,
        db_index: u16,
        cx: &mut Context<Self>,
    ) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(db) = conn
                .redis_databases
                .iter_mut()
                .find(|d| d.index == db_index)
        {
            db.is_expanded = !db.is_expanded;
            // If expanding and keys haven't been loaded yet, trigger load
            if db.is_expanded && db.keys.is_empty() && !db.is_loading {
                db.is_loading = true;
                should_load = true;
            }
        }
        cx.notify();

        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadRedisKeys {
                connection_id: conn_id,
                database_index: db_index,
            });
        }
    }

    /// Toggle Redis databases section expand/collapse
    fn toggle_redis_databases_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.redis_databases_expanded = !conn.redis_databases_expanded;
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle the saved-queries section expand/collapse
    fn toggle_queries_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.queries_expanded = !conn.queries_expanded;
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Select a connection
    fn select_connection(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.selected_connection = Some(id);
        self.active_leaf_item_id = None;
        cx.emit(ConnectionSidebarEvent::Selected(id));
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    fn set_active_leaf_item(&mut self, item_id: Option<String>, cx: &mut Context<Self>) {
        if self.active_leaf_item_id != item_id {
            self.active_leaf_item_id = item_id;
            self.virtual_rows_dirty = true;
            cx.notify();
        }
    }

    fn is_leaf_item_active(&self, item_id: &str) -> bool {
        self.active_leaf_item_id.as_deref() == Some(item_id)
    }

    /// Activate (Enter) the selected connection: connect if disconnected, toggle expand if connected
    fn activate_selected_connection(&mut self, cx: &mut Context<Self>) {
        let Some(conn_id) = self.selected_connection else {
            return;
        };
        let Some(conn) = self.connections.iter().find(|c| c.id == conn_id) else {
            return;
        };
        if conn.is_connecting {
            return;
        }
        if conn.is_connected {
            self.toggle_expand(conn_id, cx);
        } else {
            cx.emit(ConnectionSidebarEvent::Connect(conn_id));
        }
    }

    /// Delete the selected connection (emits event — host is responsible for confirmation)
    fn delete_selected_connection(&mut self, cx: &mut Context<Self>) {
        let Some(conn_id) = self.selected_connection else {
            return;
        };
        cx.emit(ConnectionSidebarEvent::DeleteConnection(conn_id));
    }

    /// Open the context menu for the selected connection at a synthetic position
    fn show_selected_context_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(conn_id) = self.selected_connection else {
            return;
        };
        let position = window.mouse_position();
        self.show_connection_context_menu(conn_id, position, window, cx);
    }

    /// Toggle connection expand/collapse
    fn toggle_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.is_expanded = !conn.is_expanded;
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle an object section expand/collapse for `database_name` (or the
    /// active database when `None`), kicking off a lazy load for still-empty
    /// lazily-loadable sections.
    fn toggle_section_expand(
        &mut self,
        id: Uuid,
        database_name: Option<&str>,
        section: SidebarSection,
        cx: &mut Context<Self>,
    ) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_section(section) {
                return;
            }

            // A lazy fetch always resolves against the connection's current
            // database, so only the scope that renders it may start one.
            let loads_into_this_scope = database_name.is_none_or(|database_name| {
                conn.databases
                    .iter()
                    .any(|database| database.is_active && database.name == database_name)
            });

            let schema = conn.schema_mut(database_name);
            let expanded = if schema.expanded_sections.remove(&section) {
                false
            } else {
                schema.expanded_sections.insert(section);
                true
            };

            let lazily_loadable = matches!(
                section,
                SidebarSection::Views
                    | SidebarSection::MaterializedViews
                    | SidebarSection::Triggers
                    | SidebarSection::Functions
                    | SidebarSection::Procedures
            );
            if expanded
                && lazily_loadable
                && loads_into_this_scope
                && schema.objects(section).is_empty()
                && schema.loading_sections.insert(section)
            {
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section,
            });
        }
    }

    fn table_key(
        conn_id: Uuid,
        database_name: Option<String>,
        object_schema: Option<String>,
        table_name: String,
    ) -> SidebarTableKey {
        SidebarTableKey {
            conn_id,
            database_name,
            schema_name: object_schema,
            table_name,
        }
    }

    fn toggle_table_details(
        &mut self,
        conn_id: Uuid,
        database_name: Option<String>,
        object_schema: Option<String>,
        table_name: String,
        cx: &mut Context<Self>,
    ) {
        let key = Self::table_key(
            conn_id,
            database_name.clone(),
            object_schema.clone(),
            table_name.clone(),
        );
        let should_load = if self.expanded_table_keys.contains(&key) {
            self.expanded_table_keys.remove(&key);
            false
        } else {
            self.expanded_table_keys.insert(key.clone());
            !self.table_details.contains_key(&key) && self.loading_table_keys.insert(key)
        };

        self.virtual_rows_dirty = true;
        cx.notify();

        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadTableDetails {
                connection_id: conn_id,
                table_name,
                object_schema,
                database_name,
            });
        }
    }

    /// Toggle a section within a specific database node. `section` is the row's
    /// section key — a schema-object key ("tables", "views", …), the schema node
    /// ("schema"), or a document section ("collections", "indexes", …).
    fn toggle_db_section(
        &mut self,
        conn_id: Uuid,
        db_name: &str,
        section: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(object_section) = SidebarSection::from_key(section)
            && object_section != SidebarSection::Queries
        {
            self.toggle_section_expand(conn_id, Some(db_name), object_section, cx);
            return;
        }

        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            let is_document = conn.is_document();
            if is_document {
                let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name) else {
                    return;
                };
                if section == "collections" {
                    db.collections_expanded = !db.collections_expanded;
                } else if !db.document_expanded_sections.remove(section) {
                    db.document_expanded_sections.insert(section.to_string());
                }
            } else if section == "schema" {
                let schema = conn.schema_mut(Some(db_name));
                schema.schema_expanded = !schema.schema_expanded;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a schema-group folder for `database_name` (or the active database
    /// when `None`).
    fn toggle_schema_group_expand(
        &mut self,
        conn_id: Uuid,
        database_name: Option<&str>,
        schema_name: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            let groups = &mut conn.schema_mut(database_name).expanded_schema_groups;
            if !groups.remove(schema_name) {
                groups.insert(schema_name.to_string());
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a section nested inside a schema-group folder.
    fn toggle_schema_group_section_expand(
        &mut self,
        conn_id: Uuid,
        database_name: Option<&str>,
        schema_name: &str,
        section: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            let section_key = format!("{schema_name}::{section}");
            let section_keys = &mut conn
                .schema_mut(database_name)
                .expanded_schema_section_keys;
            if !section_keys.remove(&section_key) {
                section_keys.insert(section_key);
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a specific database node expand/collapse.
    /// When expanding a database that has no schema loaded yet, automatically
    /// triggers schema loading so the user doesn't need a second click.
    fn toggle_database_expand(&mut self, id: Uuid, db_name: &str, cx: &mut Context<Self>) {
        let mut should_load_schema = false;
        let mut should_load_collections = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            let is_document = conn.is_document();
            let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name) else {
                return;
            };
            db.is_expanded = !db.is_expanded;
            if is_document {
                db.collections_expanded = db.is_expanded;
                if db.is_expanded && db.collections.is_empty() && !db.collections_loading {
                    db.collections_loading = true;
                    should_load_collections = true;
                }
            } else if db.is_expanded && db.schema.is_none() && !db.is_active && !db.is_loading {
                should_load_schema = true;
            }
        }
        if should_load_schema {
            cx.emit(ConnectionSidebarEvent::ConnectToDatabase {
                connection_id: id,
                database_name: db_name.to_string(),
            });
        }
        if should_load_collections {
            cx.emit(ConnectionSidebarEvent::LoadDocumentCollections {
                connection_id: id,
                database_name: db_name.to_string(),
            });
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Get selected connection ID
    pub fn selected(&self) -> Option<Uuid> {
        self.selected_connection
    }

    /// Set the selected connection from external source (e.g., WorkspaceState sync)
    ///
    /// Unlike `select_connection`, this does NOT emit a Selected event,
    /// as it's meant to sync UI state with an external source of truth.
    pub fn set_selected(&mut self, id: Option<Uuid>, cx: &mut Context<Self>) {
        if self.selected_connection != id {
            self.selected_connection = id;
            self.active_leaf_item_id = None;
            self.virtual_rows_dirty = true;
            cx.notify();
        }
    }

    fn object_capabilities_for_connection(&self, conn_id: Uuid) -> SidebarObjectCapabilities {
        self.connections
            .iter()
            .find(|connection| connection.id == conn_id)
            .map(|connection| connection.object_capabilities)
            .unwrap_or_default()
    }

    fn supports_sidebar_section(&self, conn_id: Uuid, section: &str) -> bool {
        let capabilities = self.object_capabilities_for_connection(conn_id);
        SidebarSection::from_key(section)
            .map(|section| capabilities.supports_section(section))
            // Unknown keys fail closed: offering an action the driver cannot
            // perform is worse than hiding it.
            .unwrap_or(false)
    }

    /// Check if an object name matches the search query (case-insensitive)
    fn matches_search(&self, name: &str) -> bool {
        if self.search_query_lowercase.is_empty() {
            return true;
        }
        if name.contains(&self.search_query_lowercase) {
            return true;
        }

        // Most DB object names are already lower-case snake_case. The fast-path
        // above avoids allocation in the common case; we only normalize when
        // mixed-case identifiers are present.
        name.to_lowercase().contains(&self.search_query_lowercase)
    }

    /// Filter a list of names by the search query
    fn filter_by_search<'a>(&self, items: &'a [String]) -> Vec<&'a String> {
        items
            .iter()
            .filter(|name| self.matches_search(name))
            .collect()
    }

    fn format_database_size_virtual_rows(bytes: i64) -> String {
        if bytes < 1024 {
            format!("{} B", bytes)
        } else if bytes < 1024 * 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else if bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        }
    }

    fn db_icon_for_virtual_rows(&self, db_type: &str) -> ZqlzIcon {
        match db_type.to_ascii_lowercase().as_str() {
            "sqlite" => ZqlzIcon::SQLite,
            "turso" => ZqlzIcon::Turso,
            "postgresql" | "postgres" => ZqlzIcon::PostgreSQL,
            "mysql" => ZqlzIcon::MySQL,
            "mariadb" => ZqlzIcon::MariaDB,
            "redis" => ZqlzIcon::Redis,
            "mongodb" => ZqlzIcon::MongoDB,
            "clickhouse" => ZqlzIcon::ClickHouse,
            "duckdb" => ZqlzIcon::DuckDB,
            "mssql" | "sqlserver" => ZqlzIcon::MsSql,
            _ => ZqlzIcon::Database,
        }
    }

    fn db_logo_for_virtual_rows(&self, db_type: &str) -> Option<DatabaseLogo> {
        match db_type.to_ascii_lowercase().as_str() {
            "sqlite" => Some(DatabaseLogo::SQLite),
            "turso" => Some(DatabaseLogo::Turso),
            "postgresql" | "postgres" => Some(DatabaseLogo::PostgreSQL),
            "mysql" => Some(DatabaseLogo::MySQL),
            "mariadb" => Some(DatabaseLogo::MariaDB),
            "redis" => Some(DatabaseLogo::Redis),
            "mongodb" => Some(DatabaseLogo::MongoDB),
            "clickhouse" => Some(DatabaseLogo::ClickHouse),
            "duckdb" => Some(DatabaseLogo::DuckDB),
            "mssql" | "sqlserver" => Some(DatabaseLogo::MsSql),
            _ => None,
        }
    }

    fn import_dropped_paths(&mut self, paths: &[PathBuf], cx: &mut Context<Self>) {
        if !paths.is_empty() {
            cx.emit(ConnectionSidebarEvent::OpenDroppedPaths(paths.to_vec()));
        }
    }

    fn sidebar_row_icon(&self, icon: &SidebarRowIcon, muted_foreground: Hsla) -> AnyElement {
        match icon {
            SidebarRowIcon::Folder => Icon::new(IconName::Folder)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Database => Icon::new(ZqlzIcon::Database)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Table => Icon::new(ZqlzIcon::Table)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::View => Icon::new(ZqlzIcon::Eye)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::MaterializedView => Icon::new(ZqlzIcon::TreeStructure)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Trigger => Icon::new(ZqlzIcon::LightningBolt)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Function => Icon::new(ZqlzIcon::Function)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Procedure => Icon::new(ZqlzIcon::Gear)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Event => Icon::new(ZqlzIcon::Calendar)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Sequence => Icon::new(ZqlzIcon::ListNumbers)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Domain | SidebarRowIcon::Type => Icon::new(ZqlzIcon::BracketsCurly)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Extension => Icon::new(ZqlzIcon::Stack)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Field => Icon::new(ZqlzIcon::Columns)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Index => Icon::new(ZqlzIcon::Key)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::ForeignKey => Icon::new(ZqlzIcon::Link)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Constraint => Icon::new(ZqlzIcon::CheckCircle)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Rule => Icon::new(ZqlzIcon::ListBullets)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
            SidebarRowIcon::Query => Icon::new(ZqlzIcon::FileSql)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
        }
    }

    /// With no known schema names any dot is treated as a schema qualifier, so
    /// a table legitimately named `foo.bar` would invent a `foo` folder. Callers
    /// must only group when the driver reports schema support.
    fn split_schema_qualified_name_for_rows<'a>(
        name: &'a str,
        schema_names: &[String],
    ) -> Option<(&'a str, &'a str)> {
        let (schema_name, object_name) = name.split_once('.')?;
        if schema_name.is_empty() || object_name.is_empty() {
            return None;
        }
        if !schema_names.is_empty() && !schema_names.iter().any(|name| name == schema_name) {
            return None;
        }

        Some((schema_name, object_name))
    }

    /// Bucket objects by their schema qualifier. Returns `None` when nothing
    /// is schema-qualified and no schemas are declared — the caller renders
    /// flat sections instead of schema folders.
    fn group_schema_sections_for_rows(
        sections: &[(SidebarSection, &[String])],
        schema_names: &[String],
        fallback_schema_name: Option<&str>,
    ) -> Option<Vec<(String, SchemaSectionGroup)>> {
        let mut groups: BTreeMap<String, SchemaSectionGroup> = BTreeMap::new();
        let mut saw_schema_qualified_name = false;

        let fallback_schema = fallback_schema_name.unwrap_or("public").to_string();

        for schema_name in schema_names {
            groups.entry(schema_name.clone()).or_default();
        }

        for (section, objects) in sections {
            for object_name in *objects {
                if let Some((schema_name, object_name)) =
                    Self::split_schema_qualified_name_for_rows(object_name, schema_names)
                {
                    saw_schema_qualified_name = true;
                    groups
                        .entry(schema_name.to_string())
                        .or_default()
                        .push(*section, object_name.to_string());
                } else {
                    groups
                        .entry(fallback_schema.clone())
                        .or_default()
                        .push(*section, object_name.clone());
                }
            }
        }

        if !saw_schema_qualified_name && schema_names.is_empty() {
            return None;
        }

        Some(groups.into_iter().collect())
    }

    fn rebuild_virtual_rows(&mut self) {
        let has_search = !self.search_query.is_empty();
        let mut matched_leaf_rows = 0usize;
        let mut rows = Vec::new();

        for connection in &self.connections {
            rows.push(SidebarVirtualRow::Connection(ConnectionRow {
                conn_id: connection.id,
                conn_name: connection.name.clone(),
                db_type: connection.db_type.clone(),
                is_connected: connection.is_connected,
                is_connecting: connection.is_connecting,
            }));

            if !(connection.is_expanded && connection.is_connected) {
                continue;
            }

            if connection.is_redis() {
                self.append_redis_virtual_rows(connection, &mut rows, &mut matched_leaf_rows);
            } else if connection.is_document() {
                self.append_document_virtual_rows(connection, &mut rows, &mut matched_leaf_rows);
            } else {
                self.append_sql_virtual_rows(connection, &mut rows, &mut matched_leaf_rows);
            }
        }

        if has_search && matched_leaf_rows == 0 {
            rows.push(SidebarVirtualRow::NoResults(NoResultsRow {
                query: self.search_query.clone(),
            }));
        }

        self.virtual_rows = rows;
    }

    fn append_redis_virtual_rows(
        &self,
        connection: &ConnectionEntry,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query.is_empty();
        let search_lowercase = self.search_query_lowercase.as_str();

        let filtered_databases: Vec<_> = connection
            .redis_databases
            .iter()
            .filter(|database| {
                if !has_search {
                    return true;
                }

                let database_name = format!("db{}", database.index);
                if database_name.contains(search_lowercase) {
                    return true;
                }

                database.keys.iter().any(|key_name| {
                    if key_name.contains(search_lowercase) {
                        return true;
                    }

                    key_name.to_lowercase().contains(search_lowercase)
                })
            })
            .collect();

        let filtered_queries: Vec<_> = connection
            .queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();

        let databases_expanded = connection.redis_databases_expanded
            || (has_search && !filtered_databases.is_empty());
        let queries_expanded =
            connection.queries_expanded || (has_search && !filtered_queries.is_empty());

        if !has_search || !filtered_databases.is_empty() {
            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!("databases-header-{}", connection.id),
                icon: SidebarRowIcon::Database,
                label: "Databases".to_string(),
                total_count: connection.redis_databases.len(),
                filtered_count: filtered_databases.len(),
                is_expanded: databases_expanded,
                depth: 1,
                action: SidebarSectionAction::RedisDatabases {
                    conn_id: connection.id,
                },
                context_menu_section: Some("redis_databases"),
            }));

            if databases_expanded {
                for database in filtered_databases {
                    let mut label = format!(
                        "db{}{}",
                        database.index,
                        database
                            .key_count
                            .map(|count| format!(" ({count})"))
                            .unwrap_or_default()
                    );

                    if database.is_loading {
                        label.push_str(" ...");
                    }

                    rows.push(SidebarVirtualRow::Leaf(LeafRow {
                        element_id: format!("redis-db-{}-{}", connection.id, database.index),
                        icon: SidebarRowIcon::Database,
                        label,
                        depth: 2,
                        kind: SidebarLeafKind::RedisDatabase {
                            conn_id: connection.id,
                            database_index: database.index,
                        },
                    }));

                    *matched_leaf_rows += 1;
                }
            }
        }

        if !has_search || !filtered_queries.is_empty() {
            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!("queries-header-{}", connection.id),
                icon: SidebarRowIcon::Query,
                label: "Saved Queries".to_string(),
                total_count: connection.queries.len(),
                filtered_count: filtered_queries.len(),
                is_expanded: queries_expanded,
                depth: 1,
                action: SidebarSectionAction::Queries {
                    conn_id: connection.id,
                },
                context_menu_section: Some("queries"),
            }));

            if queries_expanded {
                Self::push_query_rows(rows, connection.id, filtered_queries, 2, matched_leaf_rows);
            }
        }
    }

    fn append_document_virtual_rows(
        &self,
        connection: &ConnectionEntry,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query_lowercase.is_empty();
        let search_lowercase = self.search_query_lowercase.as_str();
        for database in &connection.databases {
            let matching_collections = database
                .collections
                .iter()
                .filter(|collection| {
                    !has_search
                        || collection.name.to_lowercase().contains(search_lowercase)
                        || database.name.to_lowercase().contains(search_lowercase)
                })
                .collect::<Vec<_>>();
            let matching_views = matching_collections
                .iter()
                .copied()
                .filter(|collection| collection.collection_type == "view")
                .collect::<Vec<_>>();
            let matching_regular_collections = matching_collections
                .iter()
                .copied()
                .filter(|collection| Self::is_regular_mongodb_collection(collection))
                .collect::<Vec<_>>();
            let matching_timeseries_collections = matching_collections
                .iter()
                .copied()
                .filter(|collection| collection.collection_type == "timeseries")
                .collect::<Vec<_>>();
            let matching_capped_collections = matching_collections
                .iter()
                .copied()
                .filter(|collection| {
                    collection
                        .options_json
                        .as_ref()
                        .and_then(|options| options.get("capped"))
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            let matching_clustered_collections = matching_collections
                .iter()
                .copied()
                .filter(|collection| collection.clustered_index_json.is_some())
                .collect::<Vec<_>>();
            let matching_indexes = database
                .indexes
                .iter()
                .filter(|index| {
                    !has_search
                        || index.name.to_lowercase().contains(search_lowercase)
                        || index.collection.to_lowercase().contains(search_lowercase)
                })
                .collect::<Vec<_>>();
            let matching_functions = database
                .functions
                .iter()
                .filter(|function| {
                    !has_search || function.name.to_lowercase().contains(search_lowercase)
                })
                .collect::<Vec<_>>();
            let matching_gridfs_buckets = database
                .gridfs_buckets
                .iter()
                .filter(|bucket| {
                    !has_search || bucket.name.to_lowercase().contains(search_lowercase)
                })
                .collect::<Vec<_>>();
            let matching_users = database
                .users
                .iter()
                .filter(|user| Self::document_admin_matches_search(user, search_lowercase))
                .collect::<Vec<_>>();
            let matching_roles = database
                .roles
                .iter()
                .filter(|role| Self::document_admin_matches_search(role, search_lowercase))
                .collect::<Vec<_>>();
            let matching_search_indexes = database
                .search_indexes
                .iter()
                .filter(|index| Self::document_admin_matches_search(index, search_lowercase))
                .collect::<Vec<_>>();
            let matching_vector_indexes = database
                .vector_indexes
                .iter()
                .filter(|index| Self::document_admin_matches_search(index, search_lowercase))
                .collect::<Vec<_>>();
            let matching_server = database
                .server
                .iter()
                .filter(|object| Self::document_admin_matches_search(object, search_lowercase))
                .collect::<Vec<_>>();
            let matching_sharding = database
                .sharding
                .iter()
                .filter(|object| Self::document_admin_matches_search(object, search_lowercase))
                .collect::<Vec<_>>();
            let matching_aggregation_pipelines = connection
                .queries
                .iter()
                .filter(|query| {
                    Self::is_mongodb_aggregation_asset(query)
                        && (!has_search
                            || Self::saved_query_matches_search(query, search_lowercase))
                })
                .collect::<Vec<_>>();
            let matching_map_reduces = connection
                .queries
                .iter()
                .filter(|query| {
                    Self::is_mongodb_map_reduce_asset(query)
                        && (!has_search
                            || Self::saved_query_matches_search(query, search_lowercase))
                })
                .collect::<Vec<_>>();
            let matching_queries = connection
                .queries
                .iter()
                .filter(|query| {
                    !Self::is_mongodb_aggregation_asset(query)
                        && !Self::is_mongodb_map_reduce_asset(query)
                        && (!has_search
                            || Self::saved_query_matches_search(query, search_lowercase))
                })
                .collect::<Vec<_>>();

            if has_search
                && matching_collections.is_empty()
                && matching_indexes.is_empty()
                && matching_functions.is_empty()
                && matching_gridfs_buckets.is_empty()
                && matching_users.is_empty()
                && matching_roles.is_empty()
                && matching_search_indexes.is_empty()
                && matching_vector_indexes.is_empty()
                && matching_server.is_empty()
                && matching_sharding.is_empty()
                && matching_aggregation_pipelines.is_empty()
                && matching_map_reduces.is_empty()
                && matching_queries.is_empty()
            {
                continue;
            }

            rows.push(SidebarVirtualRow::Database(DatabaseRow {
                conn_id: connection.id,
                database_name: database.name.clone(),
                is_expanded: database.is_expanded,
                has_schema: !database.collections.is_empty(),
                is_active: database.is_active,
                size_label: database
                    .size_bytes
                    .map(Self::format_database_size_virtual_rows),
            }));

            if database.is_expanded {
                if database.collections_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!(
                            "loading-collections-{}-{}",
                            connection.id, database.name
                        ),
                        text: "Loading collections...".to_string(),
                        depth: 2,
                    }));
                }

                let collections_expanded = database.collections_expanded
                    || (has_search && !matching_regular_collections.is_empty());
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("document-collections-{}-{}", connection.id, database.name),
                    icon: SidebarRowIcon::Table,
                    label: "Collections".to_string(),
                    total_count: database
                        .collections
                        .iter()
                        .filter(|collection| {
                            collection.collection_type != "view"
                                && collection.collection_type != "timeseries"
                                && !collection.name.starts_with("system.")
                        })
                        .count(),
                    filtered_count: matching_regular_collections.len(),
                    is_expanded: collections_expanded,
                    depth: 2,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database.name.clone(),
                        section: "collections",
                    },
                    context_menu_section: Some("collections"),
                }));
                if collections_expanded {
                    for collection in matching_regular_collections {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "document-collection-{}-{}-{}",
                                connection.id, database.name, collection.name
                            ),
                            icon: SidebarRowIcon::Table,
                            label: collection.name.clone(),
                            depth: 3,
                            kind: SidebarLeafKind::DocumentCollection {
                                conn_id: connection.id,
                                database_name: database.name.clone(),
                                collection_name: collection.name.clone(),
                                object_type: "document_collection",
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }

                let document_sections = [
                    ("views", "Views", SidebarRowIcon::View, matching_views.len()),
                    (
                        "timeseries",
                        "Time Series",
                        SidebarRowIcon::Sequence,
                        matching_timeseries_collections.len(),
                    ),
                    (
                        "capped",
                        "Capped",
                        SidebarRowIcon::Database,
                        matching_capped_collections.len(),
                    ),
                    (
                        "clustered",
                        "Clustered",
                        SidebarRowIcon::Index,
                        matching_clustered_collections.len(),
                    ),
                    (
                        "functions",
                        "Functions",
                        SidebarRowIcon::Function,
                        matching_functions.len(),
                    ),
                    (
                        "indexes",
                        "Indexes",
                        SidebarRowIcon::Index,
                        matching_indexes.len(),
                    ),
                    (
                        "map_reduces",
                        "MapReduces",
                        SidebarRowIcon::Procedure,
                        matching_map_reduces.len(),
                    ),
                    (
                        "aggregation_pipelines",
                        "Aggregation Pipelines",
                        SidebarRowIcon::Sequence,
                        matching_aggregation_pipelines.len(),
                    ),
                    (
                        "search_indexes",
                        "Search Indexes",
                        SidebarRowIcon::Index,
                        matching_search_indexes.len(),
                    ),
                    (
                        "vector_indexes",
                        "Vector Indexes",
                        SidebarRowIcon::Index,
                        matching_vector_indexes.len(),
                    ),
                    (
                        "gridfs_buckets",
                        "GridFS Buckets",
                        SidebarRowIcon::Database,
                        matching_gridfs_buckets.len(),
                    ),
                    (
                        "users",
                        "Users",
                        SidebarRowIcon::Procedure,
                        matching_users.len(),
                    ),
                    (
                        "roles",
                        "Roles",
                        SidebarRowIcon::Procedure,
                        matching_roles.len(),
                    ),
                    (
                        "sharding",
                        "Sharding",
                        SidebarRowIcon::Database,
                        matching_sharding.len(),
                    ),
                    (
                        "server",
                        "Server",
                        SidebarRowIcon::Database,
                        matching_server.len(),
                    ),
                    (
                        "queries",
                        "Queries",
                        SidebarRowIcon::Query,
                        connection.queries.len(),
                    ),
                ];

                for (section_key, label, icon, count) in document_sections {
                    let is_expanded = database.document_expanded_sections.contains(section_key)
                        || (has_search && count > 0);
                    rows.push(SidebarVirtualRow::Section(SectionRow {
                        element_id: format!(
                            "document-section-{}-{}-{}",
                            connection.id, database.name, section_key
                        ),
                        icon,
                        label: label.to_string(),
                        total_count: count,
                        filtered_count: count,
                        is_expanded,
                        depth: 2,
                        action: SidebarSectionAction::DatabaseSection {
                            conn_id: connection.id,
                            database_name: database.name.clone(),
                            section: section_key,
                        },
                        context_menu_section: match section_key {
                            "views" | "indexes" | "users" | "roles" => Some(section_key),
                            _ => None,
                        },
                    }));

                    if !is_expanded {
                        continue;
                    }

                    match section_key {
                        "views" => {
                            for collection in &matching_views {
                                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                    element_id: format!(
                                        "document-view-{}-{}-{}",
                                        connection.id, database.name, collection.name
                                    ),
                                    icon: SidebarRowIcon::View,
                                    label: collection.name.clone(),
                                    depth: 3,
                                    kind: SidebarLeafKind::DocumentCollection {
                                        conn_id: connection.id,
                                        database_name: database.name.clone(),
                                        collection_name: collection.name.clone(),
                                        object_type: "document_view",
                                    },
                                }));
                                *matched_leaf_rows += 1;
                            }
                        }
                        "timeseries" => {
                            for collection in &matching_timeseries_collections {
                                Self::push_document_collection_leaf(
                                    rows,
                                    connection.id,
                                    &database.name,
                                    &collection.name,
                                    SidebarRowIcon::Sequence,
                                    "document-timeseries",
                                    3,
                                );
                                *matched_leaf_rows += 1;
                            }
                        }
                        "capped" => {
                            for collection in &matching_capped_collections {
                                Self::push_document_collection_leaf(
                                    rows,
                                    connection.id,
                                    &database.name,
                                    &collection.name,
                                    SidebarRowIcon::Database,
                                    "document-capped",
                                    3,
                                );
                                *matched_leaf_rows += 1;
                            }
                        }
                        "clustered" => {
                            for collection in &matching_clustered_collections {
                                Self::push_document_collection_leaf(
                                    rows,
                                    connection.id,
                                    &database.name,
                                    &collection.name,
                                    SidebarRowIcon::Index,
                                    "document-clustered",
                                    3,
                                );
                                *matched_leaf_rows += 1;
                            }
                        }
                        "functions" => {
                            for function in &matching_functions {
                                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                    element_id: format!(
                                        "document-function-{}-{}-{}",
                                        connection.id, database.name, function.name
                                    ),
                                    icon: SidebarRowIcon::Function,
                                    label: function.name.clone(),
                                    depth: 3,
                                    kind: SidebarLeafKind::MetadataObject {
                                        conn_id: connection.id,
                                        object_name: function.name.clone(),
                                        object_schema: None,
                                        database_name: Some(database.name.clone()),
                                        object_type: "document_function",
                                    },
                                }));
                                *matched_leaf_rows += 1;
                            }
                        }
                        "indexes" => {
                            for index in &matching_indexes {
                                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                    element_id: format!(
                                        "document-index-{}-{}-{}-{}",
                                        connection.id, database.name, index.collection, index.name
                                    ),
                                    icon: SidebarRowIcon::Index,
                                    label: format!("{}.{}", index.collection, index.name),
                                    depth: 3,
                                    kind: SidebarLeafKind::MetadataObject {
                                        conn_id: connection.id,
                                        object_name: index.name.clone(),
                                        object_schema: Some(index.collection.clone()),
                                        database_name: Some(database.name.clone()),
                                        object_type: "document_index",
                                    },
                                }));
                                *matched_leaf_rows += 1;
                            }
                        }
                        "gridfs_buckets" => {
                            for bucket in &matching_gridfs_buckets {
                                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                    element_id: format!(
                                        "document-gridfs-{}-{}-{}",
                                        connection.id, database.name, bucket.name
                                    ),
                                    icon: SidebarRowIcon::Database,
                                    label: bucket.name.clone(),
                                    depth: 3,
                                    kind: SidebarLeafKind::DocumentCollection {
                                        conn_id: connection.id,
                                        database_name: database.name.clone(),
                                        collection_name: bucket.files_collection.clone(),
                                        object_type: "document_gridfs_bucket",
                                    },
                                }));
                                *matched_leaf_rows += 1;
                            }
                        }
                        "map_reduces" => Self::push_query_rows(
                            rows,
                            connection.id,
                            matching_map_reduces.clone(),
                            3,
                            matched_leaf_rows,
                        ),
                        "aggregation_pipelines" => Self::push_query_rows(
                            rows,
                            connection.id,
                            matching_aggregation_pipelines.clone(),
                            3,
                            matched_leaf_rows,
                        ),
                        "users" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_user",
                                SidebarRowIcon::Procedure,
                                &matching_users,
                                matched_leaf_rows,
                            );
                        }
                        "roles" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_role",
                                SidebarRowIcon::Procedure,
                                &matching_roles,
                                matched_leaf_rows,
                            );
                        }
                        "search_indexes" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_search_index",
                                SidebarRowIcon::Index,
                                &matching_search_indexes,
                                matched_leaf_rows,
                            );
                        }
                        "vector_indexes" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_vector_index",
                                SidebarRowIcon::Index,
                                &matching_vector_indexes,
                                matched_leaf_rows,
                            );
                        }
                        "server" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_server",
                                SidebarRowIcon::Database,
                                &matching_server,
                                matched_leaf_rows,
                            );
                        }
                        "sharding" => {
                            Self::push_document_admin_rows(
                                rows,
                                connection.id,
                                &database.name,
                                "document_sharding",
                                SidebarRowIcon::Database,
                                &matching_sharding,
                                matched_leaf_rows,
                            );
                        }
                        "queries" => Self::push_query_rows(
                            rows,
                            connection.id,
                            matching_queries.clone(),
                            3,
                            matched_leaf_rows,
                        ),
                        _ => {}
                    }
                }
            }
        }
    }

    fn append_sql_virtual_rows(
        &self,
        connection: &ConnectionEntry,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        if connection.databases.is_empty() {
            let scope = SectionScope::Connection {
                current_schema: connection.schema.schema_name.as_deref(),
            };
            self.append_section_rows_for_scope(
                connection,
                &scope,
                &connection.schema,
                1,
                rows,
                matched_leaf_rows,
            );
            return;
        }

        for database in &connection.databases {
            rows.push(SidebarVirtualRow::Database(DatabaseRow {
                conn_id: connection.id,
                database_name: database.name.clone(),
                is_expanded: database.is_expanded,
                has_schema: database.schema.is_some(),
                is_active: database.is_active,
                size_label: database
                    .size_bytes
                    .map(Self::format_database_size_virtual_rows),
            }));

            if !database.is_expanded {
                continue;
            }

            if Self::should_render_database_loading(database) {
                rows.push(SidebarVirtualRow::Loading(LoadingRow {
                    element_id: format!("loading-schema-{}-{}", connection.id, database.name),
                    text: "Loading schema...".to_string(),
                    depth: 2,
                }));
                continue;
            }

            if Self::should_skip_unloaded_database(database) {
                continue;
            }

            // Only the active database can still be without a schema of its own
            // here — the checks above skipped the rest — and it renders the
            // connection-scope state it has not taken ownership of yet.
            let schema_data = connection.schema(Some(&database.name));

            if Self::should_flatten_database_schema_group(&connection.db_type) {
                self.append_database_objects_rows(
                    connection,
                    database,
                    schema_data,
                    2,
                    rows,
                    matched_leaf_rows,
                );
                continue;
            }

            if let Some(groups) = Self::group_schema_sections_for_rows(
                &schema_data.object_sections(),
                &schema_data.schema_names,
                schema_data
                    .schema_name
                    .as_deref()
                    .or(connection.object_capabilities.default_schema),
            ) {
                self.append_grouped_objects_rows(
                    connection,
                    &database.name,
                    schema_data,
                    &groups,
                    2,
                    rows,
                    matched_leaf_rows,
                );
                continue;
            }

            // A schema node whose name repeats the database node would just add
            // a level of nesting with nothing in it.
            let show_schema_node = schema_data
                .schema_name
                .as_ref()
                .is_some_and(|schema_name| schema_name != &database.name);

            if show_schema_node {
                rows.push(SidebarVirtualRow::SchemaNode(SchemaNodeRow {
                    conn_id: connection.id,
                    database_name: database.name.clone(),
                    schema_name: schema_data.schema_name.clone().unwrap_or_default(),
                    is_expanded: schema_data.schema_expanded,
                }));
            }

            if !show_schema_node || schema_data.schema_expanded {
                self.append_database_objects_rows(
                    connection,
                    database,
                    schema_data,
                    if show_schema_node { 3 } else { 2 },
                    rows,
                    matched_leaf_rows,
                );
            }
        }
    }

    fn should_render_database_loading(database: &SidebarDatabaseInfo) -> bool {
        database.is_loading
    }

    fn should_skip_unloaded_database(database: &SidebarDatabaseInfo) -> bool {
        database.schema.is_none() && !database.is_active && !database.is_loading
    }

    fn push_document_collection_leaf(
        rows: &mut Vec<SidebarVirtualRow>,
        connection_id: Uuid,
        database_name: &str,
        collection_name: &str,
        icon: SidebarRowIcon,
        element_prefix: &str,
        depth: usize,
    ) {
        rows.push(SidebarVirtualRow::Leaf(LeafRow {
            element_id: format!(
                "{element_prefix}-{connection_id}-{database_name}-{collection_name}"
            ),
            icon,
            label: collection_name.to_string(),
            depth,
            kind: SidebarLeafKind::DocumentCollection {
                conn_id: connection_id,
                database_name: database_name.to_string(),
                collection_name: collection_name.to_string(),
                object_type: "document_collection",
            },
        }));
    }

    fn push_document_admin_rows(
        rows: &mut Vec<SidebarVirtualRow>,
        connection_id: Uuid,
        database_name: &str,
        object_type: &'static str,
        icon: SidebarRowIcon,
        objects: &[&DocumentAdminObjectInfo],
        matched_leaf_rows: &mut usize,
    ) {
        for object in objects {
            let label = object
                .unavailable_reason
                .as_ref()
                .map(|_| format!("{} unavailable", object.name))
                .unwrap_or_else(|| object.name.clone());
            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                element_id: format!(
                    "{object_type}-{connection_id}-{database_name}-{}",
                    object.name
                ),
                icon,
                label,
                depth: 3,
                kind: SidebarLeafKind::MetadataObject {
                    conn_id: connection_id,
                    object_name: object.name.clone(),
                    object_schema: Some(object.kind.clone()),
                    database_name: Some(database_name.to_string()),
                    object_type,
                },
            }));
            *matched_leaf_rows += 1;
        }
    }

    fn is_mongodb_aggregation_asset(query: &SavedQueryInfo) -> bool {
        let folder = query.folder.as_deref().unwrap_or_default().to_lowercase();
        let name = query.name.to_lowercase();
        let query_text = query.query_text.to_lowercase();

        folder.contains("aggregation")
            || folder.contains("pipeline")
            || name.contains("aggregation")
            || name.contains("pipeline")
            || query_text.contains(".aggregate(")
            || query_text.contains("\"aggregate\"")
            || query_text.contains("'aggregate'")
            || query_text.contains("aggregate:")
    }

    fn is_regular_mongodb_collection(collection: &DocumentCollectionInfo) -> bool {
        let is_capped = collection
            .options_json
            .as_ref()
            .and_then(|options| options.get("capped"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        collection.collection_type != "view"
            && collection.collection_type != "timeseries"
            && !is_capped
            && collection.clustered_index_json.is_none()
            && !collection.name.starts_with("system.")
    }

    fn is_mongodb_map_reduce_asset(query: &SavedQueryInfo) -> bool {
        let folder = query.folder.as_deref().unwrap_or_default().to_lowercase();
        let name = query.name.to_lowercase();
        let query_text = query.query_text.to_lowercase();

        folder.contains("mapreduce")
            || folder.contains("map_reduce")
            || folder.contains("map reduce")
            || name.contains("mapreduce")
            || name.contains("map_reduce")
            || name.contains("map reduce")
            || query_text.contains(".mapreduce(")
            || query_text.contains("\"mapreduce\"")
            || query_text.contains("'mapreduce'")
            || query_text.contains("mapreduce:")
    }

    fn saved_query_matches_search(query: &SavedQueryInfo, search_lowercase: &str) -> bool {
        query.name.to_lowercase().contains(search_lowercase)
            || query
                .folder
                .as_deref()
                .map(|folder| folder.to_lowercase().contains(search_lowercase))
                .unwrap_or(false)
            || query.query_text.to_lowercase().contains(search_lowercase)
    }

    fn document_admin_matches_search(
        object: &zqlz_core::DocumentAdminObjectInfo,
        search_lowercase: &str,
    ) -> bool {
        search_lowercase.is_empty()
            || object.name.to_lowercase().contains(search_lowercase)
            || object.kind.to_lowercase().contains(search_lowercase)
            || object
                .unavailable_reason
                .as_deref()
                .map(|reason| reason.to_lowercase().contains(search_lowercase))
                .unwrap_or(false)
    }

    fn push_query_rows(
        rows: &mut Vec<SidebarVirtualRow>,
        connection_id: Uuid,
        queries: Vec<&SavedQueryInfo>,
        depth: usize,
        matched_leaf_rows: &mut usize,
    ) {
        let mut root_queries = Vec::new();
        let mut folder_queries: BTreeMap<String, Vec<&SavedQueryInfo>> = BTreeMap::new();

        for query in queries {
            match query
                .folder
                .as_deref()
                .map(str::trim)
                .filter(|folder| !folder.is_empty())
            {
                Some(folder) => folder_queries
                    .entry(folder.to_string())
                    .or_default()
                    .push(query),
                None => root_queries.push(query),
            }
        }

        for query in root_queries {
            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                element_id: format!("query-{connection_id}-{}", query.id),
                icon: SidebarRowIcon::Query,
                label: query.name.clone(),
                depth,
                kind: SidebarLeafKind::Query {
                    conn_id: connection_id,
                    query_id: query.id,
                    query_name: query.name.clone(),
                },
            }));
            *matched_leaf_rows += 1;
        }

        for (folder, queries) in folder_queries {
            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                element_id: format!("query-folder-{connection_id}-{folder}"),
                icon: SidebarRowIcon::Folder,
                label: folder.clone(),
                depth,
                kind: SidebarLeafKind::QueryFolder {
                    conn_id: connection_id,
                    folder,
                },
            }));

            for query in queries {
                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                    element_id: format!("query-{connection_id}-{}", query.id),
                    icon: SidebarRowIcon::Query,
                    label: query.name.clone(),
                    depth: depth + 1,
                    kind: SidebarLeafKind::Query {
                        conn_id: connection_id,
                        query_id: query.id,
                        query_name: query.name.clone(),
                    },
                }));
                *matched_leaf_rows += 1;
            }
        }
    }

    /// Whether a database node should render its sections directly instead of
    /// nesting them under schema folders.
    ///
    /// Drivers where the database *is* the namespace report no schema support,
    /// so grouping would render a folder that just repeats the database node.
    fn should_flatten_database_schema_group(driver_type: &str) -> bool {
        !SidebarObjectCapabilities::for_driver(driver_type).supports_schemas
    }

    /// Render one object section (header, loading placeholder, leaves) for
    /// any of the three tree scopes. This is the single place section rows
    /// are produced.
    #[allow(clippy::too_many_arguments)]
    fn append_section_rows(
        &self,
        connection: &ConnectionEntry,
        scope: &SectionScope<'_>,
        section: SidebarSection,
        objects: &[String],
        is_loading: bool,
        stored_expanded: bool,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        if !connection.object_capabilities.supports_section(section) {
            return;
        }

        let has_search = !self.search_query.is_empty();
        let filtered = self.filter_by_search(objects);
        // While searching, hide sections with no matches — unless a load is
        // still in flight, where hiding would look like "no results".
        if has_search && !is_loading && filtered.is_empty() {
            return;
        }

        let is_expanded = stored_expanded || (has_search && !filtered.is_empty());
        let conn_id = connection.id;
        let element_suffix = scope.element_suffix(conn_id);
        let element_prefix = Self::section_element_prefix(section);

        rows.push(SidebarVirtualRow::Section(SectionRow {
            element_id: format!("{element_prefix}-header-{element_suffix}"),
            icon: Self::section_icon(section),
            label: section.label().to_string(),
            total_count: objects.len(),
            filtered_count: filtered.len(),
            is_expanded,
            depth,
            action: scope.action(conn_id, section.as_key()),
            context_menu_section: Self::section_context_menu(section),
        }));

        if !is_expanded {
            return;
        }

        let leaf_depth = depth + 1;
        if is_loading {
            rows.push(SidebarVirtualRow::Loading(LoadingRow {
                element_id: format!("loading-{element_prefix}-{element_suffix}"),
                text: "Loading...".to_string(),
                depth: leaf_depth,
            }));
            return;
        }

        let leaf_prefix = Self::section_leaf_prefix(section);
        let database_name = scope.database_name().map(ToOwned::to_owned);
        let icon = Self::section_icon(section);
        for object_name in filtered {
            let object_schema = match scope {
                SectionScope::SchemaGroup { schema_name, .. } => Some((*schema_name).to_string()),
                SectionScope::Connection { current_schema, .. }
                | SectionScope::Database { current_schema, .. } => {
                    current_schema.map(ToOwned::to_owned)
                }
            };
            let table = TableRef::new(
                database_name.clone(),
                object_schema.clone(),
                object_name.clone(),
            );

            let kind = match section {
                SidebarSection::Tables => SidebarLeafKind::Table { conn_id, table },
                SidebarSection::Views => SidebarLeafKind::View {
                    conn_id,
                    view: table.view(),
                },
                SidebarSection::MaterializedViews => SidebarLeafKind::MaterializedView {
                    conn_id,
                    view: table.view(),
                },
                SidebarSection::Triggers => SidebarLeafKind::Trigger {
                    conn_id,
                    trigger_name: object_name.clone(),
                    object_schema: object_schema.clone(),
                },
                SidebarSection::Functions => SidebarLeafKind::Function {
                    conn_id,
                    function_name: object_name.clone(),
                    object_schema: object_schema.clone(),
                    database_name: database_name.clone(),
                },
                SidebarSection::Procedures => SidebarLeafKind::Procedure {
                    conn_id,
                    procedure_name: object_name.clone(),
                    object_schema: object_schema.clone(),
                    database_name: database_name.clone(),
                },
                SidebarSection::Events
                | SidebarSection::Sequences
                | SidebarSection::Domains
                | SidebarSection::Types
                | SidebarSection::Extensions => SidebarLeafKind::MetadataObject {
                    conn_id,
                    object_name: object_name.clone(),
                    object_schema: object_schema.clone(),
                    database_name: database_name.clone(),
                    object_type: section.as_key(),
                },
                SidebarSection::Queries | SidebarSection::RedisDatabases => continue,
            };

            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                element_id: format!("{leaf_prefix}-{element_suffix}-{object_name}"),
                icon,
                label: object_name.clone(),
                depth: leaf_depth,
                kind,
            }));
            *matched_leaf_rows += 1;

            if section == SidebarSection::Tables {
                self.append_table_detail_rows(
                    rows,
                    conn_id,
                    database_name.clone(),
                    object_schema,
                    object_name,
                    leaf_depth + 1,
                );
            }
        }
    }

    /// Render the saved-queries section; identical in every tree scope.
    fn append_queries_section(
        &self,
        connection: &ConnectionEntry,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query.is_empty();
        let filtered_queries: Vec<_> = connection
            .queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();
        if has_search && filtered_queries.is_empty() {
            return;
        }

        let queries_expanded =
            connection.queries_expanded || (has_search && !filtered_queries.is_empty());
        rows.push(SidebarVirtualRow::Section(SectionRow {
            element_id: format!("queries-header-{}", connection.id),
            icon: SidebarRowIcon::Query,
            label: "Queries".to_string(),
            total_count: connection.queries.len(),
            filtered_count: filtered_queries.len(),
            is_expanded: queries_expanded,
            depth,
            action: SidebarSectionAction::Queries {
                conn_id: connection.id,
            },
            context_menu_section: Some("queries"),
        }));

        if queries_expanded {
            Self::push_query_rows(
                rows,
                connection.id,
                filtered_queries,
                depth + 1,
                matched_leaf_rows,
            );
        }
    }

    fn section_icon(section: SidebarSection) -> SidebarRowIcon {
        match section {
            SidebarSection::Tables => SidebarRowIcon::Table,
            SidebarSection::Views => SidebarRowIcon::View,
            SidebarSection::MaterializedViews => SidebarRowIcon::MaterializedView,
            SidebarSection::Triggers => SidebarRowIcon::Trigger,
            SidebarSection::Functions => SidebarRowIcon::Function,
            SidebarSection::Procedures => SidebarRowIcon::Procedure,
            SidebarSection::Events => SidebarRowIcon::Event,
            SidebarSection::Sequences => SidebarRowIcon::Sequence,
            SidebarSection::Domains => SidebarRowIcon::Domain,
            SidebarSection::Types => SidebarRowIcon::Type,
            SidebarSection::Extensions => SidebarRowIcon::Extension,
            SidebarSection::Queries => SidebarRowIcon::Query,
            SidebarSection::RedisDatabases => SidebarRowIcon::Database,
        }
    }

    fn section_element_prefix(section: SidebarSection) -> &'static str {
        match section {
            SidebarSection::MaterializedViews => "matviews",
            other => other.as_key(),
        }
    }

    fn section_leaf_prefix(section: SidebarSection) -> &'static str {
        match section {
            SidebarSection::Tables => "table",
            SidebarSection::Views => "view",
            SidebarSection::MaterializedViews => "matview",
            SidebarSection::Triggers => "trigger",
            SidebarSection::Functions => "function",
            SidebarSection::Procedures => "procedure",
            SidebarSection::Events => "event",
            SidebarSection::Sequences => "sequence",
            SidebarSection::Domains => "domain",
            SidebarSection::Types => "type",
            SidebarSection::Extensions => "extension",
            SidebarSection::Queries => "query",
            SidebarSection::RedisDatabases => "redis-db",
        }
    }

    /// Sections without creation/design menus render no section context menu.
    fn section_context_menu(section: SidebarSection) -> Option<&'static str> {
        match section {
            SidebarSection::Sequences
            | SidebarSection::Domains
            | SidebarSection::Types
            | SidebarSection::Extensions => None,
            other => Some(other.as_key()),
        }
    }

    /// Render the schema-group folders and their sections for a grouped
    /// (multi-schema) database.
    #[allow(clippy::too_many_arguments)]
    fn append_grouped_objects_rows(
        &self,
        connection: &ConnectionEntry,
        database_name: &str,
        schema: &DatabaseSchemaData,
        groups: &[(String, SchemaSectionGroup)],
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query.is_empty();

        for (schema_name, group) in groups {
            let filtered_count = group
                .0
                .values()
                .map(|objects| self.filter_by_search(objects).len())
                .sum::<usize>();
            let schema_has_matches = self.matches_search(schema_name) || filtered_count > 0;

            if has_search && !schema_has_matches {
                continue;
            }

            let schema_is_expanded = schema.expanded_schema_groups.contains(schema_name)
                || (has_search && schema_has_matches);

            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!(
                    "schema-group-{}-{}",
                    connection.id,
                    schema_name.replace(' ', "-")
                ),
                icon: SidebarRowIcon::Folder,
                label: schema_name.clone(),
                total_count: group.total_count(),
                filtered_count,
                is_expanded: schema_is_expanded,
                depth,
                action: SidebarSectionAction::SchemaGroup {
                    conn_id: connection.id,
                    database_name: database_name.to_string(),
                    schema_name: schema_name.clone(),
                },
                context_menu_section: None,
            }));

            if !schema_is_expanded {
                continue;
            }

            let scope = SectionScope::SchemaGroup {
                database_name,
                schema_name,
            };
            for section in SidebarSection::OBJECT_SECTIONS {
                let section_key = format!("{schema_name}::{}", section.as_key());
                self.append_section_rows(
                    connection,
                    &scope,
                    section,
                    group.objects(section),
                    schema.loading_sections.contains(&section),
                    schema.expanded_schema_section_keys.contains(&section_key),
                    depth + 1,
                    rows,
                    matched_leaf_rows,
                );
            }
        }

        self.append_queries_section(connection, depth, rows, matched_leaf_rows);
    }

    #[allow(clippy::too_many_arguments)]
    fn append_table_detail_rows(
        &self,
        rows: &mut Vec<SidebarVirtualRow>,
        conn_id: Uuid,
        database_name: Option<String>,
        object_schema: Option<String>,
        table_name: &str,
        depth: usize,
    ) {
        let key = Self::table_key(
            conn_id,
            database_name,
            object_schema,
            table_name.to_string(),
        );
        if !self.expanded_table_keys.contains(&key) {
            return;
        }

        if self.loading_table_keys.contains(&key) {
            rows.push(SidebarVirtualRow::Loading(LoadingRow {
                element_id: format!("loading-table-details-{conn_id}-{table_name}"),
                text: "Loading metadata...".to_string(),
                depth,
            }));
            return;
        }

        let Some(details) = self.table_details.get(&key) else {
            return;
        };

        for (section, icon, count) in [
            ("Fields", SidebarRowIcon::Field, details.fields.len()),
            ("Indexes", SidebarRowIcon::Index, details.indexes.len()),
            (
                "Foreign Keys",
                SidebarRowIcon::ForeignKey,
                details.foreign_keys.len(),
            ),
            ("Uniques", SidebarRowIcon::Constraint, details.uniques.len()),
            ("Checks", SidebarRowIcon::Constraint, details.checks.len()),
            (
                "Excludes",
                SidebarRowIcon::Constraint,
                details.excludes.len(),
            ),
            ("Rules", SidebarRowIcon::Rule, 0),
            ("Triggers", SidebarRowIcon::Trigger, details.triggers.len()),
        ] {
            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                element_id: format!(
                    "table-detail-{}-{}-{}-{}",
                    conn_id,
                    key.schema_name.as_deref().unwrap_or_default(),
                    table_name,
                    section.to_ascii_lowercase().replace(' ', "-")
                ),
                icon,
                label: format!("{section} ({count})"),
                depth,
                kind: SidebarLeafKind::TableChildSummary,
            }));
        }
    }

    fn append_database_objects_rows(
        &self,
        connection: &ConnectionEntry,
        database: &SidebarDatabaseInfo,
        schema: &DatabaseSchemaData,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let scope = SectionScope::Database {
            database_name: &database.name,
            current_schema: schema.schema_name.as_deref(),
        };

        self.append_section_rows_for_scope(
            connection,
            &scope,
            schema,
            depth,
            rows,
            matched_leaf_rows,
        );
    }

    /// Emit every object section plus saved queries for one tree scope.
    fn append_section_rows_for_scope(
        &self,
        connection: &ConnectionEntry,
        scope: &SectionScope<'_>,
        schema: &DatabaseSchemaData,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        for section in SidebarSection::OBJECT_SECTIONS {
            self.append_section_rows(
                connection,
                scope,
                section,
                schema.objects(section),
                schema.loading_sections.contains(&section),
                schema.expanded_sections.contains(&section),
                depth,
                rows,
                matched_leaf_rows,
            );
        }

        self.append_queries_section(connection, depth, rows, matched_leaf_rows);
    }
    fn render_virtual_row(
        &mut self,
        row: SidebarVirtualRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match row {
            SidebarVirtualRow::Connection(row) => self.render_connection_virtual_row(row, cx),
            SidebarVirtualRow::Database(row) => self.render_database_virtual_row(row, cx),
            SidebarVirtualRow::SchemaNode(row) => self.render_schema_node_virtual_row(row, cx),
            SidebarVirtualRow::Section(row) => self.render_section_virtual_row(row, cx),
            SidebarVirtualRow::Leaf(row) => self.render_leaf_virtual_row(row, cx),
            SidebarVirtualRow::Loading(row) => self.render_loading_virtual_row(row, cx),
            SidebarVirtualRow::NoResults(row) => self.render_no_results_virtual_row(row, cx),
        }
    }

    fn render_connection_virtual_row(
        &self,
        row: ConnectionRow,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let ConnectionRow {
            conn_id,
            conn_name,
            db_type,
            is_connected,
            is_connecting,
        } = row;
        let theme = cx.theme();
        let row_background = Hsla::transparent_black();
        let row_selected_background = theme.sidebar_accent.opacity(0.18);
        let row_hover_background = theme.sidebar_accent.opacity(0.08);
        let subtle_action_border = theme.sidebar_border.opacity(0.5);
        let is_selected = self.selected_connection == Some(conn_id);
        let foreground = if is_selected {
            theme.sidebar_accent_foreground
        } else {
            theme.sidebar_foreground
        };
        let db_icon = self.db_icon_for_virtual_rows(&db_type);
        let db_logo = self.db_logo_for_virtual_rows(&db_type);
        let conn_id_for_row_click = conn_id;
        let conn_id_for_right_click = conn_id;
        let conn_id_for_new_query = conn_id;
        let conn_id_for_disconnect = conn_id;

        h_flex()
            .id(SharedString::from(format!("conn-{conn_id}")))
            .group("conn-row")
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .px_2()
            .gap_2()
            .items_center()
            .text_color(foreground)
            .bg(if is_selected {
                row_selected_background
            } else {
                row_background
            })
            .cursor_pointer()
            .hover(move |this| {
                if is_selected {
                    this.bg(row_selected_background)
                } else {
                    this.bg(row_hover_background)
                }
            })
            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                this.select_connection(conn_id_for_row_click, cx);
                if event.click_count() == 2 {
                    this.activate_selected_connection(cx);
                }
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    this.select_connection(conn_id_for_right_click, cx);
                    this.show_connection_context_menu(
                        conn_id_for_right_click,
                        event.position,
                        window,
                        cx,
                    );
                }),
            )
            .child(
                div()
                    .size_4()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when_some(db_logo, |this, logo| this.child(logo.small()))
                    .when(db_logo.is_none(), |this| {
                        this.child(Icon::new(db_icon).size_4())
                    }),
            )
            .child(body_small(conn_name).truncate().flex_1())
            .child(
                zqlz_ui::widgets::StatusDot::new()
                    .status(if is_connecting {
                        zqlz_ui::widgets::ConnectionStatus::Connecting
                    } else if is_connected {
                        zqlz_ui::widgets::ConnectionStatus::Connected
                    } else {
                        zqlz_ui::widgets::ConnectionStatus::Disconnected
                    })
                    .with_size(zqlz_ui::widgets::Size::XSmall)
                    .into_any_element(),
            )
            .child(
                h_flex()
                    .gap_1()
                    .when(is_connected, |this| {
                        this.child(
                            div()
                                .id(SharedString::from(format!("conn-new-query-{conn_id}")))
                                .size_4()
                                .border_1()
                                .border_color(subtle_action_border)
                                .flex()
                                .items_center()
                                .justify_center()
                                .cursor_pointer()
                                .hover(|el| el.bg(theme.sidebar_accent.opacity(0.1)))
                                .on_click(cx.listener(move |_this, _: &ClickEvent, _, cx| {
                                    cx.stop_propagation();
                                    cx.emit(ConnectionSidebarEvent::NewQuery(
                                        conn_id_for_new_query,
                                    ));
                                }))
                                .child(
                                    Icon::new(ZqlzIcon::FileSql)
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                ),
                        )
                        .child(
                            div()
                                .id(SharedString::from(format!("conn-disconnect-{conn_id}")))
                                .size_4()
                                .border_1()
                                .border_color(subtle_action_border)
                                .flex()
                                .items_center()
                                .justify_center()
                                .cursor_pointer()
                                .hover(|el| el.bg(theme.danger.opacity(0.1)))
                                .on_click(cx.listener(move |_this, _: &ClickEvent, _, cx| {
                                    cx.stop_propagation();
                                    cx.emit(ConnectionSidebarEvent::Disconnect(
                                        conn_id_for_disconnect,
                                    ));
                                }))
                                .child(
                                    Icon::new(ZqlzIcon::X)
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                ),
                        )
                    })
                    .when(is_connecting, |this| {
                        this.child(
                            div()
                                .h_full()
                                .px_2()
                                .border_1()
                                .border_color(subtle_action_border)
                                .flex()
                                .items_center()
                                .bg(theme.primary.opacity(0.6))
                                .child(body_small("...").color(theme.primary_foreground)),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_database_virtual_row(&self, row: DatabaseRow, cx: &mut Context<Self>) -> AnyElement {
        let DatabaseRow {
            conn_id,
            database_name,
            is_expanded,
            has_schema,
            is_active,
            size_label,
        } = row;
        let database_name_for_click = database_name.clone();
        let theme = cx.theme();
        let row_hover_background = theme.sidebar_accent.opacity(0.08);
        h_flex()
            .id(SharedString::from(format!(
                "db-node-{conn_id}-{database_name}"
            )))
            .debug_selector(|| format!("database:{database_name}"))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(Self::sidebar_indent(1))
            .pr_2()
            .gap_2()
            .items_center()
            .text_xs()
            .text_color(if is_expanded || is_active {
                theme.muted_foreground
            } else {
                theme.muted_foreground.opacity(0.5)
            })
            .cursor_pointer()
            .hover(move |this| this.bg(row_hover_background))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                this.toggle_database_expand(conn_id, &database_name_for_click, cx);
            }))
            .child(
                Icon::new(if is_expanded {
                    IconName::ChevronDown
                } else {
                    IconName::ChevronRight
                })
                .size_3(),
            )
            .child(
                Icon::new(ZqlzIcon::Database)
                    .size_3()
                    .when(!has_schema && !is_expanded && !is_active, |this| {
                        this.text_color(theme.muted_foreground.opacity(0.5))
                    }),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(database_name),
            )
            .when_some(size_label, |this, size_label| {
                this.child(body_small(size_label).color(theme.muted_foreground.opacity(0.4)))
            })
            .into_any_element()
    }

    fn render_schema_node_virtual_row(
        &self,
        row: SchemaNodeRow,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let row_hover_background = theme.sidebar_accent.opacity(0.08);
        h_flex()
            .id(SharedString::from(format!(
                "schema-node-{}-{}",
                row.conn_id, row.database_name
            )))
            .debug_selector(|| format!("schema-node:{}", row.schema_name))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(Self::sidebar_indent(2))
            .pr_2()
            .gap_2()
            .items_center()
            .text_xs()
            .text_color(if row.is_expanded {
                theme.muted_foreground
            } else {
                theme.muted_foreground.opacity(0.5)
            })
            .cursor_pointer()
            .hover(move |this| this.bg(row_hover_background))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                this.toggle_db_section(row.conn_id, &row.database_name, "schema", cx);
            }))
            .child(
                Icon::new(if row.is_expanded {
                    IconName::ChevronDown
                } else {
                    IconName::ChevronRight
                })
                .size_3(),
            )
            .child(Icon::new(IconName::Folder).size_3())
            .child(row.schema_name)
            .into_any_element()
    }

    fn render_section_virtual_row(&self, row: SectionRow, cx: &mut Context<Self>) -> AnyElement {
        let SectionRow {
            element_id,
            icon: row_icon,
            label,
            total_count,
            filtered_count,
            is_expanded,
            depth,
            action,
            context_menu_section,
        } = row;
        let action_for_click = action.clone();
        let action_for_context = action;
        let root_query_drop_connection = match &action_for_click {
            SidebarSectionAction::Queries { conn_id } => Some(*conn_id),
            _ => None,
        };
        let theme = cx.theme();
        let has_search = !self.search_query.is_empty();
        let indent = Self::sidebar_indent(depth);
        let row_hover_background = theme.sidebar_accent.opacity(0.08);
        let text_color = if is_expanded {
            theme.muted_foreground
        } else {
            theme.muted_foreground.opacity(0.65)
        };
        let icon = self.sidebar_row_icon(&row_icon, theme.muted_foreground);

        h_flex()
            .id(SharedString::from(element_id))
            .debug_selector(|| format!("section:{label}"))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(indent)
            .pr_2()
            .gap_2()
            .items_center()
            .text_xs()
            .text_color(text_color)
            .cursor_pointer()
            .hover(move |this| this.bg(row_hover_background))
            .on_click(
                cx.listener(move |this, _: &ClickEvent, _, cx| match &action_for_click {
                    SidebarSectionAction::RedisDatabases { conn_id } => {
                        this.toggle_redis_databases_expand(*conn_id, cx)
                    }
                    SidebarSectionAction::Queries { conn_id } => {
                        this.toggle_queries_expand(*conn_id, cx)
                    }
                    SidebarSectionAction::ConnectionSection { conn_id, section } => {
                        if let Some(section) = SidebarSection::from_key(section) {
                            this.toggle_section_expand(*conn_id, None, section, cx);
                        }
                    }
                    SidebarSectionAction::DatabaseSection {
                        conn_id,
                        database_name,
                        section,
                    } => this.toggle_db_section(*conn_id, database_name, section, cx),
                    SidebarSectionAction::SchemaGroup {
                        conn_id,
                        database_name,
                        schema_name,
                    } => this.toggle_schema_group_expand(
                        *conn_id,
                        Some(database_name),
                        schema_name,
                        cx,
                    ),
                    SidebarSectionAction::SchemaGroupSection {
                        conn_id,
                        database_name,
                        schema_name,
                        section,
                    } => this.toggle_schema_group_section_expand(
                        *conn_id,
                        Some(database_name),
                        schema_name,
                        section,
                        cx,
                    ),
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    let (conn_id, database_name) = match &action_for_context {
                        SidebarSectionAction::RedisDatabases { conn_id }
                        | SidebarSectionAction::Queries { conn_id }
                        | SidebarSectionAction::ConnectionSection { conn_id, .. } => {
                            (*conn_id, None)
                        }
                        SidebarSectionAction::DatabaseSection {
                            conn_id,
                            database_name,
                            ..
                        }
                        | SidebarSectionAction::SchemaGroup {
                            conn_id,
                            database_name,
                            ..
                        }
                        | SidebarSectionAction::SchemaGroupSection {
                            conn_id,
                            database_name,
                            ..
                        } => (*conn_id, Some(database_name.clone())),
                    };

                    if let Some(section) = context_menu_section {
                        cx.stop_propagation();
                        this.show_section_context_menu(
                            conn_id,
                            database_name,
                            section,
                            event.position,
                            window,
                            cx,
                        );
                    }
                }),
            )
            .when_some(root_query_drop_connection, |this, conn_id| {
                this.drag_over::<DragSavedQuery>(move |this, drag, _, cx| {
                    if drag.connection_id == conn_id {
                        this.bg(cx.theme().drop_target)
                    } else {
                        this
                    }
                })
                .on_drop(cx.listener(
                    move |_this, drag: &DragSavedQuery, _window, cx| {
                        if drag.connection_id == conn_id {
                            cx.emit(ConnectionSidebarEvent::MoveSavedQueryToFolder {
                                connection_id: conn_id,
                                query_id: drag.query_id,
                                query_name: drag.query_name.to_string(),
                                folder: None,
                            });
                        }
                    },
                ))
            })
            .child(
                Icon::new(if is_expanded {
                    IconName::ChevronDown
                } else {
                    IconName::ChevronRight
                })
                .size_3(),
            )
            .child(icon)
            .child(if has_search {
                format!("{} ({}/{})", label, filtered_count, total_count)
            } else {
                format!("{} ({})", label, total_count)
            })
            .into_any_element()
    }

    fn render_leaf_virtual_row(&self, row: LeafRow, cx: &mut Context<Self>) -> AnyElement {
        let LeafRow {
            element_id,
            icon: row_icon,
            label,
            depth,
            kind,
        } = row;
        let kind_for_click = kind.clone();
        let kind_for_context = kind;
        let table_toggle = if let SidebarLeafKind::Table { conn_id, table, .. } = &kind_for_click
        {
            let key = Self::table_key(
                *conn_id,
                table.database.clone(),
                table.schema.clone(),
                table.name.clone(),
            );
            Some((
                *conn_id,
                table.database.clone(),
                table.schema.clone(),
                table.name.clone(),
                self.expanded_table_keys.contains(&key),
            ))
        } else {
            None
        };
        let theme = cx.theme();
        let indent = Self::sidebar_indent(depth);
        let is_active = self.is_leaf_item_active(&element_id);
        let active_background = theme.sidebar_accent.opacity(0.14);
        let hover_background = theme.sidebar_accent.opacity(0.08);
        let text_color = if is_active {
            theme.sidebar_accent_foreground
        } else {
            theme.muted_foreground
        };
        let icon = self.sidebar_row_icon(&row_icon, theme.muted_foreground);
        let element_id_for_click = element_id.clone();
        let query_drag = match &kind_for_click {
            SidebarLeafKind::Query {
                conn_id,
                query_id,
                query_name,
            } => Some(DragSavedQuery {
                connection_id: *conn_id,
                query_id: *query_id,
                query_name: SharedString::from(query_name.clone()),
            }),
            _ => None,
        };
        let query_folder_drop = match &kind_for_click {
            SidebarLeafKind::QueryFolder { conn_id, folder } => Some((*conn_id, folder.clone())),
            _ => None,
        };

        h_flex()
            .id(SharedString::from(element_id))
            .debug_selector(|| format!("leaf:{label}"))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(indent)
            .pr_2()
            .gap_2()
            .items_center()
            .text_sm()
            .text_color(text_color)
            .when(is_active, |this| this.bg(active_background))
            .cursor_pointer()
            .hover(move |this| {
                if is_active {
                    this.bg(active_background)
                } else {
                    this.bg(hover_background)
                }
            })
            .when_some(table_toggle, |this, toggle| {
                let (conn_id, database_name, object_schema, table_name, is_expanded) = toggle;
                this.child(
                    div()
                        .id(SharedString::from(format!(
                            "table-toggle-{conn_id}-{table_name}"
                        )))
                        .size_3()
                        .flex()
                        .items_center()
                        .justify_center()
                        .cursor_pointer()
                        .child(
                            Icon::new(if is_expanded {
                                IconName::ChevronDown
                            } else {
                                IconName::ChevronRight
                            })
                            .size_3(),
                        )
                        .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                            cx.stop_propagation();
                            this.toggle_table_details(
                                conn_id,
                                database_name.clone(),
                                object_schema.clone(),
                                table_name.clone(),
                                cx,
                            );
                        })),
                )
            })
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                this.set_active_leaf_item(Some(element_id_for_click.clone()), cx);
                match &kind_for_click {
                    SidebarLeafKind::Table { conn_id, table } => {
                        cx.emit(ConnectionSidebarEvent::OpenTable {
                            connection_id: *conn_id,
                            table: table.clone(),
                        })
                    }
                    SidebarLeafKind::View { conn_id, view }
                    | SidebarLeafKind::MaterializedView { conn_id, view } => {
                        cx.emit(ConnectionSidebarEvent::OpenView {
                            connection_id: *conn_id,
                            view: view.clone(),
                        })
                    }
                    SidebarLeafKind::Trigger {
                        conn_id,
                        trigger_name,
                        object_schema,
                    } => cx.emit(ConnectionSidebarEvent::DesignTrigger {
                        connection_id: *conn_id,
                        trigger_name: trigger_name.clone(),
                        object_schema: object_schema.clone(),
                    }),
                    SidebarLeafKind::Function {
                        conn_id,
                        function_name,
                        object_schema,
                        database_name,
                    } => cx.emit(ConnectionSidebarEvent::OpenFunction {
                        connection_id: *conn_id,
                        function_name: function_name.clone(),
                        object_schema: object_schema.clone(),
                        database_name: database_name.clone(),
                    }),
                    SidebarLeafKind::Procedure {
                        conn_id,
                        procedure_name,
                        object_schema,
                        database_name,
                    } => cx.emit(ConnectionSidebarEvent::OpenProcedure {
                        connection_id: *conn_id,
                        procedure_name: procedure_name.clone(),
                        object_schema: object_schema.clone(),
                        database_name: database_name.clone(),
                    }),
                    SidebarLeafKind::MetadataObject {
                        conn_id,
                        object_name,
                        object_schema,
                        database_name,
                        object_type,
                    } => cx.emit(ConnectionSidebarEvent::OpenGenericObjectDefinition {
                        connection_id: *conn_id,
                        object_ref: ObjectsPanelObjectRef::new(
                            Self::normalized_manifest_object_type(object_type),
                            object_name.clone(),
                        )
                        .with_database_option(database_name.clone())
                        .with_schema_option(object_schema.clone()),
                    }),
                    SidebarLeafKind::TableChildSummary => {}
                    SidebarLeafKind::Query {
                        conn_id,
                        query_id,
                        query_name,
                    } => cx.emit(ConnectionSidebarEvent::OpenSavedQuery {
                        connection_id: *conn_id,
                        query_id: *query_id,
                        query_name: query_name.clone(),
                    }),
                    SidebarLeafKind::QueryFolder { .. } => {}
                    SidebarLeafKind::RedisDatabase {
                        conn_id,
                        database_index,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenRedisDatabase {
                        connection_id: *conn_id,
                        database_index: *database_index,
                    }),
                    SidebarLeafKind::DocumentCollection {
                        conn_id,
                        database_name,
                        collection_name,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenDocumentCollection {
                        connection_id: *conn_id,
                        database_name: database_name.clone(),
                        collection_name: collection_name.clone(),
                    }),
                }
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    match &kind_for_context {
                        SidebarLeafKind::Table { conn_id, table } => this.show_table_context_menu(
                            *conn_id,
                            table.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::View { conn_id, view } => this.show_view_context_menu(
                            *conn_id,
                            view.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::MaterializedView { conn_id, view } => this
                            .show_materialized_view_context_menu(
                                *conn_id,
                                view.clone(),
                                event.position,
                                window,
                                cx,
                            ),
                        SidebarLeafKind::Trigger {
                            conn_id,
                            trigger_name,
                            object_schema,
                        } => this.show_trigger_context_menu(
                            *conn_id,
                            trigger_name.clone(),
                            object_schema.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::Function {
                            conn_id,
                            function_name,
                            object_schema,
                            ..
                        } => this.show_function_context_menu(
                            *conn_id,
                            function_name.clone(),
                            object_schema.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::Procedure {
                            conn_id,
                            procedure_name,
                            object_schema,
                            ..
                        } => this.show_procedure_context_menu(
                            *conn_id,
                            procedure_name.clone(),
                            object_schema.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::MetadataObject {
                            conn_id,
                            object_name,
                            object_schema,
                            database_name,
                            object_type,
                        } => this.show_metadata_object_context_menu(
                            *conn_id,
                            object_name.clone(),
                            object_schema.clone(),
                            database_name.clone(),
                            object_type.to_string(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::TableChildSummary => {}
                        SidebarLeafKind::Query {
                            conn_id,
                            query_id,
                            query_name,
                        } => this.show_query_context_menu(
                            *conn_id,
                            *query_id,
                            query_name.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::QueryFolder { .. } => {}
                        SidebarLeafKind::RedisDatabase {
                            conn_id,
                            database_index,
                            ..
                        } => this.show_redis_db_context_menu(
                            *conn_id,
                            *database_index,
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::DocumentCollection {
                            conn_id,
                            database_name,
                            collection_name,
                            object_type,
                        } => this.show_metadata_object_context_menu(
                            *conn_id,
                            collection_name.clone(),
                            None,
                            Some(database_name.clone()),
                            object_type.to_string(),
                            event.position,
                            window,
                            cx,
                        ),
                    }
                }),
            )
            .when_some(query_drag, |this, drag| {
                this.on_drag(drag, |drag, _, _, cx| {
                    cx.stop_propagation();
                    cx.new(|_| drag.clone())
                })
            })
            .when_some(query_folder_drop, |this, (conn_id, folder)| {
                let folder_for_drop = folder.clone();
                this.drag_over::<DragSavedQuery>(move |this, drag, _, cx| {
                    if drag.connection_id == conn_id {
                        this.bg(cx.theme().drop_target)
                    } else {
                        this
                    }
                })
                .on_drop(cx.listener(
                    move |_this, drag: &DragSavedQuery, _window, cx| {
                        if drag.connection_id == conn_id {
                            cx.emit(ConnectionSidebarEvent::MoveSavedQueryToFolder {
                                connection_id: conn_id,
                                query_id: drag.query_id,
                                query_name: drag.query_name.to_string(),
                                folder: Some(folder_for_drop.clone()),
                            });
                        }
                    },
                ))
            })
            .child(icon)
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .when(is_active, |this| this.font_weight(FontWeight::SEMIBOLD))
                    .child(label),
            )
            .into_any_element()
    }

    fn render_loading_virtual_row(&self, row: LoadingRow, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let indent = Self::sidebar_indent(row.depth);
        h_flex()
            .id(SharedString::from(row.element_id))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(indent)
            .pr_2()
            .items_center()
            .text_xs()
            .text_color(theme.muted_foreground.opacity(0.6))
            .child(row.text)
            .into_any_element()
    }

    fn render_no_results_virtual_row(
        &self,
        row: NoResultsRow,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .px_2()
            .items_center()
            .text_sm()
            .text_color(theme.muted_foreground.opacity(0.75))
            .child(format!("No objects match \"{}\"", row.query))
            .into_any_element()
    }
}

impl Render for ConnectionSidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let has_connections = !self.connections.is_empty();
        // Ensure search input is initialized when we have connections
        if has_connections {
            self.ensure_search_input(window, cx);
        }

        // Clone the search input state for use in closures
        let search_input_state = self.search_input_state.clone();

        if self.virtual_rows_dirty {
            self.rebuild_virtual_rows();
            self.virtual_rows_dirty = false;
        }
        let row_count = self.virtual_rows.len();

        if has_connections && row_count == 0 {
            tracing::warn!(
                connections = self.connections.len(),
                query = %self.search_query,
                "Connection sidebar has connections but no virtual rows"
            );
        }

        // Now get theme after all &mut self operations are done
        let theme = cx.theme();

        v_flex()
            .id("connection-sidebar")
            .key_context("ConnectionSidebar")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &ActivateConnection, _, cx| {
                this.activate_selected_connection(cx);
            }))
            .on_action(cx.listener(|this, _: &DeleteSelectedConnection, _, cx| {
                this.delete_selected_connection(cx);
            }))
            .on_action(cx.listener(|this, _: &ShowContextMenu, window, cx| {
                this.show_selected_context_menu(window, cx);
            }))
            .size_full()
            .bg(theme.sidebar)
            .border_r_1()
            .border_color(theme.sidebar_border)
            .font_family(cx.theme().font_family.clone())
            // Search input - only show when there are connections
            .when_some(search_input_state, |this, input_state| {
                this.child(
                    div()
                        .w_full()
                        .px_2()
                        .pt_2()
                        .pb_1()
                        .child(
                            Input::new(&input_state)
                                .small()
                                .w_full()
                                .cleanable(true)
                                .prefix(
                                    Icon::new(IconName::Search)
                                        .size_3()
                                        .text_color(theme.muted_foreground),
                                ),
                        ),
                )
            })
            .child(
                div()
                    .id("connection-list")
                    .flex_1()
                    .w_full()
                    .relative()
                    .overflow_hidden()
                    .drag_over::<ExternalPaths>(|this, _, _, cx| this.bg(cx.theme().drop_target))
                    .on_drop(cx.listener(|this, paths: &ExternalPaths, _, cx| {
                        this.import_dropped_paths(paths.paths(), cx);
                        cx.stop_propagation();
                    }))
                    .on_mouse_down(
                        gpui::MouseButton::Right,
                        cx.listener(|this, event: &MouseDownEvent, window, cx| {
                            this.show_sidebar_context_menu(event.position, window, cx);
                        }),
                    )
                    .when(!has_connections, |this| {
                        this.child(
                            v_flex()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .gap_4()
                                .p_4()
                                .child(
                                    body_small("No connections yet")
                                        .color(theme.muted_foreground)
                                        .text_center(),
                                )
                                .child(
                                    Button::new("add-first-connection")
                                        .primary()
                                        .small()
                                        .label("Add Connection")
                                        .on_click(cx.listener(|_this, _, _, cx| {
                                            cx.emit(ConnectionSidebarEvent::AddConnection);
                                        })),
                                ),
                        )
                    })
                    .when(has_connections, |this| {
                        if row_count == 0 {
                            this.child(
                                v_flex()
                                    .size_full()
                                    .justify_center()
                                    .items_center()
                                    .px_3()
                                    .child(
                                        body_small("Unable to render connection list")
                                            .color(theme.muted_foreground),
                                    ),
                            )
                        } else {
                            this.child(
                                uniform_list(
                                    "connection-sidebar-rows",
                                    row_count,
                                    cx.processor(
                                        move |sidebar, visible_range: Range<usize>, window, cx| {
                                            let total_rows = sidebar.virtual_rows.len();
                                            let start = visible_range.start.min(total_rows);
                                            let end = visible_range.end.min(total_rows);

                                            if visible_range.end > total_rows {
                                                tracing::debug!(
                                                    ?visible_range,
                                                    total_rows,
                                                    "Sidebar virtual list visible range exceeded available rows"
                                                );
                                            }

                                            sidebar.rendered_rows_len = end.saturating_sub(start);

                                            (start..end)
                                                .map(|index| {
                                                    sidebar.render_virtual_row(
                                                        sidebar.virtual_rows[index].clone(),
                                                        window,
                                                        cx,
                                                    )
                                                })
                                                .collect::<Vec<_>>()
                                        },
                                    ),
                                )
                                .flex_grow_1()
                                .size_full()
                                .pl_2()
                                .track_scroll(&self.scroll_handle)
                                .with_sizing_behavior(ListSizingBehavior::Auto)
                                .into_any_element(),
                            )
                        }
                    })
                    .when(has_connections && row_count > 0, |this| {
                        this.child(
                            div()
                                .absolute()
                                .top_0()
                                .right_0()
                                .bottom_0()
                                .w(px(16.0))
                                .child(Scrollbar::vertical(&self.scroll_handle)),
                        )
                    }),
            )
            .children(self.sidebar_context_menu.clone())
            .children(self.connection_context_menu.clone())
            .children(self.table_context_menu.clone())
            .children(self.view_context_menu.clone())
            .children(self.query_context_menu.clone())
            .children(self.function_context_menu.clone())
            .children(self.procedure_context_menu.clone())
            .children(self.trigger_context_menu.clone())
            .children(self.section_context_menu.clone())
            .children(self.materialized_view_context_menu.clone())
            .children(self.redis_db_context_menu.clone())
            .children(self.metadata_object_context_menu.clone())
    }
}

impl Focusable for ConnectionSidebar {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ConnectionSidebar {}
impl EventEmitter<ConnectionSidebarEvent> for ConnectionSidebar {}

impl Panel for ConnectionSidebar {
    fn panel_name(&self) -> &'static str {
        "ConnectionSidebar"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Connections"
    }

    fn title_suffix(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        Some(
            h_flex()
                .gap_1()
                .child(
                    Button::new("add-connection")
                        .ghost()
                        .xsmall()
                        .icon(IconName::Plus)
                        .tooltip("Add Connection")
                        .on_click(cx.listener(|_this, _, _, cx| {
                            cx.emit(ConnectionSidebarEvent::AddConnection);
                        })),
                )
                .child(
                    Button::new("refresh-connections")
                        .ghost()
                        .xsmall()
                        .icon(ZqlzIcon::ArrowsClockwise)
                        .tooltip("Refresh Connections")
                        .on_click(cx.listener(|_this, _, _, cx| {
                            cx.emit(ConnectionSidebarEvent::RefreshConnections);
                        })),
                ),
        )
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionSidebar;
    use crate::widgets::sidebar::types::{
        SavedQueryInfo, SidebarDatabaseInfo, SidebarSection,
    };
    use uuid::Uuid;
    use zqlz_core::{DocumentAdminObjectInfo, DocumentCollectionInfo};

    #[test]
    fn sidebar_groups_postgres_metadata_sections_by_schema() {
        let schema_names = vec!["audit".to_string(), "public".to_string()];
        let tables = vec!["audit.events".to_string()];
        let events = vec!["audit.nightly_rollup".to_string()];
        let sequences = vec!["audit.audit_id_seq".to_string()];
        let domains = vec!["public.email_address".to_string()];
        let types = vec!["audit.audit_event".to_string()];
        let extensions = vec!["postgis".to_string()];
        let groups = ConnectionSidebar::group_schema_sections_for_rows(
            &[
                (SidebarSection::Tables, tables.as_slice()),
                (SidebarSection::Events, events.as_slice()),
                (SidebarSection::Sequences, sequences.as_slice()),
                (SidebarSection::Domains, domains.as_slice()),
                (SidebarSection::Types, types.as_slice()),
                (SidebarSection::Extensions, extensions.as_slice()),
            ],
            &schema_names,
            Some("public"),
        )
        .expect("schema names should force grouped sidebar rows");

        let audit = groups
            .iter()
            .find(|(schema_name, _)| schema_name == "audit")
            .map(|(_, group)| group)
            .expect("audit schema should be grouped");
        assert_eq!(audit.objects(SidebarSection::Tables), ["events".to_string()]);
        assert_eq!(
            audit.objects(SidebarSection::Events),
            ["nightly_rollup".to_string()]
        );
        assert_eq!(
            audit.objects(SidebarSection::Sequences),
            ["audit_id_seq".to_string()]
        );
        assert_eq!(
            audit.objects(SidebarSection::Types),
            ["audit_event".to_string()]
        );

        let public = groups
            .iter()
            .find(|(schema_name, _)| schema_name == "public")
            .map(|(_, group)| group)
            .expect("public schema should be grouped");
        assert_eq!(
            public.objects(SidebarSection::Domains),
            ["email_address".to_string()]
        );
        assert_eq!(
            public.objects(SidebarSection::Extensions),
            ["postgis".to_string()]
        );
    }

    #[test]
    fn sidebar_does_not_split_sqlite_table_names_that_contain_dots() {
        let schema_names = vec!["main".to_string()];
        let tables = vec!["public.activity_communications".to_string()];
        let groups = ConnectionSidebar::group_schema_sections_for_rows(
            &[(SidebarSection::Tables, tables.as_slice())],
            &schema_names,
            Some("main"),
        )
        .expect("schema names should force grouped sidebar rows");

        assert!(
            groups
                .iter()
                .all(|(schema_name, _)| schema_name != "public")
        );
        let main = groups
            .iter()
            .find(|(schema_name, _)| schema_name == "main")
            .map(|(_, group)| group)
            .expect("main schema should be grouped");
        assert_eq!(
            main.objects(SidebarSection::Tables),
            ["public.activity_communications".to_string()]
        );
    }

    #[test]
    fn drivers_without_a_schema_level_render_database_sections_flat() {
        // The reported bug: MySQL's `art-of-protest` grew a schema folder that
        // just repeated the database node. Flattening now follows the driver's
        // namespace model rather than a name allowlist, so single-namespace
        // drivers never nest.
        for driver in ["mysql", "mariadb", "sqlite", "turso"] {
            assert!(
                ConnectionSidebar::should_flatten_database_schema_group(driver),
                "{driver} has no schema level and must render sections flat"
            );
        }

        assert!(!ConnectionSidebar::should_flatten_database_schema_group(
            "postgres"
        ));
    }

    #[test]
    fn schema_support_matches_driver_namespace_model() {
        use crate::widgets::sidebar::types::SidebarObjectCapabilities;

        // Only drivers this crate always compiles in are asserted; optional
        // drivers are absent from the registry in a default test build.
        assert!(SidebarObjectCapabilities::for_driver("postgres").supports_schemas);
        assert!(SidebarObjectCapabilities::for_driver("postgresql").supports_schemas);
        assert!(!SidebarObjectCapabilities::for_driver("mysql").supports_schemas);
        assert!(!SidebarObjectCapabilities::for_driver("mariadb").supports_schemas);
        assert!(!SidebarObjectCapabilities::for_driver("sqlite").supports_schemas);
        assert!(!SidebarObjectCapabilities::for_driver("turso").supports_schemas);
    }

    #[test]
    fn mongodb_saved_query_classifier_splits_pipelines_and_map_reduce_assets() {
        let aggregation = SavedQueryInfo {
            id: Uuid::new_v4(),
            name: "daily revenue".to_string(),
            query_text: "db.orders.aggregate([{ $match: { status: 'paid' } }])".to_string(),
            folder: None,
        };
        let map_reduce = SavedQueryInfo {
            id: Uuid::new_v4(),
            name: "legacy rollup".to_string(),
            query_text: "db.orders.mapReduce(map, reduce, { out: 'rollup' })".to_string(),
            folder: None,
        };
        let regular = SavedQueryInfo {
            id: Uuid::new_v4(),
            name: "open orders".to_string(),
            query_text: "db.orders.find({ status: 'open' })".to_string(),
            folder: None,
        };
        let aggregation_command = SavedQueryInfo {
            id: Uuid::new_v4(),
            name: "paid totals".to_string(),
            query_text: r#"{ "aggregate": "orders", "pipeline": [{ "$match": { "status": "paid" } }], "cursor": {} }"#.to_string(),
            folder: None,
        };
        let map_reduce_command = SavedQueryInfo {
            id: Uuid::new_v4(),
            name: "legacy command".to_string(),
            query_text: r#"{ "mapReduce": "orders", "map": "function() {}", "reduce": "function() {}", "out": "order_rollup" }"#.to_string(),
            folder: None,
        };

        assert!(ConnectionSidebar::is_mongodb_aggregation_asset(
            &aggregation
        ));
        assert!(ConnectionSidebar::is_mongodb_map_reduce_asset(&map_reduce));
        assert!(ConnectionSidebar::is_mongodb_aggregation_asset(
            &aggregation_command
        ));
        assert!(ConnectionSidebar::is_mongodb_map_reduce_asset(
            &map_reduce_command
        ));
        assert!(!ConnectionSidebar::is_mongodb_aggregation_asset(&regular));
        assert!(!ConnectionSidebar::is_mongodb_map_reduce_asset(&regular));
    }

    #[test]
    fn mongodb_regular_collection_predicate_excludes_special_collection_sections() {
        fn collection(name: &str, collection_type: &str) -> DocumentCollectionInfo {
            DocumentCollectionInfo {
                database: "app".to_string(),
                name: name.to_string(),
                collection_type: collection_type.to_string(),
                document_count: None,
                size_bytes: None,
                index_count: None,
                options_json: None,
                validator_json: None,
                collation_json: None,
                view_on: None,
                pipeline_json: None,
                timeseries_json: None,
                clustered_index_json: None,
                change_stream_pre_and_post_images: None,
            }
        }

        let normal = collection("orders", "collection");
        let view = collection("orders_view", "view");
        let timeseries = collection("events", "timeseries");
        let mut capped = collection("auditLogs", "collection");
        capped.options_json = Some(serde_json::json!({ "capped": true }));
        let mut clustered = collection("clusteredEvents", "collection");
        clustered.clustered_index_json = Some(serde_json::json!({ "key": { "_id": 1 } }));
        let system = collection("system.views", "collection");

        assert!(ConnectionSidebar::is_regular_mongodb_collection(&normal));
        assert!(!ConnectionSidebar::is_regular_mongodb_collection(&view));
        assert!(!ConnectionSidebar::is_regular_mongodb_collection(
            &timeseries
        ));
        assert!(!ConnectionSidebar::is_regular_mongodb_collection(&capped));
        assert!(!ConnectionSidebar::is_regular_mongodb_collection(
            &clustered
        ));
        assert!(!ConnectionSidebar::is_regular_mongodb_collection(&system));
    }

    #[test]
    fn mongodb_admin_metadata_search_matches_server_sharding_and_unavailable_rows() {
        let sharded_collection = DocumentAdminObjectInfo {
            database: "zqlz_feature_lab".to_string(),
            kind: "sharded_collection".to_string(),
            name: "zqlz_feature_lab.orders".to_string(),
            details_json: serde_json::Value::Null,
            unavailable_reason: None,
        };
        let unavailable_search = DocumentAdminObjectInfo {
            database: "zqlz_feature_lab".to_string(),
            kind: "search_index".to_string(),
            name: "$listSearchIndexes".to_string(),
            details_json: serde_json::Value::Null,
            unavailable_reason: Some("command is not supported".to_string()),
        };

        assert!(ConnectionSidebar::document_admin_matches_search(
            &sharded_collection,
            "orders"
        ));
        assert!(ConnectionSidebar::document_admin_matches_search(
            &sharded_collection,
            "sharded"
        ));
        assert!(ConnectionSidebar::document_admin_matches_search(
            &unavailable_search,
            "not supported"
        ));
        assert!(!ConnectionSidebar::document_admin_matches_search(
            &unavailable_search,
            "warehouse"
        ));
    }

    #[test]
    fn unloaded_expanded_database_without_loading_does_not_render_loading_row() {
        let database = SidebarDatabaseInfo {
            name: "postgres".to_string(),
            size_bytes: None,
            is_active: false,
            is_expanded: true,
            is_loading: false,
            schema: None,
            collections: Vec::new(),
            indexes: Vec::new(),
            functions: Vec::new(),
            gridfs_buckets: Vec::new(),
            users: Vec::new(),
            roles: Vec::new(),
            search_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            server: Vec::new(),
            sharding: Vec::new(),
            collections_expanded: false,
            collections_loading: false,
            document_expanded_sections: std::collections::HashSet::new(),
        };

        assert!(!ConnectionSidebar::should_render_database_loading(
            &database
        ));
        assert!(ConnectionSidebar::should_skip_unloaded_database(&database));
    }

    #[test]
    fn loading_database_renders_loading_row() {
        let database = SidebarDatabaseInfo {
            name: "postgres".to_string(),
            size_bytes: None,
            is_active: false,
            is_expanded: true,
            is_loading: true,
            schema: None,
            collections: Vec::new(),
            indexes: Vec::new(),
            functions: Vec::new(),
            gridfs_buckets: Vec::new(),
            users: Vec::new(),
            roles: Vec::new(),
            search_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            server: Vec::new(),
            sharding: Vec::new(),
            collections_expanded: false,
            collections_loading: false,
            document_expanded_sections: std::collections::HashSet::new(),
        };

        assert!(ConnectionSidebar::should_render_database_loading(&database));
        assert!(!ConnectionSidebar::should_skip_unloaded_database(&database));
    }

    mod golden {
        //! Golden tests pinning the exact row sequence emitted by
        //! `rebuild_virtual_rows` for representative connection shapes, so the
        //! tree-building refactor can prove it preserves (or deliberately
        //! changes) rendering behavior.

        use super::super::*;
        use crate::widgets::sidebar::types::ConnectionEntry;
        // `use gpui::*` above re-exports gpui's `test` macro, which would make
        // the `#[test]` emitted by `#[gpui::test]` resolve back to itself.
        use core::prelude::v1::test;
        use gpui::TestAppContext;
        use uuid::Uuid;

        fn row_signature(row: &SidebarVirtualRow) -> String {
            match row {
                SidebarVirtualRow::Connection(row) => {
                    format!("connection:{}", row.conn_name)
                }
                SidebarVirtualRow::Database(row) => format!(
                    "database:{}:expanded={}:active={}",
                    row.database_name, row.is_expanded, row.is_active
                ),
                SidebarVirtualRow::SchemaNode(row) => format!(
                    "schema-node:{}/{}:expanded={}",
                    row.database_name, row.schema_name, row.is_expanded
                ),
                SidebarVirtualRow::Section(row) => format!(
                    "section:{}:({}/{}):depth={}:expanded={}",
                    row.label, row.filtered_count, row.total_count, row.depth, row.is_expanded
                ),
                SidebarVirtualRow::Leaf(row) => {
                    format!("leaf:{}:depth={}", row.label, row.depth)
                }
                SidebarVirtualRow::Loading(row) => format!("loading:depth={}", row.depth),
                SidebarVirtualRow::NoResults(row) => format!("no-results:{}", row.query),
            }
        }

        fn connection(db_type: &str) -> ConnectionEntry {
            let mut connection = ConnectionEntry::new(
                Uuid::new_v4(),
                format!("{db_type}-conn"),
                db_type.to_string(),
            );
            connection.is_connected = true;
            connection.is_expanded = true;
            connection
        }

        fn database(
            name: &str,
            schema: Option<DatabaseSchemaData>,
        ) -> SidebarDatabaseInfo {
            SidebarDatabaseInfo {
                name: name.to_string(),
                size_bytes: None,
                is_active: true,
                is_expanded: true,
                is_loading: false,
                schema,
                collections: Vec::new(),
                indexes: Vec::new(),
                functions: Vec::new(),
                gridfs_buckets: Vec::new(),
                users: Vec::new(),
                roles: Vec::new(),
                search_indexes: Vec::new(),
                vector_indexes: Vec::new(),
                server: Vec::new(),
                sharding: Vec::new(),
                collections_expanded: false,
                collections_loading: false,
                document_expanded_sections: std::collections::HashSet::new(),
            }
        }

        fn rebuilt_signatures(
            cx: &mut TestAppContext,
            setup: impl FnOnce(&mut ConnectionSidebar, &mut Context<ConnectionSidebar>),
        ) -> Vec<String> {
            let sidebar = cx.new(ConnectionSidebar::new);
            sidebar.update(cx, |sidebar, cx| {
                setup(sidebar, cx);
                sidebar.rebuild_virtual_rows();
                sidebar.virtual_rows.iter().map(row_signature).collect()
            })
        }

        #[gpui::test]
        fn mysql_database_renders_flat_sections(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("mysql");
                let schema = DatabaseSchemaData {
                    schema_name: Some("art".to_string()),
                    tables: vec!["orders".to_string(), "users".to_string()],
                    views: vec!["v1".to_string()],
                    events: vec!["nightly".to_string()],
                    expanded_sections: [SidebarSection::Tables].into_iter().collect(),
                    ..DatabaseSchemaData::default()
                };
                connection.databases = vec![database("art", Some(schema))];
                sidebar.connections.push(connection);
            });

            assert_eq!(
                rows,
                vec![
                    "connection:mysql-conn",
                    "database:art:expanded=true:active=true",
                    "section:Tables:(2/2):depth=2:expanded=true",
                    "leaf:orders:depth=3",
                    "leaf:users:depth=3",
                    "section:Views:(1/1):depth=2:expanded=false",
                    "section:Triggers:(0/0):depth=2:expanded=false",
                    "section:Functions:(0/0):depth=2:expanded=false",
                    "section:Procedures:(0/0):depth=2:expanded=false",
                    "section:Events:(1/1):depth=2:expanded=false",
                    "section:Queries:(0/0):depth=2:expanded=false",
                ]
            );
        }

        #[gpui::test]
        fn postgres_multi_schema_renders_grouped_folders(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("postgres");
                let schema = DatabaseSchemaData {
                    schema_name: Some("public".to_string()),
                    schema_names: vec!["audit".to_string(), "public".to_string()],
                    tables: vec!["public.users".to_string(), "audit.events".to_string()],
                    sequences: vec!["public.id_seq".to_string()],
                    // Legacy names: these sets store the EXPANDED groups/sections.
                    expanded_schema_groups: ["public".to_string()].into_iter().collect(),
                    expanded_schema_section_keys: ["public::tables".to_string()]
                        .into_iter()
                        .collect(),
                    ..DatabaseSchemaData::default()
                };
                connection.databases = vec![database("app", Some(schema))];
                sidebar.connections.push(connection);
            });

            assert_eq!(
                rows,
                vec![
                    "connection:postgres-conn",
                    "database:app:expanded=true:active=true",
                    "section:audit:(1/1):depth=2:expanded=false",
                    "section:public:(2/2):depth=2:expanded=true",
                    "section:Tables:(1/1):depth=3:expanded=true",
                    "leaf:users:depth=4",
                    "section:Views:(0/0):depth=3:expanded=false",
                    "section:Materialized Views:(0/0):depth=3:expanded=false",
                    "section:Functions:(0/0):depth=3:expanded=false",
                    "section:Procedures:(0/0):depth=3:expanded=false",
                    "section:Sequences:(1/1):depth=3:expanded=false",
                    "section:Domains:(0/0):depth=3:expanded=false",
                    "section:Types:(0/0):depth=3:expanded=false",
                    "section:Extensions:(0/0):depth=3:expanded=false",
                    "section:Queries:(0/0):depth=2:expanded=false",
                ]
            );
        }

        /// Table actions run against the leaf's `TableRef`, so it must name the
        /// leaf's own database and schema even when its connection isn't selected.
        #[gpui::test]
        fn table_leaves_carry_their_database_and_schema(cx: &mut TestAppContext) {
            let sidebar = cx.new(ConnectionSidebar::new);
            let leaves = sidebar.update(cx, |sidebar, _cx| {
                let mut postgres = connection("postgres");
                postgres.databases = vec![database(
                    "app",
                    Some(DatabaseSchemaData {
                        schema_name: Some("public".to_string()),
                        schema_names: vec!["audit".to_string(), "public".to_string()],
                        tables: vec!["public.events".to_string(), "audit.odd.name".to_string()],
                        expanded_schema_groups: ["audit".to_string()].into_iter().collect(),
                        expanded_schema_section_keys: ["audit::tables".to_string()]
                            .into_iter()
                            .collect(),
                        ..DatabaseSchemaData::default()
                    }),
                )];
                let mut mysql = connection("mysql");
                mysql.databases = vec![database(
                    "art",
                    Some(DatabaseSchemaData {
                        schema_name: Some("art".to_string()),
                        tables: vec!["orders".to_string()],
                        expanded_sections: [SidebarSection::Tables].into_iter().collect(),
                        ..DatabaseSchemaData::default()
                    }),
                )];
                sidebar.connections.push(postgres);
                sidebar.connections.push(mysql);
                sidebar.rebuild_virtual_rows();
                sidebar
                    .virtual_rows
                    .iter()
                    .filter_map(|row| match row {
                        SidebarVirtualRow::Leaf(LeafRow {
                            kind: SidebarLeafKind::Table { table, .. },
                            ..
                        }) => Some(table.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            });

            assert_eq!(
                leaves,
                vec![
                    TableRef::new(
                        Some("app".to_string()),
                        Some("audit".to_string()),
                        "odd.name"
                    ),
                    TableRef::new(Some("art".to_string()), Some("art".to_string()), "orders"),
                ]
            );
        }

        #[gpui::test]
        fn postgres_single_schema_renders_schema_node(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("postgres");
                let schema = DatabaseSchemaData {
                    schema_name: Some("public".to_string()),
                    tables: vec!["users".to_string()],
                    sequences: vec!["id_seq".to_string()],
                    schema_expanded: true,
                    expanded_sections: [SidebarSection::Tables].into_iter().collect(),
                    ..DatabaseSchemaData::default()
                };
                connection.databases = vec![database("app", Some(schema))];
                sidebar.connections.push(connection);
            });

            assert_eq!(
                rows,
                vec![
                    "connection:postgres-conn",
                    "database:app:expanded=true:active=true",
                    "schema-node:app/public:expanded=true",
                    "section:Tables:(1/1):depth=3:expanded=true",
                    "leaf:users:depth=4",
                    "section:Views:(0/0):depth=3:expanded=false",
                    "section:Materialized Views:(0/0):depth=3:expanded=false",
                    "section:Functions:(0/0):depth=3:expanded=false",
                    "section:Procedures:(0/0):depth=3:expanded=false",
                    "section:Sequences:(1/1):depth=3:expanded=false",
                    "section:Domains:(0/0):depth=3:expanded=false",
                    "section:Types:(0/0):depth=3:expanded=false",
                    "section:Extensions:(0/0):depth=3:expanded=false",
                    "section:Queries:(0/0):depth=3:expanded=false",
                ]
            );
        }

        #[gpui::test]
        fn search_filters_and_auto_expands_matching_sections(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("mysql");
                let schema = DatabaseSchemaData {
                    schema_name: Some("art".to_string()),
                    tables: vec!["orders".to_string(), "users".to_string()],
                    views: vec!["v1".to_string()],
                    ..DatabaseSchemaData::default()
                };
                connection.databases = vec![database("art", Some(schema))];
                sidebar.connections.push(connection);
                sidebar.search_query = "user".to_string();
                sidebar.search_query_lowercase = "user".to_string();
            });

            assert_eq!(
                rows,
                vec![
                    "connection:mysql-conn",
                    "database:art:expanded=true:active=true",
                    "section:Tables:(1/2):depth=2:expanded=true",
                    "leaf:users:depth=3",
                ]
            );
        }

        #[gpui::test]
        fn search_without_matches_renders_no_results(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("mysql");
                let schema = DatabaseSchemaData {
                    schema_name: Some("art".to_string()),
                    tables: vec!["orders".to_string()],
                    ..DatabaseSchemaData::default()
                };
                connection.databases = vec![database("art", Some(schema))];
                sidebar.connections.push(connection);
                sidebar.search_query = "zzz".to_string();
                sidebar.search_query_lowercase = "zzz".to_string();
            });

            assert_eq!(
                rows,
                vec![
                    "connection:mysql-conn",
                    "database:art:expanded=true:active=true",
                    "no-results:zzz",
                ]
            );
        }

        #[gpui::test]
        fn loading_database_renders_loading_row_only(cx: &mut TestAppContext) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("postgres");
                let mut loading_database = database("app", None);
                loading_database.is_loading = true;
                connection.databases = vec![loading_database];
                sidebar.connections.push(connection);
            });

            assert_eq!(
                rows,
                vec![
                    "connection:postgres-conn",
                    "database:app:expanded=true:active=true",
                    "loading:depth=2",
                ]
            );
        }

        #[gpui::test]
        fn connection_without_databases_renders_connection_level_sections(
            cx: &mut TestAppContext,
        ) {
            let rows = rebuilt_signatures(cx, |sidebar, _cx| {
                let mut connection = connection("mysql");
                connection.schema = DatabaseSchemaData {
                    tables: vec!["users".to_string()],
                    loading_sections: [SidebarSection::Tables].into_iter().collect(),
                    expanded_sections: [SidebarSection::Tables].into_iter().collect(),
                    ..DatabaseSchemaData::default()
                };
                sidebar.connections.push(connection);
            });

            assert_eq!(
                rows,
                vec![
                    "connection:mysql-conn",
                    "section:Tables:(1/1):depth=1:expanded=true",
                    "loading:depth=2",
                    "section:Views:(0/0):depth=1:expanded=false",
                    "section:Triggers:(0/0):depth=1:expanded=false",
                    "section:Functions:(0/0):depth=1:expanded=false",
                    "section:Procedures:(0/0):depth=1:expanded=false",
                    "section:Events:(0/0):depth=1:expanded=false",
                    "section:Queries:(0/0):depth=1:expanded=false",
                ]
            );
        }

        #[gpui::test]
        fn lazily_loaded_views_render_under_the_active_database(cx: &mut TestAppContext) {
            let connection_id = Uuid::new_v4();
            let rows = rebuilt_signatures(cx, |sidebar, cx| {
                let mut connection = connection("mysql");
                connection.id = connection_id;
                sidebar.connections.push(connection);
                sidebar.merge_databases(
                    connection_id,
                    vec![("app".to_string(), None)],
                    Some("app"),
                    cx,
                );
                if let Some(database) = sidebar
                    .connections
                    .iter_mut()
                    .find(|connection| connection.id == connection_id)
                    .and_then(|connection| connection.databases.first_mut())
                {
                    database.is_expanded = true;
                }
                sidebar.set_views_only(connection_id, vec!["v1".to_string()], cx);
            });

            let views_row = rows
                .iter()
                .find(|row| row.contains("Views"))
                .expect("views section should render");
            assert_eq!(
                views_row, "section:Views:(1/1):depth=2:expanded=false",
                "lazily loaded views must reach the active database node; rows: {rows:#?}"
            );
        }

        #[gpui::test]
        fn database_discovery_carries_over_connection_scope_state(cx: &mut TestAppContext) {
            let connection_id = Uuid::new_v4();
            let rows = rebuilt_signatures(cx, |sidebar, cx| {
                let mut connection = connection("mysql");
                connection.id = connection_id;
                sidebar.connections.push(connection);

                // Loaded and expanded before the database list arrives.
                sidebar.set_tables_only(
                    connection_id,
                    vec!["users".to_string()],
                    Some("app".to_string()),
                    Vec::new(),
                    cx,
                );
                sidebar.toggle_section_expand(connection_id, None, SidebarSection::Tables, cx);

                sidebar.merge_databases(
                    connection_id,
                    vec![("app".to_string(), None), ("other".to_string(), None)],
                    Some("app"),
                    cx,
                );
                if let Some(database) = sidebar
                    .connections
                    .iter_mut()
                    .find(|connection| connection.id == connection_id)
                    .and_then(|connection| connection.databases.first_mut())
                {
                    database.is_expanded = true;
                }
            });

            assert_eq!(
                rows,
                vec![
                    "connection:mysql-conn",
                    "database:app:expanded=true:active=true",
                    "section:Tables:(1/1):depth=2:expanded=true",
                    "leaf:users:depth=3",
                    "section:Views:(0/0):depth=2:expanded=false",
                    "section:Triggers:(0/0):depth=2:expanded=false",
                    "section:Functions:(0/0):depth=2:expanded=false",
                    "section:Procedures:(0/0):depth=2:expanded=false",
                    "section:Events:(0/0):depth=2:expanded=false",
                    "section:Queries:(0/0):depth=2:expanded=false",
                    "database:other:expanded=false:active=false",
                ]
            );
        }

        #[gpui::test]
        fn active_database_without_its_own_schema_toggles_connection_scope(
            cx: &mut TestAppContext,
        ) {
            let connection_id = Uuid::new_v4();
            let rows = rebuilt_signatures(cx, |sidebar, cx| {
                let mut connection = connection("mysql");
                connection.id = connection_id;
                connection.schema = DatabaseSchemaData {
                    tables: vec!["users".to_string()],
                    ..DatabaseSchemaData::default()
                };
                // The placeholder node created at connect time, before the
                // schema has a home of its own.
                let mut placeholder = database("app", None);
                placeholder.is_expanded = true;
                connection.databases = vec![placeholder];
                sidebar.connections.push(connection);

                // The row carries a database-scoped action, which must still
                // land on the connection-scope state rendering behind it.
                sidebar.toggle_db_section(connection_id, "app", "tables", cx);
            });

            assert!(
                rows.contains(&"section:Tables:(1/1):depth=2:expanded=true".to_string()),
                "toggling the rendered scope must expand it; rows: {rows:#?}"
            );
        }
    }
}
