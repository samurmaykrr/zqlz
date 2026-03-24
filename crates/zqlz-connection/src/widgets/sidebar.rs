//! Connection sidebar panel
//!
//! Displays a tree view of all database connections and their objects.

mod menus;
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
use zqlz_ui::widgets::{
    ActiveTheme, DatabaseLogo, Icon, IconName, Sizable, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::{Scrollbar, ScrollbarShow},
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
        table_name: String,
        database_name: Option<String>,
    },
    /// User wants to open a view
    OpenView {
        connection_id: Uuid,
        view_name: String,
        database_name: Option<String>,
    },
    /// User wants to design/edit a view SQL definition
    DesignView {
        connection_id: Uuid,
        view_name: String,
        object_schema: Option<String>,
    },
    /// User wants to create a new view
    NewView { connection_id: Uuid },
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
    CopyViewName { view_name: String },
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
    /// User dropped one or more external paths onto the sidebar
    OpenDroppedPaths(Vec<PathBuf>),

    // Table-specific events
    /// User wants to design/edit a table structure
    DesignTable {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to create a new table
    NewTable { connection_id: Uuid },
    /// User wants to delete a table
    DeleteTable {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to empty/truncate a table
    EmptyTable {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to duplicate a table
    DuplicateTable {
        connection_id: Uuid,
        table_name: String,
    },
    /// User wants to rename a table
    RenameTable {
        connection_id: Uuid,
        table_name: String,
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
    CopyTableName { table_name: String },
    /// User wants to refresh a specific connection's schema
    RefreshSchema { connection_id: Uuid },

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
    },

    // Procedure events
    /// User wants to open/view a procedure definition
    OpenProcedure {
        connection_id: Uuid,
        procedure_name: String,
        object_schema: Option<String>,
    },

    // Trigger events
    /// User wants to design/edit a trigger definition
    DesignTrigger {
        connection_id: Uuid,
        trigger_name: String,
        object_schema: Option<String>,
    },
    /// User wants to create a new trigger
    NewTrigger { connection_id: Uuid },
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

    // Multi-database events
    /// User wants to connect to a different database on the same server
    ConnectToDatabase {
        connection_id: Uuid,
        database_name: String,
    },

    /// User expanded a section that has not been loaded yet (lazy loading)
    LoadSection {
        connection_id: Uuid,
        /// One of: "views", "materialized_views", "triggers", "functions", "procedures"
        section: &'static str,
    },
}

/// Connection sidebar showing all connections and their schema objects
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
            _subscriptions: Vec::new(),
        }
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

    /// Toggle tables section expand/collapse
    fn toggle_tables_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.tables_expanded = !conn.tables_expanded;
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle views section expand/collapse
    fn toggle_views_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_views {
                return;
            }
            conn.views_expanded = !conn.views_expanded;
            if conn.views_expanded && conn.views.is_empty() && !conn.views_loading {
                conn.views_loading = true;
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section: "views",
            });
        }
    }

    /// Toggle materialized views section expand/collapse
    fn toggle_materialized_views_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_materialized_views {
                return;
            }
            conn.materialized_views_expanded = !conn.materialized_views_expanded;
            if conn.materialized_views_expanded
                && conn.materialized_views.is_empty()
                && !conn.materialized_views_loading
            {
                conn.materialized_views_loading = true;
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section: "materialized_views",
            });
        }
    }

    /// Toggle triggers section expand/collapse
    fn toggle_triggers_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_triggers {
                return;
            }
            conn.triggers_expanded = !conn.triggers_expanded;
            if conn.triggers_expanded && conn.triggers.is_empty() && !conn.triggers_loading {
                conn.triggers_loading = true;
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section: "triggers",
            });
        }
    }

    /// Toggle functions section expand/collapse
    fn toggle_functions_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_functions {
                return;
            }
            conn.functions_expanded = !conn.functions_expanded;
            if conn.functions_expanded && conn.functions.is_empty() && !conn.functions_loading {
                conn.functions_loading = true;
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section: "functions",
            });
        }
    }

    /// Toggle procedures section expand/collapse
    fn toggle_procedures_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if !conn.object_capabilities.supports_procedures {
                return;
            }
            conn.procedures_expanded = !conn.procedures_expanded;
            if conn.procedures_expanded && conn.procedures.is_empty() && !conn.procedures_loading {
                conn.procedures_loading = true;
                should_load = true;
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
        if should_load {
            cx.emit(ConnectionSidebarEvent::LoadSection {
                connection_id: id,
                section: "procedures",
            });
        }
    }

    /// Toggle a schema section within a specific database node.
    /// `section` identifies which section to toggle (e.g. "tables", "views", "schema").
    fn toggle_db_section(
        &mut self,
        conn_id: Uuid,
        db_name: &str,
        section: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name)
            && let Some(schema) = &mut db.schema
        {
            match section {
                "schema" => schema.schema_expanded = !schema.schema_expanded,
                "tables" => schema.tables_expanded = !schema.tables_expanded,
                "views" => schema.views_expanded = !schema.views_expanded,
                "materialized_views" => {
                    schema.materialized_views_expanded = !schema.materialized_views_expanded
                }
                "triggers" => schema.triggers_expanded = !schema.triggers_expanded,
                "functions" => schema.functions_expanded = !schema.functions_expanded,
                "procedures" => schema.procedures_expanded = !schema.procedures_expanded,
                _ => {}
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle schema-group expansion for a multi-schema active database that
    /// still uses connection-level fallback data.
    fn toggle_schema_group_expand(
        &mut self,
        conn_id: Uuid,
        schema_name: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if conn.collapsed_schema_groups.contains(schema_name) {
                conn.collapsed_schema_groups.remove(schema_name);
            } else {
                conn.collapsed_schema_groups.insert(schema_name.to_string());
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a section expansion for a grouped schema in the connection-level
    /// fallback data set.
    fn toggle_schema_section_expand(
        &mut self,
        conn_id: Uuid,
        schema_name: &str,
        section: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            let section_key = format!("{schema_name}::{section}");
            if conn.collapsed_schema_section_keys.contains(&section_key) {
                conn.collapsed_schema_section_keys.remove(&section_key);
            } else {
                conn.collapsed_schema_section_keys.insert(section_key);
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle schema-group expansion within a specific database node.
    fn toggle_db_schema_group_expand(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        schema_name: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(database) = conn
                .databases
                .iter_mut()
                .find(|database| database.name == database_name)
            && let Some(schema) = &mut database.schema
        {
            if schema.collapsed_schema_groups.contains(schema_name) {
                schema.collapsed_schema_groups.remove(schema_name);
            } else {
                schema
                    .collapsed_schema_groups
                    .insert(schema_name.to_string());
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a section expansion within a grouped schema inside a database node.
    fn toggle_db_schema_section_expand(
        &mut self,
        conn_id: Uuid,
        database_name: &str,
        schema_name: &str,
        section: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id)
            && let Some(database) = conn
                .databases
                .iter_mut()
                .find(|database| database.name == database_name)
            && let Some(schema) = &mut database.schema
        {
            let section_key = format!("{schema_name}::{section}");
            if schema.collapsed_schema_section_keys.contains(&section_key) {
                schema.collapsed_schema_section_keys.remove(&section_key);
            } else {
                schema.collapsed_schema_section_keys.insert(section_key);
            }
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle queries section expand/collapse
    fn toggle_queries_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.queries_expanded = !conn.queries_expanded;
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle a specific database node expand/collapse.
    /// When expanding a database that has no schema loaded yet, automatically
    /// triggers schema loading so the user doesn't need a second click.
    fn toggle_database_expand(&mut self, id: Uuid, db_name: &str, cx: &mut Context<Self>) {
        let mut should_load_schema = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id)
            && let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name)
        {
            db.is_expanded = !db.is_expanded;
            if db.is_expanded && db.schema.is_none() && !db.is_active && !db.is_loading {
                should_load_schema = true;
            }
        }
        if should_load_schema {
            cx.emit(ConnectionSidebarEvent::ConnectToDatabase {
                connection_id: id,
                database_name: db_name.to_string(),
            });
        }
        self.virtual_rows_dirty = true;
        cx.notify();
    }

    /// Toggle schema-level node expand/collapse
    #[allow(dead_code)]
    fn toggle_schema_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.schema_expanded = !conn.schema_expanded;
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
        match section {
            "tables" | "queries" | "redis_databases" => true,
            "views" => capabilities.supports_views,
            "materialized_views" => capabilities.supports_materialized_views,
            "triggers" => capabilities.supports_triggers,
            "functions" => capabilities.supports_functions,
            "procedures" => capabilities.supports_procedures,
            _ => true,
        }
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

    fn current_schema_for_virtual_rows(&self, database_name: Option<&str>) -> Option<String> {
        self.selected_connection.and_then(|selected_connection| {
            self.connections
                .iter()
                .find(|connection| connection.id == selected_connection)
                .and_then(|connection| {
                    database_name
                        .and_then(|database_name| {
                            connection
                                .databases
                                .iter()
                                .find(|database| database.name == database_name)
                                .and_then(|database| {
                                    database
                                        .schema
                                        .as_ref()
                                        .and_then(|schema| schema.schema_name.clone())
                                })
                        })
                        .or_else(|| connection.schema_name.clone())
                })
        })
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

    fn push_section_row(
        rows: &mut Vec<SidebarVirtualRow>,
        section_row: SectionRow,
        has_search: bool,
    ) -> bool {
        let should_include = !has_search || section_row.filtered_count > 0;
        if should_include {
            rows.push(SidebarVirtualRow::Section(section_row));
        }
        should_include
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
            SidebarRowIcon::Query => Icon::new(ZqlzIcon::FileSql)
                .size_3()
                .text_color(muted_foreground)
                .into_any_element(),
        }
    }

    fn split_schema_qualified_name_for_rows(name: &str) -> Option<(&str, &str)> {
        let (schema_name, object_name) = name.split_once('.')?;
        if schema_name.is_empty() || object_name.is_empty() {
            return None;
        }

        Some((schema_name, object_name))
    }

    #[allow(clippy::too_many_arguments)]
    fn group_schema_sections_for_rows(
        tables: &[String],
        views: &[String],
        materialized_views: &[String],
        triggers: &[String],
        functions: &[String],
        procedures: &[String],
        schema_names: &[String],
        fallback_schema_name: Option<&str>,
    ) -> Option<Vec<(String, SchemaSectionGroup)>> {
        let mut groups: BTreeMap<String, SchemaSectionGroup> = BTreeMap::new();
        let mut saw_schema_qualified_name = false;

        let fallback_schema = fallback_schema_name.unwrap_or("public").to_string();

        for schema_name in schema_names {
            groups.entry(schema_name.clone()).or_default();
        }

        for table_name in tables {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(table_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .tables
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .tables
                    .push(table_name.clone());
            }
        }

        for view_name in views {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(view_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .views
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .views
                    .push(view_name.clone());
            }
        }

        for view_name in materialized_views {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(view_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .materialized_views
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .materialized_views
                    .push(view_name.clone());
            }
        }

        for trigger_name in triggers {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(trigger_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .triggers
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .triggers
                    .push(trigger_name.clone());
            }
        }

        for function_name in functions {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(function_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .functions
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .functions
                    .push(function_name.clone());
            }
        }

        for procedure_name in procedures {
            if let Some((schema_name, object_name)) =
                Self::split_schema_qualified_name_for_rows(procedure_name)
            {
                saw_schema_qualified_name = true;
                groups
                    .entry(schema_name.to_string())
                    .or_default()
                    .procedures
                    .push(object_name.to_string());
            } else {
                groups
                    .entry(fallback_schema.clone())
                    .or_default()
                    .procedures
                    .push(procedure_name.clone());
            }
        }

        if !saw_schema_qualified_name {
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

        let databases_expanded =
            connection.redis_databases_expanded || (has_search && !filtered_databases.is_empty());
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

        if !connection.queries.is_empty() && (!has_search || !filtered_queries.is_empty()) {
            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!("queries-header-{}", connection.id),
                icon: SidebarRowIcon::Query,
                label: "Saved Queries".to_string(),
                total_count: connection.queries.len(),
                filtered_count: filtered_queries.len(),
                is_expanded: queries_expanded,
                depth: 1,
                action: SidebarSectionAction::ConnectionSection {
                    conn_id: connection.id,
                    section: "queries",
                },
                context_menu_section: Some("queries"),
            }));

            if queries_expanded {
                for query in filtered_queries {
                    rows.push(SidebarVirtualRow::Leaf(LeafRow {
                        element_id: format!("query-{}-{}", connection.id, query.id),
                        icon: SidebarRowIcon::Query,
                        label: query.name.clone(),
                        depth: 2,
                        kind: SidebarLeafKind::Query {
                            conn_id: connection.id,
                            query_id: query.id,
                            query_name: query.name.clone(),
                        },
                    }));

                    *matched_leaf_rows += 1;
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
            self.append_connection_level_objects_rows(connection, None, 1, rows, matched_leaf_rows);
            return;
        }

        let active_database_name = connection
            .databases
            .iter()
            .find(|database| database.is_active)
            .map(|database| database.name.clone());
        let fallback_tree_depth = if connection.schema_name.as_ref().is_some_and(|schema_name| {
            active_database_name.as_deref() != Some(schema_name.as_str())
        }) {
            3
        } else {
            2
        };

        let mut fallback_rendered = false;

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

            if !(database.schema.is_some() || database.is_active) {
                rows.push(SidebarVirtualRow::Loading(LoadingRow {
                    element_id: format!("loading-schema-{}-{}", connection.id, database.name),
                    text: "Loading schema...".to_string(),
                    depth: 2,
                }));
                continue;
            }

            if let Some(schema_data) = &database.schema {
                if let Some(groups) = Self::group_schema_sections_for_rows(
                    &schema_data.tables,
                    &schema_data.views,
                    &schema_data.materialized_views,
                    &schema_data.triggers,
                    &schema_data.functions,
                    &schema_data.procedures,
                    &schema_data.schema_names,
                    schema_data.schema_name.as_deref(),
                ) {
                    self.append_grouped_objects_rows(
                        connection,
                        Some(database.name.as_str()),
                        &groups,
                        &connection.queries,
                        connection.queries_expanded,
                        schema_data.tables_loading,
                        schema_data.views_loading,
                        schema_data.materialized_views_loading,
                        schema_data.triggers_loading,
                        schema_data.functions_loading,
                        schema_data.procedures_loading,
                        &schema_data.collapsed_schema_groups,
                        &schema_data.collapsed_schema_section_keys,
                        2,
                        rows,
                        matched_leaf_rows,
                    );
                    continue;
                }

                let schema_name = schema_data
                    .schema_name
                    .clone()
                    .or_else(|| connection.schema_name.clone());
                let show_schema_node = schema_name
                    .as_ref()
                    .is_some_and(|active_schema| active_schema != &database.name);

                if show_schema_node {
                    rows.push(SidebarVirtualRow::SchemaNode(SchemaNodeRow {
                        conn_id: connection.id,
                        database_name: database.name.clone(),
                        schema_name: schema_name.unwrap_or_default(),
                        is_expanded: schema_data.schema_expanded,
                        has_database_schema: true,
                    }));
                }

                if !show_schema_node || schema_data.schema_expanded {
                    self.append_database_level_objects_rows(
                        connection,
                        database,
                        if show_schema_node { 3 } else { 2 },
                        rows,
                        matched_leaf_rows,
                    );
                }

                continue;
            }

            if !fallback_rendered
                && let Some(groups) = Self::group_schema_sections_for_rows(
                    &connection.tables,
                    &connection.views,
                    &connection.materialized_views,
                    &connection.triggers,
                    &connection.functions,
                    &connection.procedures,
                    &connection.schema_names,
                    connection.schema_name.as_deref(),
                )
            {
                self.append_grouped_objects_rows(
                    connection,
                    None,
                    &groups,
                    &connection.queries,
                    connection.queries_expanded,
                    connection.tables_loading,
                    connection.views_loading,
                    connection.materialized_views_loading,
                    connection.triggers_loading,
                    connection.functions_loading,
                    connection.procedures_loading,
                    &connection.collapsed_schema_groups,
                    &connection.collapsed_schema_section_keys,
                    2,
                    rows,
                    matched_leaf_rows,
                );
                fallback_rendered = true;
                continue;
            }

            if !fallback_rendered {
                let schema_name = connection.schema_name.clone();
                let show_schema_node = schema_name
                    .as_ref()
                    .is_some_and(|active_schema| active_schema != &database.name);

                if show_schema_node {
                    rows.push(SidebarVirtualRow::SchemaNode(SchemaNodeRow {
                        conn_id: connection.id,
                        database_name: database.name.clone(),
                        schema_name: schema_name.unwrap_or_default(),
                        is_expanded: connection.schema_expanded,
                        has_database_schema: false,
                    }));
                }

                if !show_schema_node || connection.schema_expanded {
                    self.append_connection_level_objects_rows(
                        connection,
                        Some(database.name.clone()),
                        fallback_tree_depth,
                        rows,
                        matched_leaf_rows,
                    );
                }
                fallback_rendered = true;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_grouped_objects_rows(
        &self,
        connection: &ConnectionEntry,
        database_name: Option<&str>,
        groups: &[(String, SchemaSectionGroup)],
        queries: &[SavedQueryInfo],
        queries_expanded: bool,
        tables_loading: bool,
        views_loading: bool,
        materialized_views_loading: bool,
        triggers_loading: bool,
        functions_loading: bool,
        procedures_loading: bool,
        expanded_schema_groups: &std::collections::HashSet<String>,
        expanded_schema_section_keys: &std::collections::HashSet<String>,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query.is_empty();
        let database_name = database_name.map(ToOwned::to_owned);

        for (schema_name, group) in groups {
            let filtered_tables = self.filter_by_search(&group.tables);
            let filtered_views = self.filter_by_search(&group.views);
            let filtered_materialized_views = self.filter_by_search(&group.materialized_views);
            let filtered_triggers = self.filter_by_search(&group.triggers);
            let filtered_functions = self.filter_by_search(&group.functions);
            let filtered_procedures = self.filter_by_search(&group.procedures);

            let schema_has_matches = self.matches_search(schema_name)
                || !filtered_tables.is_empty()
                || !filtered_views.is_empty()
                || !filtered_materialized_views.is_empty()
                || !filtered_triggers.is_empty()
                || !filtered_functions.is_empty()
                || !filtered_procedures.is_empty();

            if has_search && !schema_has_matches {
                continue;
            }

            let schema_is_expanded =
                expanded_schema_groups.contains(schema_name) || (has_search && schema_has_matches);

            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!(
                    "schema-group-{}-{}",
                    connection.id,
                    schema_name.replace(' ', "-")
                ),
                icon: SidebarRowIcon::Folder,
                label: schema_name.clone(),
                total_count: group.tables.len()
                    + group.views.len()
                    + group.materialized_views.len()
                    + group.triggers.len()
                    + group.functions.len()
                    + group.procedures.len(),
                filtered_count: filtered_tables.len()
                    + filtered_views.len()
                    + filtered_materialized_views.len()
                    + filtered_triggers.len()
                    + filtered_functions.len()
                    + filtered_procedures.len(),
                is_expanded: schema_is_expanded,
                depth,
                action: SidebarSectionAction::SchemaGroup {
                    conn_id: connection.id,
                    database_name: database_name.clone(),
                    schema_name: schema_name.clone(),
                },
                context_menu_section: None,
            }));

            if !schema_is_expanded {
                continue;
            }

            let section_depth = depth + 1;
            let leaf_depth = depth + 2;

            let tables_section_key = format!("{}::tables", schema_name);
            let tables_expanded = expanded_schema_section_keys.contains(&tables_section_key)
                || (has_search && !filtered_tables.is_empty());
            if !has_search || tables_loading || !filtered_tables.is_empty() {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("tables-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::Table,
                    label: "Tables".to_string(),
                    total_count: group.tables.len(),
                    filtered_count: filtered_tables.len(),
                    is_expanded: tables_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "tables",
                    },
                    context_menu_section: Some("tables"),
                }));

                if tables_expanded {
                    if tables_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!("loading-tables-{}-{}", connection.id, schema_name),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for table_name in &filtered_tables {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "table-{}-{}-{}",
                                    connection.id, schema_name, table_name
                                ),
                                icon: SidebarRowIcon::Table,
                                label: (*table_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::Table {
                                    conn_id: connection.id,
                                    open_table_name: format!("{}.{}", schema_name, table_name),
                                    menu_table_name: (*table_name).clone(),
                                    object_schema: Some(schema_name.clone()),
                                    database_name: database_name.clone(),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }

            let views_section_key = format!("{}::views", schema_name);
            let views_expanded = expanded_schema_section_keys.contains(&views_section_key)
                || (has_search && !filtered_views.is_empty());
            if connection.object_capabilities.supports_views
                && (!has_search || views_loading || !filtered_views.is_empty())
            {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("views-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::View,
                    label: "Views".to_string(),
                    total_count: group.views.len(),
                    filtered_count: filtered_views.len(),
                    is_expanded: views_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "views",
                    },
                    context_menu_section: Some("views"),
                }));

                if views_expanded {
                    if views_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!("loading-views-{}-{}", connection.id, schema_name),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for view_name in &filtered_views {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "view-{}-{}-{}",
                                    connection.id, schema_name, view_name
                                ),
                                icon: SidebarRowIcon::View,
                                label: (*view_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::View {
                                    conn_id: connection.id,
                                    open_view_name: format!("{}.{}", schema_name, view_name),
                                    menu_view_name: (*view_name).clone(),
                                    object_schema: Some(schema_name.clone()),
                                    database_name: database_name.clone(),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }

            let mat_views_section_key = format!("{}::materialized_views", schema_name);
            let mat_views_expanded = expanded_schema_section_keys.contains(&mat_views_section_key)
                || (has_search && !filtered_materialized_views.is_empty());
            if connection.object_capabilities.supports_materialized_views
                && (!has_search
                    || materialized_views_loading
                    || !filtered_materialized_views.is_empty())
            {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("matviews-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::MaterializedView,
                    label: "Materialized Views".to_string(),
                    total_count: group.materialized_views.len(),
                    filtered_count: filtered_materialized_views.len(),
                    is_expanded: mat_views_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "materialized_views",
                    },
                    context_menu_section: Some("materialized_views"),
                }));

                if mat_views_expanded {
                    if materialized_views_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!(
                                "loading-matviews-{}-{}",
                                connection.id, schema_name
                            ),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for view_name in &filtered_materialized_views {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "matview-{}-{}-{}",
                                    connection.id, schema_name, view_name
                                ),
                                icon: SidebarRowIcon::MaterializedView,
                                label: (*view_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::MaterializedView {
                                    conn_id: connection.id,
                                    open_view_name: format!("{}.{}", schema_name, view_name),
                                    menu_view_name: (*view_name).clone(),
                                    database_name: database_name.clone(),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }

            let triggers_section_key = format!("{}::triggers", schema_name);
            let triggers_expanded = expanded_schema_section_keys.contains(&triggers_section_key)
                || (has_search && !filtered_triggers.is_empty());
            if connection.object_capabilities.supports_triggers
                && (!has_search || triggers_loading || !filtered_triggers.is_empty())
            {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("triggers-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::Trigger,
                    label: "Triggers".to_string(),
                    total_count: group.triggers.len(),
                    filtered_count: filtered_triggers.len(),
                    is_expanded: triggers_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "triggers",
                    },
                    context_menu_section: Some("triggers"),
                }));

                if triggers_expanded {
                    if triggers_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!(
                                "loading-triggers-{}-{}",
                                connection.id, schema_name
                            ),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for trigger_name in &filtered_triggers {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "trigger-{}-{}-{}",
                                    connection.id, schema_name, trigger_name
                                ),
                                icon: SidebarRowIcon::Trigger,
                                label: (*trigger_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::Trigger {
                                    conn_id: connection.id,
                                    trigger_name: (*trigger_name).clone(),
                                    object_schema: Some(schema_name.clone()),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }

            let functions_section_key = format!("{}::functions", schema_name);
            let functions_expanded = expanded_schema_section_keys.contains(&functions_section_key)
                || (has_search && !filtered_functions.is_empty());
            if connection.object_capabilities.supports_functions
                && (!has_search || functions_loading || !filtered_functions.is_empty())
            {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("functions-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::Function,
                    label: "Functions".to_string(),
                    total_count: group.functions.len(),
                    filtered_count: filtered_functions.len(),
                    is_expanded: functions_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "functions",
                    },
                    context_menu_section: Some("functions"),
                }));

                if functions_expanded {
                    if functions_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!(
                                "loading-functions-{}-{}",
                                connection.id, schema_name
                            ),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for function_name in &filtered_functions {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "function-{}-{}-{}",
                                    connection.id, schema_name, function_name
                                ),
                                icon: SidebarRowIcon::Function,
                                label: (*function_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::Function {
                                    conn_id: connection.id,
                                    function_name: (*function_name).clone(),
                                    object_schema: Some(schema_name.clone()),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }

            let procedures_section_key = format!("{}::procedures", schema_name);
            let procedures_expanded = expanded_schema_section_keys
                .contains(&procedures_section_key)
                || (has_search && !filtered_procedures.is_empty());
            if connection.object_capabilities.supports_procedures
                && (!has_search || procedures_loading || !filtered_procedures.is_empty())
            {
                rows.push(SidebarVirtualRow::Section(SectionRow {
                    element_id: format!("procedures-header-{}-{}", connection.id, schema_name),
                    icon: SidebarRowIcon::Procedure,
                    label: "Procedures".to_string(),
                    total_count: group.procedures.len(),
                    filtered_count: filtered_procedures.len(),
                    is_expanded: procedures_expanded,
                    depth: section_depth,
                    action: SidebarSectionAction::SchemaGroupSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        schema_name: schema_name.clone(),
                        section: "procedures",
                    },
                    context_menu_section: Some("procedures"),
                }));

                if procedures_expanded {
                    if procedures_loading {
                        rows.push(SidebarVirtualRow::Loading(LoadingRow {
                            element_id: format!(
                                "loading-procedures-{}-{}",
                                connection.id, schema_name
                            ),
                            text: "Loading...".to_string(),
                            depth: leaf_depth,
                        }));
                    } else {
                        for procedure_name in &filtered_procedures {
                            rows.push(SidebarVirtualRow::Leaf(LeafRow {
                                element_id: format!(
                                    "procedure-{}-{}-{}",
                                    connection.id, schema_name, procedure_name
                                ),
                                icon: SidebarRowIcon::Procedure,
                                label: (*procedure_name).clone(),
                                depth: leaf_depth,
                                kind: SidebarLeafKind::Procedure {
                                    conn_id: connection.id,
                                    procedure_name: (*procedure_name).clone(),
                                    object_schema: Some(schema_name.clone()),
                                },
                            }));

                            *matched_leaf_rows += 1;
                        }
                    }
                }
            }
        }

        let filtered_queries: Vec<_> = queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();
        let queries_expanded = queries_expanded || (has_search && !filtered_queries.is_empty());
        if !queries.is_empty() && (!has_search || !filtered_queries.is_empty()) {
            rows.push(SidebarVirtualRow::Section(SectionRow {
                element_id: format!("queries-header-{}", connection.id),
                icon: SidebarRowIcon::Query,
                label: "Queries".to_string(),
                total_count: queries.len(),
                filtered_count: filtered_queries.len(),
                is_expanded: queries_expanded,
                depth,
                action: SidebarSectionAction::ConnectionSection {
                    conn_id: connection.id,
                    section: "queries",
                },
                context_menu_section: Some("queries"),
            }));

            if queries_expanded {
                for query in filtered_queries {
                    rows.push(SidebarVirtualRow::Leaf(LeafRow {
                        element_id: format!("query-{}-{}", connection.id, query.id),
                        icon: SidebarRowIcon::Query,
                        label: query.name.clone(),
                        depth: depth + 1,
                        kind: SidebarLeafKind::Query {
                            conn_id: connection.id,
                            query_id: query.id,
                            query_name: query.name.clone(),
                        },
                    }));

                    *matched_leaf_rows += 1;
                }
            }
        }
    }

    fn append_connection_level_objects_rows(
        &self,
        connection: &ConnectionEntry,
        database_name: Option<String>,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let has_search = !self.search_query.is_empty();
        let filtered_tables = self.filter_by_search(&connection.tables);
        let filtered_views = self.filter_by_search(&connection.views);
        let filtered_materialized_views = self.filter_by_search(&connection.materialized_views);
        let filtered_triggers = self.filter_by_search(&connection.triggers);
        let filtered_functions = self.filter_by_search(&connection.functions);
        let filtered_procedures = self.filter_by_search(&connection.procedures);
        let filtered_queries: Vec<_> = connection
            .queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();

        let tables_expanded =
            connection.tables_expanded || (has_search && !filtered_tables.is_empty());
        let views_expanded =
            connection.views_expanded || (has_search && !filtered_views.is_empty());
        let mat_views_expanded = connection.materialized_views_expanded
            || (has_search && !filtered_materialized_views.is_empty());
        let triggers_expanded =
            connection.triggers_expanded || (has_search && !filtered_triggers.is_empty());
        let functions_expanded =
            connection.functions_expanded || (has_search && !filtered_functions.is_empty());
        let procedures_expanded =
            connection.procedures_expanded || (has_search && !filtered_procedures.is_empty());
        let queries_expanded =
            connection.queries_expanded || (has_search && !filtered_queries.is_empty());

        let include_tables_section = Self::push_section_row(
            rows,
            SectionRow {
                element_id: format!("tables-header-{}", connection.id),
                icon: SidebarRowIcon::Table,
                label: "Tables".to_string(),
                total_count: connection.tables.len(),
                filtered_count: filtered_tables.len(),
                is_expanded: tables_expanded,
                depth,
                action: SidebarSectionAction::ConnectionSection {
                    conn_id: connection.id,
                    section: "tables",
                },
                context_menu_section: Some("tables"),
            },
            has_search,
        );

        if include_tables_section && tables_expanded {
            if connection.tables_loading {
                rows.push(SidebarVirtualRow::Loading(LoadingRow {
                    element_id: format!("loading-tables-{}", connection.id),
                    text: "Loading...".to_string(),
                    depth: depth + 1,
                }));
            } else {
                for table_name in &filtered_tables {
                    let object_schema =
                        self.current_schema_for_virtual_rows(database_name.as_deref());
                    rows.push(SidebarVirtualRow::Leaf(LeafRow {
                        element_id: format!("table-{}-{}", connection.id, table_name),
                        icon: SidebarRowIcon::Table,
                        label: (*table_name).clone(),
                        depth: depth + 1,
                        kind: SidebarLeafKind::Table {
                            conn_id: connection.id,
                            open_table_name: (*table_name).clone(),
                            menu_table_name: (*table_name).clone(),
                            object_schema,
                            database_name: database_name.clone(),
                        },
                    }));
                    *matched_leaf_rows += 1;
                }
            }
        }

        if connection.object_capabilities.supports_views {
            let include_views_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("views-header-{}", connection.id),
                    icon: SidebarRowIcon::View,
                    label: "Views".to_string(),
                    total_count: connection.views.len(),
                    filtered_count: filtered_views.len(),
                    is_expanded: views_expanded,
                    depth,
                    action: SidebarSectionAction::ConnectionSection {
                        conn_id: connection.id,
                        section: "views",
                    },
                    context_menu_section: Some("views"),
                },
                has_search,
            );

            if include_views_section && views_expanded {
                if connection.views_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-views-{}", connection.id),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for view_name in &filtered_views {
                        let object_schema =
                            self.current_schema_for_virtual_rows(database_name.as_deref());
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!("view-{}-{}", connection.id, view_name),
                            icon: SidebarRowIcon::View,
                            label: (*view_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::View {
                                conn_id: connection.id,
                                open_view_name: (*view_name).clone(),
                                menu_view_name: (*view_name).clone(),
                                object_schema,
                                database_name: database_name.clone(),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_materialized_views {
            let include_materialized_views_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("matviews-header-{}", connection.id),
                    icon: SidebarRowIcon::MaterializedView,
                    label: "Materialized Views".to_string(),
                    total_count: connection.materialized_views.len(),
                    filtered_count: filtered_materialized_views.len(),
                    is_expanded: mat_views_expanded,
                    depth,
                    action: SidebarSectionAction::ConnectionSection {
                        conn_id: connection.id,
                        section: "materialized_views",
                    },
                    context_menu_section: Some("materialized_views"),
                },
                has_search,
            );

            if include_materialized_views_section && mat_views_expanded {
                if connection.materialized_views_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-matviews-{}", connection.id),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for view_name in &filtered_materialized_views {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!("matview-{}-{}", connection.id, view_name),
                            icon: SidebarRowIcon::MaterializedView,
                            label: (*view_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::MaterializedView {
                                conn_id: connection.id,
                                open_view_name: (*view_name).clone(),
                                menu_view_name: (*view_name).clone(),
                                database_name: database_name.clone(),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_triggers {
            let include_triggers_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("triggers-header-{}", connection.id),
                    icon: SidebarRowIcon::Trigger,
                    label: "Triggers".to_string(),
                    total_count: connection.triggers.len(),
                    filtered_count: filtered_triggers.len(),
                    is_expanded: triggers_expanded,
                    depth,
                    action: SidebarSectionAction::ConnectionSection {
                        conn_id: connection.id,
                        section: "triggers",
                    },
                    context_menu_section: Some("triggers"),
                },
                has_search,
            );

            if include_triggers_section && triggers_expanded {
                if connection.triggers_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-triggers-{}", connection.id),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for trigger_name in &filtered_triggers {
                        let object_schema =
                            self.current_schema_for_virtual_rows(database_name.as_deref());
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!("trigger-{}-{}", connection.id, trigger_name),
                            icon: SidebarRowIcon::Trigger,
                            label: (*trigger_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Trigger {
                                conn_id: connection.id,
                                trigger_name: (*trigger_name).clone(),
                                object_schema,
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_functions {
            let include_functions_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("functions-header-{}", connection.id),
                    icon: SidebarRowIcon::Function,
                    label: "Functions".to_string(),
                    total_count: connection.functions.len(),
                    filtered_count: if has_search {
                        filtered_functions.len()
                    } else {
                        connection.functions.len()
                    },
                    is_expanded: functions_expanded,
                    depth,
                    action: SidebarSectionAction::ConnectionSection {
                        conn_id: connection.id,
                        section: "functions",
                    },
                    context_menu_section: Some("functions"),
                },
                has_search,
            );

            if include_functions_section && functions_expanded {
                if connection.functions_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-functions-{}", connection.id),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    let function_names: Vec<&String> = if has_search {
                        filtered_functions.into_iter().collect()
                    } else {
                        connection.functions.iter().collect()
                    };

                    for function_name in function_names {
                        let object_schema =
                            self.current_schema_for_virtual_rows(database_name.as_deref());
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!("function-{}-{}", connection.id, function_name),
                            icon: SidebarRowIcon::Function,
                            label: function_name.clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Function {
                                conn_id: connection.id,
                                function_name: function_name.clone(),
                                object_schema,
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_procedures {
            let include_procedures_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("procedures-header-{}", connection.id),
                    icon: SidebarRowIcon::Procedure,
                    label: "Procedures".to_string(),
                    total_count: connection.procedures.len(),
                    filtered_count: if has_search {
                        filtered_procedures.len()
                    } else {
                        connection.procedures.len()
                    },
                    is_expanded: procedures_expanded,
                    depth,
                    action: SidebarSectionAction::ConnectionSection {
                        conn_id: connection.id,
                        section: "procedures",
                    },
                    context_menu_section: Some("procedures"),
                },
                has_search,
            );

            if include_procedures_section && procedures_expanded {
                if connection.procedures_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-procedures-{}", connection.id),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    let procedure_names: Vec<&String> = if has_search {
                        filtered_procedures.into_iter().collect()
                    } else {
                        connection.procedures.iter().collect()
                    };

                    for procedure_name in procedure_names {
                        let object_schema =
                            self.current_schema_for_virtual_rows(database_name.as_deref());
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!("procedure-{}-{}", connection.id, procedure_name),
                            icon: SidebarRowIcon::Procedure,
                            label: procedure_name.clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Procedure {
                                conn_id: connection.id,
                                procedure_name: procedure_name.clone(),
                                object_schema,
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        let include_queries_section = Self::push_section_row(
            rows,
            SectionRow {
                element_id: format!("queries-header-{}", connection.id),
                icon: SidebarRowIcon::Query,
                label: "Queries".to_string(),
                total_count: connection.queries.len(),
                filtered_count: filtered_queries.len(),
                is_expanded: queries_expanded,
                depth,
                action: SidebarSectionAction::ConnectionSection {
                    conn_id: connection.id,
                    section: "queries",
                },
                context_menu_section: Some("queries"),
            },
            has_search,
        );

        if include_queries_section && queries_expanded {
            for query in filtered_queries {
                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                    element_id: format!("query-{}-{}", connection.id, query.id),
                    icon: SidebarRowIcon::Query,
                    label: query.name.clone(),
                    depth: depth + 1,
                    kind: SidebarLeafKind::Query {
                        conn_id: connection.id,
                        query_id: query.id,
                        query_name: query.name.clone(),
                    },
                }));
                *matched_leaf_rows += 1;
            }
        }
    }

    fn append_database_level_objects_rows(
        &self,
        connection: &ConnectionEntry,
        database: &SidebarDatabaseInfo,
        depth: usize,
        rows: &mut Vec<SidebarVirtualRow>,
        matched_leaf_rows: &mut usize,
    ) {
        let Some(schema) = &database.schema else {
            return;
        };

        let has_search = !self.search_query.is_empty();
        let filtered_tables = self.filter_by_search(&schema.tables);
        let filtered_views = self.filter_by_search(&schema.views);
        let filtered_materialized_views = self.filter_by_search(&schema.materialized_views);
        let filtered_triggers = self.filter_by_search(&schema.triggers);
        let filtered_functions = self.filter_by_search(&schema.functions);
        let filtered_procedures = self.filter_by_search(&schema.procedures);
        let filtered_queries: Vec<_> = connection
            .queries
            .iter()
            .filter(|query| self.matches_search(&query.name))
            .collect();

        let tables_expanded = schema.tables_expanded || (has_search && !filtered_tables.is_empty());
        let views_expanded = schema.views_expanded || (has_search && !filtered_views.is_empty());
        let mat_views_expanded = schema.materialized_views_expanded
            || (has_search && !filtered_materialized_views.is_empty());
        let triggers_expanded =
            schema.triggers_expanded || (has_search && !filtered_triggers.is_empty());
        let functions_expanded =
            schema.functions_expanded || (has_search && !filtered_functions.is_empty());
        let procedures_expanded =
            schema.procedures_expanded || (has_search && !filtered_procedures.is_empty());
        let queries_expanded =
            connection.queries_expanded || (has_search && !filtered_queries.is_empty());

        let database_name = database.name.clone();

        let include_tables_section = Self::push_section_row(
            rows,
            SectionRow {
                element_id: format!("tables-header-{}-{}", connection.id, database_name),
                icon: SidebarRowIcon::Table,
                label: "Tables".to_string(),
                total_count: schema.tables.len(),
                filtered_count: filtered_tables.len(),
                is_expanded: tables_expanded,
                depth,
                action: SidebarSectionAction::DatabaseSection {
                    conn_id: connection.id,
                    database_name: database_name.clone(),
                    section: "tables",
                },
                context_menu_section: Some("tables"),
            },
            has_search,
        );

        if include_tables_section && tables_expanded {
            if schema.tables_loading {
                rows.push(SidebarVirtualRow::Loading(LoadingRow {
                    element_id: format!("loading-tables-{}-{}", connection.id, database_name),
                    text: "Loading...".to_string(),
                    depth: depth + 1,
                }));
            } else {
                for table_name in &filtered_tables {
                    rows.push(SidebarVirtualRow::Leaf(LeafRow {
                        element_id: format!(
                            "table-{}-{}-{}",
                            connection.id, database_name, table_name
                        ),
                        icon: SidebarRowIcon::Table,
                        label: (*table_name).clone(),
                        depth: depth + 1,
                        kind: SidebarLeafKind::Table {
                            conn_id: connection.id,
                            open_table_name: (*table_name).clone(),
                            menu_table_name: (*table_name).clone(),
                            object_schema: self
                                .current_schema_for_virtual_rows(Some(&database_name)),
                            database_name: Some(database_name.clone()),
                        },
                    }));
                    *matched_leaf_rows += 1;
                }
            }
        }

        if connection.object_capabilities.supports_views {
            let include_views_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("views-header-{}-{}", connection.id, database_name),
                    icon: SidebarRowIcon::View,
                    label: "Views".to_string(),
                    total_count: schema.views.len(),
                    filtered_count: filtered_views.len(),
                    is_expanded: views_expanded,
                    depth,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        section: "views",
                    },
                    context_menu_section: Some("views"),
                },
                has_search,
            );

            if include_views_section && views_expanded {
                if schema.views_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-views-{}-{}", connection.id, database_name),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for view_name in &filtered_views {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "view-{}-{}-{}",
                                connection.id, database_name, view_name
                            ),
                            icon: SidebarRowIcon::View,
                            label: (*view_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::View {
                                conn_id: connection.id,
                                open_view_name: (*view_name).clone(),
                                menu_view_name: (*view_name).clone(),
                                object_schema: self
                                    .current_schema_for_virtual_rows(Some(&database_name)),
                                database_name: Some(database_name.clone()),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_materialized_views {
            let include_materialized_views_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("matviews-header-{}-{}", connection.id, database_name),
                    icon: SidebarRowIcon::MaterializedView,
                    label: "Materialized Views".to_string(),
                    total_count: schema.materialized_views.len(),
                    filtered_count: filtered_materialized_views.len(),
                    is_expanded: mat_views_expanded,
                    depth,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        section: "materialized_views",
                    },
                    context_menu_section: Some("materialized_views"),
                },
                has_search,
            );

            if include_materialized_views_section && mat_views_expanded {
                if schema.materialized_views_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-matviews-{}-{}", connection.id, database_name),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for view_name in &filtered_materialized_views {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "matview-{}-{}-{}",
                                connection.id, database_name, view_name
                            ),
                            icon: SidebarRowIcon::MaterializedView,
                            label: (*view_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::MaterializedView {
                                conn_id: connection.id,
                                open_view_name: (*view_name).clone(),
                                menu_view_name: (*view_name).clone(),
                                database_name: Some(database_name.clone()),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_triggers {
            let include_triggers_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("triggers-header-{}-{}", connection.id, database_name),
                    icon: SidebarRowIcon::Trigger,
                    label: "Triggers".to_string(),
                    total_count: schema.triggers.len(),
                    filtered_count: filtered_triggers.len(),
                    is_expanded: triggers_expanded,
                    depth,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        section: "triggers",
                    },
                    context_menu_section: Some("triggers"),
                },
                has_search,
            );

            if include_triggers_section && triggers_expanded {
                if schema.triggers_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!("loading-triggers-{}-{}", connection.id, database_name),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for trigger_name in &filtered_triggers {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "trigger-{}-{}-{}",
                                connection.id, database_name, trigger_name
                            ),
                            icon: SidebarRowIcon::Trigger,
                            label: (*trigger_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Trigger {
                                conn_id: connection.id,
                                trigger_name: (*trigger_name).clone(),
                                object_schema: self
                                    .current_schema_for_virtual_rows(Some(&database_name)),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_functions {
            let include_functions_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("functions-header-{}-{}", connection.id, database_name),
                    icon: SidebarRowIcon::Function,
                    label: "Functions".to_string(),
                    total_count: schema.functions.len(),
                    filtered_count: filtered_functions.len(),
                    is_expanded: functions_expanded,
                    depth,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        section: "functions",
                    },
                    context_menu_section: Some("functions"),
                },
                has_search,
            );

            if include_functions_section && functions_expanded {
                if schema.functions_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!(
                            "loading-functions-{}-{}",
                            connection.id, database_name
                        ),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for function_name in &filtered_functions {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "function-{}-{}-{}",
                                connection.id, database_name, function_name
                            ),
                            icon: SidebarRowIcon::Function,
                            label: (*function_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Function {
                                conn_id: connection.id,
                                function_name: (*function_name).clone(),
                                object_schema: self
                                    .current_schema_for_virtual_rows(Some(&database_name)),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        if connection.object_capabilities.supports_procedures {
            let include_procedures_section = Self::push_section_row(
                rows,
                SectionRow {
                    element_id: format!("procedures-header-{}-{}", connection.id, database_name),
                    icon: SidebarRowIcon::Procedure,
                    label: "Procedures".to_string(),
                    total_count: schema.procedures.len(),
                    filtered_count: filtered_procedures.len(),
                    is_expanded: procedures_expanded,
                    depth,
                    action: SidebarSectionAction::DatabaseSection {
                        conn_id: connection.id,
                        database_name: database_name.clone(),
                        section: "procedures",
                    },
                    context_menu_section: Some("procedures"),
                },
                has_search,
            );

            if include_procedures_section && procedures_expanded {
                if schema.procedures_loading {
                    rows.push(SidebarVirtualRow::Loading(LoadingRow {
                        element_id: format!(
                            "loading-procedures-{}-{}",
                            connection.id, database_name
                        ),
                        text: "Loading...".to_string(),
                        depth: depth + 1,
                    }));
                } else {
                    for procedure_name in &filtered_procedures {
                        rows.push(SidebarVirtualRow::Leaf(LeafRow {
                            element_id: format!(
                                "procedure-{}-{}-{}",
                                connection.id, database_name, procedure_name
                            ),
                            icon: SidebarRowIcon::Procedure,
                            label: (*procedure_name).clone(),
                            depth: depth + 1,
                            kind: SidebarLeafKind::Procedure {
                                conn_id: connection.id,
                                procedure_name: (*procedure_name).clone(),
                                object_schema: self
                                    .current_schema_for_virtual_rows(Some(&database_name)),
                            },
                        }));
                        *matched_leaf_rows += 1;
                    }
                }
            }
        }

        let include_queries_section = Self::push_section_row(
            rows,
            SectionRow {
                element_id: format!("queries-header-{}", connection.id),
                icon: SidebarRowIcon::Query,
                label: "Queries".to_string(),
                total_count: connection.queries.len(),
                filtered_count: filtered_queries.len(),
                is_expanded: queries_expanded,
                depth,
                action: SidebarSectionAction::ConnectionSection {
                    conn_id: connection.id,
                    section: "queries",
                },
                context_menu_section: Some("queries"),
            },
            has_search,
        );

        if include_queries_section && queries_expanded {
            for query in filtered_queries {
                rows.push(SidebarVirtualRow::Leaf(LeafRow {
                    element_id: format!("query-{}-{}", connection.id, query.id),
                    icon: SidebarRowIcon::Query,
                    label: query.name.clone(),
                    depth: depth + 1,
                    kind: SidebarLeafKind::Query {
                        conn_id: connection.id,
                        query_id: query.id,
                        query_name: query.name.clone(),
                    },
                }));
                *matched_leaf_rows += 1;
            }
        }
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
        let row_selected_background = theme.list_active;
        let row_hover_background = theme.list_hover;
        let subtle_action_border = theme.border.opacity(0.6);
        let is_selected = self.selected_connection == Some(conn_id);
        let db_icon = self.db_icon_for_virtual_rows(&db_type);
        let db_logo = self.db_logo_for_virtual_rows(&db_type);
        let conn_id_for_row_click = conn_id;
        let conn_id_for_right_click = conn_id;
        let conn_id_for_new_query = conn_id;
        let conn_id_for_disconnect = conn_id;
        let conn_id_for_connect = conn_id;

        h_flex()
            .id(SharedString::from(format!("conn-{conn_id}")))
            .group("conn-row")
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .px_2()
            .gap_1p5()
            .items_center()
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
                                .hover(|el| el.bg(theme.accent.opacity(0.15)))
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
                                .hover(|el| el.bg(theme.danger.opacity(0.15)))
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
                    })
                    .when(!is_connected && !is_connecting, |this| {
                        this.child(
                            div()
                                .id(SharedString::from(format!("conn-connect-{conn_id}")))
                                .size_4()
                                .border_1()
                                .border_color(subtle_action_border)
                                .bg(theme.primary)
                                .flex()
                                .items_center()
                                .justify_center()
                                .cursor_pointer()
                                .hover(|el| el.bg(theme.primary.opacity(0.9)))
                                .on_click(cx.listener(move |_this, _: &ClickEvent, _, cx| {
                                    cx.stop_propagation();
                                    cx.emit(ConnectionSidebarEvent::Connect(conn_id_for_connect));
                                }))
                                .child(
                                    Icon::new(IconName::Plus)
                                        .size_3()
                                        .text_color(theme.primary_foreground),
                                ),
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
        h_flex()
            .id(SharedString::from(format!(
                "db-node-{conn_id}-{database_name}"
            )))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(px(20.0))
            .pr_2()
            .gap_1p5()
            .items_center()
            .text_xs()
            .text_color(if is_expanded || is_active {
                theme.muted_foreground
            } else {
                theme.muted_foreground.opacity(0.5)
            })
            .cursor_pointer()
            .hover(|this| this.bg(theme.list_hover))
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
        h_flex()
            .id(SharedString::from(format!(
                "schema-node-{}-{}",
                row.conn_id, row.database_name
            )))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(px(32.0))
            .pr_2()
            .gap_1p5()
            .items_center()
            .text_xs()
            .text_color(if row.is_expanded {
                theme.muted_foreground
            } else {
                theme.muted_foreground.opacity(0.5)
            })
            .cursor_pointer()
            .hover(|this| this.bg(theme.list_hover))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                if row.has_database_schema {
                    this.toggle_db_section(row.conn_id, &row.database_name, "schema", cx);
                } else {
                    this.toggle_schema_expand(row.conn_id, cx);
                }
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
        let theme = cx.theme();
        let has_search = !self.search_query.is_empty();
        let indent = px(8.0 + depth as f32 * 12.0);
        let text_color = if is_expanded {
            theme.foreground
        } else {
            theme.muted_foreground
        };
        let icon = self.sidebar_row_icon(&row_icon, theme.muted_foreground);

        h_flex()
            .id(SharedString::from(element_id))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(indent)
            .pr_2()
            .gap_1p5()
            .items_center()
            .text_xs()
            .text_color(text_color)
            .cursor_pointer()
            .hover(|this| this.bg(theme.list_hover))
            .on_click(
                cx.listener(move |this, _: &ClickEvent, _, cx| match &action_for_click {
                    SidebarSectionAction::RedisDatabases { conn_id } => {
                        this.toggle_redis_databases_expand(*conn_id, cx)
                    }
                    SidebarSectionAction::ConnectionSection { conn_id, section } => {
                        match *section {
                            "tables" => this.toggle_tables_expand(*conn_id, cx),
                            "views" => this.toggle_views_expand(*conn_id, cx),
                            "materialized_views" => {
                                this.toggle_materialized_views_expand(*conn_id, cx)
                            }
                            "triggers" => this.toggle_triggers_expand(*conn_id, cx),
                            "functions" => this.toggle_functions_expand(*conn_id, cx),
                            "procedures" => this.toggle_procedures_expand(*conn_id, cx),
                            "queries" => this.toggle_queries_expand(*conn_id, cx),
                            _ => {}
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
                    } => {
                        if let Some(database_name) = database_name {
                            this.toggle_db_schema_group_expand(
                                *conn_id,
                                database_name,
                                schema_name,
                                cx,
                            );
                        } else {
                            this.toggle_schema_group_expand(*conn_id, schema_name, cx);
                        }
                    }
                    SidebarSectionAction::SchemaGroupSection {
                        conn_id,
                        database_name,
                        schema_name,
                        section,
                    } => {
                        if let Some(database_name) = database_name {
                            this.toggle_db_schema_section_expand(
                                *conn_id,
                                database_name,
                                schema_name,
                                section,
                                cx,
                            );
                        } else {
                            this.toggle_schema_section_expand(*conn_id, schema_name, section, cx);
                        }
                    }
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    let conn_id = match &action_for_context {
                        SidebarSectionAction::RedisDatabases { conn_id }
                        | SidebarSectionAction::ConnectionSection { conn_id, .. }
                        | SidebarSectionAction::DatabaseSection { conn_id, .. }
                        | SidebarSectionAction::SchemaGroup { conn_id, .. }
                        | SidebarSectionAction::SchemaGroupSection { conn_id, .. } => *conn_id,
                    };

                    if let Some(section) = context_menu_section {
                        cx.stop_propagation();
                        this.show_section_context_menu(
                            conn_id,
                            section,
                            event.position,
                            window,
                            cx,
                        );
                    }
                }),
            )
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
        let theme = cx.theme();
        let indent = px(8.0 + depth as f32 * 12.0);
        let is_active = self.is_leaf_item_active(&element_id);
        let text_color = if is_active {
            theme.foreground
        } else {
            theme.muted_foreground
        };
        let icon = self.sidebar_row_icon(&row_icon, theme.muted_foreground);
        let element_id_for_click = element_id.clone();

        h_flex()
            .id(SharedString::from(element_id))
            .w_full()
            .h(px(SIDEBAR_ROW_HEIGHT))
            .pl(indent)
            .pr_2()
            .gap_1p5()
            .items_center()
            .text_sm()
            .text_color(text_color)
            .cursor_pointer()
            .hover(|this| this.bg(theme.list_hover))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                this.set_active_leaf_item(Some(element_id_for_click.clone()), cx);
                match &kind_for_click {
                    SidebarLeafKind::Table {
                        conn_id,
                        open_table_name,
                        database_name,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenTable {
                        connection_id: *conn_id,
                        table_name: open_table_name.clone(),
                        database_name: database_name.clone(),
                    }),
                    SidebarLeafKind::View {
                        conn_id,
                        open_view_name,
                        database_name,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenView {
                        connection_id: *conn_id,
                        view_name: open_view_name.clone(),
                        database_name: database_name.clone(),
                    }),
                    SidebarLeafKind::MaterializedView {
                        conn_id,
                        open_view_name,
                        database_name,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenView {
                        connection_id: *conn_id,
                        view_name: open_view_name.clone(),
                        database_name: database_name.clone(),
                    }),
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
                    } => cx.emit(ConnectionSidebarEvent::OpenFunction {
                        connection_id: *conn_id,
                        function_name: function_name.clone(),
                        object_schema: object_schema.clone(),
                    }),
                    SidebarLeafKind::Procedure {
                        conn_id,
                        procedure_name,
                        object_schema,
                    } => cx.emit(ConnectionSidebarEvent::OpenProcedure {
                        connection_id: *conn_id,
                        procedure_name: procedure_name.clone(),
                        object_schema: object_schema.clone(),
                    }),
                    SidebarLeafKind::Query {
                        conn_id,
                        query_id,
                        query_name,
                    } => cx.emit(ConnectionSidebarEvent::OpenSavedQuery {
                        connection_id: *conn_id,
                        query_id: *query_id,
                        query_name: query_name.clone(),
                    }),
                    SidebarLeafKind::RedisDatabase {
                        conn_id,
                        database_index,
                        ..
                    } => cx.emit(ConnectionSidebarEvent::OpenRedisDatabase {
                        connection_id: *conn_id,
                        database_index: *database_index,
                    }),
                }
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    match &kind_for_context {
                        SidebarLeafKind::Table {
                            conn_id,
                            menu_table_name,
                            object_schema,
                            database_name,
                            ..
                        } => this.show_table_context_menu(
                            *conn_id,
                            menu_table_name.clone(),
                            object_schema.clone(),
                            database_name.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::View {
                            conn_id,
                            menu_view_name,
                            object_schema,
                            database_name,
                            ..
                        } => this.show_view_context_menu(
                            *conn_id,
                            menu_view_name.clone(),
                            object_schema.clone(),
                            database_name.clone(),
                            event.position,
                            window,
                            cx,
                        ),
                        SidebarLeafKind::MaterializedView {
                            conn_id,
                            menu_view_name,
                            database_name,
                            ..
                        } => this.show_materialized_view_context_menu(
                            *conn_id,
                            menu_view_name.clone(),
                            database_name.clone(),
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
                        } => this.show_procedure_context_menu(
                            *conn_id,
                            procedure_name.clone(),
                            object_schema.clone(),
                            event.position,
                            window,
                            cx,
                        ),
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
                    }
                }),
            )
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
        let indent = px(8.0 + row.depth as f32 * 12.0);
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
            .px_3()
            .items_center()
            .text_sm()
            .text_color(theme.muted_foreground)
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
            .border_color(theme.border)
            .font_family(cx.theme().font_family.clone())
            // Search input - only show when there are connections
            .when_some(search_input_state, |this, input_state| {
                this.child(
                    div()
                        .w_full()
                        .px_2()
                        .py_1()
                        .border_b_1()
                        .border_color(theme.border)
                        .child(
                            Input::new(&input_state)
                                .small()
                                .w_full()
                                .appearance(false)
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
                                .flex_grow()
                                .size_full()
                                .pr(px(16.0))
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
                                .child(
                                    Scrollbar::vertical(&self.scroll_handle)
                                        .scrollbar_show(ScrollbarShow::Always),
                                ),
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
