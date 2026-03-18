//! Connection sidebar panel
//!
//! Displays a tree view of all database connections and their objects.

mod menus;
mod render;
mod state;
pub mod types;

pub use types::*;

use gpui::prelude::FluentBuilder;
use gpui::*;
use menus::state::ContextMenuState;
use std::path::PathBuf;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, Sizable, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
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

    /// Search query for filtering schema objects
    search_query: String,

    /// Input state for search field (lazily initialized)
    search_input_state: Option<Entity<InputState>>,

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
            search_query: String::new(),
            search_input_state: None,
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
                    if let InputEvent::Change = event {
                        if let Some(input) = &this.search_input_state {
                            this.search_query = input.read(cx).value().to_string();
                        }
                        cx.notify();
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
        cx.notify();
    }

    /// Select a connection
    fn select_connection(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.selected_connection = Some(id);
        cx.emit(ConnectionSidebarEvent::Selected(id));
        cx.notify();
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
        cx.notify();
    }

    /// Toggle tables section expand/collapse
    fn toggle_tables_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.tables_expanded = !conn.tables_expanded;
        }
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
            if let Some(index) = conn
                .collapsed_schema_groups
                .iter()
                .position(|group| group == schema_name)
            {
                conn.collapsed_schema_groups.remove(index);
            } else {
                conn.collapsed_schema_groups.push(schema_name.to_string());
            }
        }
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
            if let Some(index) = conn
                .collapsed_schema_section_keys
                .iter()
                .position(|key| key == &section_key)
            {
                conn.collapsed_schema_section_keys.remove(index);
            } else {
                conn.collapsed_schema_section_keys.push(section_key);
            }
        }
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
            if let Some(index) = schema
                .collapsed_schema_groups
                .iter()
                .position(|group| group == schema_name)
            {
                schema.collapsed_schema_groups.remove(index);
            } else {
                schema.collapsed_schema_groups.push(schema_name.to_string());
            }
        }
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
            if let Some(index) = schema
                .collapsed_schema_section_keys
                .iter()
                .position(|key| key == &section_key)
            {
                schema.collapsed_schema_section_keys.remove(index);
            } else {
                schema.collapsed_schema_section_keys.push(section_key);
            }
        }
        cx.notify();
    }

    /// Toggle queries section expand/collapse
    fn toggle_queries_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.queries_expanded = !conn.queries_expanded;
        }
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
        cx.notify();
    }

    /// Toggle schema-level node expand/collapse
    #[allow(dead_code)]
    fn toggle_schema_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.schema_expanded = !conn.schema_expanded;
        }
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
        if self.search_query.is_empty() {
            return true;
        }
        name.to_lowercase()
            .contains(&self.search_query.to_lowercase())
    }

    /// Filter a list of names by the search query
    fn filter_by_search<'a>(&self, items: &'a [String]) -> Vec<&'a String> {
        items
            .iter()
            .filter(|name| self.matches_search(name))
            .collect()
    }

    fn import_dropped_paths(&mut self, paths: &[PathBuf], cx: &mut Context<Self>) {
        if !paths.is_empty() {
            cx.emit(ConnectionSidebarEvent::OpenDroppedPaths(paths.to_vec()));
        }
    }
}

impl Render for ConnectionSidebar {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let connections: Vec<_> = self.connections.clone();
        let has_connections = !connections.is_empty();
        let has_search_query = !self.search_query.is_empty();
        let search_query = self.search_query.clone();

        // Ensure search input is initialized when we have connections
        if has_connections {
            self.ensure_search_input(window, cx);
        }

        // Clone the search input state for use in closures
        let search_input_state = self.search_input_state.clone();

        // Pre-render connection elements to avoid borrow issues in closure
        let connection_elements: Vec<_> = if !connections.is_empty() {
            let total = connections.len();
            connections
                .iter()
                .enumerate()
                .map(|(i, conn)| self.render_connection(conn, i == total - 1, window, cx))
                .collect()
        } else {
            vec![]
        };

        // Pre-calculate if any matches exist (for "no results" message)
        let any_matches = if has_search_query && has_connections {
            connections.iter().any(|conn| {
                let capabilities = conn.object_capabilities;
                conn.is_expanded
                    && conn.is_connected
                    && (conn.tables.iter().any(|t| self.matches_search(t))
                        || (capabilities.supports_views
                            && conn.views.iter().any(|v| self.matches_search(v)))
                        || (capabilities.supports_triggers
                            && conn.triggers.iter().any(|t| self.matches_search(t)))
                        || (capabilities.supports_functions
                            && conn.functions.iter().any(|f| self.matches_search(f)))
                        || (capabilities.supports_procedures
                            && conn.procedures.iter().any(|p| self.matches_search(p)))
                        || conn.queries.iter().any(|q| self.matches_search(&q.name)))
            })
        } else {
            true // No search query, so "matches" is irrelevant
        };

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
                    .overflow_y_scroll()
                    .py_1()
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
                    .when(connections.is_empty(), |this| {
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
                    .when(!connections.is_empty(), |this| {
                        this.children(connection_elements)
                    })
                    // Show "no results" message when searching with no matches
                    .when(
                        has_search_query && has_connections && !any_matches,
                        |this| {
                            this.child(
                                div()
                                    .w_full()
                                    .p_4()
                                    .text_sm()
                                    .text_color(theme.muted_foreground)
                                    .text_center()
                                    .child(format!("No objects match \"{}\"", search_query)),
                            )
                        },
                    ),
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
