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
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::PopupMenu,
    typography::{body_small, caption},
    v_flex, ActiveTheme, ConnectionStatus, DatabaseLogo, Icon, IconName, Sizable, StatusDot,
    ZqlzIcon,
};

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
        object_type: String, // "view", "function", "procedure", "trigger"
    },

    // Function events
    /// User wants to open/view a function definition
    OpenFunction {
        connection_id: Uuid,
        function_name: String,
    },

    // Procedure events
    /// User wants to open/view a procedure definition
    OpenProcedure {
        connection_id: Uuid,
        procedure_name: String,
    },

    // Trigger events
    /// User wants to design/edit a trigger definition
    DesignTrigger {
        connection_id: Uuid,
        trigger_name: String,
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
    fn toggle_redis_database_expand(
        &mut self,
        conn_id: Uuid,
        db_index: u16,
        cx: &mut Context<Self>,
    ) {
        let mut should_load = false;
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(db) = conn
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
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.views_expanded = !conn.views_expanded;
        }
        cx.notify();
    }

    /// Toggle materialized views section expand/collapse
    fn toggle_materialized_views_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.materialized_views_expanded = !conn.materialized_views_expanded;
        }
        cx.notify();
    }

    /// Toggle triggers section expand/collapse
    fn toggle_triggers_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.triggers_expanded = !conn.triggers_expanded;
        }
        cx.notify();
    }

    /// Toggle functions section expand/collapse
    fn toggle_functions_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.functions_expanded = !conn.functions_expanded;
        }
        cx.notify();
    }

    /// Toggle procedures section expand/collapse
    fn toggle_procedures_expand(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            conn.procedures_expanded = !conn.procedures_expanded;
        }
        cx.notify();
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
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == conn_id) {
            if let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name) {
                if let Some(schema) = &mut db.schema {
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
        if let Some(conn) = self.connections.iter_mut().find(|c| c.id == id) {
            if let Some(db) = conn.databases.iter_mut().find(|d| d.name == db_name) {
                db.is_expanded = !db.is_expanded;
                if db.is_expanded && db.schema.is_none() && !db.is_active && !db.is_loading {
                    should_load_schema = true;
                }
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
                conn.is_expanded
                    && conn.is_connected
                    && (conn.tables.iter().any(|t| self.matches_search(t))
                        || conn.views.iter().any(|v| self.matches_search(v))
                        || conn.triggers.iter().any(|t| self.matches_search(t))
                        || conn.functions.iter().any(|f| self.matches_search(f))
                        || conn.procedures.iter().any(|p| self.matches_search(p))
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
