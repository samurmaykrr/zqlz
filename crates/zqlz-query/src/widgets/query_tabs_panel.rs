//! Query tabs panel
//!
//! A panel that manages multiple query editors with tabs.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_services::SchemaService;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex, v_flex, ActiveTheme,
};

use super::{DiagnosticInfo, EditorObjectType, QueryEditor, QueryEditorEvent};

/// Events emitted by the query tabs panel
#[derive(Clone, Debug)]
pub enum QueryTabsPanelEvent {
    /// User requested to execute a query (entire content)
    ExecuteQuery {
        sql: String,
        connection_id: Option<Uuid>,
        editor_index: usize,
    },
    /// User requested to execute selection or current statement
    ExecuteSelection {
        sql: String,
        connection_id: Option<Uuid>,
        editor_index: usize,
    },
    /// User requested to explain a query (entire content)
    ExplainQuery {
        sql: String,
        connection_id: Option<Uuid>,
        editor_index: usize,
    },
    /// User requested to explain selection or current statement
    ExplainSelection {
        sql: String,
        connection_id: Option<Uuid>,
        editor_index: usize,
    },
    /// User requested to cancel the currently executing query
    CancelQuery { editor_index: usize },
    /// User wants to add a new connection
    AddConnection,
    /// User requested to save a database object (view, procedure, function, trigger)
    SaveObject {
        connection_id: Uuid,
        object_type: EditorObjectType,
        definition: String,
        editor_index: usize,
    },
    /// User requested to preview DDL for a database object
    PreviewDdl {
        object_type: EditorObjectType,
        definition: String,
        editor_index: usize,
    },
    /// User requested to save the query (Cmd+S / Ctrl+S)
    SaveQuery {
        saved_query_id: Option<Uuid>,
        connection_id: Option<Uuid>,
        sql: String,
        editor_index: usize,
    },
    /// Diagnostics changed in an editor (for Problems panel)
    DiagnosticsChanged {
        diagnostics: Vec<DiagnosticInfo>,
        editor_index: usize,
    },
    /// Active editor changed (for scoping diagnostics and results)
    ActiveEditorChanged { editor_index: Option<usize> },
    /// User requested to switch connection
    SwitchConnection {
        connection_id: Uuid,
        editor_index: usize,
    },
    /// User requested to switch database
    SwitchDatabase {
        database_name: String,
        editor_index: usize,
    },
}

/// A panel that manages multiple query editor tabs
pub struct QueryTabsPanel {
    focus_handle: FocusHandle,
    query_editors: Vec<Entity<QueryEditor>>,
    active_editor_index: Option<usize>,
    show_welcome: bool,
    schema_service: Option<Arc<SchemaService>>,
    _subscriptions: Vec<Subscription>,
}

impl QueryTabsPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        tracing::debug!("QueryTabsPanel::new() - initializing new instance");
        Self {
            focus_handle: cx.focus_handle(),
            query_editors: Vec::new(),
            active_editor_index: None,
            show_welcome: true,
            schema_service: None,
            _subscriptions: Vec::new(),
        }
    }

    /// Set the schema service (required before creating queries)
    pub fn set_schema_service(&mut self, schema_service: Arc<SchemaService>) {
        self.schema_service = Some(schema_service);
    }

    /// Get a weak reference to an editor by index
    pub fn get_editor(&self, index: usize) -> Option<WeakEntity<QueryEditor>> {
        self.query_editors.get(index).map(|e| e.downgrade())
    }

    /// Create a new query tab
    pub fn new_query(
        &mut self,
        connection_id: Option<Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(connection_id = ?connection_id, "Creating new query");
        let name = format!("Query {}", self.query_editors.len() + 1);

        // Get schema_service from stored reference
        let schema_service = match &self.schema_service {
            Some(service) => service.clone(),
            None => {
                tracing::error!("SchemaService not set on QueryTabsPanel");
                return;
            }
        };

        let editor = cx.new(|cx| QueryEditor::new(name, connection_id, schema_service, window, cx));

        let editor_index = self.query_editors.len();
        let subscription = cx.subscribe(&editor, {
            move |_this, _editor, event: &QueryEditorEvent, cx| match event {
                QueryEditorEvent::ExecuteQuery { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExecuteQuery {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExecuteSelection { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExecuteSelection {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExplainQuery { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExplainQuery {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExplainSelection { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExplainSelection {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::CancelQuery => {
                    cx.emit(QueryTabsPanelEvent::CancelQuery { editor_index });
                }
                QueryEditorEvent::SaveObject {
                    connection_id,
                    object_type,
                    definition,
                } => {
                    cx.emit(QueryTabsPanelEvent::SaveObject {
                        connection_id: *connection_id,
                        object_type: object_type.clone(),
                        definition: definition.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::PreviewDdl {
                    object_type,
                    definition,
                } => {
                    cx.emit(QueryTabsPanelEvent::PreviewDdl {
                        object_type: object_type.clone(),
                        definition: definition.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::SaveQuery {
                    saved_query_id,
                    connection_id,
                    sql,
                } => {
                    cx.emit(QueryTabsPanelEvent::SaveQuery {
                        saved_query_id: *saved_query_id,
                        connection_id: *connection_id,
                        sql: sql.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::DiagnosticsChanged { diagnostics } => {
                    cx.emit(QueryTabsPanelEvent::DiagnosticsChanged {
                        diagnostics: diagnostics.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::SwitchConnection { connection_id } => {
                    cx.emit(QueryTabsPanelEvent::SwitchConnection {
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::SwitchDatabase { database_name } => {
                    cx.emit(QueryTabsPanelEvent::SwitchDatabase {
                        database_name: database_name.clone(),
                        editor_index,
                    });
                }
            }
        });
        self._subscriptions.push(subscription);

        self.query_editors.push(editor);
        self.active_editor_index = Some(self.query_editors.len() - 1);
        self.show_welcome = false;
        cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
            editor_index: self.active_editor_index,
        });
        cx.emit(PanelEvent::LayoutChanged);
        cx.notify();
    }

    /// Open a query tab with a specific SQL content
    pub fn open_table_query(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name = table_name.clone();

        // Get schema_service from stored reference
        let schema_service = match &self.schema_service {
            Some(service) => service.clone(),
            None => {
                tracing::error!("SchemaService not set on QueryTabsPanel");
                return;
            }
        };

        let editor =
            cx.new(|cx| QueryEditor::new(name, Some(connection_id), schema_service, window, cx));

        let sql = format!("SELECT * FROM {} LIMIT 100;", table_name);
        editor.update(cx, |editor, cx| {
            editor.set_content(sql, window, cx);
        });

        let editor_index = self.query_editors.len();
        let subscription = cx.subscribe(&editor, {
            move |_this, _editor, event: &QueryEditorEvent, cx| match event {
                QueryEditorEvent::ExecuteQuery { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExecuteQuery {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExecuteSelection { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExecuteSelection {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExplainQuery { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExplainQuery {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::ExplainSelection { sql, connection_id } => {
                    cx.emit(QueryTabsPanelEvent::ExplainSelection {
                        sql: sql.clone(),
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::CancelQuery => {
                    cx.emit(QueryTabsPanelEvent::CancelQuery { editor_index });
                }
                QueryEditorEvent::SaveObject {
                    connection_id,
                    object_type,
                    definition,
                } => {
                    cx.emit(QueryTabsPanelEvent::SaveObject {
                        connection_id: *connection_id,
                        object_type: object_type.clone(),
                        definition: definition.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::PreviewDdl {
                    object_type,
                    definition,
                } => {
                    cx.emit(QueryTabsPanelEvent::PreviewDdl {
                        object_type: object_type.clone(),
                        definition: definition.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::SaveQuery {
                    saved_query_id,
                    connection_id,
                    sql,
                } => {
                    cx.emit(QueryTabsPanelEvent::SaveQuery {
                        saved_query_id: *saved_query_id,
                        connection_id: *connection_id,
                        sql: sql.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::DiagnosticsChanged { diagnostics } => {
                    cx.emit(QueryTabsPanelEvent::DiagnosticsChanged {
                        diagnostics: diagnostics.clone(),
                        editor_index,
                    });
                }
                QueryEditorEvent::SwitchConnection { connection_id } => {
                    cx.emit(QueryTabsPanelEvent::SwitchConnection {
                        connection_id: *connection_id,
                        editor_index,
                    });
                }
                QueryEditorEvent::SwitchDatabase { database_name } => {
                    cx.emit(QueryTabsPanelEvent::SwitchDatabase {
                        database_name: database_name.clone(),
                        editor_index,
                    });
                }
            }
        });
        self._subscriptions.push(subscription);

        self.query_editors.push(editor);
        self.active_editor_index = Some(self.query_editors.len() - 1);
        self.show_welcome = false;
        cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
            editor_index: self.active_editor_index,
        });
        cx.emit(PanelEvent::LayoutChanged);
        cx.notify();
    }

    /// Close a query tab
    pub fn close_query(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.query_editors.len() {
            self.query_editors.remove(index);
            if self.query_editors.is_empty() {
                self.active_editor_index = None;
                self.show_welcome = true;
            } else if let Some(active) = self.active_editor_index {
                if active >= self.query_editors.len() {
                    self.active_editor_index = Some(self.query_editors.len() - 1);
                } else if active > index {
                    self.active_editor_index = Some(active - 1);
                }
            }
            cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
                editor_index: self.active_editor_index,
            });
            cx.emit(PanelEvent::LayoutChanged);
            cx.notify();
        }
    }

    /// Select a query tab
    pub fn select_query(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.query_editors.len() {
            self.active_editor_index = Some(index);
            // Emit event so MainView can update WorkspaceState
            cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
                editor_index: Some(index),
            });
            cx.notify();
        }
    }

    /// Get the active editor
    pub fn active_editor(&self) -> Option<Entity<QueryEditor>> {
        self.active_editor_index
            .and_then(|idx| self.query_editors.get(idx).cloned())
    }

    /// Get the active editor index
    pub fn active_editor_index(&self) -> Option<usize> {
        self.active_editor_index
    }

    /// Get an editor by index
    pub fn editor_at(&self, index: usize) -> Option<Entity<QueryEditor>> {
        self.query_editors.get(index).cloned()
    }

    /// Set executing state for an editor
    pub fn set_editor_executing(&self, index: usize, executing: bool, cx: &mut App) {
        if let Some(editor) = self.query_editors.get(index) {
            editor.update(cx, |editor, cx| {
                editor.set_executing(executing, cx);
            });
        }
    }

    /// Execute the entire query in the active editor
    pub fn execute_query(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(editor) = self.active_editor() {
            editor.update(cx, |editor, cx| {
                editor.emit_execute_query(cx);
            });
        }
    }

    /// Execute the selected text or full query in the active editor
    pub fn execute_selection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(editor) = self.active_editor() {
            editor.update(cx, |editor, cx| {
                editor.emit_execute_selection(cx);
            });
        }
    }

    /// Activate the next tab
    pub fn activate_next_tab(&mut self, cx: &mut Context<Self>) {
        if let Some(active) = self.active_editor_index {
            let next_index = (active + 1) % self.query_editors.len();
            self.select_query(next_index, cx);
        }
    }

    /// Activate the previous tab
    pub fn activate_prev_tab(&mut self, cx: &mut Context<Self>) {
        if let Some(active) = self.active_editor_index {
            let prev_index = if active == 0 {
                self.query_editors.len().saturating_sub(1)
            } else {
                active - 1
            };
            self.select_query(prev_index, cx);
        }
    }

    /// Close the active tab
    pub fn close_active_tab(&mut self, cx: &mut Context<Self>) {
        if let Some(active) = self.active_editor_index {
            self.close_query(active, cx);
        }
    }

    /// Close all tabs except the active one
    pub fn close_other_tabs(&mut self, cx: &mut Context<Self>) {
        if let Some(active_idx) = self.active_editor_index {
            // Keep only the active editor
            let active_editor = self.query_editors.get(active_idx).cloned();
            if let Some(editor) = active_editor {
                self.query_editors = vec![editor];
                self.active_editor_index = Some(0);
                cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
                    editor_index: self.active_editor_index,
                });
                cx.emit(PanelEvent::LayoutChanged);
                cx.notify();
            }
        }
    }

    /// Close all tabs to the right of the active one
    pub fn close_tabs_to_right(&mut self, cx: &mut Context<Self>) {
        if let Some(active_idx) = self.active_editor_index {
            // Remove all editors after the active index
            self.query_editors.truncate(active_idx + 1);
            cx.emit(PanelEvent::LayoutChanged);
            cx.notify();
        }
    }

    /// Close all tabs
    pub fn close_all_tabs(&mut self, cx: &mut Context<Self>) {
        self.query_editors.clear();
        self.active_editor_index = None;
        self.show_welcome = true;
        cx.emit(QueryTabsPanelEvent::ActiveEditorChanged {
            editor_index: self.active_editor_index,
        });
        cx.emit(PanelEvent::LayoutChanged);
        cx.notify();
    }

    /// Activate a specific tab by index (1-based for user, 0-based internally)
    pub fn activate_tab_by_number(&mut self, number: usize, cx: &mut Context<Self>) {
        if number > 0 && number <= self.query_editors.len() {
            self.select_query(number - 1, cx);
        }
    }

    /// Get the number of open tabs
    pub fn tab_count(&self) -> usize {
        self.query_editors.len()
    }

    /// Render the tabs bar
    fn render_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .id("tabs-bar")
            .w_full()
            .h(px(32.0))
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .overflow_x_scroll()
            .children(self.query_editors.iter().enumerate().map(|(idx, editor)| {
                let is_active = self.active_editor_index == Some(idx);
                let editor_name = editor.read(cx).name();
                let display_name = if editor_name.is_empty() {
                    format!("Query {}", idx + 1)
                } else {
                    editor_name
                };

                h_flex()
                    .id(SharedString::from(format!("tab-{}", idx)))
                    .h_full()
                    .px_3()
                    .gap_2()
                    .items_center()
                    .cursor_pointer()
                    .border_r_1()
                    .border_color(theme.border)
                    .when(is_active, |this| {
                        this.bg(theme.tab_active)
                            .border_b_2()
                            .border_color(theme.accent)
                    })
                    .when(!is_active, |this| this.hover(|el| el.bg(theme.list_hover)))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.select_query(idx, cx);
                    }))
                    .child(
                        div()
                            .text_sm()
                            .when(is_active, |this| this.font_weight(FontWeight::MEDIUM))
                            .child(display_name),
                    )
                    .child(
                        div()
                            .id(SharedString::from(format!("close-tab-{}", idx)))
                            .size_4()
                            .rounded_sm()
                            .flex()
                            .items_center()
                            .justify_center()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .hover(|el| el.bg(theme.muted).text_color(theme.foreground))
                            .on_click(cx.listener(move |this, _event: &ClickEvent, _, cx| {
                                this.close_query(idx, cx);
                            }))
                            .child("x"),
                    )
            }))
            .child(
                div()
                    .id("new-tab-button")
                    .h_full()
                    .px_2()
                    .flex()
                    .items_center()
                    .cursor_pointer()
                    .text_color(theme.muted_foreground)
                    .hover(|this| this.text_color(theme.foreground))
                    .on_click(cx.listener(|this, _, window, cx| {
                        tracing::info!("New query button clicked");
                        this.new_query(None, window, cx);
                    }))
                    .child("+"),
            )
    }

    /// Render the welcome panel when no editors are open
    fn render_welcome(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_6()
            .p_8()
            .child(
                div()
                    .size_16()
                    .rounded_2xl()
                    .bg(theme.accent)
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(
                        div()
                            .text_2xl()
                            .font_weight(FontWeight::BOLD)
                            .text_color(gpui::white())
                            .child("Z"),
                    ),
            )
            .child(
                v_flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .text_xl()
                            .font_weight(FontWeight::BOLD)
                            .child("Welcome to ZQLZ"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("A modern database IDE for developers"),
                    ),
            )
            .child(
                h_flex()
                    .gap_3()
                    .child(
                        Button::new("welcome-add-connection")
                            .primary()
                            .label("Add Connection")
                            .on_click(cx.listener(|_this, _, _, cx| {
                                cx.emit(QueryTabsPanelEvent::AddConnection);
                            })),
                    )
                    .child(
                        Button::new("welcome-new-query")
                            .outline()
                            .label("New Query")
                            .on_click(cx.listener(|this, _, window, cx| {
                                tracing::info!("New query welcome button clicked");
                                this.new_query(None, window, cx);
                            })),
                    ),
            )
    }
}

impl Render for QueryTabsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        tracing::debug!(
            editors_count = self.query_editors.len(),
            active_index = ?self.active_editor_index,
            show_welcome = self.show_welcome,
            "QueryTabsPanel render"
        );
        let theme = cx.theme();

        v_flex()
            .id("query-tabs-panel")
            .key_context("QueryTabsPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .map(|this| {
                if self.show_welcome && self.query_editors.is_empty() {
                    this.child(self.render_welcome(cx))
                } else {
                    this.child(self.render_tabs(cx)).child(
                        div().flex_1().w_full().overflow_hidden().map(|content| {
                            if let Some(active_idx) = self.active_editor_index {
                                if let Some(editor) = self.query_editors.get(active_idx) {
                                    content.child(editor.clone())
                                } else {
                                    content.child(self.render_welcome(cx))
                                }
                            } else {
                                content.child(self.render_welcome(cx))
                            }
                        }),
                    )
                }
            })
    }
}

impl Focusable for QueryTabsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for QueryTabsPanel {}
impl EventEmitter<QueryTabsPanelEvent> for QueryTabsPanel {}

impl Panel for QueryTabsPanel {
    fn panel_name(&self) -> &'static str {
        "QueryTabsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Query Editor"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
