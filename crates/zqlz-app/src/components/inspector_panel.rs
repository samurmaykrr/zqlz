//! Inspector Panel
//!
//! A container panel for the right dock that manages multiple inspection tools
//! (Schema, Cell Editor, Key Editor, Query History). Navigation icons are rendered in the status bar.

use gpui::*;
use zqlz_query::widgets::{QueryHistoryPanel, QueryHistoryPanelEvent};
use zqlz_ui::widgets::{
    dock::{Panel, PanelEvent, TitleStyle},
    v_flex, ActiveTheme,
};

use crate::components::RowEditorMode;
use crate::components::{CellEditorPanel, KeyValueEditorPanel, SchemaDetailsPanel};

/// The currently active view in the inspector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectorView {
    Schema,
    CellEditor,
    KeyEditor,
    QueryHistory,
    // TODO: Templates
    // TODO: Projects
}

/// Events emitted by the InspectorPanel
#[derive(Debug, Clone)]
pub enum InspectorPanelEvent {
    /// The active view was changed
    ViewChanged(InspectorView),
    /// User clicked on a query history entry to load it
    OpenQuery { sql: String },
    /// User requested to clear all query history
    ClearHistory,
}

/// The main inspector panel component
pub struct InspectorPanel {
    focus_handle: FocusHandle,
    active_view: InspectorView,

    // Child panels
    schema_panel: Entity<SchemaDetailsPanel>,
    cell_editor_panel: Entity<CellEditorPanel>,
    key_editor_panel: Entity<KeyValueEditorPanel>,
    query_history_panel: Entity<QueryHistoryPanel>,
    // TODO: TemplateLibraryPanel
    // TODO: ProjectManagerPanel
    _subscriptions: Vec<Subscription>,
}

impl InspectorPanel {
    pub fn new(
        schema_panel: Entity<SchemaDetailsPanel>,
        cell_editor_panel: Entity<CellEditorPanel>,
        key_editor_panel: Entity<KeyValueEditorPanel>,
        query_history_panel: Entity<QueryHistoryPanel>,
        cx: &mut Context<Self>,
    ) -> Self {
        // Subscribe to QueryHistoryPanel events
        let history_subscription = cx.subscribe(
            &query_history_panel,
            |_this, _panel, event: &QueryHistoryPanelEvent, cx| match event {
                QueryHistoryPanelEvent::OpenQuery { sql } => {
                    cx.emit(InspectorPanelEvent::OpenQuery { sql: sql.clone() });
                }
                QueryHistoryPanelEvent::ClearHistory => {
                    cx.emit(InspectorPanelEvent::ClearHistory);
                }
            },
        );

        Self {
            focus_handle: cx.focus_handle(),
            active_view: InspectorView::Schema,
            schema_panel,
            cell_editor_panel,
            key_editor_panel,
            query_history_panel,
            _subscriptions: vec![history_subscription],
        }
    }

    /// Set the active view
    pub fn set_active_view(&mut self, view: InspectorView, cx: &mut Context<Self>) {
        if self.active_view != view {
            self.active_view = view;
            cx.emit(InspectorPanelEvent::ViewChanged(view));
            cx.notify();
        }
    }

    /// Get the active view
    pub fn active_view(&self) -> InspectorView {
        self.active_view
    }

    /// Get the query history panel
    pub fn query_history_panel(&self) -> &Entity<QueryHistoryPanel> {
        &self.query_history_panel
    }
}

impl Render for InspectorPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .child(match self.active_view {
                        InspectorView::Schema => self.schema_panel.clone().into_any_element(),
                        InspectorView::CellEditor => {
                            self.cell_editor_panel.clone().into_any_element()
                        }
                        InspectorView::KeyEditor => {
                            self.key_editor_panel.clone().into_any_element()
                        }
                        InspectorView::QueryHistory => {
                            self.query_history_panel.clone().into_any_element()
                        }
                    }),
            )
    }
}

impl Focusable for InspectorPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for InspectorPanel {}
impl EventEmitter<InspectorPanelEvent> for InspectorPanel {}

impl Panel for InspectorPanel {
    fn panel_name(&self) -> &'static str {
        "InspectorPanel"
    }

    fn title(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match self.active_view {
            InspectorView::Schema => "Schema".to_string(),
            InspectorView::CellEditor => "Cell Editor".to_string(),
            InspectorView::KeyEditor => {
                let mode = self.key_editor_panel.read(cx).mode().clone();
                match mode {
                    RowEditorMode::RedisKey => "Key Editor".to_string(),
                    RowEditorMode::SqlRow => "Row Editor".to_string(),
                }
            }
            InspectorView::QueryHistory => "Query History".to_string(),
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
