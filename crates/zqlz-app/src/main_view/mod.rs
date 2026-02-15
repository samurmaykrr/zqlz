//! Main view for ZQLZ Database IDE
//!
//! # Architecture Overview
//!
//! The ZQLZ application uses a **4-panel dock system** to organize the workspace:
//!
//! ## 1. CENTER DOCK (Main Work Area)
//! The center area contains **multi-tab panels** for:
//! - **Query Editors**: SQL query editors with syntax highlighting, IntelliSense, and execution controls
//! - **Table Viewers**: Display table data with pagination, filtering, and inline editing
//! - **Each tab is closable and scrollable** when there are many tabs open
//!
//! Navigation: These appear as top-level tabs (e.g., "Query 1", "contents", "item_attributes")
//! - Click "+" Query button in title bar to create new query editors
//! - Click table names in left sidebar to open table viewers
//! - All tabs appear at the same level with close buttons via the "..." menu
//!
//! ## 2. LEFT DOCK (Navigation Sidebar)
//! The left sidebar manages database connections and schema navigation:
//! - **Connection list**: Add, connect, disconnect, and manage database connections
//! - **Schema tree**: Browse tables, views, indexes, triggers, functions, and procedures
//! - **Right-click context menus**: Quick actions like "Open Table", "New Query", etc.
//!
//! Toggle: Cmd/Ctrl + B or via toolbar button
//!
//! ## 3. RIGHT DOCK (Inspection & Editing)
//! The right sidebar contains tools for inspecting and editing data:
//! - **Schema Details Tab**: View table structure, columns, indexes, foreign keys, and DDL
//! - **Cell Editor Tab**: Edit individual cell values with multi-line support and NULL handling
//!
//! Toggle: Cmd/Ctrl + Shift + B or via toolbar button
//!
//! ## 4. BOTTOM DOCK (Query Results)
//! The bottom panel displays query execution results:
//! - **Results grid**: Tabular display of query results with sorting and filtering
//! - **Execution stats**: Query duration, row count, and success/error status
//! - **Error messages**: Detailed error information when queries fail
//!
//! Toggle: Cmd/Ctrl + J or via toolbar button

mod connection_handlers;
mod connection_window;
mod event_handlers;
mod query_handlers;
mod saved_query_handlers;
mod tab_menu;
mod table_handlers;
mod table_handlers_utils;
mod ui_components;
mod versioning_handlers;
mod view_handlers;

pub use connection_window::ConnectionWindow;

use gpui::*;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::QueryCancelHandle;
use zqlz_settings::{ThemeModePreference, WorkspaceId, ZqlzSettings, load_layout, save_layout};
use zqlz_ui::widgets::{
    dock::{DockArea, DockAreaState, DockEvent, DockItem, DockPlacement, PanelStyle, PanelView},
    v_flex,
};
use zqlz_versioning::{
    DatabaseObjectType, VersionRepository,
    widgets::{DiffViewer, DiffViewerEvent, VersionHistoryPanel, VersionHistoryPanelEvent},
};

use crate::actions::{
    ExecuteSelection, NewConnection, NewQuery, OpenCommandPalette, OpenSettings, Quit, Refresh,
    RefreshConnectionsList, StopQuery, ToggleBottomPanel, ToggleLeftSidebar, ToggleRightSidebar,
};
use crate::app::AppState;
use crate::components::{
    CellEditorPanel, CommandPalette, CommandPaletteEvent, ConnectionEntry, ConnectionSidebar,
    ConnectionSidebarEvent, InspectorPanel, InspectorView, KeyValueEditorEvent,
    KeyValueEditorPanel, ObjectsPanel, ObjectsPanelEvent, ProblemEntry, ProblemsPanel,
    ProblemsPanelEvent, ProblemSeverity, ProjectManagerEvent, ProjectManagerPanel,
    QueryHistoryPanel, QueryTabsPanel, QueryTabsPanelEvent, ResultsPanel, ResultsPanelEvent,
    SchemaDetailsPanel, SettingsPanel, SettingsPanelEvent, TemplateLibraryEvent, TemplateLibraryPanel,
};
use crate::workspace_state::{
    DiagnosticSeverity, EditorDiagnostic, WorkspaceState, WorkspaceStateEvent,
};
use zqlz_query::{DiagnosticInfo, DiagnosticInfoSeverity};
use zqlz_zed_adapter::{SettingsBridge, ThemeBridge};

pub use tab_menu::TabContextMenuState;

const DOCK_AREA_ID: &str = "main-dock";

/// Version number for the dock layout schema.
///
/// This version is used to invalidate saved layouts when the structure changes.
/// When you make breaking changes to the panel structure (e.g., changing from nested
/// tabs to flat tabs), increment this version to force all users to use the new default layout.
///
/// History:
/// - v1: Initial version with nested QueryTabsPanel
/// - v2: Refactored to flat tab structure (Query editors as top-level tabs)
const DOCK_AREA_VERSION: usize = 2;

/// Events emitted by the main view
#[derive(Clone, Debug)]
pub enum MainViewEvent {
    /// A connection was established
    ConnectionEstablished(Uuid),
    /// A connection was closed
    ConnectionClosed(Uuid),
}

/// Main application view orchestrating the 4-panel dock system.
pub struct MainView {
    focus_handle: FocusHandle,
    /// Centralized workspace state - single source of truth for UI state
    workspace_state: Entity<WorkspaceState>,
    dock_area: Entity<DockArea>,
    connection_sidebar: Entity<ConnectionSidebar>,
    query_tabs_panel: Entity<QueryTabsPanel>,
    query_counter: usize,
    #[allow(dead_code)]
    results_panel: Entity<ResultsPanel>,
    #[allow(dead_code)]
    problems_panel: Entity<ProblemsPanel>,
    #[allow(dead_code)]
    schema_details_panel: Entity<SchemaDetailsPanel>,
    #[allow(dead_code)]
    cell_editor_panel: Entity<CellEditorPanel>,
    #[allow(dead_code)]
    key_value_editor_panel: Entity<KeyValueEditorPanel>,
    inspector_panel: Entity<InspectorPanel>,
    /// Settings panel - stored persistently to listen for settings changes
    settings_panel: Option<Entity<SettingsPanel>>,
    // TODO: TemplateLibraryPanel
    // template_library_panel: Entity<TemplateLibraryPanel>,
    // TODO: ProjectManagerPanel
    // project_manager_panel: Entity<ProjectManagerPanel>,
    objects_panel: Entity<ObjectsPanel>,
    workspace_id: WorkspaceId,
    tab_context_menu: Option<Entity<TabContextMenuState>>,
    query_editors: Vec<WeakEntity<crate::components::QueryEditor>>,
    command_palette: Option<Entity<CommandPalette>>,
    /// DEPRECATED: Use workspace_state for query tracking instead
    /// Running query tasks, keyed by editor index. Dropping a task cancels it.
    /// NOTE: Kept for QueryTabsPanel editors until they're fully migrated to WorkspaceState.
    running_query_tasks: HashMap<usize, Task<()>>,
    /// DEPRECATED: Use workspace_state for cancel handles instead
    /// Cancel handles for running queries, keyed by editor index.
    /// Used to interrupt the actual database query (not just drop the task).
    /// NOTE: Kept for QueryTabsPanel editors until they're fully migrated to WorkspaceState.
    query_cancel_handles: HashMap<usize, Arc<dyn QueryCancelHandle>>,
    /// Version repository for database object version control
    version_repository: Arc<VersionRepository>,
    /// Version history panel (opened on demand)
    version_history_panel: Option<Entity<VersionHistoryPanel>>,
    /// Diff viewer panel (opened on demand)
    diff_viewer_panel: Option<Entity<DiffViewer>>,
    _subscriptions: Vec<Subscription>,
}

impl MainView {
    /// Creates a new MainView with the default 4-panel dock layout.
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let workspace_id = WorkspaceId::default_workspace();

        // Create centralized workspace state
        let workspace_state = cx.new(|_cx| WorkspaceState::new());

        let (connection_manager, version_repository) = {
            let Some(app_state) = cx.try_global::<AppState>() else {
                panic!("AppState must be initialized before creating MainView");
            };
            (
                app_state.connections.clone(),
                app_state.version_repository.clone(),
            )
        };

        let connection_sidebar = cx.new(|cx| {
            let mut sidebar = ConnectionSidebar::new(cx);
            // Load saved connections from AppState
            if let Some(app_state) = cx.try_global::<AppState>() {
                let saved = app_state.saved_connections();
                let entries: Vec<_> = saved
                    .into_iter()
                    .map(|s| ConnectionEntry::new(s.id, s.name, s.driver))
                    .collect();
                sidebar.set_connections(entries, cx);
            }
            sidebar
        });
        let query_tabs_panel = cx.new(|cx| {
            let mut panel = QueryTabsPanel::new(cx);
            // Set schema service from AppState
            if let Some(app_state) = cx.try_global::<AppState>() {
                panel.set_schema_service(app_state.schema_service.clone());
            }
            panel
        });
        let results_panel = cx.new(|cx| ResultsPanel::new(cx));
        let problems_panel = cx.new(|cx| ProblemsPanel::new(window, cx));
        let schema_details_panel = cx.new(|cx| SchemaDetailsPanel::new(cx));
        let cell_editor_panel = cx.new(|cx| CellEditorPanel::new(window, cx));
        let key_value_editor_panel = cx.new(|cx| KeyValueEditorPanel::new(window, cx));
        let query_history_panel = cx.new(|cx| QueryHistoryPanel::new(cx));
        // TODO: Re-enable when ready
        // let template_library_panel = cx.new(|cx| TemplateLibraryPanel::new(window, cx));
        // let project_manager_panel = cx.new(|cx| ProjectManagerPanel::new(window, cx));
        let objects_panel = cx.new(|cx| ObjectsPanel::new(window, cx));

        let inspector_panel = cx.new(|cx| {
            InspectorPanel::new(
                schema_details_panel.clone(),
                cell_editor_panel.clone(),
                key_value_editor_panel.clone(),
                query_history_panel.clone(),
                cx,
            )
        });

        let dock_area =
            cx.new(|cx| DockArea::new(DOCK_AREA_ID, Some(DOCK_AREA_VERSION), window, cx));

        // Always show tab bar, even when there's only one tab
        dock_area.update(cx, |area, cx| {
            area.set_panel_style(PanelStyle::TabBar, window, cx);
        });

        let weak_dock_area = dock_area.downgrade();

        let loaded_from_saved = if let Ok(Some(persisted)) = load_layout(&workspace_id) {
            tracing::info!("Loading saved dock layout");
            dock_area.update(cx, |area, cx| {
                if let Err(e) = area.load(persisted.state, window, cx) {
                    tracing::warn!("Failed to load saved layout: {}, using default", e);
                    false
                } else {
                    true
                }
            })
        } else {
            false
        };

        if loaded_from_saved {
            dock_area.update(cx, |area, cx| {
                area.set_dock_open(DockPlacement::Left, true, window, cx);
            });
        }

        if !loaded_from_saved {
            dock_area.update(cx, |area, cx| {
                let left_panel =
                    DockItem::tab(connection_sidebar.clone(), &weak_dock_area, window, cx);

                let center_panel =
                    DockItem::tab(objects_panel.clone(), &weak_dock_area, window, cx);

                // Create bottom dock with tabs for Results and Problems panels
                let bottom_panel =
                    DockItem::tabs(
                        vec![
                            Arc::new(results_panel.clone()) as Arc<dyn PanelView>,
                            Arc::new(problems_panel.clone()) as Arc<dyn PanelView>,
                        ],
                        &weak_dock_area,
                        window,
                        cx,
                    );

                let right_panel = DockItem::panel(Arc::new(inspector_panel.clone()));

                area.set_left_dock(left_panel, Some(px(250.)), true, window, cx);
                area.set_center(center_panel, window, cx);
                area.set_bottom_dock(bottom_panel, Some(px(200.)), true, window, cx);
                area.set_right_dock(right_panel, Some(px(320.)), true, window, cx);

                area.set_dock_collapsible(
                    Edges {
                        left: true,
                        bottom: true,
                        right: true,
                        ..Default::default()
                    },
                    window,
                    cx,
                );
            });
        }

        let sidebar_subscription = cx.subscribe_in(&connection_sidebar, window, {
            move |this, _sidebar, event: &ConnectionSidebarEvent, window, cx| {
                this.handle_sidebar_event(event.clone(), window, cx);
            }
        });

        let query_tabs_subscription = cx.subscribe_in(&query_tabs_panel, window, {
            let results_panel = results_panel.clone();
            move |this, panel, event: &QueryTabsPanelEvent, window, cx| {
                this.handle_query_tabs_event(
                    event.clone(),
                    panel.clone(),
                    results_panel.clone(),
                    window,
                    cx,
                );
            }
        });

        let results_panel_subscription = cx.subscribe_in(&results_panel, window, {
            move |this, _panel, event: &ResultsPanelEvent, window, cx| {
                this.handle_results_panel_event(event.clone(), window, cx);
            }
        });

        let dock_subscription = cx.subscribe_in(&dock_area, window, {
            let dock_area = dock_area.downgrade();
            move |this, _dock_area, event: &DockEvent, _window, cx| match event {
                DockEvent::LayoutChanged => {
                    tracing::debug!("Dock layout changed, saving layout...");
                    if let Some(dock_area) = dock_area.upgrade() {
                        this.save_dock_layout(&dock_area, cx);
                    }
                }
                _ => {}
            }
        });

        let cell_editor_subscription = cx.subscribe_in(&cell_editor_panel, window, {
            move |this, _editor, event: &crate::components::CellEditorEvent, window, cx| {
                this.handle_cell_editor_event(event.clone(), window, cx);
            }
        });

        let inspector_panel_subscription = cx.subscribe_in(&inspector_panel, window, {
            move |this, _panel, event: &crate::components::InspectorPanelEvent, window, cx| {
                this.handle_inspector_panel_event(event.clone(), window, cx);
            }
        });

        let key_value_editor_subscription = cx.subscribe_in(&key_value_editor_panel, window, {
            move |this, _panel, event: &KeyValueEditorEvent, window, cx| {
                this.handle_key_value_editor_event(event.clone(), window, cx);
            }
        });

        let objects_panel_subscription = cx.subscribe_in(&objects_panel, window, {
            move |this, _panel, event: &ObjectsPanelEvent, window, cx| {
                this.handle_objects_panel_event(event, window, cx);
            }
        });

        // TODO: Re-enable when ready
        // let template_library_subscription = cx.subscribe_in(&template_library_panel, window, {
        //     move |this, _panel, event: &TemplateLibraryEvent, window, cx| {
        //         this.handle_template_library_event(event, window, cx);
        //     }
        // });

        // let project_manager_subscription = cx.subscribe_in(&project_manager_panel, window, {
        //     move |this, _panel, event: &ProjectManagerEvent, window, cx| {
        //         this.handle_project_manager_event(event, window, cx);
        //     }
        // });

        let appearance_subscription = cx.observe_window_appearance(window, |_this, _window, cx| {
            let settings = ZqlzSettings::global(cx);
            if settings.appearance.theme_mode == ThemeModePreference::System {
                tracing::debug!("System appearance changed, reapplying theme");
                let settings = settings.clone();
                settings.apply(cx);
                // Sync Zed's theme selection (light/dark switch) and then sync
                // colors back to ZQLZ's Theme global.
                SettingsBridge::apply_zqlz_settings_to_zed(cx);
                ThemeBridge::sync_zed_theme_to_zqlz(cx);
            }
        });

        let tab_menu_subscription = dock_area.read(cx).center_tab_panel().map(|center_panel| {
            cx.subscribe_in(&center_panel, window, {
                move |this,
                      _panel,
                      event: &zqlz_ui::widgets::dock::TabContextMenuEvent,
                      window,
                      cx| {
                    this.handle_tab_context_menu(event.tab_index, event.position, window, cx);
                }
            })
        });

        // Subscribe to workspace state changes for centralized state management
        let workspace_state_subscription = cx.subscribe_in(&workspace_state, window, {
            let objects_panel = objects_panel.clone();
            move |this, _state, event: &WorkspaceStateEvent, window, cx| {
                this.handle_workspace_state_event(event, &objects_panel, window, cx);
            }
        });

        // Subscribe to problems panel events for navigation
        let problems_panel_subscription = cx.subscribe_in(&problems_panel, window, {
            let problems_panel = problems_panel.clone();
            move |this, _panel, event: &ProblemsPanelEvent, window, cx| {
                this.handle_problems_panel_event(event, &problems_panel, window, cx);
            }
        });

        // Observe inspector panel to re-render status bar icons when active view changes
        let inspector_panel_observation = cx.observe(&inspector_panel, |_this, _panel, cx| {
            cx.notify();
        });

        Self {
            focus_handle: cx.focus_handle(),
            workspace_state,
            dock_area,
            connection_sidebar,
            query_tabs_panel,
            query_counter: 0,
            results_panel,
            problems_panel,
            schema_details_panel,
            cell_editor_panel,
            key_value_editor_panel,
            inspector_panel,
            settings_panel: None,
            // template_library_panel,
            // project_manager_panel,
            objects_panel,
            workspace_id,
            tab_context_menu: None,
            query_editors: Vec::new(),
            command_palette: None,
            running_query_tasks: HashMap::new(),
            query_cancel_handles: HashMap::new(),
            version_repository,
            version_history_panel: None,
            diff_viewer_panel: None,
            _subscriptions: vec![
                sidebar_subscription,
                query_tabs_subscription,
                results_panel_subscription,
                dock_subscription,
                cell_editor_subscription,
                inspector_panel_subscription,
                key_value_editor_subscription,
                objects_panel_subscription,
                // template_library_subscription,
                // project_manager_subscription,
                appearance_subscription,
                workspace_state_subscription,
                problems_panel_subscription,
                inspector_panel_observation,
            ]
            .into_iter()
            .chain(tab_menu_subscription)
            .collect(),
        }
    }

    /// Save the current dock layout to disk
    fn save_dock_layout(&self, dock_area: &Entity<DockArea>, cx: &App) {
        let state: DockAreaState = dock_area.read(cx).dump(cx);

        if let Err(e) = save_layout(&self.workspace_id, &state) {
            tracing::error!("Failed to save dock layout: {}", e);
        } else {
            tracing::debug!("Dock layout saved successfully");
        }
    }

    /// Get the centralized workspace state
    pub fn workspace_state(&self) -> &Entity<WorkspaceState> {
        &self.workspace_state
    }

    /// Get the active connection ID from WorkspaceState
    ///
    /// This is the canonical way to get the active connection. Use this instead
    /// of the deprecated `selected_connection_id` field.
    fn active_connection_id(&self, cx: &App) -> Option<Uuid> {
        self.workspace_state.read(cx).active_connection_id()
    }

    /// Refresh the query history panel with latest entries from AppState
    fn refresh_query_history(&self, cx: &mut Context<Self>) {
        if let Some(app_state) = cx.try_global::<AppState>() {
            let entries = app_state.query_history_entries();
            self.inspector_panel.update(cx, |panel, cx| {
                panel.query_history_panel().update(cx, |history_panel, cx| {
                    history_panel.update_entries(entries, cx);
                });
            });
        }
    }

    /// Convert EditorDiagnostic from WorkspaceState to DiagnosticInfo for ResultsPanel
    fn convert_to_diagnostic_info(diag: &EditorDiagnostic) -> DiagnosticInfo {
        DiagnosticInfo {
            line: diag.line,
            column: diag.column,
            end_line: diag.end_line,
            end_column: diag.end_column,
            message: diag.message.clone(),
            severity: match diag.severity {
                DiagnosticSeverity::Error => DiagnosticInfoSeverity::Error,
                DiagnosticSeverity::Warning => DiagnosticInfoSeverity::Warning,
                DiagnosticSeverity::Info => DiagnosticInfoSeverity::Info,
                DiagnosticSeverity::Hint => DiagnosticInfoSeverity::Hint,
            },
            source: diag.source.clone(),
        }
    }

    /// Convert DiagnosticInfo to ProblemEntry for ProblemsPanel
    fn convert_to_problem_entry(diag: &DiagnosticInfo) -> ProblemEntry {
        ProblemEntry {
            line: diag.line,
            column: diag.column,
            end_line: diag.end_line,
            end_column: diag.end_column,
            message: diag.message.clone(),
            severity: match diag.severity {
                DiagnosticInfoSeverity::Error => ProblemSeverity::Error,
                DiagnosticInfoSeverity::Warning => ProblemSeverity::Warning,
                DiagnosticInfoSeverity::Info => ProblemSeverity::Info,
                DiagnosticInfoSeverity::Hint => ProblemSeverity::Hint,
            },
            source: diag.source.clone(),
        }
    }

    /// Handle workspace state events
    ///
    /// This is the central event handler for workspace state changes.
    /// It propagates changes to relevant panels.
    fn handle_workspace_state_event(
        &mut self,
        event: &WorkspaceStateEvent,
        _objects_panel: &Entity<ObjectsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            WorkspaceStateEvent::ActiveConnectionChanged(connection_id) => {
                tracing::debug!(
                    "MainView: handling ActiveConnectionChanged({:?})",
                    connection_id
                );

                // Sync sidebar selection (in case change came from elsewhere)
                self.connection_sidebar.update(cx, |sidebar, cx| {
                    sidebar.set_selected(*connection_id, cx);
                });

                // Refresh objects panel for the new connection
                if connection_id.is_some() {
                    self.refresh_objects_panel(window, cx);
                }
            }

            WorkspaceStateEvent::ConnectionStatusChanged { id, connected } => {
                tracing::debug!(
                    "MainView: handling ConnectionStatusChanged({}, connected={})",
                    id,
                    connected
                );

                // Sync sidebar connected state
                self.connection_sidebar.update(cx, |sidebar, cx| {
                    sidebar.set_connected(*id, *connected, cx);
                });

                // If disconnected and this was the active connection, clear objects panel
                if !connected {
                    let active_conn = self.workspace_state.read(cx).active_connection_id();
                    if active_conn == Some(*id) {
                        self.objects_panel.update(cx, |panel, cx| {
                            panel.clear(cx);
                        });
                    }
                }
            }

            WorkspaceStateEvent::SchemaRefreshed(connection_id) => {
                tracing::debug!("MainView: handling SchemaRefreshed({})", connection_id);

                // Refresh the objects panel if it's showing this connection
                let active_conn = self.workspace_state.read(cx).active_connection_id();
                if active_conn == Some(*connection_id) {
                    self.refresh_objects_panel(window, cx);
                }
            }

            WorkspaceStateEvent::QueryStarted { editor_id, .. } => {
                tracing::debug!("MainView: query started for {:?}", editor_id);
                cx.notify();
            }

            WorkspaceStateEvent::QueryCompleted { editor_id, success } => {
                tracing::debug!(
                    "MainView: query completed for {:?}, success={}",
                    editor_id,
                    success
                );
                cx.notify();
            }

            WorkspaceStateEvent::ActiveEditorChanged(editor_id) => {
                tracing::debug!("MainView: active editor changed to {:?}", editor_id);

                // Update ResultsPanel with the active editor ID and its diagnostics
                let diagnostics = if let Some(id) = editor_id {
                    self.workspace_state
                        .read(cx)
                        .diagnostics_for_editor(*id)
                        .iter()
                        .map(Self::convert_to_diagnostic_info)
                        .collect()
                } else {
                    Vec::new()
                };

                self.results_panel.update(cx, |panel, cx| {
                    // Set the active editor ID so problems are scoped correctly
                    panel.set_active_editor_id(editor_id.map(|id| id.0), cx);
                    // Update problems for the active editor
                    panel.set_problems(diagnostics, cx);
                });

                cx.notify();
            }

            WorkspaceStateEvent::DiagnosticsChanged(editor_id) => {
                tracing::debug!("MainView: diagnostics changed for {:?}", editor_id);

                // Only update the ResultsPanel if the diagnostics are for the active editor
                let active_editor_id = self.workspace_state.read(cx).active_editor_id();
                
                if Some(*editor_id) == active_editor_id {
                    // Get diagnostics from WorkspaceState and push to ResultsPanel
                    let diagnostics = self
                        .workspace_state
                        .read(cx)
                        .diagnostics_for_editor(*editor_id);
                    let diagnostic_infos: Vec<DiagnosticInfo> = diagnostics
                        .iter()
                        .map(Self::convert_to_diagnostic_info)
                        .collect();

                    self.results_panel.update(cx, |panel, cx| {
                        panel.set_problems(diagnostic_infos.clone(), cx);
                    });

                    // Also update the ProblemsPanel with the same diagnostics
                    let problem_entries: Vec<ProblemEntry> = diagnostic_infos
                        .iter()
                        .map(Self::convert_to_problem_entry)
                        .collect();

                    self.problems_panel.update(cx, |panel, cx| {
                        panel.update_problems(problem_entries, cx);
                    });

                    cx.notify();
                } else {
                    tracing::debug!(
                        "MainView: ignoring diagnostics for {:?} (active editor is {:?})",
                        editor_id,
                        active_editor_id
                    );
                }
            }

            // Other events - log and continue
            _ => {
                tracing::trace!("MainView: unhandled workspace state event: {:?}", event);
            }
        }
    }

    /// Handle events from the Problems panel
    fn handle_problems_panel_event(
        &mut self,
        event: &ProblemsPanelEvent,
        _problems_panel: &Entity<ProblemsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ProblemsPanelEvent::NavigateToProblem {
                line,
                column,
                end_line,
                end_column,
            } => {
                tracing::debug!(
                    "MainView: navigate to problem at {}:{} (end: {:?}:{:?})",
                    line,
                    column,
                    end_line,
                    end_column
                );

                // Navigate using the most recent query editor (the active one)
                if let Some(editor_weak) = self.query_editors.last() {
                    if let Some(editor) = editor_weak.upgrade() {
                        // Focus the editor and navigate to the problem location
                        let focus_handle = editor.read(cx).editor_focus_handle(cx);
                        focus_handle.focus(window, cx);

                        // Navigate to the problem position
                        editor.update(cx, |editor, cx| {
                            editor.navigate_to(
                                *line,
                                *column,
                                *end_line,
                                *end_column,
                                window,
                                cx,
                            );
                        });

                        tracing::debug!(
                            "MainView: navigated to problem at line {}, column {}",
                            line,
                            column
                        );
                    }
                } else {
                    tracing::warn!("MainView: no query editor available for navigation");
                }
            }
        }
    }
}

impl Render for MainView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use zqlz_ui::widgets::ActiveTheme;

        let bg_color = cx.theme().background;
        let fg_color = cx.theme().foreground;
        let font_size = cx.theme().font_size;

        let dialog_layer = zqlz_ui::widgets::Root::render_dialog_layer(window, cx);
        let sheet_layer = zqlz_ui::widgets::Root::render_sheet_layer(window, cx);
        let notification_layer = zqlz_ui::widgets::Root::render_notification_layer(window, cx);

        div()
            .id("main-view")
            .key_context("MainView")
            .track_focus(&self.focus_handle)
            .size_full()
            .relative()
            .bg(bg_color)
            .text_color(fg_color)
            .text_size(font_size)
            .on_action(cx.listener(Self::handle_open_settings))
            .on_action(cx.listener(Self::handle_quit))
            .on_action(cx.listener(Self::handle_new_query))
            .on_action(cx.listener(Self::handle_new_connection))
            .on_action(cx.listener(Self::handle_refresh_connections_list))
            .on_action(cx.listener(Self::handle_execute_query))
            .on_action(cx.listener(Self::handle_execute_selection))
            .on_action(cx.listener(Self::handle_stop_query))
            .on_action(cx.listener(Self::handle_refresh))
            .on_action(cx.listener(Self::handle_toggle_left_sidebar))
            .on_action(cx.listener(Self::handle_toggle_right_sidebar))
            .on_action(cx.listener(Self::handle_toggle_bottom_panel))
            .on_action(cx.listener(Self::handle_toggle_problems_panel))
            .on_action(cx.listener(Self::handle_open_command_palette))
            // Tab navigation actions
            .on_action(cx.listener(Self::handle_activate_next_tab))
            .on_action(cx.listener(Self::handle_activate_prev_tab))
            .on_action(cx.listener(Self::handle_close_active_tab))
            .on_action(cx.listener(Self::handle_close_other_tabs))
            .on_action(cx.listener(Self::handle_close_tabs_to_right))
            .on_action(cx.listener(Self::handle_close_all_tabs))
            .on_action(cx.listener(Self::handle_activate_tab_1))
            .on_action(cx.listener(Self::handle_activate_tab_2))
            .on_action(cx.listener(Self::handle_activate_tab_3))
            .on_action(cx.listener(Self::handle_activate_tab_4))
            .on_action(cx.listener(Self::handle_activate_tab_5))
            .on_action(cx.listener(Self::handle_activate_tab_6))
            .on_action(cx.listener(Self::handle_activate_tab_7))
            .on_action(cx.listener(Self::handle_activate_tab_8))
            .on_action(cx.listener(Self::handle_activate_tab_9))
            .on_action(cx.listener(Self::handle_activate_last_tab))
            .child(
                v_flex()
                    .size_full()
                    .child(self.render_title_bar(cx))
                    .child(
                        div()
                            .flex_1()
                            .w_full()
                            .overflow_hidden()
                            .child(self.dock_area.clone()),
                    )
                    .child(self.render_status_bar(cx)),
            )
            .children(dialog_layer)
            .children(sheet_layer)
            .children(notification_layer)
            .children(self.tab_context_menu.clone())
            .children(self.render_command_palette_overlay(cx))
    }
}

impl Focusable for MainView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<MainViewEvent> for MainView {}
