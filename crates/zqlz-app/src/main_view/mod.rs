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

mod command_palette_helpers;
mod connection_handlers;
mod connection_window;
mod event_handlers;
mod object_designer_handlers;
mod objects_panel_action_helpers;
mod query_facade;
mod query_handlers;
mod refresh;
mod rename_window;
mod saved_query_handlers;
mod tab_menu;
pub(crate) mod table_handlers;
mod table_handlers_utils;
mod table_workflow_adapter;
mod ui_components;
mod versioning_facade;
mod versioning_handlers;
mod view_handlers;
use gpui::*;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;
use zqlz_services::{RefreshIntent, RefreshPlan, RefreshPlanStep, SurfaceRefreshKind};
use zqlz_settings::{ThemeModePreference, WorkspaceId, ZqlzSettings, load_layout};
use zqlz_ui::widgets::{
    dock::{DockArea, DockEvent, DockItem, DockPlacement, PanelStyle, PanelView},
    v_flex,
};
use zqlz_versioning::{
    VersionRepository,
    widgets::{DiffViewer, VersionHistoryPanel},
};

use crate::app::AppState;
use crate::components::{
    CellEditorPanel, CommandPalette, ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent,
    InspectorPanel, KeyValueEditorEvent, KeyValueEditorPanel, ObjectsPanel, ObjectsPanelEvent,
    ProblemEntry, ProblemSeverity, ProblemsPanel, ProblemsPanelEvent, QueryHistoryPanel,
    ResultsPanel, ResultsPanelEvent, SchemaDetailsPanel, SettingsPanel,
};
use crate::workspace::WorkspaceController;
use crate::workspace_state::{
    DiagnosticSeverity, EditorDiagnostic, RefreshScope, WorkspaceSession,
    WorkspaceSessionViewerKind, WorkspaceState, WorkspaceStateEvent,
};
use zqlz_query::{DiagnosticInfo, DiagnosticInfoSeverity};

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
#[allow(dead_code)]
pub enum MainViewEvent {
    /// A connection was established
    ConnectionEstablished(Uuid),
    /// A connection was closed
    ConnectionClosed(Uuid),
}

/// Main application view orchestrating the 4-panel dock system.
pub struct MainView {
    focus_handle: FocusHandle,
    workspace_controller: Entity<WorkspaceController>,
    /// Centralized workspace state - single source of truth for UI state
    workspace_state: Entity<WorkspaceState>,
    dock_area: Entity<DockArea>,
    connection_sidebar: Entity<ConnectionSidebar>,
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
    show_settings_page: bool,
    objects_panel: Entity<ObjectsPanel>,
    tab_context_menu: Option<Entity<TabContextMenuState>>,
    query_editors: Vec<WeakEntity<crate::components::QueryEditor>>,
    query_editor_subscriptions: HashMap<EntityId, Subscription>,
    table_viewer_subscriptions: HashMap<EntityId, Subscription>,
    command_palette: Option<Entity<CommandPalette>>,
    command_palette_closing: bool,
    _command_palette_subscription: Option<Subscription>,
    /// Version repository for database object version control
    version_repository: Arc<VersionRepository>,
    /// Version history panel (opened on demand)
    version_history_panel: Option<Entity<VersionHistoryPanel>>,
    /// Diff viewer panel (opened on demand)
    diff_viewer_panel: Option<Entity<DiffViewer>>,
    /// Task for the most recent table-open async work. Replaced on every new
    /// open_table_viewer call so that stale loads for a previous table are
    /// automatically cancelled when the user rapidly clicks another table.
    active_table_load_task: Option<Task<anyhow::Result<()>>>,
    /// Monotonic ownership token for `active_table_load_task`.
    ///
    /// The open-viewer async flow only clears `active_table_load_task` when
    /// the completion path still owns the latest token. This prevents an older
    /// finishing task from clearing a newer in-flight task reference.
    active_table_load_task_generation: u64,
    restored_session_viewer_connections: HashSet<Uuid>,
    _subscriptions: Vec<Subscription>,
}

impl MainView {
    fn refresh_workspace_window_title(&self, window: &mut Window, cx: &App) {
        self.workspace_controller
            .read(cx)
            .refresh_window_title(window, cx);
    }

    fn pin_dirty_preview_tab(&self, is_dirty: bool, cx: &mut Context<Self>) {
        if is_dirty {
            self.workspace_controller
                .update(cx, |workspace, cx| workspace.pin_active_preview_tab(cx));
        }
    }

    fn activate_existing_query_editor(
        &self,
        editor: &Entity<crate::components::QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let panel_id = editor.entity_id();
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.activate_panel_by_id(panel_id, window, cx);
        });
        let focus_handle = editor.read(cx).editor_focus_handle(cx);
        window.focus(&focus_handle, cx);
    }

    fn prune_closed_query_editors(&mut self) {
        self.query_editors
            .retain(|query_editor| query_editor.upgrade().is_some());
    }

    /// Request a canonical refresh scope through workspace state so all refresh
    /// entry points converge into a single handling path.
    fn request_refresh(&mut self, scope: RefreshScope, cx: &mut Context<Self>) {
        self.workspace_state.update(cx, |workspace_state, cx| {
            workspace_state.request_refresh(scope, cx);
        });
    }

    fn refresh_intent_from_scope(scope: &RefreshScope) -> RefreshIntent {
        match scope {
            RefreshScope::ConnectionsList => RefreshIntent::ConnectionsList,
            RefreshScope::ActiveConnectionSurfaces => RefreshIntent::ActiveConnectionSurfaces,
            RefreshScope::ConnectionSurfaces(connection_id) => {
                RefreshIntent::ConnectionSurfaces(*connection_id)
            }
        }
    }

    fn connected_sidebar_connection_ids(&self, cx: &App) -> Vec<Uuid> {
        self.connection_sidebar
            .read(cx)
            .connections()
            .iter()
            .filter(|connection| connection.is_connected)
            .map(|connection| connection.id)
            .collect()
    }

    fn execute_refresh_plan(&mut self, plan: RefreshPlan, cx: &mut Context<Self>) {
        for step in plan.steps {
            match step {
                RefreshPlanStep::RefreshConnectionsList => {
                    self.refresh_connections_list_preserving_state(cx);
                }
                RefreshPlanStep::RefreshConnectionSurfaces {
                    connection_id,
                    kind,
                } => {
                    let target = connection_id
                        .map(crate::main_view::refresh::RefreshTarget::Connection)
                        .unwrap_or(crate::main_view::refresh::RefreshTarget::ActiveConnection);
                    let options = match kind {
                        SurfaceRefreshKind::SidebarAndObjects => {
                            crate::main_view::refresh::SurfaceRefreshOptions::SIDEBAR_AND_OBJECTS
                        }
                        SurfaceRefreshKind::ConnectionsList => {
                            crate::main_view::refresh::SurfaceRefreshOptions::CONNECTIONS_LIST
                        }
                    };
                    self.refresh_connection_surfaces(target, options, cx);
                }
            }
        }
    }

    /// Creates a new MainView with the default 4-panel dock layout.
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let workspace_id = WorkspaceId::default_workspace();

        let (_connection_manager, version_repository) = {
            let Some(app_state) = cx.try_global::<AppState>() else {
                panic!("AppState must be initialized before creating MainView");
            };
            (
                app_state.connections.clone(),
                app_state.version_repository.clone(),
            )
        };

        let restore_session = ZqlzSettings::global(cx).workspace.restore_tabs_on_startup;
        let persisted_session = if restore_session {
            cx.try_global::<AppState>().and_then(|app_state| {
                match app_state.storage.load_workspace_session() {
                    Ok(session) => session,
                    Err(error) => {
                        tracing::warn!(%error, "failed to load workspace session");
                        None
                    }
                }
            })
        } else {
            None
        };

        if !restore_session {
            tracing::debug!("Workspace tab restore skipped by settings");
        }
        let restored_session = persisted_session.clone();

        let workspace_state = cx.new(|_cx| {
            if let Some(session) = persisted_session {
                WorkspaceState::from_persisted_session(session)
            } else {
                WorkspaceState::new()
            }
        });

        let connection_sidebar = cx.new(|cx| {
            let mut sidebar = ConnectionSidebar::new(cx);
            // Load saved connections from AppState
            if let Some(app_state) = cx.try_global::<AppState>() {
                let saved = app_state.connection_service.list_saved_connections();
                let entries: Vec<_> = saved
                    .into_iter()
                    .map(|s| ConnectionEntry::new(s.id, s.name, s.driver))
                    .collect();
                sidebar.set_connections(entries, cx);
            }
            sidebar
        });
        let results_panel = cx.new(ResultsPanel::new);
        let problems_panel = cx.new(|cx| ProblemsPanel::new(window, cx));
        let schema_details_panel = cx.new(SchemaDetailsPanel::new);
        let cell_editor_panel = cx.new(|cx| CellEditorPanel::new(window, cx));
        let key_value_editor_panel = cx.new(|cx| KeyValueEditorPanel::new(window, cx));
        let query_history_panel = cx.new(|cx| QueryHistoryPanel::new(window, cx));
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
        let workspace_controller =
            cx.new(|_| WorkspaceController::new(weak_dock_area.clone(), workspace_id.clone()));
        crate::window_manager::register_main_workspace(window, workspace_controller.clone(), cx);
        let main_view = cx.entity().downgrade();
        window.on_window_should_close(cx, move |window, cx| {
            if main_view.upgrade().is_some() {
                window.dispatch_action(crate::actions::CloseWindow.boxed_clone(), cx);
                false
            } else {
                true
            }
        });

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
                let bottom_panel = DockItem::tabs(
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

        let results_panel_subscription = cx.subscribe_in(&results_panel, window, {
            move |this, _panel, event: &ResultsPanelEvent, window, cx| {
                this.handle_results_panel_event(event.clone(), window, cx);
            }
        });

        let dock_subscription = cx.subscribe_in(&dock_area, window, {
            move |this, _dock_area, event: &DockEvent, window, cx| {
                if let DockEvent::LayoutChanged = event {
                    this.refresh_workspace_window_title(window, cx);
                    tracing::debug!("Dock layout changed, saving layout...");
                    this.workspace_controller.read(cx).save_layout(cx);
                }
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

        let appearance_subscription = cx.observe_window_appearance(window, |_this, _window, cx| {
            let settings = ZqlzSettings::global(cx);
            if settings.appearance.theme_mode == ThemeModePreference::System {
                tracing::debug!("System appearance changed, reapplying theme");
                let settings = settings.clone();
                settings.apply(cx);
            }
        });
        let window_activation_subscription = cx.observe_window_activation(window, {
            move |_this, window, cx| {
                crate::window_manager::mark_active_main_workspace(window, cx);
            }
        });

        let tab_menu_subscription =
            workspace_controller
                .read(cx)
                .center_tab_panel(cx)
                .map(|center_panel| {
                    cx.subscribe_in(&center_panel, window, {
                        move |this,
                              _panel,
                              event: &zqlz_ui::widgets::dock::TabContextMenuEvent,
                              window,
                              cx| {
                            this.handle_tab_context_menu(
                                event.tab_index,
                                event.position,
                                window,
                                cx,
                            );
                        }
                    })
                });

        let tab_close_request_subscription = workspace_controller
            .read(cx)
            .center_tab_panel(cx)
            .map(|center_panel| {
                cx.subscribe_in(&center_panel, window, {
                    move |this,
                          _panel,
                          event: &zqlz_ui::widgets::dock::TabCloseRequestEvent,
                          window,
                          cx| {
                        this.handle_tab_close_request(event.tab_index, window, cx);
                    }
                })
            });

        let tab_command_subscription =
            workspace_controller
                .read(cx)
                .center_tab_panel(cx)
                .map(|center_panel| {
                    cx.subscribe_in(&center_panel, window, {
                        move |this,
                              _panel,
                              event: &zqlz_ui::widgets::dock::TabCommandEvent,
                              window,
                              cx| {
                            this.handle_tab_command(event.command, window, cx);
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

        let mut main_view = Self {
            focus_handle: cx.focus_handle(),
            workspace_controller,
            workspace_state,
            dock_area,
            connection_sidebar,
            query_counter: 0,
            results_panel,
            problems_panel,
            schema_details_panel,
            cell_editor_panel,
            key_value_editor_panel,
            inspector_panel,
            settings_panel: None,
            show_settings_page: false,
            // template_library_panel,
            // project_manager_panel,
            objects_panel,
            tab_context_menu: None,
            query_editors: Vec::new(),
            query_editor_subscriptions: HashMap::new(),
            table_viewer_subscriptions: HashMap::new(),
            command_palette: None,
            command_palette_closing: false,
            _command_palette_subscription: None,
            version_repository,
            version_history_panel: None,
            diff_viewer_panel: None,
            active_table_load_task: None,
            active_table_load_task_generation: 0,
            restored_session_viewer_connections: HashSet::new(),
            _subscriptions: vec![
                sidebar_subscription,
                results_panel_subscription,
                dock_subscription,
                cell_editor_subscription,
                inspector_panel_subscription,
                key_value_editor_subscription,
                objects_panel_subscription,
                // template_library_subscription,
                // project_manager_subscription,
                appearance_subscription,
                window_activation_subscription,
                workspace_state_subscription,
                problems_panel_subscription,
                inspector_panel_observation,
            ]
            .into_iter()
            .chain(tab_menu_subscription)
            .chain(tab_close_request_subscription)
            .chain(tab_command_subscription)
            .collect(),
        };

        main_view.refresh_workspace_window_title(window, cx);

        if let Some(session) = restored_session {
            main_view.restore_workspace_session_query_tabs(session.clone(), window, cx);
            main_view.restore_workspace_session_viewer_tabs(session, None, window, cx);
        }
        main_view.persist_workspace_session(cx);
        main_view.refresh_query_history(cx);

        main_view
    }

    fn persist_workspace_session(&self, cx: &App) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::warn!("skipped workspace session save because AppState is unavailable");
            return;
        };

        let session = self.workspace_state.read(cx).persisted_session();
        if let Err(error) = app_state.storage.save_workspace_session(&session) {
            tracing::warn!(%error, "failed to save workspace session");
        }
    }

    fn clear_persisted_workspace_session(&self, cx: &App) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::warn!("skipped workspace session clear because AppState is unavailable");
            return;
        };

        if let Err(error) = app_state
            .storage
            .save_workspace_session(&WorkspaceSession::default())
        {
            tracing::warn!(%error, "failed to clear workspace session");
        }
    }

    fn should_persist_workspace_session(event: &WorkspaceStateEvent) -> bool {
        matches!(
            event,
            WorkspaceStateEvent::ActiveConnectionChanged(_)
                | WorkspaceStateEvent::ActiveDatabaseChanged(_)
                | WorkspaceStateEvent::ActiveEditorChanged(_)
                | WorkspaceStateEvent::EditorAdded(_)
                | WorkspaceStateEvent::EditorRemoved(_)
                | WorkspaceStateEvent::EditorStateChanged(_)
                | WorkspaceStateEvent::ViewerTabsChanged
        )
    }

    fn restore_workspace_session_query_tabs(
        &mut self,
        session: WorkspaceSession,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut active_editor = None;
        let active_editor_id = session.active_editor_id;
        let mut max_restored_editor_id = 0;

        for tab in session.open_query_tabs {
            max_restored_editor_id = max_restored_editor_id.max(tab.id.0);
            let content = tab.draft_text.unwrap_or_default();
            let editor = self.open_query_editor_with_content_for_editor_id(
                query_handlers::QueryEditorContentOpenRequest {
                    editor_id: tab.id,
                    display_name: tab.display_name,
                    content,
                    file_path: tab.document_path,
                    connection_id: tab.connection_id,
                },
                window,
                cx,
            );

            if Some(tab.id) == active_editor_id {
                active_editor = Some(editor);
            }
        }

        if let Some(editor) = active_editor {
            let focus_handle = editor.read(cx).editor_focus_handle(cx);
            window.focus(&focus_handle, cx);
            self.workspace_state.update(cx, |state, cx| {
                state.set_active_editor(active_editor_id, cx);
            });
        }

        self.query_counter = self.query_counter.max(max_restored_editor_id);
    }

    fn restore_workspace_session_viewer_tabs(
        &mut self,
        session: WorkspaceSession,
        connection_filter: Option<Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let connection_service = cx
            .try_global::<AppState>()
            .map(|app_state| app_state.connection_service.clone());

        for tab in session.open_viewer_tabs {
            if connection_filter.is_some_and(|connection_id| tab.connection_id != connection_id) {
                continue;
            }

            let Some(connection_service) = connection_service.as_ref() else {
                tracing::warn!(
                    connection_id = %tab.connection_id,
                    "Skipped session viewer restore because AppState is unavailable"
                );
                continue;
            };

            if connection_service
                .get_connection(tab.connection_id)
                .is_none()
            {
                tracing::debug!(
                    connection_id = %tab.connection_id,
                    "Deferred session viewer restore until connection is active"
                );
                continue;
            }

            let restored_tab = tab.clone();
            match tab.kind {
                WorkspaceSessionViewerKind::Table {
                    table_name,
                    database_name,
                    is_view,
                    viewer_state,
                } => {
                    self.open_table_viewer_with_session_state(
                        table_handlers::TableViewerSessionOpenRequest {
                            connection_id: tab.connection_id,
                            table_name,
                            database_name,
                            is_view,
                            session_state: viewer_state,
                        },
                        window,
                        cx,
                    );
                }
                WorkspaceSessionViewerKind::RedisDatabase { database_index } => {
                    self.open_redis_database(tab.connection_id, database_index, window, cx);
                }
                WorkspaceSessionViewerKind::RedisKey {
                    database_index,
                    key_name,
                } => {
                    self.open_redis_key(tab.connection_id, database_index, key_name, window, cx);
                }
                WorkspaceSessionViewerKind::Collection {
                    database_name,
                    collection_name,
                    viewer_state,
                } => {
                    self.open_document_collection_viewer(
                        tab.connection_id,
                        database_name,
                        collection_name,
                        viewer_state,
                        window,
                        cx,
                    );
                }
            }
            self.workspace_state.update(cx, |state, cx| {
                state.record_open_viewer_tab(restored_tab, cx);
            });
        }
    }

    pub(super) fn restore_workspace_session_viewer_tabs_for_connection(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self
            .restored_session_viewer_connections
            .insert(connection_id)
        {
            return;
        }

        let session = self.workspace_state.read(cx).persisted_session();
        self.restore_workspace_session_viewer_tabs(session, Some(connection_id), window, cx);
    }

    /// Get the centralized workspace state
    #[allow(dead_code)]
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
        if Self::should_persist_workspace_session(event) {
            self.persist_workspace_session(cx);
        }

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

                // Refresh connected metadata surfaces for the new connection so the
                // sidebar and objects panel stay in lockstep after selection changes.
                if connection_id.is_some() {
                    self.refresh_connection_surfaces(
                        crate::main_view::refresh::RefreshTarget::ActiveConnection,
                        crate::main_view::refresh::SurfaceRefreshOptions::SELECTION_SYNC_OBJECTS_ONLY,
                        cx,
                    );
                }
            }

            WorkspaceStateEvent::ActiveDatabaseChanged(database_name) => {
                tracing::debug!(
                    "MainView: handling ActiveDatabaseChanged({:?})",
                    database_name
                );

                self.sync_active_query_editor_database_selection(database_name.clone(), window, cx);

                if let Some(connection_id) = self.workspace_state.read(cx).active_connection_id() {
                    self.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
                }
            }

            WorkspaceStateEvent::RefreshRequested(scope) => {
                tracing::debug!("MainView: handling RefreshRequested({:?})", scope);

                let connected_connection_ids = self.connected_sidebar_connection_ids(cx);
                let plan = RefreshPlan::from_intent(
                    Self::refresh_intent_from_scope(scope),
                    &connected_connection_ids,
                );
                self.execute_refresh_plan(plan, cx);
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

            WorkspaceStateEvent::QueryCancelled(editor_id) => {
                tracing::debug!("MainView: query cancelled for {:?}", editor_id);
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
                    panel.set_problems(diagnostics.clone(), cx);
                });

                let problem_entries: Vec<ProblemEntry> = diagnostics
                    .iter()
                    .map(Self::convert_to_problem_entry)
                    .collect();
                self.problems_panel.update(cx, |panel, cx| {
                    panel.update_problems(problem_entries, cx);
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

                if let Some(editor) = self.active_query_editor(cx) {
                    let focus_handle = editor.read(cx).editor_focus_handle(cx);
                    focus_handle.focus(window, cx);

                    editor.update(cx, |editor, cx| {
                        editor.navigate_to(*line, *column, *end_line, *end_column, window, cx);
                    });

                    tracing::debug!(
                        "MainView: navigated to problem at line {}, column {}",
                        line,
                        column
                    );
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
            .on_action(cx.listener(Self::handle_install_cli))
            .on_action(cx.listener(Self::handle_quit))
            .on_action(cx.listener(Self::handle_new_window))
            .on_action(cx.listener(Self::handle_close_window))
            .on_action(cx.listener(Self::handle_minimize_window))
            .on_action(cx.listener(Self::handle_zoom_window))
            .on_action(cx.listener(Self::handle_new_query))
            .on_action(cx.listener(Self::handle_new_connection))
            .on_action(cx.listener(Self::handle_refresh_connection))
            .on_action(cx.listener(Self::handle_refresh_connections_list))
            .on_action(cx.listener(Self::handle_execute_query))
            .on_action(cx.listener(Self::handle_execute_selection))
            .on_action(cx.listener(Self::handle_execute_current_statement))
            .on_action(cx.listener(Self::handle_explain_query))
            .on_action(cx.listener(Self::handle_explain_selection))
            .on_action(cx.listener(Self::handle_stop_query))
            .on_action(cx.listener(Self::handle_save_query_as))
            .on_action(cx.listener(Self::handle_refresh))
            .on_action(cx.listener(Self::handle_toggle_left_sidebar))
            .on_action(cx.listener(Self::handle_toggle_right_sidebar))
            .on_action(cx.listener(Self::handle_toggle_bottom_panel))
            .on_action(cx.listener(Self::handle_toggle_all_docks))
            .on_action(cx.listener(Self::handle_focus_editor))
            .on_action(cx.listener(Self::handle_focus_results))
            .on_action(cx.listener(Self::handle_focus_sidebar))
            .on_action(cx.listener(Self::handle_toggle_problems_panel))
            .on_action(cx.listener(Self::handle_open_command_palette))
            // Tab navigation actions
            .on_action(cx.listener(Self::handle_activate_next_tab))
            .on_action(cx.listener(Self::handle_activate_prev_tab))
            .on_action(cx.listener(Self::handle_navigate_tab_back))
            .on_action(cx.listener(Self::handle_navigate_tab_forward))
            .on_action(cx.listener(Self::handle_close_editor))
            .on_action(cx.listener(Self::handle_close_active_tab))
            .on_action(cx.listener(Self::handle_close_other_tabs))
            .on_action(cx.listener(Self::handle_close_tabs_to_right))
            .on_action(cx.listener(Self::handle_close_tabs_to_left))
            .on_action(cx.listener(Self::handle_close_clean_tabs))
            .on_action(cx.listener(Self::handle_close_all_tabs))
            .on_action(cx.listener(Self::handle_move_tab_to_new_window))
            .on_action(cx.listener(Self::handle_toggle_pin_active_tab))
            .on_action(cx.listener(Self::handle_pin_tab))
            .on_action(cx.listener(Self::handle_unpin_tab))
            .on_action(cx.listener(Self::handle_mark_active_tab_as_preview))
            .on_action(cx.listener(Self::handle_clear_active_tab_preview))
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
                    .child(div().flex_1().w_full().overflow_hidden().child(
                        if self.show_settings_page {
                            self.settings_panel
                                .clone()
                                .map(|panel| panel.into_any_element())
                                .unwrap_or_else(|| self.dock_area.clone().into_any_element())
                        } else {
                            self.dock_area.clone().into_any_element()
                        },
                    ))
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
