// Event handlers for MainView

use std::path::Path;
#[cfg(not(target_os = "windows"))]
use std::path::PathBuf;
use std::sync::Arc;

use super::{
    MainView,
    command_palette_helpers::{build_static_commands_for_features, route_command_palette_event},
    objects_panel_action_helpers::{
        ObjectsPanelActionRegistry, ResolvedObjectsPanelAction, ResolvedObjectsPanelActionExt,
        SelectedObjectRef, classify_objects_panel_action_resolution,
        objects_panel_action_feature_availability, objects_panel_action_issue_message,
    },
    saved_query_handlers::{save_query_for_editor, try_update_saved_query_for_editor},
};
use crate::actions::*;
use crate::app::AppState;
use crate::components::{
    CommandPalette, CommandPaletteEvent, CommandUsagePersistence, ConnectionSidebarEvent,
    ObjectsPanelEvent, ProjectManagerEvent, QueryEditor, ResultsPanelEvent, SettingsPanel,
    SettingsPanelEvent, TableViewerPanel, TemplateLibraryEvent,
};
use crate::main_view::table_handlers::table_ops::design::TableDesignSaveRequest;
use crate::workspace::WorkspaceItemCloseIntent;
use crate::workspace_state::{EditorId, QueryCancellationOutcome, RefreshScope};
use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::{ConnectionFeatureSet, ObjectFormMode, extension_supports_database_file_open};
use zqlz_query::plan_query_database_selection;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::{Button, ButtonVariant, ButtonVariants},
    dialog::DialogButtonProps,
    input::{Input, InputState},
    notification::Notification,
    typography::body_small,
    v_flex,
};

#[cfg(not(target_os = "windows"))]
struct CliInstallPaths {
    cli_path: PathBuf,
    symlink_path: PathBuf,
    symlink_parent: PathBuf,
}

impl MainView {
    /// Emits structured objects-panel action telemetry so phase-0 metrics can be
    /// computed from logs without changing runtime behavior.
    fn emit_objects_panel_action_telemetry(
        action_id: &str,
        object_count: usize,
        action_resolution: &ResolvedObjectsPanelAction,
    ) {
        let telemetry = classify_objects_panel_action_resolution(action_resolution);

        tracing::info!(
            target: "objects_panel.telemetry",
            metric = "objects_panel_action_resolution",
            action_id,
            object_count,
            resolution = telemetry.resolution,
            handler_registered = telemetry.handler_registered,
            unknown_or_unsupported = telemetry.unknown_or_unsupported,
            "Objects panel action telemetry"
        );
    }

    /// Applies StopQuery UI side effects only when cancellation actually stops
    /// an active execution.
    fn apply_stop_query_action_outcome(
        &mut self,
        cancellation_outcome: QueryCancellationOutcome,
        editor_id: EditorId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match cancellation_outcome {
            QueryCancellationOutcome::CancelledActiveExecution => {
                tracing::info!("Cancelling query for editor {:?}", editor_id);

                // Reset executing state only for the currently focused dock query editor.
                // Using the fallback editor list here can clear an unrelated editor when
                // no query tab is active in the dock.
                let active_dock_query_editor =
                    self.workspace_controller.read(cx).active_query_editor(cx);

                if let Some(editor) = active_dock_query_editor {
                    editor.update(cx, |editor, cx| {
                        editor.set_executing(false, cx);
                    });
                } else {
                    tracing::debug!(
                        "Skipped query editor executing-state reset because active dock panel is not a query editor"
                    );
                }

                // Update results panel to show cancellation message
                let results_panel = self.results_panel.clone();
                let now = chrono::Utc::now();
                let execution = crate::components::QueryExecution {
                    sql: String::new(),
                    start_time: now,
                    end_time: now,
                    duration_ms: 0,
                    connection_name: None,
                    database_name: None,
                    statements: vec![crate::components::StatementResult {
                        sql: String::new(),
                        duration_ms: 0,
                        result: None,
                        error: Some("Query cancelled by user".to_string()),
                        affected_rows: 0,
                    }],
                };

                results_panel.update(cx, |panel, cx| {
                    panel.set_loading(false, cx);
                });

                let results_panel = results_panel.downgrade();
                cx.spawn_in(window, async move |_this, cx| {
                    if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                        panel.set_execution(execution, window, cx);
                    }) {
                        tracing::warn!(%error, "failed to update results panel after query cancellation");
                    }
                    anyhow::Ok(())
                })
                .detach();

                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::warning("Query cancelled"),
                    cx,
                );
            }
            _ => {
                tracing::debug!(
                    editor_id = ?editor_id,
                    cancellation_outcome = ?cancellation_outcome,
                    "No active query execution was cancelled; cancellation still routed to workspace state"
                );
            }
        }
    }

    /// Find the active query editor from the dock, falling back to the most
    /// recently created editor in `self.query_editors`.
    pub(super) fn active_query_editor(&self, cx: &App) -> Option<Entity<QueryEditor>> {
        if let Some(editor) = self.workspace_controller.read(cx).active_query_editor(cx) {
            return Some(editor);
        }
        self.query_editors
            .iter()
            .rev()
            .find_map(|weak| weak.upgrade())
    }

    pub(crate) fn open_external_path(
        &mut self,
        path: &Path,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase());

        match extension.as_deref() {
            Some("sql") => {
                let path = path.to_path_buf();
                cx.spawn_in(window, async move |this, cx| {
                    let path_for_read = path.clone();
                    let read_result = cx
                        .background_spawn(async move { std::fs::read_to_string(&path_for_read) })
                        .await;

                    match read_result {
                        Ok(content) => {
                            if let Err(error) = this.update_in(cx, |this, window, cx| {
                                this.query_facade_open_sql_file_in_query_editor(
                                    &path, content, window, cx,
                                );
                            }) {
                                tracing::warn!(%error, "failed to open SQL file in window");
                            }
                        }
                        Err(error) => {
                            if let Err(update_error) = this.update_in(cx, |_, window, cx| {
                                window.push_notification(
                                    zqlz_ui::widgets::notification::Notification::error(format!(
                                        "Failed to open SQL file: {}",
                                        error
                                    )),
                                    cx,
                                );
                            }) {
                                tracing::warn!(%update_error, "failed to surface SQL open error");
                            }
                        }
                    }
                })
                .detach();
            }
            Some(extension) if extension_supports_database_file_open(extension) => {
                self.import_database_file_and_open_query(path, window, cx);
            }
            _ => {
                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::warning(
                        "Unsupported file type for direct open",
                    ),
                    cx,
                );
            }
        }
    }

    /// Handles events emitted by the left sidebar (ConnectionSidebar).
    pub(super) fn handle_sidebar_event(
        &mut self,
        event: ConnectionSidebarEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(block_reason) = self.sidebar_event_feature_block_reason(&event, cx) {
            tracing::warn!(
                reason = %block_reason,
                "Blocked sidebar action because connection feature set marks it unavailable"
            );
            window.push_notification(Notification::warning(block_reason), cx);
            return;
        }

        match event {
            ConnectionSidebarEvent::AddConnection => {
                self.open_new_connection_dialog(window, cx);
            }
            ConnectionSidebarEvent::NewGroup => {
                let message = "Connection groups are not available yet";
                tracing::info!(reason = %message, "Blocked unavailable sidebar action");
                window.push_notification(Notification::warning(message), cx);
            }
            ConnectionSidebarEvent::CloseAllConnections => {
                self.close_all_connections(cx);
            }
            ConnectionSidebarEvent::Connect(connection_id) => {
                self.connect_to_database(connection_id, window, cx);
            }
            ConnectionSidebarEvent::Disconnect(connection_id) => {
                self.disconnect_from_database(connection_id, cx);
            }
            ConnectionSidebarEvent::Selected(connection_id) => {
                let is_connected = self.workspace_state.read(cx).is_connected(connection_id);

                if is_connected {
                    // Update WorkspaceState with the selected connection (source of truth).
                    // This emits ActiveConnectionChanged, which drives panel refresh observers.
                    self.workspace_state.update(cx, |state, cx| {
                        state.set_active_connection(Some(connection_id), cx);
                    });
                } else {
                    tracing::debug!(
                        connection_id = %connection_id,
                        "Ignoring disconnected sidebar selection"
                    );
                }

                cx.notify();
            }
            ConnectionSidebarEvent::NewQuery(connection_id) => {
                // First set the connection in WorkspaceState, then create the query.
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                });
                self.query_facade_handle_new_query(&NewQuery, window, cx);
            }
            ConnectionSidebarEvent::RefreshConnections => {
                self.request_refresh(RefreshScope::ConnectionsList, cx);
            }
            ConnectionSidebarEvent::DeleteConnection(connection_id) => {
                self.delete_connection(connection_id, window, cx);
            }
            ConnectionSidebarEvent::DuplicateConnection(connection_id) => {
                self.duplicate_connection(connection_id, window, cx);
            }
            ConnectionSidebarEvent::OpenConnectionSettings(connection_id) => {
                self.open_connection_settings(connection_id, window, cx);
            }
            ConnectionSidebarEvent::OpenDroppedPaths(paths) => {
                for path in paths {
                    self.open_external_path(&path, window, cx);
                }
            }
            ConnectionSidebarEvent::ConnectToDatabase {
                connection_id,
                database_name,
            } => {
                tracing::info!(
                    "Select database '{}' on connection {}",
                    database_name,
                    connection_id
                );

                let active_database_before = {
                    let workspace_state = self.workspace_state.read(cx);
                    workspace_state.active_database().map(str::to_owned)
                };
                let database_selection_plan = plan_query_database_selection(
                    &database_name,
                    active_database_before.as_deref(),
                );
                let database_name = database_selection_plan.database_name;
                let should_refresh_connection_surfaces =
                    database_selection_plan.should_refresh_connection_surfaces;

                self.connection_sidebar.update(cx, |sidebar, cx| {
                    sidebar.set_database_loading(connection_id, &database_name, true, cx);
                });

                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                    state.set_active_database(Some(database_name.clone()), cx);
                });

                if should_refresh_connection_surfaces {
                    self.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
                }
            }
            ConnectionSidebarEvent::LoadSection {
                connection_id,
                section,
            } => {
                self.load_sidebar_section(connection_id, section, window, cx);
            }
            ConnectionSidebarEvent::DesignView {
                connection_id,
                view_name,
                object_schema,
            } => {
                self.design_view(connection_id, view_name, object_schema, window, cx);
            }
            ConnectionSidebarEvent::NewView { connection_id } => {
                self.new_view(connection_id, window, cx);
            }
            ConnectionSidebarEvent::DeleteView {
                connection_id,
                view_name,
            } => {
                self.delete_view(connection_id, view_name, window, cx);
            }
            ConnectionSidebarEvent::DuplicateView {
                connection_id,
                view_name,
            } => {
                self.duplicate_view(connection_id, view_name, window, cx);
            }
            ConnectionSidebarEvent::RenameView {
                connection_id,
                view_name,
            } => {
                self.rename_view(connection_id, view_name, window, cx);
            }
            ConnectionSidebarEvent::CopyViewName { view_name } => {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(view_name));
            }
            ConnectionSidebarEvent::OpenTable {
                connection_id,
                table_name,
                database_name,
            } => {
                self.open_table_viewer(connection_id, table_name, database_name, false, window, cx);
            }
            ConnectionSidebarEvent::LoadTableDetails {
                connection_id,
                table_name,
                object_schema,
                database_name,
            } => {
                self.load_sidebar_table_details(
                    connection_id,
                    table_name,
                    object_schema,
                    database_name,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::OpenView {
                connection_id,
                view_name,
                database_name,
            } => {
                self.open_table_viewer(connection_id, view_name, database_name, true, window, cx);
            }
            ConnectionSidebarEvent::DesignTable {
                connection_id,
                table_name,
            } => {
                self.design_table(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::NewTable { connection_id } => {
                self.new_table(connection_id, window, cx);
            }
            ConnectionSidebarEvent::DeleteTable {
                connection_id,
                table_name,
            } => {
                self.delete_table(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::EmptyTable {
                connection_id,
                table_name,
            } => {
                self.empty_table(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::DuplicateTable {
                connection_id,
                table_name,
            } => {
                self.duplicate_table(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::RenameTable {
                connection_id,
                table_name,
            } => {
                self.rename_table(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::ImportData {
                connection_id,
                table_name,
            } => {
                self.import_data(connection_id, table_name, window, cx);
            }
            ConnectionSidebarEvent::ExportData {
                connection_id,
                table_name,
            } => {
                let table_names = if table_name.is_empty() {
                    Vec::new()
                } else {
                    vec![table_name]
                };
                self.export_data(connection_id, table_names, window, cx);
            }
            ConnectionSidebarEvent::DumpTableSql {
                connection_id,
                table_name,
                include_data,
            } => {
                self.dump_table_sql(connection_id, table_name, include_data, window, cx);
            }
            ConnectionSidebarEvent::CopyTableName { table_name } => {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(table_name));
            }
            ConnectionSidebarEvent::RefreshSchema { connection_id } => {
                self.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
            }
            ConnectionSidebarEvent::OpenSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                tracing::info!(query_id = %query_id, query_name = %query_name, connection_id = %connection_id, "Opening saved query from sidebar");
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                });
                self.query_facade_open_saved_query(query_id, connection_id, window, cx);
            }
            ConnectionSidebarEvent::DeleteSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                self.query_facade_delete_saved_query(
                    query_id,
                    query_name,
                    connection_id,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::RenameSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                self.query_facade_rename_saved_query(
                    query_id,
                    query_name,
                    connection_id,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::ViewHistory {
                connection_id,
                object_name,
                object_schema,
                object_type,
            } => {
                let registry = ObjectsPanelActionRegistry::default();
                match registry.resolve_versioned_object_type("view_history", &object_type) {
                    Ok(db_object_type) => {
                        self.versioning_facade_show_version_history(
                            connection_id,
                            object_name,
                            object_schema,
                            db_object_type,
                            window,
                            cx,
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            action_id = %error.action_id,
                            kind_id = %error.kind_id,
                            "Unknown object type for version history"
                        );
                    }
                }
            }
            ConnectionSidebarEvent::OpenFunction {
                connection_id,
                function_name,
                object_schema,
            } => {
                self.open_function_definition(
                    connection_id,
                    function_name,
                    object_schema,
                    None,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::OpenProcedure {
                connection_id,
                procedure_name,
                object_schema,
            } => {
                self.open_procedure_definition(
                    connection_id,
                    procedure_name,
                    object_schema,
                    None,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::DesignTrigger {
                connection_id,
                trigger_name,
                object_schema,
            } => {
                self.design_trigger(connection_id, trigger_name, object_schema, None, window, cx);
            }
            ConnectionSidebarEvent::NewTrigger { connection_id } => {
                self.new_trigger(connection_id, window, cx);
            }
            ConnectionSidebarEvent::DeleteTrigger {
                connection_id,
                trigger_name,
            } => {
                self.delete_trigger(connection_id, trigger_name, window, cx);
            }
            ConnectionSidebarEvent::OpenTriggerDesigner {
                connection_id,
                trigger_name,
                object_schema,
            } => {
                self.open_trigger_designer(
                    connection_id,
                    trigger_name,
                    object_schema,
                    None,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::OpenGenericObjectDefinition {
                connection_id,
                object_ref,
            } => {
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                    if let Some(database_name) = object_ref.database.clone() {
                        state.set_active_database(Some(database_name), cx);
                    }
                });
                self.open_generic_object_definition(
                    connection_id,
                    object_ref.kind_id.clone(),
                    SelectedObjectRef {
                        name: object_ref.name,
                        schema: object_ref.schema,
                        signature: object_ref.signature,
                        associated_table: None,
                    },
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::OpenObjectDesigner {
                connection_id,
                kind_id,
                mode,
                object_ref,
            } => {
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                    if let Some(database_name) = object_ref
                        .as_ref()
                        .and_then(|object_ref| object_ref.database.clone())
                    {
                        state.set_active_database(Some(database_name), cx);
                    }
                });
                self.open_object_designer(
                    connection_id,
                    object_ref
                        .as_ref()
                        .map(|object_ref| object_ref.kind_id.clone())
                        .unwrap_or(kind_id),
                    mode,
                    object_ref,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::LoadRedisKeys {
                connection_id,
                database_index,
            } => {
                self.load_redis_keys(connection_id, database_index, window, cx);
            }
            ConnectionSidebarEvent::OpenRedisKey {
                connection_id,
                database_index,
                key_name,
            } => {
                self.open_redis_key(connection_id, database_index, key_name, window, cx);
            }
            ConnectionSidebarEvent::OpenRedisDatabase {
                connection_id,
                database_index,
            } => {
                self.open_redis_database(connection_id, database_index, window, cx);
            }
            ConnectionSidebarEvent::LoadDocumentCollections {
                connection_id,
                database_name,
            } => {
                self.load_document_collections(connection_id, database_name, window, cx);
            }
            ConnectionSidebarEvent::OpenDocumentCollection {
                connection_id,
                database_name,
                collection_name,
            } => {
                self.open_document_collection_viewer(
                    connection_id,
                    database_name,
                    collection_name,
                    None,
                    window,
                    cx,
                );
            }
        }
    }

    pub(super) fn sidebar_connection_feature_set(
        &self,
        connection_id: Uuid,
        cx: &App,
    ) -> Option<ConnectionFeatureSet> {
        let active_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(str::to_owned);

        let workspace_state = self.workspace_state.read(cx);
        workspace_state
            .connection_feature_set(connection_id, active_database.as_deref())
            .cloned()
            .or_else(|| {
                workspace_state
                    .connection_feature_set(connection_id, None)
                    .cloned()
            })
    }

    fn sidebar_event_feature_block_reason(
        &self,
        event: &ConnectionSidebarEvent,
        cx: &App,
    ) -> Option<String> {
        let (connection_id, availability) = match event {
            ConnectionSidebarEvent::NewQuery(connection_id)
            | ConnectionSidebarEvent::OpenSavedQuery { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.query.execute)
            }
            ConnectionSidebarEvent::OpenTable { connection_id, .. }
            | ConnectionSidebarEvent::OpenView { connection_id, .. }
            | ConnectionSidebarEvent::ExportData { connection_id, .. }
            | ConnectionSidebarEvent::DumpTableSql { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.data_editing.browse_rows)
            }
            ConnectionSidebarEvent::NewTable { connection_id }
            | ConnectionSidebarEvent::NewView { connection_id }
            | ConnectionSidebarEvent::NewTrigger { connection_id } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.objects.create_objects)
            }
            ConnectionSidebarEvent::DesignTable { connection_id, .. }
            | ConnectionSidebarEvent::DesignView { connection_id, .. }
            | ConnectionSidebarEvent::OpenFunction { connection_id, .. }
            | ConnectionSidebarEvent::OpenProcedure { connection_id, .. }
            | ConnectionSidebarEvent::DesignTrigger { connection_id, .. }
            | ConnectionSidebarEvent::OpenTriggerDesigner { connection_id, .. }
            | ConnectionSidebarEvent::RenameTable { connection_id, .. }
            | ConnectionSidebarEvent::RenameView { connection_id, .. }
            | ConnectionSidebarEvent::DuplicateTable { connection_id, .. }
            | ConnectionSidebarEvent::DuplicateView { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.objects.edit_objects)
            }
            ConnectionSidebarEvent::DeleteTable { connection_id, .. }
            | ConnectionSidebarEvent::DeleteView { connection_id, .. }
            | ConnectionSidebarEvent::DeleteTrigger { connection_id, .. }
            | ConnectionSidebarEvent::EmptyTable { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.objects.delete_objects)
            }
            ConnectionSidebarEvent::OpenGenericObjectDefinition { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.objects.browse_objects)
            }
            ConnectionSidebarEvent::OpenObjectDesigner {
                connection_id,
                mode,
                ..
            } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                let availability = match mode {
                    ObjectFormMode::Create => features.objects.create_objects,
                    ObjectFormMode::Edit => features.objects.edit_objects,
                    ObjectFormMode::Drop => features.objects.delete_objects,
                };
                (*connection_id, availability)
            }
            ConnectionSidebarEvent::LoadRedisKeys { connection_id, .. }
            | ConnectionSidebarEvent::OpenRedisKey { connection_id, .. }
            | ConnectionSidebarEvent::OpenRedisDatabase { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.stores.key_value)
            }
            ConnectionSidebarEvent::LoadDocumentCollections { connection_id, .. }
            | ConnectionSidebarEvent::OpenDocumentCollection { connection_id, .. } => {
                let features = self.sidebar_connection_feature_set(*connection_id, cx)?;
                (*connection_id, features.stores.document)
            }
            _ => return None,
        };

        if availability.available {
            None
        } else {
            Some(format!(
                "Action unavailable for connection {}: {}",
                connection_id,
                availability.reason_or("This feature is unavailable for this connection")
            ))
        }
    }

    /// Handles right-click on a tab in the center dock.
    pub(super) fn handle_tab_context_menu(
        &mut self,
        tab_index: usize,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let focus_handle = self.focus_handle.clone();
        let Some(new_menu) = self.workspace_controller.update(cx, |workspace, cx| {
            workspace.tab_context_menu(tab_index, focus_handle, window, cx)
        }) else {
            tracing::warn!("No center TabPanel found for context menu");
            return;
        };

        if self.tab_context_menu.is_none() {
            self.tab_context_menu = Some(super::tab_menu::TabContextMenuState::new(window, cx));
        }

        if let Some(menu_state) = &self.tab_context_menu {
            menu_state.update(cx, |state, cx| {
                state.show(new_menu, tab_index, position, window, cx);
            });
        }
    }

    pub(super) fn handle_tab_close_request(
        &mut self,
        tab_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let close_intent = self.workspace_controller.update(cx, |workspace, cx| {
            workspace.activate_tab_index_and_close_intent(tab_index, window, cx)
        });
        self.handle_workspace_close_intent(close_intent, window, cx);
    }

    pub(super) fn handle_open_settings(
        &mut self,
        _action: &OpenSettings,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("OpenSettings action handler triggered (cmd-,)");
        self.open_settings_panel(window, cx);
    }

    pub(super) fn handle_install_cli(
        &mut self,
        _action: &InstallCli,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        #[cfg(target_os = "windows")]
        {
            window.push_notification(
                Notification::warning(
                    "CLI is installed by default on Windows when Add to PATH is enabled.",
                ),
                cx,
            );
            return;
        }

        #[cfg(not(target_os = "windows"))]
        {
            let install_paths = match Self::resolve_cli_install_paths() {
                Ok(paths) => paths,
                Err(error_message) => {
                    window.push_notification(Notification::error(error_message), cx);
                    return;
                }
            };

            if Self::cli_symlink_already_points_to_target(&install_paths) {
                window.push_notification(
                    Notification::success(format!(
                        "CLI is already installed at {}.",
                        install_paths.symlink_path.display()
                    )),
                    cx,
                );
                return;
            }

            Self::remove_existing_cli_symlink(&install_paths.symlink_path);

            if std::os::unix::fs::symlink(&install_paths.cli_path, &install_paths.symlink_path)
                .is_ok()
            {
                window.push_notification(
                    Notification::success(format!(
                        "Installed zqlz CLI to {}.",
                        install_paths.symlink_path.display()
                    )),
                    cx,
                );
                return;
            }

            #[cfg(target_os = "macos")]
            {
                Self::prompt_cli_install_with_macos_elevation(install_paths, window, cx);
            }

            #[cfg(any(target_os = "linux", target_os = "freebsd"))]
            {
                Self::show_cli_manual_install_command(install_paths, window, cx);
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn resolve_cli_install_paths() -> Result<CliInstallPaths, String> {
        let current_executable = std::env::current_exe()
            .map_err(|error| format!("Failed to determine app executable path: {error}"))?;

        let executable_directory = current_executable
            .parent()
            .ok_or_else(|| "Failed to determine app executable directory.".to_string())?;

        let candidate_cli_paths = [
            executable_directory.join("zqlz"),
            executable_directory.join("cli"),
            executable_directory.join("../bin/zqlz"),
            executable_directory.join("../bin/cli"),
        ];

        let cli_path = candidate_cli_paths
            .into_iter()
            .find(|path| path.exists())
            .ok_or_else(|| {
                "Could not locate bundled CLI binary. Reinstall ZQLZ and try again.".to_string()
            })?;

        #[cfg(target_os = "macos")]
        let symlink_path = PathBuf::from("/usr/local/bin/zqlz");

        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        let symlink_path = {
            let home_directory = dirs::home_dir()
                .ok_or_else(|| "Could not determine home directory for CLI install.".to_string())?;
            home_directory.join(".local/bin/zqlz")
        };

        let symlink_parent = symlink_path
            .parent()
            .ok_or_else(|| "Invalid symlink destination for zqlz CLI.".to_string())?
            .to_path_buf();

        Ok(CliInstallPaths {
            cli_path,
            symlink_path,
            symlink_parent,
        })
    }

    #[cfg(not(target_os = "windows"))]
    fn cli_symlink_already_points_to_target(install_paths: &CliInstallPaths) -> bool {
        std::fs::read_link(&install_paths.symlink_path)
            .ok()
            .as_ref()
            .is_some_and(|target| target == &install_paths.cli_path)
    }

    #[cfg(not(target_os = "windows"))]
    fn remove_existing_cli_symlink(symlink_path: &Path) {
        if let Err(error) = std::fs::remove_file(symlink_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::debug!(%error, "failed to remove existing zqlz symlink before install");
        }
    }

    #[cfg(target_os = "macos")]
    fn prompt_cli_install_with_macos_elevation(
        install_paths: CliInstallPaths,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use smol::process::Command;

        let shell_command = format!(
            "mkdir -p '{}' && ln -sf '{}' '{}'",
            install_paths.symlink_parent.display(),
            install_paths.cli_path.display(),
            install_paths.symlink_path.display()
        );
        let osascript_expression = format!(
            "do shell script \"{}\" with administrator privileges",
            shell_command.replace('"', "\\\"")
        );
        let symlink_display = install_paths.symlink_path.display().to_string();
        let window_handle = window.window_handle();

        cx.spawn(async move |_this, cx| {
            let osascript_result = Command::new("/usr/bin/osascript")
                .args(["-e", &osascript_expression])
                .output()
                .await;

            if let Err(error) = window_handle.update(cx, |_, window, cx| {
                Self::handle_cli_install_macos_osascript_result(
                    osascript_result,
                    &symlink_display,
                    window,
                    cx,
                )
            }) {
                tracing::warn!(?error, "Failed to update window after CLI install prompt");
            }

            Ok::<(), anyhow::Error>(())
        })
        .detach();
    }

    #[cfg(target_os = "macos")]
    fn handle_cli_install_macos_osascript_result(
        osascript_result: std::io::Result<std::process::Output>,
        symlink_display: &str,
        window: &mut Window,
        cx: &mut App,
    ) {
        match osascript_result {
            Ok(output) if output.status.success() => {
                window.push_notification(
                    Notification::success(format!("Installed zqlz CLI to {symlink_display}.")),
                    cx,
                );
            }
            Ok(output) => {
                let error_message = String::from_utf8_lossy(&output.stderr).to_string();
                let message = if error_message.trim().is_empty() {
                    "Failed to install CLI symlink with elevated privileges.".to_string()
                } else {
                    format!("Failed to install CLI symlink: {}", error_message.trim())
                };
                window.push_notification(Notification::error(message), cx);
            }
            Err(error) => {
                window.push_notification(
                    Notification::error(format!("Failed to run osascript: {error}")),
                    cx,
                );
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    fn show_cli_manual_install_command(
        install_paths: CliInstallPaths,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let manual_command = format!(
            "mkdir -p '{}' && ln -sf '{}' '{}'",
            install_paths.symlink_parent.display(),
            install_paths.cli_path.display(),
            install_paths.symlink_path.display()
        );
        window.push_notification(
            Notification::warning(format!(
                "Could not install CLI symlink automatically. Run: {manual_command}"
            )),
            cx,
        );
    }

    /// Handle opening the command palette
    pub(super) fn handle_open_command_palette(
        &mut self,
        _action: &OpenCommandPalette,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("Opening command palette");

        let active_connection_id = self.workspace_state.read(cx).active_connection_id();
        let active_features = active_connection_id
            .and_then(|connection_id| self.sidebar_connection_feature_set(connection_id, cx));
        let commands = build_static_commands_for_features(active_features.as_ref());

        // If the palette is already open, reset it and re-focus.
        if let Some(palette) = &self.command_palette {
            palette.update(cx, |palette, cx| {
                palette.reset(commands, window, cx);
            });
            self.load_schema_commands_into_palette(cx);
            self.load_document_symbol_commands_into_palette(cx);
            palette.update(cx, |palette, cx| {
                palette.focus(window, cx);
            });
            return;
        }

        let persistence = cx
            .try_global::<AppState>()
            .map(|state| Arc::clone(&state.storage) as Arc<dyn CommandUsagePersistence>);

        let action_context = window.focused(cx).map(|handle| handle.downgrade());

        let palette =
            cx.new(|cx| CommandPalette::new(commands, persistence, action_context, window, cx));

        self.load_schema_commands_into_palette_for(&palette, cx);
        self.load_document_symbol_commands_into_palette_for(&palette, cx);

        let subscription = cx.subscribe_in(
            &palette,
            window,
            |this, _palette, event: &CommandPaletteEvent, window, cx| {
                route_command_palette_event(this, event, window, cx);
            },
        );
        self._command_palette_subscription = Some(subscription);

        palette.update(cx, |palette, cx| {
            palette.focus(window, cx);
        });

        self.command_palette = Some(palette);
        cx.notify();
    }

    /// Load schema commands (tables/views) from the active connection into an
    /// already-stored palette entity.
    fn load_schema_commands_into_palette(&self, cx: &mut Context<Self>) {
        if let Some(palette) = &self.command_palette {
            self.load_schema_commands_into_palette_for(palette, cx);
        }
    }

    fn load_document_symbol_commands_into_palette(&self, cx: &mut Context<Self>) {
        if let Some(palette) = &self.command_palette {
            self.load_document_symbol_commands_into_palette_for(palette, cx);
        }
    }

    fn load_document_symbol_commands_into_palette_for(
        &self,
        palette: &Entity<CommandPalette>,
        cx: &mut Context<Self>,
    ) {
        let Some(editor) = self.active_query_editor(cx) else {
            return;
        };
        let symbols: Vec<(String, usize, usize)> = editor
            .read(cx)
            .document_symbols(cx)
            .into_iter()
            .map(|symbol| (symbol.label, symbol.line, symbol.column))
            .collect();
        if symbols.is_empty() {
            return;
        }

        palette.update(cx, |palette, cx| {
            palette.add_document_symbol_commands(&symbols, cx);
        });
    }

    /// Load schema commands into a specific palette entity reference.
    fn load_schema_commands_into_palette_for(
        &self,
        palette: &Entity<CommandPalette>,
        cx: &mut Context<Self>,
    ) {
        let active_connection_id = self.workspace_state.read(cx).active_connection_id();
        let Some(connection_id) = active_connection_id else {
            return;
        };
        if self
            .sidebar_connection_feature_set(connection_id, cx)
            .is_some_and(|features| !features.data_editing.browse_rows.available)
        {
            return;
        }
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let schema_service = app_state.schema_service.clone();
        let Some(palette_data) = app_state
            .connection_service
            .build_palette_schema_commands_data(connection_id, schema_service.as_ref())
        else {
            return;
        };

        palette.update(cx, |palette, cx| {
            palette.add_schema_commands(
                connection_id,
                &palette_data.connection_name,
                &palette_data.tables,
                &palette_data.views,
                palette_data.object_capabilities,
                cx,
            );
        });
    }

    /// Begin the dismiss animation, then actually drop the palette after a delay.
    pub(super) fn begin_dismiss_command_palette(&mut self, cx: &mut Context<Self>) {
        if self.command_palette_closing || self.command_palette.is_none() {
            return;
        }
        self.command_palette_closing = true;
        cx.notify();

        // Allow the exit animation to play before removing the palette.
        const EXIT_ANIMATION_DURATION_MS: u64 = 150;
        cx.spawn(async move |this, cx| {
            cx.background_spawn(async {
                smol::Timer::after(std::time::Duration::from_millis(EXIT_ANIMATION_DURATION_MS))
                    .await;
            })
            .await;
            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.dismiss_command_palette(cx);
                })
            })
        })
        .detach();
    }

    /// Immediately drop the command palette and its event subscription.
    pub(super) fn dismiss_command_palette(&mut self, cx: &mut Context<Self>) {
        self.command_palette = None;
        self.command_palette_closing = false;
        self._command_palette_subscription = None;
        cx.notify();
    }

    pub(super) fn handle_quit(
        &mut self,
        _action: &Quit,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Quit action handler triggered (cmd-q)");
        cx.quit();
    }

    pub(super) fn handle_new_connection(
        &mut self,
        _action: &NewConnection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Opening new connection dialog");
        self.open_new_connection_dialog(window, cx);
    }

    pub(super) fn handle_refresh_connections_list(
        &mut self,
        _action: &RefreshConnectionsList,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("RefreshConnectionsList action received - requesting refresh intent");
        self.request_refresh(RefreshScope::ConnectionsList, cx);
    }

    pub(super) fn handle_toggle_left_sidebar(
        &mut self,
        _action: &ToggleLeftSidebar,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("ToggleLeftSidebar action handler triggered (cmd-b)");
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Left, window, cx);
        });
    }

    pub(super) fn handle_toggle_right_sidebar(
        &mut self,
        _action: &ToggleRightSidebar,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Right, window, cx);
        });
    }

    pub(super) fn handle_toggle_bottom_panel(
        &mut self,
        _action: &ToggleBottomPanel,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Bottom, window, cx);
        });
    }

    pub(super) fn handle_toggle_all_docks(
        &mut self,
        _action: &ToggleAllDocks,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.toggle_all_docks(window, cx);
        });
    }

    /// Handle ToggleProblemsPanel action - shows/focuses the Problems panel
    pub(super) fn handle_toggle_problems_panel(
        &mut self,
        _action: &crate::actions::ToggleProblemsPanel,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.reveal_panel(
                "Problems",
                zqlz_ui::widgets::dock::DockPlacement::Bottom,
                window,
                cx,
            );
        });
    }

    /// Handle ExecuteQuery action - executes the entire query
    pub(super) fn handle_execute_query(
        &mut self,
        _action: &crate::actions::ExecuteQuery,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExecuteQuery action triggered");
        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.emit_execute_query(cx);
            });
        }
    }

    /// Handle ExecuteSelection action - executes selected text or entire query
    pub(super) fn handle_execute_selection(
        &mut self,
        _action: &crate::actions::ExecuteSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExecuteSelection action triggered");
        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.emit_execute_selection(cx);
            });
        }
    }

    /// Handle ExecuteCurrentStatement action - executes the current statement in the active editor.
    pub(super) fn handle_execute_current_statement(
        &mut self,
        _action: &crate::actions::ExecuteCurrentStatement,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExecuteCurrentStatement action triggered");
        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.emit_execute_selection(cx);
            });
        }
    }

    /// Handle ExplainQuery action - explains the entire query in the active editor.
    pub(super) fn handle_explain_query(
        &mut self,
        _action: &crate::actions::ExplainQuery,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExplainQuery action triggered");
        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.emit_explain_query(cx);
            });
        }
    }

    /// Handle ExplainSelection action - explains the current selection or statement.
    pub(super) fn handle_explain_selection(
        &mut self,
        _action: &crate::actions::ExplainSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExplainSelection action triggered");
        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.emit_explain_selection(cx);
            });
        }
    }

    /// Handle StopQuery action - stops the currently executing query
    pub(super) fn handle_stop_query(
        &mut self,
        _action: &crate::actions::StopQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("StopQuery action triggered");

        let active_editor_id = self.workspace_state.read(cx).active_editor_id();

        if let Some(editor_id) = active_editor_id {
            let active_dock_query_editor =
                self.workspace_controller.read(cx).active_query_editor(cx);

            let cancellation_outcome =
                self.cancel_query_for_editor(active_dock_query_editor, editor_id, cx);

            if let Some(cancellation_outcome) = cancellation_outcome {
                self.apply_stop_query_action_outcome(cancellation_outcome, editor_id, window, cx);
            } else {
                tracing::debug!(
                    editor_id = ?editor_id,
                    "Skipped StopQuery action UI side effects because workspace state was unavailable"
                );
            }
        } else {
            tracing::debug!("No active editor to stop query for");
        }

        cx.notify();
    }

    /// Focus the active editor area.
    pub(super) fn handle_focus_editor(
        &mut self,
        _action: &crate::actions::FocusEditor,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(focus_handle) = self
            .workspace_controller
            .read(cx)
            .active_center_item_focus_handle(cx)
        {
            window.focus(&focus_handle, cx);
        }
    }

    /// Focus the results panel.
    pub(super) fn handle_focus_results(
        &mut self,
        _action: &crate::actions::FocusResults,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let focus_handle = self.results_panel.read(cx).focus_handle(cx);
        window.focus(&focus_handle, cx);
    }

    /// Focus the connection sidebar.
    pub(super) fn handle_focus_sidebar(
        &mut self,
        _action: &crate::actions::FocusSidebar,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let focus_handle = self.connection_sidebar.read(cx).focus_handle(cx);
        window.focus(&focus_handle, cx);
    }

    /// Refresh the currently active connection surfaces.
    pub(super) fn handle_refresh_connection(
        &mut self,
        _action: &crate::actions::RefreshConnection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("RefreshConnection action received - requesting refresh intent");
        self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
    }

    /// Save the active query through the save dialog, regardless of whether it already has an id.
    pub(super) fn handle_save_query_as(
        &mut self,
        _action: &crate::actions::SaveQueryAs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(editor) = self.active_query_editor(cx) else {
            tracing::debug!("SaveQueryAs ignored because there is no active editor");
            return;
        };

        let (sql, connection_id) = {
            let editor = editor.read(cx);
            (
                editor.content(cx).to_string(),
                editor
                    .connection_id()
                    .or_else(|| self.active_connection_id(cx)),
            )
        };

        let Some(connection_id) = connection_id else {
            use zqlz_ui::widgets::{WindowExt, notification::Notification};
            window.push_notification(
                Notification::warning(
                    "No connection selected. Please connect to a database first.",
                ),
                cx,
            );
            return;
        };

        self.query_facade_show_save_query_dialog(
            editor.downgrade(),
            sql,
            connection_id,
            window,
            cx,
        );
    }

    // ====================
    // Tab Navigation Actions
    // ====================

    pub(super) fn handle_activate_next_tab(
        &mut self,
        _action: &crate::actions::ActivateNextTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.activate_next_tab(window, cx));
    }

    pub(super) fn handle_activate_prev_tab(
        &mut self,
        _action: &crate::actions::ActivatePrevTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.activate_prev_tab(window, cx));
    }

    pub(super) fn handle_navigate_tab_back(
        &mut self,
        _action: &crate::actions::NavigateTabBack,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.navigate_tab_back(window, cx));
    }

    pub(super) fn handle_navigate_tab_forward(
        &mut self,
        _action: &crate::actions::NavigateTabForward,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.navigate_tab_forward(window, cx)
        });
    }

    pub(super) fn handle_close_active_tab(
        &mut self,
        _action: &crate::actions::CloseActiveTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_active_workspace_item(window, cx);
    }

    pub(super) fn handle_close_editor(
        &mut self,
        _action: &crate::actions::CloseEditor,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_active_workspace_item(window, cx);
    }

    fn close_active_workspace_item(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        tracing::info!("CloseActiveTab action handler triggered!");
        let close_intent = self
            .workspace_controller
            .read(cx)
            .active_center_item_close_intent(cx);
        self.handle_workspace_close_intent(close_intent, window, cx);
    }

    fn handle_workspace_close_intent(
        &mut self,
        close_intent: WorkspaceItemCloseIntent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match close_intent {
            WorkspaceItemCloseIntent::CloseNow => {
                let close_result = self
                    .workspace_controller
                    .update(cx, |workspace, cx| workspace.close_active_tab(window, cx));
                if close_result == crate::workspace::WorkspaceCloseResult::NotClosed {
                    tracing::debug!("Active workspace item was not closed");
                }
            }
            WorkspaceItemCloseIntent::Blocked => {
                tracing::debug!("Active workspace item blocked close request");
            }
            WorkspaceItemCloseIntent::SavedQuery {
                query_id,
                editor,
                sql,
            } => {
                self.open_close_saved_query_unsaved_dialog(query_id, editor, sql, window, cx);
            }
            WorkspaceItemCloseIntent::NewQuery {
                editor,
                connection_id,
                sql,
                current_name,
            } => {
                self.open_close_new_query_unsaved_dialog(
                    editor,
                    connection_id,
                    sql,
                    current_name,
                    window,
                    cx,
                );
            }
            WorkspaceItemCloseIntent::DirtyNonQuery => {
                self.open_close_non_query_unsaved_dialog(window, cx);
            }
        }
    }

    pub(super) fn handle_new_window(
        &mut self,
        _action: &crate::actions::NewWindow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let result = self.workspace_controller.update(cx, |controller, cx| {
            controller.open_new_window(cx, |window, cx| {
                zqlz_settings::ZqlzSettings::global(cx).clone().apply(cx);

                let main_view = cx.new(|cx| MainView::new(window, cx));
                cx.new(|cx| zqlz_ui::widgets::Root::new(main_view, window, cx))
            })
        });

        if let Err(error) = result {
            tracing::error!(%error, "Failed to open new main window");
        }
    }

    pub(super) fn handle_close_window(
        &mut self,
        _action: &crate::actions::CloseWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let close_intent = self.workspace_controller.read(cx).window_close_intent(cx);

        match close_intent {
            crate::workspace::WorkspaceWindowCloseIntent::CloseNow => {
                if zqlz_settings::ZqlzSettings::global(cx)
                    .workspace
                    .confirm_close_window
                {
                    let workspace_controller = self.workspace_controller.clone();
                    let main_view = cx.entity().downgrade();
                    window.open_dialog(cx, move |dialog, _window, _cx| {
                        let workspace_controller = workspace_controller.clone();
                        let main_view = main_view.clone();
                        dialog
                            .title("Close Window?")
                            .child(
                                div()
                                    .text_sm()
                                    .child("Close all open tabs and this window?"),
                            )
                            .confirm()
                            .button_props(
                                DialogButtonProps::default()
                                    .ok_text("Close All")
                                    .ok_variant(ButtonVariant::Danger)
                                    .cancel_text("Cancel"),
                            )
                            .on_ok(move |_, window, cx| {
                                if let Some(main_view) = main_view.upgrade() {
                                    main_view.read(cx).clear_persisted_workspace_session(cx);
                                }
                                workspace_controller.update(cx, |controller, cx| {
                                    controller.close_window_now(window, cx);
                                });
                                true
                            })
                    });
                } else {
                    self.workspace_controller
                        .update(cx, |controller, cx| controller.close_window_now(window, cx));
                }
            }
            crate::workspace::WorkspaceWindowCloseIntent::ConfirmDiscard { dirty_count } => {
                let prompt = crate::workspace::window_close_discard_prompt(dirty_count);
                let workspace_controller = self.workspace_controller.clone();
                let main_view = cx.entity().downgrade();
                window.open_dialog(cx, move |dialog, _window, _cx| {
                    let workspace_controller = workspace_controller.clone();
                    let main_view = main_view.clone();
                    dialog
                        .title(prompt.title)
                        .child(div().text_sm().child(prompt.message.clone()))
                        .confirm()
                        .button_props(
                            DialogButtonProps::default()
                                .ok_text(prompt.discard_button)
                                .ok_variant(ButtonVariant::Danger)
                                .cancel_text(prompt.cancel_button),
                        )
                        .on_ok(move |_, window, cx| {
                            if let Some(main_view) = main_view.upgrade() {
                                main_view.read(cx).clear_persisted_workspace_session(cx);
                            }
                            workspace_controller.update(cx, |controller, cx| {
                                controller.close_window_now(window, cx);
                            });
                            true
                        })
                });
            }
        }
    }

    pub(super) fn handle_minimize_window(
        &mut self,
        _action: &crate::actions::MinimizeWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.read(cx).minimize_window(window);
    }

    pub(super) fn handle_zoom_window(
        &mut self,
        _action: &crate::actions::ZoomWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.read(cx).zoom_window(window);
    }

    fn open_close_saved_query_unsaved_dialog(
        &mut self,
        query_id: Uuid,
        editor_weak: WeakEntity<QueryEditor>,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace_controller = self.workspace_controller.clone();
        let prompt = crate::workspace::dirty_saved_query_close_prompt();
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let workspace_controller = workspace_controller.clone();
            let editor_weak = editor_weak.clone();
            let sql = sql.clone();

            dialog
                .title(prompt.title)
                .w(px(prompt.width_px))
                .child(
                    v_flex()
                        .gap_3()
                        .child(body_small(prompt.intro))
                        .child(body_small(prompt.message)),
                )
                .footer(move |_ok, cancel, _window, _cx| {
                    let save_sql = sql.clone();
                    vec![
                        cancel(_window, _cx),
                        Button::new("dont-save")
                            .label(prompt.discard_button)
                            .ghost()
                            .on_click({
                                let workspace_controller = workspace_controller.clone();
                                move |_, window, cx| {
                                    crate::workspace::force_close_active_tab_and_dialog(
                                        &workspace_controller,
                                        window,
                                        cx,
                                    );
                                }
                            })
                            .into_any_element(),
                        Button::new("save")
                            .label(prompt.save_button)
                            .primary()
                            .on_click({
                                let editor_weak = editor_weak.clone();
                                let workspace_controller = workspace_controller.clone();
                                let save_sql = save_sql.clone();
                                move |_, window, cx| {
                                    let decision = crate::workspace::save_close_decision(
                                        try_update_saved_query_for_editor(
                                            query_id,
                                            save_sql.clone(),
                                            editor_weak.clone(),
                                            cx,
                                        ),
                                    );
                                    match decision {
                                        crate::workspace::WorkspaceSaveCloseDecision::CloseTab => {
                                            let close_result =
                                                crate::workspace::force_close_active_tab_and_dialog(
                                                    &workspace_controller,
                                                    window,
                                                    cx,
                                                );
                                            if close_result
                                                == crate::workspace::WorkspaceCloseResult::Closed
                                            {
                                                window.push_notification(
                                                    Notification::success("Query saved"),
                                                    cx,
                                                );
                                            }
                                        }
                                        crate::workspace::WorkspaceSaveCloseDecision::ShowError(
                                            error_message,
                                        ) => {
                                            window.push_notification(
                                                Notification::error(error_message),
                                                cx,
                                            );
                                        }
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                })
                .on_cancel(move |_, _, _| true)
        });
    }

    fn open_close_new_query_unsaved_dialog(
        &mut self,
        editor_weak: WeakEntity<QueryEditor>,
        connection_id: Option<Uuid>,
        sql: String,
        current_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let prompt = crate::workspace::dirty_new_query_close_prompt();
        let Some(connection_id) = connection_id else {
            if let Some(message) = prompt.missing_connection_message {
                window.push_notification(Notification::warning(message), cx);
            }
            return;
        };

        let connection_name = cx
            .try_global::<AppState>()
            .and_then(|state| {
                state
                    .connection_service
                    .get_saved_connection_name(connection_id)
            })
            .unwrap_or_else(|| {
                prompt
                    .unknown_connection_name
                    .unwrap_or("Unknown")
                    .to_string()
            });

        let name_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder(prompt.query_name_placeholder.unwrap_or_default());
            state.set_value(current_name, window, cx);
            state
        });
        let error_message: Entity<Option<String>> = cx.new(|_| None);
        let workspace_controller = self.workspace_controller.clone();
        let sidebar_weak = self.connection_sidebar.downgrade();
        let focus_name_input = name_input.clone();

        cx.observe(&name_input, {
            let error_message = error_message.clone();
            move |_, _, cx| {
                error_message.update(cx, |message, cx| {
                    if message.is_some() {
                        *message = None;
                        cx.notify();
                    }
                });
            }
        })
        .detach();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let workspace_controller = workspace_controller.clone();
            let editor_weak = editor_weak.clone();
            let sidebar_weak = sidebar_weak.clone();
            let connection_name = connection_name.clone();
            let name_input = name_input.clone();
            let error_message = error_message.clone();
            let error_message_for_save = error_message.clone();
            let sql = sql.clone();

            dialog
                .title(prompt.title)
                .w(px(prompt.width_px))
                .child(
                    v_flex()
                        .gap_3()
                        .child(body_small(prompt.intro))
                        .child(body_small(prompt.message))
                        .child(
                            v_flex()
                                .gap_1()
                                .child(body_small(prompt.query_name_label.unwrap_or_default()))
                                .child(Input::new(&name_input)),
                        )
                        .child(
                            v_flex()
                                .gap_1()
                                .child(body_small(prompt.save_location_label.unwrap_or_default()))
                                .child(
                                    v_flex().child(
                                        div()
                                            .px_3()
                                            .py_2()
                                            .border_1()
                                            .border_color(cx.theme().border)
                                            .bg(cx.theme().muted)
                                            .rounded_md()
                                            .child(connection_name.clone()),
                                    ),
                                ),
                        )
                        .child({
                            let error = error_message.read(cx).clone();
                            div().text_xs().h(px(16.0)).when_some(error, |this, err| {
                                this.text_color(cx.theme().danger_text).child(err)
                            })
                        }),
                )
                .footer(move |_ok, cancel, _window, _cx| {
                    let save_sql = sql.clone();
                    vec![
                        cancel(_window, _cx),
                        Button::new("dont-save")
                            .label(prompt.discard_button)
                            .ghost()
                            .on_click({
                                let workspace_controller = workspace_controller.clone();
                                move |_, window, cx| {
                                    crate::workspace::force_close_active_tab_and_dialog(
                                        &workspace_controller,
                                        window,
                                        cx,
                                    );
                                }
                            })
                            .into_any_element(),
                        Button::new("save")
                            .label(prompt.save_button)
                            .primary()
                            .on_click({
                                let workspace_controller = workspace_controller.clone();
                                let editor_weak = editor_weak.clone();
                                let sidebar_weak = sidebar_weak.clone();
                                let name_input = name_input.clone();
                                let error_message_for_save = error_message_for_save.clone();
                                let save_sql = save_sql.clone();
                                move |_, window, cx| {
                                    let query_name =
                                        name_input.read(cx).text().to_string().trim().to_string();

                                    let decision = crate::workspace::save_close_decision(
                                        save_query_for_editor(
                                            editor_weak.clone(),
                                            save_sql.clone(),
                                            connection_id,
                                            query_name,
                                            sidebar_weak.clone(),
                                            window,
                                            cx,
                                        ),
                                    );

                                    match decision {
                                        crate::workspace::WorkspaceSaveCloseDecision::CloseTab => {
                                            crate::workspace::force_close_active_tab_and_dialog(
                                                &workspace_controller,
                                                window,
                                                cx,
                                            );
                                        }
                                        crate::workspace::WorkspaceSaveCloseDecision::ShowError(
                                            error,
                                        ) => {
                                            error_message_for_save.update(cx, |message, cx| {
                                                *message = Some(error);
                                                cx.notify();
                                            });
                                        }
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                })
                .on_cancel(move |_, _, _| true)
        });

        focus_name_input.focus_handle(cx).focus(window, cx);
    }

    fn open_close_non_query_unsaved_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let workspace_controller = self.workspace_controller.clone();
        let prompt = crate::workspace::dirty_non_query_close_prompt();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let workspace_controller = workspace_controller.clone();

            dialog
                .title(prompt.title)
                .child(prompt.message)
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text(prompt.discard_button)
                        .cancel_text(prompt.cancel_button),
                )
                .on_ok(move |_, window, cx| {
                    crate::workspace::force_close_active_tab_and_dialog(
                        &workspace_controller,
                        window,
                        cx,
                    );
                    true
                })
        });
    }

    pub(super) fn handle_close_other_tabs(
        &mut self,
        _action: &crate::actions::CloseOtherTabs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.close_other_tabs(window, cx));
    }

    pub(super) fn handle_tab_command(
        &mut self,
        command: zqlz_ui::widgets::dock::TabCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let zqlz_ui::widgets::dock::TabCommand::MoveToNewWindow { index } = command {
            self.move_tab_to_new_window(index, window, cx);
            return;
        }

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.handle_tab_command(command, window, cx);
        });
    }

    pub(super) fn handle_move_tab_to_new_window(
        &mut self,
        _action: &crate::actions::MoveTabToNewWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(active_index) = self
            .workspace_controller
            .read(cx)
            .active_center_tab_index(cx)
        else {
            window.push_notification(Notification::info("No active tab to move"), cx);
            return;
        };

        self.move_tab_to_new_window(active_index, window, cx);
    }

    fn move_tab_to_new_window(
        &mut self,
        tab_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let can_move = self
            .workspace_controller
            .read(cx)
            .center_tab_metadata(cx)
            .get(tab_index)
            .is_some_and(|metadata| metadata.can_move_to_new_window);
        if !can_move {
            window.push_notification(
                Notification::info("This tab cannot move to another window yet"),
                cx,
            );
            return;
        }

        let detached_item = self.workspace_controller.update(cx, |workspace, cx| {
            workspace.detach_center_item_at(tab_index, window, cx)
        });

        let Some(detached_item) = detached_item else {
            window.push_notification(Notification::info("Tab is no longer available"), cx);
            return;
        };

        let query_editor = detached_item.panel.view().downcast::<QueryEditor>().ok();
        if let Some(query_editor) = &query_editor {
            self.detach_query_editor_subscription(query_editor);
        }
        if let Ok(table_viewer) = detached_item.panel.view().downcast::<TableViewerPanel>() {
            self.detach_table_viewer_subscription(&table_viewer);
        }

        let result = self.workspace_controller.update(cx, |controller, cx| {
            controller.open_new_window(cx, move |window, cx| {
                zqlz_settings::ZqlzSettings::global(cx).clone().apply(cx);

                let main_view = cx.new(|cx| MainView::new(window, cx));
                main_view.update(cx, |main_view, cx| {
                    if let Some(query_editor) = query_editor {
                        main_view.adopt_moved_query_editor(query_editor, window, cx);
                    }
                    main_view.workspace_controller.update(cx, |workspace, cx| {
                        workspace.add_detached_center_item(detached_item, window, cx);
                    });
                });

                cx.new(|cx| zqlz_ui::widgets::Root::new(main_view, window, cx))
            })
        });

        if let Err(error) = result {
            tracing::error!(%error, "Failed to move tab to new window");
            window.push_notification(Notification::error("Failed to move tab to new window"), cx);
        }
    }

    pub(super) fn handle_close_tabs_to_right(
        &mut self,
        _action: &crate::actions::CloseTabsToRight,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.close_tabs_to_right(window, cx)
        });
    }

    pub(super) fn handle_close_tabs_to_left(
        &mut self,
        _action: &crate::actions::CloseTabsToLeft,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.close_tabs_to_left(window, cx));
    }

    pub(super) fn handle_close_clean_tabs(
        &mut self,
        _action: &crate::actions::CloseCleanTabs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.close_clean_tabs(window, cx));
    }

    pub(super) fn handle_toggle_pin_active_tab(
        &mut self,
        _action: &crate::actions::TogglePinActiveTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.toggle_pin_active_tab(cx));
    }

    pub(super) fn handle_pin_tab(
        &mut self,
        _action: &crate::actions::PinTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.pin_active_tab(cx));
    }

    pub(super) fn handle_unpin_tab(
        &mut self,
        _action: &crate::actions::UnpinTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.unpin_active_tab(cx));
    }

    pub(super) fn handle_mark_active_tab_as_preview(
        &mut self,
        _action: &crate::actions::MarkActiveTabAsPreview,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.mark_active_tab_as_preview(cx));
    }

    pub(super) fn handle_clear_active_tab_preview(
        &mut self,
        _action: &crate::actions::ClearActiveTabPreview,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.clear_active_tab_preview(cx));
    }

    pub(super) fn handle_close_all_tabs(
        &mut self,
        _action: &crate::actions::CloseAllTabs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.close_all_tabs(window, cx));
    }

    fn activate_tab_number(&self, number: usize, window: &mut Window, cx: &mut Context<Self>) {
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.activate_tab_by_number(number, window, cx)
        });
    }

    pub(super) fn handle_activate_tab_1(
        &mut self,
        _action: &crate::actions::ActivateTab1,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(1, window, cx);
    }

    pub(super) fn handle_activate_tab_2(
        &mut self,
        _action: &crate::actions::ActivateTab2,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(2, window, cx);
    }

    pub(super) fn handle_activate_tab_3(
        &mut self,
        _action: &crate::actions::ActivateTab3,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(3, window, cx);
    }

    pub(super) fn handle_activate_tab_4(
        &mut self,
        _action: &crate::actions::ActivateTab4,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(4, window, cx);
    }

    pub(super) fn handle_activate_tab_5(
        &mut self,
        _action: &crate::actions::ActivateTab5,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(5, window, cx);
    }

    pub(super) fn handle_activate_tab_6(
        &mut self,
        _action: &crate::actions::ActivateTab6,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(6, window, cx);
    }

    pub(super) fn handle_activate_tab_7(
        &mut self,
        _action: &crate::actions::ActivateTab7,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(7, window, cx);
    }

    pub(super) fn handle_activate_tab_8(
        &mut self,
        _action: &crate::actions::ActivateTab8,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(8, window, cx);
    }

    pub(super) fn handle_activate_tab_9(
        &mut self,
        _action: &crate::actions::ActivateTab9,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.activate_tab_number(9, window, cx);
    }

    pub(super) fn handle_activate_last_tab(
        &mut self,
        _action: &crate::actions::ActivateLastTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.workspace_controller
            .update(cx, |workspace, cx| workspace.activate_last_tab(window, cx));
    }

    // ====================
    // Universal Refresh Action
    // ====================

    /// Handle Refresh action (Cmd+R / Ctrl+R)
    ///
    /// This is a universal refresh action that delegates to the appropriate panel
    /// based on what's currently focused/active:
    /// - TableViewer: Reloads the current table data
    /// - QueryEditor (View): Re-executes the view query
    /// - ConnectionSidebar: Refreshes connections or schema
    /// - ObjectsPanel: Reloads the objects list
    pub(super) fn handle_refresh(
        &mut self,
        _action: &Refresh,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Refresh action triggered");

        let active_panel_name = self
            .workspace_controller
            .read(cx)
            .active_center_panel_name(cx);
        if let Some(panel_name) = active_panel_name {
            tracing::info!("Active panel for refresh: {}", panel_name);
        } else {
            tracing::debug!("No active panel, defaulting refresh route");
        }

        match active_panel_name {
            Some("TableViewer") => {
                if let Some(viewer) = self
                    .workspace_controller
                    .read(cx)
                    .active_center_view::<TableViewerPanel>(cx)
                {
                    viewer.update(cx, |viewer, cx| {
                        viewer.refresh(cx);
                    });
                }
            }
            Some("QueryEditor") => {
                tracing::info!("Refreshing QueryEditor - executing query");
                self.handle_execute_query(&crate::actions::ExecuteQuery, window, cx);
            }
            Some("ObjectsPanel") => {
                self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
            }
            Some("ConnectionSidebar") | None => {
                self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
            }
            Some(panel_name) => {
                let message = format!("Refresh is not available for {}", panel_name);
                tracing::debug!(panel = %panel_name, "Blocked unavailable refresh action");
                window.push_notification(Notification::warning(message), cx);
            }
        }
    }

    pub(super) fn open_settings_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Create the settings panel if it doesn't exist
        if self.settings_panel.is_none() {
            let settings_panel = cx.new(|cx| SettingsPanel::new(window, cx));
            self.settings_panel = Some(settings_panel.clone());

            // Subscribe to settings changes
            let _settings_panel_for_sub = settings_panel.clone();
            cx.subscribe(
                &settings_panel,
                move |_this, _, event: &SettingsPanelEvent, _cx| match event {
                    SettingsPanelEvent::SettingsChanged => {
                        tracing::debug!("Settings changed");
                        _this.sync_query_editor_settings(_cx);
                    }
                    SettingsPanelEvent::BackRequested => {
                        _this.show_settings_page = false;
                        _cx.notify();
                    }
                },
            )
            .detach();
        }

        self.show_settings_page = true;
        cx.notify();
    }

    /// Handle objects panel events
    pub(super) fn handle_objects_panel_event(
        &mut self,
        event: &ObjectsPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ObjectsPanelEvent::InvokeAction {
                connection_id,
                action_id,
                object_refs,
            } => {
                let database_name = self
                    .workspace_state
                    .read(cx)
                    .active_database()
                    .map(ToString::to_string);
                let feature_availability = self
                    .workspace_state
                    .read(cx)
                    .connection_feature_set(*connection_id, database_name.as_deref())
                    .map(|features| {
                        objects_panel_action_feature_availability(&features.objects, action_id)
                    });
                if let Some(availability) = feature_availability
                    && !availability.available
                {
                    window.push_notification(
                        Notification::warning(availability.reason_or(format!(
                            "The '{action_id}' action is not available for this connection"
                        ))),
                        cx,
                    );
                    return;
                }
                let registry = ObjectsPanelActionRegistry::default();
                let manifest = self.objects_panel.read(cx).current_manifest(cx);
                let action_resolution = registry.resolve_objects_panel_action_with_manifest(
                    action_id,
                    object_refs,
                    database_name,
                    Some(&manifest),
                );
                Self::emit_objects_panel_action_telemetry(
                    action_id,
                    object_refs.len(),
                    &action_resolution,
                );
                // Surface resolution failures directly because the planner still has to
                // degrade safely while the registry cutover continues.
                if let Some(message) = objects_panel_action_issue_message(&action_resolution) {
                    window.push_notification(Notification::error(message), cx);
                }
                let connection_id = *connection_id;
                action_resolution.execute(self, connection_id, window, cx);
            }
            ObjectsPanelEvent::ActiveKindChanged {
                connection_id,
                kind_id,
                scope_id,
            } => {
                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available for objects panel kind load");
                    return;
                };
                let connection_service = app_state.connection_service.clone();
                let objects_panel = self.objects_panel.downgrade();
                let connection_id = *connection_id;
                let kind_id = kind_id.clone();
                let scope_id = scope_id.clone();
                let target_database = self
                    .workspace_state
                    .read(cx)
                    .active_database()
                    .map(ToString::to_string);

                self.objects_panel
                    .update(cx, |panel, cx| panel.set_loading(true, cx));

                cx.spawn(async move |_this, cx| {
                    match connection_service
                        .load_objects_panel_kind_data(
                            connection_id,
                            target_database,
                            kind_id.as_str(),
                            scope_id,
                        )
                        .await
                    {
                        Ok(data) => {
                            if let Err(error) = objects_panel.update(cx, |panel, cx| {
                                panel.set_loading(false, cx);
                                panel.replace_kind_objects(kind_id.as_str(), data, cx);
                            }) {
                                tracing::warn!(
                                    %error,
                                    connection_id = %connection_id,
                                    kind_id = %kind_id,
                                    "Failed to apply lazy objects panel kind data"
                                );
                            }
                        }
                        Err(error) => {
                            if let Err(clear_error) = objects_panel.update(cx, |panel, cx| {
                                panel.set_loading(false, cx);
                            }) {
                                tracing::warn!(
                                    %clear_error,
                                    connection_id = %connection_id,
                                    kind_id = %kind_id,
                                    "Failed to clear lazy objects panel loading state"
                                );
                            }
                            tracing::warn!(
                                %error,
                                connection_id = %connection_id,
                                kind_id = %kind_id,
                                "Failed to load lazy objects panel kind data"
                            );
                        }
                    }
                })
                .detach();
            }
        }
    }

    /// Handle table designer panel events
    pub(super) fn handle_table_designer_event(
        &mut self,
        panel: Entity<zqlz_table_designer::TableDesignerPanel>,
        event: zqlz_table_designer::TableDesignerEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            zqlz_table_designer::TableDesignerEvent::Save {
                connection_id,
                design,
                is_new,
                original_design,
            } => {
                self.save_table_design(
                    TableDesignSaveRequest {
                        connection_id,
                        design,
                        is_new,
                        original_design,
                        panel,
                    },
                    window,
                    cx,
                );
            }
            zqlz_table_designer::TableDesignerEvent::Cancel => {
                self.workspace_controller.update(cx, |workspace, cx| {
                    workspace.remove_center_item(std::sync::Arc::new(panel), window, cx);
                });
                tracing::info!("Table designer closed");
            }
            zqlz_table_designer::TableDesignerEvent::PreviewDdl { .. } => {
                // DDL preview is handled internally by the panel.
            }
        }
    }

    fn sync_query_editor_settings(&mut self, cx: &mut Context<Self>) {
        let mut live_editors = Vec::with_capacity(self.query_editors.len());

        for weak_editor in self.query_editors.drain(..) {
            if let Some(editor) = weak_editor.upgrade() {
                editor.update(cx, |editor, cx| {
                    editor.refresh_settings(cx);
                });
                live_editors.push(editor.downgrade());
            }
        }

        self.query_editors = live_editors;
    }

    /// Handle events from the template library panel
    #[allow(dead_code)]
    pub(super) fn handle_template_library_event(
        &mut self,
        event: &TemplateLibraryEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            TemplateLibraryEvent::UseTemplate { template_sql, .. } => {
                tracing::info!("Using template from library");

                if let Some(editor) = self.active_query_editor(cx) {
                    editor.update(cx, |editor, cx| {
                        editor.set_content(template_sql.clone(), window, cx);
                    });
                } else {
                    let editor = self.query_facade_create_new_query_editor(window, cx);
                    editor.update(cx, |editor, cx| {
                        editor.set_content(template_sql.clone(), window, cx);
                    });
                }
            }
            TemplateLibraryEvent::EditTemplate(_) => {
                // Template editing is handled within the panel.
            }
            TemplateLibraryEvent::TemplateDeleted(_) => {
                tracing::info!("Template deleted");
            }
            TemplateLibraryEvent::TemplateSaved(_) => {
                tracing::info!("Template saved");
            }
        }
    }

    /// Handle events from the project manager panel
    #[allow(dead_code)]
    pub(super) fn handle_project_manager_event(
        &mut self,
        event: &ProjectManagerEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ProjectManagerEvent::ProjectSelected(project_id) => {
                tracing::info!("Project selected: {}", project_id);
            }
            ProjectManagerEvent::OpenModel {
                project_id,
                model_id,
            } => {
                tracing::info!("Opening model {} from project {}", model_id, project_id);

                self.query_facade_open_project_model(*project_id, *model_id, window, cx);
            }
            ProjectManagerEvent::CreateModel(project_id) => {
                tracing::info!("Creating new model in project {}", project_id);
                // Create a new query editor for the model
                self.query_facade_create_new_query_editor(window, cx);
            }
            ProjectManagerEvent::CompileModel {
                project_id,
                model_id,
            } => {
                tracing::info!("Compiling model {} from project {}", model_id, project_id);
            }
            ProjectManagerEvent::ProjectsChanged => {
                tracing::info!("Projects list changed");
            }
        }
    }

    /// Handle events from the results panel (e.g., clicking on problems to navigate)
    pub(super) fn handle_results_panel_event(
        &mut self,
        event: ResultsPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ResultsPanelEvent::GoToLine { line, column } => {
                self.handle_results_panel_go_to_line_event(line, column, window, cx);
            }
            ResultsPanelEvent::ReloadDiagnostics => {
                self.handle_results_panel_reload_diagnostics_event(window, cx);
            }
        }
    }

    fn handle_results_panel_go_to_line_event(
        &mut self,
        line: usize,
        column: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("GoToLine event: line={}, column={}", line, column);

        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.go_to_line(line.saturating_sub(1), column.saturating_sub(1), window, cx);
            });
        } else {
            tracing::debug!("No active query editor found to navigate to line");
        }
    }

    fn handle_results_panel_reload_diagnostics_event(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ReloadDiagnostics event received");

        self.results_panel.update(cx, |panel, cx| {
            panel.set_diagnostics_loading(true, cx);
        });

        if let Some(editor) = self.active_query_editor(cx) {
            editor.update(cx, |editor, cx| {
                editor.reload_diagnostics(cx);
            });
        } else {
            tracing::debug!("ReloadDiagnostics: no active editor found");
            self.results_panel.update(cx, |panel, cx| {
                panel.set_diagnostics_loading(false, cx);
            });
        }
    }

    /// Convert UI Diagnostic to EditorDiagnostic for WorkspaceState
    #[allow(dead_code)]
    fn convert_to_editor_diagnostic(
        diag: &zqlz_ui::widgets::highlighter::Diagnostic,
    ) -> crate::workspace_state::EditorDiagnostic {
        use crate::workspace_state::{DiagnosticSeverity, EditorDiagnostic};

        EditorDiagnostic {
            line: diag.range.start.line as usize,
            column: diag.range.start.character as usize,
            end_line: diag.range.end.line as usize,
            end_column: diag.range.end.character as usize,
            message: diag.message.to_string(),
            severity: match diag.severity {
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Error => {
                    DiagnosticSeverity::Error
                }
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Warning => {
                    DiagnosticSeverity::Warning
                }
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Info => DiagnosticSeverity::Info,
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Hint => DiagnosticSeverity::Hint,
            },
            source: diag.source.as_ref().map(|s| s.to_string()),
        }
    }

    /// Handle events from the inspector panel
    pub(super) fn handle_inspector_panel_event(
        &mut self,
        event: crate::components::InspectorPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            crate::components::InspectorPanelEvent::ViewChanged(view) => {
                tracing::debug!(?view, "Inspector panel view changed");
            }
            crate::components::InspectorPanelEvent::OpenQuery { sql } => {
                self.handle_inspector_open_query_event(sql, window, cx);
            }
            crate::components::InspectorPanelEvent::ClearHistory => {
                // Clear query history in AppState.
                if let Some(app_state) = cx.try_global::<AppState>() {
                    app_state.clear_query_history();
                    tracing::info!("Query history cleared");
                }

                // Update the history panel to reflect the cleared state.
                self.inspector_panel.update(cx, |panel, cx| {
                    panel.query_history_panel().update(cx, |history_panel, cx| {
                        history_panel.update_entries(Vec::new(), cx);
                    });
                });

                self.refresh_query_history(cx);
            }
        }
    }

    fn handle_inspector_open_query_event(
        &mut self,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Opening query from history");

        if let Some(editor) = self.active_query_editor(cx) {
            self.handle_inspector_open_query_with_active_editor(editor, sql, window, cx);
        } else {
            self.handle_inspector_open_query_without_active_editor(sql, window, cx);
        }
    }

    fn handle_inspector_open_query_with_active_editor(
        &mut self,
        editor: Entity<QueryEditor>,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Always ask whether to reuse the current tab so history-open does
        // not unexpectedly replace in-progress editor state.
        let this_weak = cx.weak_entity();

        window.open_dialog(cx, move |dialog, _window, _cx| {
            let editor_for_current = editor.clone();
            let sql_for_current = sql.clone();

            let this_weak_for_new = this_weak.clone();
            let sql_for_new = sql.clone();

            dialog
                .title("Open Query")
                .child("Open this query in the current tab or in a new tab?")
                .overlay_closable(false)
                .close_button(false)
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Current Tab")
                        .cancel_text("Cancel"),
                )
                .on_ok(move |_, window, cx| {
                    editor_for_current.update(cx, |query_editor, cx| {
                        query_editor.set_text(&sql_for_current, window, cx);
                    });
                    let focus_handle = editor_for_current.read(cx).focus_handle(cx);
                    window.focus(&focus_handle, cx);
                    true
                })
                .footer(move |ok, cancel, window, cx| {
                    let main_view = this_weak_for_new.clone();
                    let sql = sql_for_new.clone();

                    let new_tab_button = Button::new("new-tab")
                        .secondary()
                        .label("New Tab")
                        .on_click(move |_, window, cx| {
                            window.close_dialog(cx);
                            if let Err(error) = main_view.update(cx, |main_view, cx| {
                                let editor =
                                    main_view.query_facade_create_new_query_editor(window, cx);
                                editor.update(cx, |query_editor, cx| {
                                    query_editor.set_text(&sql, window, cx);
                                });
                                let focus_handle = editor.read(cx).focus_handle(cx);
                                window.focus(&focus_handle, cx);
                            }) {
                                tracing::warn!(%error, "Failed to open inspector query in new tab");
                            }
                        })
                        .into_any_element();

                    vec![cancel(window, cx), new_tab_button, ok(window, cx)]
                })
        });
    }

    fn handle_inspector_open_query_without_active_editor(
        &mut self,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("No active query editor found, creating new one");
        let editor = self.query_facade_create_new_query_editor(window, cx);
        editor.update(cx, |query_editor, cx| {
            query_editor.set_text(&sql, window, cx);
        });
        let focus_handle = editor.read(cx).focus_handle(cx);
        window.focus(&focus_handle, cx);
    }
}
