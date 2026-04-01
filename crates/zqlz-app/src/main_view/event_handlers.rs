// Event handlers for MainView

use std::path::{Path, PathBuf};
use std::sync::Arc;

use gpui::prelude::FluentBuilder;
use gpui::*;

use crate::actions::*;
use crate::app::AppState;
use crate::components::{
    Command, CommandCategory, CommandPalette, CommandPaletteEvent, CommandUsagePersistence,
    ConnectionSidebarEvent, ObjectsPanelEvent, ProjectManagerEvent, QueryEditor, ResultsPanelEvent,
    SettingsPanel, SettingsPanelEvent, TableViewerPanel, TemplateLibraryEvent,
};
use crate::main_view::table_handlers::table_ops::design::TableDesignSaveRequest;
use crate::workspace_state::RefreshScope;
use zqlz_connection::SidebarObjectCapabilities;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::{Button, ButtonVariants},
    dialog::DialogButtonProps,
    input::{Input, InputState},
    notification::Notification,
    typography::body_small,
    v_flex,
};
use zqlz_versioning::DatabaseObjectType;

use super::{
    MainView,
    saved_query_handlers::{save_query_for_editor, update_saved_query_for_editor},
};

impl MainView {
    /// Find the active query editor from the dock, falling back to the most
    /// recently created editor in `self.query_editors`.
    pub(super) fn active_query_editor(&self, cx: &App) -> Option<Entity<QueryEditor>> {
        if let Some(panel) = self.dock_area.read(cx).active_panel(cx)
            && let Ok(editor) = panel.view().downcast::<QueryEditor>()
        {
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
                                this.open_sql_file_in_query_editor(&path, content, window, cx);
                            }) {
                                tracing::warn!(%error, "failed to open SQL file in window");
                            }
                        }
                        Err(error) => {
                            if let Err(update_error) = this.update_in(cx, |_, window, cx| {
                                window.push_notification(
                                    zqlz_ui::widgets::notification::Notification::error(format!(
                                        "Failed to open SQL file: {error}"
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
            Some("db") | Some("sqlite") | Some("sqlite3") | Some("duckdb") => {
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

    fn open_dropped_paths(
        &mut self,
        paths: Vec<PathBuf>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for path in paths {
            self.open_external_path(&path, window, cx);
        }
    }

    /// Handles events emitted by the left sidebar (ConnectionSidebar).
    pub(super) fn handle_sidebar_event(
        &mut self,
        event: ConnectionSidebarEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ConnectionSidebarEvent::AddConnection => {
                self.open_new_connection_dialog(window, cx);
            }
            ConnectionSidebarEvent::CloseAllConnections => {
                let connected_ids: Vec<uuid::Uuid> = self
                    .connection_sidebar
                    .read(cx)
                    .connections()
                    .iter()
                    .filter(|c| c.is_connected)
                    .map(|c| c.id)
                    .collect();
                for id in connected_ids {
                    self.disconnect_from_database(id, cx);
                }
            }
            ConnectionSidebarEvent::NewGroup => {
                // TODO: Implement connection grouping
                tracing::info!("New Group requested (not yet implemented)");
            }
            ConnectionSidebarEvent::Connect(id) => {
                self.connect_to_database(id, window, cx);
            }
            ConnectionSidebarEvent::Disconnect(id) => {
                self.disconnect_from_database(id, cx);
            }
            ConnectionSidebarEvent::Selected(id) => {
                // Update WorkspaceState with the selected connection (source of truth)
                // This emits ActiveConnectionChanged event, which triggers refresh_objects_panel
                // via handle_workspace_state_event - no need to call it directly here
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(id), cx);
                });
                cx.notify();
            }
            ConnectionSidebarEvent::NewQuery(id) => {
                // First set the connection in WorkspaceState, then create the query
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(id), cx);
                });
                self.handle_new_query(&NewQuery, window, cx);
            }
            ConnectionSidebarEvent::RefreshConnections => {
                self.request_refresh(RefreshScope::ConnectionsList, cx);
            }
            ConnectionSidebarEvent::OpenTable {
                connection_id,
                table_name,
                database_name,
            } => {
                self.open_table_viewer(
                    connection_id,
                    table_name,
                    database_name.clone(),
                    false,
                    window,
                    cx,
                );
            }
            ConnectionSidebarEvent::OpenView {
                connection_id,
                view_name,
                database_name,
            } => {
                // Views can be queried like tables, so we reuse the table viewer
                self.open_table_viewer(
                    connection_id,
                    view_name,
                    database_name.clone(),
                    true,
                    window,
                    cx,
                );
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
                self.copy_view_name(&view_name, cx);
            }
            ConnectionSidebarEvent::DeleteConnection(id) => {
                self.delete_connection(id, window, cx);
            }
            ConnectionSidebarEvent::DuplicateConnection(id) => {
                self.duplicate_connection(id, window, cx);
            }
            ConnectionSidebarEvent::OpenConnectionSettings(id) => {
                self.open_connection_settings(id, window, cx);
            }
            ConnectionSidebarEvent::OpenDroppedPaths(paths) => {
                self.open_dropped_paths(paths, window, cx);
            }

            // Table-specific events
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
                self.delete_tables(connection_id, vec![table_name.clone()], window, cx);
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
                    vec![]
                } else {
                    vec![table_name.clone()]
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
                self.copy_table_name(&table_name, cx);
            }
            ConnectionSidebarEvent::RefreshSchema { connection_id } => {
                self.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
            }

            // Saved queries events
            ConnectionSidebarEvent::OpenSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                tracing::info!(query_id = %query_id, query_name = %query_name, connection_id = %connection_id, "Opening saved query from sidebar");
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                });
                self.open_saved_query(query_id, connection_id, window, cx);
            }
            ConnectionSidebarEvent::DeleteSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                self.delete_saved_query(query_id, query_name, connection_id, window, cx);
            }
            ConnectionSidebarEvent::RenameSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                self.rename_saved_query(query_id, query_name, connection_id, window, cx);
            }

            // Version history events
            ConnectionSidebarEvent::ViewHistory {
                connection_id,
                object_name,
                object_schema,
                object_type,
            } => {
                let db_object_type = match object_type.as_str() {
                    "table" => DatabaseObjectType::Table,
                    "view" => DatabaseObjectType::View,
                    "function" => DatabaseObjectType::Function,
                    "procedure" => DatabaseObjectType::Procedure,
                    "trigger" => DatabaseObjectType::Trigger,
                    _ => {
                        tracing::warn!("Unknown object type for version history: {}", object_type);
                        return;
                    }
                };
                self.show_version_history(
                    connection_id,
                    object_name,
                    object_schema,
                    db_object_type,
                    window,
                    cx,
                );
            }

            // Function events
            ConnectionSidebarEvent::OpenFunction {
                connection_id,
                function_name,
                object_schema,
            } => {
                // Open a query editor with the function definition
                self.open_function_definition(
                    connection_id,
                    function_name,
                    object_schema,
                    window,
                    cx,
                );
            }

            // Procedure events
            ConnectionSidebarEvent::OpenProcedure {
                connection_id,
                procedure_name,
                object_schema,
            } => {
                // Open a query editor with the procedure definition
                self.open_procedure_definition(
                    connection_id,
                    procedure_name,
                    object_schema,
                    window,
                    cx,
                );
            }

            // Trigger events
            ConnectionSidebarEvent::DesignTrigger {
                connection_id,
                trigger_name,
                object_schema,
            } => {
                self.design_trigger(connection_id, trigger_name, object_schema, window, cx);
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
                self.open_trigger_designer(connection_id, trigger_name, object_schema, window, cx);
            }

            // Redis-specific events
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

            // Multi-database events
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
                let database_changed =
                    active_database_before.as_deref() != Some(database_name.as_str());

                self.connection_sidebar.update(cx, |sidebar, cx| {
                    sidebar.set_database_loading(connection_id, &database_name, true, cx);
                });

                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_connection(Some(connection_id), cx);
                    state.set_active_database(Some(database_name.clone()), cx);
                });

                if !database_changed {
                    self.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
                }
            }
            ConnectionSidebarEvent::LoadSection {
                connection_id,
                section,
            } => {
                self.load_sidebar_section(connection_id, section, window, cx);
            }
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
        let center_panel = self.dock_area.read(cx).center_tab_panel();
        if center_panel.is_none() {
            tracing::warn!("No center TabPanel found for context menu");
            return;
        }
        let center_panel = center_panel.unwrap();
        let center_panel_weak = center_panel.downgrade();

        let tab_count = center_panel.read(cx).panel_count();
        let is_last_tab = tab_index >= tab_count.saturating_sub(1);

        let focus_handle = self.focus_handle.clone();

        let new_menu =
            zqlz_ui::widgets::menu::PopupMenu::build(window, cx, |menu, _window, _cx| {
                menu.action_context(focus_handle)
                    .item(
                        zqlz_ui::widgets::menu::PopupMenuItem::new("Close")
                            .action(crate::actions::CloseActiveTab.boxed_clone())
                            .on_click({
                                let panel = center_panel_weak.clone();
                                move |_event, window, cx| {
                                    _ = panel.update(cx, |p, cx| {
                                        p.close_panel_at(tab_index, window, cx);
                                    });
                                }
                            }),
                    )
                    .item(
                        zqlz_ui::widgets::menu::PopupMenuItem::new("Close Other Tabs")
                            .action(crate::actions::CloseOtherTabs.boxed_clone())
                            .on_click({
                                let panel = center_panel_weak.clone();
                                move |_event, window, cx| {
                                    _ = panel.update(cx, |p, cx| {
                                        p.close_other_tabs(tab_index, window, cx);
                                    });
                                }
                            }),
                    )
                    .item(
                        zqlz_ui::widgets::menu::PopupMenuItem::new("Close Tabs to the Right")
                            .action(crate::actions::CloseTabsToRight.boxed_clone())
                            .disabled(is_last_tab)
                            .on_click({
                                let panel = center_panel_weak.clone();
                                move |_event, window, cx| {
                                    _ = panel.update(cx, |p, cx| {
                                        p.close_tabs_to_right(tab_index, window, cx);
                                    });
                                }
                            }),
                    )
                    .separator()
                    .item(
                        zqlz_ui::widgets::menu::PopupMenuItem::new("Close All")
                            .action(crate::actions::CloseAllTabs.boxed_clone())
                            .on_click({
                                let panel = center_panel_weak.clone();
                                move |_event, window, cx| {
                                    _ = panel.update(cx, |p, cx| {
                                        p.close_all_tabs(window, cx);
                                    });
                                }
                            }),
                    )
            });

        if self.tab_context_menu.is_none() {
            self.tab_context_menu = Some(super::tab_menu::TabContextMenuState::new(window, cx));
        }

        if let Some(menu_state) = &self.tab_context_menu {
            menu_state.update(cx, |state, cx| {
                state.menu_subscription.take();
                state.position = position;
                state.tab_index = tab_index;
                state.menu = new_menu.clone();

                let menu_entity = state.menu.clone();
                let menu_state_entity = cx.entity().clone();
                state.menu_subscription = Some(cx.subscribe(
                    &menu_entity,
                    move |_state, _, _event: &DismissEvent, cx| {
                        let menu_state = menu_state_entity.clone();
                        cx.defer(move |cx| {
                            menu_state.update(cx, |state, cx| {
                                state.open = false;
                                cx.notify();
                            });
                        });
                    },
                ));

                state.open = true;

                if !new_menu.focus_handle(cx).contains_focused(window, cx) {
                    new_menu.focus_handle(cx).focus(window, cx);
                }

                cx.notify();
            });
        }
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

    /// Handle opening the command palette
    pub(super) fn handle_open_command_palette(
        &mut self,
        _action: &OpenCommandPalette,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("Opening command palette");

        let commands = Self::build_static_commands();

        // If the palette is already open, reset it and re-focus.
        if let Some(palette) = &self.command_palette {
            palette.update(cx, |palette, cx| {
                palette.reset(commands, window, cx);
            });
            self.load_schema_commands_into_palette(cx);
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

        let subscription = cx.subscribe_in(
            &palette,
            window,
            |this, _palette, event: &CommandPaletteEvent, window, cx| match event {
                CommandPaletteEvent::Dismissed => {
                    this.begin_dismiss_command_palette(cx);
                }
                CommandPaletteEvent::CommandExecuted(cmd_id) => {
                    tracing::debug!(command_id = %cmd_id, "Command executed from palette");
                }
                CommandPaletteEvent::ConnectToConnection(connection_id) => {
                    this.connect_to_database(*connection_id, window, cx);
                }
                CommandPaletteEvent::OpenTable {
                    connection_id,
                    table_name,
                } => {
                    this.open_table_viewer(
                        *connection_id,
                        table_name.clone(),
                        None,
                        false,
                        window,
                        cx,
                    );
                }
                CommandPaletteEvent::OpenView {
                    connection_id,
                    view_name,
                } => {
                    this.open_table_viewer(
                        *connection_id,
                        view_name.clone(),
                        None,
                        true,
                        window,
                        cx,
                    );
                }
            },
        );
        self._command_palette_subscription = Some(subscription);

        palette.update(cx, |palette, cx| {
            palette.focus(window, cx);
        });

        self.command_palette = Some(palette);
        cx.notify();
    }

    fn build_static_commands() -> Vec<Command> {
        vec![
            // ── Application ─────────────────────────────────────────
            Command::new_static(
                "settings",
                "Open Settings",
                CommandCategory::Application,
                OpenSettings,
            ),
            Command::new_static("refresh", "Refresh", CommandCategory::Application, Refresh),
            Command::new_static("quit", "Quit", CommandCategory::Application, Quit),
            // ── Connection ──────────────────────────────────────────
            Command::new_static(
                "new-connection",
                "New Connection",
                CommandCategory::Connection,
                NewConnection,
            ),
            Command::new_static(
                "refresh-connection",
                "Refresh Connection",
                CommandCategory::Connection,
                RefreshConnection,
            ),
            Command::new_static(
                "refresh-connections-list",
                "Refresh Connections List",
                CommandCategory::Connection,
                RefreshConnectionsList,
            ),
            // ── Query ───────────────────────────────────────────────
            Command::new_static("new-query", "New Query", CommandCategory::Query, NewQuery),
            Command::new_static(
                "execute-query",
                "Execute Query",
                CommandCategory::Query,
                ExecuteQuery,
            ),
            Command::new_static(
                "execute-selection",
                "Execute Selection",
                CommandCategory::Query,
                ExecuteSelection,
            ),
            Command::new_static(
                "execute-current-statement",
                "Execute Current Statement",
                CommandCategory::Query,
                ExecuteCurrentStatement,
            ),
            Command::new_static(
                "explain-query",
                "Explain Query",
                CommandCategory::Query,
                ExplainQuery,
            ),
            Command::new_static(
                "explain-selection",
                "Explain Selection",
                CommandCategory::Query,
                ExplainSelection,
            ),
            Command::new_static(
                "stop-query",
                "Stop Query",
                CommandCategory::Query,
                StopQuery,
            ),
            Command::new_static(
                "format-query",
                "Format Query",
                CommandCategory::Query,
                zqlz_text_editor::actions::FormatSQL,
            ),
            Command::new_static(
                "save-query",
                "Save Query",
                CommandCategory::Query,
                SaveQuery,
            ),
            Command::new_static(
                "save-query-as",
                "Save Query As…",
                CommandCategory::Query,
                SaveQueryAs,
            ),
            Command::new_static(
                "toggle-problems-panel",
                "Toggle Problems Panel",
                CommandCategory::Query,
                ToggleProblemsPanel,
            ),
            // ── Editor ──────────────────────────────────────────────
            Command::new_static(
                "toggle-line-comment",
                "Toggle Line Comment",
                CommandCategory::Editor,
                zqlz_text_editor::actions::ToggleLineComment,
            ),
            Command::new_static(
                "delete-line",
                "Delete Line",
                CommandCategory::Editor,
                zqlz_text_editor::actions::DeleteLine,
            ),
            Command::new_static(
                "move-line-up",
                "Move Line Up",
                CommandCategory::Editor,
                zqlz_text_editor::actions::MoveLineUp,
            ),
            Command::new_static(
                "move-line-down",
                "Move Line Down",
                CommandCategory::Editor,
                zqlz_text_editor::actions::MoveLineDown,
            ),
            Command::new_static(
                "find-next",
                "Find Next",
                CommandCategory::Editor,
                zqlz_text_editor::actions::FindNext,
            ),
            Command::new_static(
                "find-previous",
                "Find Previous",
                CommandCategory::Editor,
                zqlz_text_editor::actions::FindPrevious,
            ),
            // ── Layout ──────────────────────────────────────────────
            Command::new_static(
                "toggle-left-sidebar",
                "Toggle Left Sidebar",
                CommandCategory::Layout,
                ToggleLeftSidebar,
            ),
            Command::new_static(
                "toggle-right-sidebar",
                "Toggle Right Sidebar",
                CommandCategory::Layout,
                ToggleRightSidebar,
            ),
            Command::new_static(
                "toggle-bottom-panel",
                "Toggle Bottom Panel",
                CommandCategory::Layout,
                ToggleBottomPanel,
            ),
            // ── Tab ─────────────────────────────────────────────────
            Command::new_static(
                "next-tab",
                "Next Tab",
                CommandCategory::Tab,
                ActivateNextTab,
            ),
            Command::new_static(
                "previous-tab",
                "Previous Tab",
                CommandCategory::Tab,
                ActivatePrevTab,
            ),
            Command::new_static(
                "close-tab",
                "Close Tab",
                CommandCategory::Tab,
                CloseActiveTab,
            ),
            Command::new_static(
                "close-other-tabs",
                "Close Other Tabs",
                CommandCategory::Tab,
                CloseOtherTabs,
            ),
            Command::new_static(
                "close-all-tabs",
                "Close All Tabs",
                CommandCategory::Tab,
                CloseAllTabs,
            ),
            // ── Focus ───────────────────────────────────────────────
            Command::new_static(
                "focus-editor",
                "Focus Editor",
                CommandCategory::Focus,
                FocusEditor,
            ),
            Command::new_static(
                "focus-results",
                "Focus Results",
                CommandCategory::Focus,
                FocusResults,
            ),
            Command::new_static(
                "focus-sidebar",
                "Focus Sidebar",
                CommandCategory::Focus,
                FocusSidebar,
            ),
        ]
    }

    /// Load schema commands (tables/views) from the active connection into an
    /// already-stored palette entity.
    fn load_schema_commands_into_palette(&self, cx: &mut Context<Self>) {
        if let Some(palette) = &self.command_palette {
            self.load_schema_commands_into_palette_for(palette, cx);
        }
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
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };

        let schema_service = app_state.schema_service.clone();
        let object_capabilities = app_state
            .connections
            .get(connection_id)
            .map(|connection| SidebarObjectCapabilities::for_connection(connection.as_ref()))
            .unwrap_or_default();
        let connection_name = app_state
            .saved_connections()
            .iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "Unknown".to_string());

        let tables: Vec<String> = schema_service
            .get_cached_tables(connection_id)
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.name)
            .collect();
        let views: Vec<String> = schema_service
            .get_cached_view_names(connection_id)
            .unwrap_or_default();

        palette.update(cx, |palette, cx| {
            palette.add_schema_commands(
                connection_id,
                &connection_name,
                &tables,
                &views,
                object_capabilities,
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
        self.dock_area.update(cx, |area, cx| {
            area.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Left, window, cx);
        });
    }

    pub(super) fn handle_toggle_right_sidebar(
        &mut self,
        _action: &ToggleRightSidebar,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area.update(cx, |area, cx| {
            area.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Right, window, cx);
        });
    }

    pub(super) fn handle_toggle_bottom_panel(
        &mut self,
        _action: &ToggleBottomPanel,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area.update(cx, |area, cx| {
            area.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Bottom, window, cx);
        });
    }

    /// Handle ToggleProblemsPanel action - shows/focuses the Problems panel
    pub(super) fn handle_toggle_problems_panel(
        &mut self,
        _action: &crate::actions::ToggleProblemsPanel,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // First, ensure the bottom dock is open
        self.dock_area.update(cx, |area, cx| {
            area.toggle_dock(zqlz_ui::widgets::dock::DockPlacement::Bottom, window, cx);
        });

        // Then activate the Problems panel
        self.dock_area.update(cx, |area, cx| {
            area.activate_panel(
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
            let is_running = self.workspace_state.read(cx).is_query_running(editor_id);
            if is_running {
                tracing::info!("Cancelling query for editor {:?}", editor_id);

                self.workspace_state.update(cx, |state, cx| {
                    state.cancel_query(editor_id, cx);
                });

                // Update the active query editor to show it's no longer executing
                if let Some(editor) = self.active_query_editor(cx) {
                    editor.update(cx, |editor, cx| {
                        editor.set_executing(false, cx);
                    });
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
                    _ = results_panel.update_in(cx, |panel, window, cx| {
                        panel.set_execution(execution, window, cx);
                    });
                    anyhow::Ok(())
                })
                .detach();

                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::warning("Query cancelled"),
                    cx,
                );
            } else {
                tracing::debug!("No running query to cancel for editor {:?}", editor_id);
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
        if let Some(editor) = self.active_query_editor(cx) {
            let focus_handle = editor.read(cx).editor_focus_handle(cx);
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

        self.show_save_query_dialog(editor.downgrade(), sql, connection_id, window, cx);
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
        self.dock_area
            .update(cx, |dock, cx| dock.activate_next_tab(window, cx));
    }

    pub(super) fn handle_activate_prev_tab(
        &mut self,
        _action: &crate::actions::ActivatePrevTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_prev_tab(window, cx));
    }

    pub(super) fn handle_close_active_tab(
        &mut self,
        _action: &crate::actions::CloseActiveTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("CloseActiveTab action handler triggered!");

        // Get the active panel from the dock area
        let active_panel = self.dock_area.read(cx).active_panel(cx);

        // Check if the active panel has unsaved changes
        let has_unsaved = active_panel
            .as_ref()
            .map(|p| p.has_unsaved_changes(cx))
            .unwrap_or(false);

        if has_unsaved {
            if let Some(panel) = active_panel
                && let Ok(editor) = panel.view().downcast::<QueryEditor>()
            {
                let saved_query_id = editor.read(cx).saved_query_id();
                let connection_id = editor.read(cx).connection_id();
                let sql = editor.read(cx).content(cx).to_string();
                let current_name = editor.read(cx).name();

                if let Some(query_id) = saved_query_id {
                    let dock_area = self.dock_area.clone();
                    let editor_weak = editor.downgrade();

                    window.open_dialog(cx, move |dialog, _window, _cx| {
                        let dock_area = dock_area.clone();
                        let editor_weak = editor_weak.clone();
                        let sql = sql.clone();

                        dialog
                            .title("Unsaved Changes")
                            .w(px(420.0))
                            .child(
                                v_flex()
                                    .gap_3()
                                    .child(body_small("This tab has unsaved changes."))
                                    .child(body_small("Do you want to save before closing?")),
                            )
                            .footer(move |_ok, cancel, _window, _cx| {
                                let save_sql = sql.clone();
                                vec![
                                    cancel(_window, _cx),
                                    Button::new("dont-save")
                                        .label("Don't Save")
                                        .ghost()
                                        .on_click({
                                            let dock_area = dock_area.clone();
                                            move |_, window, cx| {
                                                dock_area.update(cx, |dock_area, cx| {
                                                    dock_area.force_close_active_tab(window, cx);
                                                });
                                                window.close_dialog(cx);
                                            }
                                        })
                                        .into_any_element(),
                                    Button::new("save")
                                        .label("Save")
                                        .primary()
                                        .on_click({
                                            let editor_weak = editor_weak.clone();
                                            let dock_area = dock_area.clone();
                                            let save_sql = save_sql.clone();
                                            move |_, window, cx| {
                                                update_saved_query_for_editor(
                                                    query_id,
                                                    save_sql.clone(),
                                                    editor_weak.clone(),
                                                    window,
                                                    cx,
                                                );
                                                dock_area.update(cx, |dock_area, cx| {
                                                    dock_area.force_close_active_tab(window, cx);
                                                });
                                                window.close_dialog(cx);
                                            }
                                        })
                                        .into_any_element(),
                                ]
                            })
                            .on_cancel(move |_, _, _| true)
                    });
                    return;
                }

                let Some(connection_id) = connection_id else {
                    window.push_notification(
                        Notification::warning(
                            "No connection selected. Please connect to a database first.",
                        ),
                        cx,
                    );
                    return;
                };

                let connection_name = cx
                    .try_global::<AppState>()
                    .and_then(|state| {
                        state
                            .saved_connections()
                            .into_iter()
                            .find(|connection| connection.id == connection_id)
                            .map(|connection| connection.name.clone())
                    })
                    .unwrap_or_else(|| "Unknown".to_string());

                let name_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Enter query name...");
                    state.set_value(current_name, window, cx);
                    state
                });
                let error_message: Entity<Option<String>> = cx.new(|_| None);
                let dock_area = self.dock_area.clone();
                let editor_weak = editor.downgrade();
                let sidebar_weak = self.connection_sidebar.downgrade();
                let focus_name_input = name_input.clone();

                cx.observe(&name_input, {
                    let error_message = error_message.clone();
                    move |_, _, cx| {
                        error_message.update(cx, |msg, cx| {
                            if msg.is_some() {
                                *msg = None;
                                cx.notify();
                            }
                        });
                    }
                })
                .detach();

                window.open_dialog(cx, move |dialog, _window, cx| {
                    let dock_area = dock_area.clone();
                    let editor_weak = editor_weak.clone();
                    let sidebar_weak = sidebar_weak.clone();
                    let connection_name = connection_name.clone();
                    let name_input = name_input.clone();
                    let error_message = error_message.clone();
                    let error_message_for_save = error_message.clone();
                    let sql = sql.clone();

                    dialog
                        .title("Unsaved Changes")
                        .w(px(440.0))
                        .child(
                            v_flex()
                                .gap_3()
                                .child(body_small("This tab has unsaved changes."))
                                .child(body_small("Save it before closing?"))
                                .child(
                                    v_flex()
                                        .gap_1()
                                        .child(body_small("Query Name:"))
                                        .child(Input::new(&name_input)),
                                )
                                .child(
                                    v_flex().gap_1().child(body_small("Save Location:")).child(
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
                                    .label("Don't Save")
                                    .ghost()
                                    .on_click({
                                        let dock_area = dock_area.clone();
                                        move |_, window, cx| {
                                            dock_area.update(cx, |dock_area, cx| {
                                                dock_area.force_close_active_tab(window, cx);
                                            });
                                            window.close_dialog(cx);
                                        }
                                    })
                                    .into_any_element(),
                                Button::new("save")
                                    .label("Save")
                                    .primary()
                                    .on_click({
                                        let dock_area = dock_area.clone();
                                        let editor_weak = editor_weak.clone();
                                        let sidebar_weak = sidebar_weak.clone();
                                        let name_input = name_input.clone();
                                        let error_message_for_save = error_message_for_save.clone();
                                        let save_sql = save_sql.clone();
                                        move |_, window, cx| {
                                            let query_name = name_input
                                                .read(cx)
                                                .text()
                                                .to_string()
                                                .trim()
                                                .to_string();

                                            match save_query_for_editor(
                                                editor_weak.clone(),
                                                save_sql.clone(),
                                                connection_id,
                                                query_name,
                                                sidebar_weak.clone(),
                                                window,
                                                cx,
                                            ) {
                                                Ok(_) => {
                                                    dock_area.update(cx, |dock_area, cx| {
                                                        dock_area
                                                            .force_close_active_tab(window, cx);
                                                    });
                                                    window.close_dialog(cx);
                                                }
                                                Err(error) => {
                                                    error_message_for_save.update(cx, |msg, cx| {
                                                        *msg = Some(error);
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
                return;
            }

            let dock_area = self.dock_area.clone();

            window.open_dialog(cx, move |dialog, _window, _cx| {
                let dock_area = dock_area.clone();

                dialog
                    .title("Unsaved Changes")
                    .child("This tab has unsaved changes. Do you want to close it anyway?")
                    .confirm()
                    .button_props(
                        DialogButtonProps::default()
                            .ok_text("Don't Save")
                            .cancel_text("Cancel"),
                    )
                    .on_ok(move |_, window, cx| {
                        dock_area.update(cx, |dock_area, cx| {
                            dock_area.force_close_active_tab(window, cx)
                        });
                        true
                    })
            });
        } else {
            // No unsaved changes, close directly
            self.dock_area
                .update(cx, |dock_area, cx| dock_area.close_active_tab(window, cx));
        }
    }

    pub(super) fn handle_close_other_tabs(
        &mut self,
        _action: &crate::actions::CloseOtherTabs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.close_other_tabs(window, cx));
    }

    pub(super) fn handle_close_tabs_to_right(
        &mut self,
        _action: &crate::actions::CloseTabsToRight,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.close_tabs_to_right(window, cx));
    }

    pub(super) fn handle_close_all_tabs(
        &mut self,
        _action: &crate::actions::CloseAllTabs,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.close_all_tabs(window, cx));
    }

    pub(super) fn handle_activate_tab_1(
        &mut self,
        _action: &crate::actions::ActivateTab1,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(1, window, cx));
    }
    pub(super) fn handle_activate_tab_2(
        &mut self,
        _action: &crate::actions::ActivateTab2,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(2, window, cx));
    }
    pub(super) fn handle_activate_tab_3(
        &mut self,
        _action: &crate::actions::ActivateTab3,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(3, window, cx));
    }
    pub(super) fn handle_activate_tab_4(
        &mut self,
        _action: &crate::actions::ActivateTab4,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(4, window, cx));
    }
    pub(super) fn handle_activate_tab_5(
        &mut self,
        _action: &crate::actions::ActivateTab5,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(5, window, cx));
    }
    pub(super) fn handle_activate_tab_6(
        &mut self,
        _action: &crate::actions::ActivateTab6,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(6, window, cx));
    }
    pub(super) fn handle_activate_tab_7(
        &mut self,
        _action: &crate::actions::ActivateTab7,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(7, window, cx));
    }
    pub(super) fn handle_activate_tab_8(
        &mut self,
        _action: &crate::actions::ActivateTab8,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(8, window, cx));
    }
    pub(super) fn handle_activate_tab_9(
        &mut self,
        _action: &crate::actions::ActivateTab9,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area
            .update(cx, |dock, cx| dock.activate_tab_by_number(9, window, cx));
    }

    pub(super) fn handle_activate_last_tab(
        &mut self,
        _action: &crate::actions::ActivateLastTab,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let count = self.dock_area.read(cx).tab_count(cx);
        if count > 0 {
            self.dock_area.update(cx, |dock, cx| {
                dock.activate_tab_by_number(count, window, cx)
            });
        }
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

        // Get the active panel from the dock area
        let active_panel = self.dock_area.read(cx).active_panel(cx);

        if let Some(panel) = active_panel {
            let panel_name = panel.panel_name(cx);
            tracing::info!("Active panel for refresh: {}", panel_name);

            match panel_name {
                "TableViewer" => {
                    // Downcast to TableViewerPanel and call refresh
                    if let Ok(viewer) = panel.view().downcast::<TableViewerPanel>() {
                        viewer.update(cx, |viewer, cx| {
                            viewer.refresh(cx);
                        });
                    }
                }
                "QueryEditor" => {
                    // For QueryEditor, execute the query to refresh results
                    tracing::info!("Refreshing QueryEditor - executing query");
                    self.handle_execute_query(&crate::actions::ExecuteQuery, window, cx);
                }
                "ConnectionSidebar" => {
                    self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
                }
                "ObjectsPanel" => {
                    self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
                }
                _ => {
                    tracing::debug!("Refresh not implemented for panel: {}", panel_name);
                }
            }
        } else {
            // No active panel - try connection sidebar if it has focus
            tracing::debug!("No active panel, checking connection sidebar");
            self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
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
                move |_this, _, event: &SettingsPanelEvent, _cx| {
                    match event {
                        SettingsPanelEvent::SettingsChanged => {
                            tracing::debug!("Settings changed");
                            // TODO: When we implement custom text editor, sync settings here
                        }
                    }
                },
            )
            .detach();
        }

        // Get the settings panel entity
        let settings_panel_entity = self
            .settings_panel
            .clone()
            .expect("settings_panel should be set");

        // Wrap in Arc for dock area
        let settings_panel: std::sync::Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            std::sync::Arc::new(settings_panel_entity.clone());

        self.dock_area.update(cx, |dock_area, cx| {
            dock_area.add_panel(
                settings_panel,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });
    }

    /// Handle objects panel events
    pub(super) fn handle_objects_panel_event(
        &mut self,
        event: &ObjectsPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            ObjectsPanelEvent::OpenTables {
                connection_id,
                table_names,
                database_name,
            } => {
                self.open_tables(
                    *connection_id,
                    table_names.clone(),
                    database_name.clone(),
                    false,
                    window,
                    cx,
                );
            }
            ObjectsPanelEvent::DesignTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.design_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::NewTable {
                connection_id,
                database_name: _,
            } => {
                self.new_table(*connection_id, window, cx);
            }
            ObjectsPanelEvent::DeleteTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.delete_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::EmptyTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.empty_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::DuplicateTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.duplicate_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::RenameTable {
                connection_id,
                table_name,
                database_name: _,
            } => {
                self.rename_table(*connection_id, table_name.clone(), window, cx);
            }
            ObjectsPanelEvent::ImportData {
                connection_id,
                table_name,
                database_name: _,
            } => {
                self.import_data(*connection_id, table_name.clone(), window, cx);
            }
            ObjectsPanelEvent::ExportTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.export_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::DumpTablesSql {
                connection_id,
                table_names,
                include_data,
                database_name: _,
            } => {
                self.dump_tables_sql(
                    *connection_id,
                    table_names.clone(),
                    *include_data,
                    window,
                    cx,
                );
            }
            ObjectsPanelEvent::CopyTableNames { table_names } => {
                self.copy_table_names(table_names, cx);
            }
            ObjectsPanelEvent::Refresh => {
                self.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
            }
            // Redis-related events
            ObjectsPanelEvent::OpenRedisDatabase {
                connection_id,
                database_index,
            } => {
                self.open_redis_database(*connection_id, *database_index, window, cx);
            }
            ObjectsPanelEvent::DeleteKeys {
                connection_id,
                key_names,
            } => {
                self.delete_keys(*connection_id, key_names.clone(), window, cx);
            }
            ObjectsPanelEvent::CopyKeyNames { key_names } => {
                self.copy_key_names(key_names, cx);
            }
            // View-related events
            ObjectsPanelEvent::OpenViews {
                connection_id,
                view_names,
                database_name,
            } => {
                self.open_tables(
                    *connection_id,
                    view_names.clone(),
                    database_name.clone(),
                    true,
                    window,
                    cx,
                );
            }
            ObjectsPanelEvent::DesignViews {
                connection_id,
                view_names,
                database_name: _,
            } => {
                self.design_views(*connection_id, view_names.clone(), window, cx);
            }
            ObjectsPanelEvent::NewView {
                connection_id,
                database_name: _,
            } => {
                self.new_view(*connection_id, window, cx);
            }
            ObjectsPanelEvent::DeleteViews {
                connection_id,
                view_names,
                database_name: _,
            } => {
                self.delete_views(*connection_id, view_names.clone(), window, cx);
            }
            ObjectsPanelEvent::DuplicateViews {
                connection_id,
                view_names,
                database_name: _,
            } => {
                self.duplicate_views(*connection_id, view_names.clone(), window, cx);
            }
            ObjectsPanelEvent::CopyViewNames { view_names } => {
                self.copy_view_names(view_names, cx);
            }
            ObjectsPanelEvent::RenameView {
                connection_id,
                view_name,
                database_name: _,
            } => {
                self.rename_view(*connection_id, view_name.clone(), window, cx);
            }
            ObjectsPanelEvent::ViewHistory {
                connection_id,
                object_name,
                object_schema,
                object_type,
            } => {
                let db_object_type = match object_type.as_str() {
                    "table" => DatabaseObjectType::Table,
                    "view" => DatabaseObjectType::View,
                    "function" => DatabaseObjectType::Function,
                    "procedure" => DatabaseObjectType::Procedure,
                    "trigger" => DatabaseObjectType::Trigger,
                    _ => {
                        tracing::warn!("Unknown object type for version history: {}", object_type);
                        return;
                    }
                };
                self.show_version_history(
                    *connection_id,
                    object_name.clone(),
                    object_schema.clone(),
                    db_object_type,
                    window,
                    cx,
                );
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
                self.close_table_designer_panel(panel, window, cx);
            }
            zqlz_table_designer::TableDesignerEvent::PreviewDdl { design: _ } => {
                // DDL preview is handled internally by the panel
            }
        }
    }

    /// Close a table designer panel
    fn close_table_designer_panel(
        &mut self,
        panel: Entity<zqlz_table_designer::TableDesignerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dock_area.update(cx, |area, cx| {
            area.remove_panel(
                std::sync::Arc::new(panel),
                zqlz_ui::widgets::dock::DockPlacement::Center,
                window,
                cx,
            );
        });
        tracing::info!("Table designer closed");
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
            TemplateLibraryEvent::UseTemplate {
                template_sql,
                default_params: _,
                template_type: _,
            } => {
                tracing::info!("Using template from library");

                // Try to find an existing query editor that we can insert into
                let existing_editor = self.active_query_editor(cx);

                if let Some(editor) = existing_editor {
                    // Insert template into the existing editor
                    editor.update(cx, |editor, cx| {
                        editor.set_content(template_sql.clone(), window, cx);
                    });
                } else {
                    // Create a new query editor first
                    let editor = self.create_new_query_editor(window, cx);
                    editor.update(cx, |editor, cx| {
                        editor.set_content(template_sql.clone(), window, cx);
                    });
                }
            }
            TemplateLibraryEvent::EditTemplate(_id) => {
                // Template editing is handled within the panel
            }
            TemplateLibraryEvent::TemplateDeleted(_id) => {
                tracing::info!("Template deleted");
            }
            TemplateLibraryEvent::TemplateSaved(_id) => {
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
                // TODO: Could update a global project context or notify other panels
            }
            ProjectManagerEvent::OpenModel {
                project_id,
                model_id,
            } => {
                tracing::info!("Opening model {} from project {}", model_id, project_id);
                // TODO: Load model SQL and open in query editor
                // For now, create a new query editor
                let editor = self.create_new_query_editor(window, cx);

                // Load the model from storage and populate the editor
                if let Some(app_state) = cx.try_global::<AppState>()
                    && let Ok(Some(model)) = app_state.storage.load_model(*model_id)
                {
                    editor.update(cx, |editor, cx| {
                        editor.set_content(model.sql.clone(), window, cx);
                    });
                }
            }
            ProjectManagerEvent::CreateModel(project_id) => {
                tracing::info!("Creating new model in project {}", project_id);
                // Create a new query editor for the model
                let _ = self.create_new_query_editor(window, cx);
            }
            ProjectManagerEvent::CompileModel {
                project_id,
                model_id,
            } => {
                tracing::info!("Compiling model {} from project {}", model_id, project_id);
                // TODO: Use DbtTemplateEngine to compile the model and show results
            }
            ProjectManagerEvent::ProjectsChanged => {
                tracing::info!("Projects list changed");
                // TODO: Could refresh other panels that depend on project list
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
                // Navigate to the specified line/column in the active query editor
                // Note: line and column are 1-indexed for display
                tracing::debug!("GoToLine event: line={}, column={}", line, column);

                if let Some(editor) = self.active_query_editor(cx) {
                    editor.update(cx, |editor, cx| {
                        editor.go_to_line(
                            line.saturating_sub(1),
                            column.saturating_sub(1),
                            window,
                            cx,
                        );
                    });
                    return;
                }

                tracing::debug!("No active query editor found to navigate to line");
            }
            ResultsPanelEvent::ReloadDiagnostics => {
                tracing::debug!("ReloadDiagnostics event received");

                self.results_panel.update(cx, |panel, cx| {
                    panel.set_diagnostics_loading(true, cx);
                });

                // Re-run diagnostics on the active editor.
                let editor_found = if let Some(editor) = self.active_query_editor(cx) {
                    editor.update(cx, |editor, cx| {
                        editor.reload_diagnostics(cx);
                    });
                    true
                } else {
                    false
                };

                if !editor_found {
                    tracing::debug!("ReloadDiagnostics: no active editor found");
                    // No work was done so clear the loading state immediately.
                    self.results_panel.update(cx, |panel, cx| {
                        panel.set_diagnostics_loading(false, cx);
                    });
                }
                // When an editor was found, set_problems (called via the
                // DiagnosticsChanged subscription) clears diagnostics_loading.
            }
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
            crate::components::InspectorPanelEvent::ViewChanged(_view) => {
                // View changed - nothing to do here
            }
            crate::components::InspectorPanelEvent::OpenQuery { sql } => {
                tracing::info!("Opening query from history");

                let active_editor = self.active_query_editor(cx);

                // When no editor exists at all, open a new tab immediately without a dialog.
                let Some(editor) = active_editor else {
                    tracing::info!("No active query editor found, creating new one");
                    let editor = self.create_new_query_editor(window, cx);
                    editor.update(cx, |editor, cx| {
                        editor.set_text(&sql, window, cx);
                    });
                    let focus_handle = editor.read(cx).focus_handle(cx);
                    window.focus(&focus_handle, cx);
                    return;
                };

                // Always ask the user whether to open the history entry in the current tab
                // or in a new tab, so that the choice is always explicit.
                let this_weak = cx.weak_entity();
                let editor = editor.clone();
                let sql = sql.clone();

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
                        // "Current Tab" is the primary (ok) action.
                        .button_props(
                            DialogButtonProps::default()
                                .ok_text("Current Tab")
                                .cancel_text("Cancel"),
                        )
                        .on_ok(move |_, window, cx| {
                            editor_for_current.update(cx, |editor, cx| {
                                editor.set_text(&sql_for_current, window, cx);
                            });
                            let focus_handle = editor_for_current.read(cx).focus_handle(cx);
                            window.focus(&focus_handle, cx);
                            true
                        })
                        // Custom footer to insert a "New Tab" button between Cancel and Current Tab.
                        .footer(move |ok, cancel, window, cx| {
                            let this_weak = this_weak_for_new.clone();
                            let sql = sql_for_new.clone();

                            let new_tab_button = Button::new("new-tab")
                                .secondary()
                                .label("New Tab")
                                .on_click(move |_, window, cx| {
                                    window.close_dialog(cx);
                                    _ = this_weak.update(cx, |this, cx| {
                                        let editor = this.create_new_query_editor(window, cx);
                                        editor.update(cx, |editor, cx| {
                                            editor.set_text(&sql, window, cx);
                                        });
                                        let focus_handle = editor.read(cx).focus_handle(cx);
                                        window.focus(&focus_handle, cx);
                                    });
                                })
                                .into_any_element();

                            vec![cancel(window, cx), new_tab_button, ok(window, cx)]
                        })
                });
            }
            crate::components::InspectorPanelEvent::ClearHistory => {
                // Clear query history in AppState
                if let Some(app_state) = cx.try_global::<AppState>() {
                    app_state.clear_query_history();
                    tracing::info!("Query history cleared");
                }

                // Update the history panel to reflect the cleared state
                self.inspector_panel.update(cx, |panel, cx| {
                    panel.query_history_panel().update(cx, |history_panel, cx| {
                        history_panel.update_entries(Vec::new(), cx);
                    });
                });

                self.refresh_query_history(cx);
            }
        }
    }
}
