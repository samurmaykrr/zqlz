// Event handlers for MainView

use gpui::*;

use crate::actions::*;
use crate::app::AppState;
use crate::components::{
    CommandPalette, CommandPaletteEvent, ConnectionEntry, ConnectionSidebarEvent,
    ObjectsPanelEvent, ProjectManagerEvent, ResultsPanelEvent, SettingsPanel, SettingsPanelEvent, TableViewerPanel,
    TemplateLibraryEvent,
};
use zqlz_ui::widgets::{WindowExt, dialog::DialogButtonProps, dock::Panel};
use zqlz_versioning::DatabaseObjectType;
use zqlz_zed_adapter::SettingsBridge;

use super::MainView;

impl MainView {
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
                tracing::debug!("Refreshing connections list (preserving active connections)");
                if let Some(app_state) = cx.try_global::<AppState>() {
                    let saved = app_state.saved_connections();

                    // Get current connection entries to preserve their state
                    let current_entries: std::collections::HashMap<uuid::Uuid, ConnectionEntry> =
                        self.connection_sidebar
                            .read(cx)
                            .connections()
                            .iter()
                            .map(|c| (c.id, c.clone()))
                            .collect();

                    let entries: Vec<_> = saved
                        .into_iter()
                        .map(|s| {
                            // If this connection already exists, preserve its full state
                            // (connected status, schema data, expansion states, etc.)
                            if let Some(existing) = current_entries.get(&s.id) {
                                let mut entry = existing.clone();
                                // Update name/driver in case they changed in saved config
                                entry.name = s.name;
                                entry.db_type = s.driver;
                                entry
                            } else {
                                // New connection, create fresh entry
                                ConnectionEntry::new(s.id, s.name, s.driver)
                            }
                        })
                        .collect();
                    self.connection_sidebar.update(cx, |sidebar, cx| {
                        sidebar.set_connections(entries, cx);
                    });
                }
            }
            ConnectionSidebarEvent::OpenTable {
                connection_id,
                table_name,
                database_name,
            } => {
                self.open_table_viewer(connection_id, table_name, database_name.clone(), false, window, cx);
            }
            ConnectionSidebarEvent::OpenView {
                connection_id,
                view_name,
                database_name,
            } => {
                // Views can be queried like tables, so we reuse the table viewer
                self.open_table_viewer(connection_id, view_name, database_name.clone(), true, window, cx);
            }
            ConnectionSidebarEvent::DesignView {
                connection_id,
                view_name,
            } => {
                self.design_view(connection_id, view_name, window, cx);
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
                self.export_data(connection_id, table_name, window, cx);
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
                self.refresh_schema(connection_id, window, cx);
            }

            // Saved queries events
            ConnectionSidebarEvent::OpenSavedQuery {
                connection_id,
                query_id,
                query_name,
            } => {
                tracing::info!(query_id = %query_id, query_name = %query_name, connection_id = %connection_id, "Opening saved query from sidebar");
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
                object_type,
            } => {
                let db_object_type = match object_type.as_str() {
                    "view" => DatabaseObjectType::View,
                    "function" => DatabaseObjectType::Function,
                    "procedure" => DatabaseObjectType::Procedure,
                    "trigger" => DatabaseObjectType::Trigger,
                    _ => {
                        tracing::warn!("Unknown object type for version history: {}", object_type);
                        return;
                    }
                };
                self.show_version_history(connection_id, object_name, db_object_type, window, cx);
            }

            // Function events
            ConnectionSidebarEvent::OpenFunction {
                connection_id,
                function_name,
            } => {
                // Open a query editor with the function definition
                self.open_function_definition(connection_id, function_name, window, cx);
            }

            // Procedure events
            ConnectionSidebarEvent::OpenProcedure {
                connection_id,
                procedure_name,
            } => {
                // Open a query editor with the procedure definition
                self.open_procedure_definition(connection_id, procedure_name, window, cx);
            }

            // Trigger events
            ConnectionSidebarEvent::DesignTrigger {
                connection_id,
                trigger_name,
            } => {
                self.design_trigger(connection_id, trigger_name, window, cx);
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
            } => {
                self.open_trigger_designer(connection_id, trigger_name, window, cx);
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
                    "Load schema for database '{}' on connection {}",
                    database_name,
                    connection_id
                );
                self.load_database_schema(connection_id, database_name, window, cx);
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
                            _ = menu_state.update(cx, |state, cx| {
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

        // Create the command palette if not already open
        if self.command_palette.is_none() {
            let palette = cx.new(|cx| CommandPalette::new(window, cx));

            // Load schema data for the active connection if available
            let schema_data = {
                let workspace_state = self.workspace_state.read(cx);
                workspace_state.active_connection_id().and_then(|connection_id| {
                    workspace_state
                        .schema_for_connection(connection_id)
                        .map(|schema| (connection_id, schema.clone()))
                })
            };

            if let Some((connection_id, schema)) = schema_data {
                // Get connection name from AppState
                let connection_name = if let Some(app_state) = cx.try_global::<AppState>() {
                    app_state
                        .saved_connections()
                        .iter()
                        .find(|c| c.id == connection_id)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| "Unknown".to_string())
                } else {
                    "Unknown".to_string()
                };

                palette.update(cx, |palette, _cx| {
                    palette.add_schema_commands(connection_id, &connection_name, &schema);
                });
            }

            // Subscribe to dismiss events
            let subscription = cx.subscribe(
                &palette,
                |this, _palette, event: &CommandPaletteEvent, cx| {
                    match event {
                        CommandPaletteEvent::Dismissed => {
                            this.command_palette = None;
                            cx.notify();
                        }
                        CommandPaletteEvent::CommandExecuted(cmd_id) => {
                            tracing::debug!("Command executed: {}", cmd_id);
                        }
                        CommandPaletteEvent::ConnectToConnection(_) => {
                            tracing::debug!("ConnectToConnection event received");
                        }
                        CommandPaletteEvent::OpenTable { .. } => {
                            tracing::debug!("OpenTable event received");
                        }
                        CommandPaletteEvent::OpenView { .. } => {
                            tracing::debug!("OpenView event received");
                        }
                    }
                },
            );
            self._subscriptions.push(subscription);

            // Focus the input field
            let input_state = palette.read(cx).input_state().clone();
            input_state.read(cx).focus_handle(cx).focus(window, cx);
            
            self.command_palette = Some(palette);
            cx.notify();
        }
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
        tracing::info!("RefreshConnectionsList action received - refreshing sidebar");
        if let Some(app_state) = cx.try_global::<AppState>() {
            let saved = app_state.saved_connections();
            tracing::info!("Found {} saved connections", saved.len());
            let entries: Vec<_> = saved
                .into_iter()
                .map(|s| ConnectionEntry::new(s.id, s.name, s.driver))
                .collect();
            self.connection_sidebar.update(cx, |sidebar, cx| {
                sidebar.set_connections(entries, cx);
            });
        }
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
            area.activate_panel("Problems", zqlz_ui::widgets::dock::DockPlacement::Bottom, window, cx);
        });
    }

    /// Handle ExecuteQuery action - executes the entire query
    pub(super) fn handle_execute_query(
        &mut self,
        _action: &crate::actions::ExecuteQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExecuteQuery action triggered");
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.execute_query(window, cx);
        });
    }

    /// Handle ExecuteSelection action - executes selected text or entire query
    pub(super) fn handle_execute_selection(
        &mut self,
        _action: &crate::actions::ExecuteSelection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ExecuteSelection action triggered");
        // Emit ExecuteSelection event from the active query editor
        // This will be handled by the query tabs panel
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.execute_selection(window, cx);
        });
    }

    /// Handle StopQuery action - stops the currently executing query
    pub(super) fn handle_stop_query(
        &mut self,
        _action: &crate::actions::StopQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("StopQuery action triggered");

        // Get the active editor index from the query tabs panel
        let active_index = self.query_tabs_panel.read(cx).active_editor_index();

        if let Some(editor_index) = active_index {
            // Check if there's a running query for this editor
            if let Some(task) = self.running_query_tasks.remove(&editor_index) {
                tracing::info!("Cancelling query for editor {}", editor_index);

                // First, call the cancel handle to interrupt the actual database query
                // This sends an interrupt signal to the database (SQLite) or cancel request (PostgreSQL)
                if let Some(cancel_handle) = self.query_cancel_handles.remove(&editor_index) {
                    tracing::debug!("Calling cancel handle for editor {}", editor_index);
                    cancel_handle.cancel();
                }

                // Then drop the task to stop waiting for the result
                drop(task);

                // Update the editor to show it's no longer executing
                self.query_tabs_panel.update(cx, |panel, cx| {
                    panel.set_editor_executing(editor_index, false, cx);
                });

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

                // Need to spawn to get window context for set_execution
                let results_panel = results_panel.downgrade();
                cx.spawn_in(window, async move |_this, cx| {
                    _ = results_panel.update_in(cx, |panel, window, cx| {
                        panel.set_execution(execution, window, cx);
                    });
                    anyhow::Ok(())
                })
                .detach();

                // Show notification
                window.push_notification(
                    zqlz_ui::widgets::notification::Notification::warning("Query cancelled"),
                    cx,
                );
            } else {
                tracing::debug!("No running query to cancel for editor {}", editor_index);
            }
        } else {
            tracing::debug!("No active editor to stop query for");
        }

        cx.notify();
    }

    // ====================
    // Tab Navigation Actions
    // ====================

    pub(super) fn handle_activate_next_tab(
        &mut self,
        _action: &crate::actions::ActivateNextTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.activate_next_tab(cx);
        });
    }

    pub(super) fn handle_activate_prev_tab(
        &mut self,
        _action: &crate::actions::ActivatePrevTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.activate_prev_tab(cx);
        });
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
            // Show confirmation dialog
            let dock_area = self.dock_area.clone();
            let query_tabs_panel = self.query_tabs_panel.clone();

            window.open_dialog(cx, move |dialog, _window, _cx| {
                let dock_area = dock_area.clone();
                let query_tabs_panel = query_tabs_panel.clone();

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
                        // User chose to close without saving
                        let closed = dock_area
                            .update(cx, |dock_area, cx| dock_area.close_active_tab(window, cx));
                        if !closed {
                            query_tabs_panel.update(cx, |panel, cx| {
                                panel.close_active_tab(cx);
                            });
                        }
                        true // Close the dialog
                    })
            });
        } else {
            // No unsaved changes, close directly
            let closed_in_dock = self
                .dock_area
                .update(cx, |dock_area, cx| dock_area.close_active_tab(window, cx));

            if closed_in_dock {
                tracing::debug!("Closed tab in dock_area");
            } else {
                // Fallback: try the query tabs panel
                tracing::debug!("No tab closed in dock_area, trying query_tabs_panel");
                self.query_tabs_panel.update(cx, |panel, cx| {
                    panel.close_active_tab(cx);
                });
            }
        }
    }

    pub(super) fn handle_close_other_tabs(
        &mut self,
        _action: &crate::actions::CloseOtherTabs,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.close_other_tabs(cx);
        });
    }

    pub(super) fn handle_close_tabs_to_right(
        &mut self,
        _action: &crate::actions::CloseTabsToRight,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel.update(cx, |panel, cx| {
            panel.close_tabs_to_right(cx);
        });
    }

    pub(super) fn handle_close_all_tabs(
        &mut self,
        _action: &crate::actions::CloseAllTabs,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("CloseAllTabs action handler triggered!");
        self.query_tabs_panel.update(cx, |panel, cx| {
            tracing::debug!("Calling query_tabs_panel.close_all_tabs()");
            panel.close_all_tabs(cx);
        });
    }

    pub(super) fn handle_activate_tab_1(
        &mut self,
        _action: &crate::actions::ActivateTab1,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(1, cx));
    }
    pub(super) fn handle_activate_tab_2(
        &mut self,
        _action: &crate::actions::ActivateTab2,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(2, cx));
    }
    pub(super) fn handle_activate_tab_3(
        &mut self,
        _action: &crate::actions::ActivateTab3,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(3, cx));
    }
    pub(super) fn handle_activate_tab_4(
        &mut self,
        _action: &crate::actions::ActivateTab4,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(4, cx));
    }
    pub(super) fn handle_activate_tab_5(
        &mut self,
        _action: &crate::actions::ActivateTab5,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(5, cx));
    }
    pub(super) fn handle_activate_tab_6(
        &mut self,
        _action: &crate::actions::ActivateTab6,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(6, cx));
    }
    pub(super) fn handle_activate_tab_7(
        &mut self,
        _action: &crate::actions::ActivateTab7,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(7, cx));
    }
    pub(super) fn handle_activate_tab_8(
        &mut self,
        _action: &crate::actions::ActivateTab8,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(8, cx));
    }
    pub(super) fn handle_activate_tab_9(
        &mut self,
        _action: &crate::actions::ActivateTab9,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel
            .update(cx, |panel, cx| panel.activate_tab_by_number(9, cx));
    }

    pub(super) fn handle_activate_last_tab(
        &mut self,
        _action: &crate::actions::ActivateLastTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.query_tabs_panel.update(cx, |panel, cx| {
            let count = panel.tab_count();
            if count > 0 {
                panel.activate_tab_by_number(count, cx);
            }
        });
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
                    // Refresh connections sidebar
                    self.connection_sidebar.update(cx, |sidebar, cx| {
                        sidebar.refresh(cx);
                    });
                }
                "ObjectsPanel" => {
                    // Refresh objects panel
                    self.objects_panel.update(cx, |panel, cx| {
                        panel.refresh(cx);
                    });
                }
                _ => {
                    tracing::debug!("Refresh not implemented for panel: {}", panel_name);
                }
            }
        } else {
            // No active panel - try connection sidebar if it has focus
            tracing::debug!("No active panel, checking connection sidebar");
            self.connection_sidebar.update(cx, |sidebar, cx| {
                sidebar.refresh(cx);
            });
        }
    }

    pub(super) fn open_settings_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Create the settings panel if it doesn't exist
        if self.settings_panel.is_none() {
            let settings_panel = cx.new(|cx| SettingsPanel::new(window, cx));
            self.settings_panel = Some(settings_panel.clone());
            
            // Subscribe to settings changes to sync with Zed editor
            let settings_panel_for_sub = settings_panel.clone();
            cx.subscribe(&settings_panel, move |_this, _, event: &SettingsPanelEvent, cx| {
                match event {
                    SettingsPanelEvent::SettingsChanged => {
                        tracing::debug!("Settings changed, syncing to Zed editor");
                        SettingsBridge::sync_settings(cx);
                    }
                }
            }).detach();
        }

        // Get the settings panel entity
        let settings_panel_entity = self.settings_panel.clone().expect("settings_panel should be set");
        
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
                self.open_tables(*connection_id, table_names.clone(), database_name.clone(), false, window, cx);
            }
            ObjectsPanelEvent::DesignTables {
                connection_id,
                table_names,
                database_name: _,
            } => {
                self.design_tables(*connection_id, table_names.clone(), window, cx);
            }
            ObjectsPanelEvent::NewTable { connection_id, database_name: _ } => {
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
                self.dump_tables_sql(*connection_id, table_names.clone(), *include_data, window, cx);
            }
            ObjectsPanelEvent::CopyTableNames { table_names } => {
                self.copy_table_names(table_names, cx);
            }
            ObjectsPanelEvent::Refresh => {
                self.refresh_objects_panel(window, cx);
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
                self.open_tables(*connection_id, view_names.clone(), database_name.clone(), true, window, cx);
            }
            ObjectsPanelEvent::DesignViews {
                connection_id,
                view_names,
                database_name: _,
            } => {
                self.design_views(*connection_id, view_names.clone(), window, cx);
            }
            ObjectsPanelEvent::NewView { connection_id, database_name: _ } => {
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
                object_type,
            } => {
                let db_object_type = match object_type.as_str() {
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
                self.save_table_design(connection_id, design, is_new, original_design, panel, window, cx);
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
                let existing_editor = self
                    .query_editors
                    .iter()
                    .rev()
                    .find_map(|weak| weak.upgrade());

                if let Some(editor) = existing_editor {
                    // Insert template into the existing editor
                    editor.update(cx, |editor, cx| {
                        editor.set_content(template_sql.clone(), window, cx);
                    });
                } else {
                    // Create a new query editor first
                    self.handle_new_query(&NewQuery, window, cx);

                    // Then set its content to the template
                    if let Some(editor_weak) = self.query_editors.last() {
                        if let Some(editor) = editor_weak.upgrade() {
                            editor.update(cx, |editor, cx| {
                                editor.set_content(template_sql.clone(), window, cx);
                            });
                        }
                    }
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
                self.handle_new_query(&NewQuery, window, cx);

                // Load the model from storage and populate the editor
                if let Some(app_state) = cx.try_global::<AppState>() {
                    if let Ok(Some(model)) = app_state.storage.load_model(*model_id) {
                        if let Some(editor_weak) = self.query_editors.last() {
                            if let Some(editor) = editor_weak.upgrade() {
                                editor.update(cx, |editor, cx| {
                                    editor.set_content(model.sql.clone(), window, cx);
                                });
                            }
                        }
                    }
                }
            }
            ProjectManagerEvent::CreateModel(project_id) => {
                tracing::info!("Creating new model in project {}", project_id);
                // Create a new query editor for the model
                self.handle_new_query(&NewQuery, window, cx);
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

                // Try to find the most recently used query editor
                for editor_weak in self.query_editors.iter().rev() {
                    if let Some(editor) = editor_weak.upgrade() {
                        editor.update(cx, |editor, cx| {
                            // Convert from 1-indexed (display) to 0-indexed (internal)
                            editor.go_to_line(
                                line.saturating_sub(1),
                                column.saturating_sub(1),
                                window,
                                cx,
                            );
                        });
                        return;
                    }
                }

                tracing::debug!("No active query editor found to navigate to line");
            }
            ResultsPanelEvent::ReloadDiagnostics => {
                tracing::debug!("ReloadDiagnostics event received");

                // Set loading state
                self.results_panel.update(cx, |panel, cx| {
                    panel.set_diagnostics_loading(true, cx);
                });

                // TODO: Implement proper diagnostics reloading
                // This requires tracking EditorId -> QueryEditor mapping
                tracing::debug!("Diagnostics reload requested (not yet fully implemented)");
                
                // Clear loading state
                self.results_panel.update(cx, |panel, cx| {
                    panel.set_diagnostics_loading(false, cx);
                });
            }
        }
    }

    /// Convert UI Diagnostic to EditorDiagnostic for WorkspaceState
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
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Info => {
                    DiagnosticSeverity::Info
                }
                zqlz_ui::widgets::highlighter::DiagnosticSeverity::Hint => {
                    DiagnosticSeverity::Hint
                }
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
                // Load query from history into the active query editor
                tracing::info!("Opening query from history");
                
                // Try to find an active query editor
                // First, try to find the most recently focused query editor
                let active_editor = self.query_editors
                    .iter()
                    .rev() // Start from most recent
                    .find_map(|weak| weak.upgrade());
                
                if let Some(editor) = active_editor {
                    // Check if editor has unsaved changes
                    let has_unsaved = editor.read(cx).has_unsaved_changes(cx);
                    
                    if has_unsaved {
                        // Show confirmation dialog
                        let editor = editor.clone();
                        let sql = sql.clone();
                        
                        window.open_dialog(cx, move |dialog, _window, _cx| {
                            let editor = editor.clone();
                            let sql = sql.clone();
                            
                            dialog
                                .title("Unsaved Changes")
                                .child("The active query has unsaved changes. Loading this query will discard those changes. Continue?")
                                .confirm()
                                .button_props(
                                    zqlz_ui::widgets::dialog::DialogButtonProps::default()
                                        .ok_text("Load Query")
                                        .cancel_text("Cancel"),
                                )
                                .on_ok(move |_, window, cx| {
                                    // User confirmed - load the query
                                    editor.update(cx, |editor, cx| {
                                        editor.set_text(&sql, window, cx);
                                    });
                                    // Focus the editor
                                    let focus_handle = editor.read(cx).focus_handle(cx);
                                    window.focus(&focus_handle, cx);
                                    true // Close the dialog
                                })
                        });
                    } else {
                        // No unsaved changes - load directly
                        editor.update(cx, |editor, cx| {
                            editor.set_text(&sql, window, cx);
                        });
                        // Focus the editor
                        let focus_handle = editor.read(cx).focus_handle(cx);
                        window.focus(&focus_handle, cx);
                    }
                } else {
                    // No query editor exists - create a new one
                    tracing::info!("No active query editor found, creating new one");
                    self.handle_new_query(&NewQuery, window, cx);
                    
                    // Get the newly created query editor and set its content
                    if let Some(editor_weak) = self.query_editors.last() {
                        if let Some(editor) = editor_weak.upgrade() {
                            editor.update(cx, |editor, cx| {
                                editor.set_text(&sql, window, cx);
                            });
                            // Focus the editor
                            let focus_handle = editor.read(cx).focus_handle(cx);
                            window.focus(&focus_handle, cx);
                        }
                    }
                }
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
            }
        }
    }
}
