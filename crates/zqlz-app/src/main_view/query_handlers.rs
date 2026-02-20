use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppState;
use crate::components::{
    ExplainResult, QueryEditor, QueryEditorEvent, QueryExecution, QueryTabsPanel,
    QueryTabsPanelEvent, ResultsPanel, StatementResult,
};
use crate::workspace_state::{DiagnosticSeverity, EditorDiagnostic, EditorId};
use zqlz_query::{DiagnosticInfo, DiagnosticInfoSeverity, EditorObjectType, QueryEngine};
use zqlz_versioning::DatabaseObjectType;

use super::MainView;

/// Convert DiagnosticInfo from query editor to EditorDiagnostic for WorkspaceState
fn convert_diagnostic(info: &DiagnosticInfo) -> EditorDiagnostic {
    EditorDiagnostic {
        line: info.line,
        column: info.column,
        end_line: info.end_line,
        end_column: info.end_column,
        message: info.message.clone(),
        severity: match info.severity {
            DiagnosticInfoSeverity::Error => DiagnosticSeverity::Error,
            DiagnosticInfoSeverity::Warning => DiagnosticSeverity::Warning,
            DiagnosticInfoSeverity::Info => DiagnosticSeverity::Info,
            DiagnosticInfoSeverity::Hint => DiagnosticSeverity::Hint,
        },
        source: info.source.clone(),
    }
}

impl MainView {
    /// Execute a query and update results using QueryService
    pub(super) fn execute_query(
        &mut self,
        query_tabs_panel: Entity<QueryTabsPanel>,
        editor_index: usize,
        sql: String,
        connection_id: Option<Uuid>,
        results_panel: Entity<ResultsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Check for destructive operations and warn user
        let engine = QueryEngine::new();
        if let Some(warning) = engine.analyze_for_destructive_operations(&sql) {
            tracing::warn!(
                operation_type = ?warning.operation_type,
                affected_object = %warning.affected_object,
                reason = %warning.reason,
                "Destructive operation detected: {}",
                warning.reason
            );
            
            // TODO: Show confirmation dialog
            // For now, we log the warning and continue execution
            // A full implementation would show a dialog and only proceed if user confirms
        }

        // Get services from app state
        let app_state = match cx.try_global::<AppState>() {
            Some(state) => state,
            None => {
                tracing::error!("AppState not available");
                return;
            }
        };

        let query_service = app_state.query_service.clone();
        let schema_service = app_state.schema_service.clone();
        let connection = connection_id.and_then(|id| app_state.connections.get(id));

        // Get connection info for display
        let connection_info = connection_id.and_then(|id| {
            app_state
                .saved_connections()
                .into_iter()
                .find(|c| c.id == id)
        });
        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
        let database_name = connection_info.as_ref().and_then(|c| {
            c.params
                .get("database")
                .or_else(|| c.params.get("path"))
                .cloned()
        });

        results_panel.update(cx, |panel, cx| {
            panel.set_loading(true, cx);
        });

        if let (Some(conn_id), Some(conn)) = (connection_id, connection) {

            // Get and store the cancel handle for this query
            // This allows us to interrupt the actual database query, not just drop the task
            if let Some(cancel_handle) = conn.cancel_handle() {
                self.query_cancel_handles
                    .insert(editor_index, cancel_handle);
            }

            // Store the task so we can cancel it later
            let sql = sql.clone();
            let query_tabs_panel = query_tabs_panel.downgrade();
            let results_panel = results_panel.downgrade();

            let task = cx.spawn_in(window, async move |this, cx| {
                    // ✅ Use QueryService - all logic encapsulated
                    let service_execution = query_service.execute_query(conn, conn_id, &sql).await;
                    let is_schema_modifying_sql = QueryEngine::new().is_schema_modifying(&sql);

                    _ = query_tabs_panel.update(cx, |panel, cx| {
                        panel.set_editor_executing(editor_index, false, cx);
                    });

                    // Convert service QueryExecution to component QueryExecution
                    let execution = match service_execution {
                        Ok(exec) => {
                            // Invalidate schema cache if DDL was executed
                            if is_schema_modifying_sql {
                                schema_service.invalidate_connection_cache(conn_id);

                                // Trigger LSP schema refresh on the editor so completions
                                // reflect the new schema without requiring a reconnect.
                                _ = query_tabs_panel.update(cx, |panel, cx| {
                                    if let Some(editor) = panel.get_editor(editor_index) {
                                        _ = editor.update(cx, |editor, cx| {
                                            editor.notify_query_executed(&sql, cx);
                                        });
                                    }
                                });
                            }

                            let start_time = chrono::Utc::now()
                                - chrono::Duration::milliseconds(exec.duration_ms as i64);
                            let end_time = chrono::Utc::now();

                            QueryExecution {
                                sql: exec.sql,
                                start_time,
                                end_time,
                                duration_ms: exec.duration_ms,
                                connection_name,
                                database_name,
                                statements: exec
                                    .statements
                                    .into_iter()
                                    .map(|s| StatementResult {
                                        sql: s.sql,
                                        duration_ms: s.duration_ms,
                                        result: s.result,
                                        error: s.error,
                                        affected_rows: s.affected_rows,
                                    })
                                    .collect(),
                            }
                        }
                        Err(e) => {
                            let now = chrono::Utc::now();
                            QueryExecution {
                                sql,
                                start_time: now,
                                end_time: now,
                                duration_ms: 0,
                                connection_name,
                                database_name,
                                statements: vec![StatementResult {
                                    sql: String::new(),
                                    duration_ms: 0,
                                    result: None,
                                    error: Some(format!("Service error: {}", e)),
                                    affected_rows: 0,
                                }],
                            }
                        }
                    };

                    _ = results_panel.update_in(cx, |panel, window, cx| {
                        panel.set_execution(execution, window, cx);
                    });

                    // Remove this task and cancel handle from the running maps when done
                    _ = this.update(cx, |main_view, _cx| {
                        main_view.running_query_tasks.remove(&editor_index);
                        main_view.query_cancel_handles.remove(&editor_index);
                    });

                    anyhow::Ok(())
                });

            // Map the task to ignore the result (we handle it inside)
            let task = cx.spawn(async move |_this, _cx| {
                let _ = task.await;
            });

            // Store the task so it can be cancelled via handle_stop_query
            self.running_query_tasks.insert(editor_index, task);
        } else {
            query_tabs_panel.update(cx, |panel, cx| {
                panel.set_editor_executing(editor_index, false, cx);
            });

            let start_time = chrono::Utc::now();
            let end_time = start_time;

            let execution = QueryExecution {
                sql,
                start_time,
                end_time,
                duration_ms: 0,
                connection_name,
                database_name,
                statements: vec![StatementResult {
                    sql: String::new(),
                    duration_ms: 0,
                    result: None,
                    error: Some(
                        "No connection selected. Please connect to a database first.".to_string(),
                    ),
                    affected_rows: 0,
                }],
            };

            // Need to spawn to get window context
            cx.spawn_in(window, async move |_main_view, cx| {
                _ = results_panel.update_in(cx, |panel, window, cx| {
                    panel.set_execution(execution, window, cx);
                });
                anyhow::Ok(())
            })
            .detach();
        }
    }

    /// Handle events from the query tabs panel
    pub(super) fn handle_query_tabs_event(
        &mut self,
        event: QueryTabsPanelEvent,
        query_tabs_panel: Entity<QueryTabsPanel>,
        results_panel: Entity<ResultsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            QueryTabsPanelEvent::ExecuteQuery {
                sql,
                connection_id,
                editor_index,
            } => {
                self.execute_query(
                    query_tabs_panel,
                    editor_index,
                    sql,
                    connection_id,
                    results_panel,
                    window,
                    cx,
                );
            }
            QueryTabsPanelEvent::ExecuteSelection {
                sql,
                connection_id,
                editor_index,
            } => {
                // Execute selection uses the same logic as ExecuteQuery
                self.execute_query(
                    query_tabs_panel,
                    editor_index,
                    sql,
                    connection_id,
                    results_panel,
                    window,
                    cx,
                );
            }
            QueryTabsPanelEvent::ExplainQuery {
                sql,
                connection_id,
                editor_index,
            } => {
                self.explain_query(
                    query_tabs_panel,
                    editor_index,
                    sql,
                    connection_id,
                    results_panel,
                    window,
                    cx,
                );
            }
            QueryTabsPanelEvent::ExplainSelection {
                sql,
                connection_id,
                editor_index,
            } => {
                // ExplainSelection uses the same logic as ExplainQuery
                self.explain_query(
                    query_tabs_panel,
                    editor_index,
                    sql,
                    connection_id,
                    results_panel,
                    window,
                    cx,
                );
            }
            QueryTabsPanelEvent::CancelQuery { editor_index } => {
                // Cancel the currently executing query for this editor
                tracing::info!(editor_index = editor_index, "Cancelling query");
                query_tabs_panel.update(cx, |panel, cx| {
                    panel.set_editor_executing(editor_index, false, cx);
                });
            }
            QueryTabsPanelEvent::AddConnection => {
                self.open_new_connection_dialog(window, cx);
            }
            QueryTabsPanelEvent::SaveObject {
                connection_id,
                object_type,
                definition,
                editor_index: _,
            } => {
                // Handle save object - save to version history
                self.handle_save_object(connection_id, object_type, definition, window, cx);
            }
            QueryTabsPanelEvent::PreviewDdl {
                object_type,
                definition: _,
                editor_index: _,
            } => {
                // Preview DDL - show the DDL that would be generated
                tracing::info!("PreviewDdl event received for {:?}", object_type);
                // TODO: Open a preview dialog or panel
            }
            QueryTabsPanelEvent::SaveQuery {
                saved_query_id,
                connection_id,
                sql,
                editor_index,
            } => {
                // Handle save query request
                self.handle_save_query_request(
                    query_tabs_panel,
                    editor_index,
                    saved_query_id,
                    connection_id,
                    sql,
                    window,
                    cx,
                );
            }
            QueryTabsPanelEvent::DiagnosticsChanged {
                diagnostics,
                editor_index,
            } => {
                // Store diagnostics in WorkspaceState (source of truth)
                let editor_id = EditorId(editor_index);
                let workspace_diagnostics: Vec<EditorDiagnostic> =
                    diagnostics.iter().map(convert_diagnostic).collect();

                self.workspace_state.update(cx, |state, cx| {
                    state.set_diagnostics(editor_id, workspace_diagnostics, cx);
                });

                // Also update results panel directly (will be replaced by subscription later)
                results_panel.update(cx, |panel, cx| {
                    panel.set_problems(diagnostics, cx);
                });
            }
            QueryTabsPanelEvent::ActiveEditorChanged { editor_index } => {
                // Update WorkspaceState with the new active editor
                let editor_id = editor_index.map(EditorId);
                self.workspace_state.update(cx, |state, cx| {
                    state.set_active_editor(editor_id, cx);
                });
            }
            QueryTabsPanelEvent::SwitchConnection { connection_id, editor_index } => {
                // TODO: Implement connection switching for editor
                tracing::info!("Switch connection requested for editor {} to connection {}", editor_index, connection_id);
            }
            QueryTabsPanelEvent::SwitchDatabase { database_name, editor_index } => {
                // TODO: Implement database switching for editor
                tracing::info!("Switch database requested for editor {} to database {}", editor_index, database_name);
            }
        }
    }

    /// Execute EXPLAIN for a query and update results panel
    pub(super) fn explain_query(
        &mut self,
        query_tabs_panel: Entity<QueryTabsPanel>,
        editor_index: usize,
        sql: String,
        connection_id: Option<Uuid>,
        results_panel: Entity<ResultsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get services from app state
        let app_state = match cx.try_global::<AppState>() {
            Some(state) => state,
            None => {
                tracing::error!("AppState not available");
                return;
            }
        };

        let query_service = app_state.query_service.clone();
        let connection = connection_id.and_then(|id| app_state.connections.get(id));

        // Get connection info for display
        let connection_info = connection_id.and_then(|id| {
            app_state
                .saved_connections()
                .into_iter()
                .find(|c| c.id == id)
        });
        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
        let database_name = connection_info.as_ref().and_then(|c| {
            c.params
                .get("database")
                .or_else(|| c.params.get("path"))
                .cloned()
        });

        results_panel.update(cx, |panel, cx| {
            panel.set_loading(true, cx);
        });

        if let (Some(conn_id), Some(conn)) = (connection_id, connection) {

            let sql = sql.clone();
            let query_tabs_panel = query_tabs_panel.downgrade();
            let results_panel = results_panel.downgrade();

            cx.spawn_in(window, async move |_this, cx| {
                    // Use QueryService to run EXPLAIN
                    let service_result = query_service.explain_query(conn, conn_id, &sql).await;

                    _ = query_tabs_panel.update(cx, |panel, cx| {
                        panel.set_editor_executing(editor_index, false, cx);
                    });

                    // Convert service ExplainResult to UI ExplainResult
                    let explain_result = match service_result {
                        Ok(result) => ExplainResult {
                            sql: result.sql,
                            duration_ms: result.duration_ms,
                            raw_output: result.raw_output,
                            query_plan: result.query_plan,
                            analyzed_plan: result.analyzed_plan,
                            error: result.error,
                            connection_name,
                            database_name,
                            timestamp: chrono::Utc::now(),
                        },
                        Err(e) => ExplainResult {
                            sql,
                            duration_ms: 0,
                            raw_output: None,
                            query_plan: None,
                            analyzed_plan: None,
                            error: Some(format!("Service error: {}", e)),
                            connection_name,
                            database_name,
                            timestamp: chrono::Utc::now(),
                        },
                    };

                    _ = results_panel.update_in(cx, |panel, window, cx| {
                        panel.add_explain_result(explain_result, window, cx);
                    });

                    anyhow::Ok(())
                })
                .detach();
        } else {
            query_tabs_panel.update(cx, |panel, cx| {
                panel.set_editor_executing(editor_index, false, cx);
            });

            let explain_result = ExplainResult {
                sql,
                duration_ms: 0,
                raw_output: None,
                query_plan: None,
                analyzed_plan: None,
                error: Some(
                    "No connection selected. Please connect to a database first.".to_string(),
                ),
                connection_name,
                database_name,
                timestamp: chrono::Utc::now(),
            };

            cx.spawn_in(window, async move |_main_view, cx| {
                _ = results_panel.update_in(cx, |panel, window, cx| {
                    panel.add_explain_result(explain_result, window, cx);
                });
                anyhow::Ok(())
            })
            .detach();
        }
    }

    /// Handle a save query request from QueryTabsPanel or a direct QueryEditor
    pub(super) fn handle_save_query_request(
        &mut self,
        query_tabs_panel: Entity<QueryTabsPanel>,
        editor_index: usize,
        saved_query_id: Option<Uuid>,
        connection_id: Option<Uuid>,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get the editor from the tabs panel
        let editor_weak = query_tabs_panel.read(cx).get_editor(editor_index);

        let Some(editor_weak) = editor_weak else {
            tracing::warn!("Could not find editor at index {}", editor_index);
            return;
        };

        // Determine the connection_id to use
        let conn_id = connection_id.or_else(|| self.active_connection_id(cx));

        let Some(conn_id) = conn_id else {
            // No connection available - show notification
            use zqlz_ui::widgets::{WindowExt, notification::Notification};
            window.push_notification(
                Notification::warning(
                    "No connection selected. Please connect to a database first.",
                ),
                cx,
            );
            return;
        };

        if let Some(query_id) = saved_query_id {
            // Update existing saved query
            self.update_saved_query(query_id, sql, editor_weak, window, cx);
        } else {
            // Show save dialog for new query
            self.show_save_query_dialog(editor_weak, sql, conn_id, window, cx);
        }
    }

    /// Handle a save query request from a dock-based QueryEditor.
    ///
    /// This is similar to `handle_save_query_request` but takes a WeakEntity<QueryEditor>
    /// directly instead of looking it up from QueryTabsPanel.
    pub(super) fn handle_dock_editor_save_query(
        &mut self,
        editor_weak: WeakEntity<QueryEditor>,
        saved_query_id: Option<Uuid>,
        connection_id: Option<Uuid>,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Determine the connection_id to use
        let conn_id = connection_id.or_else(|| self.active_connection_id(cx));

        let Some(conn_id) = conn_id else {
            // No connection available - show notification
            use zqlz_ui::widgets::{WindowExt, notification::Notification};
            window.push_notification(
                Notification::warning(
                    "No connection selected. Please connect to a database first.",
                ),
                cx,
            );
            return;
        };

        if let Some(query_id) = saved_query_id {
            // Update existing saved query
            self.update_saved_query(query_id, sql, editor_weak, window, cx);
        } else {
            // Show save dialog for new query
            self.show_save_query_dialog(editor_weak, sql, conn_id, window, cx);
        }
    }

    /// Creates a new query editor tab in the center dock.
    pub(super) fn handle_new_query(
        &mut self,
        _action: &crate::actions::NewQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Creating new query tab");
        self.query_counter += 1;
        let query_name = format!("Query {}", self.query_counter);

        // Get active connection from WorkspaceState (source of truth)
        let active_conn_id = self.active_connection_id(cx);
        tracing::debug!(
            query_name = %query_name,
            connection_id = ?active_conn_id,
            "Creating query editor"
        );

        // Get the connection object, driver type, and connection name if we have a selected connection
        let (connection, driver_type, connection_name) = active_conn_id
            .and_then(|id| {
                cx.try_global::<AppState>().and_then(|state| {
                    let conn = state.connections.get(id)?;
                    let saved = state.saved_connections().into_iter().find(|c| c.id == id)?;
                    Some((conn, saved.driver.clone(), saved.name.clone()))
                })
            })
            .map(|(conn, driver, name)| (Some(conn), Some(driver), Some(name)))
            .unwrap_or((None, None, None));

        // Get schema_service from global AppState
        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .expect("AppState not initialized");

        // Create an EditorId in WorkspaceState to track this editor
        let editor_id = self.workspace_state.update(cx, |state, cx| {
            state.create_editor(active_conn_id, query_name.clone(), cx)
        });

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new(query_name.clone(), active_conn_id, schema_service, window, cx);
            
            // If we have a connection, set it immediately so schema loads
            if let Some(ref conn) = connection {
                tracing::debug!(connection_id = ?active_conn_id, driver_type = ?driver_type, "Setting connection on new QueryEditor");
                editor.set_connection(active_conn_id, connection_name.clone(), Some(conn.clone()), driver_type, cx);
            } else {
                tracing::warn!("No connection available for new QueryEditor");
            }
            
            editor
        });

        let subscription = self.subscribe_query_editor(
            &query_editor,
            query_name.clone(),
            editor_id,
            window,
            cx,
        );

        self._subscriptions.push(subscription);

        // Track this query editor in MainView
        self.query_editors.push(query_editor.downgrade());

        let query_editor_panel: Arc<dyn zqlz_ui::widgets::dock::PanelView> = Arc::new(query_editor);
        self.dock_area.update(cx, |area, cx| {
            area.add_panel(
                query_editor_panel,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });
    }

    /// Subscribe to a QueryEditor's events, wiring up execution, explain, cancel, save, and diagnostics.
    ///
    /// This is the shared subscription logic used by both `handle_new_query` and
    /// `open_query_editor_with_saved_query` to ensure all editor types have full functionality.
    pub(super) fn subscribe_query_editor(
        &self,
        query_editor: &Entity<QueryEditor>,
        query_name: String,
        editor_id: EditorId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Subscription {
        let results_panel = self.results_panel.clone();
        let query_editor_weak = query_editor.downgrade();
        let workspace_state = self.workspace_state.downgrade();
        cx.subscribe_in(query_editor, window, {
            move |_this, _editor, event: &QueryEditorEvent, window, cx| {
                match event {
                    QueryEditorEvent::ExecuteQuery { sql, connection_id } => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };
                        
                        tracing::info!(query_name = %query_name, "Executing query from editor");
                        
                        editor.update(cx, |editor, cx| {
                            editor.set_executing(true, cx);
                        });
                        
                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available");
                            return;
                        };
                        
                        // ✅ Get QueryService
                        let query_service = app_state.query_service.clone();
                        
                        let conn_id = connection_id.or_else(|| {
                            app_state.saved_connections()
                                .first()
                                .map(|c| c.id)
                        });
                        
                        let Some(conn_id) = conn_id else {
                            tracing::warn!("No connection available for query execution");
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        let Some(conn) = app_state.connections.get(conn_id) else {
                            tracing::error!("Connection not found: {}", conn_id);
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };

                        // Get cancel handle for WorkspaceState tracking
                        let cancel_handle = conn.cancel_handle();
                        
                        // Get connection info for display (before releasing app_state borrow)
                        let connection_info = app_state.saved_connections().into_iter().find(|c| c.id == conn_id);
                        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
                        let database_name = connection_info.as_ref().and_then(|c| {
                            c.params.get("database").or_else(|| c.params.get("path")).cloned()
                        });
                        
                        let sql = sql.clone();
                        
                        // Track query in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade() {
                            if let Some(handle) = cancel_handle {
                                state.update(cx, |state, cx| {
                                    state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                                });
                            }
                        }
                        
                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();
                        
                        cx.spawn_in(window, async move |_this, cx| {
                            tracing::debug!(sql = %sql, "Executing query");
                            
                            // ✅ Use QueryService - all logic encapsulated
                            let service_execution = query_service.execute_query(conn, conn_id, &sql).await;
                            let query_success = service_execution.is_ok();

                            // Keep a copy for the DDL check below; `sql` may be moved
                            // into the error-branch QueryExecution before we get to it.
                            let sql_for_ddl = sql.clone();

                            // Convert service QueryExecution to component QueryExecution
                            let execution = match service_execution {
                                Ok(exec) => {
                                    tracing::info!(statements = exec.statements.len(), duration_ms = exec.duration_ms, "Query executed successfully");
                                    
                                    let start_time = chrono::Utc::now() - chrono::Duration::milliseconds(exec.duration_ms as i64);
                                    let end_time = chrono::Utc::now();
                                    
                                    QueryExecution {
                                        sql: exec.sql,
                                        start_time,
                                        end_time,
                                        duration_ms: exec.duration_ms,
                                        connection_name,
                                        database_name,
                                        statements: exec.statements.into_iter().map(|s| StatementResult {
                                            sql: s.sql,
                                            duration_ms: s.duration_ms,
                                            result: s.result,
                                            error: s.error,
                                            affected_rows: s.affected_rows,
                                        }).collect(),
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, "Query execution failed");
                                    let now = chrono::Utc::now();
                                    QueryExecution {
                                        sql,
                                        start_time: now,
                                        end_time: now,
                                        duration_ms: 0,
                                        connection_name,
                                        database_name,
                                        statements: vec![StatementResult {
                                            sql: String::new(),
                                            duration_ms: 0,
                                            result: None,
                                            error: Some(format!("Service error: {}", e)),
                                            affected_rows: 0,
                                        }],
                                    }
                                }
                            };
                            
                            _ = results_panel.update_in(cx, |panel, window, cx| {
                                panel.set_execution(execution, window, cx);
                            });

                            // Mark query as completed in WorkspaceState
                            if let Some(state) = workspace_state_weak.upgrade() {
                                _ = state.update(cx, |state, cx| {
                                    state.complete_query(editor_id, query_success, cx);
                                });
                            }
                            
                            // Always reset executing state when query completes
                            if let Err(e) = editor_weak.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            }) {
                                tracing::warn!("Failed to reset executing state: {}", e);
                            }

                            // On successful DDL execution, invalidate the schema cache so
                            // column completions reflect the new database structure.
                            if query_success {
                                _ = editor_weak.update(cx, |editor, cx| {
                                    editor.notify_query_executed(&sql_for_ddl, cx);
                                });
                            }

                            anyhow::Ok(())
                        }).detach();
                    }
                    QueryEditorEvent::ExecuteSelection { sql, connection_id } => {
                        // ExecuteSelection is the same as ExecuteQuery, just with different SQL
                        // (selected text vs. full content)
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };
                        
                        tracing::info!(query_name = %query_name, "Executing selection from editor");
                        
                        editor.update(cx, |editor, cx| {
                            editor.set_executing(true, cx);
                        });
                        
                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available");
                            return;
                        };
                        
                        let query_service = app_state.query_service.clone();
                        
                        let conn_id = connection_id.or_else(|| {
                            app_state.saved_connections()
                                .first()
                                .map(|c| c.id)
                        });
                        
                        let Some(conn_id) = conn_id else {
                            tracing::warn!("No connection available for query execution");
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        let Some(conn) = app_state.connections.get(conn_id) else {
                            tracing::error!("Connection not found: {}", conn_id);
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };

                        // Get cancel handle for WorkspaceState tracking
                        let cancel_handle = conn.cancel_handle();
                        
                        // Get connection info for display (before releasing app_state borrow)
                        let connection_info = app_state.saved_connections().into_iter().find(|c| c.id == conn_id);
                        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
                        let database_name = connection_info.as_ref().and_then(|c| {
                            c.params.get("database").or_else(|| c.params.get("path")).cloned()
                        });
                        
                        let sql = sql.clone();
                        
                        // Track query in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade() {
                            if let Some(handle) = cancel_handle {
                                state.update(cx, |state, cx| {
                                    state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                                });
                            }
                        }
                        
                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();
                        
                        cx.spawn_in(window, async move |_this, cx| {
                            tracing::debug!(sql = %sql, "Executing selection");

                            let service_execution = query_service.execute_query(conn, conn_id, &sql).await;
                            let query_success = service_execution.is_ok();

                            // Keep a copy for the DDL check below; `sql` may be moved
                            // into the error-branch QueryExecution before we get to it.
                            let sql_for_ddl = sql.clone();

                            let execution = match service_execution {
                                Ok(exec) => {
                                    tracing::info!(statements = exec.statements.len(), duration_ms = exec.duration_ms, "Selection executed successfully");
                                    
                                    let start_time = chrono::Utc::now() - chrono::Duration::milliseconds(exec.duration_ms as i64);
                                    let end_time = chrono::Utc::now();
                                    
                                    QueryExecution {
                                        sql: exec.sql,
                                        start_time,
                                        end_time,
                                        duration_ms: exec.duration_ms,
                                        connection_name,
                                        database_name,
                                        statements: exec.statements.into_iter().map(|s| StatementResult {
                                            sql: s.sql,
                                            duration_ms: s.duration_ms,
                                            result: s.result,
                                            error: s.error,
                                            affected_rows: s.affected_rows,
                                        }).collect(),
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, "Selection execution failed");
                                    let now = chrono::Utc::now();
                                    QueryExecution {
                                        sql,
                                        start_time: now,
                                        end_time: now,
                                        duration_ms: 0,
                                        connection_name,
                                        database_name,
                                        statements: vec![StatementResult {
                                            sql: String::new(),
                                            duration_ms: 0,
                                            result: None,
                                            error: Some(format!("Service error: {}", e)),
                                            affected_rows: 0,
                                        }],
                                    }
                                }
                            };
                            
                            _ = results_panel.update_in(cx, |panel, window, cx| {
                                panel.set_execution(execution, window, cx);
                            });

                            // Mark query as completed in WorkspaceState
                            if let Some(state) = workspace_state_weak.upgrade() {
                                _ = state.update(cx, |state, cx| {
                                    state.complete_query(editor_id, query_success, cx);
                                });
                            }
                            
                            // Always reset executing state when query completes
                            if let Err(e) = editor_weak.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            }) {
                                tracing::warn!("Failed to reset executing state: {}", e);
                            }

                            // On successful DDL execution, invalidate the schema cache so
                            // column completions reflect the new database structure.
                            if query_success {
                                _ = editor_weak.update(cx, |editor, cx| {
                                    editor.notify_query_executed(&sql_for_ddl, cx);
                                });
                            }

                            anyhow::Ok(())
                        }).detach();
                    }
                    QueryEditorEvent::ExplainQuery { sql, connection_id } => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };
                        
                        tracing::info!(query_name = %query_name, "Explaining query from editor");
                        
                        editor.update(cx, |editor, cx| {
                            editor.set_executing(true, cx);
                        });
                        
                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available");
                            return;
                        };
                        
                        let query_service = app_state.query_service.clone();
                        
                        let conn_id = connection_id.or_else(|| {
                            app_state.saved_connections()
                                .first()
                                .map(|c| c.id)
                        });
                        
                        let Some(conn_id) = conn_id else {
                            tracing::warn!("No connection available for explain");
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        let Some(conn) = app_state.connections.get(conn_id) else {
                            tracing::error!("Connection not found: {}", conn_id);
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        // Get cancel handle for WorkspaceState tracking
                        let cancel_handle = conn.cancel_handle();
                        
                        // Get connection info for display (before releasing app_state borrow)
                        let connection_info = app_state.saved_connections().into_iter().find(|c| c.id == conn_id);
                        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
                        let database_name = connection_info.as_ref().and_then(|c| {
                            c.params.get("database").or_else(|| c.params.get("path")).cloned()
                        });
                        
                        let sql = sql.clone();
                        
                        // Track explain in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade() {
                            if let Some(handle) = cancel_handle {
                                state.update(cx, |state, cx| {
                                    state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                                });
                            }
                        }
                        
                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();
                        
                        cx.spawn_in(window, async move |_this, cx| {
                            tracing::debug!(sql = %sql, "Explaining query");
                            
                            let service_result = query_service.explain_query(conn, conn_id, &sql).await;
                            let explain_success = service_result.is_ok();
                            
                            let explain_result = match service_result {
                                Ok(result) => {
                                    tracing::info!(duration_ms = result.duration_ms, "Explain completed");
                                    ExplainResult {
                                        sql: result.sql,
                                        duration_ms: result.duration_ms,
                                        raw_output: result.raw_output,
                                        query_plan: result.query_plan,
                                        analyzed_plan: result.analyzed_plan,
                                        error: result.error,
                                        connection_name,
                                        database_name,
                                        timestamp: chrono::Utc::now(),
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, "Explain failed");
                                    ExplainResult {
                                        sql,
                                        duration_ms: 0,
                                        raw_output: None,
                                        query_plan: None,
                                        analyzed_plan: None,
                                        error: Some(format!("Service error: {}", e)),
                                        connection_name,
                                        database_name,
                                        timestamp: chrono::Utc::now(),
                                    }
                                }
                            };
                            
                            _ = results_panel.update_in(cx, |panel, window, cx| {
                                panel.add_explain_result(explain_result, window, cx);
                            });

                            // Mark explain as completed in WorkspaceState
                            if let Some(state) = workspace_state_weak.upgrade() {
                                _ = state.update(cx, |state, cx| {
                                    state.complete_query(editor_id, explain_success, cx);
                                });
                            }
                            
                            // Always reset executing state when explain completes
                            if let Err(e) = editor_weak.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            }) {
                                tracing::warn!("Failed to reset executing state: {}", e);
                            }
                            
                            anyhow::Ok(())
                        }).detach();
                    }
                    QueryEditorEvent::ExplainSelection { sql, connection_id } => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };
                        
                        tracing::info!(query_name = %query_name, "Explaining selection from editor");
                        
                        editor.update(cx, |editor, cx| {
                            editor.set_executing(true, cx);
                        });
                        
                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available");
                            return;
                        };
                        
                        let query_service = app_state.query_service.clone();
                        
                        let conn_id = connection_id.or_else(|| {
                            app_state.saved_connections()
                                .first()
                                .map(|c| c.id)
                        });
                        
                        let Some(conn_id) = conn_id else {
                            tracing::warn!("No connection available for explain");
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        let Some(conn) = app_state.connections.get(conn_id) else {
                            tracing::error!("Connection not found: {}", conn_id);
                            editor.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            });
                            return;
                        };
                        
                        // Get cancel handle for WorkspaceState tracking
                        let cancel_handle = conn.cancel_handle();
                        
                        // Get connection info for display (before releasing app_state borrow)
                        let connection_info = app_state.saved_connections().into_iter().find(|c| c.id == conn_id);
                        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
                        let database_name = connection_info.as_ref().and_then(|c| {
                            c.params.get("database").or_else(|| c.params.get("path")).cloned()
                        });
                        
                        let sql = sql.clone();
                        
                        // Track explain in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade() {
                            if let Some(handle) = cancel_handle {
                                state.update(cx, |state, cx| {
                                    state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                                });
                            }
                        }
                        
                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();
                        
                        cx.spawn_in(window, async move |_this, cx| {
                            tracing::debug!(sql = %sql, "Explaining selection");
                            
                            let service_result = query_service.explain_query(conn, conn_id, &sql).await;
                            let explain_success = service_result.is_ok();
                            
                            let explain_result = match service_result {
                                Ok(result) => {
                                    tracing::info!(duration_ms = result.duration_ms, "Explain completed");
                                    ExplainResult {
                                        sql: result.sql,
                                        duration_ms: result.duration_ms,
                                        raw_output: result.raw_output,
                                        query_plan: result.query_plan,
                                        analyzed_plan: result.analyzed_plan,
                                        error: result.error,
                                        connection_name,
                                        database_name,
                                        timestamp: chrono::Utc::now(),
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(error = %e, "Explain failed");
                                    ExplainResult {
                                        sql,
                                        duration_ms: 0,
                                        raw_output: None,
                                        query_plan: None,
                                        analyzed_plan: None,
                                        error: Some(format!("Service error: {}", e)),
                                        connection_name,
                                        database_name,
                                        timestamp: chrono::Utc::now(),
                                    }
                                }
                            };
                            
                            _ = results_panel.update_in(cx, |panel, window, cx| {
                                panel.add_explain_result(explain_result, window, cx);
                            });

                            // Mark explain as completed in WorkspaceState
                            if let Some(state) = workspace_state_weak.upgrade() {
                                _ = state.update(cx, |state, cx| {
                                    state.complete_query(editor_id, explain_success, cx);
                                });
                            }
                            
                            // Always reset executing state when explain completes
                            if let Err(e) = editor_weak.update(cx, |editor, cx| {
                                editor.set_executing(false, cx);
                            }) {
                                tracing::warn!("Failed to reset executing state: {}", e);
                            }
                            
                            anyhow::Ok(())
                        }).detach();
                    }
                    QueryEditorEvent::CancelQuery => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };
                        
                        tracing::info!(query_name = %query_name, "Query cancelled by user");
                        
                        editor.update(cx, |editor, cx| {
                            editor.set_executing(false, cx);
                        });
                        
                        // Note: The actual database query may still complete in the background,
                        // but the UI will no longer show it as executing
                    }
                    QueryEditorEvent::SaveObject { .. } => {
                        // View/procedure/function editors use a separate subscription
                        // in view_handlers.rs - this is for regular query editors only
                        tracing::debug!("SaveObject event received on regular query editor - ignoring");
                    }
                    QueryEditorEvent::PreviewDdl { .. } => {
                        // TODO: Implement DDL preview modal
                        tracing::debug!("PreviewDdl event not implemented for regular query editors");
                    }
                    QueryEditorEvent::SaveQuery {
                        saved_query_id,
                        connection_id,
                        sql,
                    } => {
                        tracing::info!(
                            saved_query_id = ?saved_query_id,
                            connection_id = ?connection_id,
                            sql_len = sql.len(),
                            "SaveQuery event received - handling save"
                        );
                        // Handle save query for dock-based editor
                        let editor_weak = query_editor_weak.clone();
                        _this.handle_dock_editor_save_query(
                            editor_weak,
                            *saved_query_id,
                            *connection_id,
                            sql.clone(),
                            window,
                            cx,
                        );
                    }
                    QueryEditorEvent::DiagnosticsChanged { diagnostics } => {
                        // Store diagnostics in WorkspaceState (source of truth)
                        if let Some(state) = workspace_state.upgrade() {
                            let workspace_diagnostics: Vec<EditorDiagnostic> =
                                diagnostics.iter().map(convert_diagnostic).collect();
                            state.update(cx, |state, cx| {
                                state.set_diagnostics(editor_id, workspace_diagnostics, cx);
                            });
                        }

                        // Also update results panel directly (will be replaced by subscription later)
                        let results = results_panel.clone();
                        results.update(cx, |panel, cx| {
                            panel.set_problems(diagnostics.clone(), cx);
                        });
                    }
                    QueryEditorEvent::SwitchConnection { connection_id } => {
                        // TODO: Implement connection switching for dock-based editor
                        tracing::info!("Switch connection requested for editor {} to connection {}", editor_id.0, connection_id);
                    }
                    QueryEditorEvent::SwitchDatabase { database_name } => {
                        // TODO: Implement database switching for dock-based editor
                        tracing::info!("Switch database requested for editor {} to database {}", editor_id.0, database_name);
                    }
                }
            }
        })
    }

    /// Handle saving a database object (view, function, procedure, trigger) to version history
    fn handle_save_object(
        &mut self,
        connection_id: Uuid,
        object_type: EditorObjectType,
        definition: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Convert EditorObjectType to DatabaseObjectType and extract name/schema
        let (db_object_type, object_name, object_schema) = match &object_type {
            EditorObjectType::Query => {
                // Queries don't get versioned
                return;
            }
            EditorObjectType::View { name, schema, .. } => {
                let name = match name {
                    Some(n) => n.clone(),
                    None => {
                        tracing::warn!("Cannot save unnamed view to version history");
                        return;
                    }
                };
                (DatabaseObjectType::View, name, schema.clone())
            }
            EditorObjectType::Function { name, schema, .. } => {
                let name = match name {
                    Some(n) => n.clone(),
                    None => {
                        tracing::warn!("Cannot save unnamed function to version history");
                        return;
                    }
                };
                (DatabaseObjectType::Function, name, schema.clone())
            }
            EditorObjectType::Procedure { name, schema, .. } => {
                let name = match name {
                    Some(n) => n.clone(),
                    None => {
                        tracing::warn!("Cannot save unnamed procedure to version history");
                        return;
                    }
                };
                (DatabaseObjectType::Procedure, name, schema.clone())
            }
            EditorObjectType::Trigger { name, schema, .. } => {
                let name = match name {
                    Some(n) => n.clone(),
                    None => {
                        tracing::warn!("Cannot save unnamed trigger to version history");
                        return;
                    }
                };
                (DatabaseObjectType::Trigger, name, schema.clone())
            }
        };

        // Generate a commit message
        let message = format!("Updated {} via editor", object_type.display_name());

        // Save to version history
        self.save_object_version(
            connection_id,
            db_object_type,
            object_schema,
            object_name,
            definition,
            message,
            window,
            cx,
        );

        tracing::info!(
            "Saved {} to version history on connection {}",
            object_type.display_name(),
            connection_id
        );
    }
}
