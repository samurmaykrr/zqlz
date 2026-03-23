use gpui::*;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppState;
use crate::components::{
    ExplainResult, QueryEditor, QueryEditorEvent, QueryExecution, QueryExecutionParams,
    StatementResult,
};
use crate::workspace_state::{DiagnosticSeverity, EditorDiagnostic, EditorId};
use zqlz_query::{DiagnosticInfo, DiagnosticInfoSeverity};
use zqlz_text_editor::{DocumentIdentity, TextDocument};

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
    fn connected_connection_options(cx: &App) -> Vec<(Uuid, String)> {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return Vec::new();
        };

        app_state
            .saved_connections()
            .into_iter()
            .filter(|saved| app_state.connections.is_connected(saved.id))
            .map(|saved| (saved.id, saved.name))
            .collect()
    }

    fn configure_query_editor_switchers(
        &self,
        query_editor: &Entity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let available_connections = Self::connected_connection_options(cx);
        query_editor.update(cx, |editor, cx| {
            editor.set_available_connections(available_connections, cx);
        });

        let connection_id = query_editor.read(cx).connection_id();
        let Some(connection_id) = connection_id else {
            query_editor.update(cx, |editor, cx| {
                editor.set_available_databases(Vec::new(), cx);
                editor.set_current_database(None, cx);
            });
            return;
        };

        let (default_database, connection) = {
            let Some(app_state) = cx.try_global::<AppState>() else {
                return;
            };

            let saved = app_state
                .saved_connections()
                .into_iter()
                .find(|saved| saved.id == connection_id);
            let default_database = saved.as_ref().and_then(|saved| {
                saved
                    .params
                    .get("database")
                    .or_else(|| saved.params.get("path"))
                    .cloned()
            });
            let connection = app_state.connections.get(connection_id);
            (default_database, connection)
        };

        query_editor.update(cx, |editor, cx| {
            editor.set_current_database(default_database, cx);
        });

        let Some(connection) = connection else {
            query_editor.update(cx, |editor, cx| {
                editor.set_available_databases(Vec::new(), cx);
            });
            return;
        };

        let query_editor_weak = query_editor.downgrade();
        cx.spawn_in(window, async move |_main_view, cx| {
            let databases = if let Some(schema_introspection) = connection.as_schema_introspection()
            {
                match schema_introspection.list_databases().await {
                    Ok(databases) => databases
                        .into_iter()
                        .map(|database| database.name)
                        .collect(),
                    Err(error) => {
                        tracing::debug!(
                            connection_id = %connection_id,
                            error = %error,
                            "failed to list databases for query editor switcher"
                        );
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };

            let _ = query_editor_weak.update(cx, |editor, cx| {
                editor.set_available_databases(databases, cx);
            });
        })
        .detach();
    }

    pub(super) fn finalize_query_editor_open(
        &mut self,
        query_editor: Entity<QueryEditor>,
        display_name: String,
        editor_id: EditorId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<QueryEditor> {
        let subscription =
            self.subscribe_query_editor(&query_editor, display_name, editor_id, window, cx);
        let document_context = query_editor.read(cx).document_context(cx);
        let is_dirty = query_editor.read(cx).is_dirty(cx);
        let display_name = query_editor.read(cx).name();
        self.refresh_workspace_document_state(
            editor_id,
            document_context,
            is_dirty,
            display_name,
            cx,
        );

        self._subscriptions.push(subscription);
        self.query_editors.push(query_editor.downgrade());

        let query_editor_panel: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(query_editor.clone());
        self.dock_area.update(cx, |area, cx| {
            area.add_panel(
                query_editor_panel,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });

        self.configure_query_editor_switchers(&query_editor, window, cx);

        let focus_handle = query_editor.read(cx).editor_focus_handle(cx);
        window.focus(&focus_handle, cx);

        query_editor
    }

    pub(super) fn create_workspace_editor(
        &self,
        connection_id: Option<Uuid>,
        display_name: String,
        cx: &mut Context<Self>,
    ) -> EditorId {
        self.workspace_state.update(cx, |state, cx| {
            state.create_editor(connection_id, display_name, cx)
        })
    }

    pub(super) fn refresh_workspace_document_state(
        &self,
        editor_id: EditorId,
        document_context: zqlz_text_editor::DocumentContext,
        is_dirty: bool,
        display_name: String,
        cx: &mut Context<Self>,
    ) {
        self.workspace_state.update(cx, |state, cx| {
            state.update_editor_document(editor_id, document_context, is_dirty, display_name, cx);
        });
    }

    pub(super) fn open_query_editor_with_content(
        &mut self,
        display_name: String,
        content: String,
        file_path: Option<String>,
        connection_id: Option<Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<QueryEditor> {
        let (connection, driver_type, connection_name) = connection_id
            .and_then(|id| {
                cx.try_global::<AppState>().and_then(|state| {
                    let connection = state.connections.get(id)?;
                    let saved = state
                        .saved_connections()
                        .into_iter()
                        .find(|saved| saved.id == id)?;
                    Some((connection, saved.driver.clone(), saved.name.clone()))
                })
            })
            .map(|(connection, driver, name)| (Some(connection), Some(driver), Some(name)))
            .unwrap_or((None, None, None));

        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .expect("AppState not initialized");

        let editor_id = self.create_workspace_editor(connection_id, display_name.clone(), cx);
        self.workspace_state.update(cx, |state, cx| {
            state.update_editor(
                editor_id,
                |editor_state| {
                    editor_state.file_path = file_path.clone();
                    editor_state.display_name = display_name.clone();
                    editor_state.is_dirty = false;
                },
                cx,
            );
        });

        let identity = file_path
            .as_ref()
            .and_then(|path| DocumentIdentity::from_path(path.clone()))
            .unwrap_or_else(|| DocumentIdentity::internal().expect("internal document uri"));
        let mut document = TextDocument::with_text(identity, &content);
        document.mark_buffer_saved();

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new_with_document(
                display_name.clone(),
                connection_id,
                document,
                schema_service,
                window,
                cx,
            );

            if let Some(ref connection) = connection {
                editor.set_connection(
                    connection_id,
                    connection_name.clone(),
                    Some(connection.clone()),
                    driver_type.clone(),
                    cx,
                );
            }

            editor
        });

        self.finalize_query_editor_open(query_editor, display_name, editor_id, window, cx)
    }

    pub(super) fn open_sql_file_in_query_editor(
        &mut self,
        path: &Path,
        content: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let display_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("Query.sql")
            .to_string();

        self.open_query_editor_with_content(
            display_name,
            content,
            Some(path.to_string_lossy().into_owned()),
            self.active_connection_id(cx),
            window,
            cx,
        );
    }

    pub(super) fn create_new_query_editor(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<QueryEditor> {
        tracing::info!("Creating new query tab");
        self.query_counter += 1;
        let query_name = format!("Query {}", self.query_counter);

        let active_conn_id = self.active_connection_id(cx);
        tracing::debug!(
            query_name = %query_name,
            connection_id = ?active_conn_id,
            "Creating query editor"
        );

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

        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .expect("AppState not initialized");

        let editor_id = self.create_workspace_editor(active_conn_id, query_name.clone(), cx);

        let query_editor = cx.new(|cx| {
            let mut editor =
                QueryEditor::new(query_name.clone(), active_conn_id, schema_service, window, cx);

            if let Some(ref conn) = connection {
                tracing::debug!(connection_id = ?active_conn_id, driver_type = ?driver_type, "Setting connection on new QueryEditor");
                editor.set_connection(
                    active_conn_id,
                    connection_name.clone(),
                    Some(conn.clone()),
                    driver_type,
                    cx,
                );
            } else {
                tracing::warn!("No connection available for new QueryEditor");
            }

            editor
        });

        self.finalize_query_editor_open(query_editor, query_name, editor_id, window, cx)
    }

    /// Handle a save query request from a dock-based QueryEditor.
    ///
    /// Takes a WeakEntity<QueryEditor> directly.
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
        let _ = self.create_new_query_editor(window, cx);
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
                    QueryEditorEvent::ExecuteQuery {
                        sql,
                        connection_id,
                        params,
                    } => {
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
                        let execution_params = params.clone();

                        // Track query in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade()
                            && let Some(handle) = cancel_handle
                        {
                            state.update(cx, |state, cx| {
                                state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                            });
                        }

                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();

                        cx.spawn_in(window, async move |this, cx| {
                            tracing::debug!(sql = %sql, "Executing query");

                            // ✅ Use QueryService - all logic encapsulated
                            let service_execution = match execution_params.as_ref() {
                                Some(QueryExecutionParams::Positional(params)) => {
                                    query_service
                                        .execute_query_with_positional_params(
                                            conn,
                                            conn_id,
                                            &sql,
                                            params,
                                        )
                                        .await
                                }
                                Some(QueryExecutionParams::Named(params)) => {
                                    query_service
                                        .execute_query_with_named_params(
                                            conn,
                                            conn_id,
                                            &sql,
                                            params,
                                        )
                                        .await
                                }
                                None => query_service.execute_query(conn, conn_id, &sql).await,
                            };
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
                                state.update(cx, |state, cx| {
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

                            _ = this.update(cx, |view, cx| {
                                view.refresh_query_history(cx);
                            });

                            anyhow::Ok(())
                        }).detach();
                    }
                    QueryEditorEvent::ExecuteSelection {
                        sql,
                        connection_id,
                        params,
                    } => {
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
                        let execution_params = params.clone();

                        // Track query in WorkspaceState (after extracting values from app_state)
                        if let Some(state) = workspace_state.upgrade()
                            && let Some(handle) = cancel_handle
                        {
                            state.update(cx, |state, cx| {
                                state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                            });
                        }

                        let results_panel = results_panel.clone();
                        let editor_weak = editor.downgrade();
                        let workspace_state_weak = workspace_state.clone();

                        cx.spawn_in(window, async move |this, cx| {
                            tracing::debug!(sql = %sql, "Executing selection");

                            let service_execution = match execution_params.as_ref() {
                                Some(QueryExecutionParams::Positional(params)) => {
                                    query_service
                                        .execute_query_with_positional_params(
                                            conn,
                                            conn_id,
                                            &sql,
                                            params,
                                        )
                                        .await
                                }
                                Some(QueryExecutionParams::Named(params)) => {
                                    query_service
                                        .execute_query_with_named_params(
                                            conn,
                                            conn_id,
                                            &sql,
                                            params,
                                        )
                                        .await
                                }
                                None => query_service.execute_query(conn, conn_id, &sql).await,
                            };
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
                                state.update(cx, |state, cx| {
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

                            _ = this.update(cx, |view, cx| {
                                view.refresh_query_history(cx);
                            });

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
                        if let Some(state) = workspace_state.upgrade()
                            && let Some(handle) = cancel_handle
                        {
                            state.update(cx, |state, cx| {
                                state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                            });
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
                                state.update(cx, |state, cx| {
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
                        if let Some(state) = workspace_state.upgrade()
                            && let Some(handle) = cancel_handle
                        {
                            state.update(cx, |state, cx| {
                                state.start_query(editor_id, sql.clone(), conn_id, handle, cx);
                            });
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
                                state.update(cx, |state, cx| {
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
                    QueryEditorEvent::DocumentStateChanged => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };

                        let document_context = editor.read(cx).document_context(cx);
                        let is_dirty = editor.read(cx).is_dirty(cx);
                        let display_name = editor.read(cx).name();
                        _this.refresh_workspace_document_state(
                            editor_id,
                            document_context,
                            is_dirty,
                            display_name,
                            cx,
                        );
                    }
                    QueryEditorEvent::SwitchConnection { connection_id } => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };

                        if editor.read(cx).connection_id() == Some(*connection_id) {
                            return;
                        }

                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available for connection switching");
                            return;
                        };

                        let Some(saved) = app_state
                            .saved_connections()
                            .into_iter()
                            .find(|saved| saved.id == *connection_id)
                        else {
                            use zqlz_ui::widgets::{WindowExt, notification::Notification};
                            window.push_notification(
                                Notification::warning("Selected connection was not found"),
                                cx,
                            );
                            return;
                        };

                        let Some(connection) = app_state.connections.get(*connection_id) else {
                            use zqlz_ui::widgets::{WindowExt, notification::Notification};
                            window.push_notification(
                                Notification::warning(
                                    "Connection is not active. Connect it from the sidebar first.",
                                ),
                                cx,
                            );
                            return;
                        };

                        let connection_name = saved.name.clone();
                        let driver_type = saved.driver.clone();
                        let default_database = saved
                            .params
                            .get("database")
                            .or_else(|| saved.params.get("path"))
                            .cloned();

                        editor.update(cx, |editor, cx| {
                            editor.set_connection(
                                Some(*connection_id),
                                Some(connection_name),
                                Some(connection),
                                Some(driver_type),
                                cx,
                            );
                            editor.set_current_database(default_database.clone(), cx);
                        });

                        _this.workspace_state.update(cx, |state, cx| {
                            state.update_editor(
                                editor_id,
                                |editor_state| {
                                    editor_state.connection_id = Some(*connection_id);
                                },
                                cx,
                            );
                        });

                        if let Some(database_name) = default_database {
                            _this.load_database_schema(*connection_id, database_name, window, cx);
                        }

                        tracing::info!(
                            "Switched connection for editor {} to {}",
                            editor_id.0,
                            connection_id
                        );
                        _this.configure_query_editor_switchers(&editor, window, cx);
                    }
                    QueryEditorEvent::SwitchDatabase { database_name } => {
                        let Some(editor) = query_editor_weak.upgrade() else {
                            return;
                        };

                        let connection_id = editor.read(cx).connection_id();
                        let Some(connection_id) = connection_id else {
                            tracing::warn!(
                                "Switch database ignored for editor {} without active connection",
                                editor_id.0
                            );
                            return;
                        };

                        editor.update(cx, |editor, cx| {
                            editor.set_current_database(Some(database_name.clone()), cx);
                        });

                        tracing::info!(
                            "Switch database requested for editor {} to database {}",
                            editor_id.0,
                            database_name
                        );
                        _this.load_database_schema(connection_id, database_name.clone(), window, cx);
                    }
                }
            }
        })
    }
}
