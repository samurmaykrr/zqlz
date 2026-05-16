use gpui::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

use crate::app::AppState;
use crate::components::{QueryEditor, QueryEditorEvent, ResultsPanel};
use crate::workspace_state::{
    DiagnosticSeverity, EditorDiagnostic, EditorId, QueryCancellationOutcome,
    QueryCompletionOutcome, RefreshScope, WorkspaceState,
};
use zqlz_core::{Connection, QueryCancelHandle};
use zqlz_query::{
    DestructiveOperationWarning, DiagnosticInfo, DiagnosticInfoSeverity, QueryConnectionCandidate,
    QueryConnectionSwitchPlanningOutcome, QueryDisplayContext, QueryService, QuerySqlSource,
    QueryWorkflowDispatch, QueryWorkflowRequest, build_query_editor_switcher_selection,
    plan_query_connection_switch, plan_query_database_selection,
    resolve_query_connection_selection, resolve_query_editor_open_connection,
    run_execute_query_workflow, run_explain_query_workflow,
};
use zqlz_text_editor::{DocumentIdentity, TextDocument};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt as _, button::ButtonVariant, dialog::DialogButtonProps,
    scroll::ScrollableElement, v_flex,
};

use super::MainView;

pub(super) struct QueryEditorContentOpenRequest {
    pub editor_id: EditorId,
    pub display_name: String,
    pub content: String,
    pub file_path: Option<String>,
    pub connection_id: Option<Uuid>,
}

fn normalize_query_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

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

struct NoopQueryCancelHandle;

impl QueryCancelHandle for NoopQueryCancelHandle {
    fn cancel(&self) {}
}

#[derive(Clone)]
struct QueryWorkflowResources {
    query_service: Arc<QueryService>,
    connection_id: Uuid,
    connection: Arc<dyn Connection>,
    cancel_handle: Option<Arc<dyn QueryCancelHandle>>,
    display_context: QueryDisplayContext,
}

#[derive(Clone)]
struct QueryWorkflowSpawnContext {
    workspace_state: WeakEntity<WorkspaceState>,
    editor: WeakEntity<QueryEditor>,
    editor_id: EditorId,
    results_panel: Entity<ResultsPanel>,
    refresh_history: bool,
}

struct PreparedQueryWorkflowEvent {
    editor: Entity<QueryEditor>,
    resources: QueryWorkflowResources,
}

impl MainView {
    fn find_open_query_editor_by_path(&self, path: &Path, cx: &App) -> Option<Entity<QueryEditor>> {
        let target_path = normalize_query_path(path);
        self.query_editors.iter().find_map(|query_editor| {
            let query_editor = query_editor.upgrade()?;
            let document_identity = query_editor.read(cx).document_identity(cx);
            let open_path = document_identity.path()?;
            (normalize_query_path(open_path) == target_path).then_some(query_editor)
        })
    }

    fn query_workflow_feature_block_reason(
        workspace_state: &WeakEntity<WorkspaceState>,
        resources: &QueryWorkflowResources,
        request: &QueryWorkflowRequest,
        cx: &App,
    ) -> Option<String> {
        let feature_set = workspace_state.upgrade()?.read_with(cx, |state, _cx| {
            state
                .connection_feature_set(
                    resources.connection_id,
                    resources.display_context.database_name.as_deref(),
                )
                .cloned()
        })?;

        match request {
            QueryWorkflowRequest::Execute { .. } => {
                (!feature_set.query.execute.available).then(|| {
                    feature_set
                        .query
                        .execute
                        .reason_or("Query execution is not available")
                })
            }
            QueryWorkflowRequest::Explain { .. } => {
                (!feature_set.query.explain.available).then(|| {
                    feature_set
                        .query
                        .explain
                        .reason_or("EXPLAIN is not available")
                })
            }
        }
    }

    /// Cancels the active query for a known editor id while preserving
    /// workspace-owned cancellation lifecycle routing.
    pub(super) fn cancel_query_for_editor(
        &self,
        editor: Option<Entity<QueryEditor>>,
        editor_id: EditorId,
        cx: &mut Context<Self>,
    ) -> Option<QueryCancellationOutcome> {
        let workspace_state = self.workspace_state.downgrade();
        if let Some(state) = workspace_state.upgrade() {
            let cancellation_outcome =
                state.update(cx, |state, cx| state.cancel_query(editor_id, cx));

            if cancellation_outcome == QueryCancellationOutcome::CancelledActiveExecution {
                if let Some(editor) = editor {
                    editor.update(cx, |editor, cx| {
                        editor.set_executing(false, cx);
                    });
                } else {
                    tracing::debug!(
                        editor_id = ?editor_id,
                        "Skipped query editor executing-state reset because editor entity was unavailable"
                    );
                }
            } else {
                tracing::debug!(
                    editor_id = ?editor_id,
                    cancellation_outcome = ?cancellation_outcome,
                    "Skipped query editor executing-state reset because no active execution was cancelled"
                );
            }

            Some(cancellation_outcome)
        } else {
            if let Some(editor) = editor {
                tracing::warn!(
                    editor_id = ?editor_id,
                    "Workspace state unavailable during query cancellation; clearing editor executing state locally"
                );

                // When workspace state is unavailable, there is no canonical
                // running-query source to resolve cancellation ownership. Clear the
                // editor's local executing flag so the UI cannot remain stuck in a
                // spinner state after an explicit user cancel action.
                editor.update(cx, |editor, cx| {
                    editor.set_executing(false, cx);
                });
            } else {
                tracing::warn!(
                    editor_id = ?editor_id,
                    "Skipped cancellation fallback executing-state reset because both workspace state and query editor were unavailable"
                );
            }

            None
        }
    }

    fn reset_query_editor_executing_state(
        editor: &WeakEntity<QueryEditor>,
        cx: &mut AsyncWindowContext,
    ) {
        if let Err(error) = editor.update(cx, |editor, cx| {
            editor.set_executing(false, cx);
        }) {
            tracing::warn!(%error, "failed to reset query editor executing state");
        }
    }

    fn query_connection_candidates(app_state: &AppState) -> Vec<QueryConnectionCandidate> {
        app_state
            .connection_service
            .list_saved_connections()
            .into_iter()
            .map(|saved| QueryConnectionCandidate {
                connection_id: saved.id,
                connection_name: saved.name,
                driver_name: saved.driver,
                params: saved.params,
            })
            .collect()
    }

    fn active_connection_ids(app_state: &AppState) -> Vec<Uuid> {
        let active_connection_ids: HashSet<Uuid> = app_state
            .connection_service
            .list_active_connections()
            .into_iter()
            .collect();

        app_state
            .connection_service
            .list_saved_connections()
            .into_iter()
            .filter(|saved| active_connection_ids.contains(&saved.id))
            .map(|saved| saved.id)
            .collect()
    }

    pub(super) fn resolve_editor_open_connection_metadata(
        selected_connection_id: Option<Uuid>,
        app_state: &AppState,
    ) -> Option<(Arc<dyn Connection>, String, String)> {
        let candidates = Self::query_connection_candidates(app_state);
        let active_connection_ids = Self::active_connection_ids(app_state);
        let selection = resolve_query_editor_open_connection(
            selected_connection_id,
            &candidates,
            &active_connection_ids,
        )?;
        let connection = app_state
            .connection_service
            .get_connection(selection.connection_id)?;

        Some((connection, selection.driver_name, selection.connection_name))
    }

    fn spawn_query_workflow_from_event(
        &self,
        request: QueryWorkflowRequest,
        resources: QueryWorkflowResources,
        spawn_context: QueryWorkflowSpawnContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let QueryWorkflowSpawnContext {
            workspace_state,
            editor,
            editor_id,
            results_panel,
            refresh_history,
        } = spawn_context;

        let tracking_sql = match &request {
            QueryWorkflowRequest::Execute { sql, .. } => sql.clone(),
            QueryWorkflowRequest::Explain { sql } => sql.clone(),
        };
        let request_kind = match &request {
            QueryWorkflowRequest::Execute { .. } => "execute",
            QueryWorkflowRequest::Explain { .. } => "explain",
        };
        let executed_sql = match &request {
            QueryWorkflowRequest::Execute { sql, .. } => Some(sql.clone()),
            QueryWorkflowRequest::Explain { .. } => None,
        };

        let mut query_execution_id = None;
        if let Some(state) = workspace_state.upgrade() {
            let cancel_handle = resources
                .cancel_handle
                .clone()
                .unwrap_or_else(|| Arc::new(NoopQueryCancelHandle));
            query_execution_id = Some(state.update(cx, |state, cx| {
                state.start_query(
                    editor_id,
                    tracking_sql.clone(),
                    resources.connection_id,
                    cancel_handle,
                    cx,
                )
            }));
        }
        results_panel.update(cx, |panel, cx| {
            panel.set_loading(true, cx);
        });

        cx.spawn_in(window, async move |this, cx| {
            tracing::debug!(sql = %tracking_sql, request_kind, "running query workflow");
            let success = match request {
                QueryWorkflowRequest::Execute {
                    sql,
                    params,
                    source,
                    destructive_warning,
                } => {
                    if let Some(warning) = destructive_warning.as_ref() {
                        tracing::warn!(
                            operation = warning.operation_type.display_name(),
                            affected_object = %warning.affected_object,
                            source = ?source,
                            reason = %warning.reason,
                            "destructive query warning detected before execution"
                        );
                    }
                    let query_outcome = run_execute_query_workflow(
                        resources.query_service.as_ref(),
                        resources.connection,
                        resources.connection_id,
                        sql,
                        params,
                        resources.display_context,
                        chrono::Utc::now(),
                    )
                    .await;

                    let query_success = query_outcome.success;
                    if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                        panel.set_execution(query_outcome.execution, window, cx);
                    }) {
                        tracing::warn!(%error, "failed to update results panel after query execution");
                    }

                    if query_success
                        && let Some(sql) = executed_sql.as_deref()
                        && let Err(error) = editor.update(cx, |editor, cx| {
                            editor.notify_query_executed(sql, cx);
                        })
                    {
                        tracing::warn!(%error, "failed to notify query editor about executed query");
                    }

                    query_success
                }
                QueryWorkflowRequest::Explain { sql } => {
                    let explain_outcome = run_explain_query_workflow(
                        resources.query_service.as_ref(),
                        resources.connection,
                        resources.connection_id,
                        sql,
                        resources.display_context,
                        chrono::Utc::now(),
                    )
                    .await;

                    let success = explain_outcome.success;
                    if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                        panel.add_explain_result(explain_outcome.explain_result, window, cx);
                    }) {
                        tracing::warn!(%error, "failed to update results panel after explain execution");
                    }

                    success
                }
            };

            if let Some(execution_id) = query_execution_id {
                if let Some(state) = workspace_state.upgrade() {
                    let completion_outcome = state.update(cx, |state, cx| {
                        state.complete_query(editor_id, execution_id, success, cx)
                    });

                    if completion_outcome == QueryCompletionOutcome::CompletedActiveExecution {
                        Self::reset_query_editor_executing_state(&editor, cx);
                    } else {
                        tracing::debug!(
                            editor_id = ?editor_id,
                            completion_outcome = ?completion_outcome,
                            "Skipped query editor executing-state reset because completion did not own active execution"
                        );
                    }
                } else {
                    tracing::warn!(
                        editor_id = ?editor_id,
                        "Workspace state unavailable during query completion; clearing editor executing state locally"
                    );
                    Self::reset_query_editor_executing_state(&editor, cx);
                }
            } else if let Some(state) = workspace_state.upgrade() {
                if state.read_with(cx, |state, _| state.is_query_running(editor_id)) {
                    tracing::debug!(
                        editor_id = ?editor_id,
                        "Skipped untracked query-completion executing-state reset because a tracked execution is currently running"
                    );
                } else {
                    tracing::warn!(
                        editor_id = ?editor_id,
                        "Query completion lacked tracked execution id; clearing editor executing state locally"
                    );
                    Self::reset_query_editor_executing_state(&editor, cx);
                }
            } else {
                tracing::warn!(
                    editor_id = ?editor_id,
                    "Workspace state unavailable during query completion; clearing editor executing state locally"
                );
                Self::reset_query_editor_executing_state(&editor, cx);
            }

            if refresh_history
                && let Err(error) = this.update(cx, |view, cx| {
                    view.refresh_query_history(cx);
                })
            {
                tracing::warn!(%error, "failed to refresh query history after execution");
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn resolve_query_workflow_resources(
        preferred_connection_id: Option<Uuid>,
        selected_database_name: Option<String>,
        app_state: &AppState,
    ) -> Option<QueryWorkflowResources> {
        let candidates = Self::query_connection_candidates(app_state);
        let selection = resolve_query_connection_selection(preferred_connection_id, &candidates)?;
        let connection_id = selection.connection_id;
        let has_selected_database = selected_database_name.is_some();
        let display_database_name = selected_database_name.or(selection.default_database_name);

        let connection = match display_database_name.as_deref() {
            Some(database_name) if has_selected_database => app_state
                .connection_service
                .get_connection_for_database_cached(connection_id, Some(database_name))?,
            database_name => app_state
                .connection_service
                .get_connection_for_database_cached(connection_id, database_name)
                .or_else(|| app_state.connection_service.get_connection(connection_id))?,
        };
        let cancel_handle = connection.cancel_handle();

        Some(QueryWorkflowResources {
            query_service: app_state.query_service.clone(),
            connection_id,
            connection,
            cancel_handle,
            display_context: QueryDisplayContext {
                connection_name: Some(selection.connection_name),
                database_name: display_database_name,
            },
        })
    }

    fn prepare_query_workflow_event(
        query_editor_weak: &WeakEntity<QueryEditor>,
        preferred_connection_id: Option<Uuid>,
        selected_database_name: Option<String>,
        query_name: &str,
        start_log_message: &'static str,
        no_connection_log_message: &'static str,
        cx: &mut Context<Self>,
    ) -> Option<PreparedQueryWorkflowEvent> {
        let editor = query_editor_weak.upgrade()?;

        tracing::info!(query_name = %query_name, "{}", start_log_message);

        editor.update(cx, |editor, cx| {
            editor.set_executing(true, cx);
        });

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            editor.update(cx, |editor, cx| {
                editor.set_executing(false, cx);
            });
            return None;
        };

        let Some(resources) = Self::resolve_query_workflow_resources(
            preferred_connection_id,
            selected_database_name,
            app_state,
        ) else {
            tracing::warn!("{}", no_connection_log_message);
            editor.update(cx, |editor, cx| {
                editor.set_executing(false, cx);
            });
            return None;
        };

        Some(PreparedQueryWorkflowEvent { editor, resources })
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_query_workflow_editor_event(
        &self,
        query_editor_weak: &WeakEntity<QueryEditor>,
        query_name: &str,
        dispatch: QueryWorkflowDispatch,
        workspace_state: &WeakEntity<WorkspaceState>,
        editor_id: EditorId,
        results_panel: &Entity<ResultsPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(prepared_event) = Self::prepare_query_workflow_event(
            query_editor_weak,
            dispatch.preferred_connection_id,
            dispatch.selected_database_name.clone(),
            query_name,
            dispatch.start_log_message,
            dispatch.no_connection_log_message,
            cx,
        ) else {
            return;
        };

        if let Some(block_reason) = Self::query_workflow_feature_block_reason(
            workspace_state,
            &prepared_event.resources,
            &dispatch.request,
            cx,
        ) {
            tracing::warn!(
                query_name = %query_name,
                reason = %block_reason,
                "Blocked query workflow because connection feature set marks it unavailable"
            );
            prepared_event.editor.update(cx, |editor, cx| {
                editor.set_executing(false, cx);
            });
            return;
        }

        let editor = prepared_event.editor.downgrade();
        let spawn_context = QueryWorkflowSpawnContext {
            workspace_state: workspace_state.clone(),
            editor,
            editor_id,
            results_panel: results_panel.clone(),
            refresh_history: dispatch.refresh_history,
        };

        if let Some(warning) = Self::destructive_warning_for_request(&dispatch.request).cloned() {
            prepared_event.editor.update(cx, |editor, cx| {
                editor.set_executing(false, cx);
            });
            self.confirm_destructive_query_and_spawn(
                warning,
                dispatch.request,
                prepared_event.resources,
                spawn_context,
                window,
                cx,
            );
            return;
        }

        self.spawn_query_workflow_from_event(
            dispatch.request,
            prepared_event.resources,
            spawn_context,
            window,
            cx,
        );
    }

    fn connected_connection_options(cx: &App) -> Vec<(Uuid, String)> {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return Vec::new();
        };

        let candidates = Self::query_connection_candidates(app_state);
        let active_connection_ids = Self::active_connection_ids(app_state);

        build_query_editor_switcher_selection(None, &candidates, &active_connection_ids)
            .available_connections
            .into_iter()
            .map(|option| (option.connection_id, option.connection_name))
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

            let candidates = Self::query_connection_candidates(app_state);
            let active_connection_ids = Self::active_connection_ids(app_state);

            let switcher_selection = build_query_editor_switcher_selection(
                Some(connection_id),
                &candidates,
                &active_connection_ids,
            );
            let connection = app_state.connection_service.get_connection(connection_id);
            (
                switcher_selection.selected_default_database_name,
                connection,
            )
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

            if let Err(error) = query_editor_weak.update(cx, |editor, cx| {
                editor.set_available_databases(databases, cx);
            }) {
                tracing::warn!(%error, %connection_id, "failed to update database switcher options");
            }
        })
        .detach();
    }

    pub(super) fn sync_active_query_editor_database_selection(
        &mut self,
        database_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(selected_database_name) = database_name else {
            return;
        };
        let Some(editor) = self.active_query_editor(cx) else {
            return;
        };
        let Some(connection_id) = editor.read(cx).connection_id() else {
            return;
        };
        if self.workspace_state.read(cx).active_connection_id() != Some(connection_id) {
            return;
        }
        if editor.read(cx).current_database().as_deref() == Some(selected_database_name.as_str()) {
            return;
        }

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available for active query editor database sync");
            return;
        };
        let Some(connection_metadata) = Self::query_connection_candidates(app_state)
            .into_iter()
            .find(|candidate| candidate.connection_id == connection_id)
        else {
            tracing::warn!(
                connection_id = %connection_id,
                "Active query editor database sync skipped because saved connection metadata was not found"
            );
            return;
        };

        let connection_service = app_state.connection_service.clone();
        editor.update(cx, |editor, cx| {
            editor.begin_current_database_switch(Some(selected_database_name.clone()), cx);
        });

        let editor_weak = editor.downgrade();
        let connection_name = connection_metadata.connection_name;
        let driver_type = connection_metadata.driver_name;
        cx.spawn_in(window, async move |_this, cx| {
            match connection_service
                .resolve_database_scoped_connection(
                    connection_id,
                    Some(selected_database_name.clone()),
                )
                .await
            {
                Ok(resolved_connection) => {
                    if let Err(error) = editor_weak.update(cx, |editor, cx| {
                        if editor.connection_id() != Some(connection_id)
                            || editor.current_database().as_deref()
                                != Some(selected_database_name.as_str())
                        {
                            return;
                        }

                        editor.set_connection(
                            Some(connection_id),
                            Some(connection_name.clone()),
                            Some(resolved_connection.connection.clone()),
                            Some(driver_type.clone()),
                            cx,
                        );
                    }) {
                        tracing::warn!(
                            %error,
                            connection_id = %connection_id,
                            database = %selected_database_name,
                            "failed to sync active query editor database connection"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        connection_id = %connection_id,
                        database = %selected_database_name,
                        "failed to resolve database-scoped connection for active query editor sync"
                    );
                    if let Err(update_error) = editor_weak.update(cx, |editor, cx| {
                        if editor.connection_id() == Some(connection_id)
                            && editor.current_database().as_deref()
                                == Some(selected_database_name.as_str())
                        {
                            editor.set_schema_loading(false, cx);
                        }
                    }) {
                        tracing::warn!(
                            %update_error,
                            connection_id = %connection_id,
                            database = %selected_database_name,
                            "failed to clear active query editor schema loading state"
                        );
                    }
                }
            }
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
        self.adopt_query_editor(query_editor.clone(), display_name, editor_id, window, cx);

        let query_editor_panel: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(query_editor.clone());
        let replaced_preview = self.workspace_controller.update(cx, |workspace, cx| {
            workspace.replace_active_preview_tab(query_editor_panel.clone(), window, cx)
        });
        if !replaced_preview {
            self.workspace_controller.update(cx, |workspace, cx| {
                workspace.add_center_item(query_editor_panel, window, cx);
            });
        }

        self.configure_query_editor_switchers(&query_editor, window, cx);

        let focus_handle = query_editor.read(cx).editor_focus_handle(cx);
        window.focus(&focus_handle, cx);

        query_editor
    }

    pub(super) fn adopt_moved_query_editor(
        &mut self,
        query_editor: Entity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let display_name = query_editor.read(cx).name();
        let connection_id = query_editor.read(cx).connection_id();
        let editor_id = self.create_workspace_editor(connection_id, display_name.clone(), cx);
        self.adopt_query_editor(query_editor, display_name, editor_id, window, cx);
    }

    pub(super) fn detach_query_editor_subscription(&mut self, query_editor: &Entity<QueryEditor>) {
        let entity_id = query_editor.entity_id();
        self.query_editor_subscriptions.remove(&entity_id);
        self.query_editors.retain(|weak_editor| {
            weak_editor
                .upgrade()
                .is_some_and(|editor| editor.entity_id() != entity_id)
        });
    }

    fn adopt_query_editor(
        &mut self,
        query_editor: Entity<QueryEditor>,
        display_name: String,
        editor_id: EditorId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let subscription =
            self.subscribe_query_editor(&query_editor, display_name, editor_id, window, cx);
        let document_context = query_editor.read(cx).document_context(cx);
        let is_dirty = query_editor.read(cx).is_dirty(cx);
        let display_name = query_editor.read(cx).name();
        let draft_text = query_editor.read(cx).content(cx).to_string();
        self.refresh_workspace_document_state(
            editor_id,
            document_context,
            is_dirty,
            display_name,
            Some(draft_text),
            cx,
        );

        self.query_editor_subscriptions
            .insert(query_editor.entity_id(), subscription);
        self.query_editors.push(query_editor.downgrade());
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
        draft_text: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.workspace_state.update(cx, |state, cx| {
            state.update_editor_document(
                editor_id,
                document_context,
                is_dirty,
                display_name,
                draft_text,
                cx,
            );
        });
    }

    fn destructive_warning_for_request(
        request: &QueryWorkflowRequest,
    ) -> Option<&DestructiveOperationWarning> {
        match request {
            QueryWorkflowRequest::Execute {
                destructive_warning,
                ..
            } => destructive_warning.as_ref(),
            QueryWorkflowRequest::Explain { .. } => None,
        }
    }

    fn confirm_destructive_query_and_spawn(
        &self,
        warning: DestructiveOperationWarning,
        request: QueryWorkflowRequest,
        resources: QueryWorkflowResources,
        spawn_context: QueryWorkflowSpawnContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let main_view = cx.entity().downgrade();
        let operation_name = warning.operation_type.display_name();
        let affected_object = warning.affected_object.clone();
        let reason = warning.reason.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let main_view = main_view.clone();
            let request = request.clone();
            let resources = resources.clone();
            let spawn_context = spawn_context.clone();

            dialog
                .title("Confirm Destructive Query")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "{} will affect '{}'.",
                            operation_name, affected_object
                        )))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child(reason.clone()),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().danger_text)
                                .child("This action cannot be undone."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Run Query")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, window, cx| {
                    let request = request.clone();
                    let resources = resources.clone();
                    let spawn_context = spawn_context.clone();
                    if let Err(error) = main_view.update(cx, |main_view, cx| {
                        main_view.spawn_query_workflow_from_event(
                            request,
                            resources,
                            spawn_context,
                            window,
                            cx,
                        );
                    }) {
                        tracing::warn!(%error, "failed to run destructive query after confirmation");
                    }
                    true
            })
            .confirm()
        });
    }

    pub(super) fn show_query_editor_ddl_preview(
        &self,
        object_type: &zqlz_query::EditorObjectType,
        definition: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let title = format!("{} DDL Preview", object_type.display_name());
        let definition = definition.trim().to_string();

        window.open_dialog(cx, move |dialog, _window, cx| {
            dialog
                .title(title.clone())
                .child(
                    v_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("Review the generated DDL before saving this object."),
                        )
                        .child(
                            div()
                                .max_h(px(360.0))
                                .overflow_y_scrollbar()
                                .p_2()
                                .rounded_md()
                                .bg(cx.theme().secondary)
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_xs()
                                .child(definition.clone()),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Close")
                        .ok_variant(ButtonVariant::Primary),
                )
                .confirm()
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
        let editor_id = self.create_workspace_editor(connection_id, display_name.clone(), cx);
        self.open_query_editor_with_content_for_editor_id(
            QueryEditorContentOpenRequest {
                editor_id,
                display_name,
                content,
                file_path,
                connection_id,
            },
            window,
            cx,
        )
    }

    pub(super) fn open_query_editor_with_content_for_editor_id(
        &mut self,
        request: QueryEditorContentOpenRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<QueryEditor> {
        let QueryEditorContentOpenRequest {
            editor_id,
            display_name,
            content,
            file_path,
            connection_id,
        } = request;

        let (connection, driver_type, connection_name) = cx
            .try_global::<AppState>()
            .and_then(|state| Self::resolve_editor_open_connection_metadata(connection_id, state))
            .map(|(connection, driver_name, connection_name)| {
                (Some(connection), Some(driver_name), Some(connection_name))
            })
            .unwrap_or((None, None, None));

        let schema_service = cx
            .try_global::<AppState>()
            .map(|state| state.schema_service.clone())
            .expect("AppState not initialized");

        self.workspace_state.update(cx, |state, cx| {
            state.update_editor(
                editor_id,
                |editor_state| {
                    editor_state.file_path = file_path.clone();
                    editor_state.display_name = display_name.clone();
                    editor_state.is_dirty = false;
                    editor_state.draft_text = Some(content.clone());
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
        self.prune_closed_query_editors();

        if let Some(editor) = self.find_open_query_editor_by_path(path, cx) {
            self.activate_existing_query_editor(&editor, window, cx);
            return;
        }

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

        let (connection, driver_type, connection_name) = cx
            .try_global::<AppState>()
            .and_then(|state| Self::resolve_editor_open_connection_metadata(active_conn_id, state))
            .map(|(connection, driver_name, connection_name)| {
                (Some(connection), Some(driver_name), Some(connection_name))
            })
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
        self.create_new_query_editor(window, cx);
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
                        database_name,
                        params,
                    } => {
                        let confirm_destructive = cx
                            .try_global::<AppState>()
                            .map(|app_state| app_state.settings.read().confirm_destructive)
                            .unwrap_or(true);
                        let start_log_message = "Executing query from editor";
                        let dispatch = QueryWorkflowDispatch::execute(
                            *connection_id,
                            database_name.clone(),
                            sql.clone(),
                            params.clone(),
                            QuerySqlSource::FullBuffer,
                            confirm_destructive,
                            start_log_message,
                        );

                        _this.dispatch_query_workflow_editor_event(
                            &query_editor_weak,
                            &query_name,
                            dispatch,
                            &workspace_state,
                            editor_id,
                            &results_panel,
                            window,
                            cx,
                        );
                    }
                    QueryEditorEvent::ExecuteSelection {
                        sql,
                        connection_id,
                        database_name,
                        params,
                    } => {
                        let confirm_destructive = cx
                            .try_global::<AppState>()
                            .map(|app_state| app_state.settings.read().confirm_destructive)
                            .unwrap_or(true);
                        let start_log_message = "Executing selection from editor";
                        let dispatch = QueryWorkflowDispatch::execute(
                            *connection_id,
                            database_name.clone(),
                            sql.clone(),
                            params.clone(),
                            QuerySqlSource::CurrentStatement,
                            confirm_destructive,
                            start_log_message,
                        );

                        _this.dispatch_query_workflow_editor_event(
                            &query_editor_weak,
                            &query_name,
                            dispatch,
                            &workspace_state,
                            editor_id,
                            &results_panel,
                            window,
                            cx,
                        );
                    }
                    QueryEditorEvent::ExplainQuery {
                        sql,
                        connection_id,
                        database_name,
                    }
                    | QueryEditorEvent::ExplainSelection {
                        sql,
                        connection_id,
                        database_name,
                    } => {
                        let start_log_message = if matches!(event, QueryEditorEvent::ExplainQuery { .. }) {
                            "Explaining query from editor"
                        } else {
                            "Explaining selection from editor"
                        };
                        let dispatch = QueryWorkflowDispatch::explain(
                            *connection_id,
                            database_name.clone(),
                            sql.clone(),
                            start_log_message,
                        );

                        _this.dispatch_query_workflow_editor_event(
                            &query_editor_weak,
                            &query_name,
                            dispatch,
                            &workspace_state,
                            editor_id,
                            &results_panel,
                            window,
                            cx,
                        );
                    }
                    QueryEditorEvent::CancelQuery => {
                        tracing::info!(query_name = %query_name, "Query cancelled by user");

                        let editor = query_editor_weak.upgrade();
                        _this.cancel_query_for_editor(editor, editor_id, cx);

                        // Query execution can still complete after cancellation,
                        // but WorkspaceState now owns cancellation lifecycle state.
                    }
                    QueryEditorEvent::SaveObject { .. } => {
                        // View/procedure/function editors use a separate subscription
                        // in view_handlers.rs - this is for regular query editors only
                        tracing::debug!("SaveObject event received on regular query editor - ignoring");
                    }
                    QueryEditorEvent::PreviewDdl {
                        object_type,
                        definition,
                    } => {
                        _this.show_query_editor_ddl_preview(object_type, definition, window, cx);
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
                        let draft_text = editor.read(cx).content(cx).to_string();
                        _this.refresh_workspace_document_state(
                            editor_id,
                            document_context,
                            is_dirty,
                            display_name,
                            Some(draft_text),
                            cx,
                        );
                        _this.pin_dirty_preview_tab(is_dirty, cx);
                        _this.refresh_workspace_window_title(window, cx);
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

                        let candidates = Self::query_connection_candidates(app_state);
                        let active_connection_ids = Self::active_connection_ids(app_state);

                        let active_database_before = {
                            let workspace_state = _this.workspace_state.read(cx);
                            workspace_state.active_database().map(str::to_owned)
                        };

                        let switch_plan = plan_query_connection_switch(
                            *connection_id,
                            &candidates,
                            &active_connection_ids,
                            active_database_before.as_deref(),
                        );

                        let (connection_name, driver_type, default_database, should_refresh_connection_surfaces) =
                            match switch_plan {
                                QueryConnectionSwitchPlanningOutcome::Ready(plan) => (
                                    plan.selection.connection_name,
                                    plan.selection.driver_name,
                                    plan.selection.default_database_name,
                                    plan.should_refresh_connection_surfaces,
                                ),
                                QueryConnectionSwitchPlanningOutcome::MissingSavedConnection => {
                                    use zqlz_ui::widgets::{WindowExt, notification::Notification};
                                    window.push_notification(
                                        Notification::warning("Selected connection was not found"),
                                        cx,
                                    );
                                    return;
                                }
                                QueryConnectionSwitchPlanningOutcome::InactiveConnection => {
                                    use zqlz_ui::widgets::{WindowExt, notification::Notification};
                                    window.push_notification(
                                        Notification::warning(
                                            "Connection is not active. Connect it from the sidebar first.",
                                        ),
                                        cx,
                                    );
                                    return;
                                }
                            };

                        let Some(connection) = app_state
                            .connection_service
                            .get_connection(*connection_id)
                        else {
                            tracing::warn!(
                                connection_id = %connection_id,
                                "connection marked active by policy resolution but no runtime handle was found"
                            );
                            return;
                        };

                        editor.update(cx, |editor, cx| {
                            editor.set_connection(
                                Some(*connection_id),
                                Some(connection_name.clone()),
                                Some(connection),
                                Some(driver_type.clone()),
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
                            state.set_active_connection(Some(*connection_id), cx);
                            state.set_active_database(default_database.clone(), cx);
                        });

                        if let Some(database_name) = default_database.as_ref() {
                            _this.connection_sidebar.update(cx, |sidebar, cx| {
                                sidebar.set_database_loading(*connection_id, database_name, true, cx);
                            });
                        }

                        if should_refresh_connection_surfaces {
                            _this.request_refresh(RefreshScope::ConnectionSurfaces(*connection_id), cx);
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

                        let active_database_before = {
                            let workspace_state = _this.workspace_state.read(cx);
                            workspace_state.active_database().map(str::to_owned)
                        };
                        let database_selection_plan =
                            plan_query_database_selection(database_name.clone(), active_database_before.as_deref());
                        let selected_database_name = database_selection_plan.database_name;
                        let should_refresh_connection_surfaces =
                            database_selection_plan.should_refresh_connection_surfaces;

                        let Some(app_state) = cx.try_global::<AppState>() else {
                            tracing::error!("No AppState available for database switching");
                            return;
                        };
                        let Some(connection_metadata) = Self::query_connection_candidates(app_state)
                            .into_iter()
                            .find(|candidate| candidate.connection_id == connection_id)
                        else {
                            tracing::warn!(
                                connection_id = %connection_id,
                                "Switch database ignored because saved connection metadata was not found"
                            );
                            return;
                        };
                        let connection_service = app_state.connection_service.clone();

                        editor.update(cx, |editor, cx| {
                            editor
                                .begin_current_database_switch(Some(selected_database_name.clone()), cx);
                        });

                        _this.connection_sidebar.update(cx, |sidebar, cx| {
                            sidebar.set_database_loading(
                                connection_id,
                                selected_database_name.as_str(),
                                true,
                                cx,
                            );
                        });

                        _this.workspace_state.update(cx, |state, cx| {
                            state.set_active_connection(Some(connection_id), cx);
                            state.set_active_database(Some(selected_database_name.clone()), cx);
                        });

                        if should_refresh_connection_surfaces {
                            _this.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
                        }

                        tracing::info!(
                            "Switch database requested for editor {} to database {}",
                            editor_id.0,
                            selected_database_name
                        );

                        let editor_weak = editor.downgrade();
                        let connection_name = connection_metadata.connection_name.clone();
                        let driver_type = connection_metadata.driver_name.clone();
                        cx.spawn_in(window, async move |_this, cx| {
                            let resolved_connection = connection_service
                                .resolve_database_scoped_connection(
                                    connection_id,
                                    Some(selected_database_name.clone()),
                                )
                                .await;

                            match resolved_connection {
                                Ok(resolved_connection) => {
                                    if let Err(error) = editor_weak.update(cx, |editor, cx| {
                                        if editor.connection_id() != Some(connection_id)
                                            || editor.current_database().as_deref()
                                                != Some(selected_database_name.as_str())
                                        {
                                            return;
                                        }

                                        editor.set_connection(
                                            Some(connection_id),
                                            Some(connection_name.clone()),
                                            Some(resolved_connection.connection.clone()),
                                            Some(driver_type.clone()),
                                            cx,
                                        );
                                    }) {
                                        tracing::warn!(
                                            %error,
                                            connection_id = %connection_id,
                                            database = %selected_database_name,
                                            "failed to apply database-scoped connection to query editor"
                                        );
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        %error,
                                        connection_id = %connection_id,
                                        database = %selected_database_name,
                                        "failed to resolve database-scoped connection for query editor"
                                    );
                                    if let Err(update_error) = editor_weak.update(cx, |editor, cx| {
                                        if editor.connection_id() == Some(connection_id)
                                            && editor.current_database().as_deref()
                                                == Some(selected_database_name.as_str())
                                        {
                                            editor.set_schema_loading(false, cx);
                                        }
                                    }) {
                                        tracing::warn!(
                                            %update_error,
                                            connection_id = %connection_id,
                                            database = %selected_database_name,
                                            "failed to clear query editor schema loading state"
                                        );
                                    }
                                }
                            }
                        })
                        .detach();
                    }
                }
            }
        })
    }
}
