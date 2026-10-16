//! MainView side of the MCP agent tools (`crate::agent_tools`).
//!
//! Every agent action goes through the same surfaces a user would touch: SQL
//! lands in a per-connection "Agent" query tab, results fill the results panel,
//! and writes wait in an approval dialog unless the connection allows them.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, LazyLock};
use std::task::Poll;

use futures::channel::oneshot;
use gpui::prelude::FluentBuilder as _;
use gpui::*;
use serde_json::{Value, json};
use uuid::Uuid;
use zqlz_connection::{AgentAccess, AgentAccessPolicy};
use zqlz_core::{Connection, QueryResult, ZqlzError};
use zqlz_query::widgets::QueryExecution;
use zqlz_query::{QueryEngine, run_execute_query_workflow, run_explain_query_workflow_with_mode};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt as _, button::ButtonVariant, dialog::DialogButtonProps,
    notification::Notification, scroll::ScrollableElement, v_flex,
};

use crate::app::AppState;
use crate::components::{QueryEditor, TableViewerPanel};
use crate::ipc_server::{open_database_session, schema_scoped};
use crate::workspace_state::RefreshScope;

use super::MainView;

/// Rows returned to the agent per statement; the user still sees the full grid.
const AGENT_ROW_LIMIT: usize = 100;

pub(crate) type AgentReply = oneshot::Receiver<Result<Value, String>>;

pub(crate) struct AgentSqlRequest {
    pub connection_id: Uuid,
    pub database: Option<String>,
    pub sql: String,
    /// `Some(reason)` for writes; shown to the user when asking for approval.
    pub write_reason: Option<String>,
}

impl MainView {
    pub(crate) fn agent_context(&self, cx: &App) -> Value {
        let workspace_state = self.workspace_state.read(cx);
        let connections: Vec<Value> = self
            .connection_sidebar
            .read(cx)
            .connections()
            .iter()
            .map(|connection| {
                json!({
                    "id": connection.id,
                    "name": connection.name,
                    "driver": connection.db_type,
                    "connected": connection.is_connected,
                    "agent_access": AgentAccessPolicy::get(cx, connection.id).as_str(),
                })
            })
            .collect();

        let workspace = self.workspace_controller.read(cx);
        let active_tab = if let Some(viewer) = workspace.active_center_view::<TableViewerPanel>(cx)
        {
            let viewer = viewer.read(cx);
            json!({
                "kind": "table",
                "connection_id": viewer.connection_id(),
                "table": viewer.table_name(),
                "database": viewer.database_name(),
            })
        } else if let Some(editor) = workspace.active_query_editor(cx) {
            let editor = editor.read(cx);
            json!({
                "kind": "query",
                "name": editor.name(),
                "is_agent_tab": self.is_agent_query_editor(editor.connection_id(), cx),
                "connection_id": editor.connection_id(),
                "database": editor.current_database(),
                "sql": editor.content(cx).to_string(),
                "selected_sql": editor.editor().read(cx).get_selected_text(cx).map(|text| text.to_string()),
            })
        } else {
            Value::Null
        };

        json!({
            "active_connection_id": workspace_state.active_connection_id(),
            "active_database": workspace_state.active_database(),
            "connections": connections,
            "active_tab": active_tab,
        })
    }

    pub(crate) fn agent_connect(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.connect_to_database(connection_id, window, cx);
    }

    /// `(connected, connecting)` as the sidebar shows it.
    pub(crate) fn agent_connection_state(&self, connection_id: Uuid, cx: &App) -> (bool, bool) {
        self.connection_sidebar
            .read(cx)
            .connections()
            .iter()
            .find(|connection| connection.id == connection_id)
            .map(|connection| (connection.is_connected, connection.is_connecting))
            .unwrap_or((false, false))
    }

    pub(crate) fn agent_run_sql(
        &mut self,
        request: AgentSqlRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AgentReply {
        let (reply, receiver) = oneshot::channel();
        let AgentSqlRequest {
            connection_id,
            database,
            sql,
            write_reason,
        } = request;

        let Some(reason) = write_reason else {
            self.agent_execute_sql(connection_id, database, sql, false, reply, window, cx);
            return receiver;
        };

        let connection_name = self.agent_connection_name(connection_id, cx);
        match AgentAccessPolicy::get(cx, connection_id) {
            AgentAccess::ReadOnly => send_reply(
                reply,
                Err(format!(
                    "'{connection_name}' is read-only for agents. The user can change this by \
                     right-clicking the connection in the ZQLZ sidebar → Agent Access."
                )),
            ),
            AgentAccess::Auto => {
                self.agent_execute_sql(connection_id, database, sql, true, reply, window, cx);
            }
            AgentAccess::Ask => {
                self.agent_query_editor(connection_id, &sql, window, cx);
                self.agent_request_approval(
                    connection_id,
                    connection_name,
                    database,
                    sql,
                    reason,
                    reply,
                    window,
                    cx,
                );
            }
        }
        receiver
    }

    pub(crate) fn agent_explain(
        &mut self,
        connection_id: Uuid,
        database: Option<String>,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AgentReply {
        let (reply, receiver) = oneshot::channel();
        let Some(resources) = cx.try_global::<AppState>().and_then(|app_state| {
            Self::resolve_query_workflow_resources(Some(connection_id), database, app_state)
        }) else {
            send_reply(reply, Err(self.not_connected_message(connection_id, cx)));
            return receiver;
        };

        self.agent_query_editor(connection_id, &sql, window, cx);
        self.results_panel
            .update(cx, |panel, cx| panel.set_loading(true, cx));
        let results_panel = self.results_panel.clone();
        cx.spawn_in(window, async move |_this, cx| {
            let outcome = run_explain_query_workflow_with_mode(
                resources.query_service.as_ref(),
                resources.connection,
                resources.connection_id,
                sql,
                false,
                resources.display_context,
                chrono::Utc::now(),
            )
            .await;

            let explain = &outcome.explain_result;
            let plan = explain
                .query_plan
                .as_ref()
                .or(explain.raw_output.as_ref())
                .map(result_json);
            let summary = match &explain.error {
                Some(error) => Err(error.clone()),
                None => Ok(json!({ "plan": plan })),
            };

            if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                panel.add_explain_result(outcome.explain_result, window, cx);
            }) {
                tracing::warn!(%error, "failed to show agent explain result");
            }
            send_reply(reply, summary);
        })
        .detach();
        receiver
    }

    /// Replaces text in the query tab the user is working in, as one undo step.
    pub(crate) fn agent_edit_active_editor(
        &mut self,
        sql: String,
        replace_selection: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Result<Value, String> {
        let active_editor = self
            .workspace_controller
            .read(cx)
            .active_query_editor(cx)
            .filter(|editor| !self.is_agent_query_editor_entity(editor));
        // The Agent tab takes focus whenever the agent runs SQL, so fall back to
        // the user's most recently opened query tab.
        let Some(editor) = active_editor.or_else(|| {
            self.query_editors
                .iter()
                .rev()
                .filter_map(|weak_editor| weak_editor.upgrade())
                .find(|editor| !self.is_agent_query_editor_entity(editor))
        }) else {
            return Err("No query tab is open. Ask the user to open the query to edit.".into());
        };

        let text_editor = editor.read(cx).editor().clone();
        text_editor.update(cx, |text_editor, cx| {
            if replace_selection {
                text_editor.insert_text(&sql, window, cx);
            } else {
                text_editor.replace_all_text(&sql, cx);
            }
        });
        self.activate_existing_query_editor(&editor, window, cx);

        let name = editor.read(cx).name();
        window.push_notification(Notification::info(format!("Agent edited {name}")), cx);
        Ok(json!({ "edited_tab": name }))
    }

    pub(crate) fn agent_open_table(
        &mut self,
        connection_id: Uuid,
        table: zqlz_core::TableRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Key-value stores have no relational browse: the sidebar opens a
        // database's key list or one key through dedicated handlers.
        if let Some(default_index) = self.key_value_default_database(connection_id, cx) {
            let named = table
                .database
                .as_deref()
                .and_then(zqlz_core::parse_redis_database_index);
            // `db15` alone is a database's key list; a bare number is a key name.
            let database_view = zqlz_core::parse_redis_database_index(&table.name)
                .filter(|_| table.name.starts_with("db"))
                .filter(|index| named.is_none_or(|named| named == *index));
            match database_view {
                Some(index) => self.open_redis_database(connection_id, index, window, cx),
                None => {
                    let index = named.unwrap_or(default_index);
                    self.open_redis_key(connection_id, index, table.name, window, cx);
                }
            }
            return;
        }
        // Document stores open a collection in their own viewer, in the named
        // database or the one the connection was saved with.
        if let Some(default_database) = self.document_default_database(connection_id, cx) {
            let database_name = table.database.unwrap_or(default_database);
            self.open_document_collection_viewer(connection_id, database_name, table.name, None, window, cx);
            return;
        }
        self.open_table_viewer(connection_id, table, window, cx);
    }

    /// The connection's own database name when it is a document store.
    fn document_default_database(&self, connection_id: Uuid, cx: &Context<Self>) -> Option<String> {
        let app_state = cx.try_global::<AppState>()?;
        let connection = app_state.connection_service.get_connection(connection_id)?;
        if connection.driver_category() != zqlz_core::DriverCategory::Document {
            return None;
        }
        let saved = app_state
            .connection_service
            .get_saved_connection(connection_id)
            .ok()?;
        Some(saved.params.get("database").cloned().unwrap_or_default())
    }

    /// The connection's own database index when it is a key-value store.
    fn key_value_default_database(&self, connection_id: Uuid, cx: &Context<Self>) -> Option<u16> {
        let app_state = cx.try_global::<AppState>()?;
        let connection = app_state.connection_service.get_connection(connection_id)?;
        if connection.driver_category() != zqlz_core::DriverCategory::KeyValue {
            return None;
        }
        let saved = app_state
            .connection_service
            .get_saved_connection(connection_id)
            .ok()?;
        Some(
            saved
                .params
                .get("database")
                .and_then(|value| zqlz_core::parse_redis_database_index(value))
                .unwrap_or(0),
        )
    }

    /// `database` is only given for schema-scoped drivers (MySQL), where it
    /// qualifies the table (`db.table`) because the designer otherwise opens
    /// tables of the sidebar's active database.
    pub(crate) fn agent_open_designer(
        &mut self,
        connection_id: Uuid,
        table: String,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let dialect = cx
            .try_global::<AppState>()
            .and_then(|state| state.connection_service.get_connection(connection_id))
            .and_then(|connection| connection.dialect_id());
        match zqlz_core::TableRef::parse(&table, dialect, database.as_deref()) {
            Ok(table) => self.design_table(connection_id, table, window, cx),
            Err(error) => window.push_notification(
                Notification::error(format!("Cannot open the designer for '{table}': {error}")),
                cx,
            ),
        }
    }

    pub(super) fn set_agent_access(
        &mut self,
        connection_id: Uuid,
        access: AgentAccess,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };
        if let Err(error) = app_state
            .storage
            .save_setting(&AgentAccess::setting_key(connection_id), access.as_str())
        {
            tracing::error!(%error, "failed to save agent access");
            window.push_notification(
                Notification::error(format!("Couldn't save agent access: {error}")),
                cx,
            );
            return;
        }

        cx.default_global::<AgentAccessPolicy>()
            .0
            .insert(connection_id, access);
        let connection_name = self.agent_connection_name(connection_id, cx);
        window.push_notification(
            Notification::info(format!(
                "Agent access for {connection_name}: {}",
                access.label()
            )),
            cx,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn agent_request_approval(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        database: Option<String>,
        sql: String,
        reason: String,
        reply: oneshot::Sender<Result<Value, String>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let destructive_warning = QueryEngine::new()
            .analyze_for_destructive_operations(&sql)
            .map(|warning| warning.reason);
        let main_view = cx.entity().downgrade();
        // Shared by the OK and Cancel handlers; whichever fires first answers.
        // If the dialog closes some other way the sender drops and the agent
        // sees the request as rejected.
        let reply = Rc::new(RefCell::new(Some(reply)));
        let title = format!("Agent wants to change {connection_name}");

        cx.spawn_in(window, {
            let reply = reply.clone();
            async move |_this, cx| {
                // Resolves true if the agent goes away (MCP cancel or
                // disconnect) while the dialog is still unanswered.
                let withdrawn =
                    futures::future::poll_fn(|task_cx| match reply.borrow_mut().as_mut() {
                        Some(reply) => reply.poll_canceled(task_cx).map(|()| true),
                        None => Poll::Ready(false),
                    })
                    .await;
                if !withdrawn {
                    return;
                }
                reply.borrow_mut().take();
                tracing::info!("agent withdrew a pending change request");
                if let Err(error) = cx.update(|window, cx| {
                    window.push_notification(
                        Notification::warning("The agent withdrew its change request."),
                        cx,
                    );
                    window.refresh();
                }) {
                    tracing::debug!(%error, "window closed before agent request was withdrawn");
                }
            }
        })
        .detach();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let sql_block = div()
                .max_h(px(320.0))
                .overflow_y_scrollbar()
                .p_2()
                .rounded_md()
                .bg(cx.theme().secondary)
                .font_family(cx.theme().mono_font_family.clone())
                .text_xs()
                .child(sql.clone());

            // Answered or withdrawn; only the latter is still on screen.
            let withdrawn = reply.borrow().is_none();
            let lead = if withdrawn {
                "The agent withdrew this request. Nothing will run.".to_string()
            } else {
                reason.clone()
            };

            dialog
                .title(title.clone())
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(lead))
                        .child(sql_block)
                        .when_some(destructive_warning.clone(), |this, warning| {
                            this.child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().danger_text)
                                    .child(warning),
                            )
                        }),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text(if withdrawn { "Close" } else { "Run" })
                        .cancel_text("Reject")
                        .ok_variant(if destructive_warning.is_some() && !withdrawn {
                            ButtonVariant::Danger
                        } else {
                            ButtonVariant::Primary
                        }),
                )
                .on_ok({
                    let main_view = main_view.clone();
                    let reply = reply.clone();
                    let database = database.clone();
                    let sql = sql.clone();
                    move |_, window, cx| {
                        let Some(reply) = reply.borrow_mut().take() else {
                            return true;
                        };
                        let database = database.clone();
                        let sql = sql.clone();
                        if let Err(error) = main_view.update(cx, |main_view, cx| {
                            main_view.agent_execute_sql(
                                connection_id,
                                database,
                                sql,
                                true,
                                reply,
                                window,
                                cx,
                            );
                        }) {
                            tracing::warn!(%error, "failed to run approved agent change");
                        }
                        true
                    }
                })
                .on_cancel({
                    let reply = reply.clone();
                    move |_, _window, _cx| {
                        if let Some(reply) = reply.borrow_mut().take() {
                            send_reply(reply, Err("The user rejected this change.".into()));
                        }
                        true
                    }
                })
                .confirm()
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn agent_execute_sql(
        &mut self,
        connection_id: Uuid,
        database: Option<String>,
        sql: String,
        is_write: bool,
        reply: oneshot::Sender<Result<Value, String>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            send_reply(reply, Err(self.not_connected_message(connection_id, cx)));
            return;
        };
        let connection_service = app_state.connection_service.clone();
        // On MySQL a database is a schema of the one shared session, which can
        // only change database for everyone. Queries aimed at a database run on
        // a session of their own, and an unknown name fails instead of running
        // against the default database.
        let own_session_database = database.clone().filter(|_| {
            connection_service
                .get_connection(connection_id)
                .is_some_and(|connection| schema_scoped(connection.driver_name()))
        });
        // The private session replaces the resolved connection, so don't demand
        // the GUI's cached connection for that database.
        let Some(mut resources) = Self::resolve_query_workflow_resources(
            Some(connection_id),
            database.filter(|_| own_session_database.is_none()),
            app_state,
        ) else {
            send_reply(reply, Err(self.not_connected_message(connection_id, cx)));
            return;
        };
        if own_session_database.is_some() {
            resources.display_context.database_name = own_session_database.clone();
        }

        let editor = self.agent_query_editor(connection_id, &sql, window, cx);
        editor.update(cx, |editor, cx| editor.set_executing(true, cx));
        self.results_panel
            .update(cx, |panel, cx| panel.set_loading(true, cx));

        let modifies_schema = is_write && QueryEngine::new().is_schema_modifying(&sql);
        let connection_name = resources
            .display_context
            .connection_name
            .clone()
            .unwrap_or_default();
        let results_panel = self.results_panel.clone();
        let editor = editor.downgrade();
        // Reads from `run_query` also get the database's own read-only mode, so a
        // function the static check cannot see into still cannot write.
        let guard_reads = !is_write;
        cx.spawn_in(window, async move |this, cx| {
            // Agent reads share the connection's single session, so two of them
            // must not interleave their BEGIN/ROLLBACK. Taken after any approval
            // dialog (reads have none), and held only for the query itself.
            let _serialized = if guard_reads {
                Some(agent_read_lock(connection_id).lock_owned().await)
            } else {
                None
            };
            let started: Result<(Arc<dyn Connection>, &'static [&'static str]), String> = async {
                if let Some(database) = &own_session_database {
                    resources.connection =
                        open_database_session(&connection_service, connection_id, database)
                            .await?;
                }
                let leave = if guard_reads {
                    // Never run an agent read without the guard it was promised.
                    enter_read_only(resources.connection.as_ref())
                        .await
                        .map_err(|error| {
                            tracing::error!(%error, "could not start read-only mode for agent query");
                            format!("Could not start a read-only session, so nothing ran: {error}")
                        })?
                } else {
                    &[]
                };
                Ok((resources.connection.clone(), leave))
            }
            .await;
            let (connection, leave) = match started {
                Ok(started) => started,
                Err(error) => {
                    results_panel.update(cx, |panel, cx| panel.set_loading(false, cx));
                    if let Err(error) =
                        editor.update(cx, |editor, cx| editor.set_executing(false, cx))
                    {
                        tracing::debug!(%error, "agent query tab closed before its query was refused");
                    }
                    send_reply(reply, Err(error));
                    return;
                }
            };

            let outcome = run_execute_query_workflow(
                resources.query_service.as_ref(),
                resources.connection,
                resources.connection_id,
                sql,
                None,
                false,
                resources.display_context,
                chrono::Utc::now(),
            )
            .await;

            for statement in leave {
                if let Err(error) = connection.execute(statement, &[]).await {
                    tracing::error!(%error, statement, "could not leave read-only mode after agent query");
                }
            }
            if own_session_database.is_some()
                && let Err(error) = connection.close().await
            {
                tracing::warn!(%error, "failed to close the agent's database session");
            }
            drop(_serialized);

            let summary = execution_json(&outcome.execution);
            let message = if !outcome.success {
                format!("Agent query failed on {connection_name}")
            } else if is_write {
                format!(
                    "Agent changed {connection_name} · {} rows affected",
                    outcome.execution.total_rows_affected()
                )
            } else {
                format!(
                    "Agent queried {connection_name} · {} rows",
                    outcome.execution.total_rows_returned()
                )
            };

            if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                panel.set_execution(outcome.execution, window, cx);
            }) {
                tracing::warn!(%error, "failed to show agent query results");
            }
            if let Err(error) = editor.update(cx, |editor, cx| editor.set_executing(false, cx)) {
                tracing::debug!(%error, "agent query tab closed before its query finished");
            }
            if let Err(error) = this.update_in(cx, |this, window, cx| {
                this.refresh_query_history(cx);
                if is_write {
                    if modifies_schema {
                        this.request_refresh(RefreshScope::ConnectionSurfaces(connection_id), cx);
                    }
                    this.refresh_table_viewers_for_connection(connection_id, cx);
                }
                let notification = if outcome.success {
                    Notification::info(message)
                } else {
                    Notification::warning(message)
                };
                window.push_notification(notification, cx);
            }) {
                tracing::warn!(%error, "failed to update workspace after agent query");
            }

            send_reply(
                reply,
                if outcome.success {
                    Ok(summary)
                } else {
                    Err(summary.to_string())
                },
            );
        })
        .detach();
    }

    /// Shows `sql` in the connection's Agent tab, opening the tab on first use.
    fn agent_query_editor(
        &mut self,
        connection_id: Uuid,
        sql: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<QueryEditor> {
        if let Some(editor) = self
            .agent_query_editors
            .get(&connection_id)
            .and_then(|editor| editor.upgrade())
        {
            editor.update(cx, |editor, cx| {
                editor.set_content(sql.to_string(), window, cx)
            });
            self.activate_existing_query_editor(&editor, window, cx);
            return editor;
        }

        let display_name = format!("Agent · {}", self.agent_connection_name(connection_id, cx));
        let editor = self.open_query_editor_with_content(
            display_name,
            sql.to_string(),
            None,
            Some(connection_id),
            window,
            cx,
        );
        self.agent_query_editors
            .insert(connection_id, editor.downgrade());
        editor
    }

    fn is_agent_query_editor(&self, connection_id: Option<Uuid>, cx: &App) -> bool {
        let Some(active) = self.workspace_controller.read(cx).active_query_editor(cx) else {
            return false;
        };
        connection_id.is_some_and(|connection_id| {
            self.agent_query_editors
                .get(&connection_id)
                .is_some_and(|editor| editor.entity_id() == active.entity_id())
        })
    }

    fn is_agent_query_editor_entity(&self, editor: &Entity<QueryEditor>) -> bool {
        self.agent_query_editors
            .values()
            .any(|agent_editor| agent_editor.entity_id() == editor.entity_id())
    }

    /// Reloads open grids so agent writes show up, skipping grids that hold
    /// the user's own unsaved edits.
    fn refresh_table_viewers_for_connection(&self, connection_id: Uuid, cx: &mut Context<Self>) {
        let viewers: Vec<Entity<TableViewerPanel>> = self
            .workspace_controller
            .read(cx)
            .center_panels(cx)
            .into_iter()
            .filter_map(|panel| panel.view().downcast::<TableViewerPanel>().ok())
            .filter(|viewer| {
                let viewer = viewer.read(cx);
                viewer.connection_id() == Some(connection_id)
                    && !viewer.table_state.as_ref().is_some_and(|table_state| {
                        table_state.read(cx).delegate().has_pending_changes()
                    })
            })
            .collect();
        for viewer in viewers {
            viewer.update(cx, |viewer, cx| viewer.refresh(cx));
        }
    }

    fn agent_connection_name(&self, connection_id: Uuid, cx: &App) -> String {
        cx.try_global::<AppState>()
            .and_then(|app_state| {
                app_state
                    .connection_service
                    .get_saved_connection_name(connection_id)
            })
            .unwrap_or_else(|| connection_id.to_string())
    }

    fn not_connected_message(&self, connection_id: Uuid, cx: &App) -> String {
        format!(
            "'{}' is not connected in ZQLZ. Call the connect tool first.",
            self.agent_connection_name(connection_id, cx)
        )
    }
}

/// Statements that put the session into the database's own read-only mode for
/// one agent read (`begin`) and take it out again (`end`). `None` where that
/// cannot be done safely, which then relies on the static
/// `QueryEngine::is_read_only` check alone.
///
/// MySQL needs the driver to keep every call of the connection on one server
/// session; a transaction on a pooled connection would not stay on the
/// connection the query lands on. The MySQL driver pins that session for user
/// queries.
fn read_only_guard(
    driver_name: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    match driver_name {
        "postgresql" => Some(("BEGIN READ ONLY", &["ROLLBACK"])),
        // Writes inside fail with error 1792, including those done by a stored
        // function the static check cannot see into.
        "mysql" => Some(("START TRANSACTION READ ONLY", &["ROLLBACK"])),
        "sqlite" => Some(("PRAGMA query_only = ON", &["PRAGMA query_only = OFF"])),
        _ => None,
    }
}

const AGENT_SAVEPOINT: &str = "SAVEPOINT zqlz_agent_read";
const AGENT_SAVEPOINT_END: &[&str] = &[
    "ROLLBACK TO SAVEPOINT zqlz_agent_read",
    "RELEASE SAVEPOINT zqlz_agent_read",
];

/// Whether the session is inside a transaction, on MySQL, which has no
/// `SAVEPOINT` probe that fails outside one. `None` when it cannot be told.
///
/// MariaDB answers `@@in_transaction`. MySQL does not have it; its
/// performance_schema records the session's transaction as `ACTIVE`, which is
/// only trustworthy while performance_schema is on (it is empty, not an
/// error, when off).
async fn mysql_in_transaction(connection: &dyn Connection) -> Option<bool> {
    const MARIADB_PROBE: &str = "SELECT @@in_transaction";
    const MYSQL_PROBE: &str = "SELECT @@performance_schema, (SELECT COUNT(*) FROM \
        performance_schema.events_transactions_current WHERE THREAD_ID = PS_CURRENT_THREAD_ID() \
        AND STATE = 'ACTIVE')";
    let text = |result: &zqlz_core::QueryResult, column: usize| {
        result
            .rows
            .first()
            .and_then(|row| row.values.get(column))
            .map(|value| value.to_string())
    };
    if let Ok(result) = connection.query(MARIADB_PROBE, &[]).await {
        return text(&result, 0).map(|value| value.trim() != "0");
    }
    let result = connection.query(MYSQL_PROBE, &[]).await.ok()?;
    if !matches!(text(&result, 0)?.trim(), "1" | "true") {
        return None;
    }
    text(&result, 1).map(|active| active.trim() != "0")
}

/// Starts the read-only scope for one agent read and returns the statements
/// that end it.
///
/// On Postgres the user may already be inside a transaction on this shared
/// session (an explicit BEGIN in an editor, an import in progress). A second
/// BEGIN would be a no-op and our ROLLBACK would then end *their* transaction,
/// so the read runs inside a savepoint instead: it cannot be made read-only
/// mid-transaction, but rolling back to the savepoint undoes anything a
/// function the static check missed did, and an error in the read does not
/// leave the user's transaction aborted. `SAVEPOINT` doubles as the probe: it
/// only succeeds inside a transaction block.
///
/// MySQL does the same, with a different probe because `SAVEPOINT` succeeds
/// outside a transaction there. `START TRANSACTION READ ONLY` inside the
/// user's transaction would silently commit it, and `SET TRANSACTION READ ONLY`
/// is refused mid-transaction, so in that case only the savepoint (and the
/// static check) protect the read. If the session cannot say whether it is in
/// a transaction (performance_schema off), the read runs on the static check
/// alone rather than risk committing the user's work.
///
/// Residual limitation: the scope lives on the connection's one session, so a
/// write another tab sends while an agent read is in flight sees the read-only
/// transaction and fails (and a transaction another tab starts in that window
/// is rolled back). The window is the length of the agent's query.
async fn enter_read_only(
    connection: &dyn Connection,
) -> Result<&'static [&'static str], ZqlzError> {
    let driver_name = connection.driver_name();
    let Some((begin, end)) = read_only_guard(driver_name) else {
        tracing::debug!(
            driver_name,
            "agent read runs without a server-side read-only guard; only the static check applies"
        );
        return Ok(&[]);
    };
    match driver_name {
        "postgresql" => match connection.execute(AGENT_SAVEPOINT, &[]).await {
            Ok(_) => return Ok(AGENT_SAVEPOINT_END),
            Err(error) if is_no_active_transaction(&error) => {}
            Err(error) => return Err(error),
        },
        "mysql" => match mysql_in_transaction(connection).await {
            Some(true) => {
                connection.execute(AGENT_SAVEPOINT, &[]).await?;
                return Ok(AGENT_SAVEPOINT_END);
            }
            Some(false) => {}
            None => {
                tracing::warn!(
                    "cannot tell whether the MySQL session is in a transaction; agent read \
                     runs without a server-side read-only guard"
                );
                return Ok(&[]);
            }
        },
        _ => {}
    }
    connection.execute(begin, &[]).await?;
    Ok(end)
}

/// SQLSTATE 25P01: `SAVEPOINT` outside a transaction block. The driver only
/// exposes the code inside the message text.
fn is_no_active_transaction(error: &ZqlzError) -> bool {
    error.to_string().contains("25P01")
}

fn agent_read_lock(connection_id: Uuid) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: LazyLock<parking_lot::Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<()>>>>> =
        LazyLock::new(Default::default);
    LOCKS.lock().entry(connection_id).or_default().clone()
}

fn send_reply(reply: oneshot::Sender<Result<Value, String>>, result: Result<Value, String>) {
    if reply.send(result).is_err() {
        tracing::debug!("agent disconnected before its request finished");
    }
}

fn execution_json(execution: &QueryExecution) -> Value {
    let statements: Vec<Value> = execution
        .statements
        .iter()
        .map(|statement| {
            json!({
                "sql": statement.sql,
                "duration_ms": statement.duration_ms,
                "affected_rows": statement.affected_rows,
                "error": statement.error,
                "skipped": statement.skipped,
                "result": statement.result.as_ref().map(result_json),
            })
        })
        .collect();
    json!({ "duration_ms": execution.duration_ms, "statements": statements })
}

fn result_json(result: &QueryResult) -> Value {
    let rows: Vec<Vec<Value>> = result
        .rows
        .iter()
        .take(AGENT_ROW_LIMIT)
        .map(|row| row.values.iter().map(|value| value.to_wire_json()).collect())
        .collect();
    json!({
        "columns": result.columns.iter().map(|column| &column.name).collect::<Vec<_>>(),
        "rows": rows,
        "row_count": result.rows.len(),
        "truncated": result.rows.len() > AGENT_ROW_LIMIT,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{AGENT_SAVEPOINT_END, agent_read_lock, enter_read_only, read_only_guard};

    #[test]
    fn read_only_guard_covers_single_session_drivers_only() {
        assert_eq!(
            read_only_guard("postgresql"),
            Some(("BEGIN READ ONLY", &["ROLLBACK"][..]))
        );
        assert!(read_only_guard("sqlite").is_some());
        assert_eq!(
            read_only_guard("mysql"),
            Some(("START TRANSACTION READ ONLY", &["ROLLBACK"][..]))
        );
        assert_eq!(read_only_guard("redis"), None);
    }

    /// A MySQL session that records what it is sent and answers the
    /// transaction probes from a script.
    mod scripted_mysql {
        use std::sync::Mutex;

        use async_trait::async_trait;
        use uuid::Uuid;
        use zqlz_core::{
            Connection, DropTableOptions, DropTriggerOptions, DropViewOptions, QueryResult,
            SqlObjectName, StatementResult, Transaction, Value, ZqlzError,
        };

        use super::super::{AGENT_SAVEPOINT_END, enter_read_only};

        /// `mariadb`: answers `@@in_transaction` with `in_transaction`.
        /// Otherwise acts like MySQL: unknown variable, then performance_schema
        /// reports `(performance_schema_on, active_transactions)`.
        struct Session {
            seen: Mutex<Vec<String>>,
            mariadb: Option<i64>,
            performance_schema: Option<(i64, i64)>,
        }

        impl Session {
            fn mysql(performance_schema: Option<(i64, i64)>) -> Self {
                Self {
                    seen: Mutex::default(),
                    mariadb: None,
                    performance_schema,
                }
            }

            fn seen(&self) -> Vec<String> {
                self.seen.lock().unwrap().clone()
            }
        }

        fn rows(values: Vec<Value>) -> QueryResult {
            QueryResult {
                id: Uuid::new_v4(),
                columns: Vec::new(),
                rows: vec![zqlz_core::Row::new(Vec::new(), values)],
                total_rows: Some(1),
                is_estimated_total: false,
                affected_rows: 0,
                execution_time_ms: 0,
                warnings: Vec::new(),
            }
        }

        fn unsupported<T>() -> zqlz_core::Result<T> {
            Err(ZqlzError::NotSupported("scripted connection".into()))
        }

        #[async_trait]
        impl Connection for Session {
            fn driver_name(&self) -> &str {
                "mysql"
            }

            async fn execute(&self, sql: &str, _: &[Value]) -> zqlz_core::Result<StatementResult> {
                self.seen.lock().unwrap().push(sql.to_string());
                Ok(StatementResult { is_query: false, result: None, affected_rows: 0, error: None })
            }

            async fn query(&self, sql: &str, _: &[Value]) -> zqlz_core::Result<QueryResult> {
                self.seen.lock().unwrap().push(sql.to_string());
                if sql.contains("@@in_transaction") {
                    return match self.mariadb {
                        Some(value) => Ok(rows(vec![Value::Int64(value)])),
                        None => Err(ZqlzError::Query(
                            "ERROR 1193 (HY000): Unknown system variable 'in_transaction'".into(),
                        )),
                    };
                }
                match self.performance_schema {
                    Some((on, active)) => Ok(rows(vec![Value::Int64(on), Value::Int64(active)])),
                    None => Err(ZqlzError::Query("ERROR 1142: SELECT command denied".into())),
                }
            }

            fn rename_table_sql(&self, _: &SqlObjectName, _: &str) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn drop_table_sql(&self, _: &SqlObjectName, _: DropTableOptions) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn drop_view_sql(&self, _: &SqlObjectName, _: DropViewOptions) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn drop_trigger_sql(
                &self,
                _: &SqlObjectName,
                _: Option<&SqlObjectName>,
                _: DropTriggerOptions,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn truncate_table_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn duplicate_table_sql(&self, _: &SqlObjectName, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn clear_table_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn table_has_rows_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn select_rows_sql(
                &self,
                _: &SqlObjectName,
                _: &[String],
                _: Option<&str>,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn select_distinct_rows_sql(
                &self,
                _: &SqlObjectName,
                _: &[String],
                _: Option<&str>,
                _: &[String],
                _: u64,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn insert_row_sql(&self, _: &SqlObjectName, _: &[String], _: usize) -> zqlz_core::Result<String> {
                unsupported()
            }
            fn performance_metrics_query_sql(&self) -> zqlz_core::Result<String> {
                unsupported()
            }
            async fn begin_transaction(&self) -> zqlz_core::Result<Box<dyn Transaction>> {
                unsupported()
            }
            async fn close(&self) -> zqlz_core::Result<()> {
                Ok(())
            }
            fn is_closed(&self) -> bool {
                false
            }
        }

        fn executed(session: &Session) -> Vec<String> {
            // Probes are queries; the scope itself is the only `execute`.
            session
                .seen()
                .into_iter()
                .filter(|sql| !sql.starts_with("SELECT"))
                .collect()
        }

        #[tokio::test]
        async fn autocommit_session_gets_a_read_only_transaction() {
            let session = Session::mysql(Some((1, 0)));
            let leave = enter_read_only(&session).await.unwrap();
            assert_eq!(executed(&session), ["START TRANSACTION READ ONLY"]);
            assert_eq!(leave, ["ROLLBACK"]);
        }

        #[tokio::test]
        async fn open_user_transaction_gets_a_savepoint_instead() {
            // START TRANSACTION would commit the user's work.
            let session = Session::mysql(Some((1, 1)));
            let leave = enter_read_only(&session).await.unwrap();
            assert_eq!(executed(&session), ["SAVEPOINT zqlz_agent_read"]);
            assert_eq!(leave, AGENT_SAVEPOINT_END);
        }

        #[tokio::test]
        async fn mariadb_reports_its_transaction_directly() {
            let mut session = Session::mysql(None);
            session.mariadb = Some(1);
            assert_eq!(enter_read_only(&session).await.unwrap(), AGENT_SAVEPOINT_END);

            session.mariadb = Some(0);
            session.seen.lock().unwrap().clear();
            assert_eq!(enter_read_only(&session).await.unwrap(), ["ROLLBACK"]);
            assert_eq!(executed(&session), ["START TRANSACTION READ ONLY"]);
        }

        #[tokio::test]
        async fn unknowable_transaction_state_runs_unguarded_instead_of_committing() {
            // performance_schema off: its table is empty, which says nothing.
            for session in [Session::mysql(Some((0, 0))), Session::mysql(None)] {
                let leave = enter_read_only(&session).await.unwrap();
                assert!(leave.is_empty());
                assert!(executed(&session).is_empty(), "{:?}", session.seen());
            }
        }
    }

    #[test]
    fn agent_read_lock_is_per_connection() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(Arc::ptr_eq(&agent_read_lock(a), &agent_read_lock(a)));
        assert!(!Arc::ptr_eq(&agent_read_lock(a), &agent_read_lock(b)));
    }

    /// Needs the zqlz-shots-pg container; run with `-- --ignored`.
    #[tokio::test]
    #[ignore = "needs the zqlz-shots-pg container on 127.0.0.1:55432"]
    async fn live_postgres_read_scope_respects_open_user_transaction() -> anyhow::Result<()> {
        use zqlz_core::{ConnectionConfig, DatabaseDriver};
        let mut config = ConnectionConfig::new_postgres("127.0.0.1", 55432, "postgres", "nimbus");
        config.password = Some("nimbus-demo".into());
        let connection = zqlz_drivers::postgres::PostgresDriver::new().connect(&config).await?;
        let connection = connection.as_ref();
        let count = |sql: &'static str| async move {
            let result = connection.query(sql, &[]).await?;
            anyhow::Ok(result.rows[0].values[0].to_string())
        };

        connection.execute("CREATE SCHEMA IF NOT EXISTS zqlz_test", &[]).await?;
        connection.execute("DROP TABLE IF EXISTS zqlz_test.agent_scope", &[]).await?;
        connection.execute("CREATE TABLE zqlz_test.agent_scope (id int)", &[]).await?;

        // Autocommit: a real read-only transaction, so writes fail inside it.
        let leave = enter_read_only(connection).await?;
        assert_eq!(leave, ["ROLLBACK"]);
        assert!(
            connection
                .execute("INSERT INTO zqlz_test.agent_scope VALUES (1)", &[])
                .await
                .is_err()
        );
        for statement in leave {
            connection.execute(statement, &[]).await?;
        }

        // Inside the user's transaction: their rows survive the agent read, and
        // a failing agent read does not abort their transaction.
        connection.execute("BEGIN", &[]).await?;
        connection.execute("INSERT INTO zqlz_test.agent_scope VALUES (2)", &[]).await?;
        let leave = enter_read_only(connection).await?;
        assert_eq!(leave, AGENT_SAVEPOINT_END);
        assert!(connection.query("SELECT 1/0", &[]).await.is_err());
        for statement in leave {
            connection.execute(statement, &[]).await?;
        }
        assert_eq!(count("SELECT count(*) FROM zqlz_test.agent_scope").await?, "1");
        connection.execute("COMMIT", &[]).await?;
        assert_eq!(count("SELECT count(*) FROM zqlz_test.agent_scope").await?, "1");

        // A user transaction that is already aborted refuses the read.
        connection.execute("BEGIN", &[]).await?;
        assert!(connection.query("SELECT 1/0", &[]).await.is_err());
        assert!(enter_read_only(connection).await.is_err());
        connection.execute("ROLLBACK", &[]).await?;

        connection.execute("DROP TABLE zqlz_test.agent_scope", &[]).await?;
        Ok(())
    }
}
