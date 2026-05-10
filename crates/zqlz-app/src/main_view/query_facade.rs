//! Query facade for app-shell entry points.
//!
//! This keeps query-related UI event call sites stable while app-only dock,
//! window, and storage concerns remain in `main_view`.

use gpui::*;
use uuid::Uuid;
use zqlz_connection::SavedQueryInfo;

use crate::actions::NewQuery;
use crate::main_view::MainView;

impl MainView {
    /// Routes "new query" actions through the query app boundary.
    pub(super) fn query_facade_handle_new_query(
        &mut self,
        action: &NewQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.handle_new_query(action, window, cx);
    }

    /// Routes saved-query open flows through the query app boundary.
    pub(super) fn query_facade_open_saved_query(
        &mut self,
        query_id: Uuid,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_saved_query(query_id, connection_id, window, cx);
    }

    /// Routes saved-query delete flows through the query app boundary.
    pub(super) fn query_facade_delete_saved_query(
        &mut self,
        query_id: Uuid,
        query_name: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_saved_query(query_id, query_name, connection_id, window, cx);
    }

    /// Routes saved-query rename flows through the query app boundary.
    pub(super) fn query_facade_rename_saved_query(
        &mut self,
        query_id: Uuid,
        query_name: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.rename_saved_query(query_id, query_name, connection_id, window, cx);
    }

    /// Routes save-as UX and persistence coordination through the query app boundary.
    pub(super) fn query_facade_show_save_query_dialog(
        &mut self,
        editor: WeakEntity<crate::components::QueryEditor>,
        sql: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.show_save_query_dialog(editor, sql, connection_id, window, cx);
    }

    /// Routes query editor bootstrapping through the query app boundary.
    pub(super) fn query_facade_create_new_query_editor(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<crate::components::QueryEditor> {
        self.create_new_query_editor(window, cx)
    }

    /// Routes project model open orchestration through the query app boundary.
    pub(super) fn query_facade_open_project_model(
        &mut self,
        project_id: Uuid,
        model_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let editor = self.query_facade_create_new_query_editor(window, cx);

        let Some(app_state) = cx.try_global::<crate::app::AppState>() else {
            tracing::warn!(
                project_id = %project_id,
                model_id = %model_id,
                "Cannot load model SQL because AppState is unavailable"
            );
            return;
        };

        let model_load_result = app_state
            .storage
            .load_model(model_id)
            .map(|model| model.map(|model| model.sql));

        match model_load_result {
            Ok(Some(sql)) => {
                editor.update(cx, |editor, cx| {
                    editor.set_content(sql.clone(), window, cx);
                });
            }
            Ok(None) => {
                tracing::warn!(
                    project_id = %project_id,
                    model_id = %model_id,
                    "Requested project model was not found in storage"
                );
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    project_id = %project_id,
                    model_id = %model_id,
                    "Failed to load project model from storage"
                );
            }
        }
    }

    /// Routes SQL file open/orchestration through the query app boundary.
    pub(super) fn query_facade_open_sql_file_in_query_editor(
        &mut self,
        path: &std::path::Path,
        content: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_sql_file_in_query_editor(path, content, window, cx);
    }

    /// Builds saved-query sidebar summaries for connected query state.
    pub(super) fn query_facade_load_saved_query_summaries(
        &self,
        connection_id: Uuid,
        cx: &App,
    ) -> Vec<SavedQueryInfo> {
        self.load_saved_queries_for_connection(connection_id, cx)
            .into_iter()
            .map(|query| SavedQueryInfo {
                id: query.id,
                name: query.name,
            })
            .collect()
    }

    /// Applies saved-query sidebar hydration after a connection becomes active.
    pub(super) fn query_facade_hydrate_connected_sidebar_saved_queries(
        &mut self,
        connection_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        let saved_queries = self.query_facade_load_saved_query_summaries(connection_id, cx);
        if saved_queries.is_empty() {
            return;
        }

        tracing::info!(
            connection_id = %connection_id,
            saved_query_count = saved_queries.len(),
            "Loaded saved queries for connected sidebar"
        );

        self.connection_sidebar.update(cx, |sidebar, cx| {
            sidebar.set_saved_queries(connection_id, saved_queries, cx);
        });
    }
}
