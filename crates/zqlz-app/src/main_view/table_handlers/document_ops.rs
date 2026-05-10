//! Document-store collection viewer operations.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{DocumentQueryRequest, DriverCategory};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::MainView;
use crate::app::AppState;
use crate::components::{TableViewerEvent, TableViewerPanel};
use crate::main_view::table_handlers::standalone_events::handle_document_commit_changes_event;
use crate::workspace_state::{
    WorkspaceSessionTableViewerState, WorkspaceSessionViewerKind, WorkspaceSessionViewerTab,
};

impl MainView {
    pub(in crate::main_view) fn load_document_collections(
        &mut self,
        connection_id: Uuid,
        database_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };
        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let document_service = app_state.document_service.clone();
        let sidebar = self.connection_sidebar.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let result = document_service
                .list_collections(connection, &database_name)
                .await;

            sidebar.update(cx, |sidebar, cx| match result {
                Ok(collections) => {
                    sidebar.set_document_collections(
                        connection_id,
                        &database_name,
                        collections
                            .into_iter()
                            .map(|collection| collection.name)
                            .collect(),
                        cx,
                    );
                }
                Err(error) => {
                    tracing::error!(
                        %error,
                        connection_id = %connection_id,
                        database = %database_name,
                        "Failed to load document collections"
                    );
                }
            });

            anyhow::Ok(())
        })
        .detach();
    }

    pub(in crate::main_view) fn open_document_collection_viewer(
        &mut self,
        connection_id: Uuid,
        database_name: String,
        collection_name: String,
        session_state: Option<WorkspaceSessionTableViewerState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Document".to_string());
        let document_service = app_state.document_service.clone();

        let viewer_entity = cx.new(TableViewerPanel::new);
        let table_viewer: Arc<dyn zqlz_ui::widgets::dock::PanelView> =
            Arc::new(viewer_entity.clone());

        viewer_entity.update(cx, |panel, cx| {
            panel.set_pending_session_state(session_state, cx);
            panel.begin_loading_table(
                connection_id,
                collection_name.clone(),
                Some(database_name.clone()),
                cx,
            )
        });

        cx.subscribe_in(&viewer_entity, window, {
            let viewer_entity_for_events = viewer_entity.clone();
            move |_this, _viewer, event: &TableViewerEvent, window, cx| match event {
                TableViewerEvent::RefreshTable {
                    connection_id,
                    table_name,
                    database_name,
                    ..
                } => {
                    _this.reload_document_collection_viewer(
                        *connection_id,
                        database_name.clone().unwrap_or_default(),
                        table_name.clone(),
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    );
                }
                TableViewerEvent::ValidationFailed { message } => {
                    window.push_notification(Notification::warning(message.clone()), cx);
                }
                TableViewerEvent::CommitChanges {
                    connection_id,
                    table_name,
                    modified_cells,
                    deleted_rows,
                    new_rows,
                    column_meta,
                    all_rows,
                } => {
                    handle_document_commit_changes_event(
                        *connection_id,
                        table_name.clone(),
                        modified_cells.clone(),
                        deleted_rows.clone(),
                        new_rows.clone(),
                        column_meta.clone(),
                        all_rows.clone(),
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    );
                }
                _ => {}
            }
        })
        .detach();

        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(table_viewer, window, cx);
        });
        self.workspace_state.update(cx, |state, cx| {
            state.record_open_viewer_tab(
                WorkspaceSessionViewerTab {
                    connection_id,
                    kind: WorkspaceSessionViewerKind::Collection {
                        database_name: database_name.clone(),
                        collection_name: collection_name.clone(),
                        viewer_state: None,
                    },
                },
                cx,
            );
        });

        let viewer_weak = viewer_entity.downgrade();
        cx.spawn_in(window, async move |_this, cx| {
            let result = document_service
                .query_documents(
                    connection,
                    DocumentQueryRequest {
                        database: database_name.clone(),
                        collection: collection_name.clone(),
                        filter_json: None,
                        projection_json: None,
                        sort_json: None,
                        skip: 0,
                        limit: 100,
                    },
                )
                .await;

            if let Err(error) = viewer_weak.update_in(cx, |viewer, window, cx| match result {
                Ok(result) => {
                    viewer.load_table(
                        connection_id,
                        connection_name.clone(),
                        collection_name.clone(),
                        Some(database_name.clone()),
                        false,
                        result,
                        DriverCategory::Document,
                        window,
                        cx,
                    );
                    viewer.set_auto_commit_mode(false, cx);
                }
                Err(error) => {
                    viewer.set_loading(false, cx);
                    window.push_notification(
                        Notification::error(format!(
                            "Failed to load collection '{}': {}",
                            collection_name, error
                        )),
                        cx,
                    );
                }
            }) {
                tracing::debug!(
                    %error,
                    "Skipped document collection load result because viewer was dropped"
                );
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn reload_document_collection_viewer(
        &mut self,
        connection_id: Uuid,
        database_name: String,
        collection_name: String,
        viewer_entity: Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };
        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let document_service = app_state.document_service.clone();
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Document".to_string());
        let request_generation =
            viewer_entity.update(cx, |viewer, cx| viewer.begin_data_request(cx));
        let viewer_weak = viewer_entity.downgrade();

        cx.spawn_in(window, async move |_this, cx| {
            let result = document_service
                .query_documents(
                    connection,
                    DocumentQueryRequest {
                        database: database_name.clone(),
                        collection: collection_name.clone(),
                        filter_json: None,
                        projection_json: None,
                        sort_json: None,
                        skip: 0,
                        limit: 100,
                    },
                )
                .await;

            if let Err(error) = viewer_weak.update_in(cx, |viewer, window, cx| {
                if !viewer.is_current_request(request_generation) {
                    return;
                }
                match result {
                    Ok(result) => {
                        viewer.load_table(
                            connection_id,
                            connection_name.clone(),
                            collection_name.clone(),
                            Some(database_name.clone()),
                            false,
                            result,
                            DriverCategory::Document,
                            window,
                            cx,
                        );
                        viewer.set_auto_commit_mode(false, cx);
                    }
                    Err(error) => {
                        viewer.set_loading(false, cx);
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to refresh collection '{}': {}",
                                collection_name, error
                            )),
                            cx,
                        );
                    }
                }
            }) {
                tracing::debug!(
                    %error,
                    "Skipped document collection refresh result because viewer was dropped"
                );
            }

            anyhow::Ok(())
        })
        .detach();
    }
}
