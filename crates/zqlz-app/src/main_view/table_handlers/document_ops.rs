//! Document-store collection viewer operations.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{DocumentDeleteRequest, DocumentQueryRequest, DriverCategory, Value};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::MainView;
use crate::app::AppState;
use crate::components::{
    CellData, InspectorView, RowData, RowEditorMode, TableViewerEvent, TableViewerPanel,
};
use crate::main_view::table_handlers::standalone_events::{
    handle_add_row_event, handle_document_commit_changes_event, handle_edit_cell_event,
};
use crate::workspace_state::{
    WorkspaceSessionTableViewerState, WorkspaceSessionViewerKind, WorkspaceSessionViewerTab,
};

fn document_id_json(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Json(value) => value.to_string(),
        _ => value.to_json_value().to_string(),
    }
}

impl MainView {
    fn apply_document_row_editor_update(
        &mut self,
        row_data: RowData,
        focused_column_index: Option<usize>,
        reveal_inspector_panel: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.key_value_editor_panel.update(cx, |editor, cx| {
            editor.edit_row(row_data, window, cx);
            if let Some(focused_column_index) = focused_column_index {
                editor.focus_field(focused_column_index, window, cx);
            }
        });

        self.inspector_panel.update(cx, |panel, cx| {
            panel.set_active_view(InspectorView::KeyEditor, cx);
        });

        if reveal_inspector_panel {
            self.workspace_controller.update(cx, |workspace, cx| {
                workspace.reveal_panel(
                    "InspectorPanel",
                    zqlz_ui::widgets::dock::DockPlacement::Right,
                    window,
                    cx,
                );
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_document_existing_row_editor_update(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        row_index: usize,
        row_values: &[zqlz_core::Value],
        column_meta: &[zqlz_core::ColumnMeta],
        all_column_names: &[String],
        source_viewer: WeakEntity<TableViewerPanel>,
        focused_column_index: Option<usize>,
        reveal_inspector_panel: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row_data = RowData {
            table_name: table_name.to_string(),
            connection_id,
            column_meta: column_meta.to_vec(),
            row_values: row_values.to_vec(),
            row_index: Some(row_index),
            is_new: false,
            source_viewer: Some(source_viewer),
            all_column_names: all_column_names.to_vec(),
        };

        self.apply_document_row_editor_update(
            row_data,
            focused_column_index,
            reveal_inspector_panel,
            window,
            cx,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_document_new_row_editor_update(
        &mut self,
        connection_id: Uuid,
        table_name: &str,
        row_values: Option<&[Value]>,
        row_index: Option<usize>,
        column_meta: &[zqlz_core::ColumnMeta],
        source_viewer: WeakEntity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let all_column_names = column_meta
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let row_values = row_values
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| column_meta.iter().map(|_| Value::Null).collect());
        let row_data = RowData {
            table_name: table_name.to_string(),
            connection_id,
            column_meta: column_meta.to_vec(),
            row_values,
            row_index,
            is_new: true,
            source_viewer: Some(source_viewer),
            all_column_names,
        };

        self.apply_document_row_editor_update(row_data, None, true, window, cx);
        self.key_value_editor_panel.update(cx, |editor, cx| {
            editor.focus_first_editable_row_field(window, cx);
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_document_rows(
        &mut self,
        connection_id: Uuid,
        collection_name: &str,
        all_column_names: &[String],
        rows_to_delete: &[Vec<Value>],
        viewer_entity: Entity<TableViewerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if rows_to_delete.is_empty() {
            return;
        }

        let Some(id_column_index) = all_column_names
            .iter()
            .position(|column_name| column_name == "_id")
        else {
            window.push_notification(
                Notification::error("Cannot delete MongoDB documents without an _id column"),
                cx,
            );
            return;
        };

        let database_name = viewer_entity.read(cx).database_name().unwrap_or_default();
        if database_name.is_empty() {
            window.push_notification(
                Notification::error("Cannot delete MongoDB documents without a database name"),
                cx,
            );
            return;
        }

        let ids_json = rows_to_delete
            .iter()
            .filter_map(|row| row.get(id_column_index).map(document_id_json))
            .collect::<Vec<_>>();
        if ids_json.len() != rows_to_delete.len() {
            window.push_notification(
                Notification::error("Some selected MongoDB documents do not have _id values"),
                cx,
            );
            return;
        }

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, Some(database_name.as_str()))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };
        let document_service = app_state.document_service.clone();
        let collection_name = collection_name.to_string();

        window
            .spawn(cx, async move |cx| {
                let result = document_service
                    .delete_documents(
                        connection,
                        DocumentDeleteRequest {
                            database: database_name,
                            collection: collection_name.clone(),
                            ids_json,
                            continue_on_error: true,
                        },
                    )
                    .await;

                match result {
                    Ok(outcome) if outcome.errors.is_empty() => {
                        viewer_entity.update(cx, |viewer, cx| viewer.refresh(cx));
                        if let Err(error) = cx.update(|window, cx| {
                            window.push_notification(
                                Notification::success(format!(
                                    "Deleted {} MongoDB document(s)",
                                    outcome.deleted_ids.len()
                                )),
                                cx,
                            );
                        }) {
                            tracing::debug!(error = %error, "Skipped MongoDB delete success notification after window closed");
                        }
                    }
                    Ok(outcome) => {
                        let message = format!(
                            "Deleted {} document(s), {} failed",
                            outcome.deleted_ids.len(),
                            outcome.errors.len()
                        );
                        if let Err(error) = cx.update(|window, cx| {
                            window.push_notification(Notification::error(message), cx);
                        }) {
                            tracing::debug!(error = %error, "Skipped MongoDB delete error notification after window closed");
                        }
                    }
                    Err(error) => {
                        if let Err(update_error) = cx.update(|window, cx| {
                            window.push_notification(
                                Notification::error(format!(
                                    "Failed to delete MongoDB documents: {}",
                                    error
                                )),
                                cx,
                            );
                        }) {
                            tracing::debug!(error = %update_error, "Skipped MongoDB delete failure notification after window closed");
                        }
                    }
                }

                anyhow::Ok(())
            })
            .detach();
    }

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
                .list_database_objects(connection, &database_name)
                .await;

            sidebar.update(cx, |sidebar, cx| match result {
                Ok(objects) => {
                    sidebar.set_document_database_objects(
                        connection_id,
                        &database_name,
                        objects,
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
            let viewer_weak_for_edit = viewer_entity.downgrade();
            let cell_editor_panel = self.cell_editor_panel.clone();
            let key_value_editor_panel = self.key_value_editor_panel.clone();
            let workspace_controller = self.workspace_controller.clone();
            let inspector_panel = self.inspector_panel.clone();
            move |_this, _viewer, event: &TableViewerEvent, window, cx| match event {
                TableViewerEvent::EditCell {
                    table_name,
                    connection_id,
                    row,
                    col,
                    column_meta,
                    column_name,
                    column_type,
                    current_value,
                    all_row_values,
                    all_column_names,
                    all_column_types,
                    raw_bytes,
                } => {
                    tracing::debug!(
                        table = %table_name,
                        column = %column_name,
                        row = *row,
                        "Opening document cell editor"
                    );
                    let cell_data = CellData {
                        table_name: table_name.clone(),
                        column_name: column_name.clone(),
                        column_type: column_type.clone(),
                        column_meta: column_meta.clone(),
                        current_value: current_value.clone(),
                        row_index: *row,
                        col_index: *col,
                        connection_id: *connection_id,
                        all_row_values: all_row_values.clone(),
                        all_column_names: all_column_names.clone(),
                        all_column_types: all_column_types.clone(),
                        raw_bytes: raw_bytes.clone(),
                    };

                    handle_edit_cell_event(
                        cell_data,
                        viewer_weak_for_edit.clone(),
                        &cell_editor_panel,
                        &workspace_controller,
                        &inspector_panel,
                        window,
                        cx,
                    );
                }
                TableViewerEvent::EditRow {
                    connection_id,
                    table_name,
                    row_index,
                    row_values,
                    column_meta,
                    all_column_names,
                } => {
                    _this.apply_document_existing_row_editor_update(
                        *connection_id,
                        table_name,
                        *row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                        viewer_weak_for_edit.clone(),
                        None,
                        true,
                        window,
                        cx,
                    );
                }
                TableViewerEvent::CellSelected {
                    connection_id,
                    table_name,
                    row_index,
                    col_index,
                    row_values,
                    column_meta,
                    all_column_names,
                } => {
                    _this.apply_document_existing_row_editor_update(
                        *connection_id,
                        table_name,
                        *row_index,
                        row_values,
                        column_meta,
                        all_column_names,
                        viewer_weak_for_edit.clone(),
                        Some(*col_index),
                        false,
                        window,
                        cx,
                    );
                }
                TableViewerEvent::RowSelected {
                    connection_id,
                    table_name,
                    row_index,
                    row_values,
                    column_meta,
                    all_column_names,
                } => {
                    let is_key_editor_active =
                        _this.inspector_panel.read(cx).active_view() == InspectorView::KeyEditor;
                    let is_row_editor_active =
                        key_value_editor_panel.read(cx).mode() == &RowEditorMode::SqlRow;
                    if is_key_editor_active && is_row_editor_active {
                        _this.apply_document_existing_row_editor_update(
                            *connection_id,
                            table_name,
                            *row_index,
                            row_values,
                            column_meta,
                            all_column_names,
                            viewer_weak_for_edit.clone(),
                            None,
                            false,
                            window,
                            cx,
                        );
                    }
                }
                TableViewerEvent::AddRow {
                    connection_id,
                    table_name,
                    all_column_names,
                } => {
                    handle_add_row_event(
                        *connection_id,
                        table_name,
                        all_column_names,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    );
                }
                TableViewerEvent::AddRowForm {
                    connection_id,
                    table_name,
                    column_meta,
                    row_values,
                    row_index,
                } => {
                    _this.apply_document_new_row_editor_update(
                        *connection_id,
                        table_name,
                        row_values.as_deref(),
                        *row_index,
                        column_meta,
                        viewer_weak_for_edit.clone(),
                        window,
                        cx,
                    );
                }
                TableViewerEvent::DeleteRows {
                    connection_id,
                    table_name,
                    all_column_names,
                    rows_to_delete,
                } => {
                    _this.delete_document_rows(
                        *connection_id,
                        table_name,
                        all_column_names,
                        rows_to_delete,
                        viewer_entity_for_events.clone(),
                        window,
                        cx,
                    );
                }
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
