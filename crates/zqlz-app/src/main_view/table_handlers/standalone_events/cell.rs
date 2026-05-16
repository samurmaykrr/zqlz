//! This module contains standalone event handlers for cell editing operations.

use gpui::*;
use uuid::Uuid;
use zqlz_core::{DocumentCellUpdateRequest, DriverCategory, Value};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt, button::ButtonVariant, dialog::DialogButtonProps, v_flex,
};

use crate::app::AppState;
use crate::components::table_viewer::delegate::SaveCellRequest;
use crate::components::{
    CellData, CellEditorPanel, InspectorPanel, InspectorView, KeyValueData, KeyValueEditorPanel,
    RedisValueType, TableViewerPanel,
};

use crate::main_view::table_handlers_utils::{
    conversion::resolve_schema_qualifier,
    redis::{fetch_redis_key_value, fetch_redis_string_bytes, parse_human_readable_ttl},
};
use crate::workspace::WorkspaceController;

pub(in crate::main_view) struct RedisKeyEditRequest {
    pub connection_id: Uuid,
    pub database_name: String,
    pub all_row_values: Vec<Value>,
    pub all_column_names: Vec<String>,
}

pub(in crate::main_view) fn handle_save_cell_event(
    request: SaveCellRequest,
    viewer_weak: WeakEntity<TableViewerPanel>,
    database_name: Option<String>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SaveCell event: table={}, column={}, new_value={}",
        request.table_name,
        request.column_name,
        request.new_value.display_for_table()
    );

    if update_pending_new_row_cell(&viewer_weak, &request, cx) {
        return;
    }

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state
        .connections
        .get_for_database_cached(request.connection_id, database_name.as_deref())
    else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let document_service = app_state.document_service.clone();
    let table_name = request.table_name;
    let column_name = request.column_name;
    let original_value = request.original_value;
    let new_value_for_update = request.new_value.clone();
    let document_id_value = request
        .all_column_names
        .iter()
        .position(|column_name| column_name == "_id")
        .and_then(|index| request.all_row_values.get(index).cloned());
    let connection = connection.clone();
    let driver_category = connection.driver_category();

    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    let cell_update_data = zqlz_services::CellUpdateData {
        column_name: column_name.clone(),
        new_value: Some(request.new_value.as_value()).filter(|value| !value.is_null()),
        all_column_names: request.all_column_names,
        all_row_values: request.all_row_values,
        all_column_types: request.all_column_types,
    };
    let row = request.row;
    let col = request.data_col;

    window.spawn(cx, async move |cx| {
        let update_result = if matches!(driver_category, DriverCategory::Document) {
            if column_name == "_id" {
                Err(anyhow::anyhow!("Cannot edit MongoDB _id values"))
            } else {
                let database = database_name
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Document database context is unavailable"));
                let id_value = document_id_value
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Row has no _id value"));
                match (database, id_value) {
                    (Ok(database), Ok(id_value)) => document_service
                        .update_document_cell(
                            connection,
                            DocumentCellUpdateRequest {
                                database,
                                collection: table_name.clone(),
                                id_json: document_id_json(&id_value),
                                field_path: column_name.clone(),
                                new_value: new_value_for_update.as_value(),
                            },
                        )
                        .await
                        .map_err(|error| anyhow::anyhow!("{}", error)),
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            }
        } else {
            table_service
                .update_cell(
                    connection,
                    &table_name,
                    schema_qualifier.as_deref(),
                    cell_update_data,
                )
                .await
                .map_err(|error| anyhow::anyhow!("{}", error))
        };

        match update_result {
            Ok(()) => {
                tracing::info!("Cell updated successfully in database");
                _ = viewer_weak.update(cx, |viewer, cx| {
                    viewer.update_cell_value(
                        row,
                        col,
                        new_value_for_update.as_value(),
                        cx,
                    );
                });
            }
            Err(e) => {
                tracing::error!("Failed to update cell: {}", e);

                let error_detail = e
                    .to_string()
                    .replace("Cell update failed: ", "")
                    .replace("Query error: ", "")
                    .replace("Failed to execute statement: ", "");

                _ = viewer_weak.update(cx, |viewer, cx| {
                    viewer.update_cell_value(row, col, original_value.as_value(), cx);
                });

                _ = cx.update(|window, cx| {
                    window.open_dialog(cx, move |dialog, _window, cx| {
                        dialog
                            .title("Failed to Save Cell")
                            .child(
                                v_flex()
                                    .gap_2()
                                    .child(
                                        div().child(format!(
                                            "Could not update column '{}':",
                                            column_name
                                        )),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().danger)
                                            .child(error_detail.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .child("The cell value has been reverted to its original value."),
                                    ),
                            )
                            .button_props(
                                DialogButtonProps::default()
                                    .ok_text("OK")
                                    // This is a dialog-level default for the eventual OK button,
                                    // so Primary stays expressed as a ButtonVariant.
                                    .ok_variant(ButtonVariant::Primary),
                            )
                            .alert()
                    });
                });
            }
        }
        anyhow::Ok(())
    })
    .detach();
}

fn pending_new_row_index_from_actual_row(
    actual_row: usize,
    total_rows: usize,
    pending_new_row_count: usize,
) -> Option<usize> {
    if actual_row >= total_rows {
        return None;
    }

    let original_row_count = total_rows.checked_sub(pending_new_row_count)?;
    if actual_row >= original_row_count {
        Some(actual_row - original_row_count)
    } else {
        None
    }
}

pub(in crate::main_view) fn update_pending_new_row_cell(
    viewer: &WeakEntity<TableViewerPanel>,
    request: &SaveCellRequest,
    cx: &mut App,
) -> bool {
    let table_state = viewer
        .read_with(cx, |panel, _cx| panel.table_state.clone())
        .unwrap_or_default();

    let Some(table_state) = table_state else {
        return false;
    };

    let actual_row = request.row;

    let new_row_index = table_state.read_with(cx, |table, _cx| {
        let delegate = table.delegate();
        pending_new_row_index_from_actual_row(
            actual_row,
            delegate.rows.len(),
            delegate.pending_changes.new_row_count(),
        )
    });

    let Some(new_row_index) = new_row_index else {
        return false;
    };

    table_state.update(cx, |table, cx| {
        let delegate = table.delegate_mut();
        let data_type = delegate
            .column_meta
            .get(request.data_col)
            .map(|column| column.data_type.as_str())
            .unwrap_or("text");
        let typed_value = request.new_value.to_value(data_type);
        delegate.pending_changes.update_new_row_cell(
            new_row_index,
            request.data_col,
            typed_value.clone(),
        );
        if let Some(row_data) = delegate.rows.get_mut(actual_row)
            && let Some(cell) = row_data.get_mut(request.data_col)
        {
            *cell = typed_value;
        }
        cx.notify();
    });

    true
}

fn document_id_json(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Json(value) => value.to_string(),
        _ => value.to_json_value().to_string(),
    }
}

pub(in crate::main_view) fn handle_edit_cell_event(
    cell_data: CellData,
    viewer_weak: WeakEntity<TableViewerPanel>,
    cell_editor_panel: &Entity<CellEditorPanel>,
    workspace_controller: &Entity<WorkspaceController>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "EditCell event: row={}, col={}, column={}, type={}",
        cell_data.row_index,
        cell_data.col_index,
        cell_data.column_name,
        cell_data.column_type
    );

    cell_editor_panel.update(cx, |editor, cx| {
        editor.edit_cell(cell_data, Some(viewer_weak), window, cx);
    });

    inspector_panel.update(cx, |panel, cx| {
        panel.set_active_view(InspectorView::CellEditor, cx);
    });

    workspace_controller.update(cx, |workspace, cx| {
        workspace.reveal_panel(
            "InspectorPanel",
            zqlz_ui::widgets::dock::DockPlacement::Right,
            window,
            cx,
        );
    });
}

pub(in crate::main_view) fn handle_redis_key_edit_event(
    request: RedisKeyEditRequest,
    key_value_editor_panel: &Entity<KeyValueEditorPanel>,
    cell_editor_panel: &Entity<CellEditorPanel>,
    workspace_controller: &Entity<WorkspaceController>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let key_idx = request
        .all_column_names
        .iter()
        .position(|c| c == "Key")
        .unwrap_or(0);
    let type_idx = request
        .all_column_names
        .iter()
        .position(|c| c == "Type")
        .unwrap_or(1);
    let ttl_idx = request
        .all_column_names
        .iter()
        .position(|c| c == "TTL")
        .unwrap_or(4);

    let key = request
        .all_row_values
        .get(key_idx)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_default();
    let value_type_str = request
        .all_row_values
        .get(type_idx)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_default();
    let ttl_str = request
        .all_row_values
        .get(ttl_idx)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_default();

    let ttl: i64 = if ttl_str.is_empty() || ttl_str == "No TTL" {
        -1
    } else {
        parse_human_readable_ttl(&ttl_str).unwrap_or(-1)
    };

    let value_type = RedisValueType::from_str(&value_type_str);

    tracing::info!(
        "Opening KeyValueEditor for Redis key: {:?}, type={:?}",
        key,
        value_type
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state
        .connections
        .get_for_database_cached(request.connection_id, Some(&request.database_name))
    else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let connection = connection.clone();
    let key_clone = key.clone();
    let connection_id = request.connection_id;
    let database_name = request.database_name;
    let key_value_editor_panel = key_value_editor_panel.clone();
    let cell_editor_panel = cell_editor_panel.clone();
    let workspace_controller = workspace_controller.clone();
    let inspector_panel = inspector_panel.clone();

    window
        .spawn(cx, async move |cx| {
            if value_type == RedisValueType::String
                && let Some(bytes) = fetch_redis_string_bytes(&connection, &key_clone).await
            {
                cx.update(|window, cx| {
                    let cell_data = CellData {
                        table_name: key_clone.clone(),
                        column_name: "value".to_string(),
                        column_type: "binary".to_string(),
                        column_meta: zqlz_core::ColumnMeta {
                            name: "value".to_string(),
                            data_type: "binary".to_string(),
                            nullable: true,
                            ordinal: 0,
                            max_length: None,
                            precision: None,
                            scale: None,
                            auto_increment: false,
                            default_value: None,
                            comment: None,
                            enum_values: None,
                        },
                        current_value: Value::Bytes(bytes.clone()),
                        row_index: 0,
                        col_index: 0,
                        connection_id,
                        all_row_values: vec![Value::Bytes(bytes.clone())],
                        all_column_names: vec!["value".to_string()],
                        all_column_types: vec!["binary".to_string()],
                        raw_bytes: Some(bytes),
                    };

                    cell_editor_panel.update(cx, |editor, cx| {
                        editor.edit_cell(cell_data, None, window, cx);
                    });

                    inspector_panel.update(cx, |panel, cx| {
                        panel.set_active_view(InspectorView::CellEditor, cx);
                    });

                    workspace_controller.update(cx, |workspace, cx| {
                        workspace.reveal_panel(
                            "InspectorPanel",
                            zqlz_ui::widgets::dock::DockPlacement::Right,
                            window,
                            cx,
                        );
                    });
                })?;

                return Ok::<_, anyhow::Error>(());
            }

            let value = fetch_redis_key_value(&connection, &key_clone, value_type).await;

            cx.update(|window, cx| {
                let kv_data = KeyValueData {
                    key: key_clone,
                    value_type,
                    value,
                    ttl,
                    size_bytes: None,
                    connection_id,
                    database_name: Some(database_name.clone()),
                    is_new: false,
                };

                key_value_editor_panel.update(cx, |editor, cx| {
                    editor.edit_key(kv_data, window, cx);
                });

                inspector_panel.update(cx, |panel, cx| {
                    panel.set_active_view(InspectorView::KeyEditor, cx);
                });

                workspace_controller.update(cx, |workspace, cx| {
                    workspace.reveal_panel(
                        "InspectorPanel",
                        zqlz_ui::widgets::dock::DockPlacement::Right,
                        window,
                        cx,
                    );
                });
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach_and_log_err(cx);
}

#[cfg(test)]
mod tests {
    use super::pending_new_row_index_from_actual_row;

    #[test]
    fn pending_row_index_returns_none_for_existing_row() {
        assert_eq!(pending_new_row_index_from_actual_row(1, 5, 2), None);
    }

    #[test]
    fn pending_row_index_maps_first_pending_row() {
        assert_eq!(pending_new_row_index_from_actual_row(3, 5, 2), Some(0));
    }

    #[test]
    fn pending_row_index_maps_last_pending_row() {
        assert_eq!(pending_new_row_index_from_actual_row(4, 5, 2), Some(1));
    }

    #[test]
    fn pending_row_index_returns_none_when_row_out_of_bounds() {
        assert_eq!(pending_new_row_index_from_actual_row(5, 5, 2), None);
    }
}
