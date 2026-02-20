//! This module contains standalone event handlers for cell editing operations.

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    v_flex,
};

use crate::app::AppState;
use crate::components::{
    CellData, CellEditorPanel, InspectorPanel, InspectorView, KeyValueData, KeyValueEditorPanel,
    RedisValueType, TableViewerPanel,
};

use crate::main_view::table_handlers_utils::{
    conversion::resolve_schema_qualifier,
    redis::{fetch_redis_key_value, parse_human_readable_ttl},
    validation::parse_inline_value,
};

pub(in crate::main_view) fn handle_save_cell_event(
    table_name: &str,
    connection_id: Uuid,
    row: usize,
    col: usize,
    column_name: &str,
    new_value: &str,
    original_value: &str,
    all_row_values: &[String],
    all_column_names: &[String],
    all_column_types: &[String],
    viewer_weak: WeakEntity<TableViewerPanel>,
    database_name: Option<String>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SaveCell event: table={}, column={}, new_value={}",
        table_name,
        column_name,
        new_value
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let table_name = table_name.to_string();
    let column_name = column_name.to_string();
    let original_value = original_value.to_string();
    let new_value_display = new_value.to_string();
    let connection = connection.clone();

    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

    if update_pending_new_row_cell(&viewer_weak, row, col, &new_value_display, cx) {
        return;
    }

    let cell_update_data = zqlz_services::CellUpdateData {
        column_name: column_name.clone(),
        new_value: parse_inline_value(&new_value_display),
        all_column_names: all_column_names.to_vec(),
        all_row_values: all_row_values.to_vec(),
        all_column_types: all_column_types.to_vec(),
    };

    window.spawn(cx, async move |cx| {
        match table_service
            .update_cell(connection, &table_name, schema_qualifier.as_deref(), cell_update_data)
            .await
        {
            Ok(()) => {
                tracing::info!("Cell updated successfully in database");
                _ = viewer_weak.update(cx, |viewer, cx| {
                    viewer.update_cell_value(
                        row,
                        col,
                        Some(new_value_display.clone()),
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
                    viewer.update_cell_value(row, col, Some(original_value.clone()), cx);
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

pub(in crate::main_view) fn update_pending_new_row_cell(
    viewer: &WeakEntity<TableViewerPanel>,
    row: usize,
    col: usize,
    new_value: &str,
    cx: &mut App,
) -> bool {
    let table_state = match viewer.read_with(cx, |panel, _cx| panel.table_state.clone()) {
        Ok(state) => state,
        Err(_) => None,
    };

    let Some(table_state) = table_state else {
        return false;
    };

    let actual_row =
        table_state.read_with(cx, |table, _cx| table.delegate().get_actual_row_index(row));

    let new_row_index = table_state.read_with(cx, |table, _cx| {
        let delegate = table.delegate();
        delegate
            .pending_changes
            .get_new_row_index(actual_row, delegate.rows.len())
    });

    let Some(new_row_index) = new_row_index else {
        return false;
    };

    table_state.update(cx, |table, cx| {
        let delegate = table.delegate_mut();
        delegate
            .pending_changes
            .update_new_row_cell(new_row_index, col, new_value.to_string());
        if let Some(row_data) = delegate.rows.get_mut(actual_row) {
            if let Some(cell) = row_data.get_mut(col) {
                *cell = new_value.to_string();
            }
        }
        cx.notify();
    });

    true
}

pub(in crate::main_view) fn handle_edit_cell_event(
    table_name: &str,
    connection_id: Uuid,
    row: usize,
    col: usize,
    column_name: &str,
    column_type: &str,
    current_value: &Option<String>,
    all_row_values: &[String],
    all_column_names: &[String],
    all_column_types: &[String],
    raw_bytes: Option<Vec<u8>>,
    viewer_weak: WeakEntity<TableViewerPanel>,
    cell_editor_panel: &Entity<CellEditorPanel>,
    dock_area: &Entity<zqlz_ui::widgets::dock::DockArea>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "EditCell event: row={}, col={}, column={}, type={}",
        row,
        col,
        column_name,
        column_type
    );

    let cell_data = CellData {
        table_name: table_name.to_string(),
        column_name: column_name.to_string(),
        column_type: column_type.to_string(),
        row_id: None,
        current_value: current_value.clone(),
        row_index: row,
        col_index: col,
        connection_id,
        all_row_values: all_row_values.to_vec(),
        all_column_names: all_column_names.to_vec(),
        all_column_types: all_column_types.to_vec(),
        raw_bytes,
    };

    _ = cell_editor_panel.update(cx, |editor, cx| {
        editor.edit_cell(cell_data, Some(viewer_weak), window, cx);
    });

    _ = inspector_panel.update(cx, |panel, cx| {
        panel.set_active_view(InspectorView::CellEditor, cx);
    });

    _ = dock_area.update(cx, |area, cx| {
        area.activate_panel(
            "InspectorPanel",
            zqlz_ui::widgets::dock::DockPlacement::Right,
            window,
            cx,
        );
    });
}

pub(in crate::main_view) fn handle_redis_key_edit_event(
    connection_id: Uuid,
    all_row_values: &[String],
    all_column_names: &[String],
    key_value_editor_panel: &Entity<KeyValueEditorPanel>,
    dock_area: &Entity<zqlz_ui::widgets::dock::DockArea>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let key_idx = all_column_names
        .iter()
        .position(|c| c == "Key")
        .unwrap_or(0);
    let type_idx = all_column_names
        .iter()
        .position(|c| c == "Type")
        .unwrap_or(1);
    let ttl_idx = all_column_names
        .iter()
        .position(|c| c == "TTL")
        .unwrap_or(4);

    let key = all_row_values.get(key_idx).cloned().unwrap_or_default();
    let value_type_str = all_row_values.get(type_idx).cloned().unwrap_or_default();
    let ttl_str = all_row_values.get(ttl_idx).cloned().unwrap_or_default();

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

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let key_clone = key.clone();
    let key_value_editor_panel = key_value_editor_panel.clone();
    let dock_area = dock_area.clone();
    let inspector_panel = inspector_panel.clone();

    window
        .spawn(cx, async move |cx| {
            let value = fetch_redis_key_value(&connection, &key_clone, value_type).await;

            cx.update(|window, cx| {
                let kv_data = KeyValueData {
                    key: key_clone,
                    value_type,
                    value,
                    ttl,
                    size_bytes: None,
                    connection_id,
                    is_new: false,
                };

                _ = key_value_editor_panel.update(cx, |editor, cx| {
                    editor.edit_key(kv_data, window, cx);
                });

                _ = inspector_panel.update(cx, |panel, cx| {
                    panel.set_active_view(InspectorView::KeyEditor, cx);
                });

                _ = dock_area.update(cx, |area, cx| {
                    area.activate_panel(
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
