//! Standalone event handlers for row operations (add, save, delete, commit).
//!
//! This module contains standalone functions that handle row-level operations on database tables:
//! - Adding new rows locally to pending changes
//! - Saving new rows by inserting them into the database
//! - Deleting rows from relational databases
//! - Deleting keys from Redis databases
//! - Committing all pending changes (updates, inserts, deletes) in batch operations

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use zqlz_core::ColumnMeta;
use zqlz_services::RowInsertData;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    notification::Notification,
    v_flex,
};

use crate::app::AppState;
use crate::components::{PendingCellChange, TableViewerEvent, TableViewerPanel};

use super::super::super::table_handlers_utils::{
    conversion::resolve_schema_qualifier,
    formatting::escape_redis_value,
    validation::parse_inline_value,
};

pub(in crate::main_view) fn handle_add_row_event(
    _connection_id: Uuid,
    table_name: &str,
    _all_column_names: &[String],
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "AddRow event: table={} - adding to pending changes",
        table_name
    );

    // Add row locally to pending changes instead of immediately inserting to database
    // The row will be committed when user clicks "Commit Changes"
    _ = viewer_entity.update(cx, |viewer, cx| {
        if let Some(table_state) = &viewer.table_state {
            table_state.update(cx, |table, cx| {
                // Add the new row
                table.delegate_mut().add_new_row();

                // Get the display row index for the new row
                // (last row in filtered view, or last row if not filtering)
                let display_row_idx = if table.delegate().is_filtering {
                    table.delegate().filtered_row_indices.len().saturating_sub(1)
                } else {
                    table.delegate().rows.len().saturating_sub(1)
                };

                // Scroll to the new row so it's visible
                table.scroll_to_row(display_row_idx, cx);

                // Select the first editable cell (column 1, since column 0 is row number)
                table.start_cell_selection(display_row_idx, 1, cx);
                table.set_selected_cell(display_row_idx, 1, cx);

                // Auto-start editing on the first cell so user can immediately type
                table
                    .delegate_mut()
                    .start_editing(display_row_idx, 1, window, cx);

                cx.notify();
            });
        }
        cx.notify();
    });
}

pub(in crate::main_view) fn handle_save_new_row_event(
    connection_id: Uuid,
    table_name: &str,
    new_row_index: usize,
    row_data: &[String],
    column_names: &[String],
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SaveNewRow event: table={}, new_row_index={}, auto-committing after all required fields filled",
        table_name,
        new_row_index
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
    let connection = connection.clone();
    // Convert row_data from Vec<String> to Vec<Option<String>> for the insert API
    let row_data: Vec<Option<String>> = row_data.iter().map(|v| Some(v.clone())).collect();
    let column_names = column_names.to_vec();
    let window_handle = window.window_handle();

    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

    // Insert the new row in background
    cx.spawn(async move |cx| {
        tracing::debug!("Inserting new row into table={}", table_name);

        let result = table_service
            .insert_row(
                connection.clone(),
                &table_name,
                schema_qualifier.as_deref(),
                RowInsertData {
                    column_names,
                    values: row_data,
                    column_types: Vec::new(),
                },
            )
            .await;

        // Extract error message before moving result into closure
        let error_message = result.as_ref().err().map(|e| e.to_string());
        let is_success = result.is_ok();

        // Update UI on foreground thread - all updates in a single closure to avoid nested update panic
        _ = viewer_entity.update(cx, |viewer, cx| {
            if is_success {
                tracing::info!("Successfully inserted new row: table={}", table_name);

                // Remove from pending changes directly (no nested viewer_entity.update)
                if let Some(table_state) = &viewer.table_state {
                    table_state.update(cx, |table, cx| {
                        let delegate = table.delegate_mut();
                        if new_row_index < delegate.pending_changes.new_rows.len() {
                            delegate.pending_changes.new_rows.remove(new_row_index);
                            tracing::info!(
                                "Removed new row {} from pending changes, remaining pending={}",
                                new_row_index,
                                delegate.pending_changes.change_count()
                            );
                        }
                        cx.notify();
                    });
                }

                // Emit refresh event directly (no nested viewer_entity.update)
                cx.emit(TableViewerEvent::RefreshTable {
                    connection_id,
                    table_name: table_name.clone(),
                    driver_category: viewer.driver_category,
                    database_name: viewer.database_name.clone(),
                });
            } else if let Some(ref err) = error_message {
                tracing::error!("Failed to insert new row: table={}, error={}", table_name, err);
            }

            Ok::<_, anyhow::Error>(())
        });

        // Show notifications via window_handle (separate from viewer_entity update)
        if is_success {
            _ = window_handle.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::success(&format!("New row inserted into {}", table_name)),
                    cx,
                );
            });
        } else if let Some(err) = error_message {
            _ = window_handle.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::error(&format!("Failed to insert new row: {}", err)),
                    cx,
                );
            });
        }

        Ok::<_, anyhow::Error>(())
    })
    .detach();
}

pub(in crate::main_view) fn handle_delete_rows_event(
    connection_id: Uuid,
    table_name: &str,
    all_column_names: &[String],
    rows_to_delete: &[Vec<String>],
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "DeleteRows event: table={}, connection={}, rows={}",
        table_name,
        connection_id,
        rows_to_delete.len()
    );

    if rows_to_delete.is_empty() {
        return;
    }

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
    let connection = connection.clone();
    // Get connection name for tab title
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    // Capture the is_view state and database_name before loading
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

    // Extract active filter/sort/search state so the post-delete refresh preserves it
    let (where_clauses, order_by_clauses, visible_columns) =
        viewer_entity.read_with(cx, |viewer, cx| {
            let mut where_clauses: Vec<String> = Vec::new();
            let mut order_by_clauses: Vec<String> = Vec::new();

            if let Some(filter_state) = &viewer.filter_panel_state {
                let (filters, sorts) = filter_state.read_with(cx, |state, _cx| {
                    (state.get_filter_conditions(), state.get_sort_criteria())
                });
                where_clauses = filters.iter().filter_map(|f| f.to_sql()).collect();
                order_by_clauses = sorts.iter().map(|s| s.to_sql()).collect();
            }

            if !viewer.search_text.is_empty() {
                let all_column_names: Vec<String> = viewer
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                if !all_column_names.is_empty() {
                    let escaped_search = viewer
                        .search_text
                        .replace("'", "''")
                        .replace('%', "\\%")
                        .replace('_', "\\_");
                    let column_conditions: Vec<String> = all_column_names
                        .iter()
                        .map(|col_name| {
                            let escaped_col =
                                format!("\"{}\"", col_name.replace('"', "\"\""));
                            format!(
                                "CAST({} AS TEXT) LIKE '%{}%' ESCAPE '\\'",
                                escaped_col, escaped_search
                            )
                        })
                        .collect();
                    where_clauses.push(format!("({})", column_conditions.join(" OR ")));
                }
            }

            let visible_columns: Vec<String> = viewer
                .column_visibility_state
                .as_ref()
                .map(|state| state.read(cx).visible_columns())
                .unwrap_or_else(|| viewer.column_meta.iter().map(|c| c.name.clone()).collect());

            (where_clauses, order_by_clauses, visible_columns)
        });

    let row_delete_data = zqlz_services::RowDeleteData {
        all_column_names: all_column_names.to_vec(),
        rows: rows_to_delete.to_vec(),
    };

    window
        .spawn(cx, async move |cx| {
            match table_service
                .delete_rows(connection.clone(), &table_name, schema_qualifier.as_deref(), row_delete_data)
                .await
            {
                Ok(deleted_count) => {
                    tracing::info!("Deleted {} rows successfully", deleted_count);

                    // Refresh the table preserving active filters/sorts/search
                    match table_service
                        .browse_table_with_filters(
                            connection,
                            &table_name,
                            schema_qualifier.as_deref(),
                            where_clauses,
                            order_by_clauses,
                            visible_columns,
                            None,
                            None,
                            None,
                        )
                        .await
                    {
                        Ok(result) => {
                            _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                                viewer.load_table(
                                    connection_id,
                                    connection_name.clone(),
                                    table_name.clone(),
                                    database_name.clone(),
                                    is_view,
                                    result,
                                    zqlz_core::DriverCategory::Relational,
                                    window,
                                    cx,
                                );
                                window.push_notification(
                                    Notification::success(&format!(
                                        "{} row(s) deleted",
                                        deleted_count
                                    )),
                                    cx,
                                );
                            });
                        }
                        Err(refresh_err) => {
                            tracing::error!(
                                "Failed to refresh table after delete: {}",
                                refresh_err
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to delete rows: {}", e);
                    _ = viewer_entity.update_in(cx, |_viewer, window, cx| {
                        window.push_notification(
                            Notification::error(&format!("Failed to delete rows: {}", e)),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_delete_redis_keys_event(
    connection_id: Uuid,
    all_column_names: &[String],
    rows_to_delete: &[Vec<String>],
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "DeleteRedisKeys event: connection={}, rows={}",
        connection_id,
        rows_to_delete.len()
    );

    if rows_to_delete.is_empty() {
        return;
    }

    let key_column_index = all_column_names
        .iter()
        .position(|name| name == "Key")
        .unwrap_or(0);

    let key_names: Vec<String> = rows_to_delete
        .iter()
        .filter_map(|row| row.get(key_column_index).cloned())
        .collect();

    if key_names.is_empty() {
        return;
    }

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();

    window
        .spawn(cx, async move |cx| {
            let mut deleted_count = 0usize;
            let mut last_error: Option<String> = None;

            for key_name in &key_names {
                let escaped_key = escape_redis_value(key_name);
                match connection
                    .execute(&format!("DEL {}", escaped_key), &[])
                    .await
                {
                    Ok(_) => {
                        deleted_count += 1;
                    }
                    Err(e) => {
                        tracing::error!("Failed to delete Redis key '{}': {}", key_name, e);
                        last_error = Some(format!("{}", e));
                    }
                }
            }

            if deleted_count > 0 {
                _ = viewer_entity.update(cx, |viewer, cx| {
                    viewer.refresh(cx);
                });
            }

            _ = viewer_entity.update_in(cx, |_viewer, window, cx| {
                if let Some(err) = last_error {
                    window.push_notification(
                        Notification::error(&format!(
                            "Deleted {} of {} key(s), error: {}",
                            deleted_count,
                            key_names.len(),
                            err
                        )),
                        cx,
                    );
                } else {
                    window.push_notification(
                        Notification::success(&format!(
                            "{} key(s) deleted",
                            deleted_count
                        )),
                        cx,
                    );
                }
            });

            anyhow::Ok(())
        })
        .detach();
}

/// Handle commit changes event - execute all pending changes in a transaction
#[allow(clippy::too_many_arguments)]
pub(in crate::main_view) fn handle_commit_changes_event(
    connection_id: Uuid,
    table_name: String,
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<String>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<String>>,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());
    let connection = connection.clone();

    // Capture viewer state before the async spawn
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let driver_category = viewer_entity.read(cx).driver_category;
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

    // Build column names from metadata
    let column_names: Vec<String> = column_meta.iter().map(|c| c.name.clone()).collect();
    let column_types: Vec<String> = column_meta.iter().map(|c| c.data_type.clone()).collect();

    window
        .spawn(cx, async move |cx| {
            let mut success_count = 0;
            let mut error_messages: Vec<String> = Vec::new();

            // Execute UPDATE statements for modified cells
            // Calculate the boundary between original and new rows so we can
            // skip any modified_cells entries that accidentally target new rows
            // (new rows are handled separately via INSERT below).
            let original_row_count = all_rows.len() - new_rows.len();
            for ((row_idx, col_idx), change) in &modified_cells {
                if *row_idx >= original_row_count {
                    tracing::warn!(
                        "Skipping modified_cell at row {} — belongs to new row (original_count={})",
                        row_idx,
                        original_row_count
                    );
                    continue;
                }
                if let Some(row_values) = all_rows.get(*row_idx) {
                    if let Some(col_name) = column_names.get(*col_idx) {
                        // Reconstruct the original row values for WHERE clause
                        // The UI has already been updated with the new value, so we need
                        // to restore original values for any modified cells in this row
                        let mut original_row_values = row_values.clone();
                        for ((mod_row, mod_col), mod_change) in &modified_cells {
                            if *mod_row == *row_idx {
                                if let Some(cell) = original_row_values.get_mut(*mod_col) {
                                    *cell = mod_change.original_value.clone();
                                }
                            }
                        }

                        let cell_update = zqlz_services::CellUpdateData {
                            column_name: col_name.clone(),
                            new_value: parse_inline_value(&change.new_value),
                            all_column_names: column_names.clone(),
                            all_row_values: original_row_values,
                            all_column_types: column_types.clone(),
                        };

                        match table_service
                            .update_cell(connection.clone(), &table_name, schema_qualifier.as_deref(), cell_update)
                            .await
                        {
                            Ok(()) => {
                                success_count += 1;
                            }
                            Err(e) => {
                                error_messages.push(format!(
                                    "Failed to update row {}, column {}: {}",
                                    row_idx + 1,
                                    col_name,
                                    e
                                ));
                            }
                        }
                    }
                }
            }

            // Execute DELETE statements for deleted rows
            if !deleted_rows.is_empty() {
                let rows_to_delete: Vec<Vec<String>> = deleted_rows
                    .iter()
                    .filter_map(|&idx| all_rows.get(idx).cloned())
                    .collect();

                if !rows_to_delete.is_empty() {
                    let row_delete_data = zqlz_services::RowDeleteData {
                        all_column_names: column_names.clone(),
                        rows: rows_to_delete,
                    };

                    match table_service
                        .delete_rows(connection.clone(), &table_name, schema_qualifier.as_deref(), row_delete_data)
                        .await
                    {
                        Ok(deleted_count) => {
                            success_count += deleted_count;
                        }
                        Err(e) => {
                            error_messages.push(format!("Failed to delete rows: {}", e));
                        }
                    }
                }
            }

            // Execute INSERT statements for new rows
            for (row_idx, row_values) in new_rows.iter().enumerate() {
                // Convert Vec<String> to Vec<Option<String>> - empty strings become None (NULL)
                let values: Vec<Option<String>> = row_values
                    .iter()
                    .map(|v| {
                        if v.is_empty() || v == "NULL" {
                            None
                        } else {
                            Some(v.clone())
                        }
                    })
                    .collect();

                let row_insert_data = zqlz_services::RowInsertData {
                    column_names: column_names.clone(),
                    values,
                    column_types: column_types.clone(),
                };

                match table_service
                    .insert_row(connection.clone(), &table_name, schema_qualifier.as_deref(), row_insert_data)
                    .await
                {
                    Ok(()) => {
                        success_count += 1;
                    }
                    Err(e) => {
                        error_messages.push(format!(
                            "Failed to insert new row {}: {}",
                            row_idx + 1,
                            e
                        ));
                    }
                }
            }

            // Clear pending changes and refresh the table
            if error_messages.is_empty() {
                // All changes committed successfully
                _ = viewer_entity.update(cx, |viewer, cx| {
                    if let Some(table_state) = &viewer.table_state {
                        table_state.update(cx, |table, cx| {
                            table.delegate_mut().clear_pending_changes();
                            cx.notify();
                        });
                    }
                });

                // Refresh table data
                if let Ok(result) = table_service
                    .browse_table(connection, &table_name, schema_qualifier.as_deref(), None, None)
                    .await
                {
                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        viewer.load_table(
                            connection_id,
                            connection_name.clone(),
                            table_name.clone(),
                            database_name.clone(),
                            is_view,
                            result,
                            driver_category,
                            window,
                            cx,
                        );
                    });
                }

                tracing::info!("{} changes committed successfully", success_count);
            } else {
                // Some changes failed
                tracing::error!(
                    "Commit partially failed: {} successes, {} errors",
                    success_count,
                    error_messages.len()
                );

                // Show error dialog
                _ = cx.update(|window, cx| {
                    window.open_dialog(cx, move |dialog, _window, cx| {
                        dialog
                            .title("Commit Changes Failed")
                            .child(
                                v_flex()
                                    .gap_2()
                                    .child(div().child(format!(
                                        "{} changes succeeded, {} failed:",
                                        success_count,
                                        error_messages.len()
                                    )))
                                    .children(error_messages.iter().take(5).map(|msg| {
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().danger)
                                            .child(msg.clone())
                                    }))
                                    .when(error_messages.len() > 5, |this| {
                                        this.child(
                                            div()
                                                .text_sm()
                                                .text_color(cx.theme().muted_foreground)
                                                .child(format!(
                                                    "... and {} more errors",
                                                    error_messages.len() - 5
                                                )),
                                        )
                                    }),
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

            anyhow::Ok(())
        })
        .detach();
}
