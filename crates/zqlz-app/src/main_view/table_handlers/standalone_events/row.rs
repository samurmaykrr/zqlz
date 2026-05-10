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
use zqlz_core::{
    ColumnMeta, DocumentCellUpdateRequest, DocumentDeleteRequest, DriverCategory,
    KeyValueDeleteRequest, Value,
};
use zqlz_services::{CommitCellChange, CommitTableChangesRequest, RowInsertData};
use zqlz_ui::widgets::{
    ActiveTheme as _, Sizable, WindowExt,
    button::ButtonVariants,
    button::{Button, ButtonVariant},
    dialog::DialogButtonProps,
    notification::Notification,
    v_flex,
};

use crate::app::AppState;
use crate::components::{PendingCellChange, TableViewerEvent, TableViewerPanel};

use super::super::super::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::sql::{
    build_search_clause_for_columns, resolve_search_columns,
};

pub(in crate::main_view) struct SaveNewRowRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub new_row_index: usize,
    pub row_data: Vec<String>,
    pub column_names: Vec<String>,
}

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

    let viewer_weak = viewer_entity.downgrade();

    // Add row locally to pending changes instead of immediately inserting to database
    // The row will be committed when user clicks "Commit Changes"
    viewer_entity.update(cx, |viewer, cx| {
        if let Some(table_state) = &viewer.table_state {
            table_state.update(cx, |table, cx| {
                // Add the new row
                table.delegate_mut().add_new_row();

                // Get the display row index for the new row
                // (last row in filtered view, or last row if not filtering)
                let display_row_idx = if table.delegate().is_filtering {
                    table
                        .delegate()
                        .filtered_row_indices
                        .len()
                        .saturating_sub(1)
                } else {
                    table.delegate().rows.len().saturating_sub(1)
                };

                // Find the first editable column (skip auto-increment columns and the row number col 0)
                let first_editable_col = table
                    .delegate()
                    .column_meta
                    .iter()
                    .enumerate()
                    .find(|(_i, col)| !col.auto_increment)
                    .map(|(i, _)| i + 1) // +1 because col 0 is the row number column
                    .unwrap_or(1);

                // Scroll to the new row so it's visible
                table.scroll_to_row(display_row_idx, cx);

                // Select the first editable cell
                table.start_cell_selection(display_row_idx, first_editable_col, cx);
                table.set_selected_cell(display_row_idx, first_editable_col, cx);

                // Auto-start editing so user can immediately type
                table
                    .delegate_mut()
                    .start_editing(display_row_idx, first_editable_col, window, cx);

                cx.notify();
            });
        }
        cx.notify();
    });

    window.push_notification(
        Notification::info("New row added. Tip: open the form editor for a safer full-row insert.")
            .title("New row created")
            .autohide(false)
            .action(move |_notification, _window, _cx| {
                let viewer_weak = viewer_weak.clone();
                Button::new("open-new-row-form")
                    .label("Open Form")
                    .small()
                    .primary()
                    .on_click(move |_, _window, cx| {
                        if let Err(error) = viewer_weak.update(cx, |viewer, cx| {
                            viewer.emit_open_new_row_in_form(cx);
                        }) {
                            tracing::debug!(error = %error, "Skipped opening new-row form after viewer dropped");
                        }
                    })
            }),
        cx,
    );
}

pub(in crate::main_view) fn handle_save_new_row_event(
    request: SaveNewRowRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SaveNewRow event: table={}, new_row_index={}, auto-committing after all required fields filled",
        request.table_name,
        request.new_row_index
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(
            request.connection_id,
            viewer_entity.read(cx).database_name().as_deref(),
        )
    else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let table_name = request.table_name;
    let connection = connection.clone();
    // Convert row_data from editable strings into typed values before insert.
    let row_data: Vec<Option<Value>> = request
        .row_data
        .into_iter()
        .map(|value| Some(Value::String(value)))
        .collect();
    let column_names = request.column_names;
    let window_handle = window.window_handle();
    let connection_id = request.connection_id;
    let new_row_index = request.new_row_index;

    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

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
        if let Err(error) = viewer_entity.update(cx, |viewer, cx| {
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
                tracing::error!(
                    "Failed to insert new row: table={}, error={}",
                    table_name,
                    err
                );
            }
            Ok::<(), anyhow::Error>(())
        }) {
            tracing::debug!(error = %error, "Skipped save-new-row UI update after viewer dropped");
        }

        // Show notifications via window_handle (separate from viewer_entity update)
        if is_success {
            if let Err(error) = window_handle.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::success(format!("New row inserted into {}", table_name)),
                    cx,
                );
            }) {
                tracing::debug!(error = %error, "Skipped save-new-row success notification after window closed");
            }
        } else if let Some(err) = error_message
            && let Err(error) = window_handle.update(cx, |_, window, cx| {
                window.push_notification(
                    Notification::error(format!("Failed to insert new row: {}", err)),
                    cx,
                );
            })
        {
            tracing::debug!(error = %error, "Skipped save-new-row error notification after window closed");
        }

        Ok::<_, anyhow::Error>(())
    })
    .detach();
}

pub(in crate::main_view) fn handle_delete_rows_event(
    connection_id: Uuid,
    table_name: &str,
    all_column_names: &[String],
    rows_to_delete: &[Vec<Value>],
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

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(
            connection_id,
            viewer_entity.read(cx).database_name().as_deref(),
        )
    else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let table_name = table_name.to_string();
    let connection = connection.clone();
    // Get connection name for tab title
    let connection_name = app_state
        .connection_service
        .get_saved_connection_name(connection_id)
        .unwrap_or_else(|| "Unknown".to_string());

    // Capture the is_view state and database_name before loading
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

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
                order_by_clauses = sorts
                    .iter()
                    .map(|sort| sort.to_sql_for_connection(connection.as_ref()))
                    .collect();
            }

            if let Some(search_clause) = build_search_clause_for_columns(
                &connection,
                &resolve_search_columns(
                    &viewer.column_meta,
                    viewer
                        .performance_profile
                        .as_ref()
                        .map(|profile| profile.searchable_columns.clone()),
                ),
                &viewer.search_text,
                false,
            ) {
                where_clauses.push(search_clause);
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
                .delete_rows(
                    connection.clone(),
                    &table_name,
                    schema_qualifier.as_deref(),
                    row_delete_data,
                )
                .await
            {
                Ok(deleted_count) => {
                    tracing::info!("Deleted {} rows successfully", deleted_count);

                    // Refresh the table preserving active filters/sorts/search
                    match table_service
                        .browse_table_with_filters(
                            connection,
                            zqlz_services::BrowseTableWithFiltersRequest {
                                table_name: &table_name,
                                schema: schema_qualifier.as_deref(),
                                where_clauses,
                                order_by_clauses,
                                visible_columns,
                                limit: None,
                                offset: None,
                                cached_total: None,
                            },
                        )
                        .await
                    {
                        Ok(result) => {
                            if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
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
                                    Notification::success(format!(
                                        "{} row(s) deleted",
                                        deleted_count
                                    )),
                                    cx,
                                );
                            }) {
                                tracing::debug!(error = %error, "Skipped delete-rows success UI update after viewer dropped");
                            }
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
                    if let Err(error) = viewer_entity.update_in(cx, |_viewer, window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to delete rows: {}", e)),
                            cx,
                        );
                    }) {
                        tracing::debug!(error = %error, "Skipped delete-rows error notification after viewer dropped");
                    }
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_delete_redis_keys_event(
    connection_id: Uuid,
    all_column_names: &[String],
    rows_to_delete: &[Vec<Value>],
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
        .filter_map(|row| row.get(key_column_index).and_then(|value| value.as_str()))
        .map(ToOwned::to_owned)
        .collect();

    if key_names.is_empty() {
        return;
    }

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let key_value_service = app_state.key_value_service.clone();

    window
        .spawn(cx, async move |cx| {
            let delete_outcome = key_value_service
                .delete_keys(
                    connection,
                    KeyValueDeleteRequest {
                        key_names: key_names.clone(),
                        continue_on_error: true,
                    },
                )
                .await;

            for error in &delete_outcome.errors {
                tracing::error!("Failed to delete Redis key: {}", error);
            }

            let deleted_count = delete_outcome.deleted_key_names.len();
            let first_error = delete_outcome.errors.first().cloned();

            if deleted_count > 0
                && let Err(error) = viewer_entity.update(cx, |viewer, cx| {
                    viewer.refresh(cx);
                    Ok::<(), anyhow::Error>(())
                })
            {
                tracing::debug!(error = %error, "Skipped Redis refresh after viewer dropped");
            }

            if let Err(error) = viewer_entity.update_in(cx, |_viewer, window, cx| {
                if let Some(err) = first_error.clone() {
                    window.push_notification(
                        Notification::error(format!(
                            "Deleted {} of {} key(s), error: {}",
                            deleted_count,
                            key_names.len(),
                            err
                        )),
                        cx,
                    );
                } else {
                    window.push_notification(
                        Notification::success(format!("{} key(s) deleted", deleted_count)),
                        cx,
                    );
                }
            }) {
                tracing::debug!(error = %error, "Skipped Redis delete notification after viewer dropped");
            }

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
    new_rows: Vec<Vec<Value>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<Value>>,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(
            connection_id,
            viewer_entity.read(cx).database_name().as_deref(),
        )
    else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let connection_name = app_state
        .connection_service
        .get_saved_connection_name(connection_id)
        .unwrap_or_else(|| "Unknown".to_string());
    let connection = connection.clone();

    // Capture viewer state before the async spawn
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let driver_category = viewer_entity.read(cx).driver_category;
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    if matches!(driver_category, DriverCategory::Document) {
        handle_document_commit_changes_event(
            connection_id,
            table_name,
            modified_cells,
            deleted_rows,
            new_rows,
            column_meta,
            all_rows,
            viewer_entity,
            window,
            cx,
        );
        return;
    }

    window
        .spawn(cx, async move |cx| {
            let commit_request = CommitTableChangesRequest {
                table_name: table_name.clone(),
                schema: schema_qualifier.clone(),
                column_meta: column_meta.clone(),
                modified_cells: modified_cells
                    .iter()
                    .map(|((row_index, column_index), change)| CommitCellChange {
                        row_index: *row_index,
                        column_index: *column_index,
                        original_value: change.original_value.as_value(),
                        new_value: change.new_value.as_value(),
                    })
                    .collect(),
                deleted_row_indices: deleted_rows.iter().copied().collect(),
                new_rows: new_rows.clone(),
                all_rows: all_rows.clone(),
                auto_increment_placeholder: Some(
                    crate::components::table_viewer::delegate::inline_edit::AUTO_INCREMENT_PLACEHOLDER
                        .to_string(),
                ),
            };

            let commit_outcome = match table_service
                .commit_table_changes(connection.clone(), commit_request)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::error!(%error, "Failed to commit changes via table service");
                    if let Err(update_error) = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(
                                "The table state is inconsistent. Please refresh the table and try again.",
                            ),
                            cx,
                        );
                    }) {
                        tracing::debug!(error = %update_error, "Skipped commit error notification after window closed");
                    }
                    return anyhow::Ok(());
                }
            };

            let successful_operations = commit_outcome.successful_operations;
            let failed_modified_cells = commit_outcome.failed_modified_cells;
            let failed_deleted_rows = commit_outcome.failed_deleted_rows;
            let failed_new_rows = commit_outcome.failed_new_rows;

            let has_failures = !failed_modified_cells.is_empty()
                || !failed_deleted_rows.is_empty()
                || !failed_new_rows.is_empty();

            if !has_failures {
                if let Err(error) = viewer_entity.update(cx, |viewer, cx| {
                    if let Some(table_state) = &viewer.table_state {
                        table_state.update(cx, |table, cx| {
                            table.delegate_mut().clear_pending_changes();
                            cx.notify();
                        });
                    }
                    Ok::<(), anyhow::Error>(())
                }) {
                    tracing::debug!(error = %error, "Skipped pending-change clear after viewer dropped");
                }

                // Refresh table data
                if let Ok(result) = table_service
                    .browse_table(connection, &table_name, schema_qualifier.as_deref(), None, None)
                    .await
                    && let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
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
                    })
                {
                    tracing::debug!(error = %error, "Skipped post-commit table reload after viewer dropped");
                }

                tracing::info!("{} changes committed successfully", successful_operations);
            } else {
                tracing::error!(
                    "Commit partially failed: {} successes, modified_failures={}, delete_failures={}, insert_failures={}",
                    successful_operations,
                    failed_modified_cells.len(),
                    failed_deleted_rows.len(),
                    failed_new_rows.len()
                );

                if let Err(error) = viewer_entity.update(cx, |viewer, cx| {
                    if let Some(table_state) = &viewer.table_state {
                        table_state.update(cx, |table, cx| {
                            table.delegate_mut().restore_failed_commit_state(
                                failed_modified_cells
                                    .iter()
                                    .map(|failure| {
                                        (
                                            failure.original_row_values.clone(),
                                            failure.column_index,
                                            PendingCellChange {
                                                original_value: crate::components::CellValue::from_value(
                                                    &failure.original_value,
                                                ),
                                                new_value: crate::components::CellValue::from_value(
                                                    &failure.new_value,
                                                ),
                                            },
                                        )
                                    })
                                    .collect(),
                                failed_deleted_rows.clone(),
                                failed_new_rows
                                    .iter()
                                    .map(|failure| failure.row_values.clone())
                                    .collect(),
                            );
                            cx.notify();
                        });
                    }
                    Ok::<(), anyhow::Error>(())
                }) {
                    tracing::debug!(error = %error, "Skipped failed-commit state restore after viewer dropped");
                }

                let mut error_messages: Vec<String> = failed_modified_cells
                    .iter()
                    .map(|failure| {
                        format!(
                            "Failed to update column '{}' on an existing row: {}",
                            failure.column_name, failure.error_message
                        )
                    })
                    .collect();

                if !failed_deleted_rows.is_empty() {
                    error_messages.push(format!(
                        "Failed to delete {} row(s)",
                        failed_deleted_rows.len()
                    ));
                }

                error_messages.extend(failed_new_rows.iter().map(|failure| {
                    format!(
                        "Failed to insert new row {}: {}",
                        failure.row_number, failure.error_message
                    )
                }));

                if let Err(error) = cx.update(|window, cx| {
                    window.open_dialog(cx, move |dialog, _window, cx| {
                        dialog
                            .title("Commit Changes Failed")
                            .child(
                                v_flex()
                                    .gap_2()
                                    .child(div().child(format!(
                                        "{} changes succeeded, {} failed:",
                                        successful_operations,
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
                                    // Alerts configure their future OK button through dialog props,
                                    // so the shared dialog keeps a Primary ButtonVariant here.
                                    .ok_variant(ButtonVariant::Primary),
                            )
                            .alert()
                    });
                }) {
                    tracing::debug!(error = %error, "Skipped commit failure dialog after window closed");
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

#[allow(clippy::too_many_arguments)]
pub(in crate::main_view) fn handle_document_commit_changes_event(
    connection_id: Uuid,
    collection_name: String,
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<Value>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<Value>>,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let database_name = viewer_entity.read(cx).database_name().unwrap_or_default();
    if database_name.is_empty() {
        window.push_notification(
            Notification::error("Cannot commit document changes without a database name"),
            cx,
        );
        return;
    }

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(connection_id, Some(database_name.as_str()))
    else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    if !new_rows.is_empty() {
        window.push_notification(
            Notification::warning("Document inserts are not part of batch commit yet"),
            cx,
        );
        return;
    }

    let Some(id_column_index) = column_meta.iter().position(|column| column.name == "_id") else {
        window.push_notification(
            Notification::error("Cannot commit document changes without an _id column"),
            cx,
        );
        return;
    };

    let document_service = app_state.document_service.clone();
    let connection = connection.clone();

    window
        .spawn(cx, async move |cx| {
            let mut errors = Vec::new();
            let mut successful_operations = 0usize;

            for ((row_index, column_index), change) in &modified_cells {
                let Some(row_values) = all_rows.get(*row_index) else {
                    errors.push(format!("Row {} is no longer loaded", row_index + 1));
                    continue;
                };
                let Some(column) = column_meta.get(*column_index) else {
                    errors.push(format!("Column {} is no longer loaded", column_index + 1));
                    continue;
                };
                if column.name == "_id" {
                    errors.push("Cannot edit MongoDB _id values".to_string());
                    continue;
                }
                let Some(id_value) = row_values.get(id_column_index) else {
                    errors.push(format!("Row {} has no _id value", row_index + 1));
                    continue;
                };

                match document_service
                    .update_document_cell(
                        connection.clone(),
                        DocumentCellUpdateRequest {
                            database: database_name.clone(),
                            collection: collection_name.clone(),
                            id_json: document_id_json(id_value),
                            field_path: column.name.clone(),
                            new_value: change.new_value.as_value(),
                        },
                    )
                    .await
                {
                    Ok(()) => successful_operations += 1,
                    Err(error) => errors.push(format!(
                        "Failed to update '{}' on row {}: {}",
                        column.name,
                        row_index + 1,
                        error
                    )),
                }
            }

            if !deleted_rows.is_empty() {
                let ids_json = deleted_rows
                    .iter()
                    .filter_map(|row_index| {
                        all_rows
                            .get(*row_index)
                            .and_then(|row| row.get(id_column_index))
                            .map(document_id_json)
                    })
                    .collect::<Vec<_>>();

                if ids_json.len() != deleted_rows.len() {
                    errors.push("Some selected document rows no longer have _id values".to_string());
                } else {
                    match document_service
                        .delete_documents(
                            connection.clone(),
                            DocumentDeleteRequest {
                                database: database_name.clone(),
                                collection: collection_name.clone(),
                                ids_json,
                                continue_on_error: true,
                            },
                        )
                        .await
                    {
                        Ok(outcome) => {
                            successful_operations += outcome.deleted_ids.len();
                            errors.extend(outcome.errors);
                        }
                        Err(error) => errors.push(format!("Failed to delete documents: {}", error)),
                    }
                }
            }

            if errors.is_empty() {
                viewer_entity.update(cx, |viewer, cx| {
                    if let Some(table_state) = &viewer.table_state {
                        table_state.update(cx, |table, cx| {
                            table.delegate_mut().clear_pending_changes();
                            cx.notify();
                        });
                    }
                    viewer.refresh(cx);
                });

                if let Err(error) = cx.update(|window, cx| {
                    window.push_notification(
                        Notification::success(format!(
                            "Committed {} document change(s)",
                            successful_operations
                        )),
                        cx,
                    );
                }) {
                    tracing::debug!(error = %error, "Skipped document commit notification after window closed");
                }
            } else if let Err(error) = cx.update(|window, cx| {
                window.open_dialog(cx, move |dialog, _window, cx| {
                    dialog
                        .title("Document Commit Failed")
                        .child(
                            v_flex()
                                .gap_2()
                                .child(div().child(format!(
                                    "{} change(s) succeeded, {} failed:",
                                    successful_operations,
                                    errors.len()
                                )))
                                .children(errors.iter().take(5).map(|message| {
                                    div()
                                        .text_sm()
                                        .text_color(cx.theme().danger)
                                        .child(message.clone())
                                })),
                        )
                        .button_props(DialogButtonProps::default().ok_text("OK"))
                        .alert()
                });
            }) {
                tracing::debug!(error = %error, "Skipped document commit failure dialog after window closed");
            }

            anyhow::Ok(())
        })
        .detach();
}

fn document_id_json(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Json(value) => value.to_string(),
        _ => value.to_json_value().to_string(),
    }
}
