//! Cell editor event handling for the main view.
//!
//! This module handles events emitted by the cell editor panel, specifically when users
//! save or cancel cell value changes in database tables. The primary responsibility is to:
//!
//! - Process cell value save requests from the cell editor
//! - Execute database update operations via the table service
//! - Handle both SQL and Redis database updates with appropriate error handling
//! - Update the source table viewer with the new value on success
//! - Rollback changes and show error dialogs on failure
//! - Manage the full lifecycle of cell editing from user input to database persistence

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::{ButtonVariant, ButtonVariants as _},
    dialog::DialogButtonProps,
    v_flex,
};

use crate::app::AppState;
use crate::components::CellEditorEvent;

use super::super::super::MainView;
use super::super::super::table_handlers_utils::conversion::resolve_schema_qualifier;

impl MainView {
    pub(in crate::main_view) fn handle_cell_editor_event(
        &mut self,
        event: CellEditorEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            CellEditorEvent::ValueSaved {
                cell_data,
                new_value,
                source_viewer,
            } => {
                tracing::info!(
                    "Cell editor saved: table={}, column={}, new_value={:?}",
                    cell_data.table_name,
                    cell_data.column_name,
                    new_value
                );

                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };

                let Some(connection) = app_state.connections.get(cell_data.connection_id) else {
                    tracing::error!("Connection not found: {}", cell_data.connection_id);
                    return;
                };

                let table_service = app_state.table_service.clone();
                let table_name = cell_data.table_name.clone();
                let column_name = cell_data.column_name.clone();
                let row_index = cell_data.row_index;
                let col_index = cell_data.col_index;
                let new_value_for_update = new_value.clone();
                let original_value = cell_data.current_value.clone();
                let is_redis = connection.driver_name() == "redis";
                let schema_qualifier = if !is_redis {
                    source_viewer
                        .as_ref()
                        .and_then(|v| {
                            v.read_with(cx, |viewer, _cx| {
                                let db = viewer.database_name();
                                resolve_schema_qualifier(connection.driver_name(), &db)
                            })
                            .ok()
                            .flatten()
                        })
                } else {
                    None
                };
                let connection = connection.clone();

                let cell_update_data = zqlz_services::CellUpdateData {
                    column_name: cell_data.column_name.clone(),
                    new_value,
                    all_column_names: cell_data.all_column_names.clone(),
                    all_row_values: cell_data.all_row_values.clone(),
                    all_column_types: cell_data.all_column_types.clone(),
                };

                cx.spawn_in(window, async move |_this, cx| {
                    // For Redis, we need to get the key type first, then call update_redis_key
                    let update_result = if is_redis {
                        // Query the key type
                        let type_result = connection
                            .query(&format!("TYPE {}", table_name), &[])
                            .await;

                        match type_result {
                            Ok(result) => {
                                let key_type = result
                                    .rows
                                    .first()
                                    .and_then(|r| r.get_by_name("value"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("string")
                                    .to_lowercase();

                                table_service
                                    .update_redis_key(
                                        connection,
                                        &table_name,
                                        &key_type,
                                        cell_update_data,
                                    )
                                    .await
                                    .map_err(|e| anyhow::anyhow!("{}", e))
                            }
                            Err(e) => Err(anyhow::anyhow!("Failed to get key type: {}", e)),
                        }
                    } else {
                        table_service
                            .update_cell(connection, &table_name, schema_qualifier.as_deref(), cell_update_data)
                            .await
                            .map_err(|e| anyhow::anyhow!("{}", e))
                    };

                    match update_result {
                        Ok(()) => {
                            tracing::info!("Cell updated successfully");

                            if let Some(viewer) = source_viewer {
                                _ = viewer.update(cx, |viewer, cx| {
                                    viewer.update_cell_value(
                                        row_index,
                                        col_index,
                                        new_value_for_update.clone(),
                                        cx,
                                    );
                                });
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to update cell: {}", e);

                            // Clean up error message for display
                            let error_detail = e
                                .to_string()
                                .replace("Cell update failed: ", "")
                                .replace("Query error: ", "")
                                .replace("Failed to execute statement: ", "");

                            // Rollback: restore original value in the viewer first
                            if let Some(ref viewer) = source_viewer {
                                _ = viewer.update(cx, |viewer, cx| {
                                    viewer.update_cell_value(
                                        row_index,
                                        col_index,
                                        original_value.clone(),
                                        cx,
                                    );
                                });
                            }

                            // Show error dialog - user must acknowledge
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
            CellEditorEvent::Cancelled => {
                tracing::debug!("Cell editor cancelled");
            }
        }
    }
}
