//! Key-value editor event handling for Redis and row editing operations.
//!
//! This module handles events from the key-value editor panel, including:
//! - Saving and updating Redis keys with various data types (String, List, Set, ZSet, Hash, Stream)
//! - Renaming keys and managing TTL values
//! - Deleting keys
//! - Saving new or existing table rows
//! - Syncing field changes between row editor and table grid

use gpui::*;
use zqlz_core::{
    DocumentCellUpdateRequest, DocumentSaveRequest, DriverCategory, KeyValueDeleteRequest,
    KeyValueKind, KeyValueSaveRequest, Value,
};
use zqlz_services::RowInsertData;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{KeyValueEditorEvent, RedisValueType, TableViewerPanel};
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::workspace::WorkspaceController;

fn map_redis_value_type(value_type: RedisValueType) -> KeyValueKind {
    match value_type {
        RedisValueType::String => KeyValueKind::String,
        RedisValueType::List => KeyValueKind::List,
        RedisValueType::Set => KeyValueKind::Set,
        RedisValueType::ZSet => KeyValueKind::ZSet,
        RedisValueType::Hash => KeyValueKind::Hash,
        RedisValueType::Stream => KeyValueKind::Stream,
        RedisValueType::Json => KeyValueKind::Json,
    }
}

fn document_id_json(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Json(value) => value.to_string(),
        _ => value.to_json_value().to_string(),
    }
}

fn is_object_id_column_type(column_type: &str) -> bool {
    matches!(
        column_type
            .trim()
            .to_lowercase()
            .split_once('(')
            .map(|(base, _)| base.trim().to_string())
            .unwrap_or_else(|| column_type.trim().to_lowercase())
            .as_str(),
        "objectid" | "object_id"
    )
}

fn is_valid_object_id_string(input: &str) -> bool {
    let input = input.trim();
    input.len() == 24 && input.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn document_insert_json_value(column_type: &str, value: &Value) -> serde_json::Value {
    if is_object_id_column_type(column_type)
        && let Value::String(value) = value
        && is_valid_object_id_string(value)
    {
        return serde_json::json!({ "$oid": value.trim() });
    }

    value.to_json_value()
}

fn refresh_active_table_viewer(workspace_controller: &Entity<WorkspaceController>, cx: &mut App) {
    if let Some(viewer) = workspace_controller
        .read(cx)
        .active_center_view::<TableViewerPanel>(cx)
    {
        viewer.update(cx, |viewer, cx| {
            viewer.refresh(cx);
        });
    }
}

impl MainView {
    pub(in crate::main_view) fn handle_key_value_editor_event(
        &mut self,
        event: KeyValueEditorEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            KeyValueEditorEvent::ValueSaved {
                original_key,
                new_key,
                connection_id,
                database_name,
                value_type,
                new_value,
                new_ttl,
            } => {
                let is_rename = original_key != new_key && !original_key.is_empty();
                tracing::info!(
                    "Key-value editor saved: original_key={}, new_key={}, type={:?}, new_ttl={:?}, is_rename={}",
                    original_key,
                    new_key,
                    value_type,
                    new_ttl,
                    is_rename
                );

                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };

                let Some(connection) = app_state
                    .connections
                    .get_for_database_cached(connection_id, database_name.as_deref())
                else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let connection = connection.clone();
                let key_value_service = app_state.key_value_service.clone();
                let key = new_key.clone();
                let workspace_controller = self.workspace_controller.clone();
                let key_value_editor_panel = self.key_value_editor_panel.clone();
                let window_handle = window.window_handle();
                let redis_value_type = map_redis_value_type(value_type);
                let original_key_for_status = original_key.clone();
                let new_key_for_status = new_key.clone();
                let new_value_for_status = new_value.clone();

                cx.spawn_in(window, async move |_this, cx| {
                    let temporary_key = format!(
                        "__zqlz_tmp__:{}:{}",
                        connection_id,
                        uuid::Uuid::new_v4()
                    );
                    let result = key_value_service
                        .save_key(
                            connection,
                            KeyValueSaveRequest {
                                original_key,
                                new_key,
                                kind: redis_value_type,
                                serialized_value: new_value,
                                ttl_seconds: new_ttl,
                                temporary_key,
                            },
                        )
                        .await;

                    match result {
                        Ok(_outcome) => {
                            tracing::info!("Redis key '{}' updated successfully", key);
                            key_value_editor_panel.update(cx, |editor, cx| {
                                editor.mark_redis_save_succeeded(
                                    &original_key_for_status,
                                    new_key_for_status,
                                    value_type,
                                    new_value_for_status,
                                    new_ttl,
                                    cx,
                                );
                            });
                            cx.update(|_window, cx| {
                                refresh_active_table_viewer(&workspace_controller, cx);
                            })?;
                        }
                        Err(error) => {
                            tracing::error!("Failed to update Redis key '{}': {}", key, error);
                            key_value_editor_panel.update(cx, |editor, cx| {
                                editor.mark_redis_save_failed(
                                    &original_key_for_status,
                                    error.to_string(),
                                    cx,
                                );
                            });
                            if let Err(update_error) = cx.update_window(window_handle, |_, window, cx| {
                                window.push_notification(
                                    Notification::error(format!(
                                        "Failed to update Redis key '{}': {}",
                                        key, error
                                    )),
                                    cx,
                                );
                            }) {
                                tracing::warn!(%update_error, "Failed to show Redis key save error notification");
                            }
                        }
                    }

                    anyhow::Ok(())
                })
                .detach();
            }
            KeyValueEditorEvent::Cancelled => {
                tracing::debug!("Key-value editor cancelled");
            }
            KeyValueEditorEvent::Deleted {
                key,
                connection_id,
                database_name,
            } => {
                tracing::info!("Key-value editor delete: key={}", key);

                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };

                let Some(connection) = app_state
                    .connections
                    .get_for_database_cached(connection_id, database_name.as_deref())
                else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let connection = connection.clone();
                let key_value_service = app_state.key_value_service.clone();
                let key_for_delete = key.clone();
                let workspace_controller = self.workspace_controller.clone();
                let key_value_editor_panel = self.key_value_editor_panel.clone();

                cx.spawn_in(window, async move |_this, cx| {
                    let result = key_value_service
                        .delete_keys(
                            connection,
                            KeyValueDeleteRequest {
                                key_names: vec![key_for_delete.clone()],
                                continue_on_error: false,
                            },
                        )
                        .await;

                    match result {
                        outcome if outcome.errors.is_empty() => {
                            tracing::info!("Redis key '{}' deleted successfully", key_for_delete);
                            key_value_editor_panel.update(cx, |editor, cx| {
                                editor.mark_redis_delete_succeeded(&key_for_delete, cx);
                            });
                            cx.update(|_window, cx| {
                                refresh_active_table_viewer(&workspace_controller, cx);
                            })?;
                        }
                        outcome => {
                            let error = outcome
                                .errors
                                .first()
                                .cloned()
                                .unwrap_or_else(|| "unknown error".to_string());
                            tracing::error!(
                                "Failed to delete Redis key '{}': {}",
                                key_for_delete,
                                error
                            );
                            key_value_editor_panel.update(cx, |editor, cx| {
                                editor.mark_redis_delete_failed(
                                    &key_for_delete,
                                    format!(
                                        "Failed to delete Redis key '{}': {}",
                                        key_for_delete, error
                                    ),
                                    cx,
                                );
                            });
                        }
                    }

                    anyhow::Ok(())
                })
                .detach();
            }
            KeyValueEditorEvent::RowSaved {
                table_name,
                connection_id,
                column_names,
                column_types,
                values: _,
                typed_values,
                is_new,
                row_index: _,
                source_viewer,
                original_row_values,
            } => {
                tracing::info!(
                    "Row editor saved: table={}, is_new={}, columns={}",
                    table_name,
                    is_new,
                    column_names.len()
                );

                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };

                let database_name = source_viewer
                    .as_ref()
                    .and_then(|v| v.read_with(cx, |viewer, _cx| viewer.database_name()).ok())
                    .flatten();

                let Some(connection) = app_state
                    .connections
                    .get_for_database_cached(connection_id, database_name.as_deref())
                else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let table_service = app_state.table_service.clone();
                let document_service = app_state.document_service.clone();
                let schema_qualifier = source_viewer.as_ref().and_then(|v| {
                    v.read_with(cx, |viewer, _cx| {
                        let db = viewer.database_name();
                        resolve_schema_qualifier(&connection, &db)
                    })
                    .ok()
                    .flatten()
                });
                let connection = connection.clone();
                let table_name = table_name.clone();
                let column_names = column_names.clone();
                let column_types = column_types.clone();
                let original_row_values = original_row_values.clone();
                let source_viewer = source_viewer.clone();
                let window_handle = window.window_handle();
                let column_types_for_updates = column_types.clone();
                let driver_category = connection.driver_category();

                cx.spawn(async move |_this, cx| {
                    let result = if matches!(driver_category, DriverCategory::Document) {
                        match database_name.clone() {
                            Some(database_name) if is_new => {
                                let document = column_names
                                    .iter()
                                    .zip(column_types.iter())
                                    .zip(typed_values.iter())
                                    .filter(|(_, value)| !value.is_null())
                                    .map(|((column_name, column_type), value)| {
                                        (
                                            column_name.clone(),
                                            document_insert_json_value(column_type, value),
                                        )
                                    })
                                    .collect::<serde_json::Map<_, _>>();
                                document_service
                                    .insert_document(
                                        connection.clone(),
                                        DocumentSaveRequest {
                                            database: database_name,
                                            collection: table_name.clone(),
                                            document_json: serde_json::Value::Object(document)
                                                .to_string(),
                                        },
                                    )
                                    .await
                                    .map(|_| ())
                            }
                            Some(database_name) => {
                                let id_column_index = column_names
                                    .iter()
                                    .position(|column_name| column_name == "_id");
                                match id_column_index.and_then(|id_column_index| {
                                    original_row_values.get(id_column_index)
                                }) {
                                    Some(id_value) => {
                                        let mut update_error: Option<String> = None;

                                        for (col_index, new_value) in
                                            typed_values.iter().enumerate()
                                        {
                                            let Some(col_name) = column_names.get(col_index) else {
                                                continue;
                                            };
                                            if col_name == "_id" {
                                                continue;
                                            }
                                            let original = original_row_values
                                                .get(col_index)
                                                .cloned()
                                                .unwrap_or_default();
                                            if *new_value == original {
                                                continue;
                                            }

                                            if let Err(error) = document_service
                                                .update_document_cell(
                                                    connection.clone(),
                                                    DocumentCellUpdateRequest {
                                                        database: database_name.clone(),
                                                        collection: table_name.clone(),
                                                        id_json: document_id_json(id_value),
                                                        field_path: col_name.clone(),
                                                        new_value: new_value.clone(),
                                                    },
                                                )
                                                .await
                                            {
                                                update_error = Some(format!(
                                                    "Failed to update column '{}': {}",
                                                    col_name, error
                                                ));
                                                break;
                                            }
                                        }

                                        match update_error {
                                            Some(error) => Err(
                                                zqlz_services::ServiceError::UpdateFailed(error),
                                            ),
                                            None => Ok(()),
                                        }
                                    }
                                    None => Err(zqlz_services::ServiceError::UpdateFailed(
                                        "Row has no _id value".to_string(),
                                    )),
                                }
                            }
                            None => Err(zqlz_services::ServiceError::UpdateFailed(
                                "Document database context is unavailable".to_string(),
                            )),
                        }
                    } else if is_new {
                        table_service
                            .insert_row(
                                connection.clone(),
                                &table_name,
                                schema_qualifier.as_deref(),
                                RowInsertData {
                                    column_names: column_names.clone(),
                                    values: typed_values
                                        .iter()
                                        .map(|value| {
                                            if value.is_null() {
                                                None
                                            } else {
                                                Some(value.clone())
                                            }
                                        })
                                        .collect(),
                                    column_types: column_types.clone(),
                                },
                            )
                            .await
                    } else {
                        // For updates, update each changed column individually
                        // using the existing cell-level update API
                        let mut update_error: Option<String> = None;

                        for (col_index, new_value) in typed_values.iter().enumerate() {
                            let Some(col_name) = column_names.get(col_index) else {
                                continue;
                            };
                            let original = original_row_values
                                .get(col_index)
                                .cloned()
                                .unwrap_or_default();

                            // Determine if this column actually changed
                            let changed = *new_value != original;

                            if !changed {
                                continue;
                            }

                            let cell_update = zqlz_services::CellUpdateData {
                                column_name: col_name.clone(),
                                new_value: Some(new_value.clone()).filter(|value| !value.is_null()),
                                all_column_names: column_names.clone(),
                                all_row_values: original_row_values.clone(),
                                all_column_types: column_types_for_updates.clone(),
                            };

                            if let Err(e) = table_service
                                .update_cell(
                                    connection.clone(),
                                    &table_name,
                                    schema_qualifier.as_deref(),
                                    cell_update,
                                )
                                .await
                            {
                                update_error =
                                    Some(format!("Failed to update column '{}': {}", col_name, e));
                                break;
                            }
                        }

                        match update_error {
                            Some(err) => Err(zqlz_services::ServiceError::UpdateFailed(err)),
                            None => Ok(()),
                        }
                    };

                    let is_success = result.is_ok();
                    let error_message = result.err().map(|e| e.to_string());
                    let action = if is_new { "inserted" } else { "updated" };

                    if is_success {
                        tracing::info!("Row {} successfully: table={}", action, table_name);

                        // Refresh source viewer
                        if let Some(viewer) = &source_viewer {
                            _ = viewer.update(cx, |viewer, cx| {
                                viewer.refresh(cx);
                            });
                        }
                    }

                    // Show notification
                    if is_success {
                        _ = window_handle.update(cx, |_, window, cx| {
                            window.push_notification(
                                Notification::success(format!("Row {} in {}", action, table_name)),
                                cx,
                            );
                        });
                    } else if let Some(err) = error_message {
                        tracing::error!("Failed to save row: table={}, error={}", table_name, err);
                        _ = window_handle.update(cx, |_, window, cx| {
                            window.push_notification(
                                Notification::error(format!(
                                    "Failed to {} row: {}",
                                    if is_new { "insert" } else { "update" },
                                    err
                                )),
                                cx,
                            );
                        });
                    }

                    Ok::<_, anyhow::Error>(())
                })
                .detach();
            }
            KeyValueEditorEvent::FieldChanged {
                col_index,
                new_value,
                typed_value,
                is_null,
                row_index,
                source_viewer,
            } => {
                // Sync the field change from the row editor back to the table grid
                if let (Some(viewer), Some(row_index)) = (&source_viewer, row_index) {
                    let display_value = if is_null {
                        Value::Null
                    } else if matches!(typed_value, Value::Null) {
                        Value::String(new_value)
                    } else {
                        typed_value.clone()
                    };
                    _ = viewer.update(cx, |panel, cx| {
                        panel.update_cell_value(row_index, col_index, display_value, cx);
                    });
                }
            }
        }
    }
}
