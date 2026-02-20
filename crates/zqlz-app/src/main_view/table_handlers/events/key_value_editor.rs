//! Key-value editor event handling for Redis and row editing operations.
//!
//! This module handles events from the key-value editor panel, including:
//! - Saving and updating Redis keys with various data types (String, List, Set, ZSet, Hash, Stream)
//! - Renaming keys and managing TTL values
//! - Deleting keys
//! - Saving new or existing table rows
//! - Syncing field changes between row editor and table grid

use gpui::*;
use zqlz_core::StatementResult;
use zqlz_services::RowInsertData;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{KeyValueEditorEvent, RedisValueType, TableViewerPanel};
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::{
    conversion::resolve_schema_qualifier,
    formatting::escape_redis_value,
};

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

                let Some(connection) = app_state.connections.get(connection_id) else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let connection = connection.clone();
                let key = new_key.clone();
                let dock_area = self.dock_area.clone();

                cx.spawn_in(window, async move |_this, cx| {
                    // If this is a rename operation, delete the old key first
                    if is_rename {
                        tracing::info!("Renaming key from '{}' to '{}'", original_key, key);
                        let del_result = connection
                            .execute(&format!("DEL {}", original_key), &[])
                            .await;
                        if let Err(e) = del_result {
                            tracing::error!(
                                "Failed to delete old key '{}' during rename: {}",
                                original_key,
                                e
                            );
                            // Continue anyway - the new key will still be created
                        }
                    }

                    // Build the SET command based on value type
                    let result = match value_type {
                        RedisValueType::String | RedisValueType::Json => {
                            // For string types, use SET command
                            let mut cmd = format!("SET {} {}", key, escape_redis_value(&new_value));
                            if let Some(ttl) = new_ttl {
                                cmd.push_str(&format!(" EX {}", ttl));
                            }
                            connection.execute(&cmd, &[]).await
                        }
                        RedisValueType::List => {
                            // For lists, we need to delete and re-add all items
                            // First delete the existing key
                            // DEL failures are intentionally ignored: if the key doesn't exist,
                            // the subsequent write still creates it correctly.
                            let _ = connection.execute(&format!("DEL {}", key), &[]).await;

                            // Parse the value as JSON array or line-separated values
                            let items: Vec<String> = if new_value.trim().starts_with('[') {
                                serde_json::from_str(&new_value).unwrap_or_else(|_| {
                                    new_value.lines().map(|s| s.to_string()).collect()
                                })
                            } else {
                                new_value.lines().map(|s| s.to_string()).collect()
                            };

                            // Add items using RPUSH
                            if !items.is_empty() {
                                let escaped_items: Vec<String> =
                                    items.iter().map(|i| escape_redis_value(i)).collect();
                                let cmd = format!("RPUSH {} {}", key, escaped_items.join(" "));
                                connection.execute(&cmd, &[]).await
                            } else {
                                // No items to add, return empty result
                                Ok(StatementResult {
                                    is_query: false,
                                    result: None,
                                    affected_rows: 0,
                                    error: None,
                                })
                            }
                        }
                        RedisValueType::Set => {
                            // For sets, delete and re-add
                            // DEL failures are intentionally ignored: a missing key is fine; the SADD below creates it.
                            let _ = connection.execute(&format!("DEL {}", key), &[]).await;

                            let items: Vec<String> = if new_value.trim().starts_with('[') {
                                serde_json::from_str(&new_value).unwrap_or_else(|_| {
                                    new_value.lines().map(|s| s.to_string()).collect()
                                })
                            } else {
                                new_value.lines().map(|s| s.to_string()).collect()
                            };

                            if !items.is_empty() {
                                let escaped_items: Vec<String> =
                                    items.iter().map(|i| escape_redis_value(i)).collect();
                                let cmd = format!("SADD {} {}", key, escaped_items.join(" "));
                                connection.execute(&cmd, &[]).await
                            } else {
                                Ok(StatementResult {
                                    is_query: false,
                                    result: None,
                                    affected_rows: 0,
                                    error: None,
                                })
                            }
                        }
                        RedisValueType::ZSet => {
                            // For sorted sets, parse as JSON object with score:member pairs
                            // DEL failures are intentionally ignored: a missing key is fine; the ZADD below creates it.
                            let _ = connection.execute(&format!("DEL {}", key), &[]).await;

                            // Try to parse as JSON object {"member": score, ...} or array of [score, member]
                            let items: Vec<(f64, String)> = if let Ok(obj) =
                                serde_json::from_str::<serde_json::Value>(&new_value)
                            {
                                if let Some(obj) = obj.as_object() {
                                    obj.iter()
                                        .filter_map(|(k, v)| {
                                            v.as_f64().map(|score| (score, k.clone()))
                                        })
                                        .collect()
                                } else if let Some(arr) = obj.as_array() {
                                    arr.chunks(2)
                                        .filter_map(|chunk| {
                                            if chunk.len() == 2 {
                                                let score = chunk[0].as_f64()?;
                                                let member = chunk[1].as_str()?.to_string();
                                                Some((score, member))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect()
                                } else {
                                    Vec::new()
                                }
                            } else {
                                Vec::new()
                            };

                            if !items.is_empty() {
                                let args: Vec<String> = items
                                    .iter()
                                    .flat_map(|(score, member)| {
                                        vec![score.to_string(), escape_redis_value(member)]
                                    })
                                    .collect();
                                let cmd = format!("ZADD {} {}", key, args.join(" "));
                                connection.execute(&cmd, &[]).await
                            } else {
                                Ok(StatementResult {
                                    is_query: false,
                                    result: None,
                                    affected_rows: 0,
                                    error: None,
                                })
                            }
                        }
                        RedisValueType::Hash => {
                            // For hashes, parse as JSON object
                            // DEL failures are intentionally ignored: a missing key is fine; the HSET below creates it.
                            let _ = connection.execute(&format!("DEL {}", key), &[]).await;

                            let fields: Vec<(String, String)> = if let Ok(obj) =
                                serde_json::from_str::<serde_json::Value>(&new_value)
                            {
                                if let Some(obj) = obj.as_object() {
                                    obj.iter()
                                        .map(|(k, v)| {
                                            let val = match v {
                                                serde_json::Value::String(s) => s.clone(),
                                                _ => v.to_string(),
                                            };
                                            (k.clone(), val)
                                        })
                                        .collect()
                                } else {
                                    Vec::new()
                                }
                            } else {
                                Vec::new()
                            };

                            if !fields.is_empty() {
                                let args: Vec<String> = fields
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        vec![escape_redis_value(k), escape_redis_value(v)]
                                    })
                                    .collect();
                                let cmd = format!("HSET {} {}", key, args.join(" "));
                                connection.execute(&cmd, &[]).await
                            } else {
                                Ok(StatementResult {
                                    is_query: false,
                                    result: None,
                                    affected_rows: 0,
                                    error: None,
                                })
                            }
                        }
                        RedisValueType::Stream => {
                            // Streams are append-only, just add a new entry
                            let cmd = format!(
                                "XADD {} * message {}",
                                key,
                                escape_redis_value(&new_value)
                            );
                            connection.execute(&cmd, &[]).await
                        }
                    };

                    // Set or remove TTL if value was set successfully
                    if result.is_ok() {
                        match new_ttl {
                            Some(ttl) => {
                                // Set the TTL — EXPIRE failures are intentionally ignored; the value write succeeded.
                                let _ = connection
                                    .execute(&format!("EXPIRE {} {}", key, ttl), &[])
                                    .await;
                            }
                            None => {
                                // Remove TTL (make key persistent) — PERSIST failures are intentionally ignored.
                                let _ = connection.execute(&format!("PERSIST {}", key), &[]).await;
                            }
                        }
                    }

                    match result {
                        Ok(_) => {
                            tracing::info!("Redis key '{}' updated successfully", key);
                            // Refresh the active TableViewer to show updated data
                            _ = dock_area.update_in(cx, |dock_area, _window, cx| {
                                if let Some(panel) = dock_area.active_panel(cx) {
                                    if panel.panel_name(cx) == "TableViewer" {
                                        if let Ok(viewer) =
                                            panel.view().downcast::<TableViewerPanel>()
                                        {
                                            viewer.update(cx, |viewer, cx| {
                                                viewer.refresh(cx);
                                            });
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to update Redis key '{}': {}", key, e);
                        }
                    }

                    anyhow::Ok(())
                })
                .detach();
            }
            KeyValueEditorEvent::Cancelled => {
                tracing::debug!("Key-value editor cancelled");
            }
            KeyValueEditorEvent::Deleted { key, connection_id } => {
                tracing::info!("Key-value editor delete: key={}", key);

                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };

                let Some(connection) = app_state.connections.get(connection_id) else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let connection = connection.clone();
                let key_for_delete = key.clone();
                let dock_area = self.dock_area.clone();

                cx.spawn_in(window, async move |_this, cx| {
                    let result = connection
                        .execute(&format!("DEL {}", key_for_delete), &[])
                        .await;

                    match result {
                        Ok(_) => {
                            tracing::info!("Redis key '{}' deleted successfully", key_for_delete);
                            // Refresh the active TableViewer to show updated data
                            _ = dock_area.update_in(cx, |dock_area, _window, cx| {
                                if let Some(panel) = dock_area.active_panel(cx) {
                                    if panel.panel_name(cx) == "TableViewer" {
                                        if let Ok(viewer) =
                                            panel.view().downcast::<TableViewerPanel>()
                                        {
                                            viewer.update(cx, |viewer, cx| {
                                                viewer.refresh(cx);
                                            });
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to delete Redis key '{}': {}",
                                key_for_delete,
                                e
                            );
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
                values,
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

                let Some(connection) = app_state.connections.get(connection_id) else {
                    tracing::error!("Connection not found: {}", connection_id);
                    return;
                };

                let table_service = app_state.table_service.clone();
                let schema_qualifier = source_viewer
                    .as_ref()
                    .and_then(|v| {
                        v.read_with(cx, |viewer, _cx| {
                            let db = viewer.database_name();
                            resolve_schema_qualifier(connection.driver_name(), &db)
                        })
                        .ok()
                        .flatten()
                    });
                let connection = connection.clone();
                let table_name = table_name.clone();
                let column_names = column_names.clone();
                let column_types = column_types.clone();
                let values = values.clone();
                let original_row_values = original_row_values.clone();
                let source_viewer = source_viewer.clone();
                let window_handle = window.window_handle();
                let column_types_for_updates = column_types.clone();

                cx.spawn(async move |_this, cx| {
                    let result = if is_new {
                        table_service
                            .insert_row(
                                connection.clone(),
                                &table_name,
                                schema_qualifier.as_deref(),
                                RowInsertData {
                                    column_names: column_names.clone(),
                                    values,
                                    column_types: column_types.clone(),
                                },
                            )
                            .await
                    } else {
                        // For updates, update each changed column individually
                        // using the existing cell-level update API
                        let mut update_error: Option<String> = None;

                        for (col_index, new_value) in values.iter().enumerate() {
                            let Some(col_name) = column_names.get(col_index) else {
                                continue;
                            };
                            let original = original_row_values.get(col_index)
                                .cloned()
                                .unwrap_or_default();

                            // Determine if this column actually changed
                            let changed = match new_value {
                                None => original != "NULL",
                                Some(val) => *val != original,
                            };

                            if !changed {
                                continue;
                            }

                            let cell_update = zqlz_services::CellUpdateData {
                                column_name: col_name.clone(),
                                new_value: new_value.clone(),
                                all_column_names: column_names.clone(),
                                all_row_values: original_row_values.clone(),
                                all_column_types: column_types_for_updates.clone(),
                            };

                            if let Err(e) = table_service
                                .update_cell(connection.clone(), &table_name, schema_qualifier.as_deref(), cell_update)
                                .await
                            {
                                update_error = Some(format!(
                                    "Failed to update column '{}': {}",
                                    col_name, e
                                ));
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
                                Notification::success(&format!(
                                    "Row {} in {}",
                                    action, table_name
                                )),
                                cx,
                            );
                        });
                    } else if let Some(err) = error_message {
                        tracing::error!("Failed to save row: table={}, error={}", table_name, err);
                        _ = window_handle.update(cx, |_, window, cx| {
                            window.push_notification(
                                Notification::error(&format!(
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
                is_null,
                row_index,
                source_viewer,
            } => {
                // Sync the field change from the row editor back to the table grid
                if let (Some(viewer), Some(row_index)) = (&source_viewer, row_index) {
                    let display_value = if is_null {
                        "NULL".to_string()
                    } else {
                        new_value.clone()
                    };
                    _ = viewer.update(cx, |panel, cx| {
                        panel.update_cell_value(row_index, col_index, Some(display_value), cx);
                    });
                }
            }
        }
    }
}
