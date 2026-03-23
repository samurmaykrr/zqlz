//! Key-value editor event handling for Redis and row editing operations.
//!
//! This module handles events from the key-value editor panel, including:
//! - Saving and updating Redis keys with various data types (String, List, Set, ZSet, Hash, Stream)
//! - Renaming keys and managing TTL values
//! - Deleting keys
//! - Saving new or existing table rows
//! - Syncing field changes between row editor and table grid

use gpui::*;
use std::sync::Arc;
use zqlz_core::{Connection, StatementResult, Value};
use zqlz_services::RowInsertData;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{KeyValueEditorEvent, RedisValueType, TableViewerPanel};
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::{
    conversion::resolve_schema_qualifier, formatting::escape_redis_value,
};

async fn execute_redis_command(
    connection: &Arc<dyn Connection>,
    command: String,
) -> anyhow::Result<StatementResult> {
    connection.execute(&command, &[]).await.map_err(Into::into)
}

fn empty_collection_save_error(value_type: RedisValueType) -> anyhow::Error {
    anyhow::anyhow!(match value_type {
        RedisValueType::List => {
            "Cannot save an empty Redis list. Add an element or delete the key explicitly."
        }
        RedisValueType::Set => {
            "Cannot save an empty Redis set. Add an element or delete the key explicitly."
        }
        RedisValueType::ZSet => {
            "Cannot save an empty Redis sorted set. Add a member or delete the key explicitly."
        }
        RedisValueType::Hash => {
            "Cannot save an empty Redis hash. Add a field or delete the key explicitly."
        }
        _ => "Cannot save an empty Redis collection.",
    })
}

fn parse_collection_items(new_value: &str) -> Vec<String> {
    if new_value.trim().starts_with('[') {
        serde_json::from_str(new_value)
            .unwrap_or_else(|_| new_value.lines().map(|value| value.to_string()).collect())
    } else {
        new_value.lines().map(|value| value.to_string()).collect()
    }
}

fn parse_zset_items(new_value: &str) -> anyhow::Result<Vec<(f64, String)>> {
    let value = serde_json::from_str::<serde_json::Value>(new_value)?;

    if let Some(map) = value.as_object() {
        let mut items = Vec::with_capacity(map.len());
        for (member, score_value) in map {
            let score = score_value.as_f64().ok_or_else(|| {
                anyhow::anyhow!("Invalid sorted set score for '{}': {}", member, score_value)
            })?;
            if !score.is_finite() {
                return Err(anyhow::anyhow!(
                    "Invalid sorted set score for '{}': {}",
                    member,
                    score_value
                ));
            }
            items.push((score, member.clone()));
        }
        return Ok(items);
    }

    if let Some(array) = value.as_array() {
        let mut items = Vec::new();
        for chunk in array.chunks(2) {
            if chunk.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid sorted set payload: expected score/member pairs"
                ));
            }

            let score = chunk[0]
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("Invalid sorted set score: {}", chunk[0]))?;
            if !score.is_finite() {
                return Err(anyhow::anyhow!("Invalid sorted set score: {}", chunk[0]));
            }

            let member = chunk[1]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid sorted set member: {}", chunk[1]))?;
            items.push((score, member.to_string()));
        }
        return Ok(items);
    }

    Err(anyhow::anyhow!(
        "Invalid sorted set payload: expected JSON object or array"
    ))
}

fn parse_hash_fields(new_value: &str) -> anyhow::Result<Vec<(String, String)>> {
    let value = serde_json::from_str::<serde_json::Value>(new_value)?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("Invalid hash payload: expected JSON object"))?;

    Ok(object
        .iter()
        .map(|(field, value)| {
            let value = match value {
                serde_json::Value::String(string) => string.clone(),
                _ => value.to_string(),
            };
            (field.clone(), value)
        })
        .collect())
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
                let key = new_key.clone();
                let dock_area = self.dock_area.clone();
                let window_handle = window.window_handle();

                cx.spawn_in(window, async move |_this, cx| {
                    let escaped_key = escape_redis_value(&key);
                    let temporary_key =
                        format!("__zqlz_tmp__:{}:{}", connection_id, uuid::Uuid::new_v4());
                    let escaped_temporary_key = escape_redis_value(&temporary_key);

                    let temp_key_created = match value_type {
                        RedisValueType::String | RedisValueType::Json => {
                            let cmd = format!(
                                "SET {} {}",
                                escaped_temporary_key,
                                escape_redis_value(&new_value)
                            );
                            execute_redis_command(&connection, cmd).await?;
                            true
                        }
                        RedisValueType::List => {
                            let items = parse_collection_items(&new_value);

                            if !items.is_empty() {
                                let escaped_items: Vec<String> =
                                    items.iter().map(|i| escape_redis_value(i)).collect();
                                let cmd = format!(
                                    "RPUSH {} {}",
                                    escaped_temporary_key,
                                    escaped_items.join(" ")
                                );
                                execute_redis_command(&connection, cmd).await?;
                                true
                            } else {
                                return Err(empty_collection_save_error(value_type));
                            }
                        }
                        RedisValueType::Set => {
                            let items = parse_collection_items(&new_value);

                            if !items.is_empty() {
                                let escaped_items: Vec<String> =
                                    items.iter().map(|i| escape_redis_value(i)).collect();
                                let cmd = format!(
                                    "SADD {} {}",
                                    escaped_temporary_key,
                                    escaped_items.join(" ")
                                );
                                execute_redis_command(&connection, cmd).await?;
                                true
                            } else {
                                return Err(empty_collection_save_error(value_type));
                            }
                        }
                        RedisValueType::ZSet => {
                            let items = parse_zset_items(&new_value)?;

                            if !items.is_empty() {
                                let args: Vec<String> = items
                                    .iter()
                                    .flat_map(|(score, member)| {
                                        vec![score.to_string(), escape_redis_value(member)]
                                    })
                                    .collect();
                                let cmd =
                                    format!("ZADD {} {}", escaped_temporary_key, args.join(" "));
                                execute_redis_command(&connection, cmd).await?;
                                true
                            } else {
                                return Err(empty_collection_save_error(value_type));
                            }
                        }
                        RedisValueType::Hash => {
                            let fields = parse_hash_fields(&new_value)?;

                            if !fields.is_empty() {
                                let args: Vec<String> = fields
                                    .iter()
                                    .flat_map(|(k, v)| {
                                        vec![escape_redis_value(k), escape_redis_value(v)]
                                    })
                                    .collect();
                                let cmd =
                                    format!("HSET {} {}", escaped_temporary_key, args.join(" "));
                                execute_redis_command(&connection, cmd).await?;
                                true
                            } else {
                                return Err(empty_collection_save_error(value_type));
                            }
                        }
                        RedisValueType::Stream => {
                            let cmd = format!(
                                "XADD {} * message {}",
                                escaped_temporary_key,
                                escape_redis_value(&new_value)
                            );
                            execute_redis_command(&connection, cmd).await?;
                            true
                        }
                    };

                    let result: anyhow::Result<()> = if temp_key_created {
                        match new_ttl {
                            Some(ttl) => {
                                execute_redis_command(
                                    &connection,
                                    format!("EXPIRE {} {}", escaped_temporary_key, ttl),
                                )
                                .await?;
                            }
                            None => {
                                execute_redis_command(
                                    &connection,
                                    format!("PERSIST {}", escaped_temporary_key),
                                )
                                .await?;
                            }
                        }

                        execute_redis_command(
                            &connection,
                            format!("RENAME {} {}", escaped_temporary_key, escaped_key),
                        )
                        .await?;

                        if is_rename {
                            let escaped_original_key = escape_redis_value(&original_key);
                            execute_redis_command(
                                &connection,
                                format!("DEL {}", escaped_original_key),
                            )
                            .await?;
                        }

                        Ok(())
                    } else {
                        Ok(())
                    };

                    match result {
                        Ok(_) => {
                            tracing::info!("Redis key '{}' updated successfully", key);
                            // Refresh the active TableViewer to show updated data
                            _ = dock_area.update_in(cx, |dock_area, _window, cx| {
                                if let Some(panel) = dock_area.active_panel(cx)
                                    && panel.panel_name(cx) == "TableViewer"
                                    && let Ok(viewer) = panel.view().downcast::<TableViewerPanel>()
                                {
                                    viewer.update(cx, |viewer, cx| {
                                        viewer.refresh(cx);
                                    });
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to update Redis key '{}': {}", key, e);
                            _ = cx.update_window(window_handle, |_, window, cx| {
                                window.push_notification(
                                    Notification::error(format!(
                                        "Failed to update Redis key '{}': {}",
                                        key, e
                                    )),
                                    cx,
                                );
                            });
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
                let key_for_delete = key.clone();
                let dock_area = self.dock_area.clone();

                cx.spawn_in(window, async move |_this, cx| {
                    let result = connection
                        .execute(&format!("DEL {}", escape_redis_value(&key_for_delete)), &[])
                        .await;

                    match result {
                        Ok(_) => {
                            tracing::info!("Redis key '{}' deleted successfully", key_for_delete);
                            // Refresh the active TableViewer to show updated data
                            _ = dock_area.update_in(cx, |dock_area, _window, cx| {
                                if let Some(panel) = dock_area.active_panel(cx)
                                    && panel.panel_name(cx) == "TableViewer"
                                    && let Ok(viewer) = panel.view().downcast::<TableViewerPanel>()
                                {
                                    viewer.update(cx, |viewer, cx| {
                                        viewer.refresh(cx);
                                    });
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

                cx.spawn(async move |_this, cx| {
                    let result = if is_new {
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
