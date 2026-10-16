//! This module contains standalone event handlers for refreshing table data.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::{FetchMode, KeyValueService, LoadKeyValueDatabaseRowsRequest};

use crate::app::AppState;
use crate::components::TableViewerPanel;

fn begin_viewer_request(viewer_entity: &Entity<TableViewerPanel>, cx: &mut App) -> u64 {
    viewer_entity.update(cx, |viewer, cx| viewer.begin_data_request(cx))
}

struct RefreshViewerRequest {
    connection_id: Uuid,
    connection_name: String,
    driver_category: DriverCategory,
    request_generation: u64,
}

struct RefreshTableRequest {
    table_name: String,
    database_name: Option<String>,
}

struct RefreshKeyValueRequest {
    viewer: RefreshViewerRequest,
    table: RefreshTableRequest,
    connection: Arc<dyn zqlz_core::Connection>,
    key_value_service: Arc<KeyValueService>,
}

fn parse_redis_database_index(database_name: Option<&str>, table_name: &str) -> u16 {
    let source = database_name.unwrap_or(table_name);
    source
        .strip_prefix("db")
        .and_then(|suffix| suffix.parse::<u16>().ok())
        .unwrap_or(0)
}

pub(in crate::main_view) fn handle_refresh_table_event(
    connection_id: Uuid,
    table_name: &str,
    driver_category: DriverCategory,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "RefreshTable event: table={}, connection={}, driver={:?}",
        table_name,
        connection_id,
        driver_category
    );

    if driver_category != DriverCategory::KeyValue {
        viewer_entity.update(cx, |viewer, cx| {
            viewer.reload(FetchMode::Refresh, window, cx);
        });
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

    let connection_name = app_state
        .connection_service
        .get_saved_connection_name(connection_id)
        .unwrap_or_else(|| "Unknown".to_string());
    let key_value_service = app_state.key_value_service.clone();
    let database_name = viewer_entity.read(cx).database_name();
    let request_generation = begin_viewer_request(&viewer_entity, cx);

    handle_refresh_keyvalue_table(
        RefreshKeyValueRequest {
            viewer: RefreshViewerRequest {
                connection_id,
                connection_name,
                driver_category,
                request_generation,
            },
            table: RefreshTableRequest {
                table_name: table_name.to_string(),
                database_name,
            },
            connection,
            key_value_service,
        },
        viewer_entity,
        window,
        cx,
    );
}

fn handle_refresh_keyvalue_table(
    request: RefreshKeyValueRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let RefreshKeyValueRequest {
        viewer,
        table,
        connection,
        key_value_service,
    } = request;
    let RefreshViewerRequest {
        connection_id,
        connection_name,
        driver_category,
        request_generation,
    } = viewer;
    let RefreshTableRequest {
        table_name,
        database_name,
    } = table;
    let database_index = parse_redis_database_index(database_name.as_deref(), &table_name);
    let is_database_view = database_name.as_deref() == Some(table_name.as_str());

    window
        .spawn(cx, async move |cx| {
            let refresh_table_name = table_name.clone();
            let query_result = if is_database_view {
                match key_value_service
                    .load_database_rows(
                        connection,
                        LoadKeyValueDatabaseRowsRequest { database_index },
                    )
                    .await
                {
                    Ok(outcome) => outcome.query_result,
                    Err(error) => {
                        tracing::error!("Failed to load key-value rows: {}", error);
                        if let Err(error) = viewer_entity.update(cx, |viewer, cx| {
                            if viewer.is_current_request(request_generation) {
                                viewer.set_loading(false, cx);
                            }
                            Ok::<(), anyhow::Error>(())
                        }) {
                            tracing::debug!(
                                "Failed to clear loading state for '{}': {}",
                                table_name,
                                error
                            );
                        }
                        return anyhow::Ok(());
                    }
                }
            } else {
                match key_value_service
                    .browse_key(connection, &table_name, Some(1000))
                    .await
                {
                    Ok(query_result) => query_result,
                    Err(error) => {
                        tracing::error!("Failed to load key-value entry: {}", error);
                        if let Err(error) = viewer_entity.update(cx, |viewer, cx| {
                            if viewer.is_current_request(request_generation) {
                                viewer.set_loading(false, cx);
                            }
                            Ok::<(), anyhow::Error>(())
                        }) {
                            tracing::debug!(
                                "Failed to clear loading state for '{}': {}",
                                table_name,
                                error
                            );
                        }
                        return anyhow::Ok(());
                    }
                }
            };

            tracing::info!(
                "Reloaded {} keys for key-value database",
                query_result.rows.len()
            );

            if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
                if !viewer.is_current_request(request_generation) {
                    tracing::debug!(
                        "Discarding stale key-value refresh result for '{}' (generation={}, current={})",
                        table_name,
                        request_generation,
                        viewer.current_request_generation()
                    );
                    return;
                }

                viewer.load_table(
                    connection_id,
                    connection_name,
                    table_name,
                    database_name,
                    true,
                    query_result,
                    driver_category,
                    window,
                    cx,
                );

                if let Some(table_state) = &viewer.table_state {
                    table_state.update(cx, |table, _cx| {
                        table.delegate_mut().set_disable_inline_edit(true);
                    });
                }
            }) {
                tracing::debug!(
                    "Failed to apply key-value refresh state for '{}': {}",
                    refresh_table_name,
                    error
                );
            }

            anyhow::Ok(())
        })
        .detach();
}
