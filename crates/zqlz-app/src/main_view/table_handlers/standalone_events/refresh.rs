//! This module contains standalone event handlers for refreshing table data.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::{
    BrowseTableWithFiltersRequest, KeyValueService, LoadKeyValueDatabaseRowsRequest, TableService,
};
use zqlz_ui::widgets::WindowExt;
use zqlz_ui::widgets::notification::Notification;

use crate::app::AppState;
use crate::components::TableViewerEvent;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::sql::{
    build_search_clause_for_columns, resolve_search_columns,
};

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
    is_view: bool,
}

struct RefreshSqlRequest {
    viewer: RefreshViewerRequest,
    table: RefreshTableRequest,
    connection: Arc<dyn zqlz_core::Connection>,
    table_service: Arc<TableService>,
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

    let table_name = table_name.to_string();
    let connection = connection.clone();
    let connection_name = app_state
        .connection_service
        .get_saved_connection_name(connection_id)
        .unwrap_or_else(|| "Unknown".to_string());

    let table_service = app_state.table_service.clone();
    let key_value_service = app_state.key_value_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();

    let request_generation = begin_viewer_request(&viewer_entity, cx);
    let viewer_request = RefreshViewerRequest {
        connection_id,
        connection_name,
        driver_category,
        request_generation,
    };
    let table_request = RefreshTableRequest {
        table_name,
        database_name,
        is_view,
    };

    match driver_category {
        DriverCategory::KeyValue => {
            handle_refresh_keyvalue_table(
                RefreshKeyValueRequest {
                    viewer: viewer_request,
                    table: table_request,
                    connection,
                    key_value_service,
                },
                viewer_entity,
                window,
                cx,
            );
        }
        _ => {
            handle_refresh_sql_table(
                RefreshSqlRequest {
                    viewer: viewer_request,
                    table: table_request,
                    connection,
                    table_service,
                },
                viewer_entity,
                window,
                cx,
            );
        }
    }
}

fn handle_refresh_sql_table(
    request: RefreshSqlRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let RefreshSqlRequest {
        viewer,
        table,
        connection,
        table_service,
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
        is_view,
    } = table;

    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    // Determine if we need a background count after the data loads.
    let needs_background_count = !connection.supports_fast_exact_count();

    // Preserve active filters and sorts when refreshing
    let (filters, sorts, visible_columns, search_text) =
        viewer_entity.read_with(cx, |viewer, cx| {
            let (filters, sorts) = viewer
                .filter_panel_state
                .as_ref()
                .map(|state| {
                    state.read_with(cx, |s, _| {
                        (s.get_filter_conditions(), s.get_sort_criteria())
                    })
                })
                .unwrap_or_else(|| (Vec::new(), Vec::new()));

            let visible_columns = viewer
                .column_visibility_state
                .as_ref()
                .map(|state| state.read(cx).visible_columns())
                .unwrap_or_else(|| viewer.column_meta.iter().map(|c| c.name.clone()).collect());

            let search_text = viewer.search_text.clone();

            (filters, sorts, visible_columns, search_text)
        });

    // Build WHERE clauses from filters
    let mut where_clauses: Vec<String> = filters.iter().filter_map(|f| f.to_sql()).collect();

    // Build search WHERE clause if search text is present
    if !search_text.is_empty() {
        let (column_meta, profile_search_columns) = viewer_entity.read_with(cx, |viewer, _cx| {
            (
                viewer.column_meta.clone(),
                viewer
                    .performance_profile
                    .as_ref()
                    .map(|profile| profile.searchable_columns.clone()),
            )
        });
        let searchable_columns = resolve_search_columns(&column_meta, profile_search_columns);

        if let Some(search_clause) =
            build_search_clause_for_columns(&connection, &searchable_columns, &search_text, false)
        {
            where_clauses.push(search_clause);
        }
    }

    // Build ORDER BY clauses from sorts
    let order_by_clauses: Vec<String> = sorts
        .iter()
        .map(|sort| sort.to_sql_for_connection(connection.as_ref()))
        .collect();

    let filter_count = where_clauses.len();
    let sort_count = order_by_clauses.len();

    tracing::info!(
        "Refreshing table '{}' with {} filter(s), {} sort(s), search='{}'",
        table_name,
        filter_count,
        sort_count,
        search_text
    );

    window
        .spawn(cx, async move |cx| {
            // Clone the connection before the browse call consumes the Arc,
            // so it remains available for the background count task below.
            let connection_for_count = if needs_background_count {
                Some(connection.clone())
            } else {
                None
            };

            let result = if where_clauses.is_empty()
                && order_by_clauses.is_empty()
                && visible_columns.is_empty()
            {
                // No filters/sorts - use simple browse_table
                table_service
                    .browse_table(connection, &table_name, schema_qualifier.as_deref(), None, None)
                    .await
            } else {
                // Filters/sorts active - use browse_table_with_filters to preserve them
                table_service
                    .browse_table_with_filters(
                        connection,
                        BrowseTableWithFiltersRequest {
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
            };

            match result {
                Ok(result) => {
                    tracing::info!(
                        "Table reloaded successfully: {} rows (filters={}, sorts={})",
                        result.rows.len(),
                        filter_count,
                        sort_count
                    );

                    if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            tracing::debug!(
                                "Discarding stale refresh result for '{}' (generation={}, current={})",
                                table_name,
                                request_generation,
                                viewer.current_request_generation()
                            );
                            return;
                        }

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
                    }) {
                        tracing::debug!(
                            "Failed to apply refreshed table state for '{}': {}",
                            table_name,
                            error
                        );
                    }

                    // For slow-count drivers, fetch the row count in the
                    // background now that data is displayed to the user.
                    if let Some(count_conn) = connection_for_count {
                        let count_table = table_name.clone();
                        let count_schema = schema_qualifier.clone();
                        let count_service = table_service.clone();
                        let count_result = cx
                            .background_spawn(async move {
                                count_service
                                    .estimate_row_count(
                                        count_conn,
                                        &count_table,
                                        count_schema.as_deref(),
                                    )
                                    .await
                            })
                            .await;
                        match count_result {
                            Ok(Some((total, is_estimated))) => {
                                if let Err(error) = viewer_entity.update(cx, |_viewer, cx| {
                                    cx.emit(TableViewerEvent::CountCompleted {
                                        connection_id,
                                        table_name: table_name.clone(),
                                        database_name: database_name.clone(),
                                        request_generation,
                                        total_rows: total,
                                        is_estimated,
                                    });
                                    Ok::<(), anyhow::Error>(())
                                }) {
                                    tracing::debug!(
                                        "Failed to deliver background row count for '{}': {}",
                                        table_name,
                                        error
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(
                                    "Background row count failed for {}: {}",
                                    table_name,
                                    e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to refresh table: {}", e);

                    if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if viewer.is_current_request(request_generation) {
                            viewer.set_loading(false, cx);
                            window.push_notification(
                                Notification::error(format!("Failed to refresh table: {}", e)),
                                cx,
                            );
                        }
                        Ok::<(), anyhow::Error>(())
                    }) {
                        tracing::debug!(
                            "Failed to clear loading state for '{}': {}",
                            table_name,
                            error
                        );
                    }
                }
            }

            anyhow::Ok(())
        })
        .detach();
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
        ..
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
