//! Helper functions for pagination table reloading.
//!
//! This module contains shared helper functions used by pagination event handlers
//! to reload table data with various pagination strategies.

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;

use crate::app::AppState;
use crate::components::TableViewerEvent;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::sql::build_search_clause_for_columns;

fn begin_viewer_request(viewer_entity: &Entity<TableViewerPanel>, cx: &mut App) -> u64 {
    viewer_entity.update(cx, |viewer, cx| {
        let request_generation = viewer.begin_data_request(cx);
        if let Some(pag_state) = &viewer.pagination_state {
            pag_state.update(cx, |state, cx| {
                state.is_loading = true;
                cx.notify();
            });
        }
        request_generation
    })
}

pub(super) struct PaginationReloadRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub cached_total: Option<u64>,
    pub request_generation: Option<u64>,
}

pub(super) struct ReversedPaginationRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub limit: usize,
    pub offset: usize,
    pub total_rows: u64,
    pub is_estimated: bool,
    pub pk_columns: Vec<String>,
    pub request_generation: Option<u64>,
}

/// Shared helper to reload table with specific limit/offset for pagination.
///
/// Preserves active filters, sorts, and search text so that
/// paginating doesn't drop the user's current query state.
///
/// When `cached_total` is `Some(count)`, the COUNT(*) query is skipped and the
/// cached value is reused. Pass `None` when filters/search/sort change so the
/// count is recalculated.
pub(in crate::main_view::table_handlers::standalone_events) fn reload_table_with_pagination(
    request: PaginationReloadRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let database_name = viewer_entity.read(cx).database_name();

    let Some(connection) = app_state
        .connections
        .get_for_database_cached(request.connection_id, database_name.as_deref())
    else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let table_name = request.table_name;
    let connection = connection.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(request.connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());
    let table_service = app_state.table_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    // Determine if we'll need a background count task after data is loaded.
    // This is needed when: no cached total provided, and the driver is slow-count.
    let needs_background_count =
        request.cached_total.is_none() && !connection.supports_fast_exact_count();

    // Extract the viewer's current filter/sort/search state so pagination preserves it
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

            // Add search text as WHERE clause (same logic as handle_apply_filters_event)
            if let Some(search_clause) = build_search_clause_for_columns(
                &connection,
                &viewer
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
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

    let request_generation = request
        .request_generation
        .unwrap_or_else(|| begin_viewer_request(&viewer_entity, cx));
    let connection_id = request.connection_id;
    let limit = request.limit;
    let offset = request.offset;
    let cached_total = request.cached_total;

    window
        .spawn(cx, async move |cx| {
            // Clone the connection before the browse call consumes the Arc,
            // so it remains available for the background count task below.
            let connection_for_count = if needs_background_count {
                Some(connection.clone())
            } else {
                None
            };

            match table_service
                .browse_table_with_filters(
                    connection,
                    zqlz_services::BrowseTableWithFiltersRequest {
                        table_name: &table_name,
                        schema: schema_qualifier.as_deref(),
                        where_clauses,
                        order_by_clauses,
                        visible_columns,
                        limit,
                        offset,
                        cached_total,
                    },
                )
                .await
            {
                Ok(result) => {
                    let rows_loaded = result.rows.len();
                    let total_records = result.total_rows;
                    let is_estimated = result.is_estimated_total;

                    tracing::info!(
                        "Pagination reload: {} rows loaded (total: {:?})",
                        rows_loaded,
                        total_records
                    );

                    if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            tracing::debug!(
                                "Discarding stale paginated reload for '{}' (generation={}, current={})",
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
                            DriverCategory::Relational,
                            window,
                            cx,
                        );

                        // Update pagination state after load
                        if let Some(pag_state) = &viewer.pagination_state {
                            pag_state.update(cx, |state, cx| {
                                state.update_after_load(rows_loaded, total_records, is_estimated, cx);
                            });
                        }
                    }) {
                        tracing::debug!(error = %error, "Paginated reload result arrived after viewer dropped");
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
                            Ok((total, is_estimated)) => {
                                viewer_entity.update(cx, |_viewer, cx| {
                                    cx.emit(TableViewerEvent::CountCompleted {
                                        connection_id,
                                        table_name: table_name.clone(),
                                        request_generation,
                                        total_rows: total,
                                        is_estimated,
                                    });
                                });
                            }
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
                    tracing::error!("Failed to reload table with pagination: {}", e);

                    viewer_entity.update(cx, |viewer, cx| {
                        if viewer.is_current_request(request_generation) {
                            viewer.set_loading(false, cx);
                            if let Some(pag_state) = &viewer.pagination_state {
                                pag_state.update(cx, |state, cx| {
                                    state.is_loading = false;
                                    cx.notify();
                                });
                            }
                        }
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

/// Reload a page using a reversed ORDER BY query for pages near the end.
///
/// Same as `reload_table_with_pagination` but delegates to
/// `TableService::browse_near_end_page` which uses `ORDER BY pk DESC`
/// with a small offset from the tail instead of `ORDER BY pk ASC` with
/// a massive offset from the head. Rows are reversed client-side.
pub(in crate::main_view::table_handlers::standalone_events) fn reload_table_reversed(
    request: ReversedPaginationRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let database_name = viewer_entity.read(cx).database_name();

    let Some(connection) = app_state
        .connections
        .get_for_database_cached(request.connection_id, database_name.as_deref())
    else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let table_name = request.table_name;
    let connection = connection.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(request.connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());
    let table_service = app_state.table_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

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
                &viewer
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
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

    let request_generation = request
        .request_generation
        .unwrap_or_else(|| begin_viewer_request(&viewer_entity, cx));
    let connection_id = request.connection_id;
    let limit = request.limit;
    let offset = request.offset;
    let total_rows = request.total_rows;
    let is_estimated = request.is_estimated;
    let pk_columns = request.pk_columns;

    window
        .spawn(cx, async move |cx| {
            match table_service
                .browse_near_end_page(
                    connection,
                    zqlz_services::BrowseNearEndPageRequest {
                        table_name: &table_name,
                        schema: schema_qualifier.as_deref(),
                        where_clauses,
                        order_by_clauses,
                        visible_columns,
                        limit,
                        offset,
                        total_rows,
                        pk_columns,
                    },
                )
                .await
            {
                Ok(result) => {
                    let rows_loaded = result.rows.len();
                    let total_records = result.total_rows;

                    tracing::info!(
                        "Reversed pagination reload: {} rows loaded (total: {:?})",
                        rows_loaded,
                        total_records
                    );

                    if let Err(error) = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            tracing::debug!(
                                "Discarding stale reversed pagination reload for '{}' (generation={}, current={})",
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
                            DriverCategory::Relational,
                            window,
                            cx,
                        );

                        if let Some(pag_state) = &viewer.pagination_state {
                            pag_state.update(cx, |state, cx| {
                                state.update_after_load(rows_loaded, total_records, is_estimated, cx);
                            });
                        }
                    }) {
                        tracing::debug!(error = %error, "Reversed pagination result arrived after viewer dropped");
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to reload table with reversed pagination: {}", e);

                    viewer_entity.update(cx, |viewer, cx| {
                        if viewer.is_current_request(request_generation) {
                            viewer.set_loading(false, cx);
                            if let Some(pag_state) = &viewer.pagination_state {
                                pag_state.update(cx, |state, cx| {
                                    state.is_loading = false;
                                    cx.notify();
                                });
                            }
                        }
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}
