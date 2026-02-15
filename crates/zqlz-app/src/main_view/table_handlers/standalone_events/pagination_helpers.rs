//! Helper functions for pagination table reloading.
//!
//! This module contains shared helper functions used by pagination event handlers
//! to reload table data with various pagination strategies.

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;

/// Shared helper to reload table with specific limit/offset for pagination.
///
/// Preserves active filters, sorts, and search text so that
/// paginating doesn't drop the user's current query state.
///
/// When `cached_total` is `Some(count)`, the COUNT(*) query is skipped and the
/// cached value is reused. Pass `None` when filters/search/sort change so the
/// count is recalculated.
pub(in crate::main_view::table_handlers::standalone_events) fn reload_table_with_pagination(
    connection_id: Uuid,
    table_name: &str,
    limit: Option<usize>,
    offset: Option<usize>,
    cached_total: Option<u64>,
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

    let table_name = table_name.to_string();
    let connection = connection.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());
    let table_service = app_state.table_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

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
                order_by_clauses = sorts.iter().map(|s| s.to_sql()).collect();
            }

            // Add search text as WHERE clause (same logic as handle_apply_filters_event)
            if !viewer.search_text.is_empty() {
                let all_column_names: Vec<String> = viewer
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                if !all_column_names.is_empty() {
                    let escaped_search = viewer.search_text
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

    // Set loading state and update pagination state
    _ = viewer_entity.update(cx, |viewer, cx| {
        viewer.set_loading(true, cx);
        if let Some(pag_state) = &viewer.pagination_state {
            pag_state.update(cx, |state, cx| {
                state.is_loading = true;
                cx.notify();
            });
        }
    });

    window
        .spawn(cx, async move |cx| {
            match table_service
                .browse_table_with_filters(
                    connection,
                    &table_name,
                    schema_qualifier.as_deref(),
                    where_clauses,
                    order_by_clauses,
                    visible_columns,
                    limit,
                    offset,
                    cached_total,
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

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
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
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to reload table with pagination: {}", e);

                    _ = viewer_entity.update(cx, |viewer, cx| {
                        viewer.set_loading(false, cx);
                        if let Some(pag_state) = &viewer.pagination_state {
                            pag_state.update(cx, |state, cx| {
                                state.is_loading = false;
                                cx.notify();
                            });
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
    connection_id: Uuid,
    table_name: &str,
    limit: usize,
    offset: usize,
    total_rows: u64,
    is_estimated: bool,
    pk_columns: Vec<String>,
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

    let table_name = table_name.to_string();
    let connection = connection.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());
    let table_service = app_state.table_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

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

    // Set loading state
    _ = viewer_entity.update(cx, |viewer, cx| {
        viewer.set_loading(true, cx);
        if let Some(pag_state) = &viewer.pagination_state {
            pag_state.update(cx, |state, cx| {
                state.is_loading = true;
                cx.notify();
            });
        }
    });

    window
        .spawn(cx, async move |cx| {
            match table_service
                .browse_near_end_page(
                    connection,
                    &table_name,
                    schema_qualifier.as_deref(),
                    where_clauses,
                    order_by_clauses,
                    visible_columns,
                    limit,
                    offset,
                    total_rows,
                    pk_columns,
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

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
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
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to reload table with reversed pagination: {}", e);

                    _ = viewer_entity.update(cx, |viewer, cx| {
                        viewer.set_loading(false, cx);
                        if let Some(pag_state) = &viewer.pagination_state {
                            pag_state.update(cx, |state, cx| {
                                state.is_loading = false;
                                cx.notify();
                            });
                        }
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}
