//! Basic pagination event handlers for page navigation and limit changes.
//!
//! This module contains handlers for:
//! - Page changes (next/previous page navigation)
//! - Last page requests
//! - Limit changes (rows per page)
//! - Limit enable/disable toggle

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;

use super::pagination_helpers::{reload_table_with_pagination, reload_table_reversed};

/// Handle pagination page change - reload table data with new offset.
///
/// When the requested page is in the "near-end" region of a large table
/// (offset past the midpoint) and primary key columns are available, this
/// uses a reversed ORDER BY with a small offset from the tail. This avoids
/// the multi-second high-OFFSET scans that MySQL performs when paginating
/// near the end of tables with millions of rows.
pub(in crate::main_view) fn handle_page_changed_event(
    connection_id: Uuid,
    table_name: &str,
    page: usize,
    limit: usize,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let offset = (page - 1) * limit;
    tracing::info!(
        "PageChanged event: table={}, page={}, limit={}, offset={}",
        table_name,
        page,
        limit,
        offset
    );

    // Reuse the cached total from PaginationState so we skip the COUNT(*)
    // query on simple page navigations (filters/search haven't changed).
    let (cached_total, pk_columns, cached_is_estimated) = viewer_entity.read_with(cx, |viewer, cx| {
        let (total, is_est) = viewer
            .pagination_state
            .as_ref()
            .map(|p| {
                let state = p.read(cx);
                (state.total_records, state.is_estimated)
            })
            .unwrap_or((None, false));
        let pk = viewer.primary_key_columns.clone();
        (total, pk, is_est)
    });

    // Determine whether this page is "near the end" and should use a reversed
    // query. Criteria: we know the total, offset is past the midpoint, and we
    // have PK columns to build a reversed ORDER BY.
    let use_reversed = match cached_total {
        Some(total) if !pk_columns.is_empty() && offset as u64 > total / 2 => true,
        _ => false,
    };

    if use_reversed {
        let total = cached_total.expect("checked above");
        reload_table_reversed(
            connection_id,
            table_name,
            limit,
            offset,
            total,
            cached_is_estimated,
            pk_columns,
            viewer_entity,
            window,
            cx,
        );
    } else {
        reload_table_with_pagination(
            connection_id,
            table_name,
            Some(limit),
            Some(offset),
            cached_total,
            viewer_entity,
            window,
            cx,
        );
    }
}

/// Handle "Last Page" request when the total row count is unknown.
///
/// When primary key columns are available, runs COUNT(\*) and a
/// reversed-ORDER-BY data fetch **concurrently** via
/// `TableService::browse_last_page`. This avoids the expensive
/// high-OFFSET scan that MySQL performs on large tables.
///
/// Falls back to the sequential COUNT → OFFSET approach when no
/// primary key information is available on the viewer.
pub(in crate::main_view) fn handle_last_page_requested_event(
    connection_id: Uuid,
    table_name: &str,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    // Guard: skip if a load is already in flight
    let already_loading = viewer_entity.read(cx).pagination_state.as_ref().is_some_and(
        |pag| pag.read(cx).is_loading,
    );
    if already_loading {
        tracing::debug!("LastPageRequested: skipping duplicate — already loading");
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

    // Extract everything we need from the viewer in a single read so we
    // don't need to borrow it again before spawning the async task.
    let viewer_state = viewer_entity.read_with(cx, |viewer, cx| {
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
            let all_column_names: Vec<String> =
                viewer.column_meta.iter().map(|c| c.name.clone()).collect();

            if !all_column_names.is_empty() {
                let escaped_search = viewer
                    .search_text
                    .replace("'", "''")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
                let column_conditions: Vec<String> = all_column_names
                    .iter()
                    .map(|col_name| {
                        let escaped_col = format!("\"{}\"", col_name.replace('"', "\"\""));
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

        let records_per_page = viewer
            .pagination_state
            .as_ref()
            .map(|p| p.read(cx).records_per_page)
            .unwrap_or(1000);

        let pk_columns = viewer.primary_key_columns.clone();

        (where_clauses, order_by_clauses, visible_columns, records_per_page, pk_columns)
    });

    let (where_clauses, order_by_clauses, visible_columns, records_per_page, pk_columns) = viewer_state;

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

    if pk_columns.is_empty() {
        // Fallback: no PK available — sequential COUNT then OFFSET query.
        tracing::info!(
            "LastPageRequested: no PK columns, falling back to sequential COUNT(*) for table={}",
            table_name
        );

        window
            .spawn(cx, async move |cx| {
                match table_service
                    .count_rows(
                        connection.clone(),
                        &table_name,
                        schema_qualifier.as_deref(),
                        where_clauses.clone(),
                    )
                    .await
                {
                    Ok(total) => {
                        let last_page = ((total as usize) + records_per_page - 1) / records_per_page;
                        let last_page = last_page.max(1);
                        let offset = (last_page - 1) * records_per_page;

                        tracing::info!(
                            "COUNT(*) returned {}, navigating to last page {} (offset {})",
                            total,
                            last_page,
                            offset
                        );

                        // Update pagination, then reload with the computed offset
                        _ = viewer_entity.update(cx, |viewer, cx| {
                            if let Some(pag_state) = &viewer.pagination_state {
                                pag_state.update(cx, |state, cx| {
                                    state.total_records = Some(total);
                                    state.current_page = last_page;
                                    cx.notify();
                                });
                            }
                        });

                        _ = cx.update(|window, cx| {
                            reload_table_with_pagination(
                                connection_id,
                                &table_name,
                                Some(records_per_page),
                                Some(offset),
                                Some(total),
                                viewer_entity.clone(),
                                window,
                                cx,
                            );
                        });
                    }
                    Err(e) => {
                        tracing::error!("On-demand COUNT(*) failed: {}", e);
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
    } else {
        // Fast path: PK available — run COUNT(*) and reversed-ORDER-BY
        // data fetch concurrently. The data query uses ORDER BY pk DESC
        // (or flipped user sorts) + LIMIT so it hits an index scan
        // instead of the expensive high-OFFSET full-table scan.
        tracing::info!(
            "LastPageRequested: using concurrent COUNT + reversed PK query for table={}, pk={:?}",
            table_name,
            pk_columns
        );

        window
            .spawn(cx, async move |cx| {
                match table_service
                    .browse_last_page(
                        connection,
                        &table_name,
                        schema_qualifier.as_deref(),
                        where_clauses,
                        order_by_clauses,
                        visible_columns,
                        records_per_page,
                        pk_columns,
                    )
                    .await
                {
                    Ok(result) => {
                        let total = result.total_rows.unwrap_or(0);
                        let last_page = ((total as usize) + records_per_page - 1) / records_per_page;
                        let last_page = last_page.max(1);
                        let rows_loaded = result.rows.len();
                        let total_records = result.total_rows;

                        tracing::info!(
                            "Last-page loaded concurrently: {} rows, total={}, page={}",
                            rows_loaded,
                            total,
                            last_page
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
                                    state.total_records = total_records;
                                    state.current_page = last_page;
                                    state.update_after_load(rows_loaded, total_records, false, cx);
                                });
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Last-page concurrent fetch failed: {}", e);
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
}

/// Handle limit change - reload table data with new limit (reset to page 1)
pub(in crate::main_view) fn handle_limit_changed_event(
    connection_id: Uuid,
    table_name: &str,
    limit: usize,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "LimitChanged event: table={}, limit={}",
        table_name,
        limit
    );

    // Reuse cached total — changing the page size doesn't change the row count.
    let cached_total = viewer_entity.read_with(cx, |viewer, cx| {
        viewer
            .pagination_state
            .as_ref()
            .and_then(|p| p.read(cx).total_records)
    });

    // When limit changes, reset to page 1 (offset 0)
    reload_table_with_pagination(
        connection_id,
        table_name,
        Some(limit),
        Some(0),
        cached_total,
        viewer_entity,
        window,
        cx,
    );
}

/// Handle limit enabled/disabled toggle - reload with or without limit
pub(in crate::main_view) fn handle_limit_enabled_changed_event(
    connection_id: Uuid,
    table_name: &str,
    enabled: bool,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "LimitEnabledChanged event: table={}, enabled={}",
        table_name,
        enabled
    );

    // Reuse cached total — toggling the limit doesn't change the row count.
    let cached_total = viewer_entity.read_with(cx, |viewer, cx| {
        viewer
            .pagination_state
            .as_ref()
            .and_then(|p| p.read(cx).total_records)
    });

    if enabled {
        // Re-enable pagination with default limit
        reload_table_with_pagination(
            connection_id,
            table_name,
            Some(1000), // Default limit
            Some(0),    // Start from beginning
            cached_total,
            viewer_entity,
            window,
            cx,
        );
    } else {
        // Disable pagination - load all rows
        reload_table_with_pagination(
            connection_id,
            table_name,
            None, // No limit
            None, // No offset
            cached_total,
            viewer_entity,
            window,
            cx,
        );
    }
}
