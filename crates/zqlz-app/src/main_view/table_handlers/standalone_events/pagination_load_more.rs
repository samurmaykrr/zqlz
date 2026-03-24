//! Load more event handler for infinite scroll pagination.
//!
//! This module contains the handler for loading additional rows
//! when the user scrolls to the bottom of the table in infinite scroll mode.

use gpui::*;

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::sql::{
    build_search_clause_for_columns, resolve_search_columns,
};

/// Handle load more event for infinite scroll
/// Fetches the next batch of rows and appends to existing data
pub(in crate::main_view) fn handle_load_more_event(
    current_offset: usize,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!("LoadMore event: current_offset={}", current_offset);

    // Get connection_id, table_name, limit, and current filter/search state from the viewer
    let viewer_info = viewer_entity.read_with(cx, |viewer, cx| {
        let connection_id = viewer.connection_id()?;
        let table_name = viewer.table_name()?;
        let limit = viewer
            .pagination_state
            .as_ref()
            .map(|p| p.read(cx).records_per_page)
            .unwrap_or(1000);

        // Extract filter/sort/search state so LoadMore preserves them
        let mut where_clauses: Vec<String> = Vec::new();
        let mut sorts = Vec::new();

        if let Some(filter_state) = &viewer.filter_panel_state {
            let (filters, current_sorts) = filter_state.read_with(cx, |state, _cx| {
                (state.get_filter_conditions(), state.get_sort_criteria())
            });
            where_clauses = filters.iter().filter_map(|f| f.to_sql()).collect();
            sorts = current_sorts;
        }

        let search_columns = resolve_search_columns(
            &viewer.column_meta,
            viewer
                .performance_profile
                .as_ref()
                .map(|profile| profile.searchable_columns.clone()),
        );
        let search_text = viewer.search_text.clone();

        let visible_columns: Vec<String> = viewer
            .column_visibility_state
            .as_ref()
            .map(|state| state.read(cx).visible_columns())
            .unwrap_or_else(|| viewer.column_meta.iter().map(|c| c.name.clone()).collect());

        Some((
            connection_id,
            table_name,
            limit,
            where_clauses,
            sorts,
            visible_columns,
            viewer.database_name.clone(),
            search_text,
            search_columns,
        ))
    });

    let Some((
        connection_id,
        table_name,
        limit,
        mut where_clauses,
        sorts,
        visible_columns,
        database_name,
        search_text,
        search_columns,
    )) = viewer_info
    else {
        tracing::warn!("LoadMore: Missing connection_id, table_name, or pagination state");
        return;
    };

    let request_generation =
        viewer_entity.read_with(cx, |viewer, _cx| viewer.current_request_generation());

    // Get app state
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("LoadMore: No AppState available");
        return;
    };

    // Get connection from app state (use database-specific connection for drivers like postgres)
    let Some(connection) = app_state
        .connections
        .get_for_database_cached(connection_id, database_name.as_deref())
    else {
        tracing::error!("LoadMore: Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let table_service = app_state.table_service.clone();
    let order_by_clauses: Vec<String> = sorts
        .iter()
        .map(|sort| sort.to_sql_for_connection(connection.as_ref()))
        .collect();
    if let Some(search_clause) =
        build_search_clause_for_columns(&connection, &search_columns, &search_text, false)
    {
        where_clauses.push(search_clause);
    }
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    window
        .spawn(cx, async move |cx| {
            match table_service
                .browse_table_with_filters(
                    connection,
                    zqlz_services::BrowseTableWithFiltersRequest {
                        table_name: &table_name,
                        schema: schema_qualifier.as_deref(),
                        where_clauses,
                        order_by_clauses,
                        visible_columns,
                        limit: Some(limit),
                        offset: Some(current_offset),
                        cached_total: Some(0),
                    },
                )
                .await
            {
                Ok(result) => {
                    let rows_loaded = result.rows.len();
                    let has_more = rows_loaded >= limit; // Has more if we got a full page

                    tracing::info!(
                        "LoadMore: {} rows fetched at offset {}, has_more={}",
                        rows_loaded,
                        current_offset,
                        has_more
                    );

                    _ = viewer_entity.update_in(cx, |viewer, _window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            tracing::debug!(
                                "Discarding stale load-more result for '{}' (generation={}, current={})",
                                table_name,
                                request_generation,
                                viewer.current_request_generation()
                            );
                            return;
                        }

                        // Pass Value rows directly (no string conversion)
                        let new_rows = result
                            .rows
                            .iter()
                            .map(|row| row.values.clone())
                            .collect();

                        // Append to existing rows via delegate
                        if let Some(table_state) = &viewer.table_state {
                            table_state.update(cx, |table, cx| {
                                table.delegate_mut().append_rows(new_rows, has_more);
                                // Propagate updated column widths (row number column
                                // resizes inside append_rows) to the table widget
                                table.refresh(cx);
                            });
                        }

                        // Update pagination state
                        if let Some(pag_state) = &viewer.pagination_state {
                            pag_state.update(cx, |state, cx| {
                                state.records_in_current_page = current_offset + rows_loaded;
                                state.has_more = has_more;
                                state.is_loading = false;
                                cx.notify();
                            });
                        }

                        cx.notify();
                    });
                }
                Err(e) => {
                    tracing::error!("LoadMore failed: {}", e);
                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            return;
                        }

                        use zqlz_ui::widgets::{WindowExt, notification::Notification};

                        if let Some(table_state) = &viewer.table_state {
                            table_state.update(cx, |table, _cx| {
                                let delegate = table.delegate_mut();
                                delegate.is_loading_more = false;
                            });
                        }
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to load more rows: {}",
                                e
                            )),
                            cx,
                        );
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}
