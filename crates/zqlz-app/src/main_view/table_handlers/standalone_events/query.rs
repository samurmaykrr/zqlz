//! This module contains standalone event handlers for query operations (filtering and sorting).

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::BrowseTableWithFiltersRequest;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::sql::{
    build_search_clause_for_columns, resolve_search_columns,
};

fn begin_viewer_request(viewer_entity: &Entity<TableViewerPanel>, cx: &mut App) -> u64 {
    viewer_entity.update(cx, |viewer, cx| viewer.begin_data_request(cx))
}

pub(in crate::main_view) struct ApplyFiltersRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub filters: Vec<crate::components::FilterCondition>,
    pub sorts: Vec<crate::components::SortCriterion>,
    pub visible_columns: Vec<String>,
    pub search_text: String,
    pub search_columns: Option<Vec<String>>,
}

pub(in crate::main_view) fn handle_apply_filters_event(
    request: ApplyFiltersRequest,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "ApplyFilters event: table={}, filters={}, sorts={}, search='{}'",
        request.table_name,
        request.filters.len(),
        request.sorts.len(),
        request.search_text
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get_for_database_cached(
        request.connection_id,
        viewer_entity.read(cx).database_name().as_deref(),
    ) else {
        tracing::error!("Connection not found: {}", request.connection_id);
        return;
    };

    let table_service = app_state.table_service.clone();
    let table_name = request.table_name;
    let connection = connection.clone();
    // Get connection name for tab title
    let connection_name = app_state
        .connection_manager()
        .get_saved(request.connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    // Convert FilterCondition to SQL WHERE fragments
    let mut where_clauses: Vec<String> =
        request.filters.iter().filter_map(|f| f.to_sql()).collect();

    // Build search WHERE clause: search across all string-like columns with CAST/LIKE
    if !request.search_text.is_empty() {
        let column_meta = viewer_entity.read(cx).column_meta.clone();
        let searchable_columns = resolve_search_columns(&column_meta, request.search_columns);

        if let Some(search_clause) = build_search_clause_for_columns(
            &connection,
            &searchable_columns,
            &request.search_text,
            false,
        ) {
            where_clauses.push(search_clause);
        }
    }

    // Convert SortCriterion to SQL ORDER BY fragments using driver-owned
    // identifier escaping from the active connection.
    let order_by_clauses: Vec<String> = request
        .sorts
        .iter()
        .map(|sort| sort.to_sql_for_connection(connection.as_ref()))
        .collect();

    let visible_columns = request.visible_columns;

    // Capture the is_view state and database_name before loading
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(&connection, &database_name);

    let filter_count = where_clauses.len();
    let sort_count = order_by_clauses.len();

    let request_generation = begin_viewer_request(&viewer_entity, cx);

    window
        .spawn(cx, async move |cx| {
            match table_service
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
            {
                Ok(result) => {
                    tracing::info!(
                        "Table loaded with filters: {} rows (filters={}, sorts={})",
                        result.rows.len(),
                        filter_count,
                        sort_count
                    );

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if !viewer.is_current_request(request_generation) {
                            tracing::debug!(
                                "Discarding stale filtered load for '{}' (generation={}, current={})",
                                table_name,
                                request_generation,
                                viewer.current_request_generation()
                            );
                            return;
                        }

                        viewer.load_table(
                            request.connection_id,
                            connection_name.clone(),
                            table_name.clone(),
                            database_name.clone(),
                            is_view,
                            result,
                            DriverCategory::Relational,
                            window,
                            cx,
                        );
                    });

                    cx.update(|_window, _cx| {
                        let msg = if filter_count == 0 && sort_count == 0 {
                            "Filters cleared".to_string()
                        } else {
                            format!(
                                "Applied {} filter(s), {} sort(s)",
                                filter_count,
                                sort_count
                            )
                        };
                        tracing::info!("{}", msg);
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to apply filters: {}", e);

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        if viewer.is_current_request(request_generation) {
                            viewer.set_loading(false, cx);
                            window.push_notification(
                                Notification::error(format!("Failed to apply filters: {}", e)),
                                cx,
                            );
                        }
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

/// Handle SortColumn event - reloads table with server-side ORDER BY
///
/// This is called when user clicks a column header to sort.
/// Instead of client-side sorting, we reload the table with ORDER BY.
pub(in crate::main_view) fn handle_sort_column_event(
    _connection_id: Uuid,
    table_name: &str,
    column_name: &str,
    direction: crate::components::SortDirection,
    viewer_entity: Entity<TableViewerPanel>,
    _window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SortColumn event: table={}, column={}, direction={:?}",
        table_name,
        column_name,
        direction
    );

    // Use the panel's apply_sort method which handles everything internally
    // (updating filter panel state, emitting ApplyFilters, etc.)
    viewer_entity.update(cx, |panel, cx| {
        panel.apply_sort(column_name.to_string(), direction, cx);
    });
}
