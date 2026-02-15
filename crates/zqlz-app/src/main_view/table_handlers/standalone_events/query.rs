//! This module contains standalone event handlers for query operations (filtering and sorting).

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::TableService;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;

pub(in crate::main_view) fn handle_apply_filters_event(
    connection_id: Uuid,
    table_name: &str,
    filters: &[crate::components::FilterCondition],
    sorts: &[crate::components::SortCriterion],
    visible_columns: &[String],
    search_text: &str,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "ApplyFilters event: table={}, filters={}, sorts={}, search='{}'",
        table_name,
        filters.len(),
        sorts.len(),
        search_text
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
    let table_name = table_name.to_string();
    let connection = connection.clone();
    // Get connection name for tab title
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    // Convert FilterCondition to SQL WHERE fragments
    let mut where_clauses: Vec<String> = filters.iter().filter_map(|f| f.to_sql()).collect();

    // Build search WHERE clause: search across all string-like columns with CAST/LIKE
    if !search_text.is_empty() {
        let column_meta = viewer_entity.read(cx).column_meta.clone();
        let searchable_columns: Vec<String> = column_meta
            .iter()
            .filter(|col| TableService::is_string_type(&col.data_type.to_lowercase()))
            .map(|col| col.name.clone())
            .collect();

        if !searchable_columns.is_empty() {
            let escaped_search = search_text.replace("'", "''").replace("%", "\\%").replace("_", "\\_");
            let column_conditions: Vec<String> = searchable_columns
                .iter()
                .map(|col_name| {
                    let escaped_col = format!("\"{}\"", col_name.replace("\"", "\"\""));
                    format!(
                        "CAST({} AS TEXT) LIKE '%{}%' ESCAPE '\\'",
                        escaped_col,
                        escaped_search
                    )
                })
                .collect();
            where_clauses.push(format!("({})", column_conditions.join(" OR ")));
        }
    }

    // Convert SortCriterion to SQL ORDER BY fragments
    let order_by_clauses: Vec<String> = sorts.iter().map(|s| s.to_sql()).collect();

    let visible_columns = visible_columns.to_vec();

    // Capture the is_view state and database_name before loading
    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

    let filter_count = where_clauses.len();
    let sort_count = order_by_clauses.len();

    _ = viewer_entity.update(cx, |viewer, cx| {
        viewer.set_loading(true, cx);
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
                    None,
                    None,
                    None,
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
                    });

                    cx.update(|window, cx| {
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
                        viewer.set_loading(false, cx);
                        window.push_notification(
                            Notification::error(&format!("Failed to apply filters: {}", e)),
                            cx,
                        );
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
    connection_id: Uuid,
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
    _ = viewer_entity.update(cx, |panel, cx| {
        panel.apply_sort(column_name.to_string(), direction, cx);
    });
}
