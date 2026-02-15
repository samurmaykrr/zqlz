//! Load more event handler for infinite scroll pagination.
//!
//! This module contains the handler for loading additional rows
//! when the user scrolls to the bottom of the table in infinite scroll mode.

use gpui::*;

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;

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
                let escaped_search = viewer.search_text
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

        Some((connection_id, table_name, limit, where_clauses, order_by_clauses, visible_columns, viewer.database_name.clone()))
    });

    let Some((connection_id, table_name, limit, where_clauses, order_by_clauses, visible_columns, database_name)) = viewer_info else {
        tracing::warn!("LoadMore: Missing connection_id, table_name, or pagination state");
        return;
    };

    // Get app state
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("LoadMore: No AppState available");
        return;
    };

    // Get connection from app state
    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("LoadMore: Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let table_service = app_state.table_service.clone();
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);

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
                    Some(limit),
                    Some(current_offset),
                    Some(0),
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
                        // Convert result rows to string vectors
                        let new_rows: Vec<Vec<String>> = result
                            .rows
                            .iter()
                            .map(|row| row.values.iter().map(|val| val.to_string()).collect())
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
                        use zqlz_ui::widgets::{WindowExt, notification::Notification};
                        
                        if let Some(table_state) = &viewer.table_state {
                            table_state.update(cx, |table, _cx| {
                                let delegate = table.delegate_mut();
                                delegate.is_loading_more = false;
                            });
                        }
                        window.push_notification(
                            Notification::error(&format!(
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
