//! This module contains miscellaneous standalone event handlers (SQL generation, foreign key loading).

use gpui::*;
use std::collections::{HashMap, HashSet};
use zqlz_core::{ColumnMeta, TableRef, Value};
use zqlz_services::{
    GenerateTableChangesSqlRequest, LoadDistinctValuesRequest, LoadForeignKeyValuesRequest,
    ModifiedCellSqlChange,
};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{PendingCellChange, TableViewerPanel};
use crate::main_view::table_handlers_utils::conversion::viewer_table;

#[allow(clippy::too_many_arguments)]
pub(in crate::main_view) fn handle_generate_sql_event(
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<Value>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<Value>>,
    viewer_entity: &Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("GenerateChangesSql: No AppState available");
        return;
    };
    let (table, connection) = match viewer_table(viewer_entity.read(cx), cx) {
        Ok(target) => target,
        Err(error) => {
            window.push_notification(
                Notification::error(format!("Cannot generate SQL: {error}")),
                cx,
            );
            return;
        }
    };

    let column_names: Vec<String> = column_meta
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let modified_cell_changes: Vec<ModifiedCellSqlChange> = modified_cells
        .into_iter()
        .map(
            |((row_index, column_index), change)| ModifiedCellSqlChange {
                row_index,
                column_index,
                new_value: change.new_value.as_value(),
            },
        )
        .collect();

    let deleted_row_indices = deleted_rows.into_iter().collect();
    let sql = app_state.table_service.generate_table_changes_sql(
        connection.as_ref(),
        GenerateTableChangesSqlRequest {
            table,
            column_names,
            modified_cells: modified_cell_changes,
            deleted_row_indices,
            new_rows,
            all_rows,
        });

    cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql.clone()));

    let statement_count = sql.lines().count();
    tracing::info!(
        "Generated {} SQL statements and copied to clipboard",
        statement_count
    );
}

#[allow(clippy::too_many_arguments)]
pub(in crate::main_view) fn handle_load_fk_values_event(
    referenced_table: &str,
    referenced_schema: Option<&str>,
    cache_key: &str,
    referenced_columns: &[String],
    query: Option<&str>,
    limit: usize,
    request_id: u64,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    use crate::components::table_viewer::delegate::FkSelectItem;

    tracing::info!(
        "LoadFkValues event: table={}, columns={:?}, query={:?}, limit={}, request_id={}",
        referenced_table,
        referenced_columns,
        query,
        limit,
        request_id
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("LoadFkValues: No AppState available");
        return;
    };

    let (current_table, connection) = match viewer_table(viewer_entity.read(cx), cx) {
        Ok(target) => target,
        Err(error) => {
            tracing::error!(%error, "LoadFkValues: no table to load from");
            return;
        }
    };
    // The key's own schema wins; otherwise it points into this table's schema.
    let referenced_table = TableRef::new(
        current_table.database,
        referenced_schema
            .map(str::trim)
            .filter(|schema| !schema.is_empty())
            .map(ToString::to_string)
            .or(current_table.schema),
        referenced_table,
    );
    let table_service = app_state.table_service.clone();
    let cache_key = cache_key.to_string();
    let referenced_columns = referenced_columns.to_vec();
    let query = query.map(|value| value.to_string());
    let effective_limit = limit.clamp(1, 10);

    window
        .spawn(cx, async move |cx| {
            let request = LoadForeignKeyValuesRequest {
                referenced_table: referenced_table.clone(),
                referenced_columns,
                query: query.clone(),
                limit: effective_limit,
            };

            match table_service
                .load_foreign_key_values(connection, request)
                .await
            {
                Ok(outcome) => {
                    let values: Vec<FkSelectItem> = outcome
                        .values
                        .into_iter()
                        .map(|option| FkSelectItem {
                            value: option.value,
                            label: option.label,
                        })
                        .collect();

                    tracing::info!(
                        "LoadFkValues: Loaded {} values from {} (query={:?}, request_id={})",
                        values.len(),
                        referenced_table.display_name(),
                        query,
                        request_id
                    );

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        viewer.set_fk_values(
                            cache_key.clone(),
                            values,
                            query.clone(),
                            request_id,
                            window,
                            cx,
                        );
                    });
                }
                Err(e) => {
                    tracing::error!(
                        "LoadFkValues: Failed to query {}: {}",
                        referenced_table.display_name(),
                        e
                    );
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_load_distinct_values_event(
    column_name: &str,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    use crate::components::table_viewer::FilterOperator;

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("LoadDistinctValues: No AppState available");
        return;
    };

    let (table, connection) = match viewer_table(viewer_entity.read(cx), cx) {
        Ok(target) => target,
        Err(error) => {
            window.push_notification(
                Notification::error(format!("Cannot load distinct values: {error}")),
                cx,
            );
            return;
        }
    };
    let column_name = column_name.to_string();
    let table_service = app_state.table_service.clone();

    window
        .spawn(cx, async move |cx| {
            match table_service
                .load_distinct_values(
                    connection,
                    LoadDistinctValuesRequest {
                        table: table.clone(),
                        column_name: column_name.clone(),
                        limit: 500,
                    },
                )
                .await
            {
                Ok(outcome) => {
                    let count = outcome.values.len();

                    if outcome.values.is_empty() {
                        _ = viewer_entity.update_in(cx, |_viewer, window, cx| {
                            window.push_notification(
                                Notification::info(format!(
                                    "No distinct values found for column '{}'",
                                    column_name
                                )),
                                cx,
                            );
                        });
                    } else {
                        let filter_value = outcome.values.join(", ");
                        _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                            viewer.add_quick_filter(
                                column_name.clone(),
                                FilterOperator::IsInList,
                                filter_value,
                                window,
                                cx,
                            );
                            window.push_notification(
                                Notification::success(format!(
                                    "Added filter with {} distinct values for '{}'",
                                    count, column_name
                                )),
                                cx,
                            );
                        });
                    }
                }
                Err(error) => {
                    tracing::error!(
                        "LoadDistinctValues: failed for {}.{}: {}",
                        table.display_name(),
                        column_name,
                        error
                    );

                    _ = viewer_entity.update_in(cx, |_viewer, window, cx| {
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to load distinct values: {}",
                                error
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
