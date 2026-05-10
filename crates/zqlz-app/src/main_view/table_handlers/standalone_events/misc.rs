//! This module contains miscellaneous standalone event handlers (SQL generation, foreign key loading).

use gpui::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use zqlz_core::{ColumnMeta, Value};
use zqlz_services::{
    GenerateTableChangesSqlRequest, LoadDistinctValuesRequest, LoadForeignKeyValuesRequest,
    ModifiedCellSqlChange,
};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{PendingCellChange, TableViewerPanel};

pub(in crate::main_view) fn handle_generate_sql_event(
    table_name: String,
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<Value>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<Value>>,
    cx: &mut App,
) {
    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("GenerateChangesSql: No AppState available");
        return;
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
    let sql = app_state
        .table_service
        .generate_table_changes_sql(GenerateTableChangesSqlRequest {
            table_name,
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
    connection_id: Uuid,
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

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(
            connection_id,
            viewer_entity.read(cx).database_name().as_deref(),
        )
    else {
        tracing::error!("LoadFkValues: Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let referenced_table = referenced_table.to_string();
    let referenced_schema = referenced_schema
        .map(str::trim)
        .filter(|schema| !schema.is_empty())
        .map(ToString::to_string);
    let table_service = app_state.table_service.clone();
    let cache_key = cache_key.to_string();
    let referenced_columns = referenced_columns.to_vec();
    let query = query.map(|value| value.to_string());
    let effective_limit = limit.clamp(1, 10);

    window
        .spawn(cx, async move |cx| {
            let request = LoadForeignKeyValuesRequest {
                referenced_table: referenced_table.clone(),
                referenced_schema,
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
                        referenced_table,
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
                    tracing::error!("LoadFkValues: Failed to query {}: {}", referenced_table, e);
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_load_distinct_values_event(
    connection_id: Uuid,
    table_name: &str,
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

    let Some(connection) = app_state
        .connection_service
        .get_connection_for_database_cached(
            connection_id,
            viewer_entity.read(cx).database_name().as_deref(),
        )
    else {
        tracing::error!(
            "LoadDistinctValues: Connection not found: {}",
            connection_id
        );
        return;
    };

    let connection = connection.clone();
    let table_name = table_name.to_string();
    let column_name = column_name.to_string();
    let table_service = app_state.table_service.clone();

    window
        .spawn(cx, async move |cx| {
            match table_service
                .load_distinct_values(
                    connection,
                    LoadDistinctValuesRequest {
                        table_name: table_name.clone(),
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
                        table_name,
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
