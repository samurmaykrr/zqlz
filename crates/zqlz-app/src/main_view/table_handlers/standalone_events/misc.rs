//! This module contains miscellaneous standalone event handlers (SQL generation, foreign key loading).

use gpui::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use zqlz_core::{ColumnMeta, SqlObjectName, Value};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::components::{CellValue, PendingCellChange, TableViewerPanel};

use crate::main_view::table_handlers_utils::formatting::{
    format_sql_value, format_sql_value_from_value,
};
use crate::main_view::table_handlers_utils::sql::escape_sql_like_literal;

pub(in crate::main_view) fn handle_generate_sql_event(
    table_name: String,
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<Value>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<Value>>,
    cx: &mut App,
) {
    let column_names: Vec<String> = column_meta.iter().map(|c| c.name.clone()).collect();
    let mut sql_statements: Vec<String> = Vec::new();
    let display_cell_value = |value: &Value| {
        if value.is_null() {
            CellValue::Null
        } else {
            CellValue::Value(value.clone())
        }
    };

    // Generate UPDATE statements for modified cells
    // Group by row to create one UPDATE per row with multiple SET clauses
    let mut row_updates: HashMap<usize, Vec<(usize, &PendingCellChange)>> = HashMap::new();
    for ((row_idx, col_idx), change) in &modified_cells {
        row_updates
            .entry(*row_idx)
            .or_default()
            .push((*col_idx, change));
    }

    for (row_idx, changes) in row_updates {
        if let Some(row_values) = all_rows.get(row_idx) {
            // Build SET clause
            let set_parts: Vec<String> = changes
                .iter()
                .filter_map(|(col_idx, change)| {
                    column_names.get(*col_idx).map(|col_name| {
                        let value = format_sql_value(&change.new_value);
                        format!("\"{}\" = {}", col_name, value)
                    })
                })
                .collect();

            // Build WHERE clause (use all columns to identify the row)
            let where_parts: Vec<String> = column_names
                .iter()
                .zip(row_values.iter())
                .map(|(col_name, value)| {
                    let cell_value = display_cell_value(value);
                    let sql_value = format_sql_value(&cell_value);
                    if cell_value.is_null() {
                        format!("\"{}\" IS NULL", col_name)
                    } else {
                        format!("\"{}\" = {}", col_name, sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "UPDATE \"{}\" SET {} WHERE {};",
                table_name,
                set_parts.join(", "),
                where_parts.join(" AND ")
            ));
        }
    }

    // Generate DELETE statements for deleted rows
    for row_idx in &deleted_rows {
        if let Some(row_values) = all_rows.get(*row_idx) {
            let where_parts: Vec<String> = column_names
                .iter()
                .zip(row_values.iter())
                .map(|(col_name, value)| {
                    let cell_value = display_cell_value(value);
                    let sql_value = format_sql_value(&cell_value);
                    if cell_value.is_null() {
                        format!("\"{}\" IS NULL", col_name)
                    } else {
                        format!("\"{}\" = {}", col_name, sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "DELETE FROM \"{}\" WHERE {};",
                table_name,
                where_parts.join(" AND ")
            ));
        }
    }

    // Generate INSERT statements for new rows
    for row_values in &new_rows {
        let column_list = column_names
            .iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(", ");

        let values: Vec<String> = row_values.iter().map(format_sql_value_from_value).collect();

        sql_statements.push(format!(
            "INSERT INTO \"{}\" ({}) VALUES ({});",
            table_name,
            column_list,
            values.join(", ")
        ));
    }

    // Copy to clipboard
    let sql = sql_statements.join("\n");
    cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql.clone()));

    tracing::info!(
        "Generated {} SQL statements and copied to clipboard",
        sql_statements.len()
    );
}

#[allow(clippy::too_many_arguments)]
pub(in crate::main_view) fn handle_load_fk_values_event(
    connection_id: Uuid,
    referenced_table: &str,
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

    let Some(connection) = app_state.connections.get_for_database_cached(
        connection_id,
        viewer_entity.read(cx).database_name().as_deref(),
    ) else {
        tracing::error!("LoadFkValues: Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let referenced_table = referenced_table.to_string();
    let referenced_columns = referenced_columns.to_vec();
    let query = query.map(|value| value.to_string());
    let effective_limit = limit.clamp(1, 10);

    window
        .spawn(cx, async move |cx| {
            let table_object_name = parse_sql_object_name(&referenced_table);
            let label_column = best_fk_label_column(
                connection.as_schema_introspection(),
                &referenced_table,
                &referenced_columns,
            )
            .await;

            let selected_columns = if referenced_columns.is_empty() {
                if let Some(label_column) = &label_column {
                    vec![label_column.clone()]
                } else {
                    vec!["id".to_string()]
                }
            } else {
                referenced_columns.clone()
            };

            let mut projected_columns = selected_columns.clone();

            if let Some(label_column) = &label_column
                && !selected_columns.iter().any(|column| column == label_column)
            {
                projected_columns.push(label_column.clone());
            }

            let mut where_parts = Vec::new();
            if let Some(query) = query.as_ref().map(|q| q.trim()).filter(|q| !q.is_empty()) {
                let escaped_like = escape_sql_like_literal(query);
                for column in &selected_columns {
                    let escaped_column = connection.quote_identifier(column);
                    let searchable_expr = connection.search_text_cast_expression(&escaped_column);
                    where_parts.push(format!(
                        "LOWER({}) LIKE LOWER('%{}%') ESCAPE '\\'",
                        searchable_expr, escaped_like
                    ));
                }
                if let Some(label_column) = &label_column
                    && !selected_columns.iter().any(|column| column == label_column)
                {
                    let escaped_column = connection.quote_identifier(label_column);
                    let searchable_expr = connection.search_text_cast_expression(&escaped_column);
                    where_parts.push(format!(
                        "LOWER({}) LIKE LOWER('%{}%') ESCAPE '\\'",
                        searchable_expr, escaped_like
                    ));
                }
            }

            let where_clause = if where_parts.is_empty() {
                None
            } else {
                Some(where_parts.join(" OR "))
            };

            let order_columns = if let Some(label_column) = &label_column {
                if selected_columns.iter().any(|column| column == label_column) {
                    selected_columns.clone()
                } else {
                    let mut with_label = selected_columns.clone();
                    with_label.push(label_column.clone());
                    with_label
                }
            } else {
                selected_columns.clone()
            };

            let sql = match connection.select_distinct_rows_sql(
                &table_object_name,
                &projected_columns,
                where_clause.as_deref(),
                &order_columns,
                effective_limit as u64,
            ) {
                Ok(sql) => sql,
                Err(error) => {
                    tracing::error!(
                        "LoadFkValues: failed to build SQL for table {}: {}",
                        referenced_table,
                        error
                    );
                    return anyhow::Ok(());
                }
            };

            match connection.query(&sql, &[]).await {
                Ok(result) => {
                    let values: Vec<FkSelectItem> = result
                        .rows
                        .iter()
                        .filter_map(|row| {
                            let value = row.values.first()?.to_string();
                            let label = if row.values.len() > 1 {
                                let extra =
                                    row.values.get(1).map(|v| v.to_string()).unwrap_or_default();
                                format!("{} - {}", value, extra)
                            } else {
                                value.clone()
                            };
                            Some(FkSelectItem { value, label })
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
                            referenced_table.clone(),
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

fn is_string_like_type(data_type: &str) -> bool {
    let normalized = data_type.to_ascii_lowercase();
    normalized.contains("char")
        || normalized.contains("text")
        || normalized.contains("name")
        || normalized.contains("json")
        || normalized.contains("uuid")
        || normalized.contains("enum")
}

async fn best_fk_label_column(
    schema_introspection: Option<&dyn zqlz_core::SchemaIntrospection>,
    table_name: &str,
    referenced_columns: &[String],
) -> Option<String> {
    let schema_introspection = schema_introspection?;

    let (schema_name, relation_name) = if table_name.contains('.') {
        let mut parts = table_name.splitn(2, '.');
        let left = parts.next();
        let right = parts.next();
        match (left, right) {
            (Some(schema), Some(table)) if !schema.is_empty() && !table.is_empty() => {
                (Some(schema), table)
            }
            _ => (None, table_name),
        }
    } else {
        (None, table_name)
    };

    let columns = schema_introspection
        .get_columns(schema_name, relation_name)
        .await
        .ok()?;

    let preferred = ["name", "title", "label", "description", "email", "username"];
    for preferred_name in preferred {
        if let Some(column) = columns.iter().find(|column| {
            !referenced_columns
                .iter()
                .any(|fk_col| fk_col == &column.name)
                && column.name.eq_ignore_ascii_case(preferred_name)
                && is_string_like_type(&column.data_type)
        }) {
            return Some(column.name.clone());
        }
    }

    columns
        .iter()
        .find(|column| {
            !referenced_columns
                .iter()
                .any(|fk_col| fk_col == &column.name)
                && is_string_like_type(&column.data_type)
        })
        .map(|column| column.name.clone())
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

    let Some(connection) = app_state.connections.get_for_database_cached(
        connection_id,
        viewer_entity.read(cx).database_name().as_deref(),
    ) else {
        tracing::error!(
            "LoadDistinctValues: Connection not found: {}",
            connection_id
        );
        return;
    };

    let connection = connection.clone();
    let table_name = table_name.to_string();
    let column_name = column_name.to_string();

    window
        .spawn(cx, async move |cx| {
            let table_object_name = parse_sql_object_name(&table_name);
            let escaped_column = connection.quote_identifier(&column_name);
            let where_clause = format!("{} IS NOT NULL", escaped_column);
            let sql = match connection.select_distinct_rows_sql(
                &table_object_name,
                std::slice::from_ref(&column_name),
                Some(&where_clause),
                std::slice::from_ref(&column_name),
                500,
            ) {
                Ok(sql) => sql,
                Err(error) => {
                    tracing::error!(
                        "LoadDistinctValues: failed to build SQL for {}.{}: {}",
                        table_name,
                        column_name,
                        error
                    );
                    return anyhow::Ok(());
                }
            };

            match connection.query(&sql, &[]).await {
                Ok(result) => {
                    let values: Vec<String> = result
                        .rows
                        .iter()
                        .filter_map(|row| row.values.first().map(|v| v.to_string()))
                        .collect();

                    let count = values.len();

                    if values.is_empty() {
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
                        let filter_value = values.join(", ");
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
                Err(e) => {
                    tracing::error!(
                        "LoadDistinctValues: Failed to query distinct values for {}.{}: {}",
                        table_name,
                        column_name,
                        e
                    );
                    _ = viewer_entity.update_in(cx, |_viewer, window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to load distinct values: {}", e)),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

fn parse_sql_object_name(object_name: &str) -> SqlObjectName {
    if object_name.contains('.') {
        let mut parts = object_name.splitn(2, '.');
        match (parts.next(), parts.next()) {
            (Some(namespace), Some(name)) if !namespace.is_empty() && !name.is_empty() => {
                SqlObjectName::with_namespace(namespace, name)
            }
            _ => SqlObjectName::new(object_name),
        }
    } else {
        SqlObjectName::new(object_name)
    }
}
