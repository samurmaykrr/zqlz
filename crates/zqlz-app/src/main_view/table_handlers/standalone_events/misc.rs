//! This module contains miscellaneous standalone event handlers (SQL generation, foreign key loading).

use gpui::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use zqlz_core::ColumnMeta;

use crate::app::AppState;
use crate::components::{PendingCellChange, TableViewerPanel};

use crate::main_view::table_handlers_utils::formatting::format_sql_value;

pub(in crate::main_view) fn handle_generate_sql_event(
    table_name: String,
    modified_cells: HashMap<(usize, usize), PendingCellChange>,
    deleted_rows: HashSet<usize>,
    new_rows: Vec<Vec<String>>,
    column_meta: Vec<ColumnMeta>,
    all_rows: Vec<Vec<String>>,
    cx: &mut App,
) {
    let column_names: Vec<String> = column_meta.iter().map(|c| c.name.clone()).collect();
    let mut sql_statements: Vec<String> = Vec::new();

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
                .map(|(col_name, value): (&String, &String)| {
                    let sql_value = format_sql_value(value);
                    if value.is_empty() || value == "NULL" {
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
                .map(|(col_name, value): (&String, &String)| {
                    let sql_value = format_sql_value(value);
                    if value.is_empty() || value == "NULL" {
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

        let values: Vec<String> = row_values.iter().map(|v| format_sql_value(v)).collect();

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

pub(in crate::main_view) fn handle_load_fk_values_event(
    connection_id: Uuid,
    referenced_table: &str,
    referenced_columns: &[String],
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    use crate::components::table_viewer::delegate::FkSelectItem;

    tracing::info!(
        "LoadFkValues event: table={}, columns={:?}",
        referenced_table,
        referenced_columns
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("LoadFkValues: No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("LoadFkValues: Connection not found: {}", connection_id);
        return;
    };

    let connection = connection.clone();
    let referenced_table = referenced_table.to_string();
    let referenced_columns = referenced_columns.to_vec();

    window
        .spawn(cx, async move |cx| {
            // Build the columns to select - typically just the PK column(s)
            // We'll also try to get a display label if there's another column
            let columns_to_select = if referenced_columns.is_empty() {
                "*".to_string()
            } else {
                referenced_columns.join(", ")
            };

            // Query the referenced table for all values
            // Using DISTINCT to avoid duplicates, ordered for better UX
            let sql = format!(
                "SELECT DISTINCT {} FROM {} ORDER BY {} LIMIT 1000",
                columns_to_select,
                referenced_table,
                referenced_columns.first().unwrap_or(&"1".to_string())
            );

            match connection.query(&sql, &[]).await {
                Ok(result) => {
                    // Convert results to FkSelectItem with value and label
                    let values: Vec<FkSelectItem> = result
                        .rows
                        .iter()
                        .filter_map(|row| {
                            let value = row.values.first()?.to_string();
                            // Try to build a display label from multiple columns if available
                            let label = if row.values.len() > 1 {
                                // Format: "id - name" or similar for better UX
                                let extra = row
                                    .values
                                    .get(1)
                                    .map(|v| v.to_string())
                                    .unwrap_or_default();
                                format!("{} - {}", value, extra)
                            } else {
                                value.clone()
                            };
                            Some(FkSelectItem { value, label })
                        })
                        .collect();

                    tracing::info!(
                        "LoadFkValues: Loaded {} values from {}",
                        values.len(),
                        referenced_table
                    );

                    // Cache the values in the viewer and update the dropdown if open
                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        viewer.set_fk_values(referenced_table.clone(), values, window, cx);
                    });
                }
                Err(e) => {
                    tracing::error!(
                        "LoadFkValues: Failed to query {}: {}",
                        referenced_table,
                        e
                    );
                }
            }

            anyhow::Ok(())
        })
        .detach();
}
