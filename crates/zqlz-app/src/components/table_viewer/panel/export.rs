use std::collections::HashSet;

use super::*;

pub(crate) fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

pub(crate) fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

fn build_csv_content(column_names: &[String], rows: &[Vec<String>]) -> String {
    let mut csv = String::new();
    let header: Vec<String> = column_names.iter().map(|n| escape_csv_field(n)).collect();
    csv.push_str(&header.join(","));
    csv.push('\n');

    for row in rows {
        let escaped_row: Vec<String> = row
            .iter()
            .map(|v| {
                if v.eq_ignore_ascii_case("null") {
                    String::new()
                } else {
                    escape_csv_field(v)
                }
            })
            .collect();
        csv.push_str(&escaped_row.join(","));
        csv.push('\n');
    }
    csv
}

fn build_json_content(column_names: &[String], rows: &[Vec<String>]) -> String {
    let mut json = String::from("[\n");
    for (row_idx, row) in rows.iter().enumerate() {
        json.push_str("  {\n");
        for (col_idx, value) in row.iter().enumerate() {
            let col_name = column_names.get(col_idx).map(|s| s.as_str()).unwrap_or("");
            let json_value = if value.eq_ignore_ascii_case("null") {
                "null".to_string()
            } else {
                format!("\"{}\"", escape_json_string(value))
            };
            json.push_str(&format!("    \"{}\": {}", col_name, json_value));
            if col_idx < row.len() - 1 {
                json.push(',');
            }
            json.push('\n');
        }
        json.push_str("  }");
        if row_idx < rows.len() - 1 {
            json.push(',');
        }
        json.push('\n');
    }
    json.push(']');
    json
}

fn build_sql_content(table_name: &str, column_names: &[String], rows: &[Vec<String>]) -> String {
    let mut sql = String::new();
    let column_list = column_names
        .iter()
        .map(|n| format!("\"{}\"", n))
        .collect::<Vec<_>>()
        .join(", ");

    for row in rows {
        let values: Vec<String> = row
            .iter()
            .map(|v| {
                if v.eq_ignore_ascii_case("null") {
                    "NULL".to_string()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            })
            .collect();

        sql.push_str(&format!(
            "INSERT INTO \"{}\" ({}) VALUES ({});\n",
            table_name,
            column_list,
            values.join(", ")
        ));
    }
    sql
}

impl TableViewerPanel {
    fn get_all_export_data(&self, cx: &Context<Self>) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        let table_state = self.table_state.as_ref()?;

        let (column_names, rows): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let data: Vec<Vec<String>> = if delegate.is_filtering {
                    delegate
                        .filtered_row_indices
                        .iter()
                        .filter_map(|&idx| delegate.rows.get(idx))
                        .map(|row| row.iter().map(|v| v.display_for_table()).collect())
                        .collect()
                } else {
                    delegate
                        .rows
                        .iter()
                        .map(|row| row.iter().map(|v| v.display_for_table()).collect())
                        .collect()
                };

                (names, data)
            });

        if rows.is_empty() {
            tracing::info!("No data to export");
            return None;
        }

        Some((column_names, rows))
    }

    /// Collect data for only the selected rows (determined by cell selection).
    /// Returns full rows for any row that has at least one selected cell.
    fn get_selected_export_data(
        &self,
        cx: &Context<Self>,
    ) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        let table_state = self.table_state.as_ref()?;

        let (column_names, rows): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let mut sorted_rows: Vec<usize> = if !table.cell_selection().is_empty() {
                    table
                        .cell_selection()
                        .selected_cells()
                        .iter()
                        .map(|cell| cell.row)
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect()
                } else {
                    table.selected_row().into_iter().collect()
                };
                sorted_rows.sort_unstable();

                let data: Vec<Vec<String>> = sorted_rows
                    .iter()
                    .map(|&display_row| delegate.get_actual_row_index(display_row))
                    .filter_map(|actual_row| delegate.rows.get(actual_row))
                    .map(|row| row.iter().map(|v| v.display_for_table()).collect())
                    .collect();

                (names, data)
            });

        if rows.is_empty() {
            tracing::info!("No selected rows to export");
            return None;
        }

        Some((column_names, rows))
    }

    // --- Clipboard exports ---

    pub fn export_csv(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let row_count = rows.len();
        let csv = build_csv_content(&column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(csv));
        tracing::info!("Exported {} rows as CSV to clipboard", row_count);
    }

    pub fn export_json(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let row_count = rows.len();
        let json = build_json_content(&column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(json));
        tracing::info!("Exported {} rows as JSON to clipboard", row_count);
    }

    pub fn export_sql(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let table_name = self
            .table_name
            .clone()
            .unwrap_or_else(|| "table".to_string());
        let row_count = rows.len();
        let sql = build_sql_content(&table_name, &column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql));
        tracing::info!("Exported {} rows as SQL INSERT to clipboard", row_count);
    }

    // --- File exports ---

    pub fn export_csv_to_file(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let default_filename = self.table_name.as_deref().unwrap_or("export");
        let receiver = cx.prompt_for_new_path(
            &std::path::PathBuf::from(format!("{}.csv", default_filename)),
            None,
        );
        cx.spawn(async move |_this, _cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => return anyhow::Ok(()),
            };
            let csv = build_csv_content(&column_names, &rows);
            if let Err(e) = std::fs::write(&path, csv) {
                tracing::error!("Failed to write CSV file: {}", e);
            } else {
                tracing::info!("Exported {} rows to {}", rows.len(), path.display());
            }
            anyhow::Ok(())
        })
        .detach();
    }

    pub fn export_json_to_file(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let default_filename = self.table_name.as_deref().unwrap_or("export");
        let receiver = cx.prompt_for_new_path(
            &std::path::PathBuf::from(format!("{}.json", default_filename)),
            None,
        );
        cx.spawn(async move |_this, _cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => return anyhow::Ok(()),
            };
            let json = build_json_content(&column_names, &rows);
            if let Err(e) = std::fs::write(&path, json) {
                tracing::error!("Failed to write JSON file: {}", e);
            } else {
                tracing::info!("Exported {} rows to {}", rows.len(), path.display());
            }
            anyhow::Ok(())
        })
        .detach();
    }

    pub fn export_sql_to_file(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_all_export_data(cx) else {
            return;
        };
        let table_name = self
            .table_name
            .clone()
            .unwrap_or_else(|| "table".to_string());
        let default_filename = self.table_name.as_deref().unwrap_or("export");
        let receiver = cx.prompt_for_new_path(
            &std::path::PathBuf::from(format!("{}.sql", default_filename)),
            None,
        );
        cx.spawn(async move |_this, _cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => return anyhow::Ok(()),
            };
            let sql = build_sql_content(&table_name, &column_names, &rows);
            if let Err(e) = std::fs::write(&path, sql) {
                tracing::error!("Failed to write SQL file: {}", e);
            } else {
                tracing::info!("Exported {} rows to {}", rows.len(), path.display());
            }
            anyhow::Ok(())
        })
        .detach();
    }

    // --- Selected rows exports ---

    pub fn export_selected_csv(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_selected_export_data(cx) else {
            return;
        };
        let row_count = rows.len();
        let csv = build_csv_content(&column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(csv));
        tracing::info!("Exported {} selected rows as CSV to clipboard", row_count);
    }

    pub fn export_selected_json(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_selected_export_data(cx) else {
            return;
        };
        let row_count = rows.len();
        let json = build_json_content(&column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(json));
        tracing::info!("Exported {} selected rows as JSON to clipboard", row_count);
    }

    pub fn export_selected_sql(&mut self, cx: &mut Context<Self>) {
        let Some((column_names, rows)) = self.get_selected_export_data(cx) else {
            return;
        };
        let table_name = self
            .table_name
            .clone()
            .unwrap_or_else(|| "table".to_string());
        let row_count = rows.len();
        let sql = build_sql_content(&table_name, &column_names, &rows);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql));
        tracing::info!(
            "Exported {} selected rows as SQL INSERT to clipboard",
            row_count
        );
    }
}
