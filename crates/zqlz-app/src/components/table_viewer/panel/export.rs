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

impl TableViewerPanel {
    pub fn export_csv(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (column_names, rows): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let data = if delegate.is_filtering {
                    delegate
                        .filtered_row_indices
                        .iter()
                        .filter_map(|&idx| delegate.rows.get(idx).cloned())
                        .collect()
                } else {
                    delegate.rows.clone()
                };

                (names, data)
            });

        if rows.is_empty() {
            tracing::info!("No data to export");
            return;
        }

        let mut csv = String::new();
        let header: Vec<String> = column_names.iter().map(|n| escape_csv_field(n)).collect();
        csv.push_str(&header.join(","));
        csv.push('\n');

        for row in &rows {
            let escaped_row: Vec<String> = row.iter().map(|v| escape_csv_field(v)).collect();
            csv.push_str(&escaped_row.join(","));
            csv.push('\n');
        }

        cx.write_to_clipboard(gpui::ClipboardItem::new_string(csv));
        tracing::info!("Exported {} rows as CSV to clipboard", rows.len());
    }

    pub fn export_json(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (column_names, rows): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let data = if delegate.is_filtering {
                    delegate
                        .filtered_row_indices
                        .iter()
                        .filter_map(|&idx| delegate.rows.get(idx).cloned())
                        .collect()
                } else {
                    delegate.rows.clone()
                };

                (names, data)
            });

        if rows.is_empty() {
            tracing::info!("No data to export");
            return;
        }

        let mut json = String::from("[\n");
        for (row_idx, row) in rows.iter().enumerate() {
            json.push_str("  {\n");
            for (col_idx, value) in row.iter().enumerate() {
                let col_name = column_names.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                let escaped_value = escape_json_string(value);
                json.push_str(&format!("    \"{}\": \"{}\"", col_name, escaped_value));
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

        cx.write_to_clipboard(gpui::ClipboardItem::new_string(json));
        tracing::info!("Exported {} rows as JSON to clipboard", rows.len());
    }

    pub fn export_sql(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let table_name = self
            .table_name
            .clone()
            .unwrap_or_else(|| "table".to_string());

        let (column_names, rows): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let data = if delegate.is_filtering {
                    delegate
                        .filtered_row_indices
                        .iter()
                        .filter_map(|&idx| delegate.rows.get(idx).cloned())
                        .collect()
                } else {
                    delegate.rows.clone()
                };

                (names, data)
            });

        if rows.is_empty() {
            tracing::info!("No data to export");
            return;
        }

        let mut sql = String::new();
        let column_list = column_names
            .iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(", ");

        for row in &rows {
            let values: Vec<String> = row
                .iter()
                .map(|v| {
                    if v.is_empty() || v == "NULL" {
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

        cx.write_to_clipboard(gpui::ClipboardItem::new_string(sql));
        tracing::info!("Exported {} rows as SQL INSERT to clipboard", rows.len());
    }
}

// re-exported via panel/mod.rs when necessary
