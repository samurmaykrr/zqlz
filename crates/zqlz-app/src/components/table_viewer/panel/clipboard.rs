use super::*;

impl TableViewerPanel {
    pub fn copy_selection(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let (column_names, data_to_copy): (Vec<String>, Vec<Vec<String>>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let column_names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();

                let rows_to_copy: Vec<Vec<String>> = if !self.selected_rows.is_empty() {
                    self.selected_rows
                        .iter()
                        .map(|&display_idx| delegate.get_actual_row_index(display_idx))
                        .filter_map(|actual_idx| delegate.rows.get(actual_idx).cloned())
                        .collect()
                } else {
                    if delegate.is_filtering {
                        delegate
                            .filtered_row_indices
                            .iter()
                            .filter_map(|&idx| delegate.rows.get(idx).cloned())
                            .collect()
                    } else {
                        delegate.rows.clone()
                    }
                };

                (column_names, rows_to_copy)
            });

        if data_to_copy.is_empty() {
            return;
        }

        let header = column_names.join("\t");
        let rows: Vec<String> = data_to_copy.iter().map(|row| row.join("\t")).collect();
        let tsv = format!("{}\n{}", header, rows.join("\n"));

        cx.write_to_clipboard(gpui::ClipboardItem::new_string(tsv));

        tracing::info!("Copied {} rows to clipboard", data_to_copy.len());
    }

    pub fn paste_clipboard(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let Some(connection_id) = self.connection_id else {
            return;
        };

        let Some(table_name) = &self.table_name else {
            return;
        };

        let Some(clipboard_item) = cx.read_from_clipboard() else {
            tracing::warn!("No clipboard data available");
            return;
        };

        let Some(clipboard_text) = clipboard_item.text() else {
            tracing::warn!("Clipboard does not contain text");
            return;
        };

        let lines: Vec<&str> = clipboard_text.lines().collect();
        if lines.is_empty() {
            return;
        }

        let (column_names, all_column_names): (Vec<String>, Vec<String>) =
            table_state.read_with(cx, |table, _cx| {
                let delegate = table.delegate();
                let names: Vec<String> = delegate
                    .column_meta
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                (names.clone(), names)
            });

        let all_column_types: Vec<String> = table_state.read_with(cx, |table, _cx| {
            table
                .delegate()
                .column_meta
                .iter()
                .map(|c| c.data_type.clone())
                .collect()
        });

        let first_line_cells: Vec<&str> = lines[0].split('\t').collect();
        let has_header = first_line_cells.len() == column_names.len()
            && first_line_cells
                .iter()
                .zip(column_names.iter())
                .all(|(a, b)| a.trim() == b);

        let data_lines = if has_header { &lines[1..] } else { &lines[..] };

        if data_lines.is_empty() {
            tracing::info!("No data rows to paste");
            return;
        }

        let start_row = self.selected_rows.iter().min().copied().map(|display_idx| {
            table_state.read_with(cx, |table, _cx| {
                table.delegate().get_actual_row_index(display_idx)
            })
        });

        let current_row_count = table_state.read_with(cx, |table, _cx| table.delegate().rows.len());

        for (line_idx, line) in data_lines.iter().enumerate() {
            let cells: Vec<&str> = line.split('\t').collect();

            let target_row = match start_row {
                Some(row) => row + line_idx,
                None => current_row_count + line_idx,
            };

            let row_exists =
                table_state.read_with(cx, |table, _cx| target_row < table.delegate().rows.len());

            if row_exists {
                let all_row_values: Vec<String> = table_state.read_with(cx, |table, _cx| {
                    table
                        .delegate()
                        .rows
                        .get(target_row)
                        .cloned()
                        .unwrap_or_default()
                });

                for (col_idx, cell_value) in cells.iter().enumerate() {
                    if col_idx >= column_names.len() {
                        break;
                    }

                    let column_name = column_names[col_idx].clone();
                    let new_value = cell_value.trim().to_string();
                    let original_value = all_row_values.get(col_idx).cloned().unwrap_or_default();

                    cx.emit(TableViewerEvent::SaveCell {
                        table_name: table_name.clone(),
                        connection_id,
                        row: target_row,
                        col: col_idx,
                        column_name,
                        new_value,
                        original_value,
                        all_row_values: all_row_values.clone(),
                        all_column_names: all_column_names.clone(),
                        all_column_types: all_column_types.clone(),
                    });
                }
            } else {
                tracing::info!(
                    "Paste: Would add new row {} with {} cells",
                    target_row,
                    cells.len()
                );
            }
        }

        tracing::info!("Pasted {} rows from clipboard", data_lines.len());
        cx.notify();
    }
}
