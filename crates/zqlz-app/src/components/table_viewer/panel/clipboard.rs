use super::*;

impl TableViewerPanel {
    pub fn copy_selection(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = &self.table_state else {
            return;
        };

        let selection_payload = table_state.read_with(cx, |table, _cx| {
            let delegate = table.delegate();
            let selected_cells = table.cell_selection().selected_cells();

            if !selected_cells.is_empty() {
                let selected_positions: std::collections::HashSet<(usize, usize)> = selected_cells
                    .iter()
                    .map(|cell| (cell.row, cell.col))
                    .collect();
                let min_row = selected_cells
                    .iter()
                    .map(|cell| cell.row)
                    .min()
                    .unwrap_or(0);
                let max_row = selected_cells
                    .iter()
                    .map(|cell| cell.row)
                    .max()
                    .unwrap_or(0);
                let min_col = selected_cells
                    .iter()
                    .map(|cell| cell.col)
                    .min()
                    .unwrap_or(0);
                let max_col = selected_cells
                    .iter()
                    .map(|cell| cell.col)
                    .max()
                    .unwrap_or(0);

                let rows: Vec<Vec<String>> = (min_row..=max_row)
                    .map(|display_row| {
                        let actual_row = delegate.get_actual_row_index(display_row);
                        (min_col..=max_col)
                            .map(|display_col| {
                                if !selected_positions.contains(&(display_row, display_col)) {
                                    return String::new();
                                }

                                if display_col == 0 {
                                    return (delegate.row_offset + actual_row + 1).to_string();
                                }

                                delegate
                                    .rows
                                    .get(actual_row)
                                    .and_then(|row| row.get(display_col - 1))
                                    .map(|value| value.display_for_table())
                                    .unwrap_or_default()
                            })
                            .collect()
                    })
                    .collect();

                return (false, rows);
            }

            let selected_rows = self.selected_display_rows(_cx);
            let rows_to_copy: Vec<Vec<String>> = if !selected_rows.is_empty() {
                selected_rows
                    .iter()
                    .map(|&display_idx| delegate.get_actual_row_index(display_idx))
                    .filter_map(|actual_idx| delegate.rows.get(actual_idx))
                    .map(|row| row.iter().map(|value| value.display_for_table()).collect())
                    .collect()
            } else {
                if delegate.is_filtering {
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
                }
            };

            (true, rows_to_copy)
        });

        let (include_header, data_to_copy) = selection_payload;

        if data_to_copy.is_empty() {
            return;
        }

        let rows: Vec<String> = data_to_copy.iter().map(|row| row.join("\t")).collect();
        let tsv = if include_header {
            let column_names: Vec<String> = table_state.read_with(cx, |table, _cx| {
                table
                    .delegate()
                    .column_meta
                    .iter()
                    .map(|column| column.name.clone())
                    .collect()
            });
            format!("{}\n{}", column_names.join("\t"), rows.join("\n"))
        } else {
            rows.join("\n")
        };

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

        let start_row = self
            .selected_display_cell_anchor(cx)
            .map(|(display_row, _display_col)| {
                table_state.read_with(cx, |table, _cx| {
                    table.delegate().get_actual_row_index(display_row)
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
                        .map(|r| r.iter().map(|v| v.display_for_table()).collect())
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
