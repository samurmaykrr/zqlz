use super::*;

impl TableViewerDelegate {
    pub fn handle_paste(
        &mut self,
        anchor: zqlz_ui::widgets::table::CellPosition,
        data: &str,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        // Parse TSV data into rows and columns
        let paste_rows: Vec<Vec<String>> = data
            .lines()
            .map(|line| line.split('\t').map(|s| s.to_string()).collect())
            .collect();

        if paste_rows.is_empty() {
            return;
        }

        let _paste_height = paste_rows.len();
        let _paste_width = paste_rows.iter().map(|row| row.len()).max().unwrap_or(0);

        let start_row = anchor.row;
        let start_col = if anchor.col == 0 { 1 } else { anchor.col };

        let mut modified_cells = Vec::new();

        let display_row_count = if self.is_filtering {
            self.filtered_row_indices.len()
        } else {
            self.rows.len()
        };

        for (paste_row_idx, paste_row) in paste_rows.iter().enumerate() {
            let display_row = start_row + paste_row_idx;

            if display_row >= display_row_count {
                break;
            }

            let actual_row = self.get_actual_row_index(display_row);

            if actual_row >= self.rows.len() {
                break;
            }

            for (paste_col_idx, paste_value) in paste_row.iter().enumerate() {
                let target_col = start_col + paste_col_idx;
                let data_col = target_col - 1;

                if data_col >= self.column_meta.len() {
                    break;
                }

                let original_value = self
                    .rows
                    .get(actual_row)
                    .and_then(|r| r.get(data_col))
                    .cloned()
                    .unwrap_or_default();

                if paste_value != &original_value {
                    modified_cells.push((actual_row, target_col, paste_value.clone()));
                }
            }
        }

        let cells_count = modified_cells.len();

        if self.auto_commit_mode {
            for (row, col, new_value) in modified_cells {
                let data_col = col - 1;

                let original_value = self
                    .rows
                    .get(row)
                    .and_then(|r| r.get(data_col))
                    .cloned()
                    .unwrap_or_default();

                let table_name = self.table_name.clone();
                let connection_id = self.connection_id;
                let all_row_values = self.rows.get(row).cloned().unwrap_or_default();
                let all_column_names: Vec<String> =
                    self.column_meta.iter().map(|c| c.name.clone()).collect();
                let all_column_types: Vec<String> = self
                    .column_meta
                    .iter()
                    .map(|c| c.data_type.clone())
                    .collect();
                let column_name = self
                    .column_meta
                    .get(data_col)
                    .map(|c| c.name.clone())
                    .unwrap_or_default();

                if let Some(row_data) = self.rows.get_mut(row) {
                    if let Some(cell) = row_data.get_mut(data_col) {
                        *cell = new_value.clone();
                    }
                }

                let viewer_panel = self.viewer_panel.clone();
                _ = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::SaveCell {
                        table_name,
                        connection_id,
                        row,
                        col,
                        column_name,
                        new_value,
                        original_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                    });
                });
            }
        } else {
            for (row, col, new_value) in modified_cells {
                let data_col = col - 1;

                let original_value = self
                    .rows
                    .get(row)
                    .and_then(|r| r.get(data_col))
                    .cloned()
                    .unwrap_or_default();

                if let Some(row_data) = self.rows.get_mut(row) {
                    if let Some(cell) = row_data.get_mut(data_col) {
                        *cell = new_value.clone();
                    }
                }

                self.pending_changes.modified_cells.insert(
                    (row, col),
                    PendingCellChange {
                        original_value,
                        new_value,
                    },
                );
            }
        }

        cx.notify();
        tracing::info!("Pasted {} cell(s)", cells_count);
    }
}
