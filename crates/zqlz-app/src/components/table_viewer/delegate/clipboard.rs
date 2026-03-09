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

                let data_type = self
                    .column_meta
                    .get(data_col)
                    .map(|c| c.data_type.as_str())
                    .unwrap_or("text");
                let typed_value = Value::parse_from_string(paste_value, data_type);

                let original_value = self
                    .rows
                    .get(actual_row)
                    .and_then(|r| r.get(data_col))
                    .cloned()
                    .unwrap_or_default();

                if typed_value != original_value {
                    modified_cells.push((actual_row, target_col, typed_value));
                }
            }
        }

        let cells_count = modified_cells.len();

        let mut undo_edits = Vec::new();

        if self.auto_commit_mode {
            for (row, col, new_value) in modified_cells {
                let data_col = col - 1;

                let original_value = self
                    .rows
                    .get(row)
                    .and_then(|r| r.get(data_col))
                    .cloned()
                    .unwrap_or_default();

                self.save_existing_cell_or_queue(row, data_col, new_value, &original_value, cx);
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

                undo_edits.push(UndoCellEdit {
                    row,
                    data_col,
                    old_value: original_value.clone(),
                    new_value: new_value.clone(),
                });

                self.store_pending_cell_change(row, data_col, new_value.clone(), &original_value);
            }
        }

        self.push_undo(UndoEntry { edits: undo_edits });
        cx.notify();
        tracing::info!("Pasted {} cell(s)", cells_count);
    }
}
