use super::*;

impl TableViewerDelegate {
    pub fn start_bulk_editing(
        &mut self,
        anchor: zqlz_ui::widgets::table::CellPosition,
        cells: Vec<zqlz_ui::widgets::table::CellPosition>,
        initial_char: Option<String>,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        tracing::info!(
            "start_bulk_editing: anchor=({}, {}), cells_count={}, initial_char={:?}",
            anchor.row,
            anchor.col,
            cells.len(),
            initial_char
        );

        if self.editing_cell.is_some() {
            tracing::info!("Skipping bulk edit: already editing a cell");
            return;
        }

        if anchor.col == 0 {
            tracing::info!("Skipping bulk edit: row number column");
            return;
        }

        if self.disable_inline_edit {
            tracing::info!("Skipping bulk edit: inline editing disabled");
            return;
        }

        self.bulk_edit_cells = Some(cells);

        let actual_row = self.get_actual_row_index(anchor.row);
        let data_col = anchor.col - 1;
        let anchor_value = self
            .rows
            .get(actual_row)
            .and_then(|r| r.get(data_col))
            .cloned()
            .unwrap_or_default();

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Edit all selected cells...")
                .emit_tab_event(true)
                .emit_arrow_event(true)
        });

        if let Some(ref char) = initial_char {
            input.update(cx, |state, cx| {
                state.replace(char.clone(), window, cx);
            });
        } else {
            let display_value = anchor_value.replace('\n', " ").replace('\r', "");
            input.update(cx, |state, cx| {
                state.replace(display_value, window, cx);
            });
        }

        self.ignore_next_blur = true;

        cx.subscribe(&input, |table, _input, event: &InputEvent, cx| {
            match event {
                InputEvent::Blur => {
                    if table.delegate().ignore_next_blur {
                        table.delegate_mut().ignore_next_blur = false;
                        return;
                    }
                    table.delegate_mut().stop_editing(true, cx);
                }
                InputEvent::PressEnter { .. } => {
                    table.delegate_mut().stop_editing(true, cx);
                }
                InputEvent::PressTab { shift } => {
                    table.delegate_mut().stop_editing(true, cx);
                    // navigation omitted for brevity
                }
                InputEvent::PressArrow { direction } => {
                    table.delegate_mut().stop_editing(true, cx);
                    // navigation omitted for brevity
                }
                _ => {}
            }
        })
        .detach();

        self.cell_input = Some(input.clone());
        self.editing_cell = Some((actual_row, anchor.col));
        self.editing_cell_has_newlines = false;

        input.update(cx, |state, cx| {
            state.focus(window, cx);
        });

        let viewer_panel = self.viewer_panel.clone();
        _ = viewer_panel.update(cx, |_panel, cx| {
            cx.emit(TableViewerEvent::InlineEditStarted);
        });

        cx.notify();
    }

    pub fn handle_bulk_paste(
        &mut self,
        cells: Vec<zqlz_ui::widgets::table::CellPosition>,
        value: String,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        tracing::info!(
            "handle_bulk_paste: cells_count={}, value_len={}",
            cells.len(),
            value.len()
        );
        self.apply_value_to_cells(&cells, value, cx);
        cx.notify();
    }

    pub(super) fn apply_value_to_cells(
        &mut self,
        cells: &[zqlz_ui::widgets::table::CellPosition],
        new_value: String,
        cx: &mut Context<TableState<Self>>,
    ) {
        tracing::info!(
            "apply_value_to_cells: processing {} cells with new_value='{}'",
            cells.len(),
            new_value
        );
        let mut updated_count = 0;

        for cell in cells {
            if cell.col == 0 {
                continue;
            }

            let row = self.get_actual_row_index(cell.row);
            let data_col = cell.col - 1;

            if row >= self.rows.len() || data_col >= self.column_meta.len() {
                continue;
            }

            let original_value = self
                .rows
                .get(row)
                .and_then(|r| r.get(data_col))
                .cloned()
                .unwrap_or_default();

            if new_value == original_value {
                continue;
            }

            if self.auto_commit_mode {
                let total_rows = self.rows.len();
                let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

                if let Some(new_row_idx) = new_row_idx {
                    if let Some(row_data) = self.rows.get_mut(row) {
                        if let Some(cell_data) = row_data.get_mut(data_col) {
                            *cell_data = new_value.clone();
                        }
                    }
                    self.pending_changes.update_new_row_cell(
                        new_row_idx,
                        data_col,
                        new_value.clone(),
                    );
                } else {
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
                        if let Some(cell_data) = row_data.get_mut(data_col) {
                            *cell_data = new_value.clone();
                        }
                    }

                    let viewer_panel = self.viewer_panel.clone();
                    _ = viewer_panel.update(cx, |_panel, cx| {
                        cx.emit(TableViewerEvent::SaveCell {
                            table_name,
                            connection_id,
                            row,
                            col: data_col,
                            column_name,
                            new_value: new_value.clone(),
                            original_value: original_value.clone(),
                            all_row_values,
                            all_column_names,
                            all_column_types,
                        });
                    });
                }
            } else {
                let total_rows = self.rows.len();
                let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

                if let Some(row_data) = self.rows.get_mut(row) {
                    if let Some(cell_data) = row_data.get_mut(data_col) {
                        if let Some(new_row_idx) = new_row_idx {
                            self.pending_changes.update_new_row_cell(
                                new_row_idx,
                                data_col,
                                new_value.clone(),
                            );
                        } else {
                            self.pending_changes.modified_cells.insert(
                                (row, data_col),
                                PendingCellChange {
                                    original_value: original_value.clone(),
                                    new_value: new_value.clone(),
                                },
                            );
                        }

                        *cell_data = new_value.clone();
                    }
                }
            }

            updated_count += 1;
        }

        tracing::info!("apply_value_to_cells: updated {} cells", updated_count);
    }
}
