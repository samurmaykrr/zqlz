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
            let display_value = if anchor_value.is_null() {
                String::new()
            } else {
                let s = anchor_value.display_for_editor();
                s.replace('\n', " ").replace('\r', "")
            };
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
                InputEvent::PressTab { shift: _ } => {
                    table.delegate_mut().stop_editing(true, cx);
                    // navigation omitted for brevity
                }
                InputEvent::PressArrow { direction: _ } => {
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
        if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
            cx.emit(TableViewerEvent::InlineEditStarted);
        }) {
            tracing::error!(
                "Failed to emit InlineEditStarted from start_bulk_editing: {:?}",
                e
            );
        }

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
        self.apply_value_to_cells(&cells, &value, cx);
        cx.notify();
    }

    pub(crate) fn apply_value_to_cells(
        &mut self,
        cells: &[zqlz_ui::widgets::table::CellPosition],
        new_value_str: &str,
        cx: &mut Context<TableState<Self>>,
    ) {
        tracing::info!(
            "apply_value_to_cells: processing {} cells with new_value='{}'",
            cells.len(),
            new_value_str
        );
        let mut updated_count = 0;
        let mut undo_edits = Vec::new();

        for cell in cells {
            if cell.col == 0 {
                continue;
            }

            let row = self.get_actual_row_index(cell.row);
            let data_col = cell.col - 1;

            if row >= self.rows.len() || data_col >= self.column_meta.len() {
                continue;
            }

            let data_type = self
                .column_meta
                .get(data_col)
                .map(|c| c.data_type.as_str())
                .unwrap_or("text");
            let new_value = Value::parse_from_string(new_value_str, data_type);

            let original_value = self
                .rows
                .get(row)
                .and_then(|r| r.get(data_col))
                .cloned()
                .unwrap_or_default();

            if new_value == original_value {
                continue;
            }

            // Same null no-op rule as single-cell stop_editing: clearing the input on a
            // NULL cell should not replace NULL with an empty string across all selected cells.
            if new_value_str.is_empty() && original_value.is_null() {
                continue;
            }

            undo_edits.push(UndoCellEdit {
                row,
                data_col,
                old_value: original_value.clone(),
                new_value: new_value.clone(),
            });

            if self.auto_commit_mode {
                let total_rows = self.rows.len();
                let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

                if let Some(new_row_idx) = new_row_idx {
                    self.apply_value_locally(row, data_col, new_value.clone());
                    self.pending_changes
                        .update_new_row_cell(new_row_idx, data_col, new_value);
                } else {
                    self.save_existing_cell_or_queue(row, data_col, new_value, &original_value, cx);
                }
            } else {
                let total_rows = self.rows.len();
                let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

                if let Some(new_row_idx) = new_row_idx {
                    self.pending_changes.update_new_row_cell(
                        new_row_idx,
                        data_col,
                        new_value.clone(),
                    );
                    self.apply_value_locally(row, data_col, new_value);
                } else {
                    self.store_pending_cell_change(row, data_col, new_value, &original_value);
                }
            }

            updated_count += 1;
        }

        self.push_undo(UndoEntry { edits: undo_edits });
        tracing::info!("apply_value_to_cells: updated {} cells", updated_count);
    }
}
