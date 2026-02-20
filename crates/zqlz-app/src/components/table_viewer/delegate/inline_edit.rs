use super::*;

impl TableViewerDelegate {
    pub fn start_editing(
        &mut self,
        row: usize,
        col: usize,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        if col == 0 {
            return;
        }

        let actual_row = self.get_actual_row_index(row);

        if let Some((editing_row, editing_col)) = self.editing_cell {
            if editing_row == actual_row && editing_col == col {
                return;
            }
        }

        if self.editing_cell.is_some() {
            if self.cell_input.is_some() {
                self.stop_editing(true, cx);
            } else {
                self.clear_all_edit_states();
            }
        }

        let data_col = col - 1;

        if self.disable_inline_edit {
            self.emit_edit_cell_event(actual_row, col, data_col, cx);
            return;
        }

        if self.is_boolean_column(data_col) {
            self.toggle_boolean_cell(actual_row, col, cx);
            return;
        }

        // Binary/blob columns cannot be inline-edited; open in the Cell Editor panel
        let value = self
            .rows
            .get(actual_row)
            .and_then(|r| r.get(data_col))
            .cloned()
            .unwrap_or_default();

        if self.is_binary_column(data_col)
            || Self::is_bytes_placeholder(&value)
            || self.raw_bytes.contains_key(&(actual_row, data_col))
        {
            self.emit_edit_cell_event(actual_row, col, data_col, cx);
            return;
        }

        let has_newlines = value.contains('\n') || value.contains('\r');
        self.editing_cell_has_newlines = has_newlines;

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Edit value...")
                .emit_tab_event(true)
                .emit_arrow_event(true)
        });

        let text = if value.is_empty() {
            String::new()
        } else {
            value.replace('\n', " ").replace('\r', "")
        };
        input.update(cx, |state, cx| {
            state.replace(text, window, cx);
        });

        self.ignore_next_blur = true;

        cx.subscribe_in(
            &input,
            window,
            move |table, _input, event: &InputEvent, window, cx| match event {
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
                    let shift = *shift;
                    let current = table.selected_cell();

                    if let Some((row, col)) = current {
                        let col_count = table.delegate().columns_count(cx);
                        let row_count = table.delegate().rows_count(cx);
                        if col_count <= 1 || row_count == 0 {
                            table.delegate_mut().stop_editing(true, cx);
                            return;
                        }

                        let next = if shift {
                            if col > 1 {
                                Some((row, col - 1))
                            } else if row > 0 {
                                Some((row - 1, col_count - 1))
                            } else {
                                None
                            }
                        } else {
                            if col + 1 < col_count {
                                Some((row, col + 1))
                            } else if row + 1 < row_count {
                                Some((row + 1, 1))
                            } else {
                                None
                            }
                        };

                        table.delegate_mut().stop_editing(true, cx);
                        // The old input's focus handle is still alive (held by
                        // the `_input` parameter in this subscription callback).
                        // When it eventually loses focus the deferred blur
                        // listener fires — ignore it.
                        table.delegate_mut().ignore_next_blur = true;

                        if let Some((next_row, next_col)) = next {
                            table.set_selected_cell(next_row, next_col, cx);

                            // Defer start_editing to a fresh update cycle so the
                            // old input entity is fully dropped and focus transfer
                            // completes cleanly before we create the new one.
                            cx.spawn_in(window, async move |this, cx| {
                                _ = this.update_in(cx, |table, window, cx| {
                                    table
                                        .delegate_mut()
                                        .start_editing(next_row, next_col, window, cx);
                                });
                                anyhow::Ok(())
                            })
                            .detach();
                        }
                    } else {
                        table.delegate_mut().stop_editing(true, cx);
                    }
                }
                _ => {}
            },
        )
        .detach();

        self.fk_select_state = None;
        self.date_picker_state = None;
        self.enum_select_state = None;

        self.cell_input = Some(input.clone());
        self.editing_cell = Some((actual_row, col));

        input.update(cx, |state, cx| {
            state.focus(window, cx);
        });

        let viewer_panel = self.viewer_panel.clone();
        cx.defer(move |cx| {
            _ = viewer_panel.update(cx, |_panel, cx| {
                cx.emit(TableViewerEvent::InlineEditStarted);
            });
        });

        if has_newlines {
            let viewer_panel = self.viewer_panel.clone();
            cx.defer(move |cx| {
                _ = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::MultiLineContentFlattened);
                });
            });
        }

        cx.notify();
    }

    fn clear_all_edit_states(&mut self) {
        self.editing_cell = None;
        self.cell_input = None;
        self.fk_select_state = None;
        self.date_picker_state = None;
        self.enum_select_state = None;
        self.bulk_edit_cells = None;
        self.editing_cell_has_newlines = false;
        self.ignore_next_blur = false;
    }

    pub fn stop_editing(&mut self, save: bool, cx: &mut Context<TableState<Self>>) {
        let _editing_position = self.editing_cell;
        let bulk_cells = self.bulk_edit_cells.take();

        if let (Some((row, col)), Some(input)) = (self.editing_cell, &self.cell_input) {
            if save {
                let new_value = input.read(cx).value().to_string();

                if let Some(cells) = bulk_cells {
                    self.apply_value_to_cells(&cells, new_value, cx);
                } else {
                    let data_col = col - 1;
                    let original_value = self
                        .rows
                        .get(row)
                        .and_then(|r| r.get(data_col))
                        .cloned()
                        .unwrap_or_default();

                    if new_value != original_value {
                        let total_rows = self.rows.len();
                        let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

                        if let Some(new_row_idx) = new_row_idx {
                            self.pending_changes.update_new_row_cell(
                                new_row_idx,
                                data_col,
                                new_value.clone(),
                            );
                            if let Some(row_data) = self.rows.get_mut(row) {
                                if let Some(cell) = row_data.get_mut(data_col) {
                                    *cell = new_value;
                                }
                            }
                        } else if self.auto_commit_mode {
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
                            cx.defer(move |cx| {
                                _ = viewer_panel.update(cx, |_panel, cx| {
                                    cx.emit(TableViewerEvent::SaveCell {
                                        table_name,
                                        connection_id,
                                        row,
                                        col: data_col,
                                        column_name,
                                        new_value,
                                        original_value,
                                        all_row_values,
                                        all_column_names,
                                        all_column_types,
                                    });
                                });
                            });
                        } else {
                            self.pending_changes.modified_cells.insert(
                                (row, data_col),
                                PendingCellChange {
                                    original_value,
                                    new_value: new_value.clone(),
                                },
                            );
                            if let Some(row_data) = self.rows.get_mut(row) {
                                if let Some(cell) = row_data.get_mut(data_col) {
                                    *cell = new_value;
                                }
                            }
                        }
                    }
                }
            }
        }

        self.clear_all_edit_states();
        cx.notify();
    }
}
