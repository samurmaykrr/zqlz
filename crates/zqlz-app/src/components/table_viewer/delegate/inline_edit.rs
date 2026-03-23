use super::*;

pub(crate) const AUTO_INCREMENT_PLACEHOLDER: &str = "(auto)";

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

        if let Some((editing_row, editing_col)) = self.editing_cell
            && editing_row == actual_row
            && editing_col == col
        {
            return;
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

        // Auto-increment columns on new rows show a placeholder and are not editable
        if self.is_auto_increment_column(data_col) && self.is_new_row(actual_row) {
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

        if self.is_binary_column(data_col) || Self::is_bytes_value(&value) {
            self.emit_edit_cell_event(actual_row, col, data_col, cx);
            return;
        }

        // Date/time columns use a date picker widget
        if self.is_date_time_column(data_col) {
            self.start_date_picker_editing(actual_row, col, data_col, &value, window, cx);
            return;
        }

        // Enum columns use a dropdown select
        if self.is_enum_column(data_col) {
            self.start_enum_editing(actual_row, col, data_col, &value, window, cx);
            return;
        }

        // Foreign key columns use a searchable dropdown
        if self.is_foreign_key_column(data_col) {
            self.start_fk_editing(actual_row, col, data_col, &value, window, cx);
            return;
        }

        self.start_text_editing(actual_row, col, value, window, cx);
    }

    fn start_date_picker_editing(
        &mut self,
        actual_row: usize,
        col: usize,
        data_col: usize,
        value: &Value,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        let mode = if self.is_date_column(data_col) {
            DatePickerMode::Date
        } else if self.is_time_column(data_col) {
            DatePickerMode::Time
        } else {
            DatePickerMode::DateTime
        };

        let nullable = self
            .column_meta
            .get(data_col)
            .map(|col_meta| col_meta.nullable)
            .unwrap_or(true);

        let display_str = value.display_for_table();
        let initial_value = if value.is_null() { "" } else { &display_str };

        let date_picker =
            cx.new(|cx| DatePickerState::new(mode, initial_value, nullable, window, cx));

        let viewer_panel = self.viewer_panel.clone();
        date_picker.update(cx, |state, _cx| {
            state.set_on_change(move |_new_value: &str, _window, _cx| {
                if let Err(error) = viewer_panel.update(_cx, |_panel, cx| {
                    cx.notify();
                }) {
                    tracing::error!("Failed to notify on date picker change: {:?}", error);
                }
            });
        });

        cx.subscribe_in(
            &date_picker,
            window,
            move |table, _picker, _event: &DismissEvent, _window, cx| {
                table.delegate_mut().stop_editing(true, cx);
            },
        )
        .detach();

        self.cell_input = None;
        self.fk_select_state = None;
        self.enum_select_state = None;
        self.date_picker_state = Some(date_picker);
        self.editing_cell = Some((actual_row, col));

        self.emit_inline_edit_started(cx);
        cx.notify();
    }

    fn start_enum_editing(
        &mut self,
        actual_row: usize,
        col: usize,
        data_col: usize,
        value: &Value,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        let enum_values = self.get_enum_values(data_col).cloned().unwrap_or_default();

        let nullable = self
            .column_meta
            .get(data_col)
            .map(|col_meta| col_meta.nullable)
            .unwrap_or(false);

        let mut items = enum_values.clone();
        if nullable && !items.iter().any(|v| v.eq_ignore_ascii_case("null")) {
            items.insert(0, "NULL".to_string());
        }

        let display_str = value.display_for_table();
        let is_null_or_empty = value.is_null() || display_str.is_empty();

        let selected_index = if is_null_or_empty {
            if nullable {
                items
                    .iter()
                    .position(|v| v == "NULL")
                    .map(|i| IndexPath::default().row(i))
            } else {
                None
            }
        } else {
            items
                .iter()
                .position(|v| v == &display_str)
                .map(|i| IndexPath::default().row(i))
        };

        let enum_select = cx.new(|cx| SelectState::new(items, selected_index, window, cx));

        cx.subscribe_in(
            &enum_select,
            window,
            move |table, _select, event: &SelectEvent<Vec<String>>, _window, cx| {
                let SelectEvent::Confirm(selected_value) = event;
                let new_value = selected_value
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| "NULL".to_string());
                table.delegate_mut().apply_edited_value(new_value, cx);
                table.delegate_mut().clear_all_edit_states();
                cx.notify();
            },
        )
        .detach();

        self.cell_input = None;
        self.fk_select_state = None;
        self.date_picker_state = None;
        self.enum_select_state = Some(enum_select);
        self.editing_cell = Some((actual_row, col));

        self.emit_inline_edit_started(cx);
        cx.notify();
    }

    fn start_fk_editing(
        &mut self,
        actual_row: usize,
        col: usize,
        data_col: usize,
        value: &Value,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        let fk_info = match self.get_fk_info(data_col).cloned() {
            Some(info) => info,
            None => return,
        };

        self.fk_request_id = self.fk_request_id.saturating_add(1);
        let request_id = self.fk_request_id;

        let cached_values = self
            .get_fk_values(&fk_info.referenced_table)
            .cloned()
            .unwrap_or_default();

        let display_str = value.display_for_table();
        let is_null_or_empty = value.is_null() || display_str.is_empty();

        let selected_index = if is_null_or_empty {
            None
        } else {
            cached_values
                .iter()
                .position(|item| item.value == display_str)
                .map(|i| IndexPath::default().row(i))
        };

        let fk_delegate = FkSelectDelegate {
            items: cached_values,
            table_name: fk_info.referenced_table.clone(),
            referenced_columns: fk_info.referenced_columns.clone(),
            connection_id: self.connection_id,
            viewer_panel: self.viewer_panel.clone(),
            request_id,
        };
        let fk_select =
            cx.new(|cx| SelectState::new(fk_delegate, selected_index, window, cx).searchable(true));

        cx.subscribe_in(
            &fk_select,
            window,
            move |table, _select, event: &SelectEvent<FkSelectDelegate>, _window, cx| {
                let SelectEvent::Confirm(selected_value) = event;
                let new_value = selected_value
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| "NULL".to_string());
                table.delegate_mut().apply_edited_value(new_value, cx);
                table.delegate_mut().clear_all_edit_states();
                cx.notify();
            },
        )
        .detach();

        self.cell_input = None;
        self.date_picker_state = None;
        self.enum_select_state = None;
        self.fk_select_state = Some(fk_select);
        self.editing_cell = Some((actual_row, col));

        // Trigger FK value loading if cache is empty
        if self.get_fk_values(&fk_info.referenced_table).is_none() {
            self.fk_loading = true;
            let viewer_panel = self.viewer_panel.clone();
            let connection_id = self.connection_id;
            let referenced_table = fk_info.referenced_table.clone();
            let referenced_columns = fk_info.referenced_columns.clone();
            cx.defer(move |cx| {
                if let Err(error) = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::LoadFkValues {
                        connection_id,
                        referenced_table,
                        referenced_columns,
                        query: None,
                        limit: 10,
                        request_id,
                    });
                }) {
                    tracing::error!("Failed to emit LoadFkValues: {:?}", error);
                }
            });
        }

        self.emit_inline_edit_started(cx);
        cx.notify();
    }

    fn start_text_editing(
        &mut self,
        actual_row: usize,
        col: usize,
        value: Value,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        let display_str = value.display_for_table();
        let has_newlines = display_str.contains('\n') || display_str.contains('\r');
        self.editing_cell_has_newlines = has_newlines;

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Edit value...")
                .emit_tab_event(true)
                .emit_arrow_event(true)
        });

        // Null values should show an empty input with placeholder, not "NULL" as text.
        let text = if value.is_null() || display_str.is_empty() {
            String::new()
        } else {
            display_str.replace('\n', " ").replace('\r', "")
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
                        } else if col + 1 < col_count {
                            Some((row, col + 1))
                        } else if row + 1 < row_count {
                            Some((row + 1, 1))
                        } else {
                            None
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
                                if let Err(e) = this.update_in(cx, |table, window, cx| {
                                    table
                                        .delegate_mut()
                                        .start_editing(next_row, next_col, window, cx);
                                }) {
                                    tracing::error!(
                                        "Failed to start editing next cell ({}, {}): {:?}",
                                        next_row,
                                        next_col,
                                        e
                                    );
                                }
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

        self.emit_inline_edit_started(cx);

        if has_newlines {
            let viewer_panel = self.viewer_panel.clone();
            cx.defer(move |cx| {
                if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::MultiLineContentFlattened);
                }) {
                    tracing::error!("Failed to emit MultiLineContentFlattened: {:?}", e);
                }
            });
        }

        cx.notify();
    }

    fn emit_inline_edit_started(&self, cx: &mut Context<TableState<Self>>) {
        let viewer_panel = self.viewer_panel.clone();
        cx.defer(move |cx| {
            if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                cx.emit(TableViewerEvent::InlineEditStarted);
            }) {
                tracing::error!("Failed to emit InlineEditStarted: {:?}", e);
            }
        });
    }

    pub(crate) fn emit_validation_failed(
        &self,
        message: String,
        cx: &mut Context<TableState<Self>>,
    ) {
        let viewer_panel = self.viewer_panel.clone();
        cx.defer(move |cx| {
            if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                cx.emit(TableViewerEvent::ValidationFailed { message });
            }) {
                tracing::error!("Failed to emit ValidationFailed: {:?}", e);
            }
        });
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

    pub(crate) fn prepare_cell_value_update(
        &self,
        row: usize,
        data_col: usize,
        new_value_str: &str,
    ) -> Result<Option<(Value, Value)>, String> {
        let original_value = self
            .rows
            .get(row)
            .and_then(|current_row| current_row.get(data_col))
            .cloned()
            .unwrap_or_default();

        let is_null_no_op = new_value_str.is_empty() && original_value.is_null();
        let data_type = self
            .column_meta
            .get(data_col)
            .map(|column| column.data_type.as_str())
            .unwrap_or("text");
        let new_value = Value::parse_from_string(new_value_str, data_type);

        if is_null_no_op || new_value == original_value {
            return Ok(None);
        }

        self.validate_cell_value(data_col, new_value_str)?;

        Ok(Some((new_value, original_value)))
    }

    pub fn stop_editing(&mut self, save: bool, cx: &mut Context<TableState<Self>>) {
        let bulk_cells = self.bulk_edit_cells.take();

        // Determine the new value from whichever edit widget is active
        let new_value_str = if save {
            if let Some(input) = &self.cell_input {
                Some(input.read(cx).value().to_string())
            } else {
                self.date_picker_state
                    .as_ref()
                    .map(|date_picker| date_picker.read(cx).value().to_string())
            }
        } else {
            None
        };

        if let (Some((row, col)), Some(new_value_str)) = (self.editing_cell, new_value_str) {
            if let Some(cells) = bulk_cells {
                self.apply_value_to_cells(&cells, &new_value_str, cx);
            } else {
                let data_col = col - 1;
                match self.prepare_cell_value_update(row, data_col, &new_value_str) {
                    Ok(Some((new_value, original_value))) => {
                        self.commit_cell_value(row, col, data_col, new_value, original_value, cx);
                    }
                    Ok(None) => {}
                    Err(message) => {
                        tracing::warn!("Cell validation failed: {}", message);
                        self.emit_validation_failed(message, cx);
                    }
                }
            }
        }

        self.clear_all_edit_states();
        cx.notify();
    }

    /// Applies a new value to the editing cell, handling new-row, auto-commit, and
    /// pending-change modes. Used by both stop_editing (for text/date inputs) and
    /// the enum/FK confirm event subscriptions.
    fn apply_edited_value(&mut self, new_value_str: String, cx: &mut Context<TableState<Self>>) {
        let Some((row, col)) = self.editing_cell else {
            return;
        };
        let data_col = col - 1;
        match self.prepare_cell_value_update(row, data_col, &new_value_str) {
            Ok(Some((new_value, original_value))) => {
                self.commit_cell_value(row, col, data_col, new_value, original_value, cx);
            }
            Ok(None) => {}
            Err(message) => {
                tracing::warn!("Cell validation failed: {}", message);
                self.emit_validation_failed(message, cx);
            }
        }
    }

    pub(crate) fn apply_cell_value_change(
        &mut self,
        row: usize,
        data_col: usize,
        new_value: Value,
        original_value: Value,
        record_undo: bool,
        cx: &mut Context<TableState<Self>>,
    ) {
        let total_rows = self.rows.len();
        let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

        if record_undo {
            self.push_undo(UndoEntry {
                edits: vec![UndoCellEdit {
                    row,
                    data_col,
                    old_value: original_value.clone(),
                    new_value: new_value.clone(),
                }],
            });
        }

        if let Some(new_row_idx) = new_row_idx {
            self.pending_changes
                .update_new_row_cell(new_row_idx, data_col, new_value.clone());
            self.apply_value_locally(row, data_col, new_value);
        } else if self.auto_commit_mode {
            self.save_existing_cell_or_queue(row, data_col, new_value, &original_value, cx);
        } else {
            self.store_pending_cell_change(row, data_col, new_value, &original_value);
        }
    }

    /// Persists a cell value change through the appropriate channel: new-row update,
    /// auto-commit (immediate SaveCell event), or pending-changes accumulation.
    pub(crate) fn commit_cell_value(
        &mut self,
        row: usize,
        _col: usize,
        data_col: usize,
        new_value: Value,
        original_value: Value,
        cx: &mut Context<TableState<Self>>,
    ) {
        self.apply_cell_value_change(row, data_col, new_value, original_value, true, cx);
    }

    pub fn is_auto_increment_column(&self, data_col: usize) -> bool {
        self.column_meta
            .get(data_col)
            .map(|col_meta| col_meta.auto_increment)
            .unwrap_or(false)
    }

    fn is_new_row(&self, row: usize) -> bool {
        let original_row_count = self.rows.len() - self.pending_changes.new_row_count();
        row >= original_row_count
    }
}
