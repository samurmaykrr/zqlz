use super::*;
use std::collections::HashSet;

type PendingCommitSnapshot = (
    HashMap<(usize, usize), PendingCellChange>,
    HashSet<usize>,
    Vec<Vec<Value>>,
    Vec<ColumnMeta>,
    String,
    Uuid,
    Vec<Vec<Value>>,
);

impl TableViewerDelegate {
    fn find_matching_row_index(
        row_strings: &[Vec<Value>],
        target_row: &[Value],
        consumed_indices: &mut HashSet<usize>,
    ) -> Option<usize> {
        row_strings
            .iter()
            .enumerate()
            .find(|(row_index, row)| {
                !consumed_indices.contains(row_index) && row.as_slice() == target_row
            })
            .map(|(row_index, _)| {
                consumed_indices.insert(row_index);
                row_index
            })
    }

    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.is_empty()
    }

    pub fn pending_change_count(&self) -> usize {
        self.pending_changes.change_count()
    }

    pub fn discard_pending_changes(&mut self) {
        for ((row, col), change) in &self.pending_changes.modified_cells {
            if let Some(row_data) = self.rows.get_mut(*row)
                && let Some(cell) = row_data.get_mut(*col)
            {
                let data_type = self
                    .column_meta
                    .get(*col)
                    .map(|c| c.data_type.as_str())
                    .unwrap_or("text");
                *cell = change.original_value.to_value(data_type);
            }
        }

        let Some(original_row_count) = self
            .rows
            .len()
            .checked_sub(self.pending_changes.new_rows.len())
        else {
            tracing::error!(
                "Cannot discard pending changes because pending new rows exceed total rows: total_rows={}, new_rows={}",
                self.rows.len(),
                self.pending_changes.new_rows.len()
            );
            return;
        };

        self.rows.truncate(original_row_count);

        self.pending_changes.clear();
        self.undo_stack.clear();
        self.redo_stack.clear();
        tracing::info!("Discarded all pending changes");
    }

    pub fn mark_row_for_deletion(&mut self, row_index: usize) {
        self.pending_changes.deleted_rows.insert(row_index);
        tracing::info!(
            "Marked row {} for deletion, pending_count={}",
            row_index,
            self.pending_changes.change_count()
        );
    }

    #[allow(dead_code)]
    pub fn unmark_row_for_deletion(&mut self, row_index: usize) {
        self.pending_changes.deleted_rows.remove(&row_index);
    }

    #[allow(dead_code)]
    pub fn is_new_row_ready_to_commit(&self, row_idx: usize) -> Option<usize> {
        let total_rows = self.rows.len();
        if let Some(new_row_idx) = self.pending_changes.get_new_row_index(row_idx, total_rows)
            && let Some(new_row_data) = self.pending_changes.new_rows.get(new_row_idx)
        {
            for (col_idx, col_meta) in self.column_meta.iter().enumerate() {
                if !col_meta.nullable && col_meta.default_value.is_none() {
                    let value = new_row_data.get(col_idx).cloned().unwrap_or_default();
                    if value.is_null() || value.display_for_table().trim().is_empty() {
                        return None;
                    }
                }
            }
            return Some(new_row_idx);
        }
        None
    }

    pub fn add_new_row(&mut self) {
        let new_row: Vec<Value> = self
            .column_meta
            .iter()
            .map(|col| {
                if col.auto_increment {
                    Value::String(super::inline_edit::AUTO_INCREMENT_PLACEHOLDER.to_string())
                } else if let Some(default) = &col.default_value {
                    if default.trim().is_empty() {
                        Value::String(String::new())
                    } else {
                        Value::String(default.clone())
                    }
                } else {
                    Value::Null
                }
            })
            .collect();

        self.pending_changes.new_rows.push(new_row.clone());
        self.rows.push(new_row);

        if self.is_filtering {
            self.filtered_row_indices.push(self.rows.len() - 1);
        }

        tracing::info!(
            "Added new row, pending_count={}",
            self.pending_changes.change_count()
        );
    }

    pub fn get_pending_changes_for_commit(&self) -> PendingCommitSnapshot {
        (
            self.pending_changes.modified_cells.clone(),
            self.pending_changes.deleted_rows.clone(),
            self.pending_changes.new_rows.clone(),
            self.column_meta.clone(),
            self.table_name.clone(),
            self.connection_id,
            self.rows.clone(),
        )
    }

    pub fn clear_pending_changes(&mut self) {
        self.pending_changes.clear();
        self.undo_stack.clear();
        self.redo_stack.clear();
    }

    pub(crate) fn build_save_cell_request(
        &self,
        row: usize,
        data_col: usize,
        new_value: &Value,
        original_value: &Value,
    ) -> Option<SaveCellRequest> {
        let column_name = self.column_meta.get(data_col)?.name.clone();

        Some(SaveCellRequest {
            table_name: self.table_name.clone(),
            connection_id: self.connection_id,
            row,
            data_col,
            column_name,
            new_value: CellValue::from_value(new_value),
            original_value: CellValue::from_value(original_value),
            all_row_values: self.rows.get(row).cloned().unwrap_or_default(),
            all_column_names: self
                .column_meta
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            all_column_types: self
                .column_meta
                .iter()
                .map(|column| column.data_type.clone())
                .collect(),
        })
    }

    pub(crate) fn apply_value_locally(&mut self, row: usize, data_col: usize, new_value: Value) {
        if let Some(row_data) = self.rows.get_mut(row)
            && let Some(cell) = row_data.get_mut(data_col)
        {
            *cell = new_value;
        }
    }

    pub(crate) fn store_pending_cell_change(
        &mut self,
        row: usize,
        data_col: usize,
        new_value: Value,
        original_value: &Value,
    ) {
        self.apply_value_locally(row, data_col, new_value.clone());
        self.pending_changes.modified_cells.insert(
            (row, data_col),
            PendingCellChange {
                original_value: CellValue::from_value(original_value),
                new_value: CellValue::from_value(&new_value),
            },
        );
    }

    pub(crate) fn save_existing_cell_or_queue(
        &mut self,
        row: usize,
        data_col: usize,
        new_value: Value,
        original_value: &Value,
        cx: &mut Context<TableState<Self>>,
    ) {
        let Some(request) = self.build_save_cell_request(row, data_col, &new_value, original_value)
        else {
            tracing::error!(
                "Cannot save cell because column metadata is missing for row={}, col={}",
                row,
                data_col
            );
            self.store_pending_cell_change(row, data_col, new_value, original_value);
            return;
        };

        self.apply_value_locally(row, data_col, new_value);

        let viewer_panel = self.viewer_panel.clone();
        cx.defer(move |cx| {
            if let Err(error) = viewer_panel.update(cx, |_panel, cx| {
                cx.emit(TableViewerEvent::SaveCell {
                    table_name: request.table_name.clone(),
                    connection_id: request.connection_id,
                    row: request.row,
                    col: request.data_col,
                    column_name: request.column_name.clone(),
                    new_value: request.new_value.clone(),
                    original_value: request.original_value.clone(),
                    all_row_values: request.all_row_values.clone(),
                    all_column_names: request.all_column_names.clone(),
                    all_column_types: request.all_column_types.clone(),
                });
            }) {
                tracing::error!("Failed to emit SaveCell: {:?}", error);
                let request = request.clone();
                if let Err(restore_error) = viewer_panel.update(cx, |panel, cx| {
                    if let Some(table_state) = &panel.table_state {
                        table_state.update(cx, |table, _cx| {
                            table.delegate_mut().pending_changes.modified_cells.insert(
                                (request.row, request.data_col),
                                PendingCellChange {
                                    original_value: request.original_value.clone(),
                                    new_value: request.new_value.clone(),
                                },
                            );
                        });
                    }
                }) {
                    tracing::error!(
                        "Failed to queue pending change after SaveCell emit failure: {:?}",
                        restore_error
                    );
                }
            }
        });
    }

    pub fn restore_failed_commit_state(
        &mut self,
        failed_modified_cells: Vec<(Vec<Value>, usize, PendingCellChange)>,
        failed_deleted_rows: Vec<Vec<Value>>,
        failed_new_rows: Vec<Vec<Value>>,
    ) {
        self.pending_changes.clear();
        self.undo_stack.clear();
        self.redo_stack.clear();

        let current_row_strings = self.rows.clone();

        let mut consumed_modified_rows = HashSet::new();
        for (original_row_values, column_index, change) in failed_modified_cells {
            let Some(row_index) = Self::find_matching_row_index(
                &current_row_strings,
                &original_row_values,
                &mut consumed_modified_rows,
            ) else {
                tracing::warn!(
                    "Skipping failed modified cell rehydration because the original row is no longer present"
                );
                continue;
            };

            let data_type = self
                .column_meta
                .get(column_index)
                .map(|column| column.data_type.as_str())
                .unwrap_or("text");
            if let Some(row_data) = self.rows.get_mut(row_index)
                && let Some(cell) = row_data.get_mut(column_index)
            {
                *cell = change.new_value.to_value(data_type);
            }

            self.pending_changes
                .modified_cells
                .insert((row_index, column_index), change);
        }

        let mut consumed_deleted_rows = HashSet::new();
        for original_row_values in failed_deleted_rows {
            let Some(row_index) = Self::find_matching_row_index(
                &current_row_strings,
                &original_row_values,
                &mut consumed_deleted_rows,
            ) else {
                tracing::warn!(
                    "Skipping failed deleted-row rehydration because the original row is no longer present"
                );
                continue;
            };

            self.pending_changes.deleted_rows.insert(row_index);
        }

        for failed_new_row in failed_new_rows {
            self.pending_changes.new_rows.push(failed_new_row.clone());
            self.rows.push(failed_new_row);

            if self.is_filtering {
                self.filtered_row_indices.push(self.rows.len() - 1);
            }
        }
    }

    /// Push an undo entry and clear the redo stack.
    /// Only records entries in non-auto-commit (batch) mode.
    pub(crate) fn push_undo(&mut self, entry: UndoEntry) {
        if self.auto_commit_mode {
            return;
        }
        if entry.edits.is_empty() {
            return;
        }
        self.undo_stack.push(entry);
        self.redo_stack.clear();
    }

    pub fn can_undo(&self) -> bool {
        !self.auto_commit_mode && !self.undo_stack.is_empty()
    }

    pub fn can_redo(&self) -> bool {
        !self.auto_commit_mode && !self.redo_stack.is_empty()
    }

    /// Undo the last edit entry: restore old values in rows and pending_changes.
    pub fn undo(&mut self) {
        let Some(entry) = self.undo_stack.pop() else {
            return;
        };

        for edit in &entry.edits {
            if let Some(row_data) = self.rows.get_mut(edit.row)
                && let Some(cell) = row_data.get_mut(edit.data_col)
            {
                *cell = edit.old_value.clone();
            }
            // Sync pending_changes: if the old_value matches the original recorded in
            // pending_changes, remove the entry entirely (cell is back to its DB state).
            // Otherwise update the pending new_value.
            if let Some(pending) = self
                .pending_changes
                .modified_cells
                .get(&(edit.row, edit.data_col))
            {
                if pending.original_value == CellValue::from_value(&edit.old_value) {
                    self.pending_changes
                        .modified_cells
                        .remove(&(edit.row, edit.data_col));
                } else {
                    self.pending_changes.modified_cells.insert(
                        (edit.row, edit.data_col),
                        PendingCellChange {
                            original_value: pending.original_value.clone(),
                            new_value: CellValue::from_value(&edit.old_value),
                        },
                    );
                }
            }
        }

        self.redo_stack.push(entry);
    }

    /// Redo the last undone entry: re-apply new values.
    pub fn redo(&mut self) {
        let Some(entry) = self.redo_stack.pop() else {
            return;
        };

        for edit in &entry.edits {
            if let Some(row_data) = self.rows.get_mut(edit.row)
                && let Some(cell) = row_data.get_mut(edit.data_col)
            {
                *cell = edit.new_value.clone();
            }
            // Re-insert into pending_changes
            let original_value = self
                .pending_changes
                .modified_cells
                .get(&(edit.row, edit.data_col))
                .map(|p| p.original_value.clone())
                .unwrap_or_else(|| CellValue::from_value(&edit.old_value));
            self.pending_changes.modified_cells.insert(
                (edit.row, edit.data_col),
                PendingCellChange {
                    original_value,
                    new_value: CellValue::from_value(&edit.new_value),
                },
            );
        }

        self.undo_stack.push(entry);
    }
}
