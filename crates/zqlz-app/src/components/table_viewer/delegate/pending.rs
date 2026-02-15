use super::*;

impl TableViewerDelegate {
    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.is_empty()
    }

    pub fn pending_change_count(&self) -> usize {
        self.pending_changes.change_count()
    }

    pub fn discard_pending_changes(&mut self) {
        for ((row, col), change) in &self.pending_changes.modified_cells {
            if let Some(row_data) = self.rows.get_mut(*row) {
                if let Some(cell) = row_data.get_mut(*col) {
                    *cell = change.original_value.clone();
                }
            }
        }

        let original_row_count = self.rows.len() - self.pending_changes.new_rows.len();
        self.rows.truncate(original_row_count);

        self.pending_changes.clear();
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

    pub fn unmark_row_for_deletion(&mut self, row_index: usize) {
        self.pending_changes.deleted_rows.remove(&row_index);
    }

    pub fn is_new_row_ready_to_commit(&self, row_idx: usize) -> Option<usize> {
        let total_rows = self.rows.len();
        if let Some(new_row_idx) = self.pending_changes.get_new_row_index(row_idx, total_rows) {
            if let Some(new_row_data) = self.pending_changes.new_rows.get(new_row_idx) {
                for (col_idx, col_meta) in self.column_meta.iter().enumerate() {
                    if !col_meta.nullable && col_meta.default_value.is_none() {
                        let value = new_row_data.get(col_idx).map(|v| v.as_str()).unwrap_or("");
                        if value.trim().is_empty() {
                            return None;
                        }
                    }
                }
                return Some(new_row_idx);
            }
        }
        None
    }

    pub fn add_new_row(&mut self) {
        let column_count = self.column_meta.len();
        let new_row: Vec<String> = vec![String::new(); column_count];

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

    pub fn get_pending_changes_for_commit(
        &self,
    ) -> (
        HashMap<(usize, usize), PendingCellChange>,
        HashSet<usize>,
        Vec<Vec<String>>,
        Vec<ColumnMeta>,
        String,
        Uuid,
        Vec<Vec<String>>,
    ) {
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
    }
}
