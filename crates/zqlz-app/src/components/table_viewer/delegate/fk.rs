use super::*;

impl TableViewerDelegate {
    pub fn set_foreign_keys(&mut self, foreign_keys: Vec<ForeignKeyInfo>) {
        self.fk_by_column.clear();

        for fk in foreign_keys {
            for column_name in &fk.columns {
                if let Some(col_idx) = self.column_meta.iter().position(|m| &m.name == column_name)
                {
                    self.fk_by_column.insert(col_idx, fk.clone());
                    tracing::debug!(
                        "FK mapping: column {} ({}) -> {} ({})",
                        col_idx,
                        column_name,
                        fk.referenced_table,
                        fk.referenced_columns.join(", ")
                    );
                }
            }
        }

        tracing::info!(
            "FK mapping complete: {} FK columns detected",
            self.fk_by_column.len()
        );
    }

    pub fn is_foreign_key_column(&self, data_col_ix: usize) -> bool {
        self.fk_by_column.contains_key(&data_col_ix)
    }

    pub fn get_fk_info(&self, data_col_ix: usize) -> Option<&ForeignKeyInfo> {
        self.fk_by_column.get(&data_col_ix)
    }

    pub fn set_fk_values(
        &mut self,
        table_name: String,
        values: Vec<FkSelectItem>,
        window: &mut Window,
        cx: &mut App,
    ) {
        self.fk_values_cache
            .insert(table_name.clone(), values.clone());
        self.fk_loading = false;

        if let Some(select_state) = &self.fk_select_state {
            let current_value = select_state.read(cx).selected_value().cloned();
            let searchable_items = SearchableVec::new(values.clone());
            select_state.update(cx, |state, cx| {
                state.set_items(searchable_items, window, cx);
                if let Some(ref current) = current_value {
                    let selected_index = values
                        .iter()
                        .position(|item| &item.value == current)
                        .map(|i| zqlz_ui::widgets::IndexPath::default().row(i));
                    if let Some(index) = selected_index {
                        state.set_selected_index(Some(index), window, cx);
                    }
                }
            });
            tracing::info!(
                "Updated FK dropdown with {} values for table {}",
                values.len(),
                table_name
            );
        }
    }

    pub fn get_fk_values(&self, table_name: &str) -> Option<&Vec<FkSelectItem>> {
        self.fk_values_cache.get(table_name)
    }
}
