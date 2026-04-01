use super::*;

#[allow(dead_code)]
impl TableViewerDelegate {
    pub(super) fn fk_cache_key(foreign_key: &ForeignKeyInfo) -> String {
        match foreign_key
            .referenced_schema
            .as_deref()
            .map(str::trim)
            .filter(|schema| !schema.is_empty())
        {
            Some(schema) => format!("{}.{}", schema, foreign_key.referenced_table),
            None => foreign_key.referenced_table.clone(),
        }
    }

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
        cache_key: String,
        values: Vec<FkSelectItem>,
        query: Option<String>,
        request_id: u64,
        window: &mut Window,
        cx: &mut App,
    ) {
        if request_id != self.fk_request_id {
            tracing::debug!(
                "Ignoring stale FK values for table {} (request_id={}, current={})",
                cache_key,
                request_id,
                self.fk_request_id
            );
            return;
        }

        if query
            .as_ref()
            .map(|search| search.trim().is_empty())
            .unwrap_or(true)
        {
            self.fk_values_cache
                .insert(cache_key.clone(), values.clone());
        }
        self.fk_loading = false;

        if let Some(select_state) = &self.fk_select_state {
            let current_value = select_state.read(cx).selected_value().cloned();
            let expected_query = query.clone().unwrap_or_default();
            select_state.update(cx, |state, cx| {
                let active_query = state.search_query(cx);
                if active_query.trim() != expected_query.trim() {
                    return;
                }

                state.update_delegate(
                    |delegate| {
                        delegate.items = values.clone();
                    },
                    window,
                    cx,
                );
                if let Some(ref current) = current_value {
                    let selected_index = values
                        .iter()
                        .position(|item| &item.value == current)
                        .map(|i| zqlz_ui::widgets::IndexPath::default().row(i));
                    state.set_selected_index(selected_index, window, cx);
                }
            });
            tracing::info!(
                "Updated FK dropdown with {} values for table {} (query={:?})",
                values.len(),
                cache_key,
                query
            );
        }
    }

    pub fn get_fk_values(&self, table_name: &str) -> Option<&Vec<FkSelectItem>> {
        self.fk_values_cache.get(table_name)
    }
}
