use super::*;

impl TableViewerDelegate {
    /// Create a new table viewer delegate from query results
    pub fn new(
        result: &QueryResult,
        table_name: String,
        connection_id: Uuid,
        viewer_panel: WeakEntity<TableViewerPanel>,
    ) -> Self {
        let rows: Vec<Vec<Value>> = result.rows.iter().map(|row| row.values.clone()).collect();
        // Create row number column as first column (fixed left)
        let row_num_width = Self::row_number_column_width(result.rows.len());
        let mut columns: Vec<Column> = vec![
            Column::new("row-num", "#")
                .width(row_num_width)
                .fixed(ColumnFixed::Left),
        ];

        columns.extend(result.columns.iter().enumerate().map(|(idx, col_meta)| {
            let width = Self::calculate_initial_column_width(idx, col_meta, &rows);
            Column::new(format!("col-{}", idx), col_meta.name.clone())
                .width(width)
                .resizable(true)
                .sortable()
        }));

        let row_original_order: Vec<u64> = (0..rows.len() as u64).collect();
        let next_row_order_token = rows.len() as u64;

        Self {
            columns,
            column_meta: result.columns.clone(),
            rows,
            row_original_order,
            next_row_order_token,
            size: Size::Small,
            table_name,
            connection_id,
            driver_category: DriverCategory::Relational,
            data_editing_features: Self::data_editing_features_for_driver_category(
                DriverCategory::Relational,
            ),
            viewer_panel,
            editing_cell: None,
            cell_input: None,
            date_picker_state: None,
            enum_select_state: None,
            bulk_edit_cells: None,
            editing_cell_has_newlines: false,
            ignore_next_blur: false,
            context_menu_selected_rows: Vec::new(),
            search_filter: None,
            filtered_row_indices: Vec::new(),
            is_filtering: false,
            pending_changes: PendingChanges::default(),
            disable_inline_edit: false,
            auto_commit_mode: true,
            row_offset: 0,
            infinite_scroll_enabled: false,
            has_more_data: false,
            is_loading_more: false,
            fk_by_column: HashMap::new(),
            fk_values_cache: HashMap::new(),
            fk_select_state: None,
            fk_loading: false,
            fk_request_id: 0,
            last_filter_conditions: Vec::new(),
            last_filter_search_text: String::new(),
            primary_key_columns: Vec::new(),
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            cell_preview_cache: HashMap::new(),
            visible_rows_range: None,
            visible_columns_range: None,
        }
    }

    pub fn set_disable_inline_edit(&mut self, disable: bool) {
        self.disable_inline_edit = disable;
    }

    pub fn can_edit_cells(&self) -> bool {
        self.data_editing_features.edit_cells.available
    }

    pub fn can_delete_rows(&self) -> bool {
        self.data_editing_features.delete_rows.available
    }

    pub fn can_insert_rows(&self) -> bool {
        self.data_editing_features.insert_rows.available
    }

    pub fn set_driver_category(&mut self, category: DriverCategory) {
        self.driver_category = category;
        self.data_editing_features = Self::data_editing_features_for_driver_category(category);
        self.apply_row_identity_editing_policy();
    }

    #[allow(dead_code)]
    pub fn set_data_editing_features(&mut self, features: DataEditingFeatureSet) {
        self.data_editing_features = features;
        self.apply_row_identity_editing_policy();
    }

    #[allow(dead_code)]
    pub fn is_inline_edit_disabled(&self) -> bool {
        self.disable_inline_edit
    }

    pub fn set_auto_commit_mode(&mut self, enabled: bool) {
        self.auto_commit_mode = enabled;
    }

    fn data_editing_features_for_driver_category(
        category: DriverCategory,
    ) -> DataEditingFeatureSet {
        let row_feature = match category {
            DriverCategory::Relational => FeatureAvailability::available(),
            DriverCategory::Document => FeatureAvailability::available(),
            DriverCategory::KeyValue => {
                FeatureAvailability::unavailable("Use the Redis key editor for key mutations")
            }
            _ => {
                FeatureAvailability::unavailable("Row editing requires a data-editable connection")
            }
        };
        let insert_rows = match category {
            DriverCategory::Relational => row_feature.clone(),
            DriverCategory::Document => {
                FeatureAvailability::unavailable("Document inserts use the document editor")
            }
            _ => row_feature.clone(),
        };

        DataEditingFeatureSet {
            browse_rows: row_feature.clone(),
            edit_cells: row_feature.clone(),
            insert_rows,
            delete_rows: row_feature,
        }
    }

    fn data_editing_features_for_row_identity(
        category: DriverCategory,
        has_primary_key: bool,
    ) -> DataEditingFeatureSet {
        let mut features = Self::data_editing_features_for_driver_category(category);

        if matches!(category, DriverCategory::Relational) && !has_primary_key {
            let unavailable =
                FeatureAvailability::unavailable("Editing requires a stable primary key");
            features.edit_cells = unavailable.clone();
            features.delete_rows = unavailable;
        }

        features
    }

    pub fn set_row_offset(&mut self, offset: usize) {
        self.row_offset = offset;
    }

    pub fn set_infinite_scroll_enabled(&mut self, enabled: bool) {
        self.infinite_scroll_enabled = enabled;
        if enabled {
            self.has_more_data = true;
            self.is_loading_more = false;
        }
    }

    pub fn set_primary_key_columns(&mut self, columns: Vec<String>) {
        self.primary_key_columns = columns;
        self.apply_row_identity_editing_policy();
    }

    fn apply_row_identity_editing_policy(&mut self) {
        self.data_editing_features = Self::data_editing_features_for_row_identity(
            self.driver_category,
            !self.primary_key_columns.is_empty(),
        );
    }

    pub fn append_rows(&mut self, new_rows: Vec<Vec<Value>>, has_more: bool) {
        self.clear_cell_preview_cache();
        let appended_count = new_rows.len() as u64;
        self.rows.extend(new_rows);
        self.row_original_order
            .extend((0..appended_count).map(|offset| self.next_row_order_token + offset));
        self.next_row_order_token = self.next_row_order_token.saturating_add(appended_count);
        self.has_more_data = has_more;
        self.is_loading_more = false;
        self.resize_row_number_column();
    }

    pub fn replace_rows(&mut self, rows: Vec<Vec<Value>>, has_more: bool) {
        self.clear_cell_preview_cache();
        let row_count = rows.len() as u64;
        self.rows = rows;
        self.row_original_order = (0..row_count).collect();
        self.next_row_order_token = row_count;
        self.has_more_data = has_more;
        self.is_loading_more = false;
        self.resize_row_number_column();
    }

    pub fn set_has_more_data(&mut self, has_more: bool) {
        self.has_more_data = has_more;
    }

    fn resize_row_number_column(&mut self) {
        let max_row = self.row_offset + self.rows.len();
        let width = Self::row_number_column_width(max_row);
        if !self.columns.is_empty() {
            self.columns[0] = self.columns[0].clone().width(width);
        }
    }

    pub(in crate::components::table_viewer) fn emit_edit_cell_event(
        &self,
        row: usize,
        _col: usize,
        data_col: usize,
        cx: &mut Context<TableState<Self>>,
    ) {
        let column_meta = self.column_meta.get(data_col);
        let cell_value = self.rows.get(row).and_then(|r| r.get(data_col));
        let current_value = cell_value.cloned().unwrap_or(Value::Null);
        let all_row_values = self.rows.get(row).cloned().unwrap_or_default();
        let all_column_names: Vec<String> =
            self.column_meta.iter().map(|c| c.name.clone()).collect();
        let all_column_types: Vec<String> = self
            .column_meta
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        // Extract raw bytes directly from Value::Bytes
        let raw_bytes = cell_value.and_then(|v| match v {
            Value::Bytes(bytes) => Some(bytes.clone()),
            _ => None,
        });

        let viewer_panel = self.viewer_panel.clone();
        let table_name = self.table_name.clone();
        let connection_id = self.connection_id;

        if let Some(col_meta) = column_meta {
            let col_meta = col_meta.clone();
            cx.defer(move |cx| {
                if let Err(e) = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::EditCell {
                        table_name,
                        connection_id,
                        row,
                        col: data_col,
                        column_meta: col_meta.clone(),
                        column_name: col_meta.name.clone(),
                        column_type: col_meta.data_type.clone(),
                        current_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                        raw_bytes,
                    });
                }) {
                    tracing::error!("Failed to emit EditCell event: {}", e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TableViewerDelegate;
    use zqlz_core::DriverCategory;

    #[test]
    fn row_identity_policy_disables_existing_row_mutations_without_primary_key() {
        let features = TableViewerDelegate::data_editing_features_for_row_identity(
            DriverCategory::Relational,
            false,
        );

        assert!(features.browse_rows.available);
        assert!(!features.edit_cells.available);
        assert!(features.insert_rows.available);
        assert!(!features.delete_rows.available);
        assert_eq!(
            features.edit_cells.reason.as_deref(),
            Some("Editing requires a stable primary key")
        );
    }

    #[test]
    fn row_identity_policy_allows_existing_row_mutations_with_primary_key() {
        let features = TableViewerDelegate::data_editing_features_for_row_identity(
            DriverCategory::Relational,
            true,
        );

        assert!(features.edit_cells.available);
        assert!(features.delete_rows.available);
    }

    #[test]
    fn document_driver_allows_cell_and_delete_batch_edits_without_relational_primary_key() {
        let features = TableViewerDelegate::data_editing_features_for_driver_category(
            DriverCategory::Document,
        );

        assert!(features.browse_rows.available);
        assert!(features.edit_cells.available);
        assert!(!features.insert_rows.available);
        assert!(features.delete_rows.available);
        assert_eq!(
            features.insert_rows.reason.as_deref(),
            Some("Document inserts use the document editor")
        );
    }

    #[test]
    fn key_value_driver_keeps_table_grid_mutations_disabled() {
        let features = TableViewerDelegate::data_editing_features_for_driver_category(
            DriverCategory::KeyValue,
        );

        assert!(!features.edit_cells.available);
        assert!(!features.insert_rows.available);
        assert!(!features.delete_rows.available);
        assert_eq!(
            features.edit_cells.reason.as_deref(),
            Some("Use the Redis key editor for key mutations")
        );
    }
}
