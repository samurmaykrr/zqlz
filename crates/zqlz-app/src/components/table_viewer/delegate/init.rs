use super::*;

impl TableViewerDelegate {
    /// Create a new table viewer delegate from query results
    pub fn new(
        result: &QueryResult,
        table_name: String,
        connection_id: Uuid,
        viewer_panel: WeakEntity<TableViewerPanel>,
    ) -> Self {
        // Create row number column as first column (fixed left)
        let row_num_width = Self::row_number_column_width(result.rows.len());
        let mut columns: Vec<Column> = vec![
            Column::new("row-num", "#")
                .width(row_num_width)
                .fixed(ColumnFixed::Left),
        ];

        columns.extend(result.columns.iter().enumerate().map(|(idx, col_meta)| {
            Column::new(format!("col-{}", idx), col_meta.name.clone())
                .width(150.0)
                .resizable(true)
                .sortable()
        }));

        let rows: Vec<Vec<Value>> = result.rows.iter().map(|row| row.values.clone()).collect();

        Self {
            columns,
            column_meta: result.columns.clone(),
            rows,
            size: Size::Small,
            table_name,
            connection_id,
            driver_category: DriverCategory::Relational,
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
        }
    }

    pub fn set_disable_inline_edit(&mut self, disable: bool) {
        self.disable_inline_edit = disable;
    }

    pub fn set_driver_category(&mut self, category: DriverCategory) {
        self.driver_category = category;
    }

    #[allow(dead_code)]
    pub fn is_inline_edit_disabled(&self) -> bool {
        self.disable_inline_edit
    }

    pub fn set_auto_commit_mode(&mut self, enabled: bool) {
        self.auto_commit_mode = enabled;
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
    }

    pub fn append_rows(&mut self, new_rows: Vec<Vec<Value>>, has_more: bool) {
        self.rows.extend(new_rows);
        self.has_more_data = has_more;
        self.is_loading_more = false;
        self.resize_row_number_column();
    }

    pub fn replace_rows(&mut self, rows: Vec<Vec<Value>>, has_more: bool) {
        self.rows = rows;
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
