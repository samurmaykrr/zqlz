use super::*;
use zqlz_core::Value;

impl TableViewerPanel {
    #[allow(dead_code)]
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.table_state = None;
        self.connection_id = None;
        self.table_name = None;
        self.database_name = None;
        self.row_count = 0;
        self.is_loading = false;
        self.loading_started_at = None;
        self._loading_timer_task = None;
        self.filter_panel_state = None;
        self.column_visibility_state = None;
        self.filter_expanded = false;
        self.column_visibility_shown = false;
        self.column_meta.clear();
        self.original_column_meta.clear();
        self.search_input = None;
        self.search_visible = false;
        self.search_text.clear();
        self.pagination_state = None;
        self.active_request_generation = 0;
        self.selection_stats = None;
        self.performance_profile = None;
        cx.notify();
    }

    pub fn begin_data_request(&mut self, cx: &mut Context<Self>) -> u64 {
        self.active_request_generation = self.active_request_generation.saturating_add(1);
        self.set_loading(true, cx);
        self.active_request_generation
    }

    pub fn current_request_generation(&self) -> u64 {
        self.active_request_generation
    }

    pub fn is_current_request(&self, request_generation: u64) -> bool {
        self.active_request_generation == request_generation
    }

    pub fn cancel_cell_editing(&mut self, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().stop_editing(false, cx);
            });
        }
    }

    pub fn update_cell_value(
        &mut self,
        row: usize,
        col: usize,
        new_value: Value,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "TableViewerPanel: Updating cell at row={}, col={} with value={:?}",
            row,
            col,
            new_value
        );

        let Some(table_state) = &self.table_state else {
            tracing::warn!("No table state available to update cell");
            return;
        };

        table_state.update(cx, |table, cx| {
            let delegate = table.delegate_mut();
            let previous_value = delegate
                .rows
                .get(row)
                .and_then(|row_data| row_data.get(col))
                .map(|cell| cell.display_for_table());

            if let Some(old_value) = previous_value {
                delegate.apply_value_locally(row, col, new_value.clone());
                let new_value_display = delegate
                    .rows
                    .get(row)
                    .and_then(|updated_row| updated_row.get(col))
                    .map(|cell| cell.display_for_table())
                    .unwrap_or_else(|| new_value.display_for_table());
                tracing::info!("Cell updated: '{}' -> '{}'", old_value, new_value_display);
                cx.notify();
            } else if delegate.rows.get(row).is_none() {
                tracing::warn!("Row {} not found in table", row);
            } else {
                tracing::warn!("Column {} not found in row {}", col, row);
            }
        });

        cx.notify();
    }

    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.is_loading = loading;
        if loading {
            self.loading_started_at = Some(std::time::Instant::now());
            // Tick every 100ms to update the elapsed timer display
            self._loading_timer_task = Some(cx.spawn(async move |this, cx| {
                loop {
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                    let should_continue = this
                        .update(cx, |panel, cx| {
                            if panel.is_loading {
                                cx.notify();
                                true
                            } else {
                                false
                            }
                        })
                        .unwrap_or(false);
                    if !should_continue {
                        break;
                    }
                }
            }));
        } else {
            self.loading_started_at = None;
            self._loading_timer_task = None;
        }
        cx.notify();
    }

    /// Begin loading a specific table — sets the table name for display while loading
    pub fn begin_loading_table(&mut self, table_name: String, cx: &mut Context<Self>) -> u64 {
        self.table_name = Some(table_name);
        self.begin_data_request(cx)
    }

    pub fn set_foreign_keys(&mut self, foreign_keys: Vec<ForeignKeyInfo>, cx: &mut Context<Self>) {
        self.foreign_keys = foreign_keys.clone();

        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, _cx| {
                table.delegate_mut().set_foreign_keys(foreign_keys);
            });
        }
    }

    pub fn set_primary_key_columns(&mut self, columns: Vec<String>) {
        self.primary_key_columns = columns;
    }

    pub fn update_column_types_from_schema(
        &mut self,
        schema_columns: &[SchemaColumnInfo],
        cx: &mut Context<Self>,
    ) {
        let merge_column = |column: &mut zqlz_core::ColumnMeta, schema_col: &SchemaColumnInfo| {
            column.data_type = schema_col.data_type.clone();
            column.nullable = schema_col.nullable;
            column.max_length = schema_col.max_length;
            column.precision = schema_col.precision;
            column.scale = schema_col.scale;
            column.auto_increment = schema_col.is_auto_increment;
            column.default_value = schema_col.default_value.clone();
            column.comment = schema_col.comment.clone();
            if let Some(enum_values) = &schema_col.enum_values {
                column.enum_values = Some(enum_values.clone());
            }
        };

        for col in &mut self.column_meta {
            if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                tracing::debug!(
                    "Updating column '{}' type from '{}' to '{}' (from schema)",
                    col.name,
                    col.data_type,
                    schema_col.data_type
                );
                merge_column(col, schema_col);
            }
        }

        for col in &mut self.original_column_meta {
            if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                merge_column(col, schema_col);
            }
        }

        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, _cx| {
                let delegate = table.delegate_mut();
                for col in &mut delegate.column_meta {
                    if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                        merge_column(col, schema_col);
                    }
                }
            });
        }
    }

    pub fn set_fk_values(
        &mut self,
        cache_key: String,
        values: Vec<crate::components::table_viewer::delegate::FkSelectItem>,
        query: Option<String>,
        request_id: u64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table
                    .delegate_mut()
                    .set_fk_values(cache_key, values, query, request_id, window, cx);
            });
        }
    }

    pub fn refresh(&self, cx: &mut Context<Self>) {
        if let (Some(connection_id), Some(table_name)) = (self.connection_id, &self.table_name) {
            tracing::info!(
                "TableViewerPanel: Refreshing table '{}' (driver={:?})",
                table_name,
                self.driver_category
            );
            cx.emit(TableViewerEvent::RefreshTable {
                connection_id,
                table_name: table_name.clone(),
                driver_category: self.driver_category,
                database_name: self.database_name.clone(),
            });
        } else {
            tracing::debug!("Cannot refresh: no table loaded");
        }
    }

    /// Update the total row count from a background count task.
    ///
    /// Called when a `CountCompleted` event arrives after the initial data
    /// load (for slow-count drivers where the count is decoupled from the
    /// data query for faster display).
    pub fn update_total_rows(
        &mut self,
        total_rows: u64,
        is_estimated: bool,
        table_name: &str,
        request_generation: u64,
        cx: &mut Context<Self>,
    ) {
        // Only apply if we're still showing the same table
        if self.table_name.as_deref() != Some(table_name)
            || !self.is_current_request(request_generation)
        {
            return;
        }
        if let Some(ref pagination_state) = self.pagination_state {
            pagination_state.update(cx, |state, cx| {
                state.total_records = Some(total_rows);
                state.is_estimated = is_estimated;
                cx.notify();
            });
        }
        cx.notify();
    }

    pub fn set_auto_commit_mode(&mut self, enabled: bool, cx: &mut Context<Self>) {
        self.auto_commit_mode = enabled;

        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |state, _| {
                state.delegate_mut().set_auto_commit_mode(enabled);
            });
        }

        cx.notify();
    }
}
