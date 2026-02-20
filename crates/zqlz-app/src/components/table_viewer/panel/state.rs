use super::*;

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
        self.selected_rows.clear();
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
        cx.notify();
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
        new_value: Option<String>,
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
            if let Some(row_data) = delegate.rows.get_mut(row) {
                if let Some(cell) = row_data.get_mut(col) {
                    let old_value = cell.clone();
                    *cell = new_value.unwrap_or_default();
                    tracing::info!("Cell updated: '{}' -> '{}'", old_value, cell);
                    cx.notify();
                } else {
                    tracing::warn!("Column {} not found in row {}", col, row);
                }
            } else {
                tracing::warn!("Row {} not found in table", row);
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
    pub fn begin_loading_table(&mut self, table_name: String, cx: &mut Context<Self>) {
        self.table_name = Some(table_name);
        self.set_loading(true, cx);
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
        for col in &mut self.column_meta {
            if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                if col.data_type != schema_col.data_type {
                    tracing::debug!(
                        "Updating column '{}' type from '{}' to '{}' (from schema)",
                        col.name,
                        col.data_type,
                        schema_col.data_type
                    );
                    col.data_type = schema_col.data_type.clone();
                }
            }
        }

        for col in &mut self.original_column_meta {
            if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                if col.data_type != schema_col.data_type {
                    col.data_type = schema_col.data_type.clone();
                }
            }
        }

        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, _cx| {
                let delegate = table.delegate_mut();
                for col in &mut delegate.column_meta {
                    if let Some(schema_col) = schema_columns.iter().find(|sc| sc.name == col.name) {
                        if col.data_type != schema_col.data_type {
                            col.data_type = schema_col.data_type.clone();
                        }
                    }
                }
            });
        }
    }

    pub fn set_fk_values(
        &mut self,
        table_name: String,
        values: Vec<crate::components::table_viewer::delegate::FkSelectItem>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table
                    .delegate_mut()
                    .set_fk_values(table_name, values, window, &mut **cx);
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
