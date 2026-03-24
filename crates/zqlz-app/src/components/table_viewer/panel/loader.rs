use super::*;

impl TableViewerPanel {
    fn is_heavy_column_type(data_type: &str) -> bool {
        let data_type = data_type.to_ascii_lowercase();
        ["json", "text", "blob", "bytea", "clob", "xml"]
            .iter()
            .any(|token| data_type.contains(token))
    }

    fn build_table_performance_profile(columns: &[ColumnMeta]) -> TablePerformanceProfile {
        let mut heavy_count = 0usize;
        let mut non_heavy_searchable_columns = Vec::new();
        let mut heavy_searchable_columns = Vec::new();

        for column in columns {
            let is_heavy = Self::is_heavy_column_type(&column.data_type);
            if is_heavy {
                heavy_count = heavy_count.saturating_add(1);
            }

            let lowered_type = column.data_type.to_ascii_lowercase();
            if zqlz_services::TableService::is_string_type(&lowered_type) {
                if is_heavy {
                    heavy_searchable_columns.push(column.name.clone());
                } else {
                    non_heavy_searchable_columns.push(column.name.clone());
                }
            }
        }

        let heavy_ratio = if columns.is_empty() {
            0.0
        } else {
            heavy_count as f32 / columns.len() as f32
        };
        let is_heavy_table = heavy_count >= 4 || heavy_ratio >= 0.3;
        let recommended_page_size = if is_heavy_table {
            if heavy_count >= 8 || heavy_ratio >= 0.6 {
                100
            } else {
                250
            }
        } else {
            1000
        };

        let mut searchable_columns = non_heavy_searchable_columns;
        searchable_columns.extend(heavy_searchable_columns);
        searchable_columns.truncate(8);

        TablePerformanceProfile {
            is_heavy_table,
            recommended_page_size,
            searchable_columns,
        }
    }

    fn capture_current_column_widths(&self, cx: &App) -> std::collections::HashMap<String, Pixels> {
        let Some(table_state) = &self.table_state else {
            return std::collections::HashMap::new();
        };

        table_state.read_with(cx, |table, _cx| {
            let delegate = table.delegate();
            let mut widths = std::collections::HashMap::new();

            for (data_index, metadata) in delegate.column_meta.iter().enumerate() {
                if let Some(column) = delegate.columns().get(data_index + 1) {
                    widths.insert(metadata.name.clone(), column.width);
                }
            }

            widths
        })
    }

    fn apply_preserved_column_widths(
        &self,
        delegate: &mut TableViewerDelegate,
        widths_by_column_name: &std::collections::HashMap<String, Pixels>,
    ) {
        if widths_by_column_name.is_empty() {
            return;
        }

        let column_names: Vec<String> = delegate
            .column_meta
            .iter()
            .map(|metadata| metadata.name.clone())
            .collect();

        for (data_index, column_name) in column_names.iter().enumerate() {
            let Some(width) = widths_by_column_name.get(column_name) else {
                continue;
            };

            if let Some(column) = delegate.columns_mut().get_mut(data_index + 1) {
                *column = column.clone().width(*width);
            }
        }
    }

    fn apply_active_sort_to_delegate_columns(&self, delegate: &mut TableViewerDelegate, cx: &App) {
        use crate::components::table_viewer::filter_types::SortDirection;
        use zqlz_ui::widgets::table::ColumnSort;

        for column_index in 1..delegate.columns().len() {
            if let Some(column) = delegate.columns_mut().get_mut(column_index) {
                *column = column.clone().sort(ColumnSort::Default);
            }
        }

        let Some(filter_state) = &self.filter_panel_state else {
            return;
        };

        let active_sort = filter_state.read_with(cx, |state, _cx| {
            state.get_sort_criteria().into_iter().next()
        });

        let Some(active_sort) = active_sort else {
            return;
        };

        let Some(data_index) = delegate
            .column_meta
            .iter()
            .position(|metadata| metadata.name == active_sort.column)
        else {
            return;
        };

        let sort = match active_sort.direction {
            SortDirection::Ascending => ColumnSort::Ascending,
            SortDirection::Descending => ColumnSort::Descending,
        };

        if let Some(column) = delegate.columns_mut().get_mut(data_index + 1) {
            *column = column.clone().sort(sort);
        }
    }

    fn apply_column_visibility_to_result(&self, result: &mut QueryResult, cx: &App) {
        let Some(column_visibility_state) = &self.column_visibility_state else {
            return;
        };

        let visible_columns = column_visibility_state.read(cx).visible_columns();

        if visible_columns.len() == result.columns.len()
            && result
                .columns
                .iter()
                .zip(visible_columns.iter())
                .all(|(column, visible_name)| column.name == *visible_name)
        {
            return;
        }

        let visible_columns_set: std::collections::HashSet<&str> =
            visible_columns.iter().map(String::as_str).collect();

        let visible_indexes: Vec<usize> = result
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                visible_columns_set
                    .contains(column.name.as_str())
                    .then_some(index)
            })
            .collect();

        result.columns = visible_indexes
            .iter()
            .filter_map(|index| result.columns.get(*index).cloned())
            .collect();

        for row in &mut result.rows {
            row.values = visible_indexes
                .iter()
                .filter_map(|index| row.values.get(*index).cloned())
                .collect();
        }
    }

    /// Load table data into the viewer (extracted from original file)
    #[allow(clippy::too_many_arguments)]
    pub fn load_table(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        table_name: String,
        database_name: Option<String>,
        is_view: bool,
        mut result: QueryResult,
        driver_category: DriverCategory,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Reuse original implementation from panel.rs
        // For brevity in this split task we call into the original logic by copying.
        // (Code copied from original file - loader logic)

        // Check if this is a re-load of the same table (filter apply / refresh)
        let is_same_table = self.connection_id == Some(connection_id)
            && self.table_name.as_ref() == Some(&table_name);

        self.performance_profile = if matches!(driver_category, DriverCategory::Relational) {
            Some(Self::build_table_performance_profile(&result.columns))
        } else {
            None
        };

        if let Some(profile) = &self.performance_profile {
            tracing::debug!(
                table = %table_name,
                is_heavy_table = profile.is_heavy_table,
                recommended_page_size = profile.recommended_page_size,
                searchable_columns = profile.searchable_columns.len(),
                "Computed table performance profile"
            );
        }

        tracing::info!(
            "load_table: table={}, is_view={}, is_same_table={}, driver={:?}, original_cols={}, result_cols={}, has_col_vis_state={}",
            table_name,
            is_view,
            is_same_table,
            driver_category,
            self.original_column_meta.len(),
            result.columns.len(),
            self.column_visibility_state.is_some()
        );

        if !self.original_column_meta.is_empty() {
            let orig_names: Vec<&str> = self
                .original_column_meta
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            tracing::info!("load_table: original_column_meta names = {:?}", orig_names);
        }

        let result_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        tracing::info!("load_table: result.columns names = {:?}", result_names);

        if let Some(ref col_vis_state) = self.column_visibility_state {
            let (total, visible_count, col_names) = col_vis_state.read_with(cx, |state, _cx| {
                let names: Vec<String> = state.visible_columns();
                (state.total_count(), state.visible_count(), names)
            });
            tracing::info!(
                "load_table: column_visibility_state has {} columns ({} visible): {:?}",
                total,
                visible_count,
                col_names
            );
        }

        if is_same_table && self.column_visibility_state.is_some() {
            self.apply_column_visibility_to_result(&mut result, cx);
        }

        let preserved_widths = if is_same_table {
            self.capture_current_column_widths(cx)
        } else {
            std::collections::HashMap::new()
        };

        self.connection_id = Some(connection_id);
        self.connection_name = Some(connection_name);
        self.table_name = Some(table_name.clone());
        self.database_name = database_name;
        self.driver_category = driver_category;
        self.is_view = is_view;
        self.row_count = result.rows.len();

        if !is_same_table {
            self.search_text.clear();
            self.search_visible = false;
            self.search_input = None;
            self._search_debounce_task = None;
        }

        let viewer_panel_weak = cx.entity().downgrade();
        let mut delegate = TableViewerDelegate::new(
            &result,
            table_name.clone(),
            connection_id,
            viewer_panel_weak.clone(),
        );

        delegate.set_auto_commit_mode(self.auto_commit_mode);
        delegate.set_driver_category(driver_category);
        delegate.set_primary_key_columns(self.primary_key_columns.clone());
        self.apply_preserved_column_widths(&mut delegate, &preserved_widths);
        self.apply_active_sort_to_delegate_columns(&mut delegate, cx);

        let table_state = cx.new(|cx| {
            TableState::new(delegate, window, cx)
                .col_resizable(true)
                .sortable(true)
                .row_selectable(true)
        });

        // Subscribe to table events...
        let viewer_panel = cx.entity().clone();
        let table_state_weak = table_state.downgrade();
        cx.subscribe_in(&table_state, window, {
            move |_this, _table, event: &zqlz_ui::widgets::table::TableEvent, window, cx| {
                use zqlz_ui::widgets::table::TableEvent;

                tracing::debug!("Table event received: {:?}", std::mem::discriminant(event));

                match event {
                    TableEvent::SelectRow(row) => {
                        let row = *row;
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(e) = table_state_weak.update(cx, |table, cx| {
                                let delegate = table.delegate();
                                let actual_row = delegate.get_actual_row_index(row);
                                if let Some(row_values) = delegate.rows.get(actual_row) {
                                    let event = TableViewerEvent::RowSelected {
                                        connection_id: delegate.connection_id,
                                        table_name: delegate.table_name.clone(),
                                        row_index: actual_row,
                                        row_values: row_values.clone(),
                                        column_meta: delegate.column_meta.clone(),
                                        all_column_names: delegate
                                            .column_meta
                                            .iter()
                                            .map(|c| c.name.clone())
                                            .collect(),
                                    };
                                    if let Some(viewer) = delegate.viewer_panel.upgrade() {
                                        viewer.update(cx, |_panel, cx| {
                                            cx.emit(event);
                                        });
                                    }
                                }
                            }) {
                                tracing::error!("Failed to emit RowSelected event: {:?}", e);
                            }

                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::ClickedCell { row, col } => {
                        let row = *row;
                        let col = *col;
                        let table_state_weak = table_state_weak.clone();
                        let _viewer_panel = viewer_panel.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(e) = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().start_editing(row, col, window, cx);
                            }) {
                                tracing::error!("Failed to start editing cell ({}, {}): {:?}", row, col, e);
                            }

                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::DoubleClickedCell { row, col } => {
                        let row = *row;
                        let col = *col;
                        let _viewer_panel = viewer_panel.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            let is_date_edit = table_state_weak
                                .update(cx, |table, _cx| {
                                    table.delegate().is_editing_date_cell()
                                })
                                .unwrap_or(false);

                            if is_date_edit {
                                return anyhow::Ok(());
                            }

                            if col == 0 {
                                tracing::debug!(
                                    "Ignoring double-click edit request for row-number column at row {}",
                                    row
                                );
                                return anyhow::Ok(());
                            }

                            if let Err(e) = table_state_weak.update_in(cx, |table, _window, cx| {
                                table.delegate_mut().stop_editing(false, cx);
                                // Ask the delegate to emit the EditCell event which opens the CellEditorPanel
                                table.delegate_mut().emit_edit_cell_event(row, col, col - 1, cx);
                            }) {
                                tracing::error!("Failed to emit EditCell event on double-click: {:?}", e);
                            }
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::PasteCells { anchor, data } => {
                        let anchor = *anchor;
                        let data = data.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(e) = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().handle_paste(anchor, &data, window, cx);
                            }) {
                                tracing::error!("Failed to handle paste: {:?}", e);
                            }
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::StartBulkEdit { initial_char } => {
                        let initial_char = initial_char.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(e) = table_state_weak.update_in(cx, |table, window, cx| {
                                let anchor = table.cell_selection().anchor();
                                let cells = table.cell_selection().selected_cells();

                                if let Some(anchor) = anchor {
                                    table.delegate_mut().start_bulk_editing(
                                        anchor,
                                        cells,
                                        initial_char,
                                        window,
                                        cx,
                                    );
                                }
                            }) {
                                tracing::error!("Failed to start bulk editing: {:?}", e);
                            }
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::BulkPasteCells { cells, value } => {
                        let cells = cells.clone();
                        let value = value.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(e) = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().handle_bulk_paste(cells, value, window, cx);
                            }) {
                                tracing::error!("Failed to handle bulk paste: {:?}", e);
                            }
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::CellSelectionChanged(selection) => {
                        viewer_panel.update(cx, |panel, cx| {
                            panel.update_selection_stats_from_table(cx);
                        });
                        cx.notify();
                        let cell_count = selection.cell_count();
                        if cell_count > 1 {
                            tracing::debug!("Multi-cell selection: {} cells selected", cell_count);
                            let table_state_weak = table_state_weak.clone();
                            cx.spawn_in(window, async move |_this, cx| {
                                if let Err(e) = table_state_weak.update_in(cx, |table, _window, cx| {
                                    table.delegate_mut().stop_editing(false, cx);
                                }) {
                                    tracing::error!("Failed to stop editing on multi-cell selection: {:?}", e);
                                }
                                anyhow::Ok(())
                            })
                            .detach();
                        }
                    }
                    TableEvent::ColumnWidthsChanged(widths) => {
                        let widths = widths.clone();
                        let table_state_weak = table_state_weak.clone();
                        cx.spawn_in(window, async move |_this, cx| {
                            if let Err(error) = table_state_weak.update(cx, |table, _cx| {
                                let columns = table.delegate_mut().columns_mut();
                                for (column_index, width) in widths.iter().enumerate() {
                                    if let Some(column) = columns.get_mut(column_index) {
                                        *column = column.clone().width(*width);
                                    }
                                }
                            }) {
                                tracing::debug!(
                                    error = %error,
                                    "Failed to persist resized column widths"
                                );
                            }

                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    _ => {}
                }
            }
        })
        .detach();

        self.table_state = Some(table_state.clone());
        self.update_selection_stats_from_table(cx);

        // Auto-expand the transaction panel as soon as unsaved edits appear.
        // Doing this in render() would be a side-effecting mutation during rendering,
        // so we use an observe callback instead.
        if !self.auto_commit_mode {
            cx.observe(&table_state, |this, table_state, cx| {
                if !this.transaction_panel_expanded
                    && table_state.read(cx).delegate().has_pending_changes()
                {
                    this.transaction_panel_expanded = true;
                    cx.notify();
                }
            })
            .detach();
        }

        self.set_loading(false, cx);

        if !self.foreign_keys.is_empty() {
            table_state.update(cx, |table, _cx| {
                table
                    .delegate_mut()
                    .set_foreign_keys(self.foreign_keys.clone());
            });
        }

        self.column_meta = result.columns.clone();

        let records_loaded = result.rows.len();
        let total_records = result.total_rows;
        let is_estimated = result.is_estimated_total;

        if !is_same_table || self.original_column_meta.is_empty() {
            tracing::info!(
                "load_table: INITIALIZING state (is_same_table={}, original_empty={})",
                is_same_table,
                self.original_column_meta.is_empty()
            );

            self.original_column_meta = result.columns.clone();

            let column_items: Vec<ColumnSelectItem> = self
                .original_column_meta
                .iter()
                .map(|col| ColumnSelectItem {
                    name: col.name.clone().into(),
                    data_type: col.data_type.clone().into(),
                    is_custom: false,
                })
                .collect();

            let filter_panel_state = cx.new(FilterPanelState::new);
            filter_panel_state.update(cx, |state, cx| {
                state.set_columns(column_items, window, cx);
            });

            let connection_id_for_filter = connection_id;
            let table_name_for_filter = table_name.clone();
            cx.subscribe_in(&filter_panel_state, window, {
                move |this, _filter_state, event: &FilterPanelEvent, _window, cx| match event {
                    FilterPanelEvent::Apply => {
                        this.apply_filters(
                            connection_id_for_filter,
                            table_name_for_filter.clone(),
                            cx,
                        );
                    }
                    FilterPanelEvent::Changed => {}
                }
            })
            .detach();

            self.filter_panel_state = Some(filter_panel_state);

            let column_visibility_state = cx.new(|cx| ColumnVisibilityState::new(window, cx));
            column_visibility_state.update(cx, |state, cx| {
                state.set_columns_from_meta(
                    self.original_column_meta
                        .iter()
                        .map(|col| (col.name.clone(), col.data_type.clone())),
                    cx,
                );
            });

            let connection_id_for_col_vis = connection_id;
            let table_name_for_col_vis = table_name.clone();
            cx.subscribe_in(&column_visibility_state, window, {
                move |this, _visibility_state, event: &ColumnVisibilityEvent, _window, cx| {
                    let conn_id = connection_id_for_col_vis;
                    let tbl_name = table_name_for_col_vis.clone();
                    match event {
                        ColumnVisibilityEvent::ColumnToggled { .. }
                        | ColumnVisibilityEvent::AllColumnsChanged => {
                            if let Some(filter_state) = &this.filter_panel_state {
                                filter_state.update(cx, |state, cx| {
                                    state.is_dirty = true;
                                    cx.notify();
                                });
                            }
                            this.apply_filters(conn_id, tbl_name, cx);
                        }
                    }
                }
            })
            .detach();

            self.column_visibility_state = Some(column_visibility_state);

            let pagination_state = cx.new(|cx| PaginationState::new(window, cx));

            let connection_id_for_pag = connection_id;
            let table_name_for_pag = table_name.clone();
            cx.subscribe_in(&pagination_state, window, {
                move |this, _pagination_state, event: &PaginationEvent, _window, cx| {
                    let conn_id = connection_id_for_pag;
                    let tbl_name = table_name_for_pag.clone();
                    match event {
                        PaginationEvent::PageChanged(page) => {
                            if let Some(pag_state) = &this.pagination_state {
                                let limit = pag_state.read(cx).records_per_page;
                                cx.emit(TableViewerEvent::PageChanged {
                                    connection_id: conn_id,
                                    table_name: tbl_name,
                                    page: *page,
                                    limit,
                                });
                            }
                        }
                        PaginationEvent::LimitChanged(limit) => {
                            cx.emit(TableViewerEvent::LimitChanged {
                                connection_id: conn_id,
                                table_name: tbl_name,
                                limit: *limit,
                            });
                        }
                        PaginationEvent::LimitEnabledChanged(enabled) => {
                            cx.emit(TableViewerEvent::LimitEnabledChanged {
                                connection_id: conn_id,
                                table_name: tbl_name,
                                enabled: *enabled,
                            });
                        }
                        PaginationEvent::RefreshRequested => {
                            this.refresh(cx);
                        }
                        PaginationEvent::ModeChanged(mode) => {
                            if let Some(table_state) = &this.table_state {
                                table_state.update(cx, |table, _cx| {
                                    let delegate = table.delegate_mut();
                                    delegate.replace_rows(Vec::new(), true);
                                    let is_infinite = matches!(
                                        mode,
                                        zqlz_ui::widgets::table::PaginationMode::InfiniteScroll
                                    );
                                    delegate.set_infinite_scroll_enabled(is_infinite);
                                });
                            }
                            this.refresh(cx);
                        }
                        PaginationEvent::LastPageRequested => {
                            cx.emit(TableViewerEvent::LastPageRequested {
                                connection_id: conn_id,
                                table_name: tbl_name,
                            });
                        }
                    }
                }
            })
            .detach();

            self.pagination_state = Some(pagination_state);

            cx.emit(TableViewerEvent::BecameActive {
                connection_id,
                table_name,
                database_name: self.database_name.clone(),
            });
        } else {
            tracing::info!("load_table: SKIPPING reinitialization - preserving existing state");
        }

        if let Some(ref pagination_state) = self.pagination_state {
            if !is_same_table && let Some(profile) = &self.performance_profile {
                pagination_state.update(cx, |state, _cx| {
                    if state.records_per_page == 1000 {
                        state.records_per_page = profile.recommended_page_size;
                    }
                });
            }

            pagination_state.update(cx, |state, cx| {
                state.update_after_load(records_loaded, total_records, is_estimated, cx);
            });

            if let Some(ref table_state) = self.table_state {
                let row_offset = pagination_state.read_with(cx, |state, _cx| {
                    if state.limit_enabled {
                        (state.current_page - 1) * state.records_per_page
                    } else {
                        0
                    }
                });

                table_state.update(cx, |table, cx| {
                    table.delegate_mut().set_row_offset(row_offset);

                    let max_row = row_offset + table.delegate().rows.len();
                    let width = TableViewerDelegate::row_number_column_width(max_row);
                    let columns = table.delegate_mut().columns_mut();
                    if !columns.is_empty() {
                        columns[0] = columns[0].clone().width(width);
                    }
                    table.refresh(cx);
                });

                let (is_infinite_mode, limit) = pagination_state.read_with(cx, |state, _cx| {
                    let is_infinite = matches!(
                        state.pagination_mode,
                        zqlz_ui::widgets::table::PaginationMode::InfiniteScroll
                    );
                    (is_infinite, state.records_per_page)
                });

                if is_infinite_mode {
                    table_state.update(cx, |table, _cx| {
                        let delegate = table.delegate_mut();
                        delegate.set_infinite_scroll_enabled(true);
                        let has_more = records_loaded >= limit;
                        delegate.set_has_more_data(has_more);
                        tracing::info!(
                            "load_table: Synced infinite scroll mode - records_loaded={}, limit={}, has_more={}",
                            records_loaded,
                            limit,
                            has_more
                        );
                    });
                }
            }
        }

        if let Some(ref col_vis_state) = self.column_visibility_state {
            let (total, visible_count) = col_vis_state.read_with(cx, |state, _cx| {
                (state.total_count(), state.visible_count())
            });
            tracing::info!(
                "load_table: FINAL column_visibility_state has {} columns ({} visible)",
                total,
                visible_count
            );
        }

        cx.notify();
    }
}
