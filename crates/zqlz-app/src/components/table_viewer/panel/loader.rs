use super::*;

impl TableViewerPanel {
    /// Load table data into the viewer (extracted from original file)
    pub fn load_table(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        table_name: String,
        database_name: Option<String>,
        is_view: bool,
        result: QueryResult,
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
                        let viewer_panel = viewer_panel.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            _ = viewer_panel.update(cx, |panel, cx| {
                                panel.toggle_row_selection(row, cx);
                            });

                            _ = table_state_weak.update(cx, |table, cx| {
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
                            });

                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::ClickedCell { row, col } => {
                        let row = *row;
                        let col = *col;
                        let table_state_weak = table_state_weak.clone();
                        let viewer_panel = viewer_panel.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            _ = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().start_editing(row, col, window, cx);
                            });

                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::DoubleClickedCell { row, col } => {
                        let row = *row;
                        let col = *col;
                        let viewer_panel = viewer_panel.clone();
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

                            _ = table_state_weak.update_in(cx, |table, _window, cx| {
                                table.delegate_mut().stop_editing(false, cx);
                                // Ask the delegate to emit the EditCell event which opens the CellEditorPanel
                                table.delegate_mut().emit_edit_cell_event(row, col, col - 1, cx);
                            });
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::PasteCells { anchor, data } => {
                        let anchor = *anchor;
                        let data = data.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            _ = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().handle_paste(anchor, &data, window, cx);
                            });
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::StartBulkEdit { initial_char } => {
                        let initial_char = initial_char.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            _ = table_state_weak.update_in(cx, |table, window, cx| {
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
                            });
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::BulkPasteCells { cells, value } => {
                        let cells = cells.clone();
                        let value = value.clone();
                        let table_state_weak = table_state_weak.clone();

                        cx.spawn_in(window, async move |_this, cx| {
                            _ = table_state_weak.update_in(cx, |table, window, cx| {
                                table.delegate_mut().handle_bulk_paste(cells, value, window, cx);
                            });
                            anyhow::Ok(())
                        })
                        .detach();
                    }
                    TableEvent::CellSelectionChanged(selection) => {
                        let cell_count = selection.cell_count();
                        if cell_count > 1 {
                            tracing::debug!("Multi-cell selection: {} cells selected", cell_count);
                            let table_state_weak = table_state_weak.clone();
                            cx.spawn_in(window, async move |_this, cx| {
                                _ = table_state_weak.update_in(cx, |table, _window, cx| {
                                    table.delegate_mut().stop_editing(false, cx);
                                });
                                anyhow::Ok(())
                            })
                            .detach();
                        }
                    }
                    _ => {}
                }
            }
        })
        .detach();

        self.table_state = Some(table_state.clone());
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

            let filter_panel_state = cx.new(|cx| FilterPanelState::new(cx));
            filter_panel_state.update(cx, |state, cx| {
                state.set_columns(column_items, window, cx);
            });

            let connection_id_for_filter = connection_id;
            let table_name_for_filter = table_name.clone();
            cx.subscribe_in(&filter_panel_state, window, {
                move |this, _filter_state, event: &FilterPanelEvent, _window, cx| {
                    match event {
                        FilterPanelEvent::Apply => {
                            this.apply_filters(
                                connection_id_for_filter,
                                table_name_for_filter.clone(),
                                cx,
                            );
                        }
                        FilterPanelEvent::Changed => {}
                    }
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
                                    let is_infinite = matches!(mode, zqlz_ui::widgets::table::PaginationMode::InfiniteScroll);
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
                    let is_infinite = matches!(state.pagination_mode, zqlz_ui::widgets::table::PaginationMode::InfiniteScroll);
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
