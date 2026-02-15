use super::*;

impl TableDelegate for TableViewerDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        if self.is_filtering { self.filtered_row_indices.len() } else { self.rows.len() }
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns.get(col_ix).cloned().unwrap_or_else(|| Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix)))
    }

    fn set_context_menu_selection(&mut self, selected_rows: Vec<usize>) {
        self.context_menu_selected_rows = selected_rows;
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let actual_row_ix = self.get_actual_row_index(row_ix);

        let is_deleted = self.pending_changes.is_row_deleted(actual_row_ix);
        let original_row_count = self.rows.len() - self.pending_changes.new_row_count();
        let is_new_row = actual_row_ix >= original_row_count;

        // Row number column
        if col_ix == 0 {
            return div()
                .h_full()
                .flex()
                .items_center()
                .justify_center()
                .px_2()
                .text_sm()
                .text_color(theme.muted_foreground)
                .when(is_deleted, |this| {
                    this.bg(theme.danger.opacity(0.15)).line_through()
                })
                .when(!is_deleted && is_new_row, |this| {
                    this.bg(theme.success.opacity(0.15))
                })
                .child((self.row_offset + actual_row_ix + 1).to_string())
                .into_any_element();
        }

        // Inline editing: render the active editor widget when this cell is being edited
        if self.editing_cell == Some((actual_row_ix, col_ix)) {
            if let Some(date_picker) = &self.date_picker_state {
                let date_picker_clone = date_picker.clone();
                let is_popover_open = date_picker.read(cx).is_popover_open();

                return div()
                    .h_full()
                    .flex()
                    .items_center()
                    .w_full()
                    .px_1()
                    .relative()
                    .child(DatePickerInline::new(date_picker))
                    .when(is_popover_open, |this| {
                        this.child(
                            deferred(
                                anchored()
                                    .snap_to_window_with_margin(px(8.))
                                    .child(
                                        div()
                                            .occlude()
                                            .mt_1()
                                            .child(DatePickerPopover::new(&date_picker_clone)),
                                    ),
                            )
                            .with_priority(1),
                        )
                    })
                    .into_any_element();
            }

            if let Some(enum_select) = &self.enum_select_state {
                let data_col_ix = col_ix - 1;
                let value = self
                    .rows
                    .get(actual_row_ix)
                    .and_then(|row| row.get(data_col_ix))
                    .cloned()
                    .unwrap_or_default();

                let display_value = if value.eq_ignore_ascii_case("null") {
                    "NULL".to_string()
                } else {
                    value
                };

                return div()
                    .h_full()
                    .flex()
                    .items_center()
                    .w_full()
                    .px_1()
                    .child(
                        Select::new(enum_select)
                            .w_full()
                            .with_size(Size::Small)
                            .appearance(false)
                            .focus_border(false)
                            .placeholder(display_value),
                    )
                    .into_any_element();
            }

            if let Some(fk_select) = &self.fk_select_state {
                let data_col_ix = col_ix - 1;
                let value = self
                    .rows
                    .get(actual_row_ix)
                    .and_then(|row| row.get(data_col_ix))
                    .cloned()
                    .unwrap_or_default();

                let display_value = if value.eq_ignore_ascii_case("null") {
                    "NULL".to_string()
                } else {
                    value
                };

                return div()
                    .h_full()
                    .flex()
                    .items_center()
                    .w_full()
                    .px_2()
                    .relative()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .w_full()
                            .child(
                                Icon::new(IconName::ExternalLink)
                                    .size_3()
                                    .text_color(theme.accent),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .overflow_hidden()
                                    .child(
                                        Select::new(fk_select)
                                            .w_full()
                                            .with_size(Size::Small)
                                            .appearance(false)
                                            .focus_border(false)
                                            .menu_min_width(px(120.))
                                            .placeholder(display_value),
                                    ),
                            ),
                    )
                    .into_any_element();
            }

            if let Some(input) = &self.cell_input {
                let has_newlines = self.editing_cell_has_newlines;
                return div()
                    .h_full()
                    .flex()
                    .items_center()
                    .w_full()
                    .px_1()
                    .gap_1()
                    .child(Input::new(input).w_full())
                    .when(has_newlines, |this| {
                        this.child(Icon::new(IconName::Info).size_4().text_color(theme.info))
                    })
                    .into_any_element();
            }
        }

        // Non-editing data cell rendering
        let data_col_ix = col_ix - 1;
        let value = self
            .rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .cloned()
            .unwrap_or_default();

        let is_modified = self
            .pending_changes
            .is_cell_modified(actual_row_ix, data_col_ix);
        let is_deleted = self.pending_changes.is_row_deleted(actual_row_ix);
        let original_row_count = self.rows.len() - self.pending_changes.new_row_count();
        let is_new_row = actual_row_ix >= original_row_count;
        let matches_search = self.cell_matches_search(&value);

        // Boolean columns get a checkbox
        if self.is_boolean_column(data_col_ix) {
            let bool_value = self.parse_boolean_value(&value);

            return div()
                .id(ElementId::NamedInteger(
                    "bool-cell".into(),
                    (actual_row_ix * 10000 + col_ix) as u64,
                ))
                .h_full()
                .flex()
                .items_center()
                .justify_center()
                .w_full()
                .cursor_pointer()
                .when(is_deleted, |this| {
                    this.bg(theme.danger.opacity(0.15))
                })
                .when(!is_deleted && is_new_row, |this| {
                    this.bg(theme.success.opacity(0.15))
                })
                .when(!is_deleted && !is_new_row && is_modified, |this| {
                    this.bg(theme.warning.opacity(0.25))
                })
                .when(
                    !is_deleted && !is_new_row && !is_modified && matches_search,
                    |this| this.bg(theme.warning.opacity(0.15)),
                )
                .child(self.render_boolean_checkbox(bool_value, is_deleted, window, cx))
                .into_any_element();
        }

        let display_value = if value.contains('\n') || value.contains('\r') {
            value
                .replace("\r\n", " ")
                .replace('\n', " ")
                .replace('\r', " ")
        } else {
            value.clone()
        };

        let fk_info = self.get_fk_info(data_col_ix).cloned();

        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .overflow_hidden()
            .text_ellipsis()
            .when(is_deleted, |this| {
                this.bg(theme.danger.opacity(0.15))
                    .line_through()
                    .text_color(theme.muted_foreground)
            })
            .when(!is_deleted && is_new_row, |this| {
                this.bg(theme.success.opacity(0.15))
            })
            .when(!is_deleted && !is_new_row && is_modified, |this| {
                this.bg(theme.warning.opacity(0.25))
            })
            .when(
                !is_deleted && !is_new_row && !is_modified && matches_search,
                |this| this.bg(theme.warning.opacity(0.15)),
            )
            .when(value.eq_ignore_ascii_case("null"), |this| {
                this.text_color(theme.muted_foreground).child("NULL")
            })
            .when(!value.eq_ignore_ascii_case("null"), |this| {
                this.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .w_full()
                        .when_some(fk_info.clone(), |this, fk| {
                            let referenced_table = fk.referenced_table.clone();
                            let connection_id = self.connection_id;
                            let viewer_panel = self.viewer_panel.clone();
                            let fk_icon_id = actual_row_ix * 10000 + col_ix;
                            this.child(
                                div()
                                    .id(("fk-icon", fk_icon_id))
                                    .cursor_pointer()
                                    .rounded_sm()
                                    .hover(|s| s.bg(theme.accent.opacity(0.15)))
                                    .child(
                                        Icon::new(IconName::ExternalLink)
                                            .size_3()
                                            .text_color(theme.muted_foreground.opacity(0.6)),
                                    )
                                    .on_click(move |_, _window, cx| {
                                        let _ = viewer_panel.update(cx, |panel, cx| {
                                            cx.emit(TableViewerEvent::NavigateToFkTable {
                                                connection_id,
                                                referenced_table: referenced_table.clone(),
                                                database_name: panel.database_name.clone(),
                                            });
                                        });
                                    }),
                            )
                        })
                        .child(
                            div()
                                .overflow_hidden()
                                .text_ellipsis()
                                .child(display_value),
                        ),
                )
            })
            .into_any_element()
    }

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        self.cell_text(row_ix, col_ix, _cx)
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) {
        self.perform_sort(col_ix, sort, window, cx)
    }

    fn context_menu(
        &mut self,
        row_ix: usize,
        col_ix_opt: Option<usize>,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> PopupMenu {
        self.context_menu(row_ix, col_ix_opt, menu, window, cx)
    }

    fn column_context_menu(
        &mut self,
        col_ix: usize,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> PopupMenu {
        self.column_context_menu(col_ix, menu, window, cx)
    }

    fn has_more(&self, _cx: &App) -> bool {
        self.infinite_scroll_enabled && self.has_more_data && !self.is_loading_more
    }

    fn load_more(&mut self, _window: &mut Window, cx: &mut Context<TableState<Self>>) {
        if self.is_loading_more { return; }
        self.is_loading_more = true;
        let viewer_panel = self.viewer_panel.clone();
        let current_row_count = self.rows.len();
        cx.spawn_in(_window, async move |_this, cx| {
            _ = viewer_panel.update_in(cx, |panel, _window, cx| {
                cx.emit(TableViewerEvent::LoadMore { current_offset: current_row_count });
            });
            anyhow::Ok(())
        }).detach();
    }

    fn is_editing(&self, _cx: &App) -> bool {
        self.editing_cell.is_some() || self.cell_input.is_some() || self.date_picker_state.is_some() || self.enum_select_state.is_some() || self.fk_select_state.is_some()
    }
}

impl TableViewerDelegate {
    pub fn is_editing_date_cell(&self) -> bool {
        self.date_picker_state.is_some()
    }
}
