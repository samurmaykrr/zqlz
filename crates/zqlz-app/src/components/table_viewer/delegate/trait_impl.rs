use super::*;

const ROW_NUMBER_SELECTION_TOOLTIP: &str =
    "Tip: Cmd/Ctrl+Click toggles multi-selection. Shift+Click selects a range.";

impl TableDelegate for TableViewerDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        if self.is_filtering {
            self.filtered_row_indices.len()
        } else {
            self.rows.len()
        }
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns
            .get(col_ix)
            .cloned()
            .unwrap_or_else(|| Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix)))
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
        let original_row_count = self
            .rows
            .len()
            .saturating_sub(self.pending_changes.new_row_count());
        let is_new_row = actual_row_ix >= original_row_count;

        // Row number column
        if col_ix == 0 {
            return div()
                .id(ElementId::NamedInteger(
                    "row-number-tip".into(),
                    (self.row_offset + actual_row_ix + 1) as u64,
                ))
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
                .tooltip(|window, cx| Tooltip::new(ROW_NUMBER_SELECTION_TOOLTIP).build(window, cx))
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
                                anchored().snap_to_window_with_margin(px(8.)).child(
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

                let display_value = if value.is_null() {
                    "NULL".to_string()
                } else {
                    value.display_for_table()
                };

                return div()
                    .h_full()
                    .flex()
                    .items_center()
                    .w_full()
                    // No container padding — Select's internal padding handles alignment,
                    // consistent with the cell_input branch.
                    .child(
                        Select::new(enum_select)
                            .w_full()
                            .with_size(Size::Small)
                            .appearance(false)
                            // Select keeps a transparent 1px border even with appearance(false).
                            // In table cells that introduces a subtle 1px geometry jump when
                            // switching between display and edit modes, so force border width to 0.
                            .border_0()
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

                let display_value = if value.is_null() {
                    "NULL".to_string()
                } else {
                    value.display_for_table()
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
                                div().flex_1().overflow_hidden().child(
                                    Select::new(fk_select)
                                        .w_full()
                                        .with_size(Size::Small)
                                        .appearance(false)
                                        // Keep inline-edit geometry stable with non-edit cell rendering.
                                        .border_0()
                                        .focus_border(false)
                                        .menu_min_width(px(180.))
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
                    // No container padding — the Input's own px (Size::Small = 8px) aligns
                    // text at the same position as non-editing cells (.px_2 = 8px).
                    .when(has_newlines, |this| this.pr_1())
                    .child(
                        Input::new(input)
                            .w_full()
                            // Strip all visual chrome so the cell's selection ring (painted by
                            // the table as an absolute overlay) is the sole editing indicator,
                            // matching how Excel/Sheets handle inline cell editing.
                            .appearance(false)
                            .with_size(Size::Small),
                    )
                    .when(has_newlines, |this| {
                        this.child(Icon::new(IconName::Info).size_4().text_color(theme.info))
                    })
                    .into_any_element();
            }
        }

        // Non-editing data cell rendering
        let data_col_ix = col_ix - 1;

        let is_modified = self
            .pending_changes
            .is_cell_modified(actual_row_ix, data_col_ix);
        let is_deleted = self.pending_changes.is_row_deleted(actual_row_ix);
        let original_row_count = self
            .rows
            .len()
            .saturating_sub(self.pending_changes.new_row_count());
        let is_new_row = actual_row_ix >= original_row_count;
        let is_boolean_column = self.is_boolean_column(data_col_ix);

        let value = self
            .rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix));
        let default_value = Value::default();
        let value = value.unwrap_or(&default_value);
        let matches_search = self.cell_matches_search(value);
        let is_null = value.is_null();
        let is_auto_increment_placeholder = matches!(value, Value::String(text) if text == super::inline_edit::AUTO_INCREMENT_PLACEHOLDER);
        let parsed_boolean_value = if is_boolean_column {
            self.parse_boolean_value(value)
        } else {
            None
        };

        // Auto-increment columns on new rows display a muted placeholder
        if is_new_row
            && self.is_auto_increment_column(data_col_ix)
            && (is_null || is_auto_increment_placeholder)
        {
            return div()
                .h_full()
                .flex()
                .items_center()
                .px_2()
                .text_sm()
                .text_color(theme.muted_foreground.opacity(0.5))
                .italic()
                .bg(theme.success.opacity(0.15))
                .child(super::inline_edit::AUTO_INCREMENT_PLACEHOLDER)
                .into_any_element();
        }

        // Boolean columns get a checkbox
        if is_boolean_column {
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
                .when(is_deleted, |this| this.bg(theme.danger.opacity(0.15)))
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
                .child(self.render_boolean_checkbox(parsed_boolean_value, is_deleted, window, cx))
                .into_any_element();
        }

        let cell_preview = self.inline_cell_preview_for_cell(actual_row_ix, data_col_ix);
        let display_value = cell_preview.text;
        let show_tooltip = cell_preview.show_tooltip;
        let tooltip_value = if show_tooltip && !is_null {
            self.rows
                .get(actual_row_ix)
                .and_then(|row| row.get(data_col_ix))
                .cloned()
        } else {
            None
        };

        let fk_info = self.get_fk_info(data_col_ix).cloned();

        div()
            .id(ElementId::NamedInteger(
                "td-tip".into(),
                (actual_row_ix * 10000 + col_ix) as u64,
            ))
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
            .when(is_null, |this| {
                this.text_color(theme.muted_foreground).child("NULL")
            })
            .when(!is_null, |this| {
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
                                        if let Err(e) = viewer_panel.update(cx, |panel, cx| {
                                            cx.emit(TableViewerEvent::NavigateToFkTable {
                                                connection_id,
                                                referenced_table: referenced_table.clone(),
                                                database_name: panel.database_name.clone(),
                                            });
                                        }) {
                                            tracing::error!(
                                                "Failed to emit NavigateToFkTable: {:?}",
                                                e
                                            );
                                        }
                                    }),
                            )
                        })
                        .child(div().overflow_hidden().text_ellipsis().child(display_value)),
                )
            })
            .when_some(tooltip_value, |this, tooltip_value| {
                this.tooltip(move |window, cx| {
                    let tooltip_text =
                        TableViewerDelegate::tooltip_text_for_cell_value(&tooltip_value);
                    Tooltip::new(tooltip_text).build(window, cx)
                })
            })
            .into_any_element()
    }

    fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        self.cell_text(row_ix, col_ix, _cx)
    }

    fn render_th(
        &mut self,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let column = self.columns.get(col_ix).cloned().unwrap_or_else(|| {
            Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix))
        });

        if col_ix == 0 {
            return div()
                .size_full()
                .flex()
                .items_center()
                .child(column.name.clone())
                .into_any_element();
        }

        let data_col_ix = col_ix - 1;
        let meta = self.column_meta.get(data_col_ix);

        let is_primary_key = meta
            .as_ref()
            .is_some_and(|m| self.primary_key_columns.contains(&m.name));
        let is_foreign_key = self.fk_by_column.contains_key(&data_col_ix);
        let is_nullable = meta.as_ref().is_some_and(|m| m.nullable);

        div()
            .size_full()
            .flex()
            .items_center()
            .gap_1()
            .when(is_primary_key, |this| {
                this.child(Icon::new(ZqlzIcon::Key).size_3().text_color(theme.warning))
            })
            .when(is_foreign_key, |this| {
                this.child(
                    Icon::new(IconName::ExternalLink)
                        .size_3()
                        .text_color(theme.accent),
                )
            })
            .child(column.name.clone())
            .when(is_nullable, |this| {
                this.child(
                    Button::new(format!("nullable-column-{}", data_col_ix))
                        .text()
                        .xsmall()
                        .label("∅")
                        .tooltip("Nullable column")
                        .text_color(theme.muted_foreground.opacity(0.6)),
                )
            })
            .into_any_element()
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
        if self.is_loading_more {
            return;
        }
        self.is_loading_more = true;
        let viewer_panel = self.viewer_panel.clone();
        let current_row_count = self.rows.len();
        cx.spawn_in(_window, async move |_this, cx| {
            if let Err(e) = viewer_panel.update_in(cx, |_panel, _window, cx| {
                cx.emit(TableViewerEvent::LoadMore {
                    current_offset: current_row_count,
                });
            }) {
                tracing::error!("Failed to emit LoadMore event: {:?}", e);
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn is_editing(&self, _cx: &App) -> bool {
        self.editing_cell.is_some()
            || self.cell_input.is_some()
            || self.date_picker_state.is_some()
            || self.enum_select_state.is_some()
            || self.fk_select_state.is_some()
    }

    fn visible_rows_changed(
        &mut self,
        visible_range: std::ops::Range<usize>,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) {
        self.visible_rows_range = Some(visible_range);
        self.warm_visible_preview_cache();
    }

    fn visible_columns_changed(
        &mut self,
        visible_range: std::ops::Range<usize>,
        _window: &mut Window,
        _cx: &mut Context<TableState<Self>>,
    ) {
        self.visible_columns_range = Some(visible_range);
        self.warm_visible_preview_cache();
    }

    fn calculate_auto_fit_width(&self, col_ix: usize, _cx: &App) -> f32 {
        self.calculate_column_width(col_ix)
    }
}

impl TableViewerDelegate {
    pub fn is_editing_date_cell(&self) -> bool {
        self.date_picker_state.is_some()
    }
}
