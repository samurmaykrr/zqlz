use std::collections::HashSet;

use super::*;
use zqlz_ui::widgets::spinner::Spinner;

impl TableViewerPanel {
    fn render_selection_stats(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let table_state = self.table_state.as_ref()?;
        let table = table_state.read(cx);
        let selection = table.cell_selection();

        let cell_count = selection.cell_count();
        if cell_count == 0 {
            return None;
        }

        let selected_cells = selection.selected_cells();
        if selected_cells.is_empty() {
            return None;
        }

        let theme = cx.theme();
        let delegate = table.delegate();

        let distinct_rows: HashSet<usize> = selected_cells.iter().map(|cell| cell.row).collect();

        let selection_label = if cell_count == 1 {
            "1 cell".to_string()
        } else if distinct_rows.len() == 1 {
            format!("{} cells in 1 row", cell_count)
        } else {
            format!("{} cells in {} rows", cell_count, distinct_rows.len())
        };

        let mut numeric_values: Vec<f64> = Vec::new();
        for cell in &selected_cells {
            let text = delegate.cell_text(cell.row, cell.col, cx);
            if let Ok(number) = text.parse::<f64>() {
                numeric_values.push(number);
            }
        }

        let stats_elements = if numeric_values.len() > 1 {
            let sum: f64 = numeric_values.iter().sum();
            let average = sum / numeric_values.len() as f64;
            let min = numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = numeric_values
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);

            let format_number = |value: f64| -> String {
                if value == value.floor() && value.abs() < 1e15 {
                    format!("{}", value as i64)
                } else {
                    format!("{:.2}", value)
                }
            };

            Some((
                format_number(sum),
                format_number(average),
                format_number(min),
                format_number(max),
            ))
        } else {
            None
        };

        Some(
            h_flex()
                .w_full()
                .h(px(24.0))
                .px_3()
                .gap_3()
                .items_center()
                .justify_end()
                .border_t_1()
                .border_color(theme.border)
                .bg(theme.tab_bar)
                .text_xs()
                .text_color(theme.muted_foreground)
                .child(selection_label)
                .when_some(stats_elements, |this, (sum, average, min, max)| {
                    this.child(div().h(px(12.0)).w(px(1.0)).bg(theme.border))
                        .child(format!("Sum: {}", sum))
                        .child(format!("Avg: {}", average))
                        .child(format!("Min: {}", min))
                        .child(format!("Max: {}", max))
                })
                .into_any_element(),
        )
    }

    pub fn render_empty(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("No table selected"),
        )
    }

    pub fn render_loading(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let elapsed_text = self
            .loading_started_at
            .map(|started| {
                let elapsed = started.elapsed();
                let table_label = self
                    .table_name
                    .as_deref()
                    .map(|name| format!("'{}'", name))
                    .unwrap_or_else(|| "table".to_string());
                format!("Loading {} ... {}ms", table_label, elapsed.as_millis())
            })
            .unwrap_or_else(|| "Loading table data...".to_string());

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_3()
            .child(Spinner::new().color(theme.muted_foreground))
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child(elapsed_text),
            )
    }

    pub fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let pagination_info = self
            .pagination_state
            .as_ref()
            .filter(|_| matches!(self.driver_category, DriverCategory::Relational))
            .map(|state| {
                let pagination = state.read(cx);
                (pagination.status_text(), pagination.last_refresh_text())
            });

        h_flex()
            .w_full()
            .h(px(32.0))
            .px_3()
            .gap_2()
            .items_center()
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .when_some(self.table_name.as_ref(), |this, name| {
                this.child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child(name.clone()),
                )
            })
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("{} rows", self.row_count)),
            )
            .child(div().flex_1())
            .when_some(pagination_info, |this, (status, refresh)| {
                this.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(status),
                )
                .child(div().h(px(12.0)).w(px(1.0)).bg(theme.border))
                .child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(refresh),
                )
            })
    }

    pub fn render_pagination_footer(
        &self,
        state: &Entity<PaginationState>,
        window: &mut Window,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        render_pagination_controls(state, window, cx)
    }
}

use gpui::prelude::FluentBuilder; // keep in scope for render impl file usage

impl Render for TableViewerPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let footer_border_color = theme.border;
        let footer_bg_color = theme.tab_bar;
        let footer_text_color = theme.muted_foreground;

        h_flex()
            .id("table-viewer")
            .key_context("TableViewerPanel")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &CancelCellEditing, _window, cx| {
                this.cancel_cell_editing(cx);
            }))
            .on_action(cx.listener(|this, _: &CommitChanges, _window, cx| {
                this.emit_commit_changes(cx);
            }))
            .on_action(cx.listener(|this, _: &DeleteSelectedRows, _window, cx| {
                this.emit_delete_rows(cx);
            }))
            .on_action(cx.listener(|this, _: &UndoEdit, _window, cx| {
                this.undo_edit(cx);
            }))
            .on_action(cx.listener(|this, _: &RedoEdit, _window, cx| {
                this.redo_edit(cx);
            }))
            .on_action(cx.listener(|this, _: &ToggleSearch, window, cx| {
                this.toggle_search(window, cx);
            }))
            .on_action(cx.listener(|this, _: &CloseSearch, _window, cx| {
                this.close_search(cx);
            }))
            .on_action(cx.listener(|this, _: &ToggleReplace, window, cx| {
                this.toggle_replace(window, cx);
            }))
            .on_action(cx.listener(|this, _: &CopySelection, _window, cx| {
                this.copy_selection(cx);
            }))
            .on_action(cx.listener(|this, _: &PasteClipboard, _window, cx| {
                this.paste_clipboard(cx);
            }))
            .on_action(cx.listener(|this, _: &OpenRowEditor, _window, cx| {
                this.emit_open_row_editor(cx);
            }))
            .size_full()
            .bg(theme.background)
            .when_some(
                self.column_visibility_state
                    .as_ref()
                    .filter(|_| self.column_visibility_shown),
                |this, col_vis_state| this.child(ColumnVisibilityPanel::new(col_vis_state)),
            )
            .child(v_flex().flex_1().h_full().overflow_hidden().map(|this| {
                if self.is_loading {
                    this.child(self.render_loading(cx))
                } else if let Some(table_state) = &self.table_state {
                    let mut content = this
                        .child(self.render_header(cx))
                        .child(self.render_toolbar(cx));

                    if self.search_visible {
                        content = content.child(self.render_search_bar(cx));
                    }

                    if let Some(filter_state) = self
                        .filter_panel_state
                        .as_ref()
                        .filter(|_| self.filter_expanded)
                    {
                        content = content.child(FilterPanel::new(filter_state));
                    }

                    content = content.child(
                        div()
                            .flex_1()
                            .w_full()
                            .overflow_hidden()
                            .child(Table::new(table_state).stripe(true)),
                    );

                    if let Some(stats) = self.render_selection_stats(cx) {
                        content = content.child(stats);
                    }

                    if let Some(pag_state) = self
                        .pagination_state
                        .as_ref()
                        .filter(|_| matches!(self.driver_category, DriverCategory::Relational))
                    {
                        content =
                            content.child(self.render_pagination_footer(pag_state, window, cx));
                    } else if matches!(self.driver_category, DriverCategory::KeyValue) {
                        // Show "X of Y keys" when filtered, or just "Y keys" when not
                        let (filtered_count, total_count) = self
                            .table_state
                            .as_ref()
                            .map(|ts| {
                                let delegate = ts.read(cx).delegate();
                                (delegate.get_search_match_count(), delegate.rows.len())
                            })
                            .unwrap_or((self.row_count, self.row_count));

                        let label = if filtered_count < total_count {
                            format!(
                                "{} of {} {}",
                                filtered_count,
                                total_count,
                                if total_count == 1 { "key" } else { "keys" }
                            )
                        } else {
                            format!(
                                "{} {}",
                                total_count,
                                if total_count == 1 { "key" } else { "keys" }
                            )
                        };

                        content = content.child(
                            h_flex()
                                .w_full()
                                .h(px(32.0))
                                .px_3()
                                .items_center()
                                .justify_end()
                                .border_t_1()
                                .border_color(footer_border_color)
                                .bg(footer_bg_color)
                                .child(div().text_xs().text_color(footer_text_color).child(label)),
                        );
                    }

                    content
                } else {
                    this.child(self.render_empty(cx))
                }
            }))
    }
}
