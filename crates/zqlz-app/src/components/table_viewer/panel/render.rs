use super::*;
use zqlz_ui::widgets::spinner::Spinner;

impl TableViewerPanel {
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

        if !self.auto_commit_mode {
            let has_pending_changes = self
                .table_state
                .as_ref()
                .map(|s| s.read(cx).delegate().has_pending_changes())
                .unwrap_or(false);
            if has_pending_changes && !self.transaction_panel_expanded {
                self.transaction_panel_expanded = true;
            }
        }

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
            .on_action(cx.listener(|this, _: &ToggleSearch, window, cx| {
                this.toggle_search(window, cx);
            }))
            .on_action(cx.listener(|this, _: &CloseSearch, _window, cx| {
                this.close_search(cx);
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
