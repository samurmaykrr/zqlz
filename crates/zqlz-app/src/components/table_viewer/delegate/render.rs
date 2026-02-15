use super::*;

impl TableViewerDelegate {
    pub fn render_boolean_checkbox(
        &self,
        value: Option<bool>,
        is_deleted: bool,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let (border_color, bg_color, icon) = match value {
            Some(true) => (
                if is_deleted {
                    theme.primary.opacity(0.5)
                } else {
                    theme.primary
                },
                if is_deleted {
                    theme.primary.opacity(0.3)
                } else {
                    theme.primary
                },
                Some(IconName::Check),
            ),
            Some(false) => (
                if is_deleted {
                    theme.input.opacity(0.5)
                } else {
                    theme.input
                },
                theme.background,
                None,
            ),
            None => (
                if is_deleted {
                    theme.muted_foreground.opacity(0.5)
                } else {
                    theme.muted_foreground
                },
                if is_deleted {
                    theme.muted.opacity(0.3)
                } else {
                    theme.muted
                },
                Some(IconName::Minus),
            ),
        };

        let icon_color = match value {
            Some(true) => theme.primary_foreground,
            Some(false) => theme.foreground,
            None => theme.muted_foreground,
        };

        div()
            .size_4()
            .flex()
            .items_center()
            .justify_center()
            .border_1()
            .border_color(border_color)
            .rounded(px(4.))
            .bg(bg_color)
            .when(is_deleted, |this| this.opacity(0.5))
            .when_some(icon, |this, icon_name| {
                this.child(Icon::new(icon_name).size_3().text_color(icon_color))
            })
    }

    pub fn render_cell_simple(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        window: &mut Window,
        cx: &mut Context<TableState<TableViewerDelegate>>,
    ) -> impl IntoElement {
        let actual_row_ix = self.get_actual_row_index(row_ix);

        if col_ix == 0 {
            let theme = cx.theme();
            return div()
                .h_full()
                .flex()
                .items_center()
                .justify_center()
                .px_2()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child((self.row_offset + actual_row_ix + 1).to_string())
                .into_any_element();
        }

        let data_col_ix = col_ix - 1;
        let value = self
            .rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .cloned()
            .unwrap_or_default();
        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .overflow_hidden()
            .text_ellipsis()
            .child(value)
            .into_any_element()
    }

    pub fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        let actual_row_ix = self.get_actual_row_index(row_ix);

        if col_ix == 0 {
            return (self.row_offset + actual_row_ix + 1).to_string();
        }

        let data_col_ix = col_ix - 1;

        if let Some(change) = self.pending_changes.get_cell_change(actual_row_ix, col_ix) {
            return change.new_value.clone();
        }

        self.rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .cloned()
            .unwrap_or_default()
    }
}
