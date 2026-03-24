use super::*;

const INLINE_PREVIEW_MAX_CHARS: usize = 120;
const INLINE_TOOLTIP_TEXT_THRESHOLD: usize = 20;
const PREVIEW_CACHE_WARM_LIMIT: usize = 2_000;

#[derive(Clone, Debug)]
pub(in crate::components::table_viewer) struct InlineCellPreview {
    pub text: String,
    pub show_tooltip: bool,
}

impl TableViewerDelegate {
    fn build_bounded_string_preview(text: &str) -> (String, bool, bool, bool) {
        let mut preview = String::with_capacity(INLINE_PREVIEW_MAX_CHARS + 1);
        let mut preview_chars = 0usize;
        let mut was_truncated = false;
        let mut had_newlines = false;
        let mut exceeds_tooltip_threshold = false;
        let mut previous_was_carriage_return = false;

        for character in text.chars() {
            if character == '\n' && previous_was_carriage_return {
                previous_was_carriage_return = false;
                continue;
            }

            let flattened_character = if matches!(character, '\n' | '\r') {
                had_newlines = true;
                previous_was_carriage_return = character == '\r';
                ' '
            } else {
                previous_was_carriage_return = false;
                character
            };

            if preview_chars < INLINE_PREVIEW_MAX_CHARS {
                preview.push(flattened_character);
                preview_chars += 1;
            } else {
                was_truncated = true;
            }

            if preview_chars > INLINE_TOOLTIP_TEXT_THRESHOLD {
                exceeds_tooltip_threshold = true;
            }

            if was_truncated {
                break;
            }
        }

        if was_truncated {
            preview.push('…');
        }

        (
            preview,
            was_truncated,
            had_newlines,
            exceeds_tooltip_threshold,
        )
    }

    fn build_inline_cell_preview(value: &Value) -> InlineCellPreview {
        match value {
            Value::String(text) => {
                let (preview_text, was_truncated, had_newlines, exceeds_tooltip_threshold) =
                    Self::build_bounded_string_preview(text);

                InlineCellPreview {
                    show_tooltip: was_truncated || had_newlines || exceeds_tooltip_threshold,
                    text: preview_text,
                }
            }
            Value::Json(json) => match json {
                serde_json::Value::Object(map) => InlineCellPreview {
                    text: format!("{{…}} ({} keys)", map.len()),
                    show_tooltip: true,
                },
                serde_json::Value::Array(items) => InlineCellPreview {
                    text: format!("[…] ({} items)", items.len()),
                    show_tooltip: true,
                },
                _ => {
                    let text = value.display_for_table();
                    InlineCellPreview {
                        show_tooltip: text.chars().count() > INLINE_TOOLTIP_TEXT_THRESHOLD,
                        text,
                    }
                }
            },
            Value::Array(items) => InlineCellPreview {
                text: format!("[…] ({} items)", items.len()),
                show_tooltip: true,
            },
            _ => {
                let text = value.display_for_table();
                InlineCellPreview {
                    show_tooltip: text.chars().count() > INLINE_TOOLTIP_TEXT_THRESHOLD,
                    text,
                }
            }
        }
    }

    pub(crate) fn clear_cell_preview_cache(&mut self) {
        self.cell_preview_cache.clear();
    }

    pub(crate) fn invalidate_cell_preview_for(&mut self, row: usize, data_col: usize) {
        self.cell_preview_cache.remove(&(row, data_col));
    }

    pub(super) fn inline_cell_preview_for_cell(
        &mut self,
        actual_row_ix: usize,
        data_col_ix: usize,
    ) -> InlineCellPreview {
        let key = (actual_row_ix, data_col_ix);
        if let Some(cached_preview) = self.cell_preview_cache.get(&key) {
            return cached_preview.clone();
        }

        let preview = self
            .rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .map(Self::build_inline_cell_preview)
            .unwrap_or_else(|| InlineCellPreview {
                text: String::new(),
                show_tooltip: false,
            });
        self.cell_preview_cache.insert(key, preview.clone());
        preview
    }

    pub(super) fn warm_visible_preview_cache(&mut self) {
        let Some(visible_rows) = self.visible_rows_range.clone() else {
            return;
        };
        let Some(visible_columns) = self.visible_columns_range.clone() else {
            return;
        };

        let display_row_count = if self.is_filtering {
            self.filtered_row_indices.len()
        } else {
            self.rows.len()
        };

        if display_row_count == 0 {
            return;
        }

        let row_start = visible_rows.start.min(display_row_count);
        let row_end = visible_rows.end.min(display_row_count);
        if row_start >= row_end {
            return;
        }

        let col_start = visible_columns.start.max(1).min(self.columns.len());
        let col_end = visible_columns.end.min(self.columns.len());
        if col_start >= col_end {
            return;
        }

        let mut warmed = 0usize;
        for display_row in row_start..row_end {
            let actual_row = self.get_actual_row_index(display_row);
            for col_ix in col_start..col_end {
                if warmed >= PREVIEW_CACHE_WARM_LIMIT {
                    return;
                }

                let data_col = col_ix - 1;
                let key = (actual_row, data_col);
                if self.cell_preview_cache.contains_key(&key) {
                    continue;
                }

                let Some(value) = self.rows.get(actual_row).and_then(|row| row.get(data_col))
                else {
                    continue;
                };
                let preview = Self::build_inline_cell_preview(value);
                self.cell_preview_cache.insert(key, preview);
                warmed = warmed.saturating_add(1);
            }
        }
    }

    pub(super) fn tooltip_text_for_cell_value(value: &Value) -> String {
        match value {
            Value::Bytes(_) => value.display_for_table(),
            _ => value.display_for_editor(),
        }
    }

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

    #[allow(dead_code)]
    pub fn render_cell_simple(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
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
        let display_text = self
            .inline_cell_preview_for_cell(actual_row_ix, data_col_ix)
            .text;
        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .overflow_hidden()
            .text_ellipsis()
            .child(display_text)
            .into_any_element()
    }

    pub fn cell_text(&self, row_ix: usize, col_ix: usize, _cx: &App) -> String {
        let actual_row_ix = self.get_actual_row_index(row_ix);

        if col_ix == 0 {
            return (self.row_offset + actual_row_ix + 1).to_string();
        }

        let data_col_ix = col_ix - 1;

        if let Some(change) = self
            .pending_changes
            .get_cell_change(actual_row_ix, data_col_ix)
        {
            return change.new_value.display_for_table();
        }

        self.rows
            .get(actual_row_ix)
            .and_then(|row| row.get(data_col_ix))
            .map(|v| v.display_for_table())
            .unwrap_or_default()
    }
}
