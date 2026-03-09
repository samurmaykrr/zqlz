use super::*;
use crate::components::table_viewer::delegate::{UndoCellEdit, UndoEntry};
use zqlz_ui::widgets::IconName;

impl TableViewerPanel {
    /// Whether this viewer uses client-side filtering (data already in memory)
    fn uses_client_side_search(&self) -> bool {
        matches!(
            self.driver_category,
            DriverCategory::KeyValue | DriverCategory::Document
        )
    }

    /// Apply client-side search filter directly on the delegate's in-memory rows.
    /// Used for KeyValue/Document drivers where all data is loaded upfront.
    fn apply_client_side_search(&mut self, cx: &mut Context<Self>) {
        let filter = if self.search_text.is_empty() {
            None
        } else {
            Some(self.search_text.to_lowercase())
        };

        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().set_search_filter(filter);
                table.refresh(cx);
            });

            self.row_count =
                table_state.read_with(cx, |table, _cx| table.delegate().get_search_match_count());
        }

        cx.notify();
    }

    /// Clear client-side search filter, restoring all rows.
    fn clear_client_side_search(&mut self, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().set_search_filter(None);
                table.refresh(cx);
            });

            self.row_count = table_state.read_with(cx, |table, _cx| table.delegate().rows.len());
        }

        cx.notify();
    }

    pub fn toggle_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.search_visible = !self.search_visible;

        if self.search_visible {
            if self.search_input.is_none() {
                let input =
                    cx.new(|cx| InputState::new(window, cx).placeholder("Search in table..."));

                cx.subscribe_in(
                    &input,
                    window,
                    |this, input, event: &InputEvent, _window, cx| match event {
                        InputEvent::Change => {
                            let text = input.read(cx).text().to_string();
                            this.on_search_changed(text, cx);
                        }
                        InputEvent::PressEnter { .. } => {}
                        _ => {}
                    },
                )
                .detach();

                self.search_input = Some(input.clone());

                input.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            } else if let Some(input) = &self.search_input {
                input.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
            }
        } else {
            let had_search = !self.search_text.is_empty();
            self.search_text.clear();
            self.search_input = None;
            self._search_debounce_task = None;
            self.replace_input = None;
            self.replace_visible = false;

            if had_search {
                if self.uses_client_side_search() {
                    self.clear_client_side_search(cx);
                } else if let (Some(connection_id), Some(table_name)) =
                    (self.connection_id, self.table_name.clone())
                {
                    self.apply_filters(connection_id, table_name, cx);
                }
            }
        }

        cx.notify();
    }

    pub fn close_search(&mut self, cx: &mut Context<Self>) {
        self.search_visible = false;
        let had_search = !self.search_text.is_empty();
        self.search_text.clear();
        self.search_input = None;
        self._search_debounce_task = None;
        self.replace_input = None;
        self.replace_visible = false;

        if had_search {
            if self.uses_client_side_search() {
                self.clear_client_side_search(cx);
            } else if let (Some(connection_id), Some(table_name)) =
                (self.connection_id, self.table_name.clone())
            {
                self.apply_filters(connection_id, table_name, cx);
            }
        }

        cx.notify();
    }

    pub fn toggle_replace(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.search_visible {
            self.toggle_search(window, cx);
        }

        self.replace_visible = !self.replace_visible;

        if self.replace_visible && self.replace_input.is_none() {
            let input = cx.new(|cx| InputState::new(window, cx).placeholder("Replace with..."));
            self.replace_input = Some(input);
        }

        if !self.replace_visible {
            self.replace_input = None;
        }

        cx.notify();
    }

    pub fn on_search_changed(&mut self, text: String, cx: &mut Context<Self>) {
        self.search_text = text;
        self._search_debounce_task = None;

        if self.uses_client_side_search() {
            self._search_debounce_task = Some(cx.spawn(async move |this, cx| {
                cx.background_spawn(async {
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                })
                .await;

                if let Err(e) = this.update(cx, |this, cx| {
                    this.apply_client_side_search(cx);
                }) {
                    tracing::error!("Failed to apply client-side search: {:?}", e);
                }
            }));
        } else {
            let connection_id = self.connection_id;
            let table_name = self.table_name.clone();

            self._search_debounce_task = Some(cx.spawn(async move |this, cx| {
                cx.background_spawn(async {
                    smol::Timer::after(std::time::Duration::from_millis(300)).await;
                })
                .await;

                if let Err(e) = this.update(cx, |this, cx| {
                    if let (Some(connection_id), Some(table_name)) = (connection_id, table_name) {
                        this.apply_filters(connection_id, table_name, cx);
                    }
                }) {
                    tracing::error!("Failed to apply server-side search filters: {:?}", e);
                }
            }));
        }

        cx.notify();
    }

    /// Replace the next matching cell value with the replacement text.
    /// Only works for client-side data (all data in memory).
    pub fn replace_next(&mut self, cx: &mut Context<Self>) {
        let search_text = self.search_text.clone();
        if search_text.is_empty() {
            return;
        }

        let replace_text = self
            .replace_input
            .as_ref()
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |table, cx| {
            let delegate = table.delegate_mut();
            let search_lower = search_text.to_lowercase();
            let row_count = if delegate.is_filtering {
                delegate.filtered_row_indices.len()
            } else {
                delegate.rows.len()
            };
            let col_count = delegate.column_meta.len();

            for display_row in 0..row_count {
                let actual_row = delegate.get_actual_row_index(display_row);
                for data_col in 0..col_count {
                    let cell_value = delegate
                        .rows
                        .get(actual_row)
                        .and_then(|r| r.get(data_col))
                        .cloned()
                        .unwrap_or_default();

                    let cell_str = cell_value.display_for_table();
                    if cell_str.to_lowercase().contains(&search_lower) {
                        let new_str =
                            case_preserving_replace(&cell_str, &search_text, &replace_text);
                        if new_str == cell_str {
                            continue;
                        }

                        let col = data_col + 1;
                        match delegate.prepare_cell_value_update(actual_row, data_col, &new_str) {
                            Ok(Some((new_value, original_value))) => {
                                delegate.commit_cell_value(
                                    actual_row,
                                    col,
                                    data_col,
                                    new_value,
                                    original_value,
                                    cx,
                                );
                                cx.notify();
                                return;
                            }
                            Ok(None) => continue,
                            Err(message) => {
                                tracing::warn!("Cell validation failed: {}", message);
                                delegate.emit_validation_failed(message, cx);
                                return;
                            }
                        }
                    }
                }
            }
        });

        cx.notify();
    }

    /// Replace all matching cell values with the replacement text.
    /// Only works for client-side data (all data in memory).
    pub fn replace_all(&mut self, cx: &mut Context<Self>) {
        let search_text = self.search_text.clone();
        if search_text.is_empty() {
            return;
        }

        let replace_text = self
            .replace_input
            .as_ref()
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();

        let Some(table_state) = &self.table_state else {
            return;
        };

        table_state.update(cx, |table, cx| {
            let delegate = table.delegate_mut();
            let search_lower = search_text.to_lowercase();
            let total_rows = delegate.rows.len();
            let row_count = if delegate.is_filtering {
                delegate.filtered_row_indices.len()
            } else {
                total_rows
            };
            let col_count = delegate.column_meta.len();

            let mut undo_edits = Vec::new();

            for display_row in 0..row_count {
                let actual_row = delegate.get_actual_row_index(display_row);
                for data_col in 0..col_count {
                    let cell_value = delegate
                        .rows
                        .get(actual_row)
                        .and_then(|r| r.get(data_col))
                        .cloned()
                        .unwrap_or_default();

                    let cell_str = cell_value.display_for_table();
                    if cell_str.to_lowercase().contains(&search_lower) {
                        let new_str =
                            case_preserving_replace(&cell_str, &search_text, &replace_text);
                        if new_str == cell_str {
                            continue;
                        }

                        match delegate.prepare_cell_value_update(actual_row, data_col, &new_str) {
                            Ok(Some((new_value, original_value))) => {
                                undo_edits.push(UndoCellEdit {
                                    row: actual_row,
                                    data_col,
                                    old_value: original_value,
                                    new_value,
                                });
                            }
                            Ok(None) => {}
                            Err(message) => {
                                tracing::warn!("Cell validation failed: {}", message);
                                delegate.emit_validation_failed(message, cx);
                                return;
                            }
                        }
                    }
                }
            }

            if !undo_edits.is_empty() {
                for edit in &undo_edits {
                    delegate.apply_cell_value_change(
                        edit.row,
                        edit.data_col,
                        edit.new_value.clone(),
                        edit.old_value.clone(),
                        false,
                        cx,
                    );
                }

                delegate.push_undo(UndoEntry { edits: undo_edits });
            }

            cx.notify();
        });

        // Re-apply client-side search highlighting after replace-all
        if self.uses_client_side_search() {
            self.apply_client_side_search(cx);
        }

        cx.notify();
    }

    pub fn render_search_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_searching = !self.search_text.is_empty();

        let result_count = if self.uses_client_side_search() && is_searching {
            self.table_state
                .as_ref()
                .map(|ts| ts.read(cx).delegate().get_search_match_count())
                .unwrap_or(0)
        } else {
            self.row_count
        };

        let bar_height = if self.replace_visible {
            px(68.0)
        } else {
            px(36.0)
        };

        v_flex()
            .w_full()
            .h(bar_height)
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
            .child(
                h_flex()
                    .w_full()
                    .h(px(36.0))
                    .px_2()
                    .gap_2()
                    .items_center()
                    .when_some(self.search_input.as_ref(), |this, input| {
                        this.child(
                            div()
                                .w(px(300.0))
                                .child(Input::new(input).small().cleanable(true)),
                        )
                    })
                    .when(is_searching, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .when(self.is_loading, |this| this.child("Searching..."))
                                .when(!self.is_loading, |this| {
                                    this.child(format!("{} results", result_count))
                                }),
                        )
                    })
                    .child(div().flex_1())
                    .child(
                        Button::new("toggle-replace")
                            .icon(IconName::Replace)
                            .ghost()
                            .xsmall()
                            .tooltip("Toggle Replace (Cmd+H)")
                            .selected(self.replace_visible)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.toggle_replace(window, cx);
                            })),
                    )
                    .child(
                        Button::new("close-search")
                            .icon(ZqlzIcon::X)
                            .ghost()
                            .xsmall()
                            .tooltip("Close Search")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close_search(cx);
                            })),
                    ),
            )
            .when(self.replace_visible, |this| {
                this.child(
                    h_flex()
                        .w_full()
                        .h(px(32.0))
                        .px_2()
                        .gap_2()
                        .items_center()
                        .when_some(self.replace_input.as_ref(), |this, input| {
                            this.child(div().w(px(300.0)).child(Input::new(input).small()))
                        })
                        .child(
                            Button::new("replace-next")
                                .label("Replace")
                                .ghost()
                                .xsmall()
                                .disabled(!is_searching)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.replace_next(cx);
                                })),
                        )
                        .child(
                            Button::new("replace-all")
                                .label("Replace All")
                                .ghost()
                                .xsmall()
                                .disabled(!is_searching)
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.replace_all(cx);
                                })),
                        ),
                )
            })
    }
}

/// Case-insensitive find-and-replace within a string. All occurrences of `search`
/// are replaced with `replacement` regardless of case.
fn case_preserving_replace(haystack: &str, search: &str, replacement: &str) -> String {
    if search.is_empty() {
        return haystack.to_string();
    }
    let search_lower = search.to_lowercase();
    let haystack_lower = haystack.to_lowercase();
    let mut result = String::with_capacity(haystack.len());
    let mut last_end = 0;

    for (start, _) in haystack_lower.match_indices(&search_lower) {
        result.push_str(&haystack[last_end..start]);
        result.push_str(replacement);
        last_end = start + search.len();
    }
    result.push_str(&haystack[last_end..]);
    result
}
