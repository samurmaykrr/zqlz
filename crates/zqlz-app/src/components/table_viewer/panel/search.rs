use super::*;

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

            self.row_count = table_state.read_with(cx, |table, _cx| {
                table.delegate().get_search_match_count()
            });
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

            self.row_count = table_state.read_with(cx, |table, _cx| {
                table.delegate().rows.len()
            });
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
                    |this, input, event: &InputEvent, window, cx| {
                        match event {
                            InputEvent::Change => {
                                let text = input.read(cx).text().to_string();
                                this.on_search_changed(text, cx);
                            }
                            InputEvent::PressEnter { .. } => {}
                            _ => {}
                        }
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

            if had_search {
                if self.uses_client_side_search() {
                    self.clear_client_side_search(cx);
                } else if let (Some(connection_id), Some(table_name)) = (self.connection_id, self.table_name.clone()) {
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

        if had_search {
            if self.uses_client_side_search() {
                self.clear_client_side_search(cx);
            } else if let (Some(connection_id), Some(table_name)) = (self.connection_id, self.table_name.clone()) {
                self.apply_filters(connection_id, table_name, cx);
            }
        }

        cx.notify();
    }

    pub fn on_search_changed(&mut self, text: String, cx: &mut Context<Self>) {
        self.search_text = text;
        self._search_debounce_task = None;

        if self.uses_client_side_search() {
            // Client-side: filter immediately with a short debounce
            self._search_debounce_task = Some(cx.spawn(async move |this, cx| {
                cx.background_spawn(async {
                    smol::Timer::after(std::time::Duration::from_millis(100)).await;
                })
                .await;

                _ = this.update(cx, |this, cx| {
                    this.apply_client_side_search(cx);
                });
            }));
        } else {
            // Server-side: debounce longer to avoid excessive queries
            let connection_id = self.connection_id;
            let table_name = self.table_name.clone();

            self._search_debounce_task = Some(cx.spawn(async move |this, cx| {
                cx.background_spawn(async {
                    smol::Timer::after(std::time::Duration::from_millis(300)).await;
                })
                .await;

                _ = this.update(cx, |this, cx| {
                    if let (Some(connection_id), Some(table_name)) = (connection_id, table_name) {
                        this.apply_filters(connection_id, table_name, cx);
                    }
                });
            }));
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

        h_flex()
            .w_full()
            .h(px(36.0))
            .px_2()
            .gap_2()
            .items_center()
            .bg(theme.tab_bar)
            .border_b_1()
            .border_color(theme.border)
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
                Button::new("close-search")
                    .icon(ZqlzIcon::X)
                    .ghost()
                    .xsmall()
                    .tooltip("Close Search")
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.close_search(cx);
                    })),
            )
    }
}
