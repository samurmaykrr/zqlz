use std::{ops::Range, rc::Rc};

use chrono::{DateTime, Utc};
use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Icon, Sizable, VirtualListScrollHandle, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex, v_virtual_list,
};

use crate::history::QueryHistoryEntry;

#[derive(Clone, Debug)]
pub enum QueryHistoryPanelEvent {
    OpenQuery { sql: String },
    ClearHistory,
}

pub struct QueryHistoryPanel {
    focus_handle: FocusHandle,
    entries: Vec<QueryHistoryEntry>,
    search_query: String,
    search_input: Entity<InputState>,
    scroll_handle: VirtualListScrollHandle,
}

impl QueryHistoryPanel {
    const SQL_PREVIEW_LENGTH: usize = 96;
    const ENTRY_HEIGHT: f32 = 84.0;

    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search history..."));
        let scroll_handle = VirtualListScrollHandle::new();

        cx.subscribe(&search_input, |this, input, event: &InputEvent, cx| {
            if let InputEvent::Change = event {
                this.search_query = input.read(cx).value().to_string();
                this.scroll_handle = VirtualListScrollHandle::new();
                cx.notify();
            }
        })
        .detach();

        Self {
            focus_handle: cx.focus_handle(),
            entries: Vec::new(),
            search_query: String::new(),
            search_input,
            scroll_handle,
        }
    }

    pub fn update_entries(&mut self, entries: Vec<QueryHistoryEntry>, cx: &mut Context<Self>) {
        self.entries = entries;
        self.scroll_handle = VirtualListScrollHandle::new();
        cx.notify();
    }

    pub fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.entries.clear();
        self.search_query.clear();
        self.search_input.update(cx, |input, cx| {
            input.set_value(String::new(), window, cx);
        });
        self.scroll_handle = VirtualListScrollHandle::new();
        cx.emit(QueryHistoryPanelEvent::ClearHistory);
        cx.notify();
    }

    pub fn set_search_query(
        &mut self,
        query: impl Into<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let query = query.into();
        self.search_query = query.clone();
        self.search_input.update(cx, |input, cx| {
            input.set_value(query, window, cx);
        });
        self.scroll_handle = VirtualListScrollHandle::new();
        cx.notify();
    }

    fn filtered_entries(&self) -> Vec<&QueryHistoryEntry> {
        if self.search_query.trim().is_empty() {
            self.entries.iter().collect()
        } else {
            self.entries
                .iter()
                .filter(|entry| entry.matches_search(&self.search_query))
                .collect()
        }
    }

    fn format_timestamp(timestamp: &DateTime<Utc>) -> String {
        let local = timestamp.with_timezone(&chrono::Local);
        local.format("%H:%M:%S").to_string()
    }

    fn entry_sizes(entries: &[&QueryHistoryEntry]) -> Rc<Vec<Size<Pixels>>> {
        Rc::new(
            entries
                .iter()
                .map(|_| size(px(0.), Self::entry_height()))
                .collect(),
        )
    }

    fn entry_height() -> Pixels {
        px(Self::ENTRY_HEIGHT)
    }

    fn render_entry(
        &self,
        entry: &QueryHistoryEntry,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let sql_for_card_click = entry.sql.clone();
        let sql_for_details_click = entry.sql.clone();
        let sql_preview = entry.sql_preview(Self::SQL_PREVIEW_LENGTH);

        let status_color = if entry.success {
            theme.green
        } else {
            theme.red
        };

        let status_icon = if entry.success {
            ZqlzIcon::CheckCircle
        } else {
            ZqlzIcon::XCircle
        };

        div()
            .w_full()
            .h(Self::entry_height())
            .child(
                div()
                    .w_full()
                    .h_full()
                    .border_b_1()
                    .border_color(theme.border)
                    .cursor_pointer()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |_this, _event, _window, cx| {
                            cx.emit(QueryHistoryPanelEvent::OpenQuery {
                                sql: sql_for_card_click.clone(),
                            });
                        }),
                    )
                    .child(
                        v_flex()
                            .size_full()
                            .gap_1()
                            .p_2()
                            .child(
                                h_flex()
                                    .w_full()
                                    .items_center()
                                    .justify_between()
                                    .gap_2()
                                    .child(
                                        div()
                                            .flex_1()
                                            .overflow_hidden()
                                            .whitespace_nowrap()
                                            .text_ellipsis()
                                            .text_sm()
                                            .text_color(theme.foreground)
                                            .font_family(theme.mono_font_family.clone())
                                            .child(sql_preview),
                                    )
                                    .child(
                                        div()
                                            .flex_shrink_0()
                                            .text_color(status_color)
                                            .child(Icon::new(status_icon).size_4()),
                                    ),
                            )
                            .child(
                                h_flex()
                                    .w_full()
                                    .items_center()
                                    .justify_between()
                                    .gap_2()
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_w(px(0.))
                                            .overflow_hidden()
                                            .whitespace_nowrap()
                                            .text_ellipsis()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(if entry.success {
                                                Self::format_timestamp(&entry.executed_at)
                                            } else {
                                                format!(
                                                    "Failed • {}",
                                                    Self::format_timestamp(&entry.executed_at)
                                                )
                                            }),
                                    )
                                    .when(entry.error.is_some(), |this| {
                                        this.child(
                                            Button::new(format!("history-open-{}", entry.id))
                                                .ghost()
                                                .xsmall()
                                                .label("Open")
                                                .on_click(cx.listener(
                                                    move |_this, _event, _window, cx| {
                                                        cx.emit(
                                                            QueryHistoryPanelEvent::OpenQuery {
                                                                sql: sql_for_details_click.clone(),
                                                            },
                                                        );
                                                    },
                                                )),
                                        )
                                    }),
                            ),
                    ),
            )
            .into_any_element()
    }
}

impl Render for QueryHistoryPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let filtered_entries = self.filtered_entries();
        let entry_sizes = Self::entry_sizes(&filtered_entries);

        let theme = cx.theme();
        let entries_count = self.entries.len();
        let filtered_count = filtered_entries.len();
        let is_empty = filtered_count == 0;
        let has_search = !self.search_query.trim().is_empty();

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                v_flex()
                    .w_full()
                    .px_2()
                    .py_2()
                    .gap_2()
                    .child(
                        h_flex()
                            .w_full()
                            .justify_between()
                            .items_center()
                            .child(
                                v_flex()
                                    .gap_0p5()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.foreground)
                                            .child("Query History"),
                                    )
                                    .child(
                                        div().text_xs().text_color(theme.muted_foreground).child(
                                            if has_search {
                                                format!(
                                                    "{filtered_count} of {entries_count} queries"
                                                )
                                            } else {
                                                format!("{entries_count} saved queries")
                                            },
                                        ),
                                    ),
                            )
                            .child(
                                Button::new("clear-history")
                                    .danger()
                                    .xsmall()
                                    .label("Clear")
                                    .icon(ZqlzIcon::Trash)
                                    .disabled(entries_count == 0)
                                    .on_click(cx.listener(|this, _event, window, cx| {
                                        this.clear(window, cx);
                                    })),
                            ),
                    )
                    .child(
                        h_flex()
                            .w_full()
                            .gap_2()
                            .items_center()
                            .child(
                                div().flex_1().child(
                                    Input::new(&self.search_input)
                                        .small()
                                        .prefix(Icon::new(ZqlzIcon::MagnifyingGlass).size_3()),
                                ),
                            )
                            .when(has_search, |this| {
                                this.child(
                                    div()
                                        .min_w(px(88.))
                                        .px_2()
                                        .py_1()
                                        .bg(theme.secondary)
                                        .border_1()
                                        .border_color(theme.border.opacity(0.6))
                                        .text_xs()
                                        .text_color(theme.secondary_foreground)
                                        .child(format!(
                                            "{filtered_count} match{}",
                                            if filtered_count == 1 { "" } else { "es" }
                                        )),
                                )
                            }),
                    ),
            )
            .child(if is_empty {
                v_flex()
                    .flex_1()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .child(
                        h_flex()
                            .text_color(theme.muted_foreground.opacity(0.5))
                            .child(Icon::new(ZqlzIcon::Clock).size_12()),
                    )
                    .child(div().text_sm().text_color(theme.muted_foreground).child(
                        if has_search {
                            "No history matches your search"
                        } else {
                            "No query history yet"
                        },
                    ))
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground.opacity(0.7))
                            .child(if has_search {
                                "Try a different search term"
                            } else {
                                "Executed queries appear here automatically"
                            }),
                    )
                    .into_any_element()
            } else {
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(
                        v_virtual_list(
                            cx.entity(),
                            "query-history-entries",
                            entry_sizes,
                            move |state: &mut QueryHistoryPanel,
                                  visible_range: Range<usize>,
                                  window,
                                  cx| {
                                let filtered_entries = state.filtered_entries();
                                let mut items = Vec::with_capacity(visible_range.len());

                                for index in visible_range {
                                    let Some(entry) = filtered_entries.get(index) else {
                                        continue;
                                    };

                                    items.push(
                                        div()
                                            .id(index)
                                            .child(state.render_entry(entry, window, cx)),
                                    );
                                }

                                items
                            },
                        )
                        .with_sizing_behavior(ListSizingBehavior::Auto)
                        .track_scroll(&self.scroll_handle),
                    )
                    .into_any_element()
            })
    }
}

impl Focusable for QueryHistoryPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for QueryHistoryPanel {}
impl EventEmitter<QueryHistoryPanelEvent> for QueryHistoryPanel {}

impl Panel for QueryHistoryPanel {
    fn panel_name(&self) -> &'static str {
        "QueryHistoryPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Query History"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
