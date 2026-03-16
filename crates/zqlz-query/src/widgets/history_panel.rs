//! Query History Panel
//!
//! Displays a list of executed queries with metadata (timestamp, duration, status)
//! and allows users to click entries to load them into the query editor.

use std::ops::Range;

use chrono::{DateTime, Utc};
use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Icon, Sizable, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex,
};

use crate::history::QueryHistoryEntry;

/// Events emitted by the QueryHistoryPanel
#[derive(Clone, Debug)]
pub enum QueryHistoryPanelEvent {
    /// User clicked on a history entry to load it into the editor
    OpenQuery { sql: String },
    /// User requested to clear all history
    ClearHistory,
}

/// Query History Panel component
pub struct QueryHistoryPanel {
    focus_handle: FocusHandle,
    /// History entries to display (most recent first)
    entries: Vec<QueryHistoryEntry>,
    /// Current search query
    search_query: String,
    /// Search input for filtering entries
    search_input: Entity<InputState>,
    /// Scroll handle for the virtualized history list
    scroll_handle: UniformListScrollHandle,
}

impl QueryHistoryPanel {
    /// Create a new query history panel
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search history..."));
        let scroll_handle = UniformListScrollHandle::new();

        cx.subscribe(&search_input, |this, input, event: &InputEvent, cx| {
            if let InputEvent::Change = event {
                this.search_query = input.read(cx).value().to_string();
                this.scroll_handle = UniformListScrollHandle::new();
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

    /// Update the displayed history entries
    pub fn update_entries(&mut self, entries: Vec<QueryHistoryEntry>, cx: &mut Context<Self>) {
        self.entries = entries;
        self.scroll_handle = UniformListScrollHandle::new();
        cx.notify();
    }

    /// Clear all history entries
    pub fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.entries.clear();
        self.search_query.clear();
        self.search_input.update(cx, |input, cx| {
            input.set_value(String::new(), window, cx);
        });
        self.scroll_handle = UniformListScrollHandle::new();
        cx.emit(QueryHistoryPanelEvent::ClearHistory);
        cx.notify();
    }

    /// Replace the search query and keep the input in sync.
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
        self.scroll_handle = UniformListScrollHandle::new();
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

    /// Format timestamp for display
    fn format_timestamp(timestamp: &DateTime<Utc>) -> String {
        let local = timestamp.with_timezone(&chrono::Local);
        local.format("%H:%M:%S").to_string()
    }

    /// Format duration for display
    fn format_duration(duration_ms: u64) -> String {
        if duration_ms < 1000 {
            format!("{}ms", duration_ms)
        } else if duration_ms < 60000 {
            format!("{:.1}s", duration_ms as f64 / 1000.0)
        } else {
            format!("{:.1}m", duration_ms as f64 / 60000.0)
        }
    }

    /// Render a single history entry
    fn render_entry(
        &self,
        entry: &QueryHistoryEntry,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let sql_clone = entry.sql.clone();

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
            .px_2()
            .py_1p5()
            .border_b_1()
            .border_color(theme.border)
            .hover(|this| this.bg(theme.border.opacity(0.3)))
            .cursor_pointer()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |_this, _event, _window, cx| {
                    cx.emit(QueryHistoryPanelEvent::OpenQuery {
                        sql: sql_clone.clone(),
                    });
                }),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        // SQL query text (truncated)
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .font_family(theme.mono_font_family.clone())
                            .child(entry.sql_preview(80)),
                    )
                    .child(
                        // Metadata row
                        h_flex()
                            .gap_2()
                            .items_center()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(
                                // Status icon
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .text_color(status_color)
                                    .child(Icon::new(status_icon).size_3())
                                    .child(if entry.success {
                                        if let Some(count) = entry.row_count {
                                            format!("{} rows", count)
                                        } else {
                                            "Success".to_string()
                                        }
                                    } else {
                                        "Failed".to_string()
                                    }),
                            )
                            .child(
                                // Duration
                                div().child(format!(
                                    "• {}",
                                    Self::format_duration(entry.duration_ms)
                                )),
                            )
                            .child(
                                // Timestamp
                                div().child(format!(
                                    "• {}",
                                    Self::format_timestamp(&entry.executed_at)
                                )),
                            ),
                    ),
            )
            .children(entry.error_preview(100).map(|error_message| {
                // Error message
                div()
                    .px_2()
                    .pb_1()
                    .text_xs()
                    .text_color(theme.red)
                    .font_family(theme.mono_font_family.clone())
                    .child(error_message)
            }))
            .into_any_element()
    }
}

impl Render for QueryHistoryPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let filtered_entries = self.filtered_entries();

        let theme = cx.theme();
        let entries_count = self.entries.len();
        let filtered_count = filtered_entries.len();
        let is_empty = filtered_count == 0;
        let has_search = !self.search_query.trim().is_empty();

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                // Toolbar
                v_flex()
                    .w_full()
                    .px_2()
                    .py_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(theme.border)
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
                                        .rounded_md()
                                        .bg(theme.secondary)
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
                // Empty state
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
                // History list
                uniform_list(
                    "query-history-entries",
                    filtered_count,
                    cx.processor(
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
                                    div().id(index).child(state.render_entry(entry, window, cx)),
                                );
                            }

                            items
                        },
                    ),
                )
                .flex_grow()
                .size_full()
                .track_scroll(&self.scroll_handle)
                .with_sizing_behavior(ListSizingBehavior::Auto)
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
