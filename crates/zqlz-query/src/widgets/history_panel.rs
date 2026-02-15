//! Query History Panel
//!
//! Displays a list of executed queries with metadata (timestamp, duration, status)
//! and allows users to click entries to load them into the query editor.

use chrono::{DateTime, Utc};
use gpui::*;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariant, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    scroll::ScrollableElement,
    v_flex, ActiveTheme, Disableable, Icon, Sizable, ZqlzIcon,
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
}

impl QueryHistoryPanel {
    /// Create a new query history panel
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            entries: Vec::new(),
        }
    }

    /// Update the displayed history entries
    pub fn update_entries(&mut self, entries: Vec<QueryHistoryEntry>, cx: &mut Context<Self>) {
        self.entries = entries;
        cx.notify();
    }

    /// Clear all history entries
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.entries.clear();
        cx.emit(QueryHistoryPanelEvent::ClearHistory);
        cx.notify();
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

    /// Truncate SQL for display
    fn truncate_sql(sql: &str, max_length: usize) -> String {
        let trimmed = sql.trim();
        if trimmed.len() <= max_length {
            trimmed.to_string()
        } else {
            format!("{}...", &trimmed[..max_length])
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
        let entry_clone = entry.clone();
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
                            .font_family("monospace")
                            .child(Self::truncate_sql(&entry_clone.sql, 80)),
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
                                    .child(if entry_clone.success {
                                        if let Some(count) = entry_clone.row_count {
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
                                    Self::format_duration(entry_clone.duration_ms)
                                )),
                            )
                            .child(
                                // Timestamp
                                div().child(format!(
                                    "• {}",
                                    Self::format_timestamp(&entry_clone.executed_at)
                                )),
                            ),
                    ),
            )
            .children(entry_clone.error.as_ref().map(|error_message| {
                // Error message
                div()
                    .px_2()
                    .pb_1()
                    .text_xs()
                    .text_color(theme.red)
                    .font_family("monospace")
                    .child(Self::truncate_sql(error_message, 100))
            }))
            .into_any_element()
    }
}

impl Render for QueryHistoryPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Render all entries first to avoid closure lifetime issues
        let entry_elements: Vec<_> = self
            .entries
            .iter()
            .map(|entry| self.render_entry(entry, window, cx))
            .collect();

        let theme = cx.theme();
        let entries_count = self.entries.len();
        let is_empty = entry_elements.is_empty();

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                // Toolbar
                h_flex()
                    .w_full()
                    .px_2()
                    .py_1p5()
                    .gap_2()
                    .items_center()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .flex_1()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child(format!("History ({} queries)", entries_count)),
                    )
                    .child(
                        Button::new("clear-history")
                            .with_variant(ButtonVariant::Ghost)
                            .xsmall()
                            .label("Clear")
                            .icon(ZqlzIcon::Trash)
                            .disabled(is_empty)
                            .on_click(cx.listener(|this, _event, _window, cx| {
                                this.clear(cx);
                            })),
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
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("No query history yet"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground.opacity(0.7))
                            .child("Execute queries to see them here"),
                    )
                    .into_any_element()
            } else {
                // History list
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .children(entry_elements)
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
