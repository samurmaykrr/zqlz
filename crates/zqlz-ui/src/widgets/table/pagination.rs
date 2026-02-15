//! Pagination state and controls for table widgets
//!
//! Provides reusable pagination state management and UI components
//! for both table viewer and query results panels.

use std::time::Instant;

use gpui::prelude::FluentBuilder;
use gpui::*;
use serde::{Deserialize, Serialize};

use crate::widgets::{
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex, ActiveTheme, Disableable, Sizable,
};

/// Pagination display mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PaginationMode {
    /// Traditional page-based pagination with page numbers
    #[default]
    PageBased,
    /// Infinite scroll - loads more data as user scrolls
    InfiniteScroll,
}

impl PaginationMode {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::PageBased => "Page Based",
            Self::InfiniteScroll => "Infinite Scroll",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::PageBased, Self::InfiniteScroll]
    }
}

/// Events emitted by pagination controls
#[derive(Clone, Debug)]
pub enum PaginationEvent {
    /// User navigated to a specific page
    PageChanged(usize),
    /// User changed the records per page limit
    LimitChanged(usize),
    /// User toggled the limit checkbox (enable/disable pagination)
    LimitEnabledChanged(bool),
    /// User requested a refresh
    RefreshRequested,
    /// User changed pagination mode
    ModeChanged(PaginationMode),
    /// User clicked "Last Page" but total_records is unknown — the
    /// handler should run COUNT(*), update total_records, then navigate.
    LastPageRequested,
}

/// Pagination state for a table or results panel
pub struct PaginationState {
    /// Current page number (1-indexed)
    pub current_page: usize,
    /// Records per page (user-selected limit)
    pub records_per_page: usize,
    /// Total records in the data source (from COUNT query, if available)
    pub total_records: Option<u64>,
    /// Whether `total_records` is an estimate from database metadata
    /// rather than an exact COUNT(*). When true, the UI shows a `~` prefix.
    pub is_estimated: bool,
    /// Number of records loaded in current page
    pub records_in_current_page: usize,
    /// Whether more pages are available
    pub has_more: bool,
    /// Last data refresh timestamp
    pub last_refresh: Option<Instant>,
    /// Current pagination mode
    pub pagination_mode: PaginationMode,
    /// Whether pagination is currently loading
    pub is_loading: bool,
    /// Whether the limit checkbox is enabled
    pub limit_enabled: bool,
    /// Available page size options
    pub available_page_sizes: Vec<usize>,
}

impl PaginationState {
    /// Create a new pagination state with defaults
    pub fn new(_window: &mut Window, _cx: &mut Context<Self>) -> Self {
        Self {
            current_page: 1,
            records_per_page: 1000,
            total_records: None,
            is_estimated: false,
            records_in_current_page: 0,
            has_more: false,
            last_refresh: None,
            pagination_mode: PaginationMode::PageBased,
            is_loading: false,
            limit_enabled: true,
            available_page_sizes: vec![100, 500, 1000, 5000, 10000],
        }
    }

    /// Calculate total number of pages
    pub fn total_pages(&self) -> Option<usize> {
        self.total_records.map(|total| {
            let total = total as u64;
            let records_per_page = self.records_per_page as u64;
            // Use saturating_add to prevent overflow when total is very large
            let pages = total.saturating_add(records_per_page - 1) / records_per_page;
            pages.min(usize::MAX as u64).max(1) as usize
        })
    }

    /// Calculate SQL OFFSET for current page
    pub fn offset(&self) -> usize {
        (self.current_page - 1) * self.records_per_page
    }

    /// Check if we can navigate to next page
    pub fn can_go_next(&self) -> bool {
        if let Some(total_pages) = self.total_pages() {
            self.current_page < total_pages
        } else {
            self.has_more
        }
    }

    /// Check if we can navigate to previous page
    pub fn can_go_prev(&self) -> bool {
        self.current_page > 1
    }

    /// Navigate to next page
    pub fn go_next(&mut self, cx: &mut Context<Self>) {
        if self.can_go_next() {
            self.current_page += 1;
            cx.emit(PaginationEvent::PageChanged(self.current_page));
        }
    }

    /// Navigate to previous page
    pub fn go_prev(&mut self, cx: &mut Context<Self>) {
        if self.can_go_prev() {
            self.current_page -= 1;
            cx.emit(PaginationEvent::PageChanged(self.current_page));
        }
    }

    /// Navigate to first page
    pub fn go_first(&mut self, cx: &mut Context<Self>) {
        if self.current_page != 1 {
            self.current_page = 1;
            cx.emit(PaginationEvent::PageChanged(self.current_page));
        }
    }

    /// Navigate to last page.
    ///
    /// Always emits `LastPageRequested` so the handler can use the fast
    /// reversed-ORDER-BY path (PK DESC) instead of the slow high-OFFSET
    /// scan that `PageChanged` would trigger via `reload_table_with_pagination`.
    pub fn go_last(&mut self, cx: &mut Context<Self>) {
        if self.is_loading {
            return;
        }
        if let Some(total_pages) = self.total_pages() {
            if self.current_page == total_pages {
                return;
            }
        }
        cx.emit(PaginationEvent::LastPageRequested);
    }

    /// Navigate to specific page
    pub fn go_to_page(&mut self, page: usize, cx: &mut Context<Self>) {
        let max_page = self.total_pages().unwrap_or(usize::MAX);
        let new_page = page.clamp(1, max_page);
        if self.current_page != new_page {
            self.current_page = new_page;
            cx.emit(PaginationEvent::PageChanged(self.current_page));
        }
    }

    /// Set records per page limit
    pub fn set_limit(&mut self, limit: usize, cx: &mut Context<Self>) {
        if self.records_per_page != limit {
            self.records_per_page = limit;
            // Reset to first page when limit changes
            self.current_page = 1;
            cx.emit(PaginationEvent::LimitChanged(limit));
        }
    }

    /// Toggle limit enabled state
    pub fn set_limit_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if self.limit_enabled != enabled {
            self.limit_enabled = enabled;
            cx.emit(PaginationEvent::LimitEnabledChanged(enabled));
        }
    }

    /// Set pagination mode (Page-based or Infinite scroll)
    pub fn set_mode(&mut self, mode: PaginationMode, cx: &mut Context<Self>) {
        if self.pagination_mode != mode {
            self.pagination_mode = mode;
            // Reset state when switching modes
            self.current_page = 1;
            cx.emit(PaginationEvent::ModeChanged(mode));
        }
    }

    /// Toggle between page-based and infinite scroll modes
    pub fn toggle_mode(&mut self, cx: &mut Context<Self>) {
        let new_mode = match self.pagination_mode {
            PaginationMode::PageBased => PaginationMode::InfiniteScroll,
            PaginationMode::InfiniteScroll => PaginationMode::PageBased,
        };
        self.set_mode(new_mode, cx);
    }

    /// Update the state after data is loaded
    pub fn update_after_load(
        &mut self,
        records_loaded: usize,
        total_records: Option<u64>,
        is_estimated: bool,
        cx: &mut Context<Self>,
    ) {
        self.records_in_current_page = records_loaded;
        self.total_records = total_records;
        self.is_estimated = is_estimated;
        self.last_refresh = Some(Instant::now());
        self.is_loading = false;

        // Determine if there are more pages based on loaded count
        if total_records.is_none() {
            self.has_more = records_loaded >= self.records_per_page;
        }

        cx.notify();
    }

    /// Get formatted "last refresh" text
    ///
    /// Shows granular time since last refresh:
    /// - 0-4 seconds: "Refreshed now"
    /// - 5-59 seconds: "Last Refresh: Xs ago" (e.g., "Last Refresh: 30s ago")
    /// - 1-59 minutes: "Last Refresh: Xm ago" (e.g., "Last Refresh: 5m ago")
    /// - 1+ hours: "Last Refresh: Xh ago" (e.g., "Last Refresh: 2h ago")
    pub fn last_refresh_text(&self) -> String {
        match self.last_refresh {
            Some(instant) => {
                let elapsed = instant.elapsed();
                let secs = elapsed.as_secs();
                if secs < 5 {
                    "Refreshed now".to_string()
                } else if secs < 60 {
                    format!("Last Refresh: {}s ago", secs)
                } else if secs < 3600 {
                    format!("Last Refresh: {}m ago", secs / 60)
                } else {
                    format!("Last Refresh: {}h ago", secs / 3600)
                }
            }
            None => "Not refreshed".to_string(),
        }
    }

    /// Get status text (e.g., "1000 records in page 1" or "8301 records loaded")
    pub fn status_text(&self) -> String {
        let approx = if self.is_estimated { "~" } else { "" };
        match self.pagination_mode {
            PaginationMode::PageBased => {
                // Page mode: show page info
                if let Some(total) = self.total_records {
                    let total_pages = self.total_pages().unwrap_or(1);
                    format!(
                        "{} records in page {} of {}{} ({}{} total)",
                        self.records_in_current_page,
                        self.current_page,
                        approx,
                        total_pages,
                        approx,
                        total
                    )
                } else {
                    format!(
                        "{} records in page {}",
                        self.records_in_current_page, self.current_page
                    )
                }
            }
            PaginationMode::InfiniteScroll => {
                // Infinite mode: show loaded count
                if let Some(total) = self.total_records {
                    format!(
                        "{} records loaded ({}{} total)",
                        self.records_in_current_page, approx, total
                    )
                } else {
                    format!("{} records loaded", self.records_in_current_page)
                }
            }
        }
    }
}

impl EventEmitter<PaginationEvent> for PaginationState {}

/// Render the pagination footer bar (controls only, no status/refresh text)
///
/// Layout: [|<] [<] [Page X] [>] [>|] | Mode toggle | ☑ Limit [< 1000 >] records per page | Loading...
pub fn render_pagination_controls(
    state: &Entity<PaginationState>,
    _window: &mut Window,
    cx: &App,
) -> impl IntoElement {
    let theme = cx.theme();
    let pagination = state.read(cx);

    let current_page = pagination.current_page;
    let can_prev = pagination.can_go_prev();
    let can_next = pagination.can_go_next();
    let can_first = current_page > 1;
    let can_last = pagination
        .total_pages()
        .map(|t| current_page < t)
        .unwrap_or(pagination.has_more);
    let limit_enabled = pagination.limit_enabled;
    let records_per_page = pagination.records_per_page;
    let is_loading = pagination.is_loading;
    let available_sizes = pagination.available_page_sizes.clone();
    let pagination_mode = pagination.pagination_mode;

    let state_for_nav = state.clone();

    h_flex()
        .w_full()
        .h(px(32.0))
        .px_2()
        .gap_1()
        .items_center()
        .bg(theme.tab_bar)
        .border_t_1()
        .border_color(theme.border)
        .when(pagination_mode == PaginationMode::PageBased, |this| {
            let state_first = state_for_nav.clone();
            let state_prev = state_for_nav.clone();
            let state_next = state_for_nav.clone();
            let state_last = state_for_nav.clone();

            this.child(
                Button::new("page-first")
                    .icon(crate::widgets::IconName::ArrowLeft)
                    .ghost()
                    .xsmall()
                    .disabled(!can_first || is_loading)
                    .tooltip("First Page")
                    .on_click(move |_, _, cx| {
                        state_first.update(cx, |s, cx| s.go_first(cx));
                    }),
            )
            .child(
                Button::new("page-prev")
                    .icon(crate::widgets::IconName::ChevronLeft)
                    .ghost()
                    .xsmall()
                    .disabled(!can_prev || is_loading)
                    .tooltip("Previous Page")
                    .on_click(move |_, _, cx| {
                        state_prev.update(cx, |s, cx| s.go_prev(cx));
                    }),
            )
            .child(
                div()
                    .px_2()
                    .py(px(2.0))
                    .min_w(px(40.0))
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(theme.border)
                    .bg(theme.background)
                    .text_xs()
                    .text_color(theme.foreground)
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(current_page.to_string()),
            )
            .child(
                Button::new("page-next")
                    .icon(crate::widgets::IconName::ChevronRight)
                    .ghost()
                    .xsmall()
                    .disabled(!can_next || is_loading)
                    .tooltip("Next Page")
                    .on_click(move |_, _, cx| {
                        state_next.update(cx, |s, cx| s.go_next(cx));
                    }),
            )
            .child(
                Button::new("page-last")
                    .icon(crate::widgets::IconName::ArrowRight)
                    .ghost()
                    .xsmall()
                    .disabled(!can_last || is_loading)
                    .tooltip("Last Page")
                    .on_click(move |_, _, cx| {
                        state_last.update(cx, |s, cx| s.go_last(cx));
                    }),
            )
            .child(div().h(px(16.0)).w(px(1.0)).mx_1().bg(theme.border))
        })
        // Mode toggle button (Page / Infinite)
        .child({
            let state_mode = state.clone();
            let icon = match pagination_mode {
                PaginationMode::PageBased => crate::widgets::IconName::LayoutDashboard,
                PaginationMode::InfiniteScroll => crate::widgets::IconName::ChevronsUpDown,
            };
            let tooltip = match pagination_mode {
                PaginationMode::PageBased => "Switch to Infinite Scroll",
                PaginationMode::InfiniteScroll => "Switch to Page Mode",
            };

            Button::new("pagination-mode-toggle")
                .icon(icon)
                .ghost()
                .xsmall()
                .disabled(is_loading)
                .tooltip(tooltip)
                .on_click(move |_, _, cx| {
                    state_mode.update(cx, |s, cx| s.toggle_mode(cx));
                })
        })
        .child(div().h(px(16.0)).w(px(1.0)).mx_1().bg(theme.border))
        // Limit checkbox
        .child({
            let state_checkbox = state.clone();
            Checkbox::new("limit-enabled")
                .checked(limit_enabled)
                .label("Limit")
                .on_click(move |checked, _, cx| {
                    state_checkbox.update(cx, |s, cx| s.set_limit_enabled(*checked, cx));
                })
        })
        // Page size cycle buttons
        .child(
            div()
                .flex()
                .items_center()
                .gap_1()
                .child({
                    let state_prev = state.clone();
                    let sizes = available_sizes.clone();
                    let current_index = sizes
                        .iter()
                        .position(|&s| s == records_per_page)
                        .unwrap_or(2);
                    let can_decrease = current_index > 0;

                    Button::new("page-size-decrease")
                        .icon(crate::widgets::IconName::ChevronLeft)
                        .ghost()
                        .xsmall()
                        .disabled(!limit_enabled || is_loading || !can_decrease)
                        .on_click(move |_event, _window, cx| {
                            if let Some(idx) = sizes.iter().position(|&s| s == records_per_page) {
                                if idx > 0 {
                                    state_prev.update(cx, |s, cx| s.set_limit(sizes[idx - 1], cx));
                                }
                            }
                        })
                })
                .child(
                    div()
                        .text_xs()
                        .text_color(if limit_enabled {
                            theme.foreground
                        } else {
                            theme.muted_foreground
                        })
                        .child(format!("{}", records_per_page))
                        .min_w(px(40.0))
                        .text_center(),
                )
                .child({
                    let state_next = state.clone();
                    let sizes = available_sizes.clone();
                    let current_index = sizes
                        .iter()
                        .position(|&s| s == records_per_page)
                        .unwrap_or(2);
                    let can_increase = current_index < sizes.len() - 1;

                    Button::new("page-size-increase")
                        .icon(crate::widgets::IconName::ChevronRight)
                        .ghost()
                        .xsmall()
                        .disabled(!limit_enabled || is_loading || !can_increase)
                        .on_click(move |_event, _window, cx| {
                            if let Some(idx) = sizes.iter().position(|&s| s == records_per_page) {
                                if idx < sizes.len() - 1 {
                                    state_next.update(cx, |s, cx| s.set_limit(sizes[idx + 1], cx));
                                }
                            }
                        })
                }),
        )
        .child(
            div()
                .text_xs()
                .text_color(theme.muted_foreground)
                .child("records per page"),
        )
        .child(div().flex_1())
        .when(is_loading, |this| {
            this.child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Loading..."),
            )
        })
}

/// Render the pagination footer bar (full version with status and refresh text)
///
/// Layout: [|<] [<] [Page X] [>] [>|] | ☑ Limit [1000 ▾] records per page | Status | Last Refresh
pub fn render_pagination_footer(
    state: &Entity<PaginationState>,
    _window: &mut Window,
    cx: &App,
) -> impl IntoElement {
    let theme = cx.theme();
    let pagination = state.read(cx);

    let current_page = pagination.current_page;
    let can_prev = pagination.can_go_prev();
    let can_next = pagination.can_go_next();
    let can_first = current_page > 1;
    let can_last = pagination
        .total_pages()
        .map(|t| current_page < t)
        .unwrap_or(pagination.has_more);
    let limit_enabled = pagination.limit_enabled;
    let records_per_page = pagination.records_per_page;
    let status_text = pagination.status_text();
    let refresh_text = pagination.last_refresh_text();
    let is_loading = pagination.is_loading;
    let available_sizes = pagination.available_page_sizes.clone();
    let pagination_mode = pagination.pagination_mode;

    let state_for_nav = state.clone();

    h_flex()
        .w_full()
        .h(px(32.0))
        .px_2()
        .gap_1()
        .items_center()
        .bg(theme.tab_bar)
        .border_t_1()
        .border_color(theme.border)
        // Only show page-based controls when in page mode
        .when(pagination_mode == PaginationMode::PageBased, |this| {
            let state_first = state_for_nav.clone();
            let state_prev = state_for_nav.clone();
            let state_next = state_for_nav.clone();
            let state_last = state_for_nav.clone();

            this
                // First page button (use ArrowLeft for "jump to start")
                .child(
                    Button::new("page-first")
                        .icon(crate::widgets::IconName::ArrowLeft)
                        .ghost()
                        .xsmall()
                        .disabled(!can_first || is_loading)
                        .tooltip("First Page")
                        .on_click(move |_, _, cx| {
                            state_first.update(cx, |s, cx| s.go_first(cx));
                        }),
                )
                // Previous page button
                .child(
                    Button::new("page-prev")
                        .icon(crate::widgets::IconName::ChevronLeft)
                        .ghost()
                        .xsmall()
                        .disabled(!can_prev || is_loading)
                        .tooltip("Previous Page")
                        .on_click(move |_, _, cx| {
                            state_prev.update(cx, |s, cx| s.go_prev(cx));
                        }),
                )
                // Page number display
                .child(
                    div()
                        .px_2()
                        .py(px(2.0))
                        .min_w(px(40.0))
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(theme.border)
                        .bg(theme.background)
                        .text_xs()
                        .text_color(theme.foreground)
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(current_page.to_string()),
                )
                // Next page button
                .child(
                    Button::new("page-next")
                        .icon(crate::widgets::IconName::ChevronRight)
                        .ghost()
                        .xsmall()
                        .disabled(!can_next || is_loading)
                        .tooltip("Next Page")
                        .on_click(move |_, _, cx| {
                            state_next.update(cx, |s, cx| s.go_next(cx));
                        }),
                )
                // Last page button (use ArrowRight for "jump to end")
                .child(
                    Button::new("page-last")
                        .icon(crate::widgets::IconName::ArrowRight)
                        .ghost()
                        .xsmall()
                        .disabled(!can_last || is_loading)
                        .tooltip("Last Page")
                        .on_click(move |_, _, cx| {
                            state_last.update(cx, |s, cx| s.go_last(cx));
                        }),
                )
                // Separator
                .child(div().h(px(16.0)).w(px(1.0)).mx_1().bg(theme.border))
        })
        // Mode toggle button (Page / Infinite)
        .child({
            let state_mode = state.clone();
            let icon = match pagination_mode {
                PaginationMode::PageBased => crate::widgets::IconName::LayoutDashboard, // Grid/pages icon
                PaginationMode::InfiniteScroll => crate::widgets::IconName::ChevronsUpDown, // Scroll icon
            };
            let tooltip = match pagination_mode {
                PaginationMode::PageBased => "Switch to Infinite Scroll",
                PaginationMode::InfiniteScroll => "Switch to Page Mode",
            };

            Button::new("pagination-mode-toggle")
                .icon(icon)
                .ghost()
                .xsmall()
                .disabled(is_loading)
                .tooltip(tooltip)
                .on_click(move |_, _, cx| {
                    state_mode.update(cx, |s, cx| s.toggle_mode(cx));
                })
        })
        // Separator
        .child(div().h(px(16.0)).w(px(1.0)).mx_1().bg(theme.border))
        // Limit checkbox
        .child({
            let state_checkbox = state.clone();
            Checkbox::new("limit-enabled")
                .checked(limit_enabled)
                .label("Limit")
                .on_click(move |checked, _, cx| {
                    state_checkbox.update(cx, |s, cx| s.set_limit_enabled(*checked, cx));
                })
        })
        // Page size cycle buttons - simpler and more performant than dropdown
        .child(
            div()
                .flex()
                .items_center()
                .gap_1()
                .child({
                    let state_prev = state.clone();
                    let sizes = available_sizes.clone();
                    let current_index = sizes
                        .iter()
                        .position(|&s| s == records_per_page)
                        .unwrap_or(2);
                    let can_decrease = current_index > 0;

                    Button::new("page-size-decrease")
                        .icon(crate::widgets::IconName::ChevronLeft)
                        .ghost()
                        .xsmall()
                        .disabled(!limit_enabled || is_loading || !can_decrease)
                        .on_click(move |_event, _window, cx| {
                            if let Some(idx) = sizes.iter().position(|&s| s == records_per_page) {
                                if idx > 0 {
                                    state_prev.update(cx, |s, cx| s.set_limit(sizes[idx - 1], cx));
                                }
                            }
                        })
                })
                .child(
                    div()
                        .text_xs()
                        .text_color(if limit_enabled {
                            theme.foreground
                        } else {
                            theme.muted_foreground
                        })
                        .child(format!("{}", records_per_page))
                        .min_w(px(40.0))
                        .text_center(),
                )
                .child({
                    let state_next = state.clone();
                    let sizes = available_sizes.clone();
                    let current_index = sizes
                        .iter()
                        .position(|&s| s == records_per_page)
                        .unwrap_or(2);
                    let can_increase = current_index < sizes.len() - 1;

                    Button::new("page-size-increase")
                        .icon(crate::widgets::IconName::ChevronRight)
                        .ghost()
                        .xsmall()
                        .disabled(!limit_enabled || is_loading || !can_increase)
                        .on_click(move |_event, _window, cx| {
                            if let Some(idx) = sizes.iter().position(|&s| s == records_per_page) {
                                if idx < sizes.len() - 1 {
                                    state_next.update(cx, |s, cx| s.set_limit(sizes[idx + 1], cx));
                                }
                            }
                        })
                }),
        )
        .child(
            div()
                .text_xs()
                .text_color(theme.muted_foreground)
                .child("records per page"),
        )
        // Separator
        .child(div().h(px(16.0)).w(px(1.0)).mx_2().bg(theme.border))
        // Status text
        .child(
            div()
                .text_xs()
                .text_color(theme.foreground)
                .child(status_text),
        )
        // Separator
        .child(div().h(px(16.0)).w(px(1.0)).mx_2().bg(theme.border))
        // Last refresh time
        .child(
            div()
                .text_xs()
                .text_color(theme.muted_foreground)
                .child(refresh_text),
        )
        // Spacer
        .child(div().flex_1())
        // Loading indicator
        .when(is_loading, |this| {
            this.child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Loading..."),
            )
        })
}
