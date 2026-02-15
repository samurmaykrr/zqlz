//! Date and DateTime Picker Widget
//!
//! A hybrid date/time picker with:
//! - Inline text input for direct typing/pasting
//! - Calendar button to open visual picker popover
//! - Full precision support (microseconds, timezone offsets)
//!
//! Design inspired by macOS date picker.
//!
//! ## Usage
//!
//! For inline editing in table cells, use the wrapper structs:
//! - `DatePickerInline` - Renders just the text input + calendar button (in cell)
//! - `DatePickerPopover` - Renders just the calendar + time spinners (in popover)
//!
//! ```rust,ignore
//! // In your cell rendering code:
//! let date_picker: Entity<DatePickerState> = ...;
//!
//! div()
//!     .child(DatePickerInline::new(&date_picker))  // Input + button in cell
//!     .when(date_picker.read(cx).is_popover_open(), |this| {
//!         this.child(deferred(anchored().child(DatePickerPopover::new(&date_picker))))
//!     })
//! ```

use std::rc::Rc;

use gpui::{
    div, prelude::FluentBuilder as _, px, AnyElement, App, AppContext, Context, DismissEvent,
    Entity, EventEmitter, FocusHandle, Focusable, InteractiveElement as _, IntoElement, KeyBinding,
    ParentElement, Render, RenderOnce, SharedString, StatefulInteractiveElement as _, Styled,
    Window,
};

use super::{
    actions::Cancel,
    button::{Button, ButtonVariants},
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex, ActiveTheme, Icon, IconName, Sizable,
};

const CONTEXT: &str = "DatePicker";

/// Initialize the date picker key bindings
pub fn init(cx: &mut App) {
    cx.bind_keys([KeyBinding::new("escape", Cancel, Some(CONTEXT))])
}

/// The mode of the date picker
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DatePickerMode {
    /// Date only (YYYY-MM-DD)
    #[default]
    Date,
    /// Time only (HH:MM:SS)
    Time,
    /// Full datetime (YYYY-MM-DD HH:MM:SS)
    DateTime,
}

/// Check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Get the number of days in a month
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Get the day of week for the first day of a month (0 = Sunday, 6 = Saturday)
fn first_day_of_month(year: i32, month: u32) -> u32 {
    // Zeller's congruence algorithm
    let m = if month < 3 { month + 12 } else { month } as i32;
    let y = if month < 3 { year - 1 } else { year };
    let q = 1i32; // First day of month
    let k = y % 100;
    let j = y / 100;

    let h = (q + (13 * (m + 1)) / 5 + k + k / 4 + j / 4 - 2 * j) % 7;
    let h = ((h % 7) + 7) % 7; // Ensure positive

    // Convert from Zeller (0=Saturday) to our format (0=Sunday)
    ((h + 6) % 7) as u32
}

/// Get current date/time as string
fn current_datetime_string(mode: DatePickerMode) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();

    // Calculate date from epoch
    let days_since_epoch = secs / 86400;
    let mut year = 1970i32;
    let mut remaining_days = days_since_epoch as i64;

    loop {
        let days_in_year: i64 = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let mut month = 1u32;
    loop {
        let dim = days_in_month(year, month) as i64;
        if remaining_days < dim {
            break;
        }
        remaining_days -= dim;
        month += 1;
    }
    let day = remaining_days as u32 + 1;

    // Calculate time
    let seconds_today = secs % 86400;
    let hour = seconds_today / 3600;
    let minute = (seconds_today % 3600) / 60;
    let second = seconds_today % 60;

    match mode {
        DatePickerMode::Date => format!("{:04}-{:02}-{:02}", year, month, day),
        DatePickerMode::Time => format!("{:02}:{:02}:{:02}.{:06}", hour, minute, second, micros),
        DatePickerMode::DateTime => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hour, minute, second, micros
        ),
    }
}

/// Parse year, month, day from a date string
fn parse_date_parts(value: &str) -> Option<(i32, u32, u32)> {
    // Try to extract date part (before space or T)
    let date_part = value.split(|c| c == ' ' || c == 'T').next()?;
    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() >= 3 {
        let year = parts[0].parse().ok()?;
        let month = parts[1].parse().ok()?;
        let day = parts[2].parse().ok()?;
        Some((year, month, day))
    } else {
        None
    }
}

/// Short month names
const MONTH_NAMES_SHORT: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Short day names
const DAY_NAMES: [&str; 7] = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

/// State for the DatePicker popover
pub struct DatePickerState {
    focus_handle: FocusHandle,
    mode: DatePickerMode,
    /// The raw text value (full precision)
    text_value: String,
    /// Input state for the text field
    input_state: Entity<InputState>,
    /// Currently viewed year/month (for calendar navigation)
    view_year: i32,
    view_month: u32,
    nullable: bool,
    /// Whether the calendar popover is open
    popover_open: bool,
    on_change: Option<Rc<dyn Fn(&str, &mut Window, &mut App)>>,
}

impl DatePickerState {
    pub fn new(
        mode: DatePickerMode,
        initial_value: &str,
        nullable: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let text_value = initial_value.to_string();

        // Parse date parts for calendar view
        let (view_year, view_month) = if let Some((y, m, _)) = parse_date_parts(initial_value) {
            (y, m)
        } else {
            // Default to current date
            let now = current_datetime_string(DatePickerMode::Date);
            if let Some((y, m, _)) = parse_date_parts(&now) {
                (y, m)
            } else {
                (2024, 1)
            }
        };

        // Create input state for text field
        let text_for_input = text_value.clone();
        let input_state = cx.new(|cx| {
            let mut state = InputState::new(window, cx);
            state.set_value(&text_for_input, window, cx);
            state
        });

        // Subscribe to input changes
        cx.subscribe_in(
            &input_state,
            window,
            |this, _input, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::Change) {
                    // Read the text value from the input state
                    let new_value = this.input_state.read(cx).text().to_string();
                    // Update our text_value
                    this.text_value = new_value.clone();

                    // Update calendar view if date is valid
                    if let Some((y, m, _)) = parse_date_parts(&new_value) {
                        this.view_year = y;
                        this.view_month = m;
                    }

                    // Emit change event to notify parent
                    this.emit_change(window, cx);
                    cx.notify();
                }
            },
        )
        .detach();

        Self {
            focus_handle: cx.focus_handle(),
            mode,
            text_value,
            input_state,
            view_year,
            view_month,
            nullable,
            popover_open: false,
            on_change: None,
        }
    }

    pub fn set_on_change(&mut self, callback: impl Fn(&str, &mut Window, &mut App) + 'static) {
        self.on_change = Some(Rc::new(callback));
    }

    /// Get the current text value
    pub fn value(&self) -> &str {
        &self.text_value
    }

    /// Check if the calendar popover is open
    pub fn is_popover_open(&self) -> bool {
        self.popover_open
    }

    /// Get the input state entity for external rendering
    pub fn input_state(&self) -> &Entity<InputState> {
        &self.input_state
    }

    /// Get the date picker mode
    pub fn mode(&self) -> DatePickerMode {
        self.mode
    }

    /// Open the calendar popover
    pub fn open_popover(&mut self, cx: &mut Context<Self>) {
        self.popover_open = true;
        cx.notify();
    }

    /// Close the calendar popover
    pub fn close_popover(&mut self, cx: &mut Context<Self>) {
        self.popover_open = false;
        cx.notify();
    }

    /// Render the inline part (Input + calendar button) for use inside a table cell
    /// Returns the inline element that should be rendered directly in the cell
    pub fn render_inline(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let input_state = self.input_state.clone();
        let mode = self.mode;
        let popover_open = self.popover_open;

        let accent_color = theme.accent;
        let muted_fg = theme.muted_foreground;
        let muted_bg = theme.muted;
        let radius = theme.radius;

        h_flex()
            .w_full()
            .h_full()
            .items_center()
            .gap_1()
            // Text input - takes most of the width
            .child(
                div()
                    .flex_1()
                    .min_w_0() // Allow shrinking
                    .child(Input::new(&input_state).appearance(false).w_full()),
            )
            // Calendar button (for date/datetime modes)
            .when(mode != DatePickerMode::Time, |this| {
                this.child(
                    div()
                        .id("inline-calendar-btn")
                        .cursor_pointer()
                        .p_1()
                        .rounded(radius)
                        .flex_shrink_0()
                        .when(popover_open, |s| s.bg(accent_color.opacity(0.2)))
                        .hover(|s| s.bg(muted_bg))
                        .on_click(cx.listener(|state, _, _, cx| {
                            state.toggle_popover(cx);
                        }))
                        .child(
                            Icon::new(IconName::Calendar)
                                .size_4()
                                .text_color(if popover_open { accent_color } else { muted_fg }),
                        ),
                )
            })
            .into_any_element()
    }

    /// Render the popover content (calendar + time spinners + action buttons)
    /// This is rendered inside an anchored/deferred popover, NOT in the cell itself
    pub fn render_popover_content(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let mode = self.mode;
        let nullable = self.nullable;

        let popover_bg = theme.popover;
        let border_color = theme.border;
        let radius = theme.radius;

        // Pre-render calendar and time spinners
        let calendar_element = if mode != DatePickerMode::Time {
            Some(self.render_calendar(cx))
        } else {
            None
        };

        let time_spinners_element = if mode != DatePickerMode::Date {
            Some(self.render_time_spinners(cx))
        } else {
            None
        };

        v_flex()
            .key_context(CONTEXT)
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &Cancel, _, cx| {
                this.close_popover(cx);
                cx.emit(DismissEvent);
            }))
            .p_3()
            .gap_2()
            .bg(popover_bg)
            .border_1()
            .border_color(border_color)
            .rounded(radius)
            .shadow_lg()
            .min_w(px(220.))
            // Calendar (for date/datetime modes)
            .when_some(calendar_element, |this, calendar| this.child(calendar))
            // Time spinners (for time/datetime modes)
            .when_some(time_spinners_element, |this, spinners| {
                this.child(
                    div()
                        .pt_2()
                        .border_t_1()
                        .border_color(border_color)
                        .child(spinners),
                )
            })
            // Action buttons
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .pt_2()
                    .border_t_1()
                    .border_color(border_color)
                    .child(
                        h_flex()
                            .gap_1()
                            .child(Button::new("now").label("Now").small().ghost().on_click(
                                cx.listener(|state, _, window, cx| {
                                    state.set_now(window, cx);
                                }),
                            ))
                            .when(nullable, |this| {
                                this.child(
                                    Button::new("clear")
                                        .label("Clear")
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|state, _, window, cx| {
                                            state.set_null(window, cx);
                                        })),
                                )
                            }),
                    )
                    .child(Button::new("done").label("OK").small().primary().on_click(
                        cx.listener(|this, _, _, cx| {
                            this.close_popover(cx);
                            cx.emit(DismissEvent);
                        }),
                    )),
            )
            .into_any_element()
    }

    /// Toggle the calendar popover
    pub fn toggle_popover(&mut self, cx: &mut Context<Self>) {
        self.popover_open = !self.popover_open;
        cx.notify();
    }

    /// Navigate to previous month
    fn prev_month(&mut self, cx: &mut Context<Self>) {
        if self.view_month == 1 {
            self.view_month = 12;
            self.view_year -= 1;
        } else {
            self.view_month -= 1;
        }
        cx.notify();
    }

    /// Navigate to next month
    fn next_month(&mut self, cx: &mut Context<Self>) {
        if self.view_month == 12 {
            self.view_month = 1;
            self.view_year += 1;
        } else {
            self.view_month += 1;
        }
        cx.notify();
    }

    /// Select a day from the calendar
    fn select_day(&mut self, day: u32, window: &mut Window, cx: &mut Context<Self>) {
        let new_date = format!("{:04}-{:02}-{:02}", self.view_year, self.view_month, day);

        // Preserve time part if present
        let new_value = if self.mode == DatePickerMode::DateTime {
            // Try to extract existing time part
            let time_part = self
                .text_value
                .split(|c| c == ' ' || c == 'T')
                .nth(1)
                .unwrap_or("00:00:00");
            format!("{} {}", new_date, time_part)
        } else {
            new_date
        };

        self.text_value = new_value.clone();
        self.input_state.update(cx, |state, cx| {
            state.set_value(&new_value, window, cx);
        });

        self.emit_change(window, cx);
        cx.notify();
    }

    /// Set to current date/time
    fn set_now(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let new_value = current_datetime_string(self.mode);
        self.text_value = new_value.clone();
        self.input_state.update(cx, |state, cx| {
            state.set_value(&new_value, window, cx);
        });

        // Update calendar view to current date
        if let Some((y, m, _)) = parse_date_parts(&new_value) {
            self.view_year = y;
            self.view_month = m;
        }

        self.emit_change(window, cx);
        cx.notify();
    }

    /// Clear to NULL/empty
    fn set_null(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.text_value.clear();
        self.input_state.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.emit_change(window, cx);
        cx.notify();
    }

    fn emit_change(&self, window: &mut Window, cx: &mut App) {
        if let Some(on_change) = &self.on_change {
            on_change(&self.text_value, window, cx);
        }
    }

    fn render_calendar(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let days = days_in_month(self.view_year, self.view_month);
        let first_day = first_day_of_month(self.view_year, self.view_month);
        let month_name = MONTH_NAMES_SHORT
            .get(self.view_month as usize - 1)
            .unwrap_or(&"");

        // Get selected day if viewing the same month
        let selected_day = parse_date_parts(&self.text_value)
            .filter(|(y, m, _)| *y == self.view_year && *m == self.view_month)
            .map(|(_, _, d)| d);

        // Get today's day if viewing current month
        let today = current_datetime_string(DatePickerMode::Date);
        let today_day = parse_date_parts(&today)
            .filter(|(y, m, _)| *y == self.view_year && *m == self.view_month)
            .map(|(_, _, d)| d);

        v_flex()
            .gap_1()
            // Month/Year header with navigation
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .id("prev-month")
                            .cursor_pointer()
                            .p_1()
                            .rounded(theme.radius)
                            .hover(|s| s.bg(theme.muted))
                            .on_click(cx.listener(|state, _, _, cx| {
                                state.prev_month(cx);
                            }))
                            .child(Icon::new(IconName::ChevronLeft).size_3()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child(format!("{} {}", month_name, self.view_year)),
                    )
                    .child(
                        div()
                            .id("next-month")
                            .cursor_pointer()
                            .p_1()
                            .rounded(theme.radius)
                            .hover(|s| s.bg(theme.muted))
                            .on_click(cx.listener(|state, _, _, cx| {
                                state.next_month(cx);
                            }))
                            .child(Icon::new(IconName::ChevronRight).size_3()),
                    ),
            )
            // Day names header
            .child(h_flex().gap_px().children(DAY_NAMES.iter().map(|name| {
                div()
                    .w(px(24.))
                    .h(px(20.))
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(*name)
            })))
            // Calendar grid
            .child(self.render_calendar_grid(days, first_day, selected_day, today_day, cx))
            .into_any_element()
    }

    fn render_calendar_grid(
        &self,
        days: u32,
        first_day: u32,
        selected_day: Option<u32>,
        today_day: Option<u32>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        // Build 6 weeks of days (42 cells)
        let mut cells: Vec<Option<u32>> = Vec::with_capacity(42);

        // Empty cells before first day
        for _ in 0..first_day {
            cells.push(None);
        }

        // Days of the month
        for day in 1..=days {
            cells.push(Some(day));
        }

        // Empty cells after last day (fill to complete weeks)
        while cells.len() % 7 != 0 || cells.len() < 35 {
            cells.push(None);
        }

        let weeks = cells.len() / 7;

        v_flex().gap_px().children((0..weeks).map(|week| {
            let week_cells = cells[week * 7..(week + 1) * 7].to_vec();
            h_flex()
                .gap_px()
                .children(week_cells.into_iter().enumerate().map(|(i, day_opt)| {
                    let cell_id = format!("day-{}-{}", week, i);
                    match day_opt {
                        Some(day) => {
                            let is_selected = selected_day == Some(day);
                            let is_today = today_day == Some(day);

                            div()
                                .id(SharedString::from(cell_id))
                                .w(px(24.))
                                .h(px(24.))
                                .flex()
                                .items_center()
                                .justify_center()
                                .text_xs()
                                .cursor_pointer()
                                .rounded(theme.radius)
                                .when(is_selected, |s| {
                                    s.bg(theme.primary).text_color(theme.primary_foreground)
                                })
                                .when(!is_selected && is_today, |s| {
                                    s.border_1().border_color(theme.primary)
                                })
                                .when(!is_selected, |s| s.hover(|s| s.bg(theme.muted)))
                                .on_click({
                                    cx.listener(move |state, _, window, cx| {
                                        state.select_day(day, window, cx);
                                    })
                                })
                                .child(day.to_string())
                                .into_any_element()
                        }
                        None => div().w(px(24.)).h(px(24.)).into_any_element(),
                    }
                }))
        }))
    }

    fn render_time_spinners(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();

        // Parse current time from text value
        let time_part = self
            .text_value
            .split(|c| c == ' ' || c == 'T')
            .nth(if self.mode == DatePickerMode::Time {
                0
            } else {
                1
            })
            .unwrap_or("00:00:00");

        let time_parts: Vec<&str> = time_part.split(':').collect();
        let hour: u32 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let minute: u32 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        // Handle seconds with potential fractional part
        let (second, micros): (u32, u32) = if let Some(sec_str) = time_parts.get(2) {
            // Remove timezone suffix if present (e.g., "+00" or "-05")
            let sec_clean = sec_str
                .split(|c: char| c == '+' || c == '-')
                .next()
                .unwrap_or(sec_str);

            if let Some(dot_pos) = sec_clean.find('.') {
                let sec: u32 = sec_clean[..dot_pos].parse().unwrap_or(0);
                let frac_str = &sec_clean[dot_pos + 1..];
                // Pad or truncate to 6 digits for microseconds
                let frac_padded = format!("{:0<6}", frac_str);
                let micros: u32 = frac_padded[..6].parse().unwrap_or(0);
                (sec, micros)
            } else {
                (sec_clean.parse().unwrap_or(0), 0)
            }
        } else {
            (0, 0)
        };

        h_flex()
            .items_center()
            .gap_1()
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Time:"),
            )
            .child(self.render_spinner("hour", hour, 23, cx))
            .child(div().text_xs().child(":"))
            .child(self.render_spinner("minute", minute, 59, cx))
            .child(div().text_xs().child(":"))
            .child(self.render_spinner("second", second, 59, cx))
            .child(div().text_xs().child("."))
            .child(self.render_microseconds_spinner(micros, cx))
            .into_any_element()
    }

    fn render_spinner(
        &self,
        field: &'static str,
        value: u32,
        max: u32,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .items_center()
            .child(
                div()
                    .id(SharedString::from(format!("{}-up", field)))
                    .cursor_pointer()
                    .rounded(theme.radius)
                    .hover(|s| s.bg(theme.muted))
                    .on_click({
                        cx.listener(move |state, _, window, cx| {
                            state.adjust_time_field(field, 1, max, window, cx);
                        })
                    })
                    .child(Icon::new(IconName::ChevronUp).size_3()),
            )
            .child(
                div()
                    .w(px(24.))
                    .text_center()
                    .text_xs()
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .child(format!("{:02}", value)),
            )
            .child(
                div()
                    .id(SharedString::from(format!("{}-down", field)))
                    .cursor_pointer()
                    .rounded(theme.radius)
                    .hover(|s| s.bg(theme.muted))
                    .on_click({
                        cx.listener(move |state, _, window, cx| {
                            state.adjust_time_field(field, -1, max, window, cx);
                        })
                    })
                    .child(Icon::new(IconName::ChevronDown).size_3()),
            )
    }

    fn render_microseconds_spinner(&self, value: u32, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .items_center()
            .child(
                div()
                    .id("micros-up")
                    .cursor_pointer()
                    .rounded(theme.radius)
                    .hover(|s| s.bg(theme.muted))
                    .on_click(cx.listener(|state, _, window, cx| {
                        state.adjust_microseconds(1000, window, cx); // Increment by 1ms
                    }))
                    .child(Icon::new(IconName::ChevronUp).size_3()),
            )
            .child(
                div()
                    .w(px(48.))
                    .text_center()
                    .text_xs()
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .child(format!("{:06}", value)),
            )
            .child(
                div()
                    .id("micros-down")
                    .cursor_pointer()
                    .rounded(theme.radius)
                    .hover(|s| s.bg(theme.muted))
                    .on_click(cx.listener(|state, _, window, cx| {
                        state.adjust_microseconds(-1000, window, cx); // Decrement by 1ms
                    }))
                    .child(Icon::new(IconName::ChevronDown).size_3()),
            )
    }

    fn adjust_time_field(
        &mut self,
        field: &str,
        delta: i32,
        max: u32,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Parse current time
        let time_part = self
            .text_value
            .split(|c| c == ' ' || c == 'T')
            .nth(if self.mode == DatePickerMode::Time {
                0
            } else {
                1
            })
            .unwrap_or("00:00:00");

        let time_parts: Vec<&str> = time_part.split(':').collect();
        let mut hour: i32 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let mut minute: i32 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        // Parse seconds with fractional part
        let sec_str = time_parts.get(2).unwrap_or(&"00");
        let sec_clean = sec_str
            .split(|c: char| c == '+' || c == '-')
            .next()
            .unwrap_or(sec_str);
        let (mut second, frac_part): (i32, &str) = if let Some(dot_pos) = sec_clean.find('.') {
            (
                sec_clean[..dot_pos].parse().unwrap_or(0),
                &sec_clean[dot_pos..],
            )
        } else {
            (sec_clean.parse().unwrap_or(0), "")
        };

        // Extract timezone suffix if present
        let tz_suffix = if let Some(pos) = time_part.rfind(|c: char| c == '+' || c == '-') {
            if pos > time_part.len() - 10 {
                // Likely a timezone
                &time_part[pos..]
            } else {
                ""
            }
        } else {
            ""
        };

        // Apply delta
        match field {
            "hour" => {
                hour = (hour + delta).rem_euclid(max as i32 + 1);
            }
            "minute" => {
                minute = (minute + delta).rem_euclid(max as i32 + 1);
            }
            "second" => {
                second = (second + delta).rem_euclid(max as i32 + 1);
            }
            _ => {}
        }

        // Rebuild time string
        let new_time = format!(
            "{:02}:{:02}:{:02}{}{}",
            hour, minute, second, frac_part, tz_suffix
        );

        // Rebuild full value
        let new_value = match self.mode {
            DatePickerMode::Time => new_time,
            DatePickerMode::DateTime | DatePickerMode::Date => {
                let date_part = self
                    .text_value
                    .split(|c| c == ' ' || c == 'T')
                    .next()
                    .unwrap_or("1970-01-01");
                format!("{} {}", date_part, new_time)
            }
        };

        self.text_value = new_value.clone();
        self.input_state.update(cx, |state, cx| {
            state.set_value(&new_value, window, cx);
        });
        self.emit_change(window, cx);
        cx.notify();
    }

    fn adjust_microseconds(&mut self, delta: i32, window: &mut Window, cx: &mut Context<Self>) {
        // Parse current time
        let time_part = self
            .text_value
            .split(|c| c == ' ' || c == 'T')
            .nth(if self.mode == DatePickerMode::Time {
                0
            } else {
                1
            })
            .unwrap_or("00:00:00");

        let time_parts: Vec<&str> = time_part.split(':').collect();
        let hour: u32 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let minute: u32 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        let sec_str = time_parts.get(2).unwrap_or(&"00");
        let sec_clean = sec_str
            .split(|c: char| c == '+' || c == '-')
            .next()
            .unwrap_or(sec_str);
        let (second, micros): (u32, u32) = if let Some(dot_pos) = sec_clean.find('.') {
            let sec: u32 = sec_clean[..dot_pos].parse().unwrap_or(0);
            let frac_str = &sec_clean[dot_pos + 1..];
            let frac_padded = format!("{:0<6}", frac_str);
            let micros: u32 = frac_padded[..6].parse().unwrap_or(0);
            (sec, micros)
        } else {
            (sec_clean.parse().unwrap_or(0), 0)
        };

        // Extract timezone suffix
        let tz_suffix = if let Some(pos) = time_part.rfind(|c: char| c == '+' || c == '-') {
            if pos > time_part.len() - 10 {
                &time_part[pos..]
            } else {
                ""
            }
        } else {
            ""
        };

        // Apply delta to microseconds
        let new_micros = (micros as i32 + delta).rem_euclid(1_000_000) as u32;

        // Rebuild time string
        let new_time = format!(
            "{:02}:{:02}:{:02}.{:06}{}",
            hour, minute, second, new_micros, tz_suffix
        );

        // Rebuild full value
        let new_value = match self.mode {
            DatePickerMode::Time => new_time,
            DatePickerMode::DateTime | DatePickerMode::Date => {
                let date_part = self
                    .text_value
                    .split(|c| c == ' ' || c == 'T')
                    .next()
                    .unwrap_or("1970-01-01");
                format!("{} {}", date_part, new_time)
            }
        };

        self.text_value = new_value.clone();
        self.input_state.update(cx, |state, cx| {
            state.set_value(&new_value, window, cx);
        });
        self.emit_change(window, cx);
        cx.notify();
    }
}

impl EventEmitter<DismissEvent> for DatePickerState {}

impl Focusable for DatePickerState {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for DatePickerState {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let input_state = self.input_state.clone();
        let popover_open = self.popover_open;
        let mode = self.mode;
        let nullable = self.nullable;

        // Pre-extract theme values to avoid borrow conflicts
        let popover_bg = theme.popover;
        let border_color = theme.border;
        let radius = theme.radius;
        let muted_bg = theme.muted;
        let accent_color = theme.accent;
        let muted_fg = theme.muted_foreground;

        // Pre-render calendar and time spinners before building the UI tree
        let calendar_element = if popover_open && mode != DatePickerMode::Time {
            Some(self.render_calendar(cx))
        } else {
            None
        };

        let time_spinners_element = if mode != DatePickerMode::Date {
            Some(self.render_time_spinners(cx))
        } else {
            None
        };

        v_flex()
            .key_context(CONTEXT)
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|_, _: &Cancel, _, cx| cx.emit(DismissEvent)))
            .p_3()
            .gap_2()
            .bg(popover_bg)
            .border_1()
            .border_color(border_color)
            .rounded(radius)
            .shadow_lg()
            .min_w(px(280.))
            // Text input at top (always visible)
            .child(
                h_flex()
                    .gap_1()
                    .w_full()
                    .child(Input::new(&input_state).appearance(false).w_full())
                    // Calendar toggle button (for date/datetime modes)
                    .when(mode != DatePickerMode::Time, |this| {
                        this.child(
                            div()
                                .id("toggle-calendar")
                                .cursor_pointer()
                                .p_1()
                                .rounded(radius)
                                .when(popover_open, |s| s.bg(accent_color.opacity(0.2)))
                                .hover(|s| s.bg(muted_bg))
                                .on_click(cx.listener(|state, _, _, cx| {
                                    state.toggle_popover(cx);
                                }))
                                .child(Icon::new(IconName::Calendar).size_4().text_color(
                                    if popover_open { accent_color } else { muted_fg },
                                )),
                        )
                    }),
            )
            // Calendar (shown when popover is open)
            .when_some(calendar_element, |this, calendar| {
                this.child(
                    div()
                        .pt_2()
                        .border_t_1()
                        .border_color(border_color)
                        .child(calendar),
                )
            })
            // Time spinners (for time/datetime modes)
            .when_some(time_spinners_element, |this, spinners| {
                this.child(
                    div()
                        .pt_2()
                        .border_t_1()
                        .border_color(border_color)
                        .child(spinners),
                )
            })
            // Action buttons
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .pt_2()
                    .border_t_1()
                    .border_color(border_color)
                    .child(
                        h_flex()
                            .gap_1()
                            .child(Button::new("now").label("Now").small().ghost().on_click(
                                cx.listener(|state, _, window, cx| {
                                    state.set_now(window, cx);
                                }),
                            ))
                            .when(nullable, |this| {
                                this.child(
                                    Button::new("clear")
                                        .label("Clear")
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|state, _, window, cx| {
                                            state.set_null(window, cx);
                                        })),
                                )
                            }),
                    )
                    .child(Button::new("done").label("OK").small().primary().on_click(
                        cx.listener(|_, _, _, cx| {
                            cx.emit(DismissEvent);
                        }),
                    )),
            )
    }
}

// =============================================================================
// Wrapper Structs for Split Rendering (macOS-style)
// =============================================================================

/// Renders just the inline part of a DatePicker (Input + calendar button)
/// Use this directly in table cells.
///
/// # Example
/// ```rust,ignore
/// div().child(DatePickerInline::new(&date_picker_entity))
/// ```
#[derive(IntoElement)]
pub struct DatePickerInline {
    state: Entity<DatePickerState>,
}

impl DatePickerInline {
    pub fn new(state: &Entity<DatePickerState>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl RenderOnce for DatePickerInline {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let state = self.state.read(cx);
        let theme = cx.theme();

        let input_state = state.input_state.clone();
        let mode = state.mode;
        let popover_open = state.popover_open;

        let accent_color = theme.accent;
        let muted_fg = theme.muted_foreground;
        let muted_bg = theme.muted;
        let radius = theme.radius;

        let state_entity = self.state.clone();

        h_flex()
            .w_full()
            .h_full()
            .items_center()
            .gap_1()
            // Text input - takes most of the width
            .child(
                div()
                    .flex_1()
                    .min_w_0() // Allow shrinking
                    .child(Input::new(&input_state).appearance(false).w_full()),
            )
            // Calendar button (for date/datetime modes)
            .when(mode != DatePickerMode::Time, |this| {
                this.child(
                    div()
                        .id("inline-calendar-btn")
                        .cursor_pointer()
                        .p_1()
                        .rounded(radius)
                        .flex_shrink_0()
                        .when(popover_open, |s| s.bg(accent_color.opacity(0.2)))
                        .hover(|s| s.bg(muted_bg))
                        .on_click({
                            let state_entity = state_entity.clone();
                            move |_, window, cx| {
                                state_entity.update(cx, |state, cx| {
                                    state.toggle_popover(cx);
                                });
                                window.refresh();
                            }
                        })
                        .child(
                            Icon::new(IconName::Calendar)
                                .size_4()
                                .text_color(if popover_open { accent_color } else { muted_fg }),
                        ),
                )
            })
    }
}

/// Renders just the popover content of a DatePicker (calendar + time spinners + buttons)
/// Use this inside an anchored/deferred popover.
///
/// # Example
/// ```rust,ignore
/// deferred(
///     anchored()
///         .snap_to_window_with_margin(px(8.))
///         .child(DatePickerPopover::new(&date_picker_entity))
/// )
/// ```
#[derive(IntoElement)]
pub struct DatePickerPopover {
    state: Entity<DatePickerState>,
}

impl DatePickerPopover {
    pub fn new(state: &Entity<DatePickerState>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl RenderOnce for DatePickerPopover {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        // We render the entity directly - it will use its Render impl
        // But we want ONLY the popover content, not the full standalone view
        // So we use a custom rendering approach

        let state = self.state.read(cx);
        let theme = cx.theme();

        let mode = state.mode;
        let nullable = state.nullable;
        let focus_handle = state.focus_handle.clone();
        let view_year = state.view_year;
        let view_month = state.view_month;
        let text_value = state.text_value.clone();

        let popover_bg = theme.popover;
        let border_color = theme.border;
        let radius = theme.radius;

        let state_entity = self.state.clone();
        let state_entity2 = self.state.clone();
        let state_entity3 = self.state.clone();
        let state_entity4 = self.state.clone();
        let state_entity5 = self.state.clone();

        v_flex()
            .key_context(CONTEXT)
            .track_focus(&focus_handle)
            .on_action({
                let state_entity = state_entity.clone();
                move |_: &Cancel, _window, cx| {
                    state_entity.update(cx, |state, cx| {
                        state.close_popover(cx);
                        cx.emit(DismissEvent);
                    });
                }
            })
            .p_3()
            .gap_2()
            .bg(popover_bg)
            .border_1()
            .border_color(border_color)
            .rounded(radius)
            .shadow_lg()
            .min_w(px(220.))
            // Calendar (for date/datetime modes)
            .when(mode != DatePickerMode::Time, |this| {
                this.child(render_calendar_static(
                    view_year,
                    view_month,
                    &text_value,
                    state_entity2.clone(),
                    cx,
                ))
            })
            // Time spinners (for time/datetime modes)
            .when(mode != DatePickerMode::Date, |this| {
                this.child(div().pt_2().border_t_1().border_color(border_color).child(
                    render_time_spinners_static(&text_value, mode, state_entity3.clone(), cx),
                ))
            })
            // Action buttons
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .pt_2()
                    .border_t_1()
                    .border_color(border_color)
                    .child(
                        h_flex()
                            .gap_1()
                            .child(Button::new("now").label("Now").small().ghost().on_click({
                                let state_entity = state_entity4.clone();
                                move |_, window, cx| {
                                    state_entity.update(cx, |state, cx| {
                                        state.set_now(window, cx);
                                    });
                                }
                            }))
                            .when(nullable, |this| {
                                this.child(
                                    Button::new("clear")
                                        .label("Clear")
                                        .small()
                                        .ghost()
                                        .on_click({
                                            let state_entity = state_entity5.clone();
                                            move |_, window, cx| {
                                                state_entity.update(cx, |state, cx| {
                                                    state.set_null(window, cx);
                                                });
                                            }
                                        }),
                                )
                            }),
                    )
                    .child(Button::new("done").label("OK").small().primary().on_click({
                        let state_entity = state_entity.clone();
                        move |_, window, cx| {
                            state_entity.update(cx, |state, cx| {
                                state.close_popover(cx);
                                cx.emit(DismissEvent);
                            });
                            window.refresh();
                        }
                    })),
            )
    }
}

// =============================================================================
// Static Helper Functions for Popover Rendering
// =============================================================================

/// Render calendar grid without needing mutable access to state
fn render_calendar_static(
    view_year: i32,
    view_month: u32,
    text_value: &str,
    state_entity: Entity<DatePickerState>,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();
    let days = days_in_month(view_year, view_month);
    let first_day = first_day_of_month(view_year, view_month);
    let month_name = MONTH_NAMES_SHORT
        .get(view_month as usize - 1)
        .unwrap_or(&"");

    // Get selected day if viewing the same month
    let selected_day = parse_date_parts(text_value)
        .filter(|(y, m, _)| *y == view_year && *m == view_month)
        .map(|(_, _, d)| d);

    // Get today's day if viewing current month
    let today = current_datetime_string(DatePickerMode::Date);
    let today_day = parse_date_parts(&today)
        .filter(|(y, m, _)| *y == view_year && *m == view_month)
        .map(|(_, _, d)| d);

    let state_prev = state_entity.clone();
    let state_next = state_entity.clone();

    v_flex()
        .gap_1()
        // Month/Year header with navigation
        .child(
            h_flex()
                .items_center()
                .justify_between()
                .child(
                    div()
                        .id("prev-month")
                        .cursor_pointer()
                        .p_1()
                        .rounded(theme.radius)
                        .hover(|s| s.bg(theme.muted))
                        .on_click(move |_, _window, cx| {
                            state_prev.update(cx, |state, cx| {
                                state.prev_month(cx);
                            });
                        })
                        .child(Icon::new(IconName::ChevronLeft).size_3()),
                )
                .child(
                    div()
                        .text_xs()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .child(format!("{} {}", month_name, view_year)),
                )
                .child(
                    div()
                        .id("next-month")
                        .cursor_pointer()
                        .p_1()
                        .rounded(theme.radius)
                        .hover(|s| s.bg(theme.muted))
                        .on_click(move |_, _window, cx| {
                            state_next.update(cx, |state, cx| {
                                state.next_month(cx);
                            });
                        })
                        .child(Icon::new(IconName::ChevronRight).size_3()),
                ),
        )
        // Day names header
        .child(h_flex().gap_px().children(DAY_NAMES.iter().map(|name| {
            div()
                .w(px(24.))
                .h(px(20.))
                .flex()
                .items_center()
                .justify_center()
                .text_xs()
                .text_color(theme.muted_foreground)
                .child(*name)
        })))
        // Calendar grid
        .child(render_calendar_grid_static(
            days,
            first_day,
            selected_day,
            today_day,
            state_entity,
            cx,
        ))
        .into_any_element()
}

fn render_calendar_grid_static(
    days: u32,
    first_day: u32,
    selected_day: Option<u32>,
    today_day: Option<u32>,
    state_entity: Entity<DatePickerState>,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();

    // Build 6 weeks of days (42 cells)
    let mut cells: Vec<Option<u32>> = Vec::with_capacity(42);

    // Empty cells before first day
    for _ in 0..first_day {
        cells.push(None);
    }

    // Days of the month
    for day in 1..=days {
        cells.push(Some(day));
    }

    // Empty cells after last day (fill to complete weeks)
    while cells.len() % 7 != 0 || cells.len() < 35 {
        cells.push(None);
    }

    let weeks = cells.len() / 7;
    let primary = theme.primary;
    let primary_fg = theme.primary_foreground;
    let muted = theme.muted;
    let radius = theme.radius;

    v_flex()
        .gap_px()
        .children((0..weeks).map(|week| {
            let week_cells = cells[week * 7..(week + 1) * 7].to_vec();
            let state_entity = state_entity.clone();
            h_flex()
                .gap_px()
                .children(week_cells.into_iter().enumerate().map(|(i, day_opt)| {
                    let cell_id = format!("day-{}-{}", week, i);
                    match day_opt {
                        Some(day) => {
                            let is_selected = selected_day == Some(day);
                            let is_today = today_day == Some(day);
                            let state_for_click = state_entity.clone();

                            div()
                                .id(SharedString::from(cell_id))
                                .w(px(24.))
                                .h(px(24.))
                                .flex()
                                .items_center()
                                .justify_center()
                                .text_xs()
                                .cursor_pointer()
                                .rounded(radius)
                                .when(is_selected, |s| s.bg(primary).text_color(primary_fg))
                                .when(!is_selected && is_today, |s| {
                                    s.border_1().border_color(primary)
                                })
                                .when(!is_selected, |s| s.hover(|s| s.bg(muted)))
                                .on_click(move |_, window, cx| {
                                    state_for_click.update(cx, |state, cx| {
                                        state.select_day(day, window, cx);
                                    });
                                })
                                .child(day.to_string())
                                .into_any_element()
                        }
                        None => div().w(px(24.)).h(px(24.)).into_any_element(),
                    }
                }))
        }))
        .into_any_element()
}

fn render_time_spinners_static(
    text_value: &str,
    mode: DatePickerMode,
    state_entity: Entity<DatePickerState>,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();

    // Parse current time from text value
    let time_part = text_value
        .split(|c| c == ' ' || c == 'T')
        .nth(if mode == DatePickerMode::Time { 0 } else { 1 })
        .unwrap_or("00:00:00");

    let time_parts: Vec<&str> = time_part.split(':').collect();
    let hour: u32 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minute: u32 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Handle seconds with potential fractional part
    let (second, micros): (u32, u32) = if let Some(sec_str) = time_parts.get(2) {
        let sec_clean = sec_str
            .split(|c: char| c == '+' || c == '-')
            .next()
            .unwrap_or(sec_str);

        if let Some(dot_pos) = sec_clean.find('.') {
            let sec: u32 = sec_clean[..dot_pos].parse().unwrap_or(0);
            let frac_str = &sec_clean[dot_pos + 1..];
            let frac_padded = format!("{:0<6}", frac_str);
            let micros: u32 = frac_padded[..6].parse().unwrap_or(0);
            (sec, micros)
        } else {
            (sec_clean.parse().unwrap_or(0), 0)
        }
    } else {
        (0, 0)
    };

    h_flex()
        .items_center()
        .gap_1()
        .child(
            div()
                .text_xs()
                .text_color(theme.muted_foreground)
                .child("Time:"),
        )
        .child(render_spinner_static(
            "hour",
            hour,
            23,
            state_entity.clone(),
            cx,
        ))
        .child(div().text_xs().child(":"))
        .child(render_spinner_static(
            "minute",
            minute,
            59,
            state_entity.clone(),
            cx,
        ))
        .child(div().text_xs().child(":"))
        .child(render_spinner_static(
            "second",
            second,
            59,
            state_entity.clone(),
            cx,
        ))
        .child(div().text_xs().child("."))
        .child(render_microseconds_spinner_static(micros, state_entity, cx))
        .into_any_element()
}

fn render_spinner_static(
    field: &'static str,
    value: u32,
    max: u32,
    state_entity: Entity<DatePickerState>,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();
    let state_up = state_entity.clone();
    let state_down = state_entity.clone();

    v_flex()
        .items_center()
        .child(
            div()
                .id(SharedString::from(format!("{}-up", field)))
                .cursor_pointer()
                .rounded(theme.radius)
                .hover(|s| s.bg(theme.muted))
                .on_click(move |_, window, cx| {
                    state_up.update(cx, |state, cx| {
                        state.adjust_time_field(field, 1, max, window, cx);
                    });
                })
                .child(Icon::new(IconName::ChevronUp).size_3()),
        )
        .child(
            div()
                .w(px(24.))
                .text_center()
                .text_xs()
                .font_weight(gpui::FontWeight::MEDIUM)
                .child(format!("{:02}", value)),
        )
        .child(
            div()
                .id(SharedString::from(format!("{}-down", field)))
                .cursor_pointer()
                .rounded(theme.radius)
                .hover(|s| s.bg(theme.muted))
                .on_click(move |_, window, cx| {
                    state_down.update(cx, |state, cx| {
                        state.adjust_time_field(field, -1, max, window, cx);
                    });
                })
                .child(Icon::new(IconName::ChevronDown).size_3()),
        )
        .into_any_element()
}

fn render_microseconds_spinner_static(
    value: u32,
    state_entity: Entity<DatePickerState>,
    cx: &App,
) -> AnyElement {
    let theme = cx.theme();
    let state_up = state_entity.clone();
    let state_down = state_entity.clone();

    v_flex()
        .items_center()
        .child(
            div()
                .id("micros-up")
                .cursor_pointer()
                .rounded(theme.radius)
                .hover(|s| s.bg(theme.muted))
                .on_click(move |_, window, cx| {
                    state_up.update(cx, |state, cx| {
                        state.adjust_microseconds(1000, window, cx);
                    });
                })
                .child(Icon::new(IconName::ChevronUp).size_3()),
        )
        .child(
            div()
                .w(px(48.))
                .text_center()
                .text_xs()
                .font_weight(gpui::FontWeight::MEDIUM)
                .child(format!("{:06}", value)),
        )
        .child(
            div()
                .id("micros-down")
                .cursor_pointer()
                .rounded(theme.radius)
                .hover(|s| s.bg(theme.muted))
                .on_click(move |_, window, cx| {
                    state_down.update(cx, |state, cx| {
                        state.adjust_microseconds(-1000, window, cx);
                    });
                })
                .child(Icon::new(IconName::ChevronDown).size_3()),
        )
        .into_any_element()
}
