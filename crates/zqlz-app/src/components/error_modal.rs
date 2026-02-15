//! Error modal for displaying critical errors and panics
//!
//! This component shows a large, prominent modal when the application
//! encounters a critical error or panic.

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, Sizable, StyledExt,
    button::{Button, ButtonVariants},
    h_flex, v_flex,
};

/// Data for displaying in the error modal
#[derive(Clone, Debug)]
pub struct ErrorData {
    pub title: String,
    pub message: String,
    pub details: Option<String>,
    pub timestamp: Option<String>,
    pub location: Option<String>,
    pub backtrace: Option<String>,
    pub system_info: Option<SystemInfoDisplay>,
}

#[derive(Clone, Debug)]
pub struct SystemInfoDisplay {
    pub os: String,
    pub arch: String,
    pub app_version: String,
}

impl ErrorData {
    /// Create a generic error
    pub fn new(title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            message: message.into(),
            details: None,
            timestamp: None,
            location: None,
            backtrace: None,
            system_info: None,
        }
    }

    /// Add optional details
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Add timestamp
    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = Some(timestamp.into());
        self
    }

    /// Add location information
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Add backtrace
    pub fn with_backtrace(mut self, backtrace: impl Into<String>) -> Self {
        self.backtrace = Some(backtrace.into());
        self
    }

    /// Add system information
    pub fn with_system_info(mut self, info: SystemInfoDisplay) -> Self {
        self.system_info = Some(info);
        self
    }
}

/// Events emitted by the error modal
#[derive(Clone, Debug)]
pub enum ErrorModalEvent {
    /// User closed the modal
    Close,
    /// User wants to copy error details
    CopyDetails,
    /// User wants to report the error
    ReportError,
}

/// Large error modal for displaying critical errors
pub struct ErrorModal {
    focus_handle: FocusHandle,
    error_data: ErrorData,
    show_full_backtrace: bool,
}

impl ErrorModal {
    pub fn new(error_data: ErrorData, cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            error_data,
            show_full_backtrace: false,
        }
    }

    fn toggle_backtrace(&mut self, cx: &mut Context<Self>) {
        self.show_full_backtrace = !self.show_full_backtrace;
        cx.notify();
    }

    fn copy_details(&mut self, cx: &mut Context<Self>) {
        let details = self.format_full_error_report();
        cx.write_to_clipboard(ClipboardItem::new_string(details));
        cx.emit(ErrorModalEvent::CopyDetails);
    }

    fn format_full_error_report(&self) -> String {
        let mut report = String::new();

        report.push_str("ZQLZ ERROR REPORT\n");
        report.push_str(
            "================================================================================\n\n",
        );

        report.push_str(&format!("Title: {}\n\n", self.error_data.title));
        report.push_str(&format!("Message:\n{}\n\n", self.error_data.message));

        if let Some(timestamp) = &self.error_data.timestamp {
            report.push_str(&format!("Timestamp: {}\n\n", timestamp));
        }

        if let Some(location) = &self.error_data.location {
            report.push_str(&format!("Location: {}\n\n", location));
        }

        if let Some(details) = &self.error_data.details {
            report.push_str(&format!("Details:\n{}\n\n", details));
        }

        if let Some(system_info) = &self.error_data.system_info {
            report.push_str("System Information:\n");
            report.push_str(&format!("  OS: {}\n", system_info.os));
            report.push_str(&format!("  Architecture: {}\n", system_info.arch));
            report.push_str(&format!("  App Version: {}\n\n", system_info.app_version));
        }

        if let Some(backtrace) = &self.error_data.backtrace {
            report.push_str(&format!("Backtrace:\n{}\n\n", backtrace));
        }

        report.push_str(
            "================================================================================\n",
        );

        report
    }

    fn render_header(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .items_center()
            .justify_between()
            .gap_4()
            .p_6()
            .bg(theme.danger)
            .child(
                h_flex()
                    .items_center()
                    .gap_3()
                    .child(
                        Icon::new(IconName::TriangleAlert)
                            .size_8()
                            .text_color(theme.danger_foreground),
                    )
                    .child(
                        div()
                            .text_xl()
                            .font_semibold()
                            .text_color(theme.danger_foreground)
                            .child(self.error_data.title.clone()),
                    ),
            )
    }

    fn render_message_section(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_2()
            .child(
                div()
                    .text_lg()
                    .font_medium()
                    .text_color(theme.foreground)
                    .child("Error Message"),
            )
            .child(
                div()
                    .p_4()
                    .rounded_md()
                    .bg(theme.muted)
                    .border_1()
                    .border_color(theme.border)
                    .text_color(theme.foreground)
                    .font_family(cx.theme().mono_font_family.clone())
                    .text_sm()
                    .child(self.error_data.message.clone()),
            )
    }

    fn render_metadata_section(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_3()
            .when_some(self.error_data.timestamp.clone(), |this, timestamp| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .font_semibold()
                                .text_color(theme.muted_foreground)
                                .child("Timestamp:"),
                        )
                        .child(div().text_color(theme.foreground).child(timestamp)),
                )
            })
            .when_some(self.error_data.location.clone(), |this, location| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .font_semibold()
                                .text_color(theme.muted_foreground)
                                .child("Location:"),
                        )
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_sm()
                                .child(location),
                        ),
                )
            })
            .when_some(self.error_data.system_info.clone(), |this, info| {
                this.child(
                    v_flex()
                        .gap_2()
                        .child(
                            div()
                                .font_semibold()
                                .text_color(theme.muted_foreground)
                                .child("System Information:"),
                        )
                        .child(
                            v_flex()
                                .pl_4()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.foreground)
                                        .child(format!("OS: {}", info.os)),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.foreground)
                                        .child(format!("Architecture: {}", info.arch)),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.foreground)
                                        .child(format!("App Version: {}", info.app_version)),
                                ),
                        ),
                )
            })
    }

    fn render_backtrace_section(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .w_full()
            .gap_2()
            .when_some(self.error_data.backtrace.clone(), |this, backtrace| {
                let truncated_lines: Vec<&str> = backtrace.lines().take(20).collect();
                let total_lines = backtrace.lines().count();
                let is_truncated = total_lines > 20;

                this.child(
                    h_flex()
                        .items_center()
                        .justify_between()
                        .child(
                            div()
                                .text_base()
                                .font_medium()
                                .text_color(theme.foreground)
                                .child("Stack Trace"),
                        )
                        .when(is_truncated, |this| {
                            this.child(
                                Button::new("toggle-backtrace")
                                    .ghost()
                                    .xsmall()
                                    .child(if self.show_full_backtrace {
                                        "Show Less".to_string()
                                    } else {
                                        format!("Show All ({} lines)", total_lines)
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.toggle_backtrace(cx);
                                    })),
                            )
                        }),
                )
                .child(
                    div()
                        .p_4()
                        .rounded_md()
                        .bg(theme.muted)
                        .border_1()
                        .border_color(theme.border)
                        .text_color(theme.foreground)
                        .font_family(cx.theme().mono_font_family.clone())
                        .text_xs()
                        .overflow_hidden()
                        .max_h(px(400.0))
                        .id("backtrace-scroll")
                        .occlude()
                        .child(if self.show_full_backtrace {
                            backtrace.clone()
                        } else {
                            truncated_lines.join("\n")
                        }),
                )
            })
    }

    fn render_actions(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .w_full()
            .justify_end()
            .gap_3()
            .child(
                Button::new("copy-details")
                    .outline()
                    .icon(IconName::Copy)
                    .child("Copy Error Details")
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.copy_details(cx);
                    })),
            )
            .child(
                Button::new("close")
                    .primary()
                    .child("Close")
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(ErrorModalEvent::Close);
                    })),
            )
    }
}

impl EventEmitter<ErrorModalEvent> for ErrorModal {}

impl Focusable for ErrorModal {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ErrorModal {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let background = theme.background;
        let danger = theme.danger;
        let border = theme.border;
        let foreground = theme.foreground;
        let muted = theme.muted;

        // Full screen overlay
        div()
            .absolute()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(gpui::rgba(0x00_00_00_CC))
            .child(
                // Main error modal container
                v_flex()
                    .w(px(900.0))
                    .max_h(px(700.0))
                    .bg(background)
                    .border_1()
                    .border_color(danger)
                    .rounded_lg()
                    .shadow_2xl()
                    .overflow_hidden()
                    .child(self.render_header(window, cx))
                    .child(
                        // Scrollable content area
                        v_flex()
                            .flex_1()
                            .overflow_hidden()
                            .id("error-modal-content")
                            .occlude()
                            .p_6()
                            .gap_6()
                            .child(self.render_message_section(window, cx))
                            .child(self.render_metadata_section(window, cx))
                            .when(self.error_data.details.is_some(), |this| {
                                let details = self.error_data.details.clone().unwrap_or_default();
                                this.child(
                                    v_flex()
                                        .w_full()
                                        .gap_2()
                                        .child(
                                            div()
                                                .text_base()
                                                .font_medium()
                                                .text_color(foreground)
                                                .child("Additional Details"),
                                        )
                                        .child(
                                            div()
                                                .p_4()
                                                .rounded_md()
                                                .bg(muted)
                                                .border_1()
                                                .border_color(border)
                                                .text_color(foreground)
                                                .text_sm()
                                                .child(details),
                                        ),
                                )
                            })
                            .child(self.render_backtrace_section(window, cx)),
                    )
                    .child(
                        // Footer actions
                        div()
                            .p_6()
                            .border_t_1()
                            .border_color(border)
                            .child(self.render_actions(window, cx)),
                    ),
            )
    }
}
