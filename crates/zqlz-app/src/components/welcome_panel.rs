//! Welcome panel
//!
//! Displayed when no query tabs are open.

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    body_small, caption,
    dock::{Panel, PanelEvent, TitleStyle},
    h2, h_flex, label, muted_small, v_flex, ActiveTheme,
};

/// Welcome panel shown on startup
#[allow(dead_code)]
pub struct WelcomePanel {
    focus_handle: FocusHandle,
}

#[allow(dead_code)]
impl WelcomePanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
        }
    }

    fn render_action_button(
        id: &str,
        title: &str,
        description: &str,
        shortcut: Option<&str>,
        theme: &zqlz_ui::widgets::theme::ThemeColor,
        cx: &App,
    ) -> impl IntoElement {
        h_flex()
            .id(SharedString::from(id.to_string()))
            .w_full()
            .max_w(px(400.0))
            .p_3()
            .gap_3()
            .rounded_lg()
            .border_1()
            .border_color(theme.border)
            .cursor_pointer()
            .hover(|this| this.bg(theme.list_hover).border_color(theme.accent))
            .child(
                div()
                    .size_10()
                    .rounded_md()
                    .bg(theme.muted)
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(h2("+").color(theme.accent)),
            )
            .child(
                v_flex()
                    .flex_1()
                    .gap_1()
                    .child(
                        h_flex()
                            .justify_between()
                            .child(label(title.to_string()).weight(FontWeight::SEMIBOLD))
                            .when_some(shortcut, |this, shortcut| {
                                this.child(
                                    div()
                                        .px_2()
                                        .py_0p5()
                                        .rounded(px(4.0))
                                        .bg(theme.muted)
                                        .child(
                                            caption(shortcut.to_string())
                                                .color(theme.muted_foreground),
                                        ),
                                )
                            }),
                    )
                    .child(muted_small(description.to_string(), cx)),
            )
    }
}

impl Render for WelcomePanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("welcome-panel")
            .key_context("WelcomePanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .items_center()
            .justify_center()
            .gap_8()
            .p_8()
            .child(
                v_flex()
                    .items_center()
                    .gap_4()
                    .child(
                        div()
                            .size_16()
                            .rounded_2xl()
                            .bg(theme.accent)
                            .flex()
                            .items_center()
                            .justify_center()
                            .child(h2("Z").weight(FontWeight::BOLD).color(gpui::white())),
                    )
                    .child(
                        v_flex()
                            .items_center()
                            .gap_1()
                            .child(h2("Welcome to ZQLZ").weight(FontWeight::BOLD))
                            .child(
                                body_small("A modern database IDE for developers")
                                    .color(theme.muted_foreground),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .gap_3()
                    .items_center()
                    .child(
                        div().mb_2().child(
                            label("Get Started")
                                .weight(FontWeight::SEMIBOLD)
                                .color(theme.muted_foreground),
                        ),
                    )
                    .child(Self::render_action_button(
                        "new-connection",
                        "New Connection",
                        "Connect to PostgreSQL, MySQL, SQLite, and more",
                        Some("Cmd+N"),
                        theme,
                        cx,
                    ))
                    .child(Self::render_action_button(
                        "open-file",
                        "Open SQL File",
                        "Open an existing .sql file",
                        Some("Cmd+O"),
                        theme,
                        cx,
                    ))
                    .child(Self::render_action_button(
                        "new-query",
                        "New Query",
                        "Create a new SQL query tab",
                        Some("Cmd+T"),
                        theme,
                        cx,
                    )),
            )
            .child(
                v_flex()
                    .gap_2()
                    .items_center()
                    .mt_4()
                    .child(muted_small("No recent connections", cx)),
            )
    }
}

impl Focusable for WelcomePanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for WelcomePanel {}

impl Panel for WelcomePanel {
    fn panel_name(&self) -> &'static str {
        "WelcomePanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Welcome"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
