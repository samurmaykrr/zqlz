// UI rendering methods for MainView

use std::time::Duration;

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    animation::cubic_bezier,
    button::{Button, ButtonVariants},
    caption, h_flex,
    tooltip::Tooltip,
    ActiveTheme, Icon, Sizable, TitleBar, ZqlzIcon,
};

use crate::actions::NewQuery;
use crate::app::AppState;
use crate::components::InspectorView;

#[cfg(not(target_os = "macos"))]
use crate::AppMenuBarGlobal;

use super::MainView;

impl MainView {
    /// Render the command palette as an overlay with open/close animations.
    pub(super) fn render_command_palette_overlay(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement> {
        let palette = self.command_palette.as_ref()?;
        let theme = cx.theme();
        let closing = self.command_palette_closing;

        let animation = Animation::new(Duration::from_secs_f64(0.2))
            .with_easing(cubic_bezier(0.32, 0.72, 0., 1.));

        Some(
            div()
                .id("command-palette-overlay")
                .occlude()
                .absolute()
                .inset_0()
                .on_mouse_down(MouseButton::Left, |_event, _, _| {
                    // Prevent propagation to elements behind
                })
                .on_scroll_wheel(|_event, _, _| {
                    // Prevent scroll from reaching background
                })
                // Semi-transparent backdrop
                .child(
                    div()
                        .id("command-palette-backdrop")
                        .absolute()
                        .inset_0()
                        .bg(theme.overlay)
                        .on_mouse_down(MouseButton::Left, {
                            cx.listener(|this, _event, _window, cx| {
                                this.begin_dismiss_command_palette(cx);
                            })
                        }),
                )
                // Centered palette container with slide + shadow animation
                .child(
                    div()
                        .absolute()
                        .top(px(80.0))
                        .left_0()
                        .right_0()
                        .flex()
                        .justify_center()
                        .child(palette.clone())
                        .with_animation(
                            ElementId::NamedInteger("palette-slide".into(), closing as u64),
                            animation.clone(),
                            move |this, delta| {
                                if closing {
                                    let y_offset = delta * px(-12.0);
                                    this.top(px(80.0) + y_offset).opacity(1.0 - delta)
                                } else {
                                    let y_offset = px(-20.0) + delta * px(20.0);
                                    this.top(px(80.0) + y_offset).opacity(delta)
                                }
                            },
                        ),
                )
                // Fade the entire overlay (backdrop + content together)
                .with_animation(
                    ElementId::NamedInteger("palette-fade".into(), closing as u64),
                    animation,
                    move |this, delta| {
                        if closing {
                            this.opacity(1.0 - delta)
                        } else {
                            this.opacity(delta)
                        }
                    },
                ),
        )
    }
    /// Render the title bar with menu items
    pub(super) fn render_title_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        // On Windows/Linux, include the AppMenuBar in the title bar
        #[cfg(not(target_os = "macos"))]
        let app_menu_bar = cx.try_global::<AppMenuBarGlobal>().map(|g| g.0.clone());

        #[cfg(not(target_os = "macos"))]
        {
            TitleBar::new()
                .child(app_menu_bar.unwrap_or_else(|| {
                    // Fallback: create a new AppMenuBar if not found
                    zqlz_ui::widgets::menu::AppMenuBar::new(cx)
                }))
                .child(self.render_title_bar_buttons(cx))
                .child(self.render_title_bar_settings(cx))
        }

        #[cfg(target_os = "macos")]
        {
            TitleBar::new()
                .child(self.render_title_bar_buttons(cx))
                .child(self.render_title_bar_settings(cx))
        }
    }

    fn render_title_bar_buttons(&self, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .w_full()
            .items_center()
            .gap_2()
            .child(
                Button::new("new-connection")
                    .ghost()
                    .small()
                    .label("+ Connection")
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_new_connection_dialog(window, cx);
                    })),
            )
            .child(
                Button::new("new-query")
                    .ghost()
                    .small()
                    .label("+ Query")
                    .on_click(cx.listener(|this, _, window, cx| {
                        tracing::info!("New query button clicked");
                        this.handle_new_query(&NewQuery, window, cx);
                    })),
            )
    }

    fn render_title_bar_settings(&self, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex().items_center().child(
            Button::new("settings")
                .ghost()
                .small()
                .label("Settings")
                .on_click(cx.listener(|this, _, window, cx| {
                    this.open_settings_panel(window, cx);
                })),
        )
    }

    /// Render the status bar
    pub(super) fn render_status_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let bg = theme.background;
        let border = theme.border;
        let muted_fg = theme.muted_foreground;

        let connection_count = if let Some(app_state) = cx.try_global::<AppState>() {
            app_state.saved_connections().len()
        } else {
            0
        };

        let connected_status = if let Some(id) = self.active_connection_id(cx) {
            if let Some(app_state) = cx.try_global::<AppState>() {
                if app_state.is_connected(id) {
                    "Connected"
                } else {
                    "Disconnected"
                }
            } else {
                "No Connection"
            }
        } else {
            "No Connection"
        };

        let active_view = self.inspector_panel.read(cx).active_view();

        h_flex()
            .w_full()
            .h(px(24.0))
            .px_4()
            .items_center()
            .justify_between()
            .bg(bg)
            .border_t_1()
            .border_color(border)
            .text_xs()
            .text_color(muted_fg)
            .child(
                caption(format!(
                    "{} | {} saved connections",
                    connected_status, connection_count
                ))
                .color(muted_fg),
            )
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(self.render_status_bar_icon_tab(
                        InspectorView::Schema,
                        ZqlzIcon::Database,
                        "Schema",
                        active_view,
                        cx,
                    ))
                    .child(self.render_status_bar_icon_tab(
                        InspectorView::CellEditor,
                        ZqlzIcon::Pencil,
                        "Cell Editor",
                        active_view,
                        cx,
                    ))
                    .child(self.render_status_bar_icon_tab(
                        InspectorView::KeyEditor,
                        ZqlzIcon::Key,
                        "Key Editor",
                        active_view,
                        cx,
                    ))
                    .child(self.render_status_bar_icon_tab(
                        InspectorView::QueryHistory,
                        ZqlzIcon::Clock,
                        "Query History",
                        active_view,
                        cx,
                    ))
                    .child(caption("ZQLZ v0.1.0").color(muted_fg).ml_2()),
            )
    }

    /// Render a single inspector view toggle icon for the status bar
    fn render_status_bar_icon_tab(
        &self,
        view: InspectorView,
        icon: ZqlzIcon,
        tooltip_text: &'static str,
        active_view: InspectorView,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_active = active_view == view;
        let theme = cx.theme();

        div()
            .id(SharedString::from(format!("status-inspector-{:?}", view)))
            .cursor_pointer()
            .p(px(3.0))
            .rounded_sm()
            .hover(|style| style.bg(theme.list_hover))
            .when(is_active, |style| style.text_color(theme.accent))
            .when(!is_active, |style| style.text_color(theme.muted_foreground))
            .child(Icon::new(icon).small())
            .tooltip(move |window, cx| Tooltip::new(tooltip_text).build(window, cx))
            .on_click(cx.listener(move |this, _, _window, cx| {
                this.inspector_panel.update(cx, |panel, cx| {
                    panel.set_active_view(view, cx);
                });
                cx.notify();
            }))
    }
}
