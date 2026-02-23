use gpui::{App, Styled};

use crate::widgets::{
    button::{Button, ButtonVariants as _},
    ActiveTheme as _, Icon, IconName, Sizable as _,
};

#[inline]
pub(crate) fn clear_button(cx: &App) -> Button {
    Button::new("clean")
        .icon(Icon::new(IconName::CircleX))
        .ghost()
        .xsmall()
        .tab_stop(false)
        .tooltip("Clear")
        .text_color(cx.theme().muted_foreground)
}
