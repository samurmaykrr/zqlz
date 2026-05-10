use gpui::{AnyElement, App, IntoElement, ParentElement};
use zqlz_ui::widgets::TitleBar;

#[cfg(not(target_os = "macos"))]
use crate::AppMenuBarGlobal;

pub fn workspace_title_bar(
    center_controls: AnyElement,
    trailing_controls: AnyElement,
    cx: &mut App,
) -> impl IntoElement {
    #[cfg(not(target_os = "macos"))]
    {
        let app_menu_bar = cx
            .try_global::<AppMenuBarGlobal>()
            .map(|global| global.0.clone())
            .unwrap_or_else(|| zqlz_ui::widgets::menu::AppMenuBar::new(cx));

        TitleBar::new()
            .on_close_window(|_, window, cx| {
                window.dispatch_action(crate::actions::CloseWindow.boxed_clone(), cx);
            })
            .child(app_menu_bar)
            .child(center_controls)
            .child(trailing_controls)
    }

    #[cfg(target_os = "macos")]
    {
        let _ = cx;
        TitleBar::new()
            .child(center_controls)
            .child(trailing_controls)
    }
}
