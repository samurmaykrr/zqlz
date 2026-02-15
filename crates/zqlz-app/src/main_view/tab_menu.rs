// ! Tab context menu for managing tabs in the center dock

use gpui::*;

/// State for the tab context menu (right-click menu on tabs).
///
/// Manages the popup menu shown when a user right-clicks on a tab in the center dock.
/// Provides actions like Close, Close Others, Close Tabs to Right, and Close All.
pub struct TabContextMenuState {
    pub menu: Entity<zqlz_ui::widgets::menu::PopupMenu>,
    pub open: bool,
    pub position: Point<Pixels>,
    pub tab_index: usize,
    pub _subscriptions: Vec<Subscription>,
    pub menu_subscription: Option<Subscription>,
}

impl TabContextMenuState {
    pub fn new(window: &mut Window, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let menu = zqlz_ui::widgets::menu::PopupMenu::build(window, cx, |menu, _, _| menu);
            Self {
                menu,
                open: false,
                position: Point::default(),
                tab_index: 0,
                _subscriptions: vec![],
                menu_subscription: None,
            }
        })
    }
}

impl Render for TabContextMenuState {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use gpui::{anchored, deferred};
        use zqlz_ui::widgets::ActiveTheme;

        if !self.open {
            return div().into_any_element();
        }

        deferred(
            anchored()
                .snap_to_window_with_margin(px(8.))
                .anchor(Corner::TopLeft)
                .position(self.position)
                .child(
                    div()
                        .occlude()
                        .font_family(cx.theme().font_family.clone())
                        .cursor_default()
                        .child(self.menu.clone()),
                ),
        )
        .with_priority(1)
        .into_any_element()
    }
}
