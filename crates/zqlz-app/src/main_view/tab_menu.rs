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
                menu_subscription: None,
            }
        })
    }

    pub fn show(
        &mut self,
        menu: Entity<zqlz_ui::widgets::menu::PopupMenu>,
        tab_index: usize,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.menu_subscription.take();
        self.position = position;
        self.tab_index = tab_index;
        self.menu = menu.clone();

        let menu_state = cx.entity().clone();
        self.menu_subscription = Some(cx.subscribe(
            &menu,
            move |_state, _, _event: &DismissEvent, cx| {
                let menu_state = menu_state.clone();
                cx.defer(move |cx| {
                    menu_state.update(cx, |state, cx| {
                        state.open = false;
                        cx.notify();
                    });
                });
            },
        ));

        self.open = true;

        if !menu.focus_handle(cx).contains_focused(window, cx) {
            menu.focus_handle(cx).focus(window, cx);
        }

        cx.notify();
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
                .anchor(Anchor::TopLeft)
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
