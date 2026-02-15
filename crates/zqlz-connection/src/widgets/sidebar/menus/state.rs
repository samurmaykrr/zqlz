//! Context menu state management
//!
//! This module provides the `ContextMenuState` struct that manages the lifecycle
//! of popup context menus in the connection sidebar. Each context menu type
//! (sidebar, connection, table, view, etc.) has its own `ContextMenuState` instance
//! that handles menu visibility, positioning, and event subscriptions.

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::menu::PopupMenu;
use zqlz_ui::widgets::ActiveTheme;

/// State for context menus
///
/// Manages the state of a single popup context menu, including its position,
/// visibility, and lifecycle. Each context menu type (sidebar, connection, table, etc.)
/// has its own `ContextMenuState` instance.
pub(in crate::widgets) struct ContextMenuState {
    /// The popup menu entity
    pub(in crate::widgets) menu: Entity<PopupMenu>,
    /// Whether the menu is currently visible
    pub(in crate::widgets) open: bool,
    /// Screen position where the menu should appear
    pub(in crate::widgets) position: Point<Pixels>,
    /// Subscriptions to keep alive (unused currently but reserved for future use)
    pub(in crate::widgets) _subscriptions: Vec<Subscription>,
    /// Subscription to the menu's dismiss event
    pub(in crate::widgets) menu_subscription: Option<Subscription>,
}

impl ContextMenuState {
    /// Create a new context menu state with an empty popup menu
    pub(in crate::widgets) fn new(window: &mut Window, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let menu = PopupMenu::build(window, cx, |menu, _, _| menu);
            Self {
                menu,
                open: false,
                position: Point::default(),
                _subscriptions: vec![],
                menu_subscription: None,
            }
        })
    }
}

impl Render for ContextMenuState {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
