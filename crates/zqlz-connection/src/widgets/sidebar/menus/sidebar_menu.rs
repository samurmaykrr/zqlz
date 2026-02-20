//! Sidebar background context menu
//!
//! Provides a context menu for the sidebar background area with options for:
//! - Creating new connections
//! - Closing all active connections
//! - Creating new connection groups
//! - Refreshing the connections list

use gpui::*;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show context menu for the sidebar background area
    ///
    /// Displays a menu with the following items:
    /// - **New Connection**: Emits `AddConnection` to create a new database connection
    /// - **Close All Connections**: Emits `CloseAllConnections` (disabled if no active connections)
    /// - **New Group**: Emits `NewGroup` to create a connection group
    /// - **Refresh**: Emits `RefreshConnections` to reload the connections list
    ///
    /// This menu appears when right-clicking on empty space in the sidebar.
    pub(in crate::widgets) fn show_sidebar_context_menu(
        &mut self,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.sidebar_context_menu.is_none() {
            self.sidebar_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let has_connected = self.connections.iter().any(|c| c.is_connected);

        if let Some(menu_state) = &self.sidebar_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _window, _cx| {
                    menu.item(PopupMenuItem::new("New Connection").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::AddConnection);
                            });
                        }
                    }))
                    .item(
                        PopupMenuItem::new("Close All Connections")
                            .disabled(!has_connected)
                            .on_click({
                                let sidebar = sidebar_weak.clone();
                                move |_event, _window, cx| {
                                    _ = sidebar.update(cx, |_sidebar, cx| {
                                        cx.emit(ConnectionSidebarEvent::CloseAllConnections);
                                    });
                                }
                            }),
                    )
                    .separator()
                    .item(PopupMenuItem::new("New Group").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::NewGroup);
                            });
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Refresh").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::RefreshConnections);
                            });
                        }
                    }))
                });

                let menu_entity = new_menu.clone();
                let menu_state_entity = cx.entity().clone();
                state.menu_subscription = Some(cx.subscribe(
                    &menu_entity,
                    move |_state, _, _event: &DismissEvent, cx| {
                        let menu_state = menu_state_entity.clone();
                        cx.defer(move |cx| {
                            _ = menu_state.update(cx, |state, cx| {
                                state.open = false;
                                cx.notify();
                            });
                        });
                    },
                ));

                state.menu = new_menu.clone();
                state.open = true;

                if !new_menu.focus_handle(cx).contains_focused(window, cx) {
                    new_menu.focus_handle(cx).focus(window, cx);
                }

                cx.notify();
            });
        }
    }
}
