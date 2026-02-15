//! Connection node context menu
//!
//! Provides a context menu for connection nodes with options that vary based on
//! connection state (connected/disconnected). Includes operations for:
//! - Connecting/disconnecting
//! - Creating new queries
//! - Managing connection settings
//! - Duplicating and deleting connections

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show connection context menu
    ///
    /// Displays a menu with different items depending on connection state:
    ///
    /// **When disconnected:**
    /// - **Connect**: Emits `Connect(conn_id)` to establish a connection
    ///
    /// **When connected:**
    /// - **New Query**: Emits `NewQuery(conn_id)` to open a new query tab
    /// - **Disconnect**: Emits `Disconnect(conn_id)` to close the connection
    ///
    /// **Always available:**
    /// - **Open Settings**: Emits `OpenConnectionSettings(conn_id)` to edit connection details
    /// - **Refresh**: Emits `RefreshConnections` to reload connection list
    /// - **Duplicate Connection**: Emits `DuplicateConnection(conn_id)` to clone this connection
    /// - **Delete Connection**: Emits `DeleteConnection(conn_id)` to remove this connection
    ///
    /// This menu appears when right-clicking on a connection node.
    pub(in crate::widgets) fn show_connection_context_menu(
        &mut self,
        conn_id: Uuid,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let is_connected = self
            .connections
            .iter()
            .find(|c| c.id == conn_id)
            .map(|c| c.is_connected)
            .unwrap_or(false);

        if self.connection_context_menu.is_none() {
            self.connection_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();

        if let Some(menu_state) = &self.connection_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _window, _cx| {
                    let menu = if !is_connected {
                        menu.item(PopupMenuItem::new("Connect").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::Connect(conn_id));
                                });
                            }
                        }))
                        .separator()
                    } else {
                        menu.item(PopupMenuItem::new("New Query").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::NewQuery(conn_id));
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Disconnect").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::Disconnect(conn_id));
                                });
                            }
                        }))
                        .separator()
                    };

                    menu.item(PopupMenuItem::new("Open Settings").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::OpenConnectionSettings(conn_id));
                            });
                        }
                    }))
                    .item(PopupMenuItem::new("Refresh").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::RefreshConnections);
                            });
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Duplicate Connection").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::DuplicateConnection(conn_id));
                            });
                        }
                    }))
                    .item(PopupMenuItem::new("Delete Connection").on_click({
                        let sidebar = sidebar_weak.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::DeleteConnection(conn_id));
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
