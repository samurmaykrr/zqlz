//! Context menu for saved query nodes
//!
//! This module provides the right-click context menu for saved query items in the sidebar.
//! Saved queries are user-created SQL queries that are stored for later reuse.
//!
//! Available menu items:
//! - **Open**: Opens the saved query in a new editor tab
//! - **Rename**: Renames the saved query
//! - **Delete**: Removes the saved query

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show saved query context menu
    ///
    /// Displays a menu for saved query operations:
    /// - **Open**: Emits `OpenSavedQuery` to open the query in an editor tab
    /// - **Rename**: Emits `RenameSavedQuery` to rename the saved query
    /// - **Delete**: Emits `DeleteSavedQuery` to remove the saved query
    ///
    /// This menu appears when right-clicking on a saved query node.
    pub(in crate::widgets) fn show_query_context_menu(
        &mut self,
        conn_id: Uuid,
        query_id: Uuid,
        query_name: String,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.query_context_menu.is_none() {
            self.query_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let query_name_for_menu = query_name.clone();

        if let Some(menu_state) = &self.query_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.item(PopupMenuItem::new("Open").on_click({
                        let sidebar = sidebar_weak.clone();
                        let name = query_name_for_menu.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::OpenSavedQuery {
                                    connection_id: conn_id,
                                    query_id,
                                    query_name: name.clone(),
                                });
                            });
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Rename").on_click({
                        let sidebar = sidebar_weak.clone();
                        let name = query_name_for_menu.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::RenameSavedQuery {
                                    connection_id: conn_id,
                                    query_id,
                                    query_name: name.clone(),
                                });
                            });
                        }
                    }))
                    .item(PopupMenuItem::new("Delete").on_click({
                        let sidebar = sidebar_weak.clone();
                        let name = query_name_for_menu.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::DeleteSavedQuery {
                                    connection_id: conn_id,
                                    query_id,
                                    query_name: name.clone(),
                                });
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
