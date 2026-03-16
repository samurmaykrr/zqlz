//! Context menu for Redis database nodes
//!
//! Provides a context menu for Redis database items (db0, db1, etc.) in the sidebar.
//!
//! Available menu items:
//! - **Open Database**: Opens the Redis database to view all keys
//! - **Refresh**: Reloads the schema information

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show Redis database context menu
    ///
    /// Displays a menu for Redis database operations:
    /// - **Open Database**: Emits `OpenRedisDatabase` to open the database viewer
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a Redis database node (e.g., db0).
    pub(in crate::widgets) fn show_redis_db_context_menu(
        &mut self,
        conn_id: Uuid,
        database_index: u16,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.selected_connection != Some(conn_id) {
            self.select_connection(conn_id, cx);
        }

        if self.redis_db_context_menu.is_none() {
            self.redis_db_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let action_context = self.focus_handle.clone();

        if let Some(menu_state) = &self.redis_db_context_menu {
            menu_state.update(cx, |state, cx| {
                state.menu_subscription.take();
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.action_context(action_context.clone())
                        .item(PopupMenuItem::new("Open Database").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::OpenRedisDatabase {
                                        connection_id: conn_id,
                                        database_index,
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Refresh").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::RefreshSchema {
                                        connection_id: conn_id,
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
                            menu_state.update(cx, |state, cx| {
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
