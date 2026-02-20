//! Context menu for database trigger nodes
//!
//! This module provides the right-click context menu for trigger items in the sidebar.
//! Triggers are database objects that automatically execute in response to certain events on a table or view.
//!
//! Available menu items:
//! - **Edit Raw SQL**: Opens the trigger definition in a raw SQL editor
//! - **Open Designer**: Opens the visual trigger designer
//! - **New Trigger**: Creates a new trigger
//! - **Delete Trigger**: Removes the trigger
//! - **View History**: Shows the version history of the trigger
//! - **Refresh**: Reloads the schema information

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show trigger context menu
    ///
    /// Displays a menu for trigger operations:
    /// - **Edit Raw SQL**: Emits `DesignTrigger` to open the trigger definition in a query editor
    /// - **Open Designer**: Emits `OpenTriggerDesigner` to open the visual trigger editor
    /// - **New Trigger**: Emits `NewTrigger` to create a new trigger
    /// - **Delete Trigger**: Emits `DeleteTrigger` to drop the trigger
    /// - **View History**: Emits `ViewHistory { object_type: "trigger" }` to show version history
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a trigger node.
    pub(in crate::widgets) fn show_trigger_context_menu(
        &mut self,
        conn_id: Uuid,
        trigger_name: String,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.trigger_context_menu.is_none() {
            self.trigger_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let trigger_for_menu = trigger_name.clone();

        if let Some(menu_state) = &self.trigger_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.max_h(px(400.0))
                        .item(PopupMenuItem::new("Edit Raw SQL").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = trigger_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DesignTrigger {
                                        connection_id: conn_id,
                                        trigger_name: name.clone(),
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Open Designer").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = trigger_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::OpenTriggerDesigner {
                                        connection_id: conn_id,
                                        trigger_name: Some(name.clone()),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("New Trigger").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::NewTrigger {
                                        connection_id: conn_id,
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Delete Trigger").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = trigger_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DeleteTrigger {
                                        connection_id: conn_id,
                                        trigger_name: name.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("View History").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = trigger_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::ViewHistory {
                                        connection_id: conn_id,
                                        object_name: name.clone(),
                                        object_type: "trigger".to_string(),
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
