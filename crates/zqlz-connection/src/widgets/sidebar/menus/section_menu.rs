//! Section header context menus
//!
//! Provides context menus for section headers (Tables, Views, Triggers, etc.)
//! These menus appear when right-clicking on a section header and offer
//! section-appropriate actions like "New Table", "New View", "Refresh", etc.

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show context menu for a section header.
    ///
    /// The menu items vary based on the section type:
    /// - **Tables**: New Table, Refresh
    /// - **Views**: New View, Refresh
    /// - **Triggers**: New Trigger, Refresh
    /// - **Functions**: Refresh
    /// - **Procedures**: Refresh
    /// - **Queries**: Refresh
    pub(in crate::widgets) fn show_section_context_menu(
        &mut self,
        conn_id: Uuid,
        section: &str,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.section_context_menu.is_none() {
            self.section_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();

        if let Some(menu_state) = &self.section_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    let menu = match section {
                        "tables" => menu
                            .item(PopupMenuItem::new("New Table").on_click({
                                let sidebar = sidebar_weak.clone();
                                move |_event, _window, cx| {
                                    _ = sidebar.update(cx, |_sidebar, cx| {
                                        cx.emit(ConnectionSidebarEvent::NewTable {
                                            connection_id: conn_id,
                                        });
                                    });
                                }
                            }))
                            .separator(),
                        "views" => menu
                            .item(PopupMenuItem::new("New View").on_click({
                                let sidebar = sidebar_weak.clone();
                                move |_event, _window, cx| {
                                    _ = sidebar.update(cx, |_sidebar, cx| {
                                        cx.emit(ConnectionSidebarEvent::NewView {
                                            connection_id: conn_id,
                                        });
                                    });
                                }
                            }))
                            .separator(),
                        "triggers" => menu
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
                            .separator(),
                        _ => menu,
                    };

                    menu.item(PopupMenuItem::new("Refresh").on_click({
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
