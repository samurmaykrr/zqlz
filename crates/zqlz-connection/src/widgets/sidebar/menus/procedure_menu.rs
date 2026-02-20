//! Context menu for database stored procedure nodes
//!
//! This module provides the right-click context menu for stored procedure items in the sidebar.
//! Stored procedures are database-stored programs that can perform operations and may or may not return values.
//!
//! Available menu items:
//! - **View Definition**: Opens the procedure's SQL definition in a viewer
//! - **View History**: Shows the version history of the procedure
//! - **Refresh**: Reloads the schema information

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show procedure context menu
    ///
    /// Displays a menu for stored procedure operations:
    /// - **View Definition**: Emits `OpenProcedure` to display the procedure's SQL definition
    /// - **View History**: Emits `ViewHistory { object_type: "procedure" }` to show version history
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a procedure node.
    pub(in crate::widgets) fn show_procedure_context_menu(
        &mut self,
        conn_id: Uuid,
        procedure_name: String,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.procedure_context_menu.is_none() {
            self.procedure_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let procedure_for_menu = procedure_name.clone();

        if let Some(menu_state) = &self.procedure_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.max_h(px(400.0))
                        .item(PopupMenuItem::new("View Definition").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = procedure_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::OpenProcedure {
                                        connection_id: conn_id,
                                        procedure_name: name.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("View History").on_click({
                            let sidebar = sidebar_weak.clone();
                            let name = procedure_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::ViewHistory {
                                        connection_id: conn_id,
                                        object_name: name.clone(),
                                        object_type: "procedure".to_string(),
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
