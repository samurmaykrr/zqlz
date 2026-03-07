//! Context menu for materialized view nodes
//!
//! Provides a context menu for materialized view items in the sidebar.
//! Materialized views are pre-computed query results stored as tables.
//!
//! Available menu items:
//! - **Open**: Opens the materialized view data in a table viewer
//! - **Export Wizard...**: Exports materialized view data
//! - **Copy Name**: Copies the view name to clipboard
//! - **Refresh**: Reloads the schema information

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show materialized view context menu
    ///
    /// Displays a menu for materialized view operations:
    /// - **Open**: Emits `OpenView` to display the materialized view data
    /// - **Export Wizard...**: Emits `ExportData` to export the data
    /// - **Copy Name**: Emits `CopyViewName` to copy the name to clipboard
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a materialized view node.
    pub(in crate::widgets) fn show_materialized_view_context_menu(
        &mut self,
        conn_id: Uuid,
        view_name: String,
        database_name: Option<String>,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.materialized_view_context_menu.is_none() {
            self.materialized_view_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let view_for_menu = view_name.clone();

        if let Some(menu_state) = &self.materialized_view_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.item(PopupMenuItem::new("Open").on_click({
                        let sidebar = sidebar_weak.clone();
                        let view = view_for_menu.clone();
                        let db_name = database_name.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::OpenView {
                                    connection_id: conn_id,
                                    view_name: view.clone(),
                                    database_name: db_name.clone(),
                                });
                            });
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Export Wizard...").on_click({
                        let sidebar = sidebar_weak.clone();
                        let view = view_for_menu.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::ExportData {
                                    connection_id: conn_id,
                                    table_name: view.clone(),
                                });
                            });
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Copy Name").on_click({
                        let sidebar = sidebar_weak.clone();
                        let view = view_for_menu.clone();
                        move |_event, _window, cx| {
                            _ = sidebar.update(cx, |_sidebar, cx| {
                                cx.emit(ConnectionSidebarEvent::CopyViewName {
                                    view_name: view.clone(),
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
