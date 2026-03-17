//! View node context menu
//!
//! Provides a context menu for database view nodes with operations for:
//! - Opening and designing views
//! - Creating, deleting, and duplicating views
//! - Exporting view data
//! - Copying view names and renaming views
//! - Viewing version history

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show view context menu
    ///
    /// Displays a menu for view operations:
    /// - **Open View**: Emits `OpenView` to display view data in a table viewer
    /// - **Design View**: Emits `DesignView` to edit the view's SQL definition
    /// - **New View**: Emits `NewView` to create a new view
    /// - **Delete View**: Emits `DeleteView` to drop the view
    /// - **Duplicate View**: Emits `DuplicateView` to copy the view definition
    /// - **Export Wizard...**: Emits `ExportData` to export view data (reuses table export)
    /// - **Copy**: Emits `CopyViewName` to copy view name to clipboard
    /// - **Rename**: Emits `RenameView` to rename the view
    /// - **View History**: Emits `ViewHistory { object_type: "view" }` to show version history
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a view node.
    pub(in crate::widgets) fn show_view_context_menu(
        &mut self,
        conn_id: Uuid,
        view_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.selected_connection != Some(conn_id) {
            self.select_connection(conn_id, cx);
        }

        let supports_new_view = self.supports_sidebar_section(conn_id, "views");

        if self.view_context_menu.is_none() {
            self.view_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let view_for_menu = view_name.clone();
        let action_context = self.focus_handle.clone();

        if let Some(menu_state) = &self.view_context_menu {
            menu_state.update(cx, |state, cx| {
                state.menu_subscription.take();
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    let menu = menu
                        .action_context(action_context.clone())
                        .max_h(px(400.0))
                        .item(PopupMenuItem::new("Open View").on_click({
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
                        .item(PopupMenuItem::new("Design View").on_click({
                            let sidebar = sidebar_weak.clone();
                            let view = view_for_menu.clone();
                            let object_schema = object_schema.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DesignView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
                                        object_schema: object_schema.clone(),
                                    });
                                });
                            }
                        }));

                    let menu = if supports_new_view {
                        menu.item(PopupMenuItem::new("New View").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::NewView {
                                        connection_id: conn_id,
                                    });
                                });
                            }
                        }))
                    } else {
                        menu
                    };

                    menu.separator()
                        .item(PopupMenuItem::new("Delete View").on_click({
                            let sidebar = sidebar_weak.clone();
                            let view = view_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DeleteView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Duplicate View").on_click({
                            let sidebar = sidebar_weak.clone();
                            let view = view_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DuplicateView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
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
                        .item(PopupMenuItem::new("Copy").on_click({
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
                        .item(PopupMenuItem::new("Rename").on_click({
                            let sidebar = sidebar_weak.clone();
                            let view = view_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::RenameView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("View History").on_click({
                            let sidebar = sidebar_weak.clone();
                            let view = view_for_menu.clone();
                            let object_schema = object_schema.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::ViewHistory {
                                        connection_id: conn_id,
                                        object_name: view.clone(),
                                        object_schema: object_schema.clone(),
                                        object_type: "view".to_string(),
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
