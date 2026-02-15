//! Table node context menu
//!
//! Provides a comprehensive context menu for table nodes with operations for:
//! - Opening and designing tables
//! - Creating, deleting, and duplicating tables
//! - Emptying table data
//! - Importing and exporting data
//! - Dumping SQL with or without data
//! - Copying table names and renaming tables

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::state::ContextMenuState;

impl ConnectionSidebar {
    /// Show table context menu
    ///
    /// Displays a comprehensive menu for table operations:
    /// - **Open Table**: Emits `OpenTable` to view table data
    /// - **Design Table**: Emits `DesignTable` to edit table structure
    /// - **New Table**: Emits `NewTable` to create a new table
    /// - **Delete Table**: Emits `DeleteTable` to drop the table
    /// - **Empty Table**: Emits `EmptyTable` to truncate all rows
    /// - **Duplicate Table**: Emits `DuplicateTable` to copy table structure and data
    /// - **Import Wizard...**: Emits `ImportData` to import data from files
    /// - **Export Wizard...**: Emits `ExportData` to export table data
    /// - **Dump SQL (Structure + Data)**: Emits `DumpTableSql { include_data: true }`
    /// - **Dump SQL (Structure Only)**: Emits `DumpTableSql { include_data: false }`
    /// - **Copy Table Name**: Emits `CopyTableName` to copy name to clipboard
    /// - **Rename**: Emits `RenameTable` to rename the table
    /// - **Refresh**: Emits `RefreshSchema` to reload schema information
    ///
    /// This menu appears when right-clicking on a table node.
    pub(in crate::widgets) fn show_table_context_menu(
        &mut self,
        conn_id: Uuid,
        table_name: String,
        database_name: Option<String>,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.table_context_menu.is_none() {
            self.table_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let table_for_menu = table_name.clone();

        if let Some(menu_state) = &self.table_context_menu {
            menu_state.update(cx, |state, cx| {
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    menu.max_h(px(400.0))
                        .item(PopupMenuItem::new("Open Table").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            let db_name = database_name.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::OpenTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                        database_name: db_name.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Design Table").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DesignTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
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
                        .item(PopupMenuItem::new("Delete Table").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DeleteTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Empty Table").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::EmptyTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Duplicate Table").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DuplicateTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Import Wizard...").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::ImportData {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Export Wizard...").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::ExportData {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Dump SQL (Structure + Data)").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DumpTableSql {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                        include_data: true,
                                    });
                                });
                            }
                        }))
                        .item(PopupMenuItem::new("Dump SQL (Structure Only)").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::DumpTableSql {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                        include_data: false,
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Copy Table Name").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::CopyTableName {
                                        table_name: table.clone(),
                                    });
                                });
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Rename").on_click({
                            let sidebar = sidebar_weak.clone();
                            let table = table_for_menu.clone();
                            move |_event, _window, cx| {
                                _ = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::RenameTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
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
