//! Application menu definitions for ZQLZ
//!
//! Sets up the native application menus for macOS and the AppMenuBar for Windows/Linux.
//! Menus are defined using GPUI's Menu and MenuItem types.

use gpui::{App, Entity, Menu, MenuItem};
use zqlz_ui::widgets::menu::AppMenuBar;

use crate::actions::{
    CloseActiveTab, ExecuteQuery, NewConnection, NewQuery, OpenSettings, Quit, StopQuery,
    ToggleBottomPanel, ToggleLeftSidebar, ToggleRightSidebar,
};

/// Initialize the application menus.
///
/// On macOS, this sets up the native menu bar.
/// On Windows/Linux, this creates an AppMenuBar entity that can be displayed in the title bar.
pub fn init(cx: &mut App) -> Entity<AppMenuBar> {
    let app_menu_bar = AppMenuBar::new(cx);
    update_menus(app_menu_bar.clone(), cx);
    app_menu_bar
}

/// Update all application menus.
fn update_menus(app_menu_bar: Entity<AppMenuBar>, cx: &mut App) {
    cx.set_menus(vec![
        // Application menu (macOS only - this is the special app menu)
        #[cfg(target_os = "macos")]
        Menu {
            name: "ZQLZ".into(),
            items: vec![
                MenuItem::action("About ZQLZ", OpenSettings),
                MenuItem::separator(),
                MenuItem::action("Settings...", OpenSettings),
                MenuItem::separator(),
                MenuItem::action("Quit ZQLZ", Quit),
            ],
        },
        // File menu
        Menu {
            name: "File".into(),
            items: vec![
                MenuItem::action("New Query", NewQuery),
                MenuItem::action("New Connection...", NewConnection),
                MenuItem::separator(),
                #[cfg(not(target_os = "macos"))]
                MenuItem::action("Settings...", OpenSettings),
                #[cfg(not(target_os = "macos"))]
                MenuItem::separator(),
                #[cfg(target_os = "macos")]
                MenuItem::action("Close Tab", CloseActiveTab),
                #[cfg(not(target_os = "macos"))]
                MenuItem::action("Exit", Quit),
            ],
        },
        // Edit menu
        Menu {
            name: "Edit".into(),
            items: vec![
                MenuItem::action("Undo", zqlz_ui::widgets::input::Undo),
                MenuItem::action("Redo", zqlz_ui::widgets::input::Redo),
                MenuItem::separator(),
                MenuItem::action("Cut", zqlz_ui::widgets::input::Cut),
                MenuItem::action("Copy", zqlz_ui::widgets::input::Copy),
                MenuItem::action("Paste", zqlz_ui::widgets::input::Paste),
                MenuItem::separator(),
                MenuItem::action("Select All", zqlz_ui::widgets::input::SelectAll),
            ],
        },
        // Query menu
        Menu {
            name: "Query".into(),
            items: vec![
                MenuItem::action("New Query", NewQuery),
                MenuItem::separator(),
                MenuItem::action("Execute Query", ExecuteQuery),
                MenuItem::action("Stop Execution", StopQuery),
            ],
        },
        // Connection menu
        Menu {
            name: "Connection".into(),
            items: vec![MenuItem::action("New Connection...", NewConnection)],
        },
        // View menu
        Menu {
            name: "View".into(),
            items: vec![
                MenuItem::action("Toggle Left Sidebar", ToggleLeftSidebar),
                MenuItem::action("Toggle Right Sidebar", ToggleRightSidebar),
                MenuItem::action("Toggle Bottom Panel", ToggleBottomPanel),
            ],
        },
        // Help menu
        Menu {
            name: "Help".into(),
            items: vec![MenuItem::action("About ZQLZ", OpenSettings)],
        },
    ]);

    // Reload the AppMenuBar to reflect changes
    app_menu_bar.update(cx, |menu_bar, cx| {
        menu_bar.reload(cx);
    });
}
