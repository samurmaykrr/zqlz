//! Application menu definitions for ZQLZ
//!
//! Sets up the native application menus for macOS and the AppMenuBar for Windows/Linux.
//! Menus are defined using GPUI's Menu and MenuItem types.

use gpui::{App, Entity, Menu, MenuItem, OsAction};
use zqlz_ui::widgets::menu::AppMenuBar;

use crate::actions::*;

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
        #[cfg(target_os = "macos")]
        Menu {
            name: "ZQLZ".into(),
            disabled: false,
            items: vec![
                MenuItem::action("About ZQLZ", OpenSettings),
                MenuItem::separator(),
                MenuItem::action("Settings...", OpenSettings),
                MenuItem::separator(),
                #[cfg(not(target_os = "windows"))]
                MenuItem::action("Install CLI", InstallCli),
                #[cfg(not(target_os = "windows"))]
                MenuItem::separator(),
                MenuItem::action("Quit ZQLZ", Quit),
            ],
        },
        Menu {
            name: "File".into(),
            disabled: false,
            items: vec![
                MenuItem::action("New Query", NewQuery),
                MenuItem::action("New Window", NewWindow),
                MenuItem::action("New Connection...", NewConnection),
                MenuItem::separator(),
                MenuItem::action("Save", SaveQuery),
                MenuItem::action("Save As...", SaveQueryAs),
                MenuItem::separator(),
                MenuItem::action("Close Editor", CloseEditor),
                MenuItem::action("Close Other Editors", CloseOtherTabs),
                MenuItem::action("Close Clean Editors", CloseCleanTabs),
                MenuItem::action("Close All Editors", CloseAllTabs),
                MenuItem::separator(),
                #[cfg(not(target_os = "macos"))]
                MenuItem::action("Settings...", OpenSettings),
                #[cfg(not(target_os = "macos"))]
                #[cfg(not(target_os = "windows"))]
                MenuItem::action("Install CLI", InstallCli),
                #[cfg(not(target_os = "macos"))]
                MenuItem::separator(),
                #[cfg(not(target_os = "macos"))]
                MenuItem::action("Exit", Quit),
            ],
        },
        Menu {
            name: "Edit".into(),
            disabled: false,
            items: vec![
                MenuItem::os_action("Undo", zqlz_text_editor::actions::Undo, OsAction::Undo),
                MenuItem::os_action("Redo", zqlz_text_editor::actions::Redo, OsAction::Redo),
                MenuItem::separator(),
                MenuItem::os_action("Cut", zqlz_text_editor::actions::Cut, OsAction::Cut),
                MenuItem::os_action("Copy", zqlz_text_editor::actions::Copy, OsAction::Copy),
                MenuItem::os_action("Paste", zqlz_text_editor::actions::Paste, OsAction::Paste),
                MenuItem::separator(),
                MenuItem::action("Find", zqlz_text_editor::actions::OpenFind),
                MenuItem::action(
                    "Find and Replace",
                    zqlz_text_editor::actions::OpenFindReplace,
                ),
                MenuItem::separator(),
                MenuItem::action(
                    "Toggle Line Comment",
                    zqlz_text_editor::actions::ToggleLineComment,
                ),
            ],
        },
        Menu {
            name: "Selection".into(),
            disabled: false,
            items: vec![
                MenuItem::os_action(
                    "Select All",
                    zqlz_text_editor::actions::SelectAll,
                    OsAction::SelectAll,
                ),
                MenuItem::action(
                    "Select All Occurrences",
                    zqlz_text_editor::actions::SelectAllOccurrences,
                ),
                MenuItem::separator(),
                MenuItem::action(
                    "Add Cursor Above",
                    zqlz_text_editor::actions::AddCursorAbove,
                ),
                MenuItem::action(
                    "Add Cursor Below",
                    zqlz_text_editor::actions::AddCursorBelow,
                ),
                MenuItem::separator(),
                MenuItem::action("Move Line Up", zqlz_text_editor::actions::MoveLineUp),
                MenuItem::action("Move Line Down", zqlz_text_editor::actions::MoveLineDown),
                MenuItem::action(
                    "Duplicate Selection",
                    zqlz_text_editor::actions::DuplicateLineDown,
                ),
            ],
        },
        Menu {
            name: "Query".into(),
            disabled: false,
            items: vec![
                MenuItem::action("New Query", NewQuery),
                MenuItem::separator(),
                MenuItem::action("Execute Query", ExecuteQuery),
                MenuItem::action("Execute Selection", ExecuteSelection),
                MenuItem::action("Execute Current Statement", ExecuteCurrentStatement),
                MenuItem::separator(),
                MenuItem::action("Explain Query", ExplainQuery),
                MenuItem::action("Explain Selection", ExplainSelection),
                MenuItem::action("Explain Analyze Query", ExplainAnalyzeQuery),
                MenuItem::action("Explain Analyze Selection", ExplainAnalyzeSelection),
                MenuItem::separator(),
                MenuItem::action("Stop Execution", StopQuery),
            ],
        },
        Menu {
            name: "Connection".into(),
            disabled: false,
            items: vec![
                MenuItem::action("New Connection...", NewConnection),
                MenuItem::separator(),
                MenuItem::action("Refresh Connection", RefreshConnection),
                MenuItem::action("Refresh Connections List", RefreshConnectionsList),
            ],
        },
        Menu {
            name: "View".into(),
            disabled: false,
            items: vec![
                MenuItem::action("Command Palette...", OpenCommandPalette),
                MenuItem::separator(),
                MenuItem::action("Toggle Left Sidebar", ToggleLeftSidebar),
                MenuItem::action("Toggle Right Sidebar", ToggleRightSidebar),
                MenuItem::action("Toggle Bottom Panel", ToggleBottomPanel),
                MenuItem::action("Toggle All Docks", ToggleAllDocks),
                MenuItem::action("Toggle Problems Panel", ToggleProblemsPanel),
                MenuItem::separator(),
                MenuItem::action("Next Tab", ActivateNextTab),
                MenuItem::action("Previous Tab", ActivatePrevTab),
                MenuItem::action("Last Tab", ActivateLastTab),
                MenuItem::action("Pin or Unpin Active Tab", TogglePinActiveTab),
                MenuItem::action("Pin Active Tab", PinTab),
                MenuItem::action("Unpin Active Tab", UnpinTab),
                MenuItem::action("Mark Active Tab as Preview", MarkActiveTabAsPreview),
                MenuItem::action("Clear Active Tab Preview", ClearActiveTabPreview),
                MenuItem::separator(),
                MenuItem::action("Move Tab to New Window", MoveTabToNewWindow),
                MenuItem::separator(),
                MenuItem::action("Close Tabs to the Left", CloseTabsToLeft),
                MenuItem::action("Close Tabs to the Right", CloseTabsToRight),
                MenuItem::action("Close All Tabs", CloseAllTabs),
            ],
        },
        Menu {
            name: "Go".into(),
            disabled: false,
            items: vec![
                MenuItem::action("Back", NavigateTabBack),
                MenuItem::action("Forward", NavigateTabForward),
                MenuItem::separator(),
                MenuItem::action("Focus Sidebar", FocusSidebar),
                MenuItem::action("Focus Editor", FocusEditor),
                MenuItem::action("Focus Results", FocusResults),
                MenuItem::separator(),
                MenuItem::action("Go to Line...", zqlz_text_editor::actions::GoToLine),
                MenuItem::separator(),
                MenuItem::action(
                    "Go to Definition",
                    zqlz_text_editor::actions::GoToDefinition,
                ),
                MenuItem::action("Find References", zqlz_text_editor::actions::FindReferences),
                MenuItem::separator(),
                MenuItem::action("Next Problem", NextProblem),
                MenuItem::action("Previous Problem", PreviousProblem),
            ],
        },
        Menu {
            name: "Window".into(),
            disabled: false,
            items: vec![
                MenuItem::action("New Window", NewWindow),
                MenuItem::action("Move Tab to New Window", MoveTabToNewWindow),
                MenuItem::separator(),
                MenuItem::action("Minimize", MinimizeWindow),
                MenuItem::action("Zoom", ZoomWindow),
                MenuItem::separator(),
                MenuItem::action("Close Editor", CloseEditor),
                MenuItem::action("Close Window", CloseWindow),
            ],
        },
        Menu {
            name: "Help".into(),
            disabled: false,
            items: vec![
                MenuItem::action("About ZQLZ", OpenSettings),
                MenuItem::action("Settings...", OpenSettings),
            ],
        },
    ]);

    // Reload the AppMenuBar to reflect changes
    app_menu_bar.update(cx, |menu_bar, cx| {
        menu_bar.reload(cx);
    });
}
