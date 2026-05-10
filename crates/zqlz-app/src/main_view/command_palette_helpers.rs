use crate::actions::*;
use crate::components::{Command, CommandCategory, CommandPaletteEvent};
use gpui::{Context, Window};
use zqlz_core::ConnectionFeatureSet;

use super::MainView;

pub(super) fn build_static_commands() -> Vec<Command> {
    vec![
        // ── Application ─────────────────────────────────────────
        Command::new_static(
            "settings",
            "Open Settings",
            CommandCategory::Application,
            OpenSettings,
        ),
        Command::new_static("refresh", "Refresh", CommandCategory::Application, Refresh),
        Command::new_static(
            "new-window",
            "New Window",
            CommandCategory::Application,
            NewWindow,
        ),
        Command::new_static(
            "close-window",
            "Close Window",
            CommandCategory::Application,
            CloseWindow,
        ),
        Command::new_static(
            "minimize-window",
            "Minimize Window",
            CommandCategory::Application,
            MinimizeWindow,
        ),
        Command::new_static(
            "zoom-window",
            "Zoom Window",
            CommandCategory::Application,
            ZoomWindow,
        ),
        Command::new_static("quit", "Quit", CommandCategory::Application, Quit),
        // ── Connection ──────────────────────────────────────────
        Command::new_static(
            "new-connection",
            "New Connection",
            CommandCategory::Connection,
            NewConnection,
        ),
        Command::new_static(
            "refresh-connection",
            "Refresh Connection",
            CommandCategory::Connection,
            RefreshConnection,
        ),
        Command::new_static(
            "refresh-connections-list",
            "Refresh Connections List",
            CommandCategory::Connection,
            RefreshConnectionsList,
        ),
        // ── Query ───────────────────────────────────────────────
        Command::new_static("new-query", "New Query", CommandCategory::Query, NewQuery),
        Command::new_static(
            "execute-query",
            "Execute Query",
            CommandCategory::Query,
            ExecuteQuery,
        ),
        Command::new_static(
            "execute-selection",
            "Execute Selection",
            CommandCategory::Query,
            ExecuteSelection,
        ),
        Command::new_static(
            "execute-current-statement",
            "Execute Current Statement",
            CommandCategory::Query,
            ExecuteCurrentStatement,
        ),
        Command::new_static(
            "explain-query",
            "Explain Query",
            CommandCategory::Query,
            ExplainQuery,
        ),
        Command::new_static(
            "explain-selection",
            "Explain Selection",
            CommandCategory::Query,
            ExplainSelection,
        ),
        Command::new_static(
            "stop-query",
            "Stop Query",
            CommandCategory::Query,
            StopQuery,
        ),
        Command::new_static(
            "format-query",
            "Format Query",
            CommandCategory::Query,
            zqlz_text_editor::actions::FormatSQL,
        ),
        Command::new_static(
            "save-query",
            "Save Query",
            CommandCategory::Query,
            SaveQuery,
        ),
        Command::new_static(
            "save-query-as",
            "Save Query As…",
            CommandCategory::Query,
            SaveQueryAs,
        ),
        Command::new_static(
            "toggle-problems-panel",
            "Toggle Problems Panel",
            CommandCategory::Query,
            ToggleProblemsPanel,
        ),
        // ── Editor ──────────────────────────────────────────────
        Command::new_static(
            "toggle-line-comment",
            "Toggle Line Comment",
            CommandCategory::Editor,
            zqlz_text_editor::actions::ToggleLineComment,
        ),
        Command::new_static(
            "delete-line",
            "Delete Line",
            CommandCategory::Editor,
            zqlz_text_editor::actions::DeleteLine,
        ),
        Command::new_static(
            "move-line-up",
            "Move Line Up",
            CommandCategory::Editor,
            zqlz_text_editor::actions::MoveLineUp,
        ),
        Command::new_static(
            "move-line-down",
            "Move Line Down",
            CommandCategory::Editor,
            zqlz_text_editor::actions::MoveLineDown,
        ),
        Command::new_static(
            "find-next",
            "Find Next",
            CommandCategory::Editor,
            zqlz_text_editor::actions::FindNext,
        ),
        Command::new_static(
            "find-previous",
            "Find Previous",
            CommandCategory::Editor,
            zqlz_text_editor::actions::FindPrevious,
        ),
        // ── Layout ──────────────────────────────────────────────
        Command::new_static(
            "toggle-left-sidebar",
            "Toggle Left Sidebar",
            CommandCategory::Layout,
            ToggleLeftSidebar,
        ),
        Command::new_static(
            "toggle-right-sidebar",
            "Toggle Right Sidebar",
            CommandCategory::Layout,
            ToggleRightSidebar,
        ),
        Command::new_static(
            "toggle-bottom-panel",
            "Toggle Bottom Panel",
            CommandCategory::Layout,
            ToggleBottomPanel,
        ),
        Command::new_static(
            "toggle-all-docks",
            "Toggle All Docks",
            CommandCategory::Layout,
            ToggleAllDocks,
        ),
        // ── Tab ─────────────────────────────────────────────────
        Command::new_static(
            "next-tab",
            "Next Tab",
            CommandCategory::Tab,
            ActivateNextTab,
        ),
        Command::new_static(
            "previous-tab",
            "Previous Tab",
            CommandCategory::Tab,
            ActivatePrevTab,
        ),
        Command::new_static(
            "last-tab",
            "Last Tab",
            CommandCategory::Tab,
            ActivateLastTab,
        ),
        Command::new_static(
            "activate-tab-1",
            "Activate Tab 1",
            CommandCategory::Tab,
            ActivateTab1,
        ),
        Command::new_static(
            "activate-tab-2",
            "Activate Tab 2",
            CommandCategory::Tab,
            ActivateTab2,
        ),
        Command::new_static(
            "activate-tab-3",
            "Activate Tab 3",
            CommandCategory::Tab,
            ActivateTab3,
        ),
        Command::new_static(
            "activate-tab-4",
            "Activate Tab 4",
            CommandCategory::Tab,
            ActivateTab4,
        ),
        Command::new_static(
            "activate-tab-5",
            "Activate Tab 5",
            CommandCategory::Tab,
            ActivateTab5,
        ),
        Command::new_static(
            "activate-tab-6",
            "Activate Tab 6",
            CommandCategory::Tab,
            ActivateTab6,
        ),
        Command::new_static(
            "activate-tab-7",
            "Activate Tab 7",
            CommandCategory::Tab,
            ActivateTab7,
        ),
        Command::new_static(
            "activate-tab-8",
            "Activate Tab 8",
            CommandCategory::Tab,
            ActivateTab8,
        ),
        Command::new_static(
            "activate-tab-9",
            "Activate Tab 9",
            CommandCategory::Tab,
            ActivateTab9,
        ),
        Command::new_static(
            "close-tab",
            "Close Tab",
            CommandCategory::Tab,
            CloseActiveTab,
        ),
        Command::new_static(
            "close-editor",
            "Close Editor",
            CommandCategory::Tab,
            CloseEditor,
        ),
        Command::new_static(
            "close-other-tabs",
            "Close Other Tabs",
            CommandCategory::Tab,
            CloseOtherTabs,
        ),
        Command::new_static(
            "close-tabs-to-right",
            "Close Tabs to the Right",
            CommandCategory::Tab,
            CloseTabsToRight,
        ),
        Command::new_static(
            "close-tabs-to-left",
            "Close Tabs to the Left",
            CommandCategory::Tab,
            CloseTabsToLeft,
        ),
        Command::new_static(
            "close-clean-tabs",
            "Close Clean Tabs",
            CommandCategory::Tab,
            CloseCleanTabs,
        ),
        Command::new_static(
            "close-all-tabs",
            "Close All Tabs",
            CommandCategory::Tab,
            CloseAllTabs,
        ),
        Command::new_static("pin-tab", "Pin Tab", CommandCategory::Tab, PinTab),
        Command::new_static("unpin-tab", "Unpin Tab", CommandCategory::Tab, UnpinTab),
        Command::new_static(
            "pin-active-tab",
            "Pin or Unpin Active Tab",
            CommandCategory::Tab,
            TogglePinActiveTab,
        ),
        Command::new_static(
            "mark-active-tab-preview",
            "Mark Active Tab as Preview",
            CommandCategory::Tab,
            MarkActiveTabAsPreview,
        ),
        Command::new_static(
            "clear-active-tab-preview",
            "Clear Active Tab Preview",
            CommandCategory::Tab,
            ClearActiveTabPreview,
        ),
        Command::new_static(
            "navigate-tab-back",
            "Go Back",
            CommandCategory::Tab,
            NavigateTabBack,
        ),
        Command::new_static(
            "navigate-tab-forward",
            "Go Forward",
            CommandCategory::Tab,
            NavigateTabForward,
        ),
        // ── Focus ───────────────────────────────────────────────
        Command::new_static(
            "focus-editor",
            "Focus Editor",
            CommandCategory::Focus,
            FocusEditor,
        ),
        Command::new_static(
            "focus-results",
            "Focus Results",
            CommandCategory::Focus,
            FocusResults,
        ),
        Command::new_static(
            "focus-sidebar",
            "Focus Sidebar",
            CommandCategory::Focus,
            FocusSidebar,
        ),
    ]
}

pub(super) fn build_static_commands_for_features(
    features: Option<&ConnectionFeatureSet>,
) -> Vec<Command> {
    build_static_commands()
        .into_iter()
        .filter(|command| static_command_available(command.id.as_str(), features))
        .collect()
}

fn static_command_available(command_id: &str, features: Option<&ConnectionFeatureSet>) -> bool {
    let Some(features) = features else {
        return true;
    };

    match command_id {
        "new-query" | "execute-query" | "execute-selection" | "execute-current-statement" => {
            features.query.execute.available
        }
        "explain-query" | "explain-selection" => features.query.explain.available,
        "stop-query" => features.query.cancel.available,
        _ => true,
    }
}

pub(super) fn route_command_palette_event(
    main_view: &mut MainView,
    event: &CommandPaletteEvent,
    window: &mut Window,
    cx: &mut Context<MainView>,
) {
    match event {
        CommandPaletteEvent::Dismissed => {
            main_view.begin_dismiss_command_palette(cx);
        }
        CommandPaletteEvent::CommandExecuted(command_id) => {
            tracing::debug!(command_id = %command_id, "Command executed from palette");
        }
        CommandPaletteEvent::ConnectToConnection(connection_id) => {
            main_view.connect_to_database(*connection_id, window, cx);
        }
        CommandPaletteEvent::OpenTable {
            connection_id,
            table_name,
        } => {
            if main_view
                .sidebar_connection_feature_set(*connection_id, cx)
                .is_some_and(|features| !features.data_editing.browse_rows.available)
            {
                return;
            }
            main_view.open_table_viewer(
                *connection_id,
                table_name.clone(),
                None,
                false,
                window,
                cx,
            );
        }
        CommandPaletteEvent::OpenView {
            connection_id,
            view_name,
        } => {
            if main_view
                .sidebar_connection_feature_set(*connection_id, cx)
                .is_some_and(|features| !features.data_editing.browse_rows.available)
            {
                return;
            }
            main_view.open_table_viewer(*connection_id, view_name.clone(), None, true, window, cx);
        }
        CommandPaletteEvent::GoToDocumentSymbol { line, column } => {
            if let Some(editor) = main_view.active_query_editor(cx) {
                editor.update(cx, |editor, cx| {
                    editor.go_to_line(*line, *column, window, cx);
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{
        DataEditingFeatureSet, DriverCategory, FeatureAvailability, ObjectFeatureSet,
        QueryFeatureSet, StoreFeatureSet,
    };

    fn feature_set_with_query_support(
        execute: bool,
        explain: bool,
        cancel: bool,
    ) -> ConnectionFeatureSet {
        let available_if = |available, reason| {
            if available {
                FeatureAvailability::available()
            } else {
                FeatureAvailability::unavailable(reason)
            }
        };

        ConnectionFeatureSet {
            driver_name: "test".to_string(),
            driver_category: DriverCategory::Relational,
            schema_introspection: FeatureAvailability::available(),
            query: QueryFeatureSet {
                execute: available_if(execute, "execute unavailable"),
                execute_multiple_statements: available_if(execute, "multi unavailable"),
                explain: available_if(explain, "explain unavailable"),
                cancel: available_if(cancel, "cancel unavailable"),
            },
            data_editing: DataEditingFeatureSet {
                browse_rows: FeatureAvailability::available(),
                edit_cells: FeatureAvailability::available(),
                insert_rows: FeatureAvailability::available(),
                delete_rows: FeatureAvailability::available(),
            },
            objects: ObjectFeatureSet {
                browse_objects: FeatureAvailability::available(),
                create_objects: FeatureAvailability::available(),
                edit_objects: FeatureAvailability::available(),
                delete_objects: FeatureAvailability::available(),
                available_kinds: Vec::new(),
                available_actions: Vec::new(),
            },
            stores: StoreFeatureSet {
                key_value: FeatureAvailability::unavailable("key-value unavailable"),
                document: FeatureAvailability::unavailable("document unavailable"),
            },
        }
    }

    #[test]
    fn static_commands_follow_query_capabilities() {
        let features = feature_set_with_query_support(false, false, false);
        let commands = build_static_commands_for_features(Some(&features));
        let ids = commands
            .iter()
            .map(|command| command.id.as_str())
            .collect::<Vec<_>>();

        assert!(!ids.contains(&"new-query"));
        assert!(!ids.contains(&"execute-query"));
        assert!(!ids.contains(&"explain-query"));
        assert!(!ids.contains(&"stop-query"));
        assert!(ids.contains(&"settings"));
    }

    #[test]
    fn static_commands_remain_available_without_feature_set() {
        let ids = build_static_commands_for_features(None)
            .into_iter()
            .map(|command| command.id)
            .collect::<Vec<_>>();

        assert!(ids.contains(&"execute-query".to_string()));
        assert!(ids.contains(&"explain-query".to_string()));
        assert!(ids.contains(&"stop-query".to_string()));
    }
}
