//! Keymap loading system for ZQLZ
//!
//! Loads keyboard shortcuts from JSON files based on the current OS.
//! The keymap files are stored in `assets/keymaps/` directory.

use gpui::{App, KeyBinding};
use serde::Deserialize;
use std::collections::HashMap;

// Embed keymap files at compile time
#[cfg(target_os = "macos")]
const DEFAULT_KEYMAP: &str = include_str!("../assets/keymaps/default-macos.json");

#[cfg(target_os = "linux")]
const DEFAULT_KEYMAP: &str = include_str!("../assets/keymaps/default-linux.json");

#[cfg(target_os = "windows")]
const DEFAULT_KEYMAP: &str = include_str!("../assets/keymaps/default-windows.json");

// Fallback for other platforms
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const DEFAULT_KEYMAP: &str = include_str!("../assets/keymaps/default-linux.json");

/// A section in the keymap file
#[derive(Debug, Deserialize)]
struct KeymapSection {
    /// Optional context predicate (e.g., "MainView", "QueryEditor")
    context: Option<String>,
    /// Key bindings mapping keystroke to action name
    bindings: HashMap<String, KeymapAction>,
}

/// An action in the keymap - can be a string or null (to unbind)
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum KeymapAction {
    /// Action name like "zqlz::OpenSettings"
    Action(String),
    /// Null to unbind a key
    Null,
}

/// Load and register all keybindings from the default keymap file
pub fn load_keymaps(cx: &mut App) {
    tracing::info!("Loading keymaps...");

    let sections: Vec<KeymapSection> = match serde_json::from_str(DEFAULT_KEYMAP) {
        Ok(sections) => sections,
        Err(e) => {
            tracing::error!("Failed to parse keymap JSON: {}", e);
            return;
        }
    };

    let mut total_bindings = 0;

    for section in sections {
        let context = section.context.as_deref();
        tracing::debug!(
            "Processing keymap section with context: {:?}",
            context.unwrap_or("global")
        );

        for (keystroke, action) in section.bindings {
            let action_name = match action {
                KeymapAction::Action(name) => name,
                KeymapAction::Null => {
                    tracing::debug!("Skipping null action for keystroke: {}", keystroke);
                    continue;
                }
            };

            if bind_action(cx, &keystroke, &action_name, context) {
                tracing::debug!(
                    "Bound {} -> {} (context: {:?})",
                    keystroke,
                    action_name,
                    context
                );
                total_bindings += 1;
            } else {
                tracing::warn!(
                    "Unknown action '{}' for keystroke '{}' - skipping",
                    action_name,
                    keystroke
                );
            }
        }
    }

    tracing::info!("Loaded {} keybindings", total_bindings);
}

/// Bind a single action by name.
/// Returns true if the action was found and bound, false otherwise.
fn bind_action(cx: &mut App, keystroke: &str, action_name: &str, context: Option<&str>) -> bool {
    use crate::actions::*;
    use zqlz_text_editor::actions as editor;

    // Macro to reduce boilerplate - creates the binding for a given action type
    macro_rules! bind {
        ($action:expr) => {{
            cx.bind_keys([KeyBinding::new(keystroke, $action, context)]);
            true
        }};
    }

    match action_name {
        // === zqlz namespace (application-level actions) ===
        "zqlz::OpenSettings" => bind!(OpenSettings),
        "zqlz::OpenCommandPalette" => bind!(OpenCommandPalette),
        "zqlz::Quit" => bind!(Quit),
        "zqlz::NewConnection" => bind!(NewConnection),
        "zqlz::RefreshConnection" => bind!(RefreshConnection),
        "zqlz::RefreshConnectionsList" => bind!(RefreshConnectionsList),
        "zqlz::NewQuery" => bind!(NewQuery),
        "zqlz::ExecuteQuery" => bind!(ExecuteQuery),
        "zqlz::ExecuteSelection" => bind!(ExecuteSelection),
        "zqlz::ExecuteCurrentStatement" => bind!(ExecuteCurrentStatement),
        "zqlz::ExplainQuery" => bind!(ExplainQuery),
        "zqlz::ExplainSelection" => bind!(ExplainSelection),
        "zqlz::StopQuery" => bind!(StopQuery),
        "zqlz::ToggleLeftSidebar" => bind!(ToggleLeftSidebar),
        "zqlz::ToggleRightSidebar" => bind!(ToggleRightSidebar),
        "zqlz::ToggleBottomPanel" => bind!(ToggleBottomPanel),
        "zqlz::FocusEditor" => bind!(FocusEditor),
        "zqlz::FocusResults" => bind!(FocusResults),
        "zqlz::FocusSidebar" => bind!(FocusSidebar),
        "zqlz::Refresh" => bind!(Refresh),

        // === tabs namespace (tab management actions) ===
        "tabs::ActivateNextTab" => bind!(ActivateNextTab),
        "tabs::ActivatePrevTab" => bind!(ActivatePrevTab),
        "tabs::CloseActiveTab" => bind!(CloseActiveTab),
        "tabs::CloseOtherTabs" => bind!(CloseOtherTabs),
        "tabs::CloseTabsToRight" => bind!(CloseTabsToRight),
        "tabs::CloseAllTabs" => bind!(CloseAllTabs),
        "tabs::ActivateTab1" => bind!(ActivateTab1),
        "tabs::ActivateTab2" => bind!(ActivateTab2),
        "tabs::ActivateTab3" => bind!(ActivateTab3),
        "tabs::ActivateTab4" => bind!(ActivateTab4),
        "tabs::ActivateTab5" => bind!(ActivateTab5),
        "tabs::ActivateTab6" => bind!(ActivateTab6),
        "tabs::ActivateTab7" => bind!(ActivateTab7),
        "tabs::ActivateTab8" => bind!(ActivateTab8),
        "tabs::ActivateTab9" => bind!(ActivateTab9),
        "tabs::ActivateLastTab" => bind!(ActivateLastTab),

        // === query_editor namespace (editor-specific actions) ===
        "query_editor::FormatQuery" => bind!(FormatQuery),
        "query_editor::SaveQuery" => bind!(SaveQuery),
        "query_editor::SaveQueryAs" => bind!(SaveQueryAs),
        "query_editor::ToggleLineComment" => bind!(ToggleLineComment),
        "query_editor::CommentSelection" => bind!(CommentSelection),
        "query_editor::UncommentSelection" => bind!(UncommentSelection),
        "query_editor::DuplicateLine" => bind!(DuplicateLine),
        "query_editor::DeleteLine" => bind!(DeleteLine),
        "query_editor::MoveLineUp" => bind!(MoveLineUp),
        "query_editor::MoveLineDown" => bind!(MoveLineDown),
        "query_editor::CopyLineUp" => bind!(CopyLineUp),
        "query_editor::CopyLineDown" => bind!(CopyLineDown),
        "query_editor::AcceptCompletion" => bind!(AcceptCompletion),
        "query_editor::CancelCompletion" => bind!(CancelCompletion),
        "query_editor::TriggerCompletion" => bind!(TriggerCompletion),
        "query_editor::TriggerParameterHints" => bind!(TriggerParameterHints),
        "query_editor::ShowHover" => bind!(ShowHover),
        "query_editor::GoToDefinition" => bind!(GoToDefinition),
        "query_editor::FindNext" => bind!(FindNext),
        "query_editor::FindPrevious" => bind!(FindPrevious),
        "query_editor::ToggleProblemsPanel" => bind!(ToggleProblemsPanel),

        // === editor namespace — mapped to zqlz_text_editor::actions ===
        "editor::MoveLeft" => bind!(editor::MoveLeft),
        "editor::MoveRight" => bind!(editor::MoveRight),
        "editor::MoveUp" => bind!(editor::MoveUp),
        "editor::MoveDown" => bind!(editor::MoveDown),
        "editor::MoveToBeginningOfLine" => bind!(editor::MoveToBeginningOfLine),
        "editor::MoveToEndOfLine" => bind!(editor::MoveToEndOfLine),
        "editor::MoveToBeginning" => bind!(editor::MoveToBeginning),
        "editor::MoveToEnd" => bind!(editor::MoveToEnd),
        "editor::MoveToPreviousWordStart" => bind!(editor::MoveToPreviousWordStart),
        "editor::MoveToNextWordEnd" => bind!(editor::MoveToNextWordEnd),
        "editor::PageUp" => bind!(editor::PageUp),
        "editor::PageDown" => bind!(editor::PageDown),
        "editor::SelectLeft" => bind!(editor::SelectLeft),
        "editor::SelectRight" => bind!(editor::SelectRight),
        "editor::SelectUp" => bind!(editor::SelectUp),
        "editor::SelectDown" => bind!(editor::SelectDown),
        "editor::SelectToBeginningOfLine" => bind!(editor::SelectToBeginningOfLine),
        "editor::SelectToEndOfLine" => bind!(editor::SelectToEndOfLine),
        "editor::SelectToBeginning" => bind!(editor::SelectToBeginning),
        "editor::SelectToEnd" => bind!(editor::SelectToEnd),
        "editor::SelectToPreviousWordStart" => bind!(editor::SelectToPreviousWordStart),
        "editor::SelectToNextWordEnd" => bind!(editor::SelectToNextWordEnd),
        "editor::SelectAll" => bind!(editor::SelectAll),
        "editor::Backspace" => bind!(editor::Backspace),
        "editor::Delete" => bind!(editor::Delete),
        "editor::Newline" => bind!(editor::Newline),
        "editor::Tab" => bind!(editor::Tab),
        "editor::Copy" => bind!(editor::Copy),
        "editor::Cut" => bind!(editor::Cut),
        "editor::Paste" => bind!(editor::Paste),
        "editor::Undo" => bind!(editor::Undo),
        "editor::Redo" => bind!(editor::Redo),
        "editor::OpenFind" => bind!(editor::OpenFind),
        "editor::OpenFindReplace" => bind!(editor::OpenFindReplace),
        "editor::FindNext" => bind!(editor::FindNext),
        "editor::FindPrevious" => bind!(editor::FindPrevious),
        "editor::TriggerCompletion" => bind!(editor::TriggerCompletion),
        "editor::AcceptCompletion" => bind!(editor::AcceptCompletion),
        "editor::DismissCompletion" => bind!(editor::DismissCompletion),
        "editor::Escape" => bind!(editor::Escape),
        // Actions that exist in the JSON but have no equivalent yet — silently ignored.
        "editor::SelectNext"
        | "editor::SelectPrevious"
        | "editor::SelectNextMatch"
        | "editor::SelectPreviousMatch"
        | "editor::SelectAllMatches"
        | "editor::SelectLine"
        | "editor::AddSelectionAbove"
        | "editor::AddSelectionBelow"
        | "editor::UndoSelection"
        | "editor::DeleteToBeginningOfLine"
        | "editor::DeleteToEndOfLine"
        | "editor::DeleteToPreviousWordStart"
        | "editor::DeleteToNextWordEnd"
        | "editor::DeleteLine"
        | "editor::DuplicateLineDown"
        | "editor::MoveLineUp"
        | "editor::MoveLineDown"
        | "editor::JoinLines"
        | "editor::Transpose"
        | "editor::Indent"
        | "editor::Outdent"
        | "editor::Fold"
        | "editor::UnfoldLines"
        | "editor::FoldAll"
        | "editor::UnfoldAll"
        | "editor::FoldAtLevel1"
        | "editor::FoldAtLevel2"
        | "editor::FoldAtLevel3"
        | "editor::FoldAtLevel4"
        | "editor::FoldAtLevel5"
        | "editor::FoldAtLevel6"
        | "editor::FoldAtLevel7"
        | "editor::FoldAtLevel8"
        | "editor::FoldAtLevel9"
        | "editor::ToggleComments"
        | "editor::FindNextMatch"
        | "editor::FindPreviousMatch"
        | "editor::SelectPageUp"
        | "editor::SelectPageDown" => false,

        // For buffer_search:: actions, silently ignored (find/replace is built into the editor).
        action_name if action_name.starts_with("buffer_search::") => false,

        // === table_viewer namespace ===
        "table_viewer::CancelCellEditing" => bind!(CancelCellEditing),
        "table_viewer::CommitChanges" => bind!(CommitChanges),
        "table_viewer::DeleteSelectedRows" => bind!(DeleteSelectedRows),
        "table_viewer::ToggleSearch" => {
            bind!(crate::components::table_viewer::ToggleSearch)
        }
        "table_viewer::CopySelection" => {
            bind!(crate::components::table_viewer::CopySelection)
        }
        "table_viewer::PasteClipboard" => {
            bind!(crate::components::table_viewer::PasteClipboard)
        }

        // === versioning namespace ===
        "versioning::ShowVersionHistory" => bind!(ShowVersionHistory),
        "versioning::CompareVersions" => bind!(CompareVersions),
        "versioning::RestoreVersion" => bind!(RestoreVersion),
        "versioning::SaveVersion" => bind!(SaveVersion),

        // Unknown action
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_keymap() {
        let sections: Vec<KeymapSection> =
            serde_json::from_str(DEFAULT_KEYMAP).expect("Failed to parse keymap JSON");

        assert!(
            !sections.is_empty(),
            "Keymap should have at least one section"
        );

        // First section should be global (no context)
        assert!(
            sections[0].context.is_none(),
            "First section should be global"
        );

        // Check that we have some bindings
        let total_bindings: usize = sections.iter().map(|s| s.bindings.len()).sum();
        assert!(total_bindings > 0, "Should have some bindings");
    }
}
