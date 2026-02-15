//! Keymap loading system for ZQLZ
//!
//! Loads keyboard shortcuts from JSON files based on the current OS.
//! The keymap files are stored in `assets/keymaps/` directory.

use gpui::{App, DummyKeyboardMapper, KeyBinding, KeyBindingContextPredicate};
use serde::Deserialize;
use std::collections::HashMap;
use zqlz_zed_adapter::actions as zed_actions;

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

        // === editor namespace (Zed editor actions) ===
        // These actions are re-exported from zqlz-zed-adapter and bound here.
        // Actions defined via the actions!() macro in Zed are automatically registered
        // in GPUI's action system when the editor crate is imported.
        // We bind the commonly-used editor actions that have public struct definitions.
        "editor::SelectNext" => bind!(zed_actions::SelectNext::default()),
        "editor::SelectPrevious" => bind!(zed_actions::SelectPrevious::default()),
        "editor::MoveToBeginningOfLine" => {
            bind!(zed_actions::MoveToBeginningOfLine::default())
        }
        "editor::MoveToEndOfLine" => bind!(zed_actions::MoveToEndOfLine::default()),
        "editor::SelectToBeginningOfLine" => {
            bind!(zed_actions::SelectToBeginningOfLine::default())
        }
        "editor::SelectToEndOfLine" => {
            bind!(zed_actions::SelectToEndOfLine::default())
        }
        "editor::DeleteToBeginningOfLine" => {
            bind!(zed_actions::DeleteToBeginningOfLine::default())
        }
        "editor::DeleteToEndOfLine" => {
            bind!(zed_actions::DeleteToEndOfLine::default())
        }
        "editor::DeleteToPreviousWordStart" => {
            bind!(zed_actions::DeleteToPreviousWordStart::default())
        }
        "editor::DeleteToNextWordEnd" => {
            bind!(zed_actions::DeleteToNextWordEnd::default())
        }
        "editor::MoveUpByLines" => bind!(zed_actions::MoveUpByLines::default()),
        "editor::MoveDownByLines" => bind!(zed_actions::MoveDownByLines::default()),
        "editor::MovePageUp" => bind!(zed_actions::MovePageUp::default()),
        "editor::MovePageDown" => bind!(zed_actions::MovePageDown::default()),
        "editor::SelectUpByLines" => bind!(zed_actions::SelectUpByLines::default()),
        "editor::SelectDownByLines" => {
            bind!(zed_actions::SelectDownByLines::default())
        }
        "editor::ToggleComments" => bind!(zed_actions::ToggleComments::default()),
        "editor::ConfirmCompletion" => {
            bind!(zed_actions::ConfirmCompletion::default())
        }
        "editor::ConfirmCodeAction" => {
            bind!(zed_actions::ConfirmCodeAction::default())
        }
        "editor::CutToEndOfLine" => bind!(zed_actions::CutToEndOfLine {
            stop_at_newlines: false
        }),

        // For editor:: actions defined via actions!() macro, use build_action
        // to create them dynamically from their string names.
        action_name if action_name.starts_with("editor::") => {
            match cx.build_action(action_name, None) {
                Ok(action) => {
                    let context_predicate =
                        context.map(|c| gpui::KeyBindingContextPredicate::parse(c).unwrap().into());
                    cx.bind_keys([KeyBinding::load(
                        keystroke,
                        action,
                        context_predicate,
                        false,
                        None,
                        &DummyKeyboardMapper,
                    )
                    .unwrap()]);
                    tracing::debug!("Bound {} -> {} via build_action", keystroke, action_name);
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        "Cannot bind '{}' - build_action failed: {:?}",
                        action_name,
                        e
                    );
                    false
                }
            }
        }

        // For buffer_search:: actions, also use build_action
        action_name if action_name.starts_with("buffer_search::") => {
            match cx.build_action(action_name, None) {
                Ok(action) => {
                    let context_predicate =
                        context.map(|c| KeyBindingContextPredicate::parse(c).unwrap().into());
                    cx.bind_keys([KeyBinding::load(
                        keystroke,
                        action,
                        context_predicate,
                        false,
                        None,
                        &DummyKeyboardMapper,
                    )
                    .unwrap()]);
                    tracing::debug!("Bound {} -> {} via build_action", keystroke, action_name);
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        "Cannot bind '{}' - build_action failed: {:?}",
                        action_name,
                        e
                    );
                    false
                }
            }
        }

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
