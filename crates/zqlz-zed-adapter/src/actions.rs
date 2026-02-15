/// Re-exports Zed editor actions for ZQLZ integration.
///
/// This module makes Zed's editor actions available to ZQLZ's action system,
/// allowing them to be bound to keybindings and invoked via the command palette.
///
/// The adapter layer re-exports essential Zed editor actions without modification.
/// This allows ZQLZ to register keybindings for these actions using the editor::
/// namespace (e.g., "editor::MoveUp", "editor::SelectNext").
///
/// # Action Organization
///
/// Actions are organized by category:
/// - Movement: Cursor navigation
/// - Selection: Text selection
/// - Editing: Text manipulation
/// - Clipboard: Copy/paste operations
/// - Undo/Redo: History navigation
/// - Line Operations: Line-level commands
/// - Comment: Code commenting
/// - Fold: Code folding
/// - Multi-cursor: Multiple cursor operations
///
/// # Usage
///
/// Call `register_editor_actions()` during ZQLZ app initialization:
///
/// ```rust,ignore
/// use zqlz_zed_adapter::register_editor_actions;
///
/// fn init_app(window: &mut Window, cx: &mut App) {
///     register_editor_actions();
///     // ... rest of initialization
/// }
/// ```
///
/// Then bind actions in keybindings configuration:
///
/// ```json
/// {
///   "bindings": {
///     "cmd-d": "editor::SelectNext",
///     "cmd-/": "editor::ToggleComments",
///     "cmd-z": "editor::Undo"
///   }
/// }
/// ```
// Re-export essential editor action structs that are publicly available
// Note: Most editor actions are defined via the actions!() macro which creates
// zero-sized types that are not individually exported. These actions are still
// available via GPUI's action dispatch system using their string names (e.g., "editor::MoveUp").
//
// Only action structs that are explicitly defined as `pub struct` can be re-exported here.

// === Publicly Available Action Structs ===

// Selection and multi-cursor
pub use editor::actions::SelectNext;
pub use editor::actions::SelectPrevious;

// Movement
pub use editor::actions::MoveDownByLines;
pub use editor::actions::MovePageDown;
pub use editor::actions::MovePageUp;
pub use editor::actions::MoveToBeginningOfLine;
pub use editor::actions::MoveToEndOfLine;
pub use editor::actions::MoveUpByLines;

// Selection with movement
pub use editor::actions::SelectDownByLines;
pub use editor::actions::SelectToBeginningOfLine;
pub use editor::actions::SelectToEndOfLine;
pub use editor::actions::SelectUpByLines;

// Deletion
pub use editor::actions::DeleteToBeginningOfLine;
pub use editor::actions::DeleteToEndOfLine;
pub use editor::actions::DeleteToNextSubwordEnd;
pub use editor::actions::DeleteToNextWordEnd;
pub use editor::actions::DeleteToPreviousSubwordStart;
pub use editor::actions::DeleteToPreviousWordStart;

// Advanced actions
pub use editor::actions::ConfirmCodeAction;
pub use editor::actions::ConfirmCompletion;
pub use editor::actions::CutToEndOfLine;
pub use editor::actions::SortLinesCaseInsensitive;
pub use editor::actions::SortLinesCaseSensitive;
pub use editor::actions::ToggleCodeActions;
pub use editor::actions::ToggleComments;

/// Registers all Zed editor actions with GPUI's action system.
///
/// This function should be called once during ZQLZ app initialization,
/// before any keybindings are registered. It ensures that all editor
/// actions are available for keybinding and can be dispatched via the
/// action system.
///
/// # Note
///
/// This function is a no-op in the current implementation because GPUI
/// automatically registers actions when they are imported. However, it's
/// provided as a clear initialization point and for future extensibility
/// if explicit registration becomes necessary.
pub fn register_editor_actions() {
    // GPUI automatically registers actions via the #[action] macro
    // This function serves as a clear initialization point and documents
    // that editor actions are being made available to ZQLZ
    //
    // If explicit registration is needed in the future, it would be done here
}

/// Returns a list of commonly used editor action names for UI display.
///
/// These action names can be used with GPUI's action dispatch system for keybindings.
/// Most editor actions are created via the actions!() macro and are accessed by their
/// string names (e.g., "editor::MoveUp") rather than as Rust type names.
///
/// This list includes the most commonly used actions that should be available in ZQLZ.
/// For the complete list of all ~500 editor actions, see:
/// https://github.com/zed-industries/zed/blob/main/crates/editor/src/actions.rs
///
/// # Usage in Keybindings
///
/// ```json
/// {
///   "bindings": {
///     "cmd-d": "editor::SelectNext",
///     "cmd-/": "editor::ToggleComments",
///     "up": "editor::MoveUp"
///   }
/// }
/// ```
pub fn list_common_editor_actions() -> Vec<&'static str> {
    vec![
        // Basic Movement (arrow keys, etc.)
        "editor::MoveUp",
        "editor::MoveDown",
        "editor::MoveLeft",
        "editor::MoveRight",
        "editor::MoveToBeginning",       // Cmd+Home
        "editor::MoveToBeginningOfLine", // Home
        "editor::MoveToEnd",             // Cmd+End
        "editor::MoveToEndOfLine",       // End
        "editor::MovePageUp",            // PageUp
        "editor::MovePageDown",          // PageDown
        // Basic Selection (shift + movement)
        "editor::SelectUp",
        "editor::SelectDown",
        "editor::SelectLeft",
        "editor::SelectRight",
        "editor::SelectAll", // Cmd+A
        "editor::SelectToBeginning",
        "editor::SelectToBeginningOfLine", // Shift+Home
        "editor::SelectToEnd",
        "editor::SelectToEndOfLine", // Shift+End
        "editor::SelectLine",        // Cmd+L
        // Multi-cursor (Zed's killer feature)
        "editor::SelectNext",        // Cmd+D (select next occurrence)
        "editor::SelectPrevious",    // Cmd+Shift+D
        "editor::SelectAllMatches",  // Cmd+Shift+L
        "editor::AddSelectionAbove", // Cmd+Alt+Up
        "editor::AddSelectionBelow", // Cmd+Alt+Down
        // Basic Editing
        "editor::Backspace",
        "editor::Delete",
        "editor::DeleteLine",              // Cmd+Shift+K
        "editor::DeleteToEndOfLine",       // Cmd+Delete
        "editor::DeleteToBeginningOfLine", // Cmd+Backspace
        "editor::Newline",                 // Enter
        "editor::NewlineAbove",            // Cmd+Shift+Enter
        "editor::NewlineBelow",            // Cmd+Enter
        // Line Operations
        "editor::DuplicateLineUp",   // Shift+Alt+Up
        "editor::DuplicateLineDown", // Shift+Alt+Down
        "editor::MoveLineUp",        // Alt+Up
        "editor::MoveLineDown",      // Alt+Down
        "editor::JoinLines",         // Cmd+J
        // Clipboard
        "editor::Copy",  // Cmd+C
        "editor::Cut",   // Cmd+X
        "editor::Paste", // Cmd+V
        // Undo/Redo
        "editor::Undo", // Cmd+Z
        "editor::Redo", // Cmd+Shift+Z
        // Indentation
        "editor::Tab",     // Tab
        "editor::Indent",  // Cmd+]
        "editor::Outdent", // Cmd+[
        // Comments
        "editor::ToggleComments", // Cmd+/
        // Code Folding
        "editor::Fold",        // Alt+Cmd+[
        "editor::UnfoldLines", // Alt+Cmd+]
        "editor::FoldAll",     // Cmd+K Cmd+0
        "editor::UnfoldAll",   // Cmd+K Cmd+J
        "editor::ToggleFold",
        // Case Conversion
        "editor::ConvertToUpperCase",
        "editor::ConvertToLowerCase",
        "editor::ConvertToTitleCase",
        "editor::ConvertToSnakeCase",
        "editor::ConvertToKebabCase",
        "editor::ConvertToCamelCase",
        // Line Sorting
        "editor::SortLinesCaseSensitive",
        "editor::SortLinesCaseInsensitive",
        "editor::ReverseLines",
        // Formatting
        "editor::Format", // Shift+Alt+F
        // Completion
        "editor::ConfirmCompletion", // Tab/Enter in completion menu
        // Code Actions
        "editor::ToggleCodeActions", // Cmd+.
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_editor_actions() {
        // Should not panic
        register_editor_actions();
    }

    #[test]
    fn test_list_common_editor_actions_not_empty() {
        let actions = list_common_editor_actions();
        assert!(!actions.is_empty(), "Should have editor actions");
        assert!(
            actions.len() > 50,
            "Should have many editor actions (got {})",
            actions.len()
        );
    }

    #[test]
    fn test_action_names_have_editor_namespace() {
        let actions = list_common_editor_actions();
        for action in actions {
            assert!(
                action.starts_with("editor::"),
                "Action '{}' should have editor:: namespace",
                action
            );
        }
    }
}
