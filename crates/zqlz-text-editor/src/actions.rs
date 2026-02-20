use gpui::{actions, App, KeyBinding};

actions!(
    editor,
    [
        // Cursor movement
        MoveLeft,
        MoveRight,
        MoveUp,
        MoveDown,
        MoveToBeginningOfLine,
        MoveToEndOfLine,
        MoveToBeginning,
        MoveToEnd,
        MoveToPreviousWordStart,
        MoveToNextWordEnd,
        MoveToParagraphStart,
        MoveToParagraphEnd,
        MoveToNextSubwordEnd,
        MoveToPreviousSubwordStart,
        PageUp,
        PageDown,
        // Selection (shift + movement)
        SelectLeft,
        SelectRight,
        SelectUp,
        SelectDown,
        SelectToBeginningOfLine,
        SelectToEndOfLine,
        SelectToBeginning,
        SelectToEnd,
        SelectToPreviousWordStart,
        SelectToNextWordEnd,
        SelectToParagraphStart,
        SelectToParagraphEnd,
        SelectToNextSubwordEnd,
        SelectToPreviousSubwordStart,
        SelectAll,
        // Editing
        Backspace,
        Delete,
        DeleteSubwordLeft,
        DeleteSubwordRight,
        Newline,
        Tab,
        ShiftTab,
        // Line editing (feat-005 through feat-015)
        MoveLineUp,
        MoveLineDown,
        DuplicateLineDown,
        DuplicateLineUp,
        DeleteLine,
        NewlineAbove,
        NewlineBelow,
        JoinLines,
        TransposeChars,
        IndentLine,
        DedentLine,
        ToggleLineComment,
        // Selection features (feat-016/017/018)
        SelectLine,
        SelectNextOccurrence,
        SelectAllOccurrences,
        // Multi-cursor (feat-021/022)
        AddCursorAbove,
        AddCursorBelow,
        // Undo selection (feat-020)
        UndoSelection,
        // Clipboard extras (feat-024)
        CutToEndOfLine,
        // Clipboard
        Copy,
        Cut,
        Paste,
        // Undo / Redo
        Undo,
        Redo,
        // Find / Replace
        OpenFind,
        OpenFindReplace,
        FindNext,
        FindPrevious,
        FindSelectAllMatches,
        // Completions
        TriggerCompletion,
        AcceptCompletion,
        DismissCompletion,
        SelectPreviousCompletion,
        SelectNextCompletion,
        // Text transforms — case (feat-033/034)
        TransformUppercase,
        TransformLowercase,
        TransformTitleCase,
        TransformSnakeCase,
        TransformCamelCase,
        TransformKebabCase,
        // Line manipulation — sort/reverse/unique (feat-035/036/037)
        SortLinesAscending,
        SortLinesDescending,
        SortLinesByLength,
        ReverseLines,
        UniqueLines,
        // Insert UUID (feat-042)
        InsertUuidV4,
        InsertUuidV7,
        // Multi-cursor extras (feat-043/044)
        RotateSelections,
        SwapSelectionEnds,
        // Clipboard extras (feat-050/051)
        CopyAsMarkdown,
        PasteAsPlainText,
        // Go-to-line dialog (feat-040)
        GoToLine,
        // Toggle soft wrap (feat-041)
        ToggleSoftWrap,
        // Format SQL (feat-049)
        FormatSQL,
        // LSP navigation (feat-045/046/047/048)
        GoToDefinition,
        FindReferences,
        RenameSymbol,
        OpenContextMenu,
        // Folding
        FoldAll,
        UnfoldAll,
        // Misc
        Escape,
    ]
);

pub const CONTEXT: &str = "TextEditor";

pub fn init(cx: &mut App) {
    cx.bind_keys([
        // Movement
        KeyBinding::new("left", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("right", MoveRight, Some(CONTEXT)),
        KeyBinding::new("up", MoveUp, Some(CONTEXT)),
        KeyBinding::new("down", MoveDown, Some(CONTEXT)),
        KeyBinding::new("home", MoveToBeginningOfLine, Some(CONTEXT)),
        KeyBinding::new("end", MoveToEndOfLine, Some(CONTEXT)),
        KeyBinding::new("pageup", PageUp, Some(CONTEXT)),
        KeyBinding::new("pagedown", PageDown, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-up", MoveToBeginning, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-down", MoveToEnd, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-left", MoveToBeginningOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-right", MoveToEndOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-a", MoveToBeginningOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-e", MoveToEndOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("alt-left", MoveToPreviousWordStart, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("alt-right", MoveToNextWordEnd, Some(CONTEXT)),
        // Paragraph movement (feat-002) — alt-up/down are free on all platforms
        KeyBinding::new("alt-up", MoveToParagraphStart, Some(CONTEXT)),
        KeyBinding::new("alt-down", MoveToParagraphEnd, Some(CONTEXT)),
        // Select to paragraph (feat-002) — use ctrl-alt-shift to avoid conflict with DuplicateLineUp/Down
        KeyBinding::new("ctrl-alt-shift-up", SelectToParagraphStart, Some(CONTEXT)),
        KeyBinding::new("ctrl-alt-shift-down", SelectToParagraphEnd, Some(CONTEXT)),
        // Subword movement (feat-003)
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-alt-right", MoveToNextSubwordEnd, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-alt-left", MoveToPreviousSubwordStart, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new(
            "ctrl-alt-shift-right",
            SelectToNextSubwordEnd,
            Some(CONTEXT),
        ),
        #[cfg(target_os = "macos")]
        KeyBinding::new(
            "ctrl-alt-shift-left",
            SelectToPreviousSubwordStart,
            Some(CONTEXT),
        ),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-alt-right", MoveToNextSubwordEnd, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-alt-left", MoveToPreviousSubwordStart, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new(
            "ctrl-alt-shift-right",
            SelectToNextSubwordEnd,
            Some(CONTEXT),
        ),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new(
            "ctrl-alt-shift-left",
            SelectToPreviousSubwordStart,
            Some(CONTEXT),
        ),
        // Subword delete (feat-003)
        KeyBinding::new("ctrl-alt-backspace", DeleteSubwordLeft, Some(CONTEXT)),
        KeyBinding::new("ctrl-alt-delete", DeleteSubwordRight, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-home", MoveToBeginning, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-end", MoveToEnd, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-left", MoveToPreviousWordStart, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-right", MoveToNextWordEnd, Some(CONTEXT)),
        // Selection
        KeyBinding::new("shift-left", SelectLeft, Some(CONTEXT)),
        KeyBinding::new("shift-right", SelectRight, Some(CONTEXT)),
        KeyBinding::new("shift-up", SelectUp, Some(CONTEXT)),
        KeyBinding::new("shift-down", SelectDown, Some(CONTEXT)),
        KeyBinding::new("shift-home", SelectToBeginningOfLine, Some(CONTEXT)),
        KeyBinding::new("shift-end", SelectToEndOfLine, Some(CONTEXT)),
        KeyBinding::new("shift-pageup", SelectToBeginning, Some(CONTEXT)),
        KeyBinding::new("shift-pagedown", SelectToEnd, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-up", SelectToBeginning, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-down", SelectToEnd, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-left", SelectToBeginningOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-right", SelectToEndOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-shift-a", SelectToBeginningOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("ctrl-shift-e", SelectToEndOfLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-home", SelectToBeginning, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-end", SelectToEnd, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-left", SelectToBeginningOfLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-right", SelectToEndOfLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("alt-shift-left", SelectToPreviousWordStart, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("alt-shift-right", SelectToNextWordEnd, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-left", SelectToPreviousWordStart, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-right", SelectToNextWordEnd, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-a", SelectAll, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-a", SelectAll, Some(CONTEXT)),
        // Editing
        KeyBinding::new("backspace", Backspace, Some(CONTEXT)),
        KeyBinding::new("delete", Delete, Some(CONTEXT)),
        KeyBinding::new("enter", Newline, Some(CONTEXT)),
        KeyBinding::new("tab", Tab, Some(CONTEXT)),
        KeyBinding::new("shift-tab", ShiftTab, Some(CONTEXT)),
        // Line editing — feat-005 through feat-015
        KeyBinding::new("ctrl-shift-up", MoveLineUp, Some(CONTEXT)),
        KeyBinding::new("ctrl-shift-down", MoveLineDown, Some(CONTEXT)),
        KeyBinding::new("alt-shift-down", DuplicateLineDown, Some(CONTEXT)),
        KeyBinding::new("alt-shift-up", DuplicateLineUp, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-k", DeleteLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-k", DeleteLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-enter", NewlineAbove, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-enter", NewlineAbove, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-enter", NewlineBelow, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-enter", NewlineBelow, Some(CONTEXT)),
        KeyBinding::new("ctrl-j", JoinLines, Some(CONTEXT)),
        KeyBinding::new("ctrl-t", TransposeChars, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-]", IndentLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-[", DedentLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-]", IndentLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-[", DedentLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-/", ToggleLineComment, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-/", ToggleLineComment, Some(CONTEXT)),
        // Selection features — feat-016/017/018
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-l", SelectLine, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-l", SelectLine, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-d", SelectNextOccurrence, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-d", SelectNextOccurrence, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-l", SelectAllOccurrences, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-l", SelectAllOccurrences, Some(CONTEXT)),
        // Multi-cursor — feat-021/022
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-alt-up", AddCursorAbove, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-alt-up", AddCursorAbove, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-alt-down", AddCursorBelow, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-alt-down", AddCursorBelow, Some(CONTEXT)),
        // Undo selection — feat-020
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-u", UndoSelection, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-u", UndoSelection, Some(CONTEXT)),
        // Cut to end of line — feat-024
        KeyBinding::new("ctrl-k", CutToEndOfLine, Some(CONTEXT)),
        // Clipboard
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-c", Copy, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-c", Copy, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-x", Cut, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-x", Cut, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-v", Paste, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-v", Paste, Some(CONTEXT)),
        // Undo / Redo
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-z", Undo, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-z", Undo, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-shift-z", Redo, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-y", Redo, Some(CONTEXT)),
        // Find / Replace
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-f", OpenFind, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-f", OpenFind, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-h", OpenFindReplace, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-h", OpenFindReplace, Some(CONTEXT)),
        KeyBinding::new("f3", FindNext, Some(CONTEXT)),
        KeyBinding::new("shift-f3", FindPrevious, Some(CONTEXT)),
        // Completions
        KeyBinding::new("ctrl-space", TriggerCompletion, Some(CONTEXT)),
        KeyBinding::new("escape", Escape, Some(CONTEXT)),
        // Text case transforms (feat-033) — chord keybindings
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-k cmd-u", TransformUppercase, Some(CONTEXT)),
        #[cfg(target_os = "macos")]
        KeyBinding::new("cmd-k cmd-l", TransformLowercase, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-u", TransformUppercase, Some(CONTEXT)),
        #[cfg(not(target_os = "macos"))]
        KeyBinding::new("ctrl-shift-l", TransformLowercase, Some(CONTEXT)),
        // Go-to-line dialog (feat-040)
        KeyBinding::new("ctrl-g", GoToLine, Some(CONTEXT)),
        // Toggle soft wrap (feat-041)
        KeyBinding::new("alt-z", ToggleSoftWrap, Some(CONTEXT)),
        // Format SQL (feat-049) — shift-alt-f on all platforms
        KeyBinding::new("shift-alt-f", FormatSQL, Some(CONTEXT)),
        // LSP navigation (feat-046/047/048)
        KeyBinding::new("f12", GoToDefinition, Some(CONTEXT)),
        KeyBinding::new("shift-f12", FindReferences, Some(CONTEXT)),
        KeyBinding::new("f2", RenameSymbol, Some(CONTEXT)),
        // Context menu keyboard trigger (feat-045) — keyboard shortcut to open at cursor
        KeyBinding::new("ctrl-shift-f10", OpenContextMenu, Some(CONTEXT)),
        // Note: SelectPreviousCompletion / SelectNextCompletion have no static
        // key bindings.  When the completion menu is open, the MoveUp / MoveDown
        // action handlers check `is_completion_menu_open` at runtime and route
        // to the menu navigation helpers instead of moving the cursor.
    ]);
}
