//! Query Editor Actions
//!
//! Actions specific to the query editor component.

use gpui::actions;

// Query Editor specific actions
actions!(
    query_editor,
    [
        // Query Execution
        FormatQuery,
        SaveQuery,
        SaveQueryAs,
        // Code Editing
        ToggleLineComment,
        CommentSelection,
        UncommentSelection,
        DuplicateLine,
        DeleteLine,
        MoveLineUp,
        MoveLineDown,
        CopyLineUp,
        CopyLineDown,
        // Navigation
        GoToDefinition,
        FindReferences,
        FindNext,
        FindPrevious,
        NextProblem,
        PreviousProblem,
        ToggleProblemsPanel,
        // IntelliSense / Completions
        AcceptCompletion,
        CancelCompletion,
        TriggerCompletion,
        TriggerParameterHints,
        ShowHover,
        // Inline Suggestions
        AcceptInlineSuggestion,
        DismissInlineSuggestion,
        // Code Actions & Rename
        ShowCodeActions,
        RenameSymbol,
        // Completion menu navigation
        ConfirmCompletion,
        CancelCompletionMenu,
        NextCompletion,
        PreviousCompletion,
    ]
);
