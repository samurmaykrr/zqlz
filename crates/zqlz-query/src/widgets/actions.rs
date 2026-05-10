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
        NextProblem,
        PreviousProblem,
        ToggleProblemsPanel,
        TriggerParameterHints,
        ShowHover,
        // Code Actions & Rename
        ShowCodeActions,
    ]
);
