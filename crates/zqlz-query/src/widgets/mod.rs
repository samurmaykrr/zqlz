//! Query UI Widgets
//!
//! UI components for query editing, execution, and results display.

mod actions;
mod history_panel;
mod problems_panel;
mod query_editor;
mod query_tabs_panel;
mod results_panel;
mod zed_input;

pub use actions::{
    AcceptCompletion, AcceptInlineSuggestion, CancelCompletion, CommentSelection, CopyLineDown, CopyLineUp,
    DeleteLine, DismissInlineSuggestion, DuplicateLine, FindNext, FindPrevious, FormatQuery, GoToDefinition,
    MoveLineDown, MoveLineUp, SaveQuery, SaveQueryAs, ShowHover, ToggleLineComment, ToggleProblemsPanel,
    TriggerCompletion, TriggerParameterHints, UncommentSelection,
};
pub use history_panel::{QueryHistoryPanel, QueryHistoryPanelEvent};
pub use problems_panel::{ProblemEntry, ProblemsPanel, ProblemsPanelEvent, ProblemSeverity};
pub use query_editor::{
    DiagnosticInfo, DiagnosticInfoSeverity, EditorMode, EditorObjectType, QueryEditor,
    QueryEditorEvent,
};
pub use query_tabs_panel::{QueryTabsPanel, QueryTabsPanelEvent};
pub use results_panel::{
    ExplainResult, QueryExecution, ResultsPanel, ResultsPanelEvent, StatementResult,
};
pub use zed_input::{ZedInput, ZedInputEvent, ZedInputState};
