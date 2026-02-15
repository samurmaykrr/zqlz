//! ZQLZ Query - Query execution, parsing, and history
//!
//! This crate provides query execution utilities, SQL parsing,
//! auto-completion, and query history management.

pub mod ai_completion;
pub mod batch;
mod engine;
mod error;
mod history;
pub mod parameters;
mod service;
mod view_models;
pub mod widgets;

pub use engine::{DestructiveOperationType, DestructiveOperationWarning, QueryEngine};
pub use error::{QueryServiceError, QueryServiceResult};
pub use history::{QueryHistory, QueryHistoryEntry};
pub use service::QueryService;
// Note: view_models types are superseded by widgets types for UI consumption
pub use view_models::StatementExecution;
// Re-export widgets for convenient access
pub use widgets::{
    AcceptCompletion, AcceptInlineSuggestion, CancelCompletion, CommentSelection, CopyLineDown, CopyLineUp,
    DeleteLine, DismissInlineSuggestion, DiagnosticInfo, DiagnosticInfoSeverity, DuplicateLine, EditorMode,
    EditorObjectType, ExplainResult, FindNext, FindPrevious, FormatQuery, GoToDefinition, MoveLineDown,
    MoveLineUp, ProblemEntry, ProblemsPanel, ProblemsPanelEvent, ProblemSeverity, QueryEditor, QueryEditorEvent,
    QueryExecution, QueryHistoryPanel, QueryHistoryPanelEvent, QueryTabsPanel, QueryTabsPanelEvent,
    ResultsPanel, ResultsPanelEvent, SaveQuery, SaveQueryAs, ShowHover, StatementResult,
    ToggleLineComment, ToggleProblemsPanel, TriggerCompletion, TriggerParameterHints, UncommentSelection,
    ZedInput, ZedInputEvent, ZedInputState,
};

// Re-export batch execution types
pub use batch::{
    BatchExecutionResult, BatchExecutor, BatchOptions, BatchResult, ExecutionMode, StatementError,
    StatementStatus, split_statements,
};

// Re-export AI completion types
pub use ai_completion::{
    AiCompletionProvider, AiProviderFactory, ColumnInfo, CompletionError, CompletionMetadata,
    CompletionRequest, CompletionResponse, CompletionResult, FunctionInfo, ParameterInfo,
    ParameterMode, ProcedureInfo, ProviderMetadata, SchemaContext, TableInfo, ViewInfo,
};
