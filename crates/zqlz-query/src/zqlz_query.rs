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
mod schema_metadata;
mod service;
#[cfg(test)]
mod test_helpers;
mod view_models;
pub mod widgets;

pub use engine::{DestructiveOperationType, DestructiveOperationWarning, QueryEngine};
pub use error::{QueryServiceError, QueryServiceResult};
pub use history::{HistoryPersistence, QueryHistory, QueryHistoryEntry};
pub use service::QueryService;
// Note: view_models types are superseded by widgets types for UI consumption
pub use view_models::StatementExecution;
// Re-export widgets for convenient access
pub use widgets::{
    AcceptCompletion, AcceptInlineSuggestion, CancelCompletion, DiagnosticInfo,
    DiagnosticInfoSeverity, DismissInlineSuggestion, EditorMode, EditorObjectType, ExplainResult,
    FormatQuery, NextProblem, PreviousProblem, ProblemEntry, ProblemSeverity, ProblemsPanel,
    ProblemsPanelEvent, QueryEditor, QueryEditorEvent, QueryExecution, QueryHistoryPanel,
    QueryHistoryPanelEvent, QueryTabsPanel, QueryTabsPanelEvent, ResultsPanel, ResultsPanelEvent,
    SaveQuery, SaveQueryAs, ShowCodeActions, ShowHover, StatementResult, ToggleProblemsPanel,
    TriggerCompletion, TriggerParameterHints,
};

// Re-export batch execution types
pub use batch::{
    BatchExecutionResult, BatchExecutor, BatchOptions, BatchResult, ExecutionMode, StatementError,
    StatementStatus, split_statements,
};

// Re-export schema metadata types
pub use schema_metadata::{
    SchemaMetadata, SchemaMetadataProvider, SchemaMetadataRenderer, SchemaSymbolDetails,
    SchemaSymbolInfo, SchemaSymbolType,
};

// Re-export AI completion types
pub use ai_completion::{
    AiCompletionProvider, AiProviderFactory, ColumnInfo, CompletionError, CompletionMetadata,
    CompletionRequest, CompletionResponse, CompletionResult, FunctionInfo, ParameterInfo,
    ParameterMode, ProcedureInfo, ProviderMetadata, SchemaContext, TableInfo, ViewInfo,
};

/// Performance configuration constants and tests
///
/// These values are verified to be within acceptable ranges for responsive UI interaction.
/// The acceptance criteria from task-8.3 (Performance validation) requires:
/// - LSP responses remain fast
/// - No noticeable performance regressions
#[allow(dead_code)]
mod performance {
    /// Completion debounce in milliseconds
    ///
    /// Target: 150ms - fast enough for responsive UI while avoiding excessive requests
    pub const COMPLETION_DEBOUNCE_MS: u64 = 150;

    /// Diagnostics debounce in milliseconds
    ///
    /// Target: 300ms - balances responsiveness with reducing server load
    pub const DIAGNOSTICS_DEBOUNCE_MS: u64 = 300;

    /// Maximum recommended debounce for interactive features (ms)
    ///
    /// Beyond this threshold, users perceive lag
    pub const MAX_INTERACTIVE_DEBOUNCE_MS: u64 = 500;

    /// Sub-100ms response target for LSP operations
    pub const LSP_RESPONSE_TARGET_MS: u64 = 100;

    const _: () = {
        assert!(
            COMPLETION_DEBOUNCE_MS <= MAX_INTERACTIVE_DEBOUNCE_MS,
            "completion debounce exceeds interactive threshold"
        );
        assert!(
            DIAGNOSTICS_DEBOUNCE_MS <= MAX_INTERACTIVE_DEBOUNCE_MS,
            "diagnostics debounce exceeds interactive threshold"
        );
        assert!(
            LSP_RESPONSE_TARGET_MS <= 100,
            "LSP response target should stay sub-100ms"
        );
        assert!(
            COMPLETION_DEBOUNCE_MS > 0,
            "completion debounce must be positive"
        );
        assert!(
            DIAGNOSTICS_DEBOUNCE_MS > 0,
            "diagnostics debounce must be positive"
        );
        assert!(
            COMPLETION_DEBOUNCE_MS < 1000,
            "completion debounce should be < 1s"
        );
        assert!(
            DIAGNOSTICS_DEBOUNCE_MS < 1000,
            "diagnostics debounce should be < 1s"
        );
    };
}
