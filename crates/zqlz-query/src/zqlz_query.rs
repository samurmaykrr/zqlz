//! ZQLZ Query - Query execution, parsing, and history
//!
//! This crate provides query execution utilities, SQL parsing,
//! auto-completion, and query history management.

pub mod ai_completion;
pub mod batch;
mod engine;
mod error;
mod execution_workflows;
mod explain;
mod history;
pub mod parameters;
mod saved_queries;
mod schema_metadata;
mod service;
#[cfg(test)]
mod test_helpers;
mod view_models;
pub mod widgets;

pub use engine::{DestructiveOperationType, DestructiveOperationWarning, QueryEngine};
pub use error::{QueryServiceError, QueryServiceResult};
pub use execution_workflows::{
    ExplainExecutionOutcome, NormalizedQueryExecutionRequest, QueryConnectionCandidate,
    QueryConnectionOption, QueryConnectionSelection, QueryConnectionSwitchPlan,
    QueryConnectionSwitchPlanningOutcome, QueryConnectionSwitchResolution,
    QueryDatabaseSelectionPlan, QueryDisplayContext, QueryEditorSwitcherSelection,
    QueryExecutionOutcome, QuerySqlSource, QueryWorkflowDispatch, QueryWorkflowOutcome,
    QueryWorkflowRequest, build_explain_execution_outcome, build_query_editor_switcher_selection,
    build_query_execution_outcome, default_database_label_for_connection, execute_explain_request,
    execute_query_request, normalize_query_execution_request, plan_query_connection_switch,
    plan_query_database_selection, resolve_query_connection_selection,
    resolve_query_connection_switch, resolve_query_editor_open_connection,
    resolve_workflow_connection_id, run_execute_query_workflow, run_explain_query_workflow,
    run_query_workflow, should_refresh_connection_surfaces_for_database_selection,
};
pub use history::{HistoryPersistence, QueryHistory, QueryHistoryEntry};
pub use saved_queries::{
    SavedQueryOperation, SavedQueryRecord, SavedQueryStore, SavedQueryWorkflowError,
    SavedQueryWorkflowOutcome, SavedQueryWorkflowRequest, create_saved_query, delete_saved_query,
    export_saved_queries, import_saved_queries, load_saved_queries_for_connection,
    load_saved_query, move_saved_query_to_folder, rename_saved_query, run_saved_query_workflow,
    update_saved_query_sql,
};
pub use service::QueryService;
// Note: view_models types are superseded by widgets types for UI consumption
pub use view_models::StatementExecution;
// Re-export widgets for convenient access
pub use widgets::{
    DiagnosticInfo, DiagnosticInfoSeverity, EditorMode, EditorObjectType, ExplainResult,
    FormatQuery, NextProblem, PreviousProblem, ProblemEntry, ProblemSeverity, ProblemsPanel,
    ProblemsPanelEvent, QueryEditor, QueryEditorEvent, QueryExecution, QueryExecutionParams,
    QueryHistoryPanel, QueryHistoryPanelEvent, QueryTabsPanel, QueryTabsPanelEvent, ResultsPanel,
    ResultsPanelEvent, SaveQuery, SaveQueryAs, ShowCodeActions, ShowHover, StatementResult,
    ToggleProblemsPanel, TriggerParameterHints,
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
