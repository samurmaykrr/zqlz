use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;
use zqlz_core::{Connection, default_database_label_for_driver};

use crate::widgets::{ExplainResult, QueryExecution, StatementResult};
use crate::{
    DestructiveOperationWarning, QueryEngine, QueryExecutionParams, QueryService,
    QueryServiceError, QueryServiceResult, view_models,
};

/// Connection/database labels that are rendered alongside query outputs.
#[derive(Clone, Debug, Default)]
pub struct QueryDisplayContext {
    pub connection_name: Option<String>,
    pub database_name: Option<String>,
}

/// Result payload produced for a standard query execution request.
pub struct QueryExecutionOutcome {
    pub execution: QueryExecution,
    pub success: bool,
}

/// Result payload produced for an EXPLAIN request.
pub struct ExplainExecutionOutcome {
    pub explain_result: ExplainResult,
    pub success: bool,
}

/// Typed workflow request for query and explain execution paths.
#[derive(Clone, Debug)]
pub enum QueryWorkflowRequest {
    Execute {
        sql: String,
        params: Option<QueryExecutionParams>,
        source: QuerySqlSource,
        destructive_warning: Option<DestructiveOperationWarning>,
    },
    Explain {
        sql: String,
    },
}

/// Source of SQL chosen by editor/UI before execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuerySqlSource {
    Selection,
    CurrentStatement,
    FullBuffer,
}

/// Normalized query execution request shared by app handlers and services.
#[derive(Clone, Debug)]
pub struct NormalizedQueryExecutionRequest {
    pub sql: String,
    pub source: QuerySqlSource,
    pub params: Option<QueryExecutionParams>,
    pub destructive_warning: Option<DestructiveOperationWarning>,
}

/// App-facing query workflow dispatch plan.
///
/// This owns request normalization and log/UI metadata so GPUI handlers only
/// collect resources, ask for confirmation if needed, and spawn execution.
#[derive(Clone, Debug)]
pub struct QueryWorkflowDispatch {
    pub preferred_connection_id: Option<Uuid>,
    pub selected_database_name: Option<String>,
    pub start_log_message: &'static str,
    pub no_connection_log_message: &'static str,
    pub request: QueryWorkflowRequest,
    pub refresh_history: bool,
}

impl QueryWorkflowDispatch {
    pub fn execute(
        preferred_connection_id: Option<Uuid>,
        selected_database_name: Option<String>,
        sql: impl Into<String>,
        params: Option<QueryExecutionParams>,
        source: QuerySqlSource,
        confirm_destructive: bool,
        start_log_message: &'static str,
    ) -> Self {
        let normalized =
            normalize_query_execution_request(sql, source, params, confirm_destructive);
        Self {
            preferred_connection_id,
            selected_database_name,
            start_log_message,
            no_connection_log_message: "No connection available for query execution",
            request: QueryWorkflowRequest::Execute {
                sql: normalized.sql,
                params: normalized.params,
                source: normalized.source,
                destructive_warning: normalized.destructive_warning,
            },
            refresh_history: true,
        }
    }

    pub fn explain(
        preferred_connection_id: Option<Uuid>,
        selected_database_name: Option<String>,
        sql: impl Into<String>,
        start_log_message: &'static str,
    ) -> Self {
        Self {
            preferred_connection_id,
            selected_database_name,
            start_log_message,
            no_connection_log_message: "No connection available for explain",
            request: QueryWorkflowRequest::Explain { sql: sql.into() },
            refresh_history: false,
        }
    }
}

/// Typed workflow response for query and explain execution paths.
pub enum QueryWorkflowOutcome {
    Execute(Box<QueryExecutionOutcome>),
    Explain(Box<ExplainExecutionOutcome>),
}

/// Minimal saved-connection snapshot needed for query workflow decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConnectionCandidate {
    pub connection_id: Uuid,
    pub connection_name: String,
    pub driver_name: String,
    pub params: HashMap<String, String>,
}

/// Resolved connection selection used by query workflow call sites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConnectionSelection {
    pub connection_id: Uuid,
    pub connection_name: String,
    pub driver_name: String,
    pub default_database_name: Option<String>,
}

/// Connection option data used to populate query-editor connection switchers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConnectionOption {
    pub connection_id: Uuid,
    pub connection_name: String,
}

/// Derived query-editor switcher state from connection candidates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryEditorSwitcherSelection {
    pub available_connections: Vec<QueryConnectionOption>,
    pub selected_default_database_name: Option<String>,
}

/// Connection-switch resolution outcome for query editors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryConnectionSwitchResolution {
    Ready(QueryConnectionSelection),
    MissingSavedConnection,
    InactiveConnection,
}

/// Planned state derived from a connection switch request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConnectionSwitchPlan {
    pub selection: QueryConnectionSelection,
    pub should_refresh_connection_surfaces: bool,
}

/// Typed planning result for a connection switch request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryConnectionSwitchPlanningOutcome {
    Ready(QueryConnectionSwitchPlan),
    MissingSavedConnection,
    InactiveConnection,
}

/// Planned state derived from selecting a specific database name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryDatabaseSelectionPlan {
    pub database_name: String,
    pub should_refresh_connection_surfaces: bool,
}

impl QueryWorkflowOutcome {
    pub fn success(&self) -> bool {
        match self {
            QueryWorkflowOutcome::Execute(outcome) => outcome.success,
            QueryWorkflowOutcome::Explain(outcome) => outcome.success,
        }
    }
}

/// Resolve which connection should be used for a query workflow request.
///
/// Selection policy is intentionally simple and stable across app surfaces:
/// prefer an explicit connection when provided, otherwise fall back to the
/// first available connected candidate.
pub fn resolve_workflow_connection_id(
    preferred_connection_id: Option<Uuid>,
    available_connection_ids: &[Uuid],
) -> Option<Uuid> {
    preferred_connection_id.or_else(|| available_connection_ids.first().copied())
}

/// Determine the default database label used for query-result display.
///
/// This keeps driver-specific display defaults out of app event handlers.
pub fn default_database_label_for_connection(
    driver_name: &str,
    params: &HashMap<String, String>,
) -> Option<String> {
    if let Some(label) = default_database_label_for_driver(driver_name) {
        return Some(label.to_string());
    }

    params
        .get("database")
        .cloned()
        .or_else(|| params.get("path").cloned())
}

/// Resolve query connection metadata for workflow execution and editor wiring.
///
/// If a preferred connection id is supplied it must exist in the candidate set,
/// otherwise no selection is returned. When no preferred id is supplied, the
/// first candidate is used.
pub fn resolve_query_connection_selection(
    preferred_connection_id: Option<Uuid>,
    candidates: &[QueryConnectionCandidate],
) -> Option<QueryConnectionSelection> {
    let candidate_ids: Vec<Uuid> = candidates
        .iter()
        .map(|candidate| candidate.connection_id)
        .collect();
    let selected_connection_id =
        resolve_workflow_connection_id(preferred_connection_id, &candidate_ids)?;
    let selected = candidates
        .iter()
        .find(|candidate| candidate.connection_id == selected_connection_id)?;

    Some(QueryConnectionSelection {
        connection_id: selected.connection_id,
        connection_name: selected.connection_name.clone(),
        driver_name: selected.driver_name.clone(),
        default_database_name: default_database_label_for_connection(
            &selected.driver_name,
            &selected.params,
        ),
    })
}

/// Resolve query-editor connection switch decisions.
///
/// This keeps saved-connection existence and active-connection gating rules in
/// `zqlz-query`, so app handlers only map typed outcomes to UI notifications.
pub fn resolve_query_connection_switch(
    selected_connection_id: Uuid,
    candidates: &[QueryConnectionCandidate],
    active_connection_ids: &[Uuid],
) -> QueryConnectionSwitchResolution {
    if !candidates
        .iter()
        .any(|candidate| candidate.connection_id == selected_connection_id)
    {
        return QueryConnectionSwitchResolution::MissingSavedConnection;
    }

    if !active_connection_ids.contains(&selected_connection_id) {
        return QueryConnectionSwitchResolution::InactiveConnection;
    }

    match resolve_query_connection_selection(Some(selected_connection_id), candidates) {
        Some(selection) => QueryConnectionSwitchResolution::Ready(selection),
        None => QueryConnectionSwitchResolution::MissingSavedConnection,
    }
}

/// Plan a query connection switch and derive follow-up refresh behavior.
pub fn plan_query_connection_switch(
    selected_connection_id: Uuid,
    candidates: &[QueryConnectionCandidate],
    active_connection_ids: &[Uuid],
    previous_active_database: Option<&str>,
) -> QueryConnectionSwitchPlanningOutcome {
    match resolve_query_connection_switch(selected_connection_id, candidates, active_connection_ids)
    {
        QueryConnectionSwitchResolution::Ready(selection) => {
            let should_refresh_connection_surfaces =
                should_refresh_connection_surfaces_for_database_selection(
                    previous_active_database,
                    selection.default_database_name.as_deref(),
                );

            QueryConnectionSwitchPlanningOutcome::Ready(QueryConnectionSwitchPlan {
                selection,
                should_refresh_connection_surfaces,
            })
        }
        QueryConnectionSwitchResolution::MissingSavedConnection => {
            QueryConnectionSwitchPlanningOutcome::MissingSavedConnection
        }
        QueryConnectionSwitchResolution::InactiveConnection => {
            QueryConnectionSwitchPlanningOutcome::InactiveConnection
        }
    }
}

/// Plan a database-selection transition and derive follow-up refresh behavior.
pub fn plan_query_database_selection(
    next_database_name: impl Into<String>,
    previous_active_database: Option<&str>,
) -> QueryDatabaseSelectionPlan {
    let database_name = next_database_name.into();
    let should_refresh_connection_surfaces =
        should_refresh_connection_surfaces_for_database_selection(
            previous_active_database,
            Some(database_name.as_str()),
        );

    QueryDatabaseSelectionPlan {
        database_name,
        should_refresh_connection_surfaces,
    }
}

/// Build query-editor switcher options and selected default database metadata.
///
/// Connection options are limited to active connections, while default database
/// metadata is resolved from saved-candidate data for the currently selected id.
pub fn build_query_editor_switcher_selection(
    selected_connection_id: Option<Uuid>,
    candidates: &[QueryConnectionCandidate],
    active_connection_ids: &[Uuid],
) -> QueryEditorSwitcherSelection {
    let available_connections = candidates
        .iter()
        .filter(|candidate| active_connection_ids.contains(&candidate.connection_id))
        .map(|candidate| QueryConnectionOption {
            connection_id: candidate.connection_id,
            connection_name: candidate.connection_name.clone(),
        })
        .collect();

    let selected_default_database_name = selected_connection_id
        .and_then(|connection_id| {
            candidates
                .iter()
                .find(|candidate| candidate.connection_id == connection_id)
        })
        .and_then(|candidate| {
            default_database_label_for_connection(&candidate.driver_name, &candidate.params)
        });

    QueryEditorSwitcherSelection {
        available_connections,
        selected_default_database_name,
    }
}

/// Resolve optional query-editor open-time connection metadata.
///
/// Opening a query editor should only bind a runtime connection when the selected
/// id is both present in saved candidates and currently active.
pub fn resolve_query_editor_open_connection(
    selected_connection_id: Option<Uuid>,
    candidates: &[QueryConnectionCandidate],
    active_connection_ids: &[Uuid],
) -> Option<QueryConnectionSelection> {
    let selected_connection_id = selected_connection_id?;
    match resolve_query_connection_switch(selected_connection_id, candidates, active_connection_ids)
    {
        QueryConnectionSwitchResolution::Ready(selection) => Some(selection),
        QueryConnectionSwitchResolution::MissingSavedConnection
        | QueryConnectionSwitchResolution::InactiveConnection => None,
    }
}

/// Decide whether a connection-surface refresh should run after a database-selection event.
///
/// A refresh is only needed when the selected database did not change compared to the
/// current workspace value, because database-change transitions trigger their own loading path.
pub fn should_refresh_connection_surfaces_for_database_selection(
    previous_active_database: Option<&str>,
    next_active_database: Option<&str>,
) -> bool {
    previous_active_database == next_active_database
}

/// Normalize SQL selected for execution and attach destructive-operation metadata.
pub fn normalize_query_execution_request(
    sql: impl Into<String>,
    source: QuerySqlSource,
    params: Option<QueryExecutionParams>,
    confirm_destructive: bool,
) -> NormalizedQueryExecutionRequest {
    let sql = sql.into();
    let destructive_warning = if confirm_destructive {
        QueryEngine::new().analyze_for_destructive_operations(&sql)
    } else {
        None
    };

    NormalizedQueryExecutionRequest {
        sql,
        source,
        params,
        destructive_warning,
    }
}

/// Execute a query request using optional execution parameters.
///
/// This keeps parameter dispatch rules in `zqlz-query` so app-layer handlers only
/// route UI events and consume typed outcomes.
pub async fn execute_query_request(
    query_service: &QueryService,
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    sql: &str,
    params: Option<&QueryExecutionParams>,
) -> QueryServiceResult<view_models::QueryExecution> {
    match params {
        Some(QueryExecutionParams::Positional(positional_params)) => {
            query_service
                .execute_query_with_positional_params(
                    connection,
                    connection_id,
                    sql,
                    positional_params,
                )
                .await
        }
        Some(QueryExecutionParams::Named(named_params)) => {
            query_service
                .execute_query_with_named_params(connection, connection_id, sql, named_params)
                .await
        }
        None => {
            query_service
                .execute_query(connection, connection_id, sql)
                .await
        }
    }
}

/// Execute an EXPLAIN request for a SQL string.
pub async fn execute_explain_request(
    query_service: &QueryService,
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    sql: &str,
) -> QueryServiceResult<view_models::ExplainResult> {
    query_service
        .explain_query(connection, connection_id, sql)
        .await
}

/// Convert a query-service execution response into the UI payload used by results panel widgets.
pub fn build_query_execution_outcome(
    service_execution: Result<view_models::QueryExecution, QueryServiceError>,
    requested_sql: String,
    display_context: QueryDisplayContext,
    completed_at: DateTime<Utc>,
) -> QueryExecutionOutcome {
    let execution = match service_execution {
        Ok(execution) => QueryExecution {
            sql: execution.sql,
            start_time: completed_at - Duration::milliseconds(execution.duration_ms as i64),
            end_time: completed_at,
            duration_ms: execution.duration_ms,
            connection_name: display_context.connection_name,
            database_name: display_context.database_name,
            statements: execution
                .statements
                .into_iter()
                .map(|statement| StatementResult {
                    sql: statement.sql,
                    duration_ms: statement.duration_ms,
                    result: statement.result,
                    error: statement.error,
                    affected_rows: statement.affected_rows,
                })
                .collect(),
        },
        Err(error) => QueryExecution {
            sql: requested_sql,
            start_time: completed_at,
            end_time: completed_at,
            duration_ms: 0,
            connection_name: display_context.connection_name,
            database_name: display_context.database_name,
            statements: vec![StatementResult {
                sql: String::new(),
                duration_ms: 0,
                result: None,
                error: Some(format!("Service error: {}", error)),
                affected_rows: 0,
            }],
        },
    };

    let success = execution
        .statements
        .iter()
        .all(|statement| statement.error.is_none());

    QueryExecutionOutcome { execution, success }
}

/// Convert a query-service explain response into the UI payload used by results panel widgets.
pub fn build_explain_execution_outcome(
    service_result: Result<view_models::ExplainResult, QueryServiceError>,
    requested_sql: String,
    display_context: QueryDisplayContext,
    timestamp: DateTime<Utc>,
) -> ExplainExecutionOutcome {
    let success = service_result.is_ok();

    let explain_result = match service_result {
        Ok(result) => ExplainResult {
            sql: result.sql,
            duration_ms: result.duration_ms,
            raw_output: result.raw_output,
            query_plan: result.query_plan,
            analyzed_plan: result.analyzed_plan,
            provider_id: result.provider_id,
            error: result.error,
            connection_name: display_context.connection_name,
            database_name: display_context.database_name,
            timestamp,
        },
        Err(error) => ExplainResult {
            sql: requested_sql,
            duration_ms: 0,
            raw_output: None,
            query_plan: None,
            analyzed_plan: None,
            provider_id: None,
            error: Some(format!("Service error: {}", error)),
            connection_name: display_context.connection_name,
            database_name: display_context.database_name,
            timestamp,
        },
    };

    ExplainExecutionOutcome {
        explain_result,
        success,
    }
}

/// Execute a typed query workflow request and return a typed outcome.
///
/// This keeps query/explain branching in `zqlz-query`, so app-layer handlers only
/// supply execution context and consume a single workflow result type.
pub async fn run_query_workflow(
    query_service: &QueryService,
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    request: QueryWorkflowRequest,
    display_context: QueryDisplayContext,
    completed_at: DateTime<Utc>,
) -> QueryWorkflowOutcome {
    match request {
        QueryWorkflowRequest::Execute { sql, params, .. } => {
            let service_execution = execute_query_request(
                query_service,
                connection,
                connection_id,
                &sql,
                params.as_ref(),
            )
            .await;

            QueryWorkflowOutcome::Execute(Box::new(build_query_execution_outcome(
                service_execution,
                sql,
                display_context,
                completed_at,
            )))
        }
        QueryWorkflowRequest::Explain { sql } => {
            let service_result =
                execute_explain_request(query_service, connection, connection_id, &sql).await;

            QueryWorkflowOutcome::Explain(Box::new(build_explain_execution_outcome(
                service_result,
                sql,
                display_context,
                completed_at,
            )))
        }
    }
}

/// Execute a typed "run query" workflow directly without outcome enum routing.
pub async fn run_execute_query_workflow(
    query_service: &QueryService,
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    sql: String,
    params: Option<QueryExecutionParams>,
    display_context: QueryDisplayContext,
    completed_at: DateTime<Utc>,
) -> QueryExecutionOutcome {
    let service_execution = execute_query_request(
        query_service,
        connection,
        connection_id,
        &sql,
        params.as_ref(),
    )
    .await;

    build_query_execution_outcome(service_execution, sql, display_context, completed_at)
}

/// Execute a typed "explain query" workflow directly without outcome enum routing.
pub async fn run_explain_query_workflow(
    query_service: &QueryService,
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    sql: String,
    display_context: QueryDisplayContext,
    completed_at: DateTime<Utc>,
) -> ExplainExecutionOutcome {
    let service_result =
        execute_explain_request(query_service, connection, connection_id, &sql).await;

    build_explain_execution_outcome(service_result, sql, display_context, completed_at)
}

#[cfg(test)]
mod tests {
    use super::{
        QueryConnectionCandidate, QueryConnectionSwitchPlanningOutcome,
        QueryConnectionSwitchResolution, QuerySqlSource, build_query_editor_switcher_selection,
        default_database_label_for_connection, normalize_query_execution_request,
        plan_query_connection_switch, plan_query_database_selection,
        resolve_query_connection_selection, resolve_query_connection_switch,
        resolve_query_editor_open_connection, resolve_workflow_connection_id,
        should_refresh_connection_surfaces_for_database_selection,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn resolve_workflow_connection_prefers_explicit_id() {
        let explicit = Uuid::new_v4();
        let fallback = Uuid::new_v4();
        let selected = resolve_workflow_connection_id(Some(explicit), &[fallback]);
        assert_eq!(selected, Some(explicit));
    }

    #[test]
    fn resolve_workflow_connection_uses_first_available_when_missing_explicit() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let selected = resolve_workflow_connection_id(None, &[first, second]);
        assert_eq!(selected, Some(first));
    }

    #[test]
    fn resolve_workflow_connection_returns_none_without_candidates() {
        let selected = resolve_workflow_connection_id(None, &[]);
        assert_eq!(selected, None);
    }

    #[test]
    fn default_database_label_for_connection_uses_sqlite_main() {
        let params = HashMap::from([(String::from("path"), String::from("/tmp/test.db"))]);
        let label = default_database_label_for_connection("sqlite", &params);
        assert_eq!(label.as_deref(), Some("main"));
    }

    #[test]
    fn default_database_label_for_connection_prefers_database_param() {
        let params = HashMap::from([
            (String::from("database"), String::from("analytics")),
            (String::from("path"), String::from("/tmp/test.db")),
        ]);
        let label = default_database_label_for_connection("postgresql", &params);
        assert_eq!(label.as_deref(), Some("analytics"));
    }

    #[test]
    fn default_database_label_for_connection_falls_back_to_path_param() {
        let params = HashMap::from([(String::from("path"), String::from("db.sqlite"))]);
        let label = default_database_label_for_connection("duckdb", &params);
        assert_eq!(label.as_deref(), Some("db.sqlite"));
    }

    #[test]
    fn should_refresh_connection_surfaces_when_database_selection_is_unchanged() {
        assert!(should_refresh_connection_surfaces_for_database_selection(
            Some("analytics"),
            Some("analytics"),
        ));
        assert!(should_refresh_connection_surfaces_for_database_selection(
            None, None,
        ));
    }

    #[test]
    fn should_not_refresh_connection_surfaces_when_database_selection_changes() {
        assert!(!should_refresh_connection_surfaces_for_database_selection(
            Some("analytics"),
            Some("warehouse"),
        ));
        assert!(!should_refresh_connection_surfaces_for_database_selection(
            Some("analytics"),
            None,
        ));
        assert!(!should_refresh_connection_surfaces_for_database_selection(
            None,
            Some("analytics"),
        ));
    }

    #[test]
    fn resolve_query_connection_selection_uses_explicit_candidate_when_present() {
        let selected_id = Uuid::new_v4();
        let candidates = vec![
            QueryConnectionCandidate {
                connection_id: selected_id,
                connection_name: "Primary".to_string(),
                driver_name: "postgres".to_string(),
                params: HashMap::from([(String::from("database"), String::from("app"))]),
            },
            QueryConnectionCandidate {
                connection_id: Uuid::new_v4(),
                connection_name: "Fallback".to_string(),
                driver_name: "sqlite".to_string(),
                params: HashMap::from([(String::from("path"), String::from("test.db"))]),
            },
        ];

        let selection = resolve_query_connection_selection(Some(selected_id), &candidates)
            .expect("selection should exist");

        assert_eq!(selection.connection_id, selected_id);
        assert_eq!(selection.connection_name, "Primary");
        assert_eq!(selection.default_database_name.as_deref(), Some("app"));
    }

    #[test]
    fn resolve_query_connection_selection_uses_first_candidate_without_preference() {
        let first_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: first_id,
            connection_name: "Local SQLite".to_string(),
            driver_name: "sqlite".to_string(),
            params: HashMap::from([(String::from("path"), String::from("local.db"))]),
        }];

        let selection =
            resolve_query_connection_selection(None, &candidates).expect("selection should exist");

        assert_eq!(selection.connection_id, first_id);
        assert_eq!(selection.default_database_name.as_deref(), Some("main"));
    }

    #[test]
    fn resolve_query_connection_selection_returns_none_for_missing_explicit_candidate() {
        let candidates = vec![QueryConnectionCandidate {
            connection_id: Uuid::new_v4(),
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::new(),
        }];

        let selection = resolve_query_connection_selection(Some(Uuid::new_v4()), &candidates);
        assert!(selection.is_none());
    }

    #[test]
    fn resolve_query_connection_switch_returns_missing_saved_connection() {
        let selected_connection_id = Uuid::new_v4();
        let resolution =
            resolve_query_connection_switch(selected_connection_id, &[], &[selected_connection_id]);

        assert_eq!(
            resolution,
            QueryConnectionSwitchResolution::MissingSavedConnection
        );
    }

    #[test]
    fn resolve_query_connection_switch_returns_inactive_connection() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::new(),
        }];

        let resolution = resolve_query_connection_switch(selected_connection_id, &candidates, &[]);

        assert_eq!(
            resolution,
            QueryConnectionSwitchResolution::InactiveConnection
        );
    }

    #[test]
    fn resolve_query_connection_switch_returns_ready_for_active_candidate() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "sqlite".to_string(),
            params: HashMap::new(),
        }];

        let resolution = resolve_query_connection_switch(
            selected_connection_id,
            &candidates,
            &[selected_connection_id],
        );

        match resolution {
            QueryConnectionSwitchResolution::Ready(selection) => {
                assert_eq!(selection.connection_id, selected_connection_id);
                assert_eq!(selection.connection_name, "Primary");
                assert_eq!(selection.default_database_name.as_deref(), Some("main"));
            }
            unexpected => panic!("expected Ready resolution, got {unexpected:?}"),
        }
    }

    #[test]
    fn build_query_editor_switcher_selection_filters_to_active_connections() {
        let active_connection_id = Uuid::new_v4();
        let inactive_connection_id = Uuid::new_v4();
        let candidates = vec![
            QueryConnectionCandidate {
                connection_id: active_connection_id,
                connection_name: "Active".to_string(),
                driver_name: "postgres".to_string(),
                params: HashMap::new(),
            },
            QueryConnectionCandidate {
                connection_id: inactive_connection_id,
                connection_name: "Inactive".to_string(),
                driver_name: "postgres".to_string(),
                params: HashMap::new(),
            },
        ];

        let selection =
            build_query_editor_switcher_selection(None, &candidates, &[active_connection_id]);

        assert_eq!(selection.available_connections.len(), 1);
        assert_eq!(
            selection.available_connections[0].connection_id,
            active_connection_id
        );
    }

    #[test]
    fn build_query_editor_switcher_selection_uses_selected_default_database() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::from([(String::from("database"), String::from("analytics"))]),
        }];

        let selection = build_query_editor_switcher_selection(
            Some(selected_connection_id),
            &candidates,
            &[selected_connection_id],
        );

        assert_eq!(
            selection.selected_default_database_name.as_deref(),
            Some("analytics")
        );
    }

    #[test]
    fn resolve_query_editor_open_connection_returns_ready_selection() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::from([(String::from("database"), String::from("app"))]),
        }];

        let selection = resolve_query_editor_open_connection(
            Some(selected_connection_id),
            &candidates,
            &[selected_connection_id],
        )
        .expect("selection should exist");

        assert_eq!(selection.connection_id, selected_connection_id);
        assert_eq!(selection.connection_name, "Primary");
        assert_eq!(selection.driver_name, "postgres");
    }

    #[test]
    fn resolve_query_editor_open_connection_returns_none_without_selected_connection() {
        let selection = resolve_query_editor_open_connection(None, &[], &[]);
        assert!(selection.is_none());
    }

    #[test]
    fn resolve_query_editor_open_connection_returns_none_for_inactive_selection() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::new(),
        }];

        let selection =
            resolve_query_editor_open_connection(Some(selected_connection_id), &candidates, &[]);
        assert!(selection.is_none());
    }

    #[test]
    fn plan_query_connection_switch_returns_ready_with_refresh_policy() {
        let selected_connection_id = Uuid::new_v4();
        let candidates = vec![QueryConnectionCandidate {
            connection_id: selected_connection_id,
            connection_name: "Primary".to_string(),
            driver_name: "postgres".to_string(),
            params: HashMap::from([(String::from("database"), String::from("analytics"))]),
        }];

        let outcome = plan_query_connection_switch(
            selected_connection_id,
            &candidates,
            &[selected_connection_id],
            Some("analytics"),
        );

        match outcome {
            QueryConnectionSwitchPlanningOutcome::Ready(plan) => {
                assert_eq!(plan.selection.connection_id, selected_connection_id);
                assert!(plan.should_refresh_connection_surfaces);
            }
            unexpected => panic!("expected Ready planning outcome, got {unexpected:?}"),
        }
    }

    #[test]
    fn plan_query_database_selection_sets_refresh_policy_for_unchanged_database() {
        let plan = plan_query_database_selection("analytics", Some("analytics"));
        assert_eq!(plan.database_name, "analytics");
        assert!(plan.should_refresh_connection_surfaces);
    }

    #[test]
    fn plan_query_database_selection_disables_refresh_for_database_change() {
        let plan = plan_query_database_selection("warehouse", Some("analytics"));
        assert_eq!(plan.database_name, "warehouse");
        assert!(!plan.should_refresh_connection_surfaces);
    }

    #[test]
    fn normalize_query_execution_request_preserves_source_and_detects_destructive_sql() {
        let request = normalize_query_execution_request(
            "DELETE FROM users",
            QuerySqlSource::CurrentStatement,
            None,
            true,
        );

        assert_eq!(request.source, QuerySqlSource::CurrentStatement);
        assert!(request.destructive_warning.is_some());
    }

    #[test]
    fn normalize_query_execution_request_skips_destructive_detection_when_disabled() {
        let request = normalize_query_execution_request(
            "DROP TABLE users",
            QuerySqlSource::Selection,
            None,
            false,
        );

        assert_eq!(request.source, QuerySqlSource::Selection);
        assert!(request.destructive_warning.is_none());
    }

    #[test]
    fn query_workflow_dispatch_execute_normalizes_request_and_history_policy() {
        let dispatch = super::QueryWorkflowDispatch::execute(
            None,
            Some("erp_lab".to_string()),
            "DROP TABLE users",
            None,
            QuerySqlSource::FullBuffer,
            true,
            "run query",
        );

        assert_eq!(dispatch.start_log_message, "run query");
        assert_eq!(dispatch.selected_database_name.as_deref(), Some("erp_lab"));
        assert_eq!(
            dispatch.no_connection_log_message,
            "No connection available for query execution"
        );
        assert!(dispatch.refresh_history);
        match dispatch.request {
            super::QueryWorkflowRequest::Execute {
                source,
                destructive_warning,
                ..
            } => {
                assert_eq!(source, QuerySqlSource::FullBuffer);
                assert!(destructive_warning.is_some());
            }
            super::QueryWorkflowRequest::Explain { .. } => {
                panic!("expected execute dispatch")
            }
        }
    }

    #[test]
    fn query_workflow_dispatch_explain_disables_history_refresh() {
        let dispatch = super::QueryWorkflowDispatch::explain(None, None, "SELECT 1", "explain");

        assert_eq!(dispatch.start_log_message, "explain");
        assert_eq!(
            dispatch.no_connection_log_message,
            "No connection available for explain"
        );
        assert!(!dispatch.refresh_history);
        assert!(matches!(
            dispatch.request,
            super::QueryWorkflowRequest::Explain { .. }
        ));
    }
}
