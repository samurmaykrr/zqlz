use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableWorkflowOperation {
    Open,
    Design,
    Delete,
    Duplicate,
    Empty,
}

impl fmt::Display for TableWorkflowOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Open => "open",
            Self::Design => "design",
            Self::Delete => "delete",
            Self::Duplicate => "duplicate",
            Self::Empty => "empty",
        };

        formatter.write_str(value)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TableWorkflowError {
    #[error("{operation} workflow requires at least one table name")]
    NoTablesSelected { operation: TableWorkflowOperation },
    #[error("{operation} workflow received a blank table name at index {index}")]
    BlankTableName {
        operation: TableWorkflowOperation,
        index: usize,
    },
}

fn validate_table_names(
    operation: TableWorkflowOperation,
    table_names: &[String],
) -> Result<(), TableWorkflowError> {
    if table_names.is_empty() {
        return Err(TableWorkflowError::NoTablesSelected { operation });
    }

    if let Some(index) = table_names
        .iter()
        .position(|table_name| table_name.trim().is_empty())
    {
        return Err(TableWorkflowError::BlankTableName { operation, index });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenTablesDecisionRequest {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
    pub database_name: Option<String>,
    pub is_view: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenTableViewerRequest {
    pub connection_id: Uuid,
    pub table_name: String,
    pub database_name: Option<String>,
    pub is_view: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenTablesDecision {
    pub requests: Vec<OpenTableViewerRequest>,
}

/// Input for deciding whether an open table viewer needs a background row count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenTableViewerCountDecisionRequest {
    pub total_rows: Option<u64>,
    pub is_key_value: bool,
    pub supports_fast_exact_count: bool,
}

/// Decision describing whether the open-viewer flow should fetch a background count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenTableViewerCountDecision {
    pub needs_background_count: bool,
}

/// Decides whether the open-viewer flow needs a background count request.
pub fn decide_open_table_viewer_count(
    request: OpenTableViewerCountDecisionRequest,
) -> OpenTableViewerCountDecision {
    OpenTableViewerCountDecision {
        needs_background_count: request.total_rows.is_none()
            && !request.is_key_value
            && !request.supports_fast_exact_count,
    }
}

pub fn decide_open_tables(
    request: OpenTablesDecisionRequest,
) -> Result<OpenTablesDecision, TableWorkflowError> {
    validate_table_names(TableWorkflowOperation::Open, &request.table_names)?;

    let requests = request
        .table_names
        .into_iter()
        .map(|table_name| OpenTableViewerRequest {
            connection_id: request.connection_id,
            table_name,
            database_name: request.database_name.clone(),
            is_view: request.is_view,
        })
        .collect();

    Ok(OpenTablesDecision { requests })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesignTablesDecisionRequest {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesignTableRequest {
    pub connection_id: Uuid,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesignTablesDecision {
    pub requests: Vec<DesignTableRequest>,
}

pub fn decide_design_tables(
    request: DesignTablesDecisionRequest,
) -> Result<DesignTablesDecision, TableWorkflowError> {
    validate_table_names(TableWorkflowOperation::Design, &request.table_names)?;

    let requests = request
        .table_names
        .into_iter()
        .map(|table_name| DesignTableRequest {
            connection_id: request.connection_id,
            table_name,
        })
        .collect();

    Ok(DesignTablesDecision { requests })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTablesDecisionRequest {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSingleDecision {
    pub connection_id: Uuid,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteBatchDecision {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
    pub continue_on_error_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteTablesDecision {
    Single(DeleteSingleDecision),
    Batch(DeleteBatchDecision),
}

pub fn decide_delete_tables(
    request: DeleteTablesDecisionRequest,
) -> Result<DeleteTablesDecision, TableWorkflowError> {
    validate_table_names(TableWorkflowOperation::Delete, &request.table_names)?;

    if request.table_names.len() == 1 {
        return Ok(DeleteTablesDecision::Single(DeleteSingleDecision {
            connection_id: request.connection_id,
            table_name: request.table_names[0].clone(),
        }));
    }

    Ok(DeleteTablesDecision::Batch(DeleteBatchDecision {
        connection_id: request.connection_id,
        table_names: request.table_names,
        continue_on_error_default: false,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateTablesDecisionRequest {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateSingleDecision {
    pub connection_id: Uuid,
    pub source_table_name: String,
    pub suggested_table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateBatchDecision {
    pub connection_id: Uuid,
    pub source_table_names: Vec<String>,
    pub suggested_suffix: String,
    pub continue_on_error_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuplicateTablesDecision {
    Single(DuplicateSingleDecision),
    Batch(DuplicateBatchDecision),
}

pub fn decide_duplicate_tables(
    request: DuplicateTablesDecisionRequest,
) -> Result<DuplicateTablesDecision, TableWorkflowError> {
    validate_table_names(TableWorkflowOperation::Duplicate, &request.table_names)?;

    if request.table_names.len() == 1 {
        let source_table_name = request.table_names[0].clone();
        return Ok(DuplicateTablesDecision::Single(DuplicateSingleDecision {
            connection_id: request.connection_id,
            suggested_table_name: format!("{}_copy", source_table_name),
            source_table_name,
        }));
    }

    Ok(DuplicateTablesDecision::Batch(DuplicateBatchDecision {
        connection_id: request.connection_id,
        source_table_names: request.table_names,
        suggested_suffix: "_copy".to_string(),
        continue_on_error_default: true,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyTablesDecisionRequest {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptySingleDecision {
    pub connection_id: Uuid,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyBatchDecision {
    pub connection_id: Uuid,
    pub table_names: Vec<String>,
    pub continue_on_error_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmptyTablesDecision {
    Single(EmptySingleDecision),
    Batch(EmptyBatchDecision),
}

pub fn decide_empty_tables(
    request: EmptyTablesDecisionRequest,
) -> Result<EmptyTablesDecision, TableWorkflowError> {
    validate_table_names(TableWorkflowOperation::Empty, &request.table_names)?;

    if request.table_names.len() == 1 {
        return Ok(EmptyTablesDecision::Single(EmptySingleDecision {
            connection_id: request.connection_id,
            table_name: request.table_names[0].clone(),
        }));
    }

    Ok(EmptyTablesDecision::Batch(EmptyBatchDecision {
        connection_id: request.connection_id,
        table_names: request.table_names,
        continue_on_error_default: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_decision_defaults_match_existing_ui_behavior() {
        let connection_id = Uuid::new_v4();
        let decision = decide_duplicate_tables(DuplicateTablesDecisionRequest {
            connection_id,
            table_names: vec!["users".to_string(), "roles".to_string()],
        })
        .expect("expected duplicate decision");

        let DuplicateTablesDecision::Batch(batch) = decision else {
            panic!("expected batch duplicate decision");
        };

        assert_eq!(batch.suggested_suffix, "_copy");
        assert!(batch.continue_on_error_default);
    }

    #[test]
    fn open_decision_rejects_empty_input() {
        let connection_id = Uuid::new_v4();
        let error = decide_open_tables(OpenTablesDecisionRequest {
            connection_id,
            table_names: Vec::new(),
            database_name: None,
            is_view: false,
        })
        .expect_err("expected no table selection error");

        assert_eq!(
            error,
            TableWorkflowError::NoTablesSelected {
                operation: TableWorkflowOperation::Open,
            }
        );
    }

    #[test]
    fn open_viewer_count_decision_skips_when_total_rows_present() {
        let decision = decide_open_table_viewer_count(OpenTableViewerCountDecisionRequest {
            total_rows: Some(123),
            is_key_value: false,
            supports_fast_exact_count: false,
        });

        assert!(!decision.needs_background_count);
    }

    #[test]
    fn open_viewer_count_decision_skips_for_key_value() {
        let decision = decide_open_table_viewer_count(OpenTableViewerCountDecisionRequest {
            total_rows: None,
            is_key_value: true,
            supports_fast_exact_count: false,
        });

        assert!(!decision.needs_background_count);
    }

    #[test]
    fn open_viewer_count_decision_skips_when_fast_exact_count_supported() {
        let decision = decide_open_table_viewer_count(OpenTableViewerCountDecisionRequest {
            total_rows: None,
            is_key_value: false,
            supports_fast_exact_count: true,
        });

        assert!(!decision.needs_background_count);
    }

    #[test]
    fn open_viewer_count_decision_needs_background_count_otherwise() {
        let decision = decide_open_table_viewer_count(OpenTableViewerCountDecisionRequest {
            total_rows: None,
            is_key_value: false,
            supports_fast_exact_count: false,
        });

        assert!(decision.needs_background_count);
    }
}
