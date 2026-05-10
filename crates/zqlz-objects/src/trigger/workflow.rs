use std::sync::Arc;

use thiserror::Error;
use zqlz_core::{Connection, ObjectType, connection_is_postgres};

use crate::{
    DropTriggerStatementRequest, ObjectDefinitionRequest, build_drop_trigger_statement,
    fetch_object_definition,
};

const CREATE_KEYWORD: &str = "CREATE";
const TRIGGER_KEYWORD: &str = "TRIGGER";

#[derive(Debug, Clone)]
pub struct TriggerReplacePlanRequest {
    pub is_new: bool,
    pub trigger_name: Option<String>,
}

impl TriggerReplacePlanRequest {
    pub fn new(is_new: bool, trigger_name: Option<String>) -> Self {
        Self {
            is_new,
            trigger_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TriggerReplacePlan {
    pub drop_statement: Option<String>,
}

#[derive(Debug, Error)]
pub enum TriggerWorkflowError {
    #[error("Failed to build trigger drop SQL: {0}")]
    DropStatementBuildFailed(String),
}

#[derive(Debug, Error)]
pub enum TriggerDefinitionError {
    #[error("Failed to fetch trigger definition: {0}")]
    FetchFailed(String),
}

pub fn plan_trigger_replace_execution(
    connection: &Arc<dyn Connection>,
    request: &TriggerReplacePlanRequest,
) -> Result<TriggerReplacePlan, TriggerWorkflowError> {
    if request.is_new || connection_is_postgres(connection.as_ref()) {
        return Ok(TriggerReplacePlan {
            drop_statement: None,
        });
    }

    let drop_statement = match request.trigger_name.as_deref() {
        Some(trigger_name) => Some(
            build_drop_trigger_statement(
                connection,
                &DropTriggerStatementRequest::new(trigger_name).with_table_name(None),
            )
            .map_err(|error| TriggerWorkflowError::DropStatementBuildFailed(error.to_string()))?,
        ),
        None => None,
    };

    Ok(TriggerReplacePlan { drop_statement })
}

pub fn extract_trigger_name_from_create_statement(definition: &str) -> Option<String> {
    let upper = definition.to_uppercase();
    let trigger_definition_prefix = format!("{CREATE_KEYWORD} {TRIGGER_KEYWORD}");
    let pos = upper.find(&trigger_definition_prefix)?;
    let after_create = &definition[pos + 14..];
    let trimmed = after_create.trim_start();

    let trimmed_upper = trimmed.to_uppercase();
    let trimmed = if let Some(stripped) = trimmed_upper.strip_prefix("OR REPLACE") {
        trimmed[trimmed.len() - stripped.len()..].trim_start()
    } else if let Some(stripped) = trimmed_upper.strip_prefix("IF NOT EXISTS") {
        trimmed[trimmed.len() - stripped.len()..].trim_start()
    } else {
        trimmed
    };

    if let Some(stripped) = trimmed.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else if let Some(stripped) = trimmed.strip_prefix('`') {
        let end = stripped.find('`')?;
        Some(stripped[..end].to_string())
    } else if let Some(stripped) = trimmed.strip_prefix('[') {
        let end = stripped.find(']')?;
        Some(stripped[..end].to_string())
    } else {
        let end = trimmed.find(char::is_whitespace)?;
        Some(trimmed[..end].to_string())
    }
}

pub async fn fetch_trigger_definition(
    connection: &Arc<dyn Connection>,
    schema_name: Option<&str>,
    trigger_name: &str,
) -> Result<String, TriggerDefinitionError> {
    fetch_object_definition(
        connection,
        &ObjectDefinitionRequest::new(ObjectType::Trigger, trigger_name)
            .with_schema(schema_name.map(ToOwned::to_owned)),
    )
    .await
    .map_err(|error| TriggerDefinitionError::FetchFailed(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::extract_trigger_name_from_create_statement;

    #[test]
    fn extracts_unquoted_trigger_name() {
        let sql = "CREATE TRIGGER trigger_name AFTER INSERT ON table_name FOR EACH ROW BEGIN SELECT 1; END;";
        assert_eq!(
            extract_trigger_name_from_create_statement(sql),
            Some("trigger_name".to_string())
        );
    }

    #[test]
    fn extracts_quoted_trigger_name_with_optional_prefixes() {
        let sql = "CREATE TRIGGER IF NOT EXISTS \"audit-trigger\" AFTER UPDATE ON users FOR EACH ROW BEGIN SELECT 1; END;";
        assert_eq!(
            extract_trigger_name_from_create_statement(sql),
            Some("audit-trigger".to_string())
        );
    }
}
