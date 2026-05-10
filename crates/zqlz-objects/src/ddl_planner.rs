use std::sync::Arc;

use thiserror::Error;
use zqlz_core::{
    Connection, DropTriggerOptions, DropViewOptions, SqlObjectName, connection_is_postgres,
};

#[derive(Debug, Clone)]
pub struct DropTriggerStatementRequest {
    pub trigger_name: String,
    pub table_name: Option<String>,
}

impl DropTriggerStatementRequest {
    pub fn new(trigger_name: impl Into<String>) -> Self {
        Self {
            trigger_name: trigger_name.into(),
            table_name: None,
        }
    }

    pub fn with_table_name(mut self, table_name: Option<String>) -> Self {
        self.table_name = table_name;
        self
    }
}

#[derive(Debug, Error)]
pub enum DdlPlannerError {
    #[error("{0}")]
    DriverError(String),
}

pub fn build_drop_view_statement(
    connection: &Arc<dyn Connection>,
    view_name: &str,
    include_if_exists: bool,
) -> Result<String, DdlPlannerError> {
    connection
        .drop_view_sql(
            &SqlObjectName::new(view_name),
            DropViewOptions {
                if_exists: include_if_exists,
                cascade: false,
            },
        )
        .map_err(|error| DdlPlannerError::DriverError(error.to_string()))
}

pub fn build_drop_trigger_statement(
    connection: &Arc<dyn Connection>,
    request: &DropTriggerStatementRequest,
) -> Result<String, DdlPlannerError> {
    let trigger_object = SqlObjectName::new(&request.trigger_name);
    let table_object = request
        .table_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .map(|name| {
            split_unquoted_schema_separator(name)
                .map(|dot_index| {
                    SqlObjectName::with_namespace(
                        normalize_identifier_segment(&name[..dot_index]),
                        normalize_identifier_segment(&name[dot_index + 1..]),
                    )
                })
                .unwrap_or_else(|| SqlObjectName::new(normalize_identifier_segment(name)))
        });

    connection
        .drop_trigger_sql(
            &trigger_object,
            table_object.as_ref(),
            DropTriggerOptions {
                if_exists: true,
                cascade: connection_is_postgres(connection.as_ref()),
            },
        )
        .map_err(|error| DdlPlannerError::DriverError(error.to_string()))
}

fn normalize_identifier_segment(segment: &str) -> String {
    segment
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

fn split_unquoted_schema_separator(identifier: &str) -> Option<usize> {
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut in_bracket_quote = false;

    let mut characters = identifier.char_indices().peekable();
    while let Some((index, character)) = characters.next() {
        if in_double_quote {
            if character == '"' {
                if matches!(characters.peek(), Some((_, '"'))) {
                    characters.next();
                } else {
                    in_double_quote = false;
                }
            }
            continue;
        }

        if in_backtick_quote {
            if character == '`' {
                if matches!(characters.peek(), Some((_, '`'))) {
                    characters.next();
                } else {
                    in_backtick_quote = false;
                }
            }
            continue;
        }

        if in_bracket_quote {
            if character == ']' {
                if matches!(characters.peek(), Some((_, ']'))) {
                    characters.next();
                } else {
                    in_bracket_quote = false;
                }
            }
            continue;
        }

        match character {
            '"' => in_double_quote = true,
            '`' => in_backtick_quote = true,
            '[' => in_bracket_quote = true,
            '.' => return Some(index),
            _ => {}
        }
    }

    None
}
