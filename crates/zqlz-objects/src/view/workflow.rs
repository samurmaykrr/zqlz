use std::sync::Arc;

use thiserror::Error;
use zqlz_core::{
    Connection, DropViewOptions, ObjectType, SqlObjectName,
    connection_supports_create_or_replace_view,
};

use crate::{ObjectDefinitionRequest, fetch_object_definition};

const CREATE_KEYWORD: &str = "CREATE";

#[derive(Debug, Clone)]
pub struct ViewSaveExecutionRequest {
    pub is_new: bool,
    pub view_name: String,
    pub definition: String,
}

impl ViewSaveExecutionRequest {
    pub fn new(view_name: impl Into<String>, definition: impl Into<String>, is_new: bool) -> Self {
        Self {
            is_new,
            view_name: view_name.into(),
            definition: definition.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ViewSaveExecutionPlan {
    pub stored_definition: String,
    pub executable_definition: String,
    pub drop_statement: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReplaceViewIdentifierRequest {
    pub definition: String,
    pub new_view_name: String,
}

impl ReplaceViewIdentifierRequest {
    pub fn new(definition: impl Into<String>, new_view_name: impl Into<String>) -> Self {
        Self {
            definition: definition.into(),
            new_view_name: new_view_name.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplaceViewIdentifierResult {
    pub replaced_definition: String,
    pub parsed_schema: Option<String>,
    pub parsed_view_name: String,
}

#[derive(Debug, Clone)]
pub struct DuplicateViewRequest {
    pub source_view_name: String,
    pub target_view_name: String,
    pub source_schema: Option<String>,
}

impl DuplicateViewRequest {
    pub fn new(source_view_name: impl Into<String>, target_view_name: impl Into<String>) -> Self {
        Self {
            source_view_name: source_view_name.into(),
            target_view_name: target_view_name.into(),
            source_schema: None,
        }
    }

    pub fn with_source_schema(mut self, source_schema: Option<String>) -> Self {
        self.source_schema = source_schema;
        self
    }
}

#[derive(Debug, Error)]
pub enum ViewSavePlanningError {
    #[error("Failed to build drop view SQL: {0}")]
    DropViewStatementBuildFailed(String),
    #[error("View save expects full CREATE VIEW DDL in the editor content")]
    InvalidCreateViewDefinition,
}

#[derive(Debug, Error)]
pub enum DuplicateViewError {
    #[error("Failed to fetch source view definition: {0}")]
    FetchDefinition(String),
    #[error("Failed to parse source view DDL")]
    ParseDefinition,
}

pub fn plan_view_save_execution(
    connection: &Arc<dyn Connection>,
    request: &ViewSaveExecutionRequest,
) -> Result<ViewSaveExecutionPlan, ViewSavePlanningError> {
    let stored_definition = request.definition.trim().to_string();

    let drop_statement =
        if !request.is_new && !connection_supports_create_or_replace_view(connection.as_ref()) {
            Some(
                connection
                    .drop_view_sql(
                        &SqlObjectName::new(&request.view_name),
                        DropViewOptions {
                            if_exists: true,
                            cascade: false,
                        },
                    )
                    .map_err(|error| {
                        ViewSavePlanningError::DropViewStatementBuildFailed(error.to_string())
                    })?,
            )
        } else {
            None
        };

    let executable_definition =
        if !request.is_new && connection_supports_create_or_replace_view(connection.as_ref()) {
            connection.normalize_create_view_sql(&stored_definition)
        } else {
            stored_definition.clone()
        };

    Ok(ViewSaveExecutionPlan {
        stored_definition,
        executable_definition,
        drop_statement,
    })
}

pub fn replace_create_view_identifier(
    connection: &Arc<dyn Connection>,
    request: &ReplaceViewIdentifierRequest,
) -> Option<ReplaceViewIdentifierResult> {
    let (existing_identifier, start, end) = parse_view_identifier(&request.definition)?;
    let (schema_name, view_name) = split_schema_and_name(&existing_identifier);

    let replaced_identifier = if let Some(schema_name) = schema_name.clone() {
        format!(
            "{}.{}",
            connection.quote_identifier(&schema_name),
            connection.quote_identifier(&request.new_view_name)
        )
    } else {
        connection.quote_identifier(&request.new_view_name)
    };

    let mut updated = String::with_capacity(request.definition.len() + replaced_identifier.len());
    updated.push_str(&request.definition[..start]);
    updated.push_str(&replaced_identifier);
    updated.push_str(&request.definition[end..]);

    Some(ReplaceViewIdentifierResult {
        replaced_definition: updated,
        parsed_schema: schema_name,
        parsed_view_name: view_name,
    })
}

pub fn extract_view_name_from_create_view(definition: &str) -> Option<(Option<String>, String)> {
    let (identifier, _, _) = parse_view_identifier(definition)?;
    let (schema_name, view_name) = split_schema_and_name(&identifier);
    if view_name.is_empty() {
        return None;
    }

    Some((schema_name, view_name))
}

pub async fn build_duplicate_view_sql(
    connection: &Arc<dyn Connection>,
    request: &DuplicateViewRequest,
) -> Result<String, DuplicateViewError> {
    let definition = fetch_object_definition(
        connection,
        &ObjectDefinitionRequest::new(ObjectType::View, &request.source_view_name)
            .with_schema(request.source_schema.clone()),
    )
    .await
    .map_err(|error| DuplicateViewError::FetchDefinition(error.to_string()))?;

    replace_create_view_identifier(
        connection,
        &ReplaceViewIdentifierRequest::new(definition, &request.target_view_name),
    )
    .map(|result| result.replaced_definition)
    .ok_or(DuplicateViewError::ParseDefinition)
}

fn split_schema_and_name(identifier: &str) -> (Option<String>, String) {
    if let Some(dot_index) = split_unquoted_schema_separator(identifier) {
        let schema = normalize_identifier_segment(&identifier[..dot_index]);
        let name = normalize_identifier_segment(&identifier[dot_index + 1..]);
        if !schema.is_empty() && !name.is_empty() {
            return (Some(schema), name);
        }
    }

    (None, normalize_identifier_segment(identifier))
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

fn find_keyword_position(haystack: &str, keyword: &str) -> Option<usize> {
    let uppercase_haystack = haystack.to_uppercase();
    let uppercase_keyword = keyword.to_uppercase();

    for (index, _) in uppercase_haystack.match_indices(&uppercase_keyword) {
        let before_is_boundary = if index == 0 {
            true
        } else {
            let before = uppercase_haystack[..index].chars().next_back();
            !matches!(before, Some(character) if character.is_ascii_alphanumeric() || character == '_')
        };

        if !before_is_boundary {
            continue;
        }

        let after_index = index + uppercase_keyword.len();
        let after_is_boundary = if after_index >= uppercase_haystack.len() {
            true
        } else {
            let after = uppercase_haystack[after_index..].chars().next();
            !matches!(after, Some(character) if character.is_ascii_alphanumeric() || character == '_')
        };

        if after_is_boundary {
            return Some(index);
        }
    }

    None
}

fn parse_identifier_end(remainder: &str) -> Option<usize> {
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut in_bracket_quote = false;

    let mut consumed = 0;
    let mut characters = remainder.char_indices().peekable();
    while let Some((index, character)) = characters.next() {
        if in_double_quote {
            consumed = index + character.len_utf8();
            if character == '"' {
                if matches!(characters.peek(), Some((_, '"'))) {
                    if let Some((escaped_index, escaped_char)) = characters.next() {
                        consumed = escaped_index + escaped_char.len_utf8();
                    }
                } else {
                    in_double_quote = false;
                }
            }
            continue;
        }

        if in_backtick_quote {
            consumed = index + character.len_utf8();
            if character == '`' {
                if matches!(characters.peek(), Some((_, '`'))) {
                    if let Some((escaped_index, escaped_char)) = characters.next() {
                        consumed = escaped_index + escaped_char.len_utf8();
                    }
                } else {
                    in_backtick_quote = false;
                }
            }
            continue;
        }

        if in_bracket_quote {
            consumed = index + character.len_utf8();
            if character == ']' {
                if matches!(characters.peek(), Some((_, ']'))) {
                    if let Some((escaped_index, escaped_char)) = characters.next() {
                        consumed = escaped_index + escaped_char.len_utf8();
                    }
                } else {
                    in_bracket_quote = false;
                }
            }
            continue;
        }

        match character {
            '"' => {
                in_double_quote = true;
                consumed = index + character.len_utf8();
            }
            '`' => {
                in_backtick_quote = true;
                consumed = index + character.len_utf8();
            }
            '[' => {
                in_bracket_quote = true;
                consumed = index + character.len_utf8();
            }
            '(' | ';' => break,
            character if character.is_whitespace() => break,
            _ => consumed = index + character.len_utf8(),
        }
    }

    (consumed > 0).then_some(consumed)
}

fn parse_view_identifier(definition: &str) -> Option<(String, usize, usize)> {
    let trimmed = definition.trim_start();
    let leading_whitespace_len = definition.len().saturating_sub(trimmed.len());

    if !trimmed.to_uppercase().starts_with(CREATE_KEYWORD) {
        return None;
    }

    let create_body = &trimmed[CREATE_KEYWORD.len()..];
    let view_keyword_offset = find_keyword_position(create_body, "VIEW")?;
    let view_keyword_start = CREATE_KEYWORD.len() + view_keyword_offset;
    let mut remainder = &trimmed[view_keyword_start + "VIEW".len()..];
    let mut cursor = view_keyword_start + "VIEW".len();

    let remainder_trimmed = remainder.trim_start();
    cursor += remainder.len().saturating_sub(remainder_trimmed.len());
    remainder = remainder_trimmed;

    if remainder.to_uppercase().starts_with("IF NOT EXISTS") {
        remainder = &remainder["IF NOT EXISTS".len()..];
        cursor += "IF NOT EXISTS".len();

        let remainder_trimmed = remainder.trim_start();
        cursor += remainder.len().saturating_sub(remainder_trimmed.len());
        remainder = remainder_trimmed;
    }

    if remainder.is_empty() {
        return None;
    }

    let identifier_end = parse_identifier_end(remainder)?;
    let raw_identifier = remainder[..identifier_end].trim().to_string();
    if raw_identifier.is_empty() {
        return None;
    }

    let name_start = leading_whitespace_len + cursor;
    let name_end = name_start + identifier_end;

    Some((raw_identifier, name_start, name_end))
}
