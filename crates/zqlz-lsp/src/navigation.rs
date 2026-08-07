use crate::SqlLsp;
use lsp_types::{GotoDefinitionResponse, Location, Position, Range, TextEdit, Uri, WorkspaceEdit};
use std::collections::HashMap;
use zqlz_core::{is_dialect_reserved_sql_symbol, is_valid_dialect_sql_identifier};
use zqlz_core::{
    qualified_sql_reference_at_offset, sql_matching_qualified_symbol_segments,
    sql_matching_symbol_segments, sql_symbol_at_offset,
};
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::RopeExt;

pub(crate) fn get_word_at_offset(text: &Rope, offset: usize) -> Option<String> {
    let sql = text.to_string();
    let offset = crate::clamp_to_char_boundary(&sql, offset);
    sql_symbol_at_offset(&sql, offset).map(|symbol| symbol.text)
}

pub(crate) fn get_definition(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
) -> Option<GotoDefinitionResponse> {
    tracing::debug!("get_definition: offset={}", offset);

    let word = match lsp.get_word_at_offset(text, offset) {
        Some(word) => {
            tracing::debug!("word extracted: '{}'", word);
            word
        }
        None => {
            tracing::debug!("no word at offset");
            return None;
        }
    };

    if word.is_empty() {
        tracing::debug!("empty word at offset");
        return None;
    }

    let word_lower = word.to_lowercase();
    if let Some((qualifier, _)) = lsp.qualified_reference_at_offset(text, offset)
        && let Some(table_name) = lsp.resolve_table_identifier(&qualifier, text, offset)
    {
        tracing::debug!("qualified column reference: {}.{}", qualifier, word);

        if let Some(columns) = lsp.columns_for_table(&table_name) {
            for column in columns {
                if column.name.to_lowercase() == word_lower {
                    tracing::debug!(table = table_name, column = %column.name, "Found column definition");
                    // A column has no location of its own, so navigate to its table.
                    return schema_object_definition("table", &table_name);
                }
            }
        }
    }

    if let Some(table_name) = lsp.resolve_table_identifier(&word, text, offset)
        && let Some(table) = lsp.table_info(&table_name)
    {
        tracing::debug!(table = %table.name, "Found table definition");
        return schema_object_definition("table", &table.name);
    }

    for (table_name, columns) in &lsp.schema_cache.columns_by_table {
        for column in columns {
            if column.name.to_lowercase() == word_lower {
                tracing::debug!(column = %column.name, table = table_name, "Found column definition");
                // A column has no location of its own, so navigate to its table.
                return schema_object_definition("table", table_name);
            }
        }
    }

    if let Some(view) = lsp.schema_cache.views.get(&word) {
        tracing::debug!(view = %view.name, "Found view definition");
        return schema_object_definition("view", &view.name);
    }

    if let Some(function) = lsp.schema_cache.functions.get(&word) {
        tracing::debug!(function = %function.name, "Found function definition");
        return schema_object_definition("function", &function.name);
    }

    if let Some(procedure) = lsp.schema_cache.procedures.get(&word) {
        tracing::debug!(procedure = %procedure.name, "Found procedure definition");
        return schema_object_definition("procedure", &procedure.name);
    }

    if let Some(trigger) = lsp.schema_cache.triggers.get(&word) {
        tracing::debug!(trigger = %trigger.name, "Found trigger definition");
        return schema_object_definition("trigger", &trigger.name);
    }

    if let Some(index) = lsp.schema_cache.indexes.get(&word) {
        tracing::debug!(index = %index.name, "Found index definition");
        return schema_object_definition("index", &index.name);
    }

    tracing::debug!("no definition found for: {}", word);
    None
}

pub(crate) fn get_references(lsp: &SqlLsp, text: &Rope, offset: usize) -> Vec<Location> {
    tracing::debug!("get_references: offset={}", offset);

    let word = match lsp.get_word_at_offset(text, offset) {
        Some(word) => {
            tracing::debug!("word extracted: '{}'", word);
            word
        }
        None => {
            tracing::debug!("no word at offset");
            return Vec::new();
        }
    };

    if word.is_empty() {
        tracing::debug!("empty word at offset");
        return Vec::new();
    }

    if is_reserved_sql_symbol(lsp, &word) {
        tracing::debug!("word is a keyword, skipping references");
        return Vec::new();
    }

    let word_lower = word.to_lowercase();
    let mut references = find_source_references(text, offset, &word);
    find_schema_references(lsp, &word_lower, &mut references);

    tracing::debug!("found {} references for: {}", references.len(), word);
    references
}

pub(crate) fn rename(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
    new_name: &str,
) -> Option<WorkspaceEdit> {
    tracing::debug!("rename: offset={}, new_name={}", offset, new_name);

    if !is_valid_sql_identifier(lsp, new_name) {
        tracing::debug!("invalid SQL identifier: {}", new_name);
        return None;
    }

    let word = match lsp.get_word_at_offset(text, offset) {
        Some(word) => {
            tracing::debug!("word extracted: '{}'", word);
            word
        }
        None => {
            tracing::debug!("no word at offset");
            return None;
        }
    };

    if word.is_empty() {
        tracing::debug!("empty word at offset");
        return None;
    }

    if is_reserved_sql_symbol(lsp, &word) {
        tracing::debug!("word is a keyword, skipping rename");
        return None;
    }

    if word.eq_ignore_ascii_case(new_name) {
        tracing::debug!("new name is the same as old name");
        return None;
    }

    let source_segments = source_reference_segments(text, offset, &word);
    let text_edits: Vec<TextEdit> = source_segments
        .into_iter()
        .map(|(start, end)| {
            let start = text.offset_to_position(start);
            let end = text.offset_to_position(end);
            TextEdit {
                range: Range {
                    start: Position {
                        line: start.line,
                        character: start.character,
                    },
                    end: Position {
                        line: end.line,
                        character: end.character,
                    },
                },
                new_text: new_name.to_string(),
            }
        })
        .collect();

    if text_edits.is_empty() {
        tracing::debug!("no locations found for rename");
        return None;
    }

    tracing::debug!("found {} locations for rename: {}", text_edits.len(), word);

    let Ok(uri) = internal_uri() else {
        return None;
    };

    Some(WorkspaceEdit {
        changes: Some(HashMap::from([(uri, text_edits)])),
        document_changes: None,
        change_annotations: None,
    })
}

fn is_reserved_sql_symbol(lsp: &SqlLsp, word: &str) -> bool {
    is_dialect_reserved_sql_symbol(&lsp.dialect.dialect_info(), word)
}

fn is_valid_sql_identifier(lsp: &SqlLsp, name: &str) -> bool {
    is_valid_dialect_sql_identifier(&lsp.dialect.dialect_info(), name)
}

fn find_source_references(text: &Rope, offset: usize, word: &str) -> Vec<Location> {
    source_reference_segments(text, offset, word)
        .into_iter()
        .filter_map(|(start, end)| reference_location(text, start, end))
        .collect()
}

fn source_reference_segments(text: &Rope, offset: usize, word: &str) -> Vec<(usize, usize)> {
    let sql = text.to_string();
    if let Some((qualifier, _)) = qualified_sql_reference_at_offset(&sql, offset) {
        let qualified_segments = sql_matching_qualified_symbol_segments(&sql, &qualifier, word);
        if !qualified_segments.is_empty() {
            return qualified_segments;
        }
    }

    sql_matching_symbol_segments(&sql, word)
}

fn reference_location(text: &Rope, start: usize, end: usize) -> Option<Location> {
    let Ok(uri) = internal_uri() else {
        return None;
    };
    let start = text.offset_to_position(start);
    let end = text.offset_to_position(end);
    Some(Location {
        uri,
        range: Range {
            start: Position {
                line: start.line,
                character: start.character,
            },
            end: Position {
                line: end.line,
                character: end.character,
            },
        },
    })
}

fn find_schema_references(lsp: &SqlLsp, word_lower: &str, references: &mut Vec<Location>) {
    for (table_name, columns) in &lsp.schema_cache.columns_by_table {
        for column in columns {
            if column.name.to_lowercase() == word_lower {
                let Ok(uri) = format!("sql://internal/table/{}", table_name).parse::<Uri>() else {
                    continue;
                };
                let location = zero_location(uri);
                if !references
                    .iter()
                    .any(|reference| reference.uri == location.uri)
                {
                    references.push(location);
                }
            }
        }
    }

    if !lsp.schema_cache.tables.contains_key(word_lower) {
        return;
    }

    for (view_name, view) in &lsp.schema_cache.views {
        let Some(definition) = view.definition.as_ref() else {
            continue;
        };
        if sql_matching_symbol_segments(definition, word_lower).is_empty() {
            continue;
        }

        let Ok(uri) = format!("sql://internal/view/{}", view_name).parse::<Uri>() else {
            continue;
        };
        let location = zero_location(uri);
        if !references
            .iter()
            .any(|reference| reference.uri == location.uri)
        {
            references.push(location);
        }
    }
}

/// Points a definition at a schema object rather than a spot in the buffer.
///
/// The object has no position in the user's query, so the URI carries its identity
/// (`sql://internal/<kind>/<name>`) for the editor to act on — the same scheme
/// `find_schema_references` already uses. The range stays zeroed; it is meaningless
/// for an object that lives in the database.
fn schema_object_definition(kind: &str, name: &str) -> Option<GotoDefinitionResponse> {
    let uri = format!("sql://internal/{kind}/{name}")
        .parse::<Uri>()
        .ok()?;
    Some(GotoDefinitionResponse::Scalar(zero_location(uri)))
}

fn zero_location(uri: Uri) -> Location {
    Location {
        uri,
        range: Range {
            start: Position {
                line: 0,
                character: 0,
            },
            end: Position {
                line: 0,
                character: 0,
            },
        },
    }
}

fn internal_uri() -> Result<Uri, <Uri as std::str::FromStr>::Err> {
    "sql://internal".parse::<Uri>()
}
