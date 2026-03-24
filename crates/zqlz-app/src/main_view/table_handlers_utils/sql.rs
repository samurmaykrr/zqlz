use std::sync::Arc;

use zqlz_core::ColumnMeta;

pub(in crate::main_view) fn escape_sql_like_literal(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''")
}

fn is_heavy_search_type(data_type: &str) -> bool {
    let lowered = data_type.to_ascii_lowercase();
    ["json", "text", "blob", "bytea", "clob", "xml"]
        .iter()
        .any(|token| lowered.contains(token))
}

pub(in crate::main_view) fn build_default_searchable_columns(
    column_meta: &[ColumnMeta],
) -> Vec<String> {
    let mut non_heavy_columns = Vec::new();
    let mut heavy_columns = Vec::new();

    for column in column_meta {
        if !zqlz_services::TableService::is_string_type(&column.data_type.to_ascii_lowercase()) {
            continue;
        }

        if is_heavy_search_type(&column.data_type) {
            heavy_columns.push(column.name.clone());
        } else {
            non_heavy_columns.push(column.name.clone());
        }
    }

    non_heavy_columns.extend(heavy_columns);
    non_heavy_columns.truncate(8);
    non_heavy_columns
}

pub(in crate::main_view) fn resolve_search_columns(
    column_meta: &[ColumnMeta],
    preferred_search_columns: Option<Vec<String>>,
) -> Vec<String> {
    preferred_search_columns
        .filter(|columns| !columns.is_empty())
        .unwrap_or_else(|| build_default_searchable_columns(column_meta))
}

pub(in crate::main_view) fn build_search_clause_for_columns(
    connection: &Arc<dyn zqlz_core::Connection>,
    columns: &[String],
    search_text: &str,
    case_insensitive: bool,
) -> Option<String> {
    let trimmed_search = search_text.trim();
    if trimmed_search.is_empty() || columns.is_empty() {
        return None;
    }

    let escaped_search = escape_sql_like_literal(trimmed_search);
    let conditions: Vec<String> = columns
        .iter()
        .map(|column_name| {
            let escaped_column = connection.quote_identifier(column_name);
            let searchable_expression = connection.search_text_cast_expression(&escaped_column);
            if case_insensitive {
                format!(
                    "LOWER({}) LIKE LOWER('%{}%') ESCAPE '\\'",
                    searchable_expression, escaped_search
                )
            } else {
                format!(
                    "{} LIKE '%{}%' ESCAPE '\\'",
                    searchable_expression, escaped_search
                )
            }
        })
        .collect();

    if conditions.is_empty() {
        None
    } else {
        Some(format!("({})", conditions.join(" OR ")))
    }
}
