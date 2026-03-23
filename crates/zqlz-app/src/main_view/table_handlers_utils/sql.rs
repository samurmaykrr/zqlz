use std::sync::Arc;

pub(in crate::main_view) fn escape_sql_like_literal(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''")
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
