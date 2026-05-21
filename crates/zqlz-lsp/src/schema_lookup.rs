use zqlz_core::{ForeignKeyInfo, qualified_sql_reference_at_offset};
use zqlz_ui::widgets::Rope;

use crate::{ColumnInfo, SqlLsp, TableInfo, TableRef, context_tables, navigation, query_sources};

pub(crate) fn qualified_reference_at_offset(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
) -> Option<(String, String)> {
    let sql = text.to_string();
    let offset = crate::clamp_to_char_boundary(&sql, offset);
    let _ = lsp;
    qualified_sql_reference_at_offset(&sql, offset)
}

pub(crate) fn resolve_table_identifier(
    lsp: &SqlLsp,
    identifier: &str,
    text: &Rope,
    offset: usize,
) -> Option<String> {
    table_info(lsp, identifier)
        .map(|table| table.name.clone())
        .or_else(|| resolve_alias_to_table(lsp, identifier, text, offset))
}

pub(crate) fn table_info<'a>(lsp: &'a SqlLsp, table_name: &str) -> Option<&'a TableInfo> {
    lsp.schema_cache
        .tables
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(table_name))
        .map(|(_, table)| table)
}

pub(crate) fn columns_for_table<'a>(
    lsp: &'a SqlLsp,
    table_name: &str,
) -> Option<&'a Vec<ColumnInfo>> {
    lsp.schema_cache
        .columns_by_table
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(table_name))
        .map(|(_, columns)| columns)
}

pub(crate) fn sequence_info<'a>(
    lsp: &'a SqlLsp,
    sequence_name: &str,
) -> Option<&'a zqlz_core::SequenceInfo> {
    lsp.schema_cache
        .sequences
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(sequence_name))
        .map(|(_, sequence)| sequence)
}

pub(crate) fn foreign_keys_for_table<'a>(
    lsp: &'a SqlLsp,
    table_name: &str,
) -> Option<&'a Vec<ForeignKeyInfo>> {
    lsp.schema_cache
        .foreign_keys_by_table
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(table_name))
        .map(|(_, foreign_keys)| foreign_keys)
}

pub(crate) fn reverse_foreign_keys_for_table<'a>(
    lsp: &'a SqlLsp,
    table_name: &str,
) -> Option<&'a Vec<(String, ForeignKeyInfo)>> {
    lsp.schema_cache
        .reverse_foreign_keys
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(table_name))
        .map(|(_, reverse_foreign_keys)| reverse_foreign_keys)
}

pub(crate) fn derived_columns_for_identifier(
    lsp: &SqlLsp,
    identifier: &str,
    text: &Rope,
) -> Option<Vec<String>> {
    derived_columns_for_identifier_from_sql(lsp, identifier, &text.to_string(), None)
}

pub(crate) fn derived_columns_for_identifier_at(
    lsp: &SqlLsp,
    identifier: &str,
    text: &Rope,
    offset: usize,
) -> Option<Vec<String>> {
    derived_columns_for_identifier_from_sql(lsp, identifier, &text.to_string(), Some(offset))
}

fn derived_columns_for_identifier_from_sql(
    lsp: &SqlLsp,
    identifier: &str,
    sql: &str,
    cursor_offset: Option<usize>,
) -> Option<Vec<String>> {
    let dialect = lsp.get_dialect();
    query_sources::derived_columns_for_identifier(identifier, sql, cursor_offset, dialect.as_ref())
}

pub(crate) fn resolve_alias_from_context(
    identifier: &str,
    available_tables: &[TableRef],
) -> String {
    let alias_map = context_tables::build_alias_map(available_tables);
    let identifier_lower = identifier.to_lowercase();

    if let Some(table_name) = alias_map.get(&identifier_lower) {
        tracing::debug!(
            "Resolved identifier '{}' to table '{}' using context",
            identifier,
            table_name
        );
        return table_name.clone();
    }

    let partial_matches: Vec<&TableRef> = available_tables
        .iter()
        .filter(|table| {
            table
                .alias
                .as_ref()
                .is_some_and(|alias| alias.to_lowercase().starts_with(&identifier_lower))
        })
        .collect();
    if let [table_ref] = partial_matches.as_slice() {
        tracing::debug!(
            "Resolved partial identifier '{}' to table '{}' using AST context",
            identifier,
            table_ref.table_name
        );
        return table_ref.table_name.clone();
    }

    tracing::debug!(
        "Could not resolve identifier '{}' from context, using as-is",
        identifier
    );
    identifier.to_string()
}

pub(crate) fn resolve_alias_to_table(
    lsp: &SqlLsp,
    alias: &str,
    text: &Rope,
    offset: usize,
) -> Option<String> {
    if let Some(table) = table_info(lsp, alias) {
        return Some(table.name.clone());
    }

    if let Some(table_name) = resolve_alias_to_table_from_ast(lsp, alias, text, offset) {
        return Some(table_name);
    }

    if derived_columns_for_identifier_at(lsp, alias, text, offset).is_some() {
        return Some(alias.to_string());
    }

    None
}

fn resolve_alias_to_table_from_ast(
    lsp: &SqlLsp,
    alias: &str,
    text: &Rope,
    offset: usize,
) -> Option<String> {
    let sql = text.to_string();
    let dialect = lsp.get_dialect();
    query_sources::resolve_alias_to_table(alias, &sql, Some(offset), dialect.as_ref())
}

pub(crate) fn get_word_at_offset(_lsp: &SqlLsp, text: &Rope, offset: usize) -> Option<String> {
    navigation::get_word_at_offset(text, offset)
}
