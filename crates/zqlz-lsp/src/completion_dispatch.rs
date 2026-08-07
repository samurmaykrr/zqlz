use lsp_types::{CompletionItem, CompletionItemKind};
use zqlz_core::SyntaxCompletionContextKind;
use zqlz_ui::widgets::Rope;

use crate::{AstSqlContext, SqlLsp, completion_context, operator_completions, schema_lookup};

/// Why the schema can offer nothing at all, if that is the situation.
///
/// `None` means the schema is loaded and populated, so the caller should explain the
/// narrower reason its own lookup came back empty.
fn schema_unavailable_reason(lsp: &SqlLsp) -> Option<(String, &'static str)> {
    if lsp.schema_loading {
        return Some((
            "Loading schema…".to_string(),
            "Fetching tables from the database",
        ));
    }

    if lsp.schema_cache.tables.is_empty() && lsp.schema_cache.views.is_empty() {
        return Some((
            "No schema loaded — connect and refresh the schema".to_string(),
            "schema",
        ));
    }

    None
}

fn relation_is_known(lsp: &SqlLsp, name: &str) -> bool {
    schema_lookup::table_info(lsp, name).is_some()
        || lsp
            .schema_cache
            .views
            .keys()
            .any(|view_name| view_name.eq_ignore_ascii_case(name))
}

/// A non-inserting menu entry that explains an otherwise silent empty result.
fn schema_hint_item(message: String, detail: &str) -> CompletionItem {
    CompletionItem {
        label: message,
        kind: Some(CompletionItemKind::TEXT),
        detail: Some(detail.to_string()),
        insert_text: Some(String::new()),
        preselect: Some(false),
        ..Default::default()
    }
}

pub(crate) fn add_completions_for_context(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
    context: &AstSqlContext,
    current_word_lower: &str,
    lines_before_cursor: &str,
    completions: &mut Vec<CompletionItem>,
) -> bool {
    match context {
        AstSqlContext::SelectList { available_tables } => {
            tracing::debug!("In SELECT list, available tables: {:?}", available_tables);

            let before_columns = completions.len();
            if available_tables.is_empty() {
                lsp.add_filtered_columns(completions);
            } else {
                lsp.add_columns_from_tables(available_tables, completions);
            }

            // Functions and keywords follow, so the menu is never empty — without an
            // explicit hint a schema that hasn't loaded its columns yet is
            // indistinguishable from one that simply has nothing to offer.
            if completions.len() == before_columns {
                let (message, detail) = schema_unavailable_reason(lsp)
                    .unwrap_or_else(|| ("Loading columns…".to_string(), "schema"));
                completions.push(schema_hint_item(message, detail));
            }

            lsp.add_filtered_functions(current_word_lower, completions);
            lsp.add_context_keywords(
                SyntaxCompletionContextKind::SelectList,
                current_word_lower,
                completions,
            );

            operator_completions::add_operator_completions(
                SyntaxCompletionContextKind::SelectList,
                current_word_lower,
                lines_before_cursor,
                &lsp.driver_type,
                completions,
            );
        }
        AstSqlContext::FromClause => {
            tracing::debug!("In FROM clause");
            lsp.add_filtered_tables(completions);
            lsp.add_filtered_views(completions);
            if !current_word_lower.is_empty() {
                lsp.add_filtered_functions(current_word_lower, completions);
            }

            if completions.is_empty() && lsp.schema_loading {
                completions.push(CompletionItem {
                    label: "Schema loading…".to_string(),
                    kind: Some(CompletionItemKind::TEXT),
                    detail: Some("Fetching tables from the database".to_string()),
                    insert_text: Some(String::new()),
                    preselect: Some(false),
                    ..Default::default()
                });
                return true;
            }

            completion_context::add_cte_name_completions(
                lsp,
                text,
                offset,
                current_word_lower,
                completions,
            );

            if !current_word_lower.is_empty() {
                lsp.add_context_keywords(
                    SyntaxCompletionContextKind::FromClause,
                    current_word_lower,
                    completions,
                );
            }
        }
        AstSqlContext::JoinClause { existing_tables } => {
            tracing::debug!("In JOIN clause, existing tables: {:?}", existing_tables);
            lsp.add_tables_with_fk_suggestions(existing_tables, completions);
            lsp.add_filtered_views(completions);

            completion_context::add_cte_name_completions(
                lsp,
                text,
                offset,
                current_word_lower,
                completions,
            );

            lsp.add_context_keywords(
                SyntaxCompletionContextKind::JoinClause,
                current_word_lower,
                completions,
            );
        }
        AstSqlContext::ConditionClause { available_tables } => {
            tracing::debug!(
                "In WHERE/HAVING clause, available tables count: {}",
                available_tables.len()
            );

            if available_tables.is_empty() {
                lsp.add_filtered_columns(completions);
            } else {
                lsp.add_columns_from_tables(available_tables, completions);
            }

            lsp.add_scalar_functions(current_word_lower, completions);
            lsp.add_context_keywords(
                SyntaxCompletionContextKind::ConditionClause,
                current_word_lower,
                completions,
            );

            operator_completions::add_operator_completions(
                SyntaxCompletionContextKind::ConditionClause,
                current_word_lower,
                lines_before_cursor,
                &lsp.driver_type,
                completions,
            );
        }
        AstSqlContext::AfterDot {
            table_or_alias,
            available_tables,
        } => {
            tracing::debug!("After dot for table/alias: {}", table_or_alias);

            let table_name = lsp.resolve_alias_from_context(table_or_alias, available_tables);
            let table_name = if table_name == *table_or_alias {
                lsp.resolve_alias_to_table(&table_name, text, offset)
                    .unwrap_or(table_name)
            } else {
                table_name
            };

            tracing::debug!("Resolved '{}' to table '{}'", table_or_alias, table_name);

            completion_context::add_after_dot_column_completions(
                lsp,
                text,
                offset,
                &table_name,
                after_dot_filter(current_word_lower),
                completions,
            );

            // An empty result here is invisible to the user (the menu simply
            // never opens), so explain WHY there are no columns instead of
            // failing silently.
            if completions.is_empty() && current_word_lower.is_empty() {
                let (message, detail) = match schema_unavailable_reason(lsp) {
                    Some(reason) => reason,
                    // A relation the schema knows about but has no cached columns for is
                    // a pending column fetch, not an unresolved name — reporting it as
                    // "unknown" sends the user hunting for a typo that isn't there.
                    None if relation_is_known(lsp, &table_name) => {
                        (format!("Loading columns for {table_name}…"), "schema")
                    }
                    None if !table_name.eq_ignore_ascii_case(table_or_alias) => {
                        (format!("No columns cached for {table_name}"), "schema")
                    }
                    None => (format!("Unknown table or alias: {table_name}"), "schema"),
                };

                completions.push(schema_hint_item(message, detail));
            }
        }
        AstSqlContext::CommonTableExpression { .. } => {
            tracing::debug!("In CTE context");
            lsp.add_context_keywords(
                SyntaxCompletionContextKind::CommonTableExpression,
                current_word_lower,
                completions,
            );
        }
        AstSqlContext::Subquery { parent_tables } => {
            tracing::debug!("In subquery, parent tables: {:?}", parent_tables);
            lsp.add_columns_from_tables(parent_tables, completions);
        }
        AstSqlContext::CreateTable => {
            tracing::debug!("In CREATE TABLE context");
            if lsp.prefer_create_table_constraints(lines_before_cursor) {
                lsp.add_create_table_constraints(current_word_lower, completions);
                lsp.add_filtered_data_types(current_word_lower, completions);
            } else {
                lsp.add_filtered_data_types(current_word_lower, completions);
                lsp.add_create_table_constraints(current_word_lower, completions);
            }
        }
        AstSqlContext::General => {
            tracing::debug!("In general context");
            lsp.add_filtered_keywords(current_word_lower, lines_before_cursor, completions);

            if !current_word_lower.is_empty() {
                lsp.add_filtered_data_types(current_word_lower, completions);
            }

            if current_word_lower.len() >= 2 {
                lsp.add_filtered_tables(completions);
            }
        }
    }

    false
}

fn after_dot_filter(current_word: &str) -> &str {
    current_word
        .rsplit_once('.')
        .map_or(current_word, |(_, filter)| filter)
}
