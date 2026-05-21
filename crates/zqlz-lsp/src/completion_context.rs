use lsp_types::{CompletionItem, CompletionItemKind};
use zqlz_ui::widgets::Rope;

use crate::{SqlLsp, completion_items, schema_lookup};

pub(crate) fn add_cte_name_completions(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
    filter: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let cte_names = lsp.context_analyzer.extract_cte_names(text, offset);
    tracing::debug!("Found {} CTEs: {:?}", cte_names.len(), cte_names);

    for cte_name in cte_names {
        if !filter.is_empty() && !cte_name.to_lowercase().starts_with(filter) {
            continue;
        }

        completions.push(CompletionItem {
            label: cte_name.clone(),
            kind: Some(CompletionItemKind::CLASS),
            detail: Some("Common Table Expression (CTE)".to_string()),
            insert_text: Some(format!("{} ", cte_name)),
            sort_text: Some(format!("0_cte_{}", cte_name)),
            ..Default::default()
        });
    }
}

pub(crate) fn add_after_dot_column_completions(
    lsp: &SqlLsp,
    text: &Rope,
    offset: usize,
    table_name: &str,
    filter: &str,
    completions: &mut Vec<CompletionItem>,
) {
    if let Some(columns) = schema_lookup::columns_for_table(lsp, table_name) {
        tracing::debug!("Found {} columns for table '{}'", columns.len(), table_name);
        for column in columns {
            let matches_filter = filter.is_empty()
                || lsp
                    .fuzzy_matcher
                    .fuzzy_match(filter, &column.name.to_lowercase())
                    .is_some_and(|result| result.is_match());

            if matches_filter {
                completions.push(completion_items::dot_column_completion(table_name, column));
            }
        }
    } else if let Some(derived_columns) =
        schema_lookup::derived_columns_for_identifier_at(lsp, table_name, text, offset)
    {
        crate::schema_completions::add_columns_from_names(&derived_columns, completions);
    } else {
        tracing::debug!("No columns found for table/alias '{}'", table_name);
    }
}
