use lsp_types::{CompletionItem, CompletionItemKind, Documentation, MarkupContent, MarkupKind};
use zqlz_core::SyntaxCompletionContextKind;

fn is_operator_char(character: char) -> bool {
    matches!(
        character,
        '=' | '<' | '>' | '!' | '-' | '#' | '@' | '?' | '|' | '&'
    )
}

/// The run of operator characters the user is mid-way through typing, e.g. the
/// `->` in `data->`. The word-based completion prefix is empty there, so the
/// operator filter has to come from the raw text before the cursor.
fn trailing_operator_run(before_cursor: &str) -> &str {
    let start = before_cursor
        .rfind(|character| !is_operator_char(character))
        .map(|index| index + before_cursor[index..].chars().next().map_or(1, char::len_utf8))
        .unwrap_or(0);
    &before_cursor[start..]
}

pub(crate) fn add_operator_completions(
    context: SyntaxCompletionContextKind,
    filter: &str,
    before_cursor: &str,
    driver_type: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let (filter, typed_run) = if filter.chars().next().is_some_and(is_operator_char) {
        (filter, "")
    } else {
        let run = trailing_operator_run(before_cursor);
        (run, run)
    };

    // Mid-operator (e.g. `data->`) nothing but operators can complete, and the
    // column/function items added earlier would crowd them past the menu limit.
    if !typed_run.is_empty() {
        completions.clear();
    }

    for operator in zqlz_core::syntax_operator_completions_for_context(context) {
        if !filter.is_empty() && !operator.insert_text.starts_with(filter) {
            continue;
        }
        if completions
            .iter()
            .any(|completion| completion.label.eq_ignore_ascii_case(operator.label))
        {
            continue;
        }

        completions.push(CompletionItem {
            label: operator.label.to_string(),
            kind: Some(CompletionItemKind::OPERATOR),
            detail: Some(operator.detail.to_string()),
            // The typed operator run stays in the document (it's not part of the
            // word the editor replaces), so insert only the remainder.
            insert_text: Some(
                operator
                    .insert_text
                    .strip_prefix(typed_run)
                    .unwrap_or(operator.insert_text)
                    .to_string(),
            ),
            sort_text: Some(operator.sort_text.to_string()),
            ..Default::default()
        });
    }

    if driver_supports_json_operators(driver_type) {
        add_json_operator_completions(filter, typed_run, completions);
    }
}

pub(crate) fn driver_supports_json_operators(driver_type: &str) -> bool {
    driver_type.trim().to_ascii_lowercase().contains("postgres")
}

fn add_json_operator_completions(
    filter: &str,
    typed_run: &str,
    completions: &mut Vec<CompletionItem>,
) {
    for operator in zqlz_core::postgres_json_operators() {
        if !filter.is_empty() && !operator.symbol.starts_with(filter) {
            continue;
        }

        let label = format!("{} ({})", operator.symbol, operator.name);
        if completions
            .iter()
            .any(|completion| completion.label.eq_ignore_ascii_case(&label))
        {
            continue;
        }

        completions.push(CompletionItem {
            label,
            kind: Some(CompletionItemKind::OPERATOR),
            detail: Some(operator.name.to_string()),
            documentation: Some(Documentation::MarkupContent(MarkupContent {
                kind: MarkupKind::Markdown,
                value: format!(
                    "{}\n\n```sql\n{}\n```",
                    operator.description, operator.example
                ),
            })),
            insert_text: Some(
                operator
                    .symbol
                    .strip_prefix(typed_run)
                    .unwrap_or(operator.symbol)
                    .to_string(),
            ),
            filter_text: Some(operator.symbol.to_string()),
            sort_text: Some(operator.sort_text.to_string()),
            ..Default::default()
        });
    }
}
