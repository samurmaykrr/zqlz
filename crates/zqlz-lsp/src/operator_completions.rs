use lsp_types::{CompletionItem, CompletionItemKind};
use zqlz_core::SyntaxCompletionContextKind;

pub(crate) fn add_operator_completions(
    context: SyntaxCompletionContextKind,
    filter: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let filter = if filter
        .chars()
        .next()
        .is_some_and(|character| matches!(character, '=' | '<' | '>' | '!'))
    {
        filter
    } else {
        ""
    };

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
            insert_text: Some(operator.insert_text.to_string()),
            sort_text: Some(operator.sort_text.to_string()),
            ..Default::default()
        });
    }
}
