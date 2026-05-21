use lsp_types::{CompletionItem, CompletionItemKind, InsertTextFormat};
use zqlz_core::{SyntaxDriverCapabilities, syntax_term_profile};
use zqlz_ui::widgets::Rope;

pub(crate) fn get_document_completions(
    capabilities: &SyntaxDriverCapabilities,
    text: &Rope,
    offset: usize,
) -> Vec<CompletionItem> {
    let source = text.to_string();
    let prefix =
        zqlz_core::syntax_completion_prefix(&source, offset, &capabilities.completion_word_chars);
    let prefix_lower = prefix.to_ascii_lowercase();
    let terms = syntax_term_profile(capabilities.profile);
    let mut completions = Vec::new();

    extend_term_completions(
        capabilities.profile,
        &mut completions,
        terms.base_keywords.iter().chain(terms.dialect_keywords),
        CompletionItemKind::KEYWORD,
        "Keyword",
        &prefix_lower,
        false,
    );
    extend_term_completions(
        capabilities.profile,
        &mut completions,
        terms.base_functions.iter().chain(terms.dialect_functions),
        CompletionItemKind::FUNCTION,
        "Function",
        &prefix_lower,
        true,
    );
    extend_term_completions(
        capabilities.profile,
        &mut completions,
        terms.base_types.iter().chain(terms.dialect_types),
        CompletionItemKind::STRUCT,
        "Type",
        &prefix_lower,
        false,
    );

    completions.sort_by(|left, right| {
        completion_sort_key(left)
            .cmp(completion_sort_key(right))
            .then_with(|| left.label.cmp(&right.label))
    });
    completions.dedup_by(|left, right| left.label == right.label);
    completions
}

pub(crate) fn is_document_completion_trigger(
    capabilities: &SyntaxDriverCapabilities,
    new_text: &str,
) -> bool {
    if capabilities
        .completion_triggers
        .iter()
        .any(|trigger| *trigger == new_text.chars().next().unwrap_or_default())
    {
        return true;
    }

    new_text.chars().any(|character| {
        zqlz_core::syntax_completion_word_char(character, &capabilities.completion_word_chars)
    })
}

fn extend_term_completions<'a>(
    profile: &str,
    completions: &mut Vec<CompletionItem>,
    terms: impl Iterator<Item = &'a &'static str>,
    kind: CompletionItemKind,
    detail_kind: &str,
    prefix_lower: &str,
    snippet: bool,
) {
    for term in terms {
        if !term_matches_prefix(term, prefix_lower) {
            continue;
        }
        completions.push(CompletionItem {
            label: (*term).to_string(),
            kind: Some(kind),
            detail: Some(format!(
                "{} {} ({})",
                profile.to_uppercase(),
                detail_kind,
                profile
            )),
            insert_text: snippet.then(|| format!("{}(${{1:value}})", term)),
            insert_text_format: snippet.then_some(InsertTextFormat::SNIPPET),
            sort_text: Some(format!("{}_{}", completion_kind_rank(kind), term)),
            ..Default::default()
        });
    }
}

fn term_matches_prefix(term: &str, prefix_lower: &str) -> bool {
    prefix_lower.is_empty() || term.to_ascii_lowercase().starts_with(prefix_lower)
}

fn completion_kind_rank(kind: CompletionItemKind) -> u8 {
    match kind {
        CompletionItemKind::KEYWORD => 1,
        CompletionItemKind::FUNCTION => 2,
        CompletionItemKind::STRUCT => 3,
        _ => 9,
    }
}

fn completion_sort_key(item: &CompletionItem) -> &str {
    item.sort_text.as_deref().unwrap_or(&item.label)
}
