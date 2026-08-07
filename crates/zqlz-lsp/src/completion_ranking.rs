use crate::{AstSqlContext, FuzzyMatcher};
use lsp_types::{CompletionItem, CompletionItemKind};
use std::collections::HashMap;
use zqlz_core::{
    SyntaxCompletionContextKind, SyntaxCompletionItemKind, compare_syntax_completion_sort_texts,
    syntax_completion_context_bucket, syntax_completion_dedup_key,
};

#[derive(Clone)]
struct ScoredCompletionItem {
    item: CompletionItem,
    fuzzy_score: i32,
    context_bucket: i32,
    exact_prefix: bool,
    original_index: usize,
}

pub(crate) fn rank_and_dedup_completions(
    fuzzy_matcher: &FuzzyMatcher,
    context: &AstSqlContext,
    filter: &str,
    completions: Vec<CompletionItem>,
    prefer_create_table_constraints: bool,
) -> Vec<CompletionItem> {
    let filter_lower = filter.to_lowercase();
    let mut best_by_key: HashMap<String, ScoredCompletionItem> = HashMap::new();

    // Explanatory hints exist precisely to be seen when nothing else can be offered,
    // so they must not be fuzzy-filtered against the typed word or ranked into the
    // tail that `truncate` discards.
    let (hints, completions): (Vec<_>, Vec<_>) =
        completions.into_iter().partition(is_explanatory_hint);

    for (original_index, item) in completions.into_iter().enumerate() {
        let match_target = item
            .filter_text
            .as_ref()
            .unwrap_or(&item.label)
            .to_lowercase();
        let exact_prefix = !filter_lower.is_empty() && match_target.starts_with(&filter_lower);
        let fuzzy_score = if filter_lower.is_empty() {
            0
        } else {
            fuzzy_matcher
                .fuzzy_match(filter, &match_target)
                .filter(|result| result.is_match())
                .map(|result| result.score)
                .unwrap_or(i32::MIN / 4)
        };

        if !filter_lower.is_empty() && fuzzy_score <= i32::MIN / 8 {
            continue;
        }

        let scored = ScoredCompletionItem {
            context_bucket: completion_context_bucket(
                context,
                &item,
                prefer_create_table_constraints,
            ),
            exact_prefix,
            fuzzy_score,
            original_index,
            item,
        };
        let key = completion_dedup_key(&scored.item);

        let replace = best_by_key
            .get(&key)
            .map(|existing| compare_scored_completions(&scored, existing).is_gt())
            .unwrap_or(true);
        if replace {
            best_by_key.insert(key, scored);
        }
    }

    let mut scored: Vec<_> = best_by_key.into_values().collect();
    scored.sort_by(|left, right| compare_scored_completions(right, left));
    hints
        .into_iter()
        .chain(scored.into_iter().map(|entry| entry.item))
        .collect()
}

/// A non-inserting `TEXT` entry whose only job is to explain an empty result.
fn is_explanatory_hint(item: &CompletionItem) -> bool {
    item.kind == Some(CompletionItemKind::TEXT)
        && item.insert_text.as_deref() == Some("")
        && item.preselect == Some(false)
}

fn compare_scored_completions(
    left: &ScoredCompletionItem,
    right: &ScoredCompletionItem,
) -> std::cmp::Ordering {
    left.context_bucket
        .cmp(&right.context_bucket)
        .then_with(|| (left.exact_prefix as u8).cmp(&(right.exact_prefix as u8)))
        .then_with(|| left.fuzzy_score.cmp(&right.fuzzy_score))
        .then_with(|| compare_completion_sort_texts(left, right))
        .then_with(|| right.original_index.cmp(&left.original_index))
        .then_with(|| right.item.label.cmp(&left.item.label))
}

fn compare_completion_sort_texts(
    left: &ScoredCompletionItem,
    right: &ScoredCompletionItem,
) -> std::cmp::Ordering {
    compare_syntax_completion_sort_texts(
        left.item.sort_text.as_deref(),
        &left.item.label,
        right.item.sort_text.as_deref(),
        &right.item.label,
    )
}

fn completion_context_bucket(
    context: &AstSqlContext,
    item: &CompletionItem,
    prefer_create_table_constraints: bool,
) -> i32 {
    syntax_completion_context_bucket(
        syntax_context_kind(context),
        syntax_completion_item_kind(item.kind),
        &item.label,
        prefer_create_table_constraints,
    )
}

fn syntax_context_kind(context: &AstSqlContext) -> SyntaxCompletionContextKind {
    match context {
        AstSqlContext::AfterDot { .. } => SyntaxCompletionContextKind::AfterDot,
        AstSqlContext::SelectList { .. } => SyntaxCompletionContextKind::SelectList,
        AstSqlContext::ConditionClause { .. } => SyntaxCompletionContextKind::ConditionClause,
        AstSqlContext::FromClause => SyntaxCompletionContextKind::FromClause,
        AstSqlContext::JoinClause { .. } => SyntaxCompletionContextKind::JoinClause,
        AstSqlContext::CreateTable => SyntaxCompletionContextKind::CreateTable,
        AstSqlContext::Subquery { .. } => SyntaxCompletionContextKind::Subquery,
        AstSqlContext::CommonTableExpression { .. } => {
            SyntaxCompletionContextKind::CommonTableExpression
        }
        AstSqlContext::General => SyntaxCompletionContextKind::General,
    }
}

fn syntax_completion_item_kind(kind: Option<CompletionItemKind>) -> SyntaxCompletionItemKind {
    match kind.unwrap_or(CompletionItemKind::TEXT) {
        CompletionItemKind::FIELD => SyntaxCompletionItemKind::Field,
        CompletionItemKind::FUNCTION => SyntaxCompletionItemKind::Function,
        CompletionItemKind::KEYWORD => SyntaxCompletionItemKind::Keyword,
        CompletionItemKind::OPERATOR => SyntaxCompletionItemKind::Operator,
        CompletionItemKind::STRUCT => SyntaxCompletionItemKind::Struct,
        CompletionItemKind::CLASS => SyntaxCompletionItemKind::Class,
        CompletionItemKind::INTERFACE => SyntaxCompletionItemKind::Interface,
        _ => SyntaxCompletionItemKind::Other,
    }
}

fn completion_dedup_key(item: &CompletionItem) -> String {
    syntax_completion_dedup_key(
        syntax_completion_item_kind(item.kind),
        &item.label,
        item.filter_text.as_deref(),
    )
}
