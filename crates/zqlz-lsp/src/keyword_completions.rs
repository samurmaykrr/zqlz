use std::collections::HashSet;

use lsp_types::CompletionItem;
use zqlz_core::{
    KeywordCategory, KeywordInfo, SyntaxCompletionContextKind, active_sql_unquoted_word_tokens,
    keyword_allowed_in_completion_context, keyword_completion_sort_prefix_for_context,
    keyword_context_categories, keyword_context_is_start, keyword_info_relevant_for_context,
    keyword_suffix_for_context,
};

use crate::{FuzzyMatcher, SqlDialect, completion_items};

pub(crate) fn keyword_completion_detail(dialects: &[SqlDialect], keyword: &str) -> String {
    for dialect in dialects {
        let dialect_info = dialect.dialect_info();
        if let Some(keyword_info) = dialect_info
            .keywords
            .iter()
            .find(|keyword_info| keyword_info.keyword.eq_ignore_ascii_case(keyword))
        {
            return keyword_info
                .description
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("SQL Keyword ({})", dialect_info.display_name));
        }
    }

    "SQL Keyword".to_string()
}

pub(crate) fn completion_keyword_infos(dialects: &[SqlDialect]) -> Vec<KeywordInfo> {
    let mut seen = HashSet::new();
    dialects
        .iter()
        .flat_map(|dialect| dialect.dialect_info().keywords)
        .filter(|keyword_info| seen.insert(keyword_info.keyword.to_ascii_uppercase()))
        .collect()
}

pub(crate) fn add_context_keywords(
    dialects: &[SqlDialect],
    context: SyntaxCompletionContextKind,
    filter: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let sort_prefix = keyword_completion_sort_prefix_for_context(context);
    for keyword in completion_keyword_infos(dialects)
        .into_iter()
        .filter(|keyword| {
            keyword_allowed_in_completion_context(
                context,
                keyword.keyword.as_ref(),
                keyword.category,
            )
        })
    {
        add_keyword_completion_with_sort_prefix(
            dialects,
            keyword,
            filter,
            sort_prefix,
            completions,
        );
    }
}

pub(crate) fn add_keyword_completion_with_sort_prefix(
    dialects: &[SqlDialect],
    keyword: KeywordInfo,
    filter: &str,
    sort_prefix: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let keyword_label = keyword.keyword.as_ref();
    if !filter.is_empty()
        && !keyword_label
            .to_ascii_lowercase()
            .starts_with(&filter.to_ascii_lowercase())
    {
        return;
    }
    if completions
        .iter()
        .any(|completion| completion.label.eq_ignore_ascii_case(keyword_label))
    {
        return;
    }

    let detail = keyword
        .description
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| keyword_completion_detail(dialects, keyword_label));
    completions.push(completion_items::keyword_completion(
        keyword_label,
        detail,
        format!(
            "{}_{}",
            sort_prefix,
            completion_items::keyword_sort_text(&keyword)
        ),
    ));
}

pub(crate) fn add_filtered_keywords(
    dialects: &[SqlDialect],
    fuzzy_matcher: &FuzzyMatcher,
    dialect_name: &str,
    filter: &str,
    context: &str,
    completions: &mut Vec<CompletionItem>,
) {
    let is_start_context = keyword_context_is_start(context);
    let context_tokens = active_sql_unquoted_word_tokens(context);
    let filter = filter.to_ascii_lowercase();
    let dialect_keywords = completion_keyword_infos(dialects);
    let relevant_categories = keyword_context_categories(context);

    for keyword in &dialect_keywords {
        let Some(suffix) = keyword_suffix_for_context(keyword, &context_tokens, &filter) else {
            continue;
        };
        if completions
            .iter()
            .any(|completion| completion.label.eq_ignore_ascii_case(&suffix))
        {
            continue;
        }

        completions.push(completion_items::keyword_completion(
            &suffix,
            keyword
                .description
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("SQL Keyword ({dialect_name})")),
            format!("2_{suffix}"),
        ));
    }

    for keyword in dialect_keywords.iter().filter(|keyword| {
        if filter.is_empty() {
            keyword_is_default_relevant(keyword, is_start_context, &relevant_categories)
        } else {
            relevant_categories.contains(&keyword.category)
                || (is_start_context
                    && matches!(
                        keyword.category,
                        KeywordCategory::Dql
                            | KeywordCategory::Dml
                            | KeywordCategory::Ddl
                            | KeywordCategory::Transaction
                    ))
        }
    }) {
        let keyword_label = keyword.keyword.as_ref();
        if !filter.is_empty()
            && fuzzy_matcher
                .fuzzy_match(&filter, &keyword_label.to_ascii_lowercase())
                .is_none_or(|result| !result.is_match())
        {
            continue;
        }

        if completions
            .iter()
            .any(|completion| completion.label.eq_ignore_ascii_case(keyword_label))
        {
            continue;
        }

        completions.push(completion_items::keyword_completion(
            keyword_label,
            keyword
                .description
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| keyword_completion_detail(dialects, keyword_label)),
            completion_items::keyword_sort_text(keyword),
        ));
    }

    if !filter.is_empty() {
        for keyword in &dialect_keywords {
            let keyword_label = keyword.keyword.as_ref();
            if completions
                .iter()
                .any(|completion| completion.label.eq_ignore_ascii_case(keyword_label))
                || fuzzy_matcher
                    .fuzzy_match(&filter, &keyword_label.to_ascii_lowercase())
                    .is_none_or(|result| !result.is_match())
            {
                continue;
            }

            completions.push(completion_items::keyword_completion(
                keyword_label,
                keyword_completion_detail(dialects, keyword_label),
                format!("5_{}", completion_items::keyword_sort_text(keyword)),
            ));
        }
    }
}

fn keyword_is_default_relevant(
    keyword: &KeywordInfo,
    is_start_context: bool,
    relevant_categories: &[KeywordCategory],
) -> bool {
    keyword_info_relevant_for_context(keyword, is_start_context, relevant_categories)
}
