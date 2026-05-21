use lsp_types::{CompletionItem, CompletionItemKind, InsertTextFormat, MarkupContent, MarkupKind};
use std::collections::{HashMap, HashSet};
use zqlz_core::KeywordDef;
use zqlz_core::command::{
    command_completion_context, command_completion_insert_text,
    command_metadata_subcommand_candidate, command_metadata_top_level_candidate,
    command_prefix_value,
};
use zqlz_core::{redis_command_catalog, redis_command_spec::RedisCommandSpec};
use zqlz_drivers::get_dialect_bundle;
use zqlz_ui::widgets::Rope;

pub(crate) fn get_redis_completions(
    driver_type: &str,
    text: &Rope,
    offset: usize,
    is_manual_trigger: bool,
) -> Vec<CompletionItem> {
    let prefix = text.to_string();
    let Some(context) = command_completion_context(&prefix, offset, is_manual_trigger) else {
        return Vec::new();
    };
    let command_catalog = redis_command_catalog::builtin_commands();

    if context.current_tokens.is_empty() {
        return redis_top_level_command_candidates(driver_type, None, command_catalog)
            .into_iter()
            .map(|command| redis_completion_item(driver_type, &command, &[], "Redis command"))
            .collect();
    }

    if context.current_tokens.len() == 1 && context.active_prefix.is_some() {
        return redis_top_level_command_candidates(
            driver_type,
            context.active_prefix.as_deref(),
            command_catalog,
        )
        .into_iter()
        .map(|command| redis_completion_item(driver_type, &command, &[], "Redis command"))
        .collect();
    }

    let command_path: Vec<&str> = context.command_path.iter().map(String::as_str).collect();

    redis_subcommand_candidates(
        driver_type,
        &command_path,
        context.active_prefix.as_deref(),
        command_catalog,
    )
    .into_iter()
    .map(|subcommand| {
        redis_completion_item(driver_type, &subcommand, &command_path, "Redis subcommand")
    })
    .collect()
}

fn redis_top_level_command_candidates(
    driver_type: &str,
    prefix: Option<&str>,
    command_catalog: &HashMap<String, RedisCommandSpec>,
) -> Vec<String> {
    let prefix = prefix.map(str::to_ascii_uppercase).unwrap_or_default();
    let mut seen = HashSet::new();
    let mut commands = Vec::new();

    if let Some(bundle) = get_dialect_bundle(driver_type.trim()) {
        for keyword in &bundle.completions.keywords {
            if let Some(command) =
                command_metadata_top_level_candidate(keyword, command_prefix_value(&prefix))
                && seen.insert(command.clone())
            {
                commands.push(command);
            }
        }
    }

    for command in
        redis_command_catalog::top_level_commands(command_catalog, command_prefix_value(&prefix))
    {
        if seen.insert(command.clone()) {
            commands.push(command);
        }
    }

    commands
}

fn redis_subcommand_candidates(
    driver_type: &str,
    command_path: &[&str],
    prefix: Option<&str>,
    command_catalog: &HashMap<String, RedisCommandSpec>,
) -> Vec<String> {
    let prefix = prefix.map(str::to_ascii_uppercase).unwrap_or_default();
    let mut seen = HashSet::new();
    let mut subcommands = Vec::new();

    if let Some(bundle) = get_dialect_bundle(driver_type.trim()) {
        for keyword in &bundle.completions.keywords {
            if let Some(candidate) = command_metadata_subcommand_candidate(
                keyword,
                command_path,
                command_prefix_value(&prefix),
            ) && seen.insert(candidate.clone())
            {
                subcommands.push(candidate);
            }
        }
    }

    for subcommand in redis_command_catalog::subcommands_for_path(
        command_catalog,
        command_path,
        command_prefix_value(&prefix),
    ) {
        if seen.insert(subcommand.clone()) {
            subcommands.push(subcommand);
        }
    }

    subcommands
}

fn redis_completion_item(
    driver_type: &str,
    label: &str,
    command_path: &[&str],
    fallback_detail: &str,
) -> CompletionItem {
    let metadata = redis_keyword_metadata(driver_type, label, command_path);
    let insert_text = metadata
        .and_then(|keyword| keyword.snippet.as_deref())
        .map(|snippet| command_completion_insert_text(snippet, command_path))
        .unwrap_or_else(|| format!("{} ", label));
    let uses_snippet = insert_text.contains("${");

    CompletionItem {
        label: label.to_string(),
        kind: Some(CompletionItemKind::KEYWORD),
        detail: metadata
            .and_then(|keyword| keyword.description.clone())
            .or_else(|| Some(fallback_detail.to_string())),
        documentation: metadata.and_then(|keyword| {
            keyword.documentation.as_ref().map(|documentation| {
                lsp_types::Documentation::MarkupContent(MarkupContent {
                    kind: MarkupKind::Markdown,
                    value: documentation.clone(),
                })
            })
        }),
        insert_text: Some(insert_text),
        insert_text_format: uses_snippet.then_some(InsertTextFormat::SNIPPET),
        filter_text: Some(label.to_string()),
        ..Default::default()
    }
}

fn redis_keyword_metadata<'a>(
    driver_type: &str,
    label: &str,
    command_path: &[&str],
) -> Option<&'a KeywordDef> {
    let bundle = get_dialect_bundle(driver_type.trim())?;
    let full_command = command_path
        .iter()
        .copied()
        .chain(std::iter::once(label))
        .collect::<Vec<_>>()
        .join(" ");

    bundle
        .completions
        .keywords
        .iter()
        .find(|keyword| keyword.name.eq_ignore_ascii_case(&full_command))
        .or_else(|| {
            let root_command = command_path.first()?;
            bundle.completions.keywords.iter().find(|keyword| {
                keyword.name.eq_ignore_ascii_case(root_command)
                    && keyword.snippet.as_ref().is_some_and(|snippet| {
                        snippet
                            .get(..full_command.len())
                            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&full_command))
                    })
            })
        })
        .or_else(|| {
            command_path.is_empty().then(|| {
                bundle
                    .completions
                    .keywords
                    .iter()
                    .find(|keyword| keyword.name.eq_ignore_ascii_case(label))
            })?
        })
}
