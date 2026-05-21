use std::collections::HashSet;

use lsp_types::{CompletionItem, CompletionItemKind};
use zqlz_core::{
    DataTypeInfo, KeywordCategory, SqlFunctionInfo, function_category_allowed_in_condition,
};

use crate::{SchemaCache, SqlDialect, completion_items};

pub(crate) fn completion_sql_dialects(dialect: SqlDialect) -> Vec<SqlDialect> {
    match dialect {
        SqlDialect::Generic => {
            let mut seen = HashSet::new();
            let mut driver_ids: Vec<_> = zqlz_core::DIALECT_REGISTRY
                .sql_profiles()
                .map(|profile| profile.id)
                .collect();
            driver_ids.sort_unstable();

            driver_ids
                .into_iter()
                .map(SqlDialect::from_driver)
                .filter(|dialect| !matches!(dialect, SqlDialect::Generic) && seen.insert(*dialect))
                .collect()
        }
        SqlDialect::Redis => Vec::new(),
        dialect => vec![dialect],
    }
}

pub(crate) fn completion_function_infos(dialects: &[SqlDialect]) -> Vec<SqlFunctionInfo> {
    let mut seen = HashSet::new();
    let mut functions = Vec::new();

    for dialect in dialects {
        for function in dialect.dialect_info().functions {
            if seen.insert(function.name.to_ascii_uppercase()) {
                functions.push(function);
            }
        }
    }

    functions
}

pub(crate) fn completion_scalar_function_infos(
    dialects: &[SqlDialect],
) -> impl Iterator<Item = SqlFunctionInfo> {
    completion_function_infos(dialects)
        .into_iter()
        .filter(|function| function_category_allowed_in_condition(function.category))
}

pub(crate) fn completion_data_type_infos(dialects: &[SqlDialect]) -> Vec<DataTypeInfo> {
    let mut seen = HashSet::new();
    let mut data_types = Vec::new();

    for dialect in dialects {
        for data_type in dialect.dialect_info().data_types {
            if seen.insert(data_type.name.to_ascii_uppercase()) {
                data_types.push(data_type);
            }
        }
    }

    data_types
}

pub(crate) fn add_create_table_keywords(
    dialects: &[SqlDialect],
    completions: &mut Vec<CompletionItem>,
) {
    for dialect in dialects {
        let dialect_info = dialect.dialect_info();
        if let Some(auto_increment) = dialect_info.auto_increment.as_ref()
            && !completions.iter().any(|completion| {
                completion
                    .label
                    .eq_ignore_ascii_case(&auto_increment.keyword)
            })
        {
            completions.push(completion_items::auto_increment_completion(
                auto_increment.keyword.as_ref(),
                auto_increment
                    .description
                    .as_ref()
                    .map(ToString::to_string)
                    .or_else(|| Some(format!("Auto-increment ({})", dialect_info.display_name))),
            ));
        }

        for keyword in dialect_info
            .keywords
            .iter()
            .filter(|keyword| keyword.category == KeywordCategory::Ddl)
        {
            if completions
                .iter()
                .any(|completion| completion.label.eq_ignore_ascii_case(&keyword.keyword))
            {
                continue;
            }

            completions.push(completion_items::ddl_keyword_completion(
                keyword,
                keyword
                    .description
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| format!("DDL keyword ({})", dialect_info.display_name)),
                completion_items::create_table_keyword_sort(keyword.keyword.as_ref()),
            ));
        }
    }
}

pub(crate) fn add_data_types(data_types: &[DataTypeInfo], completions: &mut Vec<CompletionItem>) {
    for data_type in data_types {
        if completion_items::data_type_label_exists(completions, data_type) {
            continue;
        }

        #[cfg(test)]
        println!("DEBUG: Adding data type: {}", data_type.name);

        completions.push(completion_items::data_type_completion(data_type));
    }
}

pub(crate) fn add_functions(
    schema_cache: &SchemaCache,
    functions: &[SqlFunctionInfo],
    completions: &mut Vec<CompletionItem>,
) {
    for function in schema_cache.functions.values() {
        completions.push(completion_items::user_function_completion(function));
    }

    add_dialect_functions(functions.iter(), completions);
}

pub(crate) fn add_scalar_functions(
    schema_cache: &SchemaCache,
    functions: impl Iterator<Item = SqlFunctionInfo>,
    completions: &mut Vec<CompletionItem>,
) {
    for function in schema_cache.functions.values() {
        if function.is_aggregate {
            continue;
        }

        completions.push(completion_items::scalar_user_function_completion(function));
    }

    add_dialect_functions(functions, completions);
}

fn add_dialect_functions<'a>(
    functions: impl Iterator<Item = impl std::borrow::Borrow<SqlFunctionInfo> + 'a>,
    completions: &mut Vec<CompletionItem>,
) {
    for function in functions {
        let function = function.borrow();
        let function_name = function.name.as_ref();
        if has_function_completion(completions, function_name) {
            continue;
        }

        completions.push(completion_items::dialect_function_completion(function));
    }
}

fn has_function_completion(completions: &[CompletionItem], function_name: &str) -> bool {
    completions.iter().any(|completion| {
        completion.kind == Some(CompletionItemKind::FUNCTION)
            && completion
                .filter_text
                .as_deref()
                .is_some_and(|existing| existing.eq_ignore_ascii_case(function_name))
    })
}
