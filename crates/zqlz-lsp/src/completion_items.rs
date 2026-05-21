use crate::{ColumnInfo, FunctionInfo, TableInfo, ViewInfo};
use lsp_types::{CompletionItem, CompletionItemKind, Documentation, MarkupContent, MarkupKind};
use zqlz_core::{
    DataTypeInfo, KeywordInfo, SqlFunctionInfo, aggregate_function_sort_text,
    create_table_keyword_insert_text, create_table_keyword_sort_text, data_type_completion_detail,
    data_type_insert_text, data_type_labels, data_type_sort_text, dialect_function_detail,
    dialect_function_sort_text, keyword_category_rank, keyword_term_rank,
};

pub(crate) fn dialect_function_documentation(function: &SqlFunctionInfo) -> Option<Documentation> {
    if function.signatures.is_empty() {
        return None;
    }

    let signatures = function
        .signatures
        .iter()
        .map(|signature| format!("`{}`", signature.signature))
        .collect::<Vec<_>>()
        .join("\n");
    Some(Documentation::MarkupContent(MarkupContent {
        kind: MarkupKind::Markdown,
        value: signatures,
    }))
}

pub(crate) fn data_type_label_exists(
    completions: &[CompletionItem],
    data_type: &DataTypeInfo,
) -> bool {
    data_type_labels(data_type).any(|label| {
        completions
            .iter()
            .any(|completion| completion.label.eq_ignore_ascii_case(label))
    })
}

pub(crate) fn keyword_sort_text(keyword: &KeywordInfo) -> String {
    let category_rank = keyword_category_rank(keyword.category);
    let term_rank = keyword_term_rank(keyword.keyword.as_ref());
    format!(
        "4_{category_rank:02}_{term_rank:02}_{:?}_{}",
        keyword.category, keyword.keyword
    )
}

pub(crate) fn keyword_completion(label: &str, detail: String, sort_text: String) -> CompletionItem {
    CompletionItem {
        label: label.to_string(),
        kind: Some(CompletionItemKind::KEYWORD),
        detail: Some(detail),
        insert_text: Some(format!("{label} ")),
        sort_text: Some(sort_text),
        ..Default::default()
    }
}

pub(crate) fn ddl_keyword_completion(
    keyword: &KeywordInfo,
    detail: String,
    sort_text: String,
) -> CompletionItem {
    let label = keyword.keyword.as_ref();
    CompletionItem {
        label: label.to_string(),
        kind: Some(CompletionItemKind::KEYWORD),
        detail: Some(detail),
        insert_text: Some(create_table_keyword_insert_text(label)),
        sort_text: Some(sort_text),
        ..Default::default()
    }
}

pub(crate) fn auto_increment_completion(label: &str, detail: Option<String>) -> CompletionItem {
    CompletionItem {
        label: label.to_string(),
        kind: Some(CompletionItemKind::KEYWORD),
        detail,
        insert_text: Some(label.to_string()),
        sort_text: Some(format!("1_{label}")),
        ..Default::default()
    }
}

pub(crate) fn create_table_keyword_sort(label: &str) -> String {
    create_table_keyword_sort_text(label)
}

pub(crate) fn data_type_completion(data_type: &DataTypeInfo) -> CompletionItem {
    CompletionItem {
        label: data_type.name.to_string(),
        kind: Some(CompletionItemKind::STRUCT),
        detail: Some(data_type_completion_detail(data_type)),
        insert_text: Some(data_type_insert_text(data_type)),
        sort_text: Some(data_type_sort_text(data_type)),
        ..Default::default()
    }
}

pub(crate) fn table_completion(table: &TableInfo) -> CompletionItem {
    CompletionItem {
        label: table.name.clone(),
        kind: Some(CompletionItemKind::CLASS),
        detail: Some("Table".to_string()),
        insert_text: Some(format!("{} ", table.name)),
        filter_text: Some(table.name.clone()),
        sort_text: Some(format!("0_{}", table.name)),
        documentation: table
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        ..Default::default()
    }
}

pub(crate) fn view_completion(view: &ViewInfo) -> CompletionItem {
    CompletionItem {
        label: view.name.clone(),
        kind: Some(CompletionItemKind::INTERFACE),
        detail: Some("View".to_string()),
        insert_text: Some(format!("{} ", view.name)),
        filter_text: Some(view.name.clone()),
        sort_text: Some(format!("1_{}", view.name)),
        ..Default::default()
    }
}

pub(crate) fn column_completion(
    table: &str,
    column: &ColumnInfo,
    sort_prefix: &str,
) -> CompletionItem {
    CompletionItem {
        label: column.name.clone(),
        kind: Some(CompletionItemKind::FIELD),
        detail: Some(format!("{}.{}: {}", table, column.name, column.data_type)),
        insert_text: Some(column.name.clone()),
        filter_text: Some(column.name.clone()),
        sort_text: Some(format!("{sort_prefix}_{table}_{}", column.name)),
        documentation: column
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        ..Default::default()
    }
}

pub(crate) fn qualified_column_completion(
    table_ref_name: &str,
    identifier: &str,
    column: &ColumnInfo,
) -> CompletionItem {
    let label = format!("{identifier}.{}", column.name);
    CompletionItem {
        label: label.clone(),
        kind: Some(CompletionItemKind::FIELD),
        detail: Some(format!(
            "{}.{}: {}",
            table_ref_name, column.name, column.data_type
        )),
        insert_text: Some(label),
        filter_text: Some(column.name.clone()),
        sort_text: Some(format!("1_{}_{}", table_ref_name, column.name)),
        documentation: column
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        ..Default::default()
    }
}

pub(crate) fn dot_column_completion(table_name: &str, column: &ColumnInfo) -> CompletionItem {
    CompletionItem {
        label: column.name.clone(),
        kind: Some(CompletionItemKind::FIELD),
        detail: Some(format!(
            "{}.{}: {} ({})",
            table_name,
            column.name,
            column.data_type,
            if column.nullable { "NULL" } else { "NOT NULL" }
        )),
        insert_text: Some(column.name.clone()),
        filter_text: Some(column.name.clone()),
        sort_text: Some(format!("0_{}", column.name)),
        documentation: column
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        ..Default::default()
    }
}

pub(crate) fn derived_column_completion(column_name: &str) -> CompletionItem {
    CompletionItem {
        label: column_name.to_string(),
        kind: Some(CompletionItemKind::FIELD),
        detail: Some("Derived column".to_string()),
        insert_text: Some(column_name.to_string()),
        filter_text: Some(column_name.to_string()),
        sort_text: Some(format!("0_derived_{column_name}")),
        ..Default::default()
    }
}

pub(crate) fn user_function_completion(function: &FunctionInfo) -> CompletionItem {
    let detail = if function.is_aggregate {
        "Aggregate Function"
    } else {
        "Function"
    };
    let sort_text = if function.is_aggregate {
        aggregate_function_sort_text(&function.name)
    } else {
        zqlz_core::scalar_function_sort_text(&function.name)
    };

    CompletionItem {
        label: format!("{}()", function.name),
        kind: Some(CompletionItemKind::FUNCTION),
        detail: Some(format!("{} → {}", detail, function.return_type)),
        sort_text: Some(sort_text),
        documentation: function
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        insert_text: Some(format!("{}()", function.name)),
        filter_text: Some(function.name.clone()),
        ..Default::default()
    }
}

pub(crate) fn scalar_user_function_completion(function: &FunctionInfo) -> CompletionItem {
    CompletionItem {
        label: format!("{}()", function.name),
        kind: Some(CompletionItemKind::FUNCTION),
        detail: Some(format!("Scalar Function → {}", function.return_type)),
        sort_text: Some(format!("3_{}", function.name)),
        documentation: function
            .comment
            .as_ref()
            .map(|comment| Documentation::String(comment.clone())),
        insert_text: Some(format!("{}()", function.name)),
        filter_text: Some(function.name.clone()),
        ..Default::default()
    }
}

pub(crate) fn dialect_function_completion(function: &SqlFunctionInfo) -> CompletionItem {
    CompletionItem {
        label: format!("{}()", function.name),
        kind: Some(CompletionItemKind::FUNCTION),
        detail: Some(dialect_function_detail(function)),
        sort_text: Some(dialect_function_sort_text(function)),
        documentation: dialect_function_documentation(function),
        insert_text: Some(format!("{}()", function.name)),
        filter_text: Some(function.name.to_string()),
        ..Default::default()
    }
}
