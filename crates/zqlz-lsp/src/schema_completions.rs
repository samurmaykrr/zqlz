use lsp_types::{CompletionItem, CompletionItemKind};

use crate::{SchemaCache, TableRef, completion_items};

pub(crate) fn add_columns_from_tables(
    schema_cache: &SchemaCache,
    tables: &[TableRef],
    completions: &mut Vec<CompletionItem>,
) {
    for table_ref in tables {
        tracing::debug!(
            table_name = %table_ref.table_name,
            alias = ?table_ref.alias,
            "add_columns_from_tables: processing table"
        );

        if let Some(columns) = columns_for_table(schema_cache, &table_ref.table_name) {
            for column in columns {
                let completion = if table_ref.alias.is_some() {
                    completion_items::qualified_column_completion(
                        &table_ref.table_name,
                        table_ref.identifier(),
                        column,
                    )
                } else {
                    completion_items::column_completion(&table_ref.table_name, column, "1")
                };
                completions.push(completion);
            }
        } else {
            tracing::debug!(
                table_name = %table_ref.table_name,
                "add_columns_from_tables: no columns found in schema cache"
            );
        }
    }
}

pub(crate) fn add_columns_from_names(
    column_names: &[String],
    completions: &mut Vec<CompletionItem>,
) {
    for column_name in column_names {
        completions.push(completion_items::derived_column_completion(column_name));
    }
}

pub(crate) fn add_tables_with_fk_suggestions(
    schema_cache: &SchemaCache,
    existing_tables: &[TableRef],
    completions: &mut Vec<CompletionItem>,
) {
    add_tables(schema_cache, completions);

    for existing_table in existing_tables {
        if let Some(referencing_tables) = schema_cache
            .reverse_foreign_keys
            .get(&existing_table.table_name)
        {
            for (source_table, foreign_key) in referencing_tables {
                completions.push(CompletionItem {
                    label: source_table.clone(),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!(
                        "Table (FK: {} → {}.{})",
                        foreign_key.columns.join(", "),
                        foreign_key.referenced_table,
                        foreign_key.referenced_columns.join(", ")
                    )),
                    sort_text: Some(format!("00_{}", source_table)),
                    ..Default::default()
                });
            }
        }

        if let Some(foreign_keys) = schema_cache
            .foreign_keys_by_table
            .get(&existing_table.table_name)
        {
            for foreign_key in foreign_keys {
                let referenced_table = &foreign_key.referenced_table;
                completions.push(CompletionItem {
                    label: referenced_table.clone(),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!(
                        "Table (FK from {}: {} → {})",
                        existing_table.table_name,
                        foreign_key.columns.join(", "),
                        foreign_key.referenced_columns.join(", ")
                    )),
                    sort_text: Some(format!("00_{}", referenced_table)),
                    ..Default::default()
                });
            }
        }
    }
}

pub(crate) fn add_tables(schema_cache: &SchemaCache, completions: &mut Vec<CompletionItem>) {
    for table in schema_cache.tables.values() {
        completions.push(completion_items::table_completion(table));
    }
}

pub(crate) fn add_views(schema_cache: &SchemaCache, completions: &mut Vec<CompletionItem>) {
    for view in schema_cache.views.values() {
        completions.push(completion_items::view_completion(view));
    }
}

pub(crate) fn add_all_columns(schema_cache: &SchemaCache, completions: &mut Vec<CompletionItem>) {
    #[cfg(test)]
    println!(
        "DEBUG add_filtered_columns: cache has {} tables",
        schema_cache.columns_by_table.len()
    );

    for (table, columns) in &schema_cache.columns_by_table {
        #[cfg(test)]
        println!(
            "DEBUG: Checking table '{}' with {} columns",
            table,
            columns.len()
        );

        for column in columns {
            #[cfg(test)]
            println!(
                "DEBUG: Adding column '{}' from table '{}'",
                column.name, table
            );

            completions.push(completion_items::column_completion(table, column, "2"));
        }
    }
}

fn columns_for_table<'a>(
    schema_cache: &'a SchemaCache,
    table_name: &str,
) -> Option<&'a Vec<crate::ColumnInfo>> {
    schema_cache
        .columns_by_table
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(table_name))
        .map(|(_, columns)| columns)
}
