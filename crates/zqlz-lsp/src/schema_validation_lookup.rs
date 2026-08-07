use std::collections::{HashMap, HashSet};

use sqlparser::ast::{ObjectName, Select, SelectItem, TableFactor, TableWithJoins};

use crate::{ColumnInfo, SchemaCache};

pub(crate) fn select_aliases(select: &Select) -> HashSet<String> {
    select
        .projection
        .iter()
        .filter_map(|projection| match projection {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect()
}

pub(crate) fn collect_select_table_aliases(
    select: &Select,
    available_tables: &mut HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
) {
    for table_with_joins in &select.from {
        collect_table_aliases(table_with_joins, available_tables, schema, cte_names);
    }
}

pub(crate) fn collect_table_aliases(
    table_with_joins: &TableWithJoins,
    aliases: &mut HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
) {
    extract_table_alias(&table_with_joins.relation, aliases, schema, cte_names);

    for join in &table_with_joins.joins {
        extract_table_alias(&join.relation, aliases, schema, cte_names);
    }
}

fn extract_table_alias(
    table_factor: &TableFactor,
    aliases: &mut HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
) {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_lookup_key(name);
            let canonical = schema_table_name(name, schema)
                .cloned()
                .or_else(|| {
                    cte_names
                        .contains(&table_name.to_ascii_lowercase())
                        .then(|| table_name.clone())
                })
                .unwrap_or_else(|| table_name.clone());

            let key = table_name.to_ascii_lowercase();
            aliases.insert(key, canonical.clone());

            if let Some(alias_obj) = alias {
                aliases.insert(alias_obj.name.value.to_lowercase(), canonical);
            }
        }
        TableFactor::Derived {
            alias: Some(alias_obj),
            ..
        } => {
            let alias_name = alias_obj.name.value.to_lowercase();
            aliases.insert(alias_name.clone(), alias_name);
        }
        _ => {}
    }
}

pub(crate) fn schema_table_name<'a>(
    table_name: &ObjectName,
    schema: &'a SchemaCache,
) -> Option<&'a String> {
    let lookup_key = object_name_lookup_key(table_name);
    schema
        .tables
        .keys()
        .find(|name| table_name_matches(name, &lookup_key))
}

/// Compares a schema-cache key against an unqualified lookup key.
///
/// Cache keys may be stored schema-qualified (e.g. `"public.users"`, when the
/// driver was queried across all schemas) or unqualified (e.g. `"users"`, when
/// queried within a single schema) depending on the driver/scope that
/// populated the cache. `object_name_lookup_key` always reduces the AST
/// reference to its unqualified base name, so the cache side must be reduced
/// the same way before comparing.
fn table_name_matches(cache_key: &str, lookup_key: &str) -> bool {
    if cache_key.eq_ignore_ascii_case(lookup_key) {
        return true;
    }
    cache_key
        .rsplit_once('.')
        .map(|(_, name)| name.eq_ignore_ascii_case(lookup_key))
        .unwrap_or(false)
}

pub(crate) fn schema_column_names(
    table_name: &ObjectName,
    schema: &SchemaCache,
) -> Option<HashSet<String>> {
    schema_columns_for_object_name(table_name, schema).map(|columns| {
        columns
            .iter()
            .map(|column| column.name.to_ascii_lowercase())
            .collect()
    })
}

pub(crate) fn schema_columns_for_object_name<'a>(
    table_name: &ObjectName,
    schema: &'a SchemaCache,
) -> Option<&'a Vec<ColumnInfo>> {
    let lookup_key = object_name_lookup_key(table_name);
    schema_columns_for_table_name(&lookup_key, schema)
}

pub(crate) fn schema_columns_for_table_name<'a>(
    table_name: &str,
    schema: &'a SchemaCache,
) -> Option<&'a Vec<ColumnInfo>> {
    schema
        .columns_by_table
        .iter()
        .find(|(name, _)| table_name_matches(name, table_name))
        .map(|(_, columns)| columns)
}

pub(crate) fn object_name_lookup_key(table_name: &ObjectName) -> String {
    table_name
        .0
        .last()
        .map(|identifier| identifier.value.clone())
        .unwrap_or_else(|| table_name.to_string())
}
