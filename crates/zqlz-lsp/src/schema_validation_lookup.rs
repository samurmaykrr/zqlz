use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Expr, ObjectName, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};

use crate::{ColumnInfo, SchemaCache};

/// Query-local relations (CTEs and aliased derived tables) keyed by lowercase
/// name. The value is the relation's output column names, or `None` when they
/// can't be known statically (e.g. it selects `*`).
pub(crate) type CteScope = HashMap<String, Option<HashSet<String>>>;

/// Output column names of a CTE or derived table, preferring an explicit
/// column list (`WITH x(a, b) AS ...`, `(...) AS d(a, b)`).
pub(crate) fn relation_output_columns(
    alias: &TableAlias,
    query: &Query,
) -> Option<HashSet<String>> {
    if !alias.columns.is_empty() {
        return Some(
            alias
                .columns
                .iter()
                .map(|column| column.value.to_lowercase())
                .collect(),
        );
    }
    query_output_columns(query)
}

fn query_output_columns(query: &Query) -> Option<HashSet<String>> {
    set_expr_output_columns(query.body.as_ref())
}

fn set_expr_output_columns(set_expr: &SetExpr) -> Option<HashSet<String>> {
    match set_expr {
        SetExpr::Select(select) => {
            let mut columns = HashSet::new();
            for projection in &select.projection {
                let name = match projection {
                    SelectItem::ExprWithAlias { alias, .. } => &alias.value,
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => &ident.value,
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                        &parts.last()?.value
                    }
                    SelectItem::UnnamedExpr(Expr::Function(function)) => {
                        &function.name.0.last()?.value
                    }
                    SelectItem::UnnamedExpr(_) => continue,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => return None,
                };
                columns.insert(name.to_lowercase());
            }
            Some(columns)
        }
        SetExpr::Query(query) => query_output_columns(query),
        SetExpr::SetOperation { left, .. } => set_expr_output_columns(left),
        _ => None,
    }
}

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

/// `cte_names` extended with the select's aliased derived tables, so their
/// output columns resolve like a CTE's.
pub(crate) fn scope_with_derived_tables(select: &Select, cte_names: &CteScope) -> CteScope {
    let mut scope = cte_names.clone();
    for table_with_joins in &select.from {
        let relations = std::iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation));
        for relation in relations {
            if let TableFactor::Derived {
                subquery,
                alias: Some(alias),
                ..
            } = relation
            {
                scope.insert(
                    alias.name.value.to_lowercase(),
                    relation_output_columns(alias, subquery),
                );
            } else if let Some(alias) = table_function_alias(relation) {
                // `JSON_TABLE(...) AS jt`, `UNNEST(...) AS u`: columns aren't known statically.
                scope.insert(alias.name.value.to_lowercase(), None);
            }
        }
    }
    scope
}

pub(crate) fn collect_select_table_aliases(
    select: &Select,
    available_tables: &mut HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &CteScope,
) {
    for table_with_joins in &select.from {
        collect_table_aliases(table_with_joins, available_tables, schema, cte_names);
    }
}

pub(crate) fn collect_table_aliases(
    table_with_joins: &TableWithJoins,
    aliases: &mut HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &CteScope,
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
    cte_names: &CteScope,
) {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_lookup_key(name);
            // Keep catalog and other-database relations qualified so
            // `information_schema.tables` or `other_db.users` can't borrow the columns
            // of a cached table with the same name.
            let canonical =
                if name.0.len() > 1 && is_unvalidated_relation(&name.to_string(), schema) {
                    name.to_string()
                } else {
                    schema_table_name(name, schema)
                        .cloned()
                        .or_else(|| {
                            cte_names
                                .contains_key(&table_name.to_ascii_lowercase())
                                .then(|| table_name.clone())
                        })
                        .unwrap_or_else(|| {
                            if is_unvalidated_relation(&name.to_string(), schema) {
                                name.to_string()
                            } else {
                                table_name.clone()
                            }
                        })
                };

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
        other => {
            if let Some(alias_obj) = table_function_alias(other) {
                let alias_name = alias_obj.name.value.to_lowercase();
                aliases.insert(alias_name.clone(), alias_name);
            }
        }
    }
}

/// Alias of a relation produced by a table function rather than a table or subquery.
fn table_function_alias(table_factor: &TableFactor) -> Option<&TableAlias> {
    match table_factor {
        TableFactor::TableFunction { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::JsonTable { alias, .. }
        | TableFactor::Pivot { alias, .. }
        | TableFactor::Unpivot { alias, .. }
        | TableFactor::MatchRecognize { alias, .. } => alias.as_ref(),
        _ => None,
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
        .chain(schema.views.keys())
        .find(|name| table_name_matches(name, &lookup_key))
}

/// Relations the schema cache can't vouch for either way: system catalogs, and
/// relations qualified with a database/schema other than the one the cache was
/// loaded for (MySQL `other_db.orders`, whose tables were never fetched).
/// Takes the dotted relation name as written in the query (quotes allowed).
pub(crate) fn is_unvalidated_relation(table_name: &str, schema: &SchemaCache) -> bool {
    let segments: Vec<&str> = table_name
        .split('.')
        .map(|segment| segment.trim_matches(|c| matches!(c, '`' | '"' | '[' | ']')))
        .collect();
    let [qualifiers @ .., name] = segments.as_slice() else {
        return false;
    };
    let Some(qualifier) = qualifiers.last() else {
        // Unqualified `pg_*` relations resolve to pg_catalog, which is always on the
        // search path ahead of user schemas; `DUAL` is MySQL's/Oracle's dummy table.
        return name.to_ascii_lowercase().starts_with("pg_") || name.eq_ignore_ascii_case("dual");
    };
    const SYSTEM_SCHEMAS: &[&str] = &[
        "pg_catalog",
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
    ];
    if SYSTEM_SCHEMAS
        .iter()
        .any(|catalog| qualifier.eq_ignore_ascii_case(catalog))
    {
        return true;
    }
    let loaded_namespaces: Vec<&str> = [&schema.database_name, &schema.schema_name]
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect();
    // Cache keys qualified with this namespace mean its tables were loaded after all.
    let namespace_cached = schema.tables.keys().chain(schema.views.keys()).any(|key| {
        key.rsplit_once('.')
            .is_some_and(|(key_qualifier, _)| key_qualifier.eq_ignore_ascii_case(qualifier))
    });
    !loaded_namespaces.is_empty()
        && !namespace_cached
        && !loaded_namespaces
            .iter()
            .any(|namespace| namespace.eq_ignore_ascii_case(qualifier))
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

/// Whether `table_name` refers to a table the schema cache already knows
/// about, independent of whether its `columns_by_table` entry has finished
/// loading yet. Used to tell "columns haven't lazy-loaded for this real
/// table" (stay silent) apart from "this table doesn't exist" (still valid
/// to flag as unknown).
pub(crate) fn is_known_table(table_name: &str, schema: &SchemaCache) -> bool {
    is_unvalidated_relation(table_name, schema)
        || schema
            .tables
            .keys()
            .chain(schema.views.keys())
            .any(|name| table_name_matches(name, table_name))
}

pub(crate) fn object_name_lookup_key(table_name: &ObjectName) -> String {
    table_name
        .0
        .last()
        .map(|identifier| identifier.value.clone())
        .unwrap_or_else(|| table_name.to_string())
}
