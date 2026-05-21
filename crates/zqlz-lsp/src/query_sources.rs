use sqlparser::ast::{
    Expr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

pub(crate) fn derived_columns_for_identifier(
    identifier: &str,
    sql: &str,
    cursor_offset: Option<usize>,
    dialect: &dyn Dialect,
) -> Option<Vec<String>> {
    if !is_query_like_sql(sql) {
        return None;
    }

    let statements = parse_sql_allowing_dangling_qualified_reference(dialect, sql, cursor_offset)?;
    let mut derived_tables = HashMap::new();

    for statement in &statements {
        collect_derived_sources_from_statement(statement, &mut derived_tables);
    }

    derived_tables.remove(&identifier.to_lowercase())
}

pub(crate) fn resolve_alias_to_table(
    alias: &str,
    sql: &str,
    cursor_offset: Option<usize>,
    dialect: &dyn Dialect,
) -> Option<String> {
    if !is_query_like_sql(sql) {
        return None;
    }

    let statements = parse_sql_allowing_dangling_qualified_reference(dialect, sql, cursor_offset)?;
    let alias_lower = alias.to_ascii_lowercase();
    let mut aliases = HashMap::new();
    for statement in &statements {
        collect_table_aliases_from_statement(statement, &mut aliases);
    }

    aliases.get(&alias_lower).cloned()
}

fn parse_sql_allowing_dangling_qualified_reference(
    dialect: &dyn Dialect,
    sql: &str,
    cursor_offset: Option<usize>,
) -> Option<Vec<Statement>> {
    if let Ok(statements) = Parser::parse_sql(dialect, sql) {
        return Some(statements);
    }

    let offset = cursor_offset?;
    let offset = crate::clamp_to_char_boundary(sql, offset);
    if offset == 0 || !sql[..offset].ends_with('.') {
        return None;
    }

    let mut sanitized = String::with_capacity(sql.len() + "__zqlz_cursor".len());
    sanitized.push_str(&sql[..offset]);
    sanitized.push_str("__zqlz_cursor");
    sanitized.push_str(&sql[offset..]);
    Parser::parse_sql(dialect, &sanitized).ok()
}

fn is_query_like_sql(sql: &str) -> bool {
    zqlz_core::sql_has_query_context_tokens(sql)
}

fn collect_derived_sources_from_statement(
    statement: &Statement,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    if let Statement::Query(query) = statement {
        collect_derived_sources_from_query(query, derived_tables);
    }
}

fn collect_derived_sources_from_query(
    query: &Query,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let columns = projected_columns_from_query(&cte.query);
            insert_derived_source(&cte.alias, columns, derived_tables);
            collect_derived_sources_from_query(&cte.query, derived_tables);
        }
    }

    collect_derived_sources_from_set_expr(query.body.as_ref(), derived_tables);
}

fn collect_derived_sources_from_set_expr(
    set_expr: &SetExpr,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    match set_expr {
        SetExpr::Select(select) => collect_derived_sources_from_select(select, derived_tables),
        SetExpr::Query(query) => collect_derived_sources_from_query(query, derived_tables),
        SetExpr::SetOperation { left, right, .. } => {
            collect_derived_sources_from_set_expr(left, derived_tables);
            collect_derived_sources_from_set_expr(right, derived_tables);
        }
        _ => {}
    }
}

fn collect_derived_sources_from_select(
    select: &Select,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    for table_with_joins in &select.from {
        collect_derived_sources_from_table_factor(&table_with_joins.relation, derived_tables);
        for join in &table_with_joins.joins {
            collect_derived_sources_from_table_factor(&join.relation, derived_tables);
        }
    }
}

fn collect_derived_sources_from_table_factor(
    table_factor: &TableFactor,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    match table_factor {
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                let columns = projected_columns_from_query(subquery);
                insert_derived_source(alias, columns, derived_tables);
            }
            collect_derived_sources_from_query(subquery, derived_tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_derived_sources_from_table_factor(&table_with_joins.relation, derived_tables);
            for join in &table_with_joins.joins {
                collect_derived_sources_from_table_factor(&join.relation, derived_tables);
            }
        }
        _ => {}
    }
}

fn projected_columns_from_query(query: &Query) -> Vec<String> {
    match query.body.as_ref() {
        SetExpr::Select(select) => projected_columns_from_select(select),
        SetExpr::Query(query) => projected_columns_from_query(query),
        SetExpr::SetOperation { left, .. } => projected_columns_from_set_expr(left),
        _ => Vec::new(),
    }
}

fn projected_columns_from_set_expr(set_expr: &SetExpr) -> Vec<String> {
    match set_expr {
        SetExpr::Select(select) => projected_columns_from_select(select),
        SetExpr::Query(query) => projected_columns_from_query(query),
        SetExpr::SetOperation { left, .. } => projected_columns_from_set_expr(left),
        _ => Vec::new(),
    }
}

fn projected_columns_from_select(select: &Select) -> Vec<String> {
    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => columns.push(alias.value.clone()),
            SelectItem::UnnamedExpr(expr) => {
                if let Some(column_name) = column_name_from_expr(expr) {
                    columns.push(column_name);
                }
            }
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
        }
    }

    columns
}

fn column_name_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        _ => None,
    }
}

fn insert_derived_source(
    alias: &TableAlias,
    columns: Vec<String>,
    derived_tables: &mut HashMap<String, Vec<String>>,
) {
    let key = alias.name.value.to_lowercase();
    if !key.is_empty() {
        derived_tables.insert(key, columns);
    }
}

fn collect_table_aliases_from_statement(
    statement: &Statement,
    aliases: &mut HashMap<String, String>,
) {
    if let Statement::Query(query) = statement {
        collect_table_aliases_from_query(query, aliases);
    }
}

fn collect_table_aliases_from_query(query: &Query, aliases: &mut HashMap<String, String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_table_aliases_from_query(&cte.query, aliases);
        }
    }

    collect_table_aliases_from_set_expr(query.body.as_ref(), aliases);
}

fn collect_table_aliases_from_set_expr(set_expr: &SetExpr, aliases: &mut HashMap<String, String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_table_aliases_from_table_factor(&table_with_joins.relation, aliases);
                for join in &table_with_joins.joins {
                    collect_table_aliases_from_table_factor(&join.relation, aliases);
                }
            }
        }
        SetExpr::Query(query) => collect_table_aliases_from_query(query, aliases),
        SetExpr::SetOperation { left, right, .. } => {
            collect_table_aliases_from_set_expr(left, aliases);
            collect_table_aliases_from_set_expr(right, aliases);
        }
        _ => {}
    }
}

fn collect_table_aliases_from_table_factor(
    table_factor: &TableFactor,
    aliases: &mut HashMap<String, String>,
) {
    match table_factor {
        TableFactor::Table {
            name,
            alias: Some(alias),
            ..
        } => {
            let table_name = object_name_lookup_key(name);
            aliases.insert(alias.name.value.to_ascii_lowercase(), table_name);
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                aliases.insert(
                    alias.name.value.to_ascii_lowercase(),
                    alias.name.value.clone(),
                );
            }
            collect_table_aliases_from_query(subquery, aliases);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_aliases_from_table_factor(&table_with_joins.relation, aliases);
            for join in &table_with_joins.joins {
                collect_table_aliases_from_table_factor(&join.relation, aliases);
            }
        }
        _ => {}
    }
}

fn object_name_lookup_key(table_name: &ObjectName) -> String {
    table_name
        .0
        .last()
        .map(|identifier| identifier.value.clone())
        .unwrap_or_else(|| table_name.to_string())
}
