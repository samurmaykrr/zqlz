use std::collections::{HashMap, HashSet};

use sqlparser::ast::{Expr, Query};

use crate::{
    SchemaCache,
    schema_validation_issue::{ValidationIssue, ValidationSeverity, ValidationSymbolRole},
    schema_validation_lookup,
    schema_validation_lookup::CteScope,
};

pub(crate) fn validate_expression<F>(
    expr: &Expr,
    available_tables: &HashMap<String, String>,
    select_aliases: &HashSet<String>,
    schema: &SchemaCache,
    cte_names: &CteScope,
    issues: &mut Vec<ValidationIssue>,
    validate_query: &mut F,
) where
    F: FnMut(&Query, &SchemaCache, &CteScope, &mut Vec<ValidationIssue>),
{
    match expr {
        Expr::Identifier(ident) => {
            let column_name = ident.value.to_lowercase();

            // `@user_var` / `@@system_var` are variables, not columns.
            if select_aliases.contains(&column_name)
                || (ident.quote_style.is_none() && ident.value.starts_with('@'))
            {
                return;
            }

            // A real table's columns can still be mid-flight through the
            // async lazy-load that populates `schema.columns_by_table` (e.g.
            // driven by sidebar table-tree expansion), even though the table
            // itself is already known via `schema.tables`. Only treat a
            // referenced table as evidence against the column when either its
            // columns have loaded, or the table doesn't actually exist (so
            // there is nothing left to wait on).
            let mut found = false;
            let mut any_table_usable = false;
            for actual_table_name in available_tables.values() {
                if let Some(relation_columns) =
                    cte_names.get(&actual_table_name.to_ascii_lowercase())
                {
                    match relation_columns {
                        Some(columns) => {
                            any_table_usable = true;
                            if columns.contains(&column_name) {
                                found = true;
                                break;
                            }
                        }
                        // A `SELECT *` relation's columns aren't known here,
                        // so it can't disprove the reference.
                        None => {
                            found = true;
                            break;
                        }
                    }
                    continue;
                }
                if let Some(columns) = schema_validation_lookup::schema_columns_for_table_name(
                    actual_table_name,
                    schema,
                ) {
                    any_table_usable = true;
                    if columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(&column_name))
                    {
                        found = true;
                        break;
                    }
                } else if !schema_validation_lookup::is_known_table(actual_table_name, schema) {
                    any_table_usable = true;
                } else {
                    // A known relation whose columns aren't loaded (or can't be, like
                    // `information_schema.tables`) may well own the column.
                    found = true;
                    break;
                }
            }

            if !found && any_table_usable {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Warning,
                    message: format!("Column '{}' may not exist in available tables", ident.value),
                    symbol: Some(ident.value.clone()),
                    symbol_role: Some(ValidationSymbolRole::Column),
                    qualifier: None,
                    line: 0,
                    column: 0,
                });
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table_or_alias = parts[0].value.to_lowercase();
            let column_name = parts[1].value.to_lowercase();

            // `@@session.sql_mode` is a system variable; an alias from an enclosing
            // query (correlated subquery) or a CTE can't be checked from here.
            if (parts[0].quote_style.is_none() && parts[0].value.starts_with('@'))
                || (!available_tables.contains_key(&table_or_alias)
                    && cte_names.contains_key(&table_or_alias))
            {
                return;
            }

            if let Some(actual_table_name) = available_tables.get(&table_or_alias) {
                if let Some(columns) = schema_validation_lookup::schema_columns_for_table_name(
                    actual_table_name,
                    schema,
                ) && !columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&column_name))
                {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Error,
                        message: format!(
                            "Column '{}' does not exist in table '{}'",
                            parts[1].value, actual_table_name
                        ),
                        symbol: Some(parts[1].value.clone()),
                        symbol_role: Some(ValidationSymbolRole::Column),
                        qualifier: Some(parts[0].value.clone()),
                        line: 0,
                        column: 0,
                    });
                }
            } else {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Warning,
                    message: format!("Unknown table or alias: {}", parts[0].value),
                    symbol: Some(parts[0].value.clone()),
                    symbol_role: Some(ValidationSymbolRole::Table),
                    qualifier: None,
                    line: 0,
                    column: 0,
                });
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expression(
                left,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            validate_expression(
                right,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Nested(expr) => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            validate_expression(
                timestamp,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            validate_expression(
                time_zone,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Trim { expr, .. } => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::Position { expr, r#in } => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            validate_expression(
                r#in,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            if let Some(substring_from) = substring_from {
                validate_expression(
                    substring_from,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
            if let Some(substring_for) = substring_for {
                validate_expression(
                    substring_for,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            for expression in [expr.as_ref(), overlay_what.as_ref(), overlay_from.as_ref()] {
                validate_expression(
                    expression,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
            if let Some(overlay_for) = overlay_for {
                validate_expression(
                    overlay_for,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(ref argument_list) = function.args {
                for argument in &argument_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expression),
                    ) = argument
                    {
                        validate_expression(
                            expression,
                            available_tables,
                            select_aliases,
                            schema,
                            cte_names,
                            issues,
                            validate_query,
                        );
                    }
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            for item in list {
                validate_expression(
                    item,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            for expression in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                validate_expression(
                    expression,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                validate_expression(
                    operand,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
            for expression in conditions.iter().chain(results.iter()) {
                validate_expression(
                    expression,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
            if let Some(else_expression) = else_result {
                validate_expression(
                    else_expression,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                    validate_query,
                );
            }
        }
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            validate_subquery(
                query,
                available_tables,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        Expr::InSubquery { expr, subquery, .. } => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_query,
            );
            validate_subquery(
                subquery,
                available_tables,
                schema,
                cte_names,
                issues,
                validate_query,
            );
        }
        _ => {}
    }
}

/// A subquery may reference the enclosing query's relations (correlated subquery).
/// Their aliases enter its scope with unknown columns, so such references are not
/// mistaken for unknown aliases.
fn validate_subquery<F>(
    query: &Query,
    available_tables: &HashMap<String, String>,
    schema: &SchemaCache,
    cte_names: &CteScope,
    issues: &mut Vec<ValidationIssue>,
    validate_query: &mut F,
) where
    F: FnMut(&Query, &SchemaCache, &CteScope, &mut Vec<ValidationIssue>),
{
    let mut scope = cte_names.clone();
    for alias in available_tables.keys() {
        scope.entry(alias.clone()).or_insert(None);
    }
    validate_query(query, schema, &scope, issues);
}
