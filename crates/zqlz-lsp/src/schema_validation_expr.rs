use std::collections::{HashMap, HashSet};

use sqlparser::ast::{Expr, Query};

use crate::{
    SchemaCache,
    schema_validation_issue::{ValidationIssue, ValidationSeverity, ValidationSymbolRole},
    schema_validation_lookup,
};

pub(crate) fn validate_expression<F>(
    expr: &Expr,
    available_tables: &HashMap<String, String>,
    select_aliases: &HashSet<String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
    validate_query: &mut F,
) where
    F: FnMut(&Query, &SchemaCache, &HashSet<String>, &mut Vec<ValidationIssue>),
{
    match expr {
        Expr::Identifier(ident) => {
            let column_name = ident.value.to_lowercase();

            if select_aliases.contains(&column_name) {
                return;
            }

            let mut found = false;
            for actual_table_name in available_tables.values() {
                if let Some(columns) = schema_validation_lookup::schema_columns_for_table_name(
                    actual_table_name,
                    schema,
                ) && columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&column_name))
                {
                    found = true;
                    break;
                }
            }

            if !found && !available_tables.is_empty() {
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
        Expr::Subquery(query) => {
            validate_query(query, schema, cte_names, issues);
        }
        _ => {}
    }
}
