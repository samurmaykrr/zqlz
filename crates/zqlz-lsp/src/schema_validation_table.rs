use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Expr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, TableFactor, TableWithJoins,
};

use crate::{
    SchemaCache,
    schema_validation_issue::{ValidationIssue, ValidationSeverity, ValidationSymbolRole},
    schema_validation_lookup,
};

pub(crate) struct TableValidationContext<'a> {
    pub(crate) schema: &'a SchemaCache,
    pub(crate) available_tables: &'a HashMap<String, String>,
    pub(crate) select_aliases: &'a HashSet<String>,
    pub(crate) cte_names: &'a HashSet<String>,
}

pub(crate) fn validate_table_with_joins<F, G>(
    table_with_joins: &TableWithJoins,
    context: TableValidationContext<'_>,
    issues: &mut Vec<ValidationIssue>,
    validate_expression: &mut F,
    validate_query: &mut G,
) where
    F: FnMut(
        &Expr,
        &HashMap<String, String>,
        &HashSet<String>,
        &SchemaCache,
        &HashSet<String>,
        &mut Vec<ValidationIssue>,
    ),
    G: FnMut(&Query, &SchemaCache, &HashSet<String>, &mut Vec<ValidationIssue>),
{
    validate_table_factor(
        &table_with_joins.relation,
        context.schema,
        context.cte_names,
        issues,
        validate_expression,
        validate_query,
    );

    for join in &table_with_joins.joins {
        validate_table_factor(
            &join.relation,
            context.schema,
            context.cte_names,
            issues,
            validate_expression,
            validate_query,
        );
        validate_join_operator(
            &join.join_operator,
            context.available_tables,
            context.select_aliases,
            context.schema,
            context.cte_names,
            issues,
            validate_expression,
        );
    }
}

pub(crate) fn validate_table_factor<F, G>(
    table_factor: &TableFactor,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
    validate_expression: &mut F,
    validate_query: &mut G,
) where
    F: FnMut(
        &Expr,
        &HashMap<String, String>,
        &HashSet<String>,
        &SchemaCache,
        &HashSet<String>,
        &mut Vec<ValidationIssue>,
    ),
    G: FnMut(&Query, &SchemaCache, &HashSet<String>, &mut Vec<ValidationIssue>),
{
    match table_factor {
        TableFactor::Table { name, .. } => {
            validate_table_reference(name, schema, cte_names, issues);
        }
        TableFactor::Derived { subquery, .. } => {
            validate_query(subquery, schema, cte_names, issues);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            let mut available_tables = HashMap::new();
            schema_validation_lookup::collect_table_aliases(
                table_with_joins,
                &mut available_tables,
                schema,
                cte_names,
            );
            validate_table_with_joins(
                table_with_joins,
                TableValidationContext {
                    schema,
                    available_tables: &available_tables,
                    select_aliases: &HashSet::new(),
                    cte_names,
                },
                issues,
                validate_expression,
                validate_query,
            );
        }
        _ => {}
    }
}

pub(crate) fn validate_table_reference(
    table_name: &ObjectName,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
) {
    if cte_names.contains(
        &schema_validation_lookup::object_name_lookup_key(table_name).to_ascii_lowercase(),
    ) {
        return;
    }
    if schema_validation_lookup::schema_table_name(table_name, schema).is_none() {
        issues.push(ValidationIssue {
            severity: ValidationSeverity::Error,
            message: format!("Table '{}' does not exist in schema", table_name),
            symbol: Some(schema_validation_lookup::object_name_lookup_key(table_name)),
            symbol_role: Some(ValidationSymbolRole::Table),
            qualifier: None,
            line: 0,
            column: 0,
        });
    }
}

pub(crate) fn validate_columns(
    table_name: &ObjectName,
    columns: &[Ident],
    schema: &SchemaCache,
    issues: &mut Vec<ValidationIssue>,
) {
    if let Some(column_names) = schema_validation_lookup::schema_column_names(table_name, schema) {
        for column in columns {
            if !column_names.contains(&column.value.to_ascii_lowercase()) {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Error,
                    message: format!(
                        "Column '{}' does not exist in table '{}'",
                        column.value, table_name
                    ),
                    symbol: Some(column.value.clone()),
                    symbol_role: Some(ValidationSymbolRole::Column),
                    qualifier: None,
                    line: 0,
                    column: 0,
                });
            }
        }
    }
}

fn validate_join_operator<F>(
    join_operator: &JoinOperator,
    available_tables: &HashMap<String, String>,
    select_aliases: &HashSet<String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
    validate_expression: &mut F,
) where
    F: FnMut(
        &Expr,
        &HashMap<String, String>,
        &HashSet<String>,
        &SchemaCache,
        &HashSet<String>,
        &mut Vec<ValidationIssue>,
    ),
{
    match join_operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => validate_join_constraint(
            constraint,
            available_tables,
            select_aliases,
            schema,
            cte_names,
            issues,
            validate_expression,
        ),
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            validate_expression(
                match_condition,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
            );
            validate_join_constraint(
                constraint,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
                validate_expression,
            );
        }
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

fn validate_join_constraint<F>(
    constraint: &JoinConstraint,
    available_tables: &HashMap<String, String>,
    select_aliases: &HashSet<String>,
    schema: &SchemaCache,
    cte_names: &HashSet<String>,
    issues: &mut Vec<ValidationIssue>,
    validate_expression: &mut F,
) where
    F: FnMut(
        &Expr,
        &HashMap<String, String>,
        &HashSet<String>,
        &SchemaCache,
        &HashSet<String>,
        &mut Vec<ValidationIssue>,
    ),
{
    match constraint {
        JoinConstraint::On(expr) => {
            validate_expression(
                expr,
                available_tables,
                select_aliases,
                schema,
                cte_names,
                issues,
            );
        }
        JoinConstraint::Using(columns) => {
            for column in columns {
                validate_join_using_column(column, available_tables, schema, issues);
            }
        }
        JoinConstraint::Natural | JoinConstraint::None => {}
    }
}

fn validate_join_using_column(
    column: &Ident,
    available_tables: &HashMap<String, String>,
    schema: &SchemaCache,
    issues: &mut Vec<ValidationIssue>,
) {
    for table_name in available_tables.values() {
        let Some(columns) =
            schema_validation_lookup::schema_columns_for_table_name(table_name, schema)
        else {
            continue;
        };
        if !columns
            .iter()
            .any(|schema_column| schema_column.name.eq_ignore_ascii_case(&column.value))
        {
            issues.push(ValidationIssue {
                severity: ValidationSeverity::Error,
                message: format!(
                    "Column '{}' does not exist in table '{}'",
                    column.value, table_name
                ),
                symbol: Some(column.value.clone()),
                symbol_role: Some(ValidationSymbolRole::Column),
                qualifier: None,
                line: 0,
                column: 0,
            });
        }
    }
}
