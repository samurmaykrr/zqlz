use std::collections::{HashMap, HashSet};

use sqlparser::ast::{GroupByExpr, Query, Select, SelectItem, SetExpr, Statement};

use crate::{
    SchemaCache, SchemaValidator, ValidationIssue, ValidationSeverity,
    schema_validation_issue::ValidationSymbolRole, schema_validation_lookup,
};

impl SchemaValidator {
    pub(super) fn validate_query(
        &self,
        query: &Query,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        self.validate_query_with_ctes(query, schema, &HashSet::new(), issues);
    }

    pub(super) fn validate_query_with_ctes(
        &self,
        query: &Query,
        schema: &SchemaCache,
        inherited_ctes: &HashSet<String>,
        issues: &mut Vec<ValidationIssue>,
    ) {
        let mut cte_names = inherited_ctes.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.validate_query_with_ctes(&cte.query, schema, &cte_names, issues);
                cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }

        self.validate_set_expr(query.body.as_ref(), schema, &cte_names, issues);

        if let SetExpr::Select(select) = query.body.as_ref()
            && let Some(order_by) = &query.order_by
        {
            let mut available_tables = HashMap::new();
            schema_validation_lookup::collect_select_table_aliases(
                select,
                &mut available_tables,
                schema,
                &cte_names,
            );
            let select_aliases = schema_validation_lookup::select_aliases(select);
            for order_by_expr in &order_by.exprs {
                self.validate_expression(
                    &order_by_expr.expr,
                    &available_tables,
                    &select_aliases,
                    schema,
                    &cte_names,
                    issues,
                );
            }
        }
    }

    fn validate_set_expr(
        &self,
        set_expr: &SetExpr,
        schema: &SchemaCache,
        cte_names: &HashSet<String>,
        issues: &mut Vec<ValidationIssue>,
    ) {
        match set_expr {
            SetExpr::Select(select) => self.validate_select(select, schema, cte_names, issues),
            SetExpr::Query(query) => {
                self.validate_query_with_ctes(query, schema, cte_names, issues)
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.validate_set_expr(left, schema, cte_names, issues);
                self.validate_set_expr(right, schema, cte_names, issues);
            }
            SetExpr::Insert(statement) | SetExpr::Update(statement) => {
                self.validate_statement(statement, schema, cte_names, issues);
            }
            SetExpr::Values(_) | SetExpr::Table(_) => {}
        }
    }

    fn validate_statement(
        &self,
        statement: &Statement,
        schema: &SchemaCache,
        cte_names: &HashSet<String>,
        issues: &mut Vec<ValidationIssue>,
    ) {
        match statement {
            Statement::Query(query) => {
                self.validate_query_with_ctes(query, schema, cte_names, issues)
            }
            Statement::Insert(insert) => {
                self.validate_table_reference(&insert.table_name, schema, cte_names, issues);
                if !insert.columns.is_empty() {
                    self.validate_columns(&insert.table_name, &insert.columns, schema, issues);
                }
            }
            Statement::Update { table, .. } => {
                self.validate_table_factor(&table.relation, schema, cte_names, issues);
            }
            Statement::Delete(delete) => {
                for table_name in &delete.tables {
                    self.validate_table_reference(table_name, schema, cte_names, issues);
                }
            }
            _ => {}
        }
    }

    fn validate_select(
        &self,
        select: &Select,
        schema: &SchemaCache,
        cte_names: &HashSet<String>,
        issues: &mut Vec<ValidationIssue>,
    ) {
        let mut available_tables = HashMap::new();

        schema_validation_lookup::collect_select_table_aliases(
            select,
            &mut available_tables,
            schema,
            cte_names,
        );

        for table_with_joins in &select.from {
            self.validate_table_with_joins(
                table_with_joins,
                schema,
                &available_tables,
                &HashSet::new(),
                cte_names,
                issues,
            );
        }

        let mut select_aliases: HashSet<String> = schema_validation_lookup::select_aliases(select);
        for projection in &select.projection {
            match projection {
                SelectItem::ExprWithAlias { expr, alias } => {
                    select_aliases.insert(alias.value.to_ascii_lowercase());
                    self.validate_expression(
                        expr,
                        &available_tables,
                        &select_aliases,
                        schema,
                        cte_names,
                        issues,
                    );
                }
                SelectItem::UnnamedExpr(expr) => {
                    self.validate_expression(
                        expr,
                        &available_tables,
                        &select_aliases,
                        schema,
                        cte_names,
                        issues,
                    );
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let table_name = object_name.to_string().to_lowercase();
                    if !available_tables.contains_key(&table_name) {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Error,
                            message: format!("Unknown table or alias: {}", object_name),
                            symbol: Some(schema_validation_lookup::object_name_lookup_key(
                                object_name,
                            )),
                            symbol_role: Some(ValidationSymbolRole::Table),
                            qualifier: None,
                            line: 0,
                            column: 0,
                        });
                    }
                }
                SelectItem::Wildcard(_) => {
                    if available_tables.is_empty() {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            message: "SELECT * with no tables specified".to_string(),
                            symbol: Some("*".to_string()),
                            symbol_role: Some(ValidationSymbolRole::Wildcard),
                            qualifier: None,
                            line: 0,
                            column: 0,
                        });
                    }
                }
            }
        }

        if let GroupByExpr::Expressions(expressions, _) = &select.group_by {
            for expression in expressions {
                self.validate_expression(
                    expression,
                    &available_tables,
                    &select_aliases,
                    schema,
                    cte_names,
                    issues,
                );
            }
        }

        if let Some(selection) = &select.selection {
            self.validate_expression(
                selection,
                &available_tables,
                &select_aliases,
                schema,
                cte_names,
                issues,
            );
        }

        if let Some(having) = &select.having {
            self.validate_expression(
                having,
                &available_tables,
                &select_aliases,
                schema,
                cte_names,
                issues,
            );
        }

        if let Some(qualify) = &select.qualify {
            self.validate_expression(
                qualify,
                &available_tables,
                &select_aliases,
                schema,
                cte_names,
                issues,
            );
        }
    }
}
