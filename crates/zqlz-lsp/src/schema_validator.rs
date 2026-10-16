use crate::{
    SchemaCache, ValidationIssue, schema_validation_expr, schema_validation_lookup::CteScope,
    schema_validation_table,
};
use sqlparser::ast::{Expr, Ident, ObjectName, TableWithJoins};
use sqlparser::dialect::{Dialect, SQLiteDialect};
use std::collections::{HashMap, HashSet};

pub struct SchemaValidator;

impl SchemaValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate(&self, sql: &str, schema: &SchemaCache) -> Vec<ValidationIssue> {
        self.validate_with_dialect(sql, schema, &SQLiteDialect {})
    }

    pub fn validate_with_dialect(
        &self,
        sql: &str,
        schema: &SchemaCache,
        dialect: &dyn Dialect,
    ) -> Vec<ValidationIssue> {
        // Nothing useful can be inferred against an empty schema — the async
        // refresh hasn't completed yet and every reference would be a false positive.
        if schema.tables.is_empty() {
            return Vec::new();
        }

        let mut issues = Vec::new();

        let statements = match crate::diagnostics_syntax::parse_supported_statements(dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => return issues, // Syntax errors handled by the tree-sitter pass
        };

        for statement in &statements {
            self.validate_statement(statement, schema, &CteScope::new(), &mut issues);
        }

        issues
    }

    pub(super) fn validate_table_with_joins(
        &self,
        table_with_joins: &TableWithJoins,
        schema: &SchemaCache,
        available_tables: &HashMap<String, String>,
        select_aliases: &HashSet<String>,
        cte_names: &CteScope,
        issues: &mut Vec<ValidationIssue>,
    ) {
        schema_validation_table::validate_table_with_joins(
            table_with_joins,
            schema_validation_table::TableValidationContext {
                schema,
                available_tables,
                select_aliases,
                cte_names,
            },
            issues,
            &mut |expr, available_tables, select_aliases, schema, cte_names, issues| {
                self.validate_expression(
                    expr,
                    available_tables,
                    select_aliases,
                    schema,
                    cte_names,
                    issues,
                );
            },
            &mut |query, schema, cte_names, issues| {
                self.validate_query_with_ctes(query, schema, cte_names, issues);
            },
        );
    }

    pub(super) fn validate_table_reference(
        &self,
        table_name: &ObjectName,
        schema: &SchemaCache,
        cte_names: &CteScope,
        issues: &mut Vec<ValidationIssue>,
    ) {
        schema_validation_table::validate_table_reference(table_name, schema, cte_names, issues);
    }

    pub(super) fn validate_columns(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        schema_validation_table::validate_columns(table_name, columns, schema, issues);
    }

    pub(super) fn validate_expression(
        &self,
        expr: &Expr,
        available_tables: &HashMap<String, String>,
        select_aliases: &HashSet<String>,
        schema: &SchemaCache,
        cte_names: &CteScope,
        issues: &mut Vec<ValidationIssue>,
    ) {
        schema_validation_expr::validate_expression(
            expr,
            available_tables,
            select_aliases,
            schema,
            cte_names,
            issues,
            &mut |query, schema, cte_names, issues| {
                self.validate_query_with_ctes(query, schema, cte_names, issues);
            },
        );
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "schema_validator_tests.rs"]
mod tests;
