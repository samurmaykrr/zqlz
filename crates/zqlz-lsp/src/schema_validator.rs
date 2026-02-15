use crate::SchemaCache;
use anyhow::Result;
use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: ValidationSeverity,
    pub message: String,
    pub line: usize,
    pub column: usize,
}

pub struct SchemaValidator {
    dialect: SQLiteDialect,
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self {
            dialect: SQLiteDialect {},
        }
    }

    pub fn validate(&self, sql: &str, schema: &SchemaCache) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        let parsed = Parser::parse_sql(&self.dialect, sql);
        let statements = match parsed {
            Ok(stmts) => stmts,
            Err(_) => return issues, // Syntax errors handled by diagnostics
        };

        for statement in statements {
            match statement {
                Statement::Query(query) => {
                    self.validate_query(&query, schema, &mut issues);
                }
                Statement::Insert(insert) => {
                    // Insert.table_name is ObjectName (not Option)
                    let table_name = &insert.table_name;
                    self.validate_table_reference(table_name, schema, &mut issues);
                    // columns is Vec<Ident>, not Option<Vec<Ident>>
                    if !insert.columns.is_empty() {
                        self.validate_columns(table_name, &insert.columns, schema, &mut issues);
                    }
                }
                Statement::Update { table, .. } => {
                    self.validate_table_factor(&table.relation, schema, &mut issues);
                }
                Statement::Delete(delete) => {
                    // Delete.tables is Vec<ObjectName> (table names to delete from)
                    for table_name in &delete.tables {
                        self.validate_table_reference(table_name, schema, &mut issues);
                    }
                    // Validate using clause if present
                    if let Some(using_tables) = &delete.using {
                        for table_with_joins in using_tables {
                            self.validate_table_with_joins(table_with_joins, schema, &mut issues);
                        }
                    }
                }
                _ => {}
            }
        }

        issues
    }

    fn validate_query(
        &self,
        query: &Query,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        if let SetExpr::Select(select) = query.body.as_ref() {
            self.validate_select(select, schema, issues);
        }
    }

    fn validate_select(
        &self,
        select: &Select,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        let mut available_tables = HashMap::new();

        for table_with_joins in &select.from {
            self.collect_table_aliases(&table_with_joins, &mut available_tables, schema);
            self.validate_table_with_joins(table_with_joins, schema, issues);
        }

        for projection in &select.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.validate_expression(expr, &available_tables, schema, issues);
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let table_name = object_name.to_string();
                    if !available_tables.contains_key(&table_name) {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Error,
                            message: format!("Unknown table or alias: {}", table_name),
                            line: 0,
                            column: 0,
                        });
                    }
                }
                SelectItem::Wildcard(_) => {
                    // * is always valid if we have tables
                    if available_tables.is_empty() {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            message: "SELECT * with no tables specified".to_string(),
                            line: 0,
                            column: 0,
                        });
                    }
                }
            }
        }

        if let Some(selection) = &select.selection {
            self.validate_expression(selection, &available_tables, schema, issues);
        }
    }

    fn validate_table_with_joins(
        &self,
        table_with_joins: &TableWithJoins,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        self.validate_table_factor(&table_with_joins.relation, schema, issues);

        for join in &table_with_joins.joins {
            self.validate_table_factor(&join.relation, schema, issues);
        }
    }

    fn validate_table_factor(
        &self,
        table_factor: &TableFactor,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                self.validate_table_reference(name, schema, issues);
            }
            TableFactor::Derived { subquery, .. } => {
                self.validate_query(subquery, schema, issues);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.validate_table_with_joins(table_with_joins, schema, issues);
            }
            _ => {}
        }
    }

    fn validate_table_reference(
        &self,
        table_name: &ObjectName,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        let name = table_name.to_string();

        if !schema.tables.contains_key(&name) {
            issues.push(ValidationIssue {
                severity: ValidationSeverity::Error,
                message: format!("Table '{}' does not exist in schema", name),
                line: 0,
                column: 0,
            });
        }
    }

    fn validate_columns(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        let table_str = table_name.to_string();

        if let Some(columns_list) = schema.columns_by_table.get(&table_str) {
            let column_names: HashSet<_> = columns_list.iter().map(|c| c.name.as_str()).collect();

            for column in columns {
                let col_name = column.value.as_str();
                if !column_names.contains(col_name) {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Error,
                        message: format!(
                            "Column '{}' does not exist in table '{}'",
                            col_name, table_str
                        ),
                        line: 0,
                        column: 0,
                    });
                }
            }
        }
    }

    fn validate_expression(
        &self,
        expr: &Expr,
        available_tables: &HashMap<String, String>,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        match expr {
            Expr::Identifier(ident) => {
                // Unqualified column reference - check if it exists in any available table
                let column_name = ident.value.as_str();
                let mut found = false;

                for actual_table_name in available_tables.values() {
                    if let Some(columns) = schema.columns_by_table.get(actual_table_name) {
                        if columns.iter().any(|c| c.name == column_name) {
                            found = true;
                            break;
                        }
                    }
                }

                if !found && !available_tables.is_empty() {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Warning,
                        message: format!(
                            "Column '{}' may not exist in available tables",
                            column_name
                        ),
                        line: 0,
                        column: 0,
                    });
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let table_or_alias = parts[0].value.as_str();
                    let column_name = parts[1].value.as_str();

                    if let Some(actual_table_name) = available_tables.get(table_or_alias) {
                        if let Some(columns) = schema.columns_by_table.get(actual_table_name) {
                            if !columns.iter().any(|c| c.name == column_name) {
                                issues.push(ValidationIssue {
                                    severity: ValidationSeverity::Error,
                                    message: format!(
                                        "Column '{}' does not exist in table '{}'",
                                        column_name, actual_table_name
                                    ),
                                    line: 0,
                                    column: 0,
                                });
                            }
                        }
                    } else {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Warning,
                            message: format!("Unknown table or alias: {}", table_or_alias),
                            line: 0,
                            column: 0,
                        });
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expression(left, available_tables, schema, issues);
                self.validate_expression(right, available_tables, schema, issues);
            }
            Expr::UnaryOp { expr, .. } => {
                self.validate_expression(expr, available_tables, schema, issues);
            }
            Expr::Nested(expr) => {
                self.validate_expression(expr, available_tables, schema, issues);
            }
            Expr::Function(func) => {
                // FunctionArguments can be None, List, or Subquery
                if let sqlparser::ast::FunctionArguments::List(ref arg_list) = func.args {
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = arg
                        {
                            self.validate_expression(e, available_tables, schema, issues);
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                self.validate_expression(expr, available_tables, schema, issues);
                for item in list {
                    self.validate_expression(item, available_tables, schema, issues);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expression(expr, available_tables, schema, issues);
                self.validate_expression(low, available_tables, schema, issues);
                self.validate_expression(high, available_tables, schema, issues);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.validate_expression(op, available_tables, schema, issues);
                }
                for cond in conditions {
                    self.validate_expression(cond, available_tables, schema, issues);
                }
                for result in results {
                    self.validate_expression(result, available_tables, schema, issues);
                }
                if let Some(else_expr) = else_result {
                    self.validate_expression(else_expr, available_tables, schema, issues);
                }
            }
            Expr::Subquery(query) => {
                self.validate_query(query, schema, issues);
            }
            _ => {}
        }
    }

    fn collect_table_aliases(
        &self,
        table_with_joins: &TableWithJoins,
        aliases: &mut HashMap<String, String>,
        schema: &SchemaCache,
    ) {
        self.extract_table_alias(&table_with_joins.relation, aliases, schema);

        for join in &table_with_joins.joins {
            self.extract_table_alias(&join.relation, aliases, schema);
        }
    }

    fn extract_table_alias(
        &self,
        table_factor: &TableFactor,
        aliases: &mut HashMap<String, String>,
        schema: &SchemaCache,
    ) {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                if schema.tables.contains_key(&table_name) {
                    if let Some(alias_obj) = alias {
                        let alias_name = alias_obj.name.value.to_string();
                        aliases.insert(alias_name, table_name.clone());
                    }
                    aliases.insert(table_name.clone(), table_name);
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(alias_obj) = alias {
                    let alias_name = alias_obj.name.value.to_string();
                    aliases.insert(alias_name.clone(), alias_name);
                }
            }
            _ => {}
        }
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnInfo;

    fn create_test_schema() -> SchemaCache {
        let mut schema = SchemaCache::default();

        // Add users table
        schema.tables.insert(
            "users".to_string(),
            crate::TableInfo {
                name: "users".to_string(),
                schema: None,
                comment: None,
                row_count: None,
            },
        );

        schema.columns_by_table.insert(
            "users".to_string(),
            vec![
                ColumnInfo {
                    table_name: "users".to_string(),
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                    is_foreign_key: false,
                    comment: None,
                },
                ColumnInfo {
                    table_name: "users".to_string(),
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: false,
                    is_foreign_key: false,
                    comment: None,
                },
                ColumnInfo {
                    table_name: "users".to_string(),
                    name: "email".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                    is_foreign_key: false,
                    comment: None,
                },
            ],
        );

        // Add orders table
        schema.tables.insert(
            "orders".to_string(),
            crate::TableInfo {
                name: "orders".to_string(),
                schema: None,
                comment: None,
                row_count: None,
            },
        );

        schema.columns_by_table.insert(
            "orders".to_string(),
            vec![
                ColumnInfo {
                    table_name: "orders".to_string(),
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                    is_foreign_key: false,
                    comment: None,
                },
                ColumnInfo {
                    table_name: "orders".to_string(),
                    name: "user_id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: false,
                    is_foreign_key: true,
                    comment: None,
                },
                ColumnInfo {
                    table_name: "orders".to_string(),
                    name: "total".to_string(),
                    data_type: "REAL".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: false,
                    is_foreign_key: false,
                    comment: None,
                },
            ],
        );

        schema
    }

    #[test]
    fn test_validate_valid_query() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT id, name FROM users WHERE id = 1";
        let issues = validator.validate(sql, &schema);

        assert!(issues.is_empty());
    }

    #[test]
    fn test_validate_unknown_table() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT * FROM products";
        let issues = validator.validate(sql, &schema);

        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("does not exist")));
    }

    #[test]
    fn test_validate_unknown_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT id, age FROM users";
        let issues = validator.validate(sql, &schema);

        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("age")));
    }

    #[test]
    fn test_validate_join_with_alias() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id";
        let issues = validator.validate(sql, &schema);

        assert!(issues.is_empty());
    }

    #[test]
    fn test_validate_invalid_qualified_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT u.invalid_column FROM users u";
        let issues = validator.validate(sql, &schema);

        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("invalid_column")));
    }

    #[test]
    fn test_validate_insert_unknown_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "INSERT INTO users (id, name, age) VALUES (1, 'test', 30)";
        let issues = validator.validate(sql, &schema);

        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("age")));
    }

    #[test]
    fn test_validate_complex_expression() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();

        let sql = "SELECT name FROM users WHERE id IN (1, 2, 3) AND name LIKE '%test%'";
        let issues = validator.validate(sql, &schema);

        assert!(issues.is_empty());
    }
}
