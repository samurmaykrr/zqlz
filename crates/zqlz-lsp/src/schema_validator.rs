use crate::SchemaCache;
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
        // Nothing useful can be inferred against an empty schema — the async
        // refresh hasn't completed yet and every reference would be a false positive.
        if schema.tables.is_empty() {
            return Vec::new();
        }

        let mut issues = Vec::new();

        let statements = match Parser::parse_sql(&self.dialect, sql) {
            Ok(stmts) => stmts,
            Err(_) => return issues, // Syntax errors handled by the tree-sitter pass
        };

        for statement in statements {
            match statement {
                Statement::Query(query) => {
                    self.validate_query(&query, schema, &mut issues);
                }
                Statement::Insert(insert) => {
                    let table_name = &insert.table_name;
                    self.validate_table_reference(table_name, schema, &mut issues);
                    if !insert.columns.is_empty() {
                        self.validate_columns(table_name, &insert.columns, schema, &mut issues);
                    }
                }
                Statement::Update { table, .. } => {
                    self.validate_table_factor(&table.relation, schema, &mut issues);
                }
                Statement::Delete(delete) => {
                    for table_name in &delete.tables {
                        self.validate_table_reference(table_name, schema, &mut issues);
                    }
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
            self.collect_table_aliases(table_with_joins, &mut available_tables, schema);
            self.validate_table_with_joins(table_with_joins, schema, issues);
        }

        // Collect aliases defined in the SELECT list (e.g. `content_id AS ci`).
        // These are valid identifiers in WHERE/HAVING on SQLite and should never
        // be flagged as unknown columns.
        let mut select_aliases: HashSet<String> = HashSet::new();
        for projection in &select.projection {
            match projection {
                SelectItem::ExprWithAlias { expr, alias } => {
                    select_aliases.insert(alias.value.to_lowercase());
                    self.validate_expression(
                        expr,
                        &available_tables,
                        &select_aliases,
                        schema,
                        issues,
                    );
                }
                SelectItem::UnnamedExpr(expr) => {
                    self.validate_expression(
                        expr,
                        &available_tables,
                        &select_aliases,
                        schema,
                        issues,
                    );
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let table_name = object_name.to_string().to_lowercase();
                    if !available_tables.contains_key(&table_name) {
                        issues.push(ValidationIssue {
                            severity: ValidationSeverity::Error,
                            message: format!("Unknown table or alias: {}", object_name),
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
                            line: 0,
                            column: 0,
                        });
                    }
                }
            }
        }

        if let Some(selection) = &select.selection {
            // Pass the full alias set so WHERE expressions like `ci = 'x'`
            // (where `ci` is a SELECT alias) are not flagged.
            self.validate_expression(
                selection,
                &available_tables,
                &select_aliases,
                schema,
                issues,
            );
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
        // SQL identifiers are case-insensitive; normalize before lookup so that
        // `web_html`, `Web_Html`, and `WEB_HTML` all match the same schema entry.
        let name_lower = table_name.to_string().to_lowercase();

        let exists = schema.tables.keys().any(|k| k.to_lowercase() == name_lower);

        if !exists {
            issues.push(ValidationIssue {
                severity: ValidationSeverity::Error,
                message: format!("Table '{}' does not exist in schema", table_name),
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
        let table_lower = table_name.to_string().to_lowercase();

        // Find the canonical table entry using a case-insensitive key match.
        let column_names: Option<HashSet<String>> = schema
            .columns_by_table
            .iter()
            .find(|(k, _)| k.to_lowercase() == table_lower)
            .map(|(_, cols)| cols.iter().map(|c| c.name.to_lowercase()).collect());

        if let Some(column_names) = column_names {
            for column in columns {
                let col_lower = column.value.to_lowercase();
                if !column_names.contains(&col_lower) {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Error,
                        message: format!(
                            "Column '{}' does not exist in table '{}'",
                            column.value, table_name
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
        select_aliases: &HashSet<String>,
        schema: &SchemaCache,
        issues: &mut Vec<ValidationIssue>,
    ) {
        match expr {
            Expr::Identifier(ident) => {
                let col_lower = ident.value.to_lowercase();

                // Skip identifiers that are SELECT-list aliases — they are perfectly
                // valid in WHERE/HAVING in SQLite and most SQL dialects.
                if select_aliases.contains(&col_lower) {
                    return;
                }

                let mut found = false;
                for actual_table_name in available_tables.values() {
                    // Case-insensitive column lookup.
                    let canonical = schema
                        .columns_by_table
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == actual_table_name.to_lowercase())
                        .map(|(_, v)| v);

                    if let Some(columns) = canonical {
                        if columns.iter().any(|c| c.name.to_lowercase() == col_lower) {
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
                            ident.value
                        ),
                        line: 0,
                        column: 0,
                    });
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let table_or_alias = parts[0].value.to_lowercase();
                let col_lower = parts[1].value.to_lowercase();

                if let Some(actual_table_name) = available_tables.get(&table_or_alias) {
                    let canonical = schema
                        .columns_by_table
                        .iter()
                        .find(|(k, _)| k.to_lowercase() == actual_table_name.to_lowercase())
                        .map(|(_, v)| v);

                    if let Some(columns) = canonical {
                        if !columns.iter().any(|c| c.name.to_lowercase() == col_lower) {
                            issues.push(ValidationIssue {
                                severity: ValidationSeverity::Error,
                                message: format!(
                                    "Column '{}' does not exist in table '{}'",
                                    parts[1].value, actual_table_name
                                ),
                                line: 0,
                                column: 0,
                            });
                        }
                    }
                } else {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Warning,
                        message: format!("Unknown table or alias: {}", parts[0].value),
                        line: 0,
                        column: 0,
                    });
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expression(left, available_tables, select_aliases, schema, issues);
                self.validate_expression(right, available_tables, select_aliases, schema, issues);
            }
            Expr::UnaryOp { expr, .. } => {
                self.validate_expression(expr, available_tables, select_aliases, schema, issues);
            }
            Expr::Nested(expr) => {
                self.validate_expression(expr, available_tables, select_aliases, schema, issues);
            }
            Expr::Function(func) => {
                if let sqlparser::ast::FunctionArguments::List(ref arg_list) = func.args {
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = arg
                        {
                            self.validate_expression(
                                e,
                                available_tables,
                                select_aliases,
                                schema,
                                issues,
                            );
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                self.validate_expression(expr, available_tables, select_aliases, schema, issues);
                for item in list {
                    self.validate_expression(
                        item,
                        available_tables,
                        select_aliases,
                        schema,
                        issues,
                    );
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expression(expr, available_tables, select_aliases, schema, issues);
                self.validate_expression(low, available_tables, select_aliases, schema, issues);
                self.validate_expression(high, available_tables, select_aliases, schema, issues);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.validate_expression(op, available_tables, select_aliases, schema, issues);
                }
                for cond in conditions {
                    self.validate_expression(
                        cond,
                        available_tables,
                        select_aliases,
                        schema,
                        issues,
                    );
                }
                for result in results {
                    self.validate_expression(
                        result,
                        available_tables,
                        select_aliases,
                        schema,
                        issues,
                    );
                }
                if let Some(else_expr) = else_result {
                    self.validate_expression(
                        else_expr,
                        available_tables,
                        select_aliases,
                        schema,
                        issues,
                    );
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
                // Use case-insensitive lookup to find the canonical schema name.
                let canonical = schema
                    .tables
                    .keys()
                    .find(|k| k.to_lowercase() == table_name.to_lowercase())
                    .cloned()
                    .unwrap_or_else(|| table_name.clone());

                let key = table_name.to_lowercase();
                aliases.insert(key.clone(), canonical.clone());

                if let Some(alias_obj) = alias {
                    aliases.insert(alias_obj.name.value.to_lowercase(), canonical);
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(alias_obj) = alias {
                    let alias_name = alias_obj.name.value.to_lowercase();
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
        let issues = validator.validate("SELECT id, name FROM users WHERE id = 1", &schema);
        assert!(issues.is_empty());
    }

    #[test]
    fn test_validate_unknown_table() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate("SELECT * FROM products", &schema);
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("does not exist")));
    }

    #[test]
    fn test_validate_unknown_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate("SELECT id, age FROM users", &schema);
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("age")));
    }

    #[test]
    fn test_validate_join_with_alias() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
            &schema,
        );
        assert!(issues.is_empty());
    }

    #[test]
    fn test_validate_invalid_qualified_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate("SELECT u.invalid_column FROM users u", &schema);
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("invalid_column")));
    }

    #[test]
    fn test_validate_insert_unknown_column() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate(
            "INSERT INTO users (id, name, age) VALUES (1, 'test', 30)",
            &schema,
        );
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| i.message.contains("age")));
    }

    #[test]
    fn test_validate_complex_expression() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        let issues = validator.validate(
            "SELECT name FROM users WHERE id IN (1, 2, 3) AND name LIKE '%test%'",
            &schema,
        );
        assert!(issues.is_empty());
    }

    #[test]
    fn test_empty_schema_skips_validation() {
        let validator = SchemaValidator::new();
        let schema = SchemaCache::default(); // empty
                                             // Should produce no issues, not a flood of "table not found" errors
        let issues = validator.validate("SELECT * FROM any_table", &schema);
        assert!(issues.is_empty());
    }

    #[test]
    fn test_select_alias_in_where_is_not_flagged() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        // `u` is a SELECT alias; it must not appear as an unknown column warning
        let issues = validator.validate("SELECT id AS u FROM users WHERE u = 1", &schema);
        assert!(
            issues.iter().all(|i| !i.message.contains("'u'")),
            "alias 'u' should not generate a warning: {:?}",
            issues
        );
    }

    #[test]
    fn test_case_insensitive_table_lookup() {
        let validator = SchemaValidator::new();
        let schema = create_test_schema();
        // Schema stores "users" lowercase; query uses mixed case
        let issues = validator.validate("SELECT id FROM Users", &schema);
        assert!(
            issues.iter().all(|i| !i.message.contains("does not exist")),
            "case-insensitive lookup should find 'users': {:?}",
            issues
        );
    }
}
