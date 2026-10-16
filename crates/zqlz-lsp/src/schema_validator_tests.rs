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
            table_type: zqlz_core::TableType::Table,
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
            table_type: zqlz_core::TableType::Table,
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
fn test_validate_column_not_flagged_while_table_columns_still_loading() {
    let validator = SchemaValidator::new();
    let mut schema = create_test_schema();

    // `pending` is a real, known table, but its `columns_by_table` entry
    // hasn't finished the async lazy-load yet — this must not be treated
    // the same as a genuinely nonexistent column.
    schema.tables.insert(
        "pending".to_string(),
        crate::TableInfo {
            name: "pending".to_string(),
            schema: None,
            comment: None,
            row_count: None,
            table_type: zqlz_core::TableType::Table,
        },
    );

    let issues = validator.validate("SELECT city FROM pending", &schema);
    assert!(
        issues.is_empty(),
        "column should not be flagged while columns are still loading: {issues:?}"
    );
}

#[test]
fn test_validate_unknown_column_still_flagged_for_genuinely_unknown_table() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();

    // `ghost` isn't in `schema.tables` at all, so there's nothing to wait
    // on — the column reference should still be flagged alongside the
    // separate "unknown table" diagnostic.
    let issues = validator.validate("SELECT ghost_column FROM ghost", &schema);
    assert!(
        issues.iter().any(|i| i.message.contains("ghost_column")),
        "column referencing a nonexistent table should still be flagged: {issues:?}"
    );
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
fn test_validate_join_on_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.missing_id = o.user_id",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_id")),
        "JOIN ON expression should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_join_using_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "SELECT * FROM users JOIN orders USING (unknown_id)",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("unknown_id")),
        "JOIN USING columns should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_group_by_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate("SELECT COUNT(*) FROM users GROUP BY missing_group", &schema);

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_group")),
        "GROUP BY expression should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_having_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "SELECT id, COUNT(*) FROM users GROUP BY id HAVING missing_having > 0",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_having")),
        "HAVING expression should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_order_by_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate("SELECT id FROM users ORDER BY missing_order", &schema);

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_order")),
        "ORDER BY expression should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_union_right_side_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "SELECT id FROM users UNION SELECT missing_union FROM orders",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_union")),
        "right side of UNION should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_nested_set_query_unknown_column() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "(SELECT id FROM users) INTERSECT (SELECT missing_intersect FROM orders)",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_intersect")),
        "nested set query branches should be schema validated: {:?}",
        issues
    );
}

#[test]
fn test_validate_cte_reference_is_in_scope() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "WITH active_users AS (SELECT id FROM users) SELECT * FROM active_users",
        &schema,
    );

    assert!(
        issues
            .iter()
            .all(|issue| !issue.message.contains("active_users")),
        "CTE table references should be resolved from parser scope: {:?}",
        issues
    );
}

#[test]
fn test_validate_cte_body_still_checks_schema_columns() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "WITH bad_users AS (SELECT missing_cte_column FROM users) SELECT * FROM bad_users",
        &schema,
    );

    assert!(
        issues
            .iter()
            .any(|issue| issue.message.contains("missing_cte_column")),
        "CTE query body should still be schema validated: {:?}",
        issues
    );
    assert!(
        issues
            .iter()
            .all(|issue| !issue.message.contains("bad_users")),
        "CTE name should not be treated as missing schema table: {:?}",
        issues
    );
}

#[test]
fn test_validate_cte_visible_inside_subquery_expression() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
            "WITH active_users AS (SELECT id FROM users) SELECT id FROM users WHERE id IN (SELECT id FROM active_users)",
            &schema,
        );

    assert!(
        issues
            .iter()
            .all(|issue| !issue.message.contains("active_users")),
        "CTE scope should flow into expression subqueries: {:?}",
        issues
    );
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

#[test]
fn test_schema_qualified_table_lookup_uses_object_name_leaf() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate("SELECT id FROM main.users", &schema);
    assert!(
        issues
            .iter()
            .all(|issue| !issue.message.contains("does not exist")),
        "schema-qualified table should resolve against schema table name: {:?}",
        issues
    );
}

#[test]
fn test_schema_qualified_cache_key_resolves_against_qualified_and_bare_queries() {
    // Postgres populates the schema cache with schema-qualified keys (e.g.
    // "public.users") when it was fetched across all schemas — no unqualified
    // "users" key ever exists in that cache, unlike `create_test_schema`'s
    // convention used elsewhere in this file.
    let validator = SchemaValidator::new();
    let mut schema = SchemaCache::default();
    schema.tables.insert(
        "public.users".to_string(),
        crate::TableInfo {
            name: "public.users".to_string(),
            schema: Some("public".to_string()),
            comment: None,
            row_count: None,
            table_type: zqlz_core::TableType::Table,
        },
    );
    schema.columns_by_table.insert(
        "public.users".to_string(),
        vec![ColumnInfo {
            table_name: "public.users".to_string(),
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            default_value: None,
            is_primary_key: true,
            is_foreign_key: false,
            comment: None,
        }],
    );

    for sql in ["SELECT id FROM public.users", "SELECT id FROM users"] {
        let issues = validator.validate(sql, &schema);
        assert!(
            issues
                .iter()
                .all(|issue| !issue.message.contains("does not exist")),
            "schema-qualified cache key should resolve for `{sql}`: {:?}",
            issues
        );
    }
}

#[test]
fn test_quoted_table_lookup_uses_identifier_value() {
    let validator = SchemaValidator::new();
    let mut schema = create_test_schema();
    schema.tables.insert(
        "Order Details".to_string(),
        crate::TableInfo {
            name: "Order Details".to_string(),
            schema: None,
            comment: None,
            row_count: None,
            table_type: zqlz_core::TableType::Table,
        },
    );
    schema.columns_by_table.insert(
        "Order Details".to_string(),
        vec![ColumnInfo {
            table_name: "Order Details".to_string(),
            name: "Line Total".to_string(),
            data_type: "REAL".to_string(),
            nullable: false,
            default_value: None,
            is_primary_key: false,
            is_foreign_key: false,
            comment: None,
        }],
    );

    let issues = validator.validate(
        r#"SELECT "Line Total" FROM "Order Details" WHERE "Line Total" > 0"#,
        &schema,
    );

    assert!(
        issues.is_empty(),
        "quoted identifiers should resolve by AST identifier value, got: {:?}",
        issues
    );
}

fn add_table(schema: &mut SchemaCache, schema_name: &str, table_name: &str, columns: &[&str]) {
    schema.tables.insert(
        table_name.to_string(),
        crate::TableInfo {
            name: table_name.to_string(),
            schema: Some(schema_name.to_string()),
            comment: None,
            row_count: None,
            table_type: zqlz_core::TableType::Table,
        },
    );
    schema.columns_by_table.insert(
        table_name.to_string(),
        columns
            .iter()
            .map(|column| ColumnInfo {
                table_name: table_name.to_string(),
                name: column.to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            })
            .collect(),
    );
}

fn create_postgres_billing_schema() -> SchemaCache {
    let mut schema = SchemaCache::default();
    add_table(
        &mut schema,
        "billing",
        "invoices",
        &["id", "organization_id", "period", "total", "status"],
    );
    add_table(&mut schema, "app", "organizations", &["id", "name", "plan"]);
    add_table(&mut schema, "analytics", "events", &["id", "name"]);
    schema
}

#[test]
fn test_cte_output_columns_and_window_names_are_not_flagged() {
    let validator = SchemaValidator::new();
    let schema = create_postgres_billing_schema();
    let issues = validator.validate_with_dialect(
        "WITH monthly AS (
  SELECT o.plan,
         date_trunc('month', lower(i.period))::date AS month,
         sum(i.total) / 100.0 AS revenue_usd
  FROM billing.invoices i
  JOIN app.organizations o ON o.id = i.organization_id
  WHERE i.status = 'paid'
  GROUP BY 1, 2
)
SELECT plan, month, revenue_usd,
       round(100.0 * revenue_usd / lag(revenue_usd) OVER w - 100, 1) AS mom_growth_pct,
       sum(revenue_usd) OVER (PARTITION BY plan ORDER BY month) AS running_total
FROM monthly
WINDOW w AS (PARTITION BY plan ORDER BY month)
ORDER BY plan, month;",
        &schema,
        &sqlparser::dialect::PostgreSqlDialect {},
    );
    assert!(issues.is_empty(), "expected no issues, got: {:?}", issues);
}

#[test]
fn test_cte_explicit_column_list_resolves_and_unknown_cte_column_is_flagged() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "WITH totals(owner, amount) AS (SELECT user_id, total FROM orders) \
         SELECT owner, amount, missing_total FROM totals ORDER BY amount",
        &schema,
    );
    assert_eq!(issues.len(), 1, "got: {:?}", issues);
    assert!(issues[0].message.contains("missing_total"));
}

#[test]
fn test_derived_table_output_columns_resolve() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "SELECT user_count FROM (SELECT count(*) AS user_count FROM users) counts",
        &schema,
    );
    assert!(issues.is_empty(), "got: {:?}", issues);
}

#[test]
fn test_wildcard_cte_columns_are_not_flagged() {
    let validator = SchemaValidator::new();
    let schema = create_test_schema();
    let issues = validator.validate(
        "WITH everyone AS (SELECT * FROM users) SELECT name, email FROM everyone",
        &schema,
    );
    assert!(issues.is_empty(), "got: {:?}", issues);
}

#[test]
fn test_misspelled_tables_still_reported() {
    let validator = SchemaValidator::new();
    let schema = create_postgres_billing_schema();
    let issues = validator.validate_with_dialect(
        "SELECT e.name FROM analytics.event e",
        &schema,
        &sqlparser::dialect::PostgreSqlDialect {},
    );
    assert!(
        issues
            .iter()
            .any(|issue| issue.message == "Table 'analytics.event' does not exist in schema"),
        "got: {:?}",
        issues
    );

    let issues = validator.validate("SELECT * FROM custmer", &create_test_schema());
    assert!(
        issues
            .iter()
            .any(|issue| issue.message == "Table 'custmer' does not exist in schema"),
        "got: {:?}",
        issues
    );
}
