//! Tests for table.column qualified completions

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_qualified_completion_after_dot() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT users.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should ONLY show columns from users table
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest user_id. Got: {:?}",
        labels
    );
    assert!(
        labels.contains(&"username".to_string()),
        "Should suggest username. Got: {:?}",
        labels
    );
    assert!(
        labels.contains(&"email".to_string()),
        "Should suggest email. Got: {:?}",
        labels
    );

    // Should NOT show keywords or columns from other tables
    assert!(
        !labels.contains(&"SELECT".to_string()),
        "Should NOT suggest keywords after dot"
    );
    assert!(
        !labels.contains(&"log_id".to_string()),
        "Should NOT suggest columns from other tables"
    );
}

#[test]
fn test_qualified_completion_with_filter() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT users.user");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should show only columns starting with "user"
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest user_id"
    );
    assert!(
        labels.contains(&"username".to_string()),
        "Should suggest username"
    );

    // Should NOT show email (doesn't start with "user")
    assert!(
        !labels.contains(&"email".to_string()),
        "Should NOT suggest email"
    );
}

#[test]
fn test_qualified_completion_with_alias_and_partial_column() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users us WHERE u.us");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest aliased columns while typing qualified reference. Got: {:?}",
        labels
    );
    assert!(
        labels.contains(&"username".to_string()),
        "Should suggest username for aliased partial qualified reference. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"FROM".to_string()),
        "Should not fall back to keyword completions for qualified alias references"
    );
}

#[test]
fn test_qualified_completion_with_alias() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u WHERE u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should resolve alias 'u' to 'users' table
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest columns from users table via alias. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_does_not_resolve_alias_prefix() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT us. FROM users user_stats JOIN audit_log us_archive ON true");
    let offset = text.to_string().find("us.").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"user_id".to_string()),
        "Qualifier should not resolve to alias by prefix. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"username".to_string()),
        "Qualifier should not resolve to alias by prefix. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_ignores_alias_like_text_in_comments() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("-- FROM users u\nSELECT u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"user_id".to_string()),
        "Comment text should not create table aliases. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"username".to_string()),
        "Comment text should not create table aliases. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_ignores_alias_like_text_in_strings() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT 'FROM users u' AS note, u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"user_id".to_string()),
        "String text should not create table aliases. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"username".to_string()),
        "String text should not create table aliases. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_ignores_cte_like_text_in_strings() {
    let mut lsp = create_test_lsp();
    let text =
        Rope::from("SELECT 'WITH fake AS (SELECT user_id, username FROM users)' AS note, fake.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"user_id".to_string()),
        "String text should not create CTE columns. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"username".to_string()),
        "String text should not create CTE columns. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_ignores_derived_table_like_text_in_strings() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT '(SELECT user_id, username FROM users) fake' AS note, fake.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"user_id".to_string()),
        "String text should not create derived-table columns. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"username".to_string()),
        "String text should not create derived-table columns. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_multiple_tables() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u, audit_log a WHERE u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should only show columns from users (referenced by 'u')
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest user_id from users"
    );
    assert!(
        !labels.contains(&"log_id".to_string()),
        "Should NOT suggest log_id from audit_log"
    );
}

#[test]
fn test_qualified_completion_after_join() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u JOIN audit_log a ON a.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should show columns from audit_log (alias 'a')
    assert!(
        labels.contains(&"log_id".to_string()),
        "Should suggest log_id. Got: {:?}",
        labels
    );
    assert!(
        labels.contains(&"log_timestamp".to_string()),
        "Should suggest log_timestamp"
    );
    assert!(
        labels.contains(&"action".to_string()),
        "Should suggest action"
    );
}

#[test]
fn test_qualified_completion_in_select_list() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT users.user_id, audit_log.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should show columns from audit_log
    assert!(
        labels.contains(&"log_id".to_string()),
        "Should suggest audit_log columns. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_unknown_table() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT nonexistent.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Unknown tables produce a single non-inserting explanatory item instead
    // of real column suggestions (or a silent empty menu).
    assert_eq!(
        completions.len(),
        1,
        "Should only show the explanatory item for unknown table. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        completions[0].label.contains("Unknown table or alias"),
        "Got: {}",
        completions[0].label
    );
    assert_eq!(completions[0].insert_text.as_deref(), Some(""));
}

#[test]
fn test_qualified_completion_case_insensitive() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT USERS.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should work with uppercase table name
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should handle uppercase table name. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_alias_case_insensitive() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users U WHERE U.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    assert!(
        labels.contains(&"user_id".to_string()),
        "Should resolve uppercase alias case-insensitively. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"SELECT".to_string()),
        "Qualified alias completion should not fall back to SQL keywords"
    );
}

#[test]
fn test_qualified_completion_nested_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (SELECT users.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should work in nested queries
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should work in nested query. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_with_schema() {
    let mut lsp = create_test_lsp();
    // Assuming schema.table.column pattern
    let text = Rope::from("SELECT main.users.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // May or may not support schema prefix depending on implementation
    println!("Completions with schema prefix: {:?}", completions);
}

#[test]
fn test_multiple_qualified_references() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT u.user_id, a.log_id FROM users u, audit_log a WHERE u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should show users columns
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should suggest users columns"
    );
}

#[test]
fn test_qualified_completion_in_where_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u WHERE u.user");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should filter columns starting with "user"
    assert!(
        labels.contains(&"user_id".to_string()),
        "Should filter and suggest user_id"
    );
}

#[test]
fn test_qualified_completion_in_order_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u ORDER BY u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should show users columns in ORDER BY
    assert!(
        !completions.is_empty(),
        "Should suggest columns in ORDER BY clause"
    );
}

#[test]
fn test_qualified_completion_in_group_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT u.username, COUNT(*) FROM users u GROUP BY u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should show users columns in GROUP BY
    assert!(
        !completions.is_empty(),
        "Should suggest columns in GROUP BY clause"
    );
}

#[test]
fn test_qualified_completion_alias_used_before_from_with_dangling_dot() {
    // Regression: `SELECT ws. FROM users as ws` — the qualifier appears before
    // the FROM clause that defines the alias, and the dangling dot makes the
    // statement unparseable. Columns of the aliased table must still complete.
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT ws. FROM users as ws");
    let offset = text.to_string().find("ws.").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        labels.contains(&"user_id".to_string()),
        "Should resolve alias defined with AS after a dangling dot. Got: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"SELECT".to_string()),
        "Should not fall back to keywords after a qualified dot. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_alias_dangling_dot_postgres_dialect() {
    // Same regression but under the PostgreSQL dialect, matching the live app.
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT ws. FROM users as ws");
    let offset = text.to_string().find("ws.").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        labels.contains(&"user_id".to_string()),
        "PostgreSQL dialect should resolve dangling-dot alias. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_manual_trigger_dangling_dot() {
    // Right-click → Complete uses the manual (INVOKED) path.
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT ws. FROM users as ws");
    let offset = text.to_string().find("ws.").unwrap() + 3;

    let completions = lsp.get_completions_with_trigger(&text, offset, true);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        labels.contains(&"user_id".to_string()),
        "Manual trigger should resolve dangling-dot alias. Got: {:?}",
        labels
    );
}

#[test]
fn test_qualified_completion_empty_schema_reports_reason() {
    use std::sync::Arc;
    use zqlz_services::SchemaService;

    // No schema cache at all: instead of silently showing nothing, after-dot
    // completion should explain why no columns are available.
    let mut lsp = crate::SqlLsp::new(Arc::new(SchemaService::new()));
    let text = Rope::from("SELECT ws. FROM users as ws");
    let offset = text.to_string().find("ws.").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);
    assert_eq!(
        completions.len(),
        1,
        "Expected a single explanatory item. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        completions[0].label.contains("No schema loaded"),
        "Got: {}",
        completions[0].label
    );
    assert_eq!(completions[0].insert_text.as_deref(), Some(""));
}

#[test]
fn test_after_dot_known_table_without_columns_reports_pending_load() {
    // A table the schema knows about but has no cached columns for is a pending
    // column fetch, not a typo — the message must not send the user hunting.
    let mut lsp = create_test_lsp();
    lsp.schema_cache.columns_by_table.remove("locations");

    let text = Rope::from("SELECT locations.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    assert_eq!(
        completions.len(),
        1,
        "Expected a single explanatory item. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        !completions[0].label.starts_with("Unknown table or alias"),
        "A known table must not be reported as unknown. Got: {}",
        completions[0].label
    );
    assert!(
        completions[0].label.contains("locations"),
        "Got: {}",
        completions[0].label
    );
}

#[test]
fn test_after_dot_alias_to_known_table_without_columns_reports_pending_load() {
    let mut lsp = create_test_lsp();
    lsp.schema_cache.columns_by_table.remove("locations");

    let text = Rope::from("select c.  from locations as c");
    let offset = text.to_string().find("c.").unwrap() + "c.".len();

    let completions = lsp.get_completions(&text, offset);
    assert_eq!(
        completions.len(),
        1,
        "Expected a single explanatory item. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        !completions[0].label.starts_with("Unknown table or alias"),
        "Resolved alias must not be reported as unknown. Got: {}",
        completions[0].label
    );
    assert!(
        completions[0].label.contains("locations"),
        "Got: {}",
        completions[0].label
    );
}

#[test]
fn test_after_dot_schema_loading_reports_loading() {
    let mut lsp = create_test_lsp();
    lsp.schema_cache.columns_by_table.remove("locations");
    lsp.schema_loading = true;

    let text = Rope::from("SELECT locations.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    assert_eq!(completions.len(), 1, "Got: {:?}", completions);
    assert!(
        completions[0].label.contains("Loading schema"),
        "Got: {}",
        completions[0].label
    );
}

#[test]
fn test_merge_table_columns_makes_after_dot_completions_available() {
    use zqlz_services::TableDetails;

    let mut lsp = create_test_lsp();
    let details = TableDetails {
        name: "invoices".to_string(),
        table_type: zqlz_core::TableType::Table,
        columns: vec![zqlz_services::ColumnInfo {
            name: "invoice_total".to_string(),
            data_type: "NUMERIC".to_string(),
            nullable: false,
            is_primary_key: false,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_auto_increment: false,
            comment: None,
            enum_values: None,
        }],
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
        primary_key_columns: Vec::new(),
        row_count: None,
    };

    lsp.merge_table_columns("invoices", &details);

    let text = Rope::from("SELECT invoices.");
    let offset = text.to_string().len();
    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        labels.contains(&"invoice_total".to_string()),
        "Merged columns should be completable. Got: {:?}",
        labels
    );
}
