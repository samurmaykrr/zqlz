//! Tests for JOIN clause completions with foreign key suggestions

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_join_suggests_related_tables() {
    let mut lsp = create_test_lsp();

    // Add foreign key relationships to test data
    let fk = zqlz_core::ForeignKeyInfo {
        name: "fk_audit_user".to_string(),
        columns: vec!["user_id".to_string()],
        referenced_table: "users".to_string(),
        referenced_schema: None,
        referenced_columns: vec!["user_id".to_string()],
        on_delete: zqlz_core::ForeignKeyAction::Cascade,
        on_update: zqlz_core::ForeignKeyAction::Cascade,
    };

    lsp.schema_cache
        .foreign_keys_by_table
        .entry("audit_log".to_string())
        .or_default()
        .push(fk.clone());

    lsp.schema_cache
        .reverse_foreign_keys
        .entry("users".to_string())
        .or_default()
        .push(("audit_log".to_string(), fk));

    let text = Rope::from("SELECT * FROM users u JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest audit_log with FK information
    assert!(
        labels.contains(&"audit_log".to_string()),
        "Should suggest audit_log table. Got: {:?}",
        labels
    );

    // Check for FK hint in detail
    let audit_log_completion = completions.iter().find(|c| c.label == "audit_log");

    assert!(
        audit_log_completion.is_some(),
        "Should have audit_log completion"
    );

    if let Some(completion) = audit_log_completion {
        assert!(
            completion
                .detail
                .as_ref()
                .map_or(false, |d| d.contains("FK")),
            "Should indicate FK relationship in detail"
        );
    }
}

#[test]
fn test_join_on_suggests_fk_columns() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u JOIN audit_log a ON u.");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest columns from users table
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
}

#[test]
fn test_left_join_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users LEFT JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest tables
    assert!(
        !labels.is_empty(),
        "Should have table suggestions for LEFT JOIN"
    );
    assert!(
        labels.contains(&"audit_log".to_string()) || labels.contains(&"locations".to_string()),
        "Should suggest available tables. Got: {:?}",
        labels
    );
}

#[test]
fn test_inner_join_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users INNER JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables for INNER JOIN
    assert!(
        !completions.is_empty(),
        "Should have completions after INNER JOIN"
    );
}

#[test]
fn test_cross_join_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users CROSS JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest tables
    assert!(
        !labels.is_empty(),
        "Should have table suggestions for CROSS JOIN"
    );
}

#[test]
fn test_join_with_alias() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u JOIN audit_log a ON ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns from both tables
    assert!(
        !completions.is_empty(),
        "Should have completions in ON clause"
    );
}

#[test]
fn test_multiple_joins() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users u JOIN audit_log a ON u.user_id = a.log_id JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should still suggest tables for the next JOIN
    assert!(
        labels.contains(&"locations".to_string()),
        "Should suggest remaining tables. Got: {:?}",
        labels
    );
}

#[test]
fn test_join_using_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users JOIN audit_log USING (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest common columns between tables
    assert!(
        !completions.is_empty(),
        "Should have completions in USING clause"
    );
}

#[test]
fn test_right_join_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users RIGHT JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables for RIGHT JOIN
    assert!(
        !completions.is_empty(),
        "Should have completions after RIGHT JOIN"
    );
}

#[test]
fn test_full_outer_join_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users FULL OUTER JOIN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables for FULL OUTER JOIN
    assert!(
        !completions.is_empty(),
        "Should have completions after FULL OUTER JOIN"
    );
}
