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

    // Should return empty or no completions for unknown table
    assert!(
        completions.is_empty(),
        "Should not suggest columns for unknown table"
    );
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
