//! Tests for subquery completions
//!
//! Tests the SQL LSP's ability to provide completions within subqueries,
//! including nested subqueries, correlated subqueries, and subqueries in
//! various contexts (SELECT, FROM, WHERE, etc.).

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_subquery_in_where_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id IN (SELECT user_id FROM )");
    let offset = text.to_string().len() - 1; // Before closing paren

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_column_reference() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id IN (SELECT  FROM audit_log)");
    let offset = text.to_string().find("(SELECT ").unwrap() + 8; // After "SELECT "

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns from audit_log
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should suggest columns in subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_correlated_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM audit_log a WHERE a.user_id = u.)",
    );
    let offset = text.to_string().find("u.").unwrap() + 2;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns from outer query table 'users'
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should suggest columns from outer query in correlated subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_nested_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE user_id IN (SELECT user_id FROM audit_log WHERE log_id IN (SELECT log_id FROM ))",
    );
    let offset = text.to_string().rfind("FROM ").unwrap() + 5;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables even in deeply nested subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in nested subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_in_select_list() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT user_id, (SELECT COUNT(*) FROM  WHERE user_id = users.user_id) FROM users",
    );
    let offset = text.to_string().find("FROM ").unwrap() + 5;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in scalar subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in SELECT list subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_in_from_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (SELECT  FROM users) subq");
    let offset = text.to_string().find("(SELECT ").unwrap() + 8;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns inside derived table
    assert!(
        completions
            .iter()
            .any(|c| c.label == "user_id" || c.label == "username"),
        "Should suggest columns in derived table subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_derived_table_alias_usage() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT subq. FROM (SELECT user_id, username FROM users) subq");
    let offset = text.to_string().find("subq.").unwrap() + 5;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns from derived table using alias
    assert!(
        completions
            .iter()
            .any(|c| c.label == "user_id" || c.label == "username"),
        "Should suggest columns from derived table alias. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_exists() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM  WHERE user_id = users.user_id)",
    );
    // Use rfind to target the second FROM (inside the EXISTS subquery), not the outer FROM
    let offset = text.to_string().rfind("FROM ").unwrap() + 5;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in EXISTS subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in EXISTS subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_not_exists() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM audit_log WHERE )");
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns/conditions in NOT EXISTS subquery
    assert!(
        !completions.is_empty(),
        "Should provide completions in NOT EXISTS subquery"
    );
}

#[test]
fn test_subquery_with_any() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id = ANY (SELECT user_id FROM )");
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in ANY subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in ANY subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_all() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id > ALL (SELECT user_id FROM )");
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in ALL subquery
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in ALL subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_in_having_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT user_id, COUNT(*) FROM audit_log GROUP BY user_id HAVING COUNT(*) > (SELECT AVG(count) FROM (SELECT user_id, COUNT(*) as count FROM ))",
    );
    let offset = text.to_string().len() - 2;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in nested subquery within HAVING
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should suggest tables in HAVING subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_in_join_condition() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users u JOIN audit_log a ON u.user_id = a.user_id AND a.log_id IN (SELECT log_id FROM )",
    );
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in subquery within JOIN condition
    assert!(
        !completions.is_empty(),
        "Should provide completions in JOIN condition subquery"
    );
}

#[test]
fn test_multiple_subqueries_in_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE user_id IN (SELECT user_id FROM audit_log) AND username IN (SELECT username FROM )",
    );
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should handle multiple subqueries independently
    assert!(
        completions.iter().any(|c| c.label == "users"),
        "Should suggest tables in second subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_union() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE user_id IN (SELECT user_id FROM audit_log UNION SELECT user_id FROM )",
    );
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest tables in UNION part of subquery
    assert!(
        completions.iter().any(|c| c.label == "users"),
        "Should suggest tables in UNION subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_order_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE user_id IN (SELECT user_id FROM audit_log ORDER BY )",
    );
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns for ORDER BY in subquery
    assert!(
        completions
            .iter()
            .any(|c| c.label == "log_timestamp" || c.label == "user_id"),
        "Should suggest columns for ORDER BY in subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_with_limit() {
    let mut lsp = create_test_lsp();
    let text =
        Rope::from("SELECT * FROM users WHERE user_id IN (SELECT user_id FROM audit_log LIMIT 10)");
    let offset = text.to_string().find("audit_log").unwrap();

    let completions = lsp.get_completions(&text, offset);

    // Should handle subqueries with LIMIT clause
    assert!(!completions.is_empty(), "Should handle subquery with LIMIT");
}

#[test]
fn test_subquery_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id IN ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest subquery snippet after IN
    // Note: This tests if subquery snippets are available
    assert!(
        !completions.is_empty(),
        "Should provide completions after IN keyword"
    );
}

#[test]
fn test_lateral_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users u, LATERAL (SELECT * FROM audit_log WHERE user_id = u.user_id) a",
    );
    let offset = text.to_string().find("u.user_id").unwrap() + 2;

    let completions = lsp.get_completions(&text, offset);

    // Should handle LATERAL subquery with reference to outer table
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should suggest columns from outer table in LATERAL subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}
