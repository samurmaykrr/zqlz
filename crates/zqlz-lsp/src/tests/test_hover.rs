//! Tests for hover information on keywords, tables, and columns

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_hover_select_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 3; // Middle of "SELECT"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for SELECT keyword");

    // Hover content should contain SELECT documentation
    // Check that it's present (format may vary)
}

#[test]
fn test_hover_from_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 11; // Middle of "FROM"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for FROM keyword");
}

#[test]
fn test_hover_where_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 1");
    let offset = 22; // Middle of "WHERE"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for WHERE keyword");
}

#[test]
fn test_hover_on_table_name() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 16; // Middle of "users"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for table name");
}

#[test]
fn test_hover_on_column_name() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_id FROM users");
    let offset = 10; // Middle of "user_id"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for column name");
}

#[test]
fn test_hover_on_qualified_column() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT users.user_id FROM users");
    let offset = 17; // Middle of "user_id" in "users.user_id"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for qualified column");
}

#[test]
fn test_hover_on_join_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users JOIN audit_log ON users.user_id = audit_log.log_id");
    let offset = 24; // Middle of "JOIN"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for JOIN keyword");
}

#[test]
fn test_hover_on_aggregate_function() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT(*) FROM users");
    let offset = 9; // Middle of "COUNT"

    let hover = lsp.get_hover(&text, offset);

    assert!(
        hover.is_some(),
        "Should provide hover for aggregate function"
    );
}

#[test]
fn test_hover_on_group_by_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT name, COUNT(*) FROM users GROUP BY name");
    let offset = 40; // Middle of "GROUP"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for GROUP BY keyword");
}

#[test]
fn test_hover_on_order_by_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users ORDER BY created_at");
    let offset = 24; // Middle of "ORDER"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for ORDER BY keyword");
}

#[test]
fn test_hover_on_limit_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users LIMIT 10");
    let offset = 21; // Middle of "LIMIT"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for LIMIT keyword");
}

#[test]
fn test_hover_on_distinct_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT DISTINCT name FROM users");
    let offset = 12; // Middle of "DISTINCT"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for DISTINCT keyword");
}

#[test]
fn test_hover_on_case_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHEN age > 18 THEN 'adult' END FROM users");
    let offset = 9; // Middle of "CASE"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for CASE keyword");
}

#[test]
fn test_hover_on_insert_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name) VALUES ('John')");
    let offset = 3; // Middle of "INSERT"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for INSERT keyword");
}

#[test]
fn test_hover_on_update_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("UPDATE users SET name = 'Jane' WHERE id = 1");
    let offset = 3; // Middle of "UPDATE"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for UPDATE keyword");
}

#[test]
fn test_hover_on_delete_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("DELETE FROM users WHERE id = 1");
    let offset = 3; // Middle of "DELETE"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for DELETE keyword");
}

#[test]
fn test_hover_on_and_operator() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE active = 1 AND age > 18");
    let offset = 40; // Middle of "AND"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for AND operator");
}

#[test]
fn test_hover_on_or_operator() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE active = 1 OR admin = 1");
    let offset = 39; // Middle of "OR"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for OR operator");
}

#[test]
fn test_hover_on_in_operator() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id IN (1, 2, 3)");
    let offset = 31; // Middle of "IN"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for IN operator");
}

#[test]
fn test_hover_on_like_operator() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name LIKE '%John%'");
    let offset = 33; // Middle of "LIKE"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for LIKE operator");
}

#[test]
fn test_hover_on_between_operator() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE age BETWEEN 18 AND 65");
    let offset = 35; // Middle of "BETWEEN"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for BETWEEN operator");
}

#[test]
fn test_hover_on_null_keyword() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE deleted_at IS NULL");
    let offset = 42; // Middle of "NULL"

    let hover = lsp.get_hover(&text, offset);

    assert!(hover.is_some(), "Should provide hover for NULL keyword");
}

#[test]
fn test_hover_on_primary_key_column() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_id FROM users");
    let offset = 10; // Middle of "user_id" (which is a PK in test data)

    let hover = lsp.get_hover(&text, offset);

    assert!(
        hover.is_some(),
        "Should provide hover for primary key column"
    );
    // Could verify it mentions "PRIMARY KEY" in the hover text
}

#[test]
fn test_hover_no_match() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 8; // On the asterisk

    let hover = lsp.get_hover(&text, offset);

    // May or may not provide hover for *
    println!("Hover on asterisk: {:?}", hover);
}

#[test]
fn test_hover_on_string_literal() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = 'John'");
    let offset = 37; // Inside 'John'

    let hover = lsp.get_hover(&text, offset);

    // Should not provide hover for string literal
    assert!(
        hover.is_none(),
        "Should not provide hover for string literal"
    );
}

#[test]
fn test_hover_on_number_literal() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 123");
    let offset = 33; // On number 123

    let hover = lsp.get_hover(&text, offset);

    // Should not provide hover for number literal
    assert!(
        hover.is_none(),
        "Should not provide hover for number literal"
    );
}
