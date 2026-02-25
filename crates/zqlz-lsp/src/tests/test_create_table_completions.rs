//! Tests for CREATE TABLE context completions
//!
//! Verifies that when the cursor is inside a `CREATE [TEMPORARY] TABLE name (...)`
//! column-definition block the LSP suggests SQL data types and column/table
//! constraints instead of function completions or unrelated keywords.

use super::test_helpers::*;
use crate::SqlDialect;
use zqlz_ui::widgets::Rope;

// ── Data type completions ────────────────────────────────────────────────────

#[test]
fn test_create_table_suggests_data_types() {
    // SQLite has a well-defined set of data types; Generic dialect may have none.
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("CREATE TABLE users (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    assert!(
        !completions.is_empty(),
        "Should have completions inside CREATE TABLE column list"
    );

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.iter().any(|l| *l == "INTEGER" || *l == "INT"),
        "Should suggest INTEGER data type. Got: {:?}",
        labels
    );
    assert!(
        labels.iter().any(|l| *l == "TEXT" || *l == "VARCHAR"),
        "Should suggest TEXT/VARCHAR data type. Got: {:?}",
        labels
    );
}

#[test]
fn test_create_table_no_function_completions() {
    // SQLite dialect has INSTR as a scalar function — verify it's excluded in DDL context.
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    // Cursor right after the opening paren with no partial word typed yet.
    let text = Rope::from("CREATE TABLE users (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    // INSTR is a scalar function that must NOT appear inside a column list.
    assert!(
        !labels.iter().any(|l| l.to_uppercase().contains("INSTR")),
        "Should NOT suggest INSTR() function inside CREATE TABLE. Got: {:?}",
        labels
    );
}

#[test]
fn test_create_table_partial_type_integer() {
    // SQLite has INTEGER as a data type; "in" must match it, not INSTR().
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    // "in" should fuzzy-match INTEGER/INT but NOT INSTR() since we're in DDL context.
    let text = Rope::from("CREATE TABLE users (\n    user_id in");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.iter().any(|l| *l == "INTEGER" || *l == "INT"),
        "Should fuzzy-match INTEGER for prefix 'in'. Got: {:?}",
        labels
    );
    assert!(
        !labels.iter().any(|l| l.to_uppercase().contains("INSTR")),
        "Should NOT fuzzy-match INSTR() for prefix 'in' inside CREATE TABLE. Got: {:?}",
        labels
    );
}

// ── Constraint completions ───────────────────────────────────────────────────

#[test]
fn test_create_table_suggests_not_null_after_type() {
    let mut lsp = create_test_lsp();
    // After a data type the user expects constraint completions.
    let text = Rope::from("CREATE TABLE users (\n    user_id INTEGER ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.contains(&"NOT NULL"),
        "Should suggest NOT NULL constraint. Got: {:?}",
        labels
    );
    assert!(
        labels.contains(&"PRIMARY KEY"),
        "Should suggest PRIMARY KEY constraint. Got: {:?}",
        labels
    );
}

#[test]
fn test_create_table_partial_constraint_primary() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE users (\n    user_id INTEGER PRIMARY");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.contains(&"PRIMARY KEY"),
        "Should fuzzy-match PRIMARY KEY for prefix 'primary'. Got: {:?}",
        labels
    );
}

#[test]
fn test_create_table_table_level_constraints() {
    let mut lsp = create_test_lsp();
    // Start of a new line inside the column list — table-level keywords should be present.
    let text = Rope::from("CREATE TABLE users (\n    user_id INTEGER,\n    ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.contains(&"CONSTRAINT") || labels.contains(&"FOREIGN KEY"),
        "Should suggest table-level constraints (CONSTRAINT / FOREIGN KEY). Got: {:?}",
        labels
    );
}

// ── Context boundary ─────────────────────────────────────────────────────────

#[test]
fn test_create_table_no_create_table_context_after_closing_paren() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    // Cursor is placed after the closing paren — outside the column list.
    let sql = "CREATE TABLE users (\n    user_id INTEGER\n)";
    let text = Rope::from(sql);
    let offset = sql.len();

    let completions = lsp.get_completions(&text, offset);

    // The cursor is outside the column list, so we must NOT be in CreateTable context.
    // Concretely: data-type completions (STRUCT kind) must not dominate — if any
    // completions exist at all they should include SQL keywords, not only data types.
    let has_only_data_types = !completions.is_empty()
        && completions
            .iter()
            .all(|c| c.kind == Some(lsp_types::CompletionItemKind::STRUCT));
    assert!(
        !has_only_data_types,
        "After closing paren completions must not be exclusively data types (wrong context). Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

// ── CREATE TEMPORARY TABLE / CREATE TEMP TABLE variants ─────────────────────

#[test]
fn test_create_temporary_table_suggests_data_types() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("CREATE TEMPORARY TABLE session_data (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    assert!(
        !completions.is_empty(),
        "Should have completions inside CREATE TEMPORARY TABLE column list"
    );

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.iter().any(|l| *l == "INTEGER" || *l == "INT"),
        "Should suggest data types for CREATE TEMPORARY TABLE. Got: {:?}",
        labels
    );
}

#[test]
fn test_create_temp_table_suggests_data_types() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("CREATE TEMP TABLE session_data (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    assert!(
        !completions.is_empty(),
        "Should have completions inside CREATE TEMP TABLE column list"
    );

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.iter().any(|l| *l == "INTEGER" || *l == "INT"),
        "Should suggest data types for CREATE TEMP TABLE. Got: {:?}",
        labels
    );
}

// ── Nested parens (CHECK / DEFAULT expressions) ──────────────────────────────

#[test]
fn test_create_table_nested_paren_still_in_context() {
    let mut lsp = create_test_lsp();
    // Cursor is inside a CHECK(…) expression nested within the column list.
    // The detect_create_table_context depth counter must stay > 0 here.
    let text = Rope::from("CREATE TABLE orders (\n    amount NUMERIC CHECK (");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // The critical invariant: the original bug returned only table names
    // (FromClause context) here. Verify that is no longer the case.
    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();
    let only_table_names = !labels.is_empty()
        && labels
            .iter()
            .all(|l| matches!(*l, "users" | "audit_log" | "locations"));
    assert!(
        !only_table_names,
        "Should NOT return only table names inside a CHECK expression. Got: {:?}",
        labels
    );
}

// ── Regression: FROM clause completions must be unaffected ──────────────────

#[test]
fn test_from_clause_not_broken_by_create_table_fix() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<&str> = completions.iter().map(|c| c.label.as_str()).collect();

    assert!(
        labels.contains(&"users"),
        "FROM clause completion must still work after the CREATE TABLE fix. Got: {:?}",
        labels
    );
}
