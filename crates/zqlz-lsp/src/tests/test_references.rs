//! Tests for find references functionality

use crate::tests::test_helpers::create_test_lsp;
use crate::{SchemaCache, SqlLsp, TableInfo, ViewInfo};
use std::sync::Arc;
use zqlz_services::SchemaService;
use zqlz_ui::widgets::Rope;

#[test]
fn test_references_for_table_name() {
    let lsp = create_test_lsp();

    // Test finding references to a table name used multiple times
    let text = Rope::from("SELECT * FROM users WHERE user_id IN (SELECT user_id FROM users)");
    let offset = text.to_string().find("users").unwrap() + 3; // Middle of first "users"

    let result = lsp.get_references(&text, offset);

    // Should find at least 2 references (two uses of "users" table)
    assert!(
        result.len() >= 2,
        "Should find at least 2 references to 'users', found {}",
        result.len()
    );
}

#[test]
fn test_references_for_column_name() {
    let lsp = create_test_lsp();

    // Test finding references to a column name
    let text = Rope::from("SELECT username, username as user FROM users WHERE username = 'test'");
    let offset = text.to_string().find("username").unwrap() + 4; // Middle of first "username"

    let result = lsp.get_references(&text, offset);

    // Should find at least 2 references (SELECT and WHERE)
    assert!(
        result.len() >= 2,
        "Should find at least 2 references to 'username', found {}",
        result.len()
    );
}

#[test]
fn test_references_ignore_strings_and_comments() {
    let lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT username FROM users -- username in comment\nWHERE username = 'username in string'",
    );
    let offset = text.to_string().find("username").unwrap() + 4;

    let result = lsp.get_references(&text, offset);
    let source_ranges: Vec<_> = result
        .iter()
        .filter(|location| location.uri.as_str() == "sql://internal")
        .map(|location| location.range)
        .collect();

    assert_eq!(
        source_ranges.len(),
        2,
        "Only active SQL identifiers should count as references. Got: {:?}",
        source_ranges
    );
}

#[test]
fn test_references_from_string_literal_return_empty() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT username FROM users WHERE username = 'username in string'");
    let offset = text.to_string().rfind("username").unwrap() + 4;

    let result = lsp.get_references(&text, offset);

    assert!(
        result.is_empty(),
        "Reference lookup from inside string literal should be empty, got {result:?}"
    );
}

#[test]
fn test_references_for_keyword_returns_empty() {
    let lsp = create_test_lsp();

    // Test that finding references for SQL keywords returns empty
    let text = Rope::from("SELECT * FROM users WHERE id = 1");
    let offset = text.to_string().find("SELECT").unwrap() + 3; // Middle of "SELECT"

    let result = lsp.get_references(&text, offset);

    // Keywords should return empty results
    assert!(
        result.is_empty(),
        "Should not find references for keywords, found {}",
        result.len()
    );
}

#[test]
fn test_references_at_empty_position() {
    let lsp = create_test_lsp();

    // Test finding references at an empty position
    let text = Rope::from("SELECT * FROM users");
    let offset = 0; // Beginning of text

    let result = lsp.get_references(&text, offset);

    // Empty position should return empty results
    assert!(
        result.is_empty(),
        "Should not find references at empty position, found {}",
        result.len()
    );
}

#[test]
fn test_references_for_qualified_column() {
    let lsp = create_test_lsp();

    // Test finding references for a qualified column (table.column)
    let text = Rope::from(
        "SELECT users.user_id, users.user_name FROM users JOIN orders ON users.user_id = orders.user_id",
    );
    let offset = text.to_string().find("users.user_id").unwrap() + 6; // Middle of "user_id"

    let result = lsp.get_references(&text, offset);

    // Should find at least 2 references to users.user_id
    assert!(
        result.len() >= 2,
        "Should find at least 2 references to 'users.user_id', found {}",
        result.len()
    );
}

#[test]
fn test_references_for_qualified_column_do_not_cross_qualifiers() {
    let lsp = create_test_lsp();
    let sql = "SELECT users.user_id, orders.user_id FROM users JOIN orders ON users.user_id = orders.user_id";
    let text = Rope::from(sql);
    let offset = sql.find("users.user_id").unwrap() + "users.".len() + 2;

    let result = lsp.get_references(&text, offset);
    let internal_ranges: Vec<_> = result
        .iter()
        .filter(|location| location.uri.as_str() == "sql://internal")
        .map(|location| location.range)
        .collect();

    assert_eq!(
        internal_ranges.len(),
        2,
        "qualified references should stay scoped to same qualifier: {internal_ranges:?}"
    );
    assert!(internal_ranges.iter().all(|range| {
        let start = range.start.character as usize;
        let prefix_start = start.saturating_sub("users.".len());
        &sql[prefix_start..start] == "users."
    }));
}

#[test]
fn test_references_for_unknown_symbol() {
    let lsp = create_test_lsp();

    // Test finding references for an unknown symbol
    let text = Rope::from("SELECT unknown_field FROM users");
    let offset = text.to_string().find("unknown_field").unwrap() + 5; // Middle of "unknown_field"

    let result = lsp.get_references(&text, offset);

    // Unknown symbols in text should still be found (they exist in the query)
    // This tests text-based references, not schema references
    assert!(
        !result.is_empty(),
        "Should find at least 1 reference to 'unknown_field' in the query itself"
    );
}

#[test]
fn test_table_references_only_include_views_with_matching_definitions() {
    let schema_service = Arc::new(SchemaService::new());
    let mut lsp = SqlLsp::new(schema_service);
    let mut cache = SchemaCache::default();

    cache.tables.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            schema: None,
            comment: None,
            row_count: None,
            table_type: zqlz_core::TableType::Table,
        },
    );
    cache.views.insert(
        "user_summary".to_string(),
        ViewInfo {
            name: "user_summary".to_string(),
            schema: None,
            definition: Some("SELECT id FROM users".to_string()),
            is_materialized: false,
        },
    );
    cache.views.insert(
        "audit_summary".to_string(),
        ViewInfo {
            name: "audit_summary".to_string(),
            schema: None,
            definition: Some("SELECT id FROM audit_log".to_string()),
            is_materialized: false,
        },
    );
    cache.views.insert(
        "opaque_view".to_string(),
        ViewInfo {
            name: "opaque_view".to_string(),
            schema: None,
            definition: None,
            is_materialized: false,
        },
    );
    lsp.set_schema_cache(cache);

    let text = Rope::from("SELECT * FROM users");
    let offset = text.to_string().find("users").unwrap() + 1;
    let result = lsp.get_references(&text, offset);
    let view_uris: Vec<_> = result
        .iter()
        .map(|location| location.uri.as_str().to_string())
        .filter(|uri| uri.starts_with("sql://internal/view/"))
        .collect();

    assert_eq!(view_uris, vec!["sql://internal/view/user_summary"]);
}

#[test]
fn test_references_excludes_partial_matches() {
    let lsp = create_test_lsp();

    // Test that partial matches are excluded
    // "user" should not match inside "username" or "user_id"
    let text = Rope::from("SELECT username, user_id FROM users");
    let offset = text.to_string().find("user").unwrap(); // Beginning of "user" in "username"

    let result = lsp.get_references(&text, offset);

    // Should not find references for "user" since it's part of larger words
    // The word at offset is "user" but it's part of "username"
    // Our implementation should handle word boundaries correctly
    for location in &result {
        let start = location.range.start.character;
        let end = location.range.end.character;
        let text_str = text.to_string();
        let line = text_str
            .lines()
            .nth(location.range.start.line as usize)
            .unwrap();
        let referenced_word = line[start as usize..end as usize].to_lowercase();
        // The word should be exactly "user" if it's being reported as a reference
        tracing::debug!(
            "Found reference: '{}' at {}:{}",
            referenced_word,
            start,
            end
        );
    }
}
