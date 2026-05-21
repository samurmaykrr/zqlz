//! Tests for hover information on keywords, tables, and columns

use super::test_helpers::*;
use crate::SqlDialect;
use lsp_types::{HoverContents, MarkedString, MarkupKind};
use zqlz_core::SequenceInfo;
use zqlz_ui::widgets::Rope;

fn hover_to_text(hover: lsp_types::Hover) -> String {
    match hover.contents {
        HoverContents::Scalar(MarkedString::String(text)) => text,
        HoverContents::Scalar(MarkedString::LanguageString(value)) => value.value,
        HoverContents::Array(items) => items
            .into_iter()
            .map(|item| match item {
                MarkedString::String(text) => text,
                MarkedString::LanguageString(value) => value.value,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        HoverContents::Markup(markup) => markup.value,
    }
}

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
fn test_hover_uses_markdown_markup_content() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 3;

    let hover = lsp.get_hover(&text, offset).expect("hover for keyword");

    match hover.contents {
        HoverContents::Markup(markup) => {
            assert_eq!(markup.kind, MarkupKind::Markdown);
            assert!(markup.value.contains("**SELECT**"));
        }
        other => panic!("expected markdown hover content, got {other:?}"),
    }
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
fn test_hover_uses_driver_metadata_for_dialect_keyword() {
    let lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("PRAGMA table_info(users)");
    let offset = 2;

    let hover = lsp
        .get_hover(&text, offset)
        .expect("hover for SQLite metadata keyword");
    let hover_text = hover_to_text(hover);

    assert!(
        hover_text.contains("**PRAGMA**"),
        "Hover should come from SQLite driver keyword metadata. Got: {hover_text}"
    );
    assert!(
        hover_text.contains("SQLite configuration"),
        "Hover should include driver-provided keyword description. Got: {hover_text}"
    );
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
fn test_hover_on_alias_qualified_column_uses_underlying_table() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT u.user_id FROM users u");
    let offset = text.to_string().find("user_id").unwrap() + 3;

    let hover = lsp
        .get_hover(&text, offset)
        .expect("hover for alias-qualified column");
    let hover_text = hover_to_text(hover);

    assert!(
        hover_text.contains("**Column: user_id**"),
        "Hover should describe the aliased column. Got: {}",
        hover_text
    );
    assert!(
        hover_text.contains("Table: `users`"),
        "Hover should resolve the alias back to the table. Got: {}",
        hover_text
    );
}

#[test]
fn test_hover_on_table_alias_shows_table_metadata() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT u.user_id FROM users u");
    let offset = text.to_string().rfind('u').unwrap();

    let hover = lsp.get_hover(&text, offset).expect("hover for table alias");
    let hover_text = hover_to_text(hover);

    assert!(
        hover_text.contains("**Table: users**"),
        "Hover on an alias should show the aliased table metadata. Got: {}",
        hover_text
    );
    assert!(
        hover_text.contains("**Columns:**"),
        "Hover on an alias should include table columns. Got: {}",
        hover_text
    );
}

#[test]
fn test_hover_on_sequence_name() {
    let mut lsp = create_test_lsp();
    let mut cache = crate::SchemaCache::default();
    cache.sequences.insert(
        "etl_stage_order_lines_stage_id_seq".to_string(),
        SequenceInfo {
            schema: Some("analytics".to_string()),
            name: "etl_stage_order_lines_stage_id_seq".to_string(),
            data_type: "bigint".to_string(),
            start_value: 1,
            min_value: 1,
            max_value: 9_223_372_036_854_775_807,
            increment_by: 1,
            current_value: Some(42),
            owner: None,
            comment: None,
        },
    );
    lsp.set_schema_cache(cache);

    let sql = r#"CREATE SEQUENCE "analytics"."etl_stage_order_lines_stage_id_seq" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE;"#;
    let text = Rope::from(sql);
    let Some(sequence_offset) = sql.find("etl_stage_order_lines_stage_id_seq") else {
        panic!("sequence name should be in SQL")
    };
    let offset = sequence_offset + 5;

    let hover = lsp.get_hover(&text, offset).expect("hover for sequence");
    let hover_text = hover_to_text(hover);

    assert!(hover_text.contains("**Sequence: etl_stage_order_lines_stage_id_seq**"));
    assert!(hover_text.contains("Schema: `analytics`"));
    assert!(hover_text.contains("Type: `bigint`"));
}

#[test]
fn test_hover_on_unknown_create_sequence_identifier_returns_none() {
    let lsp = create_test_lsp();
    let sql = r#"CREATE SEQUENCE "analytics"."missing_sequence" AS bigint INCREMENT BY 1;"#;
    let text = Rope::from(sql);
    let Some(sequence_offset) = sql.find("missing_sequence") else {
        panic!("sequence name should be in SQL")
    };
    let offset = sequence_offset + 5;

    let hover = lsp.get_hover(&text, offset);

    assert!(
        hover.is_none(),
        "DDL object names should not fall through to query-derived hover"
    );
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
    // "GROUP" starts at offset 33; use offset 35 to land in the middle of "GROUP"
    let offset = 35;

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
fn test_hover_ignores_table_name_inside_string_literal() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT 'users' AS literal_name");
    let offset = text.to_string().find("users").unwrap() + 2;

    let hover = lsp.get_hover(&text, offset);

    assert!(
        hover.is_none(),
        "Should not provide table hover for identifier text inside string literal"
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
