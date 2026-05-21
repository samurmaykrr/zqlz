use super::*;

#[test]
fn qualified_reference_prefix_detects_bare_alias_after_dot() {
    assert_eq!(
        zqlz_core::qualified_sql_reference_prefix("SELECT u. "),
        Some("u".to_string())
    );
}

#[test]
fn qualified_reference_prefix_detects_partial_column_after_dot() {
    assert_eq!(
        zqlz_core::qualified_sql_reference_prefix("SELECT users.us"),
        Some("users".to_string())
    );
}

#[test]
fn qualified_reference_prefix_ignores_protected_text() {
    assert_eq!(
        zqlz_core::qualified_sql_reference_prefix("SELECT 'users.us"),
        None
    );
    assert_eq!(
        zqlz_core::qualified_sql_reference_prefix("SELECT u.id -- users.us"),
        None
    );
}

#[test]
fn incomplete_context_rescue_ignores_protected_clause_keywords() {
    let analyzer = ContextAnalyzer::new().unwrap();

    assert!(matches!(
        analyzer.analyze(&Rope::from("-- FROM users\n"), "-- FROM users\n".len()),
        SqlContext::General
    ));
    assert!(matches!(
        analyzer.analyze(
            &Rope::from("SELECT 'FROM users' "),
            "SELECT 'FROM users' ".len()
        ),
        SqlContext::SelectList { .. }
    ));
    assert!(matches!(
        analyzer.analyze(&Rope::from(r#"SELECT "FROM" "#), r#"SELECT "FROM" "#.len()),
        SqlContext::SelectList { .. }
    ));
}

#[test]
fn incomplete_context_rescue_detects_real_partial_table_context() {
    let analyzer = ContextAnalyzer::new().unwrap();

    assert!(matches!(
        analyzer.analyze(&Rope::from("SELECT * FROM us"), "SELECT * FROM us".len()),
        SqlContext::FromClause
    ));
    assert!(matches!(
        analyzer.analyze(
            &Rope::from("SELECT * FROM users JOIN or"),
            "SELECT * FROM users JOIN or".len()
        ),
        SqlContext::JoinClause { .. }
    ));
}
