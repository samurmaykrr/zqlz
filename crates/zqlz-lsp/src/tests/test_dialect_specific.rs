#![allow(unused_mut)]

//! Tests for database dialect-specific features
//!
//! Tests SQL completions and syntax support for different database dialects:
//! SQLite, MySQL/MariaDB, PostgreSQL, and SQL Server.

use super::test_helpers::*;
use crate::SqlDialect;
use sqlparser::parser::Parser;
use zqlz_ui::widgets::Rope;

// ===== SQLite-specific tests =====

#[test]
fn test_sqlparser_dialect_selection_uses_core_driver_registry_aliases() {
    let mut turso_lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    turso_lsp.driver_type = "turso".to_string();
    let turso_dialect = turso_lsp.get_dialect();
    Parser::parse_sql(turso_dialect.as_ref(), "SELECT [name] FROM users")
        .expect("turso alias should use SQLite sqlparser dialect");

    let mut mssql_lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    mssql_lsp.driver_type = "mssql".to_string();
    let mssql_dialect = mssql_lsp.get_dialect();
    Parser::parse_sql(mssql_dialect.as_ref(), "SELECT TOP 1 * FROM users")
        .expect("mssql alias should use SQL Server sqlparser dialect");
}

#[test]
fn test_mongodb_completions_use_document_driver_terms_not_sql_fallback() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MongoDB);
    let text = Rope::from("db.orders.aggregate([{ $gr");
    let completions = lsp.get_completions(&text, text.len());
    let labels = completions
        .iter()
        .map(|completion| completion.label.as_str())
        .collect::<Vec<_>>();

    assert!(
        labels.contains(&"$group"),
        "Mongo document completions should include driver operator terms. Got: {labels:?}"
    );
    assert!(
        !labels.contains(&"SELECT"),
        "Mongo document completions should not fall back to SQL keywords. Got: {labels:?}"
    );
}

#[test]
fn test_mongodb_type_completion_comes_from_driver_syntax_profile() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MongoDB);
    let text = Rope::from("Object");
    let completions = lsp.get_completions(&text, text.len());

    assert!(
        completions
            .iter()
            .any(|completion| completion.label == "ObjectId"),
        "Mongo document completions should include BSON types from driver syntax metadata. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.as_str())
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlite_autoincrement_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINC");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest AUTOINCREMENT (SQLite-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "AUTOINCREMENT"),
        "Should suggest SQLite AUTOINCREMENT keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlite_pragma_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("PRAG");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest PRAGMA (SQLite-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "PRAGMA"),
        "Should suggest SQLite PRAGMA keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlite_without_rowid() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("CREATE TABLE test (id INT) WITHOUT ROW");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest ROWID (for WITHOUT ROWID syntax)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "ROWID"),
        "Should suggest SQLite ROWID keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlite_json_functions() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("SELECT JSON_EXT");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest JSON_EXTRACT (SQLite-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "JSON_EXTRACT()"),
        "Should suggest SQLite JSON_EXTRACT function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlite_datetime_functions() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLite);
    let text = Rope::from("SELECT STRF");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest STRFTIME (SQLite date function)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "STRFTIME()"),
        "Should suggest SQLite STRFTIME function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

// ===== MySQL-specific tests =====

#[test]
fn test_mysql_auto_increment_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let text = Rope::from("CREATE TABLE test (id INT AUTO_INCR");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest AUTO_INCREMENT (MySQL-specific, with underscore)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "AUTO_INCREMENT"),
        "Should suggest MySQL AUTO_INCREMENT keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_mysql_show_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let text = Rope::from("SHOW");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest SHOW and related keywords
    assert!(
        completions.iter().any(|c| c.label.to_uppercase() == "SHOW"
            || c.label.to_uppercase() == "TABLES"
            || c.label.to_uppercase() == "DATABASES"),
        "Should suggest MySQL SHOW keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_mysql_tinyint_datatype() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let text = Rope::from("CREATE TABLE test (flag TINY");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest TINYINT (MySQL data type)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "TINYINT"),
        "Should suggest MySQL TINYINT data type. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_mysql_concat_ws_function() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let text = Rope::from("SELECT CONCAT_");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CONCAT_WS (MySQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "CONCAT_WS()"),
        "Should suggest MySQL CONCAT_WS function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_mysql_unsigned_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::MySQL);
    let text = Rope::from("CREATE TABLE test (id INT UNSIG");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest UNSIGNED (MySQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "UNSIGNED"),
        "Should suggest MySQL UNSIGNED keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    assert!(
        completions.iter().any(|completion| {
            completion.label.eq_ignore_ascii_case("UNSIGNED")
                && completion
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("negative values"))
        }),
        "UNSIGNED completion should use driver metadata detail. Got: {:?}",
        completions
            .iter()
            .map(|completion| (&completion.label, &completion.detail))
            .collect::<Vec<_>>()
    );
}

// ===== PostgreSQL-specific tests =====

#[test]
fn test_postgresql_serial_datatype() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("CREATE TABLE test (id SERIA");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest SERIAL (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "SERIAL"),
        "Should suggest PostgreSQL SERIAL data type. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_postgresql_returning_clause() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("INSERT INTO users (username) VALUES ('test') RETUR");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest RETURNING (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "RETURNING"),
        "Should suggest PostgreSQL RETURNING keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_postgresql_jsonb_datatype() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("CREATE TABLE test (data JSONB");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest or recognize JSONB (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "JSONB" || c.label.to_uppercase().contains("JSONB")),
        "Should suggest PostgreSQL JSONB data type. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_postgresql_lateral_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT * FROM users, LATER");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest LATERAL (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "LATERAL"),
        "Should suggest PostgreSQL LATERAL keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    let lateral = completions
        .iter()
        .find(|completion| completion.label.eq_ignore_ascii_case("LATERAL"))
        .expect("LATERAL completion");
    assert!(
        lateral
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("Lateral join")),
        "LATERAL completion should use active dialect keyword metadata detail, got: {:?}",
        lateral.detail
    );
}

#[test]
fn test_postgresql_string_agg_function() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT STRING_AG");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest STRING_AGG (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "STRING_AGG()"),
        "Should suggest PostgreSQL STRING_AGG function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_postgresql_generate_series() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT * FROM GENERATE_SER");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest GENERATE_SERIES (PostgreSQL-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "GENERATE_SERIES()"),
        "Should suggest PostgreSQL GENERATE_SERIES function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

// ===== SQL Server-specific tests =====

#[test]
fn test_sqlserver_identity_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    let text = Rope::from("CREATE TABLE test (id INT IDENTI");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest IDENTITY (SQL Server-specific)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "IDENTITY"),
        "Should suggest SQL Server IDENTITY keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlserver_top_keyword() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    let text = Rope::from("SELECT TO");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest TOP (SQL Server-specific SELECT modifier)
    assert!(
        completions.iter().any(|c| c.label.to_uppercase() == "TOP"),
        "Should suggest SQL Server TOP keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    assert!(
        completions.iter().any(|completion| {
            completion.label.eq_ignore_ascii_case("TOP")
                && completion
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("T-SQL"))
        }),
        "TOP completion should use driver-owned dialect metadata. Got: {:?}",
        completions
            .iter()
            .map(|completion| (&completion.label, &completion.detail))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlserver_uniqueidentifier_datatype() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    let text = Rope::from("CREATE TABLE test (id UNIQUEIDEN");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest UNIQUEIDENTIFIER (SQL Server data type)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "UNIQUEIDENTIFIER"),
        "Should suggest SQL Server UNIQUEIDENTIFIER data type. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlserver_row_number_function() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    let text = Rope::from("SELECT ROW_NUMB");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest ROW_NUMBER (SQL Server window function)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "ROW_NUMBER()"),
        "Should suggest SQL Server ROW_NUMBER function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_sqlserver_isnull_function() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::SQLServer);
    let text = Rope::from("SELECT ISNU");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest ISNULL (SQL Server-specific, different from IS NULL)
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "ISNULL()"),
        "Should suggest SQL Server ISNULL function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

// ===== Common SQL features across all dialects =====

#[test]
fn test_common_keywords_all_dialects() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SEL");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Common SQL keywords should be available in all dialects
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "SELECT"),
        "Should suggest common SELECT keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_common_functions_all_dialects() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COU");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Common functions like COUNT should be available
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "COUNT()"),
        "Should suggest common COUNT function. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_common_datatypes_all_dialects() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE test (id INTEG");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Common data types like INTEGER should be available
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "INTEGER"),
        "Should suggest common INTEGER data type. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

// ===== Dialect detection and switching =====

#[test]
fn test_dialect_specific_keyword_not_in_generic() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("PRAG");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // If using generic dialect, SQLite-specific keywords shouldn't dominate
    // But in practice, we show all keywords for better UX
    // This test just verifies PRAGMA is present if dialect support is working
    let has_pragma = completions
        .iter()
        .any(|c| c.label.to_uppercase() == "PRAGMA");

    // The presence of PRAGMA indicates dialect support is working
    assert!(
        has_pragma
            || completions
                .iter()
                .any(|c| c.label.to_uppercase().starts_with("PRAG")),
        "Should handle dialect-specific keywords. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_generic_completion_uses_core_sql_registry_profiles() {
    let lsp = create_test_lsp_with_dialect(SqlDialect::Generic);
    let dialects = lsp.completion_sql_dialects();

    assert!(
        dialects.contains(&SqlDialect::ClickHouse) && dialects.contains(&SqlDialect::DuckDB),
        "generic SQL completion dialects should come from core SQL registry profiles. Got: {:?}",
        dialects
    );
}

#[test]
fn test_signature_help_ignores_commas_inside_string_literals() {
    let lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT CONCAT('last, first', ");
    let offset = text.to_string().len();

    let signature = lsp
        .get_signature_help(&text, offset)
        .expect("signature help for CONCAT");

    assert_eq!(
        signature.active_parameter,
        Some(1),
        "comma inside string literal should not advance active parameter"
    );
}

#[test]
fn test_signature_help_returns_outer_function_after_nested_call() {
    let lsp = create_test_lsp_with_dialect(SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT CONCAT(LOWER(name), ");
    let offset = text.to_string().len();

    let signature = lsp
        .get_signature_help(&text, offset)
        .expect("signature help for outer CONCAT");

    assert_eq!(
        signature.active_parameter,
        Some(1),
        "closed nested function call should return to outer active parameter"
    );
    assert!(
        signature
            .signatures
            .iter()
            .any(|signature| signature.label.starts_with("CONCAT(")),
        "signature help should describe outer CONCAT call: {:?}",
        signature.signatures
    );
}

#[test]
fn test_hover_shows_dialect_info() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT(*) FROM users");
    let offset = text.to_string().find("COUNT").unwrap() + 3;

    let hover = lsp.get_hover(&text, offset);

    // Hover should provide function documentation
    assert!(hover.is_some(), "Should provide hover info for functions");
}

#[test]
fn test_case_expression_all_dialects() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHE");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // CASE WHEN should be available in all dialects
    assert!(
        completions.iter().any(|c| c.label.to_uppercase() == "WHEN"),
        "Should suggest WHEN for CASE expression. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}
