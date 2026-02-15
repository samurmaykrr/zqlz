//! Edge Case Tests - Special Characters
//!
//! Tests handling of special characters, SQL injection prevention, Unicode,
//! and reserved keywords. These tests ensure that drivers properly escape
//! and handle potentially problematic characters in data and identifiers.

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;
use zqlz_core::{Connection, Value};

/// Helper function to execute SQL with cross-driver parameter syntax
async fn execute_sql(
    conn: &dyn Connection,
    driver: TestDriver,
    sql: &str,
    params: &[Value],
) -> Result<u64> {
    let converted_sql = if driver == TestDriver::Postgres {
        sql.to_string()
    } else {
        let mut result = sql.to_string();
        for i in (1..=10).rev() {
            result = result.replace(&format!("${}", i), "?");
        }
        result
    };

    let result = conn
        .execute(&converted_sql, params)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(result.affected_rows)
}

/// Helper function to query SQL with cross-driver parameter syntax
async fn query_sql(
    conn: &dyn Connection,
    driver: TestDriver,
    sql: &str,
    params: &[Value],
) -> Result<zqlz_core::QueryResult> {
    let converted_sql = if driver == TestDriver::Postgres {
        sql.to_string()
    } else {
        let mut result = sql.to_string();
        for i in (1..=10).rev() {
            result = result.replace(&format!("${}", i), "?");
        }
        result
    };

    conn.query(&converted_sql, params)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_quotes_in_string(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE special_chars_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS special_chars_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE special_chars_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Test various quote scenarios
    let test_strings = vec![
        "It's a test",                           // Single quote
        r#"He said "hello""#,                   // Double quotes
        r#"It's "complex" text"#,               // Both quotes
        "Multiple '' single quotes",            // Escaped singles
        r#"Multiple "" double quotes"#,         // Double doubles
        "'Start and end with single'",          // Quotes at edges
        r#""Start and end with double""#,       // Double at edges
    ];

    for (idx, test_str) in test_strings.iter().enumerate() {
        let insert_sql = "INSERT INTO special_chars_test (id, data) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[Value::Int64((idx + 1) as i64), Value::String(test_str.to_string())],
        )
        .await?;

        let select_sql = "SELECT data FROM special_chars_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64((idx + 1) as i64)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("data")
            .and_then(|v| v.as_str())
            .context("Failed to get data as string")?;

        assert_eq!(retrieved, *test_str, "Quote string should match exactly");
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_backslash_in_string(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE backslash_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS backslash_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE backslash_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Test backslash scenarios
    let test_strings = vec![
        r"C:\Windows\System32",              // Windows path
        r"C:\Users\John's Folder",          // Path with quote
        r"Line with \ backslash",           // Mid-string backslash
        r"\\network\share",                 // UNC path
        r"\t\n\r",                         // Escape sequences as literals
        r"End with backslash\",            // Trailing backslash
        r"\Start with backslash",          // Leading backslash
    ];

    for (idx, test_str) in test_strings.iter().enumerate() {
        let insert_sql = "INSERT INTO backslash_test (id, data) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[Value::Int64((idx + 1) as i64), Value::String(test_str.to_string())],
        )
        .await?;

        let select_sql = "SELECT data FROM backslash_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64((idx + 1) as i64)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("data")
            .and_then(|v| v.as_str())
            .context("Failed to get data as string")?;

        assert_eq!(retrieved, *test_str, "Backslash string should match exactly");
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_unicode_string(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE unicode_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS unicode_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE unicode_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Test various Unicode scenarios
    let test_strings = vec![
        "Hello 世界",                          // Chinese characters
        "Привет мир",                         // Cyrillic
        "مرحبا بالعالم",                      // Arabic (RTL)
        "こんにちは世界",                      // Japanese
        "🎉🚀💯",                              // Emojis
        "Café résumé naïve",                 // Accented Latin
        "Greek: Ω α β γ",                    // Greek letters
        "Math: ∑∫∂√∞",                       // Mathematical symbols
        "Currency: €£¥₹",                    // Currency symbols
        "Mixed: Hello世界🚀Привет",           // Mixed scripts
    ];

    for (idx, test_str) in test_strings.iter().enumerate() {
        let insert_sql = "INSERT INTO unicode_test (id, data) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[Value::Int64((idx + 1) as i64), Value::String(test_str.to_string())],
        )
        .await?;

        let select_sql = "SELECT data FROM unicode_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64((idx + 1) as i64)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("data")
            .and_then(|v| v.as_str())
            .context("Failed to get data as string")?;

        assert_eq!(retrieved, *test_str, "Unicode string should match exactly");
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_newlines_tabs(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE whitespace_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS whitespace_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE whitespace_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Test whitespace scenarios
    let test_strings = vec![
        "Line 1\nLine 2",                   // Newline
        "Line 1\r\nLine 2",                 // Windows CRLF
        "Tab\there",                        // Tab character
        "Multiple\n\nNewlines",             // Multiple newlines
        "  Leading spaces",                 // Leading spaces
        "Trailing spaces  ",                // Trailing spaces
        "\tTab start",                      // Leading tab
        "Tab end\t",                        // Trailing tab
        "Mixed\t\n\r spaces",               // Multiple whitespace types
    ];

    for (idx, test_str) in test_strings.iter().enumerate() {
        let insert_sql = "INSERT INTO whitespace_test (id, data) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[Value::Int64((idx + 1) as i64), Value::String(test_str.to_string())],
        )
        .await?;

        let select_sql = "SELECT data FROM whitespace_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64((idx + 1) as i64)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("data")
            .and_then(|v| v.as_str())
            .context("Failed to get data as string")?;

        assert_eq!(
            retrieved, *test_str,
            "Whitespace string should match exactly"
        );
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_sql_injection_attempt(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE injection_test (
                id INTEGER PRIMARY KEY,
                username TEXT,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS injection_test (
                id INTEGER PRIMARY KEY,
                username TEXT,
                data TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE injection_test (
                id INTEGER PRIMARY KEY,
                username TEXT,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Insert a legitimate row
    let insert_sql = "INSERT INTO injection_test (id, username, data) VALUES ($1, $2, $3)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[
            Value::Int64(1),
            Value::String("admin".to_string()),
            Value::String("secret data".to_string()),
        ],
    )
    .await?;

    // Attempt various SQL injection patterns (these should be treated as literal strings)
    let injection_attempts = vec![
        "' OR '1'='1",                                    // Classic injection
        "'; DROP TABLE injection_test; --",              // Table drop
        "admin'--",                                       // Comment out rest
        "1' UNION SELECT * FROM injection_test--",       // Union attack
        "' OR 1=1--",                                     // Boolean bypass
        "'; DELETE FROM injection_test WHERE '1'='1",    // Delete attack
        "\\'; DROP TABLE injection_test;--",             // Escaped quote
    ];

    for (idx, injection_str) in injection_attempts.iter().enumerate() {
        let id = (idx + 2) as i64;
        let insert_sql = "INSERT INTO injection_test (id, username, data) VALUES ($1, $2, $3)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(id),
                Value::String(injection_str.to_string()),
                Value::String("test data".to_string()),
            ],
        )
        .await?;

        // Verify the injection string is stored as literal text
        let select_sql = "SELECT username FROM injection_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64(id)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("username")
            .and_then(|v| v.as_str())
            .context("Failed to get username as string")?;

        assert_eq!(
            retrieved, *injection_str,
            "Injection attempt should be stored as literal string"
        );
    }

    // Verify original data still exists (table wasn't dropped)
    let count_sql = "SELECT COUNT(*) as cnt FROM injection_test";
    let result = query_sql(conn.as_ref(), driver, count_sql, &[]).await?;

    let count = result
        .rows[0]
        .get_by_name("cnt")
        .and_then(|v| v.as_i64())
        .context("Failed to get count")?;

    assert_eq!(
        count,
        (injection_attempts.len() + 1) as i64,
        "All rows should exist, table should not be dropped"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_special_reserved_keyword_identifier(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Test that reserved keywords can be used as identifiers when properly quoted
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            r#"CREATE TEMPORARY TABLE "select" (
                "order" INTEGER PRIMARY KEY,
                "where" TEXT,
                "group" TEXT
            )"#
        }
        TestDriver::Mysql => {
            r#"CREATE TEMPORARY TABLE IF NOT EXISTS `select` (
                `order` INTEGER PRIMARY KEY,
                `where` TEXT,
                `group` TEXT
            )"#
        }
        TestDriver::Sqlite => {
            r#"CREATE TEMPORARY TABLE "select" (
                "order" INTEGER PRIMARY KEY,
                "where" TEXT,
                "group" TEXT
            )"#
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Insert data using reserved keyword column names
    let insert_sql = match driver {
        TestDriver::Postgres => {
            r#"INSERT INTO "select" ("order", "where", "group") VALUES ($1, $2, $3)"#
        }
        TestDriver::Mysql => {
            r#"INSERT INTO `select` (`order`, `where`, `group`) VALUES (?, ?, ?)"#
        }
        TestDriver::Sqlite => {
            r#"INSERT INTO "select" ("order", "where", "group") VALUES (?, ?, ?)"#
        }
        _ => unreachable!(),
    };

    let params = vec![
        Value::Int64(1),
        Value::String("condition".to_string()),
        Value::String("category".to_string()),
    ];

    let result = conn
        .execute(insert_sql, &params)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    assert_eq!(result.affected_rows, 1, "Should insert one row");

    // Query using reserved keyword identifiers
    let select_sql = match driver {
        TestDriver::Postgres => r#"SELECT "order", "where", "group" FROM "select" WHERE "order" = $1"#,
        TestDriver::Mysql => r#"SELECT `order`, `where`, `group` FROM `select` WHERE `order` = ?"#,
        TestDriver::Sqlite => r#"SELECT "order", "where", "group" FROM "select" WHERE "order" = ?"#,
        _ => unreachable!(),
    };

    let query_result = conn
        .query(select_sql, &[Value::Int64(1)])
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    assert_eq!(query_result.rows.len(), 1, "Should retrieve one row");

    let order_val = query_result
        .rows[0]
        .get_by_name("order")
        .and_then(|v| v.as_i64())
        .context("Failed to get order value")?;

    let where_val = query_result
        .rows[0]
        .get_by_name("where")
        .and_then(|v| v.as_str())
        .context("Failed to get where value")?;

    let group_val = query_result
        .rows[0]
        .get_by_name("group")
        .and_then(|v| v.as_str())
        .context("Failed to get group value")?;

    assert_eq!(order_val, 1, "Order value should match");
    assert_eq!(where_val, "condition", "Where value should match");
    assert_eq!(group_val, "category", "Group value should match");

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn integration_test_special_characters_handling_works() -> Result<()> {
        let conn = test_connection(TestDriver::Sqlite).await?;

        let create_table_sql = "CREATE TEMPORARY TABLE special_integration (
            id INTEGER PRIMARY KEY,
            data TEXT
        )";

        execute_sql(conn.as_ref(), TestDriver::Sqlite, create_table_sql, &[]).await?;

        // Test a mix of special characters in one scenario
        let complex_string = r#"Complex: It's "quoted" with\backslash and émojis 🎉
Multiple lines
	With tabs
And SQL: '; DROP TABLE--"#;

        let insert_sql = "INSERT INTO special_integration (id, data) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            TestDriver::Sqlite,
            insert_sql,
            &[Value::Int64(1), Value::String(complex_string.to_string())],
        )
        .await?;

        let select_sql = "SELECT data FROM special_integration WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            TestDriver::Sqlite,
            select_sql,
            &[Value::Int64(1)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let retrieved = result
            .rows[0]
            .get_by_name("data")
            .and_then(|v| v.as_str())
            .context("Failed to get data as string")?;

        assert_eq!(
            retrieved, complex_string,
            "Complex string with mixed special chars should match exactly"
        );

        Ok(())
    }
}
