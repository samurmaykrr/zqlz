//! Edge Case Tests - Boundary Values
//!
//! Tests handling of boundary values for numeric and date types, including
//! maximum and minimum values, precision limits, and overflow/underflow detection.
//! These tests ensure that drivers properly handle extreme values within type limits.

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
async fn test_boundary_max_integer(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_int_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_int_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_int_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Maximum 32-bit signed integer value
    let max_int: i64 = 2147483647;

    let insert_sql = "INSERT INTO boundary_int_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::Int64(max_int)],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_int_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved_value = result
        .rows[0]
        .get_by_name("value")
        .and_then(|v| v.as_i64())
        .context("Failed to get value as i64")?;

    assert_eq!(
        retrieved_value, max_int,
        "Max integer value should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_min_integer(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_int_min_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_int_min_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_int_min_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Minimum 32-bit signed integer value
    let min_int: i64 = -2147483648;

    let insert_sql = "INSERT INTO boundary_int_min_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::Int64(min_int)],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_int_min_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved_value = result
        .rows[0]
        .get_by_name("value")
        .and_then(|v| v.as_i64())
        .context("Failed to get value as i64")?;

    assert_eq!(
        retrieved_value, min_int,
        "Min integer value should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_max_bigint(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_bigint_test (
                id INTEGER PRIMARY KEY,
                value BIGINT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_bigint_test (
                id INTEGER PRIMARY KEY,
                value BIGINT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_bigint_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Maximum 64-bit signed integer value
    let max_bigint: i64 = i64::MAX;

    let insert_sql = "INSERT INTO boundary_bigint_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::Int64(max_bigint)],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_bigint_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved_value = result
        .rows[0]
        .get_by_name("value")
        .and_then(|v| v.as_i64())
        .context("Failed to get value as i64")?;

    assert_eq!(
        retrieved_value, max_bigint,
        "Max bigint value should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_min_bigint(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_bigint_min_test (
                id INTEGER PRIMARY KEY,
                value BIGINT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_bigint_min_test (
                id INTEGER PRIMARY KEY,
                value BIGINT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_bigint_min_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Minimum 64-bit signed integer value
    let min_bigint: i64 = i64::MIN;

    let insert_sql = "INSERT INTO boundary_bigint_min_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::Int64(min_bigint)],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_bigint_min_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved_value = result
        .rows[0]
        .get_by_name("value")
        .and_then(|v| v.as_i64())
        .context("Failed to get value as i64")?;

    assert_eq!(
        retrieved_value, min_bigint,
        "Min bigint value should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_small_decimal(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_decimal_small_test (
                id INTEGER PRIMARY KEY,
                value DECIMAL(20, 10)
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_decimal_small_test (
                id INTEGER PRIMARY KEY,
                value DECIMAL(20, 10)
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_decimal_small_test (
                id INTEGER PRIMARY KEY,
                value REAL
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Very small decimal value with high precision
    let small_decimal = "0.0000000001";

    let insert_sql = "INSERT INTO boundary_decimal_small_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::String(small_decimal.to_string())],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_decimal_small_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved = result.rows[0]
        .get_by_name("value")
        .context("Failed to get value")?;

    // For SQLite (REAL), precision may be slightly different
    match driver {
        TestDriver::Sqlite => {
            let value = retrieved.as_f64().context("Failed to get value as f64")?;
            assert!(
                (value - 0.0000000001).abs() < 1e-9,
                "Small decimal value should be approximately preserved in SQLite"
            );
        }
        _ => {
            if let Some(value_str) = retrieved.as_str() {
                let parsed: f64 = value_str.parse().context("Failed to parse decimal")?;
                assert!(
                    (parsed - 0.0000000001).abs() < 1e-10,
                    "Small decimal value should be preserved with high precision"
                );
            } else if let Value::Decimal(value_str) = retrieved {
                let parsed: f64 = value_str.parse().context("Failed to parse decimal")?;
                assert!(
                    (parsed - 0.0000000001).abs() < 1e-10,
                    "Small decimal value should be preserved with high precision"
                );
            } else if let Some(value_f64) = retrieved.as_f64() {
                assert!(
                    (value_f64 - 0.0000000001).abs() < 1e-10,
                    "Small decimal value should be preserved with high precision"
                );
            } else {
                anyhow::bail!("Failed to get value as string or f64");
            }
        }
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_large_decimal(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_decimal_large_test (
                id INTEGER PRIMARY KEY,
                value DECIMAL(20, 2)
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_decimal_large_test (
                id INTEGER PRIMARY KEY,
                value DECIMAL(20, 2)
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_decimal_large_test (
                id INTEGER PRIMARY KEY,
                value REAL
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Large decimal value approaching DECIMAL(20,2) limit
    let large_decimal = "999999999999999999.99";

    let insert_sql = "INSERT INTO boundary_decimal_large_test (id, value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::String(large_decimal.to_string())],
    )
    .await?;

    let select_sql = "SELECT value FROM boundary_decimal_large_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let retrieved = result.rows[0]
        .get_by_name("value")
        .context("Failed to get value")?;

    // SQLite uses REAL (floating point), others use DECIMAL
    match driver {
        TestDriver::Sqlite => {
            let value = retrieved.as_f64().context("Failed to get value as f64")?;
            assert!(
                (value - 999999999999999999.99).abs() < 1.0,
                "Large decimal value should be approximately preserved in SQLite"
            );
        }
        _ => {
            if let Some(value_str) = retrieved.as_str() {
                let parsed: f64 = value_str.parse().context("Failed to parse decimal")?;
                assert!(
                    (parsed - 999999999999999999.99).abs() < 0.01,
                    "Large decimal value should be preserved"
                );
            } else if let Value::Decimal(value_str) = retrieved {
                let parsed: f64 = value_str.parse().context("Failed to parse decimal")?;
                assert!(
                    (parsed - 999999999999999999.99).abs() < 0.01,
                    "Large decimal value should be preserved"
                );
            } else if let Some(value_f64) = retrieved.as_f64() {
                assert!(
                    (value_f64 - 999999999999999999.99).abs() < 0.01,
                    "Large decimal value should be preserved"
                );
            } else {
                anyhow::bail!("Failed to get value as string or f64");
            }
        }
    }

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_date_min(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_date_min_test (
                id INTEGER PRIMARY KEY,
                date_value DATE
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_date_min_test (
                id INTEGER PRIMARY KEY,
                date_value DATE
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_date_min_test (
                id INTEGER PRIMARY KEY,
                date_value TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Minimum date supported by most databases (conservative)
    let min_date = match driver {
        TestDriver::Postgres => "1000-01-01",
        TestDriver::Mysql => "1000-01-01",
        TestDriver::Sqlite => "1000-01-01",
        _ => unreachable!(),
    };

    let insert_sql = "INSERT INTO boundary_date_min_test (id, date_value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::String(min_date.to_string())],
    )
    .await?;

    let select_sql = "SELECT date_value FROM boundary_date_min_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let date_value = result.rows[0]
        .get_by_name("date_value")
        .context("Failed to get date_value")?;
    let retrieved_date = match date_value {
        Value::String(text) => text.clone(),
        Value::Date(date) => date.to_string(),
        Value::DateTime(datetime) => datetime.to_string(),
        Value::DateTimeUtc(datetime) => datetime.to_string(),
        _ => anyhow::bail!("Unexpected date_value type: {:?}", date_value),
    };

    assert!(
        retrieved_date.contains("1000-01-01"),
        "Minimum date should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_date_max(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_date_max_test (
                id INTEGER PRIMARY KEY,
                date_value DATE
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_date_max_test (
                id INTEGER PRIMARY KEY,
                date_value DATE
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_date_max_test (
                id INTEGER PRIMARY KEY,
                date_value TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Maximum date supported by most databases (conservative)
    let max_date = match driver {
        TestDriver::Postgres => "9999-12-31",
        TestDriver::Mysql => "9999-12-31",
        TestDriver::Sqlite => "9999-12-31",
        _ => unreachable!(),
    };

    let insert_sql = "INSERT INTO boundary_date_max_test (id, date_value) VALUES ($1, $2)";
    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::String(max_date.to_string())],
    )
    .await?;

    let select_sql = "SELECT date_value FROM boundary_date_max_test WHERE id = $1";
    let result = query_sql(conn.as_ref(), driver, select_sql, &[Value::Int64(1)]).await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve one row");

    let date_value = result.rows[0]
        .get_by_name("date_value")
        .context("Failed to get date_value")?;
    let retrieved_date = match date_value {
        Value::String(text) => text.clone(),
        Value::Date(date) => date.to_string(),
        Value::DateTime(datetime) => datetime.to_string(),
        Value::DateTimeUtc(datetime) => datetime.to_string(),
        _ => anyhow::bail!("Unexpected date_value type: {:?}", date_value),
    };

    assert!(
        retrieved_date.contains("9999-12-31"),
        "Maximum date should be preserved"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_boundary_timestamp(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE boundary_timestamp_test (
                id INTEGER PRIMARY KEY,
                ts_value TIMESTAMP
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS boundary_timestamp_test (
                id INTEGER PRIMARY KEY,
                ts_value TIMESTAMP
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE boundary_timestamp_test (
                id INTEGER PRIMARY KEY,
                ts_value TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Test timestamp with various boundary scenarios
    let timestamps = match driver {
        TestDriver::Postgres => vec![
            "1970-01-01 00:00:00",
            "2038-01-19 03:14:07",
            "2000-12-31 23:59:59",
        ],
        TestDriver::Mysql => vec![
            "1970-01-01 00:00:01",
            "2038-01-19 03:14:07",
            "2000-12-31 23:59:59",
        ],
        TestDriver::Sqlite => vec![
            "1970-01-01 00:00:00",
            "2038-01-19 03:14:07",
            "2000-12-31 23:59:59",
        ],
        _ => unreachable!(),
    };

    for (idx, ts) in timestamps.iter().enumerate() {
        let insert_sql = "INSERT INTO boundary_timestamp_test (id, ts_value) VALUES ($1, $2)";
        execute_sql(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64((idx + 1) as i64),
                Value::String(ts.to_string()),
            ],
        )
        .await?;

        let select_sql = "SELECT ts_value FROM boundary_timestamp_test WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            driver,
            select_sql,
            &[Value::Int64((idx + 1) as i64)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let ts_value = result.rows[0]
            .get_by_name("ts_value")
            .context("Failed to get ts_value")?;
        let retrieved_ts = match ts_value {
            Value::String(text) => text.clone(),
            Value::DateTime(datetime) => datetime.to_string(),
            Value::DateTimeUtc(datetime) => datetime.to_string(),
            Value::Date(date) => date.to_string(),
            _ => anyhow::bail!("Unexpected ts_value type: {:?}", ts_value),
        };

        // Timestamp formats may vary slightly (e.g., with/without microseconds)
        let expected_parts: Vec<&str> = ts.split_whitespace().collect();
        for part in expected_parts {
            assert!(
                retrieved_ts.contains(part),
                "Timestamp should contain expected parts: {} in {}",
                part,
                retrieved_ts
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn integration_test_boundary_values_work() -> Result<()> {
        let conn = test_connection(TestDriver::Sqlite).await?;

        let create_table_sql = "CREATE TEMPORARY TABLE boundary_integration (
            id INTEGER PRIMARY KEY,
            max_int INTEGER,
            min_int INTEGER,
            max_bigint INTEGER,
            small_decimal REAL,
            date_value TEXT,
            timestamp_value TEXT
        )";

        execute_sql(
            conn.as_ref(),
            TestDriver::Sqlite,
            create_table_sql,
            &[],
        )
        .await?;

        let insert_sql = "INSERT INTO boundary_integration 
            (id, max_int, min_int, max_bigint, small_decimal, date_value, timestamp_value) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)";

        execute_sql(
            conn.as_ref(),
            TestDriver::Sqlite,
            insert_sql,
            &[
                Value::Int64(1),
                Value::Int64(2147483647),
                Value::Int64(-2147483648),
                Value::Int64(i64::MAX),
                Value::String("0.0000000001".to_string()),
                Value::String("1000-01-01".to_string()),
                Value::String("2038-01-19 03:14:07".to_string()),
            ],
        )
        .await?;

        let select_sql = "SELECT * FROM boundary_integration WHERE id = $1";
        let result = query_sql(
            conn.as_ref(),
            TestDriver::Sqlite,
            select_sql,
            &[Value::Int64(1)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Should retrieve one row");

        let max_int = result
            .rows[0]
            .get_by_name("max_int")
            .and_then(|v| v.as_i64())
            .context("Failed to get max_int")?;
        assert_eq!(max_int, 2147483647);

        let min_int = result
            .rows[0]
            .get_by_name("min_int")
            .and_then(|v| v.as_i64())
            .context("Failed to get min_int")?;
        assert_eq!(min_int, -2147483648);

        let max_bigint = result
            .rows[0]
            .get_by_name("max_bigint")
            .and_then(|v| v.as_i64())
            .context("Failed to get max_bigint")?;
        assert_eq!(max_bigint, i64::MAX);

        let date_value = result
            .rows[0]
            .get_by_name("date_value")
            .and_then(|v| v.as_str())
            .context("Failed to get date_value")?;
        assert_eq!(date_value, "1000-01-01");

        Ok(())
    }
}
