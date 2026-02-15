//! Edge Case Tests - Large Data Sets
//!
//! Tests handling of large result sets, batch operations, and memory management.
//! These tests validate that drivers can handle significant data volumes without
//! performance degradation or resource exhaustion.

use crate::fixtures::{sql_drivers, test_connection, TestDriver};
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
async fn test_large_select_10k_rows(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table with 10k rows
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE large_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS large_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE large_test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Insert 10k rows (batch insert for efficiency)
    let batch_size = 100;
    let total_rows = 10_000;

    for batch_start in (0..total_rows).step_by(batch_size) {
        let mut insert_sql = "INSERT INTO large_test (id, data) VALUES ".to_string();
        let mut values = Vec::new();

        for i in 0..batch_size.min(total_rows - batch_start) {
            let id = batch_start + i;
            if i > 0 {
                insert_sql.push_str(", ");
            }

            if driver == TestDriver::Postgres {
                insert_sql.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
            } else {
                insert_sql.push_str("(?, ?)");
            }

            values.push(Value::Int64(id as i64));
            values.push(Value::String(format!("data_value_{}", id)));
        }

        execute_sql(conn.as_ref(), driver, &insert_sql, &values).await?;
    }

    // Select all 10k rows
    let result = query_sql(conn.as_ref(), driver, "SELECT * FROM large_test", &[]).await?;

    assert_eq!(
        result.rows.len(),
        total_rows,
        "Should retrieve all 10k rows"
    );

    // Verify first and last rows
    let first_row = &result.rows[0];
    let first_id = first_row
        .get_by_name("id")
        .and_then(|v| v.as_i64())
        .context("First row should have id")?;
    assert_eq!(first_id, 0, "First row ID should be 0");

    let last_row = &result.rows[result.rows.len() - 1];
    let last_id = last_row
        .get_by_name("id")
        .and_then(|v| v.as_i64())
        .context("Last row should have id")?;
    assert_eq!(last_id, 9999, "Last row ID should be 9999");

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_large_insert_batch_1k_rows(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE batch_insert_test (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INTEGER
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS batch_insert_test (
                id INTEGER AUTO_INCREMENT PRIMARY KEY,
                name TEXT,
                value INTEGER
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE batch_insert_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                value INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Batch insert 1000 rows
    let batch_size = 100;
    let total_rows = 1000;
    let mut total_inserted = 0u64;

    for batch_start in (0..total_rows).step_by(batch_size) {
        let mut insert_sql = "INSERT INTO batch_insert_test (name, value) VALUES ".to_string();
        let mut values = Vec::new();

        for i in 0..batch_size.min(total_rows - batch_start) {
            let row_num = batch_start + i;
            if i > 0 {
                insert_sql.push_str(", ");
            }

            if driver == TestDriver::Postgres {
                insert_sql.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
            } else {
                insert_sql.push_str("(?, ?)");
            }

            values.push(Value::String(format!("row_{}", row_num)));
            values.push(Value::Int64((row_num * 10) as i64));
        }

        let affected = execute_sql(conn.as_ref(), driver, &insert_sql, &values).await?;
        total_inserted += affected;
    }

    assert_eq!(
        total_inserted, total_rows as u64,
        "Should have inserted 1000 rows"
    );

    // Verify inserted rows
    let result = query_sql(
        conn.as_ref(),
        driver,
        "SELECT COUNT(*) as count FROM batch_insert_test",
        &[],
    )
    .await?;

    let count = result.rows[0]
        .get_by_name("count")
        .and_then(|v| v.as_i64())
        .context("Should get count")?;
    assert_eq!(count, total_rows as i64, "Should have 1000 rows in table");

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_large_long_string_payload(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE large_string_test (
                id INTEGER PRIMARY KEY,
                content TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS large_string_test (
                id INTEGER PRIMARY KEY,
                content TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE large_string_test (
                id INTEGER PRIMARY KEY,
                content TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Create a 1MB string
    let large_string = "A".repeat(1_000_000);

    // Insert the large string
    let insert_sql = if driver == TestDriver::Postgres {
        "INSERT INTO large_string_test (id, content) VALUES ($1, $2)"
    } else {
        "INSERT INTO large_string_test (id, content) VALUES (?, ?)"
    };

    execute_sql(
        conn.as_ref(),
        driver,
        insert_sql,
        &[Value::Int64(1), Value::String(large_string.clone())],
    )
    .await?;

    // Retrieve and verify
    let result = query_sql(
        conn.as_ref(),
        driver,
        if driver == TestDriver::Postgres {
            "SELECT content FROM large_string_test WHERE id = $1"
        } else {
            "SELECT content FROM large_string_test WHERE id = ?"
        },
        &[Value::Int64(1)],
    )
    .await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve 1 row");

    let retrieved_content = result.rows[0]
        .get_by_name("content")
        .and_then(|v| v.as_str())
        .context("Should get content")?;
    assert_eq!(
        retrieved_content.len(),
        1_000_000,
        "Retrieved string should be 1MB"
    );
    assert_eq!(
        retrieved_content, large_string,
        "Retrieved string should match inserted string"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_large_many_columns_result(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table with many columns (50 columns)
    let mut create_table_sql = match driver {
        TestDriver::Postgres => "CREATE TEMPORARY TABLE many_columns_test (id INTEGER PRIMARY KEY".to_string(),
        TestDriver::Mysql => "CREATE TEMPORARY TABLE IF NOT EXISTS many_columns_test (id INTEGER PRIMARY KEY".to_string(),
        TestDriver::Sqlite => "CREATE TEMPORARY TABLE many_columns_test (id INTEGER PRIMARY KEY".to_string(),
        _ => unreachable!(),
    };

    for i in 1..=50 {
        create_table_sql.push_str(&format!(", col{} INTEGER", i));
    }
    create_table_sql.push_str(")");

    execute_sql(conn.as_ref(), driver, &create_table_sql, &[]).await?;

    // Insert a row with values in all columns
    let mut insert_sql = "INSERT INTO many_columns_test (id".to_string();
    for i in 1..=50 {
        insert_sql.push_str(&format!(", col{}", i));
    }
    insert_sql.push_str(") VALUES (");

    if driver == TestDriver::Postgres {
        insert_sql.push_str("$1");
        for i in 2..=51 {
            insert_sql.push_str(&format!(", ${}", i));
        }
    } else {
        insert_sql.push_str("?");
        for _ in 2..=51 {
            insert_sql.push_str(", ?");
        }
    }
    insert_sql.push_str(")");

    let mut values = vec![Value::Int64(1)];
    for i in 1..=50 {
        values.push(Value::Int64(i));
    }

    execute_sql(conn.as_ref(), driver, &insert_sql, &values).await?;

    // Select the row
    let result = query_sql(
        conn.as_ref(),
        driver,
        "SELECT * FROM many_columns_test",
        &[],
    )
    .await?;

    assert_eq!(result.rows.len(), 1, "Should retrieve 1 row");
    assert_eq!(
        result.columns.len(),
        51,
        "Should have 51 columns (id + 50 cols)"
    );

    // Verify some column values
    let row = &result.rows[0];
    let id = row
        .get_by_name("id")
        .and_then(|v| v.as_i64())
        .context("Should get id")?;
    assert_eq!(id, 1, "ID should be 1");

    let col1 = row
        .get_by_name("col1")
        .and_then(|v| v.as_i64())
        .context("Should get col1")?;
    assert_eq!(col1, 1, "col1 should be 1");

    let col50 = row
        .get_by_name("col50")
        .and_then(|v| v.as_i64())
        .context("Should get col50")?;
    assert_eq!(col50, 50, "col50 should be 50");

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_large_update_many_rows(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE update_test (
                id INTEGER PRIMARY KEY,
                status TEXT,
                counter INTEGER
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS update_test (
                id INTEGER PRIMARY KEY,
                status TEXT,
                counter INTEGER
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE update_test (
                id INTEGER PRIMARY KEY,
                status TEXT,
                counter INTEGER
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Insert 5000 rows
    let batch_size = 100;
    let total_rows = 5000;

    for batch_start in (0..total_rows).step_by(batch_size) {
        let mut insert_sql = "INSERT INTO update_test (id, status, counter) VALUES ".to_string();
        let mut values = Vec::new();

        for i in 0..batch_size.min(total_rows - batch_start) {
            let id = batch_start + i;
            if i > 0 {
                insert_sql.push_str(", ");
            }

            if driver == TestDriver::Postgres {
                insert_sql.push_str(&format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            } else {
                insert_sql.push_str("(?, ?, ?)");
            }

            values.push(Value::Int64(id as i64));
            values.push(Value::String("pending".to_string()));
            values.push(Value::Int64(0));
        }

        execute_sql(conn.as_ref(), driver, &insert_sql, &values).await?;
    }

    // Update all rows
    let affected = execute_sql(
        conn.as_ref(),
        driver,
        if driver == TestDriver::Postgres {
            "UPDATE update_test SET status = $1, counter = counter + $2"
        } else {
            "UPDATE update_test SET status = ?, counter = counter + ?"
        },
        &[Value::String("completed".to_string()), Value::Int64(1)],
    )
    .await?;

    assert_eq!(
        affected, total_rows as u64,
        "Should have updated 5000 rows"
    );

    // Verify updates
    let result = query_sql(
        conn.as_ref(),
        driver,
        if driver == TestDriver::Postgres {
            "SELECT COUNT(*) as count FROM update_test WHERE status = $1 AND counter = $2"
        } else {
            "SELECT COUNT(*) as count FROM update_test WHERE status = ? AND counter = ?"
        },
        &[Value::String("completed".to_string()), Value::Int64(1)],
    )
    .await?;

    let count = result.rows[0]
        .get_by_name("count")
        .and_then(|v| v.as_i64())
        .context("Should get count")?;
    assert_eq!(
        count, total_rows as i64,
        "All 5000 rows should be updated"
    );

    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_large_delete_many_rows(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Create temporary table
    let create_table_sql = match driver {
        TestDriver::Postgres => {
            "CREATE TEMPORARY TABLE delete_test (
                id INTEGER PRIMARY KEY,
                category TEXT
            )"
        }
        TestDriver::Mysql => {
            "CREATE TEMPORARY TABLE IF NOT EXISTS delete_test (
                id INTEGER PRIMARY KEY,
                category TEXT
            )"
        }
        TestDriver::Sqlite => {
            "CREATE TEMPORARY TABLE delete_test (
                id INTEGER PRIMARY KEY,
                category TEXT
            )"
        }
        _ => unreachable!(),
    };

    execute_sql(conn.as_ref(), driver, create_table_sql, &[]).await?;

    // Insert 3000 rows
    let batch_size = 100;
    let total_rows = 3000;

    for batch_start in (0..total_rows).step_by(batch_size) {
        let mut insert_sql = "INSERT INTO delete_test (id, category) VALUES ".to_string();
        let mut values = Vec::new();

        for i in 0..batch_size.min(total_rows - batch_start) {
            let id = batch_start + i;
            if i > 0 {
                insert_sql.push_str(", ");
            }

            if driver == TestDriver::Postgres {
                insert_sql.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
            } else {
                insert_sql.push_str("(?, ?)");
            }

            values.push(Value::Int64(id as i64));
            // Mark half as "old" for deletion
            values.push(Value::String(if id < 1500 {
                "old".to_string()
            } else {
                "new".to_string()
            }));
        }

        execute_sql(conn.as_ref(), driver, &insert_sql, &values).await?;
    }

    // Delete 1500 "old" rows
    let affected = execute_sql(
        conn.as_ref(),
        driver,
        if driver == TestDriver::Postgres {
            "DELETE FROM delete_test WHERE category = $1"
        } else {
            "DELETE FROM delete_test WHERE category = ?"
        },
        &[Value::String("old".to_string())],
    )
    .await?;

    assert_eq!(affected, 1500, "Should have deleted 1500 rows");

    // Verify remaining rows
    let result = query_sql(
        conn.as_ref(),
        driver,
        "SELECT COUNT(*) as count FROM delete_test",
        &[],
    )
    .await?;

    let count = result.rows[0]
        .get_by_name("count")
        .and_then(|v| v.as_i64())
        .context("Should get count")?;
    assert_eq!(count, 1500, "Should have 1500 rows remaining");

    // Verify only "new" rows remain
    let result = query_sql(
        conn.as_ref(),
        driver,
        if driver == TestDriver::Postgres {
            "SELECT COUNT(*) as count FROM delete_test WHERE category = $1"
        } else {
            "SELECT COUNT(*) as count FROM delete_test WHERE category = ?"
        },
        &[Value::String("new".to_string())],
    )
    .await?;

    let new_count = result.rows[0]
        .get_by_name("count")
        .and_then(|v| v.as_i64())
        .context("Should get new_count")?;
    assert_eq!(new_count, 1500, "All remaining rows should be 'new'");

    Ok(())
}

#[tokio::test]
async fn integration_test_large_data_handling_works() -> Result<()> {
    let conn = test_connection(TestDriver::Sqlite).await?;

    // Create a test table
    conn.execute(
        "CREATE TEMPORARY TABLE large_data (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    // Insert 100 rows
    for i in 0..100 {
        conn.execute(
            "INSERT INTO large_data (id, data) VALUES (?, ?)",
            &[
                Value::Int64(i),
                Value::String(format!("data_value_{}", i)),
            ],
        )
        .await?;
    }

    // Query all rows
    let result = conn.query("SELECT * FROM large_data", &[]).await?;

    assert_eq!(result.rows.len(), 100, "Should have 100 rows");

    // Verify first and last
    let first_id = result.rows[0]
        .get_by_name("id")
        .and_then(|v| v.as_i64())
        .context("Should get first id")?;
    assert_eq!(first_id, 0);

    let last_id = result.rows[99]
        .get_by_name("id")
        .and_then(|v| v.as_i64())
        .context("Should get last id")?;
    assert_eq!(last_id, 99);

    Ok(())
}
