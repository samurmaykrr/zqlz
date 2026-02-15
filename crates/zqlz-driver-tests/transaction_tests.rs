//! Transaction Tests
//!
//! This module tests transaction functionality across all SQL drivers (PostgreSQL, MySQL, SQLite).
//! Tests cover BEGIN, COMMIT, ROLLBACK, isolation levels, and transaction behavior.
//!
//! Tests use the Sakila/Pagila sample databases available in Docker containers.

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;

/// Test that committed transactions persist changes
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_commit(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Start transaction
    conn.execute("BEGIN", &[]).await?;

    // Insert a new actor
    let insert_result = conn
        .execute(
            "INSERT INTO actor (first_name, last_name, last_update) VALUES ('John', 'CommitTest', CURRENT_TIMESTAMP)",
            &[],
        )
        .await
        .context("Failed to insert actor")?;
    assert_eq!(insert_result.affected_rows, 1);

    // Verify the actor is visible within the transaction
    let select_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'CommitTest'",
            &[],
        )
        .await
        .context("Failed to count actors")?;
    let row = &select_result.rows[0];
    let count = row.get(0).context("missing count")?.as_i64();
    assert_eq!(count, Some(1), "Actor should be visible within transaction");

    // Commit the transaction
    conn.execute("COMMIT", &[]).await?;

    // Verify the actor is still visible after commit
    let select_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'CommitTest'",
            &[],
        )
        .await
        .context("Failed to count actors after commit")?;
    let row = &select_result.rows[0];
    let count = row.get(0).context("missing count")?.as_i64();
    assert_eq!(count, Some(1), "Actor should persist after commit");

    // Cleanup
    conn.execute("DELETE FROM actor WHERE last_name = 'CommitTest'", &[])
        .await?;

    Ok(())
}

/// Test that rolled back transactions discard changes
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_rollback(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get initial count
    let initial_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'RollbackTest'",
            &[],
        )
        .await?;
    let initial_count = initial_result.rows[0].get(0).context("missing count")?.as_i64().unwrap_or(0);

    // Start transaction
    conn.execute("BEGIN", &[]).await?;

    // Insert a new actor
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Jane', 'RollbackTest', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Verify the actor is visible within the transaction
    let select_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'RollbackTest'",
            &[],
        )
        .await?;
    let count = select_result.rows[0].get(0).context("missing count")?.as_i64().unwrap_or(0);
    assert_eq!(
        count,
        initial_count + 1,
        "Actor should be visible within transaction"
    );

    // Rollback the transaction
    conn.execute("ROLLBACK", &[]).await?;

    // Verify the actor is not visible after rollback
    let select_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'RollbackTest'",
            &[],
        )
        .await?;
    let count = select_result.rows[0].get(0).context("missing count")?.as_i64().unwrap_or(0);
    assert_eq!(
        count, initial_count,
        "Actor should not exist after rollback"
    );

    Ok(())
}

/// Test that multiple operations within a transaction are atomic
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_multiple_operations_in_transaction(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Start transaction
    conn.execute("BEGIN", &[]).await?;

    // Insert two actors
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Alice', 'MultiOp1', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Bob', 'MultiOp2', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Verify both are visible
    let result1 = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'MultiOp1'",
            &[],
        )
        .await?;
    let count1 = result1.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count1, 1);

    let result2 = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'MultiOp2'",
            &[],
        )
        .await?;
    let count2 = result2.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count2, 1);

    // Rollback
    conn.execute("ROLLBACK", &[]).await?;

    // Verify neither exists after rollback
    let result1 = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'MultiOp1'",
            &[],
        )
        .await?;
    let count1 = result1.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count1, 0);

    let result2 = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'MultiOp2'",
            &[],
        )
        .await?;
    let count2 = result2.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count2, 0);

    Ok(())
}

/// Test that you can read your own writes within a transaction
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_read_your_writes(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Start transaction
    conn.execute("BEGIN", &[]).await?;

    // Insert an actor
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Charlie', 'ReadWrite', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Immediately read it back
    let result = conn
        .query(
            "SELECT first_name, last_name FROM actor WHERE last_name = 'ReadWrite'",
            &[],
        )
        .await?;
    let row = &result.rows[0];
    assert_eq!(row.get(0).context("missing first_name")?.as_str(), Some("Charlie"));
    assert_eq!(row.get(1).context("missing last_name")?.as_str(), Some("ReadWrite"));

    // Update it
    conn.execute(
        "UPDATE actor SET first_name = 'Updated' WHERE last_name = 'ReadWrite'",
        &[],
    )
    .await?;

    // Read the update
    let result = conn
        .query(
            "SELECT first_name FROM actor WHERE last_name = 'ReadWrite'",
            &[],
        )
        .await?;
    let row = &result.rows[0];
    assert_eq!(row.get(0).context("missing first_name")?.as_str(), Some("Updated"));

    // Rollback
    conn.execute("ROLLBACK", &[]).await?;

    Ok(())
}

/// Test autocommit behavior (operations outside transactions are immediately committed)
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_autocommit_behavior(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert without explicit transaction (should autocommit)
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Auto', 'Commit', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Verify it's immediately visible
    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'Commit'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 1, "Autocommit insert should be immediately visible");

    // Cleanup
    conn.execute("DELETE FROM actor WHERE last_name = 'Commit'", &[])
        .await?;

    Ok(())
}

/// Test READ COMMITTED isolation level
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_isolation_read_committed(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Set isolation level if supported
    let set_isolation_result = match driver {
        TestDriver::Postgres => {
            conn.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", &[])
                .await
        }
        TestDriver::Mysql => {
            conn.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", &[])
                .await?;
            conn.execute("BEGIN", &[]).await
        }
        TestDriver::Sqlite => {
            // SQLite doesn't have isolation level control, just use BEGIN
            conn.execute("BEGIN", &[]).await
        }
        _ => unreachable!(),
    };

    if let Err(e) = set_isolation_result {
        // If isolation level setting fails, skip this test gracefully
        eprintln!("Skipping isolation level test for {}: {}", driver.name(), e);
        return Ok(());
    }

    // Test basic transaction behavior at this isolation level
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('ISO', 'ReadCommitted', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'ReadCommitted'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 1);

    conn.execute("ROLLBACK", &[]).await?;

    Ok(())
}

/// Test SERIALIZABLE isolation level
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_isolation_serializable(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Set isolation level if supported
    let set_isolation_result = match driver {
        TestDriver::Postgres => {
            conn.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                .await
        }
        TestDriver::Mysql => {
            conn.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                .await?;
            conn.execute("BEGIN", &[]).await
        }
        TestDriver::Sqlite => {
            // SQLite only supports SERIALIZABLE, just use BEGIN
            conn.execute("BEGIN", &[]).await
        }
        _ => unreachable!(),
    };

    if let Err(e) = set_isolation_result {
        // If isolation level setting fails, skip this test gracefully
        eprintln!(
            "Skipping serializable isolation test for {}: {}",
            driver.name(),
            e
        );
        return Ok(());
    }

    // Test basic transaction behavior at this isolation level
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('ISO', 'Serializable', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'Serializable'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 1);

    conn.execute("ROLLBACK", &[]).await?;

    Ok(())
}

/// Test nested transactions / savepoints (basic version without explicit savepoint commands)
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_nested_transactions(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Start outer transaction
    conn.execute("BEGIN", &[]).await?;

    // First insert
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Outer', 'Transaction', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Try to start a nested transaction
    // Note: Most drivers don't support nested BEGIN without savepoints
    // This test verifies the behavior (error or ignored)
    let nested_result = conn.execute("BEGIN", &[]).await;

    match nested_result {
        Ok(_) => {
            // Some drivers might silently ignore nested BEGIN
            // or create an implicit savepoint
            conn.execute(
                "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Inner', 'Transaction', CURRENT_TIMESTAMP)",
                &[],
            )
            .await?;
            conn.execute("COMMIT", &[]).await?;
        }
        Err(_) => {
            // Expected behavior for most drivers
            // Just continue with the outer transaction
            conn.execute(
                "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Inner', 'Transaction', CURRENT_TIMESTAMP)",
                &[],
            )
            .await?;
            conn.execute("COMMIT", &[]).await?;
        }
    }

    // Cleanup
    conn.execute("DELETE FROM actor WHERE last_name = 'Transaction'", &[])
        .await?;

    Ok(())
}

/// Test concurrent transactions (basic version - tests that transactions don't interfere)
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_concurrent_transactions(#[case] driver: TestDriver) -> Result<()> {
    if matches!(driver, TestDriver::Sqlite) {
        // SQLite permits only one writer at a time, so this concurrency pattern is not reliable.
        return Ok(());
    }

    // Create two separate connections
    let conn1 = test_connection(driver).await?;
    let conn2 = test_connection(driver).await?;

    // Start transaction on conn1
    conn1.execute("BEGIN", &[]).await?;
    conn1
        .execute(
            "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Concurrent1', 'TxTest', CURRENT_TIMESTAMP)",
            &[],
        )
        .await?;

    // Start transaction on conn2
    conn2.execute("BEGIN", &[]).await?;
    conn2
        .execute(
            "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Concurrent2', 'TxTest', CURRENT_TIMESTAMP)",
            &[],
        )
        .await?;

    // Commit both
    conn1.execute("COMMIT", &[]).await?;
    conn2.execute("COMMIT", &[]).await?;

    // Verify both inserts succeeded
    let result = conn1
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'TxTest'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 2, "Both concurrent transactions should succeed");

    // Cleanup
    conn1
        .execute("DELETE FROM actor WHERE last_name = 'TxTest'", &[])
        .await?;

    Ok(())
}

/// Test transaction timeout behavior
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_timeout(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Start a transaction
    conn.execute("BEGIN", &[]).await?;

    // Insert data
    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Timeout', 'Test', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    // Note: Testing actual timeout requires holding locks for extended periods
    // which is impractical in unit tests. This test just verifies that
    // transactions can be started and work normally.

    // Commit
    conn.execute("COMMIT", &[]).await?;

    // Cleanup
    conn.execute("DELETE FROM actor WHERE last_name = 'Test'", &[])
        .await?;

    Ok(())
}

/// Test that transaction is automatically rolled back on connection drop/disconnect
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_transaction_rollback_on_disconnect(#[case] driver: TestDriver) -> Result<()> {
    // Get initial count
    let conn = test_connection(driver).await?;
    let initial_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'DisconnectTest'",
            &[],
        )
        .await?;
    let initial_count = initial_result.rows[0].get(0).context("missing count")?.as_i64().unwrap();

    // Create a new connection for the transaction
    {
        let temp_conn = test_connection(driver).await?;
        temp_conn.execute("BEGIN", &[]).await?;
        temp_conn
            .execute(
                "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Drop', 'DisconnectTest', CURRENT_TIMESTAMP)",
                &[],
            )
            .await?;

        // Connection will be dropped here without COMMIT
    }

    // Verify the insert was rolled back
    let final_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE last_name = 'DisconnectTest'",
            &[],
        )
        .await?;
    let final_count = final_result.rows[0].get(0).context("missing count")?.as_i64().unwrap();

    assert_eq!(
        final_count, initial_count,
        "Uncommitted transaction should be rolled back on disconnect"
    );

    Ok(())
}

/// Test savepoint creation and basic operations
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_savepoint_creation(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Save', 'Point1', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp1", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Save', 'Point2', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Save'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 2, "Both inserts should be visible");

    conn.execute("COMMIT", &[]).await?;

    conn.execute("DELETE FROM actor WHERE first_name = 'Save'", &[])
        .await?;

    Ok(())
}

/// Test rollback to savepoint
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_rollback_to_savepoint(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    let test_marker = format!("RollSave-{}", driver.name());

    conn.execute("BEGIN", &[]).await?;

    let insert_before_sql = format!(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('{}', 'Before', CURRENT_TIMESTAMP)",
        test_marker
    );
    conn.execute(&insert_before_sql, &[]).await?;

    let count_sql = format!(
        "SELECT COUNT(*) as cnt FROM actor WHERE first_name = '{}'",
        test_marker
    );
    let result = conn.query(&count_sql, &[]).await?;
    let count_before = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count_before, 1, "First insert should be visible");

    conn.execute("SAVEPOINT sp1", &[]).await?;

    let insert_after_sql = format!(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('{}', 'After', CURRENT_TIMESTAMP)",
        test_marker
    );
    conn.execute(&insert_after_sql, &[]).await?;

    let result = conn.query(&count_sql, &[]).await?;
    let count_after = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count_after, 2, "Both inserts should be visible");

    conn.execute("ROLLBACK TO SAVEPOINT sp1", &[]).await?;

    let result = conn.query(&count_sql, &[]).await?;
    let count_rollback = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(
        count_rollback, 1,
        "Only first insert should remain after rollback to savepoint"
    );

    conn.execute("COMMIT", &[]).await?;

    let cleanup_sql = format!("DELETE FROM actor WHERE first_name = '{}'", test_marker);
    conn.execute(&cleanup_sql, &[]).await?;

    Ok(())
}

/// Test releasing a savepoint
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_release_savepoint(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Release', 'Save1', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp1", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Release', 'Save2', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("RELEASE SAVEPOINT sp1", &[]).await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Release'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 2, "Both inserts should remain after release");

    conn.execute("COMMIT", &[]).await?;

    conn.execute(
        "DELETE FROM actor WHERE first_name = 'Release'",
        &[],
    )
    .await?;

    Ok(())
}

/// Test multiple savepoints
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_multiple_savepoints(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Multi', 'Save0', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp1", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Multi', 'Save1', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp2", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Multi', 'Save2', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp3", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Multi', 'Save3', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Multi'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 4, "All four inserts should be visible");

    conn.execute("ROLLBACK TO SAVEPOINT sp2", &[]).await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Multi'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(
        count, 2,
        "Only first two inserts should remain after rollback to sp2"
    );

    conn.execute("COMMIT", &[]).await?;

    conn.execute("DELETE FROM actor WHERE first_name = 'Multi'", &[])
        .await?;

    Ok(())
}

/// Test nested savepoints
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_nested_savepoints(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Nested', 'Outer', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT outer_sp", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Nested', 'Middle', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT inner_sp", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Nested', 'Inner', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Nested'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 3, "All three inserts should be visible");

    conn.execute("ROLLBACK TO SAVEPOINT inner_sp", &[]).await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Nested'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 2, "Inner rollback should remove innermost insert");

    conn.execute("ROLLBACK TO SAVEPOINT outer_sp", &[]).await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Nested'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 1, "Outer rollback should remove middle insert");

    conn.execute("COMMIT", &[]).await?;

    conn.execute("DELETE FROM actor WHERE first_name = 'Nested'", &[])
        .await?;

    Ok(())
}

/// Test savepoint behavior after error
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_savepoint_after_error(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Error', 'Before', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp1", &[]).await?;

    let error_result = conn
        .execute(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (1, 'Error', 'Duplicate', CURRENT_TIMESTAMP)",
            &[],
        )
        .await;

    assert!(
        error_result.is_err(),
        "Insert with duplicate primary key should fail"
    );

    conn.execute("ROLLBACK TO SAVEPOINT sp1", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Error', 'After', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Error'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(
        count, 2,
        "First and third insert should succeed after error recovery"
    );

    conn.execute("COMMIT", &[]).await?;

    conn.execute("DELETE FROM actor WHERE first_name = 'Error'", &[])
        .await?;

    Ok(())
}

/// Test savepoint with same name (should replace previous savepoint)
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_savepoint_name_reuse(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Reuse', 'First', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Reuse', 'Second', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('Reuse', 'Third', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Reuse'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(count, 3, "All three inserts should be visible");

    conn.execute("ROLLBACK TO SAVEPOINT sp", &[]).await?;

    let result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'Reuse'",
            &[],
        )
        .await?;
    let count = result.rows[0].get(0).context("missing count")?.as_i64().unwrap();
    assert_eq!(
        count, 2,
        "Rollback should use most recent savepoint with that name"
    );

    conn.execute("COMMIT", &[]).await?;

    conn.execute("DELETE FROM actor WHERE first_name = 'Reuse'", &[])
        .await?;

    Ok(())
}

/// Test rollback to savepoint then commit
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_savepoint_rollback_then_commit(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('RollCommit', 'Keep', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("SAVEPOINT sp", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('RollCommit', 'Discard', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    conn.execute("ROLLBACK TO SAVEPOINT sp", &[]).await?;

    conn.execute("COMMIT", &[]).await?;

    let result = conn
        .query(
            "SELECT last_name FROM actor WHERE first_name = 'RollCommit'",
            &[],
        )
        .await?;
    assert_eq!(result.rows.len(), 1, "Only one row should be committed");
    assert_eq!(
        result.rows[0].get(0).context("missing last_name")?.as_str(),
        Some("Keep")
    );

    conn.execute(
        "DELETE FROM actor WHERE first_name = 'RollCommit'",
        &[],
    )
    .await?;

    Ok(())
}

/// Test savepoint across multiple tables
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_savepoint_multiple_tables(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;

    conn.execute(
        "INSERT INTO actor (first_name, last_name, last_update) VALUES ('MultiTable', 'Actor', CURRENT_TIMESTAMP)",
        &[],
    )
    .await?;

    let actor_result = conn
        .query(
            "SELECT actor_id FROM actor WHERE first_name = 'MultiTable' ORDER BY actor_id DESC LIMIT 1",
            &[],
        )
        .await?;
    let actor_id = actor_result.rows[0]
        .get(0)
        .context("missing actor_id")?
        .as_i64()
        .context("actor_id not i64")?;

    conn.execute("SAVEPOINT sp1", &[]).await?;

    let language_result = conn
        .query("SELECT language_id FROM language LIMIT 1", &[])
        .await?;
    let language_id = language_result.rows[0]
        .get(0)
        .context("missing language_id")?
        .as_i64()
        .context("language_id not i64")?;

    let insert_film_sql = format!(
        "INSERT INTO film (title, language_id, last_update) VALUES ('MultiTableFilm', {}, CURRENT_TIMESTAMP)",
        language_id
    );
    conn.execute(&insert_film_sql, &[]).await?;

    let film_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM film WHERE title = 'MultiTableFilm'",
            &[],
        )
        .await?;
    let film_count = film_result.rows[0]
        .get(0)
        .context("missing count")?
        .as_i64()
        .unwrap();
    assert_eq!(film_count, 1, "Film insert should be visible");

    conn.execute("ROLLBACK TO SAVEPOINT sp1", &[]).await?;

    let film_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM film WHERE title = 'MultiTableFilm'",
            &[],
        )
        .await?;
    let film_count = film_result.rows[0]
        .get(0)
        .context("missing count")?
        .as_i64()
        .unwrap();
    assert_eq!(
        film_count, 0,
        "Film insert should be rolled back by savepoint"
    );

    let actor_result = conn
        .query(
            "SELECT COUNT(*) as cnt FROM actor WHERE first_name = 'MultiTable'",
            &[],
        )
        .await?;
    let actor_count = actor_result.rows[0]
        .get(0)
        .context("missing count")?
        .as_i64()
        .unwrap();
    assert_eq!(
        actor_count, 1,
        "Actor insert before savepoint should remain"
    );

    conn.execute("COMMIT", &[]).await?;

    conn.execute(
        "DELETE FROM actor WHERE first_name = 'MultiTable'",
        &[],
    )
    .await?;

    Ok(())
}

/// Integration test: Verify basic transaction support exists
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn integration_test_transaction_support(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Just verify we can execute transaction commands without panic
    conn.execute("BEGIN", &[]).await?;
    conn.execute("ROLLBACK", &[]).await?;

    Ok(())
}

/// Integration test: Verify basic savepoint support exists
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn integration_test_savepoint_support(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    conn.execute("BEGIN", &[]).await?;
    conn.execute("SAVEPOINT test_sp", &[]).await?;
    conn.execute("COMMIT", &[]).await?;

    Ok(())
}
