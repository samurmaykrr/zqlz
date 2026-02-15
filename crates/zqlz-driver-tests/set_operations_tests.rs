//! Query Tests - Set Operations
//!
//! Tests UNION, UNION ALL, INTERSECT, and EXCEPT operations across SQL drivers.
//! These tests use the Sakila/Pagila sample database tables.

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;

/// Test UNION operation removes duplicates
#[rstest]
#[tokio::test]
async fn test_set_union(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // UNION should remove duplicate rows
    let query = "
        SELECT first_name FROM actor WHERE actor_id = 1
        UNION
        SELECT first_name FROM actor WHERE actor_id = 1
    ";

    let result = conn.query(query, &[]).await?;

    // Should only have 1 row even though we selected the same row twice
    assert_eq!(result.rows.len(), 1, "UNION should remove duplicate rows");

    Ok(())
}

/// Test UNION ALL operation keeps duplicates
#[rstest]
#[tokio::test]
async fn test_set_union_all(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // UNION ALL should keep duplicate rows
    let query = "
        SELECT first_name FROM actor WHERE actor_id = 1
        UNION ALL
        SELECT first_name FROM actor WHERE actor_id = 1
    ";

    let result = conn.query(query, &[]).await?;

    // Should have 2 rows since UNION ALL keeps duplicates
    assert_eq!(
        result.rows.len(),
        2,
        "UNION ALL should keep duplicate rows"
    );

    Ok(())
}

/// Test INTERSECT operation returns common rows
#[rstest]
#[tokio::test]
async fn test_set_intersect(
    #[values(TestDriver::Postgres, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // INTERSECT should return only rows that appear in both queries
    // Note: MySQL doesn't support INTERSECT natively (need to use INNER JOIN workaround)
    let query = "
        SELECT first_name FROM actor WHERE actor_id <= 3
        INTERSECT
        SELECT first_name FROM actor WHERE actor_id >= 2 AND actor_id <= 5
    ";

    let result = conn.query(query, &[]).await?;

    // Should return rows with actor_id 2 and 3 (the intersection)
    assert!(
        result.rows.len() >= 2,
        "INTERSECT should return common rows"
    );

    Ok(())
}

/// Test INTERSECT on MySQL using workaround
#[rstest]
#[tokio::test]
async fn test_set_intersect_mysql(#[values(TestDriver::Mysql)] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // MySQL doesn't support INTERSECT, use INNER JOIN as workaround
    let query = "
        SELECT DISTINCT a1.first_name 
        FROM actor a1
        INNER JOIN actor a2 ON a1.first_name = a2.first_name
        WHERE a1.actor_id <= 3 AND a2.actor_id >= 2 AND a2.actor_id <= 5
    ";

    let result = conn.query(query, &[]).await?;

    // Should return common rows
    assert!(
        result.rows.len() >= 1,
        "MySQL INTERSECT workaround should return common rows"
    );

    Ok(())
}

/// Test EXCEPT operation returns difference
#[rstest]
#[tokio::test]
async fn test_set_except(
    #[values(TestDriver::Postgres, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // EXCEPT should return rows from first query that are NOT in second query
    // Note: MySQL doesn't support EXCEPT natively (need to use LEFT JOIN workaround)
    let query = "
        SELECT first_name FROM actor WHERE actor_id <= 5
        EXCEPT
        SELECT first_name FROM actor WHERE actor_id >= 4
    ";

    let result = conn.query(query, &[]).await?;

    // Should return rows with actor_id 1, 2, 3 (not 4 or 5)
    assert!(result.rows.len() >= 3, "EXCEPT should return difference");

    Ok(())
}

/// Test EXCEPT on MySQL using workaround
#[rstest]
#[tokio::test]
async fn test_set_except_mysql(#[values(TestDriver::Mysql)] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // MySQL doesn't support EXCEPT, use LEFT JOIN as workaround
    let query = "
        SELECT DISTINCT a1.first_name
        FROM actor a1
        LEFT JOIN actor a2 ON a1.first_name = a2.first_name AND a2.actor_id >= 4
        WHERE a1.actor_id <= 5 AND a2.actor_id IS NULL
    ";

    let result = conn.query(query, &[]).await?;

    // Should return rows from first set that are not in second set
    assert!(
        result.rows.len() >= 1,
        "MySQL EXCEPT workaround should return difference"
    );

    Ok(())
}

/// Test UNION with ORDER BY
#[rstest]
#[tokio::test]
async fn test_set_union_with_order_by(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // UNION with ORDER BY should return sorted results
    let query = "
        SELECT first_name FROM actor WHERE actor_id <= 2
        UNION
        SELECT first_name FROM actor WHERE actor_id >= 199
        ORDER BY first_name ASC
    ";

    let result = conn.query(query, &[]).await?;

    // Verify we got results
    assert!(
        result.rows.len() >= 2,
        "UNION with ORDER BY should return results"
    );

    // Verify results are ordered
    let first_name_0 = result.rows[0]
        .get_by_name("first_name")
        .context("Missing first_name column")?
        .as_str()
        .context("first_name should be string")?;

    let first_name_1 = result.rows[1]
        .get_by_name("first_name")
        .context("Missing first_name column")?
        .as_str()
        .context("first_name should be string")?;

    assert!(
        first_name_0 <= first_name_1,
        "Results should be ordered by first_name"
    );

    Ok(())
}

/// Test nested set operations
#[rstest]
#[tokio::test]
async fn test_set_nested_operations(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Test nested UNION operations
    let query = "
        SELECT first_name FROM actor WHERE actor_id = 1
        UNION
        SELECT first_name FROM actor WHERE actor_id = 2
        UNION
        SELECT first_name FROM actor WHERE actor_id = 3
    ";

    let result = conn.query(query, &[]).await?;

    // Should return 3 distinct names
    assert_eq!(
        result.rows.len(),
        3,
        "Nested UNION should return all distinct rows"
    );

    Ok(())
}

/// Test that UNION requires compatible column types
#[rstest]
#[tokio::test]
async fn test_set_union_compatible_types(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // UNION should work with compatible types (both strings)
    let query = "
        SELECT first_name FROM actor WHERE actor_id = 1
        UNION
        SELECT last_name FROM actor WHERE actor_id = 2
    ";

    let result = conn.query(query, &[]).await?;

    // Should return 2 rows (first name + last name)
    assert_eq!(
        result.rows.len(),
        2,
        "UNION should work with compatible types"
    );

    Ok(())
}

/// Test UNION with different column counts (should fail)
#[rstest]
#[tokio::test]
async fn test_set_union_mismatched_columns(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // UNION with mismatched column counts should fail
    let query = "
        SELECT first_name FROM actor WHERE actor_id = 1
        UNION
        SELECT first_name, last_name FROM actor WHERE actor_id = 2
    ";

    let result = conn.query(query, &[]).await;

    // Should fail with an error
    assert!(
        result.is_err(),
        "UNION with mismatched column counts should fail"
    );

    Ok(())
}

/// Integration test - basic UNION without Sakila data
#[rstest]
#[tokio::test]
async fn integration_test_set_operations_work(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Simple UNION that doesn't require Sakila tables
    let query = "
        SELECT 1 AS num, 'first' AS label
        UNION
        SELECT 2 AS num, 'second' AS label
    ";

    let result = conn.query(query, &[]).await?;

    // Should have 2 rows
    assert_eq!(result.rows.len(), 2, "UNION should return 2 rows");

    // Verify first row
    let row0_num = result.rows[0]
        .get_by_name("num")
        .context("Missing num column")?
        .as_i64()
        .context("num should be integer")?;
    assert_eq!(row0_num, 1, "First row num should be 1");

    // Verify second row
    let row1_num = result.rows[1]
        .get_by_name("num")
        .context("Missing num column")?
        .as_i64()
        .context("num should be integer")?;
    assert_eq!(row1_num, 2, "Second row num should be 2");

    Ok(())
}
