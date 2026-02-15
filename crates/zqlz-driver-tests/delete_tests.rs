//! DELETE operation tests across all SQL drivers.
//!
//! Tests DELETE scenarios using Sakila/Pagila tables, including:
//! - Single row deletion
//! - Multiple row deletion
//! - Empty result sets (no matching rows)
//! - Foreign key constraint violations
//! - Affected row count verification
//!
//! All tests are parameterized across PostgreSQL, MySQL, and SQLite.

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::{Connection, QueryResult, StatementResult, Value};

    /// Helper to execute SQL that works across drivers (handles parameter syntax).
    async fn execute_sql(
        conn: &dyn Connection,
        sql: &str,
        params: &[Value],
        driver: TestDriver,
    ) -> Result<StatementResult> {
        let sql = match driver {
            TestDriver::Postgres => sql.to_string(),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut result = sql.to_string();
                for i in (1..=params.len()).rev() {
                    result = result.replace(&format!("${}", i), "?");
                }
                result
            }
            TestDriver::Redis => sql.to_string(),
        };
        conn.execute(&sql, params).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL that works across drivers.
    async fn query_sql(
        conn: &dyn Connection,
        sql: &str,
        params: &[Value],
        driver: TestDriver,
    ) -> Result<QueryResult> {
        let sql = match driver {
            TestDriver::Postgres => sql.to_string(),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut result = sql.to_string();
                for i in (1..=params.len()).rev() {
                    result = result.replace(&format!("${}", i), "?");
                }
                result
            }
            TestDriver::Redis => sql.to_string(),
        };
        conn.query(&sql, params).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_actor_by_name(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test actor
        execute_sql(
            conn.as_ref(),
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (99999, $1, $2, CURRENT_TIMESTAMP)",
            &[
                Value::String("DELETE".to_string()),
                Value::String("TESTACTOR".to_string()),
            ],
            driver,
        )
        .await
        .context("Failed to insert test actor")?;

        // Delete the actor by name
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM actor WHERE last_name = $1",
            &[Value::String("TESTACTOR".to_string())],
            driver,
        )
        .await
        .context("Failed to delete actor")?;

        // Verify affected rows
        assert_eq!(
            result.affected_rows, 1,
            "Should have deleted 1 row, got {}",
            result.affected_rows
        );

        // Verify actor was deleted
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT actor_id FROM actor WHERE actor_id = $1",
            &[Value::Int64(99999)],
            driver,
        )
        .await
        .context("Failed to verify deletion")?;

        assert_eq!(
            query_result.rows.len(),
            0,
            "Actor should have been deleted"
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_multiple_actors_by_prefix(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test actors with same prefix
        for i in 0..3 {
            execute_sql(
                conn.as_ref(),
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
                &[
                    Value::Int64(90000 + i),
                    Value::String(format!("TEST{}", i)),
                    Value::String("BATCH".to_string()),
                ],
                driver,
            )
            .await
            .with_context(|| format!("Failed to insert test actor {}", i))?;
        }

        // Delete all actors with prefix using LIKE
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM actor WHERE last_name = $1",
            &[Value::String("BATCH".to_string())],
            driver,
        )
        .await
        .context("Failed to delete actors")?;

        // Verify affected rows
        assert_eq!(
            result.affected_rows, 3,
            "Should have deleted 3 rows, got {}",
            result.affected_rows
        );

        // Verify all actors were deleted
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT actor_id FROM actor WHERE actor_id >= $1 AND actor_id <= $2",
            &[
                Value::Int64(90000),
                Value::Int64(90002),
            ],
            driver,
        )
        .await
        .context("Failed to verify deletion")?;

        assert_eq!(
            query_result.rows.len(),
            0,
            "All actors should have been deleted"
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_no_matching_rows(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Delete actor with nonexistent ID
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(999999999)],
            driver,
        )
        .await
        .context("Failed to delete actor")?;

        // Verify zero affected rows
        assert_eq!(
            result.affected_rows, 0,
            "Should have deleted 0 rows, got {}",
            result.affected_rows
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_film_referenced_by_inventory_fk_violation(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find a film that has inventory records
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT film_id FROM inventory LIMIT 1",
            &[],
            driver,
        )
        .await
        .context("Failed to find film with inventory")?;

        if query_result.rows.is_empty() {
            // Skip test if no inventory exists
            return Ok(());
        }

        let film_id = query_result.rows[0]
            .get_by_name("film_id")
            .context("film_id column not found")?
            .as_i64()
            .context("film_id is not an integer")?;

        // Try to delete the film (should fail due to FK constraint)
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(film_id)],
            driver,
        )
        .await;

        // Should fail with FK constraint violation
        assert!(
            result.is_err(),
            "DELETE should fail due to foreign key constraint"
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_inventory_without_dependent_rentals(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find a film to create inventory for
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT film_id FROM film LIMIT 1",
            &[],
            driver,
        )
        .await
        .context("Failed to find film")?;

        if query_result.rows.is_empty() {
            return Ok(());
        }

        let film_id = query_result.rows[0]
            .get_by_name("film_id")
            .context("film_id column not found")?
            .as_i64()
            .context("film_id is not an integer")?;

        // Find a store
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT store_id FROM store LIMIT 1",
            &[],
            driver,
        )
        .await
        .context("Failed to find store")?;

        if query_result.rows.is_empty() {
            return Ok(());
        }

        let store_id = query_result.rows[0]
            .get_by_name("store_id")
            .context("store_id column not found")?
            .as_i64()
            .context("store_id is not an integer")?;

        // Insert test inventory
        execute_sql(
            conn.as_ref(),
            "INSERT INTO inventory (inventory_id, film_id, store_id, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99999),
                Value::Int64(film_id),
                Value::Int64(store_id),
            ],
            driver,
        )
        .await
        .context("Failed to insert test inventory")?;

        // Delete the inventory (should succeed since no rentals reference it)
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM inventory WHERE inventory_id = $1",
            &[Value::Int64(99999)],
            driver,
        )
        .await
        .context("Failed to delete inventory")?;

        // Verify affected rows
        assert_eq!(
            result.affected_rows, 1,
            "Should have deleted 1 row, got {}",
            result.affected_rows
        );

        // Verify inventory was deleted
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT inventory_id FROM inventory WHERE inventory_id = $1",
            &[Value::Int64(99999)],
            driver,
        )
        .await
        .context("Failed to verify deletion")?;

        assert_eq!(
            query_result.rows.len(),
            0,
            "Inventory should have been deleted"
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_customer_with_rentals_fk_violation(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find a customer that has rental records
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT customer_id FROM rental LIMIT 1",
            &[],
            driver,
        )
        .await
        .context("Failed to find customer with rentals")?;

        if query_result.rows.is_empty() {
            // Skip test if no rentals exist
            return Ok(());
        }

        let customer_id = query_result.rows[0]
            .get_by_name("customer_id")
            .context("customer_id column not found")?
            .as_i64()
            .context("customer_id is not an integer")?;

        // Try to delete the customer (should fail due to FK constraint)
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM customer WHERE customer_id = $1",
            &[Value::Int64(customer_id)],
            driver,
        )
        .await;

        // Should fail with FK constraint violation
        assert!(
            result.is_err(),
            "DELETE should fail due to foreign key constraint"
        );

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_affected_rows_count(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert multiple test actors
        let ids = [90010, 90011, 90012, 90013, 90014];
        for id in &ids {
            execute_sql(
                conn.as_ref(),
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
                &[
                    Value::Int64(*id),
                    Value::String("COUNT".to_string()),
                    Value::String("TEST".to_string()),
                ],
                driver,
            )
            .await
            .with_context(|| format!("Failed to insert test actor {}", id))?;
        }

        // Delete with range condition
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM actor WHERE actor_id >= $1 AND actor_id <= $2",
            &[
                Value::Int64(90010),
                Value::Int64(90014),
            ],
            driver,
        )
        .await
        .context("Failed to delete actors")?;

        // Verify affected rows count
        assert_eq!(
            result.affected_rows, 5,
            "Should have deleted 5 rows, got {}",
            result.affected_rows
        );

        // Verify all actors were deleted
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT COUNT(*) as count FROM actor WHERE actor_id >= $1 AND actor_id <= $2",
            &[
                Value::Int64(90010),
                Value::Int64(90014),
            ],
            driver,
        )
        .await
        .context("Failed to verify deletion")?;

        let count = query_result.rows[0]
            .get_by_name("count")
            .context("count column not found")?
            .as_i64()
            .context("count is not an integer")?;

        assert_eq!(count, 0, "All actors should have been deleted");

        Ok(())
    }

    /// Integration test that works without Sakila data - tests basic DELETE functionality.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_delete_works(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create a temporary table
        let create_sql = match driver {
            TestDriver::Postgres => {
                "CREATE TEMP TABLE test_delete_temp (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            }
            TestDriver::Mysql => {
                "CREATE TEMPORARY TABLE test_delete_temp (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            }
            TestDriver::Sqlite => {
                "CREATE TEMP TABLE test_delete_temp (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            }
            TestDriver::Redis => return Ok(()), // Skip for Redis
        };

        execute_sql(conn.as_ref(), create_sql, &[], driver)
            .await
            .context("Failed to create temporary table")?;

        // Insert test data
        execute_sql(
            conn.as_ref(),
            "INSERT INTO test_delete_temp (id, name) VALUES (1, $1), (2, $2), (3, $3)",
            &[
                Value::String("Alice".to_string()),
                Value::String("Bob".to_string()),
                Value::String("Charlie".to_string()),
            ],
            driver,
        )
        .await
        .context("Failed to insert test data")?;

        // Delete one row
        let result = execute_sql(
            conn.as_ref(),
            "DELETE FROM test_delete_temp WHERE id = $1",
            &[Value::Int64(2)],
            driver,
        )
        .await
        .context("Failed to delete row")?;

        // Verify affected rows
        assert_eq!(
            result.affected_rows, 1,
            "Should have deleted 1 row, got {}",
            result.affected_rows
        );

        // Verify deletion
        let query_result = query_sql(
            conn.as_ref(),
            "SELECT COUNT(*) as count FROM test_delete_temp",
            &[],
            driver,
        )
        .await
        .context("Failed to count rows")?;

        let count = query_result.rows[0]
            .get_by_name("count")
            .context("count column not found")?
            .as_i64()
            .context("count is not an integer")?;

        assert_eq!(count, 2, "Should have 2 rows remaining after DELETE");

        Ok(())
    }
}
