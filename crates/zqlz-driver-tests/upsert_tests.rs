//! UPSERT operation tests for different database drivers.
//!
//! This module tests INSERT ... ON CONFLICT ... DO UPDATE (PostgreSQL/SQLite)
//! and INSERT ... ON DUPLICATE KEY UPDATE (MySQL) operations across all SQL drivers.
//!
//! # Driver-Specific Syntax
//!
//! - **PostgreSQL**: `INSERT ... ON CONFLICT (column) DO UPDATE SET ...`
//! - **MySQL**: `INSERT ... ON DUPLICATE KEY UPDATE ...`
//! - **SQLite**: `INSERT ... ON CONFLICT (column) DO UPDATE SET ...` (since 3.24.0)
//!
//! # Test Coverage
//!
//! Tests verify that:
//! - UPSERT inserts new rows when no conflict exists
//! - UPSERT updates existing rows on conflict
//! - Syntax differences are handled correctly
//! - DO NOTHING variant works (PostgreSQL/SQLite)
//! - Subset column updates work
//! - RETURNING clause works (PostgreSQL only)
//!
//! # Usage
//!
//! Run all upsert tests:
//! ```bash
//! cargo test -p zqlz-driver-tests upsert_tests
//! ```
//!
//! Run specific test:
//! ```bash
//! cargo test -p zqlz-driver-tests test_upsert_insert_new_actor
//! ```

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper function to execute UPSERT statements with driver-specific syntax.
    ///
    /// Converts PostgreSQL $1, $2 parameter syntax to ? for MySQL/SQLite.
    async fn execute_upsert(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = match driver {
            TestDriver::Postgres => (sql.to_string(), params.to_vec()),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut param_num = 1;
                let mut converted_sql = sql.to_string();
                while converted_sql.contains(&format!("${}", param_num)) {
                    converted_sql =
                        converted_sql.replace(&format!("${}", param_num), "?");
                    param_num += 1;
                }
                (converted_sql, params.to_vec())
            }
            TestDriver::Redis => {
                anyhow::bail!("Redis does not support SQL UPSERT operations")
            }
        };

        conn.execute(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper function to query with driver-specific parameter syntax.
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = match driver {
            TestDriver::Postgres => (sql.to_string(), params.to_vec()),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut param_num = 1;
                let mut converted_sql = sql.to_string();
                while converted_sql.contains(&format!("${}", param_num)) {
                    converted_sql =
                        converted_sql.replace(&format!("${}", param_num), "?");
                    param_num += 1;
                }
                (converted_sql, params.to_vec())
            }
            TestDriver::Redis => {
                anyhow::bail!("Redis does not support SQL queries")
            }
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Test UPSERT that inserts a new actor when no conflict exists.
    ///
    /// Verifies that UPSERT behaves like INSERT when the row doesn't exist.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_upsert_insert_new_actor(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_id = 99991_i64;
        let first_name = "NewFirst";
        let last_name = "NewLast";

        let upsert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW()) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = EXCLUDED.first_name, \
                 last_name = EXCLUDED.last_name"
            }
            TestDriver::Mysql => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, NOW()) \
                 ON DUPLICATE KEY UPDATE \
                 first_name = VALUES(first_name), \
                 last_name = VALUES(last_name)"
            }
            TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now')) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = excluded.first_name, \
                 last_name = excluded.last_name"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        let result = conn
            .execute(
                upsert_sql,
                &[
                    Value::Int64(test_id),
                    Value::String(first_name.to_string()),
                    Value::String(last_name.to_string()),
                ],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Verify the row was inserted
        assert_eq!(
            result.affected_rows, 1,
            "UPSERT should insert 1 new row"
        );

        // Verify data
        let verify_result = query_sql(
            driver,
            &conn,
            "SELECT first_name, last_name FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        assert_eq!(verify_result.rows.len(), 1);
        let row = &verify_result.rows[0];
        assert_eq!(
            row.get_by_name("first_name")
                .context("first_name column missing")?
                .as_str()
                .context("first_name not a string")?,
            first_name
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("last_name column missing")?
                .as_str()
                .context("last_name not a string")?,
            last_name
        );

        // Cleanup
        execute_upsert(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Test UPSERT that updates an existing actor on conflict.
    ///
    /// Verifies that UPSERT behaves like UPDATE when the row already exists.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_upsert_update_existing_actor(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_id = 99992_i64;
        let first_name_initial = "InitialFirst";
        let last_name_initial = "InitialLast";
        let first_name_updated = "UpdatedFirst";
        let last_name_updated = "UpdatedLast";

        // First, insert initial data
        let insert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW())"
            }
            TestDriver::Mysql | TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now'))"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        execute_upsert(
            driver,
            &conn,
            insert_sql,
            &[
                Value::Int64(test_id),
                Value::String(first_name_initial.to_string()),
                Value::String(last_name_initial.to_string()),
            ],
        )
        .await?;

        // Now perform UPSERT which should update
        let upsert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW()) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = EXCLUDED.first_name, \
                 last_name = EXCLUDED.last_name"
            }
            TestDriver::Mysql => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, NOW()) \
                 ON DUPLICATE KEY UPDATE \
                 first_name = VALUES(first_name), \
                 last_name = VALUES(last_name)"
            }
            TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now')) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = excluded.first_name, \
                 last_name = excluded.last_name"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        let result = conn
            .execute(
                upsert_sql,
                &[
                    Value::Int64(test_id),
                    Value::String(first_name_updated.to_string()),
                    Value::String(last_name_updated.to_string()),
                ],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // MySQL returns 2 for affected_rows on duplicate key update (1 delete + 1 insert)
        // PostgreSQL and SQLite return 1
        assert!(
            result.affected_rows >= 1,
            "UPSERT should update at least 1 row"
        );

        // Verify data was updated
        let verify_result = query_sql(
            driver,
            &conn,
            "SELECT first_name, last_name FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        assert_eq!(verify_result.rows.len(), 1);
        let row = &verify_result.rows[0];
        assert_eq!(
            row.get_by_name("first_name")
                .context("first_name column missing")?
                .as_str()
                .context("first_name not a string")?,
            first_name_updated
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("last_name column missing")?
                .as_str()
                .context("last_name not a string")?,
            last_name_updated
        );

        // Cleanup
        execute_upsert(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Test UPSERT with DO NOTHING on conflict (PostgreSQL/SQLite).
    ///
    /// Verifies that ON CONFLICT DO NOTHING leaves the row unchanged.
    /// MySQL doesn't support DO NOTHING, so this test only runs on PostgreSQL/SQLite.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_upsert_do_nothing_on_conflict(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_id = 99993_i64;
        let first_name_initial = "InitialFirst";
        let last_name_initial = "InitialLast";
        let first_name_attempt = "AttemptFirst";
        let last_name_attempt = "AttemptLast";

        // First, insert initial data
        let insert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW())"
            }
            TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now'))"
            }
            _ => anyhow::bail!("Unsupported driver"),
        };

        execute_upsert(
            driver,
            &conn,
            insert_sql,
            &[
                Value::Int64(test_id),
                Value::String(first_name_initial.to_string()),
                Value::String(last_name_initial.to_string()),
            ],
        )
        .await?;

        // Now perform UPSERT with DO NOTHING
        let upsert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW()) \
                 ON CONFLICT (actor_id) DO NOTHING"
            }
            TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now')) \
                 ON CONFLICT (actor_id) DO NOTHING"
            }
            _ => anyhow::bail!("Unsupported driver"),
        };

        conn.execute(
            upsert_sql,
            &[
                Value::Int64(test_id),
                Value::String(first_name_attempt.to_string()),
                Value::String(last_name_attempt.to_string()),
            ],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Verify data was NOT updated (still has initial values)
        let verify_result = query_sql(
            driver,
            &conn,
            "SELECT first_name, last_name FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        assert_eq!(verify_result.rows.len(), 1);
        let row = &verify_result.rows[0];
        assert_eq!(
            row.get_by_name("first_name")
                .context("first_name column missing")?
                .as_str()
                .context("first_name not a string")?,
            first_name_initial,
            "DO NOTHING should preserve original first_name"
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("last_name column missing")?
                .as_str()
                .context("last_name not a string")?,
            last_name_initial,
            "DO NOTHING should preserve original last_name"
        );

        // Cleanup
        execute_upsert(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Test UPSERT updating only a subset of columns on conflict.
    ///
    /// Verifies that only specified columns are updated, others remain unchanged.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_upsert_update_subset_columns(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_id = 99994_i64;
        let first_name_initial = "InitialFirst";
        let last_name_initial = "InitialLast";
        let first_name_updated = "UpdatedFirst";

        // First, insert initial data
        let insert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW())"
            }
            TestDriver::Mysql | TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now'))"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        execute_upsert(
            driver,
            &conn,
            insert_sql,
            &[
                Value::Int64(test_id),
                Value::String(first_name_initial.to_string()),
                Value::String(last_name_initial.to_string()),
            ],
        )
        .await?;

        // Now perform UPSERT updating only first_name
        let upsert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES ($1, $2, $3, NOW()) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = EXCLUDED.first_name"
            }
            TestDriver::Mysql => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, NOW()) \
                 ON DUPLICATE KEY UPDATE \
                 first_name = VALUES(first_name)"
            }
            TestDriver::Sqlite => {
                "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                 VALUES (?, ?, ?, datetime('now')) \
                 ON CONFLICT (actor_id) DO UPDATE SET \
                 first_name = excluded.first_name"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        execute_upsert(
            driver,
            &conn,
            upsert_sql,
            &[
                Value::Int64(test_id),
                Value::String(first_name_updated.to_string()),
                Value::String("IgnoredLast".to_string()),
            ],
        )
        .await?;

        // Verify only first_name was updated, last_name unchanged
        let verify_result = query_sql(
            driver,
            &conn,
            "SELECT first_name, last_name FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        assert_eq!(verify_result.rows.len(), 1);
        let row = &verify_result.rows[0];
        assert_eq!(
            row.get_by_name("first_name")
                .context("first_name column missing")?
                .as_str()
                .context("first_name not a string")?,
            first_name_updated,
            "first_name should be updated"
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("last_name column missing")?
                .as_str()
                .context("last_name not a string")?,
            last_name_initial,
            "last_name should remain unchanged"
        );

        // Cleanup
        execute_upsert(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Test UPSERT with RETURNING clause (PostgreSQL only).
    ///
    /// Verifies that RETURNING clause returns inserted/updated data.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[tokio::test]
    async fn test_upsert_with_returning_if_supported(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_id = 99995_i64;
        let first_name = "ReturningFirst";
        let last_name = "ReturningLast";

        let upsert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
                          VALUES ($1, $2, $3, NOW()) \
                          ON CONFLICT (actor_id) DO UPDATE SET \
                          first_name = EXCLUDED.first_name, \
                          last_name = EXCLUDED.last_name \
                          RETURNING actor_id, first_name, last_name";

        let result = conn
            .query(
                upsert_sql,
                &[
                    Value::Int64(test_id),
                    Value::String(first_name.to_string()),
                    Value::String(last_name.to_string()),
                ],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // RETURNING should return the inserted row
        assert_eq!(result.rows.len(), 1, "RETURNING should return 1 row");
        let row = &result.rows[0];

        assert_eq!(
            row.get_by_name("actor_id")
                .context("actor_id column missing")?
                .as_i64()
                .context("actor_id not an integer")?,
            test_id
        );
        assert_eq!(
            row.get_by_name("first_name")
                .context("first_name column missing")?
                .as_str()
                .context("first_name not a string")?,
            first_name
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("last_name column missing")?
                .as_str()
                .context("last_name not a string")?,
            last_name
        );

        // Cleanup
        execute_upsert(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Integration test: Basic UPSERT functionality without requiring Sakila data.
    ///
    /// Creates a temporary table and tests basic UPSERT insert and update operations.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_upsert_works(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create temporary table
        let create_table_sql = match driver {
            TestDriver::Postgres => {
                "CREATE TEMP TABLE test_upsert (id INT PRIMARY KEY, name TEXT)"
            }
            TestDriver::Mysql => {
                "CREATE TEMPORARY TABLE test_upsert (id INT PRIMARY KEY, name TEXT)"
            }
            TestDriver::Sqlite => {
                "CREATE TEMP TABLE test_upsert (id INT PRIMARY KEY, name TEXT)"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        conn.execute(create_table_sql, &[])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Test insert (new row)
        let upsert_sql = match driver {
            TestDriver::Postgres => {
                "INSERT INTO test_upsert (id, name) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
            }
            TestDriver::Mysql => {
                "INSERT INTO test_upsert (id, name) VALUES (?, ?) \
                 ON DUPLICATE KEY UPDATE name = VALUES(name)"
            }
            TestDriver::Sqlite => {
                "INSERT INTO test_upsert (id, name) VALUES (?, ?) \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name"
            }
            TestDriver::Redis => anyhow::bail!("Redis does not support SQL"),
        };

        execute_upsert(
            driver,
            &conn,
            upsert_sql,
            &[Value::Int64(1), Value::String("Alice".to_string())],
        )
        .await?;

        // Verify insert
        let result = query_sql(
            driver,
            &conn,
            "SELECT name FROM test_upsert WHERE id = $1",
            &[Value::Int64(1)],
        )
        .await?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .get_by_name("name")
                .context("name column missing")?
                .as_str()
                .context("name not a string")?,
            "Alice"
        );

        // Test update (existing row)
        execute_upsert(
            driver,
            &conn,
            upsert_sql,
            &[Value::Int64(1), Value::String("Bob".to_string())],
        )
        .await?;

        // Verify update
        let result = query_sql(
            driver,
            &conn,
            "SELECT name FROM test_upsert WHERE id = $1",
            &[Value::Int64(1)],
        )
        .await?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .get_by_name("name")
                .context("name column missing")?
                .as_str()
                .context("name not a string")?,
            "Bob"
        );

        Ok(())
    }
}
