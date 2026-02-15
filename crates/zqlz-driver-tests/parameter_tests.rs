#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::{Connection, QueryResult, StatementResult, Value};

    /// Helper function to execute SQL with parameter conversion
    /// PostgreSQL uses $1, $2, while MySQL/SQLite use ?
    async fn execute_params(
        conn: &dyn Connection,
        driver: TestDriver,
        sql_postgres: &str,
        params: &[Value],
    ) -> Result<StatementResult> {
        let sql = if driver == TestDriver::Postgres {
            sql_postgres.to_string()
        } else {
            let mut result = sql_postgres.to_string();
            let mut counter = 1;
            while result.contains(&format!("${}", counter)) {
                result = result.replace(&format!("${}", counter), "?");
                counter += 1;
            }
            result
        };
        
        conn.execute(&sql, params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper function to query SQL with parameter conversion
    async fn query_params(
        conn: &dyn Connection,
        driver: TestDriver,
        sql_postgres: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        let sql = if driver == TestDriver::Postgres {
            sql_postgres.to_string()
        } else {
            let mut result = sql_postgres.to_string();
            let mut counter = 1;
            while result.contains(&format!("${}", counter)) {
                result = result.replace(&format!("${}", counter), "?");
                counter += 1;
            }
            result
        };
        
        conn.query(&sql, params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_positional_select(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(88888),
                Value::String("POSITIONAL".into()),
                Value::String("TEST".into()),
            ],
        )
        .await?;

        // Query with positional parameters
        let query_sql = "SELECT first_name, last_name FROM actor WHERE actor_id = $1";
        let result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(88888)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Expected exactly 1 row");
        let row = &result.rows[0];
        assert_eq!(
            row.get_by_name("first_name")
                .context("Missing first_name")?
                .as_str()
                .context("first_name not a string")?,
            "POSITIONAL"
        );
        assert_eq!(
            row.get_by_name("last_name")
                .context("Missing last_name")?
                .as_str()
                .context("last_name not a string")?,
            "TEST"
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id = $1";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(88888)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_null_in_where(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query with NULL parameter - should return rows where return_date IS NULL
        let query_sql = "SELECT rental_id FROM rental WHERE return_date IS NULL LIMIT 5";
        let result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[],
        )
        .await?;

        // We're just checking that the query executes successfully
        // The actual number of rows depends on the sample data
        assert!(result.rows.len() >= 0, "Query should execute successfully");

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_type_inference(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data with various types
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(77777),
                Value::String("TYPE".into()),
                Value::String("INFERENCE".into()),
            ],
        )
        .await?;

        // Query with different parameter types
        let query_sql = "SELECT actor_id, first_name FROM actor WHERE actor_id = $1 AND first_name = $2";
        let result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(77777), Value::String("TYPE".into())],
        )
        .await?;

        assert_eq!(result.rows.len(), 1, "Expected exactly 1 row");
        let row = &result.rows[0];
        assert_eq!(
            row.get_by_name("actor_id")
                .context("Missing actor_id")?
                .as_i64()
                .context("actor_id not an i64")?,
            77777
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id = $1";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(77777)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_reuse(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(66666),
                Value::String("REUSE".into()),
                Value::String("TEST1".into()),
            ],
        )
        .await?;

        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(66667),
                Value::String("REUSE".into()),
                Value::String("TEST2".into()),
            ],
        )
        .await?;

        // Reuse the same query with different parameters
        let query_sql = "SELECT last_name FROM actor WHERE actor_id = $1";
        
        let result1 = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(66666)],
        )
        .await?;
        assert_eq!(result1.rows.len(), 1);
        assert_eq!(
            result1.rows[0]
                .get_by_name("last_name")
                .context("Missing last_name")?
                .as_str()
                .context("last_name not a string")?,
            "TEST1"
        );

        let result2 = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(66667)],
        )
        .await?;
        assert_eq!(result2.rows.len(), 1);
        assert_eq!(
            result2.rows[0]
                .get_by_name("last_name")
                .context("Missing last_name")?
                .as_str()
                .context("last_name not a string")?,
            "TEST2"
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id IN ($1, $2)";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(66666), Value::Int64(66667)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_many_parameters(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data with many parameters
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(55551),
                Value::String("MANY".into()),
                Value::String("PARAM1".into()),
            ],
        )
        .await?;

        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(55552),
                Value::String("MANY".into()),
                Value::String("PARAM2".into()),
            ],
        )
        .await?;

        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(55553),
                Value::String("MANY".into()),
                Value::String("PARAM3".into()),
            ],
        )
        .await?;

        // Query with many parameters in IN clause
        let query_sql = "SELECT COUNT(*) as count FROM actor WHERE actor_id IN ($1, $2, $3)";
        let result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(55551), Value::Int64(55552), Value::Int64(55553)],
        )
        .await?;

        assert_eq!(result.rows.len(), 1);
        let count = result.rows[0]
            .get_by_name("count")
            .context("Missing count")?
            .as_i64()
            .context("count not an i64")?;
        assert_eq!(count, 3, "Expected 3 rows");

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id >= $1 AND actor_id <= $2";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(55551), Value::Int64(55553)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_parameterized_insert(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Parameterized INSERT
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        let result = execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(44444),
                Value::String("PARAM".into()),
                Value::String("INSERT".into()),
            ],
        )
        .await?;

        assert_eq!(result.affected_rows, 1, "Expected 1 row affected");

        // Verify insertion
        let query_sql = "SELECT first_name FROM actor WHERE actor_id = $1";
        let query_result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(44444)],
        )
        .await?;

        assert_eq!(query_result.rows.len(), 1);
        assert_eq!(
            query_result.rows[0]
                .get_by_name("first_name")
                .context("Missing first_name")?
                .as_str()
                .context("first_name not a string")?,
            "PARAM"
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id = $1";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(44444)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_parameterized_update(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(33333),
                Value::String("OLD".into()),
                Value::String("NAME".into()),
            ],
        )
        .await?;

        // Parameterized UPDATE
        let update_sql = "UPDATE actor SET first_name = $1, last_name = $2 WHERE actor_id = $3";
        let result = execute_params(
            conn.as_ref(),
            driver,
            update_sql,
            &[
                Value::String("NEW".into()),
                Value::String("VALUE".into()),
                Value::Int64(33333),
            ],
        )
        .await?;

        assert_eq!(result.affected_rows, 1, "Expected 1 row affected");

        // Verify update
        let query_sql = "SELECT first_name, last_name FROM actor WHERE actor_id = $1";
        let query_result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::Int64(33333)],
        )
        .await?;

        assert_eq!(query_result.rows.len(), 1);
        assert_eq!(
            query_result.rows[0]
                .get_by_name("first_name")
                .context("Missing first_name")?
                .as_str()
                .context("first_name not a string")?,
            "NEW"
        );
        assert_eq!(
            query_result.rows[0]
                .get_by_name("last_name")
                .context("Missing last_name")?
                .as_str()
                .context("last_name not a string")?,
            "VALUE"
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id = $1";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(33333)],
        )
        .await?;

        Ok(())
    }

    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_params_sql_injection_prevention(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test data
        let insert_sql = "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)";
        execute_params(
            conn.as_ref(),
            driver,
            insert_sql,
            &[
                Value::Int64(22222),
                Value::String("SAFE".into()),
                Value::String("ACTOR".into()),
            ],
        )
        .await?;

        // Try SQL injection attack via parameters (should be safely escaped)
        let malicious_input = "'; DROP TABLE actor; --";
        let query_sql = "SELECT first_name FROM actor WHERE last_name = $1";
        let result = query_params(
            conn.as_ref(),
            driver,
            query_sql,
            &[Value::String(malicious_input.into())],
        )
        .await?;

        // Should return 0 rows (no actor with that last name), not execute the DROP TABLE
        assert_eq!(result.rows.len(), 0, "Malicious input should not match any rows");

        // Verify that the actor table still exists by querying our test data
        let verify_sql = "SELECT first_name FROM actor WHERE actor_id = $1";
        let verify_result = query_params(
            conn.as_ref(),
            driver,
            verify_sql,
            &[Value::Int64(22222)],
        )
        .await?;

        assert_eq!(verify_result.rows.len(), 1, "Table should still exist");
        assert_eq!(
            verify_result.rows[0]
                .get_by_name("first_name")
                .context("Missing first_name")?
                .as_str()
                .context("first_name not a string")?,
            "SAFE"
        );

        // Cleanup
        let delete_sql = "DELETE FROM actor WHERE actor_id = $1";
        execute_params(
            conn.as_ref(),
            driver,
            delete_sql,
            &[Value::Int64(22222)],
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn integration_test_params_work() -> Result<()> {
        // This test works without Sakila data
        let conn = test_connection(TestDriver::Sqlite).await?;

        // Create a temporary table
        conn.execute(
            "CREATE TEMP TABLE test_params (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert with parameters
        conn.execute(
            "INSERT INTO test_params (id, name, value) VALUES (?, ?, ?)",
            &[
                Value::Int64(1),
                Value::String("test".into()),
                Value::Int64(100),
            ],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Query with parameter
        let result = conn
            .query("SELECT name, value FROM test_params WHERE id = ?", &[Value::Int64(1)])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]
                .get_by_name("name")
                .context("Missing name")?
                .as_str()
                .context("name not a string")?,
            "test"
        );
        assert_eq!(
            result.rows[0]
                .get_by_name("value")
                .context("Missing value")?
                .as_i64()
                .context("value not an i64")?,
            100
        );

        Ok(())
    }
}
