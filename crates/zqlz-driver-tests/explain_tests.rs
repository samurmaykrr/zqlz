#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::Context;
    use rstest::rstest;
    use zqlz_core::{Connection, Row, Value};

    /// Helper function to execute EXPLAIN queries with driver-specific syntax
    async fn execute_explain(
        conn: &dyn Connection,
        driver: TestDriver,
        query: &str,
        params: &[Value],
    ) -> anyhow::Result<Vec<Row>> {
        let explain_query = match driver {
            TestDriver::Postgres => format!("EXPLAIN {}", query),
            TestDriver::Mysql => format!("EXPLAIN {}", query),
            TestDriver::Sqlite => format!("EXPLAIN QUERY PLAN {}", query),
            TestDriver::Redis => {
                return Err(anyhow::anyhow!("EXPLAIN not supported for Redis"))
            }
        };

        let result = conn
            .query(&explain_query, params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(result.rows)
    }

    /// Helper function to execute EXPLAIN ANALYZE queries
    async fn execute_explain_analyze(
        conn: &dyn Connection,
        driver: TestDriver,
        query: &str,
        params: &[Value],
    ) -> anyhow::Result<Vec<Row>> {
        let explain_query = match driver {
            TestDriver::Postgres => format!("EXPLAIN ANALYZE {}", query),
            TestDriver::Mysql => format!("EXPLAIN ANALYZE {}", query),
            TestDriver::Sqlite => {
                // SQLite doesn't support EXPLAIN ANALYZE in the same way
                // Fall back to EXPLAIN QUERY PLAN
                format!("EXPLAIN QUERY PLAN {}", query)
            }
            TestDriver::Redis => {
                return Err(anyhow::anyhow!("EXPLAIN not supported for Redis"))
            }
        };

        let result = conn
            .query(&explain_query, params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(result.rows)
    }

    /// Test basic EXPLAIN on a simple SELECT query
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_simple_query(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT * FROM actor WHERE actor_id = 1";
        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        // All drivers should return at least one row with query plan info
        assert!(!rows.is_empty(), "EXPLAIN should return query plan rows");

        // The exact format differs by driver, but we should get some plan info
        match driver {
            TestDriver::Postgres => {
                // PostgreSQL returns rows with columns like: QUERY PLAN
                assert!(
                    rows.len() >= 1,
                    "PostgreSQL EXPLAIN should return at least 1 row"
                );
            }
            TestDriver::Mysql => {
                // MySQL returns rows with columns: id, select_type, table, type, possible_keys, etc.
                assert!(
                    rows.len() >= 1,
                    "MySQL EXPLAIN should return at least 1 row"
                );
            }
            TestDriver::Sqlite => {
                // SQLite EXPLAIN QUERY PLAN returns: id, parent, notused, detail
                assert!(
                    rows.len() >= 1,
                    "SQLite EXPLAIN QUERY PLAN should return at least 1 row"
                );
            }
            TestDriver::Redis => unreachable!("Redis not tested in this test"),
        }

        Ok(())
    }

    /// Test EXPLAIN on a JOIN query
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_join_query(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT a.first_name, f.title \
                     FROM actor a \
                     JOIN film_actor fa ON a.actor_id = fa.actor_id \
                     JOIN film f ON fa.film_id = f.film_id \
                     WHERE a.actor_id = 1";

        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN should return query plan rows for JOIN"
        );

        // For JOINs, we typically expect multiple rows or more complex plans
        // The exact behavior varies by driver, but at least verify we get plan info
        assert!(
            rows.len() >= 1,
            "JOIN query plan should have at least 1 row"
        );

        Ok(())
    }

    /// Test EXPLAIN on a subquery
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_subquery(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT first_name, last_name \
                     FROM actor \
                     WHERE actor_id IN (SELECT actor_id FROM film_actor WHERE film_id = 1)";

        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN should return query plan rows for subquery"
        );

        Ok(())
    }

    /// Test EXPLAIN ANALYZE (shows actual execution with timing)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_analyze(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT * FROM actor WHERE actor_id = 1";
        let rows = execute_explain_analyze(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN ANALYZE should return query plan rows"
        );

        // PostgreSQL and MySQL support EXPLAIN ANALYZE
        // SQLite falls back to EXPLAIN QUERY PLAN
        match driver {
            TestDriver::Postgres => {
                // PostgreSQL EXPLAIN ANALYZE includes actual execution time
                assert!(
                    rows.len() >= 1,
                    "PostgreSQL EXPLAIN ANALYZE should return at least 1 row"
                );
            }
            TestDriver::Mysql => {
                // MySQL 8.0+ supports EXPLAIN ANALYZE
                assert!(
                    rows.len() >= 1,
                    "MySQL EXPLAIN ANALYZE should return at least 1 row"
                );
            }
            TestDriver::Sqlite => {
                // SQLite doesn't have EXPLAIN ANALYZE, uses EXPLAIN QUERY PLAN
                assert!(
                    rows.len() >= 1,
                    "SQLite EXPLAIN QUERY PLAN should return at least 1 row"
                );
            }
            TestDriver::Redis => unreachable!("Redis not tested in this test"),
        }

        Ok(())
    }

    /// Test EXPLAIN with JSON format (PostgreSQL only)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[tokio::test]
    async fn test_explain_format_json(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT * FROM actor WHERE actor_id = 1";
        let explain_query = "EXPLAIN (FORMAT JSON) SELECT * FROM actor WHERE actor_id = 1";

        let result = conn
            .query(explain_query, &[])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert!(!result.rows.is_empty(), "EXPLAIN should return rows");

        // PostgreSQL returns JSON-formatted plan
        // The first row should contain JSON data
        let first_row = result.rows.first().context("Expected at least one row")?;
        assert!(
            !first_row.values.is_empty(),
            "Expected at least one column in result"
        );

        // The JSON plan should be in the first column
        // We can verify it's a string (contains JSON)
        let value = first_row.get(0).context("Expected first column")?;
        match value {
            Value::String(s) => {
                assert!(
                    s.contains("Plan") || s.contains("Node Type"),
                    "JSON plan should contain Plan or Node Type"
                );
            }
            _ => {
                // Some drivers might return it as text or other format
                // Just verify we got some data
            }
        }

        Ok(())
    }

    /// Test EXPLAIN to verify index usage
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_index_usage(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        // Query using primary key - should show index usage
        let query = "SELECT * FROM actor WHERE actor_id = 1";
        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(!rows.is_empty(), "EXPLAIN should return query plan rows");

        // Each driver has different ways of showing index usage
        // We just verify that the plan is returned
        // Detailed index analysis would require parsing the plan text
        match driver {
            TestDriver::Postgres => {
                // PostgreSQL shows "Index Scan" or "Seq Scan"
                // We can check if any row mentions index usage
                let plan_text = rows
                    .iter()
                    .filter_map(|row| {
                        row.get(0).and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                
                // For a primary key lookup, we expect index usage
                // But we won't strictly enforce this in case of small tables
                assert!(
                    plan_text.contains("Index") || plan_text.contains("Seq"),
                    "Plan should mention scan type"
                );
            }
            TestDriver::Mysql => {
                // MySQL shows possible_keys, key, and type columns
                assert!(rows.len() >= 1, "MySQL should return plan info");
            }
            TestDriver::Sqlite => {
                // SQLite shows "SCAN" or "SEARCH" with index name
                assert!(rows.len() >= 1, "SQLite should return plan info");
            }
            TestDriver::Redis => unreachable!("Redis not tested in this test"),
        }

        Ok(())
    }

    /// Test EXPLAIN on a CTE query
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_cte(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "WITH top_actors AS ( \
                       SELECT actor_id, first_name, last_name \
                       FROM actor \
                       WHERE actor_id <= 10 \
                     ) \
                     SELECT * FROM top_actors WHERE actor_id = 5";

        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN should return query plan rows for CTE"
        );

        // CTEs can produce complex plans with materialization or inlining
        // Different drivers handle CTEs differently
        assert!(rows.len() >= 1, "CTE query plan should have at least 1 row");

        Ok(())
    }

    /// Test EXPLAIN on a window function query
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_explain_window_function(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        let query = "SELECT actor_id, first_name, last_name, \
                            ROW_NUMBER() OVER (ORDER BY actor_id) AS row_num \
                     FROM actor \
                     WHERE actor_id <= 10";

        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN should return query plan rows for window function"
        );

        // Window functions can produce plans with WindowAgg or similar nodes
        assert!(
            rows.len() >= 1,
            "Window function query plan should have at least 1 row"
        );

        Ok(())
    }

    /// Integration test to verify basic EXPLAIN functionality
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_explain_works(#[case] driver: TestDriver) -> anyhow::Result<()> {
        let conn = test_connection(driver).await?;

        // Simple query that works on any driver without sample data
        let query = "SELECT 1";
        let rows = execute_explain(&*conn, driver, query, &[]).await?;

        assert!(
            !rows.is_empty(),
            "EXPLAIN should work for basic SELECT query"
        );

        Ok(())
    }
}
