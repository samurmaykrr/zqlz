#[cfg(test)]
mod edge_case_empty_results_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Test SELECT with WHERE condition that matches no rows
    #[rstest]
    #[tokio::test]
    async fn test_empty_select_no_rows(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query with impossible condition
        let result = conn
            .query("SELECT * FROM actor WHERE actor_id = -999999", &[])
            .await?;

        assert_eq!(result.rows.len(), 0, "Should return zero rows");
        assert!(result.columns.len() > 0, "Columns should still be defined");

        Ok(())
    }

    /// Test UPDATE with WHERE condition that matches no rows
    #[rstest]
    #[tokio::test]
    async fn test_empty_update_no_rows(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Update with impossible condition
        let result = conn
            .execute("UPDATE actor SET first_name = 'TEST' WHERE actor_id = -999999", &[])
            .await?;

        assert_eq!(
            result.affected_rows, 0,
            "Should affect zero rows when WHERE matches nothing"
        );

        Ok(())
    }

    /// Test DELETE with WHERE condition that matches no rows
    #[rstest]
    #[tokio::test]
    async fn test_empty_delete_no_rows(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Delete with impossible condition
        let result = conn
            .execute("DELETE FROM actor WHERE actor_id = -999999", &[])
            .await?;

        assert_eq!(
            result.affected_rows, 0,
            "Should affect zero rows when WHERE matches nothing"
        );

        Ok(())
    }

    /// Test aggregate functions on empty result set (COUNT should be 0, others should be NULL)
    #[rstest]
    #[tokio::test]
    async fn test_empty_aggregation_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Aggregate query on empty set
        let result = conn
            .query(
                "SELECT COUNT(*) as cnt, MAX(actor_id) as max_id, MIN(actor_id) as min_id, AVG(actor_id) as avg_id FROM actor WHERE actor_id = -999999",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 1, "Aggregate should return one row");
        let row = &result.rows[0];

        // COUNT should be 0
        let count = row
            .get_by_name("cnt")
            .context("cnt column should exist")?
            .as_i64()
            .context("cnt should be i64")?;
        assert_eq!(count, 0, "COUNT(*) on empty set should be 0");

        // MAX, MIN, AVG should be NULL on empty set
        let max_val = row
            .get_by_name("max_id")
            .context("max_id column should exist")?;
        assert!(
            matches!(max_val, Value::Null),
            "MAX on empty set should be NULL"
        );

        let min_val = row
            .get_by_name("min_id")
            .context("min_id column should exist")?;
        assert!(
            matches!(min_val, Value::Null),
            "MIN on empty set should be NULL"
        );

        let avg_val = row
            .get_by_name("avg_id")
            .context("avg_id column should exist")?;
        assert!(
            matches!(avg_val, Value::Null),
            "AVG on empty set should be NULL"
        );

        Ok(())
    }

    /// Test JOIN that produces empty result set
    #[rstest]
    #[tokio::test]
    async fn test_empty_join_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // JOIN with impossible condition
        let result = conn
            .query(
                "SELECT a.first_name, f.title FROM actor a INNER JOIN film_actor fa ON a.actor_id = fa.actor_id INNER JOIN film f ON fa.film_id = f.film_id WHERE a.actor_id = -999999",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 0, "JOIN should return zero rows");
        assert!(result.columns.len() > 0, "Columns should still be defined");

        Ok(())
    }

    /// Test iteration over empty result set (should not panic or error)
    #[rstest]
    #[tokio::test]
    async fn test_empty_iteration_over_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        let result = conn
            .query("SELECT * FROM actor WHERE actor_id = -999999", &[])
            .await?;

        // Iterate over empty result set (should not panic)
        let mut count = 0;
        for _row in &result.rows {
            count += 1;
        }

        assert_eq!(count, 0, "Iteration should visit zero rows");

        Ok(())
    }

    /// Test UNION of two empty result sets
    #[rstest]
    #[tokio::test]
    async fn test_empty_union_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // UNION of two queries that return no rows
        let result = conn
            .query(
                "SELECT actor_id, first_name FROM actor WHERE actor_id = -999999 UNION SELECT actor_id, first_name FROM actor WHERE actor_id = -999998",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 0, "UNION of empty sets should be empty");

        Ok(())
    }

    /// Test GROUP BY on empty result set
    #[rstest]
    #[tokio::test]
    async fn test_empty_group_by_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // GROUP BY on empty set
        let result = conn
            .query(
                "SELECT first_name, COUNT(*) as cnt FROM actor WHERE actor_id = -999999 GROUP BY first_name",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 0, "GROUP BY on empty set should be empty");

        Ok(())
    }

    /// Test ORDER BY on empty result set
    #[rstest]
    #[tokio::test]
    async fn test_empty_order_by_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // ORDER BY on empty set
        let result = conn
            .query("SELECT * FROM actor WHERE actor_id = -999999 ORDER BY last_name ASC", &[])
            .await?;

        assert_eq!(result.rows.len(), 0, "ORDER BY on empty set should be empty");

        Ok(())
    }

    /// Test LIMIT/OFFSET on empty result set
    #[rstest]
    #[tokio::test]
    async fn test_empty_limit_offset_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // LIMIT/OFFSET on empty set
        let result = conn
            .query("SELECT * FROM actor WHERE actor_id = -999999 LIMIT 10 OFFSET 5", &[])
            .await?;

        assert_eq!(
            result.rows.len(),
            0,
            "LIMIT/OFFSET on empty set should be empty"
        );

        Ok(())
    }

    /// Test DISTINCT on empty result set
    #[rstest]
    #[tokio::test]
    async fn test_empty_distinct_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // DISTINCT on empty set
        let result = conn
            .query("SELECT DISTINCT first_name FROM actor WHERE actor_id = -999999", &[])
            .await?;

        assert_eq!(result.rows.len(), 0, "DISTINCT on empty set should be empty");

        Ok(())
    }

    /// Test subquery that returns empty result
    #[rstest]
    #[tokio::test]
    async fn test_empty_subquery_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Subquery in WHERE clause that returns no rows
        let result = conn
            .query(
                "SELECT * FROM film WHERE film_id IN (SELECT film_id FROM film WHERE film_id = -999999)",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 0, "Subquery returning empty set should produce empty result");

        Ok(())
    }

    /// Test LEFT JOIN where all right-side matches are empty
    #[rstest]
    #[tokio::test]
    async fn test_empty_left_join_right_side(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert a test actor, then LEFT JOIN with film_actor (no matches)
        let actor_id = 99998;
        conn.execute(
            &format!("INSERT INTO actor (actor_id, first_name, last_name) VALUES ({}, 'TEMP', 'ACTOR')", actor_id),
            &[],
        )
        .await?;

        // LEFT JOIN - actor exists but has no films
        let result = conn
            .query(
                &format!("SELECT a.first_name, f.title FROM actor a LEFT JOIN film_actor fa ON a.actor_id = fa.actor_id LEFT JOIN film f ON fa.film_id = f.film_id WHERE a.actor_id = {}", actor_id),
                &[],
            )
            .await?;

        // Should return one row with actor data, but film columns are NULL
        assert_eq!(
            result.rows.len(),
            1,
            "LEFT JOIN should return one row even when right side is empty"
        );

        let row = &result.rows[0];
        let first_name = row
            .get_by_name("first_name")
            .context("first_name should exist")?
            .as_str()
            .context("first_name should be string")?;
        assert_eq!(first_name, "TEMP");

        let title = row.get_by_name("title").context("title should exist")?;
        assert!(matches!(title, Value::Null), "title should be NULL");

        // Cleanup
        conn.execute(&format!("DELETE FROM actor WHERE actor_id = {}", actor_id), &[])
            .await?;

        Ok(())
    }

    /// Test IN clause with empty list (should match nothing)
    #[rstest]
    #[tokio::test]
    async fn test_empty_in_clause(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // IN clause with values that don't exist
        let result = conn
            .query("SELECT * FROM actor WHERE actor_id IN (-1, -2, -3)", &[])
            .await?;

        assert_eq!(result.rows.len(), 0, "IN clause with non-existent values should return empty result");

        Ok(())
    }

    /// Test HAVING clause that filters out all groups
    #[rstest]
    #[tokio::test]
    async fn test_empty_having_result(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // GROUP BY with HAVING that matches no groups
        let result = conn
            .query(
                "SELECT first_name, COUNT(*) as cnt FROM actor GROUP BY first_name HAVING COUNT(*) > 999999",
                &[],
            )
            .await?;

        assert_eq!(result.rows.len(), 0, "HAVING that matches no groups should return empty result");

        Ok(())
    }

    /// Integration test: empty results work without Docker/Sakila
    #[rstest]
    #[tokio::test]
    async fn integration_test_empty_results_work(
        #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create temporary table (MySQL requires explicit TEMPORARY keyword and session handling)
        let create_sql = match driver {
            TestDriver::Mysql => {
                // MySQL: CREATE TEMPORARY TABLE creates a session-specific table
                "CREATE TEMPORARY TABLE IF NOT EXISTS test_empty_edge (id INTEGER PRIMARY KEY, name TEXT)"
            }
            _ => {
                // PostgreSQL/SQLite: Standard TEMPORARY TABLE
                "CREATE TEMPORARY TABLE test_empty_edge (id INTEGER PRIMARY KEY, name TEXT)"
            }
        };
        
        conn.execute(create_sql, &[]).await?;

        // Query empty table
        let result = conn.query("SELECT * FROM test_empty_edge", &[]).await?;
        assert_eq!(result.rows.len(), 0, "Empty table should return zero rows");

        // Update empty table
        let update_result = conn
            .execute("UPDATE test_empty_edge SET name = 'test' WHERE id = 1", &[])
            .await?;
        assert_eq!(update_result.affected_rows, 0, "Update on empty table should affect zero rows");

        // Delete from empty table
        let delete_result = conn
            .execute("DELETE FROM test_empty_edge WHERE id = 1", &[])
            .await?;
        assert_eq!(delete_result.affected_rows, 0, "Delete on empty table should affect zero rows");

        // Aggregate on empty table
        let agg_result = conn
            .query("SELECT COUNT(*) as cnt FROM test_empty_edge", &[])
            .await?;
        assert_eq!(agg_result.rows.len(), 1);
        let count = agg_result.rows[0]
            .get_by_name("cnt")
            .context("cnt should exist")?
            .as_i64()
            .context("cnt should be i64")?;
        assert_eq!(count, 0, "COUNT on empty table should be 0");

        // Cleanup (DROP TEMPORARY TABLE for MySQL)
        let drop_sql = match driver {
            TestDriver::Mysql => "DROP TEMPORARY TABLE IF EXISTS test_empty_edge",
            _ => "DROP TABLE IF EXISTS test_empty_edge",
        };
        let _ = conn.execute(drop_sql, &[]).await; // Ignore errors on cleanup

        Ok(())
    }
}
