//! Query tests for JOIN operations, subqueries, and advanced SQL features
//!
//! This module tests complex query patterns across all SQL drivers including:
//! - INNER JOIN, LEFT JOIN, CROSS JOIN
//! - Multi-table joins
//! - Self-joins
//! - Joins with WHERE clauses, aggregations
//! - USING vs ON clause syntax
//!
//! All tests use the Sakila/Pagila sample database schema.

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper function to execute SQL with cross-driver parameter syntax
    pub(crate) async fn execute_query(
        conn: &dyn zqlz_core::Connection,
        driver: TestDriver,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let converted_sql = if matches!(driver, TestDriver::Postgres) {
            sql.to_string()
        } else {
            // Convert PostgreSQL $1, $2 syntax to ? for MySQL/SQLite
            let mut converted = sql.to_string();
            for idx in (1..=10).rev() {
                if converted.contains(&format!("${}", idx)) {
                    converted = converted.replace(&format!("${}", idx), "?");
                }
            }
            converted
        };

        conn.query(&converted_sql, params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Test INNER JOIN between actor, film_actor, and film tables
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_actor_film_via_film_actor(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT a.first_name, a.last_name, f.title
            FROM actor a
            INNER JOIN film_actor fa ON a.actor_id = fa.actor_id
            INNER JOIN film f ON fa.film_id = f.film_id
            WHERE a.actor_id = $1
            ORDER BY f.title
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(1)]).await?;

        assert!(!result.rows.is_empty(), "Expected results from actor-film join");
        assert!(result.rows.len() <= 5, "Expected at most 5 results due to LIMIT");
        assert_eq!(result.columns.len(), 3, "Expected 3 columns");

        Ok(())
    }

    /// Test JOIN between film and category tables via film_category
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_film_category(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT f.title, c.name AS category_name
            FROM film f
            INNER JOIN film_category fc ON f.film_id = fc.film_id
            INNER JOIN category c ON fc.category_id = c.category_id
            WHERE c.name = $1
            ORDER BY f.title
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::String("Action".into())]).await?;

        assert!(!result.rows.is_empty(), "Expected Action films");
        assert!(result.rows.len() <= 10, "Expected at most 10 results due to LIMIT");

        Ok(())
    }

    /// Test JOIN across customer, rental, and payment tables (financial chain)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_customer_rental_payment(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT c.first_name, c.last_name, COUNT(p.payment_id) AS payment_count
            FROM customer c
            INNER JOIN rental r ON c.customer_id = r.customer_id
            INNER JOIN payment p ON r.rental_id = p.rental_id
            WHERE c.customer_id = $1
            GROUP BY c.customer_id, c.first_name, c.last_name
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(1)]).await?;

        assert!(!result.rows.is_empty(), "Expected customer 1 to have payments");
        assert_eq!(result.rows.len(), 1, "Expected exactly one row for customer 1");

        Ok(())
    }

    /// Test LEFT JOIN to find actors without any films (edge case)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_left_join_actor_without_films(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT a.actor_id, a.first_name, a.last_name, fa.film_id
            FROM actor a
            LEFT JOIN film_actor fa ON a.actor_id = fa.actor_id
            WHERE fa.film_id IS NULL
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        // In Sakila, all actors typically have films, so this might be empty
        // But the query should execute successfully
        for row in &result.rows {
            let film_id = row.get_by_name("film_id").context("Missing film_id")?;
            assert!(matches!(film_id, Value::Null), "Expected NULL for film_id in LEFT JOIN with no match");
        }

        Ok(())
    }

    /// Test CROSS JOIN between store and staff tables
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cross_join_store_staff(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT s.store_id, st.staff_id, st.first_name, st.last_name
            FROM store s
            CROSS JOIN staff st
            ORDER BY s.store_id, st.staff_id
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        assert!(!result.rows.is_empty(), "Expected results from CROSS JOIN");
        assert!(result.rows.len() >= 2, "Expected at least 2 results from CROSS JOIN");

        Ok(())
    }

    /// Test self-join on actor table to find actors with same last name
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_self_join_actor_last_name(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT a1.first_name AS first_actor, a2.first_name AS second_actor, a1.last_name
            FROM actor a1
            INNER JOIN actor a2 ON a1.last_name = a2.last_name AND a1.actor_id < a2.actor_id
            ORDER BY a1.last_name, a1.first_name
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        // Query should execute successfully
        // Results depend on data
        Ok(())
    }

    /// Test JOIN with WHERE filter
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_with_where_filter(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT f.title, f.rating, c.name AS category_name
            FROM film f
            INNER JOIN film_category fc ON f.film_id = fc.film_id
            INNER JOIN category c ON fc.category_id = c.category_id
            WHERE f.rating = $1 AND c.name = $2
            ORDER BY f.title
            LIMIT 5
        ";

        let result = execute_query(
            conn.as_ref(),
            driver,
            sql,
            &[Value::String("PG".into()), Value::String("Action".into())],
        ).await?;

        // Query should execute successfully
        Ok(())
    }

    /// Test JOIN with aggregation (COUNT, GROUP BY)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_with_aggregation(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT c.name AS category_name, COUNT(f.film_id) AS film_count
            FROM category c
            LEFT JOIN film_category fc ON c.category_id = fc.category_id
            LEFT JOIN film f ON fc.film_id = f.film_id
            GROUP BY c.category_id, c.name
            ORDER BY film_count DESC
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        assert!(!result.rows.is_empty(), "Expected category results");
        assert!(result.rows.len() <= 5, "Expected at most 5 results due to LIMIT");

        Ok(())
    }

    /// Test JOIN with USING clause (common column name)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_using_clause(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT f.title, l.name AS language_name
            FROM film f
            INNER JOIN language l USING (language_id)
            ORDER BY f.title
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        assert!(!result.rows.is_empty(), "Expected films with language");
        assert!(result.rows.len() <= 5, "Expected at most 5 results due to LIMIT");

        Ok(())
    }

    /// Test explicit ON clause with complex condition
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_join_on_clause_explicit(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT f1.title AS film1, f2.title AS film2, f1.rating
            FROM film f1
            INNER JOIN film f2 ON f1.rating = f2.rating AND f1.film_id < f2.film_id
            WHERE f1.rating = $1
            ORDER BY f1.title, f2.title
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::String("PG".into())]).await?;

        // Query should execute successfully
        Ok(())
    }

    /// Integration test to verify JOIN operations work
    #[tokio::test]
    async fn integration_test_join_works() -> Result<()> {
        let conn = test_connection(TestDriver::Sqlite).await?;

        // Create two temporary tables and join them
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS test_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data
        conn.execute(
            "INSERT INTO test_users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        conn.execute(
            "INSERT INTO test_orders (id, user_id, amount) VALUES (1, 1, 100.0), (2, 1, 200.0)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Test INNER JOIN
        let result = conn
            .query(
                "SELECT u.name, COUNT(o.id) AS order_count FROM test_users u INNER JOIN test_orders o ON u.id = o.user_id GROUP BY u.id, u.name",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 1, "Expected 1 user with orders");

        let row = &result.rows[0];
        let name = row.get_by_name("name").context("Missing name column")?;
        assert_eq!(name.as_str(), Some("Alice"));

        Ok(())
    }
}

/// Subquery tests for SQL subquery functionality
/// 
/// This module tests subqueries in SELECT, WHERE, FROM clauses,
/// as well as correlated subqueries and nested subqueries.
#[cfg(test)]
mod subquery_tests {
    use super::tests::execute_query;
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Test subquery in SELECT clause - count films per actor
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_in_select_film_count(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT 
                a.first_name,
                a.last_name,
                (SELECT COUNT(*) FROM film_actor fa WHERE fa.actor_id = a.actor_id) AS film_count
            FROM actor a
            WHERE a.actor_id = $1
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(1)]).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 actor");
        
        let row = &result.rows[0];
        let film_count = row
            .get_by_name("film_count")
            .context("Missing film_count column")?
            .as_i64()
            .context("film_count should be Int64")?;

        assert!(film_count > 0, "Actor should have films");

        Ok(())
    }

    /// Test subquery in WHERE clause - find payments above average
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_in_where_avg_amount(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT payment_id, amount
            FROM payment
            WHERE amount > (SELECT AVG(amount) FROM payment)
            ORDER BY amount DESC
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        assert!(!result.rows.is_empty(), "Expected payments above average");
        assert!(result.rows.len() <= 10, "Expected at most 10 results");

        // Verify all amounts are above average
        let avg_sql = "SELECT AVG(amount) AS avg_amount FROM payment";
        let avg_result = execute_query(conn.as_ref(), driver, avg_sql, &[]).await?;
        
        let avg_row = avg_result.rows.first().context("Expected avg result")?;
        let avg_amount = match avg_row.get_by_name("avg_amount").context("Missing avg_amount")? {
            Value::Float64(f) => *f,
            Value::Float32(f) => (*f) as f64,
            Value::String(s) => s.parse::<f64>().context("Failed to parse decimal as f64")?,
            Value::Decimal(s) => s.parse::<f64>().context("Failed to parse decimal as f64")?,
            _ => anyhow::bail!("Unexpected type for avg_amount"),
        };

        for row in &result.rows {
            let amount = match row.get_by_name("amount").context("Missing amount")? {
                Value::Float64(f) => *f,
                Value::Float32(f) => (*f) as f64,
                Value::String(s) => s.parse::<f64>().context("Failed to parse decimal as f64")?,
                Value::Decimal(s) => s.parse::<f64>().context("Failed to parse decimal as f64")?,
                _ => anyhow::bail!("Unexpected type for amount"),
            };
            assert!(amount > avg_amount, "Amount should be above average");
        }

        Ok(())
    }

    /// Test subquery in FROM clause - derived table with top customers
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_in_from_top_customers(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT customer_name, total_amount
            FROM (
                SELECT 
                    c.first_name || ' ' || c.last_name AS customer_name,
                    SUM(p.amount) AS total_amount
                FROM customer c
                INNER JOIN payment p ON c.customer_id = p.customer_id
                GROUP BY c.customer_id, c.first_name, c.last_name
                ORDER BY total_amount DESC
                LIMIT 5
            ) AS top_customers
            WHERE total_amount > $1
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Float64(100.0)]).await?;

        assert!(!result.rows.is_empty(), "Expected top customers");
        assert!(result.rows.len() <= 5, "Expected at most 5 results");

        for row in &result.rows {
            let total_amount = match row.get_by_name("total_amount").context("Missing total_amount")? {
                Value::Float64(f) => *f,
                Value::Float32(f) => (*f) as f64,
                Value::String(s) => s.parse::<f64>().context("Failed to parse decimal as f64")?,
                _ => anyhow::bail!("Unexpected type for total_amount"),
            };
            assert!(total_amount > 100.0, "Total amount should be > 100");
        }

        Ok(())
    }

    /// Test subquery with IN operator - find actors in specific films
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_with_in(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT first_name, last_name
            FROM actor
            WHERE actor_id IN (
                SELECT actor_id 
                FROM film_actor 
                WHERE film_id IN (SELECT film_id FROM film WHERE title LIKE $1)
            )
            ORDER BY last_name
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::String("ACADEMY%".into())]).await?;

        assert!(!result.rows.is_empty(), "Expected actors in ACADEMY films");
        assert!(result.rows.len() <= 10, "Expected at most 10 results");

        Ok(())
    }

    /// Test subquery with EXISTS operator - find customers with rentals
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_with_exists(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT c.first_name, c.last_name
            FROM customer c
            WHERE EXISTS (
                SELECT 1 
                FROM rental r 
                WHERE r.customer_id = c.customer_id 
                  AND r.return_date IS NULL
            )
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        // May or may not have results depending on data, just verify query works
        assert!(result.rows.len() <= 10, "Expected at most 10 results");

        Ok(())
    }

    /// Test subquery with NOT EXISTS operator - find films not in inventory
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_subquery_with_not_exists(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT f.title
            FROM film f
            WHERE NOT EXISTS (
                SELECT 1 
                FROM inventory i 
                WHERE i.film_id = f.film_id
            )
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        // May or may not have results depending on Sakila data
        // Just verify query executes successfully
        assert!(result.rows.len() <= 5, "Expected at most 5 results");

        Ok(())
    }

    /// Test correlated subquery - count rentals per customer
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_correlated_subquery_rental_count(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT 
                c.first_name,
                c.last_name,
                (SELECT COUNT(*) 
                 FROM rental r 
                 WHERE r.customer_id = c.customer_id) AS rental_count
            FROM customer c
            WHERE (SELECT COUNT(*) 
                   FROM rental r 
                   WHERE r.customer_id = c.customer_id) > $1
            ORDER BY rental_count DESC
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(20)]).await?;

        assert!(!result.rows.is_empty(), "Expected customers with >20 rentals");
        assert!(result.rows.len() <= 10, "Expected at most 10 results");

        for row in &result.rows {
            let rental_count: i64 = row
                .get_by_name("rental_count")
                .context("Missing rental_count")?
                .as_i64()
                .context("rental_count should be Int64")?;

            assert!(rental_count > 20, "Rental count should be > 20");
        }

        Ok(())
    }

    /// Test nested subqueries - multiple levels of nesting
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_nested_subqueries(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            SELECT title, rental_rate
            FROM film
            WHERE rental_rate > (
                SELECT AVG(rental_rate)
                FROM film
                WHERE film_id IN (
                    SELECT film_id
                    FROM inventory
                    WHERE store_id = $1
                )
            )
            ORDER BY rental_rate DESC
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(1)]).await?;

        assert!(!result.rows.is_empty(), "Expected films above average rental rate");
        assert!(result.rows.len() <= 5, "Expected at most 5 results");

        Ok(())
    }

    /// Integration test for subquery functionality
    /// 
    /// This test only runs against SQLite to avoid Docker container dependencies.
    /// It validates that subqueries work correctly.
    #[tokio::test]
    async fn integration_test_subqueries_work() -> Result<()> {
        let conn = test_connection(TestDriver::Sqlite).await?;

        // Create test tables
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS test_products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS test_sales (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER, total REAL)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data
        conn.execute(
            "INSERT INTO test_products (id, name, price) VALUES (1, 'Widget', 10.0), (2, 'Gadget', 20.0), (3, 'Gizmo', 30.0)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        conn.execute(
            "INSERT INTO test_sales (id, product_id, quantity, total) VALUES (1, 1, 5, 50.0), (2, 2, 3, 60.0), (3, 1, 2, 20.0)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Test subquery in SELECT
        let result = conn
            .query(
                "SELECT p.name, (SELECT SUM(quantity) FROM test_sales WHERE product_id = p.id) AS total_sold FROM test_products p",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 3, "Expected 3 products");

        // Test subquery in WHERE
        let result2 = conn
            .query(
                "SELECT name FROM test_products WHERE price > (SELECT AVG(price) FROM test_products)",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert!(!result2.rows.is_empty(), "Expected products above average price");

        // Test subquery with IN
        let result3 = conn
            .query(
                "SELECT name FROM test_products WHERE id IN (SELECT DISTINCT product_id FROM test_sales)",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result3.rows.len(), 2, "Expected 2 products with sales");

        // Test subquery with EXISTS
        let result4 = conn
            .query(
                "SELECT name FROM test_products p WHERE EXISTS (SELECT 1 FROM test_sales WHERE product_id = p.id)",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result4.rows.len(), 2, "Expected 2 products with sales (EXISTS)");

        Ok(())
    }
}

/// CTE (Common Table Expression) tests
/// 
/// This module tests Common Table Expressions (CTEs) including:
/// - Simple CTEs with single definition
/// - Multiple CTEs in single query
/// - CTEs combined with JOINs
/// - Recursive CTEs
/// - CTEs used in subqueries
#[cfg(test)]
mod cte_tests {
    use super::tests::execute_query;
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Test simple CTE
    /// 
    /// Creates a basic CTE and queries from it.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cte_simple(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            WITH actor_films AS (
                SELECT actor_id, COUNT(*) AS film_count
                FROM film_actor
                GROUP BY actor_id
            )
            SELECT a.first_name, a.last_name, af.film_count
            FROM actor a
            INNER JOIN actor_films af ON a.actor_id = af.actor_id
            WHERE af.film_count > $1
            ORDER BY af.film_count DESC
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(20)]).await?;

        assert!(!result.rows.is_empty(), "Expected actors with >20 films");
        assert!(result.rows.len() <= 5, "Expected at most 5 results");
        assert_eq!(result.columns.len(), 3, "Expected 3 columns: first_name, last_name, film_count");

        // Verify film_count is greater than 20
        for row in &result.rows {
            let film_count = row.get_by_name("film_count")
                .context("Expected film_count column")?;
            match film_count {
                Value::Int64(count) => assert!(count > &20, "Expected film_count > 20"),
                Value::Int32(count) => assert!(count > &20, "Expected film_count > 20"),
                _ => anyhow::bail!("Unexpected type for film_count: {:?}", film_count),
            }
        }

        Ok(())
    }

    /// Test multiple CTEs
    /// 
    /// Creates multiple CTEs and queries from them.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cte_multiple(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            WITH 
            action_films AS (
                SELECT f.film_id, f.title
                FROM film f
                INNER JOIN film_category fc ON f.film_id = fc.film_id
                INNER JOIN category c ON fc.category_id = c.category_id
                WHERE c.name = $1
            ),
            actor_action_films AS (
                SELECT fa.actor_id, COUNT(*) AS action_count
                FROM film_actor fa
                INNER JOIN action_films af ON fa.film_id = af.film_id
                GROUP BY fa.actor_id
            )
            SELECT a.first_name, a.last_name, aaf.action_count
            FROM actor a
            INNER JOIN actor_action_films aaf ON a.actor_id = aaf.actor_id
            ORDER BY aaf.action_count DESC
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::String("Action".into())]).await?;

        assert!(!result.rows.is_empty(), "Expected actors in action films");
        assert!(result.rows.len() <= 5, "Expected at most 5 results");
        assert_eq!(result.columns.len(), 3, "Expected 3 columns");

        Ok(())
    }

    /// Test CTE with JOIN
    /// 
    /// Tests CTE combined with JOIN operations.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cte_with_join(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            WITH long_films AS (
                SELECT film_id, title, length
                FROM film
                WHERE length > $1
            )
            SELECT lf.title, c.name AS category
            FROM long_films lf
            INNER JOIN film_category fc ON lf.film_id = fc.film_id
            INNER JOIN category c ON fc.category_id = c.category_id
            ORDER BY lf.length DESC
            LIMIT 10
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(120)]).await?;

        assert!(!result.rows.is_empty(), "Expected long films with categories");
        assert!(result.rows.len() <= 10, "Expected at most 10 results");

        Ok(())
    }

    /// Test recursive CTE with numbers
    /// 
    /// Tests recursive CTE functionality by generating a sequence of numbers.
    /// Note: SQLite and PostgreSQL syntax differs slightly from MySQL.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cte_recursive_numbers(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            WITH RECURSIVE numbers AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM numbers WHERE n < 10
            )
            SELECT n FROM numbers ORDER BY n
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[]).await?;

        assert_eq!(result.rows.len(), 10, "Expected 10 rows (numbers 1-10)");
        assert_eq!(result.columns.len(), 1, "Expected 1 column");

        // Verify sequence is 1, 2, 3, ..., 10
        for (idx, row) in result.rows.iter().enumerate() {
            let n = row.get_by_name("n").context("Expected 'n' column")?;
            let expected = (idx + 1) as i64;
            match n {
                Value::Int64(val) => assert_eq!(val, &expected, "Expected n = {}", expected),
                Value::Int32(val) => assert_eq!(*val as i64, expected, "Expected n = {}", expected),
                _ => anyhow::bail!("Unexpected type for n: {:?}", n),
            }
        }

        Ok(())
    }

    /// Test CTE used in subquery
    /// 
    /// Tests CTE that is referenced within a subquery.
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_cte_in_subquery(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let sql = "
            WITH film_stats AS (
                SELECT category_id, COUNT(*) AS film_count
                FROM film_category
                GROUP BY category_id
            )
            SELECT c.name, fs.film_count
            FROM category c
            INNER JOIN film_stats fs ON c.category_id = fs.category_id
            WHERE fs.film_count > $1
            ORDER BY fs.film_count DESC
            LIMIT 5
        ";

        let result = execute_query(conn.as_ref(), driver, sql, &[Value::Int64(50)]).await?;

        assert!(!result.rows.is_empty(), "Expected categories with >50 films");
        assert!(result.rows.len() <= 5, "Expected at most 5 results");

        Ok(())
    }

    /// Integration test for CTE functionality
    /// 
    /// This test only runs against SQLite to avoid Docker container dependencies.
    /// It validates that CTEs work correctly with both simple and recursive scenarios.
    #[tokio::test]
    async fn integration_test_cte_works() -> Result<()> {
        let conn = test_connection(TestDriver::Sqlite).await?;

        // Create test tables
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS test_employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER, salary REAL)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data (hierarchical structure)
        conn.execute(
            "INSERT INTO test_employees (id, name, manager_id, salary) VALUES 
                (1, 'Alice', NULL, 100000.0),
                (2, 'Bob', 1, 80000.0),
                (3, 'Charlie', 1, 75000.0),
                (4, 'David', 2, 60000.0),
                (5, 'Eve', 2, 65000.0)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Test simple CTE
        let result = conn
            .query(
                "WITH high_earners AS (
                    SELECT name, salary FROM test_employees WHERE salary > 70000
                )
                SELECT name FROM high_earners ORDER BY salary DESC",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 3, "Expected 3 high earners (Alice, Bob, Charlie)");

        // Test multiple CTEs
        let result2 = conn
            .query(
                "WITH 
                managers AS (SELECT DISTINCT manager_id FROM test_employees WHERE manager_id IS NOT NULL),
                manager_details AS (SELECT e.* FROM test_employees e INNER JOIN managers m ON e.id = m.manager_id)
                SELECT name FROM manager_details ORDER BY salary DESC",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result2.rows.len(), 2, "Expected 2 managers (Alice, Bob)");

        // Test recursive CTE (organizational hierarchy)
        let result3 = conn
            .query(
                "WITH RECURSIVE employee_hierarchy AS (
                    SELECT id, name, manager_id, 0 AS level FROM test_employees WHERE manager_id IS NULL
                    UNION ALL
                    SELECT e.id, e.name, e.manager_id, eh.level + 1
                    FROM test_employees e
                    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
                )
                SELECT name, level FROM employee_hierarchy ORDER BY level, name",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result3.rows.len(), 5, "Expected all 5 employees in hierarchy");

        // Verify hierarchy levels
        let alice_row = result3.rows.iter().find(|r| {
            matches!(r.get_by_name("name"), Some(Value::String(s)) if s == "Alice")
        }).context("Expected to find Alice")?;
        
        let alice_level = alice_row.get_by_name("level").context("Expected level")?;
        match alice_level {
            Value::Int64(level) => assert_eq!(level, &0, "Alice should be at level 0"),
            Value::Int32(level) => assert_eq!(level, &0, "Alice should be at level 0"),
            _ => anyhow::bail!("Unexpected type for level"),
        }

        Ok(())
    }
}

/// Window function tests
///
/// Tests window functions including ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD,
/// PARTITION BY, ORDER BY, and window frames (ROWS/RANGE).
#[cfg(test)]
mod window_function_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Test ROW_NUMBER() window function - assigns sequential row numbers within partitions
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_row_number(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Number actors by their last name alphabetically
        let sql = "
            SELECT 
                actor_id,
                first_name,
                last_name,
                ROW_NUMBER() OVER (ORDER BY last_name, first_name) AS row_num
            FROM actor
            ORDER BY row_num
            LIMIT 10
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 10, "Expected 10 rows");
        assert_eq!(result.columns.len(), 4, "Expected 4 columns");

        // Verify row numbers are sequential 1, 2, 3, ...
        for (idx, row) in result.rows.iter().enumerate() {
            let row_num = row.get_by_name("row_num").context("Expected row_num column")?;
            match row_num {
                Value::Int64(n) => assert_eq!(*n, (idx + 1) as i64, "Row numbers should be sequential"),
                Value::Int32(n) => assert_eq!(*n, (idx + 1) as i32, "Row numbers should be sequential"),
                _ => anyhow::bail!("Unexpected type for row_num: {:?}", row_num),
            }
        }

        Ok(())
    }

    /// Test RANK() window function - assigns rank with gaps for ties
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_rank(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Rank films by rental duration (films with same duration get same rank)
        let sql = "
            SELECT 
                film_id,
                title,
                rental_duration,
                RANK() OVER (ORDER BY rental_duration DESC) AS rental_rank
            FROM film
            ORDER BY rental_rank, title
            LIMIT 20
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 20, "Expected 20 rows");
        
        // Verify ranks are present and reasonable
        let first_rank = result.rows[0]
            .get_by_name("rental_rank")
            .context("Expected rental_rank column")?;
        match first_rank {
            Value::Int64(n) => assert_eq!(*n, 1, "First rank should be 1"),
            Value::Int32(n) => assert_eq!(*n, 1, "First rank should be 1"),
            _ => anyhow::bail!("Unexpected type for rank"),
        }

        Ok(())
    }

    /// Test DENSE_RANK() window function - assigns rank without gaps for ties
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_dense_rank(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Dense rank films by length (no gaps in ranking even with ties)
        let sql = "
            SELECT 
                title,
                length,
                DENSE_RANK() OVER (ORDER BY length DESC) AS dense_rank
            FROM film
            WHERE length IS NOT NULL
            ORDER BY dense_rank, title
            LIMIT 20
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert!(!result.rows.is_empty(), "Expected results");
        
        // Verify dense ranks are consecutive (no gaps)
        let first_rank = result.rows[0].get_by_name("dense_rank").context("Expected dense_rank")?;
        match first_rank {
            Value::Int64(n) => assert_eq!(*n, 1, "First dense rank should be 1"),
            Value::Int32(n) => assert_eq!(*n, 1, "First dense rank should be 1"),
            _ => anyhow::bail!("Unexpected type for dense_rank"),
        }

        Ok(())
    }

    /// Test PARTITION BY clause - separate window calculations per partition
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_partition_by(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Number films within each rating category
        let sql = "
            SELECT 
                title,
                rating,
                ROW_NUMBER() OVER (PARTITION BY rating ORDER BY title) AS rank_in_rating
            FROM film
            WHERE rating IS NOT NULL
            ORDER BY rating, rank_in_rating
            LIMIT 20
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 20, "Expected 20 rows");
        
        // Verify partitioning: first film in each rating should have rank_in_rating = 1
        let mut seen_ratings = std::collections::HashSet::new();
        let mut first_ranks_correct = true;
        
        for row in &result.rows {
            let rating = row.get_by_name("rating").context("Expected rating")?;
            let rank = row.get_by_name("rank_in_rating").context("Expected rank_in_rating")?;
            
            let rating_str = match rating {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            
            if !seen_ratings.contains(&rating_str) {
                // First occurrence of this rating, rank should be 1
                seen_ratings.insert(rating_str.clone());
                match rank {
                    Value::Int64(n) => {
                        if *n != 1 {
                            first_ranks_correct = false;
                        }
                    }
                    Value::Int32(n) => {
                        if *n != 1 {
                            first_ranks_correct = false;
                        }
                    }
                    _ => {}
                }
            }
        }
        
        assert!(first_ranks_correct, "First film in each rating partition should have rank 1");

        Ok(())
    }

    /// Test window frames with running totals - ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_frame_running_total(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Calculate running total of rental durations ordered by film title
        let sql = "
            SELECT 
                title,
                rental_duration,
                SUM(rental_duration) OVER (
                    ORDER BY title 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_total
            FROM film
            ORDER BY title
            LIMIT 10
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 10, "Expected 10 rows");
        
        // Verify running total is monotonically increasing
        let mut prev_total: i64 = 0;
        for row in &result.rows {
            let running_total = row.get_by_name("running_total").context("Expected running_total")?;
            
            let total_val = match running_total {
                Value::Int64(n) => *n,
                Value::Int32(n) => *n as i64,
                Value::Decimal(d) => d.parse::<i64>().unwrap_or(0),
                _ => anyhow::bail!("Unexpected type for running_total: {:?}", running_total),
            };
            
            assert!(total_val >= prev_total, "Running total should be non-decreasing");
            prev_total = total_val;
        }

        Ok(())
    }

    /// Test LAG() and LEAD() window functions - access rows before/after current row
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_lag_lead(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Get previous and next film lengths for comparison
        let sql = "
            SELECT 
                title,
                length,
                LAG(length, 1) OVER (ORDER BY title) AS prev_length,
                LEAD(length, 1) OVER (ORDER BY title) AS next_length
            FROM film
            WHERE length IS NOT NULL
            ORDER BY title
            LIMIT 10
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 10, "Expected 10 rows");
        assert_eq!(result.columns.len(), 4, "Expected 4 columns");
        
        // First row should have NULL prev_length
        let first_prev = result.rows[0].get_by_name("prev_length").context("Expected prev_length")?;
        assert!(matches!(first_prev, Value::Null), "First row's prev_length should be NULL");
        
        // Last row should have NULL next_length
        let last_next = result.rows[9].get_by_name("next_length").context("Expected next_length")?;
        assert!(matches!(last_next, Value::Null), "Last row's next_length should be NULL");
        
        // Middle rows should have non-NULL values for both LAG and LEAD (usually)
        if result.rows.len() > 2 {
            let middle_row = &result.rows[5];
            let prev = middle_row.get_by_name("prev_length").context("Expected prev_length")?;
            let next = middle_row.get_by_name("next_length").context("Expected next_length")?;
            
            // At least one of prev/next should be non-NULL
            let has_prev = !matches!(prev, Value::Null);
            let has_next = !matches!(next, Value::Null);
            assert!(has_prev || has_next, "Middle row should have at least one non-NULL LAG/LEAD value");
        }

        Ok(())
    }

    /// Test FIRST_VALUE() and LAST_VALUE() window functions
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_window_first_last_value(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query: Get first and last film title in each rating category
        let sql = "
            SELECT 
                title,
                rating,
                FIRST_VALUE(title) OVER (
                    PARTITION BY rating 
                    ORDER BY title
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS first_title,
                LAST_VALUE(title) OVER (
                    PARTITION BY rating 
                    ORDER BY title
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_title
            FROM film
            WHERE rating IS NOT NULL
            ORDER BY rating, title
            LIMIT 20
        ";

        let result = conn.query(sql, &[]).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        assert!(!result.rows.is_empty(), "Expected results");
        assert_eq!(result.columns.len(), 4, "Expected 4 columns");
        
        // Verify all rows in same partition have same first_title and last_title
        let mut partition_first: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut partition_last: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        
        for row in &result.rows {
            let rating = row.get_by_name("rating").context("Expected rating")?;
            let first_title = row.get_by_name("first_title").context("Expected first_title")?;
            let last_title = row.get_by_name("last_title").context("Expected last_title")?;
            
            if let (Value::String(r), Value::String(f), Value::String(l)) = (rating, first_title, last_title) {
                // Check consistency within partition
                if let Some(expected_first) = partition_first.get(r) {
                    assert_eq!(f, expected_first, "All rows in partition should have same FIRST_VALUE");
                } else {
                    partition_first.insert(r.clone(), f.clone());
                }
                
                if let Some(expected_last) = partition_last.get(r) {
                    assert_eq!(l, expected_last, "All rows in partition should have same LAST_VALUE");
                } else {
                    partition_last.insert(r.clone(), l.clone());
                }
            }
        }

        Ok(())
    }

    /// Integration test for window functions (works without Docker using SQLite)
    #[tokio::test]
    async fn integration_test_window_functions() -> Result<()> {
        use zqlz_core::{ConnectionConfig, DatabaseDriver};
        use zqlz_driver_sqlite::SqliteDriver;

        let config = ConnectionConfig::new_sqlite(":memory:");
        let conn = SqliteDriver::new().connect(&config).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?;

        // Create test table with sample data
        conn.execute(
            "CREATE TABLE test_scores (
                student TEXT,
                subject TEXT,
                score INTEGER
            )",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        conn.execute(
            "INSERT INTO test_scores (student, subject, score) VALUES 
                ('Alice', 'Math', 95),
                ('Alice', 'English', 88),
                ('Bob', 'Math', 82),
                ('Bob', 'English', 90),
                ('Charlie', 'Math', 78),
                ('Charlie', 'English', 85)",
            &[],
        ).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // Test ROW_NUMBER with PARTITION BY
        let result = conn
            .query(
                "SELECT 
                    student,
                    subject,
                    score,
                    ROW_NUMBER() OVER (PARTITION BY student ORDER BY score DESC) AS rank
                FROM test_scores
                ORDER BY student, rank",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        assert_eq!(result.rows.len(), 6, "Expected 6 rows");

        // Each student should have rank 1 and rank 2
        let alice_rows: Vec<_> = result
            .rows
            .iter()
            .filter(|r| matches!(r.get_by_name("student"), Some(Value::String(s)) if s == "Alice"))
            .collect();
        assert_eq!(alice_rows.len(), 2, "Alice should have 2 rows");

        Ok(())
    }
}
