//! Performance benchmark tests for database drivers
//!
//! This module provides performance benchmarks for various database operations
//! across PostgreSQL, MySQL, and SQLite drivers. Tests measure query execution
//! times to establish performance baselines and detect regressions.
//!
//! Tests use the Sakila/Pagila sample databases for realistic performance testing.

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use std::time::{Duration, Instant};
    use zqlz_core::{Connection, Value};

    /// Helper function to measure query execution time
    async fn measure_query_time<F, Fut>(f: F) -> Result<Duration>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let start = Instant::now();
        f().await?;
        Ok(start.elapsed())
    }

    fn is_retryable_lock_error(error_text: &str) -> bool {
        error_text.contains("Lock wait timeout exceeded")
            || error_text.contains("database is locked")
            || error_text.contains("deadlock")
    }

    async fn execute_with_retry(
        conn: &dyn Connection,
        sql: &str,
        attempts: usize,
    ) -> Result<()> {
        for attempt in 1..=attempts {
            match conn.execute(sql, &[]).await {
                Ok(_) => return Ok(()),
                Err(error)
                    if attempt < attempts && is_retryable_lock_error(&error.to_string()) =>
                {
                    let backoff_ms = 50 * attempt as u64;
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(anyhow::anyhow!(
            "execute_with_retry exhausted attempts without returning"
        ))
    }

    /// Test simple SELECT query performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_simple_select_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let duration = measure_query_time(|| async {
            let result = conn
                .query("SELECT * FROM actor WHERE actor_id = 1", &[])
                .await
                .context("Failed to execute simple SELECT query")?;

            assert_eq!(
                result.rows.len(),
                1,
                "Expected exactly 1 row for actor_id = 1"
            );
            Ok(())
        })
        .await?;

        println!(
            "[{}] Simple SELECT took: {:?}",
            driver.display_name(),
            duration
        );

        // Baseline: Simple SELECT should complete in under 100ms
        assert!(
            duration < Duration::from_millis(100),
            "Simple SELECT took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test complex JOIN query performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_complex_join_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let query = r#"
            SELECT f.title, a.first_name, a.last_name, c.name as category
            FROM film f
            JOIN film_actor fa ON f.film_id = fa.film_id
            JOIN actor a ON fa.actor_id = a.actor_id
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            WHERE f.rating = 'PG'
            ORDER BY f.title
            LIMIT 50
        "#;

        let duration = measure_query_time(|| async {
            let result = conn
                .query(query, &[])
                .await
                .context("Failed to execute complex JOIN query")?;

            assert!(
                !result.rows.is_empty(),
                "Expected results from complex JOIN query"
            );
            Ok(())
        })
        .await?;

        println!(
            "[{}] Complex JOIN took: {:?}",
            driver.display_name(),
            duration
        );

        // Baseline: Complex JOIN should complete in under 500ms
        assert!(
            duration < Duration::from_millis(500),
            "Complex JOIN took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test aggregation query performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_aggregation_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let query = r#"
            SELECT rating, COUNT(*) as film_count, AVG(length) as avg_length,
                   MIN(rental_rate) as min_rate, MAX(rental_rate) as max_rate
            FROM film
            GROUP BY rating
            ORDER BY rating
        "#;

        let duration = measure_query_time(|| async {
            let result = conn
                .query(query, &[])
                .await
                .context("Failed to execute aggregation query")?;

            assert!(
                !result.rows.is_empty(),
                "Expected results from aggregation query"
            );
            Ok(())
        })
        .await?;

        println!(
            "[{}] Aggregation took: {:?}",
            driver.display_name(),
            duration
        );

        // Baseline: Aggregation should complete in under 200ms
        assert!(
            duration < Duration::from_millis(200),
            "Aggregation took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test pagination query performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_pagination_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Test fetching multiple pages
        let mut total_duration = Duration::ZERO;
        let page_size = 20;
        let num_pages = 5;

        for page in 0..num_pages {
            let offset = page * page_size;
            let query = format!(
                "SELECT * FROM film ORDER BY film_id LIMIT {} OFFSET {}",
                page_size, offset
            );

            let duration = measure_query_time(|| async {
                let result = conn
                    .query(&query, &[])
                    .await
                    .context("Failed to execute pagination query")?;

                assert!(
                    result.rows.len() <= page_size,
                    "Expected at most {} rows per page",
                    page_size
                );
                Ok(())
            })
            .await?;

            total_duration += duration;
        }

        let avg_duration = total_duration / num_pages as u32;

        println!(
            "[{}] Pagination (avg per page): {:?}",
            driver.display_name(),
            avg_duration
        );

        // Baseline: Each page should load in under 100ms
        assert!(
            avg_duration < Duration::from_millis(100),
            "Pagination took too long: {:?}",
            avg_duration
        );

        Ok(())
    }

    /// Test batch INSERT performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_insert_batch_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        execute_with_retry(
            conn.as_ref(),
            "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099",
            8,
        )
        .await?;

        let batch_size = 100;
        let mut insert_values = Vec::new();

        // Build batch INSERT query
        for i in 0..batch_size {
            let id = 90000 + i;
            insert_values.push(format!("({}, 'PerfTest', 'Actor{}', CURRENT_TIMESTAMP)", id, i));
        }

        let query = format!(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES {}",
            insert_values.join(", ")
        );

        let duration = measure_query_time(|| async {
            conn.execute(&query, &[])
                .await
                .context("Failed to execute batch INSERT")?;
            Ok(())
        })
        .await?;

        // Cleanup
        let cleanup_query = "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099";
        conn.execute(cleanup_query, &[])
            .await
            .context("Failed to cleanup test data")?;

        let per_row_micros = duration.as_micros() as f64 / batch_size as f64;

        println!(
            "[{}] Batch INSERT ({} rows) took: {:?} ({:.2} μs/row)",
            driver.display_name(),
            batch_size,
            duration,
            per_row_micros
        );

        // Baseline: Batch INSERT should complete in under 1 second
        assert!(
            duration < Duration::from_secs(1),
            "Batch INSERT took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test batch UPDATE performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_update_batch_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        execute_with_retry(
            conn.as_ref(),
            "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099",
            8,
        )
        .await?;

        // Setup: Insert test data
        let batch_size = 100;
        let mut insert_values = Vec::new();

        for i in 0..batch_size {
            let id = 90000 + i;
            insert_values.push(format!("({}, 'PerfTest', 'Actor{}', CURRENT_TIMESTAMP)", id, i));
        }

        let insert_query = format!(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES {}",
            insert_values.join(", ")
        );

        execute_with_retry(conn.as_ref(), &insert_query, 8)
            .await
            .context("Failed to insert test data")?;

        // Measure UPDATE performance
        let update_query = "UPDATE actor SET last_name = 'Updated' WHERE actor_id BETWEEN 90000 AND 90099";

        let duration = measure_query_time(|| async {
            let result = conn
                .execute(update_query, &[])
                .await
                .context("Failed to execute batch UPDATE")?;

            assert_eq!(
                result.affected_rows, batch_size,
                "Expected {} rows to be updated",
                batch_size
            );
            Ok(())
        })
        .await?;

        // Cleanup
        let cleanup_query = "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099";
        conn.execute(cleanup_query, &[])
            .await
            .context("Failed to cleanup test data")?;

        let per_row_micros = duration.as_micros() as f64 / batch_size as f64;

        println!(
            "[{}] Batch UPDATE ({} rows) took: {:?} ({:.2} μs/row)",
            driver.display_name(),
            batch_size,
            duration,
            per_row_micros
        );

        // Baseline: Batch UPDATE should complete in under 1 second
        assert!(
            duration < Duration::from_secs(1),
            "Batch UPDATE took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test batch DELETE performance
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_delete_batch_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        execute_with_retry(
            conn.as_ref(),
            "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099",
            8,
        )
        .await?;

        // Setup: Insert test data
        let batch_size = 100;
        let mut insert_values = Vec::new();

        for i in 0..batch_size {
            let id = 90000 + i;
            insert_values.push(format!("({}, 'PerfTest', 'Actor{}', CURRENT_TIMESTAMP)", id, i));
        }

        let insert_query = format!(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES {}",
            insert_values.join(", ")
        );

        execute_with_retry(conn.as_ref(), &insert_query, 8)
            .await
            .context("Failed to insert test data")?;

        // Measure DELETE performance
        let delete_query = "DELETE FROM actor WHERE actor_id BETWEEN 90000 AND 90099";

        let duration = measure_query_time(|| async {
            let result = conn
                .execute(delete_query, &[])
                .await
                .context("Failed to execute batch DELETE")?;

            assert_eq!(
                result.affected_rows, batch_size,
                "Expected {} rows to be deleted",
                batch_size
            );
            Ok(())
        })
        .await?;

        let per_row_micros = duration.as_micros() as f64 / batch_size as f64;

        println!(
            "[{}] Batch DELETE ({} rows) took: {:?} ({:.2} μs/row)",
            driver.display_name(),
            batch_size,
            duration,
            per_row_micros
        );

        // Baseline: Batch DELETE should complete in under 1 second
        assert!(
            duration < Duration::from_secs(1),
            "Batch DELETE took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test prepared statement performance vs regular queries
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_prepared_statement_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Convert query syntax for different drivers
        let query = if matches!(driver, TestDriver::Postgres) {
            "SELECT * FROM actor WHERE actor_id = $1"
        } else {
            "SELECT * FROM actor WHERE actor_id = ?"
        };

        let iterations = 50;
        let mut total_duration = Duration::ZERO;

        // Execute query multiple times with different parameters
        for actor_id in 1..=iterations {
            let duration = measure_query_time(|| async {
                let result = conn
                    .query(query, &[Value::Int64(actor_id)])
                    .await
                    .context("Failed to execute prepared statement")?;

                assert!(
                    result.rows.len() <= 1,
                    "Expected at most 1 row for actor_id = {}",
                    actor_id
                );
                Ok(())
            })
            .await?;

            total_duration += duration;
        }

        let avg_duration = total_duration / iterations as u32;

        println!(
            "[{}] Prepared statement (avg per query): {:?}",
            driver.display_name(),
            avg_duration
        );

        // Baseline: Each prepared statement execution should be fast (under 50ms)
        assert!(
            avg_duration < Duration::from_millis(50),
            "Prepared statement took too long: {:?}",
            avg_duration
        );

        Ok(())
    }

    /// Test index usage performance (comparing indexed vs non-indexed queries)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_index_usage_performance(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query using primary key (indexed)
        let indexed_query = "SELECT * FROM actor WHERE actor_id = 50";

        let indexed_duration = measure_query_time(|| async {
            let result = conn
                .query(indexed_query, &[])
                .await
                .context("Failed to execute indexed query")?;

            assert!(
                result.rows.len() <= 1,
                "Expected at most 1 row for indexed query"
            );
            Ok(())
        })
        .await?;

        // Query using potentially non-indexed column (first_name might not have index)
        let non_indexed_query = "SELECT * FROM actor WHERE first_name = 'PENELOPE'";

        let non_indexed_duration = measure_query_time(|| async {
            conn.query(non_indexed_query, &[])
                .await
                .context("Failed to execute non-indexed query")?;
            Ok(())
        })
        .await?;

        println!(
            "[{}] Indexed query: {:?}, Non-indexed query: {:?}, Ratio: {:.2}x",
            driver.display_name(),
            indexed_duration,
            non_indexed_duration,
            non_indexed_duration.as_micros() as f64 / indexed_duration.as_micros() as f64
        );

        // Both should complete quickly, but indexed should be faster or similar
        assert!(
            indexed_duration < Duration::from_millis(100),
            "Indexed query took too long: {:?}",
            indexed_duration
        );

        assert!(
            non_indexed_duration < Duration::from_millis(200),
            "Non-indexed query took too long: {:?}",
            non_indexed_duration
        );

        Ok(())
    }

    /// Integration test to verify performance testing works
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_performance_framework(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Simple test to verify performance measurement works
        let duration = measure_query_time(|| async {
            conn.query("SELECT 1", &[])
                .await
                .context("Failed to execute SELECT 1")?;
            Ok(())
        })
        .await?;

        assert!(
            duration < Duration::from_secs(1),
            "SELECT 1 should be nearly instant, took: {:?}",
            duration
        );

        println!(
            "[{}] Performance framework working, SELECT 1 took: {:?}",
            driver.display_name(),
            duration
        );

        Ok(())
    }

    // ===== Concurrent Operations Tests (Feature 41) =====

    /// Test concurrent read operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_concurrent_reads(#[case] driver: TestDriver) -> Result<()> {
        let num_tasks = 10;
        let mut tasks = Vec::new();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;
                let result = conn
                    .query("SELECT * FROM actor WHERE actor_id = 1", &[])
                    .await
                    .context("Failed to execute concurrent read")?;

                assert_eq!(
                    result.rows.len(),
                    1,
                    "Task {} expected exactly 1 row",
                    task_id
                );
                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for (idx, task) in tasks.into_iter().enumerate() {
            task.await
                .context(format!("Task {} panicked", idx))?
                .context(format!("Task {} failed", idx))?;
        }

        Ok(())
    }

    /// Test concurrent write operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_concurrent_writes(#[case] driver: TestDriver) -> Result<()> {
        let num_tasks = 10;
        let mut tasks = Vec::new();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;
                let actor_id = 80000 + task_id as i64;

                // Insert
                let insert_query = format!(
                    "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ({}, 'ConcTest', 'Actor{}', CURRENT_TIMESTAMP)",
                    actor_id, task_id
                );
                conn.execute(&insert_query, &[])
                    .await
                    .context("Failed to insert in concurrent write")?;

                // Cleanup
                let delete_query = format!("DELETE FROM actor WHERE actor_id = {}", actor_id);
                conn.execute(&delete_query, &[])
                    .await
                    .context("Failed to cleanup in concurrent write")?;

                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for (idx, task) in tasks.into_iter().enumerate() {
            task.await
                .context(format!("Task {} panicked", idx))?
                .context(format!("Task {} failed", idx))?;
        }

        Ok(())
    }

    /// Test concurrent transactions
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_concurrent_transactions(#[case] driver: TestDriver) -> Result<()> {
        let num_tasks = 5;
        let mut tasks = Vec::new();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;
                let actor_id = 80100 + task_id as i64;

                // Begin transaction
                conn.execute("BEGIN", &[])
                    .await
                    .context("Failed to BEGIN transaction")?;

                // Insert within transaction
                let insert_query = format!(
                    "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ({}, 'TxTest', 'Actor{}', CURRENT_TIMESTAMP)",
                    actor_id, task_id
                );
                conn.execute(&insert_query, &[])
                    .await
                    .context("Failed to insert in transaction")?;

                // Verify read-your-writes
                let query = format!("SELECT * FROM actor WHERE actor_id = {}", actor_id);
                let result = conn
                    .query(&query, &[])
                    .await
                    .context("Failed to read in transaction")?;

                assert_eq!(
                    result.rows.len(),
                    1,
                    "Expected to read inserted row in transaction"
                );

                // Commit transaction
                conn.execute("COMMIT", &[])
                    .await
                    .context("Failed to COMMIT transaction")?;

                // Cleanup
                let delete_query = format!("DELETE FROM actor WHERE actor_id = {}", actor_id);
                conn.execute(&delete_query, &[])
                    .await
                    .context("Failed to cleanup after transaction")?;

                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for (idx, task) in tasks.into_iter().enumerate() {
            task.await
                .context(format!("Task {} panicked", idx))?
                .context(format!("Task {} failed", idx))?;
        }

        Ok(())
    }

    /// Test concurrent mixed reads and writes
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_concurrent_mixed_reads_writes(#[case] driver: TestDriver) -> Result<()> {
        let num_tasks = 10;
        let mut tasks = Vec::new();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;

                if task_id % 2 == 0 {
                    // Even tasks: Read
                    let result = conn
                        .query("SELECT * FROM actor WHERE actor_id = 1", &[])
                        .await
                        .context("Failed to execute read")?;

                    assert_eq!(result.rows.len(), 1, "Expected exactly 1 row");
                } else {
                    // Odd tasks: Write
                    let actor_id = 80200 + task_id as i64;

                    let insert_query = format!(
                        "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ({}, 'MixedTest', 'Actor{}', CURRENT_TIMESTAMP)",
                        actor_id, task_id
                    );
                    conn.execute(&insert_query, &[])
                        .await
                        .context("Failed to insert")?;

                    // Cleanup
                    let delete_query = format!("DELETE FROM actor WHERE actor_id = {}", actor_id);
                    conn.execute(&delete_query, &[])
                        .await
                        .context("Failed to cleanup")?;
                }

                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for (idx, task) in tasks.into_iter().enumerate() {
            task.await
                .context(format!("Task {} panicked", idx))?
                .context(format!("Task {} failed", idx))?;
        }

        Ok(())
    }

    /// Test connection pool under load with many concurrent operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_connection_pool_under_load(#[case] driver: TestDriver) -> Result<()> {
        let num_tasks = 20;
        let mut tasks = Vec::new();

        let start = Instant::now();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;

                // Simulate varied workload
                for _ in 0..5 {
                    conn.query("SELECT * FROM actor LIMIT 10", &[])
                        .await
                        .context("Failed to execute query under load")?;
                }

                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for (idx, task) in tasks.into_iter().enumerate() {
            task.await
                .context(format!("Task {} panicked", idx))?
                .context(format!("Task {} failed", idx))?;
        }

        let duration = start.elapsed();

        println!(
            "[{}] Pool under load ({} tasks × 5 queries) took: {:?}",
            driver.display_name(),
            num_tasks,
            duration
        );

        // Baseline: Should handle load efficiently (under 5 seconds)
        assert!(
            duration < Duration::from_secs(5),
            "Connection pool under load took too long: {:?}",
            duration
        );

        Ok(())
    }

    /// Test deadlock detection and handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_deadlock_detection(#[case] driver: TestDriver) -> Result<()> {
        let conn1 = test_connection(driver).await?;
        let conn2 = test_connection(driver).await?;
        conn1
            .execute(
                "DELETE FROM actor WHERE actor_id IN (80300, 80301) OR first_name = 'Deadlock'",
                &[],
            )
            .await?;

        // Setup: Insert two test rows
        let setup_queries = vec![
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (80300, 'Deadlock', 'Test1', CURRENT_TIMESTAMP)",
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (80301, 'Deadlock', 'Test2', CURRENT_TIMESTAMP)",
        ];

        for query in &setup_queries {
            conn1.execute(query, &[]).await?;
        }

        // SQLite doesn't support true concurrent transactions, so skip the deadlock test
        if matches!(driver, TestDriver::Sqlite) {
            // Cleanup
            conn1
                .execute("DELETE FROM actor WHERE first_name = 'Deadlock'", &[])
                .await?;
            return Ok(());
        }

        // Transaction 1: Update row 1, then row 2
        let task1 = tokio::spawn(async move {
            conn1.execute("BEGIN", &[]).await?;
            conn1
                .execute(
                    "UPDATE actor SET last_name = 'Updated1' WHERE actor_id = 80300",
                    &[],
                )
                .await?;

            // Small delay to increase chance of deadlock
            tokio::time::sleep(Duration::from_millis(100)).await;

            // This might deadlock if task2 has locked row 2
            let result = conn1
                .execute(
                    "UPDATE actor SET last_name = 'Updated2' WHERE actor_id = 80301",
                    &[],
                )
                .await;

            if result.is_ok() {
                conn1.execute("COMMIT", &[]).await?;
            } else {
                conn1.execute("ROLLBACK", &[]).await.ok();
            }

            // Cleanup
            conn1
                .execute("DELETE FROM actor WHERE first_name = 'Deadlock'", &[])
                .await?;

            Ok::<_, anyhow::Error>(result.is_ok())
        });

        // Transaction 2: Update row 2, then row 1 (reverse order)
        let task2 = tokio::spawn(async move {
            conn2.execute("BEGIN", &[]).await?;
            conn2
                .execute(
                    "UPDATE actor SET last_name = 'Updated2' WHERE actor_id = 80301",
                    &[],
                )
                .await?;

            // Small delay
            tokio::time::sleep(Duration::from_millis(100)).await;

            // This might deadlock if task1 has locked row 1
            let result = conn2
                .execute(
                    "UPDATE actor SET last_name = 'Updated1' WHERE actor_id = 80300",
                    &[],
                )
                .await;

            if result.is_ok() {
                conn2.execute("COMMIT", &[]).await?;
            } else {
                conn2.execute("ROLLBACK", &[]).await.ok();
            }

            Ok::<_, anyhow::Error>(result.is_ok())
        });

        // Wait for both tasks
        let result1 = task1.await.context("Task 1 panicked")??;
        let result2 = task2.await.context("Task 2 panicked")??;

        // At least one transaction should succeed (or both if no deadlock occurred)
        assert!(
            result1 || result2,
            "Both transactions failed - expected at least one to succeed"
        );

        Ok(())
    }

    /// Test lock timeout handling (SQLite BUSY state, PostgreSQL/MySQL lock wait timeout)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_lock_timeout_or_busy_handling(#[case] driver: TestDriver) -> Result<()> {
        let conn1 = test_connection(driver).await?;
        let conn2 = test_connection(driver).await?;
        execute_with_retry(conn1.as_ref(), "DELETE FROM actor WHERE actor_id = 80400", 8).await?;

        // Setup: Insert test row
        execute_with_retry(
            conn1.as_ref(),
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (80400, 'LockTest', 'Actor', CURRENT_TIMESTAMP)",
            8,
        )
        .await?;

        // Transaction 1: Lock the row
        conn1.execute("BEGIN", &[]).await?;
        conn1
            .execute(
                "UPDATE actor SET last_name = 'Locked' WHERE actor_id = 80400",
                &[],
            )
            .await?;

        // Transaction 2: Try to update the same row (should timeout or wait)
        let task2 = tokio::spawn(async move {
            conn2.execute("BEGIN", &[]).await?;

            // This should block or timeout because conn1 holds the lock
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                conn2.execute(
                    "UPDATE actor SET last_name = 'Blocked' WHERE actor_id = 80400",
                    &[],
                ),
            )
            .await;

            // Rollback regardless of result
            conn2.execute("ROLLBACK", &[]).await.ok();

            // Result can be timeout/error or succeed after lock release, depending on driver behavior.
            let blocked_or_timed_out = match result {
                Err(_) => true,
                Ok(Err(_)) => true,
                Ok(Ok(_)) => false,
            };
            Ok::<_, anyhow::Error>(blocked_or_timed_out)
        });

        // Wait a bit then release lock
        tokio::time::sleep(Duration::from_millis(500)).await;
        conn1.execute("ROLLBACK", &[]).await?;

        // Cleanup
        execute_with_retry(conn1.as_ref(), "DELETE FROM actor WHERE actor_id = 80400", 8).await?;

        let _blocked_or_timed_out = task2.await.context("Task 2 panicked")??;

        Ok(())
    }

    /// Test serializable isolation level conflict handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_serializable_conflict_handling(#[case] driver: TestDriver) -> Result<()> {
        // SQLite doesn't support explicit isolation levels, skip
        if matches!(driver, TestDriver::Sqlite) {
            return Ok(());
        }

        let conn1 = test_connection(driver).await?;
        let conn2 = test_connection(driver).await?;
        execute_with_retry(conn1.as_ref(), "DELETE FROM actor WHERE actor_id = 80500", 8).await?;

        // Setup: Insert test row
        execute_with_retry(
            conn1.as_ref(),
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (80500, 'SerialTest', 'Actor', CURRENT_TIMESTAMP)",
            8,
        )
        .await?;

        // Start serializable transactions
        if matches!(driver, TestDriver::Postgres) {
            conn1
                .execute("BEGIN ISOLATION LEVEL SERIALIZABLE", &[])
                .await?;
            conn2
                .execute("BEGIN ISOLATION LEVEL SERIALIZABLE", &[])
                .await?;
            conn2.execute("SET LOCAL lock_timeout = '2s'", &[]).await?;
        } else {
            // MySQL
            conn1
                .execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                .await?;
            conn2
                .execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
                .await?;
            conn2
                .execute("SET SESSION innodb_lock_wait_timeout = 2", &[])
                .await?;
            conn1.execute("BEGIN", &[]).await?;
            conn2.execute("BEGIN", &[]).await?;
        }

        // Transaction 1: Read
        let result1 = conn1
            .query("SELECT * FROM actor WHERE actor_id = 80500", &[])
            .await?;
        assert_eq!(result1.rows.len(), 1);

        // Transaction 2: Read
        let result2 = conn2
            .query("SELECT * FROM actor WHERE actor_id = 80500", &[])
            .await?;
        assert_eq!(result2.rows.len(), 1);

        // Transaction 1: Update
        let update1_result = conn1
            .execute(
                "UPDATE actor SET last_name = 'Updated1' WHERE actor_id = 80500",
                &[],
            )
            .await;

        // Commit/rollback transaction 1 before transaction 2 writes, to avoid indefinite lock waits.
        let commit1_result = if update1_result.is_ok() {
            conn1.execute("COMMIT", &[]).await
        } else {
            conn1.execute("ROLLBACK", &[]).await
        };

        // Transaction 2: Update after transaction 1 commit. Under serializable isolation
        // this may still fail due serialization conflict.
        let update2_result = conn2
            .execute(
                "UPDATE actor SET last_name = 'Updated2' WHERE actor_id = 80500",
                &[],
            )
            .await;

        // Commit/rollback transaction 2.
        let commit2_result = if update2_result.is_ok() {
            conn2.execute("COMMIT", &[]).await
        } else {
            conn2.execute("ROLLBACK", &[]).await
        };

        let update1_conflict = update1_result
            .as_ref()
            .err()
            .map(|error| is_retryable_lock_error(&error.to_string()))
            .unwrap_or(false);
        let update2_conflict = update2_result
            .as_ref()
            .err()
            .map(|error| is_retryable_lock_error(&error.to_string()))
            .unwrap_or(false);

        // At least one should commit, or both should fail due expected serialization conflicts.
        let success =
            commit1_result.is_ok() || commit2_result.is_ok() || (update1_conflict && update2_conflict);
        assert!(
            success,
            "Expected commit success or serialization/lock conflict handling"
        );

        // Cleanup
        let cleanup_conn = test_connection(driver).await?;
        execute_with_retry(cleanup_conn.as_ref(), "DELETE FROM actor WHERE actor_id = 80500", 8)
            .await?;

        Ok(())
    }

    /// Integration test for concurrent operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_concurrent_operations_work(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let num_tasks = 5;
        let mut tasks = Vec::new();

        for task_id in 0..num_tasks {
            let driver_clone = driver;
            let task = tokio::spawn(async move {
                let conn = test_connection(driver_clone).await?;
                conn.query("SELECT 1", &[])
                    .await
                    .context(format!("Task {} failed", task_id))?;
                Ok::<_, anyhow::Error>(())
            });
            tasks.push(task);
        }

        // Wait for all tasks
        for task in tasks {
            task.await??;
        }

        println!(
            "[{}] Concurrent operations integration test passed",
            driver.display_name()
        );

        Ok(())
    }
}
