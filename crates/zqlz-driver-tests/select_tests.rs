//! SELECT operation tests for all SQL database drivers.
//!
//! This module tests comprehensive SELECT functionality across PostgreSQL, MySQL,
//! and SQLite drivers using the Sakila/Pagila sample databases. Each test is
//! parameterized to run against all SQL drivers using rstest.
//!
//! Test categories:
//! - Basic SELECT with WHERE conditions
//! - Pattern matching with LIKE
//! - DISTINCT selections
//! - Aggregate functions (COUNT, AVG, MIN, MAX, SUM)
//! - GROUP BY clauses
//! - ORDER BY with LIMIT/OFFSET
//! - IN clauses
//! - NULL handling
//! - Empty result sets
//!
//! All tests use the pre-loaded Sakila/Pagila data from the Docker containers.

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;
use zqlz_core::Value;

/// Test selecting a single row by primary key
#[rstest]
#[tokio::test]
async fn test_select_actor_by_primary_key(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Sakila/Pagila databases have actor with actor_id=1 (typically "PENELOPE GUINESS")
    let result = conn
        .query("SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = 1", &[])
        .await
        .context("failed to query actor by id")?;

    assert_eq!(result.rows.len(), 1, "should return exactly one row");
    assert_eq!(result.columns.len(), 3, "should have 3 columns");

    let row = &result.rows[0];
    let actor_id = row.get(0).context("missing actor_id")?;
    assert_eq!(actor_id.as_i64(), Some(1), "actor_id should be 1");

    // Verify we got a non-empty name
    let first_name = row.get(1).context("missing first_name")?;
    assert!(!first_name.as_str().unwrap_or("").is_empty(), "first_name should not be empty");

    Ok(())
}

/// Test selecting by name with WHERE condition
#[rstest]
#[tokio::test]
async fn test_select_actor_by_name(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query for actors with a specific first name
    let result = conn
        .query(
            "SELECT actor_id, first_name, last_name FROM actor WHERE first_name = 'PENELOPE'",
            &[],
        )
        .await
        .context("failed to query actor by name")?;

    // There should be at least one PENELOPE in Sakila
    assert!(result.rows.len() >= 1, "should have at least one PENELOPE");

    // Verify all returned rows have first_name = 'PENELOPE'
    for row in &result.rows {
        let first_name = row.get(1).context("missing first_name")?.as_str().unwrap_or("");
        assert_eq!(first_name, "PENELOPE", "all rows should have first_name = PENELOPE");
    }

    Ok(())
}

/// Test selecting films by rating
#[rstest]
#[tokio::test]
async fn test_select_films_by_rating(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query for PG-rated films
    let result = conn
        .query("SELECT film_id, title, rating FROM film WHERE rating = 'PG'", &[])
        .await
        .context("failed to query films by rating")?;

    // There should be multiple PG-rated films
    assert!(result.rows.len() > 0, "should have at least one PG-rated film");

    // Verify all returned films have rating = 'PG'
    for row in &result.rows {
        let rating = row.get(2).context("missing rating")?.as_str().unwrap_or("");
        assert_eq!(rating, "PG", "all rows should have rating = PG");
    }

    Ok(())
}

/// Test LIKE pattern matching
#[rstest]
#[tokio::test]
async fn test_select_films_with_like_pattern(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query for films starting with 'ACADEMY'
    let result = conn
        .query("SELECT film_id, title FROM film WHERE title LIKE 'ACADEMY%'", &[])
        .await
        .context("failed to query films with LIKE")?;

    // There should be at least one film starting with 'ACADEMY'
    assert!(result.rows.len() > 0, "should have at least one film starting with ACADEMY");

    // Verify all returned films start with 'ACADEMY'
    for row in &result.rows {
        let title = row.get(1).context("missing title")?.as_str().unwrap_or("");
        assert!(title.starts_with("ACADEMY"), "title should start with ACADEMY: {}", title);
    }

    Ok(())
}

/// Test DISTINCT selections
#[rstest]
#[tokio::test]
async fn test_select_distinct_ratings(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get distinct ratings
    let result = conn
        .query("SELECT DISTINCT rating FROM film ORDER BY rating", &[])
        .await
        .context("failed to query distinct ratings")?;

    // Sakila has 5 ratings: G, PG, PG-13, R, NC-17
    assert!(result.rows.len() >= 4, "should have at least 4 distinct ratings");

    // Verify no duplicates
    let mut seen_ratings = std::collections::HashSet::new();
    for row in &result.rows {
        let rating = row.get(0).context("missing rating")?.as_str().unwrap_or("");
        assert!(
            seen_ratings.insert(rating.to_string()),
            "rating {} appeared multiple times",
            rating
        );
    }

    Ok(())
}

/// Test COUNT aggregate function
#[rstest]
#[tokio::test]
async fn test_select_count_films(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Count all films
    let result = conn
        .query("SELECT COUNT(*) as film_count FROM film", &[])
        .await
        .context("failed to count films")?;

    assert_eq!(result.rows.len(), 1, "should return exactly one row");

    let count = result.rows[0]
        .get(0)
        .context("missing count")?
        .as_i64()
        .context("count should be a number")?;

    // Sakila database has 1000 films
    assert!(count > 900, "should have at least 900 films, got {}", count);

    Ok(())
}

/// Test AVG aggregate function
#[rstest]
#[tokio::test]
async fn test_select_avg_replacement_cost(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get average replacement cost
    let result = conn
        .query("SELECT AVG(replacement_cost) as avg_cost FROM film", &[])
        .await
        .context("failed to get average replacement cost")?;

    assert_eq!(result.rows.len(), 1, "should return exactly one row");

    let avg_cost = result.rows[0].get(0).context("missing avg_cost")?;

    // Average should be a reasonable number (Sakila films are in $9-29 range)
    match avg_cost {
        Value::Float32(v) => assert!(*v > 0.0 && *v < 100.0, "avg cost should be reasonable"),
        Value::Float64(v) => assert!(*v > 0.0 && *v < 100.0, "avg cost should be reasonable"),
        Value::Decimal(s) => {
            let val: f64 = s.parse().context("failed to parse decimal")?;
            assert!(val > 0.0 && val < 100.0, "avg cost should be reasonable");
        }
        _ => panic!("avg_cost should be a numeric type, got {:?}", avg_cost),
    }

    Ok(())
}

/// Test MIN and MAX aggregate functions
#[rstest]
#[tokio::test]
async fn test_select_min_max_length(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get min and max film length
    let result = conn
        .query("SELECT MIN(length) as min_length, MAX(length) as max_length FROM film", &[])
        .await
        .context("failed to get min/max length")?;

    assert_eq!(result.rows.len(), 1, "should return exactly one row");

    let min_length = result.rows[0]
        .get(0)
        .context("missing min_length")?
        .as_i64()
        .context("min_length should be a number")?;

    let max_length = result.rows[0]
        .get(1)
        .context("missing max_length")?
        .as_i64()
        .context("max_length should be a number")?;

    assert!(min_length > 0, "min length should be positive");
    assert!(max_length > min_length, "max length should be greater than min length");
    assert!(max_length <= 200, "max length should be reasonable (under 200 minutes)");

    Ok(())
}

/// Test GROUP BY with COUNT
#[rstest]
#[tokio::test]
async fn test_select_group_by_rating(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Count films by rating
    let result = conn
        .query(
            "SELECT rating, COUNT(*) as count FROM film GROUP BY rating ORDER BY rating",
            &[],
        )
        .await
        .context("failed to group by rating")?;

    // Should have multiple rating groups
    assert!(result.rows.len() >= 4, "should have at least 4 rating groups");

    let mut total_count: i64 = 0;
    for row in &result.rows {
        let rating = row.get(0).context("missing rating")?.as_str().unwrap_or("");
        let count = row.get(1).context("missing count")?.as_i64().unwrap_or(0);

        assert!(!rating.is_empty(), "rating should not be empty");
        assert!(count > 0, "each rating should have at least one film");
        total_count += count;
    }

    // Total should be around 1000 films
    assert!(total_count > 900, "total films should be around 1000, got {}", total_count);

    Ok(())
}

/// Test ORDER BY DESC with LIMIT
#[rstest]
#[tokio::test]
async fn test_select_order_by_desc_limit(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get top 5 most expensive films by replacement cost
    let result = conn
        .query(
            "SELECT film_id, title, replacement_cost FROM film ORDER BY replacement_cost DESC LIMIT 5",
            &[],
        )
        .await
        .context("failed to order by desc with limit")?;

    assert_eq!(result.rows.len(), 5, "should return exactly 5 rows");

    // Verify ordering - each row should have cost >= next row
    let mut prev_cost: Option<f64> = None;
    for row in &result.rows {
        let cost = row.get(2).context("missing replacement_cost")?;
        let cost_f64 = match cost {
            Value::Float32(v) => *v as f64,
            Value::Float64(v) => *v,
            Value::Decimal(s) => s.parse().context("failed to parse decimal")?,
            v => panic!("expected numeric type for cost, got {:?}", v),
        };

        if let Some(prev) = prev_cost {
            assert!(
                cost_f64 <= prev,
                "rows should be in descending order: {} > {}",
                cost_f64,
                prev
            );
        }
        prev_cost = Some(cost_f64);
    }

    Ok(())
}

/// Test IN clause
#[rstest]
#[tokio::test]
async fn test_select_in_clause(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get actors with specific IDs
    let result = conn
        .query(
            "SELECT actor_id, first_name, last_name FROM actor WHERE actor_id IN (1, 2, 3) ORDER BY actor_id",
            &[],
        )
        .await
        .context("failed to query with IN clause")?;

    assert_eq!(result.rows.len(), 3, "should return exactly 3 actors");

    // Verify we got the correct IDs
    let ids: Vec<i64> = result
        .rows
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "should have actor IDs 1, 2, 3");

    Ok(())
}

/// Test OFFSET pagination
#[rstest]
#[tokio::test]
async fn test_select_offset_pagination(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Get first page
    let page1 = conn
        .query("SELECT actor_id FROM actor ORDER BY actor_id LIMIT 5", &[])
        .await
        .context("failed to get page 1")?;

    assert_eq!(page1.rows.len(), 5, "page 1 should have 5 rows");

    // Get second page
    let page2 = conn
        .query("SELECT actor_id FROM actor ORDER BY actor_id LIMIT 5 OFFSET 5", &[])
        .await
        .context("failed to get page 2")?;

    assert_eq!(page2.rows.len(), 5, "page 2 should have 5 rows");

    // Verify no overlap
    let page1_ids: Vec<i64> = page1
        .rows
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_i64()))
        .collect();
    let page2_ids: Vec<i64> = page2
        .rows
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_i64()))
        .collect();

    for id in &page2_ids {
        assert!(!page1_ids.contains(id), "page 2 should not contain IDs from page 1");
    }

    Ok(())
}

/// Test IS NULL condition
#[rstest]
#[tokio::test]
async fn test_select_rental_return_date_is_null(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Find rentals that haven't been returned yet
    let result = conn
        .query("SELECT rental_id, rental_date, return_date FROM rental WHERE return_date IS NULL LIMIT 10", &[])
        .await
        .context("failed to query with IS NULL")?;

    // There might be unreturned rentals in the sample data
    // Even if there are none, the query should succeed
    assert!(result.rows.len() <= 10, "should respect LIMIT");

    // Verify all return_date values are NULL
    for row in &result.rows {
        let return_date = row.get(2).context("missing return_date")?;
        assert!(return_date.is_null(), "return_date should be NULL");
    }

    Ok(())
}

/// Test empty result set
#[rstest]
#[tokio::test]
async fn test_select_empty_result_set(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query that should return no results
    let result = conn
        .query(
            "SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = -999999",
            &[],
        )
        .await
        .context("failed to query with impossible condition")?;

    assert_eq!(result.rows.len(), 0, "should return empty result set");
    assert_eq!(result.columns.len(), 3, "should still have column metadata");

    Ok(())
}

/// Test selecting with multiple conditions
#[rstest]
#[tokio::test]
async fn test_select_multiple_conditions(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query with multiple WHERE conditions
    let result = conn
        .query(
            "SELECT film_id, title, rating, length FROM film WHERE rating = 'PG' AND length > 100 LIMIT 10",
            &[],
        )
        .await
        .context("failed to query with multiple conditions")?;

    assert!(result.rows.len() <= 10, "should respect LIMIT");

    // Verify all rows match both conditions
    for row in &result.rows {
        let rating = row.get(2).context("missing rating")?.as_str().unwrap_or("");
        let length = row.get(3).context("missing length")?.as_i64().unwrap_or(0);

        assert_eq!(rating, "PG", "rating should be PG");
        assert!(length > 100, "length should be > 100, got {}", length);
    }

    Ok(())
}

/// Test OR conditions
#[rstest]
#[tokio::test]
async fn test_select_or_conditions(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query with OR conditions
    let result = conn
        .query(
            "SELECT actor_id, first_name, last_name FROM actor WHERE first_name = 'PENELOPE' OR first_name = 'NICK' ORDER BY actor_id",
            &[],
        )
        .await
        .context("failed to query with OR conditions")?;

    assert!(result.rows.len() > 0, "should have at least one matching actor");

    // Verify all rows match at least one condition
    for row in &result.rows {
        let first_name = row.get(1).context("missing first_name")?.as_str().unwrap_or("");
        assert!(
            first_name == "PENELOPE" || first_name == "NICK",
            "first_name should be PENELOPE or NICK, got {}",
            first_name
        );
    }

    Ok(())
}

/// Test BETWEEN condition
#[rstest]
#[tokio::test]
async fn test_select_between(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query with BETWEEN
    let result = conn
        .query("SELECT film_id, title, length FROM film WHERE length BETWEEN 100 AND 120", &[])
        .await
        .context("failed to query with BETWEEN")?;

    assert!(result.rows.len() > 0, "should have at least one film in range");

    // Verify all rows are in range
    for row in &result.rows {
        let length = row.get(2).context("missing length")?.as_i64().unwrap_or(0);
        assert!(
            length >= 100 && length <= 120,
            "length should be between 100 and 120, got {}",
            length
        );
    }

    Ok(())
}

/// Test NOT operator
#[rstest]
#[tokio::test]
async fn test_select_not_operator(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Query with NOT
    let result = conn
        .query("SELECT film_id, title, rating FROM film WHERE NOT rating = 'PG' LIMIT 10", &[])
        .await
        .context("failed to query with NOT operator")?;

    assert!(result.rows.len() <= 10, "should respect LIMIT");

    // Verify no rows have rating = 'PG'
    for row in &result.rows {
        let rating = row.get(2).context("missing rating")?.as_str().unwrap_or("");
        assert_ne!(rating, "PG", "rating should not be PG");
    }

    Ok(())
}

/// Integration test to verify basic SELECT works
#[rstest]
#[tokio::test]
async fn integration_test_select_works(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Simple query that should always work
    let result = conn.query("SELECT 1 as test_value", &[]).await?;

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 1);

    let value = result.rows[0].get(0).context("missing test_value")?;
    assert_eq!(value.as_i64(), Some(1));

    Ok(())
}
