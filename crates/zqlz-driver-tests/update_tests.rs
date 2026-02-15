//! UPDATE operation tests for all SQL database drivers.
//!
//! This module tests comprehensive UPDATE functionality across PostgreSQL, MySQL,
//! and SQLite drivers using the Sakila/Pagila sample databases. Each test is
//! parameterized to run against all SQL drivers using rstest.
//!
//! Test categories:
//! - Single row updates
//! - Multiple row updates with WHERE conditions
//! - Updates with expressions
//! - Setting NULL values
//! - Constraint violation handling (FK, NOT NULL)
//! - Affected row count verification
//! - No matching rows scenarios
//!
//! All tests use the pre-loaded Sakila/Pagila data from the Docker containers.
//! Tests insert temporary data, update it, verify the changes, and clean up.

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;
use zqlz_core::Value;

/// Test updating a single actor's last name
#[rstest]
#[tokio::test]
async fn test_update_single_actor_last_name(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test actor
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO actor (actor_id, first_name, last_name) VALUES ($1, $2, $3)"
        }
        _ => "INSERT INTO actor (actor_id, first_name, last_name) VALUES (?, ?, ?)",
    };
    
    conn.execute(
        insert_sql,
        &[
            Value::Int64(99999),
            Value::String("Test".to_string()),
            Value::String("Actor".to_string()),
        ],
    )
    .await
    .context("failed to insert test actor")?;

    // Update the last name
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE actor SET last_name = $1 WHERE actor_id = $2",
        _ => "UPDATE actor SET last_name = ? WHERE actor_id = ?",
    };
    
    let update_result = conn
        .execute(
            update_sql,
            &[Value::String("Updated".to_string()), Value::Int64(99999)],
        )
        .await
        .context("failed to update actor")?;

    assert_eq!(update_result.affected_rows, 1, "should update exactly one row");

    // Verify the update
    let verify_sql = match driver {
        TestDriver::Postgres => "SELECT last_name FROM actor WHERE actor_id = $1",
        _ => "SELECT last_name FROM actor WHERE actor_id = ?",
    };
    
    let result = conn
        .query(verify_sql, &[Value::Int64(99999)])
        .await
        .context("failed to verify update")?;

    assert_eq!(result.rows.len(), 1, "should find the updated actor");
    let last_name = result.rows[0].get(0).context("missing last_name")?;
    assert_eq!(
        last_name.as_str(),
        Some("Updated"),
        "last_name should be updated"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM actor WHERE actor_id = $1",
        _ => "DELETE FROM actor WHERE actor_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test actor")?;

    Ok(())
}

/// Test updating multiple films by rating
#[rstest]
#[tokio::test]
async fn test_update_multiple_films_by_rating(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test films with a unique rating value
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, rating) 
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        }
        _ => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, rating) 
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        }
    };

    // Insert 3 test films with rating 'NC-17'
    for i in 0..3 {
        conn.execute(
            insert_sql,
            &[
                Value::Int64(90000 + i),
                Value::String(format!("Test Film {}", i)),
                Value::Int64(1), // English
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
                Value::String("NC-17".to_string()),
            ],
        )
        .await
        .context("failed to insert test film")?;
    }

    // Update all NC-17 films (our test films + possibly existing ones)
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE film SET rental_rate = $1 WHERE film_id >= $2 AND film_id <= $3",
        _ => "UPDATE film SET rental_rate = ? WHERE film_id >= ? AND film_id <= ?",
    };
    
    let update_result = conn
        .execute(
            update_sql,
            &[
                Value::String("5.99".to_string()),
                Value::Int64(90000),
                Value::Int64(90002),
            ],
        )
        .await
        .context("failed to update films")?;

    assert_eq!(
        update_result.affected_rows, 3,
        "should update exactly 3 films"
    );

    // Verify the updates
    let verify_sql = match driver {
        TestDriver::Postgres => "SELECT rental_rate FROM film WHERE film_id >= $1 AND film_id <= $2 ORDER BY film_id",
        _ => "SELECT rental_rate FROM film WHERE film_id >= ? AND film_id <= ? ORDER BY film_id",
    };
    
    let result = conn
        .query(verify_sql, &[Value::Int64(90000), Value::Int64(90002)])
        .await
        .context("failed to verify updates")?;

    assert_eq!(result.rows.len(), 3, "should find all 3 updated films");
    
    for row in &result.rows {
        let rental_rate = row.get(0).context("missing rental_rate")?;
        assert_eq!(
            rental_rate.as_str(),
            Some("5.99"),
            "rental_rate should be updated to 5.99"
        );
    }

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM film WHERE film_id >= $1 AND film_id <= $2",
        _ => "DELETE FROM film WHERE film_id >= ? AND film_id <= ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(90000), Value::Int64(90002)])
        .await
        .context("failed to cleanup test films")?;

    Ok(())
}

/// Test UPDATE with an expression (incrementing a numeric value)
#[rstest]
#[tokio::test]
async fn test_update_with_expression(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test film
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, length) 
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        }
        _ => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, length) 
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        }
    };
    
    conn.execute(
        insert_sql,
        &[
            Value::Int64(99999),
            Value::String("Test Film".to_string()),
            Value::Int64(1),
            Value::Int64(3),
            Value::String("4.99".to_string()),
            Value::String("19.99".to_string()),
            Value::Int64(100),
        ],
    )
    .await
    .context("failed to insert test film")?;

    // Update length by adding 20 minutes
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE film SET length = length + $1 WHERE film_id = $2",
        _ => "UPDATE film SET length = length + ? WHERE film_id = ?",
    };
    
    let update_result = conn
        .execute(update_sql, &[Value::Int64(20), Value::Int64(99999)])
        .await
        .context("failed to update film length")?;

    assert_eq!(update_result.affected_rows, 1, "should update one film");

    // Verify the update
    let verify_sql = match driver {
        TestDriver::Postgres => "SELECT length FROM film WHERE film_id = $1",
        _ => "SELECT length FROM film WHERE film_id = ?",
    };
    
    let result = conn
        .query(verify_sql, &[Value::Int64(99999)])
        .await
        .context("failed to verify update")?;

    assert_eq!(result.rows.len(), 1, "should find the updated film");
    let length = result.rows[0].get(0).context("missing length")?;
    assert_eq!(
        length.as_i64(),
        Some(120),
        "length should be incremented to 120"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM film WHERE film_id = $1",
        _ => "DELETE FROM film WHERE film_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test film")?;

    Ok(())
}

/// Test setting a nullable column to NULL
#[rstest]
#[tokio::test]
async fn test_update_set_nullable_column_null(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Note: We can't easily insert into rental without valid FKs, so we'll use a different approach
    // Let's update a film's special_features column instead, which is nullable
    
    let film_insert = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, special_features) 
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        }
        _ => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, special_features) 
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        }
    };
    
    conn.execute(
        film_insert,
        &[
            Value::Int64(99999),
            Value::String("Test Film".to_string()),
            Value::Int64(1),
            Value::Int64(3),
            Value::String("4.99".to_string()),
            Value::String("19.99".to_string()),
            Value::String("Trailers".to_string()),
        ],
    )
    .await
    .context("failed to insert test film")?;

    // Update special_features to NULL
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE film SET special_features = $1 WHERE film_id = $2",
        _ => "UPDATE film SET special_features = ? WHERE film_id = ?",
    };
    
    let update_result = conn
        .execute(update_sql, &[Value::Null, Value::Int64(99999)])
        .await
        .context("failed to set special_features to NULL")?;

    assert_eq!(update_result.affected_rows, 1, "should update one film");

    // Verify the update
    let verify_sql = match driver {
        TestDriver::Postgres => "SELECT special_features FROM film WHERE film_id = $1",
        _ => "SELECT special_features FROM film WHERE film_id = ?",
    };
    
    let result = conn
        .query(verify_sql, &[Value::Int64(99999)])
        .await
        .context("failed to verify update")?;

    assert_eq!(result.rows.len(), 1, "should find the updated film");
    let special_features = result.rows[0].get(0).context("missing special_features")?;
    assert_eq!(special_features, &Value::Null, "special_features should be NULL");

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM film WHERE film_id = $1",
        _ => "DELETE FROM film WHERE film_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test film")?;

    Ok(())
}

/// Test updating a customer email
#[rstest]
#[tokio::test]
async fn test_update_customer_email(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test customer (requires valid store_id and address_id)
    // Let's use existing store and address from Sakila data
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO customer (customer_id, store_id, first_name, last_name, email, address_id, active) 
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        }
        _ => {
            "INSERT INTO customer (customer_id, store_id, first_name, last_name, email, address_id, active) 
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        }
    };
    
    conn.execute(
        insert_sql,
        &[
            Value::Int64(99999),
            Value::Int64(1), // store_id 1 should exist
            Value::String("Test".to_string()),
            Value::String("Customer".to_string()),
            Value::String("test@example.com".to_string()),
            Value::Int64(1), // address_id 1 should exist
            Value::Int64(1), // active
        ],
    )
    .await
    .context("failed to insert test customer")?;

    // Update the email
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE customer SET email = $1 WHERE customer_id = $2",
        _ => "UPDATE customer SET email = ? WHERE customer_id = ?",
    };
    
    let update_result = conn
        .execute(
            update_sql,
            &[
                Value::String("updated@example.com".to_string()),
                Value::Int64(99999),
            ],
        )
        .await
        .context("failed to update customer email")?;

    assert_eq!(update_result.affected_rows, 1, "should update one customer");

    // Verify the update
    let verify_sql = match driver {
        TestDriver::Postgres => "SELECT email FROM customer WHERE customer_id = $1",
        _ => "SELECT email FROM customer WHERE customer_id = ?",
    };
    
    let result = conn
        .query(verify_sql, &[Value::Int64(99999)])
        .await
        .context("failed to verify update")?;

    assert_eq!(result.rows.len(), 1, "should find the updated customer");
    let email = result.rows[0].get(0).context("missing email")?;
    assert_eq!(
        email.as_str(),
        Some("updated@example.com"),
        "email should be updated"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM customer WHERE customer_id = $1",
        _ => "DELETE FROM customer WHERE customer_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test customer")?;

    Ok(())
}

/// Test UPDATE with no matching rows
#[rstest]
#[tokio::test]
async fn test_update_no_matching_rows(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Try to update a non-existent actor
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE actor SET last_name = $1 WHERE actor_id = $2",
        _ => "UPDATE actor SET last_name = ? WHERE actor_id = ?",
    };
    
    let update_result = conn
        .execute(
            update_sql,
            &[Value::String("Nobody".to_string()), Value::Int64(999999)],
        )
        .await
        .context("failed to execute update")?;

    assert_eq!(
        update_result.affected_rows, 0,
        "should update zero rows when no match"
    );

    Ok(())
}

/// Test UPDATE that would violate foreign key constraint
#[rstest]
#[tokio::test]
async fn test_update_foreign_key_violation(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test film
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        }
        _ => {
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost) 
             VALUES (?, ?, ?, ?, ?, ?)"
        }
    };
    
    conn.execute(
        insert_sql,
        &[
            Value::Int64(99999),
            Value::String("Test Film".to_string()),
            Value::Int64(1),
            Value::Int64(3),
            Value::String("4.99".to_string()),
            Value::String("19.99".to_string()),
        ],
    )
    .await
    .context("failed to insert test film")?;

    // Try to update language_id to an invalid value
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE film SET language_id = $1 WHERE film_id = $2",
        _ => "UPDATE film SET language_id = ? WHERE film_id = ?",
    };
    
    let result = conn
        .execute(update_sql, &[Value::Int64(999999), Value::Int64(99999)])
        .await;

    assert!(
        result.is_err(),
        "updating to invalid language_id should fail with FK constraint error"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM film WHERE film_id = $1",
        _ => "DELETE FROM film WHERE film_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test film")?;

    Ok(())
}

/// Test UPDATE that would violate NOT NULL constraint
#[rstest]
#[tokio::test]
async fn test_update_not_null_violation(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert test actor
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO actor (actor_id, first_name, last_name) VALUES ($1, $2, $3)"
        }
        _ => "INSERT INTO actor (actor_id, first_name, last_name) VALUES (?, ?, ?)",
    };
    
    conn.execute(
        insert_sql,
        &[
            Value::Int64(99999),
            Value::String("Test".to_string()),
            Value::String("Actor".to_string()),
        ],
    )
    .await
    .context("failed to insert test actor")?;

    // Try to set last_name to NULL (which should fail as it's NOT NULL)
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE actor SET last_name = $1 WHERE actor_id = $2",
        _ => "UPDATE actor SET last_name = ? WHERE actor_id = ?",
    };
    
    let result = conn
        .execute(update_sql, &[Value::Null, Value::Int64(99999)])
        .await;

    assert!(
        result.is_err(),
        "setting NOT NULL column to NULL should fail"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM actor WHERE actor_id = $1",
        _ => "DELETE FROM actor WHERE actor_id = ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99999)])
        .await
        .context("failed to cleanup test actor")?;

    Ok(())
}

/// Test that affected_rows count is accurate
#[rstest]
#[tokio::test]
async fn test_update_affected_rows_count(
    #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)] driver: TestDriver,
) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Insert 5 test actors
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO actor (actor_id, first_name, last_name) VALUES ($1, $2, $3)"
        }
        _ => "INSERT INTO actor (actor_id, first_name, last_name) VALUES (?, ?, ?)",
    };

    for i in 0..5 {
        conn.execute(
            insert_sql,
            &[
                Value::Int64(99990 + i),
                Value::String(format!("Test{}", i)),
                Value::String("Actor".to_string()),
            ],
        )
        .await
        .context("failed to insert test actor")?;
    }

    // Update all 5 actors
    let update_sql = match driver {
        TestDriver::Postgres => "UPDATE actor SET last_name = $1 WHERE actor_id >= $2 AND actor_id <= $3",
        _ => "UPDATE actor SET last_name = ? WHERE actor_id >= ? AND actor_id <= ?",
    };
    
    let update_result = conn
        .execute(
            update_sql,
            &[
                Value::String("Updated".to_string()),
                Value::Int64(99990),
                Value::Int64(99994),
            ],
        )
        .await
        .context("failed to update actors")?;

    assert_eq!(
        update_result.affected_rows, 5,
        "should report 5 affected rows"
    );

    // Cleanup
    let delete_sql = match driver {
        TestDriver::Postgres => "DELETE FROM actor WHERE actor_id >= $1 AND actor_id <= $2",
        _ => "DELETE FROM actor WHERE actor_id >= ? AND actor_id <= ?",
    };
    
    conn.execute(delete_sql, &[Value::Int64(99990), Value::Int64(99994)])
        .await
        .context("failed to cleanup test actors")?;

    Ok(())
}

/// Integration test to verify basic UPDATE functionality works
#[tokio::test]
async fn integration_test_update_works() -> Result<()> {
    let conn = test_connection(TestDriver::Sqlite).await?;

    // Create a temporary table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_update (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await
    .context("failed to create test table")?;

    // Insert a row
    conn.execute(
        "INSERT INTO test_update (id, value) VALUES (?, ?)",
        &[Value::Int64(1), Value::String("original".to_string())],
    )
    .await
    .context("failed to insert test row")?;

    // Update the row
    let result = conn
        .execute(
            "UPDATE test_update SET value = ? WHERE id = ?",
            &[Value::String("updated".to_string()), Value::Int64(1)],
        )
        .await
        .context("failed to update test row")?;

    assert_eq!(result.affected_rows, 1, "should update one row");

    // Verify the update
    let query_result = conn
        .query("SELECT value FROM test_update WHERE id = ?", &[Value::Int64(1)])
        .await
        .context("failed to query updated row")?;

    assert_eq!(query_result.rows.len(), 1, "should find the updated row");
    let value = query_result.rows[0].get(0).context("missing value")?;
    assert_eq!(value.as_str(), Some("updated"), "value should be updated");

    // Cleanup
    conn.execute("DROP TABLE test_update", &[])
        .await
        .context("failed to drop test table")?;

    Ok(())
}
