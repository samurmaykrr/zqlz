//! INSERT operation tests using Sakila/Pagila sample data
//!
//! Tests cover:
//! - Basic single-row inserts
//! - Batch inserts
//! - RETURNING clause (where supported)
//! - Constraint violations (NOT NULL, foreign keys, primary keys)
//! - Special character and Unicode handling
//! - Minimal required fields vs full inserts

use crate::fixtures::{test_connection, TestDriver};
use anyhow::{Context, Result};
use rstest::rstest;
use zqlz_core::Value;

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_actor_basic(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Insert a new actor with unique name
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('Test', 'Actor')";
    let result = conn.execute(sql, &[]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert exactly one row");
    
    // Clean up
    conn.execute("DELETE FROM actor WHERE first_name = 'Test' AND last_name = 'Actor'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[tokio::test]
async fn test_insert_actor_returning_if_supported(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // PostgreSQL supports RETURNING clause
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('Return', 'Test') RETURNING actor_id";
    let result = conn.query(sql, &[]).await?;
    
    assert_eq!(result.rows.len(), 1, "Should return exactly one row");
    let row = &result.rows[0];
    let actor_id = row.get_by_name("actor_id")
        .and_then(|v| v.as_i64())
        .context("Should return actor_id")?;
    
    assert!(actor_id > 0, "Returned actor_id should be positive");
    
    // Clean up
    conn.execute("DELETE FROM actor WHERE actor_id = $1", &[Value::Int64(actor_id)]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_actor_batch(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Batch insert multiple actors
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('Batch', 'One'), ('Batch', 'Two'), ('Batch', 'Three')";
    let result = conn.execute(sql, &[]).await?;
    
    assert_eq!(result.affected_rows, 3, "Should insert exactly three rows");
    
    // Verify all three were inserted
    let verify_sql = "SELECT COUNT(*) as count FROM actor WHERE first_name = 'Batch'";
    let verify_result = conn.query(verify_sql, &[]).await?;
    let count = verify_result.rows[0].get_by_name("count")
        .and_then(|v| v.as_i64())
        .context("Should get count")?;
    
    assert_eq!(count, 3, "Should find all three inserted actors");
    
    // Clean up
    conn.execute("DELETE FROM actor WHERE first_name = 'Batch'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_actor_not_null_violation(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Try to insert actor with NULL last_name (NOT NULL constraint)
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('Test', NULL)";
    let result = conn.execute(sql, &[]).await;
    
    assert!(result.is_err(), "Should fail due to NOT NULL constraint on last_name");
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_actor_special_characters(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Insert actor with special characters
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('John''s', 'O''Brien')";
    let result = conn.execute(sql, &[]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Verify the data was stored correctly
    let verify_sql = "SELECT first_name, last_name FROM actor WHERE first_name = 'John''s'";
    let verify_result = conn.query(verify_sql, &[]).await?;
    
    assert_eq!(verify_result.rows.len(), 1, "Should find the inserted actor");
    let row = &verify_result.rows[0];
    
    let first_name = row.get_by_name("first_name")
        .and_then(|v| v.as_str())
        .context("Should get first_name")?;
    let last_name = row.get_by_name("last_name")
        .and_then(|v| v.as_str())
        .context("Should get last_name")?;
    
    assert_eq!(first_name, "John's", "First name should preserve apostrophe");
    assert_eq!(last_name, "O'Brien", "Last name should preserve apostrophe");
    
    // Clean up
    conn.execute("DELETE FROM actor WHERE first_name = 'John''s'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_actor_unicode(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Insert actor with Unicode characters (Japanese, emoji, accented)
    let sql = "INSERT INTO actor (first_name, last_name) VALUES ('山田', '太郎')";
    let result = conn.execute(sql, &[]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Verify the data was stored correctly
    let verify_sql = "SELECT first_name, last_name FROM actor WHERE first_name = '山田'";
    let verify_result = conn.query(verify_sql, &[]).await?;
    
    assert_eq!(verify_result.rows.len(), 1, "Should find the inserted actor");
    let row = &verify_result.rows[0];
    
    let first_name = row.get_by_name("first_name")
        .and_then(|v| v.as_str())
        .context("Should get first_name")?;
    let last_name = row.get_by_name("last_name")
        .and_then(|v| v.as_str())
        .context("Should get last_name")?;
    
    assert_eq!(first_name, "山田", "First name should preserve Unicode");
    assert_eq!(last_name, "太郎", "Last name should preserve Unicode");
    
    // Clean up
    conn.execute("DELETE FROM actor WHERE first_name = '山田'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_film_minimal_required_fields(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Get a valid language_id to satisfy foreign key constraint
    let lang_sql = "SELECT language_id FROM language LIMIT 1";
    let lang_result = conn.query(lang_sql, &[]).await?;
    let language_id = lang_result.rows[0].get_by_name("language_id")
        .and_then(|v| v.as_i64())
        .context("Should get language_id")?;
    
    // Insert film with only required fields (title and language_id)
    // Note: Using prepared statements for proper parameter binding
    let sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (title, language_id) VALUES ('Test Film', $1)"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "INSERT INTO film (title, language_id) VALUES ('Test Film', ?)"
        },
        _ => unreachable!(),
    };
    
    let result = conn.execute(sql, &[Value::Int64(language_id)]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Clean up
    conn.execute("DELETE FROM film WHERE title = 'Test Film'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_film_invalid_language_fk(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Try to insert film with invalid language_id (foreign key constraint)
    let sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO film (title, language_id) VALUES ('Invalid FK Film', $1)"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "INSERT INTO film (title, language_id) VALUES ('Invalid FK Film', ?)"
        },
        _ => unreachable!(),
    };
    
    let result = conn.execute(sql, &[Value::Int64(99999)]).await;
    
    assert!(result.is_err(), "Should fail due to foreign key constraint violation");
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_customer_with_valid_store_and_address(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Get valid store_id and address_id to satisfy foreign key constraints
    let store_sql = "SELECT store_id FROM store LIMIT 1";
    let store_result = conn.query(store_sql, &[]).await?;
    let store_id = store_result.rows[0].get_by_name("store_id")
        .and_then(|v| v.as_i64())
        .context("Should get store_id")?;
    
    let address_sql = "SELECT address_id FROM address LIMIT 1";
    let address_result = conn.query(address_sql, &[]).await?;
    let address_id = address_result.rows[0].get_by_name("address_id")
        .and_then(|v| v.as_i64())
        .context("Should get address_id")?;
    
    // Insert customer with valid foreign keys
    let sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO customer (store_id, first_name, last_name, email, address_id) VALUES ($1, 'Test', 'Customer', 'test@example.com', $2)"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "INSERT INTO customer (store_id, first_name, last_name, email, address_id) VALUES (?, 'Test', 'Customer', 'test@example.com', ?)"
        },
        _ => unreachable!(),
    };
    
    let result = conn.execute(sql, &[Value::Int64(store_id), Value::Int64(address_id)]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Clean up
    conn.execute("DELETE FROM customer WHERE email = 'test@example.com'", &[]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_inventory_valid_film(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Get valid film_id and store_id to satisfy foreign key constraints
    let film_sql = "SELECT film_id FROM film LIMIT 1";
    let film_result = conn.query(film_sql, &[]).await?;
    let film_id = film_result.rows[0].get_by_name("film_id")
        .and_then(|v| v.as_i64())
        .context("Should get film_id")?;
    
    let store_sql = "SELECT store_id FROM store LIMIT 1";
    let store_result = conn.query(store_sql, &[]).await?;
    let store_id = store_result.rows[0].get_by_name("store_id")
        .and_then(|v| v.as_i64())
        .context("Should get store_id")?;
    
    // Insert inventory item
    let sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO inventory (film_id, store_id) VALUES ($1, $2)"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "INSERT INTO inventory (film_id, store_id) VALUES (?, ?)"
        },
        _ => unreachable!(),
    };
    
    let result = conn.execute(sql, &[Value::Int64(film_id), Value::Int64(store_id)]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Get the inventory_id for cleanup (PostgreSQL last_insert_id equivalent)
    // This is tricky across drivers, so we'll just delete by film_id which should be unique enough
    let cleanup_sql = match driver {
        TestDriver::Postgres => {
            "DELETE FROM inventory WHERE film_id = $1 AND inventory_id = (SELECT MAX(inventory_id) FROM inventory WHERE film_id = $1)"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "DELETE FROM inventory WHERE film_id = ? AND inventory_id = (SELECT MAX(inventory_id) FROM inventory WHERE film_id = ?)"
        },
        _ => unreachable!(),
    };
    
    conn.execute(cleanup_sql, &[Value::Int64(film_id), Value::Int64(film_id)]).await?;
    
    Ok(())
}

#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[tokio::test]
async fn test_insert_duplicate_primary_key(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;
    
    // Get an existing actor_id
    let sql = "SELECT actor_id FROM actor LIMIT 1";
    let result = conn.query(sql, &[]).await?;
    let existing_id = result.rows[0].get_by_name("actor_id")
        .and_then(|v| v.as_i64())
        .context("Should get actor_id")?;
    
    // Try to insert with duplicate primary key
    let insert_sql = match driver {
        TestDriver::Postgres => {
            "INSERT INTO actor (actor_id, first_name, last_name) VALUES ($1, 'Duplicate', 'Key')"
        },
        TestDriver::Mysql | TestDriver::Sqlite => {
            "INSERT INTO actor (actor_id, first_name, last_name) VALUES (?, 'Duplicate', 'Key')"
        },
        _ => unreachable!(),
    };
    
    let result = conn.execute(insert_sql, &[Value::Int64(existing_id)]).await;
    
    assert!(result.is_err(), "Should fail due to primary key constraint violation");
    
    Ok(())
}

#[tokio::test]
async fn integration_test_insert_works() -> Result<()> {
    // Basic sanity test that doesn't require Sakila data
    let conn = test_connection(TestDriver::Sqlite).await?;
    
    // Create a temp table for testing
    conn.execute("CREATE TEMP TABLE test_insert (id INTEGER PRIMARY KEY, name TEXT)", &[]).await?;
    
    let result = conn.execute("INSERT INTO test_insert (name) VALUES ('test')", &[]).await?;
    
    assert_eq!(result.affected_rows, 1, "Should insert one row");
    
    // Verify insert worked
    let verify = conn.query("SELECT name FROM test_insert WHERE name = 'test'", &[]).await?;
    assert_eq!(verify.rows.len(), 1, "Should find inserted row");
    
    Ok(())
}
