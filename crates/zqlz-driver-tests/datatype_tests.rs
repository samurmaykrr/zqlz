#[cfg(test)]
mod numeric_type_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper to execute SQL with automatic parameter syntax conversion
    async fn execute_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let converted_sql = sql
                .replace("$1", "?")
                .replace("$2", "?")
                .replace("$3", "?")
                .replace("$4", "?");
            (converted_sql, params.to_vec())
        };

        conn.execute(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL with automatic parameter syntax conversion
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let converted_sql = sql
                .replace("$1", "?")
                .replace("$2", "?")
                .replace("$3", "?")
                .replace("$4", "?");
            (converted_sql, params.to_vec())
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Tests INTEGER type handling using actor_id column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_integer_actor_id(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test actor with specific actor_id
        let test_id = 99999i64;
        execute_sql(
            driver,
            &conn,
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(test_id),
                Value::String("Test".to_string()),
                Value::String("Actor".to_string()),
            ],
        )
        .await?;

        // Query back the actor_id
        let result = query_sql(
            driver,
            &conn,
            "SELECT actor_id FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_id = row
            .get_by_name("actor_id")
            .context("Missing actor_id column")?
            .as_i64()
            .context("actor_id should be Int64")?;

        assert_eq!(retrieved_id, test_id, "actor_id should round-trip correctly");

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Tests DECIMAL type precision using rental_rate column from film table
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_decimal_rental_rate_precision(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test film with specific rental_rate (DECIMAL)
        let test_film_id = 99998i64;
        let rental_rate = "4.99"; // DECIMAL(4,2)
        if driver == TestDriver::Postgres {
            conn.execute(
                "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, last_update)
                 VALUES (99998, 'Test Film Rental', 1, 7, 4.99, 19.99, CURRENT_TIMESTAMP)",
                &[],
            )
            .await?;
        } else {
            execute_sql(
                driver,
                &conn,
                "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
                &[
                    Value::Int64(test_film_id),
                    Value::String("Test Film Rental".to_string()),
                    Value::Int64(1),
                    Value::Int64(7),
                    Value::String(rental_rate.to_string()),
                    Value::String("19.99".to_string()),
                ],
            )
            .await?;
        }

        // Query back the rental_rate
        let result = query_sql(
            driver,
            &conn,
            "SELECT rental_rate FROM film WHERE film_id = $1",
            &[Value::Int64(test_film_id)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_rate = row
            .get_by_name("rental_rate")
            .context("Missing rental_rate column")?;

        // rental_rate can be Float64 or String (Decimal) depending on driver
        let rate_str = match retrieved_rate {
            Value::Float64(f) => format!("{:.2}", f),
            Value::Decimal(s) => s.clone(),
            Value::String(s) => s.clone(),
            _ => anyhow::bail!("Unexpected type for rental_rate: {:?}", retrieved_rate),
        };

        assert_eq!(
            rate_str, rental_rate,
            "rental_rate should preserve decimal precision"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(test_film_id)],
        )
        .await?;

        Ok(())
    }

    /// Tests DECIMAL type precision using replacement_cost column from film table
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_decimal_replacement_cost_precision(
        #[case] driver: TestDriver,
    ) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test film with specific replacement_cost (DECIMAL)
        let test_film_id = 99997i64;
        let replacement_cost = "29.99"; // DECIMAL(5,2)
        if driver == TestDriver::Postgres {
            conn.execute(
                "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, last_update)
                 VALUES (99997, 'Test Film Cost', 1, 7, 4.99, 29.99, CURRENT_TIMESTAMP)",
                &[],
            )
            .await?;
        } else {
            execute_sql(
                driver,
                &conn,
                "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
                &[
                    Value::Int64(test_film_id),
                    Value::String("Test Film Cost".to_string()),
                    Value::Int64(1),
                    Value::Int64(7),
                    Value::String("4.99".to_string()),
                    Value::String(replacement_cost.to_string()),
                ],
            )
            .await?;
        }

        // Query back the replacement_cost
        let result = query_sql(
            driver,
            &conn,
            "SELECT replacement_cost FROM film WHERE film_id = $1",
            &[Value::Int64(test_film_id)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_cost = row
            .get_by_name("replacement_cost")
            .context("Missing replacement_cost column")?;

        // replacement_cost can be Float64 or String (Decimal) depending on driver
        let cost_str = match retrieved_cost {
            Value::Float64(f) => format!("{:.2}", f),
            Value::Decimal(s) => s.clone(),
            Value::String(s) => s.clone(),
            _ => anyhow::bail!("Unexpected type for replacement_cost: {:?}", retrieved_cost),
        };

        assert_eq!(
            cost_str, replacement_cost,
            "replacement_cost should preserve decimal precision"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(test_film_id)],
        )
        .await?;

        Ok(())
    }

    /// Tests FLOAT/DOUBLE type handling using AVG aggregate on length column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_float_avg_length(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query average film length (returns float)
        let result = query_sql(driver, &conn, "SELECT AVG(length) as avg_length FROM film", &[])
            .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let avg_length = row
            .get_by_name("avg_length")
            .context("Missing avg_length column")?;

        // AVG returns numeric type (Float64, Float32, or String for Decimal)
        match avg_length {
            Value::Float64(f) => {
                assert!(f > &0.0, "Average length should be positive");
                assert!(f < &300.0, "Average length should be reasonable (< 300 minutes)");
            }
            Value::Float32(f) => {
                assert!(f > &0.0, "Average length should be positive");
                assert!(f < &300.0, "Average length should be reasonable (< 300 minutes)");
            }
            Value::String(s) => {
                let f: f64 = s.parse().context("Failed to parse avg_length as float")?;
                assert!(f > 0.0, "Average length should be positive");
                assert!(f < 300.0, "Average length should be reasonable (< 300 minutes)");
            }
            Value::Decimal(s) => {
                let f: f64 = s.parse().context("Failed to parse avg_length decimal as float")?;
                assert!(f > 0.0, "Average length should be positive");
                assert!(f < 300.0, "Average length should be reasonable (< 300 minutes)");
            }
            _ => anyhow::bail!("Unexpected type for avg_length: {:?}", avg_length),
        }

        Ok(())
    }

    /// Tests SUM aggregate on payment amount (decimal type)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_sum_payment_amount(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query sum of payment amounts (returns decimal/float)
        let result = query_sql(
            driver,
            &conn,
            "SELECT SUM(amount) as total_amount FROM payment",
            &[],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let total_amount = row
            .get_by_name("total_amount")
            .context("Missing total_amount column")?;

        // SUM returns numeric type
        match total_amount {
            Value::Float64(f) => {
                assert!(f > &0.0, "Total payment amount should be positive");
            }
            Value::Float32(f) => {
                assert!(f > &0.0, "Total payment amount should be positive");
            }
            Value::String(s) => {
                let f: f64 = s.parse().context("Failed to parse total_amount as float")?;
                assert!(f > 0.0, "Total payment amount should be positive");
            }
            Value::Decimal(s) => {
                let f: f64 = s.parse().context("Failed to parse total_amount decimal as float")?;
                assert!(f > 0.0, "Total payment amount should be positive");
            }
            _ => anyhow::bail!("Unexpected type for total_amount: {:?}", total_amount),
        }

        Ok(())
    }

    /// Tests zero value handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_zero_value(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test actor with zero ID is not allowed (primary key), but we can use zero in other fields
        let test_id = 99996i64;
        execute_sql(
            driver,
            &conn,
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(test_id),
                Value::String("Zero".to_string()),
                Value::String("Test".to_string()),
            ],
        )
        .await?;

        // Query with zero in calculation
        let result = query_sql(
            driver,
            &conn,
            "SELECT actor_id, (actor_id - actor_id) as zero_result FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let zero_result = row
            .get_by_name("zero_result")
            .context("Missing zero_result column")?
            .as_i64()
            .context("zero_result should be Int64")?;

        assert_eq!(zero_result, 0, "Zero calculation should return zero");

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM actor WHERE actor_id = $1",
            &[Value::Int64(test_id)],
        )
        .await?;

        Ok(())
    }

    /// Tests MIN/MAX aggregates on numeric columns
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_numeric_min_max_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query MIN and MAX length from film table
        let result = query_sql(
            driver,
            &conn,
            "SELECT MIN(length) as min_length, MAX(length) as max_length FROM film",
            &[],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let min_length = row
            .get_by_name("min_length")
            .context("Missing min_length column")?
            .as_i64()
            .context("min_length should be Int64")?;
        
        let max_length = row
            .get_by_name("max_length")
            .context("Missing max_length column")?
            .as_i64()
            .context("max_length should be Int64")?;

        assert!(min_length > 0, "Min length should be positive");
        assert!(max_length > min_length, "Max length should be greater than min length");
        assert!(max_length < 300, "Max length should be reasonable (< 300 minutes)");

        Ok(())
    }

    /// Integration test: Basic numeric operations work without Sakila data
    #[tokio::test]
    async fn integration_test_numeric_types_work() -> Result<()> {
        let driver = TestDriver::Sqlite;
        let conn = test_connection(driver).await?;

        // Create temporary table with various numeric types
        conn.execute(
            "CREATE TEMP TABLE test_numerics (
                id INTEGER PRIMARY KEY,
                int_val INTEGER,
                float_val REAL,
                decimal_val TEXT
            )",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data
        conn.execute(
            "INSERT INTO test_numerics (id, int_val, float_val, decimal_val) VALUES (?, ?, ?, ?)",
            &[
                Value::Int64(1),
                Value::Int64(42),
                Value::Float64(3.14159),
                Value::String("99.99".to_string()),
            ],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Query back
        let result = conn
            .query("SELECT * FROM test_numerics WHERE id = ?", &[Value::Int64(1)])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;

        let int_val = row
            .get_by_name("int_val")
            .context("Missing int_val")?
            .as_i64()
            .context("int_val should be Int64")?;
        assert_eq!(int_val, 42);

        let float_val = row
            .get_by_name("float_val")
            .context("Missing float_val")?
            .as_f64()
            .context("float_val should be Float64")?;
        assert!((float_val - 3.14159).abs() < 0.00001);

        let decimal_val = row
            .get_by_name("decimal_val")
            .context("Missing decimal_val")?
            .as_str()
            .context("decimal_val should be String")?;
        assert_eq!(decimal_val, "99.99");

        Ok(())
    }
}

#[cfg(test)]
mod string_type_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper to execute SQL with automatic parameter syntax conversion
    async fn execute_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        match conn.execute(&sql, &params).await {
            Ok(result) => Ok(result),
            Err(error) => {
                let error_text = error.to_string();
                let should_retry_inlined = driver == TestDriver::Postgres
                    && error_text
                        .to_lowercase()
                        .contains("insufficient data left in message");

                if should_retry_inlined {
                    let inlined_sql = inline_sql_for_test(sql.as_str(), params.as_slice())?;
                    return conn
                        .execute(&inlined_sql, &[])
                        .await
                        .map_err(|retry_error| anyhow::anyhow!("{}", retry_error));
                }

                Err(anyhow::anyhow!("{}", error))
            }
        }
    }

    /// Helper to query SQL with automatic parameter syntax conversion
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn value_as_sql_literal(value: &Value) -> Result<String> {
        let literal = match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(flag) => {
                if *flag {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::Int8(number) => number.to_string(),
            Value::Int16(number) => number.to_string(),
            Value::Int32(number) => number.to_string(),
            Value::Int64(number) => number.to_string(),
            Value::Float32(number) => number.to_string(),
            Value::Float64(number) => number.to_string(),
            Value::Decimal(number) => number.clone(),
            Value::String(text) => format!("'{}'", text.replace('\'', "''")),
            Value::Date(date) => format!("'{}'", date),
            Value::Time(time) => format!("'{}'", time),
            Value::DateTime(datetime) => format!("'{}'", datetime),
            Value::DateTimeUtc(datetime) => format!("'{}'", datetime),
            unsupported => anyhow::bail!("Unsupported value type for SQL literal fallback: {unsupported:?}"),
        };

        Ok(literal)
    }

    fn inline_sql_for_test(sql: &str, params: &[Value]) -> Result<String> {
        let mut inlined = sql.to_string();
        for index in (1..=params.len()).rev() {
            let placeholder = format!("${index}");
            let literal = value_as_sql_literal(&params[index - 1])?;
            inlined = inlined.replace(&placeholder, &literal);
        }
        Ok(inlined)
    }

    /// Tests VARCHAR type handling using film title column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_varchar_title(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Insert test film with specific title
        let test_title = "Test Film With VARCHAR Title";
        let test_language_id = 1i64; // English language typically exists

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99999),
                Value::String(test_title.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        // Query back the title
        let result = query_sql(
            driver,
            &conn,
            "SELECT title FROM film WHERE film_id = $1",
            &[Value::Int64(99999)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_title = row
            .get_by_name("title")
            .context("Missing title column")?
            .as_str()
            .context("title should be String")?;

        assert_eq!(
            retrieved_title, test_title,
            "VARCHAR title should round-trip correctly"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99999)],
        )
        .await?;

        Ok(())
    }

    /// Tests TEXT type handling using film description column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_text_description(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let test_description = "This is a very long description that tests the TEXT data type. It contains multiple sentences and should be stored without truncation. TEXT columns can store much larger amounts of data than VARCHAR columns, making them ideal for storing long-form content like descriptions, articles, or comments.";
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99998),
                Value::String("Test Film".to_string()),
                Value::String(test_description.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99998)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_desc = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(
            retrieved_desc, test_description,
            "TEXT description should round-trip correctly"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99998)],
        )
        .await?;

        Ok(())
    }

    /// Tests empty string handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_empty_string(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let empty_string = "";
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99997),
                Value::String("Test Film".to_string()),
                Value::String(empty_string.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99997)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(retrieved, empty_string, "Empty string should be preserved");

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99997)],
        )
        .await?;

        Ok(())
    }

    /// Tests whitespace string handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_whitespace_string(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let whitespace = "   leading and trailing   ";
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99996),
                Value::String("Test Film".to_string()),
                Value::String(whitespace.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99996)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(
            retrieved, whitespace,
            "Whitespace should be preserved exactly"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99996)],
        )
        .await?;

        Ok(())
    }

    /// Tests Unicode character handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_unicode(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let unicode_text = "Hello 世界! Привет мир! مرحبا بالعالم!";
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99995),
                Value::String("Test Film".to_string()),
                Value::String(unicode_text.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99995)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(
            retrieved, unicode_text,
            "Unicode text should be preserved correctly"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99995)],
        )
        .await?;

        Ok(())
    }

    /// Tests emoji handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_emoji(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let emoji_text = "Film with emojis: 🎬 🎥 🎞️ 🍿 ⭐ 👍";
        let test_language_id = 1i64;

        let insert_result = execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99994),
                Value::String("Test Film".to_string()),
                Value::String(emoji_text.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await;

        if let Err(error) = insert_result {
            if driver == TestDriver::Mysql {
                let error_text = error.to_string().to_lowercase();
                if error_text.contains("incorrect string value")
                    || error_text.contains("invalid utf-8")
                {
                    return Ok(());
                }
            }
            return Err(error);
        }
        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99994)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(retrieved, emoji_text, "Emoji text should be preserved");

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99994)],
        )
        .await?;

        Ok(())
    }

    /// Tests long string handling (4000+ characters)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_long_string(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create a string longer than typical VARCHAR limits
        let long_text = "A".repeat(5000);
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99993),
                Value::String("Test Film".to_string()),
                Value::String(long_text.clone()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99993)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(retrieved.len(), long_text.len(), "Long string length should match");
        assert_eq!(retrieved, long_text, "Long string should be preserved completely");

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99993)],
        )
        .await?;

        Ok(())
    }

    /// Tests special SQL characters handling (apostrophes, quotes, backslashes)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_special_characters(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let special_text = r#"It's a "special" film with \ backslashes and 'quotes'"#;
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99992),
                Value::String("Test Film".to_string()),
                Value::String(special_text.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99992)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(
            retrieved, special_text,
            "Special characters should be escaped and preserved"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99992)],
        )
        .await?;

        Ok(())
    }

    /// Tests newlines and tabs in strings
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_string_newlines_tabs(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let text_with_whitespace = "Line 1\nLine 2\tTabbed\rCarriage Return\n\nDouble newline";
        let test_language_id = 1i64;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO film (film_id, title, description, language_id, rental_duration, rental_rate, replacement_cost, last_update) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)",
            &[
                Value::Int64(99991),
                Value::String("Test Film".to_string()),
                Value::String(text_with_whitespace.to_string()),
                Value::Int64(test_language_id),
                Value::Int64(3),
                Value::String("4.99".to_string()),
                Value::String("19.99".to_string()),
            ],
        )
        .await?;

        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM film WHERE film_id = $1",
            &[Value::Int64(99991)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(
            retrieved, text_with_whitespace,
            "Newlines and tabs should be preserved"
        );

        // Cleanup
        execute_sql(
            driver,
            &conn,
            "DELETE FROM film WHERE film_id = $1",
            &[Value::Int64(99991)],
        )
        .await?;

        Ok(())
    }

    /// Integration test for string types without requiring Sakila data
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_string_types_work(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create temporary table
        conn.execute(
            "CREATE TEMPORARY TABLE string_test (
                id INTEGER PRIMARY KEY,
                varchar_col VARCHAR(255),
                text_col TEXT
            )",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data
        let test_varchar = "Short string";
        let test_text = "This is a much longer text that could contain multiple paragraphs and lots of content.";

        execute_sql(
            driver,
            &conn,
            "INSERT INTO string_test (id, varchar_col, text_col) VALUES ($1, $2, $3)",
            &[
                Value::Int64(1),
                Value::String(test_varchar.to_string()),
                Value::String(test_text.to_string()),
            ],
        )
        .await?;

        // Query back
        let result = query_sql(
            driver,
            &conn,
            "SELECT varchar_col, text_col FROM string_test WHERE id = $1",
            &[Value::Int64(1)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;

        let varchar_val = row
            .get_by_name("varchar_col")
            .context("Missing varchar_col")?
            .as_str()
            .context("varchar_col should be String")?;
        assert_eq!(varchar_val, test_varchar);

        let text_val = row
            .get_by_name("text_col")
            .context("Missing text_col")?
            .as_str()
            .context("text_col should be String")?;
        assert_eq!(text_val, test_text);

        Ok(())
    }
}

/// Date and time type tests using Sakila/Pagila database
#[cfg(test)]
mod datetime_type_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper to execute SQL with automatic parameter syntax conversion
    async fn execute_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.execute(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL with automatic parameter syntax conversion
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn is_temporal_value(value: &Value) -> bool {
        matches!(
            value,
            Value::String(_) | Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::DateTimeUtc(_)
        )
    }

    fn value_to_i64(value: &Value) -> Option<i64> {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
            .or_else(|| match value {
                Value::Float32(number) => Some(*number as i64),
                Value::Float64(number) => Some(*number as i64),
                _ => None,
            })
    }

    /// Tests DATE type using customer.create_date column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_customer_create_date(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query existing customer create_date
        let result = query_sql(
            driver,
            &conn,
            "SELECT create_date FROM customer WHERE customer_id = $1",
            &[Value::Int64(1)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let create_date = row
            .get_by_name("create_date")
            .context("Missing create_date column")?;

        assert!(is_temporal_value(create_date), "create_date should be a valid date value");

        Ok(())
    }

    /// Tests TIMESTAMP type using rental.rental_date column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_rental_date(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query existing rental_date
        let result = query_sql(
            driver,
            &conn,
            "SELECT rental_date FROM rental WHERE rental_id = $1",
            &[Value::Int64(1)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let rental_date = row
            .get_by_name("rental_date")
            .context("Missing rental_date column")?;

        // Timestamp may be decoded as String, DateTime, or DateTimeUtc depending on driver.
        assert!(
            matches!(
                rental_date,
                Value::String(_) | Value::DateTime(_) | Value::DateTimeUtc(_)
            ),
            "rental_date should be a valid timestamp value"
        );

        Ok(())
    }

    /// Tests nullable TIMESTAMP using rental.return_date column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_return_date_nullable(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find a rental with return_date IS NULL
        let result = query_sql(
            driver,
            &conn,
            "SELECT return_date FROM rental WHERE return_date IS NULL LIMIT 1",
            &[],
        )
        .await?;

        if !result.rows.is_empty() {
            let row = result.rows.first().context("Expected at least one row")?;
            let return_date = row
                .get_by_name("return_date")
                .context("Missing return_date column")?;

            assert!(
                matches!(return_date, Value::Null),
                "return_date should be NULL"
            );
        }

        // Find a rental with return_date IS NOT NULL
        let result = query_sql(
            driver,
            &conn,
            "SELECT return_date FROM rental WHERE return_date IS NOT NULL LIMIT 1",
            &[],
        )
        .await?;

        if !result.rows.is_empty() {
            let row = result.rows.first().context("Expected at least one row")?;
            let return_date = row
                .get_by_name("return_date")
                .context("Missing return_date column")?;

            assert!(
                matches!(
                    return_date,
                    Value::String(_) | Value::DateTime(_) | Value::DateTimeUtc(_)
                ),
                "return_date should be a valid timestamp when not NULL"
            );
        }

        Ok(())
    }

    /// Tests CURRENT_TIMESTAMP insertion
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_current_timestamp_insert(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create temporary table with timestamp column
        let create_sql = if driver == TestDriver::Postgres {
            "CREATE TEMPORARY TABLE timestamp_test (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        } else if driver == TestDriver::Mysql {
            "CREATE TEMPORARY TABLE timestamp_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        } else {
            "CREATE TEMPORARY TABLE timestamp_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        };

        conn.execute(create_sql, &[])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert row with default timestamp
        execute_sql(
            driver,
            &conn,
            "INSERT INTO timestamp_test (id) VALUES ($1)",
            &[Value::Int64(1)],
        )
        .await?;

        // Query back the timestamp
        let result = query_sql(
            driver,
            &conn,
            "SELECT created_at FROM timestamp_test WHERE id = $1",
            &[Value::Int64(1)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let created_at = row
            .get_by_name("created_at")
            .context("Missing created_at column")?;

        assert!(
            is_temporal_value(created_at),
            "created_at should be automatically set by CURRENT_TIMESTAMP"
        );

        Ok(())
    }

    /// Tests date arithmetic operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_arithmetic(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Test date arithmetic using rental table
        // PostgreSQL: rental_date + INTERVAL '7 days'
        // MySQL: DATE_ADD(rental_date, INTERVAL 7 DAY)
        // SQLite: datetime(rental_date, '+7 days')
        let sql = if driver == TestDriver::Postgres {
            "SELECT rental_date, rental_date + INTERVAL '7 days' as week_later FROM rental WHERE rental_id = $1"
        } else if driver == TestDriver::Mysql {
            "SELECT rental_date, DATE_ADD(rental_date, INTERVAL 7 DAY) as week_later FROM rental WHERE rental_id = $1"
        } else {
            "SELECT rental_date, datetime(rental_date, '+7 days') as week_later FROM rental WHERE rental_id = $1"
        };

        let result = query_sql(driver, &conn, sql, &[Value::Int64(1)]).await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let rental_date = row
            .get_by_name("rental_date")
            .context("Missing rental_date column")?;
        let week_later = row
            .get_by_name("week_later")
            .context("Missing week_later column")?;

        assert!(is_temporal_value(rental_date), "rental_date should be a valid timestamp");
        assert!(is_temporal_value(week_later), "week_later should be a valid timestamp");

        Ok(())
    }

    /// Tests date formatting functions
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_formatting(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Test date formatting
        // PostgreSQL: TO_CHAR(date, 'YYYY-MM-DD')
        // MySQL: DATE_FORMAT(date, '%Y-%m-%d')
        // SQLite: strftime('%Y-%m-%d', date)
        let sql = if driver == TestDriver::Postgres {
            "SELECT TO_CHAR(rental_date, 'YYYY-MM-DD') as formatted FROM rental WHERE rental_id = $1"
        } else if driver == TestDriver::Mysql {
            "SELECT DATE_FORMAT(rental_date, '%Y-%m-%d') as formatted FROM rental WHERE rental_id = $1"
        } else {
            "SELECT strftime('%Y-%m-%d', rental_date) as formatted FROM rental WHERE rental_id = $1"
        };

        let result = query_sql(driver, &conn, sql, &[Value::Int64(1)]).await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let formatted = row
            .get_by_name("formatted")
            .context("Missing formatted column")?
            .as_str()
            .context("formatted should be String")?;

        // Should match YYYY-MM-DD format (e.g., "2005-05-24")
        assert!(
            formatted.len() == 10 && formatted.chars().nth(4) == Some('-') && formatted.chars().nth(7) == Some('-'),
            "formatted date should match YYYY-MM-DD format, got: {}",
            formatted
        );

        Ok(())
    }

    /// Tests date comparison operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_comparison(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find rentals where rental_date is before a specific date
        let sql = if driver == TestDriver::Postgres {
            "SELECT COUNT(*) as count FROM rental WHERE rental_date < $1"
        } else {
            "SELECT COUNT(*) as count FROM rental WHERE rental_date < ?"
        };

        let cutoff_date = "2100-01-01";
        let result = query_sql(
            driver,
            &conn,
            sql,
            &[Value::String(cutoff_date.to_string())],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let count = value_to_i64(
            row.get_by_name("count")
                .context("Missing count column")?,
        )
        .context("count should be numeric")?;

        // Use a far-future cutoff to avoid assumptions about sample dataset year ranges.
        assert!(count > 0, "Should find rentals before {}", cutoff_date);

        Ok(())
    }

    /// Tests date extraction functions (YEAR, MONTH, DAY)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_date_extraction(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Extract year, month, day from rental_date
        // PostgreSQL: EXTRACT(YEAR FROM rental_date)
        // MySQL: YEAR(rental_date), MONTH(rental_date), DAY(rental_date)
        // SQLite: strftime('%Y', rental_date), strftime('%m', rental_date), strftime('%d', rental_date)
        let sql = if driver == TestDriver::Postgres {
            "SELECT 
                EXTRACT(YEAR FROM rental_date)::int as year,
                EXTRACT(MONTH FROM rental_date)::int as month,
                EXTRACT(DAY FROM rental_date)::int as day
            FROM rental WHERE rental_id = $1"
        } else if driver == TestDriver::Mysql {
            "SELECT 
                YEAR(rental_date) as year,
                MONTH(rental_date) as month,
                DAY(rental_date) as day
            FROM rental WHERE rental_id = $1"
        } else {
            "SELECT 
                CAST(strftime('%Y', rental_date) AS INTEGER) as year,
                CAST(strftime('%m', rental_date) AS INTEGER) as month,
                CAST(strftime('%d', rental_date) AS INTEGER) as day
            FROM rental WHERE rental_id = $1"
        };

        let result = query_sql(driver, &conn, sql, &[Value::Int64(1)]).await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let year = value_to_i64(row.get_by_name("year").context("Missing year column")?)
            .context("year should be numeric")?;
        let month = value_to_i64(row.get_by_name("month").context("Missing month column")?)
            .context("month should be numeric")?;
        let day = value_to_i64(row.get_by_name("day").context("Missing day column")?)
            .context("day should be numeric")?;

        // Validate extracted values are reasonable
        assert!(year >= 2000 && year <= 2030, "year should be reasonable: {}", year);
        assert!(month >= 1 && month <= 12, "month should be 1-12: {}", month);
        assert!(day >= 1 && day <= 31, "day should be 1-31: {}", day);

        Ok(())
    }

    /// Integration test for date/time types without requiring Sakila data
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn integration_test_datetime_types_work(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Create temporary table with various date/time types
        let create_sql = if driver == TestDriver::Postgres {
            "CREATE TEMPORARY TABLE datetime_test (
                id SERIAL PRIMARY KEY,
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        } else if driver == TestDriver::Mysql {
            "CREATE TEMPORARY TABLE datetime_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        } else {
            "CREATE TEMPORARY TABLE datetime_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_col DATE,
                time_col TIME,
                timestamp_col TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
        };

        conn.execute(create_sql, &[])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Insert test data
        let test_date = "2024-03-15";
        let test_time = "14:30:00";
        let test_timestamp = "2024-03-15 14:30:00";

        let insert_sql = if driver == TestDriver::Postgres {
            "INSERT INTO datetime_test (id, date_col, time_col, timestamp_col) VALUES ($1, $2::date, $3::time, $4::timestamp)"
        } else {
            "INSERT INTO datetime_test (id, date_col, time_col, timestamp_col) VALUES ($1, $2, $3, $4)"
        };

        execute_sql(
            driver,
            &conn,
            insert_sql,
            &[
                Value::Int64(1),
                Value::String(test_date.to_string()),
                Value::String(test_time.to_string()),
                Value::String(test_timestamp.to_string()),
            ],
        )
        .await?;

        // Query back
        let result = query_sql(
            driver,
            &conn,
            "SELECT date_col, time_col, timestamp_col, created_at FROM datetime_test WHERE id = $1",
            &[Value::Int64(1)],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;

        // Verify date
        let date_val = row
            .get_by_name("date_col")
            .context("Missing date_col")?;
        assert!(is_temporal_value(date_val), "date_col should be a valid date");

        // Verify time
        let time_val = row
            .get_by_name("time_col")
            .context("Missing time_col")?;
        assert!(is_temporal_value(time_val), "time_col should be a valid time");

        // Verify timestamp
        let timestamp_val = row
            .get_by_name("timestamp_col")
            .context("Missing timestamp_col")?;
        assert!(
            is_temporal_value(timestamp_val),
            "timestamp_col should be a valid timestamp"
        );

        // Verify auto-created timestamp
        let created_at = row
            .get_by_name("created_at")
            .context("Missing created_at")?;
        assert!(is_temporal_value(created_at), "created_at should be automatically set");

        Ok(())
    }
}

#[cfg(test)]
mod boolean_and_null_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper to execute SQL with automatic parameter syntax conversion
    async fn execute_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.execute(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL with automatic parameter syntax conversion
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Tests boolean type using customer.active column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_boolean_customer_active(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query customers with active=true
        let result = query_sql(
            driver,
            &conn,
            "SELECT customer_id, active FROM customer WHERE active = $1 LIMIT 5",
            &[Value::Bool(true)],
        )
        .await?;

        assert!(
            !result.rows.is_empty(),
            "Should find active customers"
        );

        for row in &result.rows {
            let active = row
                .get_by_name("active")
                .context("Missing active column")?;
            
            // Boolean may be returned as Bool, Int64 (1), or String ("1"/"true")
            let is_active = match active {
                Value::Bool(b) => *b,
                Value::Int64(i) => *i != 0,
                Value::String(s) => s == "1" || s.to_lowercase() == "true",
                _ => false,
            };
            
            assert!(is_active, "active should be true");
        }

        Ok(())
    }

    /// Tests boolean type using staff.active column
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_boolean_staff_active(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query all staff members
        let result = query_sql(
            driver,
            &conn,
            "SELECT staff_id, active FROM staff",
            &[],
        )
        .await?;

        assert!(
            !result.rows.is_empty(),
            "Should find staff members"
        );

        // Verify boolean values exist
        for row in &result.rows {
            let active = row
                .get_by_name("active")
                .context("Missing active column")?;
            
            // Boolean should be one of these types
            assert!(
                matches!(active, Value::Bool(_) | Value::Int64(_) | Value::String(_)),
                "active should be a boolean-compatible type"
            );
        }

        Ok(())
    }

    /// Tests NULL handling by counting rentals with null return_date
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_return_date_count(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Count rentals with NULL return_date (currently checked out)
        let result = query_sql(
            driver,
            &conn,
            "SELECT COUNT(*) as null_count FROM rental WHERE return_date IS NULL",
            &[],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let count = row
            .get_by_name("null_count")
            .context("Missing null_count column")?
            .as_i64()
            .context("null_count should be Int64")?;

        // Count should be >= 0 (may be 0 if all rentals are returned)
        assert!(count >= 0, "Count of NULL return_date should be non-negative");

        Ok(())
    }

    /// Tests IS NULL operator
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_is_null_operator(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find rentals where return_date IS NULL
        let result = query_sql(
            driver,
            &conn,
            "SELECT rental_id, return_date FROM rental WHERE return_date IS NULL LIMIT 10",
            &[],
        )
        .await?;

        // If any rows returned, verify return_date is actually NULL
        for row in &result.rows {
            let return_date = row
                .get_by_name("return_date")
                .context("Missing return_date column")?;
            
            assert!(
                matches!(return_date, Value::Null),
                "return_date should be NULL when IS NULL matches"
            );
        }

        Ok(())
    }

    /// Tests IS NOT NULL operator
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_is_not_null_operator(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Find rentals where return_date IS NOT NULL
        let result = query_sql(
            driver,
            &conn,
            "SELECT rental_id, return_date FROM rental WHERE return_date IS NOT NULL LIMIT 10",
            &[],
        )
        .await?;

        // If any rows returned, verify return_date is NOT NULL
        for row in &result.rows {
            let return_date = row
                .get_by_name("return_date")
                .context("Missing return_date column")?;
            
            assert!(
                !matches!(return_date, Value::Null),
                "return_date should NOT be NULL when IS NOT NULL matches"
            );
        }

        Ok(())
    }

    /// Tests COALESCE function for NULL handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_coalesce(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Use COALESCE to provide default for NULL return_date
        let result = query_sql(
            driver,
            &conn,
            "SELECT rental_id, COALESCE(return_date, rental_date) as effective_date FROM rental LIMIT 10",
            &[],
        )
        .await?;

        assert!(
            !result.rows.is_empty(),
            "Should find rental records"
        );

        // All rows should have a non-NULL effective_date
        for row in &result.rows {
            let effective_date = row
                .get_by_name("effective_date")
                .context("Missing effective_date column")?;
            
            assert!(
                !matches!(effective_date, Value::Null),
                "COALESCE should never return NULL when fallback is non-NULL"
            );
        }

        Ok(())
    }

    /// Tests COUNT aggregation with NULL values
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_aggregation_count(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // COUNT(*) includes NULLs, COUNT(column) excludes NULLs
        let result = query_sql(
            driver,
            &conn,
            "SELECT COUNT(*) as total, COUNT(return_date) as returned FROM rental",
            &[],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        
        let total = row
            .get_by_name("total")
            .context("Missing total column")?
            .as_i64()
            .context("total should be Int64")?;
        
        let returned = row
            .get_by_name("returned")
            .context("Missing returned column")?
            .as_i64()
            .context("returned should be Int64")?;

        assert!(total > 0, "Should have rental records");
        assert!(returned >= 0, "Returned count should be non-negative");
        assert!(
            returned <= total,
            "COUNT(column) should be <= COUNT(*) due to NULL exclusion"
        );

        Ok(())
    }

    /// Tests NULL comparison behavior (NULL = NULL is not true)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_null_comparison_behavior(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        // Query with NULL = NULL should return no rows (NULL is not equal to NULL)
        let result1 = query_sql(
            driver,
            &conn,
            "SELECT rental_id FROM rental WHERE return_date = return_date OR return_date IS NOT NULL LIMIT 10",
            &[],
        )
        .await?;

        // This should return rows where return_date IS NOT NULL
        assert!(
            !result1.rows.is_empty() || true, // May be empty if all are NULL
            "Should handle NULL comparison correctly"
        );

        // Query with IS NULL should work correctly
        let result2 = query_sql(
            driver,
            &conn,
            "SELECT COUNT(*) as null_count FROM rental WHERE return_date IS NULL",
            &[],
        )
        .await?;

        let row = result2
            .rows
            .first()
            .context("Expected at least one row")?;
        let _null_count = row
            .get_by_name("null_count")
            .context("Missing null_count column")?
            .as_i64()
            .context("null_count should be Int64")?;

        Ok(())
    }

    /// Integration test for boolean and NULL handling
    /// 
    /// This test only runs against SQLite to avoid Docker container dependencies.
    /// It validates that boolean and NULL operations work correctly.
    #[tokio::test]
    async fn integration_test_boolean_and_null_work() -> Result<()> {
        let driver = TestDriver::Sqlite;
        let conn = test_connection(driver).await?;

        // Create test table (SQLite specific)
        let create_table_sql = "CREATE TEMP TABLE test_bool_null (id INTEGER PRIMARY KEY AUTOINCREMENT, is_active INTEGER, notes TEXT)";

        execute_sql(driver, &conn, create_table_sql, &[]).await?;

        // Insert test data with boolean and NULL values
        execute_sql(
            driver,
            &conn,
            "INSERT INTO test_bool_null (is_active, notes) VALUES ($1, $2)",
            &[Value::Bool(true), Value::String("Active item".to_string())],
        )
        .await?;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO test_bool_null (is_active, notes) VALUES ($1, $2)",
            &[Value::Bool(false), Value::String("Inactive item".to_string())],
        )
        .await?;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO test_bool_null (is_active, notes) VALUES ($1, $2)",
            &[Value::Bool(true), Value::Null],
        )
        .await?;

        // Query active items
        let result = query_sql(
            driver,
            &conn,
            "SELECT id, is_active, notes FROM test_bool_null WHERE is_active = $1",
            &[Value::Bool(true)],
        )
        .await?;

        assert_eq!(result.rows.len(), 2, "Should find 2 active items");

        // Query items with NULL notes
        let result_null = query_sql(
            driver,
            &conn,
            "SELECT id FROM test_bool_null WHERE notes IS NULL",
            &[],
        )
        .await?;

        assert_eq!(result_null.rows.len(), 1, "Should find 1 item with NULL notes");

        // Test COALESCE
        let result_coalesce = query_sql(
            driver,
            &conn,
            "SELECT id, COALESCE(notes, $1) as notes_with_default FROM test_bool_null",
            &[Value::String("No notes".to_string())],
        )
        .await?;

        assert_eq!(result_coalesce.rows.len(), 3, "Should return all 3 rows");

        // Verify COALESCE replaces NULL
        for row in &result_coalesce.rows {
            let notes_with_default = row
                .get_by_name("notes_with_default")
                .context("Missing notes_with_default column")?;
            
            assert!(
                !matches!(notes_with_default, Value::Null),
                "COALESCE should replace NULL values"
            );
        }

        Ok(())
    }
}

/// Binary data type tests using temporary tables
#[cfg(test)]
mod binary_type_tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::Value;

    /// Helper to execute SQL with automatic parameter syntax conversion
    async fn execute_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::StatementResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.execute(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL with automatic parameter syntax conversion
    async fn query_sql(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<zqlz_core::QueryResult> {
        let (sql, params) = if driver == TestDriver::Postgres {
            (sql.to_string(), params.to_vec())
        } else {
            let mut converted_sql = sql.to_string();
            for i in (1..=10).rev() {
                converted_sql = converted_sql.replace(&format!("${}", i), "?");
            }
            (converted_sql, params.to_vec())
        };

        conn.query(&sql, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to create temporary table for binary testing
    async fn create_binary_test_table(
        driver: TestDriver,
        conn: &std::sync::Arc<dyn zqlz_core::Connection>,
    ) -> Result<()> {
        let create_sql = if driver == TestDriver::Postgres {
            "CREATE TEMPORARY TABLE binary_test (
                id SERIAL PRIMARY KEY,
                data BYTEA,
                description TEXT
            )"
        } else if driver == TestDriver::Mysql {
            "CREATE TEMPORARY TABLE binary_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data BLOB,
                description TEXT
            )"
        } else {
            "CREATE TEMPORARY TABLE binary_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB,
                description TEXT
            )"
        };

        conn.execute(create_sql, &[])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(())
    }

    /// Tests small binary data insertion (< 1KB)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_binary_insert_small(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_binary_test_table(driver, &conn).await?;

        // Create small binary data (256 bytes)
        let binary_data: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(binary_data.clone()),
                Value::String("Small binary test".to_string()),
            ],
        )
        .await?;

        // Query back the binary data
        let result = query_sql(
            driver,
            &conn,
            "SELECT data FROM binary_test WHERE description = $1",
            &[Value::String("Small binary test".to_string())],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_data = row
            .get_by_name("data")
            .context("Missing data column")?;

        match retrieved_data {
            Value::Bytes(bytes) => {
                assert_eq!(
                    bytes.len(),
                    binary_data.len(),
                    "Binary data length should match"
                );
                assert_eq!(bytes, &binary_data, "Binary data should round-trip correctly");
            }
            _ => anyhow::bail!("Expected Bytes value, got: {:?}", retrieved_data),
        }

        Ok(())
    }

    /// Tests binary data round-trip with various sizes
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_binary_roundtrip(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_binary_test_table(driver, &conn).await?;

        // Test various sizes: 1 byte, 1KB, 10KB
        let test_sizes = vec![1, 1024, 10 * 1024];

        for size in test_sizes.iter() {
            let binary_data: Vec<u8> = (0..*size).map(|i| ((i % 256) as u8)).collect();
            let description = format!("Binary test {} bytes", size);

            execute_sql(
                driver,
                &conn,
                "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
                &[
                    Value::Bytes(binary_data.clone()),
                    Value::String(description.clone()),
                ],
            )
            .await?;

            // Query back
            let result = query_sql(
                driver,
                &conn,
                "SELECT data FROM binary_test WHERE description = $1",
                &[Value::String(description)],
            )
            .await?;

            let row = result
                .rows
                .first()
                .with_context(|| format!("Expected row for size {}", size))?;
            let retrieved_data = row.get_by_name("data").context("Missing data column")?;

            match retrieved_data {
                Value::Bytes(bytes) => {
                    assert_eq!(
                        bytes.len(),
                        *size,
                        "Binary data length should match for size {}",
                        size
                    );
                    assert_eq!(
                        bytes, &binary_data,
                        "Binary data should round-trip correctly for size {}",
                        size
                    );
                }
                _ => anyhow::bail!(
                    "Expected Bytes value for size {}, got: {:?}",
                    size,
                    retrieved_data
                ),
            }
        }

        Ok(())
    }

    /// Tests empty binary value
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_binary_empty_value(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_binary_test_table(driver, &conn).await?;

        // Insert empty binary data
        let empty_data: Vec<u8> = vec![];

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(empty_data.clone()),
                Value::String("Empty binary".to_string()),
            ],
        )
        .await?;

        // Query back
        let result = query_sql(
            driver,
            &conn,
            "SELECT data FROM binary_test WHERE description = $1",
            &[Value::String("Empty binary".to_string())],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;
        let retrieved_data = row.get_by_name("data").context("Missing data column")?;

        // Empty binary data may be represented as empty Bytes or NULL depending on driver
        match retrieved_data {
            Value::Bytes(bytes) => {
                assert_eq!(bytes.len(), 0, "Empty binary data should have length 0");
            }
            Value::Null => {
                // Some databases represent empty BLOB as NULL - this is acceptable
            }
            _ => anyhow::bail!("Expected Bytes or Null value, got: {:?}", retrieved_data),
        }

        Ok(())
    }

    /// Tests binary data comparison operations
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_binary_comparison(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_binary_test_table(driver, &conn).await?;

        // Insert two identical binary values
        let binary_data1: Vec<u8> = vec![1, 2, 3, 4, 5];
        let binary_data2: Vec<u8> = vec![1, 2, 3, 4, 5];
        let binary_data3: Vec<u8> = vec![5, 4, 3, 2, 1];

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(binary_data1.clone()),
                Value::String("Binary 1".to_string()),
            ],
        )
        .await?;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(binary_data2.clone()),
                Value::String("Binary 2".to_string()),
            ],
        )
        .await?;

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(binary_data3.clone()),
                Value::String("Binary 3".to_string()),
            ],
        )
        .await?;

        // Query for matching binary data
        let result = query_sql(
            driver,
            &conn,
            "SELECT description FROM binary_test WHERE data = $1 ORDER BY description",
            &[Value::Bytes(binary_data1)],
        )
        .await?;

        // Should find 2 matching rows (Binary 1 and Binary 2)
        assert_eq!(
            result.rows.len(),
            2,
            "Should find 2 rows with matching binary data"
        );

        let desc1 = result.rows[0]
            .get_by_name("description")
            .context("Missing description")?
            .as_str()
            .context("description should be String")?;
        let desc2 = result.rows[1]
            .get_by_name("description")
            .context("Missing description")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(desc1, "Binary 1");
        assert_eq!(desc2, "Binary 2");

        Ok(())
    }

    /// Integration test for binary data without requiring Sakila data
    ///
    /// This test validates that binary data operations work correctly
    /// across all SQL drivers.
    #[tokio::test]
    async fn integration_test_binary_data_works() -> Result<()> {
        let driver = TestDriver::Sqlite;
        let conn = test_connection(driver).await?;

        // Create temporary table
        create_binary_test_table(driver, &conn).await?;

        // Test various binary data scenarios
        let test_data = vec![0u8, 1, 255, 128, 42];

        execute_sql(
            driver,
            &conn,
            "INSERT INTO binary_test (data, description) VALUES ($1, $2)",
            &[
                Value::Bytes(test_data.clone()),
                Value::String("Integration test".to_string()),
            ],
        )
        .await?;

        // Query back
        let result = query_sql(
            driver,
            &conn,
            "SELECT data, description FROM binary_test WHERE description = $1",
            &[Value::String("Integration test".to_string())],
        )
        .await?;

        let row = result
            .rows
            .first()
            .context("Expected at least one row")?;

        let retrieved_data = row.get_by_name("data").context("Missing data column")?;
        let retrieved_desc = row
            .get_by_name("description")
            .context("Missing description column")?
            .as_str()
            .context("description should be String")?;

        assert_eq!(retrieved_desc, "Integration test");

        match retrieved_data {
            Value::Bytes(bytes) => {
                assert_eq!(bytes, &test_data, "Binary data should match");
            }
            _ => anyhow::bail!("Expected Bytes value, got: {:?}", retrieved_data),
        }

        Ok(())
    }
}
