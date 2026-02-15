//! JSON data type tests for database drivers.
//!
//! This module tests JSON data storage, retrieval, and manipulation across
//! PostgreSQL, MySQL, and SQLite drivers. Each test is parameterized to run
//! against all applicable drivers using rstest.
//!
//! Test categories:
//! - JSON insert and retrieval
//! - JSON extraction and path queries
//! - JSON array handling
//! - JSON object creation
//! - Nested JSON structures
//! - NULL handling in JSON

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::{Connection, QueryResult, StatementResult, Value};

    /// Helper to execute SQL that works across drivers (handles parameter syntax).
    async fn execute_sql(
        conn: &dyn Connection,
        sql: &str,
        params: &[Value],
        driver: TestDriver,
    ) -> Result<StatementResult> {
        let sql = match driver {
            TestDriver::Postgres => sql
                .replace(
                    "INSERT INTO json_test (data) VALUES ($1)",
                    "INSERT INTO json_test (data) VALUES ($1::jsonb)",
                )
                .replace("SET data = $1", "SET data = $1::jsonb"),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut result = sql.to_string();
                for i in (1..=params.len()).rev() {
                    result = result.replace(&format!("${}", i), "?");
                }
                result
            }
            TestDriver::Redis => sql.to_string(),
        };
        conn.execute(&sql, params).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Helper to query SQL that works across drivers.
    async fn query_sql(
        conn: &dyn Connection,
        sql: &str,
        params: &[Value],
        driver: TestDriver,
    ) -> Result<QueryResult> {
        let sql = match driver {
            TestDriver::Postgres => sql.to_string(),
            TestDriver::Mysql | TestDriver::Sqlite => {
                let mut result = sql.to_string();
                for i in (1..=params.len()).rev() {
                    result = result.replace(&format!("${}", i), "?");
                }
                result
            }
            TestDriver::Redis => sql.to_string(),
        };
        conn.query(&sql, params).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    fn json_value_as_string(value: &Value) -> Option<String> {
        match value {
            Value::String(text) => Some(text.clone()),
            Value::Json(json_value) => Some(json_value.to_string()),
            _ => value.as_str().map(|text| text.to_string()),
        }
    }

    /// Helper function to create a temporary JSON table for testing
    async fn create_json_table(conn: &dyn Connection, driver: TestDriver) -> Result<()> {
        let create_sql = match driver {
            TestDriver::Postgres => {
                "CREATE TEMPORARY TABLE json_test (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            }
            TestDriver::Mysql => {
                "CREATE TEMPORARY TABLE json_test (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            }
            TestDriver::Sqlite => {
                "CREATE TEMPORARY TABLE json_test (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"
            }
            _ => anyhow::bail!("JSON tests only support SQL drivers"),
        };

        conn.execute(create_sql, &[]).await?;
        Ok(())
    }

    /// Helper function to drop the temporary JSON table
    async fn drop_json_table(conn: &dyn Connection) -> Result<()> {
        conn.execute("DROP TABLE IF EXISTS json_test", &[]).await?;
        Ok(())
    }

    /// Test inserting and retrieving simple JSON objects
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_insert_and_retrieve(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert JSON object
        let json_data = r#"{"name": "John Doe", "age": 30, "active": true}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Retrieve JSON
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        // Verify JSON contains expected data (allow for formatting differences)
        assert!(data.contains("John Doe"), "JSON should contain name");
        assert!(data.contains("30"), "JSON should contain age");
        assert!(data.contains("true") || data.contains("1"), "JSON should contain active=true");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test JSON extraction using path queries
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_json_extract(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert JSON object
        let json_data = r#"{"person": {"name": "Alice", "age": 25}}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Extract nested value using driver-specific syntax
        let extract_sql = match driver {
            TestDriver::Postgres => "SELECT data->'person'->>'name' as name FROM json_test WHERE id = 1",
            TestDriver::Mysql => "SELECT JSON_EXTRACT(data, '$.person.name') as name FROM json_test WHERE id = 1",
            _ => anyhow::bail!("JSON extraction not implemented for this driver"),
        };

        let result = query_sql(conn.as_ref(), extract_sql, &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let name = result.rows[0]
            .get_by_name("name")
            .context("Missing name column")?
            .as_str()
            .context("name should be string")?;

        // MySQL returns values with quotes, Postgres doesn't
        assert!(
            name == "Alice" || name == "\"Alice\"",
            "Expected name to be Alice (got: {})",
            name
        );

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test JSON array handling
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_array_handling(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert JSON array
        let json_data = r#"{"tags": ["rust", "database", "json"]}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Retrieve JSON
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        // Verify JSON contains array
        assert!(data.contains("rust"), "JSON should contain 'rust'");
        assert!(data.contains("database"), "JSON should contain 'database'");
        assert!(data.contains("json"), "JSON should contain 'json'");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test JSON object creation with nested structures
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_nested_extraction(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert complex nested JSON
        let json_data = r#"{
            "user": {
                "profile": {
                    "name": "Bob",
                    "email": "bob@example.com"
                },
                "settings": {
                    "notifications": true,
                    "theme": "dark"
                }
            }
        }"#;

        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Retrieve and verify nested structure
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        // Verify nested data is present
        assert!(data.contains("Bob"), "JSON should contain name");
        assert!(data.contains("bob@example.com"), "JSON should contain email");
        assert!(data.contains("dark"), "JSON should contain theme");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test NULL handling in JSON
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_null_handling(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert JSON with null values
        let json_data = r#"{"name": "Charlie", "middle_name": null, "age": 40}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Retrieve JSON
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        // Verify null is preserved in JSON
        assert!(data.contains("null"), "JSON should contain null value");
        assert!(data.contains("Charlie"), "JSON should contain name");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test inserting NULL as entire JSON column value
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_column_null(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert NULL into JSON column
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::Null],
            driver,
        )
        .await?;

        // Retrieve NULL value
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0].get_by_name("data").context("Missing data column")?;

        // Verify column value is NULL
        assert!(matches!(data, Value::Null), "Expected NULL value");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test JSON with special characters
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_special_characters(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert JSON with special characters
        let json_data = r#"{"message": "Hello \"World\"", "emoji": "😀", "newline": "line1\nline2"}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Retrieve JSON
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        // Verify special characters are preserved (escaped)
        assert!(
            data.contains("World") || data.contains("\\\"World\\\""),
            "JSON should contain escaped quotes"
        );
        assert!(data.contains("😀"), "JSON should contain emoji");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Test updating JSON data
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[case::sqlite(TestDriver::Sqlite)]
    #[tokio::test]
    async fn test_json_update(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;
        create_json_table(conn.as_ref(), driver).await?;

        // Insert initial JSON
        let json_data = r#"{"counter": 1, "status": "active"}"#;
        execute_sql(
            conn.as_ref(),
            "INSERT INTO json_test (data) VALUES ($1)",
            &[Value::String(json_data.to_string())],
            driver,
        )
        .await?;

        // Update JSON
        let new_json = r#"{"counter": 2, "status": "inactive"}"#;
        execute_sql(
            conn.as_ref(),
            "UPDATE json_test SET data = $1 WHERE id = 1",
            &[Value::String(new_json.to_string())],
            driver,
        )
        .await?;

        // Verify update
        let result = query_sql(conn.as_ref(), "SELECT data FROM json_test WHERE id = 1", &[], driver).await?;

        assert_eq!(result.rows.len(), 1, "Expected 1 row");
        let data = result.rows[0]
            .get_by_name("data")
            .context("Missing data column")
            .and_then(|value| json_value_as_string(value).context("data should be string or json"))?;

        assert!(data.contains("2"), "JSON should contain counter=2");
        assert!(data.contains("inactive"), "JSON should contain status=inactive");

        drop_json_table(conn.as_ref()).await?;
        Ok(())
    }

    /// Integration test for basic JSON functionality (works without Docker)
    #[tokio::test]
    async fn integration_test_json_works() -> Result<()> {
        use zqlz_driver_sqlite::SqliteDriver;
        use zqlz_core::DatabaseDriver;

        // Use in-memory SQLite database
        let driver = SqliteDriver::new();
        let config = zqlz_core::ConnectionConfig {
            id: uuid::Uuid::new_v4(),
            name: "test".to_string(),
            driver: "sqlite".to_string(),
            host: String::new(),
            port: 0,
            database: Some(":memory:".to_string()),
            username: None,
            password: None,
            params: std::collections::HashMap::new(),
            color: None,
            group: None,
            notes: None,
            created_at: chrono::Utc::now(),
            last_used_at: None,
        };

        let conn = driver.connect(&config).await?;

        // Create table
        conn.execute(
            "CREATE TABLE test_json (id INTEGER PRIMARY KEY, data TEXT)",
            &[],
        )
        .await?;

        // Insert JSON
        let json_data = r#"{"test": "value", "number": 42}"#;
        conn.execute(
            "INSERT INTO test_json (id, data) VALUES (?, ?)",
            &[Value::Int64(1), Value::String(json_data.to_string())],
        )
        .await?;

        // Query JSON
        let result = conn.query("SELECT data FROM test_json WHERE id = ?", &[Value::Int64(1)]).await?;

        assert_eq!(result.rows.len(), 1);
        let data = result.rows[0].get_by_name("data").context("Missing data column")?;
        let json_str = data.as_str().context("data should be string")?;
        assert!(json_str.contains("test"));
        assert!(json_str.contains("42"));

        Ok(())
    }
}
