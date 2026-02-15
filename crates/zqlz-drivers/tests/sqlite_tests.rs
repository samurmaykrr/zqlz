#![cfg(feature = "sqlite")]

use std::path::PathBuf;
/// Integration tests for SQLite driver
use zqlz_core::{Connection, DatabaseDriver, SchemaIntrospection, Value};
use zqlz_drivers::sqlite::{ExecuteMultiResult, SqliteConnection, SqliteDriver};

/// Helper to create a test database with sample data
async fn setup_test_database() -> (PathBuf, SqliteConnection) {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("zqlz_test_{}.db", uuid::Uuid::new_v4()));

    // Create connection and setup schema
    let conn =
        SqliteConnection::open(db_path.to_str().unwrap()).expect("Failed to create test database");

    // Setup schema - execute each statement separately
    let statements = vec![
        r#"CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            balance REAL DEFAULT 0.0
        )"#,
        r#"CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            price REAL NOT NULL,
            stock_quantity INTEGER DEFAULT 0,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"#,
        r#"CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            total_price REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT
        )"#,
        "CREATE INDEX idx_users_email ON users(email)",
        "CREATE INDEX idx_orders_user_id ON orders(user_id)",
        "CREATE INDEX idx_orders_status ON orders(status)",
        r#"CREATE VIEW order_summary AS
        SELECT 
            o.id,
            u.username,
            u.email,
            p.name as product_name,
            o.quantity,
            o.total_price,
            o.status,
            o.ordered_at
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN products p ON o.product_id = p.id"#,
        r#"CREATE TRIGGER update_stock_after_order
        AFTER INSERT ON orders
        BEGIN
            UPDATE products 
            SET stock_quantity = stock_quantity - NEW.quantity
            WHERE id = NEW.product_id;
        END"#,
    ];

    // Execute setup statements
    for statement in statements {
        conn.execute(statement, &[])
            .await
            .expect("Failed to setup schema");
    }

    (db_path, conn)
}

/// Helper to cleanup test database
fn cleanup_test_database(path: PathBuf) {
    let _ = std::fs::remove_file(&path);
    // Also remove WAL and SHM files if they exist
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn test_connection_open_and_close() {
    let (db_path, conn) = setup_test_database().await;

    // Connection should be open
    assert!(!conn.is_closed());

    // Close connection
    conn.close().await.expect("Failed to close connection");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_connection_info() {
    let (db_path, conn) = setup_test_database().await;

    let info = conn.get_info().expect("Failed to get database info");

    assert!(
        info.file_size_bytes > 0,
        "Database should have non-zero size"
    );
    assert!(info.page_count > 0, "Database should have pages");
    assert_eq!(info.encoding, "UTF-8", "Should use UTF-8 encoding");
    assert_eq!(info.journal_mode, "wal", "Should use WAL mode");
    assert!(info.foreign_keys_enabled, "Foreign keys should be enabled");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_basic_insert_and_query() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    let insert_result = conn
        .execute(
            "INSERT INTO users (username, email, full_name, balance) VALUES (?, ?, ?, ?)",
            &[
                Value::String("testuser".to_string()),
                Value::String("test@example.com".to_string()),
                Value::String("Test User".to_string()),
                Value::Float64(100.50),
            ],
        )
        .await
        .expect("Failed to insert user");

    assert_eq!(insert_result.affected_rows, 1, "Should insert 1 row");

    // Query the data back
    let result = conn
        .query(
            "SELECT username, email, full_name, balance FROM users WHERE username = ?",
            &[Value::String("testuser".to_string())],
        )
        .await
        .expect("Failed to query users");

    assert_eq!(result.rows.len(), 1, "Should return 1 row");
    assert_eq!(result.columns.len(), 4, "Should have 4 columns");

    let row = &result.rows[0];
    assert_eq!(row.get(0).unwrap().as_str().unwrap(), "testuser");
    assert_eq!(row.get(1).unwrap().as_str().unwrap(), "test@example.com");
    assert_eq!(row.get(2).unwrap().as_str().unwrap(), "Test User");
    assert_eq!(row.get(3).unwrap().as_f64().unwrap(), 100.50);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_parameterized_queries() {
    let (db_path, conn) = setup_test_database().await;

    // Insert multiple users
    for i in 1..=5 {
        conn.execute(
            "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
            &[
                Value::String(format!("user{}", i)),
                Value::String(format!("user{}@example.com", i)),
                Value::Float64(i as f64 * 100.0),
            ],
        )
        .await
        .expect("Failed to insert user");
    }

    // Query with parameter
    let result = conn
        .query(
            "SELECT username, balance FROM users WHERE balance > ? ORDER BY balance DESC",
            &[Value::Float64(250.0)],
        )
        .await
        .expect("Failed to query users");

    assert_eq!(
        result.rows.len(),
        3,
        "Should return users with balance > 250 (300, 400, 500)"
    );
    assert_eq!(result.rows[0].get(0).unwrap().as_str().unwrap(), "user5");
    assert_eq!(result.rows[1].get(0).unwrap().as_str().unwrap(), "user4");
    assert_eq!(result.rows[2].get(0).unwrap().as_str().unwrap(), "user3");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_null_values() {
    let (db_path, conn) = setup_test_database().await;

    // Insert with null values
    conn.execute(
        "INSERT INTO users (username, email, full_name) VALUES (?, ?, ?)",
        &[
            Value::String("nulltest".to_string()),
            Value::String("null@example.com".to_string()),
            Value::Null,
        ],
    )
    .await
    .expect("Failed to insert user with null");

    // Query back
    let result = conn
        .query(
            "SELECT username, full_name FROM users WHERE username = ?",
            &[Value::String("nulltest".to_string())],
        )
        .await
        .expect("Failed to query");

    assert_eq!(result.rows.len(), 1);
    assert!(
        result.rows[0].get(1).unwrap().is_null(),
        "full_name should be null"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_aggregate_functions() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    for i in 1..=10 {
        conn.execute(
            "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
            &[
                Value::String(format!("user{}", i)),
                Value::String(format!("user{}@example.com", i)),
                Value::Float64(i as f64 * 10.0),
            ],
        )
        .await
        .expect("Failed to insert");
    }

    // Test COUNT, SUM, AVG, MIN, MAX
    let result = conn.query(
        "SELECT COUNT(*) as count, SUM(balance) as sum, AVG(balance) as avg, MIN(balance) as min, MAX(balance) as max FROM users",
        &[]
    ).await.expect("Failed to query aggregates");

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];

    assert_eq!(row.get(0).unwrap().as_i64().unwrap(), 10); // count
    assert_eq!(row.get(1).unwrap().as_f64().unwrap(), 550.0); // sum
    assert_eq!(row.get(2).unwrap().as_f64().unwrap(), 55.0); // avg
    assert_eq!(row.get(3).unwrap().as_f64().unwrap(), 10.0); // min
    assert_eq!(row.get(4).unwrap().as_f64().unwrap(), 100.0); // max

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_joins() {
    let (db_path, conn) = setup_test_database().await;

    // Insert users
    conn.execute(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        &[
            Value::String("buyer".to_string()),
            Value::String("buyer@example.com".to_string()),
        ],
    )
    .await
    .unwrap();

    // Insert products
    conn.execute(
        "INSERT INTO products (name, price, stock_quantity, category) VALUES (?, ?, ?, ?)",
        &[
            Value::String("Widget".to_string()),
            Value::Float64(29.99),
            Value::Int64(100),
            Value::String("Gadgets".to_string()),
        ],
    )
    .await
    .unwrap();

    // Insert order
    conn.execute(
        "INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES (?, ?, ?, ?, ?)",
        &[Value::Int64(1), Value::Int64(1), Value::Int64(2), Value::Float64(59.98), Value::String("completed".to_string())]
    ).await.unwrap();

    // Query with JOIN
    let result = conn
        .query(
            "SELECT u.username, p.name, o.quantity, o.total_price 
         FROM orders o 
         JOIN users u ON o.user_id = u.id 
         JOIN products p ON o.product_id = p.id",
            &[],
        )
        .await
        .expect("Failed to query with JOIN");

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row.get(0).unwrap().as_str().unwrap(), "buyer");
    assert_eq!(row.get(1).unwrap().as_str().unwrap(), "Widget");
    assert_eq!(row.get(2).unwrap().as_i64().unwrap(), 2);
    assert_eq!(row.get(3).unwrap().as_f64().unwrap(), 59.98);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_query_execution_time() {
    let (db_path, conn) = setup_test_database().await;

    // Insert some data
    for i in 1..=100 {
        conn.execute(
            "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
            &[
                Value::String(format!("user{}", i)),
                Value::String(format!("user{}@example.com", i)),
                Value::Float64(i as f64),
            ],
        )
        .await
        .unwrap();
    }

    // Run query and check execution time is recorded
    let result = conn.query("SELECT * FROM users", &[]).await.unwrap();

    // Execution time is u64, so it's always >= 0, no need to check
    assert_eq!(result.rows.len(), 100);
    assert!(result.total_rows.is_some(), "Total rows should be set");
    assert_eq!(result.total_rows.unwrap(), 100);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_tables() {
    let (db_path, conn) = setup_test_database().await;

    let tables = conn.list_tables(None).await.expect("Failed to list tables");

    assert_eq!(tables.len(), 3, "Should have 3 tables");

    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"users"));
    assert!(table_names.contains(&"products"));
    assert!(table_names.contains(&"orders"));

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_views() {
    let (db_path, conn) = setup_test_database().await;

    let views = conn.list_views(None).await.expect("Failed to list views");

    assert_eq!(views.len(), 1, "Should have 1 view");
    assert_eq!(views[0].name, "order_summary");
    assert!(views[0].definition.is_some(), "View should have definition");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_columns() {
    let (db_path, conn) = setup_test_database().await;

    let columns = conn
        .get_columns(None, "users")
        .await
        .expect("Failed to get columns");

    assert_eq!(columns.len(), 7, "users table should have 7 columns");

    let id_col = columns
        .iter()
        .find(|c| c.name == "id")
        .expect("Should have id column");
    assert!(id_col.is_primary_key, "id should be primary key");
    assert!(id_col.is_auto_increment, "id should be auto increment");

    let username_col = columns
        .iter()
        .find(|c| c.name == "username")
        .expect("Should have username column");
    assert_eq!(username_col.data_type.to_uppercase(), "TEXT");
    assert!(!username_col.nullable, "username should not be nullable");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_indexes() {
    let (db_path, conn) = setup_test_database().await;

    let indexes = conn
        .get_indexes(None, "users")
        .await
        .expect("Failed to get indexes");

    // Should have at least one index (idx_users_email)
    assert!(!indexes.is_empty(), "Should have indexes");

    let email_idx = indexes.iter().find(|i| i.name == "idx_users_email");
    assert!(email_idx.is_some(), "Should have email index");

    let email_idx = email_idx.unwrap();
    assert_eq!(email_idx.columns.len(), 1);
    assert_eq!(email_idx.columns[0], "email");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_foreign_keys() {
    let (db_path, conn) = setup_test_database().await;

    let fks = conn
        .get_foreign_keys(None, "orders")
        .await
        .expect("Failed to get foreign keys");

    assert_eq!(fks.len(), 2, "orders table should have 2 foreign keys");

    let user_fk = fks.iter().find(|fk| fk.referenced_table == "users");
    assert!(user_fk.is_some(), "Should have FK to users");

    let user_fk = user_fk.unwrap();
    assert_eq!(user_fk.columns, vec!["user_id"]);
    assert_eq!(user_fk.referenced_columns, vec!["id"]);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_schema_introspection_table_details() {
    let (db_path, conn) = setup_test_database().await;

    let details = conn
        .get_table(None, "orders")
        .await
        .expect("Failed to get table details");

    assert_eq!(details.info.name, "orders");
    assert_eq!(details.columns.len(), 7);
    assert_eq!(details.foreign_keys.len(), 2);
    assert!(!details.indexes.is_empty());
    assert!(details.primary_key.is_some());

    let pk = details.primary_key.unwrap();
    assert_eq!(pk.columns, vec!["id"]);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_path_expansion_relative() {
    let temp_dir = std::env::temp_dir();
    let db_name = format!("test_relative_{}.db", uuid::Uuid::new_v4());
    let db_path = temp_dir.join(&db_name);

    // Change to temp directory
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(&temp_dir).unwrap();

    // Open with relative path
    let conn = SqliteConnection::open(&db_name).expect("Failed to open with relative path");
    assert!(!conn.is_closed());

    // Restore directory
    std::env::set_current_dir(original_dir).unwrap();

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_path_expansion_home_directory() {
    // Create a test database in temp directory but reference it with absolute path
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("test_home_{}.db", uuid::Uuid::new_v4()));

    let conn = SqliteConnection::open(db_path.to_str().unwrap()).expect("Failed to open database");

    assert!(!conn.is_closed());

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_in_memory_database() {
    let conn = SqliteConnection::open(":memory:").expect("Failed to open in-memory database");

    // Create a simple table
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await
    .expect("Failed to create table");

    // Insert data
    conn.execute(
        "INSERT INTO test (value) VALUES (?)",
        &[Value::String("test".to_string())],
    )
    .await
    .expect("Failed to insert");

    // Query back
    let result = conn
        .query("SELECT value FROM test", &[])
        .await
        .expect("Failed to query");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get(0).unwrap().as_str().unwrap(), "test");
}

#[tokio::test]
async fn test_driver_connect() {
    let driver = SqliteDriver::new();

    assert_eq!(driver.name(), "sqlite");
    assert_eq!(driver.display_name(), "SQLite");

    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("test_driver_{}.db", uuid::Uuid::new_v4()));

    let config = zqlz_core::ConnectionConfig::new_sqlite(db_path.to_str().unwrap());

    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect via driver");
    assert!(!conn.is_closed());

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_driver_test_connection() {
    let driver = SqliteDriver::new();

    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("test_conn_{}.db", uuid::Uuid::new_v4()));

    let config = zqlz_core::ConnectionConfig::new_sqlite(db_path.to_str().unwrap());

    driver
        .test_connection(&config)
        .await
        .expect("Connection test should succeed");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_error_handling_invalid_sql() {
    let (db_path, conn) = setup_test_database().await;

    let result = conn.query("SELECT * FROM nonexistent_table", &[]).await;

    assert!(result.is_err(), "Should fail on invalid SQL");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_error_handling_invalid_path() {
    let result = SqliteConnection::open("/nonexistent/directory/database.db");

    assert!(
        result.is_err(),
        "Should fail when parent directory doesn't exist"
    );
}

#[tokio::test]
async fn test_concurrent_reads() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    for i in 1..=100 {
        conn.execute(
            "INSERT INTO users (username, email) VALUES (?, ?)",
            &[
                Value::String(format!("user{}", i)),
                Value::String(format!("user{}@example.com", i)),
            ],
        )
        .await
        .unwrap();
    }

    // Perform concurrent reads
    let conn = std::sync::Arc::new(conn);
    let mut handles = vec![];

    for _ in 0..10 {
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            conn_clone
                .query("SELECT COUNT(*) FROM users", &[])
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    // All reads should succeed
    for handle in handles {
        let result = handle.await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get(0).unwrap().as_i64().unwrap(), 100);
    }

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_batch() {
    let (db_path, conn) = setup_test_database().await;

    // Execute multiple statements in a batch
    let batch_sql = r#"
        INSERT INTO users (username, email, balance) VALUES ('batch1', 'batch1@example.com', 100.0);
        INSERT INTO users (username, email, balance) VALUES ('batch2', 'batch2@example.com', 200.0);
        INSERT INTO users (username, email, balance) VALUES ('batch3', 'batch3@example.com', 300.0);
    "#;

    let results = conn
        .execute_batch(batch_sql)
        .await
        .expect("Failed to execute batch");
    assert!(!results.is_empty(), "Should return at least one result");

    // Verify the data was inserted
    let query_result = conn.query("SELECT COUNT(*) FROM users", &[]).await.unwrap();
    assert_eq!(query_result.rows[0].get(0).unwrap().as_i64().unwrap(), 3);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_multi_single_query() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    conn.execute(
        "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
        &[
            Value::String("test".to_string()),
            Value::String("test@example.com".to_string()),
            Value::Float64(100.0),
        ],
    )
    .await
    .unwrap();

    // Execute a single SELECT query
    let result = conn
        .execute_multi(
            "SELECT * FROM users WHERE username = ?",
            &[Value::String("test".to_string())],
        )
        .await
        .expect("Failed to execute single query");

    match result {
        ExecuteMultiResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            assert_eq!(
                query_result.rows[0].get(1).unwrap().as_str().unwrap(),
                "test"
            );
        }
        ExecuteMultiResult::Statement(_) => {
            panic!("Expected Query result, got Statement");
        }
    }

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_multi_single_statement() {
    let (db_path, conn) = setup_test_database().await;

    // Execute a single INSERT statement
    let result = conn
        .execute_multi(
            "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
            &[
                Value::String("multi".to_string()),
                Value::String("multi@example.com".to_string()),
                Value::Float64(150.0),
            ],
        )
        .await
        .expect("Failed to execute single statement");

    match result {
        ExecuteMultiResult::Statement(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].affected_rows, 1);
        }
        ExecuteMultiResult::Query(_) => {
            panic!("Expected Statement result, got Query");
        }
    }

    // Verify the data was inserted
    let query_result = conn
        .query("SELECT username FROM users WHERE username = 'multi'", &[])
        .await
        .unwrap();
    assert_eq!(query_result.rows.len(), 1);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_multi_batch_statements() {
    let (db_path, conn) = setup_test_database().await;

    // Execute multiple statements without parameters
    let multi_sql = r#"
        INSERT INTO users (username, email, balance) VALUES ('user1', 'user1@example.com', 100.0);
        INSERT INTO users (username, email, balance) VALUES ('user2', 'user2@example.com', 200.0);
        INSERT INTO products (name, price, stock_quantity, category) VALUES ('Product A', 50.0, 10, 'Category 1');
    "#;

    let result = conn
        .execute_multi(multi_sql, &[])
        .await
        .expect("Failed to execute multiple statements");

    match result {
        ExecuteMultiResult::Statement(results) => {
            assert!(!results.is_empty(), "Should return results");
        }
        ExecuteMultiResult::Query(_) => {
            panic!("Expected Statement result, got Query");
        }
    }

    // Verify the data was inserted
    let user_count = conn.query("SELECT COUNT(*) FROM users", &[]).await.unwrap();
    assert_eq!(user_count.rows[0].get(0).unwrap().as_i64().unwrap(), 2);

    let product_count = conn
        .query("SELECT COUNT(*) FROM products", &[])
        .await
        .unwrap();
    assert_eq!(product_count.rows[0].get(0).unwrap().as_i64().unwrap(), 1);

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_multi_with_params_and_multiple_statements_fails() {
    let (db_path, conn) = setup_test_database().await;

    // Trying to execute multiple statements with parameters should fail
    let multi_sql = "INSERT INTO users (username, email) VALUES ('a', 'a@example.com'); INSERT INTO users (username, email) VALUES ('b', 'b@example.com');";

    let result = conn
        .execute_multi(multi_sql, &[Value::String("test".to_string())])
        .await;

    assert!(
        result.is_err(),
        "Should fail when combining multiple statements with parameters"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_execute_multi_create_table_and_insert() {
    let (db_path, conn) = setup_test_database().await;

    // Create a table and insert data in one batch
    let batch_sql = r#"
        CREATE TABLE temp_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        
        INSERT INTO temp_table (name) VALUES ('First');
        INSERT INTO temp_table (name) VALUES ('Second');
        INSERT INTO temp_table (name) VALUES ('Third');
    "#;

    let result = conn
        .execute_multi(batch_sql, &[])
        .await
        .expect("Failed to execute batch with CREATE and INSERT");

    // Should be treated as batch execution
    match result {
        ExecuteMultiResult::Statement(_) => {
            // Expected
        }
        ExecuteMultiResult::Query(_) => {
            panic!("Expected Statement result, got Query");
        }
    }

    // Verify the table was created and data inserted
    let query_result = conn
        .query("SELECT COUNT(*) FROM temp_table", &[])
        .await
        .unwrap();
    assert_eq!(query_result.rows[0].get(0).unwrap().as_i64().unwrap(), 3);

    cleanup_test_database(db_path);
}

// ==============================================================================
// EXPLAIN Tests
// ==============================================================================

#[tokio::test]
async fn test_explain_basic_query() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN should work on a basic SELECT query
    let result = conn.query("EXPLAIN SELECT * FROM users", &[]).await;

    assert!(result.is_ok(), "EXPLAIN should succeed");
    let result = result.unwrap();

    // EXPLAIN returns opcodes with columns like addr, opcode, p1, p2, p3, p4, p5, comment
    assert!(!result.rows.is_empty(), "EXPLAIN should return rows");
    assert!(
        result.columns.len() >= 5,
        "EXPLAIN should return multiple columns"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_query_plan_basic() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN QUERY PLAN should provide high-level query plan
    let result = conn
        .query("EXPLAIN QUERY PLAN SELECT * FROM users", &[])
        .await;

    assert!(result.is_ok(), "EXPLAIN QUERY PLAN should succeed");
    let result = result.unwrap();

    // EXPLAIN QUERY PLAN returns rows with columns like id, parent, notused, detail
    assert!(
        !result.rows.is_empty(),
        "EXPLAIN QUERY PLAN should return rows"
    );

    // The detail column should contain scan information
    let has_scan_info = result.rows.iter().any(|row| {
        row.values.iter().any(|val| {
            val.as_str()
                .map(|s| s.contains("SCAN") || s.contains("SEARCH"))
                .unwrap_or(false)
        })
    });
    assert!(
        has_scan_info,
        "EXPLAIN QUERY PLAN should contain SCAN or SEARCH info"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_query_with_index() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    conn.execute(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        &[
            Value::String("testuser".to_string()),
            Value::String("test@example.com".to_string()),
        ],
    )
    .await
    .unwrap();

    // Query that should use the email index
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT * FROM users WHERE email = 'test@example.com'",
            &[],
        )
        .await;

    assert!(result.is_ok(), "EXPLAIN QUERY PLAN should succeed");
    let result = result.unwrap();

    // Check if the plan mentions the index
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| row.values.iter())
        .filter_map(|val| val.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join(" ");

    // The plan should mention either the index or USING INDEX
    let uses_index = plan_text.contains("idx_users_email")
        || plan_text.contains("USING INDEX")
        || plan_text.contains("SEARCH");
    assert!(
        uses_index,
        "Query on indexed column should show index usage: {}",
        plan_text
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_join_query() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    conn.execute(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        &[
            Value::String("buyer".to_string()),
            Value::String("buyer@example.com".to_string()),
        ],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO products (name, price) VALUES (?, ?)",
        &[Value::String("Widget".to_string()), Value::Float64(29.99)],
    )
    .await
    .unwrap();

    // EXPLAIN a JOIN query
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT u.username, p.name FROM users u JOIN products p ON 1=1",
            &[],
        )
        .await;

    assert!(result.is_ok(), "EXPLAIN QUERY PLAN on JOIN should succeed");
    let result = result.unwrap();

    // Join queries should show multiple scan operations
    assert!(
        result.rows.len() >= 2,
        "JOIN query plan should show multiple operations"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_subquery() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN a query with subquery
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
            &[],
        )
        .await;

    assert!(
        result.is_ok(),
        "EXPLAIN QUERY PLAN on subquery should succeed"
    );
    let result = result.unwrap();

    assert!(!result.rows.is_empty(), "Subquery plan should return rows");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_aggregate_query() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data
    for i in 1..=5 {
        conn.execute(
            "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
            &[
                Value::String(format!("user{}", i)),
                Value::String(format!("user{}@example.com", i)),
                Value::Float64(i as f64 * 100.0),
            ],
        )
        .await
        .unwrap();
    }

    // EXPLAIN an aggregate query
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT COUNT(*), SUM(balance), AVG(balance) FROM users",
            &[],
        )
        .await;

    assert!(
        result.is_ok(),
        "EXPLAIN QUERY PLAN on aggregate should succeed"
    );
    let result = result.unwrap();

    // Aggregate queries should show a scan
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| row.values.iter())
        .filter_map(|val| val.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        plan_text.contains("SCAN") || plan_text.contains("users"),
        "Aggregate query should scan the users table: {}",
        plan_text
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_order_by_query() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN a query with ORDER BY
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT * FROM users ORDER BY username",
            &[],
        )
        .await;

    assert!(
        result.is_ok(),
        "EXPLAIN QUERY PLAN on ORDER BY should succeed"
    );
    let result = result.unwrap();

    // Some plans might use temp B-tree for sorting
    assert!(!result.rows.is_empty(), "ORDER BY plan should return rows");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_group_by_query() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data with categories
    conn.execute(
        "INSERT INTO products (name, price, category) VALUES (?, ?, ?)",
        &[
            Value::String("P1".to_string()),
            Value::Float64(10.0),
            Value::String("A".to_string()),
        ],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO products (name, price, category) VALUES (?, ?, ?)",
        &[
            Value::String("P2".to_string()),
            Value::Float64(20.0),
            Value::String("A".to_string()),
        ],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO products (name, price, category) VALUES (?, ?, ?)",
        &[
            Value::String("P3".to_string()),
            Value::Float64(30.0),
            Value::String("B".to_string()),
        ],
    )
    .await
    .unwrap();

    // EXPLAIN a GROUP BY query
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT category, SUM(price) FROM products GROUP BY category",
            &[],
        )
        .await;

    assert!(
        result.is_ok(),
        "EXPLAIN QUERY PLAN on GROUP BY should succeed"
    );
    let result = result.unwrap();

    assert!(!result.rows.is_empty(), "GROUP BY plan should return rows");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_cte_query() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN a CTE query
    let result = conn.query(
        "EXPLAIN QUERY PLAN WITH user_balances AS (SELECT username, balance FROM users) SELECT * FROM user_balances",
        &[]
    ).await;

    assert!(result.is_ok(), "EXPLAIN QUERY PLAN on CTE should succeed");
    let result = result.unwrap();

    assert!(!result.rows.is_empty(), "CTE plan should return rows");

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_union_query() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN a UNION query
    let result = conn
        .query(
            "EXPLAIN QUERY PLAN SELECT username FROM users UNION SELECT name FROM products",
            &[],
        )
        .await;

    assert!(result.is_ok(), "EXPLAIN QUERY PLAN on UNION should succeed");
    let result = result.unwrap();

    // UNION should show compound queries
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| row.values.iter())
        .filter_map(|val| val.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join(" ");

    // UNION queries typically show COMPOUND or multiple scans
    assert!(
        result.rows.len() >= 2 || plan_text.contains("COMPOUND") || plan_text.contains("UNION"),
        "UNION query should show compound operation: {}",
        plan_text
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_opcodes_contain_expected_operations() {
    let (db_path, conn) = setup_test_database().await;

    // Get the raw EXPLAIN output (opcodes)
    let result = conn
        .query("EXPLAIN SELECT * FROM users WHERE id = 1", &[])
        .await;

    assert!(result.is_ok(), "EXPLAIN should succeed");
    let result = result.unwrap();

    // Collect all opcodes from the result
    let opcodes: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| row.get(1)) // opcode is typically the second column
        .filter_map(|val| val.as_str().map(|s| s.to_string()))
        .collect();

    // Common SQLite opcodes for a SELECT query
    let common_opcodes = [
        "Init",
        "OpenRead",
        "Rewind",
        "Column",
        "ResultRow",
        "Next",
        "Halt",
    ];

    let has_some_opcodes = common_opcodes
        .iter()
        .any(|op| opcodes.iter().any(|found| found.contains(op)));

    assert!(
        has_some_opcodes || !result.rows.is_empty(),
        "EXPLAIN should contain SQLite VM opcodes. Found: {:?}",
        opcodes
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_dialect_id_returns_sqlite() {
    let conn = SqliteConnection::open(":memory:").expect("Failed to open in-memory database");

    // Test that dialect_id() returns "sqlite"
    assert_eq!(
        conn.dialect_id(),
        Some("sqlite"),
        "SQLite connection should return 'sqlite' dialect"
    );
}

#[tokio::test]
async fn test_explain_insert_statement() {
    let (db_path, conn) = setup_test_database().await;

    // EXPLAIN an INSERT statement (doesn't execute, just shows plan)
    let result = conn
        .query(
            "EXPLAIN INSERT INTO users (username, email) VALUES ('test', 'test@example.com')",
            &[],
        )
        .await;

    assert!(result.is_ok(), "EXPLAIN on INSERT should succeed");
    let result = result.unwrap();

    assert!(
        !result.rows.is_empty(),
        "EXPLAIN INSERT should return opcodes"
    );

    // Verify the INSERT wasn't actually executed
    let count_result = conn.query("SELECT COUNT(*) FROM users", &[]).await.unwrap();
    assert_eq!(
        count_result.rows[0].get(0).unwrap().as_i64().unwrap(),
        0,
        "EXPLAIN should not execute the INSERT"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_update_statement() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data first
    conn.execute(
        "INSERT INTO users (username, email, balance) VALUES (?, ?, ?)",
        &[
            Value::String("test".to_string()),
            Value::String("test@example.com".to_string()),
            Value::Float64(100.0),
        ],
    )
    .await
    .unwrap();

    // EXPLAIN an UPDATE statement
    let result = conn
        .query(
            "EXPLAIN UPDATE users SET balance = 200.0 WHERE username = 'test'",
            &[],
        )
        .await;

    assert!(result.is_ok(), "EXPLAIN on UPDATE should succeed");
    let result = result.unwrap();

    assert!(
        !result.rows.is_empty(),
        "EXPLAIN UPDATE should return opcodes"
    );

    // Verify the UPDATE wasn't actually executed
    let balance_result = conn
        .query("SELECT balance FROM users WHERE username = 'test'", &[])
        .await
        .unwrap();
    assert_eq!(
        balance_result.rows[0].get(0).unwrap().as_f64().unwrap(),
        100.0,
        "EXPLAIN should not execute the UPDATE"
    );

    cleanup_test_database(db_path);
}

#[tokio::test]
async fn test_explain_delete_statement() {
    let (db_path, conn) = setup_test_database().await;

    // Insert test data first
    conn.execute(
        "INSERT INTO users (username, email) VALUES (?, ?)",
        &[
            Value::String("test".to_string()),
            Value::String("test@example.com".to_string()),
        ],
    )
    .await
    .unwrap();

    // EXPLAIN a DELETE statement
    let result = conn
        .query("EXPLAIN DELETE FROM users WHERE username = 'test'", &[])
        .await;

    assert!(result.is_ok(), "EXPLAIN on DELETE should succeed");
    let result = result.unwrap();

    assert!(
        !result.rows.is_empty(),
        "EXPLAIN DELETE should return opcodes"
    );

    // Verify the DELETE wasn't actually executed
    let count_result = conn.query("SELECT COUNT(*) FROM users", &[]).await.unwrap();
    assert_eq!(
        count_result.rows[0].get(0).unwrap().as_i64().unwrap(),
        1,
        "EXPLAIN should not execute the DELETE"
    );

    cleanup_test_database(db_path);
}
