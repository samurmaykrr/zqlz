//! Integration tests for TableService
//!
//! Tests the async table operations (browse_table, browse_table_with_filters,
//! update_cell, insert_row, delete_rows) using the MockConnection.

mod common;

use std::sync::Arc;
use zqlz_core::{Connection, Value};
use zqlz_services::{CellUpdateData, RowDeleteData, RowInsertData, TableService};

use common::{mock_query_result, MockConnection};

fn count_result(count: i64) -> zqlz_core::QueryResult {
    mock_query_result(vec!["count"], vec![vec![Value::Int64(count)]])
}

fn rows_result() -> zqlz_core::QueryResult {
    mock_query_result(
        vec!["id", "name", "email"],
        vec![
            vec![
                Value::Int64(1),
                Value::String("Alice".into()),
                Value::String("alice@example.com".into()),
            ],
            vec![
                Value::Int64(2),
                Value::String("Bob".into()),
                Value::String("bob@example.com".into()),
            ],
        ],
    )
}

// ============ browse_table Tests ============

#[tokio::test]
async fn browse_table_builds_correct_sql() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            // First query: COUNT(*), second: SELECT *
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(conn.clone() as Arc<dyn Connection>, "users", None, None, None)
        .await
        .expect("should browse table");

    // Verify total_rows was set from COUNT query
    assert_eq!(result.total_rows, Some(42));

    // Verify SQL queries were issued (order is non-deterministic with tokio::join!)
    let log = conn.query_log();
    assert!(log.len() >= 2, "should issue at least 2 queries: {:?}", log);
    assert!(
        log.iter().any(|q| q.contains("COUNT(*)")),
        "should have a count query: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("SELECT *")),
        "should have a select query: {:?}",
        log
    );
    let select_query = log.iter().find(|q| q.contains("SELECT *")).unwrap();
    assert!(
        select_query.contains("LIMIT 100"),
        "should use default limit: {}",
        select_query
    );
    assert!(
        select_query.contains("OFFSET 0"),
        "should use default offset: {}",
        select_query
    );
}

#[tokio::test]
async fn browse_table_respects_custom_limit_and_offset() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(100))
            .with_result(rows_result()),
    );
    let service = TableService::new(50);

    service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "orders",
            None,
            Some(25),
            Some(50),
        )
        .await
        .expect("should browse with custom pagination");

    let log = conn.query_log();
    let select_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a SELECT query");
    assert!(
        select_query.contains("LIMIT 25"),
        "should use custom limit: {}",
        select_query
    );
    assert!(
        select_query.contains("OFFSET 50"),
        "should use custom offset: {}",
        select_query
    );
}

#[tokio::test]
async fn browse_table_with_schema_qualifier() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(10))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "users",
            Some("my_database"),
            None,
            None,
        )
        .await
        .expect("should browse with schema qualifier");

    let log = conn.query_log();
    // SQLite uses double-quote identifier escaping — all queries should use the qualified table name
    assert!(
        log.iter().all(|q| q.contains("\"my_database\".\"users\"")),
        "all queries should have qualified table name: {:?}",
        log
    );
}

// ============ browse_table_with_filters Tests ============

#[tokio::test]
async fn browse_with_filters_adds_where_clause() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(5))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec!["status = 'active'".to_string(), "age > 18".to_string()],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .await
        .expect("should browse with filters");

    let log = conn.query_log();
    let count_query = log
        .iter()
        .find(|q| q.contains("COUNT(*)"))
        .expect("should have a COUNT query");
    assert!(
        count_query.contains("WHERE status = 'active' AND age > 18"),
        "count query should have WHERE clause: {}",
        count_query
    );
    let select_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a SELECT query");
    assert!(
        select_query.contains("WHERE status = 'active' AND age > 18"),
        "select query should have WHERE clause: {}",
        select_query
    );
}

#[tokio::test]
async fn browse_with_filters_adds_order_by() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(10))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec!["name ASC".to_string(), "created_at DESC".to_string()],
            vec![],
            None,
            None,
            None,
        )
        .await
        .expect("should browse with sorting");

    let log = conn.query_log();
    let select_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a SELECT query");
    assert!(
        select_query.contains("ORDER BY name ASC, created_at DESC"),
        "should have ORDER BY clause: {}",
        select_query
    );
}

#[tokio::test]
async fn browse_with_visible_columns() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(10))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec!["id".to_string(), "name".to_string()],
            None,
            None,
            None,
        )
        .await
        .expect("should browse with column selection");

    let log = conn.query_log();
    let select_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a SELECT query");
    // PostgreSQL uses double-quote identifier escaping
    assert!(
        select_query.contains("\"id\", \"name\""),
        "should select specific columns: {}",
        select_query
    );
}

// ============ update_cell Tests ============

#[tokio::test]
async fn update_cell_succeeds() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let cell_data = CellUpdateData {
        column_name: "email".to_string(),
        new_value: Some("new@example.com".to_string()),
        all_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        all_row_values: vec!["1".to_string(), "Alice".to_string(), "old@example.com".to_string()],
        all_column_types: vec!["int4".to_string(), "text".to_string(), "text".to_string()],
    };

    service
        .update_cell(conn.clone() as Arc<dyn Connection>, "users", None, cell_data)
        .await
        .expect("update should succeed");
}

// ============ insert_row Tests ============

#[tokio::test]
async fn insert_row_builds_correct_sql() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("mysql"));
    let service = TableService::new(100);

    let insert_data = RowInsertData {
        column_names: vec!["name".to_string(), "email".to_string()],
        values: vec![
            Some("Charlie".to_string()),
            Some("charlie@example.com".to_string()),
        ],
        column_types: Vec::new(),
    };

    service
        .insert_row(conn.clone() as Arc<dyn Connection>, "users", None, insert_data)
        .await
        .expect("insert should succeed");
}

#[tokio::test]
async fn insert_row_uses_postgres_placeholders() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let insert_data = RowInsertData {
        column_names: vec!["name".to_string(), "email".to_string()],
        values: vec![
            Some("Charlie".to_string()),
            Some("charlie@example.com".to_string()),
        ],
        column_types: vec!["text".to_string(), "text".to_string()],
    };

    service
        .insert_row(conn.clone() as Arc<dyn Connection>, "users", None, insert_data)
        .await
        .expect("insert should succeed");

    let log = conn.query_log();
    let insert_query = log
        .iter()
        .find(|q| q.contains("INSERT INTO"))
        .expect("should have an INSERT query");

    assert!(
        insert_query.contains("$1") && insert_query.contains("$2"),
        "should use postgres placeholders: {}",
        insert_query
    );
}

#[tokio::test]
async fn insert_row_with_null_values() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let insert_data = RowInsertData {
        column_names: vec!["name".to_string(), "email".to_string()],
        values: vec![Some("Dave".to_string()), None],
        column_types: Vec::new(),
    };

    service
        .insert_row(conn.clone() as Arc<dyn Connection>, "users", None, insert_data)
        .await
        .expect("insert with NULLs should succeed");
}

#[tokio::test]
async fn insert_row_with_empty_columns_fails() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let insert_data = RowInsertData {
        column_names: vec![],
        values: vec![],
        column_types: Vec::new(),
    };

    let result = service
        .insert_row(conn.clone() as Arc<dyn Connection>, "users", None, insert_data)
        .await;

    assert!(result.is_err(), "should fail with no columns");
}

// ============ delete_rows Tests ============

#[tokio::test]
async fn delete_rows_with_primary_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let delete_data = RowDeleteData {
        all_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        rows: vec![
            vec!["1".to_string(), "Alice".to_string(), "alice@example.com".to_string()],
        ],
    };

    let deleted = service
        .delete_rows(conn.clone() as Arc<dyn Connection>, "users", None, delete_data)
        .await
        .expect("delete should succeed");

    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn delete_zero_rows_returns_zero() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let delete_data = RowDeleteData {
        all_column_names: vec!["id".to_string()],
        rows: vec![],
    };

    let deleted = service
        .delete_rows(conn.clone() as Arc<dyn Connection>, "users", None, delete_data)
        .await
        .expect("delete of zero rows should succeed");

    assert_eq!(deleted, 0);
}

// ============ Error Handling Tests ============

#[tokio::test]
async fn browse_table_uses_estimated_count_for_slow_drivers() {
    // MySQL, PostgreSQL, MSSQL, ClickHouse should use metadata-based
    // estimated counts instead of expensive COUNT(*) full table scans.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(54_305_000))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            None,
            None,
        )
        .await
        .expect("should browse with estimated count");

    assert_eq!(
        result.total_rows,
        Some(54_305_000),
        "slow drivers should have estimated total_rows from metadata"
    );
    assert!(
        result.is_estimated_total,
        "total should be marked as estimated"
    );

    let log = conn.query_log();
    assert_eq!(log.len(), 2, "should issue 2 queries (data + estimate): {:?}", log);
    assert!(
        !log.iter().any(|q| q.contains("COUNT(*)")),
        "should NOT have a COUNT(*) query for mysql: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("information_schema.TABLES")),
        "should use information_schema for mysql estimate: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_table_runs_count_for_fast_drivers() {
    // SQLite and DuckDB should run COUNT(*) since it's essentially free.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            None,
            None,
        )
        .await
        .expect("should browse with count");

    assert_eq!(result.total_rows, Some(42), "fast drivers should have total_rows");

    let log = conn.query_log();
    assert!(log.len() >= 2, "should issue at least 2 queries: {:?}", log);
    assert!(
        log.iter().any(|q| q.contains("COUNT(*)")),
        "should have a count query for sqlite: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_table_on_failing_connection_returns_error() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_failure(),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(conn as Arc<dyn Connection>, "users", None, None, None)
        .await;

    assert!(result.is_err());
}

// ============ Estimated Count Tests ============

#[tokio::test]
async fn browse_table_exact_count_not_estimated_for_fast_drivers() {
    // SQLite/DuckDB use exact COUNT(*), so is_estimated_total should be false.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            None,
            None,
        )
        .await
        .expect("should browse with exact count");

    assert_eq!(result.total_rows, Some(42));
    assert!(
        !result.is_estimated_total,
        "fast drivers should return exact count, not estimated"
    );
}

#[tokio::test]
async fn browse_table_estimated_count_for_postgres() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("pg_class", count_result(1_000_000))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            None,
            None,
        )
        .await
        .expect("should browse with estimated count");

    assert_eq!(
        result.total_rows,
        Some(1_000_000),
        "should have estimated total from pg_class"
    );
    assert!(
        result.is_estimated_total,
        "postgres should use estimated count"
    );

    let log = conn.query_log();
    assert!(
        !log.iter().any(|q| q.contains("COUNT(*)")),
        "should NOT use COUNT(*) for postgres: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("pg_class")),
        "should query pg_class for postgres estimate: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_with_filters_skips_estimate_for_slow_drivers() {
    // When filters are active on a slow-count driver, metadata estimates
    // don't apply (they reflect the whole table, not the filtered subset).
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec!["status = 'active'".to_string()],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .await
        .expect("should browse with filters");

    assert_eq!(
        result.total_rows, None,
        "filtered queries on slow drivers should not have total_rows"
    );
    assert!(
        !result.is_estimated_total,
        "no estimate should be flagged when skipping count"
    );

    let log = conn.query_log();
    assert_eq!(log.len(), 1, "should only issue 1 query (data only): {:?}", log);
    assert!(
        !log.iter().any(|q| q.contains("COUNT(*)")),
        "should NOT run COUNT(*): {:?}",
        log
    );
    assert!(
        !log.iter().any(|q| q.contains("information_schema")),
        "should NOT run metadata estimate with active filters: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_with_filters_no_filters_uses_estimate_for_slow_drivers() {
    // When no filters are active on a slow-count driver, the estimated
    // count from metadata should be used.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(10_000))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            None,
        )
        .await
        .expect("should browse without filters");

    assert_eq!(
        result.total_rows,
        Some(10_000),
        "unfiltered queries on slow drivers should have estimated total"
    );
    assert!(
        result.is_estimated_total,
        "should be flagged as estimated"
    );

    let log = conn.query_log();
    assert_eq!(log.len(), 2, "should issue 2 queries (data + estimate): {:?}", log);
    assert!(
        log.iter().any(|q| q.contains("information_schema.TABLES")),
        "should use information_schema for estimate: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_with_filters_uses_cached_total_when_provided() {
    // When a cached total is provided, no count query should be issued
    // regardless of driver type.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    let result = service
        .browse_table_with_filters(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            None,
            None,
            Some(5000),
        )
        .await
        .expect("should browse with cached total");

    assert_eq!(
        result.total_rows,
        Some(5000),
        "should reuse cached total"
    );
    assert!(
        !result.is_estimated_total,
        "cached totals are not marked as estimated"
    );

    let log = conn.query_log();
    assert_eq!(log.len(), 1, "should only issue data query when cached total provided: {:?}", log);
}

#[tokio::test]
async fn estimate_row_count_mysql_returns_estimated() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(54_305_000)),
    );
    let service = TableService::new(100);

    let (count, is_estimated) = service
        .estimate_row_count(conn as Arc<dyn Connection>, "booking", Some("hotel_db"))
        .await
        .expect("should estimate row count");

    assert_eq!(count, 54_305_000);
    assert!(is_estimated, "mysql estimate should be flagged as estimated");
}

#[tokio::test]
async fn estimate_row_count_postgres_returns_estimated() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("pg_class", count_result(2_000_000)),
    );
    let service = TableService::new(100);

    let (count, is_estimated) = service
        .estimate_row_count(conn.clone() as Arc<dyn Connection>, "events", Some("public"))
        .await
        .expect("should estimate row count");

    assert_eq!(count, 2_000_000);
    assert!(is_estimated, "postgres estimate should be flagged as estimated");

    let log = conn.query_log();
    assert!(
        log.iter().any(|q| q.contains("pg_class") && q.contains("pg_namespace")),
        "should query pg_class with namespace: {:?}",
        log
    );
}

#[tokio::test]
async fn estimate_row_count_sqlite_falls_back_to_exact_count() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(500)),
    );
    let service = TableService::new(100);

    let (count, is_estimated) = service
        .estimate_row_count(conn.clone() as Arc<dyn Connection>, "users", None)
        .await
        .expect("should count rows exactly");

    assert_eq!(count, 500);
    assert!(!is_estimated, "sqlite should use exact count, not estimated");

    let log = conn.query_log();
    assert!(
        log.iter().any(|q| q.contains("COUNT(*)")),
        "sqlite should fall back to COUNT(*): {:?}",
        log
    );
}

#[tokio::test]
async fn estimate_row_count_duckdb_falls_back_to_exact_count() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("duckdb")
            .with_query_response("COUNT(*)", count_result(12345)),
    );
    let service = TableService::new(100);

    let (count, is_estimated) = service
        .estimate_row_count(conn as Arc<dyn Connection>, "measurements", None)
        .await
        .expect("should count rows exactly");

    assert_eq!(count, 12345);
    assert!(!is_estimated, "duckdb should use exact count, not estimated");
}


#[tokio::test]
async fn count_rows_with_filters() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("COUNT(*)", count_result(1234)),
    );
    let service = TableService::new(100);

    let total = service
        .count_rows(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec!["status = 'active'".to_string(), "age > 18".to_string()],
        )
        .await
        .expect("should count with filters");

    assert_eq!(total, 1234);

    let log = conn.query_log();
    assert_eq!(log.len(), 1);
    assert!(log[0].contains("WHERE status = 'active' AND age > 18"));
}

#[tokio::test]
async fn count_rows_on_failing_connection_returns_error() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_failure(),
    );
    let service = TableService::new(100);

    let result = service
        .count_rows(conn as Arc<dyn Connection>, "users", None, vec![])
        .await;

    assert!(result.is_err());
}

// ============ browse_last_page Tests ============

#[tokio::test]
async fn browse_last_page_reverses_rows_and_returns_total() {
    let data_result = mock_query_result(
        vec!["id", "name"],
        vec![
            vec![Value::Int64(100), Value::String("Zara".into())],
            vec![Value::Int64(99), Value::String("Yuri".into())],
            vec![Value::Int64(98), Value::String("Xena".into())],
        ],
    );

    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(100))
            .with_result(data_result),
    );
    let service = TableService::new(1000);

    let result = service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            3,
            vec!["id".to_string()],
        )
        .await
        .expect("should browse last page");

    // Rows should be reversed client-side to restore display order
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].values[0], Value::Int64(98));
    assert_eq!(result.rows[1].values[0], Value::Int64(99));
    assert_eq!(result.rows[2].values[0], Value::Int64(100));

    assert_eq!(result.total_rows, Some(100));
}

#[tokio::test]
async fn browse_last_page_uses_pk_desc_without_user_sorts() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(50))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            10,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let data_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a data query");
    assert!(
        data_query.contains("ORDER BY `id` DESC"),
        "should use PK DESC when no user sorts: {}",
        data_query
    );
    assert!(
        data_query.contains("LIMIT 10"),
        "should use the requested limit: {}",
        data_query
    );
    assert!(
        !data_query.contains("OFFSET"),
        "should NOT have OFFSET (that's the whole point): {}",
        data_query
    );
}

#[tokio::test]
async fn browse_last_page_flips_user_sorts() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("COUNT(*)", count_result(200))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "events",
            None,
            vec![],
            vec!["\"created_at\" ASC".to_string(), "\"id\" DESC".to_string()],
            vec![],
            25,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let data_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a data query");
    assert!(
        data_query.contains("ORDER BY \"created_at\" DESC, \"id\" ASC"),
        "should flip user sort directions: {}",
        data_query
    );
}

#[tokio::test]
async fn browse_last_page_with_schema_qualifier() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(54_000_000))
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "booking",
            Some("hotel_db"),
            vec![],
            vec![],
            vec![],
            1000,
            vec!["booking_id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    assert!(
        log.iter().all(|q| q.contains("`hotel_db`.`booking`")),
        "all queries should use qualified table name: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_last_page_with_where_clauses() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(500))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec!["status = 'active'".to_string()],
            vec![],
            vec![],
            50,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    // Both data and count queries should have the WHERE clause
    assert!(
        log.iter().all(|q| q.contains("WHERE status = 'active'")),
        "all queries should include WHERE clause: {:?}",
        log
    );
}

#[tokio::test]
async fn browse_last_page_with_visible_columns() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("COUNT(*)", count_result(100))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec!["id".to_string(), "name".to_string()],
            20,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let data_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a data query");
    assert!(
        data_query.contains("\"id\", \"name\""),
        "should select specific columns: {}",
        data_query
    );
}

#[tokio::test]
async fn browse_last_page_with_composite_pk() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("COUNT(*)", count_result(1000))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "order_items",
            None,
            vec![],
            vec![],
            vec![],
            50,
            vec!["order_id".to_string(), "item_id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let data_query = log
        .iter()
        .find(|q| q.contains("SELECT") && !q.contains("COUNT(*)"))
        .expect("should have a data query");
    assert!(
        data_query.contains("ORDER BY \"order_id\" DESC, \"item_id\" DESC"),
        "should reverse all PK columns: {}",
        data_query
    );
}

#[tokio::test]
async fn browse_last_page_on_failing_connection_returns_error() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_failure(),
    );
    let service = TableService::new(100);

    let result = service
        .browse_last_page(
            conn as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            50,
            vec!["id".to_string()],
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn browse_last_page_runs_count_and_data_concurrently() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );
    let service = TableService::new(100);

    service
        .browse_last_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            10,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    assert_eq!(log.len(), 2, "should issue exactly 2 queries: {:?}", log);
    assert!(
        log.iter().any(|q| q.contains("COUNT(*)")),
        "should have a COUNT query: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("SELECT") && !q.contains("COUNT(*)")),
        "should have a data query: {:?}",
        log
    );
}

// ============ browse_near_end_page Tests ============

#[tokio::test]
async fn browse_near_end_page_reverses_rows_and_preserves_total() {
    let data_result = mock_query_result(
        vec!["id", "name"],
        vec![
            vec![Value::Int64(99), Value::String("Yuri".into())],
            vec![Value::Int64(98), Value::String("Xena".into())],
        ],
    );

    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(data_result),
    );
    let service = TableService::new(1000);

    // Simulate navigating to the second-to-last page of a 100-row table
    // with limit=2: offset=96, so rows 96-97 (0-indexed).
    let result = service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            2,    // limit
            96,   // offset (near the end of 100 rows)
            100,  // total_rows
            vec!["id".to_string()],
        )
        .await
        .expect("should browse near-end page");

    // Rows should be reversed client-side to restore display order
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], Value::Int64(98));
    assert_eq!(result.rows[1].values[0], Value::Int64(99));

    // Total should be preserved
    assert_eq!(result.total_rows, Some(100));
}

#[tokio::test]
async fn browse_near_end_page_computes_correct_reverse_offset() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    // Total=54_305_000, limit=1000, offset=54_302_000 (page 54303)
    // reverse_offset = 54_305_000 - 54_302_000 - 1000 = 2000
    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "booking",
            Some("hotel_db"),
            vec![],
            vec![],
            vec![],
            1000,
            54_302_000,
            54_305_000,
            vec!["booking_id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    assert_eq!(log.len(), 1, "should issue exactly 1 query (no COUNT): {:?}", log);
    let query = &log[0];
    assert!(
        query.contains("ORDER BY `booking_id` DESC"),
        "should use reversed PK order: {}",
        query
    );
    assert!(
        query.contains("LIMIT 1000 OFFSET 2000"),
        "should use reverse_offset=2000: {}",
        query
    );
    assert!(
        query.contains("`hotel_db`.`booking`"),
        "should use qualified table name: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_last_page_has_zero_reverse_offset() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    // Total=100, limit=10, offset=90 (last page) → reverse_offset=0
    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            10,
            90,
            100,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("LIMIT 10 OFFSET 0"),
        "last page should have reverse_offset=0: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_partial_last_page_adjusts_limit() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    // Total=95, limit=10, offset=90 → only 5 rows remain
    // reverse_offset = 95 - 90 - 10 = -5 → saturates to 0
    // reverse_limit = min(10, 95 - 90) = 5
    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            10,
            90,
            95,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("LIMIT 5 OFFSET 0"),
        "partial last page should adjust limit to remaining rows: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_flips_user_sorts() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "events",
            None,
            vec![],
            vec!["\"created_at\" ASC".to_string(), "\"id\" DESC".to_string()],
            vec![],
            100,
            900,
            1000,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("ORDER BY \"created_at\" DESC, \"id\" ASC"),
        "should flip user sort directions: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_with_where_clauses() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec!["status = 'active'".to_string()],
            vec![],
            vec![],
            10,
            90,
            100,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("WHERE status = 'active'"),
        "should include WHERE clause: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_with_visible_columns() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec!["id".to_string(), "name".to_string()],
            10,
            90,
            100,
            vec!["id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("\"id\", \"name\""),
        "should select specific columns: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_with_composite_pk() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_result(rows_result()),
    );
    let service = TableService::new(1000);

    service
        .browse_near_end_page(
            conn.clone() as Arc<dyn Connection>,
            "order_items",
            None,
            vec![],
            vec![],
            vec![],
            50,
            950,
            1000,
            vec!["order_id".to_string(), "item_id".to_string()],
        )
        .await
        .expect("should succeed");

    let log = conn.query_log();
    let query = &log[0];
    assert!(
        query.contains("ORDER BY \"order_id\" DESC, \"item_id\" DESC"),
        "should reverse all PK columns: {}",
        query
    );
}

#[tokio::test]
async fn browse_near_end_page_on_failing_connection_returns_error() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_failure(),
    );
    let service = TableService::new(100);

    let result = service
        .browse_near_end_page(
            conn as Arc<dyn Connection>,
            "users",
            None,
            vec![],
            vec![],
            vec![],
            10,
            90,
            100,
            vec!["id".to_string()],
        )
        .await;

    assert!(result.is_err());
}
