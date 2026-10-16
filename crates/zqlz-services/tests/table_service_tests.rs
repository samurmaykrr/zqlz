//! Integration tests for TableService
//!
//! Tests the async table operations (fetch_rows, update_cell,
//! insert_row, delete_rows) using the MockConnection.

mod common;

use std::sync::Arc;
use zqlz_core::{AffectedRowCountFidelity, Connection, RowIdentifier, TableRef, Value};
use zqlz_services::{
    plan_fetch, CellUpdateData, FetchMode, KnownTotal, QuerySnapshot, RowDeleteData, RowInsertData,
    RowWindow, TableService,
};

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

fn empty_count_result() -> zqlz_core::QueryResult {
    mock_query_result(vec!["count"], Vec::new())
}

// ============ First-page browse Tests ============

/// The first page an open fetches: no filters, no key, 100 rows.
async fn browse_first_page(
    conn: &Arc<MockConnection>,
    table: TableRef,
) -> zqlz_services::ServiceResult<zqlz_core::QueryResult> {
    let query = QuerySnapshot {
        page_size: Some(100),
        current_page: 1,
        fast_exact_count: conn.supports_fast_exact_count(),
        ..QuerySnapshot::default()
    };
    let plan = plan_fetch(FetchMode::Page(1), &query).expect("a first-page plan");
    TableService::new(100)
        .fetch_rows(conn.clone() as Arc<dyn Connection>, &table, &query, plan)
        .await
        .map(|outcome| outcome.result)
}

fn users() -> TableRef {
    TableRef::new(None, None, "users")
}

#[tokio::test]
async fn first_page_builds_correct_sql() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            // First query: COUNT(*), second: SELECT *
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );

    let result = browse_first_page(&conn, users())
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
async fn first_page_qualifies_the_table_with_its_schema() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(10))
            .with_result(rows_result()),
    );

    browse_first_page(
        &conn,
        TableRef::new(None, Some("my_database".into()), "users"),
    )
    .await
    .expect("should browse with schema qualifier");

    let log = conn.query_log();
    assert!(
        log.iter().all(|q| q.contains("\"my_database\".\"users\"")),
        "all queries should have qualified table name: {:?}",
        log
    );
}

// ============ fetch_rows Tests ============

fn users_query() -> QuerySnapshot {
    QuerySnapshot {
        pk_columns: vec!["\"id\"".to_string()],
        page_size: Some(100),
        current_page: 1,
        ..QuerySnapshot::default()
    }
}

async fn fetch(
    conn: &Arc<MockConnection>,
    mode: FetchMode,
    query: &QuerySnapshot,
) -> zqlz_services::FetchOutcome {
    let plan = plan_fetch(mode, query).expect("a fetch plan");
    TableService::new(100)
        .fetch_rows(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            query,
            plan,
        )
        .await
        .expect("fetch rows")
}

#[tokio::test]
async fn fetch_rows_selects_filters_and_orders_with_a_pk_tiebreaker() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(10))
            .with_result(rows_result()),
    );
    let query = QuerySnapshot {
        where_clauses: vec!["status = 'active'".to_string()],
        order_by_clauses: vec!["\"name\" DESC".to_string()],
        visible_columns: vec!["id".to_string(), "name".to_string()],
        ..users_query()
    };

    fetch(&conn, FetchMode::Page(3), &query).await;

    let log = conn.query_log();
    assert!(
        log.iter().any(|sql| sql
            == "SELECT \"id\", \"name\" FROM \"users\" WHERE status = 'active' ORDER BY \"name\" DESC, \"id\" ASC LIMIT 100 OFFSET 200"),
        "data query should carry columns, filters, sorts and the page window: {log:?}"
    );
}

#[tokio::test]
async fn fetch_rows_skips_the_estimate_under_filters() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let query = QuerySnapshot {
        where_clauses: vec!["status = 'active'".to_string()],
        ..users_query()
    };

    let outcome = fetch(&conn, FetchMode::Page(1), &query).await;

    assert_eq!(outcome.total, None);
    assert_eq!(conn.query_log().len(), 1, "{:?}", conn.query_log());
}

#[tokio::test]
async fn fetch_rows_estimates_an_unfiltered_slow_count() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(10_000))
            .with_result(rows_result()),
    );

    let outcome = fetch(&conn, FetchMode::Page(1), &users_query()).await;

    assert_eq!(outcome.result.total_rows, Some(10_000));
    assert!(outcome.result.is_estimated_total);
}

#[tokio::test]
async fn fetch_rows_drops_an_estimate_below_the_rows_already_read() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(0))
            .with_result(rows_result()),
    );

    let outcome = fetch(&conn, FetchMode::Page(1), &users_query()).await;

    assert_eq!(outcome.result.rows.len(), 2);
    assert_eq!(outcome.result.total_rows, None);
    assert!(outcome.total.is_none());
}

#[tokio::test]
async fn fetch_rows_reuses_a_total_counted_under_the_same_filters() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );
    let query = QuerySnapshot {
        known_total: Some(KnownTotal {
            rows: 5000,
            is_estimated: false,
            where_clauses: Vec::new(),
        }),
        ..users_query()
    };

    let outcome = fetch(&conn, FetchMode::Page(2), &query).await;

    assert_eq!(outcome.result.total_rows, Some(5000));
    assert_eq!(conn.query_log().len(), 1, "{:?}", conn.query_log());
}

#[tokio::test]
async fn fetch_rows_last_page_counts_then_reads_only_the_remainder() {
    let tail = mock_query_result(
        vec!["id"],
        vec![vec![Value::Int64(1050)], vec![Value::Int64(1049)]],
    );
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("COUNT(*)", count_result(1050))
            .with_result(tail),
    );
    let query = QuerySnapshot {
        pk_columns: vec!["`id`".to_string()],
        where_clauses: vec!["status = 'active'".to_string()],
        ..users_query()
    };

    let outcome = fetch(&conn, FetchMode::Last, &query).await;

    assert_eq!(outcome.plan.page, 11);
    assert_eq!(
        outcome.plan.window,
        RowWindow::FromEnd { limit: 50, skip: 0 }
    );
    assert_eq!(outcome.result.total_rows, Some(1050));
    assert_eq!(
        outcome.result.rows[0].values[0],
        Value::Int64(1049),
        "reversed rows are flipped back into display order"
    );
    let log = conn.query_log();
    assert!(
        log[0].contains("COUNT(*)") && log[0].contains("WHERE status = 'active'"),
        "the count carries the filter: {log:?}"
    );
    assert!(
        log[1].contains("ORDER BY `id` DESC LIMIT 50"),
        "the data query reads the remainder from the end: {log:?}"
    );
}

#[tokio::test]
async fn fetch_rows_rejects_key_value_connections() {
    let conn = Arc::new(MockConnection::new("cache").with_driver("redis"));
    let result = TableService::new(100)
        .fetch_rows(
            conn as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            &users_query(),
            plan_fetch(FetchMode::Page(1), &users_query()).expect("plan"),
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn fetch_rows_on_failing_connection_returns_error() {
    let conn = Arc::new(MockConnection::new("test_db").with_failure());
    let result = TableService::new(100)
        .fetch_rows(
            conn as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            &users_query(),
            plan_fetch(FetchMode::Page(1), &users_query()).expect("plan"),
        )
        .await;

    assert!(result.is_err());
}

// ============ update_cell Tests ============

#[tokio::test]
async fn update_cell_succeeds() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let cell_data = CellUpdateData {
        column_name: "email".to_string(),
        new_value: Some(Value::String("new@example.com".to_string())),
        all_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        all_row_values: vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::String("old@example.com".to_string()),
        ],
        all_column_types: vec!["int4".to_string(), "text".to_string(), "text".to_string()],
    };

    service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            cell_data,
        )
        .await
        .expect("update should succeed");
}

#[tokio::test]
async fn update_cell_uses_complete_primary_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            CellUpdateData {
                column_name: "email".to_string(),
                new_value: Some(Value::String("new@example.com".to_string())),
                all_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
                all_row_values: vec![
                    Value::Int32(1),
                    Value::String("Alice".to_string()),
                    Value::String("old@example.com".to_string()),
                ],
                all_column_types: vec!["int4".to_string(), "text".to_string(), "text".to_string()],
            },
        )
        .await
        .expect("update should succeed");

    let update = conn
        .last_cell_update()
        .expect("update request should be captured");
    assert!(matches!(
        update.row_identifier,
        RowIdentifier::PrimaryKey(values)
            if values == vec![("id".to_string(), Value::Int32(1))]
    ));
}

#[tokio::test]
async fn update_cell_does_not_use_partial_or_null_primary_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("sqlite"));
    let service = TableService::new(100);

    service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "composite_nullable"),
            CellUpdateData {
                column_name: "builder_document_json".to_string(),
                new_value: Some(Value::String("{\"new\":true}".to_string())),
                all_column_names: vec![
                    "content_id".to_string(),
                    "status_id".to_string(),
                    "builder_document_json".to_string(),
                ],
                all_row_values: vec![
                    Value::String("system.brand.logo.primary_logo".to_string()),
                    Value::Null,
                    Value::String("{\"old\":true}".to_string()),
                ],
                all_column_types: vec!["text".to_string(), "text".to_string(), "text".to_string()],
            },
        )
        .await
        .expect("update should succeed");

    let update = conn
        .last_cell_update()
        .expect("update request should be captured");
    assert!(matches!(
        update.row_identifier,
        RowIdentifier::FullRow(values)
            if values == vec![
                (
                    "content_id".to_string(),
                    Value::String("system.brand.logo.primary_logo".to_string())
                ),
                ("status_id".to_string(), Value::Null),
                (
                    "builder_document_json".to_string(),
                    Value::String("{\"old\":true}".to_string())
                ),
            ]
    ));
}

/// Without a primary key, a unique key identifies the row just as precisely,
/// and keeps the statement off every other column.
#[tokio::test]
async fn update_cell_falls_back_to_a_unique_key_without_a_primary_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("sqlite"));
    let service = TableService::new(100);

    service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "sessions"),
            CellUpdateData {
                column_name: "token".to_string(),
                new_value: Some(Value::String("new-token".to_string())),
                all_column_names: vec!["email".to_string(), "token".to_string()],
                all_row_values: vec![
                    Value::String("alice@example.com".to_string()),
                    Value::String("old-token".to_string()),
                ],
                all_column_types: vec!["text".to_string(), "text".to_string()],
            },
        )
        .await
        .expect("update should succeed");

    let update = conn
        .last_cell_update()
        .expect("update request should be captured");
    assert!(matches!(
        update.row_identifier,
        RowIdentifier::PrimaryKey(values)
            if values == vec![(
                "email".to_string(),
                Value::String("alice@example.com".to_string())
            )]
    ));
}

/// A row whose unique key is NULL cannot be matched on it, because NULL never
/// compares equal to itself.
#[tokio::test]
async fn update_cell_skips_a_null_unique_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("sqlite"));
    let service = TableService::new(100);

    service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "sessions"),
            CellUpdateData {
                column_name: "token".to_string(),
                new_value: Some(Value::String("new-token".to_string())),
                all_column_names: vec!["email".to_string(), "token".to_string()],
                all_row_values: vec![Value::Null, Value::String("old-token".to_string())],
                all_column_types: vec!["text".to_string(), "text".to_string()],
            },
        )
        .await
        .expect("update should succeed");

    let update = conn
        .last_cell_update()
        .expect("update request should be captured");
    assert!(matches!(
        update.row_identifier,
        RowIdentifier::FullRow(values) if values.len() == 2
    ));
}

/// The regression test for the false "modified or deleted by another user"
/// error: on a driver that reports matched rows, zero genuinely means the row
/// is gone and must stay a hard error.
#[tokio::test]
async fn update_cell_zero_affected_rows_errors_on_exact_drivers() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_cell_update_affected_rows(0),
    );
    let service = TableService::new(100);

    let result = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            sample_cell_update(),
        )
        .await;

    assert!(
        result.is_err(),
        "zero matched rows on an exact-count driver must be an error"
    );
}

/// ClickHouse fabricates a zero count, so zero carries no information and must
/// not be reported as a vanished row.
#[tokio::test]
async fn update_cell_zero_affected_rows_is_not_an_error_when_counts_unavailable() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("clickhouse")
            .with_cell_update_affected_rows(0)
            .with_affected_row_count_fidelity(AffectedRowCountFidelity::Unavailable),
    );
    let service = TableService::new(100);

    let outcome = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            sample_cell_update(),
        )
        .await
        .expect("drivers without row counts must not fail the update");

    assert!(
        outcome.unconfirmed_reason.is_some(),
        "an unconfirmable write should say so"
    );
}

/// A SQL Server trigger running `SET NOCOUNT ON` suppresses the count, so zero
/// is inconclusive rather than a failure.
#[tokio::test]
async fn update_cell_zero_affected_rows_warns_on_best_effort() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mssql")
            .with_cell_update_affected_rows(0)
            .with_affected_row_count_fidelity(AffectedRowCountFidelity::BestEffort),
    );
    let service = TableService::new(100);

    let outcome = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            sample_cell_update(),
        )
        .await
        .expect("a suppressed row count must not fail the update");

    assert!(outcome.unconfirmed_reason.is_some());
}

#[tokio::test]
async fn update_cell_reports_affected_rows_on_success() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("postgresql"));
    let service = TableService::new(100);

    let outcome = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            sample_cell_update(),
        )
        .await
        .expect("update should succeed");

    assert_eq!(outcome.affected_rows, 1);
    assert!(outcome.unconfirmed_reason.is_none());
}

/// The database may store something other than what was submitted (a DECIMAL
/// rounded to scale, a truncated string). The outcome must carry the stored
/// value so the grid can show the truth instead of the typed value.
#[tokio::test]
async fn update_cell_reads_back_the_stored_value() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response(
                "SELECT \"email\"",
                mock_query_result(
                    vec!["email"],
                    vec![vec![Value::String("rounded@example.com".to_string())]],
                ),
            ),
    );
    let service = TableService::new(100);

    let outcome = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            sample_cell_update(),
        )
        .await
        .expect("update should succeed");

    assert_eq!(
        outcome.stored_value,
        Some(Value::String("rounded@example.com".to_string())),
        "the value the database actually holds should be reported back"
    );
}

/// A full-row identifier matches on the pre-update value of the edited column,
/// so re-reading with it would select nothing and wrongly blank the cell.
#[tokio::test]
async fn update_cell_skips_read_back_without_a_primary_key() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("sqlite"));
    let service = TableService::new(100);

    let outcome = service
        .update_cell(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "composite_nullable"),
            CellUpdateData {
                column_name: "builder_document_json".to_string(),
                new_value: Some(Value::String("{\"new\":true}".to_string())),
                all_column_names: vec![
                    "content_id".to_string(),
                    "status_id".to_string(),
                    "builder_document_json".to_string(),
                ],
                all_row_values: vec![
                    Value::String("system.brand.logo.primary_logo".to_string()),
                    Value::Null,
                    Value::String("{\"old\":true}".to_string()),
                ],
                all_column_types: vec!["text".to_string(), "text".to_string(), "text".to_string()],
            },
        )
        .await
        .expect("update should succeed");

    assert!(
        outcome.stored_value.is_none(),
        "read-back must be skipped when the row has no primary key"
    );
}

fn sample_cell_update() -> CellUpdateData {
    CellUpdateData {
        column_name: "email".to_string(),
        new_value: Some(Value::String("new@example.com".to_string())),
        all_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
        all_row_values: vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::String("old@example.com".to_string()),
        ],
        all_column_types: vec!["int4".to_string(), "text".to_string(), "text".to_string()],
    }
}

// ============ insert_row Tests ============

#[tokio::test]
async fn insert_row_builds_correct_sql() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("mysql"));
    let service = TableService::new(100);

    let insert_data = RowInsertData {
        column_names: vec!["name".to_string(), "email".to_string()],
        values: vec![
            Some(Value::String("Charlie".to_string())),
            Some(Value::String("charlie@example.com".to_string())),
        ],
        column_types: Vec::new(),
    };

    service
        .insert_row(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            insert_data,
        )
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
            Some(Value::String("Charlie".to_string())),
            Some(Value::String("charlie@example.com".to_string())),
        ],
        column_types: vec!["text".to_string(), "text".to_string()],
    };

    service
        .insert_row(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            insert_data,
        )
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
        values: vec![Some(Value::String("Dave".to_string())), None],
        column_types: Vec::new(),
    };

    service
        .insert_row(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            insert_data,
        )
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
        .insert_row(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            insert_data,
        )
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
        rows: vec![vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::String("alice@example.com".to_string()),
        ]],
    };

    let deleted = service
        .delete_rows(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            delete_data,
        )
        .await
        .expect("delete should succeed");

    assert_eq!(deleted, 1);
}

/// A table with no primary key is still editable: the row is matched on all
/// its columns, and the driver narrows the statement so duplicate rows are not
/// deleted together.
#[tokio::test]
async fn delete_row_without_a_key_is_scoped_to_a_single_row() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("sqlite"));
    let service = TableService::new(100);

    let delete_data = RowDeleteData {
        all_column_names: vec!["workflow_id".to_string(), "workflow_jinja".to_string()],
        rows: vec![vec![Value::String("search_web".to_string()), Value::Null]],
    };

    let deleted = service
        .delete_rows(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "workflows"),
            delete_data,
        )
        .await
        .expect("delete should succeed");

    assert_eq!(deleted, 1);
    let delete_sql = conn
        .query_log()
        .into_iter()
        .find(|sql| sql.starts_with("DELETE FROM"))
        .expect("delete statement should be logged");
    assert_eq!(
        delete_sql,
        "DELETE FROM \"workflows\" WHERE rowid IN (SELECT rowid FROM \"workflows\" \
         WHERE \"workflow_id\" = ? AND \"workflow_jinja\" IS NULL LIMIT 1)"
    );
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
        .delete_rows(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
            delete_data,
        )
        .await
        .expect("delete of zero rows should succeed");

    assert_eq!(deleted, 0);
}

// ============ Error Handling Tests ============

#[tokio::test]
async fn first_page_uses_estimated_count_for_slow_drivers() {
    // MySQL, PostgreSQL, MSSQL, ClickHouse should use metadata-based
    // estimated counts instead of expensive COUNT(*) full table scans.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_query_response("information_schema.TABLES", count_result(54_305_000))
            .with_result(rows_result()),
    );

    let result = browse_first_page(&conn, users())
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
    assert_eq!(
        log.len(),
        2,
        "should issue 2 queries (data + estimate): {:?}",
        log
    );
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
async fn first_page_runs_count_for_fast_drivers() {
    // SQLite and DuckDB should run COUNT(*) since it's essentially free.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );

    let result = browse_first_page(&conn, users())
        .await
        .expect("should browse with count");

    assert_eq!(
        result.total_rows,
        Some(42),
        "fast drivers should have total_rows"
    );

    let log = conn.query_log();
    assert!(log.len() >= 2, "should issue at least 2 queries: {:?}", log);
    assert!(
        log.iter().any(|q| q.contains("COUNT(*)")),
        "should have a count query for sqlite: {:?}",
        log
    );
}

#[tokio::test]
async fn first_page_on_failing_connection_returns_error() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_failure(),
    );
    let result = browse_first_page(&conn, users()).await;

    assert!(result.is_err());
}

// ============ Estimated Count Tests ============

#[tokio::test]
async fn first_page_exact_count_not_estimated_for_fast_drivers() {
    // SQLite/DuckDB use exact COUNT(*), so is_estimated_total should be false.
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("sqlite")
            .with_query_response("COUNT(*)", count_result(42))
            .with_result(rows_result()),
    );

    let result = browse_first_page(&conn, users())
        .await
        .expect("should browse with exact count");

    assert_eq!(result.total_rows, Some(42));
    assert!(
        !result.is_estimated_total,
        "fast drivers should return exact count, not estimated"
    );
}

#[tokio::test]
async fn first_page_estimated_count_for_postgres() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("pg_class", count_result(1_000_000))
            .with_result(rows_result()),
    );

    let result = browse_first_page(&conn, users())
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
async fn estimate_row_count_uses_embedded_schema_for_postgres() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("pg_class", count_result(2_000_000)),
    );
    let service = TableService::new(100);

    let (total, is_estimated) = service
        .estimate_row_count(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, Some("orders".into()), "cart_items"),
        )
        .await
        .expect("should estimate count for qualified table")
        .expect("metadata estimate should be available");

    assert_eq!(total, 2_000_000);
    assert!(is_estimated);

    let log = conn.query_log();
    assert!(
        log.iter()
            .any(|query| query.contains("pg_namespace.nspname")),
        "qualified postgres estimate should filter by namespace: {:?}",
        log
    );
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
        .estimate_row_count(
            conn as Arc<dyn Connection>,
            &TableRef::new(None, Some("hotel_db".into()), "booking"),
        )
        .await
        .expect("should estimate row count")
        .expect("metadata estimate should be available");

    assert_eq!(count, 54_305_000);
    assert!(
        is_estimated,
        "mysql estimate should be flagged as estimated"
    );
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
        .estimate_row_count(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, Some("public".into()), "events"),
        )
        .await
        .expect("should estimate row count")
        .expect("metadata estimate should be available");

    assert_eq!(count, 2_000_000);
    assert!(
        is_estimated,
        "postgres estimate should be flagged as estimated"
    );

    let log = conn.query_log();
    assert!(
        log.iter()
            .any(|q| q.contains("pg_class") && q.contains("pg_namespace")),
        "should query pg_class with namespace: {:?}",
        log
    );
}

#[tokio::test]
async fn estimate_row_count_postgres_returns_none_when_metadata_missing() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("postgresql")
            .with_query_response("pg_class", empty_count_result()),
    );
    let service = TableService::new(100);

    let count = service
        .estimate_row_count(
            conn as Arc<dyn Connection>,
            &TableRef::new(None, Some("zqlz_lab".into()), "orders"),
        )
        .await
        .expect("metadata miss should not fail table load");

    assert_eq!(count, None);
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
        .estimate_row_count(
            conn.clone() as Arc<dyn Connection>,
            &TableRef::new(None, None, "users"),
        )
        .await
        .expect("should count rows exactly")
        .expect("exact count should be available");

    assert_eq!(count, 500);
    assert!(
        !is_estimated,
        "sqlite should use exact count, not estimated"
    );

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
        .estimate_row_count(
            conn as Arc<dyn Connection>,
            &TableRef::new(None, None, "measurements"),
        )
        .await
        .expect("should count rows exactly")
        .expect("exact count should be available");

    assert_eq!(count, 12345);
    assert!(
        !is_estimated,
        "duckdb should use exact count, not estimated"
    );
}

// ============ Relation resolution round-trip tests ============

#[tokio::test]
async fn resolving_a_table_with_a_known_schema_skips_the_catalog() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("mysql"));
    let table = TableRef::new(
        Some("seekunique-dev".into()),
        Some("seekunique-dev".into()),
        "accounting_payments",
    );

    let resolved = TableService::new(100)
        .resolve_table(conn.clone() as Arc<dyn Connection>, table.clone())
        .await;

    assert_eq!(resolved, table);
    assert!(
        conn.list_tables_schemas().is_empty(),
        "a known schema needs no catalog listing: {:?}",
        conn.list_tables_schemas()
    );
}

#[tokio::test]
async fn resolving_finds_views_and_marks_them() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("mysql"));

    let resolved = TableService::new(100)
        .resolve_table(
            conn.clone() as Arc<dyn Connection>,
            TableRef::new(Some("app".into()), None, "active_users"),
        )
        .await;

    assert!(resolved.is_view);
}

#[tokio::test]
async fn fetching_a_resolved_table_never_touches_the_catalog() {
    let conn = Arc::new(
        MockConnection::new("test_db")
            .with_driver("mysql")
            .with_result(rows_result()),
    );

    browse_first_page(
        &conn,
        TableRef::new(None, Some("seekunique-dev".into()), "accounting_payments"),
    )
    .await
    .expect("should browse a resolved table");

    assert!(
        conn.list_tables_schemas().is_empty(),
        "paging must not re-resolve the table: {:?}",
        conn.list_tables_schemas()
    );
    assert!(
        conn.query_log()
            .iter()
            .any(|query| query.contains("`seekunique-dev`.`accounting_payments`")),
        "should render the schema and name as two identifiers: {:?}",
        conn.query_log()
    );
}

#[tokio::test]
async fn resolving_a_bare_name_lists_tables_once() {
    let conn = Arc::new(MockConnection::new("test_db").with_driver("mysql"));

    TableService::new(100)
        .resolve_table(
            conn.clone() as Arc<dyn Connection>,
            TableRef::new(None, None, "users"),
        )
        .await;

    assert_eq!(
        conn.list_tables_schemas().len(),
        1,
        "resolution should list tables once: {:?}",
        conn.list_tables_schemas()
    );
}

#[tokio::test]
async fn sqlite_resolution_keeps_dotted_names_and_splits_only_confirmed_legacy_names() {
    let connection: Arc<dyn Connection> = Arc::new(
        zqlz_drivers::sqlite::SqliteConnection::open(":memory:").expect("open sqlite memory db"),
    );
    for sql in [
        "CREATE TABLE \"odd.name\" (id INTEGER PRIMARY KEY)",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
    ] {
        connection.execute(sql, &[]).await.expect(sql);
    }
    let service = TableService::new(100);
    let resolve =
        |name: &str| service.resolve_table(connection.clone(), TableRef::new(None, None, name));

    assert_eq!(resolve("odd.name").await.name, "odd.name");
    let legacy = resolve("main.users").await;
    assert_eq!(
        (legacy.schema.as_deref(), legacy.name.as_str()),
        (Some("main"), "users")
    );
    let unknown = resolve("nowhere.users").await;
    assert_eq!(
        (unknown.schema, unknown.name.as_str()),
        (None, "nowhere.users")
    );
}

// ============ fetch_rows against SQLite ============

async fn seeded_sqlite() -> Arc<dyn Connection> {
    let connection: Arc<dyn Connection> = Arc::new(
        zqlz_drivers::sqlite::SqliteConnection::open(":memory:").expect("open sqlite memory db"),
    );
    connection
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL)",
            &[],
        )
        .await
        .expect("create items");
    connection
        .execute(
            "WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < 2500) \
             INSERT INTO items (id, bucket) SELECT id, id % 7 FROM n",
            &[],
        )
        .await
        .expect("seed items");
    connection
}

fn items_query(connection: &dyn Connection) -> QuerySnapshot {
    QuerySnapshot {
        pk_columns: vec![connection.quote_identifier("id")],
        page_size: Some(300),
        current_page: 1,
        fast_exact_count: connection.supports_fast_exact_count(),
        ..QuerySnapshot::default()
    }
}

async fn fetch_items(
    connection: &Arc<dyn Connection>,
    mode: FetchMode,
    query: &QuerySnapshot,
) -> zqlz_services::FetchOutcome {
    TableService::new(100)
        .fetch_rows(
            connection.clone(),
            &TableRef::new(None, None, "items"),
            query,
            plan_fetch(mode, query).expect("a fetch plan"),
        )
        .await
        .expect("fetch items")
}

fn ids(outcome: &zqlz_services::FetchOutcome) -> Vec<i64> {
    outcome
        .result
        .rows
        .iter()
        .filter_map(|row| row.values.first().and_then(Value::as_i64))
        .collect()
}

#[tokio::test]
async fn sqlite_reversed_page_matches_the_forward_page() {
    let connection = seeded_sqlite().await;
    let query = QuerySnapshot {
        order_by_clauses: vec![format!("{} DESC", connection.quote_identifier("bucket"))],
        ..items_query(connection.as_ref())
    };
    let forward = fetch_items(&connection, FetchMode::Page(7), &query).await;
    assert!(matches!(forward.plan.window, RowWindow::Forward { .. }));

    let with_total = QuerySnapshot {
        known_total: forward.total.clone(),
        ..query
    };
    let reversed = fetch_items(&connection, FetchMode::Page(7), &with_total).await;
    assert!(matches!(reversed.plan.window, RowWindow::FromEnd { .. }));

    assert_eq!(ids(&forward), ids(&reversed));
}

#[tokio::test]
async fn sqlite_last_page_returns_the_remainder() {
    let connection = seeded_sqlite().await;
    let outcome = fetch_items(
        &connection,
        FetchMode::Last,
        &items_query(connection.as_ref()),
    )
    .await;

    assert_eq!(outcome.plan.page, 9);
    assert_eq!(ids(&outcome), (2401..=2500).collect::<Vec<_>>());
}

#[tokio::test]
async fn sqlite_count_carries_the_where_clause() {
    let connection = seeded_sqlite().await;
    let query = QuerySnapshot {
        where_clauses: vec!["bucket = 0".to_string()],
        ..items_query(connection.as_ref())
    };
    let outcome = fetch_items(&connection, FetchMode::Page(1), &query).await;

    assert_eq!(outcome.result.total_rows, Some(357));
    assert_eq!(
        outcome.total.map(|total| total.where_clauses),
        Some(vec!["bucket = 0".to_string()])
    );
}

#[tokio::test]
async fn sqlite_limit_off_returns_every_row() {
    let connection = seeded_sqlite().await;
    let query = QuerySnapshot {
        page_size: None,
        ..items_query(connection.as_ref())
    };
    let outcome = fetch_items(&connection, FetchMode::Page(1), &query).await;

    assert_eq!(outcome.result.rows.len(), 2500);
    assert_eq!(outcome.result.total_rows, Some(2500));
}
