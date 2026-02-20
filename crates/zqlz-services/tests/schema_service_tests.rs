//! Integration tests for SchemaService
//!
//! Verifies that `load_database_schema()` correctly resolves database/schema names
//! per driver type and passes them to introspection methods — the core fix for the
//! MySQL "no db connected" bug where `DATABASE()` returning NULL caused empty results.

mod common;

use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{Connection, ObjectType, Value};
use zqlz_services::SchemaService;

use common::{
    mock_single_value_result, mysql_connection, postgres_connection, MockConnection,
};

// ============ MySQL Driver Path Tests ============

#[tokio::test]
async fn mysql_resolves_database_name_before_introspection() {
    let conn = mysql_connection("my_app_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(schema.database_name.as_deref(), Some("my_app_db"));
    // MySQL uses DATABASE() for both database_name and schema_name queries
    assert_eq!(schema.schema_name.as_deref(), Some("my_app_db"));
}

#[tokio::test]
async fn mysql_returns_tables_from_mock_introspection() {
    let conn = mysql_connection("test_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(schema.tables.len(), 2);
    assert!(schema.tables.contains(&"users".to_string()));
    assert!(schema.tables.contains(&"posts".to_string()));
}

#[tokio::test]
async fn mysql_queries_database_name_via_select_database() {
    let conn = mysql_connection("shop_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    let log = conn.query_log();
    // The first query should be SELECT DATABASE() for the db name
    assert!(
        log.iter().any(|q| q.contains("SELECT DATABASE()")),
        "should query DATABASE() for MySQL. Log: {:?}",
        log
    );
    // Should NOT query current_database() (PostgreSQL style)
    assert!(
        !log.iter().any(|q| q.contains("current_database()")),
        "MySQL should not use PostgreSQL-style queries"
    );
}

// ============ PostgreSQL Driver Path Tests ============

#[tokio::test]
async fn postgres_resolves_database_and_schema_name() {
    let conn = postgres_connection("pagila", "public");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(schema.database_name.as_deref(), Some("pagila"));
    assert_eq!(schema.schema_name.as_deref(), Some("public"));
}

#[tokio::test]
async fn postgres_queries_current_database_and_schema() {
    let conn = postgres_connection("my_db", "public");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    let log = conn.query_log();
    assert!(
        log.iter().any(|q| q.contains("current_database()")),
        "should query current_database() for PostgreSQL. Log: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("current_schema()")),
        "should query current_schema() for PostgreSQL. Log: {:?}",
        log
    );
}

// ============ SQLite Driver Path Tests ============

#[tokio::test]
async fn sqlite_uses_main_as_database_name() {
    let db_result = mock_single_value_result("main", Value::String("main".to_string()));
    let conn = Arc::new(
        MockConnection::new("test.db")
            .with_driver("sqlite")
            .with_query_response("SELECT 'main'", db_result),
    );
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(schema.database_name.as_deref(), Some("main"));
}

// ============ MSSQL Driver Path Tests ============

#[tokio::test]
async fn mssql_resolves_database_and_schema_name() {
    let db_result = mock_single_value_result("DB_NAME()", Value::String("AdventureWorks".to_string()));
    let schema_result =
        mock_single_value_result("SCHEMA_NAME()", Value::String("dbo".to_string()));
    let conn = Arc::new(
        MockConnection::new("AdventureWorks")
            .with_driver("mssql")
            .with_query_response("DB_NAME()", db_result)
            .with_query_response("SCHEMA_NAME()", schema_result),
    );
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(schema.database_name.as_deref(), Some("AdventureWorks"));
    assert_eq!(schema.schema_name.as_deref(), Some("dbo"));
}

#[tokio::test]
async fn mssql_queries_db_name_and_schema_name() {
    let db_result = mock_single_value_result("DB_NAME()", Value::String("master".to_string()));
    let schema_result =
        mock_single_value_result("SCHEMA_NAME()", Value::String("dbo".to_string()));
    let conn = Arc::new(
        MockConnection::new("master")
            .with_driver("mssql")
            .with_query_response("DB_NAME()", db_result)
            .with_query_response("SCHEMA_NAME()", schema_result),
    );
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    let log = conn.query_log();
    assert!(
        log.iter().any(|q| q.contains("DB_NAME()")),
        "should query DB_NAME() for MSSQL. Log: {:?}",
        log
    );
    assert!(
        log.iter().any(|q| q.contains("SCHEMA_NAME()")),
        "should query SCHEMA_NAME() for MSSQL. Log: {:?}",
        log
    );
}

// ============ Cache Tests ============

#[tokio::test]
async fn cache_hit_returns_cached_data_without_database_query() {
    let conn = mysql_connection("cached_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    // First load populates cache
    let schema1 = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("first load should succeed");

    let first_query_count = conn.query_count();

    // Second load should be served from cache
    let schema2 = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("second load should succeed");

    // No additional queries should have been executed
    assert_eq!(
        conn.query_count(),
        first_query_count,
        "cache hit should not execute additional queries"
    );

    assert_eq!(schema1.tables.len(), schema2.tables.len());
    assert_eq!(schema2.table_infos.len(), 2);
}

#[tokio::test]
async fn invalidate_cache_forces_reload() {
    let conn = mysql_connection("invalidated_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    // First load
    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("first load should succeed");

    let first_query_count = conn.query_count();

    // Invalidate
    service.invalidate_connection_cache(conn_id);

    // Reload should hit the database again
    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("reload should succeed");

    assert!(
        conn.query_count() > first_query_count,
        "invalidation should force new queries. Before: {}, After: {}",
        first_query_count,
        conn.query_count()
    );
}

// ============ Schema Content Tests ============

#[tokio::test]
async fn load_schema_populates_views_and_triggers() {
    let conn = mysql_connection("full_schema_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    // Mock returns 1 view and 1 trigger
    assert_eq!(schema.views.len(), 1);
    assert!(schema.views.contains(&"active_users".to_string()));

    assert_eq!(schema.triggers.len(), 1);
    assert!(schema.triggers.contains(&"update_timestamp".to_string()));
}

#[tokio::test]
async fn load_schema_populates_table_indexes() {
    let conn = mysql_connection("indexed_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    // Mock returns indexes for each table
    assert!(!schema.table_indexes.is_empty());
    // "users" should have an index
    if let Some(user_indexes) = schema.table_indexes.get("users") {
        assert!(!user_indexes.is_empty());
        assert_eq!(user_indexes[0].name, "idx_users_email");
    }
}

// ============ Partial Failure Tests ============

#[tokio::test]
async fn partial_failure_returns_what_succeeded() {
    // The mock with should_fail=false gives us standard results;
    // there's no way to make individual introspection methods fail selectively
    // in the current mock. This test verifies the graceful-degradation paths
    // still return a DatabaseSchema even when some fields are empty.
    let conn = mysql_connection("partial_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should succeed even with some empty results");

    // Functions and procedures return empty from mock — that's fine
    assert!(schema.functions.is_empty());
    assert!(schema.procedures.is_empty());

    // But tables and views should still be populated
    assert!(!schema.tables.is_empty());
}

// ============ Schema Not Supported Tests ============

#[tokio::test]
async fn schema_not_supported_returns_error() {
    // Build a connection that returns None for as_schema_introspection
    let conn = Arc::new(NoSchemaConnection);
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let result = service.load_database_schema(conn, conn_id).await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(
        format!("{}", err).contains("not supported"),
        "expected SchemaNotSupported error, got: {}",
        err
    );
}

/// A connection that doesn't support schema introspection
struct NoSchemaConnection;

#[async_trait::async_trait]
impl Connection for NoSchemaConnection {
    fn driver_name(&self) -> &str {
        "no_schema"
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> zqlz_core::Result<zqlz_core::StatementResult> {
        Ok(zqlz_core::StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> zqlz_core::Result<zqlz_core::QueryResult> {
        Ok(zqlz_core::QueryResult::empty())
    }

    async fn update_cell(&self, _request: zqlz_core::CellUpdateRequest) -> zqlz_core::Result<u64> {
        Ok(0)
    }

    async fn begin_transaction(&self) -> zqlz_core::Result<Box<dyn zqlz_core::Transaction>> {
        Err(zqlz_core::ZqlzError::NotImplemented("no tx".into()))
    }

    async fn close(&self) -> zqlz_core::Result<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn zqlz_core::SchemaIntrospection> {
        None
    }
}

// ============ get_table_details Tests ============

#[tokio::test]
async fn get_table_details_returns_columns_with_pk() {
    let conn = mysql_connection("details_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let details = service
        .get_table_details(conn.clone() as Arc<dyn Connection>, conn_id, "users", None)
        .await
        .expect("should load table details");

    assert_eq!(details.name, "users");
    assert_eq!(details.columns.len(), 3);

    // "id" should be marked as primary key
    let id_col = details.columns.iter().find(|c| c.name == "id").expect("should have id column");
    assert!(id_col.is_primary_key);

    // "name" should NOT be marked as primary key
    let name_col = details.columns.iter().find(|c| c.name == "name").expect("should have name column");
    assert!(!name_col.is_primary_key);

    assert_eq!(details.primary_key_columns, vec!["id".to_string()]);
}

#[tokio::test]
async fn get_table_details_returns_indexes() {
    let conn = mysql_connection("index_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let details = service
        .get_table_details(conn.clone() as Arc<dyn Connection>, conn_id, "users", None)
        .await
        .expect("should load table details");

    assert_eq!(details.indexes.len(), 1);
    assert_eq!(details.indexes[0].name, "idx_users_email");
    assert!(details.indexes[0].is_unique);
}

#[tokio::test]
async fn get_table_details_caches_columns() {
    let conn = mysql_connection("cache_cols_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    // SchemaCache.set_columns requires a pre-existing cache entry (created by set_tables),
    // so we must load the full schema first to initialize the cache entry.
    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("schema load");

    // Now get_table_details should cache columns into the existing entry
    service
        .get_table_details(conn.clone() as Arc<dyn Connection>, conn_id, "users", None)
        .await
        .expect("first load");

    // Verify columns are now cached
    let cached = service.cache().get_columns(conn_id, "users");
    assert!(cached.is_some(), "columns should be cached after first load");
    assert_eq!(cached.unwrap().len(), 3);
}

#[tokio::test]
async fn get_table_details_uses_cached_columns() {
    let conn = mysql_connection("cached_cols_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    // First load the schema to create the cache entry (required by SchemaCache.set_columns)
    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("schema load");

    // Now pre-populate the column cache for "users" (overwriting any existing)
    service.cache().set_columns(
        conn_id,
        "users",
        vec![zqlz_core::ColumnInfo {
            name: "cached_col".to_string(),
            ordinal: 0,
            data_type: "TEXT".to_string(),
            nullable: false,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
            ..Default::default()
        }],
    );

    // Should use cached columns instead of querying the mock
    let details = service
        .get_table_details(conn.clone() as Arc<dyn Connection>, conn_id, "users", None)
        .await
        .expect("should use cached columns");

    assert_eq!(details.columns.len(), 1);
    assert_eq!(details.columns[0].name, "cached_col");
}

// ============ generate_ddl Tests ============

#[tokio::test]
async fn generate_ddl_returns_ddl_string() {
    let conn = mysql_connection("ddl_db");
    let service = SchemaService::new();

    let ddl = service
        .generate_ddl(
            conn.clone() as Arc<dyn Connection>,
            ObjectType::Table,
            None,
            "users".to_string(),
        )
        .await
        .expect("should generate DDL");

    assert!(ddl.contains("CREATE TABLE"));
}

#[tokio::test]
async fn generate_ddl_fails_when_schema_not_supported() {
    let conn = Arc::new(NoSchemaConnection);
    let service = SchemaService::new();

    let result = service
        .generate_ddl(conn, ObjectType::Table, None, "users".to_string())
        .await;

    assert!(result.is_err());
}

// ============ get_cached_tables Tests ============

#[tokio::test]
async fn get_cached_tables_returns_none_before_load() {
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    assert!(service.get_cached_tables(conn_id).is_none());
}

#[tokio::test]
async fn get_cached_tables_returns_tables_after_load() {
    let conn = mysql_connection("cached_tables_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("load should succeed");

    let cached = service.get_cached_tables(conn_id);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().len(), 2);
}

#[tokio::test]
async fn get_cached_tables_returns_none_after_invalidation() {
    let conn = mysql_connection("inv_tables_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("load should succeed");

    service.invalidate_connection_cache(conn_id);

    assert!(service.get_cached_tables(conn_id).is_none());
}

// ============ Empty Database Tests ============

#[tokio::test]
async fn empty_database_returns_empty_schema() {
    // Build a connection whose mock returns empty tables
    let db_result = mock_single_value_result("DATABASE()", Value::String("empty_db".to_string()));
    let conn = Arc::new(EmptyDatabaseConnection {
        driver: "mysql".to_string(),
        db_result,
    });
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema(conn as Arc<dyn Connection>, conn_id)
        .await
        .expect("should succeed with empty database");

    assert!(schema.tables.is_empty());
    assert!(schema.views.is_empty());
    assert!(schema.triggers.is_empty());
    assert_eq!(schema.database_name.as_deref(), Some("empty_db"));
}

/// Connection mock that returns empty results for all introspection methods
struct EmptyDatabaseConnection {
    driver: String,
    db_result: zqlz_core::QueryResult,
}

#[async_trait::async_trait]
impl Connection for EmptyDatabaseConnection {
    fn driver_name(&self) -> &str {
        &self.driver
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> zqlz_core::Result<zqlz_core::StatementResult> {
        Ok(zqlz_core::StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> zqlz_core::Result<zqlz_core::QueryResult> {
        if sql.contains("DATABASE()") {
            return Ok(self.db_result.clone());
        }
        Ok(zqlz_core::QueryResult::empty())
    }

    async fn update_cell(&self, _request: zqlz_core::CellUpdateRequest) -> zqlz_core::Result<u64> {
        Ok(0)
    }

    async fn begin_transaction(&self) -> zqlz_core::Result<Box<dyn zqlz_core::Transaction>> {
        Err(zqlz_core::ZqlzError::NotImplemented("no tx".into()))
    }

    async fn close(&self) -> zqlz_core::Result<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn zqlz_core::SchemaIntrospection> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl zqlz_core::SchemaIntrospection for EmptyDatabaseConnection {
    async fn list_databases(&self) -> zqlz_core::Result<Vec<zqlz_core::DatabaseInfo>> {
        Ok(vec![])
    }

    async fn list_schemas(&self) -> zqlz_core::Result<Vec<zqlz_core::SchemaInfo>> {
        Ok(vec![])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::TableInfo>> {
        Ok(vec![])
    }

    async fn list_views(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::ViewInfo>> {
        Ok(vec![])
    }

    async fn get_table(&self, _schema: Option<&str>, _name: &str) -> zqlz_core::Result<zqlz_core::TableDetails> {
        Err(zqlz_core::ZqlzError::NotImplemented("empty".into()))
    }

    async fn get_columns(&self, _schema: Option<&str>, _table: &str) -> zqlz_core::Result<Vec<zqlz_core::ColumnInfo>> {
        Ok(vec![])
    }

    async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> zqlz_core::Result<Vec<zqlz_core::IndexInfo>> {
        Ok(vec![])
    }

    async fn get_foreign_keys(&self, _schema: Option<&str>, _table: &str) -> zqlz_core::Result<Vec<zqlz_core::ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn get_primary_key(&self, _schema: Option<&str>, _table: &str) -> zqlz_core::Result<Option<zqlz_core::PrimaryKeyInfo>> {
        Ok(None)
    }

    async fn get_constraints(&self, _schema: Option<&str>, _table: &str) -> zqlz_core::Result<Vec<zqlz_core::ConstraintInfo>> {
        Ok(vec![])
    }

    async fn list_functions(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::ProcedureInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(&self, _schema: Option<&str>, _table: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::TriggerInfo>> {
        Ok(vec![])
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::SequenceInfo>> {
        Ok(vec![])
    }

    async fn list_types(&self, _schema: Option<&str>) -> zqlz_core::Result<Vec<zqlz_core::TypeInfo>> {
        Ok(vec![])
    }

    async fn generate_ddl(&self, _object: &zqlz_core::DatabaseObject) -> zqlz_core::Result<String> {
        Ok(String::new())
    }

    async fn get_dependencies(&self, _object: &zqlz_core::DatabaseObject) -> zqlz_core::Result<Vec<zqlz_core::Dependency>> {
        Ok(vec![])
    }
}
