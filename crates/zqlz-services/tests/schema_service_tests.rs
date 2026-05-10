//! Integration tests for SchemaService
//!
//! Verifies that `load_database_schema()` correctly resolves database/schema names
//! per driver type and passes them to introspection methods — the core fix for the
//! MySQL "no db connected" bug where `DATABASE()` returning NULL caused empty results.

mod common;

use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    Connection, ConnectionScope, ObjectType, ResolvedConnectionScope, TableType, Value,
};
use zqlz_services::SchemaService;

use common::{mock_single_value_result, mysql_connection, postgres_connection, MockConnection};

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

#[tokio::test]
async fn mysql_explicit_target_database_skips_select_database_query() {
    let conn = mysql_connection("default_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let schema = service
        .load_database_schema_for_database(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            Some("analytics_db"),
        )
        .await
        .expect("should load schema");

    assert_eq!(schema.database_name.as_deref(), Some("analytics_db"));
    assert!(schema.tables.contains(&"users".to_string()));

    let log = conn.query_log();
    assert!(
        !log.iter().any(|q| q.contains("SELECT DATABASE()")),
        "explicit target database should not query DATABASE(). Log: {:?}",
        log
    );
}

#[tokio::test]
async fn mysql_explicit_target_database_clears_stale_objects_panel_cache() {
    let conn = mysql_connection("default_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load initial schema");

    assert!(
        service
            .get_cached_objects_panel_rows_for_kind(conn_id, "table")
            .is_some(),
        "baseline schema load should populate the per-kind objects panel cache"
    );

    let schema = service
        .load_database_schema_for_database(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            Some("analytics_db"),
        )
        .await
        .expect("should load targeted schema");

    assert_eq!(schema.database_name.as_deref(), Some("analytics_db"));
    assert!(
        service
            .get_cached_objects_panel_rows_by_kind(conn_id)
            .is_none(),
        "explicit target-database loads should not leave stale cached rows behind"
    );
}

#[tokio::test]
async fn mysql_explicit_target_database_preserves_object_detail_caches_for_reloaded_schema() {
    let conn = mysql_connection("default_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();
    let connection: Arc<dyn Connection> = conn.clone();

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("should load initial schema");

    let initial_details = service
        .get_table_details(connection.clone(), conn_id, "users", None)
        .await
        .expect("should load initial table details");
    let initial_column_names: Vec<String> = initial_details
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let initial_ddl = service
        .get_or_generate_ddl(&connection, conn_id, "users", None, Some(ObjectType::Table))
        .await
        .expect("should generate initial DDL");

    let cached_get_columns_count = conn.get_columns_count();
    let cached_generate_ddl_count = conn.generate_ddl_count();

    service
        .load_database_schema_for_database(connection.clone(), conn_id, Some("analytics_db"))
        .await
        .expect("should load targeted schema");

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("should reload the default schema after target switch");

    let reloaded_details = service
        .get_table_details(connection.clone(), conn_id, "users", None)
        .await
        .expect("should reuse cached table details after schema reload");
    let reloaded_ddl = service
        .get_or_generate_ddl(&connection, conn_id, "users", None, Some(ObjectType::Table))
        .await
        .expect("should reuse cached DDL after schema reload");

    let reloaded_column_names: Vec<String> = reloaded_details
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();

    assert_eq!(reloaded_details.name, initial_details.name);
    assert_eq!(reloaded_details.table_type, initial_details.table_type);
    assert_eq!(reloaded_column_names, initial_column_names);
    assert_eq!(reloaded_ddl, initial_ddl);
    assert_eq!(
        conn.get_columns_count(),
        cached_get_columns_count,
        "targeted database switches should not evict cached table details for the reloaded schema"
    );
    assert_eq!(
        conn.generate_ddl_count(),
        cached_generate_ddl_count,
        "targeted database switches should not evict cached DDL for the reloaded schema"
    );
}

#[tokio::test]
async fn load_database_schema_preserves_manifest_coverage_provenance_on_cache_hit() {
    let conn = mysql_connection("telemetry_cache_hit_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("initial schema load should succeed");
    assert_eq!(
        service
            .cache()
            .get_objects_panel_manifest_has_driver_manifest(conn_id),
        Some(true),
        "initial load should cache driver-manifest provenance for later cache hits"
    );

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("cached schema load should succeed");

    assert_eq!(
        service
            .cache()
            .get_objects_panel_manifest_has_driver_manifest(conn_id),
        Some(true),
        "cache hit should preserve driver-manifest provenance for telemetry reuse"
    );
}

#[tokio::test]
async fn load_objects_panel_data_for_kind_reuses_cached_schema() {
    let conn = mysql_connection("kind_scoped_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let first = service
        .load_objects_panel_data_for_kind(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            None,
            "table",
            None,
        )
        .await
        .expect("kind-scoped load should succeed");

    assert!(
        first.rows.iter().all(|row| row.object_kind_id() == "table"),
        "kind-scoped load should only return rows for the requested kind"
    );

    let query_count_after_first_load = conn.query_count();

    let second = service
        .load_objects_panel_data_for_kind(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            None,
            "table",
            None,
        )
        .await
        .expect("cached kind-scoped load should succeed");

    assert_eq!(second.rows.len(), first.rows.len());
    assert_eq!(
        conn.query_count(),
        query_count_after_first_load,
        "cached kind-scoped loads should not issue additional schema queries"
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

#[tokio::test]
async fn postgres_full_schema_load_uses_all_schema_scope() {
    let conn = postgres_connection("erp_lab", "public");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load schema");

    assert_eq!(
        conn.list_tables_schemas().first(),
        Some(&None),
        "PostgreSQL sidebar/object-panel full loads must query all user schemas"
    );
}

#[tokio::test]
async fn postgres_tables_only_bootstrap_uses_all_schema_scope() {
    let conn = postgres_connection("erp_lab", "public");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_tables_only(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should load tables");

    assert_eq!(
        conn.list_tables_schemas(),
        vec![None],
        "PostgreSQL bootstrap loads must not be scoped to public"
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
    let db_result =
        mock_single_value_result("DB_NAME()", Value::String("AdventureWorks".to_string()));
    let schema_result = mock_single_value_result("SCHEMA_NAME()", Value::String("dbo".to_string()));
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
    let schema_result = mock_single_value_result("SCHEMA_NAME()", Value::String("dbo".to_string()));
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

    async fn execute(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::StatementResult> {
        Ok(zqlz_core::StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::QueryResult> {
        Ok(zqlz_core::QueryResult::empty())
    }

    fn rename_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        new_table_name: &str,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropTableOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_view_sql(
        &self,
        view_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropViewOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &zqlz_core::SqlObjectName,
        _table_name: Option<&zqlz_core::SqlObjectName>,
        options: zqlz_core::DropTriggerOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn truncate_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &zqlz_core::SqlObjectName,
        new_table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> zqlz_core::Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> zqlz_core::Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> zqlz_core::Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> zqlz_core::Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> zqlz_core::Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    fn should_use_ddl_column_fallback(&self, table_type: TableType, _error_message: &str) -> bool {
        table_type == TableType::VirtualTable
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
    let id_col = details
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("should have id column");
    assert!(id_col.is_primary_key);

    // "name" should NOT be marked as primary key
    let name_col = details
        .columns
        .iter()
        .find(|c| c.name == "name")
        .expect("should have name column");
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
    assert!(
        cached.is_some(),
        "columns should be cached after first load"
    );
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

#[tokio::test]
async fn table_details_cache_is_scoped_by_schema() {
    let conn = mysql_connection("scope_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("schema load");

    let schema_a_details = service
        .get_table_details(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            "users",
            Some("schema_a"),
        )
        .await
        .expect("details load for schema_a");
    let schema_b_details = service
        .get_table_details(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            "users",
            Some("schema_b"),
        )
        .await
        .expect("details load for schema_b");

    assert_eq!(schema_a_details.name, "users");
    assert_eq!(schema_b_details.name, "users");

    assert!(service
        .peek_table_details_cache(conn_id, "users", Some("schema_a"))
        .is_some());
    assert!(service
        .peek_table_details_cache(conn_id, "users", Some("schema_b"))
        .is_some());
    assert!(service
        .peek_table_details_cache(conn_id, "users", Some("schema_c"))
        .is_none());
}

#[tokio::test]
async fn ddl_cache_is_scoped_by_schema() {
    let conn = mysql_connection("scope_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("schema load");

    let ddl_a = service
        .get_or_generate_ddl(
            &(conn.clone() as Arc<dyn Connection>),
            conn_id,
            "users",
            Some("schema_a"),
            Some(ObjectType::Table),
        )
        .await;
    let ddl_b = service
        .get_or_generate_ddl(
            &(conn.clone() as Arc<dyn Connection>),
            conn_id,
            "users",
            Some("schema_b"),
            Some(ObjectType::Table),
        )
        .await;

    assert!(ddl_a.is_some());
    assert!(ddl_b.is_some());
}

#[tokio::test]
async fn invalidate_connection_cache_clears_generated_ddl_cache() {
    let conn = mysql_connection("ddl_invalidation_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();
    let connection: Arc<dyn Connection> = conn.clone();

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("schema load");

    let initial_generate_ddl_count = conn.generate_ddl_count();

    let ddl = service
        .get_or_generate_ddl(&connection, conn_id, "users", None, Some(ObjectType::Table))
        .await
        .expect("initial DDL generation");

    assert!(ddl.contains("CREATE TABLE"));
    assert_eq!(
        conn.generate_ddl_count(),
        initial_generate_ddl_count + 1,
        "the first DDL lookup should invoke the connection"
    );

    let cached_ddl = service
        .get_or_generate_ddl(&connection, conn_id, "users", None, Some(ObjectType::Table))
        .await
        .expect("cached DDL lookup");

    assert_eq!(cached_ddl, ddl);
    assert_eq!(
        conn.generate_ddl_count(),
        initial_generate_ddl_count + 1,
        "the second DDL lookup should reuse the cache"
    );

    service.invalidate_connection_cache(conn_id);

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("schema reload after invalidation");

    let reloaded_ddl = service
        .get_or_generate_ddl(&connection, conn_id, "users", None, Some(ObjectType::Table))
        .await
        .expect("reloaded DDL generation");

    assert_eq!(reloaded_ddl, ddl);
    assert_eq!(
        conn.generate_ddl_count(),
        initial_generate_ddl_count + 2,
        "invalidating and reloading the connection should force DDL regeneration"
    );
}

#[tokio::test]
async fn invalidate_connection_cache_clears_table_details_cache() {
    let conn = mysql_connection("table_details_invalidation_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();
    let connection: Arc<dyn Connection> = conn.clone();

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("schema load");

    let initial_details = service
        .get_table_details(connection.clone(), conn_id, "users", None)
        .await
        .expect("initial table details");

    let initial_get_columns_count = conn.get_columns_count();

    assert!(
        service
            .peek_table_details_cache(conn_id, "users", None)
            .is_some(),
        "table details should be cached after the first lookup"
    );

    service.invalidate_connection_cache(conn_id);

    assert!(
        service
            .peek_table_details_cache(conn_id, "users", None)
            .is_none(),
        "refresh invalidation should clear cached table details"
    );
    assert!(
        service.get_all_cached_table_details(conn_id).is_none(),
        "refresh invalidation should also clear the aggregate table-details cache view"
    );

    service
        .load_database_schema(connection.clone(), conn_id)
        .await
        .expect("schema reload after invalidation");

    let reloaded_details = service
        .get_table_details(connection, conn_id, "users", None)
        .await
        .expect("reloaded table details");

    assert_eq!(
        reloaded_details.columns.len(),
        initial_details.columns.len()
    );
    assert!(
        conn.get_columns_count() > initial_get_columns_count,
        "table details should be regenerated after refresh invalidation"
    );
    assert!(
        service
            .peek_table_details_cache(conn_id, "users", None)
            .is_some(),
        "table details should be cached again after reload"
    );
}

#[tokio::test]
async fn get_table_details_falls_back_for_sqlite_virtual_tables() {
    let conn = Arc::new(SqliteVirtualTableFallbackConnection);
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_tables_only(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("should cache virtual table metadata");

    let details = service
        .get_table_details(
            conn.clone() as Arc<dyn Connection>,
            conn_id,
            "items_fts",
            None,
        )
        .await
        .expect("should fall back to DDL parsing");

    assert_eq!(details.table_type, TableType::VirtualTable);
    let column_names: Vec<&str> = details
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(column_names, vec!["title", "body"]);
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

// ============ get_cached_view_names Tests ============

#[tokio::test]
async fn get_cached_view_names_returns_none_after_invalidation_and_reloads() {
    let conn = mysql_connection("cached_views_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("initial load should succeed");

    let initial_view_names = service
        .get_cached_view_names(conn_id)
        .expect("view names should be cached after the initial load");

    assert_eq!(initial_view_names, vec!["active_users".to_string()]);

    service.invalidate_connection_cache(conn_id);

    assert!(
        service.get_cached_view_names(conn_id).is_none(),
        "refresh invalidation should clear cached view names"
    );

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("reload after invalidation should succeed");

    let reloaded_view_names = service
        .get_cached_view_names(conn_id)
        .expect("view names should be cached again after reload");

    assert_eq!(
        reloaded_view_names, initial_view_names,
        "refresh reload should repopulate the same cached view names"
    );
}

#[tokio::test]
async fn refresh_invalidation_clears_kind_scoped_objects_panel_cache() {
    let conn = mysql_connection("refresh_kind_cache_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("initial load should succeed");

    let cached_table_rows = service.get_cached_objects_panel_rows_for_kind(conn_id, "table");
    assert!(
        cached_table_rows
            .as_ref()
            .is_some_and(|rows| !rows.is_empty()),
        "table kind rows should be cached after initial schema load"
    );

    service.invalidate_connection_cache(conn_id);

    assert!(
        service
            .get_cached_objects_panel_rows_for_kind(conn_id, "table")
            .is_none(),
        "kind-scoped objects panel cache should be cleared after refresh invalidation"
    );

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("reload after invalidation should succeed");

    let recached_table_rows = service.get_cached_objects_panel_rows_for_kind(conn_id, "table");
    assert!(
        recached_table_rows
            .as_ref()
            .is_some_and(|rows| !rows.is_empty()),
        "table kind rows should be repopulated after refresh-driven reload"
    );
}

#[tokio::test]
async fn refresh_reload_keeps_manifest_and_kind_cache_coherent() {
    let conn = mysql_connection("refresh_manifest_rows_coherence_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    let initial_schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("initial load should succeed");

    let initial_manifest_kind_ids: std::collections::HashSet<String> = initial_schema
        .objects_panel_manifest
        .as_ref()
        .expect("initial schema should include objects panel manifest")
        .object_kinds
        .iter()
        .map(|kind| kind.id.clone())
        .collect();
    assert!(
        !initial_manifest_kind_ids.is_empty(),
        "initial schema should expose at least one manifest kind"
    );

    let initial_row_kind_ids: std::collections::HashSet<String> = initial_schema
        .objects_panel_data
        .as_ref()
        .expect("initial schema should include objects panel rows")
        .rows
        .iter()
        .map(|row| row.object_kind_id().to_string())
        .collect();
    assert!(
        initial_row_kind_ids
            .iter()
            .all(|kind_id| initial_manifest_kind_ids.contains(kind_id)),
        "every loaded row kind should be declared by the manifest"
    );

    let initial_cached_rows_by_kind = service
        .get_cached_objects_panel_rows_by_kind(conn_id)
        .expect("kind cache should be available after initial load");

    service.invalidate_connection_cache(conn_id);

    assert!(
        service
            .get_cached_objects_panel_rows_by_kind(conn_id)
            .is_none(),
        "kind cache should be cleared by refresh invalidation"
    );

    let reloaded_schema = service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("reload after invalidation should succeed");

    let reloaded_manifest_kind_ids: std::collections::HashSet<String> = reloaded_schema
        .objects_panel_manifest
        .as_ref()
        .expect("reloaded schema should include objects panel manifest")
        .object_kinds
        .iter()
        .map(|kind| kind.id.clone())
        .collect();
    assert_eq!(
        reloaded_manifest_kind_ids, initial_manifest_kind_ids,
        "refresh reload should keep the same manifest kind taxonomy for stable introspection"
    );

    let reloaded_cached_rows_by_kind = service
        .get_cached_objects_panel_rows_by_kind(conn_id)
        .expect("kind cache should be repopulated after reload");

    assert!(
        reloaded_cached_rows_by_kind
            .keys()
            .all(|kind_id| reloaded_manifest_kind_ids.contains(kind_id)),
        "reloaded kind cache should only contain kinds declared in the reloaded manifest"
    );

    let initial_kind_counts: std::collections::HashMap<String, usize> = initial_cached_rows_by_kind
        .iter()
        .map(|(kind_id, rows)| (kind_id.clone(), rows.len()))
        .collect();
    let reloaded_kind_counts: std::collections::HashMap<String, usize> =
        reloaded_cached_rows_by_kind
            .iter()
            .map(|(kind_id, rows)| (kind_id.clone(), rows.len()))
            .collect();

    assert_eq!(
        reloaded_kind_counts, initial_kind_counts,
        "refresh reload should preserve per-kind row counts for stable source data"
    );
}

#[tokio::test]
async fn refresh_invalidation_clears_objects_panel_manifest_and_data_cache() {
    let conn = mysql_connection("refresh_objects_panel_manifest_cache_db");
    let service = SchemaService::new();
    let conn_id = Uuid::new_v4();

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("initial load should succeed");

    let initial_objects_panel_data = service
        .cache()
        .get_objects_panel_data(conn_id)
        .expect("objects panel data should be cached after initial load");
    let initial_objects_panel_manifest = service
        .cache()
        .get_objects_panel_manifest(conn_id)
        .expect("objects panel manifest should be cached after initial load");
    let initial_kind_ids: std::collections::BTreeSet<String> = initial_objects_panel_manifest
        .object_kinds
        .iter()
        .map(|kind| kind.id.clone())
        .collect();
    let initial_row_kind_ids: std::collections::BTreeSet<String> = initial_objects_panel_data
        .rows
        .iter()
        .map(|row| row.object_kind_id().to_string())
        .collect();

    assert!(
        initial_row_kind_ids.is_subset(&initial_kind_ids),
        "cached objects panel data should only include kinds declared by the cached manifest"
    );

    service.invalidate_connection_cache(conn_id);

    assert!(
        service.cache().get_objects_panel_data(conn_id).is_none(),
        "objects panel data cache should be cleared by refresh invalidation"
    );
    assert!(
        service
            .cache()
            .get_objects_panel_manifest(conn_id)
            .is_none(),
        "objects panel manifest cache should be cleared by refresh invalidation"
    );

    service
        .load_database_schema(conn.clone() as Arc<dyn Connection>, conn_id)
        .await
        .expect("reload after invalidation should succeed");

    let reloaded_objects_panel_data = service
        .cache()
        .get_objects_panel_data(conn_id)
        .expect("objects panel data should be repopulated after reload");
    let reloaded_objects_panel_manifest = service
        .cache()
        .get_objects_panel_manifest(conn_id)
        .expect("objects panel manifest should be repopulated after reload");
    let reloaded_kind_ids: std::collections::BTreeSet<String> = reloaded_objects_panel_manifest
        .object_kinds
        .iter()
        .map(|kind| kind.id.clone())
        .collect();
    let reloaded_row_kind_ids: std::collections::BTreeSet<String> = reloaded_objects_panel_data
        .rows
        .iter()
        .map(|row| row.object_kind_id().to_string())
        .collect();

    assert_eq!(
        reloaded_kind_ids, initial_kind_ids,
        "refresh reload should preserve the cached manifest taxonomy"
    );
    assert_eq!(
        reloaded_row_kind_ids, initial_row_kind_ids,
        "refresh reload should repopulate the same cached objects panel row kinds"
    );
    assert_eq!(
        reloaded_objects_panel_data.rows.len(),
        initial_objects_panel_data.rows.len(),
        "refresh reload should repopulate the same number of cached objects panel rows"
    );
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

    async fn resolve_scope(
        &self,
        scope: ConnectionScope,
    ) -> zqlz_core::Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                let database_name = self.current_database_name().await?;
                resolved.effective_database = database_name.clone();
                resolved.effective_namespace = database_name.clone();
                resolved.introspection_scope = database_name;
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.effective_database = Some(database_name.clone());
                resolved.effective_namespace = Some(database_name.clone());
                resolved.introspection_scope = Some(database_name);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    async fn current_database_name(&self) -> zqlz_core::Result<Option<String>> {
        let result = self.query("SELECT DATABASE()", &[]).await?;
        Ok(result.rows.first().and_then(|row| {
            row.values.first().and_then(|value| match value {
                Value::String(value) => Some(value.clone()),
                Value::Null => None,
                other => Some(other.to_string()),
            })
        }))
    }

    async fn current_namespace_name(&self) -> zqlz_core::Result<Option<String>> {
        self.current_database_name().await
    }

    async fn execute(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::StatementResult> {
        Ok(zqlz_core::StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(
        &self,
        sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::QueryResult> {
        if sql.contains("DATABASE()") {
            return Ok(self.db_result.clone());
        }
        Ok(zqlz_core::QueryResult::empty())
    }

    fn rename_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        new_table_name: &str,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropTableOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_view_sql(
        &self,
        view_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropViewOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &zqlz_core::SqlObjectName,
        _table_name: Option<&zqlz_core::SqlObjectName>,
        options: zqlz_core::DropTriggerOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn truncate_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &zqlz_core::SqlObjectName,
        new_table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> zqlz_core::Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> zqlz_core::Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> zqlz_core::Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> zqlz_core::Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> zqlz_core::Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    fn should_use_ddl_column_fallback(&self, _table_type: TableType, _error_message: &str) -> bool {
        true
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

    async fn list_tables(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TableInfo>> {
        Ok(vec![])
    }

    async fn list_views(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::ViewInfo>> {
        Ok(vec![])
    }

    async fn get_table(
        &self,
        _schema: Option<&str>,
        _name: &str,
    ) -> zqlz_core::Result<zqlz_core::TableDetails> {
        Err(zqlz_core::ZqlzError::NotImplemented("empty".into()))
    }

    async fn get_columns(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ColumnInfo>> {
        Ok(vec![])
    }

    async fn get_indexes(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::IndexInfo>> {
        Ok(vec![])
    }

    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Option<zqlz_core::PrimaryKeyInfo>> {
        Ok(None)
    }

    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ConstraintInfo>> {
        Ok(vec![])
    }

    async fn list_functions(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_procedures(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::ProcedureInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TriggerInfo>> {
        Ok(vec![])
    }

    async fn list_sequences(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::SequenceInfo>> {
        Ok(vec![])
    }

    async fn list_types(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TypeInfo>> {
        Ok(vec![])
    }

    async fn generate_ddl(&self, _object: &zqlz_core::DatabaseObject) -> zqlz_core::Result<String> {
        Ok(String::new())
    }

    async fn get_dependencies(
        &self,
        _object: &zqlz_core::DatabaseObject,
    ) -> zqlz_core::Result<Vec<zqlz_core::Dependency>> {
        Ok(vec![])
    }
}

struct SqliteVirtualTableFallbackConnection;

#[async_trait::async_trait]
impl Connection for SqliteVirtualTableFallbackConnection {
    fn driver_name(&self) -> &str {
        "sqlite"
    }

    fn should_use_ddl_column_fallback(&self, _table_type: TableType, _error_message: &str) -> bool {
        true
    }

    async fn execute(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::StatementResult> {
        Ok(zqlz_core::StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(
        &self,
        _sql: &str,
        _params: &[Value],
    ) -> zqlz_core::Result<zqlz_core::QueryResult> {
        Ok(zqlz_core::QueryResult::empty())
    }

    fn rename_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        new_table_name: &str,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropTableOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_view_sql(
        &self,
        view_name: &zqlz_core::SqlObjectName,
        options: zqlz_core::DropViewOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &zqlz_core::SqlObjectName,
        _table_name: Option<&zqlz_core::SqlObjectName>,
        options: zqlz_core::DropTriggerOptions,
    ) -> zqlz_core::Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn truncate_table_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &zqlz_core::SqlObjectName,
        new_table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> zqlz_core::Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
    ) -> zqlz_core::Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> zqlz_core::Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> zqlz_core::Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &zqlz_core::SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> zqlz_core::Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> zqlz_core::Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
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
impl zqlz_core::SchemaIntrospection for SqliteVirtualTableFallbackConnection {
    async fn list_databases(&self) -> zqlz_core::Result<Vec<zqlz_core::DatabaseInfo>> {
        Ok(vec![])
    }

    async fn list_schemas(&self) -> zqlz_core::Result<Vec<zqlz_core::SchemaInfo>> {
        Ok(vec![])
    }

    async fn list_tables(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TableInfo>> {
        Ok(vec![zqlz_core::TableInfo {
            schema: Some("main".to_string()),
            name: "items_fts".to_string(),
            table_type: TableType::VirtualTable,
            owner: None,
            row_count: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        }])
    }

    async fn list_views(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::ViewInfo>> {
        Ok(vec![])
    }

    async fn get_table(
        &self,
        _schema: Option<&str>,
        _name: &str,
    ) -> zqlz_core::Result<zqlz_core::TableDetails> {
        Err(zqlz_core::ZqlzError::NotImplemented("unused".into()))
    }

    async fn get_columns(
        &self,
        _schema: Option<&str>,
        table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ColumnInfo>> {
        if table == "items_fts" {
            Err(zqlz_core::ZqlzError::Schema(
                "vtable constructor failed: items_fts".into(),
            ))
        } else {
            Ok(vec![])
        }
    }

    async fn get_indexes(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::IndexInfo>> {
        Ok(vec![])
    }

    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Option<zqlz_core::PrimaryKeyInfo>> {
        Ok(None)
    }

    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> zqlz_core::Result<Vec<zqlz_core::ConstraintInfo>> {
        Ok(vec![])
    }

    async fn list_functions(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_procedures(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::ProcedureInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TriggerInfo>> {
        Ok(vec![])
    }

    async fn list_sequences(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::SequenceInfo>> {
        Ok(vec![])
    }

    async fn list_types(
        &self,
        _schema: Option<&str>,
    ) -> zqlz_core::Result<Vec<zqlz_core::TypeInfo>> {
        Ok(vec![])
    }

    async fn generate_ddl(&self, object: &zqlz_core::DatabaseObject) -> zqlz_core::Result<String> {
        if object.name == "items_fts" {
            Ok(
                "CREATE VIRTUAL TABLE items_fts USING fts5(title, body, tokenize='porter')"
                    .to_string(),
            )
        } else {
            Err(zqlz_core::ZqlzError::NotFound(object.name.clone()))
        }
    }

    async fn get_dependencies(
        &self,
        _object: &zqlz_core::DatabaseObject,
    ) -> zqlz_core::Result<Vec<zqlz_core::Dependency>> {
        Ok(vec![])
    }
}
