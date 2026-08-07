//! Common test utilities and mocks

use async_trait::async_trait;
use std::sync::Arc;
use zqlz_core::{
    AffectedRowCountFidelity, BindPlaceholderPolicy, CellUpdateRequest, ColumnInfo, ColumnMeta,
    Connection, ConnectionScope,
    DatabaseObject, ForeignKeyInfo, IndexInfo, PrimaryKeyInfo, QueryResult,
    ResolvedConnectionScope, Result, Row, SchemaIntrospection, SingleRowDmlScope, SqlObjectName,
    StatementResult, TableInfo, TableType, Transaction, TriggerInfo, Value, ViewInfo, ZqlzError,
};

/// Mock connection for testing service-layer logic without a real database.
///
/// Supports configurable driver identity, schema introspection results, and
/// SQL-pattern-based query responses for testing driver-specific code paths
/// (e.g. `SELECT DATABASE()` for MySQL vs `SELECT current_database()` for Postgres).
pub struct MockConnection {
    #[allow(dead_code)]
    pub name: String,
    pub driver: String,
    pub should_fail: bool,
    /// Default query result returned when no pattern matches
    pub query_results: Vec<QueryResult>,
    /// SQL-pattern-based responses: if a query contains the pattern string,
    /// the corresponding result is returned instead of the default.
    pub query_responses: Vec<(String, QueryResult)>,
    pub query_count: Arc<parking_lot::Mutex<usize>>,
    /// Counts column introspection calls so cache invalidation tests can verify
    /// whether table details were regenerated after a refresh.
    pub get_columns_count: Arc<parking_lot::Mutex<usize>>,
    /// Counts DDL generation calls so cache invalidation tests can observe when
    /// the service had to regenerate object DDL instead of serving cached data.
    pub generate_ddl_count: Arc<parking_lot::Mutex<usize>>,
    /// Log of all SQL queries executed, for assertion in tests
    pub query_log: Arc<parking_lot::Mutex<Vec<String>>>,
    /// Schema arguments passed to list_tables, for scope assertions.
    pub list_tables_schemas: Arc<parking_lot::Mutex<Vec<Option<String>>>>,
    /// Last cell update request, for service-layer update assertions.
    pub last_cell_update: Arc<parking_lot::Mutex<Option<CellUpdateRequest>>>,
    /// Whether this mock answers the schema-wide column fetch. Off by default so
    /// existing tests keep exercising the per-table fallback.
    pub bulk_columns_supported: bool,
    /// Counts schema-wide column fetches.
    pub list_all_columns_count: Arc<parking_lot::Mutex<usize>>,
    /// Rows reported by `update_cell`. Configurable so tests can exercise the
    /// zero-row branch that a hardcoded `1` used to hide.
    pub cell_update_affected_rows: u64,
    /// Fidelity reported for affected-row counts.
    pub affected_row_count_fidelity: AffectedRowCountFidelity,
}

impl MockConnection {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            driver: "mock".to_string(),
            should_fail: false,
            query_results: vec![],
            query_responses: vec![],
            query_count: Arc::new(parking_lot::Mutex::new(0)),
            get_columns_count: Arc::new(parking_lot::Mutex::new(0)),
            generate_ddl_count: Arc::new(parking_lot::Mutex::new(0)),
            query_log: Arc::new(parking_lot::Mutex::new(Vec::new())),
            list_tables_schemas: Arc::new(parking_lot::Mutex::new(Vec::new())),
            last_cell_update: Arc::new(parking_lot::Mutex::new(None)),
            bulk_columns_supported: false,
            list_all_columns_count: Arc::new(parking_lot::Mutex::new(0)),
            cell_update_affected_rows: 1,
            affected_row_count_fidelity: AffectedRowCountFidelity::Exact,
        }
    }

    #[allow(dead_code)]
    pub fn with_cell_update_affected_rows(mut self, rows: u64) -> Self {
        self.cell_update_affected_rows = rows;
        self
    }

    #[allow(dead_code)]
    pub fn with_affected_row_count_fidelity(
        mut self,
        fidelity: AffectedRowCountFidelity,
    ) -> Self {
        self.affected_row_count_fidelity = fidelity;
        self
    }

    #[allow(dead_code)]
    pub fn with_bulk_columns(mut self) -> Self {
        self.bulk_columns_supported = true;
        self
    }

    #[allow(dead_code)]
    pub fn list_all_columns_count(&self) -> usize {
        *self.list_all_columns_count.lock()
    }

    /// The fixture column set, shared by the per-table and schema-wide fetches so
    /// tests can assert the two agree.
    #[allow(dead_code)]
    fn mock_columns_for(&self, table: &str) -> Vec<ColumnInfo> {
        match table {
            "users" => vec![
                ColumnInfo {
                    name: "id".to_string(),
                    ordinal: 0,
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    is_auto_increment: true,
                    is_unique: true,
                    ..Default::default()
                },
                ColumnInfo {
                    name: "name".to_string(),
                    ordinal: 1,
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    ..Default::default()
                },
                ColumnInfo {
                    name: "email".to_string(),
                    ordinal: 2,
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ..Default::default()
                },
            ],
            _ => Vec::new(),
        }
    }

    pub fn with_driver(mut self, driver: impl Into<String>) -> Self {
        self.driver = driver.into();
        self
    }

    #[allow(dead_code)]
    pub fn with_failure(mut self) -> Self {
        self.should_fail = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_result(mut self, result: QueryResult) -> Self {
        self.query_results.push(result);
        self
    }

    /// Register a response for queries containing the given SQL pattern.
    pub fn with_query_response(
        mut self,
        sql_contains: impl Into<String>,
        result: QueryResult,
    ) -> Self {
        self.query_responses.push((sql_contains.into(), result));
        self
    }

    #[allow(dead_code)]
    pub fn query_count(&self) -> usize {
        *self.query_count.lock()
    }

    #[allow(dead_code)]
    pub fn get_columns_count(&self) -> usize {
        *self.get_columns_count.lock()
    }

    #[allow(dead_code)]
    pub fn generate_ddl_count(&self) -> usize {
        *self.generate_ddl_count.lock()
    }

    pub fn query_log(&self) -> Vec<String> {
        self.query_log.lock().clone()
    }

    #[allow(dead_code)]
    pub fn list_tables_schemas(&self) -> Vec<Option<String>> {
        self.list_tables_schemas.lock().clone()
    }

    #[allow(dead_code)]
    pub fn last_cell_update(&self) -> Option<CellUpdateRequest> {
        self.last_cell_update.lock().clone()
    }

    async fn current_scalar_string(&self, sql: &str) -> Result<Option<String>> {
        let result = self.query(sql, &[]).await?;
        Ok(result.rows.first().and_then(|row| {
            row.values.first().and_then(|value| match value {
                Value::String(value) => Some(value.clone()),
                Value::Null => None,
                other => Some(other.to_string()),
            })
        }))
    }
}

#[async_trait]
impl Connection for MockConnection {
    fn driver_name(&self) -> &str {
        &self.driver
    }

    fn dialect_id(&self) -> Option<&'static str> {
        match self.driver.as_str() {
            "postgres" | "postgresql" => Some("postgresql"),
            "mysql" => Some("mysql"),
            "mssql" => Some("mssql"),
            "sqlite" => Some("sqlite"),
            "duckdb" => Some("duckdb"),
            _ => None,
        }
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match self.driver.as_str() {
            "postgres" | "postgresql" | "mssql" => match scope {
                ConnectionScope::Default => {
                    resolved.normalized_scope = ConnectionScope::Default;
                    resolved.effective_database = self.current_database_name().await?;
                    resolved.effective_namespace = self.current_namespace_name().await?;
                }
                ConnectionScope::Database(database_name) => {
                    let database_name = database_name.trim().to_string();
                    resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                    resolved.effective_database = Some(database_name.clone());
                    resolved.physical_database_key = Some(database_name);
                    resolved.requires_dedicated_connection = true;
                    resolved.effective_namespace = self.current_namespace_name().await?;
                }
                ConnectionScope::Namespace(namespace) => {
                    let namespace = namespace.trim().to_string();
                    resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                    resolved.effective_database = self.current_database_name().await?;
                    resolved.effective_namespace = Some(namespace.clone());
                    resolved.introspection_scope = Some(namespace);
                }
                ConnectionScope::KeyValueDatabase(index) => {
                    resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
                }
            },
            "mysql" => match scope {
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
            },
            "sqlite" | "duckdb" => match scope {
                ConnectionScope::Default => {
                    resolved.normalized_scope = ConnectionScope::Default;
                    resolved.effective_database = Some("main".to_string());
                    resolved.effective_namespace = Some("main".to_string());
                    resolved.introspection_scope = Some("main".to_string());
                }
                ConnectionScope::Database(namespace) | ConnectionScope::Namespace(namespace) => {
                    let namespace = namespace.trim().to_string();
                    resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                    resolved.effective_database = Some(namespace.clone());
                    resolved.effective_namespace = Some(namespace.clone());
                    resolved.introspection_scope = Some(namespace);
                }
                ConnectionScope::KeyValueDatabase(index) => {
                    resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
                }
            },
            _ => {
                resolved.normalized_scope = scope;
            }
        }

        Ok(resolved)
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        match self.driver.as_str() {
            "postgres" | "postgresql" => {
                self.current_scalar_string("SELECT current_database()")
                    .await
            }
            "mysql" => self.current_scalar_string("SELECT DATABASE()").await,
            "mssql" => self.current_scalar_string("SELECT DB_NAME()").await,
            "sqlite" | "duckdb" => Ok(Some("main".to_string())),
            _ => Ok(None),
        }
    }

    async fn current_namespace_name(&self) -> Result<Option<String>> {
        match self.driver.as_str() {
            "postgres" | "postgresql" => {
                self.current_scalar_string("SELECT current_schema()").await
            }
            "mysql" => self.current_scalar_string("SELECT DATABASE()").await,
            "mssql" => self.current_scalar_string("SELECT SCHEMA_NAME()").await,
            "sqlite" | "duckdb" => Ok(Some("main".to_string())),
            _ => Ok(None),
        }
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        match self.driver.as_str() {
            "postgres" | "postgresql" => BindPlaceholderPolicy::DollarNumbered,
            _ => BindPlaceholderPolicy::QuestionMark,
        }
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        match self.driver.as_str() {
            "mysql" => format!("`{}`", identifier.replace('`', "``")),
            _ => {
                let escaped_identifier = identifier.replace('"', "\"\"");
                format!("\"{}\"", escaped_identifier)
            }
        }
    }

    /// Mirrors the SQLite/PostgreSQL shape so tests can assert that keyless
    /// statements are narrowed to one row.
    fn single_row_dml_scope(
        &self,
        qualified_table: &str,
        where_clause: &str,
    ) -> SingleRowDmlScope {
        match self.driver.as_str() {
            "mysql" => SingleRowDmlScope::StatementSuffix(" LIMIT 1"),
            "mock" => SingleRowDmlScope::Unsupported,
            _ => SingleRowDmlScope::by_row_identity("rowid", qualified_table, where_clause),
        }
    }

    fn paginated_select_sql(&self, base_sql: &str, limit: u64, offset: u64) -> String {
        format!("{} LIMIT {} OFFSET {}", base_sql, limit, offset)
    }

    fn limited_select_sql(&self, base_sql: &str, limit: u64) -> String {
        format!("{} LIMIT {}", base_sql, limit)
    }

    fn supports_fast_exact_count(&self) -> bool {
        matches!(self.driver.as_str(), "mock" | "sqlite" | "duckdb")
    }

    fn should_use_ddl_column_fallback(&self, table_type: TableType, _error_message: &str) -> bool {
        self.driver == "sqlite" && table_type == TableType::VirtualTable
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        let result = match self.driver.as_str() {
            "mysql" => {
                self.query(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1",
                    &[],
                )
                .await?
            }
            "postgres" | "postgresql" => {
                let sql = if table_name.namespace.is_some() {
                    "SELECT reltuples FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE pg_namespace.nspname = $1 AND pg_class.relname = $2"
                } else {
                    "SELECT reltuples FROM pg_class WHERE relname = $1"
                };
                self.query(sql, &[]).await?
            }
            "mssql" => {
                self.query(
                    "SELECT SUM(rows) FROM sys.partitions WHERE object_id = OBJECT_ID(@P1)",
                    &[],
                )
                .await?
            }
            "clickhouse" => {
                self.query(
                    "SELECT total_rows FROM system.tables WHERE database = ? AND name = ?",
                    &[],
                )
                .await?
            }
            _ => return Ok(None),
        };

        Ok(result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
        self.query_log.lock().push(sql.to_string());
        if self.should_fail {
            Err(ZqlzError::Query("Execute failed".into()))
        } else {
            Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 1,
                error: None,
            })
        }
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        *self.query_count.lock() += 1;
        self.query_log.lock().push(sql.to_string());

        if self.should_fail {
            return Err(ZqlzError::Query("Query failed".into()));
        }

        // Check pattern-based responses first
        for (pattern, result) in &self.query_responses {
            if sql.contains(pattern.as_str()) {
                return Ok(result.clone());
            }
        }

        if let Some(result) = self.query_results.first() {
            Ok(result.clone())
        } else {
            Ok(QueryResult::empty())
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: zqlz_core::DropTableOptions,
    ) -> Result<String> {
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
        view_name: &SqlObjectName,
        options: zqlz_core::DropViewOptions,
    ) -> Result<String> {
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
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: zqlz_core::DropTriggerOptions,
    ) -> Result<String> {
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

    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String> {
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
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String> {
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
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String> {
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

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        if self.should_fail {
            Err(ZqlzError::Query("Update failed".into()))
        } else {
            *self.last_cell_update.lock() = Some(request);
            Ok(self.cell_update_affected_rows)
        }
    }

    fn affected_row_count_fidelity(&self) -> AffectedRowCountFidelity {
        self.affected_row_count_fidelity
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Err(ZqlzError::NotImplemented(
            "Transactions not implemented in mock".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.should_fail
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }
}

#[async_trait]
impl SchemaIntrospection for MockConnection {
    async fn list_databases(&self) -> Result<Vec<zqlz_core::DatabaseInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list databases".into()));
        }
        Ok(vec![])
    }

    async fn list_schemas(&self) -> Result<Vec<zqlz_core::SchemaInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list schemas".into()));
        }
        Ok(vec![])
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list tables".into()));
        }

        self.list_tables_schemas
            .lock()
            .push(schema.map(ToOwned::to_owned));

        Ok(vec![
            TableInfo {
                schema: None,
                name: "users".to_string(),
                table_type: TableType::Table,
                owner: None,
                row_count: Some(100),
                size_bytes: Some(1024),
                comment: None,
                index_count: Some(2),
                trigger_count: Some(0),
                key_value_info: None,
            },
            TableInfo {
                schema: None,
                name: "posts".to_string(),
                table_type: TableType::Table,
                owner: None,
                row_count: Some(50),
                size_bytes: Some(512),
                comment: None,
                index_count: Some(1),
                trigger_count: Some(1),
                key_value_info: None,
            },
        ])
    }

    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list views".into()));
        }

        Ok(vec![ViewInfo {
            schema: None,
            name: "active_users".to_string(),
            is_materialized: false,
            definition: Some("SELECT * FROM users WHERE active = 1".to_string()),
            owner: None,
            comment: None,
        }])
    }

    async fn get_table(
        &self,
        _schema: Option<&str>,
        _name: &str,
    ) -> Result<zqlz_core::TableDetails> {
        Err(ZqlzError::NotImplemented(
            "get_table not implemented in mock".into(),
        ))
    }

    async fn list_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<std::collections::HashMap<String, Vec<ColumnInfo>>>> {
        if !self.bulk_columns_supported {
            return Ok(None);
        }

        *self.list_all_columns_count.lock() += 1;

        let mut columns_by_table = std::collections::HashMap::new();
        for table in ["users", "posts"] {
            // Reuses the per-table shape so bulk and per-table agree, without
            // going through the counted `get_columns` entry point.
            columns_by_table.insert(table.to_string(), self.mock_columns_for(table));
        }
        let _ = schema;
        Ok(Some(columns_by_table))
    }

    async fn get_columns(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to get columns".into()));
        }

        *self.get_columns_count.lock() += 1;

        Ok(self.mock_columns_for(table))
    }

    async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> Result<Vec<IndexInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to get indexes".into()));
        }

        Ok(vec![IndexInfo {
            name: "idx_users_email".to_string(),
            columns: vec!["email".to_string()],
            is_unique: true,
            is_primary: false,
            index_type: "BTREE".to_string(),
            comment: None,
            ..Default::default()
        }])
    }

    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to get primary key".into()));
        }

        match table {
            "users" => Ok(Some(PrimaryKeyInfo {
                name: Some("pk_users".to_string()),
                columns: vec!["id".to_string()],
            })),
            "composite_nullable" => Ok(Some(PrimaryKeyInfo {
                name: Some("pk_composite_nullable".to_string()),
                columns: vec!["content_id".to_string(), "status_id".to_string()],
            })),
            _ => Ok(None),
        }
    }

    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<zqlz_core::ConstraintInfo>> {
        Ok(vec![])
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<zqlz_core::FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_procedures(
        &self,
        _schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::ProcedureInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list triggers".into()));
        }

        Ok(vec![TriggerInfo {
            schema: None,
            name: "update_timestamp".to_string(),
            table_name: "posts".to_string(),
            timing: zqlz_core::TriggerTiming::Before,
            events: vec![zqlz_core::TriggerEvent::Update],
            for_each: zqlz_core::TriggerForEach::Row,
            definition: Some("CREATE TRIGGER update_timestamp...".to_string()),
            enabled: true,
            comment: None,
        }])
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<zqlz_core::SequenceInfo>> {
        Ok(vec![])
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<zqlz_core::TypeInfo>> {
        Ok(vec![])
    }

    async fn generate_ddl(&self, _object: &DatabaseObject) -> Result<String> {
        *self.generate_ddl_count.lock() += 1;
        Ok("CREATE TABLE mock (id INTEGER);".to_string())
    }

    async fn get_dependencies(
        &self,
        _object: &DatabaseObject,
    ) -> Result<Vec<zqlz_core::Dependency>> {
        Ok(vec![])
    }
}

/// Helper to create a mock query result with typed columns and row data
pub fn mock_query_result(column_names: Vec<&str>, row_data: Vec<Vec<Value>>) -> QueryResult {
    let columns: Vec<ColumnMeta> = column_names
        .iter()
        .enumerate()
        .map(|(i, name)| ColumnMeta {
            name: name.to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal: i,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        })
        .collect();

    let rows: Vec<Row> = row_data
        .into_iter()
        .map(|values| Row::new(column_names.iter().map(|s| s.to_string()).collect(), values))
        .collect();

    QueryResult {
        id: uuid::Uuid::new_v4(),
        columns,
        rows,
        total_rows: None,
        is_estimated_total: false,
        affected_rows: 0,
        execution_time_ms: 0,
        warnings: vec![],
    }
}

/// Helper to create a single-value query result (e.g. `SELECT DATABASE()`)
#[allow(dead_code)]
pub fn mock_single_value_result(column_name: &str, value: Value) -> QueryResult {
    mock_query_result(vec![column_name], vec![vec![value]])
}

/// Helper to create a test connection with "mock" driver
#[allow(dead_code)]
pub fn test_connection() -> Arc<dyn Connection> {
    Arc::new(MockConnection::new("test_db"))
}

/// Helper to create a failing connection
#[allow(dead_code)]
pub fn failing_connection() -> Arc<dyn Connection> {
    Arc::new(MockConnection::new("failing_db").with_failure())
}

/// Helper to create a MySQL-flavored mock connection that responds to
/// `SELECT DATABASE()` with the given database name.
#[allow(dead_code)]
pub fn mysql_connection(database_name: &str) -> Arc<MockConnection> {
    let db_result =
        mock_single_value_result("DATABASE()", Value::String(database_name.to_string()));
    Arc::new(
        MockConnection::new(database_name)
            .with_driver("mysql")
            .with_query_response("DATABASE()", db_result),
    )
}

/// Same as [`mysql_connection`], but the mock answers the schema-wide column fetch.
#[allow(dead_code)]
pub fn mysql_connection_with_bulk_columns(database_name: &str) -> Arc<MockConnection> {
    let db_result =
        mock_single_value_result("DATABASE()", Value::String(database_name.to_string()));
    Arc::new(
        MockConnection::new(database_name)
            .with_driver("mysql")
            .with_query_response("DATABASE()", db_result)
            .with_bulk_columns(),
    )
}

/// Helper to create a PostgreSQL-flavored mock connection that responds to
/// `current_database()` and `current_schema()` queries.
#[allow(dead_code)]
pub fn postgres_connection(database_name: &str, schema_name: &str) -> Arc<MockConnection> {
    let db_result = mock_single_value_result(
        "current_database()",
        Value::String(database_name.to_string()),
    );
    let schema_result =
        mock_single_value_result("current_schema()", Value::String(schema_name.to_string()));
    Arc::new(
        MockConnection::new(database_name)
            .with_driver("postgresql")
            .with_query_response("current_database()", db_result)
            .with_query_response("current_schema()", schema_result),
    )
}
