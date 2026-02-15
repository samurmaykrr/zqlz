//! Common test utilities and mocks

use async_trait::async_trait;
use std::sync::Arc;
use zqlz_core::{
    CellUpdateRequest, ColumnInfo, Connection, DatabaseObject, ForeignKeyInfo, IndexInfo,
    PrimaryKeyInfo, QueryResult, Result, Row, ColumnMeta, SchemaIntrospection, StatementResult,
    TableInfo, TableType, Transaction, TriggerInfo, Value, ViewInfo, ZqlzError,
};

/// Mock connection for testing service-layer logic without a real database.
///
/// Supports configurable driver identity, schema introspection results, and
/// SQL-pattern-based query responses for testing driver-specific code paths
/// (e.g. `SELECT DATABASE()` for MySQL vs `SELECT current_database()` for Postgres).
pub struct MockConnection {
    pub name: String,
    pub driver: String,
    pub should_fail: bool,
    /// Default query result returned when no pattern matches
    pub query_results: Vec<QueryResult>,
    /// SQL-pattern-based responses: if a query contains the pattern string,
    /// the corresponding result is returned instead of the default.
    pub query_responses: Vec<(String, QueryResult)>,
    pub query_count: Arc<parking_lot::Mutex<usize>>,
    /// Log of all SQL queries executed, for assertion in tests
    pub query_log: Arc<parking_lot::Mutex<Vec<String>>>,
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
            query_log: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    pub fn with_driver(mut self, driver: impl Into<String>) -> Self {
        self.driver = driver.into();
        self
    }

    pub fn with_failure(mut self) -> Self {
        self.should_fail = true;
        self
    }

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

    pub fn query_count(&self) -> usize {
        *self.query_count.lock()
    }

    pub fn query_log(&self) -> Vec<String> {
        self.query_log.lock().clone()
    }
}

#[async_trait]
impl Connection for MockConnection {
    fn driver_name(&self) -> &str {
        &self.driver
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<StatementResult> {
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

    async fn update_cell(&self, _request: CellUpdateRequest) -> Result<u64> {
        if self.should_fail {
            Err(ZqlzError::Query("Update failed".into()))
        } else {
            Ok(1)
        }
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

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to list tables".into()));
        }

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

    async fn get_columns(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        if self.should_fail {
            return Err(ZqlzError::Schema("Failed to get columns".into()));
        }

        match table {
            "users" => Ok(vec![
                ColumnInfo {
                    name: "id".to_string(),
                    ordinal: 0,
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: true,
                    is_auto_increment: true,
                    is_unique: true,
                    foreign_key: None,
                    comment: None,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    ordinal: 1,
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
                },
                ColumnInfo {
                    name: "email".to_string(),
                    ordinal: 2,
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    is_unique: false,
                    foreign_key: None,
                    comment: None,
                },
            ]),
            _ => Ok(vec![]),
        }
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

    async fn list_functions(
        &self,
        _schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::FunctionInfo>> {
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

    async fn list_sequences(
        &self,
        _schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::SequenceInfo>> {
        Ok(vec![])
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<zqlz_core::TypeInfo>> {
        Ok(vec![])
    }

    async fn generate_ddl(&self, _object: &DatabaseObject) -> Result<String> {
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
pub fn mock_single_value_result(column_name: &str, value: Value) -> QueryResult {
    mock_query_result(vec![column_name], vec![vec![value]])
}

/// Helper to create a test connection with "mock" driver
pub fn test_connection() -> Arc<dyn Connection> {
    Arc::new(MockConnection::new("test_db"))
}

/// Helper to create a failing connection
pub fn failing_connection() -> Arc<dyn Connection> {
    Arc::new(MockConnection::new("failing_db").with_failure())
}

/// Helper to create a MySQL-flavored mock connection that responds to
/// `SELECT DATABASE()` with the given database name.
pub fn mysql_connection(database_name: &str) -> Arc<MockConnection> {
    let db_result = mock_single_value_result(
        "DATABASE()",
        Value::String(database_name.to_string()),
    );
    Arc::new(
        MockConnection::new(database_name)
            .with_driver("mysql")
            .with_query_response("DATABASE()", db_result),
    )
}

/// Helper to create a PostgreSQL-flavored mock connection that responds to
/// `current_database()` and `current_schema()` queries.
pub fn postgres_connection(database_name: &str, schema_name: &str) -> Arc<MockConnection> {
    let db_result = mock_single_value_result(
        "current_database()",
        Value::String(database_name.to_string()),
    );
    let schema_result = mock_single_value_result(
        "current_schema()",
        Value::String(schema_name.to_string()),
    );
    Arc::new(
        MockConnection::new(database_name)
            .with_driver("postgresql")
            .with_query_response("current_database()", db_result)
            .with_query_response("current_schema()", schema_result),
    )
}
