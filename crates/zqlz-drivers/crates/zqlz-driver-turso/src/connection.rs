//! Turso remote connection implementation.

use std::collections::HashMap;
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, ColumnInfo, ColumnMeta,
    Connection, ConnectionScope, ConstraintInfo, DatabaseInfo, DatabaseObject, Dependency,
    DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig, ExplainParserKind,
    ForeignKeyChecksSql, ForeignKeyInfo, FunctionInfo, ImportIndexCapabilities,
    IndexInfo, ObjectType, ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelObjectRef,
    ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, QueryCancelHandle, QueryResult,
    ResolvedConnectionScope, Result, Row, RowIdentifier, SchemaInfo, SchemaIntrospection,
    SequenceInfo, SingleRowDmlScope, SqlObjectName, StatementResult, TableDetails, TableInfo,
    TableType, Transaction,
    TriggerInfo, TypeInfo, Value, ViewInfo, ZqlzError,
};
use zqlz_schema_engine::{DefaultDialect, SchemaEngine};

use crate::schema::TursoCatalog;

/// Execute a query against a Turso connection on the Turso runtime. Shared by
/// `TursoConnection` and the schema-introspection adapter.
pub(crate) async fn run_turso_query(
    connection: &libsql::Connection,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    let connection = connection.clone();
    let sql = sql.to_string();
    let params = params.to_vec();
    run_on_turso_runtime(async move {
        TursoConnection::query_with_connection(&connection, &sql, &params).await
    })
    .await
}

fn get_turso_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-turso-runtime")
            .build()
            .expect("Failed to create Tokio runtime for Turso driver")
    })
}

async fn run_on_turso_runtime<T, F>(future: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    get_turso_runtime()
        .spawn(future)
        .await
        .map_err(|error| ZqlzError::Connection(format!("Turso runtime task failed: {}", error)))?
}

/// Cancel handle for Turso queries.
pub struct TursoCancelHandle {
    connection: libsql::Connection,
}

impl QueryCancelHandle for TursoCancelHandle {
    fn cancel(&self) {
        if let Err(error) = self.connection.interrupt() {
            tracing::warn!(error = %error, "failed to interrupt Turso query");
        }
    }
}

/// Turso remote connection wrapper.
pub struct TursoConnection {
    database: libsql::Database,
    connection: libsql::Connection,
    closed: AtomicBool,
    pub(crate) schema_engine: SchemaEngine,
}

impl TursoConnection {
    pub async fn connect(url: String, auth_token: String) -> Result<Self> {
        let database = run_on_turso_runtime(async move {
            libsql::Builder::new_remote(url, auth_token)
                .build()
                .await
                .map_err(|error| {
                    ZqlzError::Connection(format!("Failed to create Turso database: {}", error))
                })
        })
        .await?;
        let connection = database.connect().map_err(|error| {
            ZqlzError::Connection(format!("Failed to connect to Turso database: {}", error))
        })?;

        let schema_engine = SchemaEngine::new(
            Arc::new(TursoCatalog::new(connection.clone())),
            Arc::new(DefaultDialect),
        );
        Ok(Self {
            database,
            connection,
            closed: AtomicBool::new(false),
            schema_engine,
        })
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(ZqlzError::Connection(
                "Turso connection is closed".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    async fn resolve_catalog_table_name(&self, requested_name: &str) -> Result<String> {
        let tables = self.list_tables(None).await?;
        if tables.iter().any(|table| table.name == requested_name) {
            return Ok(requested_name.to_string());
        }

        if let Some((schema_name, relation_name)) = requested_name.split_once('.')
            && schema_name != "main"
            && tables.iter().any(|table| table.name == relation_name)
        {
            return Ok(relation_name.to_string());
        }

        Ok(requested_name.to_string())
    }

    fn sqlite_identifier_literal(identifier: &str) -> String {
        identifier.replace('"', "\"\"")
    }

    fn sqlite_string_literal(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn classify_sqlite_table_type(create_sql: Option<&str>) -> TableType {
        let Some(create_sql) = create_sql else {
            return TableType::Table;
        };

        if create_sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("CREATE VIRTUAL TABLE")
        {
            TableType::VirtualTable
        } else {
            TableType::Table
        }
    }

    pub(crate) async fn query_with_connection(
        connection: &libsql::Connection,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        let libsql_params = values_to_libsql(params);
        let statement = connection
            .prepare(sql)
            .await
            .map_err(|error| ZqlzError::Query(format!("Failed to prepare query: {}", error)))?;
        let statement_columns = statement.columns();
        let column_count = statement.column_count();
        let mut columns = Vec::with_capacity(column_count);
        let mut column_names = Vec::with_capacity(column_count);

        for index in 0..column_count {
            let statement_column = statement_columns.get(index);
            let name = statement_column
                .map(|column| column.name().to_string())
                .unwrap_or_else(|| format!("column_{}", index + 1));
            let data_type = statement_column
                .and_then(|column| column.decl_type())
                .unwrap_or("DYNAMIC")
                .to_string();

            column_names.push(name.clone());
            columns.push(ColumnMeta {
                name,
                data_type,
                nullable: true,
                ordinal: index,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            });
        }

        let mut query_rows = statement
            .query(libsql::params_from_iter(libsql_params))
            .await
            .map_err(|error| ZqlzError::Query(format!("Failed to execute query: {}", error)))?;
        let mut rows = Vec::new();

        while let Some(row) = query_rows
            .next()
            .await
            .map_err(|error| ZqlzError::Query(format!("Failed to fetch row: {}", error)))?
        {
            let mut values = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                values.push(libsql_row_value_to_zqlz(&row, index)?);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();
        Ok(QueryResult {
            id: uuid::Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(total_rows as u64),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn execute_with_connection(
        connection: &libsql::Connection,
        sql: &str,
        params: &[Value],
    ) -> Result<StatementResult> {
        let libsql_params = values_to_libsql(params);
        let affected_rows = connection
            .execute(sql, libsql::params_from_iter(libsql_params))
            .await
            .map_err(|error| ZqlzError::Query(format!("Failed to execute statement: {}", error)))?;

        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows,
            error: None,
        })
    }
}

#[async_trait]
impl Connection for TursoConnection {
    fn driver_name(&self) -> &str {
        "turso"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("sqlite")
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = Some("main".to_string());
                resolved.effective_namespace = Some("main".to_string());
                resolved.introspection_scope = Some("main".to_string());
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let namespace = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                resolved.effective_database = Some(namespace.clone());
                resolved.effective_namespace = Some(namespace.clone());
                resolved.introspection_scope = Some(namespace);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::sqlite()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::Sqlite
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        format!("\"{}\"", Self::sqlite_identifier_literal(identifier))
    }

    fn render_qualified_name(&self, object_name: &SqlObjectName) -> String {
        match object_name.namespace.as_deref() {
            Some("main") | None => self.quote_identifier(&object_name.name),
            Some(namespace) => {
                self.quote_identifier(&format!("{}.{}", namespace, object_name.name))
            }
        }
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    fn max_bind_parameters(&self) -> usize {
        32_766
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("CAST({} AS TEXT)", expression_sql)
    }

    fn supports_partial_indexes(&self) -> bool {
        true
    }

    fn supports_include_indexes(&self) -> bool {
        false
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        true
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: true,
            supports_include: false,
            supports_nulls_ordering: true,
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        if table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "Turso does not support schema-qualified RENAME TABLE".to_string(),
            ));
        }

        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.quote_identifier(&table_name.name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String> {
        if table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "Turso does not support schema-qualified DROP TABLE".to_string(),
            ));
        }
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "Turso does not support DROP TABLE ... CASCADE".to_string(),
            ));
        }

        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&table_name.name));
        Ok(sql)
    }

    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String> {
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "Turso does not support DROP VIEW ... CASCADE".to_string(),
            ));
        }
        if view_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "Turso does not support schema-qualified DROP VIEW".to_string(),
            ));
        }

        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&view_name.name));
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        if trigger_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "Turso does not support schema-qualified DROP TRIGGER".to_string(),
            ));
        }
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "Turso does not support DROP TRIGGER ... CASCADE".to_string(),
            ));
        }

        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.quote_identifier(&trigger_name.name));
        Ok(sql)
    }

    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String> {
        if source_table_name.namespace.is_some() || new_table_name.namespace.is_some() {
            return Err(ZqlzError::NotSupported(
                "Turso does not support schema-qualified table duplication".to_string(),
            ));
        }

        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.quote_identifier(&new_table_name.name),
            self.quote_identifier(&source_table_name.name)
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

    fn reset_table_identity_sql(&self, table_name: &SqlObjectName) -> Option<String> {
        Some(format!(
            "DELETE FROM sqlite_sequence WHERE name = '{}'",
            table_name.name.replace('\'', "''")
        ))
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        let table_name = sequence_name
            .split_once('.')
            .map(|(table, _)| table)
            .unwrap_or(sequence_name);
        Some(format!(
            "INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('{}', {})",
            table_name.replace('\'', "''"),
            current_value
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT seq FROM sqlite_sequence WHERE name = '{}'",
            table_name.replace('\'', "''")
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(Value::as_i64)
            .filter(|value| *value > 0))
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Unknown
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        Some(ForeignKeyChecksSql {
            disable_sql: "PRAGMA foreign_keys = OFF".to_string(),
            enable_sql: "PRAGMA foreign_keys = ON".to_string(),
        })
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        Ok(Some("main".to_string()))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some("main".to_string()))
    }

    fn has_session_namespace(&self) -> bool {
        false
    }

    fn supports_fast_exact_count(&self) -> bool {
        true
    }

    fn should_use_schema_only_table_browse_fallback(
        &self,
        table_type: TableType,
        error_message: &str,
    ) -> bool {
        table_type == TableType::VirtualTable
            && error_message
                .to_ascii_lowercase()
                .contains("vtable constructor failed")
    }

    fn should_use_ddl_column_fallback(&self, table_type: TableType, _error_message: &str) -> bool {
        table_type == TableType::VirtualTable
    }

    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        let (where_clause, mut params) = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported by Turso. Use primary key or full row identifier."
                        .to_string(),
                ));
            }
            RowIdentifier::PrimaryKey(primary_key_values) => {
                let conditions = primary_key_values
                    .iter()
                    .enumerate()
                    .map(|(index, (column_name, _))| {
                        format!(
                            "{} = {}",
                            self.quote_identifier(column_name),
                            self.format_bind_placeholder(index + 1)
                        )
                    })
                    .collect::<Vec<_>>();
                let params = primary_key_values
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                (conditions.join(" AND "), params)
            }
            RowIdentifier::FullRow(row_values) => {
                let mut next_param_index = 1usize;
                let conditions = row_values
                    .iter()
                    .map(|(column_name, value)| {
                        if value == &Value::Null {
                            format!("{} IS NULL", self.quote_identifier(column_name))
                        } else {
                            let placeholder = self.format_bind_placeholder(next_param_index);
                            next_param_index += 1;
                            format!("{} = {}", self.quote_identifier(column_name), placeholder)
                        }
                    })
                    .collect::<Vec<_>>();
                let params = row_values
                    .iter()
                    .filter(|(_, value)| value != &Value::Null)
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                (conditions.join(" AND "), params)
            }
        };

        let table_name = self.quote_identifier(&request.table_name);
        let column_name = self.quote_identifier(&request.column_name);
        let scope = match &request.row_identifier {
            RowIdentifier::FullRow(_) => self.single_row_dml_scope(&table_name, &where_clause),
            _ => SingleRowDmlScope::Unsupported,
        };
        let (where_clause, statement_suffix) = scope.apply(&where_clause);
        let sql = if let Some(new_value) = &request.new_value {
            params.insert(0, new_value.clone());
            format!(
                "UPDATE {} SET {} = {} WHERE {}{}",
                table_name,
                column_name,
                self.format_bind_placeholder(0),
                where_clause,
                statement_suffix
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {}{}",
                table_name, column_name, where_clause, statement_suffix
            )
        };

        let result = self.execute(&sql, &params).await?;
        Ok(result.affected_rows)
    }

    /// Turso speaks SQLite, where every keyless table still has a `rowid`.
    fn single_row_dml_scope(
        &self,
        qualified_table: &str,
        where_clause: &str,
    ) -> SingleRowDmlScope {
        SingleRowDmlScope::by_row_identity("rowid", qualified_table, where_clause)
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        if table_name
            .namespace
            .as_deref()
            .is_some_and(|namespace| namespace != "main")
        {
            return Ok(None);
        }

        let sql = format!(
            "SELECT COALESCE(CAST(stat AS INTEGER), 0) FROM sqlite_stat1 WHERE tbl = '{}' LIMIT 1",
            Self::sqlite_string_literal(&table_name.name)
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(Value::as_i64)
            .and_then(|value| u64::try_from(value).ok()))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let connection = self.connection.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        run_on_turso_runtime(async move {
            Self::execute_with_connection(&connection, &sql, &params).await
        })
        .await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let connection = self.connection.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        run_on_turso_runtime(async move {
            Self::query_with_connection(&connection, &sql, &params).await
        })
        .await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.ensure_not_closed()?;
        let connection = self.connection.clone();
        let transaction = run_on_turso_runtime(async move {
            connection.transaction().await.map_err(|error| {
                ZqlzError::Query(format!("Failed to begin Turso transaction: {}", error))
            })
        })
        .await?;
        Ok(Box::new(TursoTransaction {
            transaction: Some(transaction),
        }))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        let connection = self.connection.clone();
        run_on_turso_runtime(async move {
            connection.reset().await;
            Ok(())
        })
        .await?;
        let _keep_database_alive = &self.database;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(TursoCancelHandle {
            connection: self.connection.clone(),
        }))
    }
}

#[async_trait]
impl SchemaIntrospection for TursoConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        Ok(vec![DatabaseInfo {
            name: "main".to_string(),
            owner: None,
            encoding: Some("UTF-8".to_string()),
            size_bytes: None,
            comment: None,
        }])
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "main".to_string(),
            owner: None,
            comment: None,
        }])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let result = self
            .query(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;
        let mut tables = Vec::new();

        for row in &result.rows {
            let name = row.get(0).and_then(Value::as_str).unwrap_or("").to_string();
            let table_type = Self::classify_sqlite_table_type(row.get(1).and_then(Value::as_str));

            tables.push(TableInfo {
                name,
                schema: Some("main".to_string()),
                table_type,
                owner: None,
                row_count: None,
                size_bytes: None,
                comment: None,
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            });
        }

        Ok(tables)
    }

    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let result = self
            .query(
                "SELECT name, sql FROM sqlite_master WHERE type = 'view' ORDER BY name",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| ViewInfo {
                name: row.get(0).and_then(Value::as_str).unwrap_or("").to_string(),
                schema: Some("main".to_string()),
                is_materialized: false,
                definition: row.get(1).and_then(Value::as_str).map(str::to_string),
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        self.schema_engine.get_table(schema, name).await
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        self.schema_engine.get_columns(schema, table).await
    }

    async fn list_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ColumnInfo>>>> {
        self.schema_engine.list_all_columns(schema).await
    }

    async fn list_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ForeignKeyInfo>>>> {
        self.schema_engine.list_all_foreign_keys(schema).await
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        self.schema_engine.get_indexes(schema, table).await
    }

    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        self.schema_engine.get_foreign_keys(schema, table).await
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        self.schema_engine.get_primary_key(schema, table).await
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        self.schema_engine.get_constraints(schema, table).await
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = if let Some(table) = table {
            format!(
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{}' ORDER BY name",
                Self::sqlite_string_literal(table)
            )
        } else {
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
                .to_string()
        };
        let result = self.query(&sql, &[]).await?;

        Ok(result
            .rows
            .iter()
            .map(|row| TriggerInfo {
                name: row.get(0).and_then(Value::as_str).unwrap_or("").to_string(),
                schema: Some("main".to_string()),
                table_name: row.get(1).and_then(Value::as_str).unwrap_or("").to_string(),
                timing: zqlz_core::TriggerTiming::After,
                events: vec![zqlz_core::TriggerEvent::Insert],
                for_each: zqlz_core::TriggerForEach::Row,
                definition: row.get(2).and_then(Value::as_str).map(str::to_string),
                enabled: true,
                comment: None,
            })
            .collect())
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        Ok(Vec::new())
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let sqlite_object_type = object_type_to_sqlite(&object.object_type)?;
        let object_name = if matches!(
            object.object_type,
            ObjectType::Table | ObjectType::View | ObjectType::MaterializedView
        ) {
            self.resolve_catalog_table_name(&object.name).await?
        } else {
            object.name.clone()
        };
        let result = self
            .query(
                "SELECT sql FROM sqlite_master WHERE name = ? AND type = ?",
                &[
                    Value::String(object_name.clone()),
                    Value::String(sqlite_object_type.to_string()),
                ],
            )
            .await?;

        result
            .rows
            .first()
            .and_then(|row| row.get(0).and_then(Value::as_str).map(str::to_string))
            .ok_or_else(|| ZqlzError::NotFound(format!("DDL not found for '{}'", object_name)))
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    async fn list_tables_extended(&self, _schema: Option<&str>) -> Result<ObjectsPanelData> {
        let result = self
            .query(
                "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;
        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(400.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("index_count", "Indexes")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("trigger_count", "Triggers")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
        ];
        let mut rows = Vec::new();

        for row in &result.rows {
            let name = row.get(0).and_then(Value::as_str).unwrap_or("").to_string();
            let object_type = row
                .get(1)
                .and_then(Value::as_str)
                .unwrap_or("table")
                .to_string();
            let table_type = if object_type == "table" {
                Self::classify_sqlite_table_type(row.get(2).and_then(Value::as_str))
            } else {
                TableType::View
            };
            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), name.clone());
            values.insert("row_count".to_string(), "-".to_string());
            values.insert("index_count".to_string(), "-".to_string());
            values.insert("trigger_count".to_string(), "-".to_string());
            if matches!(table_type, TableType::VirtualTable) {
                values.insert("type".to_string(), table_type.display_name().to_string());
            }

            rows.push(ObjectsPanelRow {
                name: name.clone(),
                schema: None,
                object_type: object_type.clone(),
                object_ref: Some(ObjectsPanelObjectRef::new(object_type, name)),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        Ok(ObjectsPanelData { columns, rows })
    }
}

/// Turso transaction wrapper.
pub struct TursoTransaction {
    transaction: Option<libsql::Transaction>,
}

#[async_trait]
impl Transaction for TursoTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| ZqlzError::Query("Transaction already closed".into()))?;
        run_on_turso_runtime(async move {
            transaction.commit().await.map_err(|error| {
                ZqlzError::Query(format!("Failed to commit transaction: {}", error))
            })
        })
        .await
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| ZqlzError::Query("Transaction already closed".into()))?;
        run_on_turso_runtime(async move {
            transaction.rollback().await.map_err(|error| {
                ZqlzError::Query(format!("Failed to rollback transaction: {}", error))
            })
        })
        .await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let transaction = self
            .transaction
            .as_ref()
            .ok_or_else(|| ZqlzError::Query("Transaction already closed".into()))?;
        let connection = std::ops::Deref::deref(transaction).clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        run_on_turso_runtime(async move {
            TursoConnection::query_with_connection(&connection, &sql, &params).await
        })
        .await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let transaction = self
            .transaction
            .as_ref()
            .ok_or_else(|| ZqlzError::Query("Transaction already closed".into()))?;
        let connection = std::ops::Deref::deref(transaction).clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        run_on_turso_runtime(async move {
            TursoConnection::execute_with_connection(&connection, &sql, &params).await
        })
        .await
    }
}


fn object_type_to_sqlite(object_type: &zqlz_core::ObjectType) -> Result<&'static str> {
    match object_type {
        zqlz_core::ObjectType::Table => Ok("table"),
        zqlz_core::ObjectType::View => Ok("view"),
        zqlz_core::ObjectType::Index => Ok("index"),
        zqlz_core::ObjectType::Trigger => Ok("trigger"),
        unsupported => Err(ZqlzError::NotImplemented(format!(
            "Turso DDL generation is not supported for {:?}",
            unsupported
        ))),
    }
}

fn values_to_libsql(values: &[Value]) -> Vec<libsql::Value> {
    values.iter().map(value_to_libsql).collect()
}

fn value_to_libsql(value: &Value) -> libsql::Value {
    match value {
        Value::Null => libsql::Value::Null,
        Value::Bool(value) => libsql::Value::Integer(i64::from(*value)),
        Value::Int8(value) => libsql::Value::Integer(i64::from(*value)),
        Value::Int16(value) => libsql::Value::Integer(i64::from(*value)),
        Value::Int32(value) => libsql::Value::Integer(i64::from(*value)),
        Value::Int64(value) => libsql::Value::Integer(*value),
        Value::Float32(value) => libsql::Value::Real(f64::from(*value)),
        Value::Float64(value) => libsql::Value::Real(*value),
        Value::Decimal(value) => libsql::Value::Text(value.clone()),
        Value::String(value) => libsql::Value::Text(value.clone()),
        Value::Bytes(value) => libsql::Value::Blob(value.clone()),
        Value::Date(value) => libsql::Value::Text(value.to_string()),
        Value::Time(value) => libsql::Value::Text(value.to_string()),
        Value::DateTime(value) => libsql::Value::Text(value.to_string()),
        Value::DateTimeUtc(value) => libsql::Value::Text(value.to_rfc3339()),
        Value::Json(value) => libsql::Value::Text(value.to_string()),
        Value::Uuid(value) => libsql::Value::Text(value.to_string()),
        Value::Array(values) => libsql::Value::Text(
            serde_json::Value::Array(values.iter().map(Value::to_json_value).collect()).to_string(),
        ),
    }
}

fn libsql_row_value_to_zqlz(row: &libsql::Row, index: usize) -> Result<Value> {
    let value = row
        .get_value(i32::try_from(index).map_err(|_| {
            ZqlzError::Query(format!("Column index is too large for libSQL: {}", index))
        })?)
        .map_err(|error| ZqlzError::Query(error.to_string()))?;
    Ok(libsql_value_to_zqlz(value))
}

fn libsql_value_to_zqlz(value: libsql::Value) -> Value {
    match value {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(value) => Value::Int64(value),
        libsql::Value::Real(value) => Value::Float64(value),
        libsql::Value::Text(value) => Value::String(value),
        libsql::Value::Blob(value) => String::from_utf8(value.clone())
            .map(Value::String)
            .unwrap_or(Value::Bytes(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_zqlz_values_to_libsql_values() {
        assert_eq!(value_to_libsql(&Value::Null), libsql::Value::Null);
        assert_eq!(
            value_to_libsql(&Value::Bool(true)),
            libsql::Value::Integer(1)
        );
        assert_eq!(
            value_to_libsql(&Value::Int64(42)),
            libsql::Value::Integer(42)
        );
        assert_eq!(
            value_to_libsql(&Value::Float64(1.5)),
            libsql::Value::Real(1.5)
        );
        assert_eq!(
            value_to_libsql(&Value::String("hello".to_string())),
            libsql::Value::Text("hello".to_string())
        );
        assert_eq!(
            value_to_libsql(&Value::Bytes(vec![1, 2, 3])),
            libsql::Value::Blob(vec![1, 2, 3])
        );
        assert_eq!(
            value_to_libsql(&Value::Json(serde_json::json!({"ok": true}))),
            libsql::Value::Text("{\"ok\":true}".to_string())
        );
    }

    #[test]
    fn converts_libsql_values_to_zqlz_values() {
        assert_eq!(libsql_value_to_zqlz(libsql::Value::Null), Value::Null);
        assert_eq!(
            libsql_value_to_zqlz(libsql::Value::Integer(7)),
            Value::Int64(7)
        );
        assert_eq!(
            libsql_value_to_zqlz(libsql::Value::Real(2.5)),
            Value::Float64(2.5)
        );
        assert_eq!(
            libsql_value_to_zqlz(libsql::Value::Text("text".to_string())),
            Value::String("text".to_string())
        );
        assert_eq!(
            libsql_value_to_zqlz(libsql::Value::Blob(vec![0xff])),
            Value::Bytes(vec![0xff])
        );
        assert_eq!(
            libsql_value_to_zqlz(libsql::Value::Blob(b"text".to_vec())),
            Value::String("text".to_string())
        );
    }
}
