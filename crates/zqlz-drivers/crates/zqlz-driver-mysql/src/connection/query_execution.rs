//! The `Connection` trait implementation (query execution, DDL/DML,
//! import/export, cell updates) and the `Transaction`/`MySqlTransaction`
//! implementation for `MySqlConnection`.
use super::*;

#[async_trait]
impl Connection for MySqlConnection {
    fn driver_name(&self) -> &str {
        "mysql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mysql")
    }

    fn table_sql_name(&self, table: &zqlz_core::TableRef) -> zqlz_core::SqlObjectName {
        // The database is the namespace; a ref from a database node carries it
        // as `database` alone.
        zqlz_core::SqlObjectName {
            namespace: table.schema.clone().or_else(|| table.database.clone()),
            name: table.name.clone(),
        }
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        let is_database = matches!(scope, ConnectionScope::Database(_));
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
                // User SQL runs on this connection's one session, which sits in
                // the database it connected to; SQL for another database needs a
                // connection opened on it (an unknown name then fails to connect).
                if is_database && self.requires_dedicated_connection_for(&database_name) {
                    resolved.physical_database_key = Some(database_name.clone());
                    resolved.requires_dedicated_connection = true;
                }
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

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::mysql()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::MySql
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        escape_identifier_mysql(identifier)
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    fn limited_select_sql(&self, base_sql: &str, limit: u64) -> String {
        format!("{} LIMIT {}", base_sql, limit)
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("CAST({} AS CHAR)", expression_sql)
    }

    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        strip_pg_casts(expression_sql)
    }

    fn semantic_default_sql(&self, kind: ImportSemanticDefault) -> Option<String> {
        match kind {
            ImportSemanticDefault::CurrentUser => Some("CURRENT_USER".to_string()),
            ImportSemanticDefault::GeneratedUuid => Some("(UUID())".to_string()),
        }
    }

    fn supports_partial_indexes(&self) -> bool {
        false
    }

    fn supports_include_indexes(&self) -> bool {
        false
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        false
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: true,
            supports_spatial: true,
            supports_partial: false,
            supports_include: false,
            supports_nulls_ordering: false,
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        let new_name = match table_name.namespace.as_deref() {
            Some(namespace) => format!(
                "{}.{}",
                self.quote_identifier(namespace),
                self.quote_identifier(new_table_name)
            ),
            None => self.quote_identifier(new_table_name),
        };

        Ok(format!(
            "RENAME TABLE {} TO {}",
            self.render_qualified_name(table_name),
            new_name
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP TABLE ... CASCADE".to_string(),
            ));
        }
        Ok(sql)
    }

    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP VIEW ... CASCADE".to_string(),
            ));
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP TRIGGER ... CASCADE".to_string(),
            ));
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
        self.truncate_table_sql(table_name)
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
        Ok(
            r#"
        SELECT
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Questions') as total_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_select') as select_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_insert') as insert_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_update') as update_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_delete') as delete_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_bytes_data') as buffer_pool_pages_data,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests') as buffer_pool_read_requests,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads') as buffer_pool_reads,
            (SELECT @@innodb_buffer_pool_size) as buffer_pool_size
        "#
            .to_string(),
        )
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        let table_name = sequence_name
            .split_once('.')
            .map(|(table, _)| table)
            .unwrap_or(sequence_name);
        Some(format!(
            "ALTER TABLE {} AUTO_INCREMENT = {}",
            self.quote_identifier(table_name),
            current_value + 1
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        let result = self
            .session
            .run(
                "SELECT AUTO_INCREMENT
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                &[Value::String(table_name.to_string())],
            )
            .await?;
        let Some(next_value) = result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_i64())
        else {
            return Ok(None);
        };

        Ok(Some(next_value - 1).filter(|value| *value > 0))
    }

    fn normalize_create_view_sql(&self, sql: &str) -> String {
        let trimmed = sql.trim();
        if trimmed
            .to_ascii_uppercase()
            .starts_with("CREATE OR REPLACE VIEW")
        {
            trimmed.to_string()
        } else if trimmed.to_ascii_uppercase().starts_with("CREATE VIEW") {
            trimmed.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
        } else {
            trimmed.to_string()
        }
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        // The session's database, since `USE` changes it. Never waits for a
        // running user statement: this feeds every scope resolution, and a long
        // `SELECT SLEEP(...)` must not freeze the sidebar. While the session is
        // busy (or unreachable) the connect-time database is the best answer.
        let current = match self.session.run_if_idle("SELECT DATABASE()", &[]).await {
            Ok(Some(result)) => result
                .rows
                .first()
                .and_then(|row| row.get(0))
                .and_then(|value| value.as_str())
                .map(ToString::to_string),
            Ok(None) | Err(_) => None,
        };
        Ok(current.or_else(|| self.default_database().map(ToString::to_string)))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        self.resolve_session_namespace().await
    }

    fn connected_database(&self) -> Option<String> {
        self.default_database().map(ToString::to_string)
    }

    fn requires_dedicated_connection_for(&self, database_name: &str) -> bool {
        // ponytail: exact match; a case variant of the connect-time database on a
        // case-insensitive server just gets a redundant connection.
        self.default_database() != Some(database_name.trim())
    }

    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Unknown
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        Some(ForeignKeyChecksSql {
            disable_sql: "SET FOREIGN_KEY_CHECKS=0".to_string(),
            enable_sql: "SET FOREIGN_KEY_CHECKS=1".to_string(),
        })
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        let schema = table_name
            .namespace
            .clone()
            .or_else(|| self.default_database().map(ToString::to_string));

        let Some(schema_name) = schema else {
            return Ok(None);
        };

        let result = self
            .query(
                "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1",
                &[
                    Value::String(schema_name),
                    Value::String(table_name.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let result = self.session.run(sql, params).await?;
        tracing::debug!(affected_rows = result.affected_rows, "statement executed");
        Ok(statement_result_from_query_result(result))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.session.run(sql, params).await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        tracing::debug!("beginning MySQL transaction");

        // The transaction lives on the session, like a user-typed START
        // TRANSACTION: later calls on this connection join it until it ends.
        self.session
            .run("START TRANSACTION", &[])
            .await
            .map_err(|e| match e {
                ZqlzError::Other(message) => {
                    ZqlzError::Query(format!("Failed to begin transaction: {message}"))
                }
                other => other,
            })?;

        tracing::debug!("MySQL transaction begun successfully");
        Ok(Box::new(MySqlTransaction {
            session: self.session.clone(),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing MySQL connection pool");
        self.session.close().await;
        let pool = self.pool.clone();
        get_mysql_runtime()
            .spawn(async move { pool.disconnect().await })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL close task failed: {}", e)))?
            .map_err(|e| {
                ZqlzError::Connection(format!("Failed to close MySQL connection: {}", e))
            })?;
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn is_busy(&self) -> bool {
        self.in_flight.is_busy()
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    // schema_engine drives delegated introspection; see src/schema.rs.

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(self.session.cancel_handle()))
    }

    /// Override update_cell to use SQL literals instead of parameters
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value (MySQL)"
        );

        // Escape table name (may include schema.table format)
        let table_identifier = escape_table_name_mysql(&request.table_name);

        // Build WHERE clause with literal values
        let where_clause = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported. Use primary key or full row identifier."
                        .to_string(),
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => pk_values
                .iter()
                .map(|(col, val)| {
                    Ok(format!(
                        "{} = {}",
                        escape_identifier_mysql(col),
                        value_to_mysql_literal(val)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND "),
            RowIdentifier::FullRow(row_values) => row_values
                .iter()
                .map(|(col, val)| {
                    if val == &Value::Null {
                        Ok(format!("{} IS NULL", escape_identifier_mysql(col)))
                    } else {
                        Ok(format!(
                            "{} = {}",
                            escape_identifier_mysql(col),
                            value_to_mysql_literal(val)?
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND "),
        };

        let set_value = match &request.new_value {
            Some(val) => value_to_mysql_literal_for_type(val, request.column_type.as_deref())?,
            None => "NULL".to_string(),
        };

        let scope = match &request.row_identifier {
            RowIdentifier::FullRow(_) => {
                self.single_row_dml_scope(&table_identifier, &where_clause)
            }
            _ => SingleRowDmlScope::Unsupported,
        };
        let (where_clause, statement_suffix) = scope.apply(&where_clause);

        let sql = format!(
            "UPDATE {} SET {} = {} WHERE {}{}",
            table_identifier,
            escape_identifier_mysql(&request.column_name),
            set_value,
            where_clause,
            statement_suffix
        );

        tracing::debug!("MySQL update SQL: {}", sql);

        // On the session, so an edit made inside the user's open transaction
        // sees and joins it instead of waiting on its row locks.
        let rows_affected = self.session.run(&sql, &[]).await?.affected_rows;

        tracing::debug!(affected_rows = rows_affected, "cell update completed");
        Ok(rows_affected)
    }

    /// InnoDB's internal row id is not selectable, but MySQL accepts a row
    /// limit directly on UPDATE and DELETE.
    fn single_row_dml_scope(
        &self,
        _qualified_table: &str,
        _where_clause: &str,
    ) -> SingleRowDmlScope {
        SingleRowDmlScope::StatementSuffix(" LIMIT 1")
    }
}

/// Escape a MySQL identifier (column name, etc.)
fn escape_identifier_mysql(identifier: &str) -> String {
    format!("`{}`", identifier.replace("`", "``"))
}

/// Escape a table name which may include schema (e.g., "schema.table")
fn escape_table_name_mysql(table_name: &str) -> String {
    if table_name.contains('.') {
        // Handle schema.table format
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            format!(
                "{}.{}",
                escape_identifier_mysql(parts[0]),
                escape_identifier_mysql(parts[1])
            )
        } else {
            escape_identifier_mysql(table_name)
        }
    } else {
        escape_identifier_mysql(table_name)
    }
}

/// MySQL transaction implementation
///
/// A transaction is `START TRANSACTION` on the connection's pinned session; it
/// holds no lock of its own, so (as with the PostgreSQL driver) other calls on
/// the same connection while it is open run inside it.
pub struct MySqlTransaction {
    session: Arc<Session>,
    committed: bool,
    rolled_back: bool,
}

impl MySqlTransaction {
    fn ensure_open(&self, action: &str) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query(format!(
                "Cannot {action} on committed transaction"
            )));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query(format!(
                "Cannot {action} on rolled back transaction"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Transaction for MySqlTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("committing MySQL transaction");
        self.session.run("COMMIT", &[]).await?;
        self.committed = true;
        tracing::debug!("MySQL transaction committed");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("rolling back MySQL transaction");
        self.session.run("ROLLBACK", &[]).await?;
        self.rolled_back = true;
        tracing::debug!("MySQL transaction rolled back");
        Ok(())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_open("execute")?;
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in transaction");

        let result = self.session.run(sql, params).await?;
        Ok(statement_result_from_query_result(result))
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_open("query")?;
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in transaction");

        self.session.run(sql, params).await
    }
}

impl Drop for MySqlTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            tracing::warn!(
                "MySQL transaction dropped without commit or rollback - will auto-rollback"
            );
            let session = self.session.clone();
            get_mysql_runtime().spawn(async move {
                if let Err(e) = session.run("ROLLBACK", &[]).await {
                    tracing::error!("Failed to rollback dropped transaction: {}", e);
                }
            });
        }
    }
}

/// Shape a statement outcome for `Connection::execute`: a result set comes back
/// as rows (with `affected_rows` = the rows it produced); a plain command
/// reports only `affected_rows`, plus an empty result carrying warnings if the
/// server raised any. Mirrors the PostgreSQL driver.
fn statement_result_from_query_result(result: QueryResult) -> StatementResult {
    if !result.columns.is_empty() {
        return StatementResult {
            is_query: true,
            affected_rows: result.rows.len() as u64,
            result: Some(result),
            error: None,
        };
    }

    StatementResult {
        is_query: false,
        affected_rows: result.affected_rows,
        result: (!result.warnings.is_empty()).then_some(result),
        error: None,
    }
}
