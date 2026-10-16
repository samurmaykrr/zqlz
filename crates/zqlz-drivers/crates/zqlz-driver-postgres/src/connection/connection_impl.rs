//! `Connection` implementation for `PostgresConnection`: query execution,
//! transactions, scope resolution, cell updates, and DDL operations
//! (create/rename/drop table/view/trigger, foreign-key checks, etc.).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, Connection,
    ConnectionScope, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig,
    ExplainParserKind, ForeignKeyChecksSql, ImportIndexCapabilities, ImportSemanticDefault,
    QueryCancelHandle, QueryResult, ResolvedConnectionScope, Result, RowIdentifier,
    SchemaIntrospection, SingleRowDmlScope, SqlObjectName, StatementResult, TableType, Transaction,
    Value, ZqlzError,
};

use crate::PostgresConnection;

use super::row_conversion::{escape_identifier_pg, escape_table_name_pg};
use super::transaction::PostgresTransaction;
use super::value_literal::{row_column_type, value_to_pg_literal_for_type};
use super::{
    PostgresCancelHandle, format_postgres_cell_update_error, format_postgres_error,
    is_postgres_foreign_table_browse_error, is_postgres_missing_user_mapping_error,
    run_postgres_statement, statement_result_from_query_result,
};

#[async_trait]
impl Connection for PostgresConnection {
    fn driver_name(&self) -> &str {
        "postgresql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("postgresql")
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::postgresql()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::PostgreSql
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        escape_identifier_pg(identifier)
    }

    fn requires_database_scoped_connection(&self) -> bool {
        true
    }

    fn connected_database(&self) -> Option<String> {
        Some(self.database.clone()).filter(|database| !database.is_empty())
    }

    fn requires_dedicated_connection_for(&self, database_name: &str) -> bool {
        // The session's own database needs no second connection.
        self.database != database_name.trim()
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = self.current_database_name().await?;
                resolved.effective_namespace = self.current_namespace_name().await?;
            }
            ConnectionScope::Database(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.effective_database = Some(database_name.clone());
                resolved.requires_dedicated_connection =
                    self.requires_dedicated_connection_for(&database_name);
                if resolved.requires_dedicated_connection {
                    resolved.physical_database_key = Some(database_name);
                }
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
        }

        Ok(resolved)
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::DollarNumbered
    }

    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("({})::text", expression_sql)
    }

    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    fn generated_column_storage_keyword(&self, _requested_stored: bool) -> &'static str {
        "STORED"
    }

    fn semantic_default_sql(&self, kind: ImportSemanticDefault) -> Option<String> {
        match kind {
            ImportSemanticDefault::CurrentUser => Some("CURRENT_USER".to_string()),
            ImportSemanticDefault::GeneratedUuid => Some("gen_random_uuid()".to_string()),
        }
    }

    fn supports_partial_indexes(&self) -> bool {
        true
    }

    fn supports_include_indexes(&self) -> bool {
        true
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        true
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: true,
            supports_gin: true,
            supports_gist: true,
            supports_spgist: true,
            supports_brin: true,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: true,
            supports_include: true,
            supports_nulls_ordering: true,
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
        options: DropTableOptions,
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

    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String> {
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
        table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        let table_name = table_name.ok_or_else(|| {
            ZqlzError::NotSupported("PostgreSQL requires a table name for DROP TRIGGER".to_string())
        })?;

        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        sql.push_str(" ON ");
        sql.push_str(&self.render_qualified_name(table_name));
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
            (SELECT sum(calls) FROM pg_stat_statements) as total_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'SELECT%') as select_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'INSERT%') as insert_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'UPDATE%') as update_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'DELETE%') as delete_queries,
            (SELECT avg(mean_exec_time) FROM pg_stat_statements WHERE calls > 0) as avg_time_ms,
            (SELECT max(max_exec_time) FROM pg_stat_statements) as max_time_ms,
            (SELECT
                CASE WHEN (blks_hit + blks_read) = 0 THEN 0
                ELSE blks_hit::float / (blks_hit + blks_read)::float END
             FROM pg_stat_database WHERE datname = current_database()) as cache_hit_ratio,
            (SELECT setting::bigint * 8192 FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers,
            (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()) as blks_hit,
            (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()) as blks_read
        "#
            .to_string(),
        )
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        Some(format!(
            "SELECT setval('{}', {}, true)",
            sequence_name.replace('\'', "''"),
            current_value
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<i64>> {
        let sequence_lookup_sql = "SELECT pg_get_serial_sequence($1, $2)";
        let sequence_lookup = self
            .query(
                sequence_lookup_sql,
                &[
                    Value::String(table_name.to_string()),
                    Value::String(column_name.to_string()),
                ],
            )
            .await?;

        let Some(sequence_name) = sequence_lookup
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_str())
            .filter(|name| !name.is_empty())
            .map(ToString::to_string)
        else {
            return Ok(None);
        };

        let sequence_sql = format!(
            "SELECT last_value, is_called FROM {}",
            escape_table_name_pg(&sequence_name)
        );
        let sequence_result = match self.query(&sequence_sql, &[]).await {
            Ok(result) => result,
            Err(error) => {
                tracing::debug!(
                    sequence_name = %sequence_name,
                    error = %error,
                    "Could not read sequence current value; skipping sequence export"
                );
                return Ok(None);
            }
        };

        let Some(row) = sequence_result.rows.first() else {
            return Ok(None);
        };

        let last_value = row.values.first().and_then(|value| value.as_i64());
        let is_called = row
            .values
            .get(1)
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        Ok(if is_called { last_value } else { None })
    }

    async fn export_named_enum_definitions(&self) -> Result<Vec<(String, Vec<String>)>> {
        let sql = "SELECT t.typname, e.enumlabel \
                   FROM pg_type t \
                   JOIN pg_enum e ON t.oid = e.enumtypid \
                   JOIN pg_namespace n ON t.typnamespace = n.oid \
                   WHERE n.nspname = current_schema() \
                   ORDER BY t.typname, e.enumsortorder";
        let result = self.query(sql, &[]).await?;

        let mut enum_values: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for row in result.rows {
            let Some(type_name) = row
                .values
                .first()
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };
            let Some(label) = row
                .values
                .get(1)
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };

            if !enum_values.contains_key(&type_name) {
                order.push(type_name.clone());
            }
            enum_values.entry(type_name).or_default().push(label);
        }

        Ok(order
            .into_iter()
            .map(|name| {
                let values = enum_values.remove(&name).unwrap_or_default();
                (name, values)
            })
            .collect())
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        let result = self.query("SELECT current_schema()", &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_str())
            .map(ToString::to_string))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        let result = self.query("SELECT current_database()", &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_str())
            .map(ToString::to_string))
    }

    fn supports_top_level_triggers(&self) -> bool {
        false
    }

    fn supports_materialized_views(&self) -> bool {
        true
    }

    fn supports_import_named_enum_types(&self) -> bool {
        true
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Enforced
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        None
    }

    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    fn should_use_schema_only_table_browse_fallback(
        &self,
        table_type: TableType,
        error_message: &str,
    ) -> bool {
        is_postgres_missing_user_mapping_error(error_message)
            || (table_type == TableType::ForeignTable
                && is_postgres_foreign_table_browse_error(error_message))
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        let result = match table_name.namespace.as_deref() {
            Some(namespace) => {
                self.query(
                    "SELECT CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'm') \
                     LIMIT 1",
                    &[
                        Value::String(namespace.to_string()),
                        Value::String(table_name.name.clone()),
                    ],
                )
                .await?
            }
            None => {
                self.query(
                    "SELECT CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() AND c.relname = $1 AND c.relkind IN ('r', 'p', 'm') \
                     LIMIT 1",
                    &[Value::String(table_name.name.clone())],
                )
                .await?
            }
        };

        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let client = self.client.lock().await;
        let result = run_postgres_statement(
            &client,
            &self.notices,
            &self.money_format,
            sql,
            params,
            "statement",
        )
        .await?;
        tracing::debug!(affected_rows = result.affected_rows, "statement executed");
        Ok(statement_result_from_query_result(result))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let client = self.client.lock().await;
        run_postgres_statement(
            &client,
            &self.notices,
            &self.money_format,
            sql,
            params,
            "query",
        )
        .await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        tracing::debug!("beginning PostgreSQL transaction");

        let client = self.client.lock().await;
        client.execute("BEGIN", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to begin transaction: {}", message))
        })?;

        // Release the lock and return the transaction
        drop(client);

        tracing::debug!("PostgreSQL transaction begun successfully");
        Ok(Box::new(PostgresTransaction {
            client: Arc::clone(&self.client),
            notices: Arc::clone(&self.notices),
            money_format: Arc::clone(&self.money_format),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing PostgreSQL connection");
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn is_busy(&self) -> bool {
        self.client.try_lock().is_err()
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(PostgresCancelHandle {
            cancel_token: self.cancel_token.clone(),
        }))
    }

    /// Override update_cell to use SQL literals instead of parameters
    /// since parameterized queries aren't fully implemented yet
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value (PostgreSQL)"
        );

        // Escape table name (may include schema.table format)
        let table_identifier = escape_table_name_pg(&request.table_name);

        // Build WHERE clause with literal values
        let where_clause = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported. Use primary key or full row identifier.".to_string()
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => pk_values
                .iter()
                .map(|(col, val)| {
                    format!(
                        "{} = {}",
                        escape_identifier_pg(col),
                        value_to_pg_literal_for_type(
                            val,
                            row_column_type(&request.row_column_types, col)
                        )
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            RowIdentifier::FullRow(row_values) => row_values
                .iter()
                .map(|(col, val)| {
                    if val == &Value::Null {
                        format!("{} IS NULL", escape_identifier_pg(col))
                    } else {
                        format!(
                            "{} = {}",
                            escape_identifier_pg(col),
                            value_to_pg_literal_for_type(
                                val,
                                row_column_type(&request.row_column_types, col)
                            )
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND "),
        };

        // Build UPDATE statement with literal value
        let set_value = match &request.new_value {
            Some(val) => value_to_pg_literal_for_type(val, request.column_type.as_deref()),
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
            escape_identifier_pg(&request.column_name),
            set_value,
            where_clause,
            statement_suffix
        );

        tracing::debug!("PostgreSQL update SQL: {}", sql);

        let client = self.client.lock().await;
        let rows_affected = client.execute(&sql, &[]).await.map_err(|e| {
            tracing::error!("PostgreSQL cell update error: {:?}", e);
            let message = format_postgres_cell_update_error(&e, &request);
            tracing::error!("Error details: {}", message);
            if let Some(db_error) = e.as_db_error() {
                tracing::error!(
                    "Database error details - Code: {:?}, Message: {}, Detail: {:?}, Hint: {:?}",
                    db_error.code(),
                    db_error.message(),
                    db_error.detail(),
                    db_error.hint()
                );
            }
            ZqlzError::Query(format!("Failed to update cell: {}", message))
        })?;

        tracing::debug!(affected_rows = rows_affected, "cell update completed");
        Ok(rows_affected)
    }

    /// `ctid` is the physical location of a row, which identifies it even in a
    /// table with no key and duplicate rows.
    fn single_row_dml_scope(&self, qualified_table: &str, where_clause: &str) -> SingleRowDmlScope {
        SingleRowDmlScope::by_row_identity("ctid", qualified_table, where_clause)
    }
}
