use super::*;

#[async_trait]
impl Connection for MongoDbConnection {
    fn driver_name(&self) -> &str {
        "mongodb"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mongodb")
    }

    fn driver_category(&self) -> DriverCategory {
        DriverCategory::Document
    }

    fn requires_database_scoped_connection(&self) -> bool {
        true
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = Some(self.database.clone());
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.physical_database_key = Some(database_name.clone());
                resolved.effective_database = Some(database_name);
                resolved.requires_dedicated_connection = true;
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some(self.database.clone()))
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::Raw
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig {
            explain_format: Cow::Borrowed("{sql}.explain()"),
            query_plan_format: None,
            analyze_format: Some(Cow::Borrowed("{sql}.explain(\"executionStats\")")),
            explain_description: Cow::Borrowed("MongoDB queryPlanner explain"),
            query_plan_description: None,
            analyze_is_safe: true,
        }
    }

    fn rename_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _new_table_name: &str,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table rename operations".to_string(),
        ))
    }

    fn drop_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _options: DropTableOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP TABLE operations".to_string(),
        ))
    }

    fn drop_view_sql(
        &self,
        _view_name: &SqlObjectName,
        _options: DropViewOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP VIEW operations".to_string(),
        ))
    }

    fn drop_trigger_sql(
        &self,
        _trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        _options: DropTriggerOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL DROP TRIGGER operations".to_string(),
        ))
    }

    fn truncate_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL TRUNCATE TABLE operations".to_string(),
        ))
    }

    fn duplicate_table_sql(
        &self,
        _source_table_name: &SqlObjectName,
        _new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table duplication operations".to_string(),
        ))
    }

    fn clear_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table clear operations".to_string(),
        ))
    }

    fn table_has_rows_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL table row-existence queries".to_string(),
        ))
    }

    fn select_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL row-selection queries".to_string(),
        ))
    }

    fn select_distinct_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
        _order_by_columns: &[String],
        _limit: u64,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL distinct-row queries".to_string(),
        ))
    }

    fn insert_row_sql(
        &self,
        _table_name: &SqlObjectName,
        _column_names: &[String],
        _value_count: usize,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL INSERT-row statement generation".to_string(),
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "MongoDB does not support SQL performance metrics queries".to_string(),
        ))
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;

        let start = Instant::now();
        let command = self.parse_query(sql)?;
        let result = self.run_command(command).await?;
        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Check for errors in the response
        if let Some(ok) = result.get("ok")
            && ok.as_f64().unwrap_or(0.0) != 1.0
        {
            let err_msg = result
                .get("errmsg")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown error");
            return Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 0,
                error: Some(format!("MongoDB error: {}", err_msg)),
            });
        }

        // Extract affected count if present
        let affected_rows = result.get("n").and_then(|n| n.as_i64()).unwrap_or(0) as u64;

        // For execute, we return a simple result without query data
        Ok(StatementResult {
            is_query: false,
            result: Some(QueryResult {
                id: Uuid::new_v4(),
                columns: Vec::new(),
                rows: Vec::new(),
                total_rows: Some(affected_rows),
                is_estimated_total: false,
                affected_rows,
                execution_time_ms,
                warnings: Vec::new(),
            }),
            affected_rows,
            error: None,
        })
    }

    async fn query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;

        let start = Instant::now();
        let command = self.parse_query(sql)?;
        let result = self.run_command(command).await?;
        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Check for errors
        if let Some(ok) = result.get("ok")
            && ok.as_f64().unwrap_or(0.0) != 1.0
        {
            let err_msg = result
                .get("errmsg")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown error");
            return Err(ZqlzError::Driver(format!("MongoDB error: {}", err_msg)));
        }

        // Convert result document to rows
        let mut rows = Vec::new();
        let mut columns = Vec::new();

        // Handle different result types
        if let Some(cursor) = result.get("cursor")
            && let Some(first_batch) = cursor.as_document().and_then(|d| d.get("firstBatch"))
            && let Some(arr) = first_batch.as_array()
        {
            // Cursor-based result (find, aggregate)
            for (i, doc) in arr.iter().enumerate() {
                if let Some(d) = doc.as_document() {
                    if i == 0 {
                        columns = d
                            .keys()
                            .enumerate()
                            .map(|(ordinal, k)| ColumnMeta {
                                name: k.clone(),
                                data_type: "bson".to_string(),
                                nullable: true,
                                ordinal,
                                max_length: None,
                                precision: None,
                                scale: None,
                                auto_increment: false,
                                default_value: None,
                                comment: None,
                                enum_values: None,
                            })
                            .collect();
                    }
                    let column_names: Vec<String> = d.keys().cloned().collect();
                    let values: Vec<Value> =
                        d.iter().map(|(_, v)| Self::bson_to_value(v)).collect();
                    rows.push(Row::new(column_names, values));
                }
            }
        } else {
            // Single document result
            columns = result
                .keys()
                .enumerate()
                .map(|(ordinal, k)| ColumnMeta {
                    name: k.clone(),
                    data_type: "bson".to_string(),
                    nullable: true,
                    ordinal,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: None,
                    enum_values: None,
                })
                .collect();
            let column_names: Vec<String> = result.keys().cloned().collect();
            let values: Vec<Value> = result.iter().map(|(_, v)| Self::bson_to_value(v)).collect();
            rows.push(Row::new(column_names, values));
        }

        let row_count = rows.len() as u64;

        Ok(QueryResult {
            id: Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(row_count),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Err(ZqlzError::NotImplemented(
            "MongoDB transactions require session management - use start_session() instead"
                .to_string(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn ping_query_sql(&self) -> &'static str {
        MONGODB_PING_COMMAND
    }

    fn as_document_store(&self) -> Option<&dyn DocumentStore> {
        Some(self)
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }
}
