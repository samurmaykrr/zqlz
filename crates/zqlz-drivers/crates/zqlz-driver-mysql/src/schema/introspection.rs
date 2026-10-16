//! The `SchemaIntrospection` trait implementation: structured listing
//! (tables/views/functions/.../sequences), object forms, DDL generation, and
//! dependency lookup for `MySqlConnection`.
use super::*;

#[async_trait]
impl SchemaIntrospection for MySqlConnection {
    fn object_kind_support(&self) -> Option<zqlz_core::ObjectKindSupport> {
        // `sequences` is MariaDB-only; refine it once the server flavor is
        // known. Before the first async call caches it, fall back to the
        // static driver-level answer (plain MySQL, no sequences).
        let mut kinds = crate::driver::OBJECT_KINDS;
        if let Some(features) = self.cached_server_features() {
            kinds.sequences = features.supports_sequences();
        }
        Some(kinds)
    }

    fn default_schema_name(&self) -> Option<String> {
        None
    }

    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT
                    s.SCHEMA_NAME,
                    s.DEFAULT_CHARACTER_SET_NAME,
                    s.DEFAULT_COLLATION_NAME,
                    CAST(COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH), 0) AS SIGNED) AS SIZE_BYTES
                 FROM information_schema.SCHEMATA s
                 LEFT JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = s.SCHEMA_NAME
                 GROUP BY s.SCHEMA_NAME, s.DEFAULT_CHARACTER_SET_NAME, s.DEFAULT_COLLATION_NAME
                 ORDER BY s.SCHEMA_NAME",
                &[],
            )
            .await?;

        let databases = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let charset = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let collation = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                Some(DatabaseInfo {
                    name: name.clone(),
                    owner: None,
                    encoding: Some(charset.to_string()),
                    size_bytes: row.get(3).and_then(|value| value.as_i64()),
                    comment: Some(if mysql_is_system_schema(&name) {
                        format!("System database. Default collation: {collation}")
                    } else {
                        format!("Default collation: {collation}")
                    }),
                })
            })
            .collect();

        Ok(databases)
    }

    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        // MySQL doesn't have schemas in the PostgreSQL sense - databases are the equivalent
        // Return an empty list since list_databases covers this
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH as SIZE_BYTES, TABLE_COMMENT
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VIEW')
                 ORDER BY TABLE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH as SIZE_BYTES, TABLE_COMMENT
              FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VIEW')
             ORDER BY TABLE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_type_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("BASE TABLE");
                let row_count = row.get(2).and_then(|v| v.as_i64());
                let size_bytes = row.get(3).and_then(|v| v.as_i64());
                let comment = row
                    .get(4)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty());

                let table_type = match table_type_str {
                    "BASE TABLE" => TableType::Table,
                    "VIEW" => TableType::View,
                    "SYSTEM VIEW" => TableType::System,
                    _ => TableType::Table,
                };

                TableInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    table_type,
                    owner: None,
                    row_count,
                    size_bytes,
                    comment,
                    index_count: None,
                    trigger_count: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(tables)
    }

    #[tracing::instrument(skip(self))]
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_NAME, VIEW_DEFINITION
                 FROM information_schema.VIEWS
                 WHERE TABLE_SCHEMA = ?
                 ORDER BY TABLE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT TABLE_NAME, VIEW_DEFINITION
             FROM information_schema.VIEWS
             WHERE TABLE_SCHEMA = DATABASE()
             ORDER BY TABLE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        let views = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                ViewInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    is_materialized: false, // MySQL doesn't have materialized views
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
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

    async fn list_all_indexes(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<IndexInfo>>>> {
        let schema = schema.or(self.default_database());
        // One STATISTICS scan for the whole schema, parsed by the same code as
        // the per-table `fetch_indexes` so bulk and single results match.
        let sql = catalog::mysql_indexes_sql(
            schema.map(|_| "= ?").unwrap_or("= DATABASE()"),
            false,
        );
        let params: Vec<Value> = schema
            .map(|schema_name| vec![Value::String(schema_name.to_string())])
            .unwrap_or_default();
        let result = self.query(&sql, &params).await?;

        let mut indexes_by_table: HashMap<String, Vec<IndexInfo>> = HashMap::new();
        for entry in catalog::mysql_index_entries(&result) {
            let index = entry.index;
            indexes_by_table
                .entry(entry.table)
                .or_default()
                .push(IndexInfo {
                    name: index.name,
                    columns: index.columns,
                    is_unique: index.is_unique,
                    is_primary: index.is_primary,
                    index_type: index.method.unwrap_or_else(|| "btree".to_string()),
                    comment: index.comment,
                    where_clause: None,
                    include_columns: index.include_columns,
                    column_descending: index.column_descending,
                });
        }

        Ok(Some(indexes_by_table))
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

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION, ROUTINE_COMMENT
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'FUNCTION'
                 ORDER BY ROUTINE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION, ROUTINE_COMMENT
             FROM information_schema.ROUTINES
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION'
             ORDER BY ROUTINE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        // One batched query for the whole schema, never one per routine.
        let mut parameters_by_routine = self
            .mysql_routine_parameters_by_routine(schema)
            .await
            .unwrap_or_default();
        let mut functions = Vec::new();
        for row in &result.rows {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let return_type = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());
            let comment = row
                .get(3)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty());

            let parameters = parameters_by_routine
                .remove(&(name.clone(), "FUNCTION".to_string()))
                .unwrap_or_default();
            functions.push(FunctionInfo {
                name,
                schema: schema.map(|s| s.to_string()),
                language: "SQL".to_string(),
                return_type,
                parameters,
                definition,
                owner: None,
                comment,
            });
        }

        Ok(functions)
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'PROCEDURE'
                 ORDER BY ROUTINE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT
             FROM information_schema.ROUTINES
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE'
             ORDER BY ROUTINE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        let mut parameters_by_routine = self
            .mysql_routine_parameters_by_routine(schema)
            .await
            .unwrap_or_default();
        let mut procedures = Vec::new();
        for row in &result.rows {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
            let comment = row
                .get(2)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty());

            let parameters = parameters_by_routine
                .remove(&(name.clone(), "PROCEDURE".to_string()))
                .unwrap_or_default();
            procedures.push(ProcedureInfo {
                name,
                schema: schema.map(|s| s.to_string()),
                language: "SQL".to_string(),
                parameters,
                definition,
                owner: None,
                comment,
            });
        }

        Ok(procedures)
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            if let Some(table_name) = table {
                (
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                     FROM information_schema.TRIGGERS
                     WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
                     ORDER BY TRIGGER_NAME",
                    vec![
                        Value::String(schema_name.to_string()),
                        Value::String(table_name.to_string()),
                    ],
                )
            } else {
                (
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                     FROM information_schema.TRIGGERS
                     WHERE TRIGGER_SCHEMA = ?
                     ORDER BY TRIGGER_NAME",
                    vec![Value::String(schema_name.to_string())],
                )
            }
        } else if let Some(table_name) = table {
            (
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                 FROM information_schema.TRIGGERS
                 WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
                 ORDER BY TRIGGER_NAME",
                vec![Value::String(table_name.to_string())],
            )
        } else {
            (
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
             FROM information_schema.TRIGGERS
             WHERE TRIGGER_SCHEMA = DATABASE()
             ORDER BY TRIGGER_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        let triggers = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let timing_str = row.get(2).and_then(|v| v.as_str()).unwrap_or("BEFORE");
                let event_str = row.get(3).and_then(|v| v.as_str()).unwrap_or("INSERT");
                let definition = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());

                let timing = match timing_str {
                    "BEFORE" => TriggerTiming::Before,
                    "AFTER" => TriggerTiming::After,
                    _ => TriggerTiming::Before,
                };

                let event = match event_str {
                    "INSERT" => TriggerEvent::Insert,
                    "UPDATE" => TriggerEvent::Update,
                    "DELETE" => TriggerEvent::Delete,
                    _ => TriggerEvent::Insert,
                };

                TriggerInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    table_name,
                    timing,
                    events: vec![event],
                    for_each: TriggerForEach::Row,
                    definition,
                    enabled: true,
                    comment: None,
                }
            })
            .collect();

        Ok(triggers)
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        // Plain MySQL has no sequences in the PostgreSQL sense; AUTO_INCREMENT
        // is the equivalent there. MariaDB does support `CREATE SEQUENCE`.
        let Some(result) = self.mysql_sequence_query_rows(schema).await? else {
            return Ok(Vec::new());
        };
        Ok(result
            .rows
            .iter()
            .map(|row| SequenceInfo {
                schema: row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                name: row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string(),
                data_type: row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("bigint")
                    .to_string(),
                start_value: row.get(3).and_then(|value| value.as_i64()).unwrap_or(1),
                min_value: row.get(4).and_then(|value| value.as_i64()).unwrap_or(1),
                max_value: row
                    .get(5)
                    .and_then(|value| value.as_i64())
                    .unwrap_or(i64::MAX),
                increment_by: row.get(6).and_then(|value| value.as_i64()).unwrap_or(1),
                current_value: None,
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        // MySQL doesn't have custom types like PostgreSQL
        Ok(Vec::new())
    }

    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let schema = schema.or(self.default_database());
        let qualify_with_schema = schema.is_none();
        let ObjectsPanelData { columns, mut rows } = self.mysql_relation_panel_data(schema).await?;

        let functions = self
            .mysql_routine_panel_rows(schema, "FUNCTION", qualify_with_schema)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "Failed to load MySQL functions for objects panel rows");
                Vec::new()
            });
        let procedures = self
            .mysql_routine_panel_rows(schema, "PROCEDURE", qualify_with_schema)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "Failed to load MySQL procedures for objects panel rows");
                Vec::new()
            });
        let triggers = self
            .mysql_trigger_panel_rows(schema, None, qualify_with_schema)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "Failed to load MySQL triggers for objects panel rows");
                Vec::new()
            });

        rows.extend(functions);
        rows.extend(procedures);
        rows.extend(triggers);
        rows.extend(
            self.mysql_event_objects_panel_data(schema)
                .await
                .map(|data| data.rows)
                .unwrap_or_else(|error| {
                    tracing::warn!(%error, "Failed to load MySQL events for objects panel rows");
                    Vec::new()
                }),
        );

        Ok(ObjectsPanelData { columns, rows })
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        let schema = schema.or(self.default_database());
        let qualify_with_schema = schema.is_none();

        match kind_id {
            "database" => self.mysql_database_objects_panel_data().await,
            "table" | "view" => Ok(self
                .mysql_relation_panel_data(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            "function" => {
                let rows = self
                    .mysql_routine_panel_rows(schema, "FUNCTION", qualify_with_schema)
                    .await?;
                Ok(ObjectsPanelData {
                    columns: mysql_function_columns(),
                    rows,
                })
            }
            "procedure" => {
                let rows = self
                    .mysql_routine_panel_rows(schema, "PROCEDURE", qualify_with_schema)
                    .await?;
                Ok(ObjectsPanelData {
                    columns: mysql_procedure_columns(),
                    rows,
                })
            }
            "trigger" => {
                let rows = self
                    .mysql_trigger_panel_rows(schema, None, qualify_with_schema)
                    .await?;
                Ok(ObjectsPanelData {
                    columns: mysql_trigger_columns(),
                    rows,
                })
            }
            "event" => self.mysql_event_objects_panel_data(schema).await,
            "index" | "constraint" | "foreign_key" | "partition" | "sequence" | "tablespace"
            | "engine" | "charset" | "collation" | "user" | "role" | "grant" | "plugin"
            | "variable" | "status_variable" | "process" | "replica_status" => {
                self.mysql_metadata_objects_panel_data(schema, kind_id)
                    .await
            }
            _ => Ok(ObjectsPanelData {
                columns: mysql_metadata_columns(),
                rows: Vec::new(),
            }),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        let features = self.mysql_server_features().await?;
        Ok(mysql_objects_panel_manifest(&features))
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        let object_ref = request.object_ref.as_ref();
        let schema = object_ref
            .and_then(|object_ref| object_ref.schema.clone())
            .or_else(|| self.default_database().map(str::to_string))
            .unwrap_or_default();
        let name = object_ref
            .map(|object_ref| object_ref.name.clone())
            .unwrap_or_default();
        let associated_table = object_ref
            .and_then(|object_ref| object_ref.signature.clone())
            .unwrap_or_default();

        match (request.kind_id.as_str(), request.mode) {
            ("database", ObjectFormMode::Create | ObjectFormMode::Drop) => Ok(Some(
                ObjectFormSpec::new("database", request.mode, "Database").sections(vec![
                    ObjectFormSection::new(vec![
                        ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                            .required()
                            .default_value(ObjectFormValue::String(name)),
                        ObjectFormField::new("charset", "Character Set", ObjectFormFieldKind::Text)
                            .placeholder("utf8mb4"),
                        ObjectFormField::new("collation", "Collation", ObjectFormFieldKind::Text)
                            .placeholder("utf8mb4_0900_ai_ci"),
                    ]),
                ]),
            )),
            ("function", ObjectFormMode::Create | ObjectFormMode::Drop) => Ok(Some(
                ObjectFormSpec::new("function", request.mode, "Function").sections(vec![
                    ObjectFormSection::new(vec![
                        ObjectFormField::new("schema", "Database", ObjectFormFieldKind::Text)
                            .default_value(ObjectFormValue::String(schema)),
                        ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                            .required()
                            .default_value(ObjectFormValue::String(name)),
                        ObjectFormField::new(
                            "parameters",
                            "Parameters",
                            ObjectFormFieldKind::StringList,
                        )
                        .placeholder("value INT"),
                        ObjectFormField::new("returns", "Returns", ObjectFormFieldKind::Text)
                            .required()
                            .placeholder("INT"),
                        ObjectFormField::new(
                            "deterministic",
                            "Deterministic",
                            ObjectFormFieldKind::Checkbox,
                        ),
                        ObjectFormField::new(
                            "sql_data_access",
                            "SQL Data Access",
                            ObjectFormFieldKind::Select,
                        )
                        .default_value(ObjectFormValue::String("CONTAINS SQL".to_string()))
                        .options(vec![
                            ObjectFormOption {
                                value: "CONTAINS SQL".to_string(),
                                label: "Contains SQL".to_string(),
                            },
                            ObjectFormOption {
                                value: "NO SQL".to_string(),
                                label: "No SQL".to_string(),
                            },
                            ObjectFormOption {
                                value: "READS SQL DATA".to_string(),
                                label: "Reads SQL Data".to_string(),
                            },
                            ObjectFormOption {
                                value: "MODIFIES SQL DATA".to_string(),
                                label: "Modifies SQL Data".to_string(),
                            },
                        ]),
                        ObjectFormField::new("body", "Body", ObjectFormFieldKind::SqlExpression)
                            .required()
                            .placeholder("RETURN 1"),
                    ]),
                ]),
            )),
            ("procedure", ObjectFormMode::Create | ObjectFormMode::Drop) => Ok(Some(
                ObjectFormSpec::new("procedure", request.mode, "Procedure").sections(vec![
                    ObjectFormSection::new(vec![
                        ObjectFormField::new("schema", "Database", ObjectFormFieldKind::Text)
                            .default_value(ObjectFormValue::String(schema)),
                        ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                            .required()
                            .default_value(ObjectFormValue::String(name)),
                        ObjectFormField::new(
                            "parameters",
                            "Parameters",
                            ObjectFormFieldKind::StringList,
                        )
                        .placeholder("IN user_id INT"),
                        ObjectFormField::new("body", "Body", ObjectFormFieldKind::SqlExpression)
                            .required()
                            .placeholder("BEGIN\n  SELECT 1;\nEND"),
                    ]),
                ]),
            )),
            ("trigger", ObjectFormMode::Create | ObjectFormMode::Drop) => Ok(Some(
                ObjectFormSpec::new("trigger", request.mode, "Trigger").sections(vec![
                    ObjectFormSection::new(vec![
                        ObjectFormField::new("schema", "Database", ObjectFormFieldKind::Text)
                            .default_value(ObjectFormValue::String(schema)),
                        ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                            .required()
                            .default_value(ObjectFormValue::String(name)),
                        ObjectFormField::new("table", "Table", ObjectFormFieldKind::Text)
                            .required()
                            .default_value(ObjectFormValue::String(associated_table)),
                        ObjectFormField::new("timing", "Timing", ObjectFormFieldKind::Select)
                            .default_value(ObjectFormValue::String("BEFORE".to_string()))
                            .options(vec![
                                ObjectFormOption {
                                    value: "BEFORE".to_string(),
                                    label: "Before".to_string(),
                                },
                                ObjectFormOption {
                                    value: "AFTER".to_string(),
                                    label: "After".to_string(),
                                },
                            ]),
                        ObjectFormField::new("event", "Event", ObjectFormFieldKind::Select)
                            .default_value(ObjectFormValue::String("INSERT".to_string()))
                            .options(vec![
                                ObjectFormOption {
                                    value: "INSERT".to_string(),
                                    label: "Insert".to_string(),
                                },
                                ObjectFormOption {
                                    value: "UPDATE".to_string(),
                                    label: "Update".to_string(),
                                },
                                ObjectFormOption {
                                    value: "DELETE".to_string(),
                                    label: "Delete".to_string(),
                                },
                            ]),
                        ObjectFormField::new("body", "Body", ObjectFormFieldKind::SqlExpression)
                            .required()
                            .placeholder("SET NEW.updated_at = NOW()"),
                    ]),
                ]),
            )),
            ("event", ObjectFormMode::Create | ObjectFormMode::Edit | ObjectFormMode::Drop) => {
                Ok(Some(
                    ObjectFormSpec::new("event", request.mode, "Event").sections(vec![
                        ObjectFormSection::new(vec![
                            ObjectFormField::new("schema", "Database", ObjectFormFieldKind::Text)
                                .default_value(ObjectFormValue::String(schema)),
                            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                                .required()
                                .default_value(ObjectFormValue::String(name)),
                            ObjectFormField::new("schedule", "Schedule", ObjectFormFieldKind::Text)
                                .required()
                                .placeholder("EVERY 1 DAY"),
                            ObjectFormField::new(
                                "enabled",
                                "Enabled",
                                ObjectFormFieldKind::Checkbox,
                            )
                            .default_value(ObjectFormValue::Bool(true)),
                            ObjectFormField::new(
                                "preserve",
                                "Preserve On Completion",
                                ObjectFormFieldKind::Checkbox,
                            )
                            .default_value(ObjectFormValue::Bool(false)),
                            ObjectFormField::new(
                                "body",
                                "Body",
                                ObjectFormFieldKind::SqlExpression,
                            )
                            .required()
                            .placeholder("DO UPDATE metrics SET value = value + 1"),
                            ObjectFormField::new(
                                "comment",
                                "Comment",
                                ObjectFormFieldKind::TextArea,
                            ),
                        ]),
                    ]),
                ))
            }
            ("sequence", ObjectFormMode::Create | ObjectFormMode::Drop) => {
                let features = self.mysql_server_features().await?;
                if !features.supports_sequences() {
                    return Ok(None);
                }
                Ok(Some(
                    ObjectFormSpec::new("sequence", request.mode, "Sequence").sections(vec![
                        ObjectFormSection::new(vec![
                            ObjectFormField::new("schema", "Database", ObjectFormFieldKind::Text)
                                .default_value(ObjectFormValue::String(schema)),
                            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                                .required()
                                .default_value(ObjectFormValue::String(name)),
                            ObjectFormField::new("start", "Start", ObjectFormFieldKind::Text)
                                .placeholder("1"),
                            ObjectFormField::new(
                                "increment",
                                "Increment",
                                ObjectFormFieldKind::Text,
                            )
                            .placeholder("1"),
                        ]),
                    ]),
                ))
            }
            _ => Ok(None),
        }
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let qualified = mysql_object_qualified_name(object);

        match object.object_type {
            ObjectType::Table => {
                let query = format!("SHOW CREATE TABLE {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[1], "table", &object.name)
            }
            ObjectType::View => {
                let query = format!("SHOW CREATE VIEW {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[1], "view", &object.name)
            }
            ObjectType::Function => {
                let query = format!("SHOW CREATE FUNCTION {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "function", &object.name)
            }
            ObjectType::Procedure => {
                let query = format!("SHOW CREATE PROCEDURE {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "procedure", &object.name)
            }
            ObjectType::Trigger => {
                let query = format!("SHOW CREATE TRIGGER {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "trigger", &object.name)
            }
            ObjectType::Event => {
                let query = format!("SHOW CREATE EVENT {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[3, 2], "event", &object.name)
            }
            ObjectType::Sequence => {
                let query = format!("SHOW CREATE SEQUENCE {}", qualified);
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[1], "sequence", &object.name)
            }
            ObjectType::Database => {
                let query = format!("SHOW CREATE DATABASE `{}`", object.name.replace("`", "``"));
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[1], "database", &object.name)
            }
            ObjectType::Index => {
                let Some(table) = object.signature.as_deref() else {
                    return Err(ZqlzError::NotImplemented(
                        "DDL generation for indexes requires table context in MySQL".into(),
                    ));
                };
                let indexes = self.get_indexes(object.schema.as_deref(), table).await?;
                let index = indexes
                    .iter()
                    .find(|index| index.name == object.name)
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Index '{}' not found on table '{}'",
                            object.name, table
                        ))
                    })?;
                Ok(mysql_index_ddl(table, index))
            }
            ObjectType::Constraint => {
                let Some(table) = object.signature.as_deref() else {
                    return Err(ZqlzError::NotImplemented(
                        "DDL generation for constraints requires table context in MySQL".into(),
                    ));
                };
                let constraints = self
                    .get_constraints(object.schema.as_deref(), table)
                    .await?;
                if let Some(constraint) = constraints
                    .iter()
                    .find(|constraint| constraint.name == object.name)
                {
                    return mysql_constraint_ddl(table, constraint);
                }

                let foreign_keys = self
                    .get_foreign_keys(object.schema.as_deref(), table)
                    .await?;
                let foreign_key = foreign_keys
                    .iter()
                    .find(|foreign_key| foreign_key.name == object.name)
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Constraint '{}' not found on table '{}'",
                            object.name, table
                        ))
                    })?;
                Ok(mysql_foreign_key_ddl(table, foreign_key))
            }
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} not supported in MySQL",
                object.object_type
            ))),
        }
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        let schema = mysql_object_form_string(&request.values, "schema");
        let schema = if schema.is_empty() {
            self.default_database()
        } else {
            Some(schema)
        };
        let name = mysql_object_form_string(&request.values, "name");
        if name.is_empty() {
            return Err(ZqlzError::Schema("Object name is required".to_string()));
        }

        match (request.kind_id.as_str(), request.mode) {
            ("database", ObjectFormMode::Create) => {
                let mut sql = format!("CREATE DATABASE {}", mysql_quote_identifier(name));
                let charset = mysql_object_form_string(&request.values, "charset");
                if !charset.is_empty() {
                    sql.push_str(&format!(
                        " CHARACTER SET {}",
                        mysql_quote_identifier(charset)
                    ));
                }
                let collation = mysql_object_form_string(&request.values, "collation");
                if !collation.is_empty() {
                    sql.push_str(&format!(" COLLATE {}", mysql_quote_identifier(collation)));
                }
                Ok(vec![format!("{sql};")])
            }
            ("database", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP DATABASE {};",
                mysql_quote_identifier(name)
            )]),
            ("function", ObjectFormMode::Create) => Ok(vec![mysql_function_form_sql(
                schema,
                name,
                &request.values,
            )?]),
            ("function", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP FUNCTION {};",
                mysql_qualified_name(schema, name)
            )]),
            ("procedure", ObjectFormMode::Create) => Ok(vec![mysql_procedure_form_sql(
                schema,
                name,
                &request.values,
            )?]),
            ("procedure", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP PROCEDURE {};",
                mysql_qualified_name(schema, name)
            )]),
            ("trigger", ObjectFormMode::Create) => {
                Ok(vec![mysql_trigger_form_sql(schema, name, &request.values)?])
            }
            ("trigger", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP TRIGGER {};",
                mysql_qualified_name(schema, name)
            )]),
            ("event", ObjectFormMode::Create) => {
                let schedule = mysql_object_form_string(&request.values, "schedule");
                let body = mysql_object_form_string(&request.values, "body");
                if schedule.is_empty() {
                    return Err(ZqlzError::Schema("Event schedule is required".to_string()));
                }
                if body.is_empty() {
                    return Err(ZqlzError::Schema("Event body is required".to_string()));
                }
                let mut sql = format!(
                    "CREATE EVENT {}\nON SCHEDULE {}\n",
                    mysql_qualified_name(schema, name),
                    schedule
                );
                if mysql_object_form_bool(&request.values, "preserve") {
                    sql.push_str("ON COMPLETION PRESERVE\n");
                }
                if !mysql_object_form_bool(&request.values, "enabled") {
                    sql.push_str("DISABLE\n");
                }
                let comment = mysql_object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    sql.push_str(&format!("COMMENT {}\n", mysql_quote_literal(comment)));
                }
                sql.push_str(body);
                Ok(vec![format!("{};", sql.trim_end_matches(';'))])
            }
            ("event", ObjectFormMode::Edit) => {
                let body = mysql_object_form_string(&request.values, "body");
                let schedule = mysql_object_form_string(&request.values, "schedule");
                let mut sql = format!("ALTER EVENT {}", mysql_qualified_name(schema, name));
                if !schedule.is_empty() {
                    sql.push_str(&format!("\nON SCHEDULE {}", schedule));
                }
                sql.push_str(if mysql_object_form_bool(&request.values, "enabled") {
                    "\nENABLE"
                } else {
                    "\nDISABLE"
                });
                let comment = mysql_object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    sql.push_str(&format!("\nCOMMENT {}", mysql_quote_literal(comment)));
                }
                if !body.is_empty() {
                    sql.push_str(&format!("\n{}", body));
                }
                Ok(vec![format!("{};", sql.trim_end_matches(';'))])
            }
            ("event", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP EVENT {};",
                mysql_qualified_name(schema, name)
            )]),
            ("sequence", ObjectFormMode::Create) => {
                let mut sql = format!("CREATE SEQUENCE {}", mysql_qualified_name(schema, name));
                let start = mysql_object_form_string(&request.values, "start");
                if !start.is_empty() {
                    sql.push_str(&format!(" START WITH {start}"));
                }
                let increment = mysql_object_form_string(&request.values, "increment");
                if !increment.is_empty() {
                    sql.push_str(&format!(" INCREMENT BY {increment}"));
                }
                Ok(vec![format!("{sql};")])
            }
            ("sequence", ObjectFormMode::Drop) => Ok(vec![format!(
                "DROP SEQUENCE {};",
                mysql_qualified_name(schema, name)
            )]),
            _ => Err(ZqlzError::NotSupported(format!(
                "Object form DDL is not supported for {}",
                request.kind_id
            ))),
        }
    }

    async fn get_dependencies(&self, object: &DatabaseObject) -> Result<Vec<Dependency>> {
        let mut dependencies = Vec::new();
        let schema = object.schema.as_deref().or(self.default_database());
        let name = &object.name;

        match object.object_type {
            ObjectType::Table => {
                // Get foreign key dependencies (tables this table depends on)
                let (fk_query, fk_params) = if let Some(schema_name) = schema {
                    (
                        "SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
                         FROM information_schema.KEY_COLUMN_USAGE
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                           AND REFERENCED_TABLE_NAME IS NOT NULL
                         GROUP BY REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME",
                        vec![
                            Value::String(schema_name.to_string()),
                            Value::String(name.to_string()),
                        ],
                    )
                } else {
                    (
                        "SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
                         FROM information_schema.KEY_COLUMN_USAGE
                         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
                           AND REFERENCED_TABLE_NAME IS NOT NULL
                         GROUP BY REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME",
                        vec![Value::String(name.to_string())],
                    )
                };

                if let Ok(result) = self.query(fk_query, &fk_params).await {
                    for row in &result.rows {
                        let ref_schema = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                        let ref_table = row
                            .get(1)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        if !ref_table.is_empty() {
                            dependencies.push(Dependency {
                                dependent: object.clone(),
                                referenced: DatabaseObject {
                                    object_type: ObjectType::Table,
                                    schema: ref_schema,
                                    name: ref_table,
                                    signature: None,
                                },
                                dependency_type: DependencyType::Normal,
                            });
                        }
                    }
                }

                // Get triggers that depend on this table
                let (trigger_query, trigger_params) = if let Some(schema_name) = schema {
                    (
                        "SELECT TRIGGER_NAME
                         FROM information_schema.TRIGGERS
                         WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?",
                        vec![
                            Value::String(schema_name.to_string()),
                            Value::String(name.to_string()),
                        ],
                    )
                } else {
                    (
                        "SELECT TRIGGER_NAME
                         FROM information_schema.TRIGGERS
                         WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?",
                        vec![Value::String(name.to_string())],
                    )
                };

                if let Ok(result) = self.query(trigger_query, &trigger_params).await {
                    for row in &result.rows {
                        let trigger_name = row
                            .get(0)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        if !trigger_name.is_empty() {
                            dependencies.push(Dependency {
                                dependent: DatabaseObject {
                                    object_type: ObjectType::Trigger,
                                    schema: schema.map(|s| s.to_string()),
                                    name: trigger_name,
                                    signature: None,
                                },
                                referenced: object.clone(),
                                dependency_type: DependencyType::Automatic,
                            });
                        }
                    }
                }
            }
            ObjectType::View => {
                // Try to find tables referenced in the view
                // MySQL doesn't have direct dependency info, but we can query information_schema.VIEW_TABLE_USAGE (MySQL 8.0+)
                let (usage_query, usage_params) = if let Some(schema_name) = schema {
                    (
                        "SELECT TABLE_SCHEMA, TABLE_NAME
                         FROM information_schema.VIEW_TABLE_USAGE
                         WHERE VIEW_SCHEMA = ? AND VIEW_NAME = ?",
                        vec![
                            Value::String(schema_name.to_string()),
                            Value::String(name.to_string()),
                        ],
                    )
                } else {
                    (
                        "SELECT TABLE_SCHEMA, TABLE_NAME
                         FROM information_schema.VIEW_TABLE_USAGE
                         WHERE VIEW_SCHEMA = DATABASE() AND VIEW_NAME = ?",
                        vec![Value::String(name.to_string())],
                    )
                };

                if let Ok(result) = self.query(usage_query, &usage_params).await {
                    for row in &result.rows {
                        let ref_schema = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
                        let ref_table = row
                            .get(1)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        if !ref_table.is_empty() {
                            dependencies.push(Dependency {
                                dependent: object.clone(),
                                referenced: DatabaseObject {
                                    object_type: ObjectType::Table,
                                    schema: ref_schema,
                                    name: ref_table,
                                    signature: None,
                                },
                                dependency_type: DependencyType::Normal,
                            });
                        }
                    }
                }
            }
            _ => {
                // For other object types, return empty dependencies
            }
        }

        Ok(dependencies)
    }
}

/// Extract the definition column from a `SHOW CREATE ...` result.
///
/// The two failure modes need distinct messages. No rows means the object is gone. A row whose
/// definition column is NULL means the server listed the object but withheld its source — for
/// routines that is what MySQL does when the user is neither the definer nor holds `SHOW_ROUTINE`,
/// so reporting it as "not found" sends people looking for an object that is actually there.
///
/// `definition_columns` is tried in order because `SHOW CREATE EVENT` places the statement at a
/// different index than the other object kinds.
fn ddl_from_show_create(
    rows: &[zqlz_core::Row],
    definition_columns: &[usize],
    object_kind: &str,
    object_name: &str,
) -> Result<String> {
    let Some(row) = rows.first() else {
        return Err(ZqlzError::NotFound(format!(
            "{} '{}'",
            object_kind, object_name
        )));
    };

    definition_columns
        .iter()
        .find_map(|index| row.get(*index).and_then(|value| value.as_str()))
        .map(|definition| definition.to_string())
        .ok_or_else(|| {
            // `Other` renders verbatim; the wrapping layers already say what failed.
            ZqlzError::Other(format!(
                "The definition of {} '{}' is not readable — the connected user may lack the privileges required to view its source",
                object_kind, object_name
            ))
        })
}
