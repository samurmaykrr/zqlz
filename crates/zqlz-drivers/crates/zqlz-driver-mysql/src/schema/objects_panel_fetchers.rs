//! Server-feature detection and the objects-panel data fetchers for
//! relations, events, and MySQL's "metadata" catalog (indexes, constraints,
//! foreign keys, partitions, sequences, tablespaces, engines, and character
//! sets/collations).
use super::*;

impl MySqlConnection {
    /// Server flavor and version, cached for the lifetime of the connection.
    ///
    /// Every schema-panel interaction (objects panel, DDL, sequence listing)
    /// consults this, and each uncached call cost a `SELECT VERSION()` round
    /// trip — five or more per interaction on a schema with several object
    /// kinds. The server flavor cannot change mid-connection, so the first
    /// query result is reused for the rest of the connection's life.
    pub(crate) async fn mysql_server_features(&self) -> Result<MysqlServerFeatures> {
        let features = self
            .server_features
            .get_or_try_init(|| async {
                let result = self
                    .query("SELECT VERSION(), @@version_comment", &[])
                    .await?;
                let version = result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                let version_comment = result
                    .rows
                    .first()
                    .and_then(|row| row.get(1))
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();

                Ok::<_, ZqlzError>(parse_mysql_server_features(version, version_comment))
            })
            .await?;

        Ok(features.clone())
    }

    /// Returns the cached server features if they have already been fetched,
    /// without issuing a query. Used by synchronous trait methods (such as
    /// `SchemaIntrospection::object_kind_support`) that cannot await.
    pub(crate) fn cached_server_features(&self) -> Option<MysqlServerFeatures> {
        self.server_features.get().cloned()
    }

    /// Tables and views only: the "table"/"view" panel kinds must not pay for
    /// routine, trigger and event queries that they would then discard.
    pub(crate) async fn mysql_relation_panel_data(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelData> {
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT
                    TABLE_NAME,
                    TABLE_TYPE,
                    TABLE_ROWS,
                    DATA_LENGTH,
                    ENGINE,
                    ROW_FORMAT,
                    AUTO_INCREMENT,
                    CREATE_TIME,
                    UPDATE_TIME,
                    TABLE_COLLATION,
                    TABLE_COMMENT,
                    CREATE_OPTIONS
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = ?
                    AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'SYSTEM VIEW')
                 ORDER BY TABLE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT
                TABLE_NAME,
                TABLE_TYPE,
                TABLE_ROWS,
                DATA_LENGTH,
                ENGINE,
                ROW_FORMAT,
                AUTO_INCREMENT,
                CREATE_TIME,
                UPDATE_TIME,
                TABLE_COLLATION,
                TABLE_COMMENT,
                CREATE_OPTIONS
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'SYSTEM VIEW')
             ORDER BY TABLE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

        let columns = mysql_objects_panel_columns();

        let rows: Vec<ObjectsPanelRow> = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let table_type_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("BASE TABLE");

                let object_type = if table_type_str.contains("VIEW") {
                    "view"
                } else {
                    "table"
                };

                let row_count = row
                    .get(2)
                    .and_then(|v| v.as_i64())
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".to_string());

                let data_length = row
                    .get(3)
                    .and_then(|v| v.as_i64())
                    .map(format_bytes_human_readable)
                    .unwrap_or_else(|| "-".to_string());

                let engine = row
                    .get(4)
                    .and_then(|v| v.as_str())
                    .filter(|s| *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                let row_format = row
                    .get(5)
                    .and_then(|v| v.as_str())
                    .filter(|s| *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                let auto_increment = row
                    .get(6)
                    .and_then(|v| v.as_i64())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string());

                let created_date = row
                    .get(7)
                    .map(|v| v.to_string())
                    .filter(|s| s != "NULL")
                    .unwrap_or_else(|| "-".to_string());

                let modified_date = row
                    .get(8)
                    .map(|v| v.to_string())
                    .filter(|s| s != "NULL")
                    .unwrap_or_else(|| "-".to_string());

                let collation = row
                    .get(9)
                    .and_then(|v| v.as_str())
                    .filter(|s| *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                // MySQL stores the literal text "VIEW" as the comment of every view.
                let comment = row
                    .get(10)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty() && *s != "NULL" && !(object_type == "view" && *s == "VIEW"))
                    .unwrap_or("-")
                    .to_string();

                let create_options = row
                    .get(11)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty() && *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), name.clone());
                values.insert("row_count".to_string(), row_count);
                values.insert("data_length".to_string(), data_length);
                values.insert("engine".to_string(), engine);
                values.insert("row_format".to_string(), row_format);
                values.insert("auto_increment".to_string(), auto_increment);
                values.insert("created_date".to_string(), created_date);
                values.insert("modified_date".to_string(), modified_date);
                values.insert("collation".to_string(), collation);
                values.insert("comment".to_string(), comment);
                values.insert("create_options".to_string(), create_options);

                ObjectsPanelRow {
                    name,
                    schema: schema.map(str::to_string),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, values["name"].clone())
                            .with_schema_option(schema.map(str::to_string)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData { columns, rows })
    }

    pub(crate) async fn mysql_database_objects_panel_data(&self) -> Result<ObjectsPanelData> {
        let result = self
            .query(
                "SELECT
                    s.SCHEMA_NAME,
                    s.DEFAULT_CHARACTER_SET_NAME,
                    s.DEFAULT_COLLATION_NAME,
                    COUNT(t.TABLE_NAME) AS TABLE_COUNT,
                    CAST(COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH), 0) AS SIGNED) AS SIZE_BYTES
                 FROM information_schema.SCHEMATA s
                 LEFT JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = s.SCHEMA_NAME
                 GROUP BY s.SCHEMA_NAME, s.DEFAULT_CHARACTER_SET_NAME, s.DEFAULT_COLLATION_NAME
                 ORDER BY s.SCHEMA_NAME",
                &[],
            )
            .await?;

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), name.clone());
                values.insert(
                    "default_charset".to_string(),
                    row.get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "default_collation".to_string(),
                    row.get(2)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "table_count".to_string(),
                    row.get(3)
                        .and_then(|value| value.as_i64())
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "size".to_string(),
                    row.get(4)
                        .and_then(|value| value.as_i64())
                        .map(format_bytes_human_readable)
                        .unwrap_or_else(|| "-".to_string()),
                );

                ObjectsPanelRow {
                    name: name.clone(),
                    schema: None,
                    object_type: "database".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("database", name.clone())
                            .with_database_option(Some(name)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData {
            columns: mysql_database_columns(),
            rows,
        })
    }

    pub(crate) async fn mysql_event_objects_panel_data(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelData> {
        if !self.mysql_server_features().await?.supports_events() {
            return Ok(ObjectsPanelData {
                columns: mysql_event_columns(),
                rows: Vec::new(),
            });
        }
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT EVENT_SCHEMA, EVENT_NAME, STATUS, INTERVAL_VALUE, INTERVAL_FIELD,
                        EXECUTE_AT, STARTS, ENDS, DEFINER, CREATED, EVENT_COMMENT
                 FROM information_schema.EVENTS
                 WHERE EVENT_SCHEMA = ?
                 ORDER BY EVENT_SCHEMA, EVENT_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT EVENT_SCHEMA, EVENT_NAME, STATUS, INTERVAL_VALUE, INTERVAL_FIELD,
                        EXECUTE_AT, STARTS, ENDS, DEFINER, CREATED, EVENT_COMMENT
                 FROM information_schema.EVENTS
                 ORDER BY EVENT_SCHEMA, EVENT_NAME",
                Vec::new(),
            )
        };
        let result = self.query(sql, &params).await?;
        let qualify_with_schema = schema.is_none();
        let rows = result
            .rows
            .iter()
            .map(|row| mysql_event_row(row, qualify_with_schema))
            .collect();

        Ok(ObjectsPanelData {
            columns: mysql_event_columns(),
            rows,
        })
    }

    pub(crate) async fn mysql_metadata_objects_panel_data(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        let schema = schema.or(self.default_database());
        let rows = match match kind_id {
            "index" => self.mysql_index_rows(schema).await,
            "constraint" => self.mysql_constraint_rows(schema).await,
            "foreign_key" => self.mysql_foreign_key_rows(schema).await,
            "partition" => self.mysql_partition_rows(schema).await,
            "sequence" => self.mysql_sequence_rows(schema).await,
            "tablespace" => self.mysql_tablespace_rows().await,
            "engine" => self.mysql_engine_rows().await,
            "charset" => self.mysql_charset_rows().await,
            "collation" => self.mysql_collation_rows().await,
            "user" => self.mysql_user_rows().await,
            "role" => self.mysql_role_rows().await,
            "grant" => self.mysql_grant_rows().await,
            "plugin" => self.mysql_plugin_rows().await,
            "variable" => self.mysql_variable_rows().await,
            "status_variable" => self.mysql_status_variable_rows().await,
            "process" => self.mysql_process_rows().await,
            "replica_status" => self.mysql_replica_status_rows().await,
            _ => Ok(Vec::new()),
        } {
            Ok(rows) => rows,
            Err(error) if mysql_metadata_kind_degrades_on_error(kind_id) => {
                tracing::warn!(
                    %error,
                    kind_id,
                    "Failed to load MySQL metadata kind; returning visible unavailable row"
                );
                vec![mysql_metadata_unavailable_row(kind_id, &error)]
            }
            Err(error) => return Err(error),
        };

        Ok(ObjectsPanelData {
            columns: mysql_metadata_columns(),
            rows,
        })
    }

    pub(crate) async fn mysql_index_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let sql = catalog::mysql_indexes_sql(
            if schema.is_some() { "= ?" } else { "IS NOT NULL" },
            false,
        );
        let params: Vec<Value> = schema
            .map(|schema_name| vec![Value::String(schema_name.to_string())])
            .unwrap_or_default();
        let result = self.query(&sql, &params).await?;

        Ok(catalog::mysql_index_entries(&result)
            .into_iter()
            .map(|entry| {
                let index = entry.index;
                let kind = if index.is_primary {
                    "Primary"
                } else if index.is_unique {
                    "Unique"
                } else {
                    "Index"
                };
                let detail = format!(
                    "{}: {}",
                    index.method.as_deref().unwrap_or("-"),
                    mysql_index_key_parts(
                        &index.columns,
                        &index.column_descending,
                        &index.include_columns,
                        false,
                    )
                    .join(", ")
                );
                mysql_metadata_row(
                    "index",
                    index.name,
                    entry.schema,
                    Some(entry.table),
                    kind,
                    detail,
                    "-",
                    index.comment.unwrap_or_else(|| "-".to_string()),
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_constraint_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, ENFORCED
                 FROM information_schema.TABLE_CONSTRAINTS
                 WHERE TABLE_SCHEMA = ?
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, ENFORCED
                 FROM information_schema.TABLE_CONSTRAINTS
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME",
                Vec::new(),
            )
        };
        let result = match self.query(sql, &params).await {
            Ok(result) => result,
            Err(_) => {
                if let Some(schema_name) = schema {
                    self.query(
                        "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, NULL
                         FROM information_schema.TABLE_CONSTRAINTS
                         WHERE TABLE_SCHEMA = ?
                         ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME",
                        &[Value::String(schema_name.to_string())],
                    )
                    .await?
                } else {
                    self.query(
                        "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, NULL
                         FROM information_schema.TABLE_CONSTRAINTS
                         ORDER BY TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME",
                        &[],
                    )
                    .await?
                }
            }
        };

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let schema = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let table = row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let name = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let kind = row
                    .get(3)
                    .and_then(|value| value.as_str())
                    .unwrap_or("Constraint");
                let state = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row("constraint", name, schema, table, kind, "-", state, "-")
            })
            .collect())
    }

    pub(crate) async fn mysql_foreign_key_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME,
                        rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE,
                        rc.UNIQUE_CONSTRAINT_SCHEMA
                 FROM information_schema.REFERENTIAL_CONSTRAINTS rc
                 WHERE rc.CONSTRAINT_SCHEMA = ?
                 ORDER BY rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME,
                        rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE,
                        rc.UNIQUE_CONSTRAINT_SCHEMA
                 FROM information_schema.REFERENTIAL_CONSTRAINTS rc
                 ORDER BY rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME",
                Vec::new(),
            )
        };
        let result = self.query(sql, &params).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let schema = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let table = row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let name = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let referenced = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let update = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let delete = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
                // Say which database a cross-database key points into.
                let referenced = match row.get(6).and_then(|value| value.as_str()) {
                    Some(referenced_schema) if Some(referenced_schema) != schema.as_deref() => {
                        format!("{referenced_schema}.{referenced}")
                    }
                    _ => referenced.to_string(),
                };
                mysql_metadata_row(
                    "foreign_key",
                    name,
                    schema,
                    table,
                    "Foreign Key",
                    format!("references {referenced}; update {update}; delete {delete}"),
                    "-",
                    "-",
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_partition_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (schema_filter, params) = match schema {
            Some(schema_name) => (
                "TABLE_SCHEMA = ? AND",
                vec![Value::String(schema_name.to_string())],
            ),
            None => ("", Vec::new()),
        };
        let sql = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME,
                    PARTITION_METHOD, SUBPARTITION_METHOD, PARTITION_EXPRESSION,
                    SUBPARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS,
                    PARTITION_COMMENT
             FROM information_schema.PARTITIONS
             WHERE {schema_filter} PARTITION_NAME IS NOT NULL
             ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION,
                      SUBPARTITION_ORDINAL_POSITION"
        );
        let result = self.query(&sql, &params).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let text = |index: usize| mysql_text(row.get(index));
                let partition = text(2).unwrap_or_default();
                // Subpartitions repeat their parent's PARTITION_NAME; without the
                // subpartition name every one of them would look identical.
                let (name, method, expression) = match text(3) {
                    Some(subpartition) => (
                        format!("{partition}.{subpartition}"),
                        format!(
                            "{}/{}",
                            text(4).unwrap_or_default(),
                            text(5).unwrap_or_default()
                        ),
                        format!(
                            "{}; sub {}",
                            text(6).unwrap_or_else(|| "-".to_string()),
                            text(7).unwrap_or_else(|| "-".to_string())
                        ),
                    ),
                    None => (
                        partition,
                        text(4).unwrap_or_else(|| "-".to_string()),
                        text(6).unwrap_or_else(|| "-".to_string()),
                    ),
                };
                let bound = match (text(4).as_deref(), text(8)) {
                    (Some(method), Some(description)) if method.starts_with("RANGE") => {
                        if description == "MAXVALUE" {
                            " LESS THAN MAXVALUE".to_string()
                        } else {
                            format!(" LESS THAN ({description})")
                        }
                    }
                    (Some(method), Some(description)) if method.starts_with("LIST") => {
                        format!(" IN ({description})")
                    }
                    _ => String::new(),
                };
                let rows = row
                    .get(9)
                    .and_then(|value| value.as_i64())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "partition",
                    name,
                    text(0),
                    text(1),
                    method,
                    format!("{expression}{bound}; rows {rows}"),
                    "-",
                    text(10)
                        .filter(|comment| !comment.is_empty())
                        .unwrap_or_else(|| "-".to_string()),
                )
            })
            .collect())
    }

    /// Runs the `information_schema.SEQUENCES` query shared by
    /// `mysql_sequence_rows` (objects panel) and `list_sequences` (structured
    /// introspection). Returns `None` on plain MySQL, which has no sequence
    /// catalog to query — MariaDB is required.
    pub(crate) async fn mysql_sequence_query_rows(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<zqlz_core::QueryResult>> {
        let features = self.mysql_server_features().await?;
        if !features.supports_sequences() {
            return Ok(None);
        }
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, DATA_TYPE,
                        START_VALUE, MINIMUM_VALUE, MAXIMUM_VALUE, INCREMENT
                 FROM information_schema.SEQUENCES
                 WHERE SEQUENCE_SCHEMA = ?
                 ORDER BY SEQUENCE_SCHEMA, SEQUENCE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, DATA_TYPE,
                        START_VALUE, MINIMUM_VALUE, MAXIMUM_VALUE, INCREMENT
                 FROM information_schema.SEQUENCES
                 ORDER BY SEQUENCE_SCHEMA, SEQUENCE_NAME",
                Vec::new(),
            )
        };
        Ok(Some(self.query(sql, &params).await?))
    }

    pub(crate) async fn mysql_sequence_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let Some(result) = self.mysql_sequence_query_rows(schema).await? else {
            return Ok(Vec::new());
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let schema = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let name = row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let data_type = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("Sequence");
                let start = row
                    .get(3)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                let min = row
                    .get(4)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                let max = row
                    .get(5)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                let increment = row
                    .get(6)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "sequence",
                    name,
                    schema,
                    None,
                    data_type,
                    format!("start {start}; min {min}; max {max}; increment {increment}"),
                    "MariaDB",
                    "-",
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_tablespace_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT TABLESPACE_NAME, ENGINE, FILE_TYPE, FILE_NAME
                 FROM information_schema.FILES
                 WHERE TABLESPACE_NAME IS NOT NULL
                   AND TABLESPACE_NAME NOT LIKE '%/%'
                 GROUP BY TABLESPACE_NAME, ENGINE, FILE_TYPE, FILE_NAME
                 ORDER BY TABLESPACE_NAME",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL tablespaces");
                return Err(error);
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let engine = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let kind = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("Tablespace");
                let file = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row("tablespace", name, None, None, kind, file, engine, "-")
            })
            .collect())
    }

    pub(crate) async fn mysql_engine_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW ENGINES", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let support = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let comment = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let transactions = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let xa = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let savepoints = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "engine",
                    name,
                    None,
                    None,
                    "Storage Engine",
                    format!("transactions {transactions}; XA {xa}; savepoints {savepoints}"),
                    support,
                    comment,
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_charset_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW CHARACTER SET", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let description = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let collation = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let maxlen = row
                    .get(3)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "charset",
                    name,
                    None,
                    None,
                    "Character Set",
                    format!("default collation {collation}; maxlen {maxlen}"),
                    "-",
                    description,
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_collation_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW COLLATION", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let charset = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let default = row.get(3).and_then(|value| value.as_str()).unwrap_or("");
                let compiled = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let sortlen = row
                    .get(5)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "collation",
                    name,
                    None,
                    None,
                    charset,
                    format!("compiled {compiled}; sortlen {sortlen}"),
                    if default == "Yes" { "Default" } else { "-" },
                    "-",
                )
            })
            .collect())
    }
}
