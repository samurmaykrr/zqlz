//! MySQL schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, DependencyType, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo,
    ObjectType, ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelRow, PrimaryKeyInfo,
    ProcedureInfo, Result, SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails, TableInfo,
    TableType, TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, ViewInfo,
    ZqlzError,
};

use crate::MySqlConnection;

#[async_trait]
impl SchemaIntrospection for MySqlConnection {
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self.query("SHOW DATABASES", &[]).await?;

        let databases = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                Some(DatabaseInfo {
                    name,
                    owner: None,
                    encoding: Some("utf8mb4".to_string()),
                    size_bytes: None,
                    comment: None,
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
        // In MySQL, schema is the database name
        let query = if let Some(db) = schema {
            format!(
                "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH as SIZE_BYTES, TABLE_COMMENT
                 FROM information_schema.TABLES 
                 WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE'
                 ORDER BY TABLE_NAME",
                db.replace("'", "''")
            )
        } else {
            "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH as SIZE_BYTES, TABLE_COMMENT
             FROM information_schema.TABLES 
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
             ORDER BY TABLE_NAME".to_string()
        };

        let result = self.query(&query, &[]).await?;

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
        let query = if let Some(db) = schema {
            format!(
                "SELECT TABLE_NAME, VIEW_DEFINITION
                 FROM information_schema.VIEWS 
                 WHERE TABLE_SCHEMA = '{}'
                 ORDER BY TABLE_NAME",
                db.replace("'", "''")
            )
        } else {
            "SELECT TABLE_NAME, VIEW_DEFINITION
             FROM information_schema.VIEWS 
             WHERE TABLE_SCHEMA = DATABASE()
             ORDER BY TABLE_NAME"
                .to_string()
        };

        let result = self.query(&query, &[]).await?;

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
        let schema = schema.or(self.default_database());
        let tables = self.list_tables(schema).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        let columns = self.get_columns(schema, name).await?;
        let indexes = self.get_indexes(schema, name).await?;
        let foreign_keys = self.get_foreign_keys(schema, name).await?;
        let primary_key = self.get_primary_key(schema, name).await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints: Vec::new(),
            triggers: Vec::new(),
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT 
                    COLUMN_NAME,
                    ORDINAL_POSITION,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_KEY,
                    EXTRA,
                    COLUMN_COMMENT
                 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'
                 ORDER BY ORDINAL_POSITION",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT 
                    COLUMN_NAME,
                    ORDINAL_POSITION,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_KEY,
                    EXTRA,
                    COLUMN_COMMENT
                 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'
                 ORDER BY ORDINAL_POSITION",
                table.replace("'", "''")
            )
        };

        let result = self.query(&query, &[]).await?;

        let columns = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let ordinal = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_nullable = row.get(3).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
                let default_value = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let max_length = row.get(5).and_then(|v| v.as_i64());
                let precision = row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32);
                let scale = row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32);
                let column_key = row.get(8).and_then(|v| v.as_str()).unwrap_or("");
                let extra = row.get(9).and_then(|v| v.as_str()).unwrap_or("");
                let comment = row
                    .get(10)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty());

                let is_primary_key = column_key == "PRI";
                let is_auto_increment = extra.contains("auto_increment");
                let is_unique = column_key == "UNI";

                ColumnInfo {
                    name,
                    ordinal,
                    data_type,
                    nullable: is_nullable,
                    default_value,
                    max_length,
                    precision,
                    scale,
                    is_primary_key,
                    is_auto_increment,
                    is_unique,
                    foreign_key: None,
                    comment,
                }
            })
            .collect();

        Ok(columns)
    }

    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT 
                    INDEX_NAME,
                    NON_UNIQUE,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMNS,
                    INDEX_TYPE
                 FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'
                 GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                 ORDER BY INDEX_NAME",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT 
                    INDEX_NAME,
                    NON_UNIQUE,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMNS,
                    INDEX_TYPE
                 FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'
                 GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                 ORDER BY INDEX_NAME",
                table.replace("'", "''")
            )
        };

        let result = self.query(&query, &[]).await?;

        let indexes = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let non_unique = row.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
                let columns_str = row.get(2).and_then(|v| v.as_str()).unwrap_or("");
                let index_type = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("BTREE")
                    .to_string();

                let is_unique = non_unique == 0;
                let is_primary = name == "PRIMARY";
                let columns: Vec<String> = columns_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();

                Some(IndexInfo {
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                    comment: None,
                })
            })
            .collect();

        Ok(indexes)
    }

    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT 
                    CONSTRAINT_NAME,
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                 FROM information_schema.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = '{}' 
                   AND TABLE_NAME = '{}'
                   AND REFERENCED_TABLE_NAME IS NOT NULL
                 ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT 
                    CONSTRAINT_NAME,
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                 FROM information_schema.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = DATABASE() 
                   AND TABLE_NAME = '{}'
                   AND REFERENCED_TABLE_NAME IS NOT NULL
                 ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION",
                table.replace("'", "''")
            )
        };

        let result = self.query(&query, &[]).await?;

        // Group by constraint name
        let mut fk_map: std::collections::HashMap<String, ForeignKeyInfo> =
            std::collections::HashMap::new();

        for row in &result.rows {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let column = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_table = row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_column = row
                .get(3)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if let Some(fk) = fk_map.get_mut(&name) {
                fk.columns.push(column);
                fk.referenced_columns.push(ref_column);
            } else {
                fk_map.insert(
                    name.clone(),
                    ForeignKeyInfo {
                        name,
                        columns: vec![column],
                        referenced_table: ref_table,
                        referenced_schema: schema.map(|s| s.to_string()),
                        referenced_columns: vec![ref_column],
                        on_update: ForeignKeyAction::NoAction,
                        on_delete: ForeignKeyAction::NoAction,
                    },
                );
            }
        }

        // Get ON UPDATE and ON DELETE rules
        let rules_query = if let Some(db) = schema {
            format!(
                "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE
                 FROM information_schema.REFERENTIAL_CONSTRAINTS
                 WHERE CONSTRAINT_SCHEMA = '{}' AND TABLE_NAME = '{}'",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE
                 FROM information_schema.REFERENTIAL_CONSTRAINTS
                 WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
                table.replace("'", "''")
            )
        };

        if let Ok(rules_result) = self.query(&rules_query, &[]).await {
            for row in &rules_result.rows {
                let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("");
                let update_rule = row.get(1).and_then(|v| v.as_str()).unwrap_or("NO ACTION");
                let delete_rule = row.get(2).and_then(|v| v.as_str()).unwrap_or("NO ACTION");

                if let Some(fk) = fk_map.get_mut(name) {
                    fk.on_update = parse_fk_action(update_rule);
                    fk.on_delete = parse_fk_action(delete_rule);
                }
            }
        }

        Ok(fk_map.into_values().collect())
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT COLUMN_NAME
                 FROM information_schema.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = '{}' 
                   AND TABLE_NAME = '{}'
                   AND CONSTRAINT_NAME = 'PRIMARY'
                 ORDER BY ORDINAL_POSITION",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT COLUMN_NAME
                 FROM information_schema.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = DATABASE() 
                   AND TABLE_NAME = '{}'
                   AND CONSTRAINT_NAME = 'PRIMARY'
                 ORDER BY ORDINAL_POSITION",
                table.replace("'", "''")
            )
        };

        let result = self.query(&query, &[]).await?;

        if result.rows.is_empty() {
            return Ok(None);
        }

        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Some(PrimaryKeyInfo {
            name: Some("PRIMARY".to_string()),
            columns,
        }))
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let schema = schema.or(self.default_database());
        let mut constraints = Vec::new();

        // Query CHECK constraints (MySQL 8.0.16+)
        let check_query = if let Some(db) = schema {
            format!(
                "SELECT 
                    cc.CONSTRAINT_NAME,
                    cc.CHECK_CLAUSE
                 FROM information_schema.CHECK_CONSTRAINTS cc
                 JOIN information_schema.TABLE_CONSTRAINTS tc 
                   ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA 
                   AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                 WHERE tc.TABLE_SCHEMA = '{}' AND tc.TABLE_NAME = '{}'
                   AND tc.CONSTRAINT_TYPE = 'CHECK'
                 ORDER BY cc.CONSTRAINT_NAME",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT 
                    cc.CONSTRAINT_NAME,
                    cc.CHECK_CLAUSE
                 FROM information_schema.CHECK_CONSTRAINTS cc
                 JOIN information_schema.TABLE_CONSTRAINTS tc 
                   ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA 
                   AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                 WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = '{}'
                   AND tc.CONSTRAINT_TYPE = 'CHECK'
                 ORDER BY cc.CONSTRAINT_NAME",
                table.replace("'", "''")
            )
        };

        // CHECK_CONSTRAINTS table only exists in MySQL 8.0.16+
        // Silently ignore errors for older versions
        if let Ok(result) = self.query(&check_query, &[]).await {
            for row in &result.rows {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                // Skip auto-generated NOT NULL constraints
                if name.ends_with("_chk") || !name.contains("chk") {
                    constraints.push(ConstraintInfo {
                        name,
                        constraint_type: ConstraintType::Check,
                        columns: Vec::new(), // CHECK constraints don't have specific columns in MySQL info
                        definition,
                    });
                }
            }
        }

        // Also include UNIQUE constraints
        let unique_query = if let Some(db) = schema {
            format!(
                "SELECT 
                    tc.CONSTRAINT_NAME,
                    GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
                 FROM information_schema.TABLE_CONSTRAINTS tc
                 JOIN information_schema.KEY_COLUMN_USAGE kcu
                   ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                   AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                   AND tc.TABLE_NAME = kcu.TABLE_NAME
                 WHERE tc.TABLE_SCHEMA = '{}' AND tc.TABLE_NAME = '{}'
                   AND tc.CONSTRAINT_TYPE = 'UNIQUE'
                 GROUP BY tc.CONSTRAINT_NAME
                 ORDER BY tc.CONSTRAINT_NAME",
                db.replace("'", "''"),
                table.replace("'", "''")
            )
        } else {
            format!(
                "SELECT 
                    tc.CONSTRAINT_NAME,
                    GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
                 FROM information_schema.TABLE_CONSTRAINTS tc
                 JOIN information_schema.KEY_COLUMN_USAGE kcu
                   ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                   AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                   AND tc.TABLE_NAME = kcu.TABLE_NAME
                 WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = '{}'
                   AND tc.CONSTRAINT_TYPE = 'UNIQUE'
                 GROUP BY tc.CONSTRAINT_NAME
                 ORDER BY tc.CONSTRAINT_NAME",
                table.replace("'", "''")
            )
        };

        if let Ok(result) = self.query(&unique_query, &[]).await {
            for row in &result.rows {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let columns_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let columns: Vec<String> = columns_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                constraints.push(ConstraintInfo {
                    name,
                    constraint_type: ConstraintType::Unique,
                    columns,
                    definition: None,
                });
            }
        }

        Ok(constraints)
    }

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION, ROUTINE_COMMENT
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = '{}' AND ROUTINE_TYPE = 'FUNCTION'
                 ORDER BY ROUTINE_NAME",
                db.replace("'", "''")
            )
        } else {
            "SELECT ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION, ROUTINE_COMMENT
             FROM information_schema.ROUTINES
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION'
             ORDER BY ROUTINE_NAME"
                .to_string()
        };

        let result = self.query(&query, &[]).await?;

        let functions = result
            .rows
            .iter()
            .map(|row| {
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

                FunctionInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    language: "SQL".to_string(),
                    return_type,
                    parameters: vec![],
                    definition,
                    owner: None,
                    comment,
                }
            })
            .collect();

        Ok(functions)
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            format!(
                "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = '{}' AND ROUTINE_TYPE = 'PROCEDURE'
                 ORDER BY ROUTINE_NAME",
                db.replace("'", "''")
            )
        } else {
            "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT
             FROM information_schema.ROUTINES
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE'
             ORDER BY ROUTINE_NAME"
                .to_string()
        };

        let result = self.query(&query, &[]).await?;

        let procedures = result
            .rows
            .iter()
            .map(|row| {
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

                ProcedureInfo {
                    name,
                    schema: schema.map(|s| s.to_string()),
                    language: "SQL".to_string(),
                    parameters: vec![],
                    definition,
                    owner: None,
                    comment,
                }
            })
            .collect();

        Ok(procedures)
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let schema = schema.or(self.default_database());
        let query = if let Some(db) = schema {
            if let Some(tbl) = table {
                format!(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                     FROM information_schema.TRIGGERS
                     WHERE TRIGGER_SCHEMA = '{}' AND EVENT_OBJECT_TABLE = '{}'
                     ORDER BY TRIGGER_NAME",
                    db.replace("'", "''"),
                    tbl.replace("'", "''")
                )
            } else {
                format!(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                     FROM information_schema.TRIGGERS
                     WHERE TRIGGER_SCHEMA = '{}'
                     ORDER BY TRIGGER_NAME",
                    db.replace("'", "''")
                )
            }
        } else if let Some(tbl) = table {
            format!(
                "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
                 FROM information_schema.TRIGGERS
                 WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = '{}'
                 ORDER BY TRIGGER_NAME",
                tbl.replace("'", "''")
            )
        } else {
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
             FROM information_schema.TRIGGERS
             WHERE TRIGGER_SCHEMA = DATABASE()
             ORDER BY TRIGGER_NAME".to_string()
        };

        let result = self.query(&query, &[]).await?;

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

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        // MySQL doesn't have sequences in the PostgreSQL sense
        // AUTO_INCREMENT is the equivalent
        Ok(Vec::new())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        // MySQL doesn't have custom types like PostgreSQL
        Ok(Vec::new())
    }

    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let schema = schema.or(self.default_database());

        let query = if let Some(db) = schema {
            format!(
                "SELECT
                    TABLE_NAME,
                    TABLE_TYPE,
                    TABLE_ROWS,
                    DATA_LENGTH,
                    ENGINE,
                    CREATE_TIME,
                    UPDATE_TIME,
                    TABLE_COLLATION,
                    TABLE_COMMENT
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = '{}'
                   AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                 ORDER BY TABLE_NAME",
                db.replace("'", "''")
            )
        } else {
            "SELECT
                TABLE_NAME,
                TABLE_TYPE,
                TABLE_ROWS,
                DATA_LENGTH,
                ENGINE,
                CREATE_TIME,
                UPDATE_TIME,
                TABLE_COLLATION,
                TABLE_COMMENT
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
             ORDER BY TABLE_NAME"
                .to_string()
        };

        let result = self.query(&query, &[]).await?;

        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(100.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("data_length", "Data Length")
                .width(100.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("engine", "Engine")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("created_date", "Created Date")
                .width(160.0)
                .min_width(100.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("modified_date", "Modified Date")
                .width(160.0)
                .min_width(100.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("collation", "Collation")
                .width(140.0)
                .min_width(80.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(200.0)
                .min_width(80.0)
                .resizable(true),
        ];

        // Query column indices:
        // 0: TABLE_NAME, 1: TABLE_TYPE, 2: TABLE_ROWS, 3: DATA_LENGTH,
        // 4: ENGINE, 5: CREATE_TIME, 6: UPDATE_TIME, 7: TABLE_COLLATION, 8: TABLE_COMMENT
        let rows = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let table_type_str = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("BASE TABLE");

                let object_type = if table_type_str == "VIEW" {
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

                let created_date = row
                    .get(5)
                    .map(|v| v.to_string())
                    .filter(|s| s != "NULL")
                    .unwrap_or_else(|| "-".to_string());

                let modified_date = row
                    .get(6)
                    .map(|v| v.to_string())
                    .filter(|s| s != "NULL")
                    .unwrap_or_else(|| "-".to_string());

                let collation = row
                    .get(7)
                    .and_then(|v| v.as_str())
                    .filter(|s| *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                let comment = row
                    .get(8)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty() && *s != "NULL")
                    .unwrap_or("-")
                    .to_string();

                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), name.clone());
                values.insert("row_count".to_string(), row_count);
                values.insert("data_length".to_string(), data_length);
                values.insert("engine".to_string(), engine);
                values.insert("created_date".to_string(), created_date);
                values.insert("modified_date".to_string(), modified_date);
                values.insert("collation".to_string(), collation);
                values.insert("comment".to_string(), comment);

                ObjectsPanelRow {
                    name,
                    object_type: object_type.to_string(),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData { columns, rows })
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let schema_prefix = object
            .schema
            .as_ref()
            .map(|s| format!("`{}`.", s))
            .unwrap_or_default();

        match object.object_type {
            ObjectType::Table => {
                let query = format!(
                    "SHOW CREATE TABLE {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(1))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!("Could not get DDL for table '{}'", object.name))
                    })
            }
            ObjectType::View => {
                let query = format!(
                    "SHOW CREATE VIEW {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(1))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!("Could not get DDL for view '{}'", object.name))
                    })
            }
            ObjectType::Function => {
                let query = format!(
                    "SHOW CREATE FUNCTION {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(2))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!(
                            "Could not get DDL for function '{}'",
                            object.name
                        ))
                    })
            }
            ObjectType::Procedure => {
                let query = format!(
                    "SHOW CREATE PROCEDURE {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(2))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!(
                            "Could not get DDL for procedure '{}'",
                            object.name
                        ))
                    })
            }
            ObjectType::Trigger => {
                let query = format!(
                    "SHOW CREATE TRIGGER {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(2))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!("Could not get DDL for trigger '{}'", object.name))
                    })
            }
            ObjectType::Database => {
                let query = format!("SHOW CREATE DATABASE `{}`", object.name.replace("`", "``"));
                let result = self.query(&query, &[]).await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(1))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::Query(format!(
                            "Could not get DDL for database '{}'",
                            object.name
                        ))
                    })
            }
            ObjectType::Index => {
                // MySQL doesn't have SHOW CREATE INDEX, construct from SHOW INDEX
                Err(ZqlzError::NotImplemented(
                    "DDL generation for indexes requires table context in MySQL".into(),
                ))
            }
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} not supported in MySQL",
                object.object_type
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
                let fk_query = if let Some(db) = schema {
                    format!(
                        "SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
                         FROM information_schema.KEY_COLUMN_USAGE
                         WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'
                           AND REFERENCED_TABLE_NAME IS NOT NULL
                         GROUP BY REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME",
                        db.replace("'", "''"),
                        name.replace("'", "''")
                    )
                } else {
                    format!(
                        "SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
                         FROM information_schema.KEY_COLUMN_USAGE
                         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'
                           AND REFERENCED_TABLE_NAME IS NOT NULL
                         GROUP BY REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME",
                        name.replace("'", "''")
                    )
                };

                if let Ok(result) = self.query(&fk_query, &[]).await {
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
                                },
                                dependency_type: DependencyType::Normal,
                            });
                        }
                    }
                }

                // Get triggers that depend on this table
                let trigger_query = if let Some(db) = schema {
                    format!(
                        "SELECT TRIGGER_NAME
                         FROM information_schema.TRIGGERS
                         WHERE TRIGGER_SCHEMA = '{}' AND EVENT_OBJECT_TABLE = '{}'",
                        db.replace("'", "''"),
                        name.replace("'", "''")
                    )
                } else {
                    format!(
                        "SELECT TRIGGER_NAME
                         FROM information_schema.TRIGGERS
                         WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = '{}'",
                        name.replace("'", "''")
                    )
                };

                if let Ok(result) = self.query(&trigger_query, &[]).await {
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
                let usage_query = if let Some(db) = schema {
                    format!(
                        "SELECT TABLE_SCHEMA, TABLE_NAME
                         FROM information_schema.VIEW_TABLE_USAGE
                         WHERE VIEW_SCHEMA = '{}' AND VIEW_NAME = '{}'",
                        db.replace("'", "''"),
                        name.replace("'", "''")
                    )
                } else {
                    format!(
                        "SELECT TABLE_SCHEMA, TABLE_NAME
                         FROM information_schema.VIEW_TABLE_USAGE
                         WHERE VIEW_SCHEMA = DATABASE() AND VIEW_NAME = '{}'",
                        name.replace("'", "''")
                    )
                };

                if let Ok(result) = self.query(&usage_query, &[]).await {
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

fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

fn format_bytes_human_readable(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;

    let bytes_f64 = bytes as f64;
    if bytes_f64 >= TB {
        format!("{:.1} TB", bytes_f64 / TB)
    } else if bytes_f64 >= GB {
        format!("{:.1} GB", bytes_f64 / GB)
    } else if bytes_f64 >= MB {
        format!("{:.1} MB", bytes_f64 / MB)
    } else if bytes_f64 >= KB {
        format!("{:.1} KB", bytes_f64 / KB)
    } else {
        format!("{} B", bytes)
    }
}
