//! MS SQL Server schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ParameterInfo,
    ParameterMode, PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection,
    SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent, TriggerForEach, TriggerInfo,
    TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use super::MssqlConnection;

#[async_trait]
impl SchemaIntrospection for MssqlConnection {
    /// List all databases on the SQL Server instance
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT 
                    name,
                    SUSER_SNAME(owner_sid) as owner,
                    CAST(SUM(size) * 8 * 1024 AS BIGINT) as size_bytes
                 FROM sys.databases d
                 LEFT JOIN sys.master_files mf ON d.database_id = mf.database_id
                 WHERE d.database_id > 4  -- Exclude system databases (master, tempdb, model, msdb)
                 GROUP BY d.name, d.owner_sid
                 ORDER BY d.name",
                &[],
            )
            .await?;

        let databases = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let owner = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                let size_bytes = row.get(2).and_then(|v| v.as_i64());

                DatabaseInfo {
                    name,
                    owner,
                    encoding: None, // SQL Server doesn't have database-level encoding like PostgreSQL
                    size_bytes,
                    comment: None,
                }
            })
            .collect();

        Ok(databases)
    }

    /// List all schemas in the current database
    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let result = self
            .query(
                "SELECT 
                    s.name,
                    p.name as owner
                 FROM sys.schemas s
                 LEFT JOIN sys.database_principals p ON s.principal_id = p.principal_id
                 WHERE s.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 
                                      'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                                      'db_backupoperator', 'db_datareader', 'db_datawriter',
                                      'db_denydatareader', 'db_denydatawriter')
                 ORDER BY s.name",
                &[],
            )
            .await?;

        let schemas = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let owner = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

                SchemaInfo {
                    name,
                    owner,
                    comment: None,
                }
            })
            .collect();

        Ok(schemas)
    }

    /// List all tables in a schema
    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    t.name AS table_name,
                    CASE 
                        WHEN t.type = 'U' THEN 'TABLE'
                        WHEN t.type = 'V' THEN 'VIEW'
                        ELSE 'TABLE'
                    END AS table_type,
                    p.rows AS row_count,
                    (SELECT SUM(a.total_pages) * 8 * 1024 
                     FROM sys.partitions sp 
                     JOIN sys.allocation_units a ON sp.partition_id = a.container_id 
                     WHERE sp.object_id = t.object_id) AS size_bytes,
                    (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0) AS index_count,
                    (SELECT COUNT(*) FROM sys.triggers tr WHERE tr.parent_id = t.object_id) AS trigger_count
                 FROM sys.tables t
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
                 WHERE s.name = @P1 AND t.type = 'U'
                 ORDER BY t.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_type_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("TABLE");
                let row_count = row.get(2).and_then(|v| v.as_i64());
                let size_bytes = row.get(3).and_then(|v| v.as_i64());
                let index_count = row.get(4).and_then(|v| v.as_i64());
                let trigger_count = row.get(5).and_then(|v| v.as_i64());

                let table_type = match table_type_str {
                    "TABLE" => TableType::Table,
                    "VIEW" => TableType::View,
                    _ => TableType::Table,
                };

                TableInfo {
                    name,
                    schema: Some(schema.to_string()),
                    table_type,
                    owner: None,
                    row_count,
                    size_bytes,
                    comment: None,
                    index_count,
                    trigger_count,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(tables)
    }

    /// List all views in a schema
    #[tracing::instrument(skip(self))]
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    v.name AS view_name,
                    OBJECT_DEFINITION(v.object_id) AS definition
                 FROM sys.views v
                 INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                 WHERE s.name = @P1
                 ORDER BY v.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

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
                    schema: Some(schema.to_string()),
                    is_materialized: false, // SQL Server has indexed views, not materialized views
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    /// Get detailed table information
    #[tracing::instrument(skip(self))]
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let schema = schema.unwrap_or("dbo");
        let tables = self.list_tables(Some(schema)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        let columns = self.get_columns(Some(schema), name).await?;
        let indexes = self.get_indexes(Some(schema), name).await?;
        let foreign_keys = self.get_foreign_keys(Some(schema), name).await?;
        let primary_key = self.get_primary_key(Some(schema), name).await?;
        let constraints = self.get_constraints(Some(schema), name).await?;
        let triggers = self.list_triggers(Some(schema), Some(name)).await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints,
            triggers,
        })
    }

    /// Get columns for a table
    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    c.name AS column_name,
                    c.column_id AS ordinal,
                    TYPE_NAME(c.user_type_id) AS data_type,
                    c.is_nullable,
                    dc.definition AS default_value,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_identity,
                    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
                    CASE WHEN uq.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_unique
                 FROM sys.columns c
                 INNER JOIN sys.tables t ON c.object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                 LEFT JOIN (
                     SELECT ic.object_id, ic.column_id 
                     FROM sys.index_columns ic
                     INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                     WHERE i.is_primary_key = 1
                 ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
                 LEFT JOIN (
                     SELECT ic.object_id, ic.column_id 
                     FROM sys.index_columns ic
                     INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                     WHERE i.is_unique = 1 AND i.is_primary_key = 0
                 ) uq ON c.object_id = uq.object_id AND c.column_id = uq.column_id
                 WHERE s.name = @P1 AND t.name = @P2
                 ORDER BY c.column_id",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

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
                let is_nullable = row.get(3).and_then(|v| v.as_bool()).unwrap_or(true);
                let default_value = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let max_length = row.get(5).and_then(|v| v.as_i64());
                let precision = row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32);
                let scale = row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32);
                let is_identity = row.get(8).and_then(|v| v.as_bool()).unwrap_or(false);
                let is_primary_key = row.get(9).and_then(|v| v.as_i64()).unwrap_or(0) == 1;
                let is_unique = row.get(10).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

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
                    is_auto_increment: is_identity,
                    is_unique,
                    foreign_key: None, // Will be filled if needed
                    comment: None,
                    ..Default::default()
                }
            })
            .collect();

        Ok(columns)
    }

    /// Get indexes for a table
    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    i.name AS index_name,
                    i.is_unique,
                    i.is_primary_key,
                    i.type_desc AS index_type,
                    STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
                 FROM sys.indexes i
                 INNER JOIN sys.tables t ON i.object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                 INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                 WHERE s.name = @P1 AND t.name = @P2 AND i.name IS NOT NULL
                 GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc
                 ORDER BY i.name",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let indexes = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_unique = row.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                let is_primary = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                let index_type = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("NONCLUSTERED")
                    .to_string();
                let columns_str = row.get(4).and_then(|v| v.as_str()).unwrap_or("");
                let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();

                IndexInfo {
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                    comment: None,
                    ..Default::default()
                }
            })
            .collect();

        Ok(indexes)
    }

    /// Get foreign keys for a table
    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    fk.name AS constraint_name,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
                    OBJECT_NAME(fkc.referenced_object_id) AS referenced_table,
                    SCHEMA_NAME(ref_t.schema_id) AS referenced_schema,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column,
                    fk.update_referential_action_desc AS on_update,
                    fk.delete_referential_action_desc AS on_delete
                 FROM sys.foreign_keys fk
                 INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                 INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 INNER JOIN sys.tables ref_t ON fkc.referenced_object_id = ref_t.object_id
                 WHERE s.name = @P1 AND t.name = @P2
                 ORDER BY fk.name, fkc.constraint_column_id",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        // Group by constraint name since a FK can span multiple columns
        let mut fk_map: std::collections::HashMap<String, ForeignKeyInfo> =
            std::collections::HashMap::new();

        for row in result.rows.iter() {
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
            let ref_schema = row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
            let ref_column = row
                .get(4)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let on_update_str = row.get(5).and_then(|v| v.as_str()).unwrap_or("NO_ACTION");
            let on_delete_str = row.get(6).and_then(|v| v.as_str()).unwrap_or("NO_ACTION");

            fk_map
                .entry(name.clone())
                .and_modify(|fk| {
                    fk.columns.push(column.clone());
                    fk.referenced_columns.push(ref_column.clone());
                })
                .or_insert(ForeignKeyInfo {
                    name,
                    columns: vec![column],
                    referenced_table: ref_table,
                    referenced_schema: ref_schema,
                    referenced_columns: vec![ref_column],
                    on_update: parse_fk_action(on_update_str),
                    on_delete: parse_fk_action(on_delete_str),
                    is_deferrable: false,
                    initially_deferred: false,
                });
        }

        Ok(fk_map.into_values().collect())
    }

    /// Get primary key for a table
    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    i.name AS constraint_name,
                    STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
                 FROM sys.indexes i
                 INNER JOIN sys.tables t ON i.object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                 INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                 WHERE s.name = @P1 AND t.name = @P2 AND i.is_primary_key = 1
                 GROUP BY i.name",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
            let columns_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let columns: Vec<String> = columns_str.split(',').map(|s| s.to_string()).collect();

            Ok(Some(PrimaryKeyInfo { name, columns }))
        } else {
            Ok(None)
        }
    }

    /// Get constraints for a table
    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    cc.name AS constraint_name,
                    'CHECK' AS constraint_type,
                    cc.definition
                 FROM sys.check_constraints cc
                 INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.name = @P2
                 UNION ALL
                 SELECT 
                    i.name AS constraint_name,
                    'UNIQUE' AS constraint_type,
                    NULL AS definition
                 FROM sys.indexes i
                 INNER JOIN sys.tables t ON i.object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.name = @P2 AND i.is_unique = 1 AND i.is_primary_key = 0
                 ORDER BY constraint_name",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            )
            .await?;

        let constraints = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let constraint_type_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("CHECK");
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                let constraint_type = match constraint_type_str {
                    "CHECK" => ConstraintType::Check,
                    "UNIQUE" => ConstraintType::Unique,
                    _ => ConstraintType::Check,
                };

                ConstraintInfo {
                    name,
                    constraint_type,
                    columns: Vec::new(), // Would need separate query
                    definition,
                }
            })
            .collect();

        Ok(constraints)
    }

    /// List all functions in a schema
    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    o.name AS function_name,
                    TYPE_NAME(ret.user_type_id) AS return_type,
                    OBJECT_DEFINITION(o.object_id) AS definition
                 FROM sys.objects o
                 INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                 LEFT JOIN sys.parameters ret ON o.object_id = ret.object_id AND ret.parameter_id = 0
                 WHERE s.name = @P1 AND o.type IN ('FN', 'IF', 'TF')
                 ORDER BY o.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

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
                    .unwrap_or("void")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                FunctionInfo {
                    name,
                    schema: Some(schema.to_string()),
                    language: "T-SQL".to_string(),
                    return_type,
                    parameters: Vec::new(), // Would need separate query for params
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(functions)
    }

    /// List all procedures in a schema
    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    p.name AS procedure_name,
                    OBJECT_DEFINITION(p.object_id) AS definition
                 FROM sys.procedures p
                 INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
                 WHERE s.name = @P1
                 ORDER BY p.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let mut procedures: Vec<ProcedureInfo> = Vec::new();

        for row in result.rows.iter() {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());

            // Get parameters for this procedure
            let parameters = self.get_procedure_parameters(schema, &name).await?;

            procedures.push(ProcedureInfo {
                name,
                schema: Some(schema.to_string()),
                language: "T-SQL".to_string(),
                parameters,
                definition,
                owner: None,
                comment: None,
            });
        }

        Ok(procedures)
    }

    /// List all triggers in a schema
    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let schema = schema.unwrap_or("dbo");

        let result = if let Some(tbl) = table {
            self.query(
                "SELECT 
                    tr.name AS trigger_name,
                    OBJECT_NAME(tr.parent_id) AS table_name,
                    CASE 
                        WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
                        WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsAfterTrigger') = 1 THEN 'AFTER'
                        ELSE 'FOR'
                    END AS timing,
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') = 1 THEN ',UPDATE' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') = 1 THEN ',DELETE' ELSE '' END AS events,
                    OBJECT_DEFINITION(tr.object_id) AS definition,
                    CASE WHEN tr.is_disabled = 0 THEN 1 ELSE 0 END AS is_enabled
                 FROM sys.triggers tr
                 INNER JOIN sys.tables t ON tr.parent_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.name = @P2
                 ORDER BY tr.name",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(tbl.to_string()),
                ],
            )
            .await?
        } else {
            self.query(
                "SELECT 
                    tr.name AS trigger_name,
                    OBJECT_NAME(tr.parent_id) AS table_name,
                    CASE 
                        WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
                        WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsAfterTrigger') = 1 THEN 'AFTER'
                        ELSE 'FOR'
                    END AS timing,
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') = 1 THEN ',UPDATE' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') = 1 THEN ',DELETE' ELSE '' END AS events,
                    OBJECT_DEFINITION(tr.object_id) AS definition,
                    CASE WHEN tr.is_disabled = 0 THEN 1 ELSE 0 END AS is_enabled
                 FROM sys.triggers tr
                 INNER JOIN sys.tables t ON tr.parent_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1
                 ORDER BY tr.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        };

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
                let timing_str = row.get(2).and_then(|v| v.as_str()).unwrap_or("AFTER");
                let events_str = row.get(3).and_then(|v| v.as_str()).unwrap_or("");
                let definition = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let is_enabled = row.get(5).and_then(|v| v.as_i64()).unwrap_or(1) == 1;

                let timing = match timing_str {
                    "AFTER" | "FOR" => TriggerTiming::After,
                    "INSTEAD OF" => TriggerTiming::InsteadOf,
                    _ => TriggerTiming::After,
                };

                let events: Vec<TriggerEvent> = events_str
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| match s.trim() {
                        "INSERT" => Some(TriggerEvent::Insert),
                        "UPDATE" => Some(TriggerEvent::Update),
                        "DELETE" => Some(TriggerEvent::Delete),
                        _ => None,
                    })
                    .collect();

                TriggerInfo {
                    name,
                    schema: Some(schema.to_string()),
                    table_name,
                    timing,
                    events,
                    for_each: TriggerForEach::Row, // SQL Server triggers are always FOR EACH STATEMENT effectively
                    definition,
                    enabled: is_enabled,
                    comment: None,
                }
            })
            .collect();

        Ok(triggers)
    }

    /// List all sequences in a schema
    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    seq.name AS sequence_name,
                    TYPE_NAME(seq.user_type_id) AS data_type,
                    CAST(seq.start_value AS BIGINT) AS start_value,
                    CAST(seq.minimum_value AS BIGINT) AS min_value,
                    CAST(seq.maximum_value AS BIGINT) AS max_value,
                    CAST(seq.increment AS BIGINT) AS increment_by,
                    CAST(seq.current_value AS BIGINT) AS current_value
                 FROM sys.sequences seq
                 INNER JOIN sys.schemas s ON seq.schema_id = s.schema_id
                 WHERE s.name = @P1
                 ORDER BY seq.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let sequences = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let data_type = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("bigint")
                    .to_string();
                let start_value = row.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
                let min_value = row.get(3).and_then(|v| v.as_i64()).unwrap_or(1);
                let max_value = row.get(4).and_then(|v| v.as_i64()).unwrap_or(i64::MAX);
                let increment_by = row.get(5).and_then(|v| v.as_i64()).unwrap_or(1);
                let current_value = row.get(6).and_then(|v| v.as_i64());

                SequenceInfo {
                    name,
                    schema: Some(schema.to_string()),
                    data_type,
                    start_value,
                    min_value,
                    max_value,
                    increment_by,
                    current_value,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(sequences)
    }

    /// List all custom types in a schema (user-defined types, table types)
    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT 
                    t.name AS type_name,
                    CASE 
                        WHEN t.is_table_type = 1 THEN 'TABLE'
                        WHEN t.is_user_defined = 1 THEN 'ALIAS'
                        ELSE 'BASE'
                    END AS type_kind
                 FROM sys.types t
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.is_user_defined = 1
                 ORDER BY t.name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let types = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let type_kind_str = row.get(1).and_then(|v| v.as_str()).unwrap_or("BASE");

                let type_kind = match type_kind_str {
                    "TABLE" => TypeKind::Composite, // Table types are similar to composite
                    "ALIAS" => TypeKind::Domain,    // Alias types are similar to domains
                    _ => TypeKind::Base,
                };

                TypeInfo {
                    name,
                    schema: Some(schema.to_string()),
                    type_kind,
                    values: None, // SQL Server doesn't have native enums
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(types)
    }

    /// Generate DDL for a database object
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        match object.object_type {
            zqlz_core::ObjectType::Table => {
                let schema = object.schema.as_deref().unwrap_or("dbo");
                let table = self.get_table(Some(schema), &object.name).await?;
                Ok(generate_table_ddl(&table, schema))
            }
            zqlz_core::ObjectType::View => {
                let schema = object.schema.as_deref().unwrap_or("dbo");
                let views = self.list_views(Some(schema)).await?;
                let view = views
                    .into_iter()
                    .find(|v| v.name == object.name)
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("View '{}' not found", object.name))
                    })?;
                Ok(view.definition.unwrap_or_else(|| {
                    format!(
                        "-- View definition not available for {}.{}",
                        schema, object.name
                    )
                }))
            }
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation not yet implemented for {:?}",
                object.object_type
            ))),
        }
    }

    /// Get object dependencies
    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        // SQL Server has sys.sql_expression_dependencies for this, but it's complex
        Ok(Vec::new())
    }
}

impl MssqlConnection {
    /// Get parameters for a stored procedure
    async fn get_procedure_parameters(
        &self,
        schema: &str,
        procedure_name: &str,
    ) -> Result<Vec<ParameterInfo>> {
        let result = self
            .query(
                "SELECT 
                    p.name AS param_name,
                    TYPE_NAME(p.user_type_id) AS data_type,
                    p.is_output,
                    p.parameter_id AS ordinal,
                    p.has_default_value,
                    CAST(p.default_value AS NVARCHAR(MAX)) AS default_value
                 FROM sys.parameters p
                 INNER JOIN sys.procedures pr ON p.object_id = pr.object_id
                 INNER JOIN sys.schemas s ON pr.schema_id = s.schema_id
                 WHERE s.name = @P1 AND pr.name = @P2 AND p.parameter_id > 0
                 ORDER BY p.parameter_id",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(procedure_name.to_string()),
                ],
            )
            .await?;

        let params = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim_start_matches('@').to_string());
                let data_type = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_output = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                let ordinal = row.get(3).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let has_default = row.get(4).and_then(|v| v.as_bool()).unwrap_or(false);
                let default_value = if has_default {
                    row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                };

                let mode = if is_output {
                    ParameterMode::Out
                } else {
                    ParameterMode::In
                };

                ParameterInfo {
                    name,
                    data_type,
                    mode,
                    default_value,
                    ordinal,
                }
            })
            .collect();

        Ok(params)
    }
}

/// Parse SQL Server foreign key action
pub(crate) fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET_NULL" | "SET NULL" => ForeignKeyAction::SetNull,
        "SET_DEFAULT" | "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "NO_ACTION" | "NO ACTION" => ForeignKeyAction::NoAction,
        _ => ForeignKeyAction::NoAction,
    }
}

/// Generate CREATE TABLE DDL from TableDetails
pub(crate) fn generate_table_ddl(table: &TableDetails, schema: &str) -> String {
    let mut ddl = format!("CREATE TABLE [{}].[{}] (\n", schema, table.info.name);

    // Add columns
    for (i, col) in table.columns.iter().enumerate() {
        let comma = if i < table.columns.len() - 1
            || table.primary_key.is_some()
            || !table.constraints.is_empty()
        {
            ","
        } else {
            ""
        };

        let null_str = if col.nullable { "NULL" } else { "NOT NULL" };
        let identity_str = if col.is_auto_increment {
            " IDENTITY(1,1)"
        } else {
            ""
        };
        let default_str = col
            .default_value
            .as_ref()
            .map(|d| format!(" DEFAULT {}", d))
            .unwrap_or_default();

        ddl.push_str(&format!(
            "    [{}] {}{}{} {}{}\n",
            col.name, col.data_type, identity_str, default_str, null_str, comma
        ));
    }

    // Add primary key constraint
    if let Some(pk) = &table.primary_key {
        let pk_name = pk.name.as_deref().unwrap_or("PK_unnamed");
        let pk_cols = pk
            .columns
            .iter()
            .map(|c| format!("[{}]", c))
            .collect::<Vec<_>>()
            .join(", ");
        let comma = if !table.constraints.is_empty() {
            ","
        } else {
            ""
        };
        ddl.push_str(&format!(
            "    CONSTRAINT [{}] PRIMARY KEY ({}){}\n",
            pk_name, pk_cols, comma
        ));
    }

    ddl.push_str(");\n");
    ddl
}
