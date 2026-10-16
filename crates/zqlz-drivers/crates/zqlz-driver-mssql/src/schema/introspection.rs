use std::collections::HashMap;

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject, Dependency,
    ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest, ObjectFormSpec,
    ObjectFormSpecRequest, ObjectKindSupport, ObjectsPanelData, ObjectsPanelManifest,
    ParameterInfo, ParameterMode, PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo,
    SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent,
    TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use super::object_forms::{
    generate_ddl_definition_unavailable_message, generate_ddl_not_found_message,
    generate_table_ddl, mssql_object_form_ddl, mssql_object_form_spec,
};
use super::objects_panel::{
    mssql_database_objects_panel_data, mssql_function_objects_panel_data,
    mssql_objects_panel_columns, mssql_objects_panel_manifest, mssql_procedure_objects_panel_data,
    mssql_schema_objects_panel_data, mssql_sequence_objects_panel_data,
    mssql_table_objects_panel_data, mssql_trigger_objects_panel_data,
    mssql_type_objects_panel_data, mssql_view_objects_panel_data,
};
use crate::connection::MssqlConnection;

pub(crate) const MSSQL_FUNCTION_DDL_SQL: &str = "SELECT
                    OBJECT_DEFINITION(o.object_id) AS definition
                 FROM sys.objects o
                 INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                 WHERE s.name = @P1 AND o.name = @P2 AND o.type IN ('FN', 'IF', 'TF', 'FS', 'FT')";

pub(crate) const MSSQL_PROCEDURE_DDL_SQL: &str = "SELECT
                    OBJECT_DEFINITION(p.object_id) AS definition
                 FROM sys.procedures p
                 INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
                 WHERE s.name = @P1 AND p.name = @P2";

pub(crate) const MSSQL_TRIGGER_DDL_SQL: &str = "SELECT
                    OBJECT_DEFINITION(tr.object_id) AS definition
                 FROM sys.triggers tr
                 INNER JOIN sys.schemas s ON tr.schema_id = s.schema_id
                 WHERE s.name = @P1 AND tr.name = @P2";

#[async_trait]
impl SchemaIntrospection for MssqlConnection {
    fn object_kind_support(&self) -> Option<ObjectKindSupport> {
        Some(crate::driver::OBJECT_KINDS)
    }

    fn default_schema_name(&self) -> Option<String> {
        Some("dbo".to_string())
    }

    /// List all databases on the SQL Server instance
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT 
                    d.name AS name,
                    SUSER_SNAME(d.owner_sid) as owner,
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
        // Without a schema, every user schema is listed; names outside `dbo`
        // come back as `schema.name`, like PostgreSQL's.
        let Some(schema) = schema else {
            let mut tables = Vec::new();
            for user_schema in self.list_schemas().await? {
                for mut table in self.list_tables(Some(&user_schema.name)).await? {
                    table.name = qualify_outside_dbo(&user_schema.name, table.name);
                    tables.push(table);
                }
            }
            return Ok(tables);
        };
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
        let Some(schema) = schema else {
            let mut views = Vec::new();
            for user_schema in self.list_schemas().await? {
                for mut view in self.list_views(Some(&user_schema.name)).await? {
                    view.name = qualify_outside_dbo(&user_schema.name, view.name);
                    views.push(view);
                }
            }
            return Ok(views);
        };
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

    /// The objects panel loads every kind at once; building it from the
    /// per-kind rows keeps the driver's own columns.
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
        for kind_id in [
            "database",
            "schema",
            "table",
            "view",
            "function",
            "procedure",
            "trigger",
            "sequence",
            "type",
        ] {
            match self.list_objects_panel_data_for_kind(schema, kind_id).await {
                Ok(kind_data) => data.rows.extend(kind_data.rows),
                Err(error) => {
                    tracing::warn!(%error, kind_id, "Failed to load objects panel kind");
                }
            }
        }
        Ok(data)
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "database" => mssql_database_objects_panel_data(self.list_databases().await?),
            "schema" => mssql_schema_objects_panel_data(self.list_schemas().await?),
            "table" => mssql_table_objects_panel_data(self.list_tables(schema).await?),
            "view" => mssql_view_objects_panel_data(self.list_views(schema).await?),
            "function" => mssql_function_objects_panel_data(self.list_functions(schema).await?),
            "procedure" => mssql_procedure_objects_panel_data(self.list_procedures(schema).await?),
            "trigger" => mssql_trigger_objects_panel_data(self.list_triggers(schema, None).await?),
            "sequence" => mssql_sequence_objects_panel_data(self.list_sequences(schema).await?),
            "type" => mssql_type_objects_panel_data(self.list_types(schema).await?),
            _ => Ok(ObjectsPanelData::new(mssql_objects_panel_columns())),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(mssql_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(mssql_object_form_spec(request))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        mssql_object_form_ddl(request)
    }

    /// Get detailed table information
    #[tracing::instrument(skip(self))]
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        self.schema_engine.get_table(schema, name).await
    }

    /// Get columns for a table
    #[tracing::instrument(skip(self))]
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

    /// Get indexes for a table
    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        self.schema_engine.get_indexes(schema, table).await
    }

    /// Get foreign keys for a table
    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        self.schema_engine.get_foreign_keys(schema, table).await
    }

    /// Get primary key for a table
    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        self.schema_engine.get_primary_key(schema, table).await
    }

    /// Get constraints for a table
    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        self.schema_engine.get_constraints(schema, table).await
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
            zqlz_core::ObjectType::Function => {
                let schema = object.schema.as_deref().unwrap_or("dbo");
                self.fetch_object_ddl(MSSQL_FUNCTION_DDL_SQL, schema, &object.name, "Function")
                    .await
            }
            zqlz_core::ObjectType::Procedure => {
                let schema = object.schema.as_deref().unwrap_or("dbo");
                self.fetch_object_ddl(MSSQL_PROCEDURE_DDL_SQL, schema, &object.name, "Procedure")
                    .await
            }
            zqlz_core::ObjectType::Trigger => {
                let schema = object.schema.as_deref().unwrap_or("dbo");
                self.fetch_object_ddl(MSSQL_TRIGGER_DDL_SQL, schema, &object.name, "Trigger")
                    .await
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
    async fn fetch_object_ddl(
        &self,
        sql: &str,
        schema: &str,
        object_name: &str,
        object_kind: &str,
    ) -> Result<String> {
        let result = self
            .query(
                sql,
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(object_name.to_string()),
                ],
            )
            .await?;

        if result.rows.is_empty() {
            return Err(ZqlzError::NotFound(generate_ddl_not_found_message(
                object_kind,
                schema,
                object_name,
            )));
        }

        result
            .rows
            .first()
            .and_then(|row| row.get(0).and_then(|value| value.as_str()))
            .map(|definition| definition.to_string())
            .filter(|definition| !definition.trim().is_empty())
            .ok_or_else(|| {
                ZqlzError::Query(generate_ddl_definition_unavailable_message(
                    object_kind,
                    schema,
                    object_name,
                ))
            })
    }

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

fn qualify_outside_dbo(schema: &str, name: String) -> String {
    if schema == "dbo" {
        name
    } else {
        format!("{schema}.{name}")
    }
}
