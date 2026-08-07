//! MS SQL Server schema introspection implementation

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest,
    ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection, ObjectFormSpec,
    ObjectFormSpecRequest, ObjectFormValue, ObjectsPanelAction, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef,
    ObjectsPanelRow, ParameterInfo, ParameterMode, PrimaryKeyInfo, ProcedureInfo, Result,
    SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType,
    TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo,
    ZqlzError,
};

use super::connection::{run_mssql_query, MssqlClient};
use super::MssqlConnection;

/// MS SQL Server implementation of the raw-catalog port. Provides the per-table
/// fetches the shared engine composes; MSSQL's listing, objects-panel,
/// object-form, and DDL logic remain on `MssqlConnection`.
pub struct MssqlCatalog {
    client: MssqlClient,
    capabilities: zqlz_core::CatalogCapabilities,
}

impl MssqlCatalog {
    pub fn new(client: MssqlClient, _database: Option<String>) -> Self {
        let capabilities = zqlz_core::CatalogCapabilities {
            driver_id: "mssql".to_string(),
            server_version: None,
            namespaces: zqlz_core::NamespaceModel::DatabasesAndSchemas {
                default_schema: "dbo".to_string(),
            },
            objects: zqlz_core::ObjectKindSupport::ALL_RELATIONAL,
            auto_increment: zqlz_core::AutoIncrementRules::default(),
            stored_source: Vec::new(),
            deferrable_constraints: false,
            panel_extras: Vec::new(),
        };
        Self {
            client,
            capabilities,
        }
    }

    async fn query(
        &self,
        sql: &str,
        params: &[zqlz_core::Value],
    ) -> Result<zqlz_core::QueryResult> {
        run_mssql_query(&self.client, sql, params).await
    }
}

/// Column query for one relation or for a whole schema.
///
/// `t.name` is appended after the original projection so both variants share one
/// row decoder. The `pk` derived table correlates on `object_id`, so it stays
/// correct without the relation predicate.
fn mssql_columns_sql(single_relation: bool) -> String {
    let relation_filter = if single_relation {
        "AND t.name = @P2"
    } else {
        ""
    };
    let order = if single_relation {
        "ORDER BY c.column_id"
    } else {
        "ORDER BY t.name, c.column_id"
    };

    format!(
        "SELECT c.name, c.column_id, TYPE_NAME(c.user_type_id), c.is_nullable,
                dc.definition, c.max_length, c.precision, c.scale, c.is_identity,
                CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END, t.name
         FROM sys.columns c
         INNER JOIN sys.tables t ON c.object_id = t.object_id
         INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
         LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
         LEFT JOIN (
             SELECT ic.object_id, ic.column_id FROM sys.index_columns ic
             INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
             WHERE i.is_primary_key = 1
         ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
         WHERE s.name = @P1 {relation_filter}
         {order}"
    )
}

fn mssql_column_row(row: &zqlz_core::Row) -> zqlz_core::RawColumnRow {
    let is_identity = row.get(8).and_then(|v| v.as_bool()).unwrap_or(false);
    let is_primary_key = row.get(9).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

    zqlz_core::RawColumnRow {
        name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
        data_type: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        is_nullable: row.get(3).and_then(|v| v.as_bool()).unwrap_or(true),
        default_value: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
        max_length: row.get(5).and_then(|v| v.as_i64()),
        precision: row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32),
        scale: row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32),
        identity: if is_identity {
            zqlz_core::RawIdentity::Declared
        } else {
            zqlz_core::RawIdentity::None
        },
        primary_key_ordinal: is_primary_key.then_some(1),
        ..Default::default()
    }
}

#[async_trait]
impl zqlz_core::CatalogSource for MssqlCatalog {
    fn capabilities(&self) -> &zqlz_core::CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawRelationRow>> {
        let schema = schema.unwrap_or("dbo").to_string();

        let tables = self
            .query(
                "SELECT t.name,
                    p.rows,
                    (SELECT SUM(a.total_pages) * 8 * 1024 FROM sys.partitions sp
                     JOIN sys.allocation_units a ON sp.partition_id = a.container_id
                     WHERE sp.object_id = t.object_id),
                    (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0),
                    (SELECT COUNT(*) FROM sys.triggers tr WHERE tr.parent_id = t.object_id)
                 FROM sys.tables t
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
                 WHERE s.name = @P1 AND t.type = 'U'
                 ORDER BY t.name",
                &[zqlz_core::Value::String(schema.clone())],
            )
            .await?;
        let mut relations: Vec<zqlz_core::RawRelationRow> = tables
            .rows
            .iter()
            .map(|row| {
                let mut relation = zqlz_core::RawRelationRow::new(
                    row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    zqlz_core::TableType::Table,
                );
                relation.schema = Some(schema.clone());
                relation.row_estimate = row.get(1).and_then(|v| v.as_i64());
                relation.size_bytes = row.get(2).and_then(|v| v.as_i64());
                relation.index_count = row.get(3).and_then(|v| v.as_i64());
                relation.trigger_count = row.get(4).and_then(|v| v.as_i64());
                relation
            })
            .collect();

        let views = self
            .query(
                "SELECT v.name, OBJECT_DEFINITION(v.object_id)
                 FROM sys.views v
                 INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                 WHERE s.name = @P1 ORDER BY v.name",
                &[zqlz_core::Value::String(schema.clone())],
            )
            .await?;
        relations.extend(views.rows.iter().map(|row| {
            let mut relation = zqlz_core::RawRelationRow::new(
                row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                zqlz_core::TableType::View,
            );
            relation.schema = Some(schema.clone());
            relation.view_definition = row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
            relation
        }));

        Ok(relations)
    }

    async fn fetch_columns(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawColumnRow>> {
        let schema = relation.schema.as_deref().unwrap_or("dbo");
        let result = self
            .query(
                &mssql_columns_sql(true),
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result.rows.iter().map(mssql_column_row).collect())
    }

    async fn fetch_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawColumnRow>>>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                &mssql_columns_sql(false),
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        let mut columns_by_relation: HashMap<String, Vec<zqlz_core::RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(relation) = row.get(10).and_then(|value| value.as_str()) else {
                continue;
            };
            columns_by_relation
                .entry(relation.to_string())
                .or_default()
                .push(mssql_column_row(row));
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawIndexRow>> {
        let schema = relation.schema.as_deref().unwrap_or("dbo");
        let result = self
            .query(
                "SELECT i.name, i.is_unique, i.is_primary_key, i.type_desc,
                        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
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
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| zqlz_core::RawIndexRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                is_unique: row.get(1).and_then(|v| v.as_bool()).unwrap_or(false),
                is_primary: row.get(2).and_then(|v| v.as_bool()).unwrap_or(false),
                method: row.get(3).and_then(|v| v.as_str()).map(ToString::to_string),
                columns: row
                    .get(4)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect(),
                ..Default::default()
            })
            .collect())
    }

    async fn fetch_foreign_keys(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawForeignKeyRow>> {
        let schema = relation.schema.as_deref().unwrap_or("dbo");
        let result = self
            .query(
                "SELECT fk.name,
                        COL_NAME(fkc.parent_object_id, fkc.parent_column_id),
                        OBJECT_NAME(fkc.referenced_object_id),
                        SCHEMA_NAME(ref_t.schema_id),
                        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id),
                        fk.update_referential_action_desc,
                        fk.delete_referential_action_desc
                 FROM sys.foreign_keys fk
                 INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                 INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 INNER JOIN sys.tables ref_t ON fkc.referenced_object_id = ref_t.object_id
                 WHERE s.name = @P1 AND t.name = @P2
                 ORDER BY fk.name, fkc.constraint_column_id",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        let mut order: Vec<String> = Vec::new();
        let mut map: std::collections::HashMap<String, zqlz_core::RawForeignKeyRow> =
            std::collections::HashMap::new();
        for row in &result.rows {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let column = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_column = row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string();
            if !map.contains_key(&name) {
                order.push(name.clone());
                map.insert(
                    name.clone(),
                    zqlz_core::RawForeignKeyRow {
                        name: name.clone(),
                        referenced_table: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        referenced_schema: row.get(3).and_then(|v| v.as_str()).map(ToString::to_string),
                        on_update: row.get(5).and_then(|v| v.as_str()).map(ToString::to_string),
                        on_delete: row.get(6).and_then(|v| v.as_str()).map(ToString::to_string),
                        ..Default::default()
                    },
                );
            }
            let entry = map.get_mut(&name).expect("entry just inserted");
            entry.columns.push(column);
            entry.referenced_columns.push(ref_column);
        }

        Ok(order.into_iter().filter_map(|name| map.remove(&name)).collect())
    }

    async fn fetch_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawForeignKeyRow>>>> {
        let schema = schema.unwrap_or("dbo");
        let result = self
            .query(
                "SELECT fk.name,
                        COL_NAME(fkc.parent_object_id, fkc.parent_column_id),
                        OBJECT_NAME(fkc.referenced_object_id),
                        SCHEMA_NAME(ref_t.schema_id),
                        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id),
                        fk.update_referential_action_desc,
                        fk.delete_referential_action_desc,
                        t.name
                 FROM sys.foreign_keys fk
                 INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                 INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 INNER JOIN sys.tables ref_t ON fkc.referenced_object_id = ref_t.object_id
                 WHERE s.name = @P1
                 ORDER BY t.name, fk.name, fkc.constraint_column_id",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?;

        // Keyed by (table, constraint): grouping on the constraint name alone would
        // merge same-named keys from different tables.
        let mut order: Vec<(String, String)> = Vec::new();
        let mut map: HashMap<(String, String), zqlz_core::RawForeignKeyRow> = HashMap::new();
        for row in &result.rows {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let table = row.get(7).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let key = (table, name.clone());

            let entry = map.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                zqlz_core::RawForeignKeyRow {
                    name,
                    referenced_table: row
                        .get(2)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    referenced_schema: row.get(3).and_then(|v| v.as_str()).map(ToString::to_string),
                    on_update: row.get(5).and_then(|v| v.as_str()).map(ToString::to_string),
                    on_delete: row.get(6).and_then(|v| v.as_str()).map(ToString::to_string),
                    ..Default::default()
                }
            });
            entry
                .columns
                .push(row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string());
            entry
                .referenced_columns
                .push(row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string());
        }

        let mut foreign_keys_by_relation: HashMap<String, Vec<zqlz_core::RawForeignKeyRow>> =
            HashMap::new();
        for key in order {
            if let Some(foreign_key) = map.remove(&key) {
                foreign_keys_by_relation
                    .entry(key.0)
                    .or_default()
                    .push(foreign_key);
            }
        }

        Ok(Some(foreign_keys_by_relation))
    }

    async fn fetch_constraints(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawConstraintRow>> {
        let schema = relation.schema.as_deref().unwrap_or("dbo");
        let result = self
            .query(
                "SELECT cc.name, 'CHECK', cc.definition
                 FROM sys.check_constraints cc
                 INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.name = @P2
                 UNION ALL
                 SELECT i.name, 'UNIQUE', NULL
                 FROM sys.indexes i
                 INNER JOIN sys.tables t ON i.object_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                 WHERE s.name = @P1 AND t.name = @P2 AND i.is_unique = 1 AND i.is_primary_key = 0
                 ORDER BY 1",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| zqlz_core::RawConstraintRow {
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                kind: row.get(1).and_then(|v| v.as_str()).unwrap_or("CHECK").to_string(),
                columns: Vec::new(),
                definition: row.get(2).and_then(|v| v.as_str()).map(ToString::to_string),
            })
            .collect())
    }

    async fn fetch_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawTriggerRow>> {
        let schema = schema.unwrap_or("dbo");
        let base = "SELECT tr.name, OBJECT_NAME(tr.parent_id),
                    CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
                         WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsAfterTrigger') = 1 THEN 'AFTER'
                         ELSE 'FOR' END,
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') = 1 THEN ',UPDATE' ELSE '' END +
                    CASE WHEN OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') = 1 THEN ',DELETE' ELSE '' END,
                    OBJECT_DEFINITION(tr.object_id),
                    CASE WHEN tr.is_disabled = 0 THEN 1 ELSE 0 END
                 FROM sys.triggers tr
                 INNER JOIN sys.tables t ON tr.parent_id = t.object_id
                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id";
        let (sql, params) = match table {
            Some(table) => (
                format!("{base} WHERE s.name = @P1 AND t.name = @P2 ORDER BY tr.name"),
                vec![
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(table.to_string()),
                ],
            ),
            None => (
                format!("{base} WHERE s.name = @P1 ORDER BY tr.name"),
                vec![zqlz_core::Value::String(schema.to_string())],
            ),
        };
        let result = self.query(&sql, &params).await?;

        Ok(result
            .rows
            .iter()
            .map(|row| zqlz_core::RawTriggerRow {
                schema: Some(schema.to_string()),
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                table_name: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                timing: row.get(2).and_then(|v| v.as_str()).map(ToString::to_string),
                events: row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect(),
                for_each: None,
                definition: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
                enabled: row.get(5).and_then(|v| v.as_i64()).unwrap_or(1) == 1,
            })
            .collect())
    }
}

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

fn mssql_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(280.0)
            .min_width(120.0)
            .sortable(),
        ObjectsPanelColumn::new("schema", "Schema")
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("type", "Type")
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("rows", "Rows")
            .width(90.0)
            .min_width(60.0)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("size", "Size")
            .width(100.0)
            .min_width(70.0)
            .sortable()
            .text_right(),
    ]
}

fn mssql_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    type_label: String,
    rows: Option<i64>,
    size: Option<i64>,
) -> ObjectsPanelRow {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), name.clone());
    values.insert(
        "schema".to_string(),
        schema.clone().unwrap_or_else(|| "-".to_string()),
    );
    values.insert("type".to_string(), type_label);
    values.insert(
        "rows".to_string(),
        rows.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "size".to_string(),
        size.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    ObjectsPanelRow {
        name: name.clone(),
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn mssql_database_objects_panel_data(databases: Vec<DatabaseInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for database in databases {
        data.rows.push(mssql_row(
            "database",
            database.name,
            None,
            "Database".to_string(),
            None,
            database.size_bytes,
        ));
    }
    Ok(data)
}

fn mssql_schema_objects_panel_data(schemas: Vec<SchemaInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for schema in schemas {
        data.rows.push(mssql_row(
            "schema",
            schema.name,
            None,
            "Schema".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

fn mssql_table_objects_panel_data(tables: Vec<TableInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for table in tables {
        data.rows.push(mssql_row(
            "table",
            table.name,
            table.schema,
            "Table".to_string(),
            table.row_count,
            table.size_bytes,
        ));
    }
    Ok(data)
}

fn mssql_view_objects_panel_data(views: Vec<ViewInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for view in views {
        data.rows.push(mssql_row(
            "view",
            view.name,
            view.schema,
            "View".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

fn mssql_function_objects_panel_data(functions: Vec<FunctionInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for function in functions {
        data.rows.push(mssql_row(
            "function",
            function.name,
            function.schema,
            "Function".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

fn mssql_procedure_objects_panel_data(procedures: Vec<ProcedureInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for procedure in procedures {
        data.rows.push(mssql_row(
            "procedure",
            procedure.name,
            procedure.schema,
            "Procedure".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

fn mssql_trigger_objects_panel_data(triggers: Vec<TriggerInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for trigger in triggers {
        data.rows.push(mssql_row(
            "trigger",
            trigger.name,
            trigger.schema,
            "Trigger".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

fn mssql_sequence_objects_panel_data(sequences: Vec<SequenceInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for sequence in sequences {
        data.rows.push(mssql_row(
            "sequence",
            sequence.name,
            sequence.schema,
            "Sequence".to_string(),
            sequence.current_value,
            None,
        ));
    }
    Ok(data)
}

fn mssql_type_objects_panel_data(types: Vec<TypeInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for data_type in types {
        data.rows.push(mssql_row(
            "type",
            data_type.name,
            data_type.schema,
            format!("{:?}", data_type.type_kind),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_objects_panel_manifest() -> ObjectsPanelManifest {
    let columns = mssql_objects_panel_columns();
    let row_actions = |kind_id: &str| {
        vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("view_history", "View History")
                .group("metadata")
                .single_selection(),
            ObjectsPanelAction::new("delete", "Drop")
                .group("danger")
                .destructive()
                .object_form(kind_id, ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ]
    };
    let kinds = vec![
        ("database", "Database", "Databases", "database"),
        ("schema", "Schema", "Schemas", "schema"),
        ("table", "Table", "Tables", "table"),
        ("view", "View", "Views", "view"),
        ("function", "Function", "Functions", "function"),
        ("procedure", "Procedure", "Procedures", "procedure"),
        ("trigger", "Trigger", "Triggers", "trigger"),
        ("sequence", "Sequence", "Sequences", "sequence"),
        ("type", "Type", "Types", "type"),
    ]
    .into_iter()
    .map(|(id, singular, plural, icon)| {
        ObjectsPanelObjectKind::new(id, singular, plural)
            .icon_key(icon)
            .columns(columns.clone())
            .row_actions(row_actions(id))
            .default_row_action("open")
    })
    .collect();

    ObjectsPanelManifest {
        object_kinds: kinds,
        toolbar_actions: vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
            ObjectsPanelAction::new("new_schema", "New Schema")
                .icon_key("create")
                .create_object_kind("schema")
                .object_form("schema", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_table", "New Table")
                .icon_key("create")
                .create_object_kind("table")
                .object_form("table", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_view", "New View")
                .icon_key("create")
                .create_object_kind("view")
                .object_form("view", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_procedure", "New Procedure")
                .icon_key("create")
                .create_object_kind("procedure")
                .object_form("procedure", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_function", "New Function")
                .icon_key("create")
                .create_object_kind("function")
                .object_form("function", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_trigger", "New Trigger")
                .icon_key("create")
                .create_object_kind("trigger")
                .object_form("trigger", ObjectFormMode::Create),
        ],
    }
}

fn mssql_object_form_spec(request: &ObjectFormSpecRequest) -> Option<ObjectFormSpec> {
    let schema = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .unwrap_or_else(|| "dbo".to_string());
    let name = request
        .object_ref
        .as_ref()
        .map(|object_ref| object_ref.name.clone())
        .unwrap_or_default();
    let fields = match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            vec![ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required()]
        }
        ("table", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("columns", "Columns", ObjectFormFieldKind::TextArea)
                .default_value(ObjectFormValue::String(
                    "[Id] int IDENTITY(1,1) NOT NULL,\n[Name] nvarchar(255) NOT NULL".to_string(),
                ))
                .required(),
        ],
        ("view", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("query", "Query", ObjectFormFieldKind::SqlExpression)
                .default_value(ObjectFormValue::String("SELECT 1 AS value".to_string()))
                .required(),
        ],
        ("procedure" | "function" | "trigger", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("body", "Body", ObjectFormFieldKind::TextArea).required(),
        ],
        (_, ObjectFormMode::Drop) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema))
                .read_only(),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(name))
                .read_only(),
            ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox).required(),
        ],
        _ => return None,
    };
    Some(
        ObjectFormSpec::new(&request.kind_id, request.mode, "SQL Server Object")
            .sections(vec![ObjectFormSection::new(fields)]),
    )
}

fn mssql_form_string(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    key: &str,
) -> String {
    values
        .get(key)
        .and_then(|value| value.as_string())
        .unwrap_or("")
        .trim()
        .to_string()
}

pub(crate) fn mssql_quote_identifier(identifier: &str) -> String {
    format!("[{}]", identifier.replace(']', "]]"))
}

fn mssql_qualified_name(schema: &str, name: &str) -> String {
    format!(
        "{}.{}",
        mssql_quote_identifier(schema),
        mssql_quote_identifier(name)
    )
}

pub(crate) fn mssql_object_form_ddl(request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
    let name = mssql_form_string(&request.values, "name");
    let schema = mssql_form_string(&request.values, "schema");
    let schema = if schema.is_empty() { "dbo" } else { &schema };
    match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Schema name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SCHEMA {}",
                mssql_quote_identifier(&name)
            )])
        }
        ("table", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Table name is required".to_string()));
            }
            let columns = mssql_form_string(&request.values, "columns");
            if columns.is_empty() {
                return Err(ZqlzError::Driver("Columns are required".to_string()));
            }
            Ok(vec![format!(
                "CREATE TABLE {} (\n{}\n)",
                mssql_qualified_name(schema, &name),
                columns
            )])
        }
        ("view", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("View name is required".to_string()));
            }
            let query = mssql_form_string(&request.values, "query");
            if query.is_empty() {
                return Err(ZqlzError::Driver("Query is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE VIEW {} AS {}",
                mssql_qualified_name(schema, &name),
                query
            )])
        }
        ("procedure", ObjectFormMode::Create)
        | ("function", ObjectFormMode::Create)
        | ("trigger", ObjectFormMode::Create) => {
            let body = mssql_form_string(&request.values, "body");
            if body.is_empty() {
                return Err(ZqlzError::Driver("Body is required".to_string()));
            }
            Ok(vec![body])
        }
        (_, ObjectFormMode::Drop) => {
            let confirmed = request
                .values
                .get("confirm")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            if !confirmed {
                return Err(ZqlzError::Driver(
                    "Drop confirmation is required".to_string(),
                ));
            }
            let object_name = if name.is_empty() {
                request
                    .object_ref
                    .as_ref()
                    .map(|object_ref| object_ref.name.as_str())
                    .unwrap_or("")
            } else {
                &name
            };
            let object_schema = request
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.schema.as_deref())
                .unwrap_or(schema);
            if object_name.is_empty() {
                return Err(ZqlzError::Driver("Object name is required".to_string()));
            }
            let kind = match request.kind_id.as_str() {
                "schema" => {
                    return Ok(vec![format!(
                        "DROP SCHEMA {}",
                        mssql_quote_identifier(object_name)
                    )]);
                }
                "table" => "TABLE",
                "view" => "VIEW",
                "procedure" => "PROCEDURE",
                "function" => "FUNCTION",
                "trigger" => "TRIGGER",
                "sequence" => "SEQUENCE",
                _ => {
                    return Err(ZqlzError::NotSupported(format!(
                        "Dropping SQL Server {} is not supported",
                        request.kind_id
                    )));
                }
            };
            Ok(vec![format!(
                "DROP {} {}",
                kind,
                mssql_qualified_name(object_schema, object_name)
            )])
        }
        _ => Err(ZqlzError::NotSupported(format!(
            "SQL Server object form is not supported for {} {:?}",
            request.kind_id, request.mode
        ))),
    }
}

pub(crate) fn generate_ddl_not_found_message(
    object_kind: &str,
    schema: &str,
    object_name: &str,
) -> String {
    format!("{} '[{}].[{}]' not found", object_kind, schema, object_name)
}

pub(crate) fn generate_ddl_definition_unavailable_message(
    object_kind: &str,
    schema: &str,
    object_name: &str,
) -> String {
    format!(
        "DDL definition for {} '[{}].[{}]' is unavailable",
        object_kind, schema, object_name
    )
}

/// Parse SQL Server foreign key action

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
