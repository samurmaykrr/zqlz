//! MS SQL Server schema introspection implementation

mod introspection;
mod object_forms;
mod objects_panel;

// Re-exported for `schema_tests`, which exercises these through `schema::*`.
#[cfg(test)]
pub(crate) use introspection::{
    MSSQL_FUNCTION_DDL_SQL, MSSQL_PROCEDURE_DDL_SQL, MSSQL_TRIGGER_DDL_SQL,
};
#[cfg(test)]
pub(crate) use object_forms::{
    generate_ddl_definition_unavailable_message, generate_ddl_not_found_message,
    generate_table_ddl, mssql_object_form_ddl, mssql_quote_identifier,
};
#[cfg(test)]
pub(crate) use objects_panel::mssql_objects_panel_manifest;

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::Result;

use super::connection::{run_mssql_query, MssqlClient};

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

    // `max_length` is declared as the type's own length (characters for the
    // n-types, precision for decimals, scale for time types), not the storage
    // bytes `sys.columns` reports, because the designer re-emits it verbatim.
    let type_name = "TYPE_NAME(c.user_type_id)";
    format!(
        "SELECT c.name, c.column_id,
                CASE WHEN {type_name} IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1
                     THEN {type_name} + '(max)' ELSE {type_name} END,
                c.is_nullable, dc.definition,
                CASE
                    WHEN {type_name} IN ('nchar', 'nvarchar') THEN IIF(c.max_length = -1, NULL, c.max_length / 2)
                    WHEN {type_name} IN ('char', 'varchar', 'binary', 'varbinary') THEN IIF(c.max_length = -1, NULL, c.max_length)
                    WHEN {type_name} IN ('decimal', 'numeric') THEN c.precision
                    WHEN {type_name} IN ('datetime2', 'datetimeoffset', 'time') THEN c.scale
                END,
                c.precision,
                CASE WHEN {type_name} IN ('decimal', 'numeric') THEN c.scale END,
                c.is_identity,
                CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END, t.name
         FROM sys.columns c
         INNER JOIN sys.objects t ON c.object_id = t.object_id AND t.type IN ('U', 'V')
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
