//! Turso schema introspection adapter.
//!
//! Turso speaks SQLite, so [`TursoCatalog`] runs the same sqlite_master queries
//! and PRAGMAs as the SQLite driver and reports raw catalog records. All
//! normalization and composition are handled by the shared `zqlz-schema-engine`.

use std::collections::HashMap;

use async_trait::async_trait;
use zqlz_core::{
    AutoIncrementRules, CatalogCapabilities, CatalogSource, DatabaseObject, NamespaceModel,
    ObjectKindSupport, ObjectType, QueryResult, RawColumnRow, RawForeignKeyRow, RawIdentity,
    RawIndexRow, RawRelationRow, RawTriggerRow, RelationRef, Result, Row, TableType, Value,
};

use crate::connection::run_turso_query;

fn sqlite_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Reads one `table_info` row starting at `base`, the offset of the `cid` column —
/// 0 for the bare PRAGMA, 1 when the bulk query prefixes the relation name.
fn column_row(row: &Row, base: usize) -> RawColumnRow {
    let pk_ordinal = row.get(base + 5).and_then(|v| v.as_i64()).unwrap_or(0);
    RawColumnRow {
        ordinal: row.get(base).and_then(|v| v.as_i64()).unwrap_or(0),
        name: row
            .get(base + 1)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        data_type: row
            .get(base + 2)
            .and_then(|v| v.as_str())
            .unwrap_or("TEXT")
            .to_string(),
        is_nullable: row.get(base + 3).and_then(|v| v.as_i64()).unwrap_or(0) == 0,
        default_value: row.get(base + 4).and_then(|v| {
            if v.is_null() {
                None
            } else {
                Some(v.to_string())
            }
        }),
        identity: RawIdentity::Unknown,
        primary_key_ordinal: (pk_ordinal > 0).then_some(pk_ordinal),
        ..Default::default()
    }
}

/// Reads one `foreign_key_list` row starting at `base`, the offset of the
/// referenced-table column. Both the bare PRAGMA and the bulk query put it at 2.
fn foreign_key_row(row: &Row, base: usize, table: &str) -> RawForeignKeyRow {
    let referenced_table = row
        .get(base)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    RawForeignKeyRow {
        name: format!("fk_{}_{}", table, referenced_table),
        columns: vec![row
            .get(base + 1)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()],
        referenced_schema: Some("main".to_string()),
        referenced_table,
        referenced_columns: vec![row
            .get(base + 2)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()],
        on_update: row
            .get(base + 3)
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        on_delete: row
            .get(base + 4)
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        is_deferrable: false,
        initially_deferred: false,
    }
}

fn classify_table_type(create_sql: Option<&str>) -> TableType {
    match create_sql {
        Some(sql)
            if sql
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE VIRTUAL TABLE") =>
        {
            TableType::VirtualTable
        }
        _ => TableType::Table,
    }
}

/// Turso implementation of the raw-catalog port (SQLite-compatible).
pub struct TursoCatalog {
    connection: libsql::Connection,
    capabilities: CatalogCapabilities,
}

impl TursoCatalog {
    pub fn new(connection: libsql::Connection) -> Self {
        let capabilities = CatalogCapabilities {
            driver_id: "turso".to_string(),
            server_version: None,
            namespaces: NamespaceModel::SingleNamespace {
                name: "main".to_string(),
            },
            objects: ObjectKindSupport {
                tables: true,
                views: true,
                triggers: true,
                ..ObjectKindSupport::NONE
            },
            auto_increment: AutoIncrementRules {
                default_markers: Vec::new(),
                integer_primary_key: true,
            },
            stored_source: vec![ObjectType::Table, ObjectType::View, ObjectType::Trigger],
            deferrable_constraints: false,
            panel_extras: Vec::new(),
        };
        Self {
            connection,
            capabilities,
        }
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        run_turso_query(&self.connection, sql, params).await
    }
}

#[async_trait]
impl CatalogSource for TursoCatalog {
    fn capabilities(&self) -> &CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(&self, _schema: Option<&str>) -> Result<Vec<RawRelationRow>> {
        let result = self
            .query(
                "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
                let object_type = row.get(1).and_then(|v| v.as_str()).unwrap_or("table");
                let create_sql = row.get(2).and_then(|v| v.as_str());
                let table_type = if object_type == "view" {
                    TableType::View
                } else {
                    classify_table_type(create_sql)
                };
                let mut relation = RawRelationRow::new(name, table_type);
                relation.schema = Some("main".to_string());
                if object_type == "view" {
                    relation.view_definition = create_sql.map(ToString::to_string);
                }
                relation
            })
            .collect())
    }

    async fn fetch_columns(&self, relation: &RelationRef) -> Result<Vec<RawColumnRow>> {
        let result = self
            .query(
                &format!(
                    "PRAGMA table_info('{}')",
                    sqlite_string_literal(&relation.name)
                ),
                &[],
            )
            .await?;

        Ok(result.rows.iter().map(|row| column_row(row, 0)).collect())
    }

    async fn fetch_all_columns(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawColumnRow>>>> {
        // Virtual tables are excluded because their module may not be available,
        // which would abort the whole statement. libsql's support for table-valued
        // PRAGMA functions also varies by backend, so a failure here degrades to the
        // per-relation path rather than reporting a schema with no columns.
        let result = match self
            .query(
                "SELECT m.name, p.cid, p.name, p.type, p.\"notnull\", p.dflt_value, p.pk
                 FROM sqlite_master m
                 JOIN pragma_table_info(m.name) p
                 WHERE m.type IN ('table', 'view')
                   AND m.name NOT LIKE 'sqlite_%'
                   AND (m.sql IS NULL OR upper(ltrim(m.sql)) NOT LIKE 'CREATE VIRTUAL TABLE%')
                 ORDER BY m.name, p.cid",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Bulk column fetch unsupported; falling back to per-table PRAGMA");
                return Ok(None);
            }
        };

        let mut columns_by_relation: HashMap<String, Vec<RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(table) = row.get(0).and_then(|value| value.as_str()) else {
                continue;
            };
            columns_by_relation
                .entry(table.to_string())
                .or_default()
                .push(column_row(row, 1));
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(&self, relation: &RelationRef) -> Result<Vec<RawIndexRow>> {
        let result = self
            .query(
                &format!(
                    "PRAGMA index_list('{}')",
                    sqlite_string_literal(&relation.name)
                ),
                &[],
            )
            .await?;

        let mut indexes = Vec::new();
        for row in &result.rows {
            let name = match row.get(1).and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };
            let is_unique = row.get(2).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

            let columns_result = self
                .query(
                    &format!("PRAGMA index_info('{}')", sqlite_string_literal(&name)),
                    &[],
                )
                .await?;
            let columns = columns_result
                .rows
                .iter()
                .filter_map(|row| row.get(2).and_then(|v| v.as_str()).map(ToString::to_string))
                .collect();

            indexes.push(RawIndexRow {
                name,
                columns,
                is_unique,
                is_primary: false,
                method: Some("btree".to_string()),
                ..Default::default()
            });
        }

        Ok(indexes)
    }

    async fn fetch_foreign_keys(&self, relation: &RelationRef) -> Result<Vec<RawForeignKeyRow>> {
        let result = self
            .query(
                &format!(
                    "PRAGMA foreign_key_list('{}')",
                    sqlite_string_literal(&relation.name)
                ),
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| foreign_key_row(row, 2, &relation.name))
            .collect())
    }

    async fn fetch_all_foreign_keys(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawForeignKeyRow>>>> {
        let result = match self
            .query(
                "SELECT m.name, f.id, f.\"table\", f.\"from\", f.\"to\", f.on_update, f.on_delete
                 FROM sqlite_master m
                 JOIN pragma_foreign_key_list(m.name) f
                 WHERE m.type = 'table'
                   AND m.name NOT LIKE 'sqlite_%'
                   AND (m.sql IS NULL OR upper(ltrim(m.sql)) NOT LIKE 'CREATE VIRTUAL TABLE%')
                 ORDER BY m.name, f.id, f.seq",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Bulk foreign key fetch unsupported; falling back to per-table PRAGMA");
                return Ok(None);
            }
        };

        let mut foreign_keys_by_relation: HashMap<String, Vec<RawForeignKeyRow>> = HashMap::new();
        for row in &result.rows {
            let Some(table) = row.get(0).and_then(|value| value.as_str()) else {
                continue;
            };
            foreign_keys_by_relation
                .entry(table.to_string())
                .or_default()
                .push(foreign_key_row(row, 2, table));
        }

        Ok(Some(foreign_keys_by_relation))
    }

    async fn fetch_triggers(
        &self,
        _schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<RawTriggerRow>> {
        let sql = match table {
            Some(table) => format!(
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{}' ORDER BY name",
                sqlite_string_literal(table)
            ),
            None => {
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
                    .to_string()
            }
        };
        let result = self.query(&sql, &[]).await?;

        Ok(result
            .rows
            .iter()
            .map(|row| RawTriggerRow {
                schema: Some("main".to_string()),
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                table_name: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                timing: None,
                events: Vec::new(),
                for_each: None,
                definition: row.get(2).and_then(|v| v.as_str()).map(ToString::to_string),
                enabled: true,
            })
            .collect())
    }

    async fn fetch_object_source(&self, object: &DatabaseObject) -> Result<Option<String>> {
        let sqlite_type = match object.object_type {
            ObjectType::Table => "table",
            ObjectType::View => "view",
            ObjectType::Index => "index",
            ObjectType::Trigger => "trigger",
            _ => return Ok(None),
        };
        let result = self
            .query(
                "SELECT sql FROM sqlite_master WHERE name = ? AND type = ?",
                &[
                    Value::String(object.name.clone()),
                    Value::String(sqlite_type.to_string()),
                ],
            )
            .await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0).and_then(|v| v.as_str()).map(ToString::to_string)))
    }
}
