//! SQLite schema introspection adapter.
//!
//! [`SqliteCatalog`] implements [`CatalogSource`]: it runs sqlite_master
//! queries and PRAGMAs and reports raw catalog records. All normalization,
//! composition, and objects-panel derivation are handled by the shared
//! `zqlz-schema-engine`, so this file holds only SQLite-specific catalog
//! access.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::Connection as RusqliteConnection;
use zqlz_core::{
    AutoIncrementRules, CatalogCapabilities, CatalogSource, DatabaseObject, NamespaceModel,
    ObjectKindSupport, ObjectType, QueryResult, RawColumnRow, RawForeignKeyRow, RawIdentity,
    RawIndexRow, RawRelationRow, RawTriggerRow, RelationRef, Result, Row, TableType, Value,
};

use crate::connection::{object_type_to_sqlite, run_sqlite_query};

fn sqlite_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Reads one `table_info` row starting at `base`, which is the offset of the
/// `cid` column — 0 for the bare PRAGMA, 1 when the bulk query prefixes the
/// relation name.
fn column_row(row: &Row, base: usize) -> RawColumnRow {
    let ordinal = row.get(base).and_then(|v| v.as_i64()).unwrap_or(0);
    let name = row
        .get(base + 1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let data_type = row
        .get(base + 2)
        .and_then(|v| v.as_str())
        .unwrap_or("TEXT")
        .to_string();
    let is_nullable = row.get(base + 3).and_then(|v| v.as_i64()).unwrap_or(0) == 0;
    let default_value = row.get(base + 4).and_then(|v| {
        if v.is_null() {
            None
        } else {
            Some(v.to_string())
        }
    });
    let pk_ordinal = row.get(base + 5).and_then(|v| v.as_i64()).unwrap_or(0);

    RawColumnRow {
        name,
        ordinal,
        data_type,
        is_nullable,
        default_value,
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
    let from_column = row
        .get(base + 1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let to_column = row
        .get(base + 2)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    RawForeignKeyRow {
        name: format!("fk_{}_{}", table, referenced_table),
        columns: vec![from_column],
        referenced_schema: Some("main".to_string()),
        referenced_table,
        referenced_columns: vec![to_column],
        on_update: row.get(base + 3).and_then(|v| v.as_str()).map(ToString::to_string),
        on_delete: row.get(base + 4).and_then(|v| v.as_str()).map(ToString::to_string),
        is_deferrable: false,
        initially_deferred: false,
    }
}

fn classify_sqlite_table_type(create_sql: Option<&str>) -> TableType {
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

/// SQLite implementation of the raw-catalog port.
pub struct SqliteCatalog {
    conn: Arc<Mutex<RusqliteConnection>>,
    capabilities: CatalogCapabilities,
}

impl SqliteCatalog {
    pub fn new(conn: Arc<Mutex<RusqliteConnection>>) -> Self {
        let capabilities = CatalogCapabilities {
            driver_id: "sqlite".to_string(),
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
        Self { conn, capabilities }
    }

    fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        run_sqlite_query(&self.conn, sql, params)
    }

    fn count_map(&self, sql: &str) -> HashMap<String, i64> {
        match self.query(sql, &[]) {
            Ok(result) => result
                .rows
                .iter()
                .filter_map(|row| Some((row.get(0)?.as_str()?.to_string(), row.get(1)?.as_i64()?)))
                .collect(),
            Err(_) => HashMap::new(),
        }
    }

    fn row_estimates(&self) -> HashMap<String, i64> {
        let stat1_missing = self
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1' LIMIT 1",
                &[],
            )
            .map(|result| result.rows.is_empty())
            .unwrap_or(true);
        if stat1_missing {
            return HashMap::new();
        }
        self.count_map(
            "SELECT tbl, MAX(CAST(substr(stat, 1, instr(stat || ' ', ' ') - 1) AS INTEGER))
             FROM sqlite_stat1
             GROUP BY tbl",
        )
    }

    fn index_counts(&self) -> HashMap<String, i64> {
        self.count_map(
            "SELECT tbl_name, COUNT(*)
             FROM sqlite_master
             WHERE type = 'index'
               AND name NOT LIKE 'sqlite_autoindex_%'
             GROUP BY tbl_name",
        )
    }

    fn trigger_counts(&self) -> HashMap<String, i64> {
        self.count_map(
            "SELECT tbl_name, COUNT(*)
             FROM sqlite_master
             WHERE type = 'trigger'
             GROUP BY tbl_name",
        )
    }
}

#[async_trait]
impl CatalogSource for SqliteCatalog {
    fn capabilities(&self) -> &CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(&self, _schema: Option<&str>) -> Result<Vec<RawRelationRow>> {
        let result = self.query(
            "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
            &[],
        )?;

        let row_estimates = self.row_estimates();
        let index_counts = self.index_counts();
        let trigger_counts = self.trigger_counts();

        let mut relations = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let object_type = row.get(1).and_then(|v| v.as_str()).unwrap_or("table");
            let create_sql = row.get(2).and_then(|v| v.as_str());
            let table_type = if object_type == "view" {
                TableType::View
            } else {
                classify_sqlite_table_type(create_sql)
            };

            let mut relation = RawRelationRow::new(name.clone(), table_type);
            relation.schema = Some("main".to_string());
            relation.row_estimate = row_estimates.get(&name).copied();
            relation.index_count = index_counts.get(&name).copied();
            relation.trigger_count = trigger_counts.get(&name).copied();
            if object_type == "view" {
                relation.view_definition = create_sql.map(ToString::to_string);
            }
            relations.push(relation);
        }

        Ok(relations)
    }

    async fn fetch_columns(&self, relation: &RelationRef) -> Result<Vec<RawColumnRow>> {
        let result = self.query(
            &format!(
                "PRAGMA table_info('{}')",
                sqlite_string_literal(&relation.name)
            ),
            &[],
        )?;

        Ok(result.rows.iter().map(|row| column_row(row, 0)).collect())
    }

    async fn fetch_all_columns(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<RawColumnRow>>>> {
        // `pragma_table_info` as a table-valued function turns what is otherwise one
        // PRAGMA per relation into a single statement. Available since SQLite 3.16;
        // the bundled build is far newer.
        //
        // Virtual tables are excluded deliberately: if their module isn't compiled
        // into this build (fts5 is not among the enabled features), the TVF raises
        // and aborts the WHOLE statement, which would leave every table without
        // columns. They keep using the per-relation path, which has a DDL-based
        // fallback for exactly this case.
        let result = match self.query(
            "SELECT m.name, p.cid, p.name, p.type, p.\"notnull\", p.dflt_value, p.pk
             FROM sqlite_master m
             JOIN pragma_table_info(m.name) p
             WHERE m.type IN ('table', 'view')
               AND m.name NOT LIKE 'sqlite_%'
               AND (m.sql IS NULL OR upper(ltrim(m.sql)) NOT LIKE 'CREATE VIRTUAL TABLE%')
             ORDER BY m.name, p.cid",
            &[],
        ) {
            Ok(result) => result,
            // One unreadable relation fails the whole statement, so degrade to the
            // per-relation path rather than reporting a schema with no columns.
            Err(error) => {
                tracing::warn!(%error, "Bulk column fetch failed; falling back to per-table PRAGMA");
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
        let result = self.query(
            &format!(
                "PRAGMA index_list('{}')",
                sqlite_string_literal(&relation.name)
            ),
            &[],
        )?;

        let mut indexes = Vec::new();
        for row in &result.rows {
            let name = match row.get(1).and_then(|v| v.as_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };
            let is_unique = row.get(2).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

            let columns_result = self.query(
                &format!("PRAGMA index_info('{}')", sqlite_string_literal(&name)),
                &[],
            )?;
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
        let result = self.query(
            &format!(
                "PRAGMA foreign_key_list('{}')",
                sqlite_string_literal(&relation.name)
            ),
            &[],
        )?;

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
        let result = match self.query(
            "SELECT m.name, f.id, f.\"table\", f.\"from\", f.\"to\", f.on_update, f.on_delete
             FROM sqlite_master m
             JOIN pragma_foreign_key_list(m.name) f
             WHERE m.type = 'table'
               AND m.name NOT LIKE 'sqlite_%'
               AND (m.sql IS NULL OR upper(ltrim(m.sql)) NOT LIKE 'CREATE VIRTUAL TABLE%')
             ORDER BY m.name, f.id, f.seq",
            &[],
        ) {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Bulk foreign key fetch failed; falling back to per-table PRAGMA");
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
        let result = self.query(&sql, &[])?;

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
        let sqlite_type = match object_type_to_sqlite(&object.object_type) {
            Ok(sqlite_type) => sqlite_type,
            Err(_) => return Ok(None),
        };
        let result = self.query(
            "SELECT sql FROM sqlite_master WHERE name = ? AND type = ?",
            &[
                Value::String(object.name.clone()),
                Value::String(sqlite_type.to_string()),
            ],
        )?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0).and_then(|v| v.as_str()).map(ToString::to_string)))
    }
}
