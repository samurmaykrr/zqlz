//! PostgreSQL raw-catalog port used by the shared schema engine, plus the
//! shared column/foreign-key query builders it reuses.

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::Result;

/// Shared body for the per-relation and schema-wide column queries.
///
/// `cls.relname` and primary-key membership are appended after the original
/// projection so both variants share one row decoder. `relkind` must be filtered
/// explicitly: without a `relname` predicate, `pg_attribute` also yields index,
/// TOAST and composite-type attributes.
const POSTGRES_COLUMNS_SELECT: &str = "SELECT
                    a.attname,
                    a.attnum,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    CASE WHEN a.attidentity <> '' THEN 'YES' ELSE 'NO' END,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_catalog.pg_type t
                        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                        JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
                        WHERE t.oid = a.atttypid
                    ),
                    cls.relname,
                    (
                        SELECT 1
                        FROM pg_catalog.pg_index i
                        WHERE i.indrelid = cls.oid
                          AND i.indisprimary
                          AND a.attnum = ANY(i.indkey)
                    ),
                    ns.nspname
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class cls ON cls.oid = a.attrelid
                 JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace
                 LEFT JOIN pg_catalog.pg_attrdef d
                    ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                 LEFT JOIN information_schema.columns c
                    ON c.table_schema = ns.nspname
                    AND c.table_name = cls.relname
                    AND c.column_name = a.attname
                 WHERE ";

const POSTGRES_COLUMNS_RELATION_FILTER: &str = "
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND cls.relkind IN ('r', 'p', 'f', 'v', 'm')";

/// Schemas that never hold user relations; mirrors the exclusion `list_tables` applies
/// when it is asked for every schema, so completions cover exactly what the sidebar lists.
const POSTGRES_USER_SCHEMA_FILTER: &str = "{schema_column} NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    AND {schema_column} NOT LIKE 'pg_temp_%'
                    AND {schema_column} NOT LIKE 'pg_toast_temp_%'";

/// `$1` is the schema. With `single_relation`, `$2` is the relation name.
pub(crate) fn postgres_columns_sql(single_relation: bool) -> String {
    let mut sql = String::from(POSTGRES_COLUMNS_SELECT);
    sql.push_str("ns.nspname = $1");
    sql.push_str(POSTGRES_COLUMNS_RELATION_FILTER);
    if single_relation {
        sql.push_str(" AND cls.relname = $2 ORDER BY a.attnum");
    } else {
        sql.push_str(" ORDER BY cls.relname, a.attnum");
    }
    sql
}

/// Columns of every relation in every user schema; takes no parameters.
pub(crate) fn postgres_all_schemas_columns_sql() -> String {
    let mut sql = String::from(POSTGRES_COLUMNS_SELECT);
    sql.push_str(&POSTGRES_USER_SCHEMA_FILTER.replace("{schema_column}", "ns.nspname"));
    sql.push_str(POSTGRES_COLUMNS_RELATION_FILTER);
    sql.push_str(" ORDER BY ns.nspname, cls.relname, a.attnum");
    sql
}

/// Shared body for the per-relation and schema-wide foreign-key queries.
///
/// `tc.table_name` is appended after the original projection so both variants
/// share one row decoder.
const POSTGRES_FOREIGN_KEYS_SELECT: &str = "SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule,
                    tc.table_name,
                    tc.table_schema,
                    ccu.table_schema AS foreign_table_schema
                 FROM information_schema.table_constraints AS tc
                 JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 JOIN information_schema.constraint_column_usage AS ccu
                   ON ccu.constraint_name = tc.constraint_name
                   AND ccu.constraint_schema = tc.constraint_schema
                 JOIN information_schema.referential_constraints AS rc
                   ON rc.constraint_name = tc.constraint_name
                   AND rc.constraint_schema = tc.constraint_schema
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND ";

pub(crate) fn postgres_foreign_keys_sql(single_relation: bool) -> String {
    let mut sql = String::from(POSTGRES_FOREIGN_KEYS_SELECT);
    sql.push_str("tc.table_schema = $1");
    if single_relation {
        sql.push_str(" AND tc.table_name = $2");
    } else {
        sql.push_str(" ORDER BY tc.table_name, tc.constraint_name");
    }
    sql
}

/// Foreign keys of every table in every user schema; takes no parameters.
pub(crate) fn postgres_all_schemas_foreign_keys_sql() -> String {
    let mut sql = String::from(POSTGRES_FOREIGN_KEYS_SELECT);
    sql.push_str(&POSTGRES_USER_SCHEMA_FILTER.replace("{schema_column}", "tc.table_schema"));
    sql.push_str(" ORDER BY tc.table_schema, tc.table_name, tc.constraint_name");
    sql
}

pub(crate) fn postgres_foreign_key_row(row: &zqlz_core::Row, schema: &str) -> zqlz_core::RawForeignKeyRow {
    zqlz_core::RawForeignKeyRow {
        name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        columns: vec![row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string()],
        // Index 8 is the referenced table's own schema; fall back to the queried one.
        referenced_schema: Some(
            row.get(8)
                .and_then(|v| v.as_str())
                .unwrap_or(schema)
                .to_string(),
        ),
        referenced_table: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        referenced_columns: vec![row
            .get(3)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()],
        on_update: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
        on_delete: row.get(5).and_then(|v| v.as_str()).map(ToString::to_string),
        is_deferrable: false,
        initially_deferred: false,
    }
}

pub(crate) fn postgres_column_row(row: &zqlz_core::Row) -> zqlz_core::RawColumnRow {
    let is_identity = row.get(8).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
    zqlz_core::RawColumnRow {
        name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
        data_type: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        is_nullable: row.get(3).and_then(|v| v.as_str()).unwrap_or("NO") == "YES",
        default_value: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
        max_length: row.get(5).and_then(|v| v.as_i64()),
        precision: row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32),
        scale: row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32),
        identity: if is_identity {
            zqlz_core::RawIdentity::Declared
        } else {
            zqlz_core::RawIdentity::Unknown
        },
        enum_values: row.get(9).and_then(|v| v.as_string_array()),
        primary_key_ordinal: row.get(11).and_then(|v| v.as_i64()),
        ..Default::default()
    }
}

/// PostgreSQL implementation of the raw-catalog port. Provides the per-table
/// fetches the shared engine composes; PostgreSQL's listing, objects-panel,
/// object-form, and DDL logic remain on `PostgresConnection`.
pub struct PostgresCatalog {
    client: std::sync::Arc<tokio::sync::Mutex<tokio_postgres::Client>>,
    capabilities: zqlz_core::CatalogCapabilities,
}

impl PostgresCatalog {
    pub fn new(client: std::sync::Arc<tokio::sync::Mutex<tokio_postgres::Client>>) -> Self {
        let capabilities = zqlz_core::CatalogCapabilities {
            driver_id: "postgres".to_string(),
            server_version: None,
            namespaces: zqlz_core::NamespaceModel::DatabasesAndSchemas {
                default_schema: "public".to_string(),
            },
            objects: zqlz_core::ObjectKindSupport::ALL_RELATIONAL,
            auto_increment: zqlz_core::AutoIncrementRules {
                default_markers: vec!["nextval(".to_string()],
                integer_primary_key: false,
            },
            stored_source: Vec::new(),
            deferrable_constraints: true,
            panel_extras: Vec::new(),
        };
        Self {
            client,
            capabilities,
        }
    }

    async fn query(&self, sql: &str, params: &[zqlz_core::Value]) -> Result<zqlz_core::QueryResult> {
        crate::connection::run_postgres_query(&self.client, sql, params).await
    }
}

#[async_trait]
impl zqlz_core::CatalogSource for PostgresCatalog {
    fn capabilities(&self) -> &zqlz_core::CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawRelationRow>> {
        let schema = schema.unwrap_or("public").to_string();

        let tables = self
            .query(
                "SELECT n.nspname, c.relname, c.relkind,
                    CASE WHEN c.relkind IN ('v','m') THEN NULL ELSE s.n_live_tup END AS row_count,
                    CASE WHEN c.relkind IN ('v','m') THEN NULL ELSE pg_total_relation_size(c.oid) END AS size_bytes,
                    obj_description(c.oid, 'pg_class') AS comment
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                 WHERE n.nspname = $1 AND c.relkind IN ('r','p','f')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.clone())],
            )
            .await?;
        let mut relations: Vec<zqlz_core::RawRelationRow> = tables
            .rows
            .iter()
            .map(|row| {
                let table_type = match row.get(2).and_then(|v| v.as_str()).unwrap_or("r") {
                    "p" => zqlz_core::TableType::PartitionedTable,
                    "f" => zqlz_core::TableType::ForeignTable,
                    _ => zqlz_core::TableType::Table,
                };
                let mut relation = zqlz_core::RawRelationRow::new(
                    row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    table_type,
                );
                relation.schema = Some(schema.clone());
                relation.row_estimate = row.get(3).and_then(|v| v.as_i64());
                relation.size_bytes = row.get(4).and_then(|v| v.as_i64());
                relation.comment = row.get(5).and_then(|v| v.as_str()).map(ToString::to_string);
                relation
            })
            .collect();

        let views = self
            .query(
                "SELECT table_name, view_definition FROM information_schema.views
                 WHERE table_schema = $1 ORDER BY table_name",
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

        let matviews = self
            .query(
                "SELECT matviewname, definition FROM pg_matviews
                 WHERE schemaname = $1 ORDER BY matviewname",
                &[zqlz_core::Value::String(schema.clone())],
            )
            .await?;
        relations.extend(matviews.rows.iter().map(|row| {
            let mut relation = zqlz_core::RawRelationRow::new(
                row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                zqlz_core::TableType::MaterializedView,
            );
            relation.schema = Some(schema.clone());
            relation.view_definition = row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
            relation
        }));

        Ok(relations)
    }

    async fn fetch_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawTriggerRow>> {
        let schema = schema.unwrap_or("public");
        let result = match table {
            Some(table) => {
                self.query(
                    "SELECT t.tgname, c.relname, pg_get_triggerdef(t.oid)
                     FROM pg_trigger t
                     JOIN pg_class c ON t.tgrelid = c.oid
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE n.nspname = $1 AND c.relname = $2 AND NOT t.tgisinternal
                     ORDER BY t.tgname",
                    &[
                        zqlz_core::Value::String(schema.to_string()),
                        zqlz_core::Value::String(table.to_string()),
                    ],
                )
                .await?
            }
            None => {
                self.query(
                    "SELECT t.tgname, c.relname, pg_get_triggerdef(t.oid)
                     FROM pg_trigger t
                     JOIN pg_class c ON t.tgrelid = c.oid
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE n.nspname = $1 AND NOT t.tgisinternal
                     ORDER BY t.tgname",
                    &[zqlz_core::Value::String(schema.to_string())],
                )
                .await?
            }
        };

        Ok(result
            .rows
            .iter()
            .map(|row| zqlz_core::RawTriggerRow {
                schema: Some(schema.to_string()),
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

    async fn fetch_columns(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawColumnRow>> {
        let schema = relation.schema.as_deref().unwrap_or("public");
        let result = self
            .query(
                &postgres_columns_sql(true),
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result.rows.iter().map(postgres_column_row).collect())
    }

    async fn fetch_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawColumnRow>>>> {
        // `None` means every user schema, keyed `schema.relation` like the names
        // `list_tables(None)` hands the sidebar and completions.
        let result = match schema {
            Some(schema) => {
                self.query(
                    &postgres_columns_sql(false),
                    &[zqlz_core::Value::String(schema.to_string())],
                )
                .await?
            }
            None => self.query(&postgres_all_schemas_columns_sql(), &[]).await?,
        };

        let mut columns_by_relation: HashMap<String, Vec<zqlz_core::RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(relation) = row.get(10).and_then(|value| value.as_str()) else {
                continue;
            };
            let key = match (schema, row.get(12).and_then(|value| value.as_str())) {
                (None, Some(relation_schema)) => {
                    super::schema_qualified_name(relation_schema, relation)
                }
                _ => relation.to_string(),
            };
            columns_by_relation
                .entry(key)
                .or_default()
                .push(postgres_column_row(row));
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawIndexRow>> {
        let schema = relation.schema.as_deref().unwrap_or("public");
        let result = self
            .query(
                "SELECT
                    i.relname AS index_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary,
                    array_agg(
                        coalesce(a.attname, pg_get_indexdef(ix.indexrelid, k.pos::int, true))
                        ORDER BY k.pos
                    ) FILTER (WHERE k.pos <= ix.indnkeyatts) AS key_columns,
                    array_agg(a.attname ORDER BY k.pos)
                        FILTER (WHERE k.pos > ix.indnkeyatts) AS include_columns,
                    am.amname AS index_method,
                    pg_get_expr(ix.indpred, ix.indrelid) AS where_clause
                 FROM pg_class t
                 JOIN pg_index ix ON t.oid = ix.indrelid
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_am am ON am.oid = i.relam
                 CROSS JOIN LATERAL unnest(ix.indkey::int2[]) WITH ORDINALITY AS k(attnum, pos)
                 LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE n.nspname = $1 AND t.relname = $2
                 GROUP BY i.relname, ix.indisunique, ix.indisprimary, ix.indnkeyatts,
                          ix.indexrelid, ix.indpred, ix.indrelid, am.amname
                 ORDER BY i.relname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                Some(zqlz_core::RawIndexRow {
                    name,
                    columns: row.get(3).and_then(|v| v.as_string_array()).unwrap_or_default(),
                    is_unique: row.get(1).and_then(|v| v.as_bool()).unwrap_or(false),
                    is_primary: row.get(2).and_then(|v| v.as_bool()).unwrap_or(false),
                    method: row.get(5).and_then(|v| v.as_str()).map(ToString::to_string),
                    where_clause: row.get(6).and_then(|v| v.as_str()).map(ToString::to_string),
                    include_columns: row
                        .get(4)
                        .and_then(|v| v.as_string_array())
                        .unwrap_or_default(),
                    column_descending: Vec::new(),
                    comment: None,
                })
            })
            .collect())
    }

    async fn fetch_foreign_keys(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawForeignKeyRow>> {
        let schema = relation.schema.as_deref().unwrap_or("public");
        let result = self
            .query(
                &postgres_foreign_keys_sql(true),
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| postgres_foreign_key_row(row, schema))
            .collect())
    }

    async fn fetch_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawForeignKeyRow>>>> {
        let result = match schema {
            Some(schema) => {
                self.query(
                    &postgres_foreign_keys_sql(false),
                    &[zqlz_core::Value::String(schema.to_string())],
                )
                .await?
            }
            None => {
                self.query(&postgres_all_schemas_foreign_keys_sql(), &[])
                    .await?
            }
        };

        let mut foreign_keys_by_relation: HashMap<String, Vec<zqlz_core::RawForeignKeyRow>> =
            HashMap::new();
        for row in &result.rows {
            let Some(relation) = row.get(6).and_then(|value| value.as_str()) else {
                continue;
            };
            let relation_schema = row.get(7).and_then(|value| value.as_str());
            let key = match (schema, relation_schema) {
                (None, Some(relation_schema)) => {
                    super::schema_qualified_name(relation_schema, relation)
                }
                _ => relation.to_string(),
            };
            foreign_keys_by_relation
                .entry(key)
                .or_default()
                .push(postgres_foreign_key_row(
                    row,
                    schema.or(relation_schema).unwrap_or("public"),
                ));
        }

        Ok(Some(foreign_keys_by_relation))
    }

    async fn fetch_constraints(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawConstraintRow>> {
        let schema = relation.schema.as_deref().unwrap_or("public");
        let result = self
            .query(
                "SELECT
                    con.conname AS constraint_name,
                    pg_get_constraintdef(con.oid) AS definition,
                    array_agg(att.attname ORDER BY att.attnum) AS columns
                 FROM pg_constraint con
                 JOIN pg_class rel ON rel.oid = con.conrelid
                 JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                 LEFT JOIN pg_attribute att
                   ON att.attrelid = rel.oid
                   AND att.attnum = ANY(con.conkey)
                 WHERE con.contype = 'c'
                   AND nsp.nspname = $1
                   AND rel.relname = $2
                 GROUP BY con.conname, con.oid
                 ORDER BY con.conname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(relation.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                Some(zqlz_core::RawConstraintRow {
                    name,
                    kind: "CHECK".to_string(),
                    columns: row.get(2).and_then(|v| v.as_string_array()).unwrap_or_default(),
                    definition: row.get(1).and_then(|v| v.as_str()).map(ToString::to_string),
                })
            })
            .collect())
    }
}

