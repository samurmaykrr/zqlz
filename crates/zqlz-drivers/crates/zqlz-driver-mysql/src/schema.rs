//! MySQL schema introspection implementation

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, DependencyType, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo,
    ObjectFormDdlRequest, ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormOption,
    ObjectFormSection, ObjectFormSpec, ObjectFormSpecRequest, ObjectFormValue, ObjectType,
    ObjectsPanelAction, ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelManifest,
    ObjectsPanelObjectKind, ObjectsPanelObjectRef, ObjectsPanelRow, ParameterInfo, ParameterMode,
    PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection, SequenceInfo,
    TableDetails, TableInfo, TableType, TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming,
    TypeInfo, Value, ViewInfo, ZqlzError,
};

use crate::MySqlConnection;

/// Read a value as text, tolerating MySQL `GROUP_CONCAT`/blob results that come
/// back as raw bytes rather than a string.
fn mysql_text(value: Option<&zqlz_core::Value>) -> Option<String> {
    match value? {
        zqlz_core::Value::String(text) => Some(text.clone()),
        zqlz_core::Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

/// MySQL implementation of the raw-catalog port. Provides the per-table fetches
/// the shared engine composes; MySQL's listing, objects-panel, object-form, and
/// DDL logic remain on `MySqlConnection`.
pub struct MySqlCatalog {
    pool: mysql_async::Pool,
    database_name: Option<String>,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    capabilities: zqlz_core::CatalogCapabilities,
    in_flight: crate::connection::InFlight,
}

impl MySqlCatalog {
    pub fn new(
        pool: mysql_async::Pool,
        database_name: Option<String>,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
        in_flight: crate::connection::InFlight,
    ) -> Self {
        let capabilities = zqlz_core::CatalogCapabilities {
            driver_id: "mysql".to_string(),
            server_version: None,
            namespaces: zqlz_core::NamespaceModel::DatabasesOnly,
            objects: zqlz_core::ObjectKindSupport::ALL_RELATIONAL,
            auto_increment: zqlz_core::AutoIncrementRules::default(),
            stored_source: Vec::new(),
            deferrable_constraints: false,
            panel_extras: Vec::new(),
        };
        Self {
            pool,
            database_name,
            cancelled,
            capabilities,
            in_flight,
        }
    }

    async fn query(
        &self,
        sql: &str,
        params: &[zqlz_core::Value],
    ) -> Result<zqlz_core::QueryResult> {
        crate::connection::run_mysql_query(
            self.pool.clone(),
            self.cancelled.clone(),
            self.in_flight.clone(),
            sql,
            params,
        )
        .await
    }

    fn resolved_schema(&self, schema: Option<&str>) -> Option<String> {
        schema
            .map(ToString::to_string)
            .or_else(|| self.database_name.clone())
    }
}

/// Column query for one relation or for a whole schema.
///
/// `TABLE_NAME` is appended after the original projection so both variants share
/// one row decoder. The schema filter must mirror `resolved_schema`: when it is
/// `None` the query uses `DATABASE()` and takes **no** parameters.
fn mysql_columns_sql(schema: Option<&str>, single_relation: bool) -> String {
    let schema_filter = schema.map(|_| "= ?").unwrap_or("= DATABASE()");
    let relation_filter = if single_relation {
        "AND TABLE_NAME = ?"
    } else {
        ""
    };
    let order = if single_relation {
        "ORDER BY ORDINAL_POSITION"
    } else {
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    };

    format!(
        "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE,
                COLUMN_KEY, EXTRA, COLUMN_COMMENT, GENERATION_EXPRESSION,
                CHARACTER_SET_NAME, COLLATION_NAME, TABLE_NAME
         FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA {schema_filter} {relation_filter}
         {order}"
    )
}

fn mysql_column_row(row: &zqlz_core::Row) -> zqlz_core::RawColumnRow {
    let data_type = row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let column_type = row.get(8).and_then(|v| v.as_str()).unwrap_or("");
    let column_key = row.get(9).and_then(|v| v.as_str()).unwrap_or("");
    let extra = row.get(10).and_then(|v| v.as_str()).unwrap_or("");
    let lower_data_type = data_type.to_lowercase();
    let enum_values = if lower_data_type == "enum" || lower_data_type == "set" {
        parse_mysql_enum_values(column_type)
    } else {
        None
    };
    let normalized_type = if column_type.is_empty() {
        data_type.clone()
    } else {
        column_type.to_string()
    };

    zqlz_core::RawColumnRow {
        name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
        data_type: normalized_type,
        is_nullable: row.get(3).and_then(|v| v.as_str()).unwrap_or("NO") == "YES",
        default_value: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
        max_length: row.get(5).and_then(|v| v.as_i64()),
        precision: row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32),
        scale: row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32),
        identity: if extra.contains("auto_increment") {
            zqlz_core::RawIdentity::Declared
        } else {
            zqlz_core::RawIdentity::None
        },
        primary_key_ordinal: (column_key == "PRI").then_some(1),
        enum_values,
        comment: row
            .get(11)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string),
        charset: row
            .get(13)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string),
        collation: row
            .get(14)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToString::to_string),
        generation_expression: row
            .get(12)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string),
        is_generated_stored: extra.to_ascii_lowercase().contains("stored generated"),
    }
}

#[async_trait]
impl zqlz_core::CatalogSource for MySqlCatalog {
    fn capabilities(&self) -> &zqlz_core::CatalogCapabilities {
        &self.capabilities
    }

    async fn fetch_relations(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawRelationRow>> {
        let schema = self.resolved_schema(schema);
        let schema_filter = schema
            .as_deref()
            .map(|_| "= ?")
            .unwrap_or("= DATABASE()");

        let tables_sql = format!(
            "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH, TABLE_COMMENT
             FROM information_schema.TABLES
             WHERE TABLE_SCHEMA {schema_filter}
               AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VIEW')
             ORDER BY TABLE_NAME"
        );
        let params: Vec<zqlz_core::Value> = schema
            .as_deref()
            .map(|s| vec![zqlz_core::Value::String(s.to_string())])
            .unwrap_or_default();
        let tables = self.query(&tables_sql, &params).await?;
        let mut relations: Vec<zqlz_core::RawRelationRow> = tables
            .rows
            .iter()
            .map(|row| {
                let table_type = match row.get(1).and_then(|v| v.as_str()).unwrap_or("BASE TABLE") {
                    "SYSTEM VIEW" => zqlz_core::TableType::System,
                    "VIEW" => zqlz_core::TableType::View,
                    _ => zqlz_core::TableType::Table,
                };
                let mut relation = zqlz_core::RawRelationRow::new(
                    row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    table_type,
                );
                relation.schema = schema.clone();
                relation.row_estimate = row.get(2).and_then(|v| v.as_i64());
                relation.size_bytes = row.get(3).and_then(|v| v.as_i64());
                relation.comment = row
                    .get(4)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string);
                relation
            })
            .collect();

        let views_sql = format!(
            "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS
             WHERE TABLE_SCHEMA {schema_filter} ORDER BY TABLE_NAME"
        );
        let views = self.query(&views_sql, &params).await?;
        relations.extend(views.rows.iter().map(|row| {
            let mut relation = zqlz_core::RawRelationRow::new(
                row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                zqlz_core::TableType::View,
            );
            relation.schema = schema.clone();
            relation.view_definition = row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
            relation
        }));

        Ok(relations)
    }

    async fn fetch_columns(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawColumnRow>> {
        let schema = self.resolved_schema(relation.schema.as_deref());
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));
        let result = self
            .query(&mysql_columns_sql(schema.as_deref(), true), &params)
            .await?;

        Ok(result.rows.iter().map(mysql_column_row).collect())
    }

    async fn fetch_all_columns(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawColumnRow>>>> {
        let schema = self.resolved_schema(schema);
        let params: Vec<zqlz_core::Value> = schema
            .as_deref()
            .map(|s| vec![zqlz_core::Value::String(s.to_string())])
            .unwrap_or_default();
        let result = self
            .query(&mysql_columns_sql(schema.as_deref(), false), &params)
            .await?;

        let mut columns_by_relation: HashMap<String, Vec<zqlz_core::RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(relation) = row.get(15).and_then(|value| value.as_str()) else {
                continue;
            };
            columns_by_relation
                .entry(relation.to_string())
                .or_default()
                .push(mysql_column_row(row));
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawIndexRow>> {
        let schema = self.resolved_schema(relation.schema.as_deref());
        let schema_filter = schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()");
        let sql = format!(
            "SELECT INDEX_NAME, NON_UNIQUE,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX),
                    INDEX_TYPE,
                    GROUP_CONCAT(COALESCE(COLLATION, '') ORDER BY SEQ_IN_INDEX),
                    GROUP_CONCAT(COALESCE(SUB_PART, '') ORDER BY SEQ_IN_INDEX),
                    MAX(COALESCE(COMMENT, ''))
             FROM information_schema.STATISTICS
             WHERE TABLE_SCHEMA {schema_filter} AND TABLE_NAME = ?
             GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
             ORDER BY INDEX_NAME"
        );
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));
        let result = self.query(&sql, &params).await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let name = mysql_text(row.get(0))?;
                let columns: Vec<String> = mysql_text(row.get(2))
                    .unwrap_or_default()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                let column_descending = mysql_text(row.get(4))
                    .unwrap_or_default()
                    .split(',')
                    .map(|value| value.eq_ignore_ascii_case("D"))
                    .collect();
                let include_columns = mysql_text(row.get(5))
                    .unwrap_or_default()
                    .split(',')
                    .enumerate()
                    .filter_map(|(index, value)| {
                        let value = value.trim();
                        if value.is_empty() {
                            None
                        } else {
                            columns.get(index).map(|column| format!("{column}({value})"))
                        }
                    })
                    .collect();
                Some(zqlz_core::RawIndexRow {
                    is_unique: row.get(1).and_then(|v| v.as_i64()).unwrap_or(1) == 0,
                    is_primary: name == "PRIMARY",
                    method: row.get(3).and_then(|v| v.as_str()).map(ToString::to_string),
                    comment: mysql_text(row.get(6)).filter(|s| !s.is_empty()),
                    column_descending,
                    include_columns,
                    columns,
                    name,
                    where_clause: None,
                })
            })
            .collect())
    }

    async fn fetch_foreign_keys(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawForeignKeyRow>> {
        let schema = self.resolved_schema(relation.schema.as_deref());
        let schema_filter = schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()");
        let sql = format!(
            "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
             FROM information_schema.KEY_COLUMN_USAGE
             WHERE TABLE_SCHEMA {schema_filter} AND TABLE_NAME = ?
               AND REFERENCED_TABLE_NAME IS NOT NULL
             ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"
        );
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));
        let result = self.query(&sql, &params).await?;

        let mut order: Vec<String> = Vec::new();
        let mut map: std::collections::HashMap<String, zqlz_core::RawForeignKeyRow> =
            std::collections::HashMap::new();
        for row in &result.rows {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let column = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_table = row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ref_column = row.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string();
            if !map.contains_key(&name) {
                order.push(name.clone());
                map.insert(
                    name.clone(),
                    zqlz_core::RawForeignKeyRow {
                        name: name.clone(),
                        referenced_schema: schema.clone(),
                        referenced_table: ref_table,
                        ..Default::default()
                    },
                );
            }
            let entry = map.get_mut(&name).expect("entry just inserted");
            entry.columns.push(column);
            entry.referenced_columns.push(ref_column);
        }

        // Attach ON UPDATE / ON DELETE rules.
        let rules_sql = format!(
            "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE
             FROM information_schema.REFERENTIAL_CONSTRAINTS
             WHERE CONSTRAINT_SCHEMA {schema_filter} AND TABLE_NAME = ?"
        );
        if let Ok(rules) = self.query(&rules_sql, &params).await {
            for row in &rules.rows {
                let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("");
                if let Some(entry) = map.get_mut(name) {
                    entry.on_update = row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
                    entry.on_delete = row.get(2).and_then(|v| v.as_str()).map(ToString::to_string);
                }
            }
        }

        Ok(order.into_iter().filter_map(|name| map.remove(&name)).collect())
    }

    async fn fetch_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawForeignKeyRow>>>> {
        let schema = self.resolved_schema(schema);
        let schema_filter = schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()");
        let params: Vec<zqlz_core::Value> = schema
            .as_deref()
            .map(|s| vec![zqlz_core::Value::String(s.to_string())])
            .unwrap_or_default();

        let result = self
            .query(
                &format!(
                    "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME, TABLE_NAME
                     FROM information_schema.KEY_COLUMN_USAGE
                     WHERE TABLE_SCHEMA {schema_filter}
                       AND REFERENCED_TABLE_NAME IS NOT NULL
                     ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"
                ),
                &params,
            )
            .await?;

        // Keyed by (table, constraint): MySQL constraint names are only unique
        // within a table, so grouping on the name alone merges unrelated keys.
        let mut order: Vec<(String, String)> = Vec::new();
        let mut map: HashMap<(String, String), zqlz_core::RawForeignKeyRow> = HashMap::new();
        for row in &result.rows {
            let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let table = row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let key = (table, name.clone());

            let entry = map.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                zqlz_core::RawForeignKeyRow {
                    name,
                    referenced_schema: schema.clone(),
                    referenced_table: row
                        .get(2)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    ..Default::default()
                }
            });
            entry
                .columns
                .push(row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string());
            entry
                .referenced_columns
                .push(row.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string());
        }

        let rules = self
            .query(
                &format!(
                    "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE, TABLE_NAME
                     FROM information_schema.REFERENTIAL_CONSTRAINTS
                     WHERE CONSTRAINT_SCHEMA {schema_filter}"
                ),
                &params,
            )
            .await;
        match rules {
            Ok(rules) => {
                for row in &rules.rows {
                    let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let table = row.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if let Some(entry) = map.get_mut(&(table, name)) {
                        entry.on_update =
                            row.get(1).and_then(|v| v.as_str()).map(ToString::to_string);
                        entry.on_delete =
                            row.get(2).and_then(|v| v.as_str()).map(ToString::to_string);
                    }
                }
            }
            // Referential rules are decoration; losing them must not lose the keys.
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL referential constraint rules");
            }
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
        let schema = self.resolved_schema(relation.schema.as_deref());
        let schema_filter = schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()");
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));

        let mut constraints = Vec::new();

        let check_sql = format!(
            "SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
             FROM information_schema.CHECK_CONSTRAINTS cc
             JOIN information_schema.TABLE_CONSTRAINTS tc
               ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
               AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
             WHERE tc.TABLE_SCHEMA {schema_filter} AND tc.TABLE_NAME = ?
               AND tc.CONSTRAINT_TYPE = 'CHECK'
             ORDER BY cc.CONSTRAINT_NAME"
        );
        if let Ok(result) = self.query(&check_sql, &params).await {
            for row in &result.rows {
                let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
                if name.ends_with("_chk") || !name.contains("chk") {
                    constraints.push(zqlz_core::RawConstraintRow {
                        name,
                        kind: "CHECK".to_string(),
                        columns: Vec::new(),
                        definition: row.get(1).and_then(|v| v.as_str()).map(ToString::to_string),
                    });
                }
            }
        }

        let unique_sql = format!(
            "SELECT tc.CONSTRAINT_NAME,
                    GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION)
             FROM information_schema.TABLE_CONSTRAINTS tc
             JOIN information_schema.KEY_COLUMN_USAGE kcu
               ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
               AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
               AND tc.TABLE_NAME = kcu.TABLE_NAME
             WHERE tc.TABLE_SCHEMA {schema_filter} AND tc.TABLE_NAME = ?
               AND tc.CONSTRAINT_TYPE = 'UNIQUE'
             GROUP BY tc.CONSTRAINT_NAME
             ORDER BY tc.CONSTRAINT_NAME"
        );
        if let Ok(result) = self.query(&unique_sql, &params).await {
            for row in &result.rows {
                let columns: Vec<String> = mysql_text(row.get(1))
                    .unwrap_or_default()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                constraints.push(zqlz_core::RawConstraintRow {
                    name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    kind: "UNIQUE".to_string(),
                    columns,
                    definition: None,
                });
            }
        }

        Ok(constraints)
    }

    async fn fetch_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<zqlz_core::RawTriggerRow>> {
        let schema = self.resolved_schema(schema);
        let schema_filter = schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()");
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        let table_filter = match table {
            Some(table) => {
                params.push(zqlz_core::Value::String(table.to_string()));
                "AND EVENT_OBJECT_TABLE = ?"
            }
            None => "",
        };
        let sql = format!(
            "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
             FROM information_schema.TRIGGERS
             WHERE TRIGGER_SCHEMA {schema_filter} {table_filter}
             ORDER BY TRIGGER_NAME"
        );
        let result = self.query(&sql, &params).await?;

        Ok(result
            .rows
            .iter()
            .map(|row| zqlz_core::RawTriggerRow {
                schema: schema.clone(),
                name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                table_name: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                timing: row.get(2).and_then(|v| v.as_str()).map(ToString::to_string),
                events: row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .map(|s| vec![s.to_string()])
                    .unwrap_or_default(),
                for_each: None,
                definition: row.get(4).and_then(|v| v.as_str()).map(ToString::to_string),
                enabled: true,
            })
            .collect())
    }
}

fn parse_mysql_enum_values(column_type: &str) -> Option<Vec<String>> {
    let open_paren = column_type.find('(')?;
    let close_paren = column_type.rfind(')')?;
    if close_paren <= open_paren {
        return None;
    }

    let inner = &column_type[open_paren + 1..close_paren];
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_string => {
                in_string = true;
                current.clear();
            }
            '\'' if in_string => {
                if chars.peek() == Some(&'\'') {
                    current.push('\'');
                    let _ = chars.next();
                } else {
                    in_string = false;
                    values.push(current.clone());
                }
            }
            _ if in_string => current.push(ch),
            _ => {}
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MysqlServerFeatures {
    is_mariadb: bool,
    major: u16,
    minor: u16,
}

impl MysqlServerFeatures {
    fn supports_events(&self) -> bool {
        true
    }

    fn supports_sequences(&self) -> bool {
        self.is_mariadb
    }
}

fn parse_mysql_server_features(version: &str, version_comment: &str) -> MysqlServerFeatures {
    let version_lower = version.to_ascii_lowercase();
    let comment_lower = version_comment.to_ascii_lowercase();
    let is_mariadb = version_lower.contains("mariadb") || comment_lower.contains("mariadb");
    let numeric_prefix = version
        .split(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .find(|part| part.chars().any(|ch| ch.is_ascii_digit()))
        .unwrap_or(version);
    let mut parts = numeric_prefix.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0);

    MysqlServerFeatures {
        is_mariadb,
        major,
        minor,
    }
}

fn mysql_quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn mysql_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn mysql_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema.filter(|schema| !schema.trim().is_empty()) {
        Some(schema) => format!(
            "{}.{}",
            mysql_quote_identifier(schema),
            mysql_quote_identifier(name)
        ),
        None => mysql_quote_identifier(name),
    }
}

fn mysql_object_form_string<'a>(
    values: &'a std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> &'a str {
    values
        .get(id)
        .and_then(ObjectFormValue::as_string)
        .unwrap_or_default()
        .trim()
}

fn mysql_object_form_bool(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> bool {
    values
        .get(id)
        .and_then(ObjectFormValue::as_bool)
        .unwrap_or(false)
}

fn mysql_object_form_string_list(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> Vec<String> {
    values
        .get(id)
        .and_then(ObjectFormValue::as_string_list)
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn mysql_function_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let function_return_type = mysql_object_form_string(values, "returns");
    let body = mysql_object_form_string(values, "body");
    if function_return_type.is_empty() {
        return Err(ZqlzError::Schema(
            "Function return type is required".to_string(),
        ));
    }
    if body.is_empty() {
        return Err(ZqlzError::Schema("Function body is required".to_string()));
    }

    let parameters = mysql_object_form_string_list(values, "parameters");
    let mut sql = format!(
        "CREATE FUNCTION {}({})\nRETURNS {}",
        mysql_qualified_name(schema, name),
        parameters.join(", "),
        function_return_type
    );
    if mysql_object_form_bool(values, "deterministic") {
        sql.push_str("\nDETERMINISTIC");
    }
    let sql_data_access = mysql_object_form_string(values, "sql_data_access");
    if !sql_data_access.is_empty() {
        sql.push_str(&format!("\n{}", sql_data_access));
    }
    sql.push_str(&format!("\n{}", body));
    Ok(format!("{};", sql.trim_end_matches(';')))
}

fn mysql_procedure_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let body = mysql_object_form_string(values, "body");
    if body.is_empty() {
        return Err(ZqlzError::Schema("Procedure body is required".to_string()));
    }

    let parameters = mysql_object_form_string_list(values, "parameters");
    let sql = format!(
        "CREATE PROCEDURE {}({})\n{}",
        mysql_qualified_name(schema, name),
        parameters.join(", "),
        body
    );
    Ok(format!("{};", sql.trim_end_matches(';')))
}

fn mysql_trigger_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let table = mysql_object_form_string(values, "table");
    let body = mysql_object_form_string(values, "body");
    if table.is_empty() {
        return Err(ZqlzError::Schema("Trigger table is required".to_string()));
    }
    if body.is_empty() {
        return Err(ZqlzError::Schema("Trigger body is required".to_string()));
    }

    let timing = mysql_object_form_string(values, "timing");
    let timing = if timing.is_empty() { "BEFORE" } else { timing };
    let event = mysql_object_form_string(values, "event");
    let event = if event.is_empty() { "INSERT" } else { event };
    let sql = format!(
        "CREATE TRIGGER {}\n{} {} ON {}\nFOR EACH ROW\n{}",
        mysql_qualified_name(schema, name),
        timing,
        event,
        mysql_qualified_name(schema, table),
        body
    );
    Ok(format!("{};", sql.trim_end_matches(';')))
}

fn mysql_index_ddl(table: &str, index: &IndexInfo) -> String {
    let columns = index
        .columns
        .iter()
        .enumerate()
        .map(|(index_position, column)| {
            let mut column_sql = mysql_quote_identifier(column);
            if index
                .column_descending
                .get(index_position)
                .copied()
                .unwrap_or(false)
            {
                column_sql.push_str(" DESC");
            }
            column_sql
        })
        .collect::<Vec<_>>()
        .join(", ");
    let index_type = index.index_type.to_ascii_uppercase();
    let create_keyword = if index_type == "FULLTEXT" || index_type == "SPATIAL" {
        format!("CREATE {index_type} INDEX")
    } else if index.is_unique {
        "CREATE UNIQUE INDEX".to_string()
    } else {
        "CREATE INDEX".to_string()
    };
    let using_clause = if matches!(index_type.as_str(), "BTREE" | "HASH") {
        format!(" USING {index_type}")
    } else {
        String::new()
    };
    let comment_clause = index
        .comment
        .as_ref()
        .filter(|comment| !comment.is_empty())
        .map(|comment| format!(" COMMENT {}", mysql_quote_literal(comment)))
        .unwrap_or_default();

    format!(
        "{} {} ON {}{} ({}){};",
        create_keyword,
        mysql_quote_identifier(&index.name),
        mysql_quote_identifier(table),
        using_clause,
        columns,
        comment_clause
    )
}

fn mysql_constraint_ddl(table: &str, constraint: &ConstraintInfo) -> Result<String> {
    match constraint.constraint_type {
        ConstraintType::Unique => Ok(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});",
            mysql_quote_identifier(table),
            mysql_quote_identifier(&constraint.name),
            constraint
                .columns
                .iter()
                .map(|column| mysql_quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        ConstraintType::Check => {
            let Some(definition) = constraint.definition.as_deref() else {
                return Err(ZqlzError::Query(format!(
                    "Could not reconstruct CHECK constraint '{}' without definition",
                    constraint.name
                )));
            };
            Ok(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({});",
                mysql_quote_identifier(table),
                mysql_quote_identifier(&constraint.name),
                definition
            ))
        }
        ConstraintType::PrimaryKey => Ok(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({});",
            mysql_quote_identifier(table),
            mysql_quote_identifier(&constraint.name),
            constraint
                .columns
                .iter()
                .map(|column| mysql_quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        ConstraintType::ForeignKey | ConstraintType::Exclusion => {
            Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} constraints is not supported in MySQL",
                constraint.constraint_type
            )))
        }
    }
}

fn mysql_foreign_key_action_clause(prefix: &str, action: ForeignKeyAction) -> &'static str {
    match (prefix, action) {
        ("ON UPDATE", ForeignKeyAction::Cascade) => " ON UPDATE CASCADE",
        ("ON UPDATE", ForeignKeyAction::SetNull) => " ON UPDATE SET NULL",
        ("ON UPDATE", ForeignKeyAction::SetDefault) => " ON UPDATE SET DEFAULT",
        ("ON UPDATE", ForeignKeyAction::Restrict) => " ON UPDATE RESTRICT",
        ("ON DELETE", ForeignKeyAction::Cascade) => " ON DELETE CASCADE",
        ("ON DELETE", ForeignKeyAction::SetNull) => " ON DELETE SET NULL",
        ("ON DELETE", ForeignKeyAction::SetDefault) => " ON DELETE SET DEFAULT",
        ("ON DELETE", ForeignKeyAction::Restrict) => " ON DELETE RESTRICT",
        _ => "",
    }
}

fn mysql_foreign_key_ddl(table: &str, foreign_key: &ForeignKeyInfo) -> String {
    let referenced_table = match foreign_key.referenced_schema.as_deref() {
        Some(schema) if !schema.is_empty() => {
            format!(
                "{}.{}",
                mysql_quote_identifier(schema),
                mysql_quote_identifier(&foreign_key.referenced_table)
            )
        }
        _ => mysql_quote_identifier(&foreign_key.referenced_table),
    };

    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}){}{};",
        mysql_quote_identifier(table),
        mysql_quote_identifier(&foreign_key.name),
        foreign_key
            .columns
            .iter()
            .map(|column| mysql_quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        referenced_table,
        foreign_key
            .referenced_columns
            .iter()
            .map(|column| mysql_quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        mysql_foreign_key_action_clause("ON UPDATE", foreign_key.on_update),
        mysql_foreign_key_action_clause("ON DELETE", foreign_key.on_delete)
    )
}

fn mysql_metadata_kind_degrades_on_error(kind_id: &str) -> bool {
    matches!(
        kind_id,
        "tablespace"
            | "user"
            | "role"
            | "grant"
            | "plugin"
            | "variable"
            | "status_variable"
            | "process"
            | "replica_status"
    )
}

fn mysql_metadata_unavailable_row(kind_id: &str, error: &ZqlzError) -> ObjectsPanelRow {
    mysql_metadata_row(
        kind_id,
        "Metadata unavailable".to_string(),
        None,
        None,
        "Privilege or catalog error",
        error.to_string(),
        "Unavailable",
        "Grant access to the admin catalog or hide this metadata kind.",
    )
}

/// Keep the MySQL objects-panel column set in one place so the manifest and
/// row loader stay aligned when metadata changes.
fn mysql_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
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
        ObjectsPanelColumn::new("row_format", "Row Format")
            .width(100.0)
            .min_width(60.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("auto_increment", "Auto Increment")
            .width(120.0)
            .min_width(70.0)
            .resizable(true)
            .sortable()
            .text_right(),
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
        ObjectsPanelColumn::new("create_options", "Create Options")
            .width(180.0)
            .min_width(80.0)
            .resizable(true),
    ]
}

/// Tables expose the MySQL parity action palette directly from metadata so we
/// do not fall back to driver-agnostic defaults in the panel layer.
fn mysql_table_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("rename", "Rename")
            .group("modify")
            .single_selection(),
        ObjectsPanelAction::new("duplicate", "Duplicate").group("modify"),
        ObjectsPanelAction::new("empty", "Empty")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("import", "Import")
            .group("data")
            .single_selection(),
        ObjectsPanelAction::new("export", "Export").group("data"),
        ObjectsPanelAction::new("dump_sql_structure_data", "Dump SQL (Structure + Data)")
            .group("data"),
        ObjectsPanelAction::new("dump_sql_structure", "Dump SQL (Structure Only)").group("data"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("delete", "Delete")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

/// Views keep the same explicit metadata-driven contract as tables, but with
/// the smaller action set that MySQL actually supports for views.
fn mysql_view_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("rename", "Rename")
            .group("modify")
            .single_selection(),
        ObjectsPanelAction::new("duplicate", "Duplicate").group("modify"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("export", "Export").group("data"),
        ObjectsPanelAction::new("delete", "Delete")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

/// The toolbar remains explicit so unsupported actions are omitted by contract
/// instead of being filtered later by UI branching.
fn mysql_toolbar_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("refresh", "Refresh")
            .icon_key("refresh")
            .refreshes_objects_panel(),
        ObjectsPanelAction::new("new_database", "New Database")
            .icon_key("create")
            .create_object_kind("database")
            .object_form("database", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_table", "New Table")
            .icon_key("create")
            .create_object_kind("table"),
        ObjectsPanelAction::new("new_view", "New View")
            .icon_key("create")
            .create_object_kind("view"),
        ObjectsPanelAction::new("new_function", "New Function")
            .icon_key("create")
            .create_object_kind("function")
            .object_form("function", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_procedure", "New Procedure")
            .icon_key("create")
            .create_object_kind("procedure")
            .object_form("procedure", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_event", "New Event")
            .icon_key("create")
            .create_object_kind("event")
            .object_form("event", ObjectFormMode::Create),
        ObjectsPanelAction::new("import", "Import Wizard...").icon_key("import"),
        ObjectsPanelAction::new("export", "Export Wizard...").icon_key("export"),
    ]
}

/// Keep MySQL routine kinds aligned on the same identity columns so their
/// manifests only need to describe kind-specific details.
fn mysql_named_object_columns(extra_columns: Vec<ObjectsPanelColumn>) -> Vec<ObjectsPanelColumn> {
    let mut columns = vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(250.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("schema_name", "Schema")
            .width(140.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
    ];

    columns.extend(extra_columns);
    columns
}

fn mysql_function_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("return_type", "Return Type")
            .width(140.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("parameters", "Parameters")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("deterministic", "Deterministic")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_data_access", "SQL Data Access")
            .width(150.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("security_type", "Security")
            .width(110.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("modified", "Modified")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(240.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

fn mysql_procedure_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("parameters", "Parameters")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("sql_data_access", "SQL Data Access")
            .width(150.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("security_type", "Security")
            .width(110.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("modified", "Modified")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(240.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

fn mysql_trigger_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("table_name", "Table")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("timing", "Timing")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("event", "Event")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("action_order", "Order")
            .width(80.0)
            .min_width(60.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
        ObjectsPanelColumn::new("character_set_client", "Client Charset")
            .width(130.0)
            .min_width(90.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("collation_connection", "Connection Collation")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
    ])
}

fn mysql_database_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(250.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("default_charset", "Charset")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("default_collation", "Collation")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("table_count", "Tables")
            .width(90.0)
            .min_width(60.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("size", "Size")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable()
            .text_right(),
    ]
}

fn mysql_event_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("status", "Status")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("schedule", "Schedule")
            .width(220.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

fn mysql_metadata_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("table_name", "Table")
            .width(160.0)
            .min_width(90.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("kind", "Kind")
            .width(130.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("detail", "Detail")
            .width(260.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("state", "State")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
    ])
}

/// MySQL routines and triggers share the same compact action palette so the
/// manifest stays metadata-driven across all routine kinds.
fn mysql_routine_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

fn mysql_metadata_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

fn mysql_event_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("delete", "Drop")
            .group("danger")
            .single_selection()
            .destructive(),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

fn mysql_display_name(
    qualify_with_schema: bool,
    schema_name: Option<&str>,
    object_name: &str,
) -> String {
    if qualify_with_schema {
        schema_name
            .map(|schema_name| format!("{schema_name}.{object_name}"))
            .unwrap_or_else(|| object_name.to_string())
    } else {
        object_name.to_string()
    }
}

fn mysql_routine_object_ref(
    kind_id: &str,
    name: String,
    schema_name: Option<String>,
) -> ObjectsPanelObjectRef {
    ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(schema_name)
}

fn mysql_display_value(row: &zqlz_core::Row, index: usize) -> String {
    row.get(index)
        .map(ToString::to_string)
        .filter(|value| value != "NULL" && !value.is_empty())
        .unwrap_or_else(|| "-".to_string())
}

fn mysql_routine_parameters_label(parameters: &[ParameterInfo]) -> String {
    if parameters.is_empty() {
        return "-".to_string();
    }

    parameters
        .iter()
        .map(|parameter| {
            let mode = match parameter.mode {
                ParameterMode::In => "IN",
                ParameterMode::Out => "OUT",
                ParameterMode::InOut => "INOUT",
                ParameterMode::Variadic => "VARIADIC",
            };
            match parameter.name.as_deref() {
                Some(name) => format!("{mode} {name} {}", parameter.data_type),
                None => format!("{mode} {}", parameter.data_type),
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn mysql_routine_panel_row(
    kind_id: &str,
    row: &zqlz_core::Row,
    parameters: Vec<ParameterInfo>,
    qualify_with_schema: bool,
) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let return_type = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
    let definition = row
        .get(3)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let comment = row
        .get(4)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let definer = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
    let security_type = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
    let deterministic = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
    let sql_data_access = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
    let sql_mode = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
    let created = mysql_display_value(row, 10);
    let modified = mysql_display_value(row, 11);

    let display_name = mysql_display_name(qualify_with_schema, schema.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("return_type".to_string(), return_type.to_string());
    values.insert(
        "parameters".to_string(),
        mysql_routine_parameters_label(&parameters),
    );
    values.insert("deterministic".to_string(), deterministic.to_string());
    values.insert("sql_data_access".to_string(), sql_data_access.to_string());
    values.insert("security_type".to_string(), security_type.to_string());
    values.insert("definer".to_string(), definer.to_string());
    values.insert("created".to_string(), created);
    values.insert("modified".to_string(), modified);
    values.insert("sql_mode".to_string(), sql_mode.to_string());
    values.insert("definition".to_string(), definition.to_string());
    values.insert("comment".to_string(), comment.to_string());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(mysql_routine_object_ref(kind_id, name, schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
fn mysql_function_row(function: FunctionInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let FunctionInfo {
        schema,
        name,
        return_type,
        definition,
        comment,
        ..
    } = function;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("return_type".to_string(), return_type);
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "function".to_string(),
        object_ref: Some(mysql_routine_object_ref("function", name, schema_name)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
fn mysql_procedure_row(procedure: ProcedureInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let ProcedureInfo {
        schema,
        name,
        definition,
        comment,
        ..
    } = procedure;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "procedure".to_string(),
        object_ref: Some(mysql_routine_object_ref("procedure", name, schema_name)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
fn mysql_trigger_timing_label(timing: TriggerTiming) -> &'static str {
    match timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    }
}

#[cfg(test)]
fn mysql_trigger_event_label(events: &[TriggerEvent]) -> String {
    let labels: Vec<&str> = events
        .iter()
        .map(|event| match event {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
            TriggerEvent::Truncate => "TRUNCATE",
        })
        .collect();

    if labels.is_empty() {
        "-".to_string()
    } else {
        labels.join(", ")
    }
}

#[cfg(test)]
fn mysql_trigger_row(trigger: TriggerInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let TriggerInfo {
        schema,
        name,
        table_name,
        timing,
        events,
        definition,
        comment,
        enabled: _,
        ..
    } = trigger;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let associated_table = table_name.clone();
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name);
    values.insert(
        "timing".to_string(),
        mysql_trigger_timing_label(timing).to_string(),
    );
    values.insert("event".to_string(), mysql_trigger_event_label(&events));
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "trigger".to_string(),
        object_ref: Some(
            mysql_routine_object_ref("trigger", name, schema_name)
                .with_signature_option(Some(associated_table)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn mysql_trigger_panel_row(row: &zqlz_core::Row, qualify_with_schema: bool) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let table_name = row
        .get(2)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let display_name = mysql_display_name(qualify_with_schema, schema.as_deref(), &name);

    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name.clone());
    values.insert("timing".to_string(), mysql_display_value(row, 3));
    values.insert("event".to_string(), mysql_display_value(row, 4));
    values.insert("action_order".to_string(), mysql_display_value(row, 5));
    values.insert("definition".to_string(), mysql_display_value(row, 6));
    values.insert("orientation".to_string(), mysql_display_value(row, 7));
    values.insert("definer".to_string(), mysql_display_value(row, 8));
    values.insert("sql_mode".to_string(), mysql_display_value(row, 9));
    values.insert("created".to_string(), mysql_display_value(row, 10));
    values.insert(
        "character_set_client".to_string(),
        mysql_display_value(row, 11),
    );
    values.insert(
        "collation_connection".to_string(),
        mysql_display_value(row, 12),
    );
    values.insert(
        "database_collation".to_string(),
        mysql_display_value(row, 13),
    );

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: "trigger".to_string(),
        object_ref: Some(
            mysql_routine_object_ref("trigger", name, schema)
                .with_signature_option(Some(table_name)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn mysql_metadata_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    table_name: Option<String>,
    kind: impl Into<String>,
    detail: impl Into<String>,
    state: impl Into<String>,
    comment: impl Into<String>,
) -> ObjectsPanelRow {
    let display_name = mysql_display_name(true, schema.as_deref(), &name);
    let associated_table = table_name;
    let table_name = associated_table
        .as_deref()
        .filter(|table_name| !table_name.is_empty())
        .unwrap_or("-")
        .to_string();
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name);
    values.insert("kind".to_string(), kind.into());
    values.insert("detail".to_string(), detail.into());
    values.insert("state".to_string(), state.into());
    values.insert("comment".to_string(), comment.into());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new(kind_id, name)
                .with_schema_option(schema)
                .with_signature_option(associated_table),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn mysql_event_row(row: &zqlz_core::Row) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let status = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
    let interval_value = row.get(3).and_then(|value| value.as_str());
    let interval_field = row.get(4).and_then(|value| value.as_str());
    let execute_at = row.get(5).map(ToString::to_string);
    let starts = row.get(6).map(ToString::to_string);
    let ends = row.get(7).map(ToString::to_string);
    let definer = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
    let created = row
        .get(9)
        .map(ToString::to_string)
        .filter(|value| value != "NULL")
        .unwrap_or_else(|| "-".to_string());
    let comment = row
        .get(10)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");

    let schedule = match (interval_value, interval_field, execute_at) {
        (Some(value), Some(field), _) if !value.is_empty() && !field.is_empty() => {
            format!("EVERY {value} {field}")
        }
        (_, _, Some(value)) if value != "NULL" => format!("AT {value}"),
        _ => "-".to_string(),
    };
    let schedule = match (starts, ends) {
        (Some(starts), Some(ends)) if starts != "NULL" && ends != "NULL" => {
            format!("{schedule}; STARTS {starts}; ENDS {ends}")
        }
        (Some(starts), _) if starts != "NULL" => format!("{schedule}; STARTS {starts}"),
        (_, Some(ends)) if ends != "NULL" => format!("{schedule}; ENDS {ends}"),
        _ => schedule,
    };

    let display_name = mysql_display_name(true, schema.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("status".to_string(), status.to_string());
    values.insert("schedule".to_string(), schedule);
    values.insert("definer".to_string(), definer.to_string());
    values.insert("created".to_string(), created);
    values.insert("comment".to_string(), comment.to_string());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: "event".to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new("event", name).with_schema_option(schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

/// Build the MySQL manifest from driver-owned metadata so parity validation can
/// exercise a stable contract rather than an inferred fallback manifest.
fn mysql_objects_panel_manifest(features: &MysqlServerFeatures) -> ObjectsPanelManifest {
    let routine_actions = mysql_routine_row_actions();
    let metadata_columns = mysql_metadata_columns();
    let metadata_actions = mysql_metadata_row_actions();

    let mut object_kinds = vec![
        ObjectsPanelObjectKind::new("database", "Database", "Databases")
            .icon_key("database")
            .columns(mysql_database_columns())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("table", "Table", "Tables")
            .icon_key("table")
            .columns(mysql_objects_panel_columns())
            .row_actions(mysql_table_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("view", "View", "Views")
            .icon_key("view")
            .columns(mysql_objects_panel_columns())
            .row_actions(mysql_view_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("function", "Function", "Functions")
            .icon_key("function")
            .columns(mysql_function_columns())
            .row_actions(routine_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("procedure", "Procedure", "Procedures")
            .icon_key("procedure")
            .columns(mysql_procedure_columns())
            .row_actions(routine_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("trigger", "Trigger", "Triggers")
            .icon_key("trigger")
            .columns(mysql_trigger_columns())
            .row_actions(routine_actions)
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("event", "Event", "Events")
            .icon_key("event")
            .columns(mysql_event_columns())
            .row_actions(mysql_event_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("index", "Index", "Indexes")
            .icon_key("index")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("constraint", "Constraint", "Constraints")
            .icon_key("constraint")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("foreign_key", "Foreign Key", "Foreign Keys")
            .icon_key("foreign_key")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("partition", "Partition", "Partitions")
            .icon_key("partition")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("tablespace", "Tablespace", "Tablespaces")
            .icon_key("tablespace")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("engine", "Engine", "Engines")
            .icon_key("engine")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("charset", "Charset", "Charsets")
            .icon_key("charset")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("collation", "Collation", "Collations")
            .icon_key("collation")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("user", "User", "Users")
            .icon_key("user")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("role", "Role", "Roles")
            .icon_key("role")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("grant", "Grant", "Grants")
            .icon_key("grant")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("plugin", "Plugin", "Plugins")
            .icon_key("plugin")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("variable", "Variable", "Variables")
            .icon_key("variable")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("status_variable", "Status Variable", "Status Variables")
            .icon_key("variable")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("process", "Process", "Processes")
            .icon_key("process")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("replica_status", "Replica Status", "Replica Status")
            .icon_key("replica")
            .columns(metadata_columns)
            .row_actions(metadata_actions)
            .default_row_action("open"),
    ];

    if features.supports_sequences() {
        object_kinds.insert(
            11,
            ObjectsPanelObjectKind::new("sequence", "Sequence", "Sequences")
                .icon_key("sequence")
                .columns(mysql_metadata_columns())
                .row_actions(mysql_metadata_row_actions())
                .default_row_action("open"),
        );
    }

    ObjectsPanelManifest {
        object_kinds,
        toolbar_actions: mysql_toolbar_actions(),
    }
}

impl MySqlConnection {
    async fn mysql_server_features(&self) -> Result<MysqlServerFeatures> {
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

        Ok(parse_mysql_server_features(version, version_comment))
    }

    async fn mysql_database_objects_panel_data(&self) -> Result<ObjectsPanelData> {
        let result = self
            .query(
                "SELECT
                    s.SCHEMA_NAME,
                    s.DEFAULT_CHARACTER_SET_NAME,
                    s.DEFAULT_COLLATION_NAME,
                    COUNT(t.TABLE_NAME) AS TABLE_COUNT,
                    COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH), 0) AS SIZE_BYTES
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

    async fn mysql_event_objects_panel_data(
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
        let rows = result.rows.iter().map(mysql_event_row).collect();

        Ok(ObjectsPanelData {
            columns: mysql_event_columns(),
            rows,
        })
    }

    async fn mysql_metadata_objects_panel_data(
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

    async fn mysql_index_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE,
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX),
                        GROUP_CONCAT(COALESCE(COLLATION, '') ORDER BY SEQ_IN_INDEX),
                        MAX(COALESCE(COMMENT, ''))
                 FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = ?
                 GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE,
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX),
                        GROUP_CONCAT(COALESCE(COLLATION, '') ORDER BY SEQ_IN_INDEX),
                        MAX(COALESCE(COMMENT, ''))
                 FROM information_schema.STATISTICS
                 GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME",
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
                let non_unique = row.get(3).and_then(|value| value.as_i64()).unwrap_or(1);
                let index_type = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let columns = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
                let directions = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
                let comment = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
                let kind = if name == "PRIMARY" {
                    "Primary"
                } else if non_unique == 0 {
                    "Unique"
                } else {
                    "Index"
                };
                let detail = format!("{index_type}: {columns} ({directions})");
                mysql_metadata_row("index", name, schema, table, kind, detail, "-", comment)
            })
            .collect())
    }

    async fn mysql_constraint_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
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

    async fn mysql_foreign_key_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME,
                        rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
                 FROM information_schema.REFERENTIAL_CONSTRAINTS rc
                 WHERE rc.CONSTRAINT_SCHEMA = ?
                 ORDER BY rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT rc.CONSTRAINT_SCHEMA, rc.TABLE_NAME, rc.CONSTRAINT_NAME,
                        rc.REFERENCED_TABLE_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
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

    async fn mysql_partition_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (sql, params) = if let Some(schema_name) = schema {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, PARTITION_METHOD,
                        PARTITION_EXPRESSION, TABLE_ROWS, PARTITION_COMMENT
                 FROM information_schema.PARTITIONS
                 WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, PARTITION_METHOD,
                        PARTITION_EXPRESSION, TABLE_ROWS, PARTITION_COMMENT
                 FROM information_schema.PARTITIONS
                 WHERE PARTITION_NAME IS NOT NULL
                 ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION",
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
                let method = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let expression = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let rows = row
                    .get(5)
                    .and_then(|value| value.as_i64())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let comment = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "partition",
                    name,
                    schema,
                    table,
                    method,
                    format!("{expression}; rows {rows}"),
                    "-",
                    comment,
                )
            })
            .collect())
    }

    async fn mysql_sequence_rows(&self, schema: Option<&str>) -> Result<Vec<ObjectsPanelRow>> {
        let features = self.mysql_server_features().await?;
        if !features.supports_sequences() {
            return Ok(Vec::new());
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
        let result = self.query(sql, &params).await?;
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

    async fn mysql_tablespace_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT TABLESPACE_NAME, ENGINE, TABLESPACE_TYPE, FILE_NAME
                 FROM information_schema.FILES
                 WHERE TABLESPACE_NAME IS NOT NULL
                 GROUP BY TABLESPACE_NAME, ENGINE, TABLESPACE_TYPE, FILE_NAME
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

    async fn mysql_engine_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
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

    async fn mysql_charset_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
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

    async fn mysql_collation_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
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

    async fn mysql_user_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT User, Host, plugin, account_locked
                 FROM mysql.user
                 ORDER BY User, Host",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL users");
                return Err(error);
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let user = row.get(0).and_then(|value| value.as_str()).unwrap_or("");
                let host = row.get(1).and_then(|value| value.as_str()).unwrap_or("%");
                let plugin = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let locked = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "user",
                    format!("{user}@{host}"),
                    None,
                    None,
                    plugin,
                    host,
                    locked,
                    "-",
                )
            })
            .collect())
    }

    async fn mysql_role_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT User, Host FROM mysql.user WHERE is_role = 'Y' ORDER BY User, Host",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return Err(error),
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let user = row.get(0).and_then(|value| value.as_str()).unwrap_or("");
                let host = row.get(1).and_then(|value| value.as_str()).unwrap_or("%");
                mysql_metadata_row(
                    "role",
                    format!("{user}@{host}"),
                    None,
                    None,
                    "Role",
                    host,
                    "-",
                    "-",
                )
            })
            .collect())
    }

    async fn mysql_grant_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT GRANTEE, TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE, IS_GRANTABLE
                 FROM information_schema.TABLE_PRIVILEGES
                 ORDER BY GRANTEE, TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL grants");
                return Err(error);
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let grantee = row.get(0).and_then(|value| value.as_str()).unwrap_or("");
                let schema = row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let table = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let privilege = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let grantable = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "grant",
                    format!("{grantee}:{privilege}"),
                    schema,
                    table,
                    privilege,
                    grantee,
                    grantable,
                    "-",
                )
            })
            .collect())
    }

    async fn mysql_plugin_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW PLUGINS", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let status = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let kind = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("Plugin");
                let library = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let license = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row("plugin", name, None, None, kind, library, status, license)
            })
            .collect())
    }

    async fn mysql_variable_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW VARIABLES", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let value = row
                    .get(1)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row("variable", name, None, None, "Variable", value, "-", "-")
            })
            .collect())
    }

    async fn mysql_status_variable_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW STATUS", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let value = row
                    .get(1)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "status_variable",
                    name,
                    None,
                    None,
                    "Status",
                    value,
                    "-",
                    "-",
                )
            })
            .collect())
    }

    async fn mysql_process_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW PROCESSLIST", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let id = row
                    .get(0)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "".to_string());
                let user = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let host = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let db = row
                    .get(3)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let command = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let state = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
                let info = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "process",
                    id,
                    db,
                    None,
                    command,
                    host,
                    state,
                    format!("{user}; {info}"),
                )
            })
            .collect())
    }

    async fn mysql_replica_status_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self.query("SHOW REPLICA STATUS", &[]).await {
            Ok(result) => result,
            Err(_) => match self.query("SHOW SLAVE STATUS", &[]).await {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(%error, "Failed to load MySQL replica status");
                    return Err(error);
                }
            },
        };
        Ok(result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let source = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let io_state = row.get(0).and_then(|value| value.as_str()).unwrap_or("-");
                let io_running = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
                let sql_running = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "replica_status",
                    format!("replica_{}", index + 1),
                    None,
                    None,
                    "Replica",
                    source,
                    format!("IO {io_running}; SQL {sql_running}"),
                    io_state,
                )
            })
            .collect())
    }

    async fn mysql_routine_parameters(
        &self,
        schema: Option<&str>,
        routine_name: &str,
        routine_type: &str,
    ) -> Result<Vec<ParameterInfo>> {
        let schema = schema.or(self.default_database());
        let Some(schema_name) = schema else {
            return Ok(Vec::new());
        };

        let result = self
            .query(
                "SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
                 FROM information_schema.PARAMETERS
                 WHERE SPECIFIC_SCHEMA = ?
                   AND SPECIFIC_NAME = ?
                   AND ROUTINE_TYPE = ?
                   AND ORDINAL_POSITION > 0
                 ORDER BY ORDINAL_POSITION",
                &[
                    Value::String(schema_name.to_string()),
                    Value::String(routine_name.to_string()),
                    Value::String(routine_type.to_string()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let mode = match row.get(2).and_then(|value| value.as_str()).unwrap_or("IN") {
                    "OUT" => ParameterMode::Out,
                    "INOUT" => ParameterMode::InOut,
                    _ => ParameterMode::In,
                };

                ParameterInfo {
                    name: row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    data_type: row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string(),
                    mode,
                    default_value: None,
                    ordinal: row
                        .get(3)
                        .and_then(|value| value.as_i64())
                        .and_then(|value| usize::try_from(value).ok())
                        .unwrap_or(0),
                }
            })
            .collect())
    }

    async fn mysql_routine_panel_rows(
        &self,
        schema: Option<&str>,
        routine_type: &str,
        qualify_with_schema: bool,
    ) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION,
                        ROUTINE_COMMENT, DEFINER, SECURITY_TYPE, IS_DETERMINISTIC,
                        SQL_DATA_ACCESS, SQL_MODE, CREATED, LAST_ALTERED
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = ?
                 ORDER BY ROUTINE_NAME",
                vec![
                    Value::String(schema_name.to_string()),
                    Value::String(routine_type.to_string()),
                ],
            )
        } else {
            (
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION,
                        ROUTINE_COMMENT, DEFINER, SECURITY_TYPE, IS_DETERMINISTIC,
                        SQL_DATA_ACCESS, SQL_MODE, CREATED, LAST_ALTERED
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = ?
                 ORDER BY ROUTINE_NAME",
                vec![Value::String(routine_type.to_string())],
            )
        };
        let result = self.query(query, &params).await?;
        let kind_id = if routine_type.eq_ignore_ascii_case("FUNCTION") {
            "function"
        } else {
            "procedure"
        };

        let mut rows = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let parameters = self
                .mysql_routine_parameters(schema_name.as_deref().or(schema), &name, routine_type)
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(
                        %error,
                        routine = %name,
                        routine_type,
                        "Failed to load MySQL routine parameters for objects panel"
                    );
                    Vec::new()
                });
            rows.push(mysql_routine_panel_row(
                kind_id,
                row,
                parameters,
                qualify_with_schema,
            ));
        }

        Ok(rows)
    }

    async fn mysql_trigger_panel_rows(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
        qualify_with_schema: bool,
    ) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let select_sql = "SELECT
                    TRIGGER_SCHEMA,
                    TRIGGER_NAME,
                    EVENT_OBJECT_TABLE,
                    ACTION_TIMING,
                    EVENT_MANIPULATION,
                    ACTION_ORDER,
                    ACTION_STATEMENT,
                    ACTION_ORIENTATION,
                    DEFINER,
                    SQL_MODE,
                    CREATED,
                    CHARACTER_SET_CLIENT,
                    COLLATION_CONNECTION,
                    DATABASE_COLLATION
                 FROM information_schema.TRIGGERS";
        let (query, params) = match (schema, table) {
            (Some(schema_name), Some(table_name)) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![
                    Value::String(schema_name.to_string()),
                    Value::String(table_name.to_string()),
                ],
            ),
            (Some(schema_name), None) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![Value::String(schema_name.to_string())],
            ),
            (None, Some(table_name)) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![Value::String(table_name.to_string())],
            ),
            (None, None) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = DATABASE()
                     ORDER BY TRIGGER_NAME"
                ),
                Vec::new(),
            ),
        };

        let result = self.query(&query, &params).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| mysql_trigger_panel_row(row, qualify_with_schema))
            .collect())
    }
}

#[async_trait]
impl SchemaIntrospection for MySqlConnection {
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT
                    s.SCHEMA_NAME,
                    s.DEFAULT_CHARACTER_SET_NAME,
                    s.DEFAULT_COLLATION_NAME,
                    COALESCE(SUM(t.DATA_LENGTH + t.INDEX_LENGTH), 0) AS SIZE_BYTES
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
                    name,
                    owner: None,
                    encoding: Some(charset.to_string()),
                    size_bytes: row.get(3).and_then(|value| value.as_i64()),
                    comment: Some(format!("Default collation: {collation}")),
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
                "SELECT ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION, ROUTINE_COMMENT
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'FUNCTION'
                 ORDER BY ROUTINE_NAME",
                vec![Value::String(schema_name.to_string())],
            )
        } else {
            (
                "SELECT ROUTINE_NAME, DATA_TYPE, ROUTINE_DEFINITION, ROUTINE_COMMENT
             FROM information_schema.ROUTINES
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION'
             ORDER BY ROUTINE_NAME",
                Vec::new(),
            )
        };

        let result = self.query(query, &params).await?;

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
            let parameters = self
                    .mysql_routine_parameters(schema, &name, "FUNCTION")
                    .await
                    .unwrap_or_else(|error| {
                        tracing::warn!(%error, function = %name, "Failed to load MySQL function parameters");
                        Vec::new()
                    });

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
            let parameters = self
                    .mysql_routine_parameters(schema, &name, "PROCEDURE")
                    .await
                    .unwrap_or_else(|error| {
                        tracing::warn!(%error, procedure = %name, "Failed to load MySQL procedure parameters");
                        Vec::new()
                    });

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
        let qualify_with_schema = schema.is_none();

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

        let mut rows: Vec<ObjectsPanelRow> = result
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

                let comment = row
                    .get(10)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty() && *s != "NULL")
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
        match kind_id {
            "database" => self.mysql_database_objects_panel_data().await,
            "table" | "view" => Ok(self
                .list_tables_extended(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            "function" => {
                let rows = self
                    .mysql_routine_panel_rows(schema, "FUNCTION", schema.is_none())
                    .await?;
                Ok(ObjectsPanelData {
                    columns: mysql_function_columns(),
                    rows,
                })
            }
            "procedure" => {
                let rows = self
                    .mysql_routine_panel_rows(schema, "PROCEDURE", schema.is_none())
                    .await?;
                Ok(ObjectsPanelData {
                    columns: mysql_procedure_columns(),
                    rows,
                })
            }
            "trigger" => {
                let rows = self
                    .mysql_trigger_panel_rows(schema, None, schema.is_none())
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

                ddl_from_show_create(&result.rows, &[1], "table", &object.name)
            }
            ObjectType::View => {
                let query = format!(
                    "SHOW CREATE VIEW {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[1], "view", &object.name)
            }
            ObjectType::Function => {
                let query = format!(
                    "SHOW CREATE FUNCTION {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "function", &object.name)
            }
            ObjectType::Procedure => {
                let query = format!(
                    "SHOW CREATE PROCEDURE {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "procedure", &object.name)
            }
            ObjectType::Trigger => {
                let query = format!(
                    "SHOW CREATE TRIGGER {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[2], "trigger", &object.name)
            }
            ObjectType::Event => {
                let query = format!(
                    "SHOW CREATE EVENT {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
                let result = self.query(&query, &[]).await?;

                ddl_from_show_create(&result.rows, &[3, 2], "event", &object.name)
            }
            ObjectType::Sequence => {
                let query = format!(
                    "SHOW CREATE SEQUENCE {}`{}`",
                    schema_prefix,
                    object.name.replace("`", "``")
                );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql_form_values(
        values: &[(&str, ObjectFormValue)],
    ) -> std::collections::BTreeMap<String, ObjectFormValue> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn mysql_features() -> MysqlServerFeatures {
        MysqlServerFeatures {
            is_mariadb: false,
            major: 8,
            minor: 0,
        }
    }

    fn mariadb_features() -> MysqlServerFeatures {
        MysqlServerFeatures {
            is_mariadb: true,
            major: 10,
            minor: 11,
        }
    }

    #[test]
    fn mariadb_objects_panel_manifest_exposes_explicit_parity_actions() {
        let manifest = mysql_objects_panel_manifest(&mariadb_features());

        manifest
            .validate()
            .expect("MySQL objects panel manifest should satisfy manifest invariants");

        let kind_ids: Vec<&str> = manifest
            .object_kinds
            .iter()
            .map(|kind| kind.id.as_str())
            .collect();
        assert_eq!(
            kind_ids,
            vec![
                "database",
                "table",
                "view",
                "function",
                "procedure",
                "trigger",
                "event",
                "index",
                "constraint",
                "foreign_key",
                "partition",
                "sequence",
                "tablespace",
                "engine",
                "charset",
                "collation",
                "user",
                "role",
                "grant",
                "plugin",
                "variable",
                "status_variable",
                "process",
                "replica_status",
            ]
        );

        let toolbar_action_ids: Vec<&str> = manifest
            .toolbar_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();
        assert_eq!(
            toolbar_action_ids,
            vec![
                "refresh",
                "new_database",
                "new_table",
                "new_view",
                "new_function",
                "new_procedure",
                "new_event",
                "import",
                "export",
            ]
        );

        fn assert_kind_actions(
            manifest: &ObjectsPanelManifest,
            kind_id: &str,
            expected_action_ids: &[&str],
            expected_default_row_action_id: Option<&str>,
        ) {
            let kind = manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == kind_id)
                .expect("kind should be present");

            let actual_action_ids: Vec<&str> = kind
                .row_actions
                .iter()
                .map(|action| action.id.as_str())
                .collect();

            assert_eq!(actual_action_ids, expected_action_ids);
            assert_eq!(
                kind.default_row_action_id.as_deref(),
                expected_default_row_action_id
            );
        }

        assert_kind_actions(
            &manifest,
            "table",
            &[
                "open",
                "design",
                "rename",
                "duplicate",
                "empty",
                "import",
                "export",
                "dump_sql_structure_data",
                "dump_sql_structure",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "delete",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "view",
            &[
                "open",
                "design",
                "rename",
                "duplicate",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "export",
                "delete",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "function",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "procedure",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "trigger",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "event",
            &[
                "open",
                "design",
                "delete",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        for kind_id in [
            "database",
            "index",
            "constraint",
            "foreign_key",
            "partition",
            "sequence",
            "tablespace",
            "engine",
            "charset",
            "collation",
            "user",
            "role",
            "grant",
            "plugin",
            "variable",
            "status_variable",
            "process",
            "replica_status",
        ] {
            assert_kind_actions(
                &manifest,
                kind_id,
                &["open", "copy_name", "copy_qualified_name", "refresh"],
                Some("open"),
            );
        }
    }

    #[test]
    fn mysql_objects_panel_manifest_hides_mariadb_only_sequence_kind() {
        let manifest = mysql_objects_panel_manifest(&mysql_features());

        manifest
            .validate()
            .expect("MySQL objects panel manifest should satisfy manifest invariants");

        let kind_ids: Vec<&str> = manifest
            .object_kinds
            .iter()
            .map(|kind| kind.id.as_str())
            .collect();

        assert!(!kind_ids.contains(&"sequence"));
        assert_eq!(kind_ids.len(), 23);
    }

    #[test]
    fn mysql_objects_panel_routine_rows_preserve_kind_and_identity() {
        let function_row = mysql_function_row(
            FunctionInfo {
                schema: Some("analytics".to_string()),
                name: "do_work".to_string(),
                language: "SQL".to_string(),
                return_type: "integer".to_string(),
                parameters: vec![],
                definition: Some("RETURN 1".to_string()),
                owner: Some("root".to_string()),
                comment: Some("Example function".to_string()),
            },
            false,
        );
        assert_eq!(function_row.object_kind_id(), "function");
        assert_eq!(function_row.name, "do_work");
        assert_eq!(function_row.object_schema(), Some("analytics"));
        assert_eq!(
            function_row.values.get("name").map(String::as_str),
            Some("do_work")
        );
        assert_eq!(
            function_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("function::::analytics::do_work::")
        );

        let procedure_row = mysql_procedure_row(
            ProcedureInfo {
                schema: Some("ops".to_string()),
                name: "run_job".to_string(),
                language: "SQL".to_string(),
                parameters: vec![],
                definition: Some("CALL work()".to_string()),
                owner: None,
                comment: None,
            },
            true,
        );
        assert_eq!(procedure_row.object_kind_id(), "procedure");
        assert_eq!(procedure_row.name, "ops.run_job");
        assert_eq!(procedure_row.object_schema(), Some("ops"));
        assert_eq!(
            procedure_row.values.get("name").map(String::as_str),
            Some("ops.run_job")
        );
        assert_eq!(
            procedure_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("procedure::::ops::run_job::")
        );

        let trigger_row = mysql_trigger_row(
            TriggerInfo {
                schema: Some("public".to_string()),
                name: "users_audit".to_string(),
                table_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Insert, TriggerEvent::Truncate],
                definition: Some("SET NEW.updated_at = NOW()".to_string()),
                for_each: TriggerForEach::Row,
                enabled: true,
                comment: None,
            },
            false,
        );
        assert_eq!(trigger_row.object_kind_id(), "trigger");
        assert_eq!(trigger_row.object_name(), "users_audit");
        assert_eq!(
            trigger_row.values.get("event").map(String::as_str),
            Some("INSERT, TRUNCATE")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("trigger::::public::users_audit::users")
        );
    }

    #[test]
    fn mysql_trigger_panel_row_exposes_catalog_metadata() {
        let row = zqlz_core::Row::new(
            vec![
                "TRIGGER_SCHEMA".to_string(),
                "TRIGGER_NAME".to_string(),
                "EVENT_OBJECT_TABLE".to_string(),
                "ACTION_TIMING".to_string(),
                "EVENT_MANIPULATION".to_string(),
                "ACTION_ORDER".to_string(),
                "ACTION_STATEMENT".to_string(),
                "ACTION_ORIENTATION".to_string(),
                "DEFINER".to_string(),
                "SQL_MODE".to_string(),
                "CREATED".to_string(),
                "CHARACTER_SET_CLIENT".to_string(),
                "COLLATION_CONNECTION".to_string(),
                "DATABASE_COLLATION".to_string(),
            ],
            vec![
                Value::String("app".to_string()),
                Value::String("users_touch".to_string()),
                Value::String("users".to_string()),
                Value::String("BEFORE".to_string()),
                Value::String("UPDATE".to_string()),
                Value::Int64(1),
                Value::String("SET NEW.updated_at = NOW()".to_string()),
                Value::String("ROW".to_string()),
                Value::String("root@localhost".to_string()),
                Value::String("STRICT_TRANS_TABLES".to_string()),
                Value::String("2026-05-03 12:00:00".to_string()),
                Value::String("utf8mb4".to_string()),
                Value::String("utf8mb4_0900_ai_ci".to_string()),
                Value::String("utf8mb4_0900_ai_ci".to_string()),
            ],
        );

        let trigger_row = mysql_trigger_panel_row(&row, true);

        assert_eq!(trigger_row.name, "app.users_touch");
        assert_eq!(trigger_row.object_name(), "users_touch");
        assert_eq!(
            trigger_row.values.get("definer").map(String::as_str),
            Some("root@localhost")
        );
        assert_eq!(
            trigger_row.values.get("action_order").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.signature.as_deref()),
            Some("users")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("trigger::::app::users_touch::users")
        );
    }

    #[test]
    fn mysql_routine_parameters_label_formats_modes_and_defaults_empty() {
        assert_eq!(mysql_routine_parameters_label(&[]), "-");
        assert_eq!(
            mysql_routine_parameters_label(&[
                ParameterInfo {
                    name: Some("user_id".to_string()),
                    data_type: "bigint unsigned".to_string(),
                    mode: ParameterMode::In,
                    default_value: None,
                    ordinal: 1,
                },
                ParameterInfo {
                    name: Some("total".to_string()),
                    data_type: "decimal(10,2)".to_string(),
                    mode: ParameterMode::Out,
                    default_value: None,
                    ordinal: 2,
                },
            ]),
            "IN user_id bigint unsigned, OUT total decimal(10,2)"
        );
    }

    #[test]
    fn mysql_routine_panel_row_exposes_catalog_metadata() {
        let row = zqlz_core::Row::new(
            vec![
                "ROUTINE_SCHEMA".to_string(),
                "ROUTINE_NAME".to_string(),
                "DATA_TYPE".to_string(),
                "ROUTINE_DEFINITION".to_string(),
                "ROUTINE_COMMENT".to_string(),
                "DEFINER".to_string(),
                "SECURITY_TYPE".to_string(),
                "IS_DETERMINISTIC".to_string(),
                "SQL_DATA_ACCESS".to_string(),
                "SQL_MODE".to_string(),
                "CREATED".to_string(),
                "LAST_ALTERED".to_string(),
            ],
            vec![
                Value::String("app".to_string()),
                Value::String("score".to_string()),
                Value::String("bigint".to_string()),
                Value::String("RETURN user_id".to_string()),
                Value::String("User score".to_string()),
                Value::String("root@localhost".to_string()),
                Value::String("DEFINER".to_string()),
                Value::String("YES".to_string()),
                Value::String("READS SQL DATA".to_string()),
                Value::String("STRICT_TRANS_TABLES".to_string()),
                Value::String("2026-05-03 10:00:00".to_string()),
                Value::String("2026-05-03 11:00:00".to_string()),
            ],
        );

        let routine_row = mysql_routine_panel_row(
            "function",
            &row,
            vec![ParameterInfo {
                name: Some("user_id".to_string()),
                data_type: "bigint".to_string(),
                mode: ParameterMode::In,
                default_value: None,
                ordinal: 1,
            }],
            true,
        );

        assert_eq!(routine_row.name, "app.score");
        assert_eq!(routine_row.object_kind_id(), "function");
        assert_eq!(routine_row.object_name(), "score");
        assert_eq!(
            routine_row.values.get("parameters").map(String::as_str),
            Some("IN user_id bigint")
        );
        assert_eq!(
            routine_row
                .values
                .get("sql_data_access")
                .map(String::as_str),
            Some("READS SQL DATA")
        );
        assert_eq!(
            routine_row.values.get("created").map(String::as_str),
            Some("2026-05-03 10:00:00")
        );
        assert_eq!(
            routine_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("function::::app::score::")
        );
    }

    #[test]
    fn mysql_metadata_rows_preserve_table_context_in_signature() {
        let row = mysql_metadata_row(
            "index",
            "idx_users_email".to_string(),
            Some("app".to_string()),
            Some("users".to_string()),
            "Index",
            "BTREE: email",
            "-",
            "-",
        );

        assert_eq!(row.object_kind_id(), "index");
        assert_eq!(
            row.object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.signature.as_deref()),
            Some("users")
        );
        assert_eq!(
            row.object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("index::::app::idx_users_email::users")
        );
    }

    #[test]
    fn mysql_index_ddl_reconstructs_create_statement() {
        let ddl = mysql_index_ddl(
            "posts",
            &IndexInfo {
                name: "idx_posts_title".to_string(),
                columns: vec!["title".to_string(), "created_at".to_string()],
                is_unique: false,
                is_primary: false,
                index_type: "BTREE".to_string(),
                comment: Some("fast lookup".to_string()),
                column_descending: vec![false, true],
                ..Default::default()
            },
        );

        assert_eq!(
            ddl,
            "CREATE INDEX `idx_posts_title` ON `posts` USING BTREE (`title`, `created_at` DESC) COMMENT 'fast lookup';"
        );
    }

    #[test]
    fn mysql_constraint_ddl_reconstructs_unique_and_check() {
        let unique = mysql_constraint_ddl(
            "users",
            &ConstraintInfo {
                name: "uq_users_email".to_string(),
                constraint_type: ConstraintType::Unique,
                columns: vec!["email".to_string()],
                definition: None,
            },
        )
        .unwrap();
        assert_eq!(
            unique,
            "ALTER TABLE `users` ADD CONSTRAINT `uq_users_email` UNIQUE (`email`);"
        );

        let check = mysql_constraint_ddl(
            "users",
            &ConstraintInfo {
                name: "chk_users_age".to_string(),
                constraint_type: ConstraintType::Check,
                columns: Vec::new(),
                definition: Some("age >= 0".to_string()),
            },
        )
        .unwrap();
        assert_eq!(
            check,
            "ALTER TABLE `users` ADD CONSTRAINT `chk_users_age` CHECK (age >= 0);"
        );
    }

    #[test]
    fn mysql_foreign_key_ddl_reconstructs_alter_table_statement() {
        let ddl = mysql_foreign_key_ddl(
            "orders",
            &ForeignKeyInfo {
                name: "fk_orders_user".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_schema: Some("app".to_string()),
                referenced_columns: vec!["id".to_string()],
                on_update: ForeignKeyAction::Cascade,
                on_delete: ForeignKeyAction::Restrict,
                is_deferrable: false,
                initially_deferred: false,
            },
        );

        assert_eq!(
            ddl,
            "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `app`.`users` (`id`) ON UPDATE CASCADE ON DELETE RESTRICT;"
        );
    }

    #[test]
    fn mysql_server_feature_parser_detects_mysql_and_mariadb() {
        let mysql = parse_mysql_server_features("8.0.36", "MySQL Community Server - GPL");
        assert!(!mysql.is_mariadb);
        assert_eq!((mysql.major, mysql.minor), (8, 0));
        assert!(mysql.supports_events());

        let mariadb = parse_mysql_server_features("10.11.7-MariaDB", "MariaDB Server");
        assert!(mariadb.is_mariadb);
        assert!(mariadb.supports_sequences());
    }

    #[test]
    fn mysql_function_form_sql_renders_create_statement() {
        let values = mysql_form_values(&[
            (
                "parameters",
                ObjectFormValue::StringList(vec![
                    "IN user_id BIGINT UNSIGNED".to_string(),
                    "IN flag TINYINT(1)".to_string(),
                ]),
            ),
            ("returns", ObjectFormValue::String("BIGINT".to_string())),
            ("deterministic", ObjectFormValue::Bool(true)),
            (
                "sql_data_access",
                ObjectFormValue::String("READS SQL DATA".to_string()),
            ),
            (
                "body",
                ObjectFormValue::String("RETURN user_id + flag".to_string()),
            ),
        ]);

        assert_eq!(
            mysql_function_form_sql(Some("analytics"), "score", &values).unwrap(),
            "CREATE FUNCTION `analytics`.`score`(IN user_id BIGINT UNSIGNED, IN flag TINYINT(1))\nRETURNS BIGINT\nDETERMINISTIC\nREADS SQL DATA\nRETURN user_id + flag;"
        );
    }

    #[test]
    fn mysql_procedure_form_sql_renders_create_statement() {
        let values = mysql_form_values(&[
            (
                "parameters",
                ObjectFormValue::StringList(vec!["IN job_id BIGINT".to_string()]),
            ),
            (
                "body",
                ObjectFormValue::String(
                    "BEGIN\n  UPDATE jobs SET ran_at = NOW() WHERE id = job_id;\nEND".to_string(),
                ),
            ),
        ]);

        assert_eq!(
            mysql_procedure_form_sql(Some("ops"), "run_job", &values).unwrap(),
            "CREATE PROCEDURE `ops`.`run_job`(IN job_id BIGINT)\nBEGIN\n  UPDATE jobs SET ran_at = NOW() WHERE id = job_id;\nEND;"
        );
    }

    #[test]
    fn mysql_trigger_form_sql_renders_defaults_and_qualified_table() {
        let values = mysql_form_values(&[
            ("table", ObjectFormValue::String("users".to_string())),
            (
                "body",
                ObjectFormValue::String("SET NEW.updated_at = NOW()".to_string()),
            ),
        ]);

        assert_eq!(
            mysql_trigger_form_sql(Some("app"), "users_touch", &values).unwrap(),
            "CREATE TRIGGER `app`.`users_touch`\nBEFORE INSERT ON `app`.`users`\nFOR EACH ROW\nSET NEW.updated_at = NOW();"
        );
    }

    #[test]
    fn mysql_routine_and_trigger_form_sql_validate_required_fields() {
        assert!(matches!(
            mysql_function_form_sql(
                None,
                "missing_return",
                &mysql_form_values(&[("body", ObjectFormValue::String("RETURN 1".to_string()))])
            ),
            Err(ZqlzError::Schema(message)) if message == "Function return type is required"
        ));
        assert!(matches!(
            mysql_procedure_form_sql(None, "missing_body", &mysql_form_values(&[])),
            Err(ZqlzError::Schema(message)) if message == "Procedure body is required"
        ));
        assert!(matches!(
            mysql_trigger_form_sql(
                None,
                "missing_table",
                &mysql_form_values(&[(
                    "body",
                    ObjectFormValue::String("SET @value = 1".to_string()),
                )])
            ),
            Err(ZqlzError::Schema(message)) if message == "Trigger table is required"
        ));
    }

    #[test]
    fn mysql_metadata_unavailable_row_is_visible_for_admin_catalog_errors() {
        assert!(mysql_metadata_kind_degrades_on_error("user"));
        assert!(mysql_metadata_kind_degrades_on_error("process"));
        assert!(!mysql_metadata_kind_degrades_on_error("index"));

        let row = mysql_metadata_unavailable_row(
            "user",
            &ZqlzError::Query("SELECT command denied".to_string()),
        );

        assert_eq!(row.object_kind_id(), "user");
        assert_eq!(row.name, "Metadata unavailable");
        assert_eq!(
            row.values.get("kind").map(String::as_str),
            Some("Privilege or catalog error")
        );
        assert_eq!(
            row.values.get("state").map(String::as_str),
            Some("Unavailable")
        );
        assert_eq!(
            row.values.get("detail").map(String::as_str),
            Some("Query error: SELECT command denied")
        );
    }
}
