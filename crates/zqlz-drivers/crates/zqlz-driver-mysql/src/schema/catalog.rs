//! MySQL implementation of the raw-catalog port (`CatalogSource`): relation,
//! column, index, constraint, and foreign-key listing.
use super::*;

pub struct MySqlCatalog {
    pool: mysql_async::Pool,
    database_name: Option<String>,
    cancel_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
    capabilities: zqlz_core::CatalogCapabilities,
    in_flight: crate::connection::InFlight,
    is_mariadb: tokio::sync::OnceCell<bool>,
}

impl MySqlCatalog {
    pub fn new(
        pool: mysql_async::Pool,
        database_name: Option<String>,
        cancel_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
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
            cancel_epoch,
            capabilities,
            in_flight,
            is_mariadb: tokio::sync::OnceCell::new(),
        }
    }

    /// Server flavor decides how `COLUMN_DEFAULT` is encoded; asked once.
    async fn is_mariadb(&self) -> bool {
        *self
            .is_mariadb
            .get_or_init(|| async {
                match self.query("SELECT VERSION(), @@version_comment", &[]).await {
                    Ok(result) => result.rows.first().is_some_and(|row| {
                        let text = format!(
                            "{} {}",
                            mysql_text(row.get(0)).unwrap_or_default(),
                            mysql_text(row.get(1)).unwrap_or_default()
                        );
                        text.to_ascii_lowercase().contains("mariadb")
                    }),
                    Err(_) => false,
                }
            })
            .await
    }

    async fn query(
        &self,
        sql: &str,
        params: &[zqlz_core::Value],
    ) -> Result<zqlz_core::QueryResult> {
        crate::connection::run_mysql_query(
            self.pool.clone(),
            self.cancel_epoch.clone(),
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

/// `STATISTICS` projection for one table or a whole schema.
///
/// Rows are deliberately **not** `GROUP_CONCAT`ed: a functional key part has a
/// NULL `COLUMN_NAME`, which `GROUP_CONCAT` silently drops (misaligning every
/// parallel list), column names may contain commas, and the default
/// `group_concat_max_len` truncates wide indexes. `SELECT *` keeps this working
/// on servers that lack newer columns (`EXPRESSION` is MySQL 8.0.13+).
pub(super) fn mysql_indexes_sql(schema_filter: &str, single_table: bool) -> String {
    let table_filter = if single_table { "AND TABLE_NAME = ?" } else { "" };
    format!(
        "SELECT * FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA {schema_filter} {table_filter}
         ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME = 'PRIMARY' DESC, INDEX_NAME, SEQ_IN_INDEX"
    )
}

pub(super) struct MysqlIndexEntry {
    pub schema: Option<String>,
    pub table: String,
    pub index: zqlz_core::RawIndexRow,
}

/// Fold ordered `STATISTICS` rows (one per key part) into one entry per index.
///
/// A functional key part is reported as its `EXPRESSION` text (always wrapped
/// in parentheses by the server) in place of a column name. Prefix lengths
/// become `column(n)` entries in `include_columns`; the spatial/fulltext
/// `SUB_PART` values are internal and ignored.
pub(super) fn mysql_index_entries(result: &zqlz_core::QueryResult) -> Vec<MysqlIndexEntry> {
    let position = |name: &str| {
        result
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    };
    let (Some(table_at), Some(index_at), Some(column_at)) = (
        position("TABLE_NAME"),
        position("INDEX_NAME"),
        position("COLUMN_NAME"),
    ) else {
        return Vec::new();
    };
    let schema_at = position("TABLE_SCHEMA");
    let non_unique_at = position("NON_UNIQUE");
    let type_at = position("INDEX_TYPE");
    let collation_at = position("COLLATION");
    let sub_part_at = position("SUB_PART");
    let expression_at = position("EXPRESSION");
    let comment_at = position("INDEX_COMMENT");

    let mut entries: Vec<MysqlIndexEntry> = Vec::new();
    for row in &result.rows {
        let Some(table) = mysql_text(row.get(table_at)) else {
            continue;
        };
        let Some(name) = mysql_text(row.get(index_at)) else {
            continue;
        };
        let schema = schema_at.and_then(|at| mysql_text(row.get(at)));
        let is_new = entries.last().is_none_or(|last| {
            last.schema != schema || last.table != table || last.index.name != name
        });
        if is_new {
            let method = type_at.and_then(|at| mysql_text(row.get(at)));
            entries.push(MysqlIndexEntry {
                schema,
                table: table.clone(),
                index: zqlz_core::RawIndexRow {
                    is_unique: non_unique_at
                        .and_then(|at| row.get(at))
                        .and_then(|value| value.as_i64())
                        .unwrap_or(1)
                        == 0,
                    is_primary: name == "PRIMARY",
                    comment: comment_at
                        .and_then(|at| mysql_text(row.get(at)))
                        .filter(|comment| !comment.is_empty()),
                    method,
                    name,
                    ..Default::default()
                },
            });
        }
        let entry = entries.last_mut().expect("entry pushed above");
        let column = mysql_text(row.get(column_at))
            .or_else(|| expression_at.and_then(|at| mysql_text(row.get(at))))
            .unwrap_or_default();
        let method = entry.index.method.as_deref().unwrap_or("");
        let prefix = sub_part_at
            .and_then(|at| row.get(at))
            .and_then(|value| value.as_i64())
            .filter(|_| !matches!(method, "SPATIAL" | "FULLTEXT"));
        if let Some(prefix) = prefix {
            entry.index.include_columns.push(format!("{column}({prefix})"));
        }
        entry.index.column_descending.push(
            collation_at
                .and_then(|at| mysql_text(row.get(at)))
                .is_some_and(|value| value.eq_ignore_ascii_case("D")),
        );
        entry.index.columns.push(column);
    }
    entries
}

/// Foreign-key columns with their ON UPDATE/ON DELETE rules in one pass.
///
/// `REFERENTIAL_CONSTRAINTS` is joined on schema + table + constraint name:
/// MySQL constraint names are only unique per table, so joining on the name
/// alone would borrow the rules of an unrelated table's key.
fn mysql_foreign_keys_sql(schema: Option<&str>, single_relation: bool) -> String {
    let schema_filter = schema.map(|_| "= ?").unwrap_or("= DATABASE()");
    let relation_filter = if single_relation {
        "AND kcu.TABLE_NAME = ?"
    } else {
        ""
    };
    format!(
        "SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME, kcu.TABLE_NAME,
                rc.UPDATE_RULE, rc.DELETE_RULE
         FROM information_schema.KEY_COLUMN_USAGE kcu
         LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND rc.TABLE_NAME = kcu.TABLE_NAME
          AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         WHERE kcu.TABLE_SCHEMA {schema_filter} {relation_filter}
           AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
         ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION"
    )
}

/// Group `mysql_foreign_keys_sql` rows into one key per (table, constraint),
/// preserving column pairing order.
fn mysql_group_foreign_keys(
    result: &zqlz_core::QueryResult,
    fallback_schema: Option<&str>,
) -> Vec<(String, zqlz_core::RawForeignKeyRow)> {
    let mut grouped: Vec<(String, zqlz_core::RawForeignKeyRow)> = Vec::new();
    for row in &result.rows {
        let name = mysql_text(row.get(0)).unwrap_or_default();
        let table = mysql_text(row.get(5)).unwrap_or_default();
        let is_new = grouped
            .last()
            .is_none_or(|(last_table, last)| *last_table != table || last.name != name);
        if is_new {
            grouped.push((
                table,
                zqlz_core::RawForeignKeyRow {
                    name,
                    referenced_schema: mysql_text(row.get(2))
                        .or_else(|| fallback_schema.map(ToString::to_string)),
                    referenced_table: mysql_text(row.get(3)).unwrap_or_default(),
                    on_update: mysql_text(row.get(6)),
                    on_delete: mysql_text(row.get(7)),
                    ..Default::default()
                },
            ));
        }
        let (_, entry) = grouped.last_mut().expect("entry pushed above");
        entry.columns.push(mysql_text(row.get(1)).unwrap_or_default());
        entry
            .referenced_columns
            .push(mysql_text(row.get(4)).unwrap_or_default());
    }
    grouped
}

/// Types whose literal defaults are written without quotes.
fn mysql_is_numeric_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "decimal"
            | "numeric"
            | "float"
            | "double"
            | "real"
            | "bit"
    )
}

/// Turn `COLUMN_DEFAULT` into SQL that can follow `DEFAULT` verbatim.
///
/// MySQL reports literal defaults unquoted (`it's`) and expression defaults
/// without the parentheses the DDL requires (`uuid()`, flagged by
/// `DEFAULT_GENERATED` in `EXTRA`), so the two cannot be told apart from the
/// raw string. `ON UPDATE` rides along because the column definition has no
/// other slot for it. MariaDB already stores defaults as SQL text.
fn mysql_default_sql(
    column_default: Option<&str>,
    extra: &str,
    data_type: &str,
    is_mariadb: bool,
) -> Option<String> {
    let extra_lower = extra.to_ascii_lowercase();
    let on_update = extra_lower.find("on update ").map(|at| &extra[at + "on update ".len()..]);

    let default = column_default.and_then(|value| {
        if is_mariadb {
            return (!value.eq_ignore_ascii_case("NULL")).then(|| value.to_string());
        }
        if extra_lower.contains("default_generated") {
            // information_schema backslash-escapes quotes inside expressions.
            let value = value.replace("\\'", "'");
            let value = value.as_str();
            let lower = value.to_ascii_lowercase();
            let wrapped = value.starts_with('(') && value.ends_with(')');
            return Some(if wrapped || lower.starts_with("current_timestamp") {
                value.to_string()
            } else {
                format!("({value})")
            });
        }
        let is_literal_number = mysql_is_numeric_type(&data_type.to_ascii_lowercase());
        Some(if is_literal_number {
            value.to_string()
        } else {
            mysql_quote_literal(value)
        })
    });

    match (default, on_update) {
        (Some(default), Some(on_update)) => Some(format!("{default} ON UPDATE {on_update}")),
        (None, Some(on_update)) => Some(format!("NULL ON UPDATE {on_update}")),
        (default, None) => default,
    }
}

fn mysql_column_row(row: &zqlz_core::Row, is_mariadb: bool) -> zqlz_core::RawColumnRow {
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
        default_value: mysql_default_sql(
            row.get(4).and_then(|v| v.as_str()),
            extra,
            &data_type,
            is_mariadb,
        ),
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

        let is_mariadb = self.is_mariadb().await;
        Ok(result
            .rows
            .iter()
            .map(|row| mysql_column_row(row, is_mariadb))
            .collect())
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

        let is_mariadb = self.is_mariadb().await;
        let mut columns_by_relation: HashMap<String, Vec<zqlz_core::RawColumnRow>> = HashMap::new();
        for row in &result.rows {
            let Some(relation) = row.get(15).and_then(|value| value.as_str()) else {
                continue;
            };
            columns_by_relation
                .entry(relation.to_string())
                .or_default()
                .push(mysql_column_row(row, is_mariadb));
        }

        Ok(Some(columns_by_relation))
    }

    async fn fetch_indexes(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawIndexRow>> {
        let schema = self.resolved_schema(relation.schema.as_deref());
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));
        let result = self
            .query(
                &mysql_indexes_sql(schema.as_deref().map(|_| "= ?").unwrap_or("= DATABASE()"), true),
                &params,
            )
            .await?;

        Ok(mysql_index_entries(&result)
            .into_iter()
            .map(|entry| entry.index)
            .collect())
    }

    async fn fetch_foreign_keys(
        &self,
        relation: &zqlz_core::RelationRef,
    ) -> Result<Vec<zqlz_core::RawForeignKeyRow>> {
        let schema = self.resolved_schema(relation.schema.as_deref());
        let mut params: Vec<zqlz_core::Value> = Vec::new();
        if let Some(s) = schema.as_deref() {
            params.push(zqlz_core::Value::String(s.to_string()));
        }
        params.push(zqlz_core::Value::String(relation.name.clone()));
        let result = self
            .query(&mysql_foreign_keys_sql(schema.as_deref(), true), &params)
            .await?;

        Ok(mysql_group_foreign_keys(&result, schema.as_deref())
            .into_iter()
            .map(|(_, foreign_key)| foreign_key)
            .collect())
    }

    async fn fetch_all_foreign_keys(
        &self,
        schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<zqlz_core::RawForeignKeyRow>>>> {
        let schema = self.resolved_schema(schema);
        let params: Vec<zqlz_core::Value> = schema
            .as_deref()
            .map(|s| vec![zqlz_core::Value::String(s.to_string())])
            .unwrap_or_default();
        let result = self
            .query(&mysql_foreign_keys_sql(schema.as_deref(), false), &params)
            .await?;

        let mut foreign_keys_by_relation: HashMap<String, Vec<zqlz_core::RawForeignKeyRow>> =
            HashMap::new();
        for (table, foreign_key) in mysql_group_foreign_keys(&result, schema.as_deref()) {
            foreign_keys_by_relation
                .entry(table)
                .or_default()
                .push(foreign_key);
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
        // A server without CHECK_CONSTRAINTS (pre-8.0.16 MySQL) just has none.
        if let Ok(result) = self.query(&check_sql, &params).await {
            for row in &result.rows {
                constraints.push(zqlz_core::RawConstraintRow {
                    name: mysql_text(row.get(0)).unwrap_or_default(),
                    kind: "CHECK".to_string(),
                    columns: Vec::new(),
                    definition: mysql_text(row.get(1)),
                });
            }
        }

        let unique_sql = format!(
            "SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME
             FROM information_schema.TABLE_CONSTRAINTS tc
             JOIN information_schema.KEY_COLUMN_USAGE kcu
               ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
               AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
               AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
               AND tc.TABLE_NAME = kcu.TABLE_NAME
             WHERE tc.TABLE_SCHEMA {schema_filter} AND tc.TABLE_NAME = ?
               AND tc.CONSTRAINT_TYPE = 'UNIQUE'
             ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION"
        );
        let result = self.query(&unique_sql, &params).await?;
        let mut unique: Vec<zqlz_core::RawConstraintRow> = Vec::new();
        for row in &result.rows {
            let name = mysql_text(row.get(0)).unwrap_or_default();
            if unique.last().is_none_or(|last| last.name != name) {
                unique.push(zqlz_core::RawConstraintRow {
                    name,
                    kind: "UNIQUE".to_string(),
                    columns: Vec::new(),
                    definition: None,
                });
            }
            if let (Some(last), Some(column)) = (unique.last_mut(), mysql_text(row.get(1))) {
                last.columns.push(column);
            }
        }
        constraints.extend(unique);

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

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{ColumnMeta, QueryResult, Row, Value};

    fn result(columns: &[&str], rows: Vec<Vec<Value>>) -> QueryResult {
        let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        let mut result = QueryResult::empty();
        result.columns = names
            .iter()
            .map(|name| ColumnMeta {
                name: name.clone(),
                ..Default::default()
            })
            .collect();
        result.rows = rows
            .into_iter()
            .map(|values| Row::new(names.clone(), values))
            .collect();
        result
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    #[test]
    fn literal_defaults_are_quoted_and_expression_defaults_are_not() {
        let sql = |default: Option<&str>, extra: &str, data_type: &str| {
            mysql_default_sql(default, extra, data_type, false)
        };
        assert_eq!(sql(Some("it's"), "", "varchar").as_deref(), Some("'it''s'"));
        assert_eq!(sql(Some("NULL"), "", "varchar").as_deref(), Some("'NULL'"));
        assert_eq!(sql(Some("1.50"), "", "decimal").as_deref(), Some("1.50"));
        assert_eq!(sql(Some("b'101'"), "", "bit").as_deref(), Some("b'101'"));
        assert_eq!(sql(Some("2020-01-01"), "", "date").as_deref(), Some("'2020-01-01'"));
        assert_eq!(sql(None, "auto_increment", "int"), None);

        // Expression defaults arrive without the parentheses DDL needs.
        assert_eq!(sql(Some("uuid()"), "DEFAULT_GENERATED", "varchar").as_deref(), Some("(uuid())"));
        assert_eq!(sql(Some("(1 + 1)"), "DEFAULT_GENERATED", "int").as_deref(), Some("(1 + 1)"));
        assert_eq!(
            sql(Some("CURRENT_TIMESTAMP(3)"), "DEFAULT_GENERATED", "datetime").as_deref(),
            Some("CURRENT_TIMESTAMP(3)")
        );
        assert_eq!(
            sql(Some("concat(_utf8mb4\\'a\\',_utf8mb4\\'b\\')"), "DEFAULT_GENERATED", "varchar")
                .as_deref(),
            Some("(concat(_utf8mb4'a',_utf8mb4'b'))")
        );
    }

    #[test]
    fn on_update_is_kept_with_the_default() {
        assert_eq!(
            mysql_default_sql(
                Some("CURRENT_TIMESTAMP"),
                "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
                "timestamp",
                false
            )
            .as_deref(),
            Some("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
        );
        assert_eq!(
            mysql_default_sql(None, "on update CURRENT_TIMESTAMP", "timestamp", false).as_deref(),
            Some("NULL ON UPDATE CURRENT_TIMESTAMP")
        );
    }

    #[test]
    fn mariadb_defaults_are_already_sql() {
        assert_eq!(
            mysql_default_sql(Some("'x'"), "", "varchar", true).as_deref(),
            Some("'x'")
        );
        assert_eq!(mysql_default_sql(Some("NULL"), "", "varchar", true), None);
    }

    #[test]
    fn index_entries_keep_functional_parts_prefixes_and_direction_aligned() {
        let columns = [
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "NON_UNIQUE",
            "INDEX_NAME",
            "SEQ_IN_INDEX",
            "COLUMN_NAME",
            "COLLATION",
            "SUB_PART",
            "INDEX_TYPE",
            "EXPRESSION",
            "INDEX_COMMENT",
        ];
        let row = |table: &str, non_unique: i64, index: &str, seq: i64, column: Option<&str>, collation: &str, sub_part: Option<i64>, kind: &str, expression: Option<&str>, comment: &str| {
            vec![
                text("db"),
                text(table),
                Value::Int64(non_unique),
                text(index),
                Value::Int64(seq),
                column.map(text).unwrap_or(Value::Null),
                text(collation),
                sub_part.map(Value::Int64).unwrap_or(Value::Null),
                text(kind),
                expression.map(text).unwrap_or(Value::Null),
                text(comment),
            ]
        };
        let result = result(
            &columns,
            vec![
                row("t", 0, "PRIMARY", 1, Some("id"), "A", None, "BTREE", None, ""),
                // `a,b` is one column name that contains a comma.
                row("t", 1, "mixed", 1, Some("a,b"), "A", Some(10), "BTREE", None, "hot path"),
                row("t", 1, "mixed", 2, None, "A", None, "BTREE", Some("(`x` + `y`)"), "hot path"),
                row("t", 1, "mixed", 3, Some("z"), "D", None, "BTREE", None, "hot path"),
                row("t", 1, "sp", 1, Some("geo"), "A", Some(32), "SPATIAL", None, ""),
                // Same index name on another table must not merge into `mixed`.
                row("u", 1, "mixed", 1, Some("q"), "A", None, "HASH", None, ""),
            ],
        );

        let entries = mysql_index_entries(&result);
        assert_eq!(entries.len(), 4);

        assert!(entries[0].index.is_primary && entries[0].index.is_unique);

        let mixed = &entries[1].index;
        assert_eq!(mixed.columns, ["a,b", "(`x` + `y`)", "z"]);
        assert_eq!(mixed.column_descending, [false, false, true]);
        assert_eq!(mixed.include_columns, ["a,b(10)"]);
        assert_eq!(mixed.comment.as_deref(), Some("hot path"));
        assert!(!mixed.is_unique);

        let spatial = &entries[2].index;
        assert!(spatial.include_columns.is_empty(), "SPATIAL SUB_PART is not a prefix");

        assert_eq!(entries[3].table, "u");
        assert_eq!(entries[3].index.method.as_deref(), Some("HASH"));
        assert_eq!(entries[3].index.columns, ["q"]);
    }

    #[test]
    fn foreign_keys_group_per_table_with_their_own_rules_and_schema() {
        let columns = [
            "CONSTRAINT_NAME",
            "COLUMN_NAME",
            "REFERENCED_TABLE_SCHEMA",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME",
            "TABLE_NAME",
            "UPDATE_RULE",
            "DELETE_RULE",
        ];
        let row = |name: &str, column: &str, ref_schema: &str, ref_table: &str, ref_column: &str, table: &str, update: &str, delete: &str| {
            vec![
                text(name),
                text(column),
                text(ref_schema),
                text(ref_table),
                text(ref_column),
                text(table),
                text(update),
                text(delete),
            ]
        };
        let result = result(
            &columns,
            vec![
                row("fk_owner", "a", "db", "parent", "a", "child1", "SET NULL", "CASCADE"),
                row("fk_owner", "b", "db", "parent", "b", "child1", "SET NULL", "CASCADE"),
                // Same constraint name on a sibling table, different rules and target database.
                row("fk_owner", "x", "other", "remote", "id", "child2", "NO ACTION", "RESTRICT"),
            ],
        );

        let grouped = mysql_group_foreign_keys(&result, Some("db"));
        assert_eq!(grouped.len(), 2);

        let (table, first) = &grouped[0];
        assert_eq!(table, "child1");
        assert_eq!(first.columns, ["a", "b"]);
        assert_eq!(first.referenced_columns, ["a", "b"]);
        assert_eq!(first.on_update.as_deref(), Some("SET NULL"));
        assert_eq!(first.on_delete.as_deref(), Some("CASCADE"));

        let (table, second) = &grouped[1];
        assert_eq!(table, "child2");
        assert_eq!(second.referenced_schema.as_deref(), Some("other"));
        assert_eq!(second.referenced_table, "remote");
        assert_eq!(second.on_delete.as_deref(), Some("RESTRICT"));
    }
}
