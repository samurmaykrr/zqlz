//! Standalone service bootstrap
//!
//! Used when no ZQLZ GUI instance is running.  Reads connections from the
//! shared SQLite storage database and operates directly through the driver layer.
//!
//! This module deliberately avoids depending on `zqlz-connection` (which
//! transitively depends on GPUI) by inlining the small pieces of
//! `SavedConnection` and driver-connect logic it needs.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_internal_storage::InternalStorage;
use zqlz_internal_storage::rusqlite;

use zqlz_core::sql_lexing::{SqlLexDialect, split_sql_statements_for_dialect};
use zqlz_core::{Connection, ConnectionConfig};
use zqlz_drivers::DriverRegistry;

use crate::ipc::{
    ColumnInfo, ColumnMeta, FunctionSummary, HistoryEntry, IndexSummary, QueryExecution,
    RelationEntry, Row, StatementResult, ViewSummary,
};

// ---------------------------------------------------------------------------
// Minimal SavedConnection (mirrors zqlz_connection::SavedConnection)
// ---------------------------------------------------------------------------

/// A saved database connection configuration.
///
/// This is a local copy of `zqlz_connection::SavedConnection` without any
/// GPUI dependency. The on-disk schema is identical so both the GUI and the
/// CLI read/write the same SQLite rows.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedConnection {
    pub id: Uuid,
    pub name: String,
    pub driver: String,
    pub params: HashMap<String, String>,
    pub folder: Option<String>,
    pub color: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub modified_at: chrono::DateTime<chrono::Utc>,
    pub last_connected: Option<chrono::DateTime<chrono::Utc>>,
}

impl SavedConnection {
    pub fn new(name: String, driver: String) -> Self {
        let now = chrono::Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            driver,
            params: HashMap::new(),
            folder: None,
            color: None,
            created_at: now,
            modified_at: now,
            last_connected: None,
        }
    }

    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.params.insert(key.to_string(), value.to_string());
        self
    }

    /// Build a driver `ConnectionConfig` from the persisted params.
    ///
    /// Mirrors `zqlz_connection::SavedConnection::to_connection_config` so the
    /// CLI produces identical configs without pulling in the GPUI dependency.
    pub fn to_connection_config(&self) -> ConnectionConfig {
        let mut config = ConnectionConfig::new(&self.driver, &self.name);

        for (key, value) in &self.params {
            config = config.with_param(key, value.clone());
        }

        config.host = self.params.get("host").cloned().unwrap_or_default();
        config.port = self
            .params
            .get("port")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0);
        config.database = self
            .params
            .get("database")
            .cloned()
            .or_else(|| self.params.get("path").cloned());
        config.username = self
            .params
            .get("username")
            .cloned()
            .or_else(|| self.params.get("user").cloned());
        config.password = self.params.get("password").cloned();

        config
    }
}

// ---------------------------------------------------------------------------
// Storage path
// ---------------------------------------------------------------------------

fn storage_db_path() -> Result<PathBuf> {
    Ok(InternalStorage::for_config_file("storage.db")?
        .path()
        .to_path_buf())
}

// ---------------------------------------------------------------------------
// Connection CRUD (direct SQLite access)
// ---------------------------------------------------------------------------

/// Load all saved connections from the shared storage database.
pub fn load_connections() -> Result<Vec<SavedConnection>> {
    let db_path = storage_db_path()?;

    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let db = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("opening storage database at {}", db_path.display()))?;

    // Ensure optional columns exist before querying — same migration guard used
    // in save_connection, needed here too for DBs created before these columns.
    for ddl in &[
        "ALTER TABLE connections ADD COLUMN folder TEXT",
        "ALTER TABLE connections ADD COLUMN color TEXT",
    ] {
        if let Err(e) = db.execute(ddl, []) {
            tracing::debug!(
                "migration step skipped ('{}') — likely already applied: {}",
                ddl,
                e
            );
        }
    }

    let mut stmt = db
        .prepare(
            "SELECT id, name, driver, params_json, folder, color, created_at, updated_at
             FROM connections ORDER BY updated_at DESC",
        )
        .context("preparing connections query")?;

    let connections: Vec<SavedConnection> = stmt
        .query_map([], |row| {
            let id_str: String = row.get(0)?;
            let name: String = row.get(1)?;
            let driver: String = row.get(2)?;
            let params_json: String = row.get(3)?;
            let folder: Option<String> = row.get(4)?;
            let color: Option<String> = row.get(5)?;
            let created_at_str: Option<String> = row.get(6)?;
            let updated_at_str: Option<String> = row.get(7)?;

            // One damaged row (hand-edited id, broken params) must not take
            // every other connection down with it.
            let (Ok(id), Ok(params)) = (
                Uuid::parse_str(&id_str),
                serde_json::from_str::<HashMap<String, String>>(&params_json),
            ) else {
                eprintln!("Warning: skipping saved connection '{name}': invalid id or parameters");
                return Ok(None);
            };

            let created_at = created_at_str
                .as_deref()
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok())
                .unwrap_or_else(chrono::Utc::now);

            let modified_at = updated_at_str
                .as_deref()
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok())
                .unwrap_or(created_at);

            Ok(Some(SavedConnection {
                id,
                name,
                driver,
                params,
                folder,
                color,
                created_at,
                modified_at,
                last_connected: None,
            }))
        })
        .context("querying connections")?
        .collect::<Result<Vec<_>, _>>()
        .context("collecting connections")?
        .into_iter()
        .flatten()
        .collect();

    Ok(connections)
}

/// Persist a new or updated connection to the shared storage database.
///
/// All params, including passwords, are stored directly in `params_json`.
pub fn save_connection(saved: &SavedConnection) -> Result<()> {
    let db_path = storage_db_path()?;

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config directory {}", parent.display()))?;
    }

    let db = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("opening storage database at {}", db_path.display()))?;

    db.execute(
        "CREATE TABLE IF NOT EXISTS connections (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            driver TEXT NOT NULL,
            params_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )",
        [],
    )
    .context("ensuring connections table exists")?;

    // Migrate older table schemas that were created without folder/color columns.
    // SQLite has no IF NOT EXISTS for ALTER TABLE, so adding a column that already
    // exists returns an error — expected during migration on an up-to-date schema.
    for ddl in &[
        "ALTER TABLE connections ADD COLUMN folder TEXT",
        "ALTER TABLE connections ADD COLUMN color TEXT",
    ] {
        if let Err(e) = db.execute(ddl, []) {
            tracing::debug!(
                "migration step skipped ('{}') — likely already applied: {}",
                ddl,
                e
            );
        }
    }

    let params_json =
        serde_json::to_string(&saved.params).context("serializing connection params")?;

    db.execute(
        "INSERT OR REPLACE INTO connections
             (id, name, driver, params_json, folder, color, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            saved.id.to_string(),
            saved.name,
            saved.driver,
            params_json,
            saved.folder,
            saved.color,
            saved.created_at.to_rfc3339(),
            chrono::Utc::now().to_rfc3339(),
        ],
    )
    .context("inserting/updating connection in storage")?;

    Ok(())
}

/// Remove a connection from the shared storage database.
pub fn delete_connection(id: Uuid) -> Result<()> {
    let db_path = storage_db_path()?;

    let db = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("opening storage database at {}", db_path.display()))?;

    db.execute(
        "DELETE FROM connections WHERE id = ?1",
        rusqlite::params![id.to_string()],
    )
    .context("deleting connection from storage")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Connection testing
// ---------------------------------------------------------------------------

/// Open a real driver connection and immediately close it as a connectivity test.
pub async fn test_connection(saved: &SavedConnection) -> Result<()> {
    let connection = open_connection(saved, None).await?;
    drop(connection);
    Ok(())
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Execute SQL against a saved connection and return a serializable execution record.
///
/// Statements run in order and stop at the first failure (the rest are
/// reported as skipped) unless `continue_on_error` is set. When
/// `single_transaction` is true, all statements are wrapped in an explicit
/// `BEGIN` / `COMMIT` block; if any statement fails the transaction is rolled
/// back instead.  The `BEGIN` failure is treated as a soft warning so that
/// drivers without explicit transaction support (e.g. Redis) still work.
pub async fn execute_query(
    saved: &SavedConnection,
    database: Option<&str>,
    sql: &str,
    single_transaction: bool,
    continue_on_error: bool,
) -> Result<QueryExecution> {
    let connection = open_connection(saved, database).await?;

    let start = std::time::Instant::now();
    let mut statement_results = Vec::new();
    let mut had_error = false;

    if single_transaction && let Err(e) = connection.execute("BEGIN", &[]).await {
        tracing::warn!(
            "BEGIN failed ({}); continuing without transaction wrapping",
            e
        );
    }

    // MySQL scripts split by MySQL's lexing rules (`#` comments, backslash
    // escapes, `DELIMITER`), not the standard ones.
    let dialect = SqlLexDialect::from_profile_id(connection.dialect_id());
    for stmt_sql in split_sql_statements_for_dialect(sql, dialect) {
        let stmt_sql = stmt_sql.trim();
        if stmt_sql.is_empty() {
            continue;
        }

        if had_error && !continue_on_error {
            statement_results.push(skipped_statement(stmt_sql));
            continue;
        }

        let stmt_start = std::time::Instant::now();
        let outcome = if is_query(stmt_sql) {
            connection
                .query(stmt_sql, &[])
                .await
                .map(|result| statement_from_query(stmt_sql, result))
        } else {
            connection
                .execute(stmt_sql, &[])
                .await
                .map(|result| statement_from_execute(stmt_sql, result))
        };
        let mut statement = outcome.unwrap_or_else(|e| {
            had_error = true;
            failed_statement(stmt_sql, e.to_string())
        });
        statement.duration_ms = stmt_start.elapsed().as_millis() as u64;
        statement_results.push(statement);
    }

    if single_transaction {
        let end_sql = if had_error { "ROLLBACK" } else { "COMMIT" };
        if let Err(e) = connection.execute(end_sql, &[]).await {
            tracing::warn!("{} failed: {}", end_sql, e);
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(QueryExecution {
        sql: sql.to_string(),
        duration_ms,
        statements: statement_results,
    })
}

fn empty_statement(sql: &str) -> StatementResult {
    StatementResult {
        sql: sql.to_string(),
        duration_ms: 0,
        columns: Vec::new(),
        rows: Vec::new(),
        affected_rows: 0,
        error: None,
        warnings: Vec::new(),
        skipped: false,
    }
}

fn failed_statement(sql: &str, error: String) -> StatementResult {
    StatementResult {
        error: Some(error),
        ..empty_statement(sql)
    }
}

fn skipped_statement(sql: &str) -> StatementResult {
    StatementResult {
        skipped: true,
        ..empty_statement(sql)
    }
}

/// A result set (possibly empty) with its columns, rows and notices.
fn result_set_statement(
    sql: &str,
    result: zqlz_core::QueryResult,
    affected_rows: u64,
) -> StatementResult {
    StatementResult {
        columns: result
            .columns
            .iter()
            .map(|c| ColumnMeta {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
            })
            .collect(),
        rows: result
            .rows
            .iter()
            .map(|r| {
                let mut row = Row {
                    values: Vec::new(),
                    cells: r.values.iter().map(|v| v.to_wire_json()).collect(),
                };
                row.normalize();
                row
            })
            .collect(),
        affected_rows,
        warnings: result.warnings,
        ..empty_statement(sql)
    }
}

/// Mirrors `statement_from_query_result` in `zqlz-query`: a command that went
/// through `Connection::query` and produced no result set reports only its
/// row count, so it is not shown as an empty grid.
fn statement_from_query(sql: &str, result: zqlz_core::QueryResult) -> StatementResult {
    if result.columns.is_empty() && result.rows.is_empty() && result.affected_rows > 0 {
        return StatementResult {
            affected_rows: result.affected_rows,
            warnings: result.warnings,
            ..empty_statement(sql)
        };
    }
    result_set_statement(sql, result, 0)
}

/// Mirrors `statement_from_statement_result` in `zqlz-query`: statements the
/// server answers with a result set (`INSERT ... RETURNING`, `CALL`, `EXECUTE`,
/// ...) keep their rows; any other `result` only carries notices.
fn statement_from_execute(sql: &str, statement: zqlz_core::StatementResult) -> StatementResult {
    match statement.result {
        Some(result) if !result.columns.is_empty() => {
            result_set_statement(sql, result, statement.affected_rows)
        }
        Some(result) => StatementResult {
            affected_rows: statement.affected_rows,
            warnings: result.warnings,
            ..empty_statement(sql)
        },
        None => StatementResult {
            affected_rows: statement.affected_rows,
            ..empty_statement(sql)
        },
    }
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

/// List database names available on a connection.
pub async fn list_databases(saved: &SavedConnection) -> Result<Vec<String>> {
    use zqlz_core::DatabaseInfo;

    let connection = open_connection(saved, None).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let databases: Vec<DatabaseInfo> = introspection
        .list_databases()
        .await
        .context("listing databases")?;

    Ok(databases.into_iter().map(|db| db.name).collect())
}

/// List table names in a database.
pub async fn list_tables(saved: &SavedConnection, database: Option<&str>) -> Result<Vec<String>> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let tables = introspection
        .list_tables(None)
        .await
        .context("listing tables")?;
    Ok(tables.into_iter().map(|t| t.name).collect())
}

/// List tables and views with their kind (`table`, `view`, `materialized view`).
pub async fn list_relations(
    saved: &SavedConnection,
    database: Option<&str>,
) -> Result<Vec<RelationEntry>> {
    let tables = list_tables(saved, database).await?;
    let mut relations: Vec<RelationEntry> = tables
        .into_iter()
        .map(|name| RelationEntry { name, kind: "table".to_string() })
        .collect();
    for view in list_views(saved, database).await? {
        if relations.iter().any(|seen| seen.name == view.name) {
            continue;
        }
        let kind = if view.is_materialized { "materialized view" } else { "view" };
        relations.push(RelationEntry { name: view.name, kind: kind.to_string() });
    }
    Ok(relations)
}

/// List columns for a specific table.
pub async fn list_columns(
    saved: &SavedConnection,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let table_info = introspection
        .get_table(None, table)
        .await
        .with_context(|| format!("fetching table '{}'", table))?;

    let columns = table_info
        .columns
        .into_iter()
        .map(|col| ColumnInfo {
            name: col.name,
            data_type: col.data_type,
            nullable: col.nullable,
            default_value: col.default_value,
            is_primary_key: col.is_primary_key,
        })
        .collect();

    Ok(columns)
}

/// List views in a database.
pub async fn list_views(
    saved: &SavedConnection,
    database: Option<&str>,
) -> Result<Vec<ViewSummary>> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    // Materialized views are listed separately by the drivers (the summary has
    // a Materialized column, so they belong here).
    let mut views = introspection
        .list_views(None)
        .await
        .context("listing views")?;
    views.extend(
        introspection
            .list_materialized_views(None)
            .await
            .context("listing materialized views")?,
    );

    Ok(views
        .into_iter()
        .map(|v| ViewSummary {
            name: v.name,
            schema: v.schema,
            is_materialized: v.is_materialized,
        })
        .collect())
}

/// List schemas / namespaces on a connection.
pub async fn list_schemas(saved: &SavedConnection) -> Result<Vec<String>> {
    let connection = open_connection(saved, None).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let schemas = introspection
        .list_schemas()
        .await
        .context("listing schemas")?;

    Ok(schemas.into_iter().map(|s| s.name).collect())
}

/// List indexes for a specific table.
pub async fn list_indexes(
    saved: &SavedConnection,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<IndexSummary>> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    // An unknown table would otherwise look like a table without indexes.
    introspection
        .get_table(None, table)
        .await
        .with_context(|| format!("fetching table '{}'", table))?;

    let indexes = introspection
        .get_indexes(None, table)
        .await
        .with_context(|| format!("listing indexes for table '{}'", table))?;

    Ok(indexes
        .into_iter()
        .map(|idx| IndexSummary {
            name: idx.name,
            columns: idx.columns,
            is_unique: idx.is_unique,
            is_primary: idx.is_primary,
            index_type: idx.index_type,
        })
        .collect())
}

/// List functions in a database.
pub async fn list_functions(
    saved: &SavedConnection,
    database: Option<&str>,
) -> Result<Vec<FunctionSummary>> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let functions = introspection
        .list_functions(None)
        .await
        .context("listing functions")?;

    Ok(functions
        .into_iter()
        .map(|f| FunctionSummary {
            name: f.name,
            schema: f.schema,
            language: f.language,
            return_type: f.return_type,
        })
        .collect())
}

/// Generate DDL for a database object using the driver's introspection engine.
pub async fn generate_ddl(
    saved: &SavedConnection,
    database: Option<&str>,
    object_type: zqlz_core::ObjectType,
    object_name: &str,
    schema: Option<&str>,
) -> Result<String> {
    let connection = open_connection(saved, database).await?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let object = zqlz_core::DatabaseObject {
        object_type,
        schema: schema.map(String::from),
        name: object_name.to_string(),
        signature: None,
    };

    introspection
        .generate_ddl(&object)
        .await
        .with_context(|| format!("generating DDL for '{}'", object_name))
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

/// Load query history entries from the shared storage database.
pub fn load_history(
    limit: usize,
    connection_name_or_id: Option<&str>,
    search: Option<&str>,
) -> Result<Vec<HistoryEntry>> {
    let db_path = storage_db_path()?;

    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let db = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("opening storage database at {}", db_path.display()))?;

    // Optionally resolve the connection filter to a UUID
    let connection_id_filter: Option<String> = if let Some(name_or_id) = connection_name_or_id {
        if Uuid::parse_str(name_or_id).is_ok() {
            Some(name_or_id.to_string())
        } else {
            let result: rusqlite::Result<String> = db.query_row(
                "SELECT id FROM connections WHERE LOWER(name) = LOWER(?1) LIMIT 1",
                rusqlite::params![name_or_id],
                |row| row.get(0),
            );
            match result {
                Ok(id) => Some(id),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    return Err(anyhow::anyhow!(
                        "no connection found matching '{}'",
                        name_or_id
                    ));
                }
                Err(e) => return Err(e).context("looking up connection by name"),
            }
        }
    } else {
        None
    };

    let mut sql_query = String::from(
        "SELECT id, connection_id, query_text, executed_at, duration_ms, row_count, success, error
         FROM query_history WHERE 1=1",
    );

    if connection_id_filter.is_some() {
        sql_query.push_str(" AND connection_id = ?");
    }
    if search.is_some() {
        sql_query.push_str(" AND query_text LIKE ?");
    }
    sql_query.push_str(" ORDER BY executed_at DESC LIMIT ?");

    let mut stmt = db.prepare(&sql_query).context("preparing history query")?;

    let search_pattern = search.map(|s| format!("%{}%", s));
    let limit_val = limit as i64;

    let entries: Vec<HistoryEntry> = stmt
        .query_map(
            rusqlite::params_from_iter(build_history_params(
                connection_id_filter.as_deref(),
                search_pattern.as_deref(),
                limit_val,
            )),
            |row| {
                let id_str: String = row.get(0)?;
                let conn_id_str: Option<String> = row.get(1)?;
                let query_text: String = row.get(2)?;
                let executed_at_str: String = row.get(3)?;
                let duration_ms: Option<u64> = row.get(4)?;
                let row_count: Option<u64> = row.get(5)?;
                let success: bool = row.get::<_, i64>(6).map(|v| v != 0)?;
                let error: Option<String> = row.get(7)?;

                let id = Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4());
                let connection_id = conn_id_str.as_deref().and_then(|s| Uuid::parse_str(s).ok());
                let executed_at = executed_at_str
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap_or_else(|_| chrono::Utc::now());

                Ok(HistoryEntry {
                    id,
                    sql: query_text,
                    connection_id,
                    executed_at,
                    duration_ms: duration_ms.unwrap_or(0),
                    row_count,
                    success,
                    error,
                })
            },
        )
        .context("querying history")?
        .collect::<Result<Vec<_>, _>>()
        .context("collecting history entries")?;

    Ok(entries)
}

/// Fetch a single history entry by its UUID. Returns `None` when the ID is not
/// found so the caller can produce a user-facing "not found" message without
/// treating the absence as an error.
pub fn load_history_entry(id: Uuid) -> Result<Option<HistoryEntry>> {
    let db_path = storage_db_path()?;

    if !db_path.exists() {
        return Ok(None);
    }

    let db = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("opening storage database at {}", db_path.display()))?;

    let result = db.query_row(
        "SELECT id, connection_id, query_text, executed_at, duration_ms, row_count, success, error
         FROM query_history WHERE id = ?1 LIMIT 1",
        rusqlite::params![id.to_string()],
        |row| {
            let id_str: String = row.get(0)?;
            let conn_id_str: Option<String> = row.get(1)?;
            let query_text: String = row.get(2)?;
            let executed_at_str: String = row.get(3)?;
            let duration_ms: Option<u64> = row.get(4)?;
            let row_count: Option<u64> = row.get(5)?;
            let success: bool = row.get::<_, i64>(6).map(|v| v != 0)?;
            let error: Option<String> = row.get(7)?;

            let entry_id = Uuid::parse_str(&id_str).unwrap_or(id);
            let connection_id = conn_id_str.as_deref().and_then(|s| Uuid::parse_str(s).ok());
            let executed_at = executed_at_str
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap_or_else(|_| chrono::Utc::now());

            Ok(HistoryEntry {
                id: entry_id,
                sql: query_text,
                connection_id,
                executed_at,
                duration_ms: duration_ms.unwrap_or(0),
                row_count,
                success,
                error,
            })
        },
    );

    match result {
        Ok(entry) => Ok(Some(entry)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e).context("querying history entry by id"),
    }
}

fn build_history_params(
    connection_id: Option<&str>,
    search_pattern: Option<&str>,
    limit: i64,
) -> Vec<Box<dyn rusqlite::ToSql>> {
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(id) = connection_id {
        params.push(Box::new(id.to_string()));
    }
    if let Some(pattern) = search_pattern {
        params.push(Box::new(pattern.to_string()));
    }
    params.push(Box::new(limit));
    params
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Open a driver connection for a saved connection config.
///
/// When `database` is `Some`, the connection is opened against that specific
/// database (for drivers like PostgreSQL that scope connections per-database).
async fn open_connection(
    saved: &SavedConnection,
    database: Option<&str>,
) -> Result<Arc<dyn Connection>> {
    let registry = DriverRegistry::with_defaults();
    let driver = registry
        .get(&saved.driver)
        .ok_or_else(|| anyhow::anyhow!("unknown driver '{}'", saved.driver))?;

    let mut config = saved.to_connection_config();
    if let Some(db) = database {
        config = config.with_param("database", db);
    }

    let connection = driver
        .connect(&config)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    Ok(connection)
}

/// Whether a statement is routed through `Connection::query` by its leading
/// keyword. Only a routing hint: `execute` also returns the rows of anything
/// else that has a result set.
fn is_query(sql: &str) -> bool {
    matches!(
        leading_word(sql).as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "TABLE" | "VALUES"
    )
}

/// First keyword of `sql`, upper-cased, skipping whitespace, comments and `(`.
fn leading_word(sql: &str) -> String {
    let mut rest = sql;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, tail)| tail);
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map_or("", |(_, tail)| tail);
        } else {
            break;
        }
    }
    rest.chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{ColumnMeta as CoreColumn, QueryResult, Value};

    fn result(
        columns: &[&str],
        rows: Vec<Vec<Value>>,
        affected: u64,
        notice: &[&str],
    ) -> QueryResult {
        let mut result = QueryResult::empty();
        result.columns = columns
            .iter()
            .map(|name| CoreColumn {
                name: name.to_string(),
                data_type: "text".into(),
                ..Default::default()
            })
            .collect();
        result.rows = rows
            .into_iter()
            .map(|values| zqlz_core::Row::new(Vec::new(), values))
            .collect();
        result.affected_rows = affected;
        result.warnings = notice.iter().map(|n| n.to_string()).collect();
        result
    }

    #[test]
    fn leading_word_skips_comments_and_parens() {
        assert_eq!(leading_word("  /* hi */ -- x\n (select 1)"), "SELECT");
        assert_eq!(leading_word("withdraw()"), "WITHDRAW");
        assert!(is_query("values (1)"));
        assert!(is_query("TABLE users"));
        assert!(!is_query("insert into t values (1) returning id"));
        assert!(!is_query("withdraw()"));
    }

    #[test]
    fn execute_with_result_set_keeps_rows_and_notices() {
        let returning = zqlz_core::StatementResult {
            is_query: true,
            result: Some(result(
                &["id", "name"],
                vec![
                    vec![Value::Int64(1), Value::Null],
                    vec![Value::Int64(2), Value::String("NULL".into())],
                ],
                0,
                &["NOTICE: hi"],
            )),
            affected_rows: 2,
            error: None,
        };
        let statement = statement_from_execute("INSERT ... RETURNING id", returning);
        assert_eq!(statement.columns.len(), 2);
        assert_eq!(statement.rows.len(), 2);
        assert_eq!(statement.affected_rows, 2);
        assert_eq!(statement.warnings, vec!["NOTICE: hi"]);
        // SQL NULL and the string 'NULL' stay distinguishable in the typed cells.
        assert_eq!(statement.rows[0].cells[1], serde_json::Value::Null);
        assert_eq!(statement.rows[1].cells[1], serde_json::json!("NULL"));
    }

    #[test]
    fn execute_without_result_set_reports_counts_and_notices_only() {
        let command = zqlz_core::StatementResult {
            is_query: false,
            result: Some(result(&[], Vec::new(), 0, &["NOTICE: hi"])),
            affected_rows: 3,
            error: None,
        };
        let statement = statement_from_execute("UPDATE t SET a = 1", command);
        assert!(statement.columns.is_empty() && statement.rows.is_empty());
        assert_eq!(statement.affected_rows, 3);
        assert_eq!(statement.warnings, vec!["NOTICE: hi"]);
    }

    #[test]
    fn query_without_result_set_is_reported_as_a_statement() {
        let statement = statement_from_query("SELECT 1 INTO t", result(&[], Vec::new(), 4, &[]));
        assert!(statement.columns.is_empty());
        assert_eq!(statement.affected_rows, 4);

        let statement =
            statement_from_query("SELECT 1 WHERE false", result(&["a"], Vec::new(), 0, &[]));
        assert_eq!(
            statement.columns.len(),
            1,
            "an empty SELECT still shows its columns"
        );
    }

    #[test]
    fn splitting_respects_dollar_quoted_bodies() {
        let statements = split_sql_statements_for_dialect(
            "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql; SELECT 1;",
            SqlLexDialect::Standard,
        );
        assert_eq!(statements.len(), 2);
    }
}
