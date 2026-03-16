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

use zqlz_core::{Connection, ConnectionConfig};
use zqlz_drivers::DriverRegistry;

use crate::ipc::{
    ColumnInfo, ColumnMeta, FunctionSummary, HistoryEntry, IndexSummary, QueryExecution, Row,
    StatementResult, ViewSummary,
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
    let config_dir = dirs::config_dir().context("could not determine config directory")?;
    Ok(config_dir.join("zqlz").join("storage.db"))
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

            let id = Uuid::parse_str(&id_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let params: HashMap<String, String> =
                serde_json::from_str(&params_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

            let created_at = created_at_str
                .as_deref()
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok())
                .unwrap_or_else(chrono::Utc::now);

            let modified_at = updated_at_str
                .as_deref()
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok())
                .unwrap_or(created_at);

            Ok(SavedConnection {
                id,
                name,
                driver,
                params,
                folder,
                color,
                created_at,
                modified_at,
                last_connected: None,
            })
        })
        .context("querying connections")?
        .collect::<Result<Vec<_>, _>>()
        .context("collecting connections")?;

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
/// When `single_transaction` is true, all statements are wrapped in an explicit
/// `BEGIN` / `COMMIT` block.  If any statement produces an error the transaction
/// is rolled back instead.  The `BEGIN` failure is treated as a soft warning so
/// that drivers without explicit transaction support (e.g. Redis) still work.
pub async fn execute_query(
    saved: &SavedConnection,
    database: Option<&str>,
    sql: &str,
    single_transaction: bool,
) -> Result<QueryExecution> {
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    let start = std::time::Instant::now();
    let statements_sql = split_statements(sql);
    let mut statement_results = Vec::new();
    let mut had_error = false;

    if single_transaction && let Err(e) = connection.execute("BEGIN", &[]).await {
        tracing::warn!(
            "BEGIN failed ({}); continuing without transaction wrapping",
            e
        );
    }

    for stmt_sql in &statements_sql {
        let stmt_sql = stmt_sql.trim();
        if stmt_sql.is_empty() {
            continue;
        }

        let stmt_start = std::time::Instant::now();

        if is_query(stmt_sql) {
            match connection.query(stmt_sql, &[]).await {
                Ok(query_result) => {
                    let duration_ms = stmt_start.elapsed().as_millis() as u64;
                    let columns = query_result
                        .columns
                        .iter()
                        .map(|c| ColumnMeta {
                            name: c.name.clone(),
                            data_type: c.data_type.clone(),
                        })
                        .collect();
                    let rows = query_result
                        .rows
                        .iter()
                        .map(|r| Row {
                            values: r.values.iter().map(|v| v.to_string()).collect(),
                        })
                        .collect();
                    statement_results.push(StatementResult {
                        sql: stmt_sql.to_string(),
                        duration_ms,
                        columns,
                        rows,
                        affected_rows: 0,
                        error: None,
                    });
                }
                Err(e) => {
                    had_error = true;
                    let duration_ms = stmt_start.elapsed().as_millis() as u64;
                    statement_results.push(StatementResult {
                        sql: stmt_sql.to_string(),
                        duration_ms,
                        columns: Vec::new(),
                        rows: Vec::new(),
                        affected_rows: 0,
                        error: Some(e.to_string()),
                    });
                }
            }
        } else {
            match connection.execute(stmt_sql, &[]).await {
                Ok(stmt_result) => {
                    let duration_ms = stmt_start.elapsed().as_millis() as u64;
                    statement_results.push(StatementResult {
                        sql: stmt_sql.to_string(),
                        duration_ms,
                        columns: Vec::new(),
                        rows: Vec::new(),
                        affected_rows: stmt_result.affected_rows,
                        error: None,
                    });
                }
                Err(e) => {
                    had_error = true;
                    let duration_ms = stmt_start.elapsed().as_millis() as u64;
                    statement_results.push(StatementResult {
                        sql: stmt_sql.to_string(),
                        duration_ms,
                        columns: Vec::new(),
                        rows: Vec::new(),
                        affected_rows: 0,
                        error: Some(e.to_string()),
                    });
                }
            }
        }
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

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

/// List database names available on a connection.
pub async fn list_databases(saved: &SavedConnection) -> Result<Vec<String>> {
    use zqlz_core::DatabaseInfo;

    let connection = open_connection(saved, None)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

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
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let tables = introspection
        .list_tables(None)
        .await
        .context("listing tables")?;
    Ok(tables.into_iter().map(|t| t.name).collect())
}

/// List columns for a specific table.
pub async fn list_columns(
    saved: &SavedConnection,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

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
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let views = introspection
        .list_views(None)
        .await
        .context("listing views")?;

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
    let connection = open_connection(saved, None)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

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
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

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
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

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
    let connection = open_connection(saved, database)
        .await
        .with_context(|| format!("connecting to '{}'", saved.name))?;

    let introspection = connection
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("driver does not support schema introspection"))?;

    let object = zqlz_core::DatabaseObject {
        object_type,
        schema: schema.map(String::from),
        name: object_name.to_string(),
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

/// Determine whether a SQL string returns rows (SELECT-like) or modifies data.
fn is_query(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("DESC ")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("TABLE ")
        || upper.starts_with("VALUES")
}

/// Split SQL text into individual statements on `;`, respecting string
/// literals and `--` / `/* */` comments.
fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                current.push(c);
                while let Some(sc) = chars.next() {
                    current.push(sc);
                    if sc == '\'' {
                        if chars.peek() == Some(&'\'') {
                            current.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            '"' => {
                current.push(c);
                while let Some(sc) = chars.next() {
                    current.push(sc);
                    if sc == '"' {
                        if chars.peek() == Some(&'"') {
                            current.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                current.push(c);
                current.push(chars.next().unwrap());
                while let Some(sc) = chars.by_ref().next() {
                    current.push(sc);
                    if sc == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                current.push(c);
                current.push(chars.next().unwrap());
                let mut prev = '\0';
                while let Some(sc) = chars.by_ref().next() {
                    current.push(sc);
                    if prev == '*' && sc == '/' {
                        break;
                    }
                    prev = sc;
                }
            }
            ';' => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}
