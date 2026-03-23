//! Local SQLite storage for app settings and connections
//!
//! This module provides a local SQLite database for storing:
//! - Application settings
//! - Saved connection details (passwords stored directly in params_json)
//! - Saved queries
//! - Recent files/queries
//! - Workspace layouts
//! - DBT-style projects and models

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use zqlz_command_palette::{CommandUsageEntry, CommandUsagePersistence};
use zqlz_connection::SavedConnection;
use zqlz_internal_storage::InternalStorage;
use zqlz_internal_storage::rusqlite::{self, Connection, params};
use zqlz_query::{HistoryPersistence, QueryHistoryEntry};
use zqlz_templates::dbt::{ModelConfig, QuotingConfig};
use zqlz_templates::project::{Model, ModelDependency, Project, SourceDefinition, SourceTable};

/// A saved query associated with a connection
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedQuery {
    /// Unique identifier
    pub id: Uuid,
    /// Display name of the query
    pub name: String,
    /// Connection this query belongs to
    pub connection_id: Uuid,
    /// The SQL text
    pub sql: String,
    /// When the query was created
    pub created_at: DateTime<Utc>,
    /// When the query was last modified
    pub updated_at: DateTime<Utc>,
}

impl SavedQuery {
    /// Create a new saved query
    pub fn new(name: String, connection_id: Uuid, sql: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            connection_id,
            sql,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Local storage manager using SQLite
pub struct LocalStorage {
    storage: InternalStorage,
}

#[allow(dead_code)]
impl LocalStorage {
    /// Create a new local storage instance
    pub fn new() -> Result<Self> {
        let storage = Self {
            storage: InternalStorage::for_config_file("storage.db")?,
        };
        storage.initialize_schema()?;

        Ok(storage)
    }

    /// Initialize the database schema
    fn initialize_schema(&self) -> Result<()> {
        let conn = self.connect()?;

        // Settings table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        // Connections table — all params including passwords are stored in params_json
        conn.execute(
            "CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                driver TEXT NOT NULL,
                params_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        // Recent connections table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS recent_connections (
                connection_id TEXT NOT NULL,
                accessed_at TEXT NOT NULL,
                PRIMARY KEY (connection_id)
            )",
            [],
        )?;

        // Query history table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS query_history (
                id TEXT PRIMARY KEY,
                connection_id TEXT,
                query_text TEXT NOT NULL,
                executed_at TEXT NOT NULL,
                duration_ms INTEGER,
                row_count INTEGER,
                success INTEGER NOT NULL,
                error TEXT
            )",
            [],
        )?;

        // Migration: add error column to databases created before it was introduced.
        // SQLite returns an error when the column already exists, which we ignore here
        // because that is the expected outcome for up-to-date databases.
        let _ = conn.execute("ALTER TABLE query_history ADD COLUMN error TEXT", []);

        // Migrations for connections columns added after initial schema.
        // SQLite has no IF NOT EXISTS for ALTER TABLE; the error on duplicate add is expected.
        let _ = conn.execute("ALTER TABLE connections ADD COLUMN folder TEXT", []);
        let _ = conn.execute("ALTER TABLE connections ADD COLUMN color TEXT", []);

        // Saved queries table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS saved_queries (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                connection_id TEXT NOT NULL,
                sql TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (connection_id) REFERENCES connections(id) ON DELETE CASCADE
            )",
            [],
        )?;

        // Index for faster lookup by connection
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_saved_queries_connection 
             ON saved_queries(connection_id)",
            [],
        )?;

        // Command palette usage stats for frecency ranking across restarts
        conn.execute(
            "CREATE TABLE IF NOT EXISTS command_usage (
                command_id TEXT PRIMARY KEY,
                use_count REAL NOT NULL,
                last_used REAL NOT NULL
            )",
            [],
        )?;

        Ok(())
    }

    /// Get a database connection
    fn connect(&self) -> Result<Connection> {
        self.storage.connect().with_context(|| {
            format!(
                "Failed to open database at {}",
                self.storage.path().display()
            )
        })
    }

    /// Save a connection, storing all params (including password) directly in params_json.
    pub fn save_connection(&self, connection: &SavedConnection) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();
        let params_json = serde_json::to_string(&connection.params)?;

        conn.execute(
            "INSERT OR REPLACE INTO connections (id, name, driver, params_json, folder, color, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                connection.id.to_string(),
                connection.name,
                connection.driver,
                params_json,
                connection.folder,
                connection.color,
                now,
                now,
            ],
        )?;

        Ok(())
    }

    /// Load all saved connections.
    pub fn load_connections(&self) -> Result<Vec<SavedConnection>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, driver, params_json, folder, color FROM connections ORDER BY updated_at DESC",
        )?;

        let connections = stmt
            .query_map([], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let name: String = row.get(1)?;
                let driver: String = row.get(2)?;
                let params_json: String = row.get(3)?;
                let folder: Option<String> = row.get(4)?;
                let color: Option<String> = row.get(5)?;

                let params: std::collections::HashMap<String, String> =
                    serde_json::from_str(&params_json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?;

                Ok(SavedConnection {
                    id,
                    name,
                    driver,
                    params,
                    folder,
                    color,
                    created_at: chrono::Utc::now(),
                    modified_at: chrono::Utc::now(),
                    last_connected: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(connections)
    }

    /// Delete a connection.
    pub fn delete_connection(&self, id: Uuid) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM connections WHERE id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }

    /// Update recent connection access time
    pub fn update_recent_connection(&self, connection_id: Uuid) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO recent_connections (connection_id, accessed_at) VALUES (?1, ?2)",
            params![connection_id.to_string(), now],
        )?;

        Ok(())
    }

    /// Get recent connections (last 10)
    pub fn get_recent_connections(&self) -> Result<Vec<Uuid>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT connection_id FROM recent_connections ORDER BY accessed_at DESC LIMIT 10",
        )?;

        let ids = stmt
            .query_map([], |row| {
                let id_str: String = row.get(0)?;
                Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    /// Save a setting
    pub fn save_setting(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?1, ?2, ?3)",
            params![key, value, now],
        )?;

        Ok(())
    }

    /// Load a setting
    pub fn load_setting(&self, key: &str) -> Result<Option<String>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?1")?;
        let result = stmt.query_row(params![key], |row| row.get(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist a query history entry to the database.
    pub fn add_query_history(&self, entry: &QueryHistoryEntry) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "INSERT OR IGNORE INTO query_history
                (id, connection_id, query_text, executed_at, duration_ms, row_count, success, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.id.to_string(),
                entry.connection_id.map(|id| id.to_string()),
                entry.sql,
                entry.executed_at.to_rfc3339(),
                entry.duration_ms as i64,
                entry.row_count.map(|c| c as i64),
                if entry.success { 1_i64 } else { 0_i64 },
                entry.error,
            ],
        )?;

        Ok(())
    }

    /// Load the most recent `limit` history entries, ordered oldest-first so
    /// callers can feed them into [`QueryHistory::load_entry`] in sequence.
    pub fn load_query_history(&self, limit: usize) -> Result<Vec<QueryHistoryEntry>> {
        let conn = self.connect()?;

        // Select the newest `limit` rows, then reverse to oldest-first ordering so
        // callers can restore them in chronological sequence.
        let mut stmt = conn.prepare(
            "SELECT id, connection_id, query_text, executed_at, duration_ms, row_count, success, error
             FROM (
                 SELECT id, connection_id, query_text, executed_at, duration_ms, row_count, success, error
                 FROM query_history
                 ORDER BY executed_at DESC
                 LIMIT ?1
             )
             ORDER BY executed_at ASC",
        )?;

        let entries = stmt
            .query_map(params![limit as i64], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let connection_id: Option<String> = row.get(1)?;
                let connection_id = connection_id.and_then(|s| Uuid::parse_str(&s).ok());

                let sql: String = row.get(2)?;

                let executed_at_str: String = row.get(3)?;
                let executed_at = DateTime::parse_from_rfc3339(&executed_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                let duration_ms: i64 = row.get(4)?;
                let row_count: Option<i64> = row.get(5)?;
                let success: i64 = row.get(6)?;
                let error: Option<String> = row.get(7)?;

                Ok(QueryHistoryEntry {
                    id,
                    sql,
                    connection_id,
                    executed_at,
                    duration_ms: duration_ms as u64,
                    row_count: row_count.map(|c| c as u64),
                    error,
                    success: success != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// Delete all rows from the query history table.
    pub fn clear_query_history(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute("DELETE FROM query_history", [])?;
        Ok(())
    }

    // ── Command palette usage persistence ───────────────────────────────

    fn upsert_command_usage(&self, entry: &CommandUsageEntry) -> Result<()> {
        let conn = self.connect()?;

        let last_used_secs = entry
            .last_used
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        conn.execute(
            "INSERT OR REPLACE INTO command_usage (command_id, use_count, last_used)
             VALUES (?1, ?2, ?3)",
            params![entry.command_id, entry.use_count as f64, last_used_secs],
        )?;

        Ok(())
    }

    fn load_command_usage(&self) -> Result<Vec<CommandUsageEntry>> {
        let conn = self.connect()?;

        let mut stmt =
            conn.prepare("SELECT command_id, use_count, last_used FROM command_usage")?;

        let entries = stmt
            .query_map([], |row| {
                let command_id: String = row.get(0)?;
                let use_count: f64 = row.get(1)?;
                let last_used_secs: f64 = row.get(2)?;

                let last_used = std::time::SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs_f64(last_used_secs);

                Ok(CommandUsageEntry {
                    command_id,
                    use_count: use_count as f32,
                    last_used,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    fn clear_command_usage(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute("DELETE FROM command_usage", [])?;
        Ok(())
    }

    /// Save a query to the database
    pub fn save_query(&self, query: &SavedQuery) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "INSERT OR REPLACE INTO saved_queries (id, name, connection_id, sql, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                query.id.to_string(),
                query.name,
                query.connection_id.to_string(),
                query.sql,
                query.created_at.to_rfc3339(),
                query.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    /// Load all saved queries for a specific connection
    pub fn load_queries_for_connection(&self, connection_id: Uuid) -> Result<Vec<SavedQuery>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, connection_id, sql, created_at, updated_at 
             FROM saved_queries 
             WHERE connection_id = ?1 
             ORDER BY name ASC",
        )?;

        let queries = stmt
            .query_map(params![connection_id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let name: String = row.get(1)?;

                let conn_id_str: String = row.get(2)?;
                let connection_id = Uuid::parse_str(&conn_id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let sql: String = row.get(3)?;

                let created_at_str: String = row.get(4)?;
                let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                let updated_at_str: String = row.get(5)?;
                let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                Ok(SavedQuery {
                    id,
                    name,
                    connection_id,
                    sql,
                    created_at,
                    updated_at,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(queries)
    }

    /// Load a single saved query by ID
    pub fn load_query(&self, query_id: Uuid) -> Result<Option<SavedQuery>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, connection_id, sql, created_at, updated_at 
             FROM saved_queries 
             WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![query_id.to_string()], |row| {
            let id_str: String = row.get(0)?;
            let id = Uuid::parse_str(&id_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let name: String = row.get(1)?;

            let conn_id_str: String = row.get(2)?;
            let connection_id = Uuid::parse_str(&conn_id_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let sql: String = row.get(3)?;

            let created_at_str: String = row.get(4)?;
            let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            let updated_at_str: String = row.get(5)?;
            let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            Ok(SavedQuery {
                id,
                name,
                connection_id,
                sql,
                created_at,
                updated_at,
            })
        });

        match result {
            Ok(query) => Ok(Some(query)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Update a saved query's SQL content
    pub fn update_query_sql(&self, query_id: Uuid, sql: &str) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE saved_queries SET sql = ?1, updated_at = ?2 WHERE id = ?3",
            params![sql, now, query_id.to_string()],
        )?;

        Ok(())
    }

    /// Rename a saved query
    pub fn rename_query(&self, query_id: Uuid, new_name: &str) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE saved_queries SET name = ?1, updated_at = ?2 WHERE id = ?3",
            params![new_name, now, query_id.to_string()],
        )?;

        Ok(())
    }

    /// Delete a saved query
    pub fn delete_query(&self, query_id: Uuid) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM saved_queries WHERE id = ?1",
            params![query_id.to_string()],
        )?;

        Ok(())
    }

    /// Delete all saved queries for a connection (used when deleting a connection)
    pub fn delete_queries_for_connection(&self, connection_id: Uuid) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM saved_queries WHERE connection_id = ?1",
            params![connection_id.to_string()],
        )?;

        Ok(())
    }

    /// Check if a query name already exists for a connection
    pub fn query_name_exists(&self, connection_id: Uuid, name: &str) -> Result<bool> {
        let conn = self.connect()?;

        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM saved_queries WHERE connection_id = ?1 AND name = ?2")?;

        let count: i64 =
            stmt.query_row(params![connection_id.to_string(), name], |row| row.get(0))?;

        Ok(count > 0)
    }
}

impl Default for LocalStorage {
    fn default() -> Self {
        Self::new().expect("Failed to initialize local storage")
    }
}

impl HistoryPersistence for LocalStorage {
    fn persist_entry(&self, entry: &QueryHistoryEntry) {
        if let Err(error) = self.add_query_history(entry) {
            tracing::error!(%error, query_id = %entry.id, "Failed to persist query history entry");
        }
    }

    fn clear_all(&self) {
        if let Err(error) = self.clear_query_history() {
            tracing::error!(%error, "Failed to clear persisted query history");
        }
    }
}

impl CommandUsagePersistence for LocalStorage {
    fn persist_usage(&self, entry: &CommandUsageEntry) {
        if let Err(error) = self.upsert_command_usage(entry) {
            tracing::error!(%error, command_id = %entry.command_id, "Failed to persist command usage");
        }
    }

    fn load_all(&self) -> Vec<CommandUsageEntry> {
        match self.load_command_usage() {
            Ok(entries) => entries,
            Err(error) => {
                tracing::error!(%error, "Failed to load command usage from storage");
                Vec::new()
            }
        }
    }

    fn clear_all(&self) {
        if let Err(error) = self.clear_command_usage() {
            tracing::error!(%error, "Failed to clear command usage");
        }
    }
}

/// Type of SQL template
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum TemplateType {
    /// Plain SQL with simple variable substitution
    #[default]
    PlainSql,
    /// DBT-style template with ref(), source(), var(), config() functions
    DbtModel,
}

#[allow(dead_code)]
impl TemplateType {
    /// Convert to string for storage
    pub fn as_str(&self) -> &'static str {
        match self {
            TemplateType::PlainSql => "plain_sql",
            TemplateType::DbtModel => "dbt_model",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Self {
        match s {
            "dbt_model" => TemplateType::DbtModel,
            _ => TemplateType::PlainSql,
        }
    }
}

impl std::fmt::Display for TemplateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A saved SQL template with parameters
#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct SavedTemplate {
    /// Unique identifier
    pub id: Uuid,
    /// Display name of the template
    pub name: String,
    /// Description of what this template does
    pub description: String,
    /// The template SQL (MiniJinja format)
    pub template_sql: String,
    /// Default parameters as JSON string
    pub default_params: String,
    /// Category/tags for organization (comma-separated)
    pub tags: String,
    /// Type of template (PlainSql or DbtModel)
    #[serde(default)]
    pub template_type: TemplateType,
    /// When the template was created
    pub created_at: DateTime<Utc>,
    /// When the template was last modified
    pub updated_at: DateTime<Utc>,
}

impl SavedTemplate {
    /// Create a new saved template
    pub fn new(
        name: String,
        description: String,
        template_sql: String,
        default_params: String,
        tags: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            description,
            template_sql,
            default_params,
            tags,
            template_type: TemplateType::PlainSql,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new DBT model template
    pub fn new_dbt(
        name: String,
        description: String,
        template_sql: String,
        default_params: String,
        tags: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            description,
            template_sql,
            default_params,
            tags,
            template_type: TemplateType::DbtModel,
            created_at: now,
            updated_at: now,
        }
    }

    /// Check if this is a DBT model template
    pub fn is_dbt(&self) -> bool {
        matches!(self.template_type, TemplateType::DbtModel)
    }

    /// Auto-detect template type from SQL content
    #[allow(dead_code)]
    pub fn detect_template_type(sql: &str) -> TemplateType {
        // Check for DBT-specific functions
        let dbt_patterns = [
            "ref(", "source(", "config(", "ref('", "source('", "config('",
        ];
        if dbt_patterns.iter().any(|p| sql.contains(p)) {
            TemplateType::DbtModel
        } else {
            TemplateType::PlainSql
        }
    }
}

#[allow(dead_code)]
impl LocalStorage {
    /// Initialize the templates table (call in initialize_schema)
    pub fn initialize_templates_schema(&self) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                template_sql TEXT NOT NULL,
                default_params TEXT NOT NULL DEFAULT '{}',
                tags TEXT NOT NULL DEFAULT '',
                template_type TEXT NOT NULL DEFAULT 'plain_sql',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        // Migration: Add template_type column if it doesn't exist (for existing databases).
        // Error is intentionally ignored — the column already exists in up-to-date databases and
        // SQLite returns an error for duplicate ADD COLUMN rather than a no-op.
        let _ = conn.execute(
            "ALTER TABLE templates ADD COLUMN template_type TEXT NOT NULL DEFAULT 'plain_sql'",
            [],
        );

        // Index for searching by name
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_templates_name ON templates(name)",
            [],
        )?;

        Ok(())
    }

    /// Save a template to the database
    pub fn save_template(&self, template: &SavedTemplate) -> Result<()> {
        // Ensure schema exists
        self.initialize_templates_schema()?;

        let conn = self.connect()?;

        conn.execute(
            "INSERT OR REPLACE INTO templates (id, name, description, template_sql, default_params, tags, template_type, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                template.id.to_string(),
                template.name,
                template.description,
                template.template_sql,
                template.default_params,
                template.tags,
                template.template_type.as_str(),
                template.created_at.to_rfc3339(),
                template.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    /// Load all saved templates
    pub fn load_templates(&self) -> Result<Vec<SavedTemplate>> {
        // Ensure schema exists
        self.initialize_templates_schema()?;

        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, description, template_sql, default_params, tags, template_type, created_at, updated_at 
             FROM templates 
             ORDER BY name ASC",
        )?;

        let templates = stmt
            .query_map([], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let name: String = row.get(1)?;
                let description: String = row.get(2)?;
                let template_sql: String = row.get(3)?;
                let default_params: String = row.get(4)?;
                let tags: String = row.get(5)?;
                let template_type_str: String = row.get(6)?;
                let template_type = TemplateType::from_str(&template_type_str);

                let created_at_str: String = row.get(7)?;
                let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                let updated_at_str: String = row.get(8)?;
                let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                Ok(SavedTemplate {
                    id,
                    name,
                    description,
                    template_sql,
                    default_params,
                    tags,
                    template_type,
                    created_at,
                    updated_at,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(templates)
    }

    /// Load a single template by ID
    pub fn load_template(&self, template_id: Uuid) -> Result<Option<SavedTemplate>> {
        // Ensure schema exists
        self.initialize_templates_schema()?;

        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, description, template_sql, default_params, tags, template_type, created_at, updated_at 
             FROM templates 
             WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![template_id.to_string()], |row| {
            let id_str: String = row.get(0)?;
            let id = Uuid::parse_str(&id_str).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            let name: String = row.get(1)?;
            let description: String = row.get(2)?;
            let template_sql: String = row.get(3)?;
            let default_params: String = row.get(4)?;
            let tags: String = row.get(5)?;
            let template_type_str: String = row.get(6)?;
            let template_type = TemplateType::from_str(&template_type_str);

            let created_at_str: String = row.get(7)?;
            let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            let updated_at_str: String = row.get(8)?;
            let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            Ok(SavedTemplate {
                id,
                name,
                description,
                template_sql,
                default_params,
                tags,
                template_type,
                created_at,
                updated_at,
            })
        });

        match result {
            Ok(template) => Ok(Some(template)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Update a template
    pub fn update_template(&self, template: &SavedTemplate) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE templates SET name = ?1, description = ?2, template_sql = ?3, default_params = ?4, tags = ?5, template_type = ?6, updated_at = ?7 WHERE id = ?8",
            params![
                template.name,
                template.description,
                template.template_sql,
                template.default_params,
                template.tags,
                template.template_type.as_str(),
                now,
                template.id.to_string()
            ],
        )?;

        Ok(())
    }

    /// Delete a template
    pub fn delete_template(&self, template_id: Uuid) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM templates WHERE id = ?1",
            params![template_id.to_string()],
        )?;

        Ok(())
    }

    /// Search templates by name or tags
    pub fn search_templates(&self, query: &str) -> Result<Vec<SavedTemplate>> {
        // Ensure schema exists
        self.initialize_templates_schema()?;

        let conn = self.connect()?;
        let search_pattern = format!("%{}%", query.to_lowercase());

        let mut stmt = conn.prepare(
            "SELECT id, name, description, template_sql, default_params, tags, template_type, created_at, updated_at 
             FROM templates 
             WHERE LOWER(name) LIKE ?1 OR LOWER(tags) LIKE ?1 OR LOWER(description) LIKE ?1
             ORDER BY name ASC",
        )?;

        let templates = stmt
            .query_map(params![search_pattern], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let name: String = row.get(1)?;
                let description: String = row.get(2)?;
                let template_sql: String = row.get(3)?;
                let default_params: String = row.get(4)?;
                let tags: String = row.get(5)?;
                let template_type_str: String = row.get(6)?;
                let template_type = TemplateType::from_str(&template_type_str);

                let created_at_str: String = row.get(7)?;
                let created_at = DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                let updated_at_str: String = row.get(8)?;
                let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                Ok(SavedTemplate {
                    id,
                    name,
                    description,
                    template_sql,
                    default_params,
                    tags,
                    template_type,
                    created_at,
                    updated_at,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(templates)
    }

    /// Get all unique tags from templates
    pub fn get_template_tags(&self) -> Result<Vec<String>> {
        // Ensure schema exists
        self.initialize_templates_schema()?;

        let conn = self.connect()?;

        let mut stmt = conn.prepare("SELECT DISTINCT tags FROM templates WHERE tags != ''")?;

        let mut all_tags: Vec<String> = Vec::new();
        let rows = stmt.query_map([], |row| {
            let tags: String = row.get(0)?;
            Ok(tags)
        })?;

        for result in rows {
            let tags_str = result?;
            for tag in tags_str.split(',') {
                let tag = tag.trim().to_string();
                if !tag.is_empty() && !all_tags.contains(&tag) {
                    all_tags.push(tag);
                }
            }
        }

        all_tags.sort();
        Ok(all_tags)
    }

    // ========================================================================
    // DBT-Style Project Storage
    // ========================================================================

    /// Initialize the projects schema (call in initialize_schema)
    pub fn initialize_projects_schema(&self) -> Result<()> {
        let conn = self.connect()?;

        // Projects table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                connection_id TEXT,
                default_schema TEXT NOT NULL DEFAULT 'public',
                default_database TEXT,
                quoting_json TEXT NOT NULL DEFAULT '{}',
                vars_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (connection_id) REFERENCES connections(id) ON DELETE SET NULL
            )",
            [],
        )?;

        // Sources table (linked to projects)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS project_sources (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                database TEXT,
                schema TEXT NOT NULL,
                tables_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
            )",
            [],
        )?;

        // Models table (linked to projects)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS project_models (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                sql TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}',
                vars_json TEXT NOT NULL DEFAULT '{}',
                tags_json TEXT NOT NULL DEFAULT '[]',
                depends_on_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
                UNIQUE (project_id, name)
            )",
            [],
        )?;

        // Indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_project_sources_project ON project_sources(project_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_project_models_project ON project_models(project_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_project_models_name ON project_models(name)",
            [],
        )?;

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Project CRUD
    // ------------------------------------------------------------------------

    /// Save a project to the database
    pub fn save_project(&self, project: &Project) -> Result<()> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let quoting_json = serde_json::to_string(&project.quoting)?;
        let vars_json = serde_json::to_string(&project.vars)?;

        conn.execute(
            "INSERT OR REPLACE INTO projects (id, name, description, connection_id, default_schema, default_database, quoting_json, vars_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                project.id.to_string(),
                project.name,
                project.description,
                project.connection_id.map(|id| id.to_string()),
                project.default_schema,
                project.default_database,
                quoting_json,
                vars_json,
                project.created_at.to_rfc3339(),
                project.updated_at.to_rfc3339(),
            ],
        )?;

        // Save sources (delete and re-insert for simplicity)
        conn.execute(
            "DELETE FROM project_sources WHERE project_id = ?1",
            params![project.id.to_string()],
        )?;

        for source in &project.sources {
            self.save_source_internal(&conn, &project.id, source)?;
        }

        Ok(())
    }

    /// Internal helper to save a source definition
    fn save_source_internal(
        &self,
        conn: &Connection,
        project_id: &Uuid,
        source: &SourceDefinition,
    ) -> Result<()> {
        let tables_json = serde_json::to_string(&source.tables)?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO project_sources (id, project_id, name, description, database, schema, tables_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                source.id.to_string(),
                project_id.to_string(),
                source.name,
                source.description,
                source.database,
                source.schema,
                tables_json,
                now,
                now,
            ],
        )?;

        Ok(())
    }

    /// Load all projects
    pub fn load_projects(&self) -> Result<Vec<Project>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, description, connection_id, default_schema, default_database, quoting_json, vars_json, created_at, updated_at
             FROM projects
             ORDER BY name ASC",
        )?;

        let projects = stmt
            .query_map([], |row| self.row_to_project(row))?
            .collect::<Result<Vec<_>, _>>()?;

        // Load sources for each project
        let mut projects_with_sources = Vec::new();
        for mut project in projects {
            project.sources = self.load_sources_for_project(project.id)?;
            projects_with_sources.push(project);
        }

        Ok(projects_with_sources)
    }

    /// Load a single project by ID
    pub fn load_project(&self, project_id: Uuid) -> Result<Option<Project>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, description, connection_id, default_schema, default_database, quoting_json, vars_json, created_at, updated_at
             FROM projects
             WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![project_id.to_string()], |row| {
            self.row_to_project(row)
        });

        match result {
            Ok(mut project) => {
                project.sources = self.load_sources_for_project(project.id)?;
                Ok(Some(project))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Helper to convert a database row to a Project
    fn row_to_project(&self, row: &rusqlite::Row) -> rusqlite::Result<Project> {
        let id_str: String = row.get(0)?;
        let id = Uuid::parse_str(&id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let name: String = row.get(1)?;
        let description: String = row.get(2)?;

        let connection_id_str: Option<String> = row.get(3)?;
        let connection_id = connection_id_str.and_then(|s| Uuid::parse_str(&s).ok());

        let default_schema: String = row.get(4)?;
        let default_database: Option<String> = row.get(5)?;

        let quoting_json: String = row.get(6)?;
        let quoting: QuotingConfig =
            serde_json::from_str(&quoting_json).unwrap_or_else(|_| QuotingConfig::all_quoted());

        let vars_json: String = row.get(7)?;
        let vars: HashMap<String, serde_json::Value> =
            serde_json::from_str(&vars_json).unwrap_or_default();

        let created_at_str: String = row.get(8)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        let updated_at_str: String = row.get(9)?;
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Project {
            id,
            name,
            description,
            connection_id,
            default_schema,
            default_database,
            quoting,
            vars,
            sources: Vec::new(), // Loaded separately
            created_at,
            updated_at,
        })
    }

    /// Load sources for a project
    fn load_sources_for_project(&self, project_id: Uuid) -> Result<Vec<SourceDefinition>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, description, database, schema, tables_json
             FROM project_sources
             WHERE project_id = ?1
             ORDER BY name ASC",
        )?;

        let sources = stmt
            .query_map(params![project_id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let id = Uuid::parse_str(&id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?;

                let name: String = row.get(1)?;
                let description: String = row.get(2)?;
                let database: Option<String> = row.get(3)?;
                let schema: String = row.get(4)?;

                let tables_json: String = row.get(5)?;
                let tables: Vec<SourceTable> =
                    serde_json::from_str(&tables_json).unwrap_or_default();

                Ok(SourceDefinition {
                    id,
                    name,
                    description,
                    database,
                    schema,
                    tables,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(sources)
    }

    /// Update a project
    pub fn update_project(&self, project: &Project) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        let quoting_json = serde_json::to_string(&project.quoting)?;
        let vars_json = serde_json::to_string(&project.vars)?;

        conn.execute(
            "UPDATE projects SET name = ?1, description = ?2, connection_id = ?3, default_schema = ?4, default_database = ?5, quoting_json = ?6, vars_json = ?7, updated_at = ?8 WHERE id = ?9",
            params![
                project.name,
                project.description,
                project.connection_id.map(|id| id.to_string()),
                project.default_schema,
                project.default_database,
                quoting_json,
                vars_json,
                now,
                project.id.to_string()
            ],
        )?;

        // Update sources (delete and re-insert)
        conn.execute(
            "DELETE FROM project_sources WHERE project_id = ?1",
            params![project.id.to_string()],
        )?;

        for source in &project.sources {
            self.save_source_internal(&conn, &project.id, source)?;
        }

        Ok(())
    }

    /// Delete a project (cascades to models and sources)
    pub fn delete_project(&self, project_id: Uuid) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM projects WHERE id = ?1",
            params![project_id.to_string()],
        )?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Model CRUD
    // ------------------------------------------------------------------------

    /// Save a model to the database
    pub fn save_model(&self, model: &Model) -> Result<()> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let config_json = serde_json::to_string(&model.config)?;
        let vars_json = serde_json::to_string(&model.vars)?;
        let tags_json = serde_json::to_string(&model.tags)?;
        let depends_on_json = serde_json::to_string(&model.depends_on)?;

        conn.execute(
            "INSERT OR REPLACE INTO project_models (id, project_id, name, description, sql, config_json, vars_json, tags_json, depends_on_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                model.id.to_string(),
                model.project_id.to_string(),
                model.name,
                model.description,
                model.sql,
                config_json,
                vars_json,
                tags_json,
                depends_on_json,
                model.created_at.to_rfc3339(),
                model.updated_at.to_rfc3339(),
            ],
        )?;

        Ok(())
    }

    /// Load all models for a project
    pub fn load_models_for_project(&self, project_id: Uuid) -> Result<Vec<Model>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, project_id, name, description, sql, config_json, vars_json, tags_json, depends_on_json, created_at, updated_at
             FROM project_models
             WHERE project_id = ?1
             ORDER BY name ASC",
        )?;

        let models = stmt
            .query_map(params![project_id.to_string()], |row| {
                self.row_to_model(row)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(models)
    }

    /// Load a single model by ID
    pub fn load_model(&self, model_id: Uuid) -> Result<Option<Model>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, project_id, name, description, sql, config_json, vars_json, tags_json, depends_on_json, created_at, updated_at
             FROM project_models
             WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![model_id.to_string()], |row| self.row_to_model(row));

        match result {
            Ok(model) => Ok(Some(model)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Load a model by name within a project
    pub fn load_model_by_name(&self, project_id: Uuid, name: &str) -> Result<Option<Model>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, project_id, name, description, sql, config_json, vars_json, tags_json, depends_on_json, created_at, updated_at
             FROM project_models
             WHERE project_id = ?1 AND name = ?2",
        )?;

        let result = stmt.query_row(params![project_id.to_string(), name], |row| {
            self.row_to_model(row)
        });

        match result {
            Ok(model) => Ok(Some(model)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Helper to convert a database row to a Model
    fn row_to_model(&self, row: &rusqlite::Row) -> rusqlite::Result<Model> {
        let id_str: String = row.get(0)?;
        let id = Uuid::parse_str(&id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let project_id_str: String = row.get(1)?;
        let project_id = Uuid::parse_str(&project_id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let name: String = row.get(2)?;
        let description: String = row.get(3)?;
        let sql: String = row.get(4)?;

        let config_json: String = row.get(5)?;
        let config: ModelConfig = serde_json::from_str(&config_json).unwrap_or_default();

        let vars_json: String = row.get(6)?;
        let vars: HashMap<String, serde_json::Value> =
            serde_json::from_str(&vars_json).unwrap_or_default();

        let tags_json: String = row.get(7)?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();

        let depends_on_json: String = row.get(8)?;
        let depends_on: Vec<ModelDependency> =
            serde_json::from_str(&depends_on_json).unwrap_or_default();

        let created_at_str: String = row.get(9)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        let updated_at_str: String = row.get(10)?;
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(Model {
            id,
            project_id,
            name,
            description,
            sql,
            config,
            vars,
            tags,
            depends_on,
            created_at,
            updated_at,
        })
    }

    /// Update a model
    pub fn update_model(&self, model: &Model) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        let config_json = serde_json::to_string(&model.config)?;
        let vars_json = serde_json::to_string(&model.vars)?;
        let tags_json = serde_json::to_string(&model.tags)?;
        let depends_on_json = serde_json::to_string(&model.depends_on)?;

        conn.execute(
            "UPDATE project_models SET name = ?1, description = ?2, sql = ?3, config_json = ?4, vars_json = ?5, tags_json = ?6, depends_on_json = ?7, updated_at = ?8 WHERE id = ?9",
            params![
                model.name,
                model.description,
                model.sql,
                config_json,
                vars_json,
                tags_json,
                depends_on_json,
                now,
                model.id.to_string()
            ],
        )?;

        Ok(())
    }

    /// Update only the SQL content of a model
    pub fn update_model_sql(&self, model_id: Uuid, sql: &str) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "UPDATE project_models SET sql = ?1, updated_at = ?2 WHERE id = ?3",
            params![sql, now, model_id.to_string()],
        )?;

        Ok(())
    }

    /// Delete a model
    pub fn delete_model(&self, model_id: Uuid) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM project_models WHERE id = ?1",
            params![model_id.to_string()],
        )?;
        Ok(())
    }

    /// Check if a model name already exists in a project
    pub fn model_name_exists(&self, project_id: Uuid, name: &str) -> Result<bool> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;

        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM project_models WHERE project_id = ?1 AND name = ?2")?;

        let count: i64 = stmt.query_row(params![project_id.to_string(), name], |row| row.get(0))?;

        Ok(count > 0)
    }

    /// Search models by name or tags across all projects
    pub fn search_models(&self, query: &str) -> Result<Vec<Model>> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;
        let search_pattern = format!("%{}%", query.to_lowercase());

        let mut stmt = conn.prepare(
            "SELECT id, project_id, name, description, sql, config_json, vars_json, tags_json, depends_on_json, created_at, updated_at
             FROM project_models
             WHERE LOWER(name) LIKE ?1 OR LOWER(description) LIKE ?1 OR LOWER(tags_json) LIKE ?1
             ORDER BY name ASC",
        )?;

        let models = stmt
            .query_map(params![search_pattern], |row| self.row_to_model(row))?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(models)
    }

    // ------------------------------------------------------------------------
    // Source CRUD (standalone methods for updating sources)
    // ------------------------------------------------------------------------

    /// Add a source to a project
    pub fn add_source_to_project(&self, project_id: Uuid, source: &SourceDefinition) -> Result<()> {
        self.initialize_projects_schema()?;
        let conn = self.connect()?;
        self.save_source_internal(&conn, &project_id, source)
    }

    /// Update a source definition
    pub fn update_source(&self, project_id: Uuid, source: &SourceDefinition) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();
        let tables_json = serde_json::to_string(&source.tables)?;

        conn.execute(
            "UPDATE project_sources SET name = ?1, description = ?2, database = ?3, schema = ?4, tables_json = ?5, updated_at = ?6 WHERE id = ?7 AND project_id = ?8",
            params![
                source.name,
                source.description,
                source.database,
                source.schema,
                tables_json,
                now,
                source.id.to_string(),
                project_id.to_string()
            ],
        )?;

        Ok(())
    }

    /// Delete a source from a project
    pub fn delete_source(&self, source_id: Uuid) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM project_sources WHERE id = ?1",
            params![source_id.to_string()],
        )?;
        Ok(())
    }
}
