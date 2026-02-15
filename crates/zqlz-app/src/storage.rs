//! Local SQLite storage for app settings and connections
//!
//! This module provides a local SQLite database for storing:
//! - Application settings
//! - Saved connection details (passwords stored securely in system keychain)
//! - Saved queries
//! - Recent files/queries
//! - Workspace layouts
//! - DBT-style projects and models

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;
use zqlz_connection::{SavedConnection, SecureStorage};
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
    db_path: PathBuf,
    /// Secure storage for credentials (system keychain)
    secure_storage: SecureStorage,
}

impl LocalStorage {
    /// Create a new local storage instance
    pub fn new() -> Result<Self> {
        let db_path = Self::get_storage_path()?;

        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let secure_storage = SecureStorage::new()
            .map_err(|e| anyhow::anyhow!("Failed to initialize secure storage: {}", e))?;

        let storage = Self {
            db_path,
            secure_storage,
        };
        storage.initialize_schema()?;

        Ok(storage)
    }

    /// Get the storage database path
    fn get_storage_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;

        let app_dir = config_dir.join("zqlz");
        Ok(app_dir.join("storage.db"))
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

        // Connections table (encrypted sensitive fields)
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
                success INTEGER NOT NULL
            )",
            [],
        )?;

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

        Ok(())
    }

    /// Get a database connection
    fn connect(&self) -> Result<Connection> {
        Connection::open(&self.db_path)
            .with_context(|| format!("Failed to open database at {:?}", self.db_path))
    }

    /// Save a connection (passwords stored securely in system keychain)
    pub fn save_connection(&self, connection: &SavedConnection) -> Result<()> {
        let conn = self.connect()?;
        let now = chrono::Utc::now().to_rfc3339();

        // Extract password from params and store in keychain
        let mut params_without_password = connection.params.clone();
        if let Some(password) = params_without_password.remove("password") {
            if !password.is_empty() {
                self.secure_storage
                    .store_password(connection.id, &password)
                    .map_err(|e| anyhow::anyhow!("Failed to store password securely: {}", e))?;
                tracing::debug!(connection_id = %connection.id, "Password stored in system keychain");
            }
        }

        // Also handle SSH passphrase if present
        if let Some(passphrase) = params_without_password.remove("ssh_passphrase") {
            if !passphrase.is_empty() {
                self.secure_storage
                    .store_ssh_passphrase(connection.id, &passphrase)
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to store SSH passphrase securely: {}", e)
                    })?;
                tracing::debug!(connection_id = %connection.id, "SSH passphrase stored in system keychain");
            }
        }

        // Store params without sensitive data
        let params_json = serde_json::to_string(&params_without_password)?;

        conn.execute(
            "INSERT OR REPLACE INTO connections (id, name, driver, params_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                connection.id.to_string(),
                connection.name,
                connection.driver,
                params_json,
                now,
                now,
            ],
        )?;

        Ok(())
    }

    /// Load all saved connections (passwords retrieved from system keychain)
    pub fn load_connections(&self) -> Result<Vec<SavedConnection>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, driver, params_json FROM connections ORDER BY updated_at DESC",
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
                    folder: None,
                    color: None,
                    created_at: chrono::Utc::now(),
                    modified_at: chrono::Utc::now(),
                    last_connected: None,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        // Migrate any old per-connection keychain entries to the new single-entry format
        // This only triggers on first run after the update and requires one keychain prompt
        let connection_ids: Vec<Uuid> = connections.iter().map(|c| c.id).collect();
        if let Err(e) = self.secure_storage.migrate_legacy_entries(&connection_ids) {
            tracing::warn!("Failed to migrate legacy keychain entries: {}", e);
        }

        // Retrieve passwords from keychain and add to params
        let mut connections_with_passwords = Vec::with_capacity(connections.len());
        for mut connection in connections {
            // Try to get password from keychain
            if let Ok(Some(password)) = self.secure_storage.get_password(connection.id) {
                connection.params.insert("password".to_string(), password);
                tracing::debug!(connection_id = %connection.id, "Password retrieved from system keychain");
            } else {
                // Migration: Check if password exists in params_json (old format)
                // This handles existing connections that haven't been migrated yet
                if let Some(password) = connection.params.get("password").cloned() {
                    if !password.is_empty() {
                        tracing::debug!(
                            connection_id = %connection.id,
                            "Migrating password from SQLite to system keychain"
                        );
                        // Migrate to keychain
                        if let Err(e) = self.secure_storage.store_password(connection.id, &password)
                        {
                            tracing::warn!(
                                connection_id = %connection.id,
                                error = %e,
                                "Failed to migrate password to keychain"
                            );
                        } else {
                            // Remove from SQLite (re-save without password)
                            // We'll do this lazily on next save
                            tracing::debug!(
                                connection_id = %connection.id,
                                "Password migrated to keychain (will be removed from SQLite on next save)"
                            );
                        }
                    }
                }
            }

            // Try to get SSH passphrase from keychain
            if let Ok(Some(passphrase)) = self.secure_storage.get_ssh_passphrase(connection.id) {
                connection
                    .params
                    .insert("ssh_passphrase".to_string(), passphrase);
            }

            connections_with_passwords.push(connection);
        }

        Ok(connections_with_passwords)
    }

    /// Delete a connection (also removes credentials from keychain)
    pub fn delete_connection(&self, id: Uuid) -> Result<()> {
        // Delete credentials from keychain first
        if let Err(e) = self.secure_storage.delete_connection_credentials(id) {
            tracing::warn!(
                connection_id = %id,
                error = %e,
                "Failed to delete credentials from keychain"
            );
        }

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

    /// Add query to history
    pub fn add_query_history(
        &self,
        connection_id: Option<Uuid>,
        query: &str,
        duration_ms: u64,
        row_count: Option<usize>,
        success: bool,
    ) -> Result<()> {
        let conn = self.connect()?;
        let id = Uuid::new_v4();
        let now = chrono::Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO query_history (id, connection_id, query_text, executed_at, duration_ms, row_count, success)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id.to_string(),
                connection_id.map(|id| id.to_string()),
                query,
                now,
                duration_ms as i64,
                row_count.map(|c| c as i64),
                if success { 1 } else { 0 },
            ],
        )?;

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

/// Type of SQL template
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateType {
    /// Plain SQL with simple variable substitution
    #[default]
    PlainSql,
    /// DBT-style template with ref(), source(), var(), config() functions
    DbtModel,
}

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

        // Migration: Add template_type column if it doesn't exist (for existing databases)
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
