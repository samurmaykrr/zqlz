//! SQLite persistence for version history
//!
//! Stores version history for database objects (stored procedures, views, triggers, etc.)
//! in a local SQLite database, similar to how saved queries are persisted.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::path::PathBuf;
use uuid::Uuid;

use crate::{DatabaseObjectType, VersionEntry};

/// Storage for version history using SQLite
pub struct VersionStorage {
    db_path: PathBuf,
}

impl VersionStorage {
    /// Create a new version storage instance
    pub fn new() -> Result<Self> {
        let db_path = Self::get_storage_path()?;

        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let storage = Self { db_path };
        storage.initialize_schema()?;

        Ok(storage)
    }

    /// Create storage with a custom path (for testing)
    pub fn with_path(db_path: PathBuf) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let storage = Self { db_path };
        storage.initialize_schema()?;

        Ok(storage)
    }

    fn get_storage_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        let app_dir = config_dir.join("zqlz");
        Ok(app_dir.join("versions.db"))
    }

    fn connect(&self) -> Result<Connection> {
        Connection::open(&self.db_path)
            .with_context(|| format!("Failed to open version database at {:?}", self.db_path))
    }

    fn initialize_schema(&self) -> Result<()> {
        let conn = self.connect()?;

        // Main versions table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS versions (
                id TEXT PRIMARY KEY,
                connection_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                object_type TEXT NOT NULL,
                object_schema TEXT,
                object_name TEXT NOT NULL,
                content TEXT NOT NULL,
                message TEXT NOT NULL,
                author TEXT,
                created_at TEXT NOT NULL,
                parent_id TEXT,
                FOREIGN KEY (parent_id) REFERENCES versions(id)
            )",
            [],
        )?;

        // Index for fast lookup by connection and object
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_versions_connection_object 
             ON versions(connection_id, object_id)",
            [],
        )?;

        // Index for parent lookups (for building history chains)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_versions_parent 
             ON versions(parent_id)",
            [],
        )?;

        // Tags table for named versions (like git tags)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS version_tags (
                id TEXT PRIMARY KEY,
                version_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (version_id) REFERENCES versions(id) ON DELETE CASCADE,
                UNIQUE(version_id, name)
            )",
            [],
        )?;

        // Tracked objects table - which objects are being tracked for a connection
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tracked_objects (
                id TEXT PRIMARY KEY,
                connection_id TEXT NOT NULL,
                object_id TEXT NOT NULL,
                object_type TEXT NOT NULL,
                object_schema TEXT,
                object_name TEXT NOT NULL,
                tracked_at TEXT NOT NULL,
                UNIQUE(connection_id, object_id)
            )",
            [],
        )?;

        Ok(())
    }

    /// Save a new version entry
    pub fn save_version(&self, entry: &VersionEntry) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "INSERT INTO versions (id, connection_id, object_id, object_type, object_schema, object_name, content, message, author, created_at, parent_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                entry.id.to_string(),
                entry.connection_id.to_string(),
                entry.object_id,
                entry.object_type.as_str(),
                entry.object_schema.as_deref(),
                entry.object_name,
                entry.content,
                entry.message,
                entry.author.as_deref(),
                entry.created_at.to_rfc3339(),
                entry.parent_id.map(|id| id.to_string()),
            ],
        )?;

        Ok(())
    }

    /// Get all versions for an object, ordered by creation time (newest first)
    pub fn get_versions_for_object(
        &self,
        connection_id: Uuid,
        object_id: &str,
    ) -> Result<Vec<VersionEntry>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, object_id, object_type, object_schema, object_name, 
                    content, message, author, created_at, parent_id
             FROM versions 
             WHERE connection_id = ?1 AND object_id = ?2
             ORDER BY created_at DESC",
        )?;

        self.query_versions(&mut stmt, params![connection_id.to_string(), object_id])
    }

    /// Get all versions for a connection
    pub fn get_versions_for_connection(&self, connection_id: Uuid) -> Result<Vec<VersionEntry>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, object_id, object_type, object_schema, object_name, 
                    content, message, author, created_at, parent_id
             FROM versions 
             WHERE connection_id = ?1
             ORDER BY created_at DESC",
        )?;

        self.query_versions(&mut stmt, params![connection_id.to_string()])
    }

    /// Get a specific version by ID
    pub fn get_version(&self, version_id: Uuid) -> Result<Option<VersionEntry>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, object_id, object_type, object_schema, object_name, 
                    content, message, author, created_at, parent_id
             FROM versions 
             WHERE id = ?1",
        )?;

        let versions = self.query_versions(&mut stmt, params![version_id.to_string()])?;
        Ok(versions.into_iter().next())
    }

    /// Get the latest version for an object
    pub fn get_latest_version(
        &self,
        connection_id: Uuid,
        object_id: &str,
    ) -> Result<Option<VersionEntry>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, object_id, object_type, object_schema, object_name, 
                    content, message, author, created_at, parent_id
             FROM versions 
             WHERE connection_id = ?1 AND object_id = ?2
             ORDER BY created_at DESC
             LIMIT 1",
        )?;

        let versions =
            self.query_versions(&mut stmt, params![connection_id.to_string(), object_id])?;
        Ok(versions.into_iter().next())
    }

    /// Get version history chain (walk parent_id links)
    pub fn get_version_history(&self, version_id: Uuid, limit: usize) -> Result<Vec<VersionEntry>> {
        let mut history = Vec::new();
        let mut current_id = Some(version_id);

        while let Some(id) = current_id {
            if history.len() >= limit {
                break;
            }

            if let Some(entry) = self.get_version(id)? {
                current_id = entry.parent_id;
                history.push(entry);
            } else {
                break;
            }
        }

        Ok(history)
    }

    /// Delete a version (and orphan its children by setting their parent_id to this version's parent)
    pub fn delete_version(&self, version_id: Uuid) -> Result<()> {
        let conn = self.connect()?;

        // Get the parent of the version we're deleting
        let parent_id: Option<String> = conn
            .query_row(
                "SELECT parent_id FROM versions WHERE id = ?1",
                params![version_id.to_string()],
                |row| row.get(0),
            )
            .ok();

        // Update children to point to the deleted version's parent
        conn.execute(
            "UPDATE versions SET parent_id = ?1 WHERE parent_id = ?2",
            params![parent_id, version_id.to_string()],
        )?;

        // Delete the version
        conn.execute(
            "DELETE FROM versions WHERE id = ?1",
            params![version_id.to_string()],
        )?;

        // Delete associated tags
        conn.execute(
            "DELETE FROM version_tags WHERE version_id = ?1",
            params![version_id.to_string()],
        )?;

        Ok(())
    }

    /// Delete all versions for an object
    pub fn delete_versions_for_object(&self, connection_id: Uuid, object_id: &str) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM versions WHERE connection_id = ?1 AND object_id = ?2",
            params![connection_id.to_string(), object_id],
        )?;

        Ok(())
    }

    /// Delete all versions for a connection
    pub fn delete_versions_for_connection(&self, connection_id: Uuid) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM versions WHERE connection_id = ?1",
            params![connection_id.to_string()],
        )?;

        conn.execute(
            "DELETE FROM tracked_objects WHERE connection_id = ?1",
            params![connection_id.to_string()],
        )?;

        Ok(())
    }

    /// Add a tag to a version
    pub fn add_tag(&self, version_id: Uuid, name: &str, description: Option<&str>) -> Result<Uuid> {
        let conn = self.connect()?;
        let tag_id = Uuid::new_v4();
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO version_tags (id, version_id, name, description, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                tag_id.to_string(),
                version_id.to_string(),
                name,
                description,
                now,
            ],
        )?;

        Ok(tag_id)
    }

    /// Remove a tag from a version
    pub fn remove_tag(&self, version_id: Uuid, name: &str) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM version_tags WHERE version_id = ?1 AND name = ?2",
            params![version_id.to_string(), name],
        )?;

        Ok(())
    }

    /// Get all tags for a version
    pub fn get_tags_for_version(&self, version_id: Uuid) -> Result<Vec<VersionTag>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, version_id, name, description, created_at
             FROM version_tags
             WHERE version_id = ?1
             ORDER BY name ASC",
        )?;

        let tags = stmt
            .query_map(params![version_id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let version_id_str: String = row.get(1)?;

                Ok(VersionTag {
                    id: Uuid::parse_str(&id_str).unwrap_or_default(),
                    version_id: Uuid::parse_str(&version_id_str).unwrap_or_default(),
                    name: row.get(2)?,
                    description: row.get(3)?,
                    created_at: parse_datetime(row.get::<_, String>(4)?),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tags)
    }

    /// Get version by tag name for an object
    pub fn get_version_by_tag(
        &self,
        connection_id: Uuid,
        object_id: &str,
        tag_name: &str,
    ) -> Result<Option<VersionEntry>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT v.id, v.connection_id, v.object_id, v.object_type, v.object_schema, 
                    v.object_name, v.content, v.message, v.author, v.created_at, v.parent_id
             FROM versions v
             INNER JOIN version_tags t ON v.id = t.version_id
             WHERE v.connection_id = ?1 AND v.object_id = ?2 AND t.name = ?3",
        )?;

        let versions = self.query_versions(
            &mut stmt,
            params![connection_id.to_string(), object_id, tag_name],
        )?;
        Ok(versions.into_iter().next())
    }

    /// Track an object for version control
    pub fn track_object(
        &self,
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<&str>,
        object_name: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        let id = Uuid::new_v4();
        let object_id = make_object_id(object_schema, object_name);
        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT OR REPLACE INTO tracked_objects (id, connection_id, object_id, object_type, object_schema, object_name, tracked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id.to_string(),
                connection_id.to_string(),
                object_id,
                object_type.as_str(),
                object_schema,
                object_name,
                now,
            ],
        )?;

        Ok(())
    }

    /// Untrack an object
    pub fn untrack_object(&self, connection_id: Uuid, object_id: &str) -> Result<()> {
        let conn = self.connect()?;

        conn.execute(
            "DELETE FROM tracked_objects WHERE connection_id = ?1 AND object_id = ?2",
            params![connection_id.to_string(), object_id],
        )?;

        Ok(())
    }

    /// Get all tracked objects for a connection
    pub fn get_tracked_objects(&self, connection_id: Uuid) -> Result<Vec<TrackedObject>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT id, connection_id, object_id, object_type, object_schema, object_name, tracked_at
             FROM tracked_objects
             WHERE connection_id = ?1
             ORDER BY object_name ASC",
        )?;

        let objects = stmt
            .query_map(params![connection_id.to_string()], |row| {
                let id_str: String = row.get(0)?;
                let conn_id_str: String = row.get(1)?;
                let object_type_str: String = row.get(3)?;

                Ok(TrackedObject {
                    id: Uuid::parse_str(&id_str).unwrap_or_default(),
                    connection_id: Uuid::parse_str(&conn_id_str).unwrap_or_default(),
                    object_id: row.get(2)?,
                    object_type: DatabaseObjectType::from_str(&object_type_str),
                    object_schema: row.get(4)?,
                    object_name: row.get(5)?,
                    tracked_at: parse_datetime(row.get::<_, String>(6)?),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(objects)
    }

    /// Check if an object is tracked
    pub fn is_object_tracked(&self, connection_id: Uuid, object_id: &str) -> Result<bool> {
        let conn = self.connect()?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tracked_objects WHERE connection_id = ?1 AND object_id = ?2",
            params![connection_id.to_string(), object_id],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    /// List all unique objects that have versions for a connection
    pub fn list_versioned_objects(&self, connection_id: Uuid) -> Result<Vec<VersionedObjectInfo>> {
        let conn = self.connect()?;

        let mut stmt = conn.prepare(
            "SELECT object_id, object_type, object_schema, object_name, 
                    COUNT(*) as version_count,
                    MAX(created_at) as latest_version_at
             FROM versions
             WHERE connection_id = ?1
             GROUP BY object_id, object_type, object_schema, object_name
             ORDER BY object_name ASC",
        )?;

        let objects = stmt
            .query_map(params![connection_id.to_string()], |row| {
                let object_type_str: String = row.get(1)?;

                Ok(VersionedObjectInfo {
                    object_id: row.get(0)?,
                    object_type: DatabaseObjectType::from_str(&object_type_str),
                    object_schema: row.get(2)?,
                    object_name: row.get(3)?,
                    version_count: row.get(4)?,
                    latest_version_at: parse_datetime(row.get::<_, String>(5)?),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(objects)
    }

    fn query_versions(
        &self,
        stmt: &mut rusqlite::Statement,
        params: impl rusqlite::Params,
    ) -> Result<Vec<VersionEntry>> {
        let versions = stmt
            .query_map(params, |row| {
                let id_str: String = row.get(0)?;
                let conn_id_str: String = row.get(1)?;
                let object_type_str: String = row.get(3)?;
                let parent_id_str: Option<String> = row.get(10)?;

                Ok(VersionEntry {
                    id: Uuid::parse_str(&id_str).unwrap_or_default(),
                    connection_id: Uuid::parse_str(&conn_id_str).unwrap_or_default(),
                    object_id: row.get(2)?,
                    object_type: DatabaseObjectType::from_str(&object_type_str),
                    object_schema: row.get(4)?,
                    object_name: row.get(5)?,
                    content: row.get(6)?,
                    message: row.get(7)?,
                    author: row.get(8)?,
                    created_at: parse_datetime(row.get::<_, String>(9)?),
                    parent_id: parent_id_str.and_then(|s| Uuid::parse_str(&s).ok()),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(versions)
    }
}

impl Default for VersionStorage {
    fn default() -> Self {
        Self::new().expect("Failed to initialize version storage")
    }
}

/// A tag on a version (like a git tag)
#[derive(Clone, Debug)]
pub struct VersionTag {
    pub id: Uuid,
    pub version_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// A tracked object (object being version controlled)
#[derive(Clone, Debug)]
pub struct TrackedObject {
    pub id: Uuid,
    pub connection_id: Uuid,
    pub object_id: String,
    pub object_type: DatabaseObjectType,
    pub object_schema: Option<String>,
    pub object_name: String,
    pub tracked_at: DateTime<Utc>,
}

/// Summary info about a versioned object
#[derive(Clone, Debug)]
pub struct VersionedObjectInfo {
    pub object_id: String,
    pub object_type: DatabaseObjectType,
    pub object_schema: Option<String>,
    pub object_name: String,
    pub version_count: i64,
    pub latest_version_at: DateTime<Utc>,
}

/// Create a unique object ID from schema and name
pub fn make_object_id(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", s, name),
        None => name.to_string(),
    }
}

fn parse_datetime(s: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{TempDir, tempdir};

    fn create_test_storage() -> (VersionStorage, TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_versions.db");
        let storage = VersionStorage::with_path(db_path).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_save_and_load_version() {
        let (storage, _dir) = create_test_storage();
        let conn_id = Uuid::new_v4();

        let entry = VersionEntry::new(
            conn_id,
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "my_function".to_string(),
            "CREATE FUNCTION my_function() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;"
                .to_string(),
            "Initial version".to_string(),
            None,
        );

        storage.save_version(&entry).unwrap();

        let loaded = storage.get_version(entry.id).unwrap().unwrap();
        assert_eq!(loaded.id, entry.id);
        assert_eq!(loaded.object_name, "my_function");
        assert_eq!(loaded.message, "Initial version");
    }

    #[test]
    fn test_version_history() {
        let (storage, _dir) = create_test_storage();
        let conn_id = Uuid::new_v4();

        let v1 = VersionEntry::new(
            conn_id,
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "my_function".to_string(),
            "v1 content".to_string(),
            "Version 1".to_string(),
            None,
        );
        storage.save_version(&v1).unwrap();

        let v2 = VersionEntry::new(
            conn_id,
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "my_function".to_string(),
            "v2 content".to_string(),
            "Version 2".to_string(),
            Some(v1.id),
        );
        storage.save_version(&v2).unwrap();

        let history = storage.get_version_history(v2.id, 10).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].message, "Version 2");
        assert_eq!(history[1].message, "Version 1");
    }

    #[test]
    fn test_tags() {
        let (storage, _dir) = create_test_storage();
        let conn_id = Uuid::new_v4();

        let entry = VersionEntry::new(
            conn_id,
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "my_function".to_string(),
            "content".to_string(),
            "Release version".to_string(),
            None,
        );
        storage.save_version(&entry).unwrap();

        storage
            .add_tag(entry.id, "v1.0", Some("First release"))
            .unwrap();

        let tags = storage.get_tags_for_version(entry.id).unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].name, "v1.0");

        let by_tag = storage
            .get_version_by_tag(conn_id, &entry.object_id, "v1.0")
            .unwrap();
        assert!(by_tag.is_some());
        assert_eq!(by_tag.unwrap().id, entry.id);
    }

    #[test]
    fn test_tracked_objects() {
        let (storage, _dir) = create_test_storage();
        let conn_id = Uuid::new_v4();

        storage
            .track_object(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "my_function",
            )
            .unwrap();

        let tracked = storage.get_tracked_objects(conn_id).unwrap();
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].object_name, "my_function");

        assert!(
            storage
                .is_object_tracked(conn_id, "public.my_function")
                .unwrap()
        );
    }
}
