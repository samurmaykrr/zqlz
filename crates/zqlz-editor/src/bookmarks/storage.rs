//! Query bookmarks storage with SQLite backend

use chrono::{DateTime, Utc};
use rusqlite::{Connection, Result as SqliteResult, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

/// A saved query bookmark
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bookmark {
    /// Unique identifier
    pub id: Uuid,
    /// Display name
    pub name: String,
    /// SQL query text
    pub query: String,
    /// Optional description
    pub description: Option<String>,
    /// Associated connection ID (optional)
    pub connection_id: Option<Uuid>,
    /// Tags for organization
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Last modified timestamp
    pub updated_at: DateTime<Utc>,
}

impl Bookmark {
    /// Create a new bookmark
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            query: query.into(),
            description: None,
            connection_id: None,
            tags: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Set the description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the connection ID
    pub fn with_connection(mut self, connection_id: Uuid) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    /// Add tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

/// SQLite-backed bookmark storage
pub struct BookmarkStorage {
    conn: Connection,
}

impl BookmarkStorage {
    /// Open or create a bookmark database at the given path
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Create an in-memory bookmark database (for testing)
    pub fn in_memory() -> SqliteResult<Self> {
        let conn = Connection::open_in_memory()?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    fn init_schema(&self) -> SqliteResult<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS bookmarks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                query TEXT NOT NULL,
                description TEXT,
                connection_id TEXT,
                tags TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bookmarks_name ON bookmarks(name);
            CREATE INDEX IF NOT EXISTS idx_bookmarks_connection ON bookmarks(connection_id);",
        )
    }

    /// Save a bookmark (insert or update)
    pub fn save(&self, bookmark: &Bookmark) -> SqliteResult<()> {
        let tags_json = serde_json::to_string(&bookmark.tags).unwrap_or_else(|_| "[]".to_string());
        self.conn.execute(
            "INSERT OR REPLACE INTO bookmarks 
             (id, name, query, description, connection_id, tags, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                bookmark.id.to_string(),
                bookmark.name,
                bookmark.query,
                bookmark.description,
                bookmark.connection_id.map(|id| id.to_string()),
                tags_json,
                bookmark.created_at.to_rfc3339(),
                bookmark.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// Get a bookmark by ID
    pub fn get(&self, id: Uuid) -> SqliteResult<Option<Bookmark>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, query, description, connection_id, tags, created_at, updated_at
             FROM bookmarks WHERE id = ?1",
        )?;

        let result = stmt.query_row(params![id.to_string()], |row| Self::row_to_bookmark(row));

        match result {
            Ok(bookmark) => Ok(Some(bookmark)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// List all bookmarks
    pub fn list(&self) -> SqliteResult<Vec<Bookmark>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, query, description, connection_id, tags, created_at, updated_at
             FROM bookmarks ORDER BY updated_at DESC",
        )?;

        let rows = stmt.query_map([], |row| Self::row_to_bookmark(row))?;
        rows.collect()
    }

    /// Search bookmarks by name or query content
    pub fn search(&self, query: &str) -> SqliteResult<Vec<Bookmark>> {
        let pattern = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            "SELECT id, name, query, description, connection_id, tags, created_at, updated_at
             FROM bookmarks WHERE name LIKE ?1 OR query LIKE ?1 ORDER BY updated_at DESC",
        )?;

        let rows = stmt.query_map(params![pattern], |row| Self::row_to_bookmark(row))?;
        rows.collect()
    }

    /// Delete a bookmark
    pub fn delete(&self, id: Uuid) -> SqliteResult<bool> {
        let affected = self.conn.execute(
            "DELETE FROM bookmarks WHERE id = ?1",
            params![id.to_string()],
        )?;
        Ok(affected > 0)
    }

    fn row_to_bookmark(row: &rusqlite::Row) -> rusqlite::Result<Bookmark> {
        let id_str: String = row.get(0)?;
        let tags_json: String = row.get(5)?;
        let created_str: String = row.get(6)?;
        let updated_str: String = row.get(7)?;
        let conn_id_str: Option<String> = row.get(4)?;

        Ok(Bookmark {
            id: Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4()),
            name: row.get(1)?,
            query: row.get(2)?,
            description: row.get(3)?,
            connection_id: conn_id_str.and_then(|s| Uuid::parse_str(&s).ok()),
            tags: serde_json::from_str(&tags_json).unwrap_or_default(),
            created_at: DateTime::parse_from_rfc3339(&created_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            updated_at: DateTime::parse_from_rfc3339(&updated_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_bookmark() {
        let bookmark = Bookmark::new("Test Query", "SELECT * FROM users")
            .with_description("Get all users")
            .with_tags(vec!["users".to_string(), "select".to_string()]);

        assert_eq!(bookmark.name, "Test Query");
        assert_eq!(bookmark.query, "SELECT * FROM users");
        assert_eq!(bookmark.description, Some("Get all users".to_string()));
        assert_eq!(bookmark.tags.len(), 2);
    }

    #[test]
    fn test_storage_crud() {
        let storage = BookmarkStorage::in_memory().unwrap();

        // Create
        let bookmark = Bookmark::new("Test", "SELECT 1");
        storage.save(&bookmark).unwrap();

        // Read
        let retrieved = storage.get(bookmark.id).unwrap().unwrap();
        assert_eq!(retrieved.name, "Test");
        assert_eq!(retrieved.query, "SELECT 1");

        // List
        let all = storage.list().unwrap();
        assert_eq!(all.len(), 1);

        // Delete
        assert!(storage.delete(bookmark.id).unwrap());
        assert!(storage.get(bookmark.id).unwrap().is_none());
    }

    #[test]
    fn test_storage_search() {
        let storage = BookmarkStorage::in_memory().unwrap();

        storage
            .save(&Bookmark::new("Users Query", "SELECT * FROM users"))
            .unwrap();
        storage
            .save(&Bookmark::new("Orders Query", "SELECT * FROM orders"))
            .unwrap();
        storage
            .save(&Bookmark::new("User Count", "SELECT COUNT(*) FROM users"))
            .unwrap();

        let results = storage.search("users").unwrap();
        assert_eq!(results.len(), 2);

        let results = storage.search("orders").unwrap();
        assert_eq!(results.len(), 1);
    }
}
