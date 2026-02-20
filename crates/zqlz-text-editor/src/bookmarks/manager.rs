//! Bookmark manager for CRUD operations and filtering

use super::storage::{Bookmark, BookmarkStorage};
use chrono::Utc;
use rusqlite::Result as SqliteResult;
use std::path::Path;
use uuid::Uuid;

/// Filter options for listing bookmarks
#[derive(Debug, Clone, Default)]
pub struct BookmarkFilter {
    /// Filter by name (substring match)
    pub name: Option<String>,
    /// Filter by connection ID
    pub connection_id: Option<Uuid>,
    /// Filter by tags (any match)
    pub tags: Vec<String>,
    /// Search in query text
    pub query_text: Option<String>,
}

impl BookmarkFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Filter by connection
    pub fn with_connection(mut self, connection_id: Uuid) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    /// Filter by tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Search in query text
    pub fn with_query_text(mut self, text: impl Into<String>) -> Self {
        self.query_text = Some(text.into());
        self
    }
}

/// Manages bookmarks with high-level CRUD operations
pub struct BookmarkManager {
    storage: BookmarkStorage,
}

impl BookmarkManager {
    /// Open a bookmark manager at the given path
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        Ok(Self {
            storage: BookmarkStorage::open(path)?,
        })
    }

    /// Create an in-memory manager for testing
    pub fn in_memory() -> SqliteResult<Self> {
        Ok(Self {
            storage: BookmarkStorage::in_memory()?,
        })
    }

    /// Add a new bookmark
    pub fn add(&self, name: impl Into<String>, query: impl Into<String>) -> SqliteResult<Bookmark> {
        let bookmark = Bookmark::new(name, query);
        self.storage.save(&bookmark)?;
        Ok(bookmark)
    }

    /// Add a bookmark with full options
    pub fn add_full(
        &self,
        name: impl Into<String>,
        query: impl Into<String>,
        description: Option<String>,
        connection_id: Option<Uuid>,
        tags: Vec<String>,
    ) -> SqliteResult<Bookmark> {
        let mut bookmark = Bookmark::new(name, query);
        bookmark.description = description;
        bookmark.connection_id = connection_id;
        bookmark.tags = tags;
        self.storage.save(&bookmark)?;
        Ok(bookmark)
    }

    /// Get a bookmark by ID
    pub fn get(&self, id: Uuid) -> SqliteResult<Option<Bookmark>> {
        self.storage.get(id)
    }

    /// Update an existing bookmark
    pub fn update(&self, mut bookmark: Bookmark) -> SqliteResult<Bookmark> {
        bookmark.updated_at = Utc::now();
        self.storage.save(&bookmark)?;
        Ok(bookmark)
    }

    /// Delete a bookmark
    pub fn delete(&self, id: Uuid) -> SqliteResult<bool> {
        self.storage.delete(id)
    }

    /// List all bookmarks
    pub fn list(&self) -> SqliteResult<Vec<Bookmark>> {
        self.storage.list()
    }

    /// List bookmarks with filter
    pub fn list_filtered(&self, filter: &BookmarkFilter) -> SqliteResult<Vec<Bookmark>> {
        let all = self.storage.list()?;
        Ok(self.apply_filter(all, filter))
    }

    /// Search bookmarks by query
    pub fn search(&self, query: &str) -> SqliteResult<Vec<Bookmark>> {
        self.storage.search(query)
    }

    fn apply_filter(&self, bookmarks: Vec<Bookmark>, filter: &BookmarkFilter) -> Vec<Bookmark> {
        bookmarks
            .into_iter()
            .filter(|b| {
                // Filter by name
                if let Some(name) = &filter.name {
                    if !b.name.to_lowercase().contains(&name.to_lowercase()) {
                        return false;
                    }
                }

                // Filter by connection
                if let Some(conn_id) = filter.connection_id {
                    if b.connection_id != Some(conn_id) {
                        return false;
                    }
                }

                // Filter by tags (any match)
                if !filter.tags.is_empty() {
                    let has_tag = filter.tags.iter().any(|t| b.tags.contains(t));
                    if !has_tag {
                        return false;
                    }
                }

                // Filter by query text
                if let Some(text) = &filter.query_text {
                    if !b.query.to_lowercase().contains(&text.to_lowercase()) {
                        return false;
                    }
                }

                true
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_bookmark() {
        let manager = BookmarkManager::in_memory().unwrap();

        let bookmark = manager.add("Test Query", "SELECT * FROM users").unwrap();

        assert_eq!(bookmark.name, "Test Query");
        assert_eq!(bookmark.query, "SELECT * FROM users");

        // Verify it's persisted
        let retrieved = manager.get(bookmark.id).unwrap().unwrap();
        assert_eq!(retrieved.name, "Test Query");
    }

    #[test]
    fn test_get_bookmark() {
        let manager = BookmarkManager::in_memory().unwrap();

        let bookmark = manager.add("Test", "SELECT 1").unwrap();
        let retrieved = manager.get(bookmark.id).unwrap();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "Test");

        // Non-existent
        let non_existent = manager.get(Uuid::new_v4()).unwrap();
        assert!(non_existent.is_none());
    }

    #[test]
    fn test_list_bookmarks_with_filter() {
        let manager = BookmarkManager::in_memory().unwrap();

        let conn_id = Uuid::new_v4();
        manager
            .add_full(
                "Users Query",
                "SELECT * FROM users",
                None,
                Some(conn_id),
                vec!["users".to_string()],
            )
            .unwrap();
        manager
            .add_full(
                "Orders Query",
                "SELECT * FROM orders",
                None,
                None,
                vec!["orders".to_string()],
            )
            .unwrap();
        manager
            .add_full(
                "User Count",
                "SELECT COUNT(*) FROM users",
                None,
                Some(conn_id),
                vec!["users".to_string(), "count".to_string()],
            )
            .unwrap();

        // Filter by connection
        let filter = BookmarkFilter::new().with_connection(conn_id);
        let results = manager.list_filtered(&filter).unwrap();
        assert_eq!(results.len(), 2);

        // Filter by tag
        let filter = BookmarkFilter::new().with_tags(vec!["users".to_string()]);
        let results = manager.list_filtered(&filter).unwrap();
        assert_eq!(results.len(), 2);

        // Filter by name
        let filter = BookmarkFilter::new().with_name("Query");
        let results = manager.list_filtered(&filter).unwrap();
        assert_eq!(results.len(), 2);

        // Filter by query text
        let filter = BookmarkFilter::new().with_query_text("COUNT");
        let results = manager.list_filtered(&filter).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_delete_bookmark() {
        let manager = BookmarkManager::in_memory().unwrap();

        let bookmark = manager.add("To Delete", "SELECT 1").unwrap();
        let id = bookmark.id;

        assert!(manager.get(id).unwrap().is_some());
        assert!(manager.delete(id).unwrap());
        assert!(manager.get(id).unwrap().is_none());

        // Delete non-existent
        assert!(!manager.delete(Uuid::new_v4()).unwrap());
    }
}
