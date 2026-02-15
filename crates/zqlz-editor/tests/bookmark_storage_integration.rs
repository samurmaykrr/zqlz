//! Integration tests for bookmark storage

use std::fs;
use tempfile::tempdir;
use uuid::Uuid;
use zqlz_editor::{Bookmark, BookmarkStorage};

#[test]
fn test_bookmark_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bookmarks.db");

    // Create storage and save a bookmark
    let bookmark_id = {
        let storage = BookmarkStorage::open(&db_path).unwrap();

        let bookmark = Bookmark::new(
            "Persistent Query",
            "SELECT * FROM users WHERE active = true",
        )
        .with_description("Get active users")
        .with_tags(vec!["users".to_string(), "active".to_string()])
        .with_connection(Uuid::new_v4());

        let id = bookmark.id;
        storage.save(&bookmark).unwrap();
        id
    };

    // Open storage again and verify persistence
    {
        let storage = BookmarkStorage::open(&db_path).unwrap();
        let retrieved = storage.get(bookmark_id).unwrap().unwrap();

        assert_eq!(retrieved.name, "Persistent Query");
        assert_eq!(retrieved.query, "SELECT * FROM users WHERE active = true");
        assert_eq!(retrieved.description, Some("Get active users".to_string()));
        assert_eq!(
            retrieved.tags,
            vec!["users".to_string(), "active".to_string()]
        );
        assert!(retrieved.connection_id.is_some());
    }

    // Verify file was created
    assert!(db_path.exists());
}

#[test]
fn test_bookmark_update() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bookmarks.db");
    let storage = BookmarkStorage::open(&db_path).unwrap();

    // Create initial bookmark
    let mut bookmark = Bookmark::new("Original Name", "SELECT 1");
    let id = bookmark.id;
    storage.save(&bookmark).unwrap();

    // Update the bookmark
    bookmark.name = "Updated Name".to_string();
    bookmark.query = "SELECT 2".to_string();
    storage.save(&bookmark).unwrap();

    // Verify update
    let retrieved = storage.get(id).unwrap().unwrap();
    assert_eq!(retrieved.name, "Updated Name");
    assert_eq!(retrieved.query, "SELECT 2");

    // Verify only one bookmark exists
    let all = storage.list().unwrap();
    assert_eq!(all.len(), 1);
}

#[test]
fn test_bookmark_search_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bookmarks.db");

    // Create and save multiple bookmarks
    {
        let storage = BookmarkStorage::open(&db_path).unwrap();
        storage
            .save(&Bookmark::new("Query A", "SELECT * FROM table_a"))
            .unwrap();
        storage
            .save(&Bookmark::new("Query B", "SELECT * FROM table_b"))
            .unwrap();
        storage
            .save(&Bookmark::new("Count A", "SELECT COUNT(*) FROM table_a"))
            .unwrap();
    }

    // Re-open and search
    {
        let storage = BookmarkStorage::open(&db_path).unwrap();

        let results = storage.search("table_a").unwrap();
        assert_eq!(results.len(), 2);

        let all = storage.list().unwrap();
        assert_eq!(all.len(), 3);
    }
}

#[test]
fn test_bookmark_delete_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("bookmarks.db");

    let bookmark_id = {
        let storage = BookmarkStorage::open(&db_path).unwrap();
        let bookmark = Bookmark::new("To Delete", "SELECT 1");
        let id = bookmark.id;
        storage.save(&bookmark).unwrap();
        id
    };

    // Re-open and delete
    {
        let storage = BookmarkStorage::open(&db_path).unwrap();
        assert!(storage.delete(bookmark_id).unwrap());
    }

    // Re-open and verify deletion persisted
    {
        let storage = BookmarkStorage::open(&db_path).unwrap();
        assert!(storage.get(bookmark_id).unwrap().is_none());
        assert!(storage.list().unwrap().is_empty());
    }
}
