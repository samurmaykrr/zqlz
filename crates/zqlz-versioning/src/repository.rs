//! Version repository for storing and managing object version history
//!
//! This module provides the main API for version control operations,
//! backed by persistent SQLite storage.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::storage::{
    TrackedObject, VersionStorage, VersionTag, VersionedObjectInfo, make_object_id,
};
use crate::{DatabaseObjectType, DiffEngine};

/// A version entry for a database object
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionEntry {
    /// Unique version ID
    pub id: Uuid,

    /// Connection this version belongs to
    pub connection_id: Uuid,

    /// Full object identifier (e.g., "public.my_function")
    pub object_id: String,

    /// Object type (procedure, view, trigger, etc.)
    pub object_type: DatabaseObjectType,

    /// Schema name (if applicable)
    pub object_schema: Option<String>,

    /// Object name
    pub object_name: String,

    /// The content at this version
    pub content: String,

    /// Commit message describing the change
    pub message: String,

    /// Author (if known)
    pub author: Option<String>,

    /// Timestamp when this version was created
    pub created_at: DateTime<Utc>,

    /// Parent version ID (None for initial version)
    pub parent_id: Option<Uuid>,
}

impl VersionEntry {
    /// Create a new version entry
    pub fn new(
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<String>,
        object_name: String,
        content: String,
        message: String,
        parent_id: Option<Uuid>,
    ) -> Self {
        let object_id = make_object_id(object_schema.as_deref(), &object_name);

        Self {
            id: Uuid::new_v4(),
            connection_id,
            object_id,
            object_type,
            object_schema,
            object_name,
            content,
            message,
            author: None,
            created_at: Utc::now(),
            parent_id,
        }
    }

    /// Create a new version entry with a specific author
    pub fn with_author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    /// Get a short hash of the version ID (for display)
    pub fn short_id(&self) -> String {
        self.id.to_string()[..8].to_string()
    }
}

/// Repository for managing version history with persistent storage
pub struct VersionRepository {
    storage: Arc<VersionStorage>,
}

impl VersionRepository {
    /// Create a new repository with default storage
    pub fn new() -> Result<Self> {
        Ok(Self {
            storage: Arc::new(VersionStorage::new()?),
        })
    }

    /// Create a repository with custom storage
    pub fn with_storage(storage: Arc<VersionStorage>) -> Self {
        Self { storage }
    }

    /// Get a reference to the underlying storage
    pub fn storage(&self) -> &VersionStorage {
        &self.storage
    }

    /// Commit a new version of an object
    ///
    /// This creates a new version entry and persists it.
    /// If there's an existing version, it will be set as the parent.
    pub fn commit(
        &self,
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<String>,
        object_name: String,
        content: String,
        message: String,
    ) -> Result<VersionEntry> {
        let object_id = make_object_id(object_schema.as_deref(), &object_name);

        // Get the latest version to set as parent
        let parent_id = self
            .storage
            .get_latest_version(connection_id, &object_id)?
            .map(|v| v.id);

        let entry = VersionEntry::new(
            connection_id,
            object_type,
            object_schema,
            object_name,
            content,
            message,
            parent_id,
        );

        self.storage.save_version(&entry)?;

        Ok(entry)
    }

    /// Commit a new version with author information
    pub fn commit_with_author(
        &self,
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<String>,
        object_name: String,
        content: String,
        message: String,
        author: String,
    ) -> Result<VersionEntry> {
        let object_id = make_object_id(object_schema.as_deref(), &object_name);

        let parent_id = self
            .storage
            .get_latest_version(connection_id, &object_id)?
            .map(|v| v.id);

        let entry = VersionEntry::new(
            connection_id,
            object_type,
            object_schema,
            object_name,
            content,
            message,
            parent_id,
        )
        .with_author(author);

        self.storage.save_version(&entry)?;

        Ok(entry)
    }

    /// Get all versions for an object
    pub fn get_versions(&self, connection_id: Uuid, object_id: &str) -> Result<Vec<VersionEntry>> {
        self.storage
            .get_versions_for_object(connection_id, object_id)
    }

    /// Get the latest version of an object
    pub fn get_latest(&self, connection_id: Uuid, object_id: &str) -> Result<Option<VersionEntry>> {
        self.storage.get_latest_version(connection_id, object_id)
    }

    /// Get a specific version by ID
    pub fn get_version(&self, version_id: Uuid) -> Result<Option<VersionEntry>> {
        self.storage.get_version(version_id)
    }

    /// Get the version history chain starting from a version
    pub fn get_history(&self, version_id: Uuid, limit: usize) -> Result<Vec<VersionEntry>> {
        self.storage.get_version_history(version_id, limit)
    }

    /// Compare two versions and return a diff
    pub fn diff(&self, from_version_id: Uuid, to_version_id: Uuid) -> Result<VersionDiff> {
        let from = self
            .storage
            .get_version(from_version_id)?
            .ok_or_else(|| anyhow::anyhow!("From version not found"))?;

        let to = self
            .storage
            .get_version(to_version_id)?
            .ok_or_else(|| anyhow::anyhow!("To version not found"))?;

        let unified = DiffEngine::unified_diff(&from.content, &to.content, 3);
        let changes = DiffEngine::changes(&from.content, &to.content);
        let is_identical = DiffEngine::is_identical(&from.content, &to.content);

        Ok(VersionDiff {
            from_version: from,
            to_version: to,
            unified_diff: unified,
            changes,
            is_identical,
        })
    }

    /// Compare a version with the previous version
    pub fn diff_with_parent(&self, version_id: Uuid) -> Result<Option<VersionDiff>> {
        let version = self
            .storage
            .get_version(version_id)?
            .ok_or_else(|| anyhow::anyhow!("Version not found"))?;

        match version.parent_id {
            Some(parent_id) => Ok(Some(self.diff(parent_id, version_id)?)),
            None => Ok(None),
        }
    }

    /// Compare current content with the latest saved version
    pub fn diff_with_current(
        &self,
        connection_id: Uuid,
        object_id: &str,
        current_content: &str,
    ) -> Result<Option<CurrentDiff>> {
        let latest = match self.storage.get_latest_version(connection_id, object_id)? {
            Some(v) => v,
            None => return Ok(None),
        };

        let unified = DiffEngine::unified_diff(&latest.content, current_content, 3);
        let changes = DiffEngine::changes(&latest.content, current_content);
        let is_identical = DiffEngine::is_identical(&latest.content, current_content);

        Ok(Some(CurrentDiff {
            latest_version: latest,
            current_content: current_content.to_string(),
            unified_diff: unified,
            changes,
            is_modified: !is_identical,
        }))
    }

    /// Tag a version with a name
    pub fn tag(&self, version_id: Uuid, name: &str, description: Option<&str>) -> Result<Uuid> {
        self.storage.add_tag(version_id, name, description)
    }

    /// Remove a tag from a version
    pub fn untag(&self, version_id: Uuid, name: &str) -> Result<()> {
        self.storage.remove_tag(version_id, name)
    }

    /// Get a version by tag name
    pub fn get_by_tag(
        &self,
        connection_id: Uuid,
        object_id: &str,
        tag_name: &str,
    ) -> Result<Option<VersionEntry>> {
        self.storage
            .get_version_by_tag(connection_id, object_id, tag_name)
    }

    /// Get all tags for a version
    pub fn get_tags(&self, version_id: Uuid) -> Result<Vec<VersionTag>> {
        self.storage.get_tags_for_version(version_id)
    }

    /// Start tracking an object for version control
    pub fn track(
        &self,
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<&str>,
        object_name: &str,
    ) -> Result<()> {
        self.storage
            .track_object(connection_id, object_type, object_schema, object_name)
    }

    /// Stop tracking an object
    pub fn untrack(&self, connection_id: Uuid, object_id: &str) -> Result<()> {
        self.storage.untrack_object(connection_id, object_id)
    }

    /// Check if an object is being tracked
    pub fn is_tracked(&self, connection_id: Uuid, object_id: &str) -> Result<bool> {
        self.storage.is_object_tracked(connection_id, object_id)
    }

    /// Get all tracked objects for a connection
    pub fn get_tracked_objects(&self, connection_id: Uuid) -> Result<Vec<TrackedObject>> {
        self.storage.get_tracked_objects(connection_id)
    }

    /// List all objects that have versions
    pub fn list_versioned_objects(&self, connection_id: Uuid) -> Result<Vec<VersionedObjectInfo>> {
        self.storage.list_versioned_objects(connection_id)
    }

    /// Delete all versions for an object
    pub fn delete_object_versions(&self, connection_id: Uuid, object_id: &str) -> Result<()> {
        self.storage
            .delete_versions_for_object(connection_id, object_id)
    }

    /// Delete all versions for a connection
    pub fn delete_connection_versions(&self, connection_id: Uuid) -> Result<()> {
        self.storage.delete_versions_for_connection(connection_id)
    }

    /// Check if there are any changes between current content and the latest version
    pub fn has_changes(
        &self,
        connection_id: Uuid,
        object_id: &str,
        current_content: &str,
    ) -> Result<bool> {
        match self.storage.get_latest_version(connection_id, object_id)? {
            Some(latest) => Ok(!DiffEngine::is_identical(&latest.content, current_content)),
            None => Ok(true), // No version exists, so there are "changes" (new content)
        }
    }
}

impl Default for VersionRepository {
    fn default() -> Self {
        Self::new().expect("Failed to create version repository")
    }
}

/// Result of comparing two versions
#[derive(Clone, Debug)]
pub struct VersionDiff {
    /// The "from" version
    pub from_version: VersionEntry,
    /// The "to" version
    pub to_version: VersionEntry,
    /// Unified diff format string
    pub unified_diff: String,
    /// Individual changes
    pub changes: Vec<crate::diff::Change>,
    /// Whether the versions are identical
    pub is_identical: bool,
}

/// Result of comparing current content with the latest saved version
#[derive(Clone, Debug)]
pub struct CurrentDiff {
    /// The latest saved version
    pub latest_version: VersionEntry,
    /// The current content being compared
    pub current_content: String,
    /// Unified diff format string
    pub unified_diff: String,
    /// Individual changes
    pub changes: Vec<crate::diff::Change>,
    /// Whether there are modifications
    pub is_modified: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::VersionStorage;
    use tempfile::{TempDir, tempdir};

    fn create_test_repo() -> (VersionRepository, TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_versions.db");
        let storage = Arc::new(VersionStorage::with_path(db_path).unwrap());
        (VersionRepository::with_storage(storage), dir)
    }

    #[test]
    fn test_commit_and_retrieve() {
        let (repo, _dir) = create_test_repo();
        let conn_id = Uuid::new_v4();

        let v1 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "CREATE FUNCTION my_func() ...".to_string(),
                "Initial version".to_string(),
            )
            .unwrap();

        let retrieved = repo.get_version(v1.id).unwrap().unwrap();
        assert_eq!(retrieved.object_name, "my_func");
        assert_eq!(retrieved.message, "Initial version");
        assert!(retrieved.parent_id.is_none());
    }

    #[test]
    fn test_version_chain() {
        let (repo, _dir) = create_test_repo();
        let conn_id = Uuid::new_v4();

        let v1 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "v1 content".to_string(),
                "Version 1".to_string(),
            )
            .unwrap();

        let v2 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "v2 content".to_string(),
                "Version 2".to_string(),
            )
            .unwrap();

        assert_eq!(v2.parent_id, Some(v1.id));

        let history = repo.get_history(v2.id, 10).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_diff() {
        let (repo, _dir) = create_test_repo();
        let conn_id = Uuid::new_v4();

        let v1 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "line1\nline2\nline3".to_string(),
                "Version 1".to_string(),
            )
            .unwrap();

        let v2 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "line1\nline2 modified\nline3".to_string(),
                "Version 2".to_string(),
            )
            .unwrap();

        let diff = repo.diff(v1.id, v2.id).unwrap();
        assert!(!diff.is_identical);
        assert!(!diff.unified_diff.is_empty());
    }

    #[test]
    fn test_has_changes() {
        let (repo, _dir) = create_test_repo();
        let conn_id = Uuid::new_v4();

        // No version exists yet
        assert!(
            repo.has_changes(conn_id, "public.my_func", "some content")
                .unwrap()
        );

        repo.commit(
            conn_id,
            DatabaseObjectType::Function,
            Some("public".to_string()),
            "my_func".to_string(),
            "original content".to_string(),
            "Initial".to_string(),
        )
        .unwrap();

        // Same content - no changes
        assert!(
            !repo
                .has_changes(conn_id, "public.my_func", "original content")
                .unwrap()
        );

        // Different content - has changes
        assert!(
            repo.has_changes(conn_id, "public.my_func", "modified content")
                .unwrap()
        );
    }
}
