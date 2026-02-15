//! Pull database versions to local repository
//!
//! This module provides functionality to pull database object definitions
//! from the database and create new versions in the local repository when
//! changes are detected.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DatabaseObjectType, VersionRepository};

/// Options for pulling versions from a database
#[derive(Clone, Debug, Default)]
pub struct PullOptions {
    /// Commit message to use when creating new versions
    pub message: Option<String>,
    /// Author to attribute changes to
    pub author: Option<String>,
    /// If true, pull all objects even if not tracked
    pub include_untracked: bool,
    /// If true, auto-track objects that aren't currently tracked
    pub auto_track: bool,
    /// If true, return what would be pulled without committing
    pub dry_run: bool,
}

impl PullOptions {
    /// Create new pull options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the commit message for new versions
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the author for new versions
    pub fn with_author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    /// Include untracked objects in the pull
    pub fn include_untracked(mut self) -> Self {
        self.include_untracked = true;
        self
    }

    /// Auto-track untracked objects when pulling
    pub fn auto_track(mut self) -> Self {
        self.auto_track = true;
        self
    }

    /// Enable dry run mode
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

/// Status of a pull operation for a single object
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullStatus {
    /// New version was created (content changed)
    Created,
    /// Object was skipped (no changes detected)
    Unchanged,
    /// Object was newly tracked (first pull)
    Tracked,
    /// Pull was skipped (dry run)
    Skipped,
    /// Pull failed
    Failed,
}

impl PullStatus {
    /// Check if the pull created a new version
    pub fn is_new_version(&self) -> bool {
        matches!(self, PullStatus::Created | PullStatus::Tracked)
    }

    /// Check if the pull was successful (not failed)
    pub fn is_success(&self) -> bool {
        !matches!(self, PullStatus::Failed)
    }
}

/// Result of a pull operation for a single object
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullResult {
    /// Connection ID
    pub connection_id: Uuid,
    /// Object identifier (schema.name)
    pub object_id: String,
    /// Object type
    pub object_type: DatabaseObjectType,
    /// Status of the pull
    pub status: PullStatus,
    /// New version ID if created
    pub version_id: Option<Uuid>,
    /// Error message if pull failed
    pub error: Option<String>,
    /// Whether the object was already tracked
    pub was_tracked: bool,
}

impl PullResult {
    fn created(
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        version_id: Uuid,
        was_tracked: bool,
    ) -> Self {
        Self {
            connection_id,
            object_id,
            object_type,
            status: if was_tracked {
                PullStatus::Created
            } else {
                PullStatus::Tracked
            },
            version_id: Some(version_id),
            error: None,
            was_tracked,
        }
    }

    fn unchanged(
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        was_tracked: bool,
    ) -> Self {
        Self {
            connection_id,
            object_id,
            object_type,
            status: PullStatus::Unchanged,
            version_id: None,
            error: None,
            was_tracked,
        }
    }

    fn skipped(
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        was_tracked: bool,
    ) -> Self {
        Self {
            connection_id,
            object_id,
            object_type,
            status: PullStatus::Skipped,
            version_id: None,
            error: None,
            was_tracked,
        }
    }

    #[allow(dead_code)]
    fn failed(
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        error: impl Into<String>,
        was_tracked: bool,
    ) -> Self {
        Self {
            connection_id,
            object_id,
            object_type,
            status: PullStatus::Failed,
            version_id: None,
            error: Some(error.into()),
            was_tracked,
        }
    }
}

/// Information about a database object to pull
#[derive(Clone, Debug)]
pub struct DatabaseObject {
    /// Object type (procedure, view, function, etc.)
    pub object_type: DatabaseObjectType,
    /// Schema name (if applicable)
    pub schema: Option<String>,
    /// Object name
    pub name: String,
    /// Object definition/content
    pub content: String,
}

impl DatabaseObject {
    /// Create a new database object
    pub fn new(
        object_type: DatabaseObjectType,
        schema: Option<String>,
        name: String,
        content: String,
    ) -> Self {
        Self {
            object_type,
            schema,
            name,
            content,
        }
    }

    /// Get the full object ID (schema.name or just name)
    pub fn object_id(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }
}

/// Pull a single database object and create a version if changed
///
/// This function compares the current database content with the latest
/// version in the repository and creates a new version if there are changes.
pub fn pull_from_database(
    repo: &VersionRepository,
    connection_id: Uuid,
    object: &DatabaseObject,
    options: &PullOptions,
) -> Result<PullResult> {
    let object_id = object.object_id();

    // Check if object is tracked
    let is_tracked = repo.is_tracked(connection_id, &object_id)?;

    if !is_tracked && !options.include_untracked {
        return Ok(PullResult::unchanged(
            connection_id,
            object_id,
            object.object_type,
            false,
        ));
    }

    // Auto-track if requested and not tracked
    if !is_tracked && options.auto_track && !options.dry_run {
        repo.track(
            connection_id,
            object.object_type,
            object.schema.as_deref(),
            &object.name,
        )
        .context("Failed to auto-track object")?;
    }

    // Check if there are changes
    let has_changes = repo.has_changes(connection_id, &object_id, &object.content)?;

    if !has_changes {
        return Ok(PullResult::unchanged(
            connection_id,
            object_id,
            object.object_type,
            is_tracked,
        ));
    }

    // Dry run - report what would happen
    if options.dry_run {
        return Ok(PullResult::skipped(
            connection_id,
            object_id,
            object.object_type,
            is_tracked,
        ));
    }

    // Create new version
    let message = options
        .message
        .clone()
        .unwrap_or_else(|| "Pulled from database".to_string());

    let version = if let Some(ref author) = options.author {
        repo.commit_with_author(
            connection_id,
            object.object_type,
            object.schema.clone(),
            object.name.clone(),
            object.content.clone(),
            message,
            author.clone(),
        )?
    } else {
        repo.commit(
            connection_id,
            object.object_type,
            object.schema.clone(),
            object.name.clone(),
            object.content.clone(),
            message,
        )?
    };

    Ok(PullResult::created(
        connection_id,
        object_id,
        object.object_type,
        version.id,
        is_tracked,
    ))
}

/// Pull all objects from a database connection
///
/// This retrieves objects from the database using the provided connection
/// and creates new versions for any that have changed.
pub async fn pull_all<C: PullConnection>(
    conn: &C,
    repo: &VersionRepository,
    connection_id: Uuid,
    options: &PullOptions,
) -> Result<Vec<PullResult>> {
    // Get list of objects to pull
    let objects = conn
        .list_objects()
        .await
        .context("Failed to list database objects")?;

    let mut results = Vec::with_capacity(objects.len());

    for object in objects {
        let result = pull_from_database(repo, connection_id, &object, options)?;
        results.push(result);
    }

    Ok(results)
}

/// Aggregate results of a pull_all operation
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PullSummary {
    /// Number of objects with new versions created
    pub created_count: usize,
    /// Number of unchanged objects (skipped)
    pub unchanged_count: usize,
    /// Number of newly tracked objects
    pub tracked_count: usize,
    /// Number of failed pulls
    pub failed_count: usize,
    /// Number of skipped (dry run)
    pub skipped_count: usize,
    /// Total objects processed
    pub total_count: usize,
}

impl PullSummary {
    /// Create a summary from pull results
    pub fn from_results(results: &[PullResult]) -> Self {
        let mut summary = Self {
            total_count: results.len(),
            ..Default::default()
        };

        for result in results {
            match result.status {
                PullStatus::Created => summary.created_count += 1,
                PullStatus::Unchanged => summary.unchanged_count += 1,
                PullStatus::Tracked => summary.tracked_count += 1,
                PullStatus::Failed => summary.failed_count += 1,
                PullStatus::Skipped => summary.skipped_count += 1,
            }
        }

        summary
    }

    /// Check if all pulls succeeded
    pub fn all_succeeded(&self) -> bool {
        self.failed_count == 0
    }

    /// Check if any changes were detected
    pub fn has_changes(&self) -> bool {
        self.created_count > 0 || self.tracked_count > 0
    }

    /// Get count of new versions created (created + tracked)
    pub fn new_versions(&self) -> usize {
        self.created_count + self.tracked_count
    }
}

/// Trait for connections that support pulling object definitions
///
/// This trait abstracts the database connection for pulling versions.
/// It allows the pull module to work with any database driver that
/// implements this interface.
#[async_trait::async_trait]
pub trait PullConnection: Send + Sync {
    /// List all versionable objects in the database
    ///
    /// Returns a list of database objects with their definitions.
    /// Implementations should return procedures, functions, views, triggers, etc.
    async fn list_objects(&self) -> Result<Vec<DatabaseObject>>;

    /// Get the definition of a specific object
    ///
    /// Returns the object definition/content for the specified object.
    async fn get_object_definition(
        &self,
        object_type: DatabaseObjectType,
        schema: Option<&str>,
        name: &str,
    ) -> Result<Option<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::VersionStorage;
    use std::sync::Arc;
    use tempfile::tempdir;

    // Mock connection for testing
    struct MockPullConnection {
        objects: Vec<DatabaseObject>,
    }

    impl MockPullConnection {
        fn new() -> Self {
            Self {
                objects: Vec::new(),
            }
        }

        fn with_objects(objects: Vec<DatabaseObject>) -> Self {
            Self { objects }
        }
    }

    #[async_trait::async_trait]
    impl PullConnection for MockPullConnection {
        async fn list_objects(&self) -> Result<Vec<DatabaseObject>> {
            Ok(self.objects.clone())
        }

        async fn get_object_definition(
            &self,
            object_type: DatabaseObjectType,
            schema: Option<&str>,
            name: &str,
        ) -> Result<Option<String>> {
            let object_id = match schema {
                Some(s) => format!("{}.{}", s, name),
                None => name.to_string(),
            };

            Ok(self
                .objects
                .iter()
                .find(|o| o.object_id() == object_id && o.object_type == object_type)
                .map(|o| o.content.clone()))
        }
    }

    fn create_test_repo() -> (VersionRepository, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_versions.db");
        let storage = Arc::new(VersionStorage::with_path(db_path).unwrap());
        (VersionRepository::with_storage(storage), dir)
    }

    fn create_test_object(name: &str, content: &str) -> DatabaseObject {
        DatabaseObject::new(
            DatabaseObjectType::Function,
            Some("public".to_string()),
            name.to_string(),
            content.to_string(),
        )
    }

    mod pull_options_tests {
        use super::*;

        #[test]
        fn test_default_options() {
            let opts = PullOptions::new();
            assert!(opts.message.is_none());
            assert!(opts.author.is_none());
            assert!(!opts.include_untracked);
            assert!(!opts.auto_track);
            assert!(!opts.dry_run);
        }

        #[test]
        fn test_builder_methods() {
            let opts = PullOptions::new()
                .with_message("Test message")
                .with_author("Test Author")
                .include_untracked()
                .auto_track()
                .dry_run();

            assert_eq!(opts.message, Some("Test message".to_string()));
            assert_eq!(opts.author, Some("Test Author".to_string()));
            assert!(opts.include_untracked);
            assert!(opts.auto_track);
            assert!(opts.dry_run);
        }
    }

    mod pull_status_tests {
        use super::*;

        #[test]
        fn test_is_new_version() {
            assert!(PullStatus::Created.is_new_version());
            assert!(PullStatus::Tracked.is_new_version());
            assert!(!PullStatus::Unchanged.is_new_version());
            assert!(!PullStatus::Skipped.is_new_version());
            assert!(!PullStatus::Failed.is_new_version());
        }

        #[test]
        fn test_is_success() {
            assert!(PullStatus::Created.is_success());
            assert!(PullStatus::Unchanged.is_success());
            assert!(PullStatus::Tracked.is_success());
            assert!(PullStatus::Skipped.is_success());
            assert!(!PullStatus::Failed.is_success());
        }
    }

    mod pull_result_tests {
        use super::*;

        #[test]
        fn test_created_result() {
            let conn_id = Uuid::new_v4();
            let version_id = Uuid::new_v4();
            let result = PullResult::created(
                conn_id,
                "public.test_func".to_string(),
                DatabaseObjectType::Function,
                version_id,
                true,
            );

            assert_eq!(result.connection_id, conn_id);
            assert_eq!(result.object_id, "public.test_func");
            assert_eq!(result.object_type, DatabaseObjectType::Function);
            assert_eq!(result.status, PullStatus::Created);
            assert_eq!(result.version_id, Some(version_id));
            assert!(result.error.is_none());
            assert!(result.was_tracked);
        }

        #[test]
        fn test_tracked_result() {
            let conn_id = Uuid::new_v4();
            let version_id = Uuid::new_v4();
            let result = PullResult::created(
                conn_id,
                "public.test_func".to_string(),
                DatabaseObjectType::Function,
                version_id,
                false, // was not tracked
            );

            assert_eq!(result.status, PullStatus::Tracked);
            assert!(!result.was_tracked);
        }

        #[test]
        fn test_unchanged_result() {
            let conn_id = Uuid::new_v4();
            let result = PullResult::unchanged(
                conn_id,
                "public.test_func".to_string(),
                DatabaseObjectType::Function,
                true,
            );

            assert_eq!(result.status, PullStatus::Unchanged);
            assert!(result.version_id.is_none());
        }

        #[test]
        fn test_failed_result() {
            let conn_id = Uuid::new_v4();
            let result = PullResult::failed(
                conn_id,
                "public.test_func".to_string(),
                DatabaseObjectType::Function,
                "Some error",
                true,
            );

            assert_eq!(result.status, PullStatus::Failed);
            assert_eq!(result.error, Some("Some error".to_string()));
        }
    }

    mod database_object_tests {
        use super::*;

        #[test]
        fn test_object_id_with_schema() {
            let obj = DatabaseObject::new(
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "my_func".to_string(),
                "content".to_string(),
            );

            assert_eq!(obj.object_id(), "public.my_func");
        }

        #[test]
        fn test_object_id_without_schema() {
            let obj = DatabaseObject::new(
                DatabaseObjectType::Function,
                None,
                "my_func".to_string(),
                "content".to_string(),
            );

            assert_eq!(obj.object_id(), "my_func");
        }
    }

    mod pull_from_database_tests {
        use super::*;

        #[test]
        fn test_pull_creates_version_on_change() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            // Track the object first
            repo.track(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "test_func",
            )
            .unwrap();

            // Commit initial version
            repo.commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "test_func".to_string(),
                "original content".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            // Pull with changed content
            let object = create_test_object("test_func", "modified content");
            let opts = PullOptions::new().with_message("Pull update");

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert_eq!(result.status, PullStatus::Created);
            assert!(result.version_id.is_some());

            // Verify new version was created
            let latest = repo
                .get_latest(conn_id, "public.test_func")
                .unwrap()
                .unwrap();
            assert_eq!(latest.content, "modified content");
        }

        #[test]
        fn test_pull_skips_unchanged() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            // Track the object
            repo.track(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "test_func",
            )
            .unwrap();

            // Commit initial version
            repo.commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "test_func".to_string(),
                "same content".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            // Pull with same content
            let object = create_test_object("test_func", "same content");
            let opts = PullOptions::new();

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert_eq!(result.status, PullStatus::Unchanged);
            assert!(result.version_id.is_none());
        }

        #[test]
        fn test_pull_untracked_object_ignored_by_default() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            // Don't track the object
            let object = create_test_object("untracked_func", "some content");
            let opts = PullOptions::new();

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert_eq!(result.status, PullStatus::Unchanged);
            assert!(!result.was_tracked);
        }

        #[test]
        fn test_pull_with_include_untracked() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            let object = create_test_object("untracked_func", "some content");
            let opts = PullOptions::new().include_untracked().auto_track();

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert_eq!(result.status, PullStatus::Tracked);
            assert!(result.version_id.is_some());

            // Verify it's now tracked
            assert!(repo.is_tracked(conn_id, "public.untracked_func").unwrap());
        }

        #[test]
        fn test_pull_dry_run() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            repo.track(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "test_func",
            )
            .unwrap();

            repo.commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "test_func".to_string(),
                "original".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            let object = create_test_object("test_func", "modified");
            let opts = PullOptions::new().dry_run();

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert_eq!(result.status, PullStatus::Skipped);

            // Verify no new version was created
            let latest = repo
                .get_latest(conn_id, "public.test_func")
                .unwrap()
                .unwrap();
            assert_eq!(latest.content, "original");
        }

        #[test]
        fn test_pull_with_author() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            repo.track(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "test_func",
            )
            .unwrap();

            let object = create_test_object("test_func", "content");
            let opts = PullOptions::new()
                .with_author("John Doe")
                .with_message("Pull with author");

            let result = pull_from_database(&repo, conn_id, &object, &opts).unwrap();

            assert!(result.status.is_new_version());

            let version = repo
                .get_version(result.version_id.unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(version.author, Some("John Doe".to_string()));
            assert_eq!(version.message, "Pull with author");
        }
    }

    mod pull_all_tests {
        use super::*;

        #[tokio::test]
        async fn test_pull_all_empty() {
            let (repo, _dir) = create_test_repo();
            let conn = MockPullConnection::new();
            let conn_id = Uuid::new_v4();
            let opts = PullOptions::new();

            let results = pull_all(&conn, &repo, conn_id, &opts).await.unwrap();

            assert!(results.is_empty());
        }

        #[tokio::test]
        async fn test_pull_all_with_objects() {
            let (repo, _dir) = create_test_repo();
            let conn_id = Uuid::new_v4();

            // Track two objects
            repo.track(
                conn_id,
                DatabaseObjectType::Function,
                Some("public"),
                "func1",
            )
            .unwrap();
            repo.track(conn_id, DatabaseObjectType::View, Some("public"), "view1")
                .unwrap();

            // Commit initial versions
            repo.commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "func1".to_string(),
                "original func1".to_string(),
                "Initial".to_string(),
            )
            .unwrap();
            repo.commit(
                conn_id,
                DatabaseObjectType::View,
                Some("public".to_string()),
                "view1".to_string(),
                "original view1".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            // Create mock connection with modified objects
            let conn = MockPullConnection::with_objects(vec![
                DatabaseObject::new(
                    DatabaseObjectType::Function,
                    Some("public".to_string()),
                    "func1".to_string(),
                    "modified func1".to_string(),
                ),
                DatabaseObject::new(
                    DatabaseObjectType::View,
                    Some("public".to_string()),
                    "view1".to_string(),
                    "original view1".to_string(), // unchanged
                ),
            ]);

            let opts = PullOptions::new();
            let results = pull_all(&conn, &repo, conn_id, &opts).await.unwrap();

            assert_eq!(results.len(), 2);

            // func1 should be created (changed)
            assert_eq!(results[0].status, PullStatus::Created);
            // view1 should be unchanged
            assert_eq!(results[1].status, PullStatus::Unchanged);
        }
    }

    mod pull_summary_tests {
        use super::*;

        #[test]
        fn test_from_results() {
            let conn_id = Uuid::new_v4();
            let results = vec![
                PullResult::created(
                    conn_id,
                    "obj1".to_string(),
                    DatabaseObjectType::Function,
                    Uuid::new_v4(),
                    true,
                ),
                PullResult::unchanged(
                    conn_id,
                    "obj2".to_string(),
                    DatabaseObjectType::Function,
                    true,
                ),
                PullResult::created(
                    conn_id,
                    "obj3".to_string(),
                    DatabaseObjectType::Function,
                    Uuid::new_v4(),
                    false, // tracked
                ),
                PullResult::failed(
                    conn_id,
                    "obj4".to_string(),
                    DatabaseObjectType::Function,
                    "error",
                    true,
                ),
            ];

            let summary = PullSummary::from_results(&results);

            assert_eq!(summary.total_count, 4);
            assert_eq!(summary.created_count, 1);
            assert_eq!(summary.unchanged_count, 1);
            assert_eq!(summary.tracked_count, 1);
            assert_eq!(summary.failed_count, 1);
        }

        #[test]
        fn test_all_succeeded() {
            let conn_id = Uuid::new_v4();
            let results = vec![
                PullResult::created(
                    conn_id,
                    "obj1".to_string(),
                    DatabaseObjectType::Function,
                    Uuid::new_v4(),
                    true,
                ),
                PullResult::unchanged(
                    conn_id,
                    "obj2".to_string(),
                    DatabaseObjectType::Function,
                    true,
                ),
            ];

            let summary = PullSummary::from_results(&results);
            assert!(summary.all_succeeded());
        }

        #[test]
        fn test_has_changes() {
            let conn_id = Uuid::new_v4();

            // No changes
            let results1 = vec![PullResult::unchanged(
                conn_id,
                "obj1".to_string(),
                DatabaseObjectType::Function,
                true,
            )];
            assert!(!PullSummary::from_results(&results1).has_changes());

            // With changes
            let results2 = vec![PullResult::created(
                conn_id,
                "obj1".to_string(),
                DatabaseObjectType::Function,
                Uuid::new_v4(),
                true,
            )];
            assert!(PullSummary::from_results(&results2).has_changes());
        }

        #[test]
        fn test_new_versions() {
            let conn_id = Uuid::new_v4();
            let results = vec![
                PullResult::created(
                    conn_id,
                    "obj1".to_string(),
                    DatabaseObjectType::Function,
                    Uuid::new_v4(),
                    true,
                ),
                PullResult::created(
                    conn_id,
                    "obj2".to_string(),
                    DatabaseObjectType::Function,
                    Uuid::new_v4(),
                    false, // tracked
                ),
                PullResult::unchanged(
                    conn_id,
                    "obj3".to_string(),
                    DatabaseObjectType::Function,
                    true,
                ),
            ];

            let summary = PullSummary::from_results(&results);
            assert_eq!(summary.new_versions(), 2);
        }
    }

    mod serialization_tests {
        use super::*;

        #[test]
        fn test_pull_status_serialization() {
            let status = PullStatus::Created;
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, "\"created\"");

            let deserialized: PullStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, PullStatus::Created);
        }

        #[test]
        fn test_pull_result_serialization() {
            let conn_id = Uuid::new_v4();
            let version_id = Uuid::new_v4();
            let result = PullResult::created(
                conn_id,
                "public.test_func".to_string(),
                DatabaseObjectType::Function,
                version_id,
                true,
            );

            let json = serde_json::to_string(&result).unwrap();
            assert!(json.contains("\"created\""));
            assert!(json.contains("\"Function\""));

            let deserialized: PullResult = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.status, PullStatus::Created);
        }

        #[test]
        fn test_pull_summary_serialization() {
            let summary = PullSummary {
                created_count: 5,
                unchanged_count: 10,
                tracked_count: 2,
                failed_count: 1,
                skipped_count: 0,
                total_count: 18,
            };

            let json = serde_json::to_string(&summary).unwrap();
            let deserialized: PullSummary = serde_json::from_str(&json).unwrap();

            assert_eq!(deserialized.created_count, 5);
            assert_eq!(deserialized.total_count, 18);
        }
    }
}
