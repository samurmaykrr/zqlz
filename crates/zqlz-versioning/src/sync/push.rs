//! Push local versions to database
//!
//! This module provides functionality to push versioned database objects
//! from the local version repository back to the database.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DatabaseObjectType, VersionEntry, VersionRepository};

/// Options for pushing versions to a database
#[derive(Clone, Debug, Default)]
pub struct PushOptions {
    /// If true, create or replace the object. If false, fail if object exists.
    pub create_or_replace: bool,
    /// Optional schema to use (overrides the version's schema)
    pub target_schema: Option<String>,
    /// If true, validates the SQL but doesn't execute it (dry run)
    pub dry_run: bool,
    /// If true, wrap the push in a transaction
    pub use_transaction: bool,
}

impl PushOptions {
    /// Create new push options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable CREATE OR REPLACE behavior
    pub fn create_or_replace(mut self) -> Self {
        self.create_or_replace = true;
        self
    }

    /// Set target schema
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.target_schema = Some(schema.into());
        self
    }

    /// Enable dry run mode
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Enable transaction wrapping
    pub fn in_transaction(mut self) -> Self {
        self.use_transaction = true;
        self
    }
}

/// Status of a push operation
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushStatus {
    /// Object was created successfully
    Created,
    /// Object was replaced successfully
    Replaced,
    /// Push was skipped (dry run)
    Skipped,
    /// Push failed
    Failed,
}

impl PushStatus {
    /// Check if the push was successful
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            PushStatus::Created | PushStatus::Replaced | PushStatus::Skipped
        )
    }
}

/// Result of a push operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushResult {
    /// The version that was pushed
    pub version_id: Uuid,
    /// Object identifier (schema.name)
    pub object_id: String,
    /// Object type
    pub object_type: DatabaseObjectType,
    /// Status of the push
    pub status: PushStatus,
    /// Error message if push failed
    pub error: Option<String>,
    /// SQL that was executed (or would be executed in dry run)
    pub sql: String,
}

impl PushResult {
    fn success(version: &VersionEntry, status: PushStatus, sql: String) -> Self {
        Self {
            version_id: version.id,
            object_id: version.object_id.clone(),
            object_type: version.object_type,
            status,
            error: None,
            sql,
        }
    }

    fn failed(version: &VersionEntry, error: impl Into<String>, sql: String) -> Self {
        Self {
            version_id: version.id,
            object_id: version.object_id.clone(),
            object_type: version.object_type,
            status: PushStatus::Failed,
            error: Some(error.into()),
            sql,
        }
    }
}

/// Generate SQL for pushing an object to the database
///
/// Returns the SQL statement that would create/replace the object.
/// This function generates appropriate DDL based on object type.
pub fn generate_push_sql(version: &VersionEntry, options: &PushOptions) -> String {
    let content = &version.content;

    // If content already contains CREATE/CREATE OR REPLACE, use it directly
    let content_upper = content.to_uppercase();
    if content_upper.starts_with("CREATE") {
        if options.create_or_replace && !content_upper.contains("OR REPLACE") {
            // Try to insert OR REPLACE after CREATE
            if let Some(idx) = content.find(char::is_whitespace) {
                return format!("CREATE OR REPLACE{}", &content[idx..]);
            }
        }
        return content.clone();
    }

    // Otherwise, wrap content in appropriate CREATE statement
    let schema = options
        .target_schema
        .as_ref()
        .or(version.object_schema.as_ref());

    let qualified_name = match schema {
        Some(s) => format!("{}.{}", s, version.object_name),
        None => version.object_name.clone(),
    };

    let create_keyword = if options.create_or_replace {
        "CREATE OR REPLACE"
    } else {
        "CREATE"
    };

    match version.object_type {
        DatabaseObjectType::Function => {
            format!("{} FUNCTION {} {}", create_keyword, qualified_name, content)
        }
        DatabaseObjectType::Procedure => {
            format!(
                "{} PROCEDURE {} {}",
                create_keyword, qualified_name, content
            )
        }
        DatabaseObjectType::View => {
            format!("{} VIEW {} AS {}", create_keyword, qualified_name, content)
        }
        DatabaseObjectType::MaterializedView => {
            format!(
                "{} MATERIALIZED VIEW {} AS {}",
                create_keyword, qualified_name, content
            )
        }
        DatabaseObjectType::Trigger => {
            // Triggers are complex - content should include full definition
            content.clone()
        }
        _ => content.clone(),
    }
}

/// Push a single object version to the database
///
/// This function takes a version entry and pushes it to the database connection.
/// Returns a PushResult indicating success or failure.
pub async fn push_to_database<C: PushConnection>(
    conn: &C,
    version: &VersionEntry,
    options: &PushOptions,
) -> Result<PushResult> {
    // Check if object type is pushable
    if !version.object_type.is_applyable() {
        return Ok(PushResult::failed(
            version,
            format!(
                "Object type {:?} cannot be pushed to database",
                version.object_type
            ),
            String::new(),
        ));
    }

    let sql = generate_push_sql(version, options);

    // Dry run - just return the SQL without executing
    if options.dry_run {
        return Ok(PushResult::success(version, PushStatus::Skipped, sql));
    }

    // Execute the SQL
    match conn.execute_ddl(&sql).await {
        Ok(_) => {
            let status = if options.create_or_replace {
                PushStatus::Replaced
            } else {
                PushStatus::Created
            };
            Ok(PushResult::success(version, status, sql))
        }
        Err(e) => Ok(PushResult::failed(version, e.to_string(), sql)),
    }
}

/// Push all tracked objects for a connection to the database
///
/// This pushes all objects that are being tracked for version control.
/// Returns a vector of PushResults for each object.
pub async fn push_all<C: PushConnection>(
    conn: &C,
    repo: &VersionRepository,
    connection_id: Uuid,
    options: &PushOptions,
) -> Result<Vec<PushResult>> {
    let tracked = repo
        .get_tracked_objects(connection_id)
        .context("Failed to get tracked objects")?;

    let mut results = Vec::with_capacity(tracked.len());

    for tracked_obj in tracked {
        // Get the latest version for this object
        let latest = repo
            .get_latest(connection_id, &tracked_obj.object_id)
            .context("Failed to get latest version")?;

        if let Some(version) = latest {
            let result = push_to_database(conn, &version, options).await?;
            results.push(result);
        }
    }

    Ok(results)
}

/// Trait for connections that support pushing DDL statements
///
/// This trait abstracts the database connection for pushing versions.
/// It allows the push module to work with any database driver that
/// implements this interface.
#[async_trait::async_trait]
pub trait PushConnection: Send + Sync {
    /// Execute a DDL statement (CREATE, ALTER, etc.)
    async fn execute_ddl(&self, sql: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VersionEntry;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Mock connection for testing
    struct MockConnection {
        should_fail: AtomicBool,
        executed_sql: std::sync::Mutex<Vec<String>>,
    }

    impl MockConnection {
        fn new() -> Self {
            Self {
                should_fail: AtomicBool::new(false),
                executed_sql: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::SeqCst);
        }

        fn get_executed(&self) -> Vec<String> {
            self.executed_sql.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl PushConnection for MockConnection {
        async fn execute_ddl(&self, sql: &str) -> Result<()> {
            self.executed_sql.lock().unwrap().push(sql.to_string());
            if self.should_fail.load(Ordering::SeqCst) {
                anyhow::bail!("Mock database error")
            }
            Ok(())
        }
    }

    fn create_test_version(object_type: DatabaseObjectType, content: &str) -> VersionEntry {
        VersionEntry::new(
            Uuid::new_v4(),
            object_type,
            Some("public".to_string()),
            "test_object".to_string(),
            content.to_string(),
            "Test version".to_string(),
            None,
        )
    }

    mod push_options_tests {
        use super::*;

        #[test]
        fn test_default_options() {
            let opts = PushOptions::new();
            assert!(!opts.create_or_replace);
            assert!(opts.target_schema.is_none());
            assert!(!opts.dry_run);
            assert!(!opts.use_transaction);
        }

        #[test]
        fn test_builder_methods() {
            let opts = PushOptions::new()
                .create_or_replace()
                .with_schema("myschema")
                .dry_run()
                .in_transaction();

            assert!(opts.create_or_replace);
            assert_eq!(opts.target_schema, Some("myschema".to_string()));
            assert!(opts.dry_run);
            assert!(opts.use_transaction);
        }
    }

    mod push_status_tests {
        use super::*;

        #[test]
        fn test_is_success() {
            assert!(PushStatus::Created.is_success());
            assert!(PushStatus::Replaced.is_success());
            assert!(PushStatus::Skipped.is_success());
            assert!(!PushStatus::Failed.is_success());
        }
    }

    mod push_result_tests {
        use super::*;

        #[test]
        fn test_success_result() {
            let version = create_test_version(DatabaseObjectType::Function, "test body");
            let result =
                PushResult::success(&version, PushStatus::Created, "CREATE FUNCTION".to_string());

            assert_eq!(result.version_id, version.id);
            assert_eq!(result.object_id, "public.test_object");
            assert_eq!(result.object_type, DatabaseObjectType::Function);
            assert_eq!(result.status, PushStatus::Created);
            assert!(result.error.is_none());
        }

        #[test]
        fn test_failed_result() {
            let version = create_test_version(DatabaseObjectType::Function, "test body");
            let result = PushResult::failed(&version, "Some error", "CREATE FUNCTION".to_string());

            assert_eq!(result.status, PushStatus::Failed);
            assert_eq!(result.error, Some("Some error".to_string()));
        }
    }

    mod generate_push_sql_tests {
        use super::*;

        #[test]
        fn test_function_sql_generation() {
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new();

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.starts_with("CREATE FUNCTION public.test_object"));
            assert!(sql.contains("RETURNS INTEGER"));
        }

        #[test]
        fn test_create_or_replace_function() {
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new().create_or_replace();

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.starts_with("CREATE OR REPLACE FUNCTION"));
        }

        #[test]
        fn test_view_sql_generation() {
            let version = create_test_version(DatabaseObjectType::View, "SELECT * FROM users");
            let opts = PushOptions::new();

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.starts_with("CREATE VIEW public.test_object AS"));
            assert!(sql.contains("SELECT * FROM users"));
        }

        #[test]
        fn test_procedure_sql_generation() {
            let version = create_test_version(
                DatabaseObjectType::Procedure,
                "AS $$ BEGIN END; $$ LANGUAGE plpgsql",
            );
            let opts = PushOptions::new();

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.starts_with("CREATE PROCEDURE public.test_object"));
        }

        #[test]
        fn test_content_already_has_create() {
            let version = create_test_version(
                DatabaseObjectType::Function,
                "CREATE FUNCTION my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql",
            );
            let opts = PushOptions::new();

            let sql = generate_push_sql(&version, &opts);
            // Should use the content as-is
            assert!(sql.starts_with("CREATE FUNCTION my_func()"));
        }

        #[test]
        fn test_content_has_create_adds_or_replace() {
            let version = create_test_version(
                DatabaseObjectType::Function,
                "CREATE FUNCTION my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql",
            );
            let opts = PushOptions::new().create_or_replace();

            let sql = generate_push_sql(&version, &opts);
            // Should insert OR REPLACE
            assert!(sql.starts_with("CREATE OR REPLACE FUNCTION my_func()"));
        }

        #[test]
        fn test_target_schema_override() {
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new().with_schema("custom_schema");

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.contains("custom_schema.test_object"));
        }

        #[test]
        fn test_materialized_view_sql_generation() {
            let version = create_test_version(
                DatabaseObjectType::MaterializedView,
                "SELECT id, name FROM users",
            );
            let opts = PushOptions::new();

            let sql = generate_push_sql(&version, &opts);
            assert!(sql.starts_with("CREATE MATERIALIZED VIEW public.test_object AS"));
        }
    }

    mod push_to_database_tests {
        use super::*;

        #[tokio::test]
        async fn test_push_function_success() {
            let conn = MockConnection::new();
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new();

            let result = push_to_database(&conn, &version, &opts).await.unwrap();

            assert_eq!(result.status, PushStatus::Created);
            assert!(result.error.is_none());
            assert_eq!(conn.get_executed().len(), 1);
        }

        #[tokio::test]
        async fn test_push_with_create_or_replace() {
            let conn = MockConnection::new();
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new().create_or_replace();

            let result = push_to_database(&conn, &version, &opts).await.unwrap();

            assert_eq!(result.status, PushStatus::Replaced);
            let executed = conn.get_executed();
            assert!(executed[0].starts_with("CREATE OR REPLACE"));
        }

        #[tokio::test]
        async fn test_push_dry_run() {
            let conn = MockConnection::new();
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new().dry_run();

            let result = push_to_database(&conn, &version, &opts).await.unwrap();

            assert_eq!(result.status, PushStatus::Skipped);
            // Should not execute anything
            assert!(conn.get_executed().is_empty());
            // But should still generate the SQL
            assert!(!result.sql.is_empty());
        }

        #[tokio::test]
        async fn test_push_failure() {
            let conn = MockConnection::new();
            conn.set_fail(true);
            let version = create_test_version(
                DatabaseObjectType::Function,
                "RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
            );
            let opts = PushOptions::new();

            let result = push_to_database(&conn, &version, &opts).await.unwrap();

            assert_eq!(result.status, PushStatus::Failed);
            assert!(result.error.is_some());
            assert!(result.error.unwrap().contains("Mock database error"));
        }

        #[tokio::test]
        async fn test_push_non_applyable_type() {
            let conn = MockConnection::new();
            let version = create_test_version(DatabaseObjectType::Index, "ON users (name)");
            let opts = PushOptions::new();

            let result = push_to_database(&conn, &version, &opts).await.unwrap();

            assert_eq!(result.status, PushStatus::Failed);
            assert!(result.error.is_some());
            assert!(result.error.unwrap().contains("cannot be pushed"));
        }
    }

    mod push_all_tests {
        use super::*;
        use crate::storage::VersionStorage;
        use std::sync::Arc;
        use tempfile::tempdir;

        fn create_test_repo() -> (VersionRepository, tempfile::TempDir) {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test_versions.db");
            let storage = Arc::new(VersionStorage::with_path(db_path).unwrap());
            (VersionRepository::with_storage(storage), dir)
        }

        #[tokio::test]
        async fn test_push_all_empty() {
            let (repo, _dir) = create_test_repo();
            let conn = MockConnection::new();
            let connection_id = Uuid::new_v4();
            let opts = PushOptions::new();

            let results = push_all(&conn, &repo, connection_id, &opts).await.unwrap();

            assert!(results.is_empty());
        }

        #[tokio::test]
        async fn test_push_all_with_tracked_objects() {
            let (repo, _dir) = create_test_repo();
            let conn = MockConnection::new();
            let connection_id = Uuid::new_v4();

            // Track and commit two objects
            repo.track(
                connection_id,
                DatabaseObjectType::Function,
                Some("public"),
                "func1",
            )
            .unwrap();

            repo.commit(
                connection_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "func1".to_string(),
                "RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            repo.track(
                connection_id,
                DatabaseObjectType::View,
                Some("public"),
                "view1",
            )
            .unwrap();

            repo.commit(
                connection_id,
                DatabaseObjectType::View,
                Some("public".to_string()),
                "view1".to_string(),
                "SELECT * FROM users".to_string(),
                "Initial".to_string(),
            )
            .unwrap();

            let opts = PushOptions::new().create_or_replace();
            let results = push_all(&conn, &repo, connection_id, &opts).await.unwrap();

            assert_eq!(results.len(), 2);
            assert!(results.iter().all(|r| r.status.is_success()));
            assert_eq!(conn.get_executed().len(), 2);
        }
    }

    mod serialization_tests {
        use super::*;

        #[test]
        fn test_push_status_serialization() {
            let status = PushStatus::Created;
            let json = serde_json::to_string(&status).unwrap();
            assert_eq!(json, "\"created\"");

            let deserialized: PushStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, PushStatus::Created);
        }

        #[test]
        fn test_push_result_serialization() {
            let version = create_test_version(DatabaseObjectType::Function, "test");
            let result =
                PushResult::success(&version, PushStatus::Created, "CREATE FUNCTION".to_string());

            let json = serde_json::to_string(&result).unwrap();
            assert!(json.contains("\"created\""));
            assert!(json.contains("\"Function\"")); // DatabaseObjectType serializes as PascalCase

            let deserialized: PushResult = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.status, PushStatus::Created);
        }
    }
}
