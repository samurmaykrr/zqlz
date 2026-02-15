//! Import and export version history to/from JSON format
//!
//! Provides functionality to export version history to a portable JSON format
//! and import it back, supporting backup/restore and team collaboration scenarios.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage::VersionTag;
use crate::{DatabaseObjectType, VersionEntry, VersionRepository};

/// Export format for version history
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionExport {
    /// Format version for future compatibility
    pub format_version: u32,
    /// When the export was created
    pub exported_at: DateTime<Utc>,
    /// Optional description of this export
    pub description: Option<String>,
    /// Exported versions
    pub versions: Vec<ExportedVersion>,
    /// Exported tags
    pub tags: Vec<ExportedTag>,
}

impl VersionExport {
    /// Create a new empty export
    pub fn new() -> Self {
        Self {
            format_version: 1,
            exported_at: Utc::now(),
            description: None,
            versions: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Add a description to the export
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Get version count
    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Get tag count
    pub fn tag_count(&self) -> usize {
        self.tags.len()
    }
}

impl Default for VersionExport {
    fn default() -> Self {
        Self::new()
    }
}

/// A version entry in export format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportedVersion {
    pub id: Uuid,
    pub connection_id: Uuid,
    pub object_id: String,
    pub object_type: DatabaseObjectType,
    pub object_schema: Option<String>,
    pub object_name: String,
    pub content: String,
    pub message: String,
    pub author: Option<String>,
    pub created_at: DateTime<Utc>,
    pub parent_id: Option<Uuid>,
}

impl From<VersionEntry> for ExportedVersion {
    fn from(entry: VersionEntry) -> Self {
        Self {
            id: entry.id,
            connection_id: entry.connection_id,
            object_id: entry.object_id,
            object_type: entry.object_type,
            object_schema: entry.object_schema,
            object_name: entry.object_name,
            content: entry.content,
            message: entry.message,
            author: entry.author,
            created_at: entry.created_at,
            parent_id: entry.parent_id,
        }
    }
}

impl From<ExportedVersion> for VersionEntry {
    fn from(exported: ExportedVersion) -> Self {
        Self {
            id: exported.id,
            connection_id: exported.connection_id,
            object_id: exported.object_id,
            object_type: exported.object_type,
            object_schema: exported.object_schema,
            object_name: exported.object_name,
            content: exported.content,
            message: exported.message,
            author: exported.author,
            created_at: exported.created_at,
            parent_id: exported.parent_id,
        }
    }
}

/// A tag in export format
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportedTag {
    pub id: Uuid,
    pub version_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<VersionTag> for ExportedTag {
    fn from(tag: VersionTag) -> Self {
        Self {
            id: tag.id,
            version_id: tag.version_id,
            name: tag.name,
            description: tag.description,
            created_at: tag.created_at,
        }
    }
}

/// Options for import operation
#[derive(Clone, Debug, Default)]
pub struct ImportOptions {
    /// Skip versions that already exist (by ID)
    pub skip_duplicates: bool,
    /// Remap connection IDs to a new connection
    pub remap_connection_id: Option<Uuid>,
}

impl ImportOptions {
    /// Create new import options
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip duplicate versions
    pub fn skip_duplicates(mut self) -> Self {
        self.skip_duplicates = true;
        self
    }

    /// Remap all versions to a different connection
    pub fn remap_to_connection(mut self, connection_id: Uuid) -> Self {
        self.remap_connection_id = Some(connection_id);
        self
    }
}

/// Result of an import operation
#[derive(Clone, Debug, Default)]
pub struct ImportResult {
    /// Number of versions imported
    pub versions_imported: usize,
    /// Number of versions skipped (duplicates)
    pub versions_skipped: usize,
    /// Number of tags imported
    pub tags_imported: usize,
    /// Number of tags skipped
    pub tags_skipped: usize,
}

impl ImportResult {
    /// Total versions processed
    pub fn total_versions(&self) -> usize {
        self.versions_imported + self.versions_skipped
    }

    /// Check if any items were imported
    pub fn has_imports(&self) -> bool {
        self.versions_imported > 0 || self.tags_imported > 0
    }
}

/// Export versions for a connection to JSON format
pub fn export_versions(
    repo: &VersionRepository,
    connection_id: Uuid,
    description: Option<&str>,
) -> Result<VersionExport> {
    let storage = repo.storage();
    let versions = storage.get_versions_for_connection(connection_id)?;

    let mut export = VersionExport::new();
    if let Some(desc) = description {
        export = export.with_description(desc);
    }

    for version in versions {
        let tags = storage.get_tags_for_version(version.id)?;
        for tag in tags {
            export.tags.push(ExportedTag::from(tag));
        }
        export.versions.push(ExportedVersion::from(version));
    }

    Ok(export)
}

/// Export versions to a JSON string
pub fn export_versions_to_json(
    repo: &VersionRepository,
    connection_id: Uuid,
    description: Option<&str>,
) -> Result<String> {
    let export = export_versions(repo, connection_id, description)?;
    let json = serde_json::to_string_pretty(&export)?;
    Ok(json)
}

/// Import versions from a VersionExport structure
pub fn import_versions(
    repo: &VersionRepository,
    export: &VersionExport,
    options: &ImportOptions,
) -> Result<ImportResult> {
    let storage = repo.storage();
    let mut result = ImportResult::default();

    for exported_version in &export.versions {
        let mut entry: VersionEntry = exported_version.clone().into();

        if let Some(new_conn_id) = options.remap_connection_id {
            entry.connection_id = new_conn_id;
        }

        if options.skip_duplicates {
            if storage.get_version(entry.id)?.is_some() {
                result.versions_skipped += 1;
                continue;
            }
        }

        storage.save_version(&entry)?;
        result.versions_imported += 1;
    }

    for exported_tag in &export.tags {
        if options.skip_duplicates {
            let existing_tags = storage.get_tags_for_version(exported_tag.version_id)?;
            if existing_tags.iter().any(|t| t.name == exported_tag.name) {
                result.tags_skipped += 1;
                continue;
            }
        }

        if storage.get_version(exported_tag.version_id)?.is_some() {
            storage.add_tag(
                exported_tag.version_id,
                &exported_tag.name,
                exported_tag.description.as_deref(),
            )?;
            result.tags_imported += 1;
        } else {
            result.tags_skipped += 1;
        }
    }

    Ok(result)
}

/// Import versions from a JSON string
pub fn import_versions_from_json(
    repo: &VersionRepository,
    json: &str,
    options: &ImportOptions,
) -> Result<ImportResult> {
    let export: VersionExport = serde_json::from_str(json)?;
    import_versions(repo, &export, options)
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_export_to_json() {
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

        repo.tag(v1.id, "v1.0", Some("First release")).unwrap();

        let json = export_versions_to_json(&repo, conn_id, Some("Test export")).unwrap();
        assert!(json.contains("my_func"));
        assert!(json.contains("v1.0"));
        assert!(json.contains("Test export"));

        let parsed: VersionExport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.versions.len(), 1);
        assert_eq!(parsed.tags.len(), 1);
        assert_eq!(parsed.format_version, 1);
    }

    #[test]
    fn test_import_from_json() {
        let (repo1, _dir1) = create_test_repo();
        let (repo2, _dir2) = create_test_repo();
        let conn_id = Uuid::new_v4();

        repo1
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "exported_func".to_string(),
                "CREATE FUNCTION exported_func() ...".to_string(),
                "To be exported".to_string(),
            )
            .unwrap();

        let json = export_versions_to_json(&repo1, conn_id, None).unwrap();

        let options = ImportOptions::new();
        let result = import_versions_from_json(&repo2, &json, &options).unwrap();

        assert_eq!(result.versions_imported, 1);
        assert_eq!(result.versions_skipped, 0);

        let imported = repo2.get_latest(conn_id, "public.exported_func").unwrap();
        assert!(imported.is_some());
        assert_eq!(imported.unwrap().object_name, "exported_func");
    }

    #[test]
    fn test_import_merge_skips_duplicates() {
        let (repo, _dir) = create_test_repo();
        let conn_id = Uuid::new_v4();

        let v1 = repo
            .commit(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "dup_func".to_string(),
                "Original content".to_string(),
                "Original version".to_string(),
            )
            .unwrap();

        let json = export_versions_to_json(&repo, conn_id, None).unwrap();

        repo.storage()
            .save_version(&VersionEntry::new(
                conn_id,
                DatabaseObjectType::Function,
                Some("public".to_string()),
                "dup_func".to_string(),
                "Modified content".to_string(),
                "Second version".to_string(),
                Some(v1.id),
            ))
            .unwrap();

        let options = ImportOptions::new().skip_duplicates();
        let result = import_versions_from_json(&repo, &json, &options).unwrap();

        assert_eq!(result.versions_imported, 0);
        assert_eq!(result.versions_skipped, 1);

        let versions = repo.get_versions(conn_id, "public.dup_func").unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn test_version_export_builder() {
        let export = VersionExport::new().with_description("My backup");
        assert_eq!(export.description, Some("My backup".to_string()));
        assert_eq!(export.format_version, 1);
        assert_eq!(export.version_count(), 0);
        assert_eq!(export.tag_count(), 0);
    }

    #[test]
    fn test_import_options_builder() {
        let new_conn_id = Uuid::new_v4();
        let options = ImportOptions::new()
            .skip_duplicates()
            .remap_to_connection(new_conn_id);

        assert!(options.skip_duplicates);
        assert_eq!(options.remap_connection_id, Some(new_conn_id));
    }

    #[test]
    fn test_import_result_methods() {
        let result = ImportResult {
            versions_imported: 5,
            versions_skipped: 2,
            tags_imported: 3,
            tags_skipped: 1,
        };

        assert_eq!(result.total_versions(), 7);
        assert!(result.has_imports());

        let empty_result = ImportResult::default();
        assert!(!empty_result.has_imports());
    }

    #[test]
    fn test_import_with_connection_remap() {
        let (repo1, _dir1) = create_test_repo();
        let (repo2, _dir2) = create_test_repo();
        let original_conn_id = Uuid::new_v4();
        let new_conn_id = Uuid::new_v4();

        repo1
            .commit(
                original_conn_id,
                DatabaseObjectType::View,
                Some("public".to_string()),
                "my_view".to_string(),
                "CREATE VIEW my_view AS SELECT 1".to_string(),
                "Initial view".to_string(),
            )
            .unwrap();

        let json = export_versions_to_json(&repo1, original_conn_id, None).unwrap();

        let options = ImportOptions::new().remap_to_connection(new_conn_id);
        let result = import_versions_from_json(&repo2, &json, &options).unwrap();

        assert_eq!(result.versions_imported, 1);

        let imported = repo2.get_latest(new_conn_id, "public.my_view").unwrap();
        assert!(imported.is_some());
        assert_eq!(imported.unwrap().connection_id, new_conn_id);
    }
}
