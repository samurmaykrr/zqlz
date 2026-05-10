use anyhow::{Result, anyhow};
use uuid::Uuid;

use crate::{VersionDiff, VersionEntry, VersionRepository};

/// Typed result for diffing a version against its parent.
#[derive(Clone, Debug)]
pub enum DiffWithParentResult {
    /// A parent exists and a diff was generated.
    Diff(Box<VersionDiff>),
    /// The selected version has no parent.
    InitialVersion,
}

/// Resolve a version by id or return a typed not-found error.
pub fn resolve_version_for_restore(
    repository: &VersionRepository,
    version_id: Uuid,
) -> Result<VersionEntry> {
    repository
        .get_version(version_id)?
        .ok_or_else(|| anyhow!("Version not found"))
}

/// Generate a diff between two explicit versions.
pub fn diff_versions(
    repository: &VersionRepository,
    from_version_id: Uuid,
    to_version_id: Uuid,
) -> Result<VersionDiff> {
    repository.diff(from_version_id, to_version_id)
}

/// Generate a diff between a version and its parent.
pub fn diff_version_with_parent(
    repository: &VersionRepository,
    version_id: Uuid,
) -> Result<DiffWithParentResult> {
    match repository.diff_with_parent(version_id)? {
        Some(diff) => Ok(DiffWithParentResult::Diff(Box::new(diff))),
        None => Ok(DiffWithParentResult::InitialVersion),
    }
}

/// Validate and create a tag on a version.
pub fn tag_version(repository: &VersionRepository, version_id: Uuid, tag_name: &str) -> Result<()> {
    let tag_name = tag_name.trim();
    if tag_name.is_empty() {
        anyhow::bail!("Tag name cannot be empty");
    }

    repository.tag(version_id, tag_name, None)?;
    Ok(())
}
