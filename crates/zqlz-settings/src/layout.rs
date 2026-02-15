//! Layout persistence for dock panels
//!
//! Provides workspace-specific layout storage and retrieval.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use zqlz_ui::widgets::dock::DockAreaState;

use crate::settings_file::layouts_dir;

/// Current layout version - increment when layout structure changes
pub const LAYOUT_VERSION: usize = 1;

/// Persisted layout data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLayout {
    /// Version for migration purposes
    pub version: usize,
    /// The dock area state
    pub state: DockAreaState,
}

impl PersistedLayout {
    pub fn new(state: DockAreaState) -> Self {
        Self {
            version: LAYOUT_VERSION,
            state,
        }
    }
}

/// Workspace identifier based on connection set or project path
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct WorkspaceId {
    identifier: String,
}

impl WorkspaceId {
    /// Create a workspace ID from connection IDs
    pub fn from_connections(connection_ids: &[uuid::Uuid]) -> Self {
        let mut ids: Vec<_> = connection_ids.iter().map(|id| id.to_string()).collect();
        ids.sort();
        Self {
            identifier: if ids.is_empty() {
                "default".to_string()
            } else {
                ids.join("-")
            },
        }
    }

    /// Create the default workspace ID (for when no connections are active)
    pub fn default_workspace() -> Self {
        Self {
            identifier: "default".to_string(),
        }
    }

    /// Get a hash suitable for use as a filename
    pub fn to_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.identifier.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Get the layout file path for this workspace
    pub fn layout_path(&self) -> Result<PathBuf> {
        layouts_dir().map(|dir| dir.join(format!("{}.json", self.to_hash())))
    }
}

/// Load a layout for a workspace
pub fn load_layout(workspace_id: &WorkspaceId) -> Result<Option<PersistedLayout>> {
    let path = workspace_id.layout_path()?;

    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read layout from {:?}", path))?;

    let layout: PersistedLayout =
        serde_json::from_str(&content).with_context(|| "Failed to parse layout JSON")?;

    // Check version compatibility
    if layout.version != LAYOUT_VERSION {
        tracing::warn!(
            "Layout version mismatch: expected {}, got {}. Using default layout.",
            LAYOUT_VERSION,
            layout.version
        );
        return Ok(None);
    }

    Ok(Some(layout))
}

/// Save a layout for a workspace
pub fn save_layout(workspace_id: &WorkspaceId, state: &DockAreaState) -> Result<()> {
    let path = workspace_id.layout_path()?;

    // Ensure layouts directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create layouts directory: {:?}", parent))?;
    }

    let layout = PersistedLayout::new(state.clone());
    let content = serde_json::to_string_pretty(&layout)?;

    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write layout to {:?}", path))?;

    tracing::debug!("Saved layout to {:?}", path);
    Ok(())
}

/// Delete a layout for a workspace
pub fn delete_layout(workspace_id: &WorkspaceId) -> Result<()> {
    let path = workspace_id.layout_path()?;

    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to delete layout at {:?}", path))?;
    }

    Ok(())
}
