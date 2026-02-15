//! UI widgets for version control
//!
//! Provides GPUI widgets for viewing and managing version history.

mod diff_viewer;
mod version_history_panel;

pub use diff_viewer::{DiffDisplayMode, DiffViewer, DiffViewerEvent};
pub use version_history_panel::{VersionHistoryPanel, VersionHistoryPanelEvent};
