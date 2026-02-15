//! Language system initialization for SQL support
//!
//! This module provides basic SQL language configuration for Zed's editor.
//! SQL syntax highlighting is handled by tree-sitter-sequel grammar with
//! Zed's Language system. The syntax theme is applied per-editor in
//! `EditorWrapper::new()`.

use gpui::App;

/// Initializes language system
///
/// Currently a placeholder. The actual SQL language creation and theme
/// application happens in `EditorWrapper::new()` when each editor is created.
///
/// # Arguments
/// * `cx` - The GPUI app context
pub fn init(_cx: &mut App) {
    tracing::info!(
        "Language system initialized (SQL highlighting via Zed editor + tree-sitter-sequel)"
    );
}
