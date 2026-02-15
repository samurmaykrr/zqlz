// Versioning handlers for MainView
//
// This module handles database object version control operations:
// view history, compare versions, restore versions, and save versions.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    dock::{DockPlacement, PanelView},
    notification::Notification,
};
use zqlz_versioning::{
    DatabaseObjectType, VersionRepository,
    widgets::{DiffViewer, DiffViewerEvent, VersionHistoryPanel, VersionHistoryPanelEvent},
};

use super::MainView;

impl MainView {
    /// Show version history for a database object.
    ///
    /// Opens or updates the version history panel for the specified object.
    pub fn show_version_history(
        &mut self,
        connection_id: Uuid,
        object_id: String,
        object_type: DatabaseObjectType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Showing version history for {} ({:?})",
            object_id,
            object_type
        );

        let repository = self.version_repository.clone();

        // Create or update the version history panel
        if let Some(panel) = &self.version_history_panel {
            // Panel exists, just update it
            panel.update(cx, |panel, cx| {
                panel.set_object(connection_id, object_id, object_type, cx);
            });
        } else {
            // Create new panel
            let panel = cx.new(|cx| {
                let mut panel = VersionHistoryPanel::new(repository, cx);
                panel.set_object(connection_id, object_id, object_type, cx);
                panel
            });

            // Subscribe to panel events
            let subscription = cx.subscribe_in(&panel, window, {
                move |this, _panel, event: &VersionHistoryPanelEvent, window, cx| {
                    this.handle_version_history_event(event.clone(), window, cx);
                }
            });
            self._subscriptions.push(subscription);

            // Add panel to the right dock
            let panel_view: Arc<dyn PanelView> = Arc::new(panel.clone());
            self.dock_area.update(cx, |area, cx| {
                area.add_panel(panel_view, DockPlacement::Right, None, window, cx);
            });

            // Open right dock if not already open
            self.dock_area.update(cx, |area, cx| {
                area.set_dock_open(DockPlacement::Right, true, window, cx);
            });

            self.version_history_panel = Some(panel);
        }
    }

    /// Handle events from the version history panel
    pub(super) fn handle_version_history_event(
        &mut self,
        event: VersionHistoryPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            VersionHistoryPanelEvent::VersionSelected(version) => {
                tracing::debug!("Version selected: {}", version.short_id());
                // Could show version details in the right panel
            }

            VersionHistoryPanelEvent::CompareVersions { from, to } => {
                tracing::info!("Comparing versions: {} -> {}", from, to);
                self.show_diff(from, to, window, cx);
            }

            VersionHistoryPanelEvent::RestoreVersion(version) => {
                tracing::info!("Restoring version: {}", version.short_id());
                self.restore_version(version.id, window, cx);
            }

            VersionHistoryPanelEvent::ViewDiff(version_id) => {
                tracing::info!("Viewing diff for version: {}", version_id);
                self.show_diff_with_parent(version_id, window, cx);
            }

            VersionHistoryPanelEvent::TagVersion(version_id) => {
                tracing::info!("Tagging version: {}", version_id);
                self.tag_version(version_id, window, cx);
            }
        }
    }

    /// Show a diff between two versions
    fn show_diff(
        &mut self,
        from_version_id: Uuid,
        to_version_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let repository = self.version_repository.clone();

        match repository.diff(from_version_id, to_version_id) {
            Ok(diff) => {
                self.open_diff_viewer(diff, window, cx);
            }
            Err(e) => {
                tracing::error!("Failed to generate diff: {}", e);
                window.push_notification(
                    Notification::error(format!("Failed to compare versions: {}", e)),
                    cx,
                );
            }
        }
    }

    /// Show diff between a version and its parent
    fn show_diff_with_parent(
        &mut self,
        version_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let repository = self.version_repository.clone();

        match repository.diff_with_parent(version_id) {
            Ok(Some(diff)) => {
                self.open_diff_viewer(diff, window, cx);
            }
            Ok(None) => {
                window.push_notification(
                    Notification::info(
                        "This is the initial version (no previous version to compare)",
                    ),
                    cx,
                );
            }
            Err(e) => {
                tracing::error!("Failed to generate diff: {}", e);
                window.push_notification(
                    Notification::error(format!("Failed to view diff: {}", e)),
                    cx,
                );
            }
        }
    }

    /// Open or update the diff viewer panel
    fn open_diff_viewer(
        &mut self,
        diff: zqlz_versioning::VersionDiff,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(panel) = &self.diff_viewer_panel {
            // Panel exists, update it
            panel.update(cx, |panel, cx| {
                panel.set_diff(diff, cx);
            });
        } else {
            // Create new panel
            let panel = cx.new(|cx| {
                let mut viewer = DiffViewer::new(cx);
                viewer.set_diff(diff, cx);
                viewer
            });

            // Subscribe to panel events
            let subscription = cx.subscribe_in(&panel, window, {
                move |this, _panel, event: &DiffViewerEvent, window, cx| {
                    this.handle_diff_viewer_event(event.clone(), window, cx);
                }
            });
            self._subscriptions.push(subscription);

            // Add panel to center dock (like a query result)
            let panel_view: Arc<dyn PanelView> = Arc::new(panel.clone());
            self.dock_area.update(cx, |area, cx| {
                area.add_panel(panel_view, DockPlacement::Center, None, window, cx);
            });

            self.diff_viewer_panel = Some(panel);
        }
    }

    /// Handle events from the diff viewer
    pub(super) fn handle_diff_viewer_event(
        &mut self,
        event: DiffViewerEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            DiffViewerEvent::Close => {
                if let Some(panel) = self.diff_viewer_panel.take() {
                    let panel_view: Arc<dyn PanelView> = Arc::new(panel);
                    self.dock_area.update(cx, |area, cx| {
                        area.remove_panel(panel_view, DockPlacement::Center, window, cx);
                    });
                }
            }

            DiffViewerEvent::RestoreFrom(version_id) => {
                self.restore_version(version_id, window, cx);
            }

            DiffViewerEvent::RestoreTo(version_id) => {
                self.restore_version(version_id, window, cx);
            }
        }
    }

    /// Restore a specific version of a database object
    fn restore_version(&mut self, version_id: Uuid, window: &mut Window, cx: &mut Context<Self>) {
        let repository = self.version_repository.clone();

        match repository.get_version(version_id) {
            Ok(Some(version)) => {
                // Copy the content to clipboard for now
                // In a full implementation, this would update the database object
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(version.content.clone()));

                window.push_notification(
                    Notification::success(format!(
                        "Version {} content copied to clipboard. Paste into editor to apply.",
                        version.short_id()
                    )),
                    cx,
                );

                tracing::info!(
                    "Restored version {} for {} (copied to clipboard)",
                    version.short_id(),
                    version.object_id
                );
            }
            Ok(None) => {
                window.push_notification(Notification::error("Version not found"), cx);
            }
            Err(e) => {
                tracing::error!("Failed to restore version: {}", e);
                window.push_notification(
                    Notification::error(format!("Failed to restore version: {}", e)),
                    cx,
                );
            }
        }
    }

    /// Tag a version with a name
    fn tag_version(&mut self, version_id: Uuid, window: &mut Window, cx: &mut Context<Self>) {
        use zqlz_ui::widgets::{
            input::{Input, InputState},
            v_flex,
        };

        let repository = self.version_repository.clone();

        let name_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Tag name (e.g., v1.0)"));
        let name_input_focus = name_input.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let repository = repository.clone();
            let name_input = name_input.clone();

            dialog
                .title("Tag Version")
                .w(px(350.0))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().text_sm().child("Enter a tag name for this version:"))
                        .child(Input::new(&name_input))
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("Tags help identify important versions like releases."),
                        ),
                )
                .on_ok(move |_, _window, cx| {
                    let tag_name = name_input.read(cx).text().to_string().trim().to_string();

                    if tag_name.is_empty() {
                        return false;
                    }

                    match repository.tag(version_id, &tag_name, None) {
                        Ok(_) => {
                            tracing::info!("Tagged version {} as '{}'", version_id, tag_name);
                        }
                        Err(e) => {
                            tracing::error!("Failed to tag version: {}", e);
                        }
                    }

                    true
                })
                .confirm()
        });

        name_input_focus.focus_handle(cx).focus(window, cx);
    }

    /// Save the current content of a database object as a new version.
    ///
    /// This is called when saving a view, function, procedure, or trigger.
    pub fn save_object_version(
        &mut self,
        connection_id: Uuid,
        object_type: DatabaseObjectType,
        object_schema: Option<String>,
        object_name: String,
        content: String,
        message: String,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let repository = self.version_repository.clone();

        match repository.commit(
            connection_id,
            object_type,
            object_schema,
            object_name.clone(),
            content,
            message,
        ) {
            Ok(version) => {
                tracing::info!(
                    "Saved version {} for {} ({})",
                    version.short_id(),
                    object_name,
                    version.object_id
                );

                // Refresh the version history panel if it's open for this object
                if let Some(panel) = &self.version_history_panel {
                    panel.update(cx, |panel, cx| {
                        panel.refresh(cx);
                    });
                }
            }
            Err(e) => {
                tracing::error!("Failed to save version: {}", e);
            }
        }
    }
}
