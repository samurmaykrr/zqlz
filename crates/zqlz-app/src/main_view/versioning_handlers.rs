// Versioning handlers for MainView
//
// This module handles database object version control operations:
// view history, compare versions, restore versions, and save versions.

use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{Connection, DropViewOptions, SqlObjectName, TableDetails};
use zqlz_schema_tools::{
    MigrationConfig, MigrationDialect, MigrationGenerator, SchemaComparator, SchemaDiff,
};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    dock::{DockPlacement, PanelView},
    notification::Notification,
    scroll::ScrollableElement,
    v_flex,
};
use zqlz_versioning::{
    DatabaseObjectType, VersionEntry,
    widgets::{DiffViewer, DiffViewerEvent, VersionHistoryPanel, VersionHistoryPanelEvent},
};

use crate::app::AppState;
use crate::main_view::refresh::{RefreshTarget, SurfaceRefreshOptions};

use super::MainView;

#[derive(Clone)]
struct RestorePlan {
    version: VersionEntry,
    statements: Vec<String>,
    preview_sql: String,
}

fn version_restore_commit_message(version: &VersionEntry) -> String {
    format!(
        "Restore {} {} to version {}",
        version.object_type.display_name().to_lowercase(),
        version.object_name,
        version.short_id()
    )
}

fn migration_dialect_for_connection(connection: &Arc<dyn Connection>) -> MigrationDialect {
    if matches!(
        connection.dialect_id(),
        Some("postgres") | Some("postgresql")
    ) {
        MigrationDialect::PostgreSQL
    } else if matches!(connection.dialect_id(), Some("mysql") | Some("mariadb")) {
        MigrationDialect::MySQL
    } else if matches!(connection.dialect_id(), Some("mssql") | Some("sqlserver")) {
        MigrationDialect::MsSql
    } else {
        MigrationDialect::SQLite
    }
}

fn quote_qualified_name(
    connection: &Arc<dyn Connection>,
    schema: Option<&str>,
    object_name: &str,
) -> String {
    match schema {
        Some(schema_name) if !schema_name.is_empty() => connection
            .render_qualified_name(&SqlObjectName::with_namespace(schema_name, object_name)),
        _ => connection.quote_identifier(object_name),
    }
}

fn is_postgres_dialect(connection: &Arc<dyn Connection>) -> bool {
    matches!(
        connection.dialect_id(),
        Some("postgres") | Some("postgresql")
    )
}

fn supports_create_or_replace_view(connection: &Arc<dyn Connection>) -> bool {
    matches!(
        connection.dialect_id(),
        Some("postgres") | Some("postgresql") | Some("mysql") | Some("mariadb")
    )
}

fn build_sql_restore_plan(
    connection: &Arc<dyn Connection>,
    version: VersionEntry,
) -> anyhow::Result<Option<RestorePlan>> {
    let trimmed_content = version.content.trim();
    if trimmed_content.is_empty() {
        anyhow::bail!("Saved version does not contain any SQL to restore")
    }

    let statements = match version.object_type {
        DatabaseObjectType::View => {
            if is_postgres_dialect(connection) {
                vec![trimmed_content.to_string()]
            } else {
                let view_object_name = match version.object_schema.as_deref() {
                    Some(schema_name) if !schema_name.is_empty() => {
                        SqlObjectName::with_namespace(schema_name, &version.object_name)
                    }
                    _ => SqlObjectName::new(&version.object_name),
                };
                let drop_view_sql = connection
                    .drop_view_sql(
                        &view_object_name,
                        DropViewOptions {
                            if_exists: true,
                            cascade: false,
                        },
                    )
                    .unwrap_or_else(|_| {
                        format!(
                            "DROP VIEW IF EXISTS {}",
                            quote_qualified_name(
                                connection,
                                version.object_schema.as_deref(),
                                &version.object_name,
                            )
                        )
                    });
                vec![
                    drop_view_sql,
                    if supports_create_or_replace_view(connection) {
                        connection.normalize_create_view_sql(trimmed_content)
                    } else {
                        trimmed_content.to_string()
                    },
                ]
            }
        }
        _ if version.object_type.is_applyable() => vec![trimmed_content.to_string()],
        _ => return Ok(None),
    };

    let preview_sql = statements.join(";\n\n") + if statements.is_empty() { "" } else { ";" };

    Ok(Some(RestorePlan {
        version,
        statements,
        preview_sql,
    }))
}

async fn build_table_restore_plan(
    connection: Arc<dyn Connection>,
    version: VersionEntry,
) -> anyhow::Result<Option<RestorePlan>> {
    let target_snapshot: TableDetails = serde_json::from_str(&version.content)?;
    let schema_introspection = connection.as_schema_introspection().ok_or_else(|| {
        anyhow::anyhow!("Schema introspection is not available for this connection")
    })?;
    let current_snapshot = schema_introspection
        .get_table(version.object_schema.as_deref(), &version.object_name)
        .await?;

    let comparator = SchemaComparator::new();
    let Some(table_diff) = comparator.compare_table_details(&target_snapshot, &current_snapshot)
    else {
        return Ok(None);
    };

    let mut schema_diff = SchemaDiff::new();
    schema_diff.modified_tables.push(table_diff);

    let generator = MigrationGenerator::with_config(MigrationConfig::for_dialect(
        migration_dialect_for_connection(&connection),
    ));
    let migration = generator.generate(&schema_diff)?;
    if migration.up_sql.is_empty() {
        return Ok(None);
    }

    Ok(Some(RestorePlan {
        version,
        preview_sql: migration.up_script(),
        statements: migration.up_sql,
    }))
}

async fn build_restore_plan(
    connection: Arc<dyn Connection>,
    version: VersionEntry,
) -> anyhow::Result<Option<RestorePlan>> {
    if version.object_type == DatabaseObjectType::Table {
        build_table_restore_plan(connection, version).await
    } else {
        build_sql_restore_plan(&connection, version)
    }
}

impl MainView {
    /// Show version history for a database object.
    ///
    /// Opens or updates the version history panel for the specified object.
    pub fn show_version_history(
        &mut self,
        connection_id: Uuid,
        object_id: String,
        object_schema: Option<String>,
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
        let object_identifier =
            zqlz_versioning::make_object_id(object_schema.as_deref(), &object_id);

        // Create or update the version history panel
        if let Some(panel) = &self.version_history_panel {
            // Panel exists, just update it
            panel.update(cx, |panel, cx| {
                panel.set_object(connection_id, object_identifier.clone(), object_type, cx);
            });
        } else {
            // Create new panel
            let panel = cx.new(|cx| {
                let mut panel = VersionHistoryPanel::new(repository, cx);
                panel.set_object(connection_id, object_identifier, object_type, cx);
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
                let Some(app_state) = cx.try_global::<AppState>() else {
                    tracing::error!("No AppState available");
                    return;
                };
                let Some(connection) = app_state.connections.get(version.connection_id) else {
                    window.push_notification(Notification::error("Connection not found"), cx);
                    return;
                };

                let connection = connection.clone();
                let repository = self.version_repository.clone();
                let main_view = cx.entity().downgrade();
                let window_handle = window.window_handle();

                cx.spawn_in(window, async move |_this, cx| {
                    match build_restore_plan(connection.clone(), version.clone()).await {
                        Ok(Some(plan)) => {
                            cx.update(|window, cx| {
                                let plan_for_dialog = plan.clone();
                                let connection = connection.clone();
                                let repository = repository.clone();
                                let main_view = main_view.clone();

                                window.open_dialog(cx, move |dialog, _window, cx| {
                                    let plan = plan_for_dialog.clone();
                                    let connection = connection.clone();
                                    let repository = repository.clone();
                                    let main_view = main_view.clone();

                                    dialog
                                        .title(format!(
                                            "Restore {} '{}'?",
                                            plan.version.object_type.display_name(),
                                            plan.version.object_name
                                        ))
                                        .w(px(720.0))
                                        .child(
                                            v_flex()
                                                .gap_3()
                                                .child(
                                                    div().text_sm().child(format!(
                                                        "This will apply saved version {} back to the database.",
                                                        plan.version.short_id()
                                                    )),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child("Preview of SQL to execute:"),
                                                )
                                                .child(
                                                    div()
                                                        .h(px(260.0))
                                                        .overflow_y_scrollbar()
                                                        .p_2()
                                                        .rounded_md()
                                                        .bg(cx.theme().secondary)
                                                        .font_family(cx.theme().mono_font_family.clone())
                                                        .text_xs()
                                                        .child(plan.preview_sql.clone()),
                                                ),
                                        )
                                        .button_props(
                                            DialogButtonProps::default()
                                                .ok_text("Apply Restore")
                                                .ok_variant(ButtonVariant::Warning),
                                        )
                                        .on_ok(move |_, _window, cx| {
                                            let plan = plan.clone();
                                            let connection = connection.clone();
                                            let repository = repository.clone();
                                            let main_view = main_view.clone();

                                            cx.spawn(async move |cx| {
                                                let mut apply_error = None;
                                                for statement in &plan.statements {
                                                    if let Err(error) = connection.execute(statement, &[]).await {
                                                        apply_error = Some(error.to_string());
                                                        break;
                                                    }
                                                }

                                                let _ = cx.update_window(window_handle, |_, window, cx| {
                                                    match apply_error {
                                                        Some(error) => {
                                                            tracing::error!(%error, object = %plan.version.object_id, "Failed to restore version");
                                                            window.push_notification(
                                                                Notification::error(format!(
                                                                    "Failed to restore {} '{}': {}",
                                                                    plan.version.object_type.display_name().to_lowercase(),
                                                                    plan.version.object_name,
                                                                    error
                                                                )),
                                                                cx,
                                                            );
                                                        }
                                                        None => {
                                                            if let Err(error) = repository.commit(
                                                                plan.version.connection_id,
                                                                plan.version.object_type,
                                                                plan.version.object_schema.clone(),
                                                                plan.version.object_name.clone(),
                                                                plan.version.content.clone(),
                                                                version_restore_commit_message(&plan.version),
                                                            ) {
                                                                tracing::error!(%error, object = %plan.version.object_id, "Failed to record restore version snapshot");
                                                            }

                                                            let _ = main_view.update(cx, |main_view, cx| {
                                                                main_view.refresh_connection_surfaces(
                                                                    RefreshTarget::Connection(plan.version.connection_id),
                                                                    SurfaceRefreshOptions::SIDEBAR_AND_OBJECTS,
                                                                    cx,
                                                                );

                                                                if let Some(panel) = &main_view.version_history_panel {
                                                                    panel.update(cx, |panel, cx| {
                                                                        panel.refresh(cx);
                                                                    });
                                                                }
                                                            });

                                                            window.push_notification(
                                                                Notification::success(format!(
                                                                    "Restored {} '{}' from version {}",
                                                                    plan.version.object_type.display_name().to_lowercase(),
                                                                    plan.version.object_name,
                                                                    plan.version.short_id()
                                                                )),
                                                                cx,
                                                            );
                                                        }
                                                    }
                                                });
                                            })
                                            .detach();

                                            true
                                        })
                                        .confirm()
                                });
                            })?;
                        }
                        Ok(None) => {
                            cx.update(|window, cx| {
                                window.push_notification(
                                    Notification::info(format!(
                                        "{} '{}' is already at version {}",
                                        version.object_type.display_name(),
                                        version.object_name,
                                        version.short_id()
                                    )),
                                    cx,
                                );
                            })?;
                        }
                        Err(error) => {
                            tracing::error!(%error, object = %version.object_id, "Failed to prepare restore plan");
                            cx.update(|window, cx| {
                                window.push_notification(
                                    Notification::error(format!(
                                        "Failed to prepare restore for {} '{}': {}",
                                        version.object_type.display_name().to_lowercase(),
                                        version.object_name,
                                        error
                                    )),
                                    cx,
                                );
                            })?;
                        }
                    }

                    anyhow::Ok(())
                })
                .detach();
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
}
