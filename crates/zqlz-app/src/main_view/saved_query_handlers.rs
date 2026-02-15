// Saved query management methods for MainView
//
// This module handles saving, loading, and managing user-saved SQL queries.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::ButtonVariant,
    dialog::DialogButtonProps,
    h_flex,
    input::{Input, InputState},
    notification::Notification,
    typography::body_small,
    v_flex, ActiveTheme as _, Icon, WindowExt, ZqlzIcon,
};

use crate::app::AppState;
use crate::components::{ConnectionSidebar, QueryEditor};
use crate::storage::SavedQuery;
use zqlz_connection::SavedQueryInfo;

use super::MainView;

/// Validates a query name and returns an error message if invalid.
fn validate_query_name(name: &str) -> Option<&'static str> {
    let name = name.trim();

    if name.is_empty() {
        return Some("Query name cannot be empty");
    }

    if name.len() > 128 {
        return Some("Query name is too long (max 128 characters)");
    }

    // Query names can be more permissive than SQL identifiers
    // But we still want to disallow some problematic characters
    for c in name.chars() {
        if c == '/' || c == '\\' || c == '\0' || c == '\n' || c == '\r' {
            return Some("Query name contains invalid characters");
        }
    }

    None
}

impl MainView {
    /// Show the save query dialog for a new query
    pub fn show_save_query_dialog(
        &mut self,
        editor: WeakEntity<QueryEditor>,
        sql: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get connection name for display
        let connection_name = cx
            .try_global::<AppState>()
            .and_then(|state| {
                state
                    .saved_connections()
                    .into_iter()
                    .find(|c| c.id == connection_id)
                    .map(|c| c.name.clone())
            })
            .unwrap_or_else(|| "Unknown".to_string());

        // Create input state for the query name
        let name_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Enter query name..."));
        let error_message: Entity<Option<String>> = cx.new(|_| None);

        // Get weak reference to sidebar for updating after save
        let sidebar_weak: WeakEntity<ConnectionSidebar> = self.connection_sidebar.downgrade();

        // Observe input changes to clear error message
        cx.observe(&name_input, {
            let error_message = error_message.clone();
            move |_, _, cx| {
                error_message.update(cx, |msg, cx| {
                    if msg.is_some() {
                        *msg = None;
                        cx.notify();
                    }
                });
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let error_message = error_message.clone();
            let sidebar_weak = sidebar_weak.clone();

            move |dialog, _window, cx| {
                let sql = sql.clone();
                let connection_name = connection_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();
                let editor_weak = editor.clone();
                let sidebar_weak = sidebar_weak.clone();

                dialog
                    .title("Save Query")
                    .w(px(420.0))
                    .child(
                        v_flex()
                            .gap_3()
                            // Query Name field
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(body_small("Query Name:"))
                                    .child(Input::new(&name_input)),
                            )
                            // Save Location (read-only, shows current connection)
                            .child(
                                v_flex().gap_1().child(body_small("Save Location:")).child(
                                    h_flex()
                                        .px_3()
                                        .py_2()
                                        .gap_2()
                                        .items_center()
                                        .bg(cx.theme().muted)
                                        .rounded_md()
                                        .border_1()
                                        .border_color(cx.theme().border)
                                        .child(
                                            Icon::new(ZqlzIcon::Database)
                                                .size_4()
                                                .text_color(cx.theme().muted_foreground),
                                        )
                                        .child(div().text_sm().child(connection_name.clone())),
                                ),
                            )
                            // Error message
                            .child({
                                let error = error_message.read(cx).clone();
                                div().text_xs().h(px(16.0)).when_some(error, |this, err| {
                                    this.text_color(gpui::red()).child(err)
                                })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let query_name = name_input.read(cx).text().to_string().trim().to_string();

                        // Validate name
                        if let Some(err) = validate_query_name(&query_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        // Get storage and check for duplicate names
                        let Some(app_state) = cx.try_global::<AppState>() else {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("Application state not available".to_string());
                                cx.notify();
                            });
                            return false;
                        };

                        let storage = &app_state.storage;

                        // Check if query name already exists
                        match storage.query_name_exists(connection_id, &query_name) {
                            Ok(true) => {
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg =
                                        Some("A query with this name already exists".to_string());
                                    cx.notify();
                                });
                                return false;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::error!("Failed to check query name: {}", e);
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg = Some("Failed to check query name".to_string());
                                    cx.notify();
                                });
                                return false;
                            }
                        }

                        // Create and save the query
                        let saved_query =
                            SavedQuery::new(query_name.clone(), connection_id, sql.clone());
                        let query_id = saved_query.id;

                        match storage.save_query(&saved_query) {
                            Ok(()) => {
                                tracing::info!("Query '{}' saved successfully", query_name);

                                // Update the editor to mark it as saved
                                _ = editor_weak.update(cx, |editor, cx| {
                                    editor.set_saved_query_id(Some(query_id), cx);
                                    editor.set_name(&query_name, cx);
                                    editor.mark_clean(cx);
                                });

                                // Update sidebar to show the new saved query
                                _ = sidebar_weak.update(cx, |sidebar, cx| {
                                    sidebar.add_saved_query(
                                        connection_id,
                                        SavedQueryInfo {
                                            id: query_id,
                                            name: query_name.clone(),
                                        },
                                        cx,
                                    );
                                });

                                true
                            }
                            Err(e) => {
                                tracing::error!("Failed to save query: {}", e);
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg = Some(format!("Failed to save: {}", e));
                                    cx.notify();
                                });
                                false
                            }
                        }
                    })
                    .button_props(
                        DialogButtonProps::default()
                            .ok_text("Save")
                            .ok_variant(ButtonVariant::Primary),
                    )
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Update an existing saved query
    pub fn update_saved_query(
        &mut self,
        query_id: Uuid,
        sql: String,
        editor: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        match app_state.storage.update_query_sql(query_id, &sql) {
            Ok(()) => {
                tracing::info!("Query updated successfully");

                // Mark editor as clean
                _ = editor.update(cx, |editor, cx| {
                    editor.mark_clean(cx);
                });

                window.push_notification(Notification::success("Query saved"), cx);
            }
            Err(e) => {
                tracing::error!("Failed to update query: {}", e);
                window.push_notification(Notification::error(format!("Failed to save: {}", e)), cx);
            }
        }
    }

    /// Open a saved query in the query editor
    pub fn open_saved_query(
        &mut self,
        query_id: Uuid,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        // Load the query
        let query = match app_state.storage.load_query(query_id) {
            Ok(Some(q)) => q,
            Ok(None) => {
                window.push_notification(Notification::error("Query not found"), cx);
                return;
            }
            Err(e) => {
                tracing::error!("Failed to load query: {}", e);
                window.push_notification(
                    Notification::error(format!("Failed to load query: {}", e)),
                    cx,
                );
                return;
            }
        };

        // Create a new query editor with the saved query
        let query_name = query.name.clone();
        let sql = query.sql.clone();

        // Use the existing new_query method but with modifications
        self.open_query_editor_with_saved_query(
            connection_id,
            query_id,
            query_name,
            sql,
            window,
            cx,
        );
    }

    /// Opens a query editor for a saved query
    fn open_query_editor_with_saved_query(
        &mut self,
        connection_id: Uuid,
        query_id: Uuid,
        name: String,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get schema service and connection from AppState
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("AppState not initialized");
            return;
        };

        let schema_service = app_state.schema_service.clone();
        let connection = app_state.connections.get(connection_id);
        let (driver_type, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or((String::new(), String::from("Unknown")));

        // Create an EditorId in WorkspaceState to track this editor
        let editor_id = self.workspace_state.update(cx, |state, cx| {
            state.create_editor(Some(connection_id), name.clone(), cx)
        });

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new(
                name.clone(),
                Some(connection_id),
                schema_service.clone(),
                window,
                cx,
            );

            // Set the connection if available
            if let Some(conn) = connection.clone() {
                editor.set_connection(
                    Some(connection_id),
                    Some(connection_name),
                    Some(conn),
                    Some(driver_type),
                    cx,
                );
            }

            // Set saved query metadata
            editor.set_saved_query_id(Some(query_id), cx);

            // Set the SQL content
            editor.set_text(&sql, window, cx);
            editor.mark_clean(cx);

            editor
        });

        // Subscribe to editor events so execution, save, explain, etc. all work
        let subscription = self.subscribe_query_editor(&query_editor, name, editor_id, window, cx);

        std::mem::forget(subscription);

        // Track this query editor
        self.query_editors.push(query_editor.downgrade());

        // Add to dock
        let query_editor_panel: Arc<dyn zqlz_ui::widgets::dock::PanelView> = Arc::new(query_editor);
        self.dock_area.update(cx, |area, cx| {
            area.add_panel(
                query_editor_panel,
                zqlz_ui::widgets::dock::DockPlacement::Center,
                None,
                window,
                cx,
            );
        });
    }

    /// Delete a saved query
    pub fn delete_saved_query(
        &mut self,
        query_id: Uuid,
        query_name: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Get weak reference to sidebar for updating after delete
        let sidebar_weak: WeakEntity<ConnectionSidebar> = self.connection_sidebar.downgrade();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let query_name = query_name.clone();
            let sidebar_weak = sidebar_weak.clone();

            dialog
                .title("Delete Query")
                .child(
                    v_flex()
                        .gap_2()
                        .child(
                            div().text_sm().child(format!(
                                "Are you sure you want to delete '{}'?",
                                query_name
                            )),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        ),
                )
                .on_ok(move |_, window, cx| {
                    let Some(app_state) = cx.try_global::<AppState>() else {
                        window.push_notification(
                            Notification::error("Application state not available"),
                            cx,
                        );
                        return true;
                    };

                    match app_state.storage.delete_query(query_id) {
                        Ok(()) => {
                            tracing::info!("Query '{}' deleted successfully", query_name);
                            window.push_notification(
                                Notification::success(format!("Query '{}' deleted", query_name)),
                                cx,
                            );

                            // Update sidebar to remove the deleted query
                            _ = sidebar_weak.update(cx, |sidebar, cx| {
                                sidebar.remove_saved_query(connection_id, query_id, cx);
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to delete query: {}", e);
                            window.push_notification(
                                Notification::error(format!("Failed to delete: {}", e)),
                                cx,
                            );
                        }
                    }

                    true
                })
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .confirm()
        });
    }

    /// Rename a saved query
    pub fn rename_saved_query(
        &mut self,
        query_id: Uuid,
        current_name: String,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Create input state with current name
        let name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(&current_name)
                .placeholder("Enter new name...")
        });
        let error_message: Entity<Option<String>> = cx.new(|_| None);

        // Get weak reference to sidebar for updating after rename
        let sidebar_weak: WeakEntity<ConnectionSidebar> = self.connection_sidebar.downgrade();

        // Observe input changes to clear error message
        cx.observe(&name_input, {
            let error_message = error_message.clone();
            move |_, _, cx| {
                error_message.update(cx, |msg, cx| {
                    if msg.is_some() {
                        *msg = None;
                        cx.notify();
                    }
                });
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let error_message = error_message.clone();
            let sidebar_weak = sidebar_weak.clone();

            move |dialog, _window, cx| {
                let current_name = current_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();
                let sidebar_weak = sidebar_weak.clone();

                dialog
                    .title("Rename Query")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(body_small("Enter a new name:"))
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div().text_xs().h(px(16.0)).when_some(error, |this, err| {
                                    this.text_color(gpui::red()).child(err)
                                })
                            }),
                    )
                    .on_ok(move |_, window, cx| {
                        let new_name = name_input.read(cx).text().to_string().trim().to_string();

                        // If name unchanged, just close
                        if new_name == current_name {
                            return true;
                        }

                        // Validate name
                        if let Some(err) = validate_query_name(&new_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        let Some(app_state) = cx.try_global::<AppState>() else {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("Application state not available".to_string());
                                cx.notify();
                            });
                            return false;
                        };

                        // Check if new name already exists
                        match app_state
                            .storage
                            .query_name_exists(connection_id, &new_name)
                        {
                            Ok(true) => {
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg =
                                        Some("A query with this name already exists".to_string());
                                    cx.notify();
                                });
                                return false;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::error!("Failed to check query name: {}", e);
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg = Some("Failed to check query name".to_string());
                                    cx.notify();
                                });
                                return false;
                            }
                        }

                        // Rename the query
                        match app_state.storage.rename_query(query_id, &new_name) {
                            Ok(()) => {
                                tracing::info!("Query renamed to '{}'", new_name);
                                window.push_notification(
                                    Notification::success(format!("Renamed to '{}'", new_name)),
                                    cx,
                                );

                                // Update sidebar to reflect the new name
                                _ = sidebar_weak.update(cx, |sidebar, cx| {
                                    sidebar.rename_saved_query(
                                        connection_id,
                                        query_id,
                                        new_name.clone(),
                                        cx,
                                    );
                                });

                                true
                            }
                            Err(e) => {
                                tracing::error!("Failed to rename query: {}", e);
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg = Some(format!("Failed to rename: {}", e));
                                    cx.notify();
                                });
                                false
                            }
                        }
                    })
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Load saved queries for a connection
    pub fn load_saved_queries_for_connection(
        &self,
        connection_id: Uuid,
        cx: &App,
    ) -> Vec<SavedQuery> {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return Vec::new();
        };

        match app_state.storage.load_queries_for_connection(connection_id) {
            Ok(queries) => queries,
            Err(e) => {
                tracing::error!(
                    "Failed to load queries for connection {}: {}",
                    connection_id,
                    e
                );
                Vec::new()
            }
        }
    }
}
