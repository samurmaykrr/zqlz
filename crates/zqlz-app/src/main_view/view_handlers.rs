// View management methods for MainView
//
// This module handles database view operations: design, create, delete, duplicate, rename.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_query::EditorObjectType;
use zqlz_trigger_designer::{
    DatabaseDialect as TriggerDialect, TriggerDesign, TriggerDesignerEvent, TriggerDesignerPanel,
};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    dock::{DockPlacement, PanelView},
    input::{Input, InputState},
    notification::Notification,
    v_flex,
};

use crate::app::AppState;
use crate::components::{ObjectsPanelEvent, QueryEditor};
use zqlz_services::SchemaService;

use super::MainView;

/// Validates a view name and returns an error message if invalid.
fn validate_view_name(name: &str) -> Option<&'static str> {
    let name = name.trim();

    if name.is_empty() {
        return Some("View name cannot be empty");
    }

    if name.len() > 128 {
        return Some("View name is too long (max 128 characters)");
    }

    // Emptiness is already guarded above, so `next()` will always yield a char
    let Some(first_char) = name.chars().next() else {
        return Some("View name cannot be empty");
    };
    if !first_char.is_alphabetic() && first_char != '_' {
        return Some("View name must start with a letter or underscore");
    }

    // Check for invalid characters (allow alphanumeric, underscore, and some databases allow $)
    for c in name.chars() {
        if !c.is_alphanumeric() && c != '_' && c != '$' {
            return Some("View name contains invalid characters");
        }
    }

    // Check for reserved SQL keywords (common ones)
    let upper = name.to_uppercase();
    let reserved = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TABLE",
        "INDEX",
        "VIEW",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "ON",
        "AS",
        "IN",
        "IS",
        "LIKE",
        "BETWEEN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "EXISTS",
        "ALL",
        "ANY",
        "SOME",
        "DISTINCT",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "INTO",
        "VALUES",
        "SET",
        "DEFAULT",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "UNIQUE",
        "CHECK",
        "CONSTRAINT",
        "DATABASE",
        "SCHEMA",
        "GRANT",
        "REVOKE",
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
    ];
    if reserved.contains(&upper.as_str()) {
        return Some("View name is a reserved SQL keyword");
    }

    None
}

/// Fetches the SELECT statement definition of a view from the database.
async fn fetch_view_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    view_name: &str,
    driver_type: &str,
) -> Result<String, String> {
    // Different databases store view definitions differently
    let sql = if driver_type.contains("postgres") {
        format!(
            "SELECT definition FROM pg_views WHERE viewname = '{}'",
            view_name.replace("'", "''")
        )
    } else {
        // SQLite: query sqlite_master
        format!(
            "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = '{}'",
            view_name.replace("'", "''")
        )
    };

    match connection.query(&sql, &[]).await {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                if let Some(value) = row.values.first() {
                    let definition = value.to_string();

                    // For PostgreSQL, the definition is just the SELECT part
                    // For SQLite, it's the full CREATE VIEW statement, so extract the SELECT
                    if driver_type.contains("sqlite") {
                        // SQLite returns: CREATE VIEW name AS SELECT ...
                        // We need to extract just the SELECT part
                        if let Some(as_pos) = definition.to_uppercase().find(" AS ") {
                            let select_part = definition[as_pos + 4..].trim();
                            return Ok(select_part.to_string());
                        }
                    }

                    return Ok(definition);
                }
            }
            Err(format!("View '{}' not found", view_name))
        }
        Err(e) => Err(format!("Failed to fetch view definition: {}", e)),
    }
}

/// Fetches the definition of a function from the database.
async fn fetch_function_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    function_name: &str,
    driver_type: &str,
) -> Result<String, String> {
    let sql = if driver_type.contains("postgres") {
        // PostgreSQL: get function definition using pg_get_functiondef
        format!(
            r#"
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.proname = '{}'
            AND n.nspname = 'public'
            LIMIT 1
            "#,
            function_name.replace("'", "''")
        )
    } else {
        // SQLite doesn't have stored functions, so this is a placeholder
        // In practice, SQLite uses user-defined functions which are created in code
        format!(
            "SELECT sql FROM sqlite_master WHERE type = 'function' AND name = '{}'",
            function_name.replace("'", "''")
        )
    };

    match connection.query(&sql, &[]).await {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                if let Some(value) = row.values.first() {
                    return Ok(value.to_string());
                }
            }
            Err(format!("Function '{}' not found", function_name))
        }
        Err(e) => Err(format!("Failed to fetch function definition: {}", e)),
    }
}

/// Fetches the definition of a stored procedure from the database.
async fn fetch_procedure_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    procedure_name: &str,
    driver_type: &str,
) -> Result<String, String> {
    let sql = if driver_type.contains("postgres") {
        // PostgreSQL: procedures are functions with prorettype = 0 (void) in newer versions
        // or use pg_get_functiondef for functions marked as procedures
        format!(
            r#"
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.proname = '{}'
            AND n.nspname = 'public'
            AND p.prokind = 'p'
            LIMIT 1
            "#,
            procedure_name.replace("'", "''")
        )
    } else {
        // SQLite doesn't have stored procedures
        format!(
            "SELECT sql FROM sqlite_master WHERE type = 'procedure' AND name = '{}'",
            procedure_name.replace("'", "''")
        )
    };

    match connection.query(&sql, &[]).await {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                if let Some(value) = row.values.first() {
                    return Ok(value.to_string());
                }
            }
            Err(format!("Procedure '{}' not found", procedure_name))
        }
        Err(e) => Err(format!("Failed to fetch procedure definition: {}", e)),
    }
}

impl MainView {
    /// Opens a QueryEditor to design/edit an existing view
    pub(super) fn design_view(
        &mut self,
        connection_id: Uuid,
        view_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Design view: {} on connection {}", view_name, connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        // Get the driver name and connection name for dialect-specific queries
        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let view_name_for_spawn = view_name.clone();
        let dock_area = self.dock_area.downgrade();

        // Format the editor title to include [View] indicator and connection name
        let editor_title = format!("[View] {} ({})", view_name, connection_name);

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the view definition
            match fetch_view_definition(&connection, &view_name_for_spawn, &driver_name).await {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        // Create a QueryEditor with EditorObjectType::View
                        let object_type = EditorObjectType::edit_view(
                            view_name_for_spawn.clone(),
                            None, // schema
                        );

                        let query_editor = cx.new(|cx| {
                            let mut editor = QueryEditor::new_for_object(
                                editor_title.clone(),
                                connection_id,
                                object_type,
                                Some(definition),
                                schema_service.clone(),
                                window,
                                cx,
                            );

                            // Set the connection so LSP and autocomplete work
                            editor.set_connection(
                                Some(connection_id),
                                Some(connection_name.clone()),
                                Some(connection.clone()),
                                Some(driver_name.clone()),
                                cx,
                            );

                            editor
                        });

                        // Subscribe to editor events to handle Save
                        _ = this.update(cx, |main_view, cx| {
                            let subscription = cx.subscribe_in(&query_editor, window, {
                                let query_editor_weak = query_editor.downgrade();
                                move |this,
                                      _editor,
                                      event: &crate::components::QueryEditorEvent,
                                      window,
                                      cx| {
                                    this.handle_view_editor_event(
                                        event,
                                        query_editor_weak.clone(),
                                        window,
                                        cx,
                                    );
                                }
                            });
                            main_view._subscriptions.push(subscription);
                            main_view.query_editors.push(query_editor.downgrade());
                        });

                        // Add to center dock
                        if let Some(dock_area) = dock_area.upgrade() {
                            dock_area.update(cx, |area, cx| {
                                area.add_panel(
                                    Arc::new(query_editor)
                                        as Arc<dyn zqlz_ui::widgets::dock::PanelView>,
                                    zqlz_ui::widgets::dock::DockPlacement::Center,
                                    None,
                                    window,
                                    cx,
                                );
                            });
                        }

                        tracing::info!("Opened view designer for '{}'", view_name_for_spawn);
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to load view definition: {}", e);
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to load view: {}", e)),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Opens a new QueryEditor for creating a new view
    pub(super) fn new_view(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("New view on connection {}", connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let schema_service = app_state.schema_service.clone();

        // Create a new view with a placeholder name
        let object_type = EditorObjectType::new_view();

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new_for_object(
                "New View".to_string(),
                connection_id,
                object_type,
                Some("SELECT * FROM table_name".to_string()), // Starter template
                schema_service.clone(),
                window,
                cx,
            );

            editor.set_connection(
                Some(connection_id),
                Some(connection_name),
                Some(connection.clone()),
                Some(driver_name),
                cx,
            );

            editor
        });

        // Subscribe to editor events
        let subscription = cx.subscribe_in(&query_editor, window, {
            let query_editor_weak = query_editor.downgrade();
            move |this, _editor, event: &crate::components::QueryEditorEvent, window, cx| {
                this.handle_view_editor_event(event, query_editor_weak.clone(), window, cx);
            }
        });
        self._subscriptions.push(subscription);
        self.query_editors.push(query_editor.downgrade());

        // Add to center dock
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

        tracing::info!("New view editor opened");
    }

    /// Deletes a view from the database
    pub(super) fn delete_view(
        &mut self,
        connection_id: Uuid,
        view_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Delete view: {} on connection {}", view_name, connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let view_name_for_dialog = view_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let view_name = view_name_for_dialog.clone();

            dialog
                .title("Delete View")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete view '{}'?",
                            view_name
                        )))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let objects_panel = objects_panel.clone();
                    let view_name = view_name.clone();

                    cx.spawn(async move |cx| {
                        let sql = format!("DROP VIEW \"{}\"", view_name);
                        match connection.execute(&sql, &[]).await {
                            Ok(_) => {
                                tracing::info!("View '{}' deleted successfully", view_name);

                                _ = connection_sidebar.update(cx, |sidebar, cx| {
                                    sidebar.remove_view(connection_id, &view_name, cx);
                                });

                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to delete view: {}", e);
                            }
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    /// Duplicates a view (creates a copy with a new name)
    pub(super) fn duplicate_view(
        &mut self,
        connection_id: Uuid,
        view_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Duplicate view: {} on connection {}",
            view_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let source_view_name = view_name.clone();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("New view name"));
        name_input.update(cx, |input, cx| {
            input.set_value(format!("{}_copy", view_name), window, cx);
        });

        let error_message: Entity<Option<String>> = cx.new(|_| None);

        cx.subscribe(&name_input, {
            let error_message = error_message.clone();
            move |_this, _input, event, cx| {
                if matches!(event, zqlz_ui::widgets::input::InputEvent::Change) {
                    error_message.update(cx, |msg, cx| {
                        if msg.is_some() {
                            *msg = None;
                            cx.notify();
                        }
                    });
                }
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let source_view_name = source_view_name.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let schema_service = schema_service.clone();
                let connection_sidebar = connection_sidebar.clone();
                let objects_panel = objects_panel.clone();
                let source_view_name = source_view_name.clone();
                let driver_name = driver_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();

                dialog
                    .title("Duplicate View")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .child(format!("Create a copy of view '{}' as:", source_view_name)),
                            )
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div()
                                    .text_xs()
                                    .when_some(error, |this, err| {
                                        this.text_color(gpui::red()).child(err)
                                    })
                                    .when(error_message.read(cx).is_none(), |this| {
                                        this.text_color(cx.theme().muted_foreground)
                                            .child("The new view will have the same definition as the source view.")
                                    })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let new_view_name = name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_view_name(&new_view_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        if new_view_name == source_view_name {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("New name must be different from the original".to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        let connection = connection.clone();
                        let schema_service = schema_service.clone();
                        let connection_sidebar = connection_sidebar.clone();
                        let objects_panel = objects_panel.clone();
                        let source_view_name = source_view_name.clone();
                        let driver_name = driver_name.clone();

                        cx.spawn(async move |cx| {
                            // First, fetch the original view definition
                            match fetch_view_definition(&connection, &source_view_name, &driver_name).await {
                                Ok(definition) => {
                                    // Create the new view with the same definition
                                    let create_sql = format!(
                                        "CREATE VIEW \"{}\" AS {}",
                                        new_view_name, definition
                                    );

                                    match connection.execute(&create_sql, &[]).await {
                                        Ok(_) => {
                                            tracing::info!(
                                                "View '{}' duplicated as '{}'",
                                                source_view_name,
                                                new_view_name
                                            );

                                            // Invalidate schema cache so refresh works correctly
                                            schema_service.invalidate_connection_cache(connection_id);

                                            _ = connection_sidebar.update(cx, |sidebar, cx| {
                                                sidebar.add_view(connection_id, new_view_name.clone(), cx);
                                            });

                                            _ = objects_panel.update(cx, |_, cx| {
                                                cx.emit(ObjectsPanelEvent::Refresh);
                                            });
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to create duplicated view: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to fetch source view definition: {}", e);
                                }
                            }
                        })
                        .detach();

                        true
                    })
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Opens the rename view dialog
    pub(super) fn rename_view(
        &mut self,
        connection_id: Uuid,
        view_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Rename view: {} on connection {}", view_name, connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let old_view_name = view_name.clone();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("New view name"));
        name_input.update(cx, |input, cx| {
            input.set_value(view_name.clone(), window, cx);
        });

        let error_message: Entity<Option<String>> = cx.new(|_| None);

        cx.subscribe(&name_input, {
            let error_message = error_message.clone();
            move |_this, _input, event, cx| {
                if matches!(event, zqlz_ui::widgets::input::InputEvent::Change) {
                    error_message.update(cx, |msg, cx| {
                        if msg.is_some() {
                            *msg = None;
                            cx.notify();
                        }
                    });
                }
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let old_view_name = old_view_name.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let connection_sidebar = connection_sidebar.clone();
                let objects_panel = objects_panel.clone();
                let old_view_name = old_view_name.clone();
                let driver_name = driver_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();

                dialog
                    .title("Rename View")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div().text_sm().child(format!(
                                    "Enter a new name for view '{}':",
                                    old_view_name
                                )),
                            )
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div().text_xs().h(px(16.0)).when_some(error, |this, err| {
                                    this.text_color(gpui::red()).child(err)
                                })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let new_view_name =
                            name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_view_name(&new_view_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        if new_view_name == old_view_name {
                            return true;
                        }

                        let connection = connection.clone();
                        let connection_sidebar = connection_sidebar.clone();
                        let objects_panel = objects_panel.clone();
                        let old_view_name = old_view_name.clone();
                        let driver_name = driver_name.clone();

                        cx.spawn(async move |cx| {
                            // Most databases don't support ALTER VIEW ... RENAME
                            // We need to: 1) Get the definition, 2) Drop the old view, 3) Create with new name
                            match fetch_view_definition(&connection, &old_view_name, &driver_name)
                                .await
                            {
                                Ok(definition) => {
                                    // Drop old view
                                    let drop_sql = format!("DROP VIEW \"{}\"", old_view_name);
                                    if let Err(e) = connection.execute(&drop_sql, &[]).await {
                                        tracing::error!("Failed to drop old view: {}", e);
                                        return;
                                    }

                                    // Create new view
                                    let create_sql = format!(
                                        "CREATE VIEW \"{}\" AS {}",
                                        new_view_name, definition
                                    );
                                    match connection.execute(&create_sql, &[]).await {
                                        Ok(_) => {
                                            tracing::info!(
                                                "View '{}' renamed to '{}' successfully",
                                                old_view_name,
                                                new_view_name
                                            );

                                            _ = connection_sidebar.update(cx, |sidebar, cx| {
                                                sidebar.remove_view(
                                                    connection_id,
                                                    &old_view_name,
                                                    cx,
                                                );
                                                sidebar.add_view(
                                                    connection_id,
                                                    new_view_name.clone(),
                                                    cx,
                                                );
                                            });

                                            _ = objects_panel.update(cx, |_, cx| {
                                                cx.emit(ObjectsPanelEvent::Refresh);
                                            });
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to create renamed view: {}", e);
                                            // Try to restore the old view
                                            let restore_sql = format!(
                                                "CREATE VIEW \"{}\" AS {}",
                                                old_view_name, definition
                                            );
                                            _ = connection.execute(&restore_sql, &[]).await;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to fetch view definition: {}", e);
                                }
                            }
                        })
                        .detach();

                        true
                    })
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Copies view name to clipboard
    pub(super) fn copy_view_name(&mut self, view_name: &str, cx: &mut Context<Self>) {
        tracing::info!("Copy view name: {}", view_name);
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(view_name.to_string()));
    }

    /// Handle events from a database object editor (Views, Functions, Procedures, Triggers)
    pub(super) fn handle_view_editor_event(
        &mut self,
        event: &crate::components::QueryEditorEvent,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::components::QueryEditorEvent;

        match event {
            QueryEditorEvent::SaveObject {
                connection_id,
                object_type,
                definition,
            } => {
                match object_type {
                    EditorObjectType::View { .. } => {
                        self.save_view(
                            *connection_id,
                            object_type.clone(),
                            definition.clone(),
                            editor_weak,
                            window,
                            cx,
                        );
                    }
                    EditorObjectType::Function { .. }
                    | EditorObjectType::Procedure { .. }
                    | EditorObjectType::Trigger { .. } => {
                        self.save_database_object(
                            *connection_id,
                            object_type.clone(),
                            definition.clone(),
                            editor_weak,
                            window,
                            cx,
                        );
                    }
                    EditorObjectType::Query => {
                        tracing::warn!("SaveObject event for Query type — should use SaveQuery instead");
                    }
                }
            }
            // For standard query execution events, delegate to the normal query handler
            QueryEditorEvent::ExecuteQuery { sql, connection_id } => {
                tracing::info!("Executing view query: {}", sql);
                // Can reuse existing query execution logic
                self.execute_view_query(sql.clone(), *connection_id, window, cx);
            }
            QueryEditorEvent::ExecuteSelection { sql, connection_id } => {
                tracing::info!("Executing view selection: {}", sql);
                self.execute_view_query(sql.clone(), *connection_id, window, cx);
            }
            // Other events can be handled as needed
            _ => {}
        }
    }

    /// Execute a query from a view editor (for testing the SELECT statement)
    fn execute_view_query(
        &mut self,
        sql: String,
        connection_id: Option<Uuid>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(conn_id) = connection_id else {
            tracing::warn!("No connection for view query");
            return;
        };

        let Some(connection) = app_state.connections.get(conn_id) else {
            tracing::error!("Connection not found: {}", conn_id);
            return;
        };

        let query_service = app_state.query_service.clone();
        let results_panel = self.results_panel.clone();
        let connection = connection.clone();

        // Get connection info for display
        let connection_info = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == conn_id);
        let connection_name = connection_info.as_ref().map(|c| c.name.clone());
        let database_name = connection_info.as_ref().and_then(|c| {
            c.params
                .get("database")
                .or_else(|| c.params.get("path"))
                .cloned()
        });

        results_panel.update(cx, |panel, cx| {
            panel.set_loading(true, cx);
        });

        cx.spawn_in(window, async move |_this, cx| {
            let service_execution = query_service.execute_query(connection, conn_id, &sql).await;

            let execution = match service_execution {
                Ok(exec) => {
                    let start_time = chrono::Utc::now()
                        - chrono::Duration::milliseconds(exec.duration_ms as i64);
                    let end_time = chrono::Utc::now();

                    crate::components::QueryExecution {
                        sql: exec.sql,
                        start_time,
                        end_time,
                        duration_ms: exec.duration_ms,
                        connection_name,
                        database_name,
                        statements: exec
                            .statements
                            .into_iter()
                            .map(|s| crate::components::StatementResult {
                                sql: s.sql,
                                duration_ms: s.duration_ms,
                                result: s.result,
                                error: s.error,
                                affected_rows: s.affected_rows,
                            })
                            .collect(),
                    }
                }
                Err(e) => {
                    let now = chrono::Utc::now();
                    crate::components::QueryExecution {
                        sql,
                        start_time: now,
                        end_time: now,
                        duration_ms: 0,
                        connection_name,
                        database_name,
                        statements: vec![crate::components::StatementResult {
                            sql: String::new(),
                            duration_ms: 0,
                            result: None,
                            error: Some(format!("Error: {}", e)),
                            affected_rows: 0,
                        }],
                    }
                }
            };

            _ = results_panel.update_in(cx, |panel, window, cx| {
                panel.set_execution(execution, window, cx);
            });

            anyhow::Ok(())
        })
        .detach();
    }

    /// Save a view (execute CREATE VIEW or CREATE OR REPLACE VIEW)
    fn save_view(
        &mut self,
        connection_id: Uuid,
        object_type: EditorObjectType,
        definition: String,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let EditorObjectType::View {
            name,
            schema: _,
            is_new,
        } = object_type
        else {
            tracing::error!("save_view called with non-view object type");
            return;
        };

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let schema_service = app_state.schema_service.clone();

        // For new views, we need to prompt for a name
        if is_new || name.is_none() {
            self.prompt_for_view_name(
                connection_id,
                definition,
                driver_name,
                connection.clone(),
                schema_service,
                editor_weak,
                window,
                cx,
            );
            return;
        }

        let Some(view_name) = name else {
            // Guarded by is_new || name.is_none() check above; this branch is unreachable
            return;
        };
        let connection = connection.clone();
        let _connection_sidebar = self.connection_sidebar.downgrade();

        // Generate the appropriate DDL based on database type
        let ddl = if driver_name.contains("postgres") {
            // PostgreSQL supports CREATE OR REPLACE VIEW
            format!("CREATE OR REPLACE VIEW \"{}\" AS {}", view_name, definition)
        } else {
            // SQLite doesn't support CREATE OR REPLACE, so we DROP and CREATE
            format!(
                "DROP VIEW IF EXISTS \"{}\"; CREATE VIEW \"{}\" AS {}",
                view_name, view_name, definition
            )
        };

        cx.spawn_in(window, async move |_this, cx| {
            // For SQLite, we need to execute multiple statements
            let statements: Vec<&str> = ddl.split(';').filter(|s| !s.trim().is_empty()).collect();

            let mut success = true;
            for stmt in statements {
                if let Err(e) = connection.execute(stmt.trim(), &[]).await {
                    tracing::error!("Failed to save view: {}", e);
                    success = false;

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to save view: {}", e)),
                            cx,
                        );
                    });
                    break;
                }
            }

            if success {
                tracing::info!("View '{}' saved successfully", view_name);

                schema_service.invalidate_connection_cache(connection_id);

                _ = editor_weak.update(cx, |editor, cx| {
                    editor.mark_clean(cx);
                });

                _ = cx.update(|window, cx| {
                    window.push_notification(
                        Notification::success(format!("View '{}' saved", view_name)),
                        cx,
                    );
                });
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Save a function, procedure, or trigger by executing the full DDL definition
    fn save_database_object(
        &mut self,
        connection_id: Uuid,
        object_type: EditorObjectType,
        definition: String,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let type_name = object_type.display_name().to_string();
        let object_name = object_type
            .object_name()
            .unwrap_or("unnamed")
            .to_string();

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();

        cx.spawn_in(window, async move |_this, cx| {
            // The editor content is the full DDL (e.g. CREATE OR REPLACE FUNCTION ...)
            match connection.execute(definition.trim(), &[]).await {
                Ok(_) => {
                    tracing::info!("{} '{}' saved successfully", type_name, object_name);

                    schema_service.invalidate_connection_cache(connection_id);

                    _ = editor_weak.update(cx, |editor, cx| {
                        editor.mark_clean(cx);
                    });

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::success(format!(
                                "{} '{}' saved",
                                type_name, object_name
                            )),
                            cx,
                        );
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to save {} '{}': {}", type_name, object_name, e);

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to save {} '{}': {}",
                                type_name, object_name, e
                            )),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Prompt user for a view name when creating a new view
    fn prompt_for_view_name(
        &mut self,
        connection_id: Uuid,
        definition: String,
        driver_name: String,
        connection: Arc<dyn zqlz_core::Connection>,
        schema_service: Arc<SchemaService>,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("View name"));
        let error_message: Entity<Option<String>> = cx.new(|_| None);
        let connection_sidebar = self.connection_sidebar.downgrade();

        cx.subscribe(&name_input, {
            let error_message = error_message.clone();
            move |_this, _input, event, cx| {
                if matches!(event, zqlz_ui::widgets::input::InputEvent::Change) {
                    error_message.update(cx, |msg, cx| {
                        if msg.is_some() {
                            *msg = None;
                            cx.notify();
                        }
                    });
                }
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let definition = definition.clone();
                let _driver_name = driver_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();
                let editor_weak = editor_weak.clone();
                let connection_sidebar = connection_sidebar.clone();
                let schema_service = schema_service.clone();

                dialog
                    .title("Save View As")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(div().text_sm().child("Enter a name for the new view:"))
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div().text_xs().h(px(16.0)).when_some(error, |this, err| {
                                    this.text_color(gpui::red()).child(err)
                                })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let view_name = name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_view_name(&view_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        let connection = connection.clone();
                        let definition = definition.clone();
                        let editor_weak = editor_weak.clone();
                        let connection_sidebar = connection_sidebar.clone();
                        let schema_service = schema_service.clone();

                        cx.spawn(async move |cx| {
                            let create_sql =
                                format!("CREATE VIEW \"{}\" AS {}", view_name, definition);

                            match connection.execute(&create_sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!("View '{}' created successfully", view_name);

                                    schema_service.invalidate_connection_cache(connection_id);

                                    // Update the editor's object type with the new name
                                    _ = editor_weak.update(cx, |editor, cx| {
                                        editor.set_object_type(
                                            EditorObjectType::edit_view(view_name.clone(), None),
                                            cx,
                                        );
                                        editor.mark_clean(cx);
                                    });

                                    _ = connection_sidebar.update(cx, |sidebar, cx| {
                                        sidebar.add_view(connection_id, view_name.clone(), cx);
                                    });
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create view: {}", e);
                                }
                            }
                        })
                        .detach();

                        true
                    })
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Open a query editor with a function definition
    pub(super) fn open_function_definition(
        &mut self,
        connection_id: Uuid,
        function_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening function definition: {} on connection {}",
            function_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let function_name_for_spawn = function_name.clone();
        let dock_area = self.dock_area.downgrade();

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the function definition
            match fetch_function_definition(&connection, &function_name_for_spawn, &driver_name)
                .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::Function {
                            name: Some(function_name_for_spawn.clone()),
                            schema: None,
                            is_new: false,
                        };

                        let query_editor = cx.new(|cx| {
                            let mut editor = QueryEditor::new_for_object(
                                function_name_for_spawn.clone(),
                                connection_id,
                                object_type,
                                Some(definition),
                                schema_service.clone(),
                                window,
                                cx,
                            );

                            editor.set_connection(
                                Some(connection_id),
                                Some(connection_name.clone()),
                                Some(connection.clone()),
                                Some(driver_name.clone()),
                                cx,
                            );

                            editor
                        });

                        // Subscribe to editor events
                        _ = this.update(cx, |main_view, cx| {
                            let subscription = cx.subscribe_in(&query_editor, window, {
                                let query_editor_weak = query_editor.downgrade();
                                move |this,
                                      _editor,
                                      event: &crate::components::QueryEditorEvent,
                                      window,
                                      cx| {
                                    this.handle_view_editor_event(
                                        event,
                                        query_editor_weak.clone(),
                                        window,
                                        cx,
                                    );
                                }
                            });
                            main_view._subscriptions.push(subscription);
                            main_view.query_editors.push(query_editor.downgrade());
                        });

                        // Add to center dock
                        let panel_view: Arc<dyn PanelView> = Arc::new(query_editor);
                        _ = dock_area.update(cx, |area, cx| {
                            area.add_panel(panel_view, DockPlacement::Center, None, window, cx);
                        });
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to fetch function definition: {}", e);
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            zqlz_ui::widgets::notification::Notification::error(format!(
                                "Failed to load function: {}",
                                e
                            )),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Open a query editor with a procedure definition
    pub(super) fn open_procedure_definition(
        &mut self,
        connection_id: Uuid,
        procedure_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening procedure definition: {} on connection {}",
            procedure_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let procedure_name_for_spawn = procedure_name.clone();
        let dock_area = self.dock_area.downgrade();

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the procedure definition
            match fetch_procedure_definition(&connection, &procedure_name_for_spawn, &driver_name)
                .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::Procedure {
                            name: Some(procedure_name_for_spawn.clone()),
                            schema: None,
                            is_new: false,
                        };

                        let query_editor = cx.new(|cx| {
                            let mut editor = QueryEditor::new_for_object(
                                procedure_name_for_spawn.clone(),
                                connection_id,
                                object_type,
                                Some(definition),
                                schema_service.clone(),
                                window,
                                cx,
                            );

                            editor.set_connection(
                                Some(connection_id),
                                Some(connection_name.clone()),
                                Some(connection.clone()),
                                Some(driver_name.clone()),
                                cx,
                            );

                            editor
                        });

                        // Subscribe to editor events
                        _ = this.update(cx, |main_view, cx| {
                            let subscription = cx.subscribe_in(&query_editor, window, {
                                let query_editor_weak = query_editor.downgrade();
                                move |this,
                                      _editor,
                                      event: &crate::components::QueryEditorEvent,
                                      window,
                                      cx| {
                                    this.handle_view_editor_event(
                                        event,
                                        query_editor_weak.clone(),
                                        window,
                                        cx,
                                    );
                                }
                            });
                            main_view._subscriptions.push(subscription);
                            main_view.query_editors.push(query_editor.downgrade());
                        });

                        // Add to center dock
                        let panel_view: Arc<dyn PanelView> = Arc::new(query_editor);
                        _ = dock_area.update(cx, |area, cx| {
                            area.add_panel(panel_view, DockPlacement::Center, None, window, cx);
                        });
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to fetch procedure definition: {}", e);
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            zqlz_ui::widgets::notification::Notification::error(format!(
                                "Failed to load procedure: {}",
                                e
                            )),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    // ============================================
    // Trigger management methods
    // ============================================

    /// Opens a QueryEditor to design/edit an existing trigger
    pub(super) fn design_trigger(
        &mut self,
        connection_id: Uuid,
        trigger_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Design trigger: {} on connection {}",
            trigger_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let trigger_name_for_spawn = trigger_name.clone();
        let dock_area = self.dock_area.downgrade();

        let editor_title = format!("[Trigger] {} ({})", trigger_name, connection_name);

        cx.spawn_in(window, async move |this, cx| {
            match fetch_trigger_definition(&connection, &trigger_name_for_spawn, &driver_name).await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type =
                            EditorObjectType::edit_trigger(trigger_name_for_spawn.clone(), None);

                        let query_editor = cx.new(|cx| {
                            let mut editor = QueryEditor::new_for_object(
                                editor_title.clone(),
                                connection_id,
                                object_type,
                                Some(definition),
                                schema_service.clone(),
                                window,
                                cx,
                            );

                            editor.set_connection(
                                Some(connection_id),
                                Some(connection_name.clone()),
                                Some(connection.clone()),
                                Some(driver_name.clone()),
                                cx,
                            );

                            editor
                        });

                        _ = this.update(cx, |main_view, cx| {
                            let subscription = cx.subscribe_in(&query_editor, window, {
                                let query_editor_weak = query_editor.downgrade();
                                move |this,
                                      _editor,
                                      event: &crate::components::QueryEditorEvent,
                                      window,
                                      cx| {
                                    this.handle_trigger_editor_event(
                                        event,
                                        query_editor_weak.clone(),
                                        window,
                                        cx,
                                    );
                                }
                            });
                            main_view._subscriptions.push(subscription);
                            main_view.query_editors.push(query_editor.downgrade());
                        });

                        if let Some(dock_area) = dock_area.upgrade() {
                            dock_area.update(cx, |area, cx| {
                                area.add_panel(
                                    Arc::new(query_editor) as Arc<dyn PanelView>,
                                    DockPlacement::Center,
                                    None,
                                    window,
                                    cx,
                                );
                            });
                        }

                        tracing::info!("Opened trigger designer for '{}'", trigger_name_for_spawn);
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to load trigger definition: {}", e);
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to load trigger: {}", e)),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Opens a new QueryEditor for creating a new trigger
    pub(super) fn new_trigger(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("New trigger on connection {}", connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let (driver_name, connection_name) = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| (c.driver.clone(), c.name.clone()))
            .unwrap_or_else(|| ("sqlite".to_string(), "Unknown".to_string()));

        let schema_service = app_state.schema_service.clone();

        let object_type = EditorObjectType::new_trigger();

        let template = if driver_name.contains("postgres") {
            r#"CREATE OR REPLACE TRIGGER trigger_name
AFTER INSERT ON table_name
FOR EACH ROW
EXECUTE FUNCTION trigger_function_name();"#
                .to_string()
        } else if driver_name.contains("mysql") {
            r#"CREATE TRIGGER trigger_name
AFTER INSERT ON table_name
FOR EACH ROW
BEGIN
    -- trigger body
END;"#
                .to_string()
        } else {
            // SQLite
            r#"CREATE TRIGGER trigger_name
AFTER INSERT ON table_name
FOR EACH ROW
BEGIN
    -- trigger body
END;"#
                .to_string()
        };

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new_for_object(
                "New Trigger".to_string(),
                connection_id,
                object_type,
                Some(template),
                schema_service.clone(),
                window,
                cx,
            );

            editor.set_connection(
                Some(connection_id),
                Some(connection_name),
                Some(connection.clone()),
                Some(driver_name),
                cx,
            );

            editor
        });

        let subscription = cx.subscribe_in(&query_editor, window, {
            let query_editor_weak = query_editor.downgrade();
            move |this, _editor, event: &crate::components::QueryEditorEvent, window, cx| {
                this.handle_trigger_editor_event(event, query_editor_weak.clone(), window, cx);
            }
        });
        self._subscriptions.push(subscription);
        self.query_editors.push(query_editor.downgrade());

        let query_editor_panel: Arc<dyn PanelView> = Arc::new(query_editor);
        self.dock_area.update(cx, |area, cx| {
            area.add_panel(query_editor_panel, DockPlacement::Center, None, window, cx);
        });

        tracing::info!("New trigger editor opened");
    }

    /// Deletes a trigger from the database
    pub(super) fn delete_trigger(
        &mut self,
        connection_id: Uuid,
        trigger_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Delete trigger: {} on connection {}",
            trigger_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let trigger_name_for_dialog = trigger_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let trigger_name = trigger_name_for_dialog.clone();
            let driver_name = driver_name.clone();

            dialog
                .title("Delete Trigger")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete trigger '{}'?",
                            trigger_name
                        )))
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let trigger_name = trigger_name.clone();
                    let driver_name = driver_name.clone();

                    cx.spawn(async move |cx| {
                        let sql = if driver_name.contains("postgres") {
                            format!(
                                "DROP TRIGGER IF EXISTS \"{}\" ON table_name CASCADE",
                                trigger_name
                            )
                        } else {
                            format!("DROP TRIGGER IF EXISTS \"{}\"", trigger_name)
                        };

                        match connection.execute(&sql, &[]).await {
                            Ok(_) => {
                                tracing::info!("Trigger '{}' deleted successfully", trigger_name);

                                _ = connection_sidebar.update(cx, |sidebar, cx| {
                                    sidebar.remove_trigger(connection_id, &trigger_name, cx);
                                });
                            }
                            Err(e) => {
                                tracing::error!("Failed to delete trigger: {}", e);
                            }
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    /// Opens the visual trigger designer panel
    pub(super) fn open_trigger_designer(
        &mut self,
        connection_id: Uuid,
        trigger_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Open trigger designer: {:?} on connection {}",
            trigger_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let dialect = if driver_name.contains("postgres") {
            TriggerDialect::Postgres
        } else if driver_name.contains("mysql") {
            TriggerDialect::Mysql
        } else {
            TriggerDialect::Sqlite
        };

        // Get available tables for the table dropdown
        let schema_service = app_state.schema_service.clone();

        if let Some(trigger_name) = trigger_name {
            // Editing an existing trigger - need to load its definition first
            let connection = connection.clone();
            let dock_area = self.dock_area.downgrade();
            let trigger_name_for_spawn = trigger_name.clone();

            cx.spawn_in(window, async move |this, cx| {
                // Fetch trigger definition
                let definition =
                    fetch_trigger_definition(&connection, &trigger_name_for_spawn, &driver_name)
                        .await;

                // Get available tables
                let tables = match schema_service
                    .load_database_schema(connection.clone(), connection_id)
                    .await
                {
                    Ok(schema) => schema.tables,
                    Err(e) => {
                        tracing::warn!("Failed to load tables for trigger designer: {}", e);
                        vec![]
                    }
                };

                cx.update(|window, cx| {
                    // Parse the trigger definition to extract details
                    // For now, create a basic design - the panel will show the raw SQL
                    let design = if let Ok(def) = &definition {
                        parse_trigger_definition(def, &trigger_name_for_spawn, dialect)
                    } else {
                        TriggerDesign::new(dialect)
                    };

                    // Create the trigger designer panel
                    let panel = cx.new(|cx| {
                        TriggerDesignerPanel::edit(
                            connection_id,
                            design,
                            tables.clone(),
                            window,
                            cx,
                        )
                    });

                    // Subscribe to trigger designer events
                    _ = this.update(cx, |main_view, cx| {
                        let panel_clone = panel.clone();
                        let subscription = cx.subscribe_in(&panel, window, {
                            move |this, _panel, event: &TriggerDesignerEvent, window, cx| {
                                this.handle_trigger_designer_event(
                                    panel_clone.clone(),
                                    event.clone(),
                                    window,
                                    cx,
                                );
                            }
                        });
                        main_view._subscriptions.push(subscription);
                    });

                    // Add to center dock
                    if let Some(dock_area) = dock_area.upgrade() {
                        dock_area.update(cx, |area, cx| {
                            area.add_panel(
                                Arc::new(panel.clone()),
                                DockPlacement::Center,
                                None,
                                window,
                                cx,
                            );
                        });
                    }

                    tracing::info!("Opened trigger designer for '{}'", trigger_name_for_spawn);
                })?;

                anyhow::Ok(())
            })
            .detach();
        } else {
            // Creating a new trigger
            let connection = connection.clone();

            cx.spawn_in(window, async move |this, cx| {
                // Get available tables
                let tables = match schema_service
                    .load_database_schema(connection.clone(), connection_id)
                    .await
                {
                    Ok(schema) => schema.tables,
                    Err(e) => {
                        tracing::warn!("Failed to load tables for trigger designer: {}", e);
                        vec![]
                    }
                };

                cx.update(|window, cx| {
                    let _design = TriggerDesign::new(dialect);

                    // Create the trigger designer panel
                    let panel = cx.new(|cx| {
                        TriggerDesignerPanel::new(
                            connection_id,
                            dialect,
                            tables.clone(),
                            window,
                            cx,
                        )
                    });

                    // Subscribe to trigger designer events
                    _ = this.update(cx, |main_view, cx| {
                        let panel_clone = panel.clone();
                        let subscription = cx.subscribe_in(&panel, window, {
                            move |this, _panel, event: &TriggerDesignerEvent, window, cx| {
                                this.handle_trigger_designer_event(
                                    panel_clone.clone(),
                                    event.clone(),
                                    window,
                                    cx,
                                );
                            }
                        });
                        main_view._subscriptions.push(subscription);
                    });

                    // Add to center dock
                    _ = this.update(cx, |main_view, cx| {
                        main_view.dock_area.update(cx, |area, cx| {
                            area.add_panel(
                                Arc::new(panel.clone()),
                                DockPlacement::Center,
                                None,
                                window,
                                cx,
                            );
                        });
                    });

                    tracing::info!("Opened new trigger designer");
                })?;

                anyhow::Ok(())
            })
            .detach();
        }
    }

    /// Handle events from the trigger designer panel
    fn handle_trigger_designer_event(
        &mut self,
        panel: Entity<TriggerDesignerPanel>,
        event: TriggerDesignerEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            TriggerDesignerEvent::Save {
                connection_id,
                design,
                is_new,
                original_name,
            } => {
                // Generate the DDL from the design
                let ddl = design.to_ddl();
                self.save_trigger_from_designer(
                    connection_id,
                    design,
                    ddl,
                    is_new,
                    original_name,
                    panel,
                    window,
                    cx,
                );
            }
            TriggerDesignerEvent::Cancel => {
                // Close the panel
                let panel_arc: Arc<dyn PanelView> = Arc::new(panel);
                self.dock_area.update(cx, |area, cx| {
                    area.remove_panel(panel_arc, DockPlacement::Center, window, cx);
                });
            }
            TriggerDesignerEvent::PreviewDdl { design: _ } => {
                // Preview is handled within the panel itself
            }
        }
    }

    /// Save a trigger from the designer (execute CREATE TRIGGER DDL)
    fn save_trigger_from_designer(
        &mut self,
        connection_id: Uuid,
        design: TriggerDesign,
        ddl: String,
        is_new: bool,
        original_name: Option<String>,
        panel: Entity<TriggerDesignerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let trigger_name = design.name.clone();
        let schema_service = app_state.schema_service.clone();

        let panel_arc: Arc<dyn PanelView> = Arc::new(panel.clone());
        let dock_area = self.dock_area.downgrade();

        cx.spawn_in(window, async move |_this, cx| {
            // If editing an existing trigger, drop it first (for SQLite/MySQL)
            // Postgres uses CREATE OR REPLACE
            if !is_new && !driver_name.contains("postgres") {
                if let Some(orig_name) = &original_name {
                    let drop_sql = format!("DROP TRIGGER IF EXISTS \"{}\"", orig_name);
                    if let Err(e) = connection.execute(&drop_sql, &[]).await {
                        tracing::warn!("Failed to drop old trigger: {}", e);
                        // Continue anyway - the create might still work
                    }
                }
            }

            // Execute the CREATE TRIGGER statement
            match connection.execute(&ddl, &[]).await {
                Ok(_) => {
                    tracing::info!("Trigger '{}' saved successfully", trigger_name);

                    schema_service.invalidate_connection_cache(connection_id);

                    cx.update(|window, cx| {
                        // Update sidebar
                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                            if is_new {
                                sidebar.add_trigger(connection_id, trigger_name.clone(), cx);
                            } else if let Some(orig_name) = &original_name {
                                if orig_name != &trigger_name {
                                    sidebar.remove_trigger(connection_id, orig_name, cx);
                                    sidebar.add_trigger(connection_id, trigger_name.clone(), cx);
                                }
                            }
                        });

                        // Show success notification
                        window.push_notification(
                            Notification::success(if is_new {
                                format!("Trigger '{}' created", trigger_name)
                            } else {
                                format!("Trigger '{}' updated", trigger_name)
                            }),
                            cx,
                        );

                        // Close the panel
                        if let Some(dock_area) = dock_area.upgrade() {
                            dock_area.update(cx, |area, cx| {
                                area.remove_panel(panel_arc, DockPlacement::Center, window, cx);
                            });
                        }
                    })?;
                }
                Err(e) => {
                    tracing::error!("Failed to save trigger: {}", e);

                    cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to save trigger: {}", e)),
                            cx,
                        );
                    })?;
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Handle events from a trigger editor (QueryEditor with EditorObjectType::Trigger)
    pub(super) fn handle_trigger_editor_event(
        &mut self,
        event: &crate::components::QueryEditorEvent,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::components::QueryEditorEvent;

        match event {
            QueryEditorEvent::SaveObject {
                connection_id,
                object_type,
                definition,
            } => {
                self.save_trigger(
                    *connection_id,
                    object_type.clone(),
                    definition.clone(),
                    editor_weak,
                    window,
                    cx,
                );
            }
            QueryEditorEvent::ExecuteQuery { sql, connection_id } => {
                tracing::info!("Executing trigger query: {}", sql);
                self.execute_view_query(sql.clone(), *connection_id, window, cx);
            }
            QueryEditorEvent::ExecuteSelection { sql, connection_id } => {
                tracing::info!("Executing trigger selection: {}", sql);
                self.execute_view_query(sql.clone(), *connection_id, window, cx);
            }
            _ => {}
        }
    }

    /// Save a trigger (execute CREATE TRIGGER)
    fn save_trigger(
        &mut self,
        connection_id: Uuid,
        object_type: EditorObjectType,
        definition: String,
        editor_weak: WeakEntity<QueryEditor>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let EditorObjectType::Trigger {
            name,
            schema: _,
            is_new,
        } = object_type
        else {
            tracing::error!("save_trigger called with non-trigger object type");
            return;
        };

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let schema_service = app_state.schema_service.clone();

        if is_new || name.is_none() {
            // For new triggers, the definition should be the full CREATE TRIGGER statement
            // Just execute it directly
            cx.spawn_in(window, async move |_this, cx| {
                match connection.execute(&definition, &[]).await {
                    Ok(_) => {
                        tracing::info!("Trigger created successfully");

                        schema_service.invalidate_connection_cache(connection_id);

                        _ = editor_weak.update(cx, |editor, cx| {
                            editor.mark_clean(cx);
                        });

                        // Try to extract trigger name from CREATE TRIGGER statement
                        let trigger_name = extract_trigger_name(&definition);
                        if let Some(name) = trigger_name {
                            _ = connection_sidebar.update(cx, |sidebar, cx| {
                                sidebar.add_trigger(connection_id, name.clone(), cx);
                            });

                            // Update editor object type with the name
                            _ = editor_weak.update(cx, |editor, cx| {
                                editor.set_object_type(
                                    EditorObjectType::edit_trigger(name, None),
                                    cx,
                                );
                            });
                        }

                        _ = cx.update(|window, cx| {
                            window.push_notification(Notification::success("Trigger created"), cx);
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to create trigger: {}", e);
                        _ = cx.update(|window, cx| {
                            window.push_notification(
                                Notification::error(format!("Failed to create trigger: {}", e)),
                                cx,
                            );
                        });
                    }
                }

                anyhow::Ok(())
            })
            .detach();
        } else {
            let trigger_name = name.unwrap();

            // For existing triggers, we need to drop and recreate
            // SQLite doesn't support CREATE OR REPLACE TRIGGER
            cx.spawn_in(window, async move |_this, cx| {
                // For non-PostgreSQL databases, drop the trigger first
                if !driver_name.contains("postgres") {
                    let drop_sql = format!("DROP TRIGGER IF EXISTS \"{}\"", trigger_name);
                    if let Err(e) = connection.execute(&drop_sql, &[]).await {
                        tracing::warn!("Failed to drop trigger before recreate: {}", e);
                    }
                }

                match connection.execute(&definition, &[]).await {
                    Ok(_) => {
                        tracing::info!("Trigger '{}' saved successfully", trigger_name);

                        schema_service.invalidate_connection_cache(connection_id);

                        _ = editor_weak.update(cx, |editor, cx| {
                            editor.mark_clean(cx);
                        });

                        _ = cx.update(|window, cx| {
                            window.push_notification(
                                Notification::success(format!("Trigger '{}' saved", trigger_name)),
                                cx,
                            );
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to save trigger: {}", e);
                        _ = cx.update(|window, cx| {
                            window.push_notification(
                                Notification::error(format!("Failed to save trigger: {}", e)),
                                cx,
                            );
                        });
                    }
                }

                anyhow::Ok(())
            })
            .detach();
        }
    }

    // ============================================
    // Multi-selection view handlers
    // ============================================

    /// Opens view designers for multiple views
    pub(super) fn design_views(
        &mut self,
        connection_id: Uuid,
        view_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        for view_name in view_names {
            self.design_view(connection_id, view_name, window, cx);
        }
    }

    /// Deletes multiple views with continue-on-error support
    pub(super) fn delete_views(
        &mut self,
        connection_id: Uuid,
        view_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use std::cell::RefCell;
        use std::rc::Rc;
        use zqlz_ui::widgets::checkbox::Checkbox;

        if view_names.is_empty() {
            return;
        }

        // For single view, use the existing dialog-based delete
        if view_names.len() == 1 {
            self.delete_view(
                connection_id,
                view_names.into_iter().next().expect("checked len == 1"),
                window,
                cx,
            );
            return;
        }

        let count = view_names.len();
        tracing::info!(
            "Delete {} views: {:?} on connection {}",
            count,
            view_names,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let schema_service = app_state.schema_service.clone();
        let continue_on_error = Rc::new(RefCell::new(false));

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let schema_service = schema_service.clone();
            let view_names = view_names.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(format!("Delete {} Views", count))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete these {} views?",
                            count
                        )))
                        .child(
                            div()
                                .text_sm()
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_color(cx.theme().muted_foreground)
                                .child(view_names.join(", ")),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
                        )
                        .child({
                            let continue_on_error = continue_on_error.clone();
                            Checkbox::new("continue-on-error")
                                .label("Continue on error")
                                .checked(false)
                                .on_click(move |checked, _window, _cx| {
                                    *continue_on_error.borrow_mut() = *checked;
                                })
                        }),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let objects_panel = objects_panel.clone();
                    let schema_service = schema_service.clone();
                    let view_names = view_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut deleted_views: Vec<String> = Vec::new();

                        for view_name in &view_names {
                            let sql = format!("DROP VIEW \"{}\"", view_name);
                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!("View '{}' deleted successfully", view_name);
                                    deleted_views.push(view_name.clone());

                                    cx.update(|cx| {
                                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                                            sidebar.remove_view(connection_id, view_name, cx);
                                        });
                                    })
;
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", view_name, e);
                                    tracing::error!("Failed to delete view {}", error_msg);

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        if !deleted_views.is_empty() {
                            schema_service.invalidate_connection_cache(connection_id);

                            cx.update(|cx| {
                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            })
;
                        }

                        if !errors.is_empty() {
                            tracing::warn!(
                                "Deleted {} of {} views. Errors: {}",
                                deleted_views.len(),
                                view_names.len(),
                                errors.join("; ")
                            );
                        } else if !deleted_views.is_empty() {
                            tracing::info!(
                                "Successfully deleted {} view(s)",
                                deleted_views.len()
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    /// Duplicates multiple views with continue-on-error support
    pub(super) fn duplicate_views(
        &mut self,
        connection_id: Uuid,
        view_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use std::cell::RefCell;
        use std::rc::Rc;
        use zqlz_ui::widgets::checkbox::Checkbox;

        if view_names.is_empty() {
            return;
        }

        // For single view, use the existing dialog-based duplicate
        if view_names.len() == 1 {
            self.duplicate_view(
                connection_id,
                view_names.into_iter().next().expect("checked len == 1"),
                window,
                cx,
            );
            return;
        }

        let count = view_names.len();
        tracing::info!(
            "Duplicate {} views: {:?} on connection {}",
            count,
            view_names,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let schema_service = app_state.schema_service.clone();
        let continue_on_error = Rc::new(RefCell::new(true));
        let new_names: Vec<String> = view_names.iter().map(|n| format!("{}_copy", n)).collect();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let schema_service = schema_service.clone();
            let view_names = view_names.clone();
            let driver_name = driver_name.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(format!("Duplicate {} Views", count))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Create copies of {} views with '_copy' suffix:",
                            count
                        )))
                        .child(
                            div()
                                .text_sm()
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_color(cx.theme().muted_foreground)
                                .child(new_names.join(", ")),
                        )
                        .child({
                            let continue_on_error = continue_on_error.clone();
                            Checkbox::new("continue-on-error")
                                .label("Continue on error")
                                .checked(true)
                                .on_click(move |checked, _window, _cx| {
                                    *continue_on_error.borrow_mut() = *checked;
                                })
                        }),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let objects_panel = objects_panel.clone();
                    let schema_service = schema_service.clone();
                    let view_names = view_names.clone();
                    let driver_name = driver_name.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut duplicated_views: Vec<String> = Vec::new();

                        for view_name in &view_names {
                            let new_name = format!("{}_copy", view_name);

                            // Fetch the source view definition, then create with new name
                            match fetch_view_definition(&connection, view_name, &driver_name).await
                            {
                                Ok(definition) => {
                                    let create_sql = format!(
                                        "CREATE VIEW \"{}\" AS {}",
                                        new_name, definition
                                    );

                                    match connection.execute(&create_sql, &[]).await {
                                        Ok(_) => {
                                            tracing::info!(
                                                "View '{}' duplicated as '{}'",
                                                view_name,
                                                new_name
                                            );
                                            duplicated_views.push(new_name.clone());

                                            cx.update(|cx| {
                                                _ = connection_sidebar
                                                    .update(cx, |sidebar, cx| {
                                                        sidebar.add_view(
                                                            connection_id,
                                                            new_name,
                                                            cx,
                                                        );
                                                    });
                                            })
;
                                        }
                                        Err(e) => {
                                            let error_msg =
                                                format!("'{}': {}", view_name, e);
                                            tracing::error!(
                                                "Failed to duplicate view {}",
                                                error_msg
                                            );

                                            if continue_on_error {
                                                errors.push(error_msg);
                                            } else {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", view_name, e);
                                    tracing::error!(
                                        "Failed to fetch view definition {}",
                                        error_msg
                                    );

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        if !duplicated_views.is_empty() {
                            schema_service.invalidate_connection_cache(connection_id);

                            cx.update(|cx| {
                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            })
;
                        }

                        if !errors.is_empty() {
                            tracing::warn!(
                                "Duplicated {} of {} views. Errors: {}",
                                duplicated_views.len(),
                                view_names.len(),
                                errors.join("; ")
                            );
                        } else if !duplicated_views.is_empty() {
                            tracing::info!(
                                "Duplicated {} view(s)",
                                duplicated_views.len()
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    /// Copies multiple view names to clipboard
    pub(super) fn copy_view_names(&mut self, view_names: &[String], cx: &mut Context<Self>) {
        let text = view_names.join("\n");
        tracing::info!("Copy {} view name(s) to clipboard", view_names.len());
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
    }
}

/// Parses a trigger definition SQL into a TriggerDesign structure.
/// This is a best-effort parser - complex triggers may not parse fully.
fn parse_trigger_definition(
    sql: &str,
    trigger_name: &str,
    dialect: TriggerDialect,
) -> TriggerDesign {
    use zqlz_trigger_designer::{TriggerEvent, TriggerTiming};

    let mut design = TriggerDesign::new(dialect);
    design.name = trigger_name.to_string();
    design.is_new = false;
    design.body = sql.to_string();

    let sql_upper = sql.to_uppercase();

    // Try to parse timing
    if sql_upper.contains("BEFORE") {
        design.timing = TriggerTiming::Before;
    } else if sql_upper.contains("AFTER") {
        design.timing = TriggerTiming::After;
    } else if sql_upper.contains("INSTEAD OF") {
        design.timing = TriggerTiming::InsteadOf;
    }

    // Try to parse events (could be multiple for Postgres)
    let mut events = Vec::new();
    if sql_upper.contains("INSERT") {
        events.push(TriggerEvent::Insert);
    }
    if sql_upper.contains("UPDATE") {
        events.push(TriggerEvent::Update);
    }
    if sql_upper.contains("DELETE") {
        events.push(TriggerEvent::Delete);
    }
    if !events.is_empty() {
        design.events = events;
    }

    // Try to parse table name - look for "ON table_name" pattern
    if let Some(on_pos) = sql_upper.find(" ON ") {
        let after_on = &sql[on_pos + 4..];
        // Find the end of the table name (space, newline, or FOR)
        let end_pos = after_on
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after_on.len());
        let table_name = after_on[..end_pos]
            .trim()
            .trim_matches('"')
            .trim_matches('`');
        design.table_name = table_name.to_string();
    }

    // Try to parse FOR EACH ROW/STATEMENT
    design.for_each_row = sql_upper.contains("FOR EACH ROW");

    design
}

/// Fetches the definition of a trigger from the database.
async fn fetch_trigger_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    trigger_name: &str,
    driver_type: &str,
) -> Result<String, String> {
    let sql = if driver_type.contains("postgres") {
        format!(
            r#"
            SELECT pg_get_triggerdef(t.oid, true)
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            WHERE t.tgname = '{}'
            LIMIT 1
            "#,
            trigger_name.replace("'", "''")
        )
    } else if driver_type.contains("mysql") {
        format!("SHOW CREATE TRIGGER `{}`", trigger_name.replace("`", "``"))
    } else {
        // SQLite
        format!(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = '{}'",
            trigger_name.replace("'", "''")
        )
    };

    match connection.query(&sql, &[]).await {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                // For MySQL SHOW CREATE TRIGGER, the SQL is in the 3rd column
                let value = if driver_type.contains("mysql") {
                    row.values.get(2).map(|v| v.to_string())
                } else {
                    row.values.first().map(|v| v.to_string())
                };

                if let Some(definition) = value {
                    return Ok(definition);
                }
            }
            Err(format!("Trigger '{}' not found", trigger_name))
        }
        Err(e) => Err(format!("Failed to fetch trigger definition: {}", e)),
    }
}

/// Extracts trigger name from a CREATE TRIGGER statement
fn extract_trigger_name(definition: &str) -> Option<String> {
    let upper = definition.to_uppercase();
    let pos = upper.find("CREATE TRIGGER")?;
    let after_create = &definition[pos + 14..];
    let trimmed = after_create.trim_start();

    // Handle "OR REPLACE" for PostgreSQL
    let trimmed = if trimmed.to_uppercase().starts_with("OR REPLACE") {
        trimmed[10..].trim_start()
    } else if trimmed.to_uppercase().starts_with("IF NOT EXISTS") {
        trimmed[13..].trim_start()
    } else {
        trimmed
    };

    // Extract the name (handle quoted identifiers)
    if trimmed.starts_with('"') {
        let end = trimmed[1..].find('"')?;
        Some(trimmed[1..end + 1].to_string())
    } else if trimmed.starts_with('`') {
        let end = trimmed[1..].find('`')?;
        Some(trimmed[1..end + 1].to_string())
    } else if trimmed.starts_with('[') {
        let end = trimmed[1..].find(']')?;
        Some(trimmed[1..end + 1].to_string())
    } else {
        // Unquoted identifier - ends at whitespace
        let end = trimmed.find(char::is_whitespace)?;
        Some(trimmed[..end].to_string())
    }
}
