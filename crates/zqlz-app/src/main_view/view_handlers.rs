// View management methods for MainView
//
// This module handles database view operations: design, create, delete, duplicate, rename.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_connection::SidebarSection;
use zqlz_core::{
    ObjectType, connection_is_mysql_compatible, connection_is_postgres, validate_view_name,
};
use zqlz_objects::{
    DropTriggerStatementRequest, ObjectDefinitionRequest,
    build_drop_trigger_statement as build_drop_trigger_statement_for_object,
    build_drop_view_statement as build_drop_view_statement_for_object, build_duplicate_view_sql,
    extract_trigger_name_from_create_statement,
    extract_view_name_from_create_view as extract_view_name_from_create_view_from_objects,
    fetch_object_definition, fetch_trigger_definition as fetch_trigger_definition_from_objects,
    plan_trigger_replace_execution, plan_view_save_execution,
};
use zqlz_query::EditorObjectType;
use zqlz_query::{
    QueryConnectionCandidate, QueryDisplayContext, resolve_query_connection_selection,
    run_execute_query_workflow,
};
use zqlz_sequence_designer::{SequenceDesign, SequenceDesignerEvent, SequenceDesignerPanel};
use zqlz_trigger_designer::{
    DatabaseDialect as TriggerDialect, TriggerDesign, TriggerDesignerEvent, TriggerDesignerPanel,
};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    dock::PanelView,
    input::{Input, InputState},
    notification::Notification,
    v_flex,
};
use zqlz_versioning::{DatabaseObjectType, VersionRepository};

use crate::app::AppState;
use crate::components::QueryEditor;
use crate::workspace_state::RefreshScope;
use zqlz_services::SchemaService;
use zqlz_text_editor::{DocumentIdentity, TextDocument};

use super::MainView;
use super::rename_window::RenameWindow;

/// Fetches the full CREATE VIEW DDL of a view from the database.
pub(in crate::main_view) async fn fetch_view_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    view_name: &str,
) -> Result<String, String> {
    fetch_database_object_definition(connection, schema_name, view_name, ObjectType::View, None)
        .await
        .map_err(|error| format!("Failed to fetch view definition: {}", error))
}

fn normalize_schema_name(schema_name: Option<&str>) -> Option<String> {
    schema_name
        .map(str::trim)
        .filter(|schema| !schema.is_empty() && *schema != "main")
        .map(ToOwned::to_owned)
}

fn editor_object_type_to_version_type(
    object_type: &EditorObjectType,
) -> Option<DatabaseObjectType> {
    match object_type {
        EditorObjectType::View { .. } => Some(DatabaseObjectType::View),
        EditorObjectType::Function { .. } => Some(DatabaseObjectType::Function),
        EditorObjectType::Procedure { .. } => Some(DatabaseObjectType::Procedure),
        EditorObjectType::Trigger { .. } => Some(DatabaseObjectType::Trigger),
        EditorObjectType::Query => None,
    }
}

fn build_sql_object_version_message(
    object_type: DatabaseObjectType,
    object_name: &str,
    is_new: bool,
) -> String {
    let action = if is_new { "Create" } else { "Update" };
    format!(
        "{} {} {}",
        action,
        object_type.display_name().to_lowercase(),
        object_name
    )
}

fn version_target_from_editor_object(
    connection_id: Uuid,
    object_type: &EditorObjectType,
) -> Option<SqlObjectVersionTarget> {
    let object_name = object_type.object_name()?.to_string();
    let object_schema = match object_type {
        EditorObjectType::View { schema, .. }
        | EditorObjectType::Function { schema, .. }
        | EditorObjectType::Procedure { schema, .. }
        | EditorObjectType::Trigger { schema, .. } => normalize_schema_name(schema.as_deref()),
        EditorObjectType::Query => None,
    };

    Some(SqlObjectVersionTarget {
        connection_id,
        object_type: editor_object_type_to_version_type(object_type)?,
        object_name,
        object_schema,
    })
}

fn is_postgres_dialect(connection: &Arc<dyn zqlz_core::Connection>) -> bool {
    connection_is_postgres(connection.as_ref())
}

pub(in crate::main_view) fn build_drop_view_statement(
    connection: &Arc<dyn zqlz_core::Connection>,
    view_name: &str,
    include_if_exists: bool,
) -> String {
    build_drop_view_statement_for_object(connection, view_name, include_if_exists).unwrap_or_else(
        |error| {
            tracing::error!(
                view = %view_name,
                %error,
                "Failed to build drop view SQL via driver"
            );
            String::new()
        },
    )
}

fn trigger_dialect_for_connection(connection: &Arc<dyn zqlz_core::Connection>) -> TriggerDialect {
    if is_postgres_dialect(connection) {
        TriggerDialect::Postgres
    } else if connection_is_mysql_compatible(connection.as_ref()) {
        TriggerDialect::Mysql
    } else {
        TriggerDialect::Sqlite
    }
}

fn record_sql_object_version(
    version_repository: &VersionRepository,
    target: &SqlObjectVersionTarget,
    content: String,
    message: String,
) -> anyhow::Result<()> {
    version_repository.commit(
        target.connection_id,
        target.object_type,
        target.object_schema.clone(),
        target.object_name.clone(),
        content,
        message,
    )?;
    Ok(())
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        current.push(ch);
        if ch == '\'' {
            if in_single_quote && chars.peek() == Some(&'\'') {
                current.push(chars.next().unwrap_or('\''));
                continue;
            }
            in_single_quote = !in_single_quote;
        } else if ch == ';' && !in_single_quote {
            let statement = current.trim();
            if !statement.is_empty() {
                statements.push(statement.to_string());
            }
            current.clear();
        }
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(format!("{};", statement));
    }

    statements
}

/// Fetches the definition of a function from the database.
async fn fetch_function_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    function_name: &str,
    signature: Option<&str>,
) -> Result<String, String> {
    fetch_database_object_definition(
        connection,
        schema_name,
        function_name,
        ObjectType::Function,
        signature,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Fetches the definition of a stored procedure from the database.
async fn fetch_procedure_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    procedure_name: &str,
    signature: Option<&str>,
) -> Result<String, String> {
    fetch_database_object_definition(
        connection,
        schema_name,
        procedure_name,
        ObjectType::Procedure,
        signature,
    )
    .await
    .map_err(|error| error.to_string())
}

async fn fetch_sequence_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    sequence_name: &str,
) -> Result<String, String> {
    if connection_is_postgres(connection.as_ref()) {
        return fetch_sequence_design(connection, schema_name, sequence_name)
            .await
            .map(|design| design.to_ddl());
    }

    fetch_database_object_definition(
        connection,
        schema_name,
        sequence_name,
        ObjectType::Sequence,
        None,
    )
    .await
    .map_err(|error| error.to_string())
}

fn build_drop_extension_statement(
    connection: &Arc<dyn zqlz_core::Connection>,
    extension_name: &str,
) -> String {
    format!(
        "DROP EXTENSION {};",
        connection.quote_identifier(extension_name)
    )
}

async fn fetch_sequence_design(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    sequence_name: &str,
) -> Result<SequenceDesign, String> {
    if !connection_is_postgres(connection.as_ref()) {
        return Err("Sequence designer is only available for PostgreSQL".to_string());
    }

    let schema = schema_name.unwrap_or("public");
    let result = connection
        .query(
            "SELECT s.sequenceowner, s.data_type::text, s.start_value, s.min_value, s.max_value,
                    s.increment_by, s.cycle, s.cache_size, s.last_value,
                    dep_ns.nspname, dep_cls.relname, dep_att.attname,
                    obj_description(seq_cls.oid, 'pg_class')
             FROM pg_catalog.pg_sequences s
             JOIN pg_catalog.pg_class seq_cls ON seq_cls.relname = s.sequencename
             JOIN pg_catalog.pg_namespace seq_ns
                  ON seq_ns.oid = seq_cls.relnamespace AND seq_ns.nspname = s.schemaname
             LEFT JOIN pg_catalog.pg_depend dep
                  ON dep.objid = seq_cls.oid AND dep.deptype = 'a'
             LEFT JOIN pg_catalog.pg_class dep_cls ON dep_cls.oid = dep.refobjid
             LEFT JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep_cls.relnamespace
             LEFT JOIN pg_catalog.pg_attribute dep_att
                  ON dep_att.attrelid = dep_cls.oid AND dep_att.attnum = dep.refobjsubid
             WHERE s.schemaname = $1 AND s.sequencename = $2
             LIMIT 1",
            &[
                zqlz_core::Value::String(schema.to_string()),
                zqlz_core::Value::String(sequence_name.to_string()),
            ],
        )
        .await
        .map_err(|error| error.to_string())?;

    let row = result
        .rows
        .first()
        .ok_or_else(|| format!("Sequence '{}.{}' not found", schema, sequence_name))?;

    let owned_by_table = row.get(10).and_then(|value| value.as_str()).map(|table| {
        row.get(9)
            .and_then(|value| value.as_str())
            .map(|schema| format!("{}.{}", schema, table))
            .unwrap_or_else(|| table.to_string())
    });

    Ok(SequenceDesign {
        schema: Some(schema.to_string()),
        name: sequence_name.to_string(),
        owner: row
            .get(0)
            .and_then(|value| value.as_str())
            .map(str::to_string),
        data_type: row
            .get(1)
            .and_then(|value| value.as_str())
            .unwrap_or("bigint")
            .to_string(),
        start_value: row.get(2).and_then(|value| value.as_i64()).unwrap_or(1),
        min_value: row.get(3).and_then(|value| value.as_i64()),
        max_value: row.get(4).and_then(|value| value.as_i64()),
        increment_by: row.get(5).and_then(|value| value.as_i64()).unwrap_or(1),
        cycle: row
            .get(6)
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        cache_size: row.get(7).and_then(|value| value.as_i64()).unwrap_or(1),
        current_value: row.get(8).and_then(|value| value.as_i64()),
        owned_by_table,
        owned_by_column: row
            .get(11)
            .and_then(|value| value.as_str())
            .map(str::to_string),
        comment: row
            .get(12)
            .and_then(|value| value.as_str())
            .map(str::to_string),
        is_new: false,
    })
}

async fn fetch_database_object_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    object_name: &str,
    object_type: ObjectType,
    signature: Option<&str>,
) -> Result<String, String> {
    fetch_object_definition(
        connection,
        &ObjectDefinitionRequest::new(object_type, object_name)
            .with_schema(normalize_schema_name(schema_name))
            .with_signature(signature.map(ToOwned::to_owned)),
    )
    .await
    .map_err(|error| error.to_string())
}

struct ObjectEditorConnection {
    connection_id: Uuid,
    connection_name: String,
    connection: Arc<dyn zqlz_core::Connection>,
    driver_name: String,
    database_name: Option<String>,
}

struct ObjectEditorDefinition {
    display_name: String,
    object_type: EditorObjectType,
    initial_content: Option<String>,
    schema_service: Arc<SchemaService>,
}

#[derive(Clone)]
struct SqlObjectVersionTarget {
    connection_id: Uuid,
    object_type: DatabaseObjectType,
    object_name: String,
    object_schema: Option<String>,
}

struct TriggerDesignerSaveRequest {
    connection_id: Uuid,
    design: TriggerDesign,
    ddl: String,
    is_new: bool,
    object_schema: Option<String>,
    original_name: Option<String>,
    panel: Entity<TriggerDesignerPanel>,
}

impl MainView {
    fn document_first_object_editor(
        definition: ObjectEditorDefinition,
        connection: ObjectEditorConnection,
        window: &mut Window,
        cx: &mut Context<QueryEditor>,
    ) -> QueryEditor {
        let document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal document uri"),
            definition.initial_content.as_deref().unwrap_or(""),
        );
        let mut editor = QueryEditor::new_for_object_with_document(
            definition.display_name,
            connection.connection_id,
            definition.object_type,
            document,
            definition.schema_service,
            window,
            cx,
        );

        editor.set_connection(
            Some(connection.connection_id),
            Some(connection.connection_name),
            Some(connection.connection),
            Some(connection.driver_name),
            cx,
        );
        editor.set_current_database(connection.database_name, cx);

        editor
    }

    fn open_workspace_object_editor(
        &mut self,
        definition: ObjectEditorDefinition,
        connection: ObjectEditorConnection,
        event_handler: fn(
            &mut MainView,
            &crate::components::QueryEditorEvent,
            WeakEntity<QueryEditor>,
            &mut Window,
            &mut Context<Self>,
        ),
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let editor_id = self.create_workspace_editor(
            Some(connection.connection_id),
            definition.display_name.clone(),
            cx,
        );
        let query_editor =
            cx.new(|cx| Self::document_first_object_editor(definition, connection, window, cx));

        let subscription = cx.subscribe_in(&query_editor, window, {
            let query_editor_weak = query_editor.downgrade();
            move |this, _editor, event: &crate::components::QueryEditorEvent, window, cx| {
                event_handler(this, event, query_editor_weak.clone(), window, cx);

                if matches!(
                    event,
                    crate::components::QueryEditorEvent::DocumentStateChanged
                ) {
                    let Some(editor) = query_editor_weak.upgrade() else {
                        return;
                    };

                    let document_context = editor.read(cx).document_context(cx);
                    let is_dirty = editor.read(cx).is_dirty(cx);
                    let display_name = editor.read(cx).name();
                    let draft_text = editor.read(cx).content(cx).to_string();
                    this.refresh_workspace_document_state(
                        editor_id,
                        document_context,
                        is_dirty,
                        display_name,
                        Some(draft_text),
                        cx,
                    );
                    this.pin_dirty_preview_tab(is_dirty, cx);
                    this.refresh_workspace_window_title(window, cx);
                }
            }
        });

        self._subscriptions.push(subscription);
        self.query_editors.push(query_editor.downgrade());

        let document_context = query_editor.read(cx).document_context(cx);
        let is_dirty = query_editor.read(cx).is_dirty(cx);
        let current_display_name = query_editor.read(cx).name();
        let draft_text = query_editor.read(cx).content(cx).to_string();
        self.refresh_workspace_document_state(
            editor_id,
            document_context,
            is_dirty,
            current_display_name,
            Some(draft_text),
            cx,
        );

        let query_editor_panel: Arc<dyn PanelView> = Arc::new(query_editor);
        self.workspace_controller.update(cx, |workspace, cx| {
            workspace.add_center_item(query_editor_panel, window, cx);
        });
    }

    /// Opens a QueryEditor to design/edit an existing view
    pub(super) fn design_view(
        &mut self,
        connection_id: Uuid,
        view_name: String,
        object_schema: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Design view: {} on connection {}", view_name, connection_id);
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        // Get the driver name and connection name for dialect-specific queries
        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let view_name_for_spawn = view_name.clone();
        // Format the editor title to include [View] indicator and connection name
        let editor_title = format!("[View] {} ({})", view_name, connection_name);

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the view definition
            match fetch_view_definition(&connection, object_schema.as_deref(), &view_name_for_spawn)
                .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::edit_view(
                            view_name_for_spawn.clone(),
                            object_schema.clone(),
                        );

                        _ = this.update(cx, |main_view, cx| {
                            main_view.open_workspace_object_editor(
                                ObjectEditorDefinition {
                                    display_name: editor_title.clone(),
                                    object_type,
                                    initial_content: Some(definition),
                                    schema_service: schema_service.clone(),
                                },
                                ObjectEditorConnection {
                                    connection_id,
                                    connection_name: connection_name.clone(),
                                    connection: connection.clone(),
                                    driver_name: driver_name.clone(),
                                    database_name: target_database.clone(),
                                },
                                MainView::handle_view_editor_event,
                                window,
                                cx,
                            );
                        });

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

    /// Opens a new QueryEditor for creating a new view using full CREATE VIEW DDL
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

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let schema_service = app_state.schema_service.clone();

        let object_type = EditorObjectType::new_view();

        self.open_workspace_object_editor(
            ObjectEditorDefinition {
                display_name: "New View".to_string(),
                object_type,
                initial_content: Some(
                    "CREATE VIEW view_name AS\nSELECT * FROM table_name;".to_string(),
                ),
                schema_service,
            },
            ObjectEditorConnection {
                connection_id,
                connection_name,
                connection: connection.clone(),
                driver_name,
                database_name: target_database,
            },
            MainView::handle_view_editor_event,
            window,
            cx,
        );

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

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let view_name_for_dialog = view_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
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
                        // This styles the shared dialog's OK action, not a concrete Button, so it
                        // must stay as a ButtonVariant to mark the action as destructive.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let main_view = main_view.clone();
                    let view_name = view_name.clone();

                    cx.spawn(async move |cx| {
                        let sql = build_drop_view_statement(&connection, &view_name, false);
                        if sql.is_empty() {
                            return;
                        }
                        match connection.execute(&sql, &[]).await {
                            Ok(_) => {
                                tracing::info!("View '{}' deleted successfully", view_name);

                                if let Err(error) =
                                    cx.update_window(window_handle, |_, _window, cx| {
                                        if let Err(error) = main_view.update(cx, |main_view, cx| {
                                            main_view.request_refresh(
                                                RefreshScope::ConnectionSurfaces(connection_id),
                                                cx,
                                            );
                                        }) {
                                            tracing::error!(
                                                %error,
                                                "Failed to refresh after deleting extension"
                                            );
                                        }
                                    })
                                {
                                    tracing::error!(
                                        %error,
                                        "Failed to update window after deleting extension"
                                    );
                                }
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

    pub(super) fn delete_extension(
        &mut self,
        connection_id: Uuid,
        extension_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Delete extension: {} on connection {}",
            extension_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let extension_name_for_dialog = extension_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let extension_name = extension_name_for_dialog.clone();

            dialog
                .title("Delete Extension")
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete extension '{}'?",
                            extension_name
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
                    let main_view = main_view.clone();
                    let extension_name = extension_name.clone();

                    cx.spawn(async move |cx| {
                        let sql = build_drop_extension_statement(&connection, &extension_name);
                        match connection.execute(&sql, &[]).await {
                            Ok(_) => {
                                tracing::info!(
                                    "Extension '{}' deleted successfully",
                                    extension_name
                                );

                                let _ = cx.update_window(window_handle, |_, _window, cx| {
                                    let _ = main_view.update(cx, |main_view, cx| {
                                        main_view.request_refresh(
                                            RefreshScope::ConnectionSurfaces(connection_id),
                                            cx,
                                        );
                                    });
                                });
                            }
                            Err(error) => {
                                tracing::error!("Failed to delete extension: {}", error);
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

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
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
                let window_handle = window_handle;
                let main_view = main_view.clone();
                let source_view_name = source_view_name.clone();
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
                                        this.text_color(cx.theme().danger_text).child(err)
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
                        let window_handle = window_handle;
                        let main_view = main_view.clone();
                        let source_view_name = source_view_name.clone();

                        cx.spawn(async move |cx| {
                            match build_duplicate_view_sql(
                                &connection,
                                &zqlz_objects::DuplicateViewRequest::new(
                                    &source_view_name,
                                    &new_view_name,
                                ),
                            )
                            .await
                            {
                                Ok(create_sql) => match connection.execute(&create_sql, &[]).await {
                                    Ok(_) => {
                                        tracing::info!(
                                            "View '{}' duplicated as '{}'",
                                            source_view_name,
                                            new_view_name
                                        );

                                        // Invalidate schema cache so refresh works correctly
                                        schema_service.invalidate_connection_cache(connection_id);

                                        let _ = cx.update_window(window_handle, |_, _window, cx| {
                                            let _ = main_view.update(cx, |main_view, cx| {
                                                main_view.request_refresh(
                                                    RefreshScope::ConnectionSurfaces(connection_id),
                                                    cx,
                                                );
                                            });
                                        });
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to create duplicated view: {}", e);
                                    }
                                },
                                Err(e) => {
                                    tracing::error!("Failed to duplicate view '{}': {}", source_view_name, e);
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
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Rename view: {} on connection {}", view_name, connection_id);

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());

        RenameWindow::open_view(
            connection_id,
            view_name,
            driver_name,
            connection.clone(),
            cx.entity().downgrade(),
            cx,
        );
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
            } => match object_type {
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
                    tracing::warn!(
                        "SaveObject event for Query type — should use SaveQuery instead"
                    );
                }
            },
            QueryEditorEvent::PreviewDdl {
                object_type,
                definition,
            } => {
                self.show_query_editor_ddl_preview(object_type, definition, window, cx);
            }
            // For standard query execution events, delegate to the normal query handler
            QueryEditorEvent::ExecuteQuery {
                sql,
                connection_id,
                database_name,
                ..
            } => {
                tracing::info!("Executing view query: {}", sql);
                self.execute_view_query(
                    sql.clone(),
                    *connection_id,
                    database_name.clone(),
                    window,
                    cx,
                );
            }
            QueryEditorEvent::ExecuteSelection {
                sql,
                connection_id,
                database_name,
                ..
            } => {
                tracing::info!("Executing view selection: {}", sql);
                self.execute_view_query(
                    sql.clone(),
                    *connection_id,
                    database_name.clone(),
                    window,
                    cx,
                );
            }
            // Other events can be handled as needed
            _ => {}
        }
    }

    /// Execute a query from a view editor
    fn execute_view_query(
        &mut self,
        sql: String,
        connection_id: Option<Uuid>,
        database_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let candidates: Vec<QueryConnectionCandidate> = app_state
            .connection_service
            .list_saved_connections()
            .into_iter()
            .map(|saved| QueryConnectionCandidate {
                connection_id: saved.id,
                connection_name: saved.name,
                driver_name: saved.driver,
                params: saved.params,
            })
            .collect();
        let Some(selection) = resolve_query_connection_selection(connection_id, &candidates) else {
            tracing::warn!("No connection for view query");
            return;
        };
        let conn_id = selection.connection_id;

        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(conn_id, database_name.as_deref())
            .or_else(|| app_state.connection_service.get_connection(conn_id))
        else {
            tracing::error!("Connection not found: {}", conn_id);
            return;
        };

        let query_service = app_state.query_service.clone();
        let results_panel = self.results_panel.clone();
        let connection = connection.clone();
        let connection_name = Some(selection.connection_name);
        let database_name = database_name.or(selection.default_database_name);

        results_panel.update(cx, |panel, cx| {
            panel.set_loading(true, cx);
        });

        cx.spawn_in(window, async move |this, cx| {
            let query_outcome = run_execute_query_workflow(
                query_service.as_ref(),
                connection,
                conn_id,
                sql,
                None,
                QueryDisplayContext {
                    connection_name,
                    database_name,
                },
                chrono::Utc::now(),
            )
            .await;

            if let Err(error) = results_panel.update_in(cx, |panel, window, cx| {
                panel.set_execution(query_outcome.execution, window, cx);
            }) {
                tracing::warn!(%error, "failed to update results panel after view query execution");
            }

            if let Err(error) = this.update(cx, |view, cx| {
                view.refresh_query_history(cx);
            }) {
                tracing::warn!(%error, "failed to refresh query history after view query execution");
            }

            anyhow::Ok(())
        })
        .detach();
    }

    /// Save a view by executing full CREATE VIEW DDL from the editor
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
            schema,
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

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let schema_service = app_state.schema_service.clone();
        let version_repository = self.version_repository.clone();
        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();

        let (view_schema_from_ddl, view_name_from_ddl) =
            match extract_view_name_from_create_view_from_objects(definition.trim()) {
                Some(parsed) => parsed,
                None => {
                    window.push_notification(
                        Notification::error(
                            "View save expects full CREATE VIEW DDL in the editor content",
                        ),
                        cx,
                    );
                    return;
                }
            };

        let (view_name, object_schema) = if is_new {
            (
                view_name_from_ddl,
                normalize_schema_name(view_schema_from_ddl.as_deref()),
            )
        } else {
            let Some(existing_view_name) = name else {
                tracing::error!("save_view called for existing view without a name");
                return;
            };

            (
                existing_view_name,
                normalize_schema_name(schema.as_deref()).or(view_schema_from_ddl),
            )
        };

        let execution_plan = match plan_view_save_execution(
            &connection,
            &zqlz_objects::ViewSaveExecutionRequest::new(&view_name, definition, is_new),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                window.push_notification(
                    Notification::error(format!("Failed to prepare view save: {}", error)),
                    cx,
                );
                return;
            }
        };

        let version_target = SqlObjectVersionTarget {
            connection_id,
            object_type: DatabaseObjectType::View,
            object_name: view_name.clone(),
            object_schema: object_schema.clone(),
        };

        cx.spawn_in(window, async move |_this, cx| {
            if let Some(drop_statement) = &execution_plan.drop_statement
                && let Err(error) = connection.execute(drop_statement, &[]).await
            {
                tracing::error!(
                    view = %view_name,
                    %error,
                    "Failed to drop existing view before save"
                );
                _ = cx.update(|window, cx| {
                    window.push_notification(
                        Notification::error(format!("Failed to save view '{}': {}", view_name, error)),
                        cx,
                    );
                });
                return anyhow::Ok(());
            }

            match connection
                .execute(execution_plan.executable_definition.trim(), &[])
                .await
            {
                Ok(_) => {
                    tracing::info!("View '{}' saved successfully", view_name);

                    let version_message = build_sql_object_version_message(
                        DatabaseObjectType::View,
                        &view_name,
                        is_new,
                    );
                    if let Err(error) = record_sql_object_version(
                        &version_repository,
                        &version_target,
                        execution_plan.stored_definition.clone(),
                        version_message,
                    ) {
                        tracing::error!(%error, view = %view_name, "Failed to store view version snapshot");
                    }

                    schema_service.invalidate_connection_cache(connection_id);

                    _ = editor_weak.update(cx, |editor, cx| {
                        if is_new {
                            editor.set_object_type(
                                EditorObjectType::edit_view(view_name.clone(), object_schema.clone()),
                                cx,
                            );
                        }
                        editor.mark_clean(cx);
                    });

                    _ = connection_sidebar.update(cx, |sidebar, cx| {
                        sidebar.clear_section_loading(connection_id, SidebarSection::Views, cx);
                    });

                    _ = _this.update(cx, |main_view, cx| {
                        main_view.request_refresh(
                            RefreshScope::ConnectionSurfaces(connection_id),
                            cx,
                        );
                    });

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::success(format!("View '{}' saved", view_name)),
                            cx,
                        );
                    });
                }
                Err(error) => {
                    tracing::error!(view = %view_name, %error, "Failed to save view");
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!(
                                "Failed to save view '{}': {}",
                                view_name, error
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
        let object_name = object_type.object_name().unwrap_or("unnamed").to_string();

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let editor_database = editor_weak
            .upgrade()
            .and_then(|editor| editor.read(cx).current_database());
        let target_database = editor_database.or_else(|| {
            self.workspace_state
                .read(cx)
                .active_database()
                .map(ToString::to_string)
        });
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let version_repository = self.version_repository.clone();
        let version_target = version_target_from_editor_object(connection_id, &object_type);
        let is_new_object = object_type.is_new();

        cx.spawn_in(window, async move |_this, cx| {
            // The editor content is the full DDL (e.g. CREATE OR REPLACE FUNCTION ...)
            match connection.execute(definition.trim(), &[]).await {
                Ok(_) => {
                    tracing::info!("{} '{}' saved successfully", type_name, object_name);

                    if let Some(version_target) = &version_target {
                        let version_message = build_sql_object_version_message(
                            version_target.object_type,
                            &version_target.object_name,
                            is_new_object,
                        );
                        if let Err(error) = record_sql_object_version(
                            &version_repository,
                            version_target,
                            definition.clone(),
                            version_message,
                        ) {
                            tracing::error!(%error, object = %object_name, "Failed to store database object version snapshot");
                        }
                    }

                    schema_service.invalidate_connection_cache(connection_id);

                    _ = editor_weak.update(cx, |editor, cx| {
                        editor.mark_clean(cx);
                    });

                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::success(format!("{} '{}' saved", type_name, object_name)),
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

    /// Open a query editor with a function definition
    pub(super) fn open_function_definition(
        &mut self,
        connection_id: Uuid,
        function_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
        signature: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening function definition: {} on connection {}",
            function_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = database_name.or_else(|| {
            self.workspace_state
                .read(cx)
                .active_database()
                .map(ToString::to_string)
        });
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let function_name_for_spawn = function_name.clone();
        let signature_for_spawn = signature.clone();

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the function definition
            match fetch_function_definition(
                &connection,
                object_schema.as_deref(),
                &function_name_for_spawn,
                signature_for_spawn.as_deref(),
            )
            .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::Function {
                            name: Some(function_name_for_spawn.clone()),
                            schema: object_schema.clone(),
                            is_new: false,
                        };

                        _ = this.update(cx, |main_view, cx| {
                            main_view.open_workspace_object_editor(
                                ObjectEditorDefinition {
                                    display_name: function_name_for_spawn.clone(),
                                    object_type,
                                    initial_content: Some(definition),
                                    schema_service: schema_service.clone(),
                                },
                                ObjectEditorConnection {
                                    connection_id,
                                    connection_name: connection_name.clone(),
                                    connection: connection.clone(),
                                    driver_name: driver_name.clone(),
                                    database_name: target_database.clone(),
                                },
                                MainView::handle_view_editor_event,
                                window,
                                cx,
                            );
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
        object_schema: Option<String>,
        database_name: Option<String>,
        signature: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening procedure definition: {} on connection {}",
            procedure_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = database_name.or_else(|| {
            self.workspace_state
                .read(cx)
                .active_database()
                .map(ToString::to_string)
        });
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let procedure_name_for_spawn = procedure_name.clone();
        let signature_for_spawn = signature.clone();

        cx.spawn_in(window, async move |this, cx| {
            // Fetch the procedure definition
            match fetch_procedure_definition(
                &connection,
                object_schema.as_deref(),
                &procedure_name_for_spawn,
                signature_for_spawn.as_deref(),
            )
            .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::Procedure {
                            name: Some(procedure_name_for_spawn.clone()),
                            schema: object_schema.clone(),
                            is_new: false,
                        };

                        _ = this.update(cx, |main_view, cx| {
                            main_view.open_workspace_object_editor(
                                ObjectEditorDefinition {
                                    display_name: procedure_name_for_spawn.clone(),
                                    object_type,
                                    initial_content: Some(definition),
                                    schema_service: schema_service.clone(),
                                },
                                ObjectEditorConnection {
                                    connection_id,
                                    connection_name: connection_name.clone(),
                                    connection: connection.clone(),
                                    driver_name: driver_name.clone(),
                                    database_name: target_database.clone(),
                                },
                                MainView::handle_view_editor_event,
                                window,
                                cx,
                            );
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

    pub(super) fn open_sequence_definition(
        &mut self,
        connection_id: Uuid,
        sequence_name: String,
        object_schema: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Opening sequence definition: {} on connection {}",
            sequence_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let sequence_name_for_spawn = sequence_name.clone();
        let editor_title = format!("[Sequence] {}", sequence_name);

        cx.spawn_in(window, async move |this, cx| {
            match fetch_sequence_definition(
                &connection,
                object_schema.as_deref(),
                &sequence_name_for_spawn,
            )
            .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        _ = this.update(cx, |main_view, cx| {
                            main_view.open_query_editor_with_content(
                                editor_title.clone(),
                                definition,
                                None,
                                Some(connection_id),
                                window,
                                cx,
                            );
                        });
                    })?;
                }
                Err(error) => {
                    tracing::error!("Failed to fetch sequence definition: {}", error);
                    _ = cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to load sequence: {}", error)),
                            cx,
                        );
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    pub(super) fn open_sequence_designer(
        &mut self,
        connection_id: Uuid,
        sequence_name: Option<String>,
        object_schema: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Open sequence designer: {:?} on connection {}",
            sequence_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();

        if let Some(sequence_name) = sequence_name {
            cx.spawn_in(window, async move |this, cx| {
                let design =
                    fetch_sequence_design(&connection, object_schema.as_deref(), &sequence_name)
                        .await;

                cx.update(|window, cx| {
                    let design = match design {
                        Ok(design) => design,
                        Err(error) => {
                            window.push_notification(
                                Notification::error(format!(
                                    "Failed to load sequence designer: {}",
                                    error
                                )),
                                cx,
                            );
                            return;
                        }
                    };
                    let panel =
                        cx.new(|cx| SequenceDesignerPanel::edit(connection_id, design, window, cx));

                    _ = this.update(cx, |main_view, cx| {
                        let panel_clone = panel.clone();
                        let subscription = cx.subscribe_in(&panel, window, {
                            move |this, _panel, event: &SequenceDesignerEvent, window, cx| {
                                this.handle_sequence_designer_event(
                                    panel_clone.clone(),
                                    event.clone(),
                                    window,
                                    cx,
                                );
                            }
                        });
                        main_view._subscriptions.push(subscription);
                        main_view.workspace_controller.update(cx, |workspace, cx| {
                            workspace.add_center_item(Arc::new(panel.clone()), window, cx);
                        });
                    });
                })?;

                anyhow::Ok(())
            })
            .detach();
        } else {
            let schema = object_schema.clone();
            let panel = cx.new(|cx| SequenceDesignerPanel::new(connection_id, schema, window, cx));
            let panel_clone = panel.clone();
            let subscription = cx.subscribe_in(&panel, window, {
                move |this, _panel, event: &SequenceDesignerEvent, window, cx| {
                    this.handle_sequence_designer_event(
                        panel_clone.clone(),
                        event.clone(),
                        window,
                        cx,
                    );
                }
            });
            self._subscriptions.push(subscription);
            self.workspace_controller.update(cx, |workspace, cx| {
                workspace.add_center_item(Arc::new(panel.clone()), window, cx);
            });
        }
    }

    fn handle_sequence_designer_event(
        &mut self,
        panel: Entity<SequenceDesignerPanel>,
        event: SequenceDesignerEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            SequenceDesignerEvent::Save {
                connection_id,
                design,
                is_new,
                original_name: _,
            } => {
                self.save_sequence_from_designer(connection_id, design, is_new, panel, window, cx);
            }
            SequenceDesignerEvent::Cancel => {
                let panel_arc: Arc<dyn PanelView> = Arc::new(panel);
                self.workspace_controller.update(cx, |workspace, cx| {
                    workspace.remove_center_item(panel_arc, window, cx);
                });
            }
            SequenceDesignerEvent::PreviewDdl { design: _ } => {}
        }
    }

    fn save_sequence_from_designer(
        &mut self,
        connection_id: Uuid,
        design: SequenceDesign,
        is_new: bool,
        panel: Entity<SequenceDesignerPanel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let ddl = design.to_ddl();
        let sequence_name = design.name.clone();
        let schema_service = app_state.schema_service.clone();
        let panel_arc: Arc<dyn PanelView> = Arc::new(panel.clone());
        let workspace_controller = self.workspace_controller.clone();
        let version_repository = self.version_repository.clone();
        let version_target = SqlObjectVersionTarget {
            connection_id,
            object_type: DatabaseObjectType::Sequence,
            object_name: sequence_name.clone(),
            object_schema: normalize_schema_name(design.schema.as_deref()),
        };

        cx.spawn_in(window, async move |_this, cx| {
            let mut execution_result = Ok(());
            for statement in split_sql_statements(&ddl) {
                if let Err(error) = connection.execute(&statement, &[]).await {
                    execution_result = Err(error);
                    break;
                }
            }

            match execution_result {
                Ok(_) => {
                    schema_service.invalidate_connection_cache(connection_id);
                    let version_message = build_sql_object_version_message(
                        DatabaseObjectType::Sequence,
                        &sequence_name,
                        is_new,
                    );
                    if let Err(error) = record_sql_object_version(
                        &version_repository,
                        &version_target,
                        ddl.clone(),
                        version_message,
                    ) {
                        tracing::error!(%error, sequence = %sequence_name, "Failed to store sequence version snapshot from designer");
                    }
                    cx.update(|window, cx| {
                        window.push_notification(
                            Notification::success(if is_new {
                                format!("Sequence '{}' created", sequence_name)
                            } else {
                                format!("Sequence '{}' updated", sequence_name)
                            }),
                            cx,
                        );
                        workspace_controller.update(cx, |workspace, cx| {
                            workspace.remove_center_item(panel_arc, window, cx);
                        });
                    })?;
                }
                Err(error) => {
                    tracing::error!("Failed to save sequence: {}", error);
                    cx.update(|window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to save sequence: {}", error)),
                            cx,
                        );
                    })?;
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
        object_schema: Option<String>,
        trigger_table_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Design trigger: {} on connection {}",
            trigger_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let trigger_name_for_spawn = trigger_name.clone();
        let trigger_table_name_for_spawn = trigger_table_name.clone();

        let editor_title = format!("[Trigger] {} ({})", trigger_name, connection_name);

        cx.spawn_in(window, async move |this, cx| {
            match fetch_trigger_definition(
                &connection,
                object_schema.as_deref(),
                &trigger_name_for_spawn,
                trigger_table_name_for_spawn.as_deref(),
            )
            .await
            {
                Ok(definition) => {
                    cx.update(|window, cx| {
                        let object_type = EditorObjectType::edit_trigger(
                            trigger_name_for_spawn.clone(),
                            object_schema.clone(),
                        );

                        _ = this.update(cx, |main_view, cx| {
                            main_view.open_workspace_object_editor(
                                ObjectEditorDefinition {
                                    display_name: editor_title.clone(),
                                    object_type,
                                    initial_content: Some(definition),
                                    schema_service: schema_service.clone(),
                                },
                                ObjectEditorConnection {
                                    connection_id,
                                    connection_name: connection_name.clone(),
                                    connection: connection.clone(),
                                    driver_name: driver_name.clone(),
                                    database_name: target_database.clone(),
                                },
                                MainView::handle_trigger_editor_event,
                                window,
                                cx,
                            );
                        });

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

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let connection_name = app_state
            .connection_service
            .get_saved_connection_name(connection_id)
            .unwrap_or_else(|| "Unknown".to_string());

        let schema_service = app_state.schema_service.clone();

        let object_type = EditorObjectType::new_trigger();

        let template = if connection_is_postgres(connection.as_ref()) {
            r#"CREATE OR REPLACE TRIGGER trigger_name
AFTER INSERT ON table_name
FOR EACH ROW
EXECUTE FUNCTION trigger_function_name();"#
                .to_string()
        } else if connection_is_mysql_compatible(connection.as_ref()) {
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

        self.open_workspace_object_editor(
            ObjectEditorDefinition {
                display_name: "New Trigger".to_string(),
                object_type,
                initial_content: Some(template),
                schema_service,
            },
            ObjectEditorConnection {
                connection_id,
                connection_name,
                connection: connection.clone(),
                driver_name,
                database_name: target_database,
            },
            MainView::handle_trigger_editor_event,
            window,
            cx,
        );

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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let trigger_table_name = app_state
            .schema_service
            .cache()
            .get_triggers(connection_id)
            .unwrap_or_default()
            .into_iter()
            .find(|trigger| trigger.name == trigger_name)
            .map(|trigger| trigger.table_name);

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let trigger_name_for_dialog = trigger_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let trigger_name = trigger_name_for_dialog.clone();
            let trigger_table_name_for_ok = trigger_table_name.clone();

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
                        // This goes through dialog configuration rather than direct button helpers,
                        // and the destructive variant keeps the trigger delete affordance explicit.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let trigger_name = trigger_name.clone();
                    let trigger_table_name = trigger_table_name_for_ok.clone();

                    cx.spawn(async move |cx| {
                        let sql = match build_drop_trigger_statement_for_object(
                            &connection,
                            &DropTriggerStatementRequest::new(&trigger_name)
                                .with_table_name(trigger_table_name.clone()),
                        ) {
                            Ok(sql) => sql,
                            Err(error) => {
                                tracing::error!(
                                    trigger = %trigger_name,
                                    %error,
                                    "Failed to build trigger drop SQL"
                                );
                                return;
                            }
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
        object_schema: Option<String>,
        trigger_table_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Open trigger designer: {:?} on connection {}",
            trigger_name,
            connection_id
        );
        let object_schema = normalize_schema_name(object_schema.as_deref());

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let target_database = self
            .workspace_state
            .read(cx)
            .active_database()
            .map(ToString::to_string);
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let dialect = trigger_dialect_for_connection(&connection);

        // Get available tables for the table dropdown
        let connection_service = app_state.connection_service.clone();
        if let Some(trigger_name) = trigger_name {
            // Editing an existing trigger - need to load its definition first
            let connection = connection.clone();
            let trigger_name_for_spawn = trigger_name.clone();

            cx.spawn_in(window, async move |this, cx| {
                // Fetch trigger definition
                let definition = fetch_trigger_definition(
                    &connection,
                    object_schema.as_deref(),
                    &trigger_name_for_spawn,
                    trigger_table_name.as_deref(),
                )
                .await;

                // Get available tables and dialect-specific trigger metadata
                let tables = match connection_service
                    .load_schema_for_database(connection_id, target_database.as_deref())
                    .await
                {
                    Ok(schema) => schema.tables,
                    Err(e) => {
                        tracing::warn!("Failed to load tables for trigger designer: {}", e);
                        vec![]
                    }
                };
                let relation_columns = load_trigger_relation_columns(
                    &connection,
                    object_schema.as_deref(),
                    trigger_table_name.as_deref(),
                )
                .await;
                let postgres_functions =
                    load_trigger_functions(&connection, object_schema.as_deref(), dialect).await;

                cx.update(|window, cx| {
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
                            relation_columns.clone(),
                            postgres_functions.clone(),
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
                        main_view.workspace_controller.update(cx, |workspace, cx| {
                            workspace.add_center_item(Arc::new(panel.clone()), window, cx);
                        });
                    });

                    tracing::info!("Opened trigger designer for '{}'", trigger_name_for_spawn);
                })?;

                anyhow::Ok(())
            })
            .detach();
        } else {
            // Creating a new trigger
            let connection_service = connection_service.clone();

            cx.spawn_in(window, async move |this, cx| {
                // Get available tables and dialect-specific trigger metadata
                let tables = match connection_service
                    .load_schema_for_database(connection_id, target_database.as_deref())
                    .await
                {
                    Ok(schema) => schema.tables,
                    Err(e) => {
                        tracing::warn!("Failed to load tables for trigger designer: {}", e);
                        vec![]
                    }
                };
                let postgres_functions =
                    load_trigger_functions(&connection, object_schema.as_deref(), dialect).await;

                cx.update(|window, cx| {
                    let _design = TriggerDesign::new(dialect);

                    // Create the trigger designer panel
                    let panel = cx.new(|cx| {
                        TriggerDesignerPanel::new(
                            connection_id,
                            dialect,
                            tables.clone(),
                            Vec::new(),
                            postgres_functions.clone(),
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

                    _ = this.update(cx, |main_view, cx| {
                        main_view.workspace_controller.update(cx, |workspace, cx| {
                            workspace.add_center_item(Arc::new(panel.clone()), window, cx);
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
                    TriggerDesignerSaveRequest {
                        connection_id,
                        object_schema: normalize_schema_name(design.schema.as_deref()),
                        design,
                        ddl,
                        is_new,
                        original_name,
                        panel,
                    },
                    window,
                    cx,
                );
            }
            TriggerDesignerEvent::Cancel => {
                let panel_arc: Arc<dyn PanelView> = Arc::new(panel);
                self.workspace_controller.update(cx, |workspace, cx| {
                    workspace.remove_center_item(panel_arc, window, cx);
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
        request: TriggerDesignerSaveRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let TriggerDesignerSaveRequest {
            connection_id,
            object_schema,
            design,
            ddl,
            is_new,
            original_name,
            panel,
        } = request;
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let trigger_name = design.name.clone();
        let schema_service = app_state.schema_service.clone();
        let version_repository = self.version_repository.clone();
        let version_target = SqlObjectVersionTarget {
            connection_id,
            object_type: DatabaseObjectType::Trigger,
            object_name: trigger_name.clone(),
            object_schema,
        };

        let panel_arc: Arc<dyn PanelView> = Arc::new(panel.clone());
        let workspace_controller = self.workspace_controller.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let replace_plan = match plan_trigger_replace_execution(
                &connection,
                &zqlz_objects::TriggerReplacePlanRequest::new(is_new, original_name.clone()),
            ) {
                Ok(plan) => plan,
                Err(error) => {
                    tracing::warn!(trigger = %trigger_name, %error, "Unable to prepare trigger replace plan");
                    return anyhow::Ok(());
                }
            };

            if let Some(drop_sql) = replace_plan.drop_statement
                && let Err(e) = connection.execute(&drop_sql, &[]).await
            {
                tracing::warn!("Failed to drop old trigger: {}", e);
                // Continue anyway - the create might still work
            }

            // Execute the CREATE TRIGGER statement
            match connection.execute(&ddl, &[]).await {
                Ok(_) => {
                    tracing::info!("Trigger '{}' saved successfully", trigger_name);

                    let version_message = build_sql_object_version_message(
                        DatabaseObjectType::Trigger,
                        &trigger_name,
                        is_new,
                    );
                    if let Err(error) = record_sql_object_version(
                        &version_repository,
                        &version_target,
                        ddl.clone(),
                        version_message,
                    ) {
                        tracing::error!(%error, trigger = %trigger_name, "Failed to store trigger version snapshot from designer");
                    }

                    schema_service.invalidate_connection_cache(connection_id);

                    cx.update(|window, cx| {
                        // Update sidebar
                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                            if is_new {
                                sidebar.add_trigger(connection_id, trigger_name.clone(), cx);
                            } else if let Some(orig_name) = &original_name
                                && orig_name != &trigger_name
                            {
                                sidebar.remove_trigger(connection_id, orig_name, cx);
                                sidebar.add_trigger(connection_id, trigger_name.clone(), cx);
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

                        workspace_controller.update(cx, |workspace, cx| {
                            workspace.remove_center_item(panel_arc, window, cx);
                        });
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
            QueryEditorEvent::PreviewDdl {
                object_type,
                definition,
            } => {
                self.show_query_editor_ddl_preview(object_type, definition, window, cx);
            }
            QueryEditorEvent::ExecuteQuery {
                sql,
                connection_id,
                database_name,
                ..
            } => {
                tracing::info!("Executing trigger query: {}", sql);
                self.execute_view_query(
                    sql.clone(),
                    *connection_id,
                    database_name.clone(),
                    window,
                    cx,
                );
            }
            QueryEditorEvent::ExecuteSelection {
                sql,
                connection_id,
                database_name,
                ..
            } => {
                tracing::info!("Executing trigger selection: {}", sql);
                self.execute_view_query(
                    sql.clone(),
                    *connection_id,
                    database_name.clone(),
                    window,
                    cx,
                );
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
            schema,
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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let schema_service = app_state.schema_service.clone();
        let version_repository = self.version_repository.clone();
        let object_schema = normalize_schema_name(schema.as_deref());
        let existing_version_target = name.clone().map(|trigger_name| SqlObjectVersionTarget {
            connection_id,
            object_type: DatabaseObjectType::Trigger,
            object_name: trigger_name,
            object_schema: object_schema.clone(),
        });

        if is_new {
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
                        let trigger_name = extract_trigger_name_from_create_statement(&definition);
                        if let Some(name) = trigger_name {
                            let version_target = SqlObjectVersionTarget {
                                connection_id,
                                object_type: DatabaseObjectType::Trigger,
                                object_name: name.clone(),
                                object_schema: object_schema.clone(),
                            };
                            let version_message = build_sql_object_version_message(
                                DatabaseObjectType::Trigger,
                                &name,
                                true,
                            );
                            if let Err(error) = record_sql_object_version(
                                &version_repository,
                                &version_target,
                                definition.clone(),
                                version_message,
                            ) {
                                tracing::error!(%error, trigger = %name, "Failed to store created trigger version snapshot");
                            }

                            _ = connection_sidebar.update(cx, |sidebar, cx| {
                                sidebar.add_trigger(connection_id, name.clone(), cx);
                            });

                            // Update editor object type with the name
                            _ = editor_weak.update(cx, |editor, cx| {
                                editor.set_object_type(
                                    EditorObjectType::edit_trigger(name, object_schema.clone()),
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
            let Some(trigger_name) = name else {
                tracing::error!("save_trigger called for existing trigger without a name");
                return;
            };

            // For existing triggers, we need to drop and recreate
            // SQLite doesn't support CREATE OR REPLACE TRIGGER
            cx.spawn_in(window, async move |_this, cx| {
                let replace_plan = match plan_trigger_replace_execution(
                    &connection,
                    &zqlz_objects::TriggerReplacePlanRequest::new(
                        false,
                        Some(trigger_name.clone()),
                    ),
                ) {
                    Ok(plan) => plan,
                    Err(error) => {
                        tracing::warn!(trigger = %trigger_name, %error, "Unable to prepare trigger replace plan");
                        return anyhow::Ok(());
                    }
                };

                if let Some(drop_sql) = replace_plan.drop_statement
                    && let Err(e) = connection.execute(&drop_sql, &[]).await
                {
                    tracing::warn!("Failed to drop trigger before recreate: {}", e);
                }

                match connection.execute(&definition, &[]).await {
                    Ok(_) => {
                        tracing::info!("Trigger '{}' saved successfully", trigger_name);

                        if let Some(version_target) = &existing_version_target {
                            let version_message = build_sql_object_version_message(
                                version_target.object_type,
                                &version_target.object_name,
                                false,
                            );
                            if let Err(error) = record_sql_object_version(
                                &version_repository,
                                version_target,
                                definition.clone(),
                                version_message,
                            ) {
                                tracing::error!(%error, trigger = %trigger_name, "Failed to store updated trigger version snapshot");
                            }
                        }

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
            self.design_view(connection_id, view_name, None, window, cx);
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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let schema_service = app_state.schema_service.clone();
        let continue_on_error = Rc::new(RefCell::new(false));

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
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
                        // Multi-delete is a destructive dialog action, so the shared dialog button
                        // is configured with Danger via ButtonVariant instead of a concrete helper.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let window_handle = window_handle;
                    let main_view = main_view.clone();
                    let schema_service = schema_service.clone();
                    let view_names = view_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut deleted_views: Vec<String> = Vec::new();

                        for view_name in &view_names {
                            let sql = build_drop_view_statement(&connection, view_name, false);
                            if sql.is_empty() {
                                if continue_on_error {
                                    errors.push(format!(
                                        "'{}': failed to build DROP VIEW SQL",
                                        view_name
                                    ));
                                    continue;
                                }
                                return;
                            }
                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!("View '{}' deleted successfully", view_name);
                                    deleted_views.push(view_name.clone());

                                    tracing::debug!(view_name = %view_name, "Queued view refresh after delete");
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

                            let _ = cx.update_window(window_handle, |_, _window, cx| {
                                let _ = main_view.update(cx, |main_view, cx| {
                                    main_view.request_refresh(
                                        RefreshScope::ConnectionSurfaces(connection_id),
                                        cx,
                                    );
                                });
                            });
                        }

                        if !errors.is_empty() {
                            tracing::warn!(
                                "Deleted {} of {} views. Errors: {}",
                                deleted_views.len(),
                                view_names.len(),
                                errors.join("; ")
                            );
                        } else if !deleted_views.is_empty() {
                            tracing::info!("Successfully deleted {} view(s)", deleted_views.len());
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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let schema_service = app_state.schema_service.clone();
        let continue_on_error = Rc::new(RefCell::new(true));
        let new_names: Vec<String> = view_names.iter().map(|n| format!("{}_copy", n)).collect();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let schema_service = schema_service.clone();
            let view_names = view_names.clone();
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
                    let window_handle = window_handle;
                    let main_view = main_view.clone();
                    let schema_service = schema_service.clone();
                    let view_names = view_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut duplicated_views: Vec<String> = Vec::new();

                        for view_name in &view_names {
                            let new_name = format!("{}_copy", view_name);

                            match build_duplicate_view_sql(
                                &connection,
                                &zqlz_objects::DuplicateViewRequest::new(view_name, &new_name),
                            )
                            .await
                            {
                                Ok(create_sql) => {
                                    match connection.execute(&create_sql, &[]).await {
                                        Ok(_) => {
                                            tracing::info!(
                                                "View '{}' duplicated as '{}'",
                                                view_name,
                                                new_name
                                            );
                                            duplicated_views.push(new_name.clone());

                                            tracing::debug!(
                                                connection_id = %connection_id,
                                                view_name = %new_name,
                                                "Queued view refresh after duplication"
                                            );
                                        }
                                        Err(e) => {
                                            let error_msg = format!("'{}': {}", view_name, e);
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
                                    tracing::error!("Failed to duplicate view {}", error_msg);

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

                            let _ = cx.update_window(window_handle, |_, _window, cx| {
                                let _ = main_view.update(cx, |main_view, cx| {
                                    main_view.request_refresh(
                                        RefreshScope::ConnectionSurfaces(connection_id),
                                        cx,
                                    );
                                });
                            });
                        }

                        if !errors.is_empty() {
                            tracing::warn!(
                                "Duplicated {} of {} views. Errors: {}",
                                duplicated_views.len(),
                                view_names.len(),
                                errors.join("; ")
                            );
                        } else if !duplicated_views.is_empty() {
                            tracing::info!("Duplicated {} view(s)", duplicated_views.len());
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }
}

/// Parses a trigger definition SQL into a TriggerDesign structure.
/// This is a best-effort parser - complex triggers may not parse fully.
fn parse_trigger_definition(
    sql: &str,
    trigger_name: &str,
    dialect: TriggerDialect,
) -> TriggerDesign {
    let mut design =
        TriggerDesign::from_sql(sql, dialect).unwrap_or_else(|| TriggerDesign::new(dialect));
    design.name = trigger_name.to_string();
    design.is_new = false;
    design
}

async fn load_trigger_relation_columns(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    table_name: Option<&str>,
) -> Vec<String> {
    let Some(table_name) = table_name else {
        return Vec::new();
    };
    let Some(schema) = connection.as_schema_introspection() else {
        return Vec::new();
    };

    match schema.get_columns(schema_name, table_name).await {
        Ok(columns) => columns.into_iter().map(|column| column.name).collect(),
        Err(error) => {
            tracing::warn!(table = %table_name, %error, "Failed to load trigger relation columns");
            Vec::new()
        }
    }
}

async fn load_trigger_functions(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    dialect: TriggerDialect,
) -> Vec<String> {
    if dialect != TriggerDialect::Postgres {
        return Vec::new();
    }
    let Some(schema) = connection.as_schema_introspection() else {
        return Vec::new();
    };

    match schema.list_functions(schema_name).await {
        Ok(functions) => functions
            .into_iter()
            .filter(|function| function.return_type.eq_ignore_ascii_case("trigger"))
            .map(|function| {
                function
                    .schema
                    .map(|schema| format!("{schema}.{}", function.name))
                    .unwrap_or(function.name)
            })
            .collect(),
        Err(error) => {
            tracing::warn!(%error, "Failed to load trigger functions");
            Vec::new()
        }
    }
}

/// Fetches the definition of a trigger from the database.
async fn fetch_trigger_definition(
    connection: &Arc<dyn zqlz_core::Connection>,
    schema_name: Option<&str>,
    trigger_name: &str,
    table_name: Option<&str>,
) -> Result<String, String> {
    if connection_is_postgres(connection.as_ref()) {
        let Some(table_name) = table_name else {
            return fetch_trigger_definition_from_objects(connection, schema_name, trigger_name)
                .await
                .map_err(|error| error.to_string());
        };
        let result = if let Some(schema_name) = schema_name {
            connection
                .query(
                    "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                     FROM pg_catalog.pg_trigger t
                     JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = $1
                       AND c.relname = $2
                       AND t.tgname = $3
                       AND NOT t.tgisinternal
                     LIMIT 1",
                    &[
                        zqlz_core::Value::String(schema_name.to_string()),
                        zqlz_core::Value::String(table_name.to_string()),
                        zqlz_core::Value::String(trigger_name.to_string()),
                    ],
                )
                .await
        } else {
            connection
                .query(
                    "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                     FROM pg_catalog.pg_trigger t
                     JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE c.relname = $1
                       AND t.tgname = $2
                       AND NOT t.tgisinternal
                       AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                       AND n.nspname NOT LIKE 'pg_temp_%'
                       AND n.nspname NOT LIKE 'pg_toast_temp_%'
                     ORDER BY n.nspname
                     LIMIT 1",
                    &[
                        zqlz_core::Value::String(table_name.to_string()),
                        zqlz_core::Value::String(trigger_name.to_string()),
                    ],
                )
                .await
        };

        match result {
            Ok(result) => {
                if let Some(definition) = result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|value| value.as_str())
                {
                    return Ok(format!("{};", definition));
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    trigger_name,
                    table_name,
                    schema_name = ?schema_name,
                    "Failed exact trigger definition lookup; falling back to driver DDL generation"
                );
            }
        }
    }

    fetch_trigger_definition_from_objects(connection, schema_name, trigger_name)
        .await
        .map_err(|error| error.to_string())
}
