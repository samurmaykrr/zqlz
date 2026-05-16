// Saved query management methods for MainView
//
// This module handles saving, loading, and managing user-saved SQL queries.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::HashSet;
use std::path::PathBuf;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, Icon, WindowExt, ZqlzIcon,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    h_flex,
    input::{Input, InputState},
    notification::Notification,
    typography::body_small,
    v_flex,
};

use crate::app::AppState;
use crate::components::{ConnectionSidebar, QueryEditor};
use zqlz_connection::SavedQueryInfo;
use zqlz_query::{
    QueryConnectionCandidate, SavedQueryOperation, SavedQueryWorkflowError,
    SavedQueryWorkflowOutcome, SavedQueryWorkflowRequest, build_query_editor_switcher_selection,
    run_saved_query_workflow,
};
use zqlz_text_editor::{DocumentIdentity, TextDocument};

use super::MainView;

fn rename_open_saved_query_editors(
    query_editors: &[WeakEntity<QueryEditor>],
    query_id: Uuid,
    new_name: &str,
    cx: &mut App,
) {
    for query_editor in query_editors {
        let Some(query_editor) = query_editor.upgrade() else {
            continue;
        };

        let is_matching_saved_query = query_editor.read(cx).saved_query_id() == Some(query_id);
        if !is_matching_saved_query {
            continue;
        }

        query_editor.update(cx, |query_editor, cx| {
            query_editor.set_name(new_name, cx);
        });
    }
}

fn find_open_saved_query_editor(
    query_editors: &[WeakEntity<QueryEditor>],
    query_id: Uuid,
    cx: &App,
) -> Option<Entity<QueryEditor>> {
    query_editors.iter().find_map(|query_editor| {
        let query_editor = query_editor.upgrade()?;
        (query_editor.read(cx).saved_query_id() == Some(query_id)).then_some(query_editor)
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn save_query_for_editor(
    editor: WeakEntity<QueryEditor>,
    sql: String,
    connection_id: Uuid,
    query_name: String,
    folder: Option<String>,
    sidebar_weak: WeakEntity<ConnectionSidebar>,
    window: &mut Window,
    cx: &mut App,
) -> Result<Uuid, String> {
    let Some(app_state) = cx.try_global::<AppState>() else {
        return Err("Application state not available".to_string());
    };
    let query_text = sql.clone();

    match run_saved_query_workflow(
        app_state.storage.as_ref(),
        SavedQueryWorkflowRequest::Create {
            name: query_name,
            connection_id,
            sql,
            folder,
        },
    )
    .and_then(SavedQueryWorkflowOutcome::into_created)
    {
        Ok(saved_query) => {
            let query_id = saved_query.id;
            let query_name = saved_query.name;

            if let Err(error) = editor.update(cx, |editor, cx| {
                editor.set_saved_query_id(Some(query_id), cx);
                editor.set_name(&query_name, cx);
                editor.mark_clean(cx);
            }) {
                tracing::warn!(%error, %query_id, "failed to update query editor after save");
            }

            if let Err(error) = sidebar_weak.update(cx, |sidebar, cx| {
                sidebar.add_saved_query(
                    connection_id,
                    SavedQueryInfo {
                        id: query_id,
                        name: query_name.clone(),
                        query_text: query_text.clone(),
                        folder: saved_query.folder.clone(),
                    },
                    cx,
                );
            }) {
                tracing::warn!(%error, %query_id, "failed to refresh sidebar after query save");
            }

            window.push_notification(
                Notification::success(format!("Query '{}' saved", query_name)),
                cx,
            );

            Ok(query_id)
        }
        Err(error) => {
            if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                tracing::error!(%storage_error, "failed to save query");
            }

            Err(error.user_message(SavedQueryOperation::Create))
        }
    }
}

pub(super) fn update_saved_query_for_editor(
    query_id: Uuid,
    sql: String,
    editor: WeakEntity<QueryEditor>,
    sidebar_weak: WeakEntity<ConnectionSidebar>,
    window: &mut Window,
    cx: &mut App,
) {
    if let Err(error_message) =
        try_update_saved_query_for_editor(query_id, sql, editor, Some(sidebar_weak), cx)
    {
        window.push_notification(Notification::error(error_message), cx);
    }
}

pub(super) fn try_update_saved_query_for_editor(
    query_id: Uuid,
    sql: String,
    editor: WeakEntity<QueryEditor>,
    sidebar_weak: Option<WeakEntity<ConnectionSidebar>>,
    cx: &mut App,
) -> Result<(), String> {
    let Some(app_state) = cx.try_global::<AppState>() else {
        return Err("Application state not available".to_string());
    };
    let query_text = sql.clone();

    match run_saved_query_workflow(
        app_state.storage.as_ref(),
        SavedQueryWorkflowRequest::UpdateSql { query_id, sql },
    )
    .and_then(SavedQueryWorkflowOutcome::into_updated)
    {
        Ok(()) => {
            let connection_id = editor
                .read_with(cx, |editor, _| editor.connection_id())
                .ok()
                .flatten();
            if let Err(error) = editor.update(cx, |editor, cx| {
                editor.mark_clean(cx);
            }) {
                tracing::warn!(%error, %query_id, "failed to mark editor clean after save");
            }
            if let (Some(connection_id), Some(sidebar_weak)) = (connection_id, sidebar_weak)
                && let Err(error) = sidebar_weak.update(cx, |sidebar, cx| {
                    sidebar.update_saved_query_text(
                        connection_id,
                        query_id,
                        query_text.clone(),
                        cx,
                    );
                })
            {
                tracing::warn!(%error, %query_id, "failed to refresh saved query text in sidebar");
            }

            Ok(())
        }
        Err(error) => {
            if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                tracing::error!(%storage_error, "failed to update query");
            }

            Err(error.user_message(SavedQueryOperation::Update))
        }
    }
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
                    .connection_service
                    .get_saved_connection_name(connection_id)
            })
            .unwrap_or_else(|| "Unknown".to_string());

        // Create input state for the query name
        let name_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Enter query name..."));
        let folder_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder("Folder name (optional, blank = root)")
        });
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
            let folder_input = folder_input.clone();
            let error_message = error_message.clone();
            let sidebar_weak = sidebar_weak.clone();

            move |dialog, _window, cx| {
                let sql = sql.clone();
                let connection_name = connection_name.clone();
                let name_input = name_input.clone();
                let folder_input = folder_input.clone();
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
                            .child(
                                v_flex()
                                    .gap_1()
                                    .child(body_small("Folder:"))
                                    .child(Input::new(&folder_input)),
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
                                    this.text_color(cx.theme().danger_text).child(err)
                                })
                            }),
                    )
                    .on_ok({
                        let folder_input = folder_input.clone();
                        move |_, _window, cx| {
                            let query_name =
                                name_input.read(cx).text().to_string().trim().to_string();
                            let folder = folder_input.read(cx).text().to_string();
                            let folder =
                                (!folder.trim().is_empty()).then(|| folder.trim().to_string());

                            match save_query_for_editor(
                                editor_weak.clone(),
                                sql.clone(),
                                connection_id,
                                query_name,
                                folder,
                                sidebar_weak.clone(),
                                _window,
                                cx,
                            ) {
                                Ok(_) => true,
                                Err(error) => {
                                    error_message_for_ok.update(cx, |msg, cx| {
                                        *msg = Some(error);
                                        cx.notify();
                                    });
                                    false
                                }
                            }
                        }
                    })
                    .button_props(
                        DialogButtonProps::default()
                            .ok_text("Save")
                            // Save is the dialog's primary commit action, and dialog props use
                            // ButtonVariant because the button instance is created later.
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
        update_saved_query_for_editor(
            query_id,
            sql,
            editor,
            self.connection_sidebar.downgrade(),
            window,
            cx,
        );
    }

    /// Open a saved query in the query editor
    pub fn open_saved_query(
        &mut self,
        query_id: Uuid,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.prune_closed_query_editors();

        if let Some(editor) = find_open_saved_query_editor(&self.query_editors, query_id, cx) {
            self.activate_existing_query_editor(&editor, window, cx);
            return;
        }

        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let query = match run_saved_query_workflow(
            app_state.storage.as_ref(),
            SavedQueryWorkflowRequest::Load { query_id },
        )
        .and_then(SavedQueryWorkflowOutcome::into_loaded)
        {
            Ok(record) => record,
            Err(error) => {
                if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                    tracing::error!(%storage_error, %query_id, "failed to load saved query");
                }
                window.push_notification(
                    Notification::error(error.user_message(SavedQueryOperation::Load)),
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
    ) -> Option<Entity<QueryEditor>> {
        // Get schema service and connection from AppState
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("AppState not initialized");
            return None;
        };

        let schema_service = app_state.schema_service.clone();
        let connection = app_state.connection_service.get_connection(connection_id);
        let (driver_type, connection_name) =
            MainView::resolve_editor_open_connection_metadata(Some(connection_id), app_state)
                .map(|(_connection, driver_name, connection_name)| (driver_name, connection_name))
                .or_else(|| {
                    app_state
                        .connection_service
                        .get_saved_connection(connection_id)
                        .ok()
                        .map(|saved| (saved.driver, saved.name))
                })
                .unwrap_or((String::new(), String::from("Unknown")));

        // Create an EditorId in WorkspaceState to track this editor
        let editor_id = self.create_workspace_editor(Some(connection_id), name.clone(), cx);

        let mut document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal document uri"),
            &sql,
        );
        document.mark_buffer_saved();

        let query_editor = cx.new(|cx| {
            let mut editor = QueryEditor::new_with_document(
                name.clone(),
                Some(connection_id),
                document,
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

            editor
        });

        let query_editor =
            self.finalize_query_editor_open(query_editor, name, editor_id, window, cx);
        self.refresh_saved_query_editor_switchers(&query_editor, cx);

        Some(query_editor)
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

                    match run_saved_query_workflow(
                        app_state.storage.as_ref(),
                        SavedQueryWorkflowRequest::Delete { query_id },
                    )
                    .and_then(SavedQueryWorkflowOutcome::into_deleted)
                    {
                        Ok(()) => {
                            tracing::info!("Query '{}' deleted successfully", query_name);
                            window.push_notification(
                                Notification::success(format!("Query '{}' deleted", query_name)),
                                cx,
                            );

                            // Update sidebar to remove the deleted query
                            if let Err(error) = sidebar_weak.update(cx, |sidebar, cx| {
                                sidebar.remove_saved_query(connection_id, query_id, cx);
                            }) {
                                tracing::warn!(
                                    %error,
                                    %connection_id,
                                    %query_id,
                                    "failed to remove deleted query from sidebar"
                                );
                            }
                        }
                        Err(error) => {
                            if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                                tracing::error!(%storage_error, "failed to delete query");
                            }
                            window.push_notification(
                                Notification::error(
                                    error.user_message(SavedQueryOperation::Delete),
                                ),
                                cx,
                            );
                        }
                    }

                    true
                })
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        // Saved-query deletion is destructive, so the shared dialog OK action is
                        // explicitly marked Danger through ButtonVariant metadata.
                        .ok_variant(ButtonVariant::Danger),
                )
                .confirm()
        });
    }

    pub fn export_saved_queries(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let content = match run_saved_query_workflow(
            app_state.storage.as_ref(),
            SavedQueryWorkflowRequest::Export { connection_id },
        )
        .and_then(SavedQueryWorkflowOutcome::into_exported)
        {
            Ok(content) => content,
            Err(error) => {
                window.push_notification(
                    Notification::error(error.user_message(SavedQueryOperation::LoadForConnection)),
                    cx,
                );
                return;
            }
        };

        let receiver = cx.prompt_for_new_path(&PathBuf::from("zqlz-queries.json"), None);
        let window_handle = window.window_handle();
        cx.spawn(async move |_this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => return anyhow::Ok(()),
            };

            match std::fs::write(&path, content) {
                Ok(()) => {
                    let _ = window_handle.update(cx, |_, window, cx| {
                        window.push_notification(
                            Notification::success(format!("Exported queries to {}", path.display())),
                            cx,
                        );
                    });
                }
                Err(error) => tracing::error!(%error, path = %path.display(), "failed to export saved queries"),
            }

            anyhow::Ok(())
        })
        .detach();
    }

    pub fn import_saved_queries(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Import Queries".into()),
        });
        let sidebar_weak = self.connection_sidebar.downgrade();
        let window_handle = window.window_handle();

        cx.spawn(async move |_this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(paths))) => match paths.first() {
                    Some(path) => path.clone(),
                    None => return anyhow::Ok(()),
                },
                _ => return anyhow::Ok(()),
            };

            let content = match std::fs::read_to_string(&path) {
                Ok(content) => content,
                Err(error) => {
                    tracing::error!(%error, path = %path.display(), "failed to read saved query import file");
                    return anyhow::Ok(());
                }
            };

            let _ = window_handle.update(cx, |_, window, cx| {
                let Some(app_state) = cx.try_global::<AppState>() else {
                    window.push_notification(Notification::error("Application state not available"), cx);
                    return;
                };

                match run_saved_query_workflow(
                    app_state.storage.as_ref(),
                    SavedQueryWorkflowRequest::Import {
                        connection_id,
                        content,
                    },
                )
                .and_then(SavedQueryWorkflowOutcome::into_imported)
                {
                    Ok(queries) => {
                        let count = queries.len();
                        let saved_queries = queries
                            .into_iter()
                            .map(|query| SavedQueryInfo {
                                id: query.id,
                                name: query.name,
                                query_text: query.sql,
                                folder: query.folder,
                            })
                            .collect::<Vec<_>>();

                        let _ = sidebar_weak.update(cx, |sidebar, cx| {
                            for query in saved_queries {
                                sidebar.add_saved_query(connection_id, query, cx);
                            }
                        });

                        window.push_notification(
                            Notification::success(format!("Imported {count} queries")),
                            cx,
                        );
                    }
                    Err(error) => {
                        window.push_notification(
                            Notification::error(error.user_message(SavedQueryOperation::Create)),
                            cx,
                        );
                    }
                }
            });

            anyhow::Ok(())
        })
        .detach();
    }

    pub fn move_saved_query_to_folder(
        &mut self,
        connection_id: Uuid,
        query_id: Uuid,
        query_name: String,
        folder: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        match run_saved_query_workflow(
            app_state.storage.as_ref(),
            SavedQueryWorkflowRequest::MoveToFolder {
                query_id,
                folder: folder.clone(),
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_updated)
        {
            Ok(()) => {
                self.connection_sidebar.update(cx, |sidebar, cx| {
                    sidebar.move_saved_query_to_folder(connection_id, query_id, folder.clone(), cx);
                });
                let destination = folder.unwrap_or_else(|| "root".to_string());
                window.push_notification(
                    Notification::success(format!("Moved '{query_name}' to {destination}")),
                    cx,
                );
            }
            Err(error) => {
                window.push_notification(
                    Notification::error(error.user_message(SavedQueryOperation::Update)),
                    cx,
                );
            }
        }
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
        let open_query_editors = self.query_editors.clone();

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
            let open_query_editors = open_query_editors.clone();

            move |dialog, _window, cx| {
                let current_name = current_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();
                let sidebar_weak = sidebar_weak.clone();
                let open_query_editors = open_query_editors.clone();

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
                                    this.text_color(cx.theme().danger_text).child(err)
                                })
                            }),
                    )
                    .on_ok(move |_, window, cx| {
                        let new_name = name_input.read(cx).text().to_string().trim().to_string();

                        // If name unchanged, just close
                        if new_name == current_name {
                            return true;
                        }

                        let Some(app_state) = cx.try_global::<AppState>() else {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("Application state not available".to_string());
                                cx.notify();
                            });
                            return false;
                        };

                        match run_saved_query_workflow(
                            app_state.storage.as_ref(),
                            SavedQueryWorkflowRequest::Rename {
                                query_id,
                                connection_id,
                                new_name: new_name.clone(),
                            },
                        )
                        .and_then(SavedQueryWorkflowOutcome::into_renamed)
                        {
                            Ok(()) => {
                                tracing::info!("Query renamed to '{}'", new_name);
                                window.push_notification(
                                    Notification::success(format!("Renamed to '{}'", new_name)),
                                    cx,
                                );

                                // Update sidebar to reflect the new name
                                if let Err(error) = sidebar_weak.update(cx, |sidebar, cx| {
                                    sidebar.rename_saved_query(
                                        connection_id,
                                        query_id,
                                        new_name.clone(),
                                        cx,
                                    );
                                }) {
                                    tracing::warn!(
                                        %error,
                                        %connection_id,
                                        %query_id,
                                        "failed to update sidebar after query rename"
                                    );
                                }

                                rename_open_saved_query_editors(
                                    &open_query_editors,
                                    query_id,
                                    &new_name,
                                    cx,
                                );

                                true
                            }
                            Err(error) => {
                                if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                                    tracing::error!(%storage_error, "failed to rename query");
                                }
                                error_message_for_ok.update(cx, |msg, cx| {
                                    *msg = Some(error.user_message(SavedQueryOperation::Rename));
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
    #[allow(dead_code)]
    pub fn load_saved_queries_for_connection(
        &self,
        connection_id: Uuid,
        cx: &App,
    ) -> Vec<zqlz_query::SavedQueryRecord> {
        let Some(app_state) = cx.try_global::<AppState>() else {
            return Vec::new();
        };

        match run_saved_query_workflow(
            app_state.storage.as_ref(),
            SavedQueryWorkflowRequest::LoadForConnection { connection_id },
        )
        .and_then(SavedQueryWorkflowOutcome::into_loaded_for_connection)
        {
            Ok(queries) => queries,
            Err(error) => {
                if let SavedQueryWorkflowError::Storage(storage_error) = &error {
                    tracing::error!(
                        "Failed to load queries for connection {}: {}",
                        connection_id,
                        storage_error
                    );
                } else {
                    tracing::error!(
                        "Failed to load queries for connection {}: {}",
                        connection_id,
                        error
                    );
                }
                Vec::new()
            }
        }
    }

    pub(super) fn refresh_saved_query_editor_switchers(
        &self,
        query_editor: &Entity<QueryEditor>,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
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
        let active_connection_ids_set: HashSet<Uuid> = app_state
            .connection_service
            .list_active_connections()
            .into_iter()
            .collect();
        let active_connection_ids: Vec<Uuid> = app_state
            .connection_service
            .list_saved_connections()
            .into_iter()
            .filter(|saved| active_connection_ids_set.contains(&saved.id))
            .map(|saved| saved.id)
            .collect();
        let selected_connection_id = query_editor.read(cx).connection_id();
        let switcher_selection = build_query_editor_switcher_selection(
            selected_connection_id,
            &candidates,
            &active_connection_ids,
        );
        let available_connections = switcher_selection
            .available_connections
            .into_iter()
            .map(|option| (option.connection_id, option.connection_name))
            .collect();

        query_editor.update(cx, |editor, cx| {
            editor.set_available_connections(available_connections, cx);
            editor.set_current_database(switcher_selection.selected_default_database_name, cx);
        });
    }
}
