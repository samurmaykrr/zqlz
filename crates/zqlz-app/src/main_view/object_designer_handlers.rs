use std::sync::Arc;

use gpui::*;
use zqlz_core::{
    ConnectionScope, ObjectFormDdlRequest, ObjectFormMode, ObjectFormSpecRequest, ObjectFormValue,
    ObjectsPanelObjectRef,
};
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt, button::ButtonVariant, dialog::DialogButtonProps,
    notification::Notification, scroll::ScrollableElement, v_flex,
};
use zqlz_versioning::{DatabaseObjectType, VersionRepository};

use crate::{app::AppState, main_view::MainView};

struct ObjectFormVersionTarget {
    object_type: DatabaseObjectType,
    object_name: String,
    object_schema: Option<String>,
}

fn object_form_value_string(request: &ObjectFormDdlRequest, field_id: &str) -> Option<String> {
    request
        .values
        .get(field_id)
        .and_then(ObjectFormValue::as_string)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn object_form_kind_version_type(kind_id: &str) -> Option<DatabaseObjectType> {
    match kind_id {
        "view" => Some(DatabaseObjectType::View),
        "materialized_view" => Some(DatabaseObjectType::MaterializedView),
        "function" => Some(DatabaseObjectType::Function),
        "procedure" => Some(DatabaseObjectType::Procedure),
        "trigger" => Some(DatabaseObjectType::Trigger),
        "event" => Some(DatabaseObjectType::Event),
        "enum" | "domain" | "range" | "composite_type" | "type" => Some(DatabaseObjectType::Type),
        "policy" => Some(DatabaseObjectType::Policy),
        _ => None,
    }
}

fn object_form_version_target(request: &ObjectFormDdlRequest) -> Option<ObjectFormVersionTarget> {
    let object_type = object_form_kind_version_type(&request.kind_id)?;
    let object_name = request
        .object_ref
        .as_ref()
        .map(|object_ref| object_ref.name.clone())
        .or_else(|| object_form_value_string(request, "name"))?;
    let object_schema = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .or_else(|| object_form_value_string(request, "schema"));

    Some(ObjectFormVersionTarget {
        object_type,
        object_name,
        object_schema,
    })
}

fn object_form_target_database(
    request: &ObjectFormDdlRequest,
    active_database: Option<String>,
) -> Option<String> {
    request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.database.clone())
        .or(active_database)
}

fn object_form_version_message(
    object_type: DatabaseObjectType,
    object_name: &str,
    mode: ObjectFormMode,
) -> String {
    let action = match mode {
        ObjectFormMode::Create => "Create",
        ObjectFormMode::Edit => "Update",
        ObjectFormMode::Drop => "Drop",
    };
    format!(
        "{} {} {}",
        action,
        object_type.display_name().to_lowercase(),
        object_name
    )
}

fn record_object_form_version_snapshot(
    version_repository: &VersionRepository,
    connection_id: uuid::Uuid,
    request: &ObjectFormDdlRequest,
    ddl: &str,
) -> anyhow::Result<bool> {
    let Some(target) = object_form_version_target(request) else {
        return Ok(false);
    };
    let message =
        object_form_version_message(target.object_type, &target.object_name, request.mode);
    version_repository.commit(
        connection_id,
        target.object_type,
        target.object_schema,
        target.object_name,
        ddl.to_string(),
        message,
    )?;
    Ok(true)
}

impl MainView {
    pub(super) fn open_object_designer(
        &mut self,
        connection_id: uuid::Uuid,
        kind_id: String,
        mode: ObjectFormMode,
        object_ref: Option<ObjectsPanelObjectRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let connection_service = app_state.connection_service.clone();
        let target_database = object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.database.clone())
            .or_else(|| {
                self.workspace_state
                    .read(cx)
                    .active_database()
                    .map(ToString::to_string)
            });
        let main_view = cx.entity().downgrade();
        let request = ObjectFormSpecRequest {
            kind_id,
            mode,
            object_ref: object_ref.clone(),
        };

        cx.spawn_in(window, async move |this, cx| {
            match connection_service
                .object_form_spec(connection_id, target_database, &request)
                .await
            {
                Ok(Some(spec)) => {
                    if let Err(error) = this.update_in(cx, |main_view, window, cx| {
                        let panel = cx.new(|cx| {
                            crate::components::ObjectDesignerPanel::new(
                                connection_id,
                                spec,
                                object_ref,
                                window,
                                cx,
                            )
                        });
                        let subscription = cx.subscribe_in(
                            &panel,
                            window,
                            |this,
                             panel,
                             event: &crate::components::ObjectDesignerPanelEvent,
                             window,
                             cx| {
                                this.handle_object_designer_event(panel.clone(), event, window, cx);
                            },
                        );
                        main_view._subscriptions.push(subscription);
                        main_view.workspace_controller.update(cx, |workspace, cx| {
                            workspace.add_center_item(Arc::new(panel), window, cx);
                        });
                    }) {
                        tracing::warn!(%error, "Failed to open object designer panel");
                    }
                }
                Ok(None) => {
                    if let Err(error) = main_view.update_in(cx, |_main_view, window, cx| {
                        window.push_notification(
                            Notification::error("Object designer is not supported for this object"),
                            cx,
                        );
                    }) {
                        tracing::warn!(%error, "Failed to show object designer unavailable notice");
                    }
                }
                Err(error) => {
                    if let Err(update_error) = main_view.update_in(cx, |_main_view, window, cx| {
                        window.push_notification(
                            Notification::error(format!("Failed to open object designer: {error}")),
                            cx,
                        );
                    }) {
                        tracing::warn!(%update_error, "Failed to show object designer error");
                    }
                }
            }
        })
        .detach();
    }

    fn handle_object_designer_event(
        &mut self,
        panel: Entity<crate::components::ObjectDesignerPanel>,
        event: &crate::components::ObjectDesignerPanelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let crate::components::ObjectDesignerPanelEvent::GenerateDdl {
            connection_id,
            execute,
            request,
        } = event;

        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let connection_service = app_state.connection_service.clone();
        let request = request.clone();
        let target_database = object_form_target_database(
            &request,
            self.workspace_state
                .read(cx)
                .active_database()
                .map(ToString::to_string),
        );
        let connection_id = *connection_id;
        let execute = *execute;
        let objects_panel = self.objects_panel.downgrade();
        let window_handle = window.window_handle();
        let version_repository = self.version_repository.clone();

        panel.update(cx, |panel, cx| panel.set_executing(execute, cx));

        cx.spawn(async move |_this, cx| {
            let ddl_result = connection_service
                .generate_object_form_ddl(connection_id, target_database.clone(), &request)
                .await;
            let statements = match ddl_result {
                Ok(statements) => statements,
                Err(error) => {
                    panel.update(cx, |panel, cx| {
                        panel.set_error(error.to_string(), cx);
                    });
                    return;
                }
            };

            let ddl = statements.join("\n");
            panel.update(cx, |panel, cx| {
                panel.set_ddl_preview(ddl.clone(), cx);
            });

            if !execute {
                return;
            }

            let _ = cx.update_window(window_handle, |_, window, cx| {
                let connection_service = connection_service.clone();
                let target_database = target_database.clone();
                let panel = panel.clone();
                let objects_panel = objects_panel.clone();
                let statements = statements.clone();
                let ddl = ddl.clone();

                window.open_dialog(cx, move |dialog, _window, cx| {
                    let connection_service = connection_service.clone();
                    let target_database = target_database.clone();
                    let panel = panel.clone();
                    let objects_panel = objects_panel.clone();
                    let statements = statements.clone();
                    let ddl = ddl.clone();
                    let request = request.clone();
                    let version_repository = version_repository.clone();

                    dialog
                        .title("Execute Object Change")
                        .child(
                            v_flex()
                                .gap_2()
                                .child(div().child("Review the generated statements before applying this object change."))
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(cx.theme().danger_text)
                                        .child("This changes database objects and may not be reversible."),
                                )
                                .child(
                                    div()
                                        .max_h(px(260.0))
                                        .overflow_y_scrollbar()
                                        .p_2()
                                        .rounded_md()
                                        .bg(cx.theme().secondary)
                                        .font_family(cx.theme().mono_font_family.clone())
                                        .text_xs()
                                        .child(ddl.clone()),
                                ),
                        )
                        .button_props(
                            DialogButtonProps::default()
                                .ok_text("Execute")
                                .ok_variant(ButtonVariant::Warning),
                        )
                        .on_ok(move |_, _window, cx| {
                            let connection_service = connection_service.clone();
                            let target_database = target_database.clone();
                            let panel = panel.clone();
                            let objects_panel = objects_panel.clone();
                            let statements = statements.clone();
                            let ddl = ddl.clone();
                            let request = request.clone();
                            let version_repository = version_repository.clone();

                            cx.spawn(async move |cx| {
                                panel.update(cx, |panel, cx| panel.set_executing(true, cx));
                                let scope = target_database
                                    .map(ConnectionScope::Database)
                                    .unwrap_or(ConnectionScope::Default);
                                let resolved_connection = match connection_service
                                    .resolve_connection(connection_id, scope)
                                    .await
                                {
                                    Ok(connection) => connection,
                                    Err(error) => {
                                        panel.update(cx, |panel, cx| {
                                            panel.set_error(error.to_string(), cx);
                                        });
                                        return;
                                    }
                                };

                                for statement in statements {
                                    if let Err(error) =
                                        resolved_connection.connection.execute(&statement, &[]).await
                                    {
                                        panel.update(cx, |panel, cx| {
                                            panel.set_error(error.to_string(), cx);
                                        });
                                        return;
                                    }
                                }

                                match record_object_form_version_snapshot(
                                    &version_repository,
                                    connection_id,
                                    &request,
                                    &ddl,
                                ) {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        tracing::debug!(
                                            kind_id = %request.kind_id,
                                            "Skipping object form version snapshot for unavailable or unnamed object"
                                        );
                                    }
                                    Err(error) => {
                                        tracing::warn!(
                                            %error,
                                            kind_id = %request.kind_id,
                                            "Failed to record object form version snapshot"
                                        );
                                    }
                                }

                                panel.update(cx, |panel, cx| {
                                    panel.set_ddl_preview(
                                        format!("{ddl}\n\n-- Executed successfully"),
                                        cx,
                                    );
                                });

                                if let Err(error) = objects_panel.update(cx, |panel, cx| {
                                    panel.refresh(cx);
                                }) {
                                    tracing::warn!(%error, "Failed to refresh objects panel after object form execute");
                                }
                            })
                            .detach();
                            true
                        })
                        .confirm()
                });
            });
        })
        .detach();
    }
}
