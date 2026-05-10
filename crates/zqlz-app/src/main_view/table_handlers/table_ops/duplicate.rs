//! This module handles table duplication operations (structure and data).

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_services::{DuplicateTableOperation, DuplicateTablesRequest};
use zqlz_table_workflows::DuplicateTablesDecision;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    checkbox::Checkbox,
    input::{Input, InputState},
    v_flex,
};

use crate::MainView;
use crate::app::AppState;
use crate::main_view::table_handlers_utils::validation::validate_table_name;
use crate::workspace_state::RefreshScope;

impl MainView {
    /// Duplicates a table (creates a copy with a new name)
    pub(in crate::main_view) fn duplicate_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Duplicate table: {} on connection {}",
            table_name,
            connection_id
        );

        let Some(decision) =
            self.decide_duplicate_tables_workflow(connection_id, vec![table_name], window, cx)
        else {
            return;
        };

        let single_decision = match decision {
            DuplicateTablesDecision::Single(single_decision) => single_decision,
            DuplicateTablesDecision::Batch(_) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    "Single-table duplicate request produced a batch decision"
                );
                return;
            }
        };

        self.open_duplicate_table_dialog(
            single_decision.connection_id,
            single_decision.source_table_name,
            single_decision.suggested_table_name,
            window,
            cx,
        );
    }

    fn open_duplicate_table_dialog(
        &mut self,
        connection_id: Uuid,
        source_table_name: String,
        suggested_table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let table_service = app_state.table_service.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let source_table_name_for_dialog = source_table_name.clone();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("New table name"));
        name_input.update(cx, |input, cx| {
            input.set_value(suggested_table_name, window, cx);
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
            let source_table_name = source_table_name_for_dialog.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let window_handle = window_handle;
                let main_view = main_view.clone();
                let table_service = table_service.clone();
                let source_table_name = source_table_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();

                dialog
                    .title("Duplicate Table")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .child(format!("Create a copy of table '{}' as:", source_table_name)),
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
                                            .child("The new table will include all data from the source table.")
                                    })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let new_table_name = name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_table_name(&new_table_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        if new_table_name == source_table_name {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("New name must be different from the original".to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        let connection = connection.clone();
                        let main_view = main_view.clone();
                        let table_service = table_service.clone();
                        let source_table_name = source_table_name.clone();

                        cx.spawn(async move |cx| {
                            let outcome = table_service
                                .duplicate_tables(
                                    connection,
                                    DuplicateTablesRequest {
                                        operations: vec![DuplicateTableOperation {
                                            source_table_name: source_table_name.clone(),
                                            target_table_name: new_table_name,
                                        }],
                                        continue_on_error: false,
                                    },
                                )
                                .await;

                            if !outcome.errors.is_empty() {
                                tracing::error!(
                                    source = %source_table_name,
                                    errors = %outcome.errors.join("; "),
                                    "Failed to duplicate table"
                                );
                                return;
                            }

                            if let Some(result) = outcome.duplicated_tables.first() {
                                tracing::info!(
                                    "Table '{}' duplicated as '{}'",
                                    result.source_table_name,
                                    result.target_table_name
                                );

                                if let Err(error) = cx.update_window(window_handle, |_, _window, cx| {
                                    if let Err(update_error) = main_view.update(cx, |main_view, cx| {
                                        main_view.request_refresh(
                                            RefreshScope::ConnectionSurfaces(connection_id),
                                            cx,
                                        );
                                    }) {
                                        tracing::warn!(
                                            %update_error,
                                            "MainView no longer available while refreshing after duplicate"
                                        );
                                    }
                                }) {
                                    tracing::warn!(
                                        %error,
                                        "Window no longer available while refreshing after duplicate"
                                    );
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

    /// Duplicates multiple tables with auto-generated names
    pub(in crate::main_view) fn duplicate_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(decision) =
            self.decide_duplicate_tables_workflow(connection_id, table_names, window, cx)
        else {
            return;
        };

        let batch_decision = match decision {
            DuplicateTablesDecision::Single(single_decision) => {
                self.open_duplicate_table_dialog(
                    single_decision.connection_id,
                    single_decision.source_table_name,
                    single_decision.suggested_table_name,
                    window,
                    cx,
                );
                return;
            }
            DuplicateTablesDecision::Batch(batch_decision) => batch_decision,
        };

        let connection_id = batch_decision.connection_id;
        let table_names = batch_decision.source_table_names;
        let count = table_names.len();
        tracing::info!(
            "Duplicate {} tables: {:?} on connection {}",
            count,
            table_names,
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
        let table_service = app_state.table_service.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let schema_service = app_state.schema_service.clone();
        let suggested_suffix = batch_decision.suggested_suffix;
        let continue_on_error = Rc::new(RefCell::new(batch_decision.continue_on_error_default));
        let new_names: Vec<String> = table_names
            .iter()
            .map(|name| format!("{}{}", name, suggested_suffix))
            .collect();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_service = table_service.clone();
            let schema_service = schema_service.clone();
            let table_names = table_names.clone();
            let dialog_suggested_suffix = suggested_suffix.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(format!("Duplicate {} Tables", count))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Create copies of {} tables with '{}' suffix:",
                            count, dialog_suggested_suffix
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
                                .checked(batch_decision.continue_on_error_default)
                                .on_click(move |checked, _window, _cx| {
                                    *continue_on_error.borrow_mut() = *checked;
                                })
                        }),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let main_view = main_view.clone();
                    let table_service = table_service.clone();
                    let schema_service = schema_service.clone();
                    let table_names = table_names.clone();
                    let suggested_suffix = dialog_suggested_suffix.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let operations: Vec<DuplicateTableOperation> = table_names
                            .iter()
                            .map(|table_name| DuplicateTableOperation {
                                source_table_name: table_name.clone(),
                                target_table_name: format!("{}{}", table_name, suggested_suffix),
                            })
                            .collect();

                        let outcome = table_service
                            .duplicate_tables(
                                connection,
                                DuplicateTablesRequest {
                                    operations,
                                    continue_on_error,
                                },
                            )
                            .await;

                        let duplicated_tables: Vec<String> = outcome
                            .duplicated_tables
                            .iter()
                            .map(|result| result.target_table_name.clone())
                            .collect();

                        if !duplicated_tables.is_empty() {
                            schema_service.invalidate_connection_cache(connection_id);

                            if let Err(error) = cx.update_window(window_handle, |_, _window, cx| {
                                if let Err(update_error) = main_view.update(cx, |main_view, cx| {
                                    main_view.request_refresh(
                                        RefreshScope::ConnectionSurfaces(connection_id),
                                        cx,
                                    );
                                }) {
                                    tracing::warn!(
                                        %update_error,
                                        "MainView no longer available while refreshing after duplicate"
                                    );
                                }
                            }) {
                                tracing::warn!(
                                    %error,
                                    "Window no longer available while refreshing after duplicate"
                                );
                            }
                        }

                        if outcome.errors.is_empty() {
                            tracing::info!("Duplicated {} table(s)", duplicated_tables.len());
                        } else {
                            tracing::warn!(
                                "Duplicated {} of {} tables. Errors: {}",
                                duplicated_tables.len(),
                                table_names.len(),
                                outcome.errors.join("; ")
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }
}
