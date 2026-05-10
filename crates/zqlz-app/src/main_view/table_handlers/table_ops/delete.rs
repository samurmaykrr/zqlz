use gpui::*;
use uuid::Uuid;
use zqlz_services::DeleteTablesRequest;
use zqlz_table_workflows::DeleteTablesDecision;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt, button::ButtonVariant, dialog::DialogButtonProps, v_flex,
};

use crate::MainView;
use crate::app::AppState;
use crate::workspace_state::RefreshScope;

impl MainView {
    #[allow(dead_code)]
    pub(in crate::main_view) fn delete_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Delete table: {} on connection {}",
            table_name,
            connection_id
        );

        let Some(decision) =
            self.decide_delete_tables_workflow(connection_id, vec![table_name], window, cx)
        else {
            return;
        };

        let single_decision = match decision {
            DeleteTablesDecision::Single(single_decision) => single_decision,
            DeleteTablesDecision::Batch(_) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    "Single-table delete request produced a batch decision"
                );
                return;
            }
        };

        self.open_delete_table_dialog(
            single_decision.connection_id,
            single_decision.table_name,
            window,
            cx,
        );
    }

    fn open_delete_table_dialog(
        &mut self,
        connection_id: Uuid,
        table_name: String,
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
        let table_name_for_dialog = table_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let table_service = table_service.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_name = table_name_for_dialog.clone();

            dialog
                .title("Delete Table")
                .child(
                    v_flex()
                        .gap_2()
                        .child(
                            div().child(format!(
                                "Are you sure you want to delete table '{}' ?",
                                table_name
                            )),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone. All data in the table will be permanently lost."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        // Delete-table confirmation is destructive, and this dialog API carries
                        // variant intent rather than a prebuilt Button instance.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let table_service = table_service.clone();
                    let main_view = main_view.clone();
                    let table_name = table_name.clone();

                    cx.spawn(async move |cx| {
                        let outcome = table_service
                            .delete_tables(
                                connection,
                                DeleteTablesRequest {
                                    table_names: vec![table_name.clone()],
                                    continue_on_error: false,
                                },
                            )
                            .await;

                        if !outcome.errors.is_empty() {
                            tracing::error!(
                                table = %table_name,
                                errors = %outcome.errors.join("; "),
                                "Failed to delete table"
                            );
                            return;
                        }

                        if outcome.deleted_table_names.is_empty() {
                            tracing::warn!(
                                table = %table_name,
                                "Delete-table workflow completed without deleting a table"
                            );
                            return;
                        }

                        tracing::info!("Table '{}' deleted successfully", table_name);

                        if let Err(error) = cx.update_window(window_handle, |_, _window, cx| {
                            if let Err(update_error) = main_view.update(cx, |main_view, cx| {
                                main_view.request_refresh(
                                    RefreshScope::ConnectionSurfaces(connection_id),
                                    cx,
                                );
                            }) {
                                tracing::warn!(
                                    %update_error,
                                    "MainView no longer available while refreshing after delete"
                                );
                            }
                        }) {
                            tracing::warn!(
                                %error,
                                "Window no longer available while refreshing after delete"
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    pub(in crate::main_view) fn delete_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use std::cell::RefCell;
        use std::rc::Rc;
        use zqlz_ui::widgets::checkbox::Checkbox;

        let Some(decision) =
            self.decide_delete_tables_workflow(connection_id, table_names, window, cx)
        else {
            return;
        };

        let batch_decision = match decision {
            DeleteTablesDecision::Single(single_decision) => {
                self.open_delete_table_dialog(
                    single_decision.connection_id,
                    single_decision.table_name,
                    window,
                    cx,
                );
                return;
            }
            DeleteTablesDecision::Batch(batch_decision) => batch_decision,
        };

        let connection_id = batch_decision.connection_id;
        let table_names = batch_decision.table_names;
        let count = table_names.len();

        tracing::info!(
            "Delete {} table(s): {:?} on connection {}",
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

        let continue_on_error = Rc::new(RefCell::new(batch_decision.continue_on_error_default));

        let title = if count == 1 {
            "Delete Table".to_string()
        } else {
            "Delete Tables".to_string()
        };
        let message = "Are you sure you want to remove the selected tables?".to_string();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_service = table_service.clone();
            let schema_service = schema_service.clone();
            let table_names = table_names.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(title.clone())
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(message.clone()))
                        .child(
                            div()
                                .text_sm()
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_color(cx.theme().muted_foreground)
                                .child(table_names.join(", ")),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone. All data in the table(s) will be permanently lost."),
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
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        // Batch delete uses dialog button metadata, so this remains an explicit
                        // destructive ButtonVariant rather than a `.danger()` helper call.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let main_view = main_view.clone();
                    let table_service = table_service.clone();
                    let schema_service = schema_service.clone();
                    let table_names = table_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let outcome = table_service
                            .delete_tables(
                                connection,
                                DeleteTablesRequest {
                                    table_names: table_names.clone(),
                                    continue_on_error,
                                },
                            )
                            .await;

                        if !outcome.deleted_table_names.is_empty() {
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
                                        "MainView no longer available while refreshing after delete"
                                    );
                                }
                            }) {
                                tracing::warn!(
                                    %error,
                                    "Window no longer available while refreshing after delete"
                                );
                            }
                        }

                        if !outcome.errors.is_empty() {
                            tracing::warn!(
                                "Deleted {} of {} tables. Errors: {}",
                                outcome.deleted_table_names.len(),
                                table_names.len(),
                                outcome.errors.join("; ")
                            );
                        } else if !outcome.deleted_table_names.is_empty() {
                            tracing::info!(
                                "Successfully deleted {} table(s)",
                                outcome.deleted_table_names.len()
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
