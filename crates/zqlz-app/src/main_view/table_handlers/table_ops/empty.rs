use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_services::EmptyTablesRequest;
use zqlz_table_workflows::EmptyTablesDecision;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt, button::ButtonVariant, checkbox::Checkbox,
    dialog::DialogButtonProps, v_flex,
};

use crate::app::AppState;
use crate::main_view::MainView;
use crate::workspace_state::RefreshScope;

impl MainView {
    pub(in crate::main_view) fn empty_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Empty table: {} on connection {}",
            table_name,
            connection_id
        );

        let Some(decision) =
            self.decide_empty_tables_workflow(connection_id, vec![table_name], window, cx)
        else {
            return;
        };

        let single_decision = match decision {
            EmptyTablesDecision::Single(single_decision) => single_decision,
            EmptyTablesDecision::Batch(_) => {
                tracing::warn!(
                    connection_id = %connection_id,
                    "Single-table empty request produced a batch decision"
                );
                return;
            }
        };

        self.open_empty_table_dialog(
            single_decision.connection_id,
            single_decision.table_name,
            window,
            cx,
        );
    }

    fn open_empty_table_dialog(
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
                .title("Empty Table")
                .child(
                    v_flex()
                        .gap_2()
                        .child(
                            div().child(format!(
                                "Are you sure you want to delete all data from table '{}'?",
                                table_name
                            )),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This will permanently delete all rows. The table structure will be preserved."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Empty Table")
                        // Emptying a table is destructive, and dialog button props are variant-
                        // based because the actual Button is created later by the dialog widget.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let table_service = table_service.clone();
                    let main_view = main_view.clone();
                    let table_name = table_name.clone();

                    cx.spawn(async move |cx| {
                        let outcome = table_service
                            .empty_tables(
                                connection,
                                EmptyTablesRequest {
                                    table_names: vec![table_name.clone()],
                                    continue_on_error: false,
                                },
                            )
                            .await;

                        if !outcome.errors.is_empty() {
                            tracing::error!(
                                table = %table_name,
                                errors = %outcome.errors.join("; "),
                                "Failed to empty table"
                            );
                            return;
                        }

                        if outcome.emptied_table_names.is_empty() {
                            tracing::warn!(
                                table = %table_name,
                                "Empty-table workflow completed without emptying a table"
                            );
                            return;
                        }

                        tracing::info!(
                            "Table '{}' emptied successfully ({} rows deleted)",
                            table_name,
                            outcome.total_rows_deleted
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
                                    "MainView no longer available while refreshing after empty"
                                );
                            }
                        }) {
                            tracing::warn!(
                                %error,
                                "Window no longer available while refreshing after empty"
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    pub(in crate::main_view) fn empty_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(decision) =
            self.decide_empty_tables_workflow(connection_id, table_names, window, cx)
        else {
            return;
        };

        let batch_decision = match decision {
            EmptyTablesDecision::Single(single_decision) => {
                self.open_empty_table_dialog(
                    single_decision.connection_id,
                    single_decision.table_name,
                    window,
                    cx,
                );
                return;
            }
            EmptyTablesDecision::Batch(batch_decision) => batch_decision,
        };

        let connection_id = batch_decision.connection_id;
        let table_names = batch_decision.table_names;
        let count = table_names.len();

        tracing::info!(
            "Empty {} tables: {:?} on connection {}",
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
        let continue_on_error = Rc::new(RefCell::new(batch_decision.continue_on_error_default));

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let table_service = table_service.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_names = table_names.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(format!("Empty {} Tables", count))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Are you sure you want to delete all data from these {} tables?",
                            count
                        )))
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
                                .child("This will permanently delete all rows. The table structure will be preserved."),
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
                        .ok_text("Empty")
                        // Batch empty uses the dialog's deferred button configuration, so Danger is
                        // expressed as a ButtonVariant instead of a direct button helper.
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let table_service = table_service.clone();
                    let main_view = main_view.clone();
                    let table_names = table_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let outcome = table_service
                            .empty_tables(
                                connection,
                                EmptyTablesRequest {
                                    table_names: table_names.clone(),
                                    continue_on_error,
                                },
                            )
                            .await;

                        // Refresh objects panel after any successful emptying (row counts changed)
                        if !outcome.emptied_table_names.is_empty()
                            && let Err(error) = cx.update_window(window_handle, |_, _window, cx| {
                                if let Err(update_error) = main_view.update(cx, |main_view, cx| {
                                    main_view.request_refresh(
                                        RefreshScope::ConnectionSurfaces(connection_id),
                                        cx,
                                    );
                                }) {
                                    tracing::warn!(
                                        %update_error,
                                        "MainView no longer available while refreshing after empty"
                                    );
                                }
                            })
                        {
                            tracing::warn!(
                                %error,
                                "Window no longer available while refreshing after empty"
                            );
                        }

                        // Log result
                        if outcome.errors.is_empty() {
                            tracing::info!(
                                "Emptied {} table(s), {} rows deleted",
                                outcome.emptied_table_names.len(),
                                outcome.total_rows_deleted
                            );
                        } else {
                            tracing::warn!(
                                "Emptied {} of {} tables. Errors: {}",
                                outcome.emptied_table_names.len(),
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
