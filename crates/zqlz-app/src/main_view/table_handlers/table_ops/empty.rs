use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_core::TableRef;
use zqlz_services::EmptyTablesRequest;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt, button::ButtonVariant, checkbox::Checkbox,
    dialog::DialogButtonProps, v_flex,
};

use crate::app::AppState;
use crate::main_view::MainView;
use crate::main_view::table_handlers::table_ops::notify_table_operation_error;
use crate::workspace_state::RefreshScope;

impl MainView {
    fn open_empty_table_dialog(
        &mut self,
        connection_id: Uuid,
        table: TableRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let table_name = table.name.clone();
        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let connection_service = app_state.connection_service.clone();
        let table_service = app_state.table_service.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let table_name_for_dialog = table_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection_service = connection_service.clone();
            let table_service = table_service.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_name = table_name_for_dialog.clone();
            let table = table.clone();

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
                    let connection_service = connection_service.clone();
                    let table_service = table_service.clone();
                    let main_view = main_view.clone();
                    let table_name = table_name.clone();
                    let table = table.clone();

                    cx.spawn(async move |cx| {
                        let resolved_connection = match connection_service
                            .resolve_connection(connection_id, table.scope())
                            .await
                        {
                            Ok(resolved_connection) => resolved_connection,
                            Err(error) => {
                                tracing::error!(%error, table = %table_name, "Failed to resolve empty-table connection");
                                notify_table_operation_error(
                                    &cx,
                                    window_handle,
                                    format!("Failed to empty '{}': {}", table_name, error),
                                );
                                return;
                            }
                        };

                        let outcome = table_service
                            .empty_tables(
                                resolved_connection.connection,
                                EmptyTablesRequest {
                                    tables: vec![table],
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
                            notify_table_operation_error(
                                &cx,
                                window_handle,
                                format!(
                                    "Failed to empty '{}': {}",
                                    table_name,
                                    outcome.errors.join("; ")
                                ),
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
        tables: Vec<TableRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let [table] = tables.as_slice() {
            self.open_empty_table_dialog(connection_id, table.clone(), window, cx);
            return;
        }
        // ponytail: one connection per batch; the objects panel lists one database at a time.
        let Some(scope) = tables.first().map(TableRef::scope) else {
            return;
        };
        let table_names: Vec<String> = tables.iter().map(|table| table.name.clone()).collect();
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

        let connection_service = app_state.connection_service.clone();
        let table_service = app_state.table_service.clone();
        let window_handle = window.window_handle();
        let main_view = cx.entity().downgrade();
        let continue_on_error = Rc::new(RefCell::new(false));

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection_service = connection_service.clone();
            let table_service = table_service.clone();
            let window_handle = window_handle;
            let main_view = main_view.clone();
            let table_names = table_names.clone();
            let tables = tables.clone();
            let scope = scope.clone();
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
                                .checked(false)
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
                    let connection_service = connection_service.clone();
                    let table_service = table_service.clone();
                    let main_view = main_view.clone();
                    let table_names = table_names.clone();
                    let tables = tables.clone();
                    let scope = scope.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let resolved_connection = match connection_service
                            .resolve_connection(connection_id, scope)
                            .await
                        {
                            Ok(resolved_connection) => resolved_connection,
                            Err(error) => {
                                tracing::error!(%error, "Failed to resolve batch empty connection");
                                notify_table_operation_error(
                                    &cx,
                                    window_handle,
                                    format!("Failed to empty tables: {}", error),
                                );
                                return;
                            }
                        };

                        let outcome = table_service
                            .empty_tables(
                                resolved_connection.connection,
                                EmptyTablesRequest {
                                    tables,
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
                            notify_table_operation_error(
                                &cx,
                                window_handle,
                                format!(
                                    "Emptied {} of {} tables. {}",
                                    outcome.emptied_table_names.len(),
                                    table_names.len(),
                                    outcome.errors.join("; ")
                                ),
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
