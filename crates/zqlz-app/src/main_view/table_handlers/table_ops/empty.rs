use gpui::prelude::FluentBuilder;
use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    checkbox::Checkbox,
    v_flex,
};

use crate::app::AppState;
use crate::components::ObjectsPanelEvent;
use crate::main_view::MainView;

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

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let objects_panel = self.objects_panel.downgrade();
        let table_name_for_dialog = table_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let objects_panel = objects_panel.clone();
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
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let objects_panel = objects_panel.clone();
                    let table_name = table_name.clone();

                    cx.spawn(async move |mut cx| {
                        let sql = format!("DELETE FROM \"{}\"", table_name);
                        match connection.execute(&sql, &[]).await {
                            Ok(result) => {
                                let rows_deleted = result.affected_rows;
                                tracing::info!(
                                    "Table '{}' emptied successfully ({} rows deleted)",
                                    table_name,
                                    rows_deleted
                                );

                                // Row counts changed, refresh objects panel
                                cx.update(|cx| {
                                    _ = objects_panel.update(cx, |_, cx| {
                                        cx.emit(ObjectsPanelEvent::Refresh);
                                    });
                                })
;
                            }
                            Err(e) => {
                                tracing::error!("Failed to empty table: {}", e);
                            }
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
        if table_names.is_empty() {
            return;
        }

        let count = table_names.len();
        let is_multi = count > 1;

        // For single table, use the existing handler
        if !is_multi {
            self.empty_table(connection_id, table_names.into_iter().next().unwrap(), window, cx);
            return;
        }

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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let objects_panel = self.objects_panel.downgrade();
        let continue_on_error = Rc::new(RefCell::new(false));

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let objects_panel = objects_panel.clone();
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
                                .checked(false)
                                .on_click(move |checked, _window, _cx| {
                                    *continue_on_error.borrow_mut() = *checked;
                                })
                        }),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Empty")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let objects_panel = objects_panel.clone();
                    let table_names = table_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |mut cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut emptied_tables: Vec<String> = Vec::new();
                        let mut total_rows = 0u64;

                        for table_name in &table_names {
                            let sql = format!("DELETE FROM \"{}\"", table_name);
                            match connection.execute(&sql, &[]).await {
                                Ok(result) => {
                                    let rows_deleted = result.affected_rows;
                                    total_rows += rows_deleted;
                                    tracing::info!(
                                        "Table '{}' emptied successfully ({} rows deleted)",
                                        table_name,
                                        rows_deleted
                                    );
                                    emptied_tables.push(table_name.clone());
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", table_name, e);
                                    tracing::error!("Failed to empty table {}", error_msg);

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        // Refresh objects panel after any successful emptying (row counts changed)
                        if !emptied_tables.is_empty() {
                            cx.update(|cx| {
                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            })
;
                        }

                        // Log result
                        if errors.is_empty() {
                            tracing::info!(
                                "Emptied {} table(s), {} rows deleted",
                                emptied_tables.len(),
                                total_rows
                            );
                        } else {
                            tracing::warn!(
                                "Emptied {} of {} tables. Errors: {}",
                                emptied_tables.len(),
                                table_names.len(),
                                errors.join("; ")
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
