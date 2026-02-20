// This module handles table renaming operations.

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    WindowExt,
    input::{Input, InputState},
    v_flex,
};

use crate::app::AppState;
use crate::components::ObjectsPanelEvent;
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::validation::validate_table_name;

impl MainView {
    pub(in crate::main_view) fn rename_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Rename table: {} on connection {}",
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

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let old_table_name = table_name.clone();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("New table name"));
        name_input.update(cx, |input, cx| {
            input.set_value(table_name.clone(), window, cx);
        });

        // Error state for validation feedback
        let error_message: Entity<Option<String>> = cx.new(|_| None);

        // Clear error when input changes
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
            let old_table_name = old_table_name.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let connection_sidebar = connection_sidebar.clone();
                let objects_panel = objects_panel.clone();
                let old_table_name = old_table_name.clone();
                let driver_name = driver_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();

                dialog
                    .title("Rename Table")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div().text_sm().child(format!(
                                    "Enter a new name for table '{}':",
                                    old_table_name
                                )),
                            )
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div()
                                    .text_xs()
                                    .h(px(16.0))
                                    .when_some(error, |this, err| {
                                        this.text_color(gpui::red()).child(err)
                                    })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let new_table_name =
                            name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_table_name(&new_table_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        if new_table_name == old_table_name {
                            return true;
                        }

                        let connection = connection.clone();
                        let connection_sidebar = connection_sidebar.clone();
                        let objects_panel = objects_panel.clone();
                        let old_table_name = old_table_name.clone();
                        let driver_name = driver_name.clone();

                        cx.spawn(async move |cx| {
                            let sql = if driver_name.contains("postgres") {
                                format!(
                                    "ALTER TABLE \"{}\" RENAME TO \"{}\"",
                                    old_table_name, new_table_name
                                )
                            } else if driver_name.contains("mysql")
                                || driver_name.contains("mariadb")
                            {
                                format!("RENAME TABLE `{}` TO `{}`", old_table_name, new_table_name)
                            } else {
                                format!(
                                    "ALTER TABLE \"{}\" RENAME TO \"{}\"",
                                    old_table_name, new_table_name
                                )
                            };

                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!(
                                        "Table '{}' renamed to '{}' successfully",
                                        old_table_name,
                                        new_table_name
                                    );

                                    cx.update(|cx| {
                                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                                            sidebar.remove_table(
                                                connection_id,
                                                &old_table_name,
                                                cx,
                                            );
                                            sidebar.add_table(
                                                connection_id,
                                                new_table_name.clone(),
                                                cx,
                                            );
                                        });
                                        _ = objects_panel.update(cx, |_, cx| {
                                            cx.emit(ObjectsPanelEvent::Refresh);
                                        });
                                    })
;
                                }
                                Err(e) => {
                                    tracing::error!("Failed to rename table: {}", e);
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
}
