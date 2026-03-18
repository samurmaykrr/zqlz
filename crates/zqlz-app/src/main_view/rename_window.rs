use std::sync::Arc;

use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, Disableable, Root,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputEvent, InputState},
    title_bar::TitleBar,
    v_flex,
};

use crate::main_view::MainView;
use crate::main_view::refresh::{RefreshTarget, SurfaceRefreshOptions};

use super::{
    table_handlers_utils::validation::validate_table_name,
    view_handlers::{
        build_create_view_statement, build_drop_view_statement, build_rename_table_statement,
        fetch_view_definition, validate_view_name,
    },
};

#[derive(Clone)]
enum RenameTarget {
    Table {
        connection_id: Uuid,
        old_name: String,
        connection: Arc<dyn zqlz_core::Connection>,
        main_view: WeakEntity<MainView>,
    },
    View {
        connection_id: Uuid,
        old_name: String,
        connection: Arc<dyn zqlz_core::Connection>,
        main_view: WeakEntity<MainView>,
    },
}

impl RenameTarget {
    fn dialog_title(&self) -> &'static str {
        match self {
            Self::Table { .. } => "Rename Table",
            Self::View { .. } => "Rename View",
        }
    }

    fn object_label(&self) -> &'static str {
        match self {
            Self::Table { .. } => "table",
            Self::View { .. } => "view",
        }
    }

    fn placeholder(&self) -> &'static str {
        match self {
            Self::Table { .. } => "New table name",
            Self::View { .. } => "New view name",
        }
    }

    fn action_label(&self) -> &'static str {
        match self {
            Self::Table { .. } => "Rename Table",
            Self::View { .. } => "Rename View",
        }
    }

    fn old_name(&self) -> &str {
        match self {
            Self::Table { old_name, .. } | Self::View { old_name, .. } => old_name,
        }
    }

    fn validate_name(&self, new_name: &str) -> Option<&'static str> {
        match self {
            Self::Table { .. } => validate_table_name(new_name),
            Self::View { .. } => validate_view_name(new_name),
        }
    }

    async fn rename(&self, new_name: &str) -> Result<(), String> {
        match self {
            Self::Table {
                old_name,
                connection,
                ..
            } => {
                let sql = build_rename_table_statement(connection, old_name, new_name);

                connection
                    .execute(&sql, &[])
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("Failed to rename table: {error}"))
            }
            Self::View {
                old_name,
                connection,
                ..
            } => {
                let definition = fetch_view_definition(connection, None, old_name).await?;

                let drop_sql = build_drop_view_statement(connection, old_name, false);
                connection
                    .execute(&drop_sql, &[])
                    .await
                    .map_err(|error| format!("Failed to drop old view: {error}"))?;

                let create_sql = build_create_view_statement(connection, new_name, &definition);
                match connection.execute(&create_sql, &[]).await {
                    Ok(_) => Ok(()),
                    Err(error) => {
                        let restore_sql =
                            build_create_view_statement(connection, old_name, &definition);
                        match connection.execute(&restore_sql, &[]).await {
                            Ok(_) => Err(format!("Failed to create renamed view: {error}")),
                            Err(restore_error) => Err(format!(
                                "Failed to create renamed view: {error}. Failed to restore original view: {restore_error}"
                            )),
                        }
                    }
                }
            }
        }
    }

    fn refresh_after_success(&self, new_name: &str, cx: &mut App) {
        match self {
            Self::Table {
                connection_id,
                main_view,
                ..
            } => {
                if let Err(error) = main_view.update(cx, |main_view, cx| {
                    main_view.refresh_connection_surfaces(
                        RefreshTarget::Connection(*connection_id),
                        SurfaceRefreshOptions::SIDEBAR_AND_OBJECTS,
                        cx,
                    );
                }) {
                    tracing::warn!(?error, new_name, "Failed to refresh after table rename");
                }
            }
            Self::View {
                connection_id,
                main_view,
                ..
            } => {
                if let Err(error) = main_view.update(cx, |main_view, cx| {
                    main_view.refresh_connection_surfaces(
                        RefreshTarget::Connection(*connection_id),
                        SurfaceRefreshOptions::SIDEBAR_AND_OBJECTS,
                        cx,
                    );
                }) {
                    tracing::warn!(?error, new_name, "Failed to refresh after view rename");
                }
            }
        }
    }
}

pub(super) struct RenameWindow {
    focus_handle: FocusHandle,
    target: RenameTarget,
    name_input: Entity<InputState>,
    error_message: Option<String>,
    is_submitting: bool,
    _subscriptions: Vec<Subscription>,
}

impl RenameWindow {
    pub(super) fn open_table(
        connection_id: Uuid,
        table_name: String,
        _driver_name: String,
        connection: Arc<dyn zqlz_core::Connection>,
        main_view: WeakEntity<MainView>,
        cx: &mut App,
    ) {
        Self::open(
            RenameTarget::Table {
                connection_id,
                old_name: table_name,
                connection,
                main_view,
            },
            cx,
        );
    }

    pub(super) fn open_view(
        connection_id: Uuid,
        view_name: String,
        _driver_name: String,
        connection: Arc<dyn zqlz_core::Connection>,
        main_view: WeakEntity<MainView>,
        cx: &mut App,
    ) {
        Self::open(
            RenameTarget::View {
                connection_id,
                old_name: view_name,
                connection,
                main_view,
            },
            cx,
        );
    }

    fn open(target: RenameTarget, cx: &mut App) {
        let window_title = target.dialog_title().to_string();
        let rename_target = target.clone();
        let window_options = WindowOptions {
            titlebar: Some(TitleBar::title_bar_options()),
            window_bounds: Some(WindowBounds::centered(size(px(560.0), px(260.0)), cx)),
            window_min_size: Some(size(px(460.0), px(220.0))),
            kind: WindowKind::Normal,
            focus: true,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            cx.open_window(window_options, move |window, cx| {
                window.activate_window();
                window.set_window_title(&window_title);

                let rename_window =
                    cx.new(|cx| RenameWindow::new(rename_target.clone(), window, cx));
                cx.new(|cx| Root::new(rename_window, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    }

    fn new(target: RenameTarget, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let placeholder = target.placeholder();
        let initial_name = target.old_name().to_string();
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder(placeholder));
        name_input.update(cx, |input, cx| {
            input.set_value(initial_name, window, cx);
        });

        let subscription = cx.subscribe(&name_input, |this, _, event, cx| {
            if matches!(event, InputEvent::Change) && this.error_message.is_some() {
                this.error_message = None;
                cx.notify();
            }
        });

        name_input.focus_handle(cx).focus(window, cx);

        Self {
            focus_handle: cx.focus_handle(),
            target,
            name_input,
            error_message: None,
            is_submitting: false,
            _subscriptions: vec![subscription],
        }
    }

    fn cancel(&mut self, window: &mut Window) {
        if !self.is_submitting {
            window.remove_window();
        }
    }

    fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_submitting {
            return;
        }

        let new_name = self
            .name_input
            .read(cx)
            .text()
            .to_string()
            .trim()
            .to_string();

        if let Some(error_message) = self.target.validate_name(&new_name) {
            self.error_message = Some(error_message.to_string());
            cx.notify();
            return;
        }

        if new_name == self.target.old_name() {
            window.remove_window();
            return;
        }

        self.is_submitting = true;
        self.error_message = None;
        cx.notify();

        let target = self.target.clone();
        let window_handle = window.window_handle();
        cx.spawn_in(window, async move |this, cx| {
            let rename_result = target.rename(&new_name).await;
            let old_name = target.old_name().to_string();
            let object_label = target.object_label().to_string();
            let rename_succeeded = rename_result.is_ok();

            let _ = this.update(cx, |this, cx| {
                this.is_submitting = false;

                match &rename_result {
                    Ok(()) => {
                        tracing::info!(
                            old_name = old_name,
                            new_name = new_name,
                            object_type = object_label,
                            "Rename completed successfully"
                        );
                        target.refresh_after_success(&new_name, cx);
                        cx.notify();
                    }
                    Err(error_message) => {
                        tracing::error!(
                            old_name = old_name,
                            new_name = new_name,
                            object_type = object_label,
                            error = %error_message,
                            "Rename failed"
                        );
                        this.error_message = Some(error_message.clone());
                        cx.notify();
                    }
                }
            });

            if rename_succeeded {
                let _ = cx.update_window(window_handle, |_, window, _cx| {
                    window.remove_window();
                    anyhow::Ok(())
                });
            } else {
                let weak_window = this.clone();
                let _ = cx.update_window(window_handle, move |_, window, cx| {
                    let _ = weak_window.update(cx, |this, cx| {
                        this.name_input.focus_handle(cx).focus(window, cx);
                    });
                });
            }
        })
        .detach();
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_1()
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(self.target.dialog_title()),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(format!(
                        "Enter a new name for {} '{}'.",
                        self.target.object_label(),
                        self.target.old_name()
                    )),
            )
    }

    fn render_error_message(&self, cx: &Context<Self>) -> impl IntoElement {
        let mut message = div().text_xs().h(px(18.0));

        if let Some(error_message) = &self.error_message {
            message = message
                .text_color(cx.theme().danger_text)
                .child(error_message.clone());
        }

        message
    }
}

impl Render for RenameWindow {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(TitleBar::new())
            .child(
                v_flex()
                    .size_full()
                    .justify_between()
                    .child(
                        v_flex().gap_4().p_4().child(self.render_header(cx)).child(
                            v_flex()
                                .gap_1()
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .child("Name"),
                                )
                                .child(Input::new(&self.name_input).w_full())
                                .child(self.render_error_message(cx)),
                        ),
                    )
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .p_4()
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .child(
                                Button::new("cancel")
                                    .label("Cancel")
                                    .ghost()
                                    .disabled(self.is_submitting)
                                    .on_click(cx.listener(|this, _, window, _cx| {
                                        this.cancel(window);
                                    })),
                            )
                            .child(
                                Button::new("rename")
                                    .label(self.target.action_label())
                                    .primary()
                                    .disabled(self.is_submitting)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.submit(window, cx);
                                    })),
                            ),
                    ),
            )
    }
}

impl Focusable for RenameWindow {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}
