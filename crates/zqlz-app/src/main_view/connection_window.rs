use gpui::*;
use zqlz_connection::{
    ConnectionForm, ConnectionFormEvent, ConnectionPicker, ConnectionPickerEvent, DatabaseType,
    SavedConnection,
};
use zqlz_ui::widgets::{
    ActiveTheme, Root, TitleBar, WindowExt, notification::Notification, v_flex,
};

use crate::app::AppState;

#[derive(Clone)]
enum ConnectionWindowMode {
    New,
    Edit { saved: SavedConnection },
}

pub struct ConnectionWindow {
    mode: ConnectionWindowMode,
    picker: Entity<ConnectionPicker>,
    connection_form: Option<Entity<ConnectionForm>>,
    _picker_subscription: Subscription,
    _form_subscription: Option<Subscription>,
}

impl ConnectionWindow {
    fn new_picker(
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (Entity<ConnectionPicker>, Subscription) {
        let picker = cx.new(|cx| ConnectionPicker::new(window, cx));
        let picker_subscription = cx.subscribe_in(
            &picker,
            window,
            |this, _, event: &ConnectionPickerEvent, window, cx| match event {
                ConnectionPickerEvent::Selected(database_type) => {
                    this.select_database(*database_type, window, cx);
                }
            },
        );

        (picker, picker_subscription)
    }

    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let (picker, picker_subscription) = Self::new_picker(window, cx);

        Self {
            mode: ConnectionWindowMode::New,
            picker,
            connection_form: None,
            _picker_subscription: picker_subscription,
            _form_subscription: None,
        }
    }

    fn new_for_edit(saved: SavedConnection, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let (picker, picker_subscription) = Self::new_picker(window, cx);
        let mut instance = Self {
            mode: ConnectionWindowMode::Edit {
                saved: saved.clone(),
            },
            picker,
            connection_form: None,
            _picker_subscription: picker_subscription,
            _form_subscription: None,
        };

        if let Some(database_type) = DatabaseType::for_saved_connection(&saved) {
            instance.install_connection_form(database_type, Some(saved), window, cx);
        }

        instance
    }

    pub fn open(cx: &mut App) {
        let window_options = WindowOptions {
            titlebar: Some(TitleBar::title_bar_options()),
            window_bounds: Some(WindowBounds::centered(size(px(700.0), px(550.0)), cx)),
            window_min_size: Some(size(px(500.0), px(400.0))),
            kind: WindowKind::Normal,
            focus: true,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            cx.open_window(window_options, |window, cx| {
                window.activate_window();
                window.set_window_title("New Connection");

                let connection_window = cx.new(|cx| ConnectionWindow::new(window, cx));
                cx.new(|cx| Root::new(connection_window, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    }

    pub fn open_for_edit(saved: SavedConnection, cx: &mut App) {
        let window_title = format!("Edit Connection - {}", saved.name);

        let window_options = WindowOptions {
            titlebar: Some(TitleBar::title_bar_options()),
            window_bounds: Some(WindowBounds::centered(size(px(700.0), px(550.0)), cx)),
            window_min_size: Some(size(px(500.0), px(400.0))),
            kind: WindowKind::Normal,
            focus: true,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            cx.open_window(window_options, |window, cx| {
                window.activate_window();
                window.set_window_title(&window_title);

                let connection_window =
                    cx.new(|cx| ConnectionWindow::new_for_edit(saved, window, cx));
                cx.new(|cx| Root::new(connection_window, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    }

    fn select_database(
        &mut self,
        database_type: DatabaseType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match &self.mode {
            ConnectionWindowMode::New => {
                self.install_connection_form(database_type, None, window, cx);
            }
            ConnectionWindowMode::Edit { saved } => {
                self.install_connection_form(database_type, Some(saved.clone()), window, cx);
            }
        }
    }

    fn install_connection_form(
        &mut self,
        database_type: DatabaseType,
        saved: Option<SavedConnection>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let form = match saved {
            Some(saved) => {
                cx.new(|cx| ConnectionForm::new_for_edit(database_type, saved, window, cx))
            }
            None => cx.new(|cx| ConnectionForm::new_for_new(database_type, window, cx)),
        };

        let form_subscription = cx.subscribe_in(
            &form,
            window,
            |this, form, event: &ConnectionFormEvent, window, cx| match event {
                ConnectionFormEvent::Save(saved) => {
                    this.save_and_close(saved.clone(), window, cx);
                }
                ConnectionFormEvent::Test(saved) => {
                    this.test_connection(form.clone(), saved.clone(), window, cx);
                }
                ConnectionFormEvent::Back => {
                    if matches!(this.mode, ConnectionWindowMode::New) {
                        this.connection_form = None;
                        cx.notify();
                    }
                }
                ConnectionFormEvent::Cancel => {
                    window.remove_window();
                }
            },
        );

        self.connection_form = Some(form);
        self._form_subscription = Some(form_subscription);
        cx.notify();
    }

    fn test_connection(
        &self,
        form: Entity<ConnectionForm>,
        mut saved: SavedConnection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            form.update(cx, |form, cx| {
                form.set_test_result(Err("Application state not available".to_string()), cx)
            });
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        saved.id = uuid::Uuid::new_v4();
        let connections = app_state.connections.clone();
        let window_handle = window.window_handle();
        window.push_notification(Notification::info("Testing connection…"), cx);

        cx.spawn(async move |_handle, cx| {
            let result = match connections.connect(&saved).await {
                Ok(connection_id) => {
                    if let Err(error) = connections.disconnect(connection_id).await {
                        tracing::warn!(
                            %error,
                            connection_id = %connection_id,
                            "Failed to close test connection"
                        );
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            };

            window_handle.update(cx, |_, window, cx| match result {
                Ok(()) => {
                    form.update(cx, |form, cx| form.set_test_result(Ok(()), cx));
                    window.push_notification(Notification::success("Connection test passed"), cx);
                }
                Err(error) => {
                    let error_message = error.to_string();
                    form.update(cx, |form, cx| {
                        form.set_test_result(Err(error_message.clone()), cx)
                    });
                    window.push_notification(
                        Notification::error(format!("Connection test failed: {error}")),
                        cx,
                    );
                }
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
    }

    fn save_and_close(&self, saved: SavedConnection, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(app_state) = cx.try_global::<AppState>() {
            app_state.save_connection(saved.clone());
        }

        let current_window = window.window_handle();
        window.remove_window();

        cx.defer(move |cx| {
            use crate::actions::RefreshConnectionsList;

            for window_handle in cx.windows() {
                if window_handle == current_window {
                    continue;
                }

                if let Err(error) = cx.update_window(window_handle, |_, window, cx| {
                    window.dispatch_action(RefreshConnectionsList.boxed_clone(), cx);
                }) {
                    tracing::warn!("Failed to dispatch RefreshConnectionsList: {:?}", error);
                }
            }
        });
    }
}

impl Render for ConnectionWindow {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(TitleBar::new())
            .child(div().flex_1().w_full().overflow_hidden().child(
                match self.connection_form.as_ref() {
                    Some(connection_form) => connection_form.clone().into_any_element(),
                    None => self.picker.clone().into_any_element(),
                },
            ))
    }
}
