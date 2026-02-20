// Connection window for creating and editing database connections
// Opens as a separate native window instead of a modal dialog

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::HashMap;
use uuid::Uuid;
use zqlz_connection::SavedConnection;
use zqlz_core::{ConnectionFieldSchema, ConnectionFieldType};
use zqlz_drivers::DriverRegistry;
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, Root, ZqlzIcon,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Input, InputState},
    title_bar::TitleBar,
    v_flex,
};

use crate::app::AppState;

use super::connection_handlers::DatabaseType;

/// Mode of the connection window
#[derive(Clone)]
enum ConnectionWindowMode {
    /// Creating a new connection
    New,
    /// Editing an existing connection
    Edit {
        /// The ID of the connection being edited
        id: Uuid,
        /// The original saved connection data
        saved: SavedConnection,
    },
}

/// A standalone window for creating and editing database connections
pub struct ConnectionWindow {
    /// Current step in the wizard (0 = select db type, 1 = configure connection)
    step: usize,
    /// Mode: new or edit
    mode: ConnectionWindowMode,
    /// Search input for filtering database types
    search_input: Entity<InputState>,
    /// Current search query
    search_query: String,
    /// View mode: true = grid, false = list
    grid_view: bool,
    /// Selected database type
    selected_db_type: Option<DatabaseType>,
    /// Connection name input (for step 2)
    name_input: Option<Entity<InputState>>,
    /// Field inputs for the selected database (for step 2)
    field_inputs: Vec<FieldInput>,
    /// Schema for the selected database
    field_schema: Option<ConnectionFieldSchema>,
    /// Currently active tab (e.g., "general", "ssl", "advanced")
    active_tab: String,
}

/// Input field info for rendering
#[derive(Clone)]
struct FieldInput {
    id: String,
    label: String,
    field_type: ConnectionFieldType,
    input: Entity<InputState>,
    required: bool,
    help_text: Option<String>,
    width: f32,
    row_group: Option<u8>,
    tab: Option<String>,
}

impl ConnectionWindow {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search databases..."));

        Self {
            step: 0,
            mode: ConnectionWindowMode::New,
            search_input,
            search_query: String::new(),
            grid_view: true,
            selected_db_type: None,
            name_input: None,
            field_inputs: Vec::new(),
            field_schema: None,
            active_tab: "general".to_string(),
        }
    }

    /// Create a new window initialized for editing an existing connection
    fn new_for_edit(saved: SavedConnection, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search databases..."));

        let mut instance = Self {
            step: 0,
            mode: ConnectionWindowMode::Edit {
                id: saved.id,
                saved: saved.clone(),
            },
            search_input,
            search_query: String::new(),
            grid_view: true,
            selected_db_type: None,
            name_input: None,
            field_inputs: Vec::new(),
            field_schema: None,
            active_tab: "general".to_string(),
        };

        // Find the database type for this connection
        let db_type = DatabaseType::all()
            .into_iter()
            .find(|db| db.id == saved.driver || (db.id == "mariadb" && saved.driver == "mysql"));

        if let Some(db_type) = db_type {
            instance.select_database_for_edit(db_type, &saved, window, cx);
        }

        instance
    }

    /// Open a new connection window
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

    /// Open a window to edit an existing connection
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
        db_type: DatabaseType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Selected database type: {}", db_type.name);

        // Get the field schema for this driver
        // MariaDB uses mysql driver
        let driver_id = if db_type.id == "mariadb" {
            "mysql"
        } else {
            db_type.id
        };

        let registry = DriverRegistry::with_defaults();
        let schema = if let Some(driver) = registry.get(driver_id) {
            let mut schema = driver.connection_field_schema();
            // Override title for MariaDB
            if db_type.id == "mariadb" {
                schema.title = std::borrow::Cow::Borrowed("MariaDB Connection");
            }
            schema
        } else {
            tracing::warn!("No driver found for: {}", db_type.id);
            return;
        };

        // Create input for connection name
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("My Connection"));

        // Create inputs for each field in the schema
        let field_inputs: Vec<FieldInput> = schema
            .fields
            .iter()
            .map(|field| {
                let placeholder = field
                    .placeholder
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                let is_password = matches!(field.field_type, ConnectionFieldType::Password);
                let input = cx.new(|cx| {
                    InputState::new(window, cx)
                        .placeholder(placeholder)
                        .masked(is_password)
                });

                // Set default value if present
                if let Some(default) = &field.default_value {
                    input.update(cx, |input, cx| {
                        input.set_value(default.to_string(), window, cx);
                    });
                }

                FieldInput {
                    id: field.id.to_string(),
                    label: field.label.to_string(),
                    field_type: field.field_type.clone(),
                    input,
                    required: field.required,
                    help_text: field.help_text.as_ref().map(|s| s.to_string()),
                    width: field.width,
                    row_group: field.row_group,
                    tab: field.tab.as_ref().map(|s| s.to_string()),
                }
            })
            .collect();

        self.selected_db_type = Some(db_type);
        self.name_input = Some(name_input.clone());
        self.field_inputs = field_inputs;
        self.field_schema = Some(schema);
        self.step = 1;

        // Focus the name input
        name_input.focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    /// Select database type and populate fields for editing an existing connection
    fn select_database_for_edit(
        &mut self,
        db_type: DatabaseType,
        saved: &SavedConnection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("Editing connection: {} ({})", saved.name, db_type.name);

        let driver_id = if db_type.id == "mariadb" {
            "mysql"
        } else {
            db_type.id
        };

        let registry = DriverRegistry::with_defaults();
        let schema = if let Some(driver) = registry.get(driver_id) {
            let mut schema = driver.connection_field_schema();
            if db_type.id == "mariadb" {
                schema.title = std::borrow::Cow::Borrowed("MariaDB Connection");
            }
            schema
        } else {
            tracing::warn!("No driver found for: {}", db_type.id);
            return;
        };

        // Create input for connection name and populate with existing value
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("My Connection"));
        name_input.update(cx, |input, cx| {
            input.set_value(saved.name.clone(), window, cx);
        });

        // Create inputs for each field and populate with existing values
        let field_inputs: Vec<FieldInput> = schema
            .fields
            .iter()
            .map(|field| {
                let placeholder = field
                    .placeholder
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                let is_password = matches!(field.field_type, ConnectionFieldType::Password);
                let input = cx.new(|cx| {
                    InputState::new(window, cx)
                        .placeholder(placeholder)
                        .masked(is_password)
                });

                // Set value from saved connection, falling back to default
                let field_id = field.id.to_string();
                let value = saved.params.get(&field_id).cloned().unwrap_or_else(|| {
                    field
                        .default_value
                        .as_ref()
                        .map(|s| s.to_string())
                        .unwrap_or_default()
                });

                if !value.is_empty() {
                    input.update(cx, |input, cx| {
                        input.set_value(value, window, cx);
                    });
                }

                FieldInput {
                    id: field_id,
                    label: field.label.to_string(),
                    field_type: field.field_type.clone(),
                    input,
                    required: field.required,
                    help_text: field.help_text.as_ref().map(|s| s.to_string()),
                    width: field.width,
                    row_group: field.row_group,
                    tab: field.tab.as_ref().map(|s| s.to_string()),
                }
            })
            .collect();

        self.selected_db_type = Some(db_type);
        self.name_input = Some(name_input.clone());
        self.field_inputs = field_inputs;
        self.field_schema = Some(schema);
        self.step = 1;

        // Focus the name input
        name_input.focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    fn go_back(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        // In edit mode, go back should close the window (can't change db type)
        if matches!(self.mode, ConnectionWindowMode::Edit { .. }) {
            return;
        }

        self.step = 0;
        self.selected_db_type = None;
        self.name_input = None;
        self.field_inputs.clear();
        self.field_schema = None;
        cx.notify();
    }

    /// Save the connection (handles both new and edit modes)
    fn save_connection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(db_type) = &self.selected_db_type else {
            return;
        };
        let Some(name_input) = &self.name_input else {
            return;
        };

        let name = name_input.read(cx).text().to_string().trim().to_string();
        if name.is_empty() {
            // TODO: Show validation error
            return;
        }

        // Collect all field values
        let mut params: HashMap<String, String> = HashMap::new();
        for field in &self.field_inputs {
            let value = field.input.read(cx).text().to_string().trim().to_string();

            // Check required fields
            if field.required && value.is_empty() {
                // TODO: Show validation error
                return;
            }

            // For boolean fields, store "true" or "false"
            let value = match &field.field_type {
                ConnectionFieldType::Boolean => {
                    if value.is_empty() {
                        "false".to_string()
                    } else {
                        value
                    }
                }
                _ => value,
            };

            if !value.is_empty() {
                params.insert(field.id.clone(), value);
            }
        }

        // Create or update SavedConnection based on mode
        let saved = match &self.mode {
            ConnectionWindowMode::New => {
                let mut saved = SavedConnection::new(name.clone(), db_type.id.to_string());
                for (key, value) in &params {
                    saved = saved.with_param(key, value);
                }
                tracing::info!("Creating new {} connection: {}", db_type.id, name);
                saved
            }
            ConnectionWindowMode::Edit {
                id,
                saved: original,
            } => {
                let mut updated = original.clone();
                updated.name = name.clone();
                updated.params = params.clone();
                tracing::info!("Updating connection {}: {}", id, name);
                updated
            }
        };

        // Save to app state
        if let Some(app_state) = cx.try_global::<AppState>() {
            app_state.save_connection(saved.clone());
        }

        // Get the current window handle so we can exclude it
        let current_window = window.window_handle();

        // Close this window first
        window.remove_window();

        // Dispatch refresh action to all other windows
        // This ensures the sidebar in the main window updates immediately
        cx.defer(move |cx| {
            use crate::actions::RefreshConnectionsList;
            let windows = cx.windows();
            tracing::debug!(
                "Dispatching RefreshConnectionsList to {} windows",
                windows.len()
            );
            for window_handle in windows {
                // Skip the window we just closed
                if window_handle == current_window {
                    continue;
                }
                let result = cx.update_window(window_handle, |_, window, cx| {
                    tracing::debug!("Dispatching RefreshConnectionsList to window");
                    window.dispatch_action(RefreshConnectionsList.boxed_clone(), cx);
                });
                if let Err(e) = result {
                    tracing::warn!("Failed to dispatch RefreshConnectionsList: {:?}", e);
                }
            }
        });
    }

    fn render_step_0(&self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let db_types = if self.search_query.is_empty() {
            DatabaseType::all()
        } else {
            DatabaseType::filter(&self.search_query)
        };

        v_flex()
            .size_full()
            .p_4()
            .gap_4()
            // Header with search and view toggle
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Select Database Type"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            // View toggle
                            .child(self.render_view_toggle(cx))
                            // Search
                            .child(div().w(px(200.0)).child(Input::new(&self.search_input))),
                    ),
            )
            // Database grid/list
            .child(
                div()
                    .id("db-list-container")
                    .flex_1()
                    .overflow_y_scroll()
                    .child(if self.grid_view {
                        self.render_db_grid(db_types, window, cx).into_any_element()
                    } else {
                        self.render_db_list(db_types, window, cx).into_any_element()
                    }),
            )
    }

    fn render_view_toggle(&self, cx: &Context<Self>) -> impl IntoElement {
        h_flex()
            .gap_0()
            .border_1()
            .border_color(cx.theme().border)
            .rounded(cx.theme().radius)
            .overflow_hidden()
            // Grid view button
            .child(
                div()
                    .id("grid-view-btn")
                    .px_2()
                    .py_1()
                    .cursor_pointer()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when(self.grid_view, |this| this.bg(cx.theme().accent))
                    .hover(|this| this.bg(cx.theme().muted))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.grid_view = true;
                        cx.notify();
                    }))
                    .child(Icon::new(IconName::LayoutDashboard).size_4().text_color(
                        if self.grid_view {
                            cx.theme().foreground
                        } else {
                            cx.theme().muted_foreground
                        },
                    )),
            )
            // List view button
            .child(
                div()
                    .id("list-view-btn")
                    .px_2()
                    .py_1()
                    .cursor_pointer()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when(!self.grid_view, |this| this.bg(cx.theme().accent))
                    .hover(|this| this.bg(cx.theme().muted))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.grid_view = false;
                        cx.notify();
                    }))
                    .child(Icon::new(ZqlzIcon::ListBullets).size_4().text_color(
                        if !self.grid_view {
                            cx.theme().foreground
                        } else {
                            cx.theme().muted_foreground
                        },
                    )),
            )
    }

    fn render_db_grid(
        &self,
        db_types: Vec<DatabaseType>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_wrap()
            .gap_3()
            .p_2()
            .children(db_types.into_iter().map(|db| {
                let db_clone = db.clone();
                div()
                    .id(SharedString::from(format!("db-{}", db.id)))
                    .w(px(160.0))
                    .h(px(120.0))
                    .p_3()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
                    .cursor_pointer()
                    .hover(|this| this.bg(cx.theme().muted).border_color(cx.theme().accent))
                    .when(!db.supported, |this| this.opacity(0.5))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if db_clone.supported {
                            this.select_database(db_clone.clone(), window, cx);
                        }
                    }))
                    .child(db.logo.clone().large())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(cx.theme().foreground)
                            .text_center()
                            .child(db.name),
                    )
            }))
    }

    fn render_db_list(
        &self,
        db_types: Vec<DatabaseType>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        v_flex()
            .gap_1()
            .p_2()
            .children(db_types.into_iter().map(|db| {
                let db_clone = db.clone();
                h_flex()
                    .id(SharedString::from(format!("db-list-{}", db.id)))
                    .w_full()
                    .px_3()
                    .py_2()
                    .gap_3()
                    .items_center()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
                    .cursor_pointer()
                    .hover(|this| this.bg(cx.theme().muted).border_color(cx.theme().accent))
                    .when(!db.supported, |this| this.opacity(0.5))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if db_clone.supported {
                            this.select_database(db_clone.clone(), window, cx);
                        }
                    }))
                    .child(db.logo.clone().medium())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(cx.theme().foreground)
                            .child(db.name),
                    )
            }))
    }

    fn render_step_1(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let Some(db_type) = &self.selected_db_type else {
            return div().into_any_element();
        };
        let Some(name_input) = &self.name_input else {
            return div().into_any_element();
        };

        let is_edit_mode = matches!(self.mode, ConnectionWindowMode::Edit { .. });

        let title = if is_edit_mode {
            self.field_schema
                .as_ref()
                .map(|s| s.title.to_string().replace("New", "Edit"))
                .unwrap_or_else(|| format!("{} Connection", db_type.name))
        } else {
            self.field_schema
                .as_ref()
                .map(|s| s.title.to_string())
                .unwrap_or_else(|| format!("New {} Connection", db_type.name))
        };

        let action_button_label = if is_edit_mode {
            "Save Connection"
        } else {
            "Create Connection"
        };
        
        // Determine which tabs exist
        let tabs = self.get_available_tabs();
        let has_tabs = tabs.len() > 1;

        v_flex()
            .size_full()
            .p_4()
            .gap_4()
            // Header with back button (only in new mode)
            .child(
                h_flex()
                    .gap_3()
                    .items_center()
                    .when(!is_edit_mode, |this| {
                        this.child(
                            Button::new("back")
                                .icon(Icon::new(IconName::ArrowLeft).size_4())
                                .ghost()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.go_back(window, cx);
                                })),
                        )
                    })
                    .child(db_type.logo.clone().medium())
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(title),
                    ),
            )
            // Tabs (if multiple tabs exist)
            .when(has_tabs, |this| {
                this.child(self.render_tabs(&tabs, cx))
            })
            // Form content
            .child(
                div()
                    .id("connection-form-content")
                    .flex_1()
                    .overflow_y_scroll()
                    .child(
                        v_flex()
                            .gap_4()
                            .p_2()
                            // Connection name (always visible)
                            .when(self.active_tab == "general", |this| {
                                this.child(
                                    v_flex()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child("Connection Name"),
                                        )
                                        .child(Input::new(name_input)),
                                )
                            })
                            // Dynamic fields filtered by active tab
                            .children(self.render_tab_fields(window, cx)),
                    ),
            )
            // Footer with action button
            .child(
                h_flex()
                    .justify_end()
                    .gap_2()
                    .pt_2()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        Button::new("cancel")
                            .label("Cancel")
                            .ghost()
                            .on_click(cx.listener(|_, _, window, _cx| {
                                window.remove_window();
                            })),
                    )
                    .child(
                        Button::new("save")
                            .label(action_button_label)
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_connection(window, cx);
                            })),
                    ),
            )
            .into_any_element()
    }
    
    fn get_available_tabs(&self) -> Vec<(String, String)> {
        let mut tabs = vec![("general".to_string(), "General".to_string())];
        let mut seen = std::collections::HashSet::new();
        seen.insert("general".to_string());
        
        for field in &self.field_inputs {
            if let Some(tab) = &field.tab {
                if !seen.contains(tab) {
                    let label = match tab.as_str() {
                        "ssl" => "SSL",
                        "ssh" => "SSH",
                        "http" => "HTTP",
                        "advanced" => "Advanced",
                        _ => tab.as_str(),
                    };
                    tabs.push((tab.clone(), label.to_string()));
                    seen.insert(tab.clone());
                }
            }
        }
        
        tabs
    }
    
    fn render_tabs(&self, tabs: &[(String, String)], cx: &Context<Self>) -> impl IntoElement {
        h_flex()
            .gap_0()
            .border_b_1()
            .border_color(cx.theme().border)
            .children(tabs.iter().map(|(tab_id, tab_label)| {
                let is_active = &self.active_tab == tab_id;
                let tab_id_clone = tab_id.clone();
                
                div()
                    .id(SharedString::from(format!("tab-{}", tab_id)))
                    .px_4()
                    .py_2()
                    .cursor_pointer()
                    .border_b_2()
                    .when(is_active, |this| {
                        this.border_color(cx.theme().accent)
                            .text_color(cx.theme().accent)
                    })
                    .when(!is_active, |this| {
                        this.border_color(gpui::transparent_black())
                            .text_color(cx.theme().muted_foreground)
                    })
                    .hover(|this| {
                        if !is_active {
                            this.text_color(cx.theme().foreground)
                        } else {
                            this
                        }
                    })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.active_tab = tab_id_clone.clone();
                        cx.notify();
                    }))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(if is_active { FontWeight::SEMIBOLD } else { FontWeight::NORMAL })
                            .child(tab_label.clone()),
                    )
            }))
    }
    
    fn render_tab_fields(&self, _window: &mut Window, cx: &mut Context<Self>) -> Vec<AnyElement> {
        // Collect fields for this tab
        let fields: Vec<&FieldInput> = self.field_inputs
            .iter()
            .filter(|field| {
                match &field.tab {
                    Some(tab) => tab == &self.active_tab,
                    None => self.active_tab == "general",
                }
            })
            .collect();
        
        // Group fields by row
        let mut groups: Vec<Vec<&FieldInput>> = Vec::new();
        let mut current_group: Option<u8> = None;
        let mut current_row: Vec<&FieldInput> = Vec::new();

        for field in &fields {
            match (field.row_group, current_group) {
                (Some(group), Some(current)) if group == current => {
                    current_row.push(field);
                }
                (Some(group), _) => {
                    if !current_row.is_empty() {
                        groups.push(current_row);
                    }
                    current_row = vec![field];
                    current_group = Some(group);
                }
                (None, _) => {
                    if !current_row.is_empty() {
                        groups.push(current_row);
                        current_row = Vec::new();
                    }
                    groups.push(vec![field]);
                    current_group = None;
                }
            }
        }

        if !current_row.is_empty() {
            groups.push(current_row);
        }
        
        // Render groups
        groups
            .into_iter()
            .map(|row| {
                if row.len() == 1 {
                    self.render_field(row[0], cx).into_any_element()
                } else {
                    h_flex()
                        .gap_3()
                        .children(row.iter().map(|field| {
                            let element = self.render_field(field, cx);
                            if field.width < 0.5 {
                                div().w(px(100.0)).child(element).into_any_element()
                            } else {
                                div().flex_1().child(element).into_any_element()
                            }
                        }))
                        .into_any_element()
                }
            })
            .collect()
    }

    fn render_field(&self, field: &FieldInput, cx: &Context<Self>) -> impl IntoElement {
        let label_element = h_flex()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(field.label.clone()),
            )
            .when(field.required, |this| {
                this.child(div().text_sm().text_color(cx.theme().danger).child("*"))
            });

        let help_element = field.help_text.as_ref().map(|help| {
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child(help.clone())
        });

        match &field.field_type {
            ConnectionFieldType::FilePath { .. } => {
                let input_for_browse = field.input.clone();

                v_flex()
                    .gap_1()
                    .child(label_element)
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Input::new(&field.input).flex_1())
                            .child(Button::new("browse").label("Browse...").on_click({
                                let input = input_for_browse.clone();
                                cx.listener(move |_, _, window, cx| {
                                    let input = input.clone();
                                    let window_handle = window.window_handle();
                                    let receiver = cx.prompt_for_paths(PathPromptOptions {
                                        files: true,
                                        directories: false,
                                        multiple: false,
                                        prompt: Some("Select File".into()),
                                    });

                                    cx.spawn(async move |_handle, cx| {
                                        if let Ok(Ok(Some(paths))) = receiver.await {
                                            if let Some(path) = paths.first() {
                                                let path_str = path.to_string_lossy().to_string();
                                                _ = window_handle.update(cx, |_, window, cx| {
                                                    input.update(cx, |input, cx| {
                                                        input.set_value(path_str, window, cx);
                                                    });
                                                });
                                            }
                                        }
                                    })
                                    .detach();
                                })
                            })),
                    )
                    .when_some(help_element, |this, help| this.child(help))
            }
            ConnectionFieldType::Select { options } => {
                // For now, render Select as a text input with placeholder showing available options
                // TODO: Implement proper dropdown/select widget
                let options_hint = options
                    .iter()
                    .map(|opt| opt.label.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                
                v_flex()
                    .gap_1()
                    .child(label_element)
                    .child(Input::new(&field.input))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("Options: {}", options_hint))
                    )
                    .when_some(help_element, |this, help| this.child(help))
            }
            ConnectionFieldType::Boolean => {
                // For checkboxes, use a simple text input accepting "true" or "false"
                // TODO: Implement proper checkbox widget
                v_flex()
                    .gap_1()
                    .child(label_element)
                    .child(Input::new(&field.input))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("Enter 'true' or 'false'")
                    )
                    .when_some(help_element, |this, help| this.child(help))
            }
            _ => v_flex()
                .gap_1()
                .child(label_element)
                .child(Input::new(&field.input))
                .when_some(help_element, |this, help| this.child(help)),
        }
    }
}

impl Render for ConnectionWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Handle search input changes
        let current_search = self.search_input.read(cx).text().to_string();
        if current_search != self.search_query {
            self.search_query = current_search;
        }

        v_flex()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            // Title bar reserves space for the macOS traffic light buttons and provides a drag region
            .child(TitleBar::new())
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(match self.step {
                        0 => self.render_step_0(window, cx).into_any_element(),
                        1 => self.render_step_1(window, cx),
                        _ => div().into_any_element(),
                    }),
            )
    }
}
