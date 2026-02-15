//! Trigger Designer Panel
//!
//! A panel for designing and modifying database triggers.
//! Features a form interface with:
//! - Trigger name input
//! - Table selection
//! - Timing selection (BEFORE/AFTER/INSTEAD OF)
//! - Event selection (INSERT/UPDATE/DELETE)
//! - Trigger body editor with syntax highlighting
//! - DDL preview

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, IndexPath, Sizable,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectEvent, SelectItem, SelectState},
    tab::{Tab, TabBar},
    typography::code,
    v_flex,
};

use crate::events::TriggerDesignerEvent;
use crate::models::{DatabaseDialect, TriggerDesign, TriggerEvent, TriggerTiming};

/// Active tab in the trigger designer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DesignerTab {
    #[default]
    General,
    Body,
    SqlPreview,
}

/// Wrapper for timing options in select
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimingOption {
    pub timing: TriggerTiming,
    pub label: String,
}

impl SelectItem for TimingOption {
    type Value = TriggerTiming;

    fn title(&self) -> SharedString {
        SharedString::from(self.label.clone())
    }

    fn value(&self) -> &Self::Value {
        &self.timing
    }
}

/// Wrapper for table options in select
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOption {
    pub name: String,
}

impl SelectItem for TableOption {
    type Value = String;

    fn title(&self) -> SharedString {
        SharedString::from(self.name.clone())
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }

    fn matches(&self, query: &str) -> bool {
        self.name.to_lowercase().contains(&query.to_lowercase())
    }
}

/// Trigger Designer Panel for creating and modifying triggers
pub struct TriggerDesignerPanel {
    focus_handle: FocusHandle,

    /// Connection ID this design is for
    connection_id: Uuid,

    /// The trigger design being edited
    design: TriggerDesign,

    /// Original trigger name (for renaming)
    original_name: Option<String>,

    /// Current active tab
    active_tab: DesignerTab,

    /// Input state for trigger name
    name_input: Entity<InputState>,

    /// Select state for table
    table_select: Entity<SelectState<Vec<TableOption>>>,

    /// Available tables
    #[allow(dead_code)]
    tables: Vec<TableOption>,

    /// Select state for timing
    timing_select: Entity<SelectState<Vec<TimingOption>>>,

    /// Available timing options for current dialect
    #[allow(dead_code)]
    timing_options: Vec<TimingOption>,

    /// Input state for WHEN condition
    when_input: Entity<InputState>,

    /// Input state for trigger body
    body_input: Entity<InputState>,

    /// Checkboxes for events
    insert_checked: bool,
    update_checked: bool,
    delete_checked: bool,

    /// FOR EACH ROW vs STATEMENT (Postgres only)
    for_each_row: bool,

    /// Generated DDL preview (cached)
    ddl_preview: Option<String>,

    /// Whether the design has been modified
    is_dirty: bool,

    /// Subscriptions to input events
    _subscriptions: Vec<gpui::Subscription>,
}

impl TriggerDesignerPanel {
    /// Create a new trigger designer for a new trigger
    pub fn new(
        connection_id: Uuid,
        dialect: DatabaseDialect,
        tables: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let design = TriggerDesign::new(dialect);
        Self::create(connection_id, design, None, tables, window, cx)
    }

    /// Create a trigger designer for editing an existing trigger
    pub fn edit(
        connection_id: Uuid,
        design: TriggerDesign,
        tables: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let original_name = Some(design.name.clone());
        Self::create(connection_id, design, original_name, tables, window, cx)
    }

    fn create(
        connection_id: Uuid,
        design: TriggerDesign,
        original_name: Option<String>,
        tables: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut subscriptions = Vec::new();

        // Create name input
        let name_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Trigger name");
            state.set_value(&design.name, window, cx);
            state
        });
        subscriptions.push(
            cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );

        // Create table options
        let table_options: Vec<TableOption> = tables
            .iter()
            .map(|name| TableOption { name: name.clone() })
            .collect();

        // Find selected table index as IndexPath
        let selected_table_idx = table_options
            .iter()
            .position(|t| t.name == design.table_name)
            .map(|i| IndexPath::default().row(i));

        let table_select = cx.new(|cx| {
            SelectState::new(table_options.clone(), selected_table_idx, window, cx).searchable(true)
        });
        subscriptions.push(cx.subscribe(
            &table_select,
            |this, _, event: &SelectEvent<Vec<TableOption>>, cx| {
                // SelectEvent::Confirm contains the Value type (String)
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.design.table_name = value.clone();
                    this.mark_dirty(cx);
                }
            },
        ));

        // Create timing options
        let timing_options: Vec<TimingOption> = TriggerTiming::all_for_dialect(design.dialect)
            .into_iter()
            .map(|t| TimingOption {
                timing: t,
                label: t.as_str().to_string(),
            })
            .collect();

        let selected_timing_idx = timing_options
            .iter()
            .position(|t| t.timing == design.timing)
            .map(|i| IndexPath::default().row(i));

        let timing_select =
            cx.new(|cx| SelectState::new(timing_options.clone(), selected_timing_idx, window, cx));
        subscriptions.push(cx.subscribe(
            &timing_select,
            |this, _, event: &SelectEvent<Vec<TimingOption>>, cx| {
                // SelectEvent::Confirm contains the Value type (TriggerTiming)
                if let SelectEvent::Confirm(Some(timing)) = event {
                    this.design.timing = *timing;
                    this.mark_dirty(cx);
                }
            },
        ));

        // Create WHEN condition input
        let when_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Optional WHEN condition");
            if let Some(ref cond) = design.when_condition {
                state.set_value(cond, window, cx);
            }
            state
        });
        subscriptions.push(
            cx.subscribe(&when_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );

        // Create body input (multi-line code editor)
        let body_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .placeholder("Trigger body (SQL statements)")
                .multi_line(true)
                .code_editor("sql");
            state.set_value(&design.body, window, cx);
            state
        });
        subscriptions.push(
            cx.subscribe(&body_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );

        // Extract event states
        let insert_checked = design.events.contains(&TriggerEvent::Insert);
        let update_checked = design.events.contains(&TriggerEvent::Update);
        let delete_checked = design.events.contains(&TriggerEvent::Delete);

        Self {
            focus_handle: cx.focus_handle(),
            connection_id,
            design,
            original_name,
            active_tab: DesignerTab::General,
            name_input,
            table_select,
            tables: table_options,
            timing_select,
            timing_options,
            when_input,
            body_input,
            insert_checked,
            update_checked,
            delete_checked,
            for_each_row: true,
            ddl_preview: None,
            is_dirty: false,
            _subscriptions: subscriptions,
        }
    }

    /// Sync form values from the input entities
    fn sync_from_inputs(&mut self, cx: &Context<Self>) {
        self.design.name = self.name_input.read(cx).value().to_string();

        let when_val = self.when_input.read(cx).value().to_string();
        self.design.when_condition = if when_val.is_empty() {
            None
        } else {
            Some(when_val)
        };

        self.design.body = self.body_input.read(cx).value().to_string();

        // Sync table from select
        if let Some(value) = self.table_select.read(cx).selected_value() {
            self.design.table_name = value.clone();
        }

        // Sync timing from select
        if let Some(value) = self.timing_select.read(cx).selected_value() {
            self.design.timing = *value;
        }

        // Sync events from checkboxes
        self.design.events.clear();
        if self.insert_checked {
            self.design.events.push(TriggerEvent::Insert);
        }
        if self.update_checked {
            self.design.events.push(TriggerEvent::Update);
        }
        if self.delete_checked {
            self.design.events.push(TriggerEvent::Delete);
        }

        self.design.for_each_row = self.for_each_row;
    }

    fn mark_dirty(&mut self, cx: &mut Context<Self>) {
        self.is_dirty = true;
        self.ddl_preview = None;
        cx.notify();
    }

    fn handle_save(&mut self, cx: &mut Context<Self>) {
        self.sync_from_inputs(cx);

        let errors = self.design.validate();
        if !errors.is_empty() {
            tracing::warn!("Trigger validation failed: {:?}", errors);
            return;
        }

        cx.emit(TriggerDesignerEvent::Save {
            connection_id: self.connection_id,
            design: self.design.clone(),
            is_new: self.design.is_new,
            original_name: self.original_name.clone(),
        });
    }

    fn handle_cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(TriggerDesignerEvent::Cancel);
    }

    fn generate_preview(&mut self, cx: &mut Context<Self>) {
        self.sync_from_inputs(cx);
        self.ddl_preview = Some(self.design.to_ddl());
        cx.notify();
    }

    /// Render the tab bar
    fn render_tab_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let active_tab = self.active_tab;

        TabBar::new("trigger-designer-tabs")
            .small()
            .w_full()
            .selected_index(match active_tab {
                DesignerTab::General => 0,
                DesignerTab::Body => 1,
                DesignerTab::SqlPreview => 2,
            })
            .on_click(cx.listener(|this, ix: &usize, _window, cx| {
                this.active_tab = match ix {
                    0 => DesignerTab::General,
                    1 => DesignerTab::Body,
                    2 => {
                        this.generate_preview(cx);
                        DesignerTab::SqlPreview
                    }
                    _ => DesignerTab::General,
                };
                cx.notify();
            }))
            .child(Tab::new().label("General"))
            .child(Tab::new().label("Body"))
            .child(Tab::new().label("SQL Preview"))
    }

    /// Render the general settings tab
    fn render_general_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_postgres = self.design.dialect == DatabaseDialect::Postgres;

        v_flex()
            .id("general-tab-content")
            .size_full()
            .p_4()
            .gap_4()
            .overflow_y_scroll()
            // Trigger Name
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Trigger Name"),
                    )
                    .child(
                        Input::new(&self.name_input)
                            .small()
                            .w_full()
                            .max_w(px(400.0)),
                    ),
            )
            // Table
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Table"),
                    )
                    .child(
                        Select::new(&self.table_select)
                            .small()
                            .w(px(300.0))
                            .placeholder("Select table..."),
                    ),
            )
            // Timing
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Timing"),
                    )
                    .child(
                        Select::new(&self.timing_select)
                            .small()
                            .w(px(200.0))
                            .placeholder("Select timing..."),
                    ),
            )
            // Events
            .child(
                v_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Events"),
                    )
                    .child(
                        h_flex()
                            .gap_4()
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        Checkbox::new("event-insert")
                                            .checked(self.insert_checked)
                                            .on_click(cx.listener(|this, _, _window, cx| {
                                                this.insert_checked = !this.insert_checked;
                                                this.mark_dirty(cx);
                                            })),
                                    )
                                    .child(div().text_sm().child("INSERT")),
                            )
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        Checkbox::new("event-update")
                                            .checked(self.update_checked)
                                            .on_click(cx.listener(|this, _, _window, cx| {
                                                this.update_checked = !this.update_checked;
                                                this.mark_dirty(cx);
                                            })),
                                    )
                                    .child(div().text_sm().child("UPDATE")),
                            )
                            .child(
                                h_flex()
                                    .gap_2()
                                    .items_center()
                                    .child(
                                        Checkbox::new("event-delete")
                                            .checked(self.delete_checked)
                                            .on_click(cx.listener(|this, _, _window, cx| {
                                                this.delete_checked = !this.delete_checked;
                                                this.mark_dirty(cx);
                                            })),
                                    )
                                    .child(div().text_sm().child("DELETE")),
                            ),
                    ),
            )
            // FOR EACH ROW (Postgres only)
            .when(is_postgres, |this| {
                this.child(
                    v_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Granularity"),
                        )
                        .child(
                            h_flex()
                                .gap_4()
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .items_center()
                                        .child(
                                            Checkbox::new("for-each-row")
                                                .checked(self.for_each_row)
                                                .on_click(cx.listener(|this, _, _window, cx| {
                                                    this.for_each_row = true;
                                                    this.mark_dirty(cx);
                                                })),
                                        )
                                        .child(div().text_sm().child("FOR EACH ROW")),
                                )
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .items_center()
                                        .child(
                                            Checkbox::new("for-each-statement")
                                                .checked(!self.for_each_row)
                                                .on_click(cx.listener(|this, _, _window, cx| {
                                                    this.for_each_row = false;
                                                    this.mark_dirty(cx);
                                                })),
                                        )
                                        .child(div().text_sm().child("FOR EACH STATEMENT")),
                                ),
                        ),
                )
            })
            // WHEN Condition
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("WHEN Condition (optional)"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("e.g., OLD.status != NEW.status"),
                    )
                    .child(
                        Input::new(&self.when_input)
                            .small()
                            .w_full()
                            .max_w(px(500.0)),
                    ),
            )
    }

    /// Render the body editor tab
    fn render_body_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let dialect_hint = match self.design.dialect {
            DatabaseDialect::Sqlite => {
                "SQLite: Use BEGIN...END block. Access OLD and NEW for row data."
            }
            DatabaseDialect::Postgres => {
                "PostgreSQL: Write the function body. Use RETURN NEW/OLD at the end."
            }
            DatabaseDialect::Mysql => {
                "MySQL: Use BEGIN...END block. Access OLD and NEW for row data."
            }
        };

        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Trigger Body"),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(dialect_hint),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .min_h(px(300.0))
                    .child(Input::new(&self.body_input).w_full().h_full()),
            )
    }

    /// Render the SQL preview tab
    fn render_sql_preview_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let ddl = self
            .ddl_preview
            .clone()
            .unwrap_or_else(|| "-- Click 'Generate Preview' to see DDL".to_string());

        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Generated DDL"),
                    )
                    .child(
                        Button::new("generate-preview")
                            .label("Refresh Preview")
                            .xsmall()
                            .ghost()
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.generate_preview(cx);
                            })),
                    ),
            )
            .child(
                div()
                    .id("sql-preview")
                    .flex_1()
                    .w_full()
                    .overflow_scroll()
                    .p_3()
                    .rounded_md()
                    .bg(theme.secondary)
                    .border_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(code(&ddl)),
            )
            .child(
                h_flex().justify_end().pt_2().child(
                    Button::new("copy-ddl")
                        .label("Copy to Clipboard")
                        .small()
                        .on_click(cx.listener(|this, _, _window, cx| {
                            if let Some(ref ddl) = this.ddl_preview {
                                cx.write_to_clipboard(ClipboardItem::new_string(ddl.clone()));
                                tracing::info!("DDL copied to clipboard");
                            }
                        })),
                ),
            )
    }

    /// Render the footer with validation and buttons
    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        // Get validation errors
        let mut design_copy = self.design.clone();
        design_copy.name = self.name_input.read(cx).value().to_string();
        design_copy.body = self.body_input.read(cx).value().to_string();
        design_copy.events.clear();
        if self.insert_checked {
            design_copy.events.push(TriggerEvent::Insert);
        }
        if self.update_checked {
            design_copy.events.push(TriggerEvent::Update);
        }
        if self.delete_checked {
            design_copy.events.push(TriggerEvent::Delete);
        }

        let errors = design_copy.validate();
        let is_valid = errors.is_empty();

        h_flex()
            .w_full()
            .justify_between()
            .p_3()
            .border_t_1()
            .border_color(theme.border)
            .child(
                h_flex()
                    .gap_2()
                    .when(!is_valid, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.danger)
                                .child(errors.first().map(|e| e.to_string()).unwrap_or_default()),
                        )
                    })
                    .when(is_valid && self.is_dirty, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(theme.warning)
                                .child("(modified)"),
                        )
                    }),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("cancel")
                            .label("Cancel")
                            .small()
                            .ghost()
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.handle_cancel(cx);
                            })),
                    )
                    .child(
                        Button::new("save")
                            .label(if self.design.is_new {
                                "Create Trigger"
                            } else {
                                "Save Changes"
                            })
                            .small()
                            .primary()
                            .disabled(!is_valid || !self.is_dirty)
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.handle_save(cx);
                            })),
                    ),
            )
    }
}

impl Render for TriggerDesignerPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let bg_color = theme.background;
        let active_tab = self.active_tab;

        // Render tab content based on active tab
        let tab_content = match active_tab {
            DesignerTab::General => self.render_general_tab(cx).into_any_element(),
            DesignerTab::Body => self.render_body_tab(cx).into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview_tab(cx).into_any_element(),
        };

        let tab_bar = self.render_tab_bar(cx).into_any_element();
        let footer = self.render_footer(cx).into_any_element();

        v_flex()
            .id("trigger-designer-panel")
            .key_context("TriggerDesignerPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(bg_color)
            .child(tab_bar)
            .child(div().flex_1().overflow_hidden().child(tab_content))
            .child(footer)
    }
}

impl Focusable for TriggerDesignerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for TriggerDesignerPanel {}
impl EventEmitter<TriggerDesignerEvent> for TriggerDesignerPanel {}

impl Panel for TriggerDesignerPanel {
    fn panel_name(&self) -> &'static str {
        "TriggerDesignerPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.design.is_new {
            SharedString::from("New Trigger")
        } else {
            SharedString::from(format!("Trigger: {}", self.design.name))
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
