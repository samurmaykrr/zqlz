//! Trigger Designer Panel

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
    scroll::ScrollableElement,
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
    connection_id: Uuid,
    design: TriggerDesign,
    original_name: Option<String>,
    active_tab: DesignerTab,
    name_input: Entity<InputState>,
    table_select: Entity<SelectState<Vec<TableOption>>>,
    #[allow(dead_code)]
    tables: Vec<TableOption>,
    timing_select: Entity<SelectState<Vec<TimingOption>>>,
    #[allow(dead_code)]
    timing_options: Vec<TimingOption>,
    when_input: Entity<InputState>,
    body_input: Entity<InputState>,
    function_schema_input: Entity<InputState>,
    function_name_input: Entity<InputState>,
    function_arguments_input: Entity<InputState>,
    comment_input: Entity<InputState>,
    insert_checked: bool,
    update_checked: bool,
    delete_checked: bool,
    truncate_checked: bool,
    for_each_row: bool,
    enabled: bool,
    update_columns: Vec<String>,
    relation_columns: Vec<String>,
    postgres_functions: Vec<String>,
    ddl_preview: Option<String>,
    is_dirty: bool,
    _subscriptions: Vec<gpui::Subscription>,
}

impl TriggerDesignerPanel {
    /// Create a new trigger designer for a new trigger
    pub fn new(
        connection_id: Uuid,
        dialect: DatabaseDialect,
        tables: Vec<String>,
        relation_columns: Vec<String>,
        postgres_functions: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let design = TriggerDesign::new(dialect);
        Self::create(
            connection_id,
            design,
            None,
            tables,
            relation_columns,
            postgres_functions,
            window,
            cx,
        )
    }

    /// Create a trigger designer for editing an existing trigger
    pub fn edit(
        connection_id: Uuid,
        design: TriggerDesign,
        tables: Vec<String>,
        relation_columns: Vec<String>,
        postgres_functions: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let original_name = Some(design.name.clone());
        Self::create(
            connection_id,
            design,
            original_name,
            tables,
            relation_columns,
            postgres_functions,
            window,
            cx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create(
        connection_id: Uuid,
        design: TriggerDesign,
        original_name: Option<String>,
        tables: Vec<String>,
        relation_columns: Vec<String>,
        postgres_functions: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut subscriptions = Vec::new();

        let name_input = input_state(&design.name, "Trigger name", false, window, cx);
        subscriptions.push(Self::dirty_subscription(&name_input, cx));

        let table_options: Vec<TableOption> = tables
            .iter()
            .map(|name| TableOption { name: name.clone() })
            .collect();
        let selected_table_idx = table_options
            .iter()
            .position(|table| table.name == design.table_name)
            .map(|index| IndexPath::default().row(index));
        let table_select = cx.new(|cx| {
            SelectState::new(table_options.clone(), selected_table_idx, window, cx).searchable(true)
        });
        subscriptions.push(cx.subscribe(
            &table_select,
            |this, _, event: &SelectEvent<Vec<TableOption>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.design.table_name = value.clone();
                    this.mark_dirty(cx);
                }
            },
        ));

        let timing_options: Vec<TimingOption> = design
            .dialect
            .timings()
            .into_iter()
            .map(|timing| TimingOption {
                timing,
                label: timing.as_str().to_string(),
            })
            .collect();
        let selected_timing_idx = timing_options
            .iter()
            .position(|option| option.timing == design.timing)
            .map(|index| IndexPath::default().row(index));
        let timing_select =
            cx.new(|cx| SelectState::new(timing_options.clone(), selected_timing_idx, window, cx));
        subscriptions.push(cx.subscribe(
            &timing_select,
            |this, _, event: &SelectEvent<Vec<TimingOption>>, cx| {
                if let SelectEvent::Confirm(Some(timing)) = event {
                    this.design.timing = *timing;
                    this.mark_dirty(cx);
                }
            },
        ));

        let when_input = input_state(
            design.when_condition.as_deref().unwrap_or_default(),
            "Optional WHEN condition",
            false,
            window,
            cx,
        );
        subscriptions.push(Self::dirty_subscription(&when_input, cx));

        let body_input = input_state(
            &design.body,
            "Trigger body (SQL statements)",
            true,
            window,
            cx,
        );
        subscriptions.push(Self::dirty_subscription(&body_input, cx));

        let function_schema_input = input_state(
            design.function_schema.as_deref().unwrap_or_default(),
            "Function schema",
            false,
            window,
            cx,
        );
        subscriptions.push(Self::dirty_subscription(&function_schema_input, cx));

        let function_name_input =
            input_state(&design.function_name, "Function name", false, window, cx);
        subscriptions.push(Self::dirty_subscription(&function_name_input, cx));

        let function_arguments_input =
            input_state(&design.function_arguments, "Arguments", false, window, cx);
        subscriptions.push(Self::dirty_subscription(&function_arguments_input, cx));

        let comment_input = input_state(
            design.comment.as_deref().unwrap_or_default(),
            "Comment",
            true,
            window,
            cx,
        );
        subscriptions.push(Self::dirty_subscription(&comment_input, cx));

        let insert_checked = design.events.contains(&TriggerEvent::Insert);
        let update_checked = design.events.contains(&TriggerEvent::Update);
        let delete_checked = design.events.contains(&TriggerEvent::Delete);
        let truncate_checked = design.events.contains(&TriggerEvent::Truncate);
        let update_columns = design.update_columns.clone();
        let for_each_row = design.for_each_row;
        let enabled = design.enabled;

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
            function_schema_input,
            function_name_input,
            function_arguments_input,
            comment_input,
            insert_checked,
            update_checked,
            delete_checked,
            truncate_checked,
            for_each_row,
            enabled,
            update_columns,
            relation_columns,
            postgres_functions,
            ddl_preview: None,
            is_dirty: false,
            _subscriptions: subscriptions,
        }
    }

    fn dirty_subscription(
        input: &Entity<InputState>,
        cx: &mut Context<Self>,
    ) -> gpui::Subscription {
        cx.subscribe(input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                this.mark_dirty(cx);
            }
        })
    }

    fn sync_from_inputs(&mut self, cx: &Context<Self>) {
        self.design.name = self.name_input.read(cx).value().to_string();
        self.design.body = self.body_input.read(cx).value().to_string();
        self.design.enabled = self.enabled;
        self.design.for_each_row = self.for_each_row;
        self.design.update_columns = self.update_columns.clone();

        let when_value = self.when_input.read(cx).value().to_string();
        self.design.when_condition = non_empty(when_value);

        let function_schema = self.function_schema_input.read(cx).value().to_string();
        self.design.function_schema = non_empty(function_schema);
        self.design.function_name = self.function_name_input.read(cx).value().to_string();
        self.design.function_arguments = self.function_arguments_input.read(cx).value().to_string();

        let comment = self.comment_input.read(cx).value().to_string();
        self.design.comment = non_empty(comment);

        if let Some(value) = self.table_select.read(cx).selected_value() {
            self.design.table_name = value.clone();
        }

        if let Some(value) = self.timing_select.read(cx).selected_value() {
            self.design.timing = *value;
        }

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
        if self.truncate_checked {
            self.design.events.push(TriggerEvent::Truncate);
        }
    }

    fn design_from_inputs(&self, cx: &Context<Self>) -> TriggerDesign {
        let mut design = self.design.clone();
        design.name = self.name_input.read(cx).value().to_string();
        design.body = self.body_input.read(cx).value().to_string();
        design.enabled = self.enabled;
        design.for_each_row = self.for_each_row;
        design.update_columns = self.update_columns.clone();
        design.when_condition = non_empty(self.when_input.read(cx).value().to_string());
        design.function_schema = non_empty(self.function_schema_input.read(cx).value().to_string());
        design.function_name = self.function_name_input.read(cx).value().to_string();
        design.function_arguments = self.function_arguments_input.read(cx).value().to_string();
        design.comment = non_empty(self.comment_input.read(cx).value().to_string());

        if let Some(value) = self.table_select.read(cx).selected_value() {
            design.table_name = value.clone();
        }

        if let Some(value) = self.timing_select.read(cx).selected_value() {
            design.timing = *value;
        }

        design.events.clear();
        if self.insert_checked {
            design.events.push(TriggerEvent::Insert);
        }
        if self.update_checked {
            design.events.push(TriggerEvent::Update);
        }
        if self.delete_checked {
            design.events.push(TriggerEvent::Delete);
        }
        if self.truncate_checked {
            design.events.push(TriggerEvent::Truncate);
        }
        design
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

    fn set_single_event(&mut self, event: TriggerEvent, cx: &mut Context<Self>) {
        self.insert_checked = event == TriggerEvent::Insert;
        self.update_checked = event == TriggerEvent::Update;
        self.delete_checked = event == TriggerEvent::Delete;
        self.truncate_checked = event == TriggerEvent::Truncate;
        self.mark_dirty(cx);
    }

    fn toggle_column(&mut self, column: &str, cx: &mut Context<Self>) {
        if let Some(index) = self
            .update_columns
            .iter()
            .position(|selected| selected == column)
        {
            self.update_columns.remove(index);
        } else {
            self.update_columns.push(column.to_string());
        }
        self.mark_dirty(cx);
    }

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
            .on_click(cx.listener(|this, index: &usize, _window, cx| {
                this.active_tab = match index {
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
            .child(
                Tab::new().label(if self.design.dialect.uses_trigger_function() {
                    "Function Body"
                } else {
                    "Body"
                }),
            )
            .child(Tab::new().label("SQL Preview"))
    }

    fn render_general_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let muted_foreground = cx.theme().muted_foreground;
        let dialect = self.design.dialect;
        let supports_update_columns = dialect.supports_update_columns() && self.update_checked;
        let supports_when = dialect.supports_when_condition();
        let uses_function = dialect.uses_trigger_function();
        let enable_row = self.render_enable_row(cx).into_any_element();
        let events = self.render_events(cx).into_any_element();
        let granularity = dialect
            .supports_statement_level()
            .then(|| self.render_granularity(cx).into_any_element());
        let update_columns =
            supports_update_columns.then(|| self.render_update_columns(cx).into_any_element());
        let function_section =
            uses_function.then(|| self.render_function_section(cx).into_any_element());

        v_flex()
            .id("general-tab-content")
            .size_full()
            .p_4()
            .gap_4()
            .overflow_y_scroll()
            .child(enable_row)
            .child(self.render_labeled_input("Trigger Name", &self.name_input, px(400.0)))
            .child(
                v_flex().gap_1().child(Self::label("Target")).child(
                    Select::new(&self.table_select)
                        .small()
                        .w(px(360.0))
                        .placeholder("Select table or view..."),
                ),
            )
            .child(
                v_flex().gap_1().child(Self::label("Timing")).child(
                    Select::new(&self.timing_select)
                        .small()
                        .w(px(220.0))
                        .placeholder("Select timing..."),
                ),
            )
            .child(events)
            .when_some(granularity, |this, granularity| this.child(granularity))
            .when_some(update_columns, |this, update_columns| {
                this.child(update_columns)
            })
            .when(supports_when, |this| {
                this.child(
                    v_flex()
                        .gap_1()
                        .child(Self::label("WHEN Clause"))
                        .child(
                            div()
                                .text_xs()
                                .text_color(muted_foreground)
                                .child("Example: OLD.status IS DISTINCT FROM NEW.status"),
                        )
                        .child(
                            Input::new(&self.when_input)
                                .small()
                                .w_full()
                                .max_w(px(560.0)),
                        ),
                )
            })
            .when_some(function_section, |this, function_section| {
                this.child(function_section)
            })
            .child(
                v_flex().gap_1().child(Self::label("Comment")).child(
                    div()
                        .w_full()
                        .max_w(px(560.0))
                        .h(px(72.0))
                        .child(Input::new(&self.comment_input).w_full().h_full()),
                ),
            )
    }

    fn render_enable_row(&self, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .gap_2()
            .items_center()
            .child(
                Checkbox::new("trigger-enabled")
                    .checked(self.enabled)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.enabled = !this.enabled;
                        this.mark_dirty(cx);
                    })),
            )
            .child(div().text_sm().child("Enable"))
            .child(
                div()
                    .ml_2()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(self.design.trigger_type.clone()),
            )
    }

    fn render_events(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let multi = self.design.dialect.supports_multi_event();

        v_flex().gap_2().child(Self::label("Events")).child(
            h_flex()
                .gap_4()
                .child(self.render_event_checkbox(
                    "event-insert",
                    "INSERT",
                    TriggerEvent::Insert,
                    multi,
                    cx,
                ))
                .child(self.render_event_checkbox(
                    "event-update",
                    "UPDATE",
                    TriggerEvent::Update,
                    multi,
                    cx,
                ))
                .child(self.render_event_checkbox(
                    "event-delete",
                    "DELETE",
                    TriggerEvent::Delete,
                    multi,
                    cx,
                ))
                .when(
                    self.design
                        .dialect
                        .events()
                        .contains(&TriggerEvent::Truncate),
                    |this| {
                        this.child(self.render_event_checkbox(
                            "event-truncate",
                            "TRUNCATE",
                            TriggerEvent::Truncate,
                            multi,
                            cx,
                        ))
                    },
                ),
        )
    }

    fn render_event_checkbox(
        &self,
        id: &'static str,
        label: &'static str,
        event: TriggerEvent,
        multi: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let checked = match event {
            TriggerEvent::Insert => self.insert_checked,
            TriggerEvent::Update => self.update_checked,
            TriggerEvent::Delete => self.delete_checked,
            TriggerEvent::Truncate => self.truncate_checked,
        };

        h_flex()
            .gap_2()
            .items_center()
            .child(Checkbox::new(id).checked(checked).on_click(cx.listener(
                move |this, _, _window, cx| {
                    if multi {
                        match event {
                            TriggerEvent::Insert => this.insert_checked = !this.insert_checked,
                            TriggerEvent::Update => this.update_checked = !this.update_checked,
                            TriggerEvent::Delete => this.delete_checked = !this.delete_checked,
                            TriggerEvent::Truncate => {
                                this.truncate_checked = !this.truncate_checked
                            }
                        }
                        this.mark_dirty(cx);
                    } else {
                        this.set_single_event(event, cx);
                    }
                },
            )))
            .child(div().text_sm().child(label))
    }

    fn render_granularity(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex().gap_2().child(Self::label("For Each")).child(
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
                        .child(div().text_sm().child("ROW")),
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
                        .child(div().text_sm().child("STATEMENT")),
                ),
        )
    }

    fn render_update_columns(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let mut list = v_flex()
            .gap_2()
            .child(Self::label("UPDATE OF Fields"))
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Only fire UPDATE trigger when selected columns change."),
            );

        let columns = self.relation_columns.clone();
        if columns.is_empty() {
            list = list.child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Column list unavailable; save still allows manual SQL body."),
            );
        } else {
            let mut column_list = v_flex()
                .gap_1()
                .max_w(px(560.0))
                .max_h(px(180.0))
                .overflow_y_scrollbar()
                .border_1()
                .border_color(theme.border)
                .rounded_sm()
                .p_2();
            for column in columns {
                let checked = self.update_columns.contains(&column);
                let column_for_click = column.clone();
                column_list = column_list.child(
                    h_flex()
                        .gap_2()
                        .items_center()
                        .child(
                            Checkbox::new(SharedString::from(format!("update-column-{column}")))
                                .checked(checked)
                                .on_click(cx.listener(move |this, _, _window, cx| {
                                    this.toggle_column(&column_for_click, cx);
                                })),
                        )
                        .child(div().text_sm().child(column)),
                );
            }
            list = list.child(column_list);
        }
        list
    }

    fn render_function_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let mut section = v_flex().gap_3().child(Self::label("Function")).child(
            h_flex()
                .gap_2()
                .child(
                    v_flex()
                        .gap_1()
                        .child(div().text_xs().child("Schema"))
                        .child(Input::new(&self.function_schema_input).small().w(px(180.0))),
                )
                .child(
                    v_flex()
                        .gap_1()
                        .child(div().text_xs().child("Name"))
                        .child(Input::new(&self.function_name_input).small().w(px(260.0))),
                )
                .child(
                    v_flex()
                        .gap_1()
                        .child(div().text_xs().child("Arguments"))
                        .child(
                            Input::new(&self.function_arguments_input)
                                .small()
                                .w(px(220.0)),
                        ),
                ),
        );

        if !self.postgres_functions.is_empty() {
            let mut suggestions = h_flex().gap_2().flex_wrap();
            for function in self.postgres_functions.iter().take(8) {
                let function = function.clone();
                let label = function.clone();
                suggestions = suggestions.child(
                    Button::new(SharedString::from(format!("function-{function}")))
                        .label(label)
                        .xsmall()
                        .secondary()
                        .on_click(cx.listener(move |this, _event, window, cx| {
                            let (schema, name) = split_qualified_name(&function);
                            this.function_schema_input.update(cx, |input, cx| {
                                input.set_value(schema.as_deref().unwrap_or_default(), window, cx);
                            });
                            this.function_name_input.update(cx, |input, cx| {
                                input.set_value(&name, window, cx);
                            });
                            this.mark_dirty(cx);
                        })),
                );
            }
            section = section
                .child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Detected trigger-capable functions"),
                )
                .child(suggestions);
        }

        section
    }

    fn render_labeled_input(
        &self,
        label: &'static str,
        input: &Entity<InputState>,
        max_width: Pixels,
    ) -> impl IntoElement {
        v_flex()
            .gap_1()
            .child(Self::label(label))
            .child(Input::new(input).small().w_full().max_w(max_width))
    }

    fn label(label: &'static str) -> impl IntoElement {
        div().text_sm().font_weight(FontWeight::MEDIUM).child(label)
    }

    fn render_body_tab(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let dialect_hint = match self.design.dialect {
            DatabaseDialect::Sqlite => {
                "SQLite inline trigger body. OLD and NEW available for row data."
            }
            DatabaseDialect::Postgres => {
                "PostgreSQL trigger function body. Return NEW, OLD, or NULL as needed."
            }
            DatabaseDialect::Mysql => {
                "MySQL inline trigger body. OLD and NEW available for row data."
            }
        };

        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(Self::label(
                if self.design.dialect.uses_trigger_function() {
                    "Function Body"
                } else {
                    "Trigger Body"
                },
            ))
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

    fn render_sql_preview_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let ddl = self
            .ddl_preview
            .clone()
            .unwrap_or_else(|| "-- Click 'Refresh Preview' to see DDL".to_string());

        v_flex()
            .size_full()
            .p_4()
            .gap_2()
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .child(Self::label("Generated DDL"))
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
                        .secondary()
                        .label("Copy to Clipboard")
                        .small()
                        .on_click(cx.listener(|this, _, _window, cx| {
                            if let Some(ref ddl) = this.ddl_preview {
                                cx.write_to_clipboard(ClipboardItem::new_string(ddl.clone()));
                            }
                        })),
                ),
            )
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let design = self.design_from_inputs(cx);
        let errors = design.validate();
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
                            div().text_xs().text_color(theme.danger).child(
                                errors
                                    .first()
                                    .map(|error| error.to_string())
                                    .unwrap_or_default(),
                            ),
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
        let background = cx.theme().background;
        let active_tab = self.active_tab;

        let tab_content = match active_tab {
            DesignerTab::General => self.render_general_tab(cx).into_any_element(),
            DesignerTab::Body => self.render_body_tab(cx).into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview_tab(cx).into_any_element(),
        };

        v_flex()
            .id("trigger-designer-panel")
            .key_context("TriggerDesignerPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(background)
            .child(self.render_tab_bar(cx))
            .child(div().flex_1().overflow_hidden().child(tab_content))
            .child(self.render_footer(cx))
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

fn input_state(
    value: &str,
    placeholder: &'static str,
    multi_line: bool,
    window: &mut Window,
    cx: &mut App,
) -> Entity<InputState> {
    cx.new(|cx| {
        let mut state = InputState::new(window, cx).placeholder(placeholder);
        if multi_line {
            state = state.multi_line(true).code_editor("sql");
        }
        state.set_value(value, window, cx);
        state
    })
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}

fn split_qualified_name(name: &str) -> (Option<String>, String) {
    if let Some((schema, name)) = name.split_once('.') {
        (Some(schema.to_string()), name.to_string())
    } else {
        (None, name.to_string())
    }
}
