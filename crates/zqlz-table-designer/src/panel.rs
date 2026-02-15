//! Table Designer Panel
//!
//! A panel for designing and modifying database table structures.
//! Features tabbed interface with:
//! - Fields: Column definitions (name, type, nullable, default, etc.)
//! - Indexes: Index management
//! - Foreign Keys: Foreign key constraints
//! - Options: Driver-specific table options
//! - SQL Preview: Generated DDL preview
//!
//! ## Usage Patterns
//!
//! ### Creating a New Table
//! 1. Right-click in schema tree → "New Table"
//! 2. MainView creates TableDesignerPanel with empty TableDesign
//! 3. User adds columns, indexes, foreign keys
//! 4. User clicks "Save" → MainView executes DDL via connection
//!
//! ### Editing an Existing Table
//! 1. Right-click on table → "Design Table"
//! 2. MainView loads existing structure via table loader
//! 3. MainView creates TableDesignerPanel with loaded TableDesign
//! 4. User modifies structure
//! 5. User clicks "Save" → MainView generates ALTER statements

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    select::{Select, SelectEvent, SelectState},
    v_flex, ActiveTheme, Disableable, Sizable,
};

#[path = "ui/mod.rs"]
mod ui;

use crate::events::TableDesignerEvent;
use crate::models::{
    get_data_types, ColumnDesign, DataTypeInfo, DatabaseDialect, ForeignKeyDesign, IndexDesign,
    TableDesign,
};
use crate::service::DdlGenerator;

/// Active tab in the table designer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DesignerTab {
    #[default]
    Fields,
    Indexes,
    ForeignKeys,
    Options,
    SqlPreview,
}

/// Table Designer Panel for creating and modifying table structures
pub struct TableDesignerPanel {
    focus_handle: FocusHandle,

    /// Connection ID this design is for
    connection_id: Uuid,

    /// The table design being edited
    design: TableDesign,

    /// Original design (for comparison when editing existing tables)
    original_design: Option<TableDesign>,

    /// Current active tab
    active_tab: DesignerTab,

    /// Selected column index in the fields tab
    selected_column_index: Option<usize>,

    /// Selected index in the indexes tab
    selected_index_index: Option<usize>,

    /// Selected foreign key in the foreign keys tab
    selected_fk_index: Option<usize>,

    /// Input state for table name
    table_name_input: Entity<InputState>,

    /// Input states for column names (indexed by column ordinal)
    column_name_inputs: Vec<Entity<InputState>>,

    /// Input states for column defaults (indexed by column ordinal)
    column_default_inputs: Vec<Entity<InputState>>,

    /// Input states for column lengths (indexed by column ordinal)
    column_length_inputs: Vec<Entity<InputState>>,

    /// Select states for column data types (indexed by column ordinal)
    column_type_selects: Vec<Entity<SelectState<Vec<DataTypeInfo>>>>,

    /// Input states for column comments (indexed by column ordinal)
    column_comment_inputs: Vec<Entity<InputState>>,

    /// Available data types for the current dialect
    data_types: Vec<DataTypeInfo>,

    /// Generated DDL preview (cached)
    ddl_preview: Option<String>,

    /// Whether the design has been modified
    is_dirty: bool,

    /// Subscriptions to input events
    _subscriptions: Vec<gpui::Subscription>,
}

impl TableDesignerPanel {
    /// Create a new table designer for a new table
    pub fn new(
        connection_id: Uuid,
        dialect: DatabaseDialect,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let design = TableDesign::empty(dialect);
        let data_types = get_data_types(&dialect);

        let table_name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Table name"));

        // Subscribe to table name input changes
        let mut subscriptions = Vec::new();
        subscriptions.push(
            cx.subscribe(&table_name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );

        Self {
            focus_handle: cx.focus_handle(),
            connection_id,
            design,
            original_design: None,
            active_tab: DesignerTab::Fields,
            selected_column_index: None,
            selected_index_index: None,
            selected_fk_index: None,
            table_name_input,
            column_name_inputs: Vec::new(),
            column_default_inputs: Vec::new(),
            column_length_inputs: Vec::new(),
            column_type_selects: Vec::new(),
            column_comment_inputs: Vec::new(),
            data_types,
            ddl_preview: None,
            is_dirty: false,
            _subscriptions: subscriptions,
        }
    }

    /// Create a table designer for editing an existing table
    pub fn edit(
        connection_id: Uuid,
        design: TableDesign,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let data_types = get_data_types(&design.dialect);

        let table_name_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Table name");
            state.set_value(&design.table_name, window, cx);
            state
        });

        // Create input states for existing columns
        let mut column_name_inputs = Vec::with_capacity(design.columns.len());
        let mut column_default_inputs = Vec::with_capacity(design.columns.len());
        let mut column_length_inputs = Vec::with_capacity(design.columns.len());
        let mut column_type_selects = Vec::with_capacity(design.columns.len());
        let mut column_comment_inputs = Vec::with_capacity(design.columns.len());

        for col in &design.columns {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Column name");
                state.set_value(&col.name, window, cx);
                state
            });
            column_name_inputs.push(name_input);

            let default_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Default");
                if let Some(ref default) = col.default_value {
                    state.set_value(default, window, cx);
                }
                state
            });
            column_default_inputs.push(default_input);

            let length_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Length");
                if let Some(length) = col.length {
                    state.set_value(&length.to_string(), window, cx);
                }
                state
            });
            column_length_inputs.push(length_input);

            let comment_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Comment");
                if let Some(ref comment) = col.comment {
                    state.set_value(comment, window, cx);
                }
                state
            });
            column_comment_inputs.push(comment_input);

            let data_types_clone = data_types.clone();
            let selected_index = data_types_clone
                .iter()
                .position(|dt| dt.name.eq_ignore_ascii_case(&col.data_type))
                .map(|i| zqlz_ui::widgets::IndexPath::default().row(i));
            let type_select = cx.new(|cx| {
                SelectState::new(data_types_clone, selected_index, window, cx).searchable(true)
            });
            column_type_selects.push(type_select);
        }

        let original_design = Some(design.clone());

        // Subscribe to table name input changes
        let mut subscriptions = Vec::new();
        subscriptions.push(
            cx.subscribe(&table_name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );

        // Subscribe to all column inputs
        for input in &column_name_inputs {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
        }
        for input in &column_default_inputs {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
        }
        for input in &column_length_inputs {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
        }
        for input in &column_comment_inputs {
            subscriptions.push(cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
        }
        for (idx, type_select) in column_type_selects.iter().enumerate() {
            subscriptions.push(cx.subscribe(
                type_select,
                move |this, _, event: &SelectEvent<Vec<DataTypeInfo>>, cx| {
                    if let SelectEvent::Confirm(Some(value)) = event {
                        if let Some(col) = this.design.columns.get_mut(idx) {
                            col.data_type = value.clone();
                            this.mark_dirty(cx);
                        }
                    }
                },
            ));
        }

        Self {
            focus_handle: cx.focus_handle(),
            connection_id,
            design,
            original_design,
            active_tab: DesignerTab::Fields,
            selected_column_index: None,
            selected_index_index: None,
            selected_fk_index: None,
            table_name_input,
            column_name_inputs,
            column_default_inputs,
            column_length_inputs,
            column_type_selects,
            column_comment_inputs,
            data_types,
            ddl_preview: None,
            is_dirty: false,
            _subscriptions: subscriptions,
        }
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.design.table_name
    }

    /// Check if this is a new table
    pub fn is_new(&self) -> bool {
        self.design.is_new
    }

    /// Mark the design as dirty (modified)
    fn mark_dirty(&mut self, cx: &mut Context<Self>) {
        self.is_dirty = true;
        self.ddl_preview = None;
        cx.notify();
    }

    /// Sync the table name from the input to the design
    fn sync_table_name(&mut self, cx: &mut Context<Self>) {
        let name = self.table_name_input.read(cx).value().to_string();
        if self.design.table_name != name {
            self.design.table_name = name;
            self.mark_dirty(cx);
        }
    }

    /// Add a new column
    fn add_column(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let ordinal = self.design.columns.len();
        self.design.columns.push(ColumnDesign::new(ordinal));

        // Create input states for the new column and subscribe to their changes
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Column name"));
        self._subscriptions.push(
            cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.column_name_inputs.push(name_input);

        let default_input = cx.new(|cx| InputState::new(window, cx).placeholder("Default"));
        self._subscriptions.push(cx.subscribe(
            &default_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            },
        ));
        self.column_default_inputs.push(default_input);

        let length_input = cx.new(|cx| InputState::new(window, cx).placeholder("Length"));
        self._subscriptions.push(
            cx.subscribe(&length_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.column_length_inputs.push(length_input);

        let data_types_clone = self.data_types.clone();
        let type_select =
            cx.new(|cx| SelectState::new(data_types_clone, None, window, cx).searchable(true));
        let col_idx = ordinal;
        self._subscriptions.push(cx.subscribe(
            &type_select,
            move |this, _, event: &SelectEvent<Vec<DataTypeInfo>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    if let Some(col) = this.design.columns.get_mut(col_idx) {
                        col.data_type = value.clone();
                        this.mark_dirty(cx);
                    }
                }
            },
        ));
        self.column_type_selects.push(type_select);

        let comment_input = cx.new(|cx| InputState::new(window, cx).placeholder("Comment"));
        self._subscriptions.push(cx.subscribe(
            &comment_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            },
        ));
        self.column_comment_inputs.push(comment_input);

        self.selected_column_index = Some(ordinal);
        self.mark_dirty(cx);
    }

    /// Remove selected column
    fn remove_column(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_column_index {
            if idx < self.design.columns.len() {
                self.design.columns.remove(idx);
                self.column_name_inputs.remove(idx);
                self.column_default_inputs.remove(idx);
                self.column_length_inputs.remove(idx);
                self.column_type_selects.remove(idx);
                self.column_comment_inputs.remove(idx);

                // Update ordinals
                for (i, col) in self.design.columns.iter_mut().enumerate() {
                    col.ordinal = i;
                }

                // Adjust selection
                if self.design.columns.is_empty() {
                    self.selected_column_index = None;
                } else if idx >= self.design.columns.len() {
                    self.selected_column_index = Some(self.design.columns.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Move selected column up
    fn move_column_up(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_column_index {
            if idx > 0 {
                self.design.columns.swap(idx, idx - 1);
                self.column_name_inputs.swap(idx, idx - 1);
                self.column_default_inputs.swap(idx, idx - 1);
                self.column_length_inputs.swap(idx, idx - 1);
                self.column_type_selects.swap(idx, idx - 1);
                self.column_comment_inputs.swap(idx, idx - 1);

                // Update ordinals
                self.design.columns[idx].ordinal = idx;
                self.design.columns[idx - 1].ordinal = idx - 1;

                self.selected_column_index = Some(idx - 1);
                self.mark_dirty(cx);
            }
        }
    }

    /// Move selected column down
    fn move_column_down(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_column_index {
            if idx < self.design.columns.len() - 1 {
                self.design.columns.swap(idx, idx + 1);
                self.column_name_inputs.swap(idx, idx + 1);
                self.column_default_inputs.swap(idx, idx + 1);
                self.column_length_inputs.swap(idx, idx + 1);
                self.column_type_selects.swap(idx, idx + 1);
                self.column_comment_inputs.swap(idx, idx + 1);

                // Update ordinals
                self.design.columns[idx].ordinal = idx;
                self.design.columns[idx + 1].ordinal = idx + 1;

                self.selected_column_index = Some(idx + 1);
                self.mark_dirty(cx);
            }
        }
    }

    /// Add a new index
    fn add_index(&mut self, cx: &mut Context<Self>) {
        self.design.indexes.push(IndexDesign::new());
        self.selected_index_index = Some(self.design.indexes.len() - 1);
        self.mark_dirty(cx);
    }

    /// Remove selected index
    fn remove_index(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_index_index {
            if idx < self.design.indexes.len() {
                self.design.indexes.remove(idx);

                if self.design.indexes.is_empty() {
                    self.selected_index_index = None;
                } else if idx >= self.design.indexes.len() {
                    self.selected_index_index = Some(self.design.indexes.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Add a new foreign key
    fn add_foreign_key(&mut self, cx: &mut Context<Self>) {
        self.design.foreign_keys.push(ForeignKeyDesign::new());
        self.selected_fk_index = Some(self.design.foreign_keys.len() - 1);
        self.mark_dirty(cx);
    }

    /// Remove selected foreign key
    fn remove_foreign_key(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_fk_index {
            if idx < self.design.foreign_keys.len() {
                self.design.foreign_keys.remove(idx);

                if self.design.foreign_keys.is_empty() {
                    self.selected_fk_index = None;
                } else if idx >= self.design.foreign_keys.len() {
                    self.selected_fk_index = Some(self.design.foreign_keys.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Sync column data from inputs to design
    fn sync_columns_from_inputs(&mut self, cx: &Context<Self>) {
        for (i, col) in self.design.columns.iter_mut().enumerate() {
            if let Some(input) = self.column_name_inputs.get(i) {
                col.name = input.read(cx).value().to_string();
            }
            if let Some(input) = self.column_default_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.default_value = if val.is_empty() { None } else { Some(val) };
            }
            if let Some(input) = self.column_length_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.length = val.parse().ok();
            }
            if let Some(input) = self.column_comment_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.comment = if val.is_empty() { None } else { Some(val) };
            }
        }
    }

    /// Generate DDL preview
    fn generate_ddl_preview(&mut self, cx: &mut Context<Self>) {
        // Sync all data from inputs
        self.sync_table_name(cx);
        self.sync_columns_from_inputs(cx);

        let preview = if self.design.is_new {
            match DdlGenerator::generate_create_table(&self.design) {
                Ok(ddl) => ddl,
                Err(e) => format!("-- Error generating DDL: {}", e),
            }
        } else if let Some(ref original) = self.original_design {
            match DdlGenerator::generate_alter_table(original, &self.design) {
                Ok(statements) if statements.is_empty() => "-- No changes detected".to_string(),
                Ok(statements) => statements.join("\n"),
                Err(e) => format!("-- Error generating DDL: {}", e),
            }
        } else {
            // Fallback: if original_design is missing for some reason,
            // show CREATE TABLE so the user at least sees something
            match DdlGenerator::generate_create_table(&self.design) {
                Ok(ddl) => format!(
                    "-- Warning: original design not available, showing CREATE TABLE instead\n\n{}",
                    ddl
                ),
                Err(e) => format!("-- Error generating DDL: {}", e),
            }
        };

        self.ddl_preview = Some(preview);
        cx.notify();
    }

    /// Handle save button click
    fn handle_save(&mut self, cx: &mut Context<Self>) {
        // Sync all data from inputs
        self.sync_table_name(cx);
        self.sync_columns_from_inputs(cx);

        // Validate
        let errors = self.design.validate();
        if !errors.is_empty() {
            tracing::warn!("Validation errors: {:?}", errors);
            return;
        }

        cx.emit(TableDesignerEvent::Save {
            connection_id: self.connection_id,
            design: self.design.clone(),
            is_new: self.design.is_new,
            original_design: self.original_design.clone(),
        });
    }

    /// Handle cancel button click
    fn handle_cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(TableDesignerEvent::Cancel);
    }

    /// Render the tab bar (delegates to ui/tab_bar.rs)
    fn render_tab_bar(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::tab_bar::render_tab_bar(self, cx)
    }

    /// Render the toolbar (delegates to ui/toolbar.rs)
    fn render_toolbar(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::toolbar::render_toolbar(self, cx)
    }

    /// Render the fields tab content (delegates to ui/fields_tab.rs)
    fn render_fields_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::fields_tab::render_fields_tab(self, cx)
    }

    /// Render column header row
    fn render_column_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .bg(theme.table_head)
            .border_1()
            .border_color(theme.border)
            .text_xs()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .w(px(180.0))
                    .px_3()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Name"),
            )
            .child(
                div()
                    .w(px(140.0))
                    .px_3()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Type"),
            )
            .child(
                div()
                    .w(px(100.0))
                    .px_3()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Length"),
            )
            .child(
                div()
                    .w(px(50.0))
                    .px_2()
                    .py_2()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("NN"),
            )
            .child(
                div()
                    .w(px(50.0))
                    .px_2()
                    .py_2()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("PK"),
            )
            .child(
                div()
                    .w(px(50.0))
                    .px_2()
                    .py_2()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("UQ"),
            )
            .child(
                div()
                    .flex_1()
                    .px_3()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Default"),
            )
            .child(div().w(px(180.0)).px_3().py_2().child("Comment"))
    }

    /// Build a single column row element
    fn build_column_row_element(
        &self,
        idx: usize,
        is_selected: bool,
        nullable: bool,
        is_primary_key: bool,
        is_unique: bool,
        name_input: Option<Entity<InputState>>,
        length_input: Option<Entity<InputState>>,
        default_input: Option<Entity<InputState>>,
        type_select: Option<Entity<SelectState<Vec<DataTypeInfo>>>>,
        comment_input: Option<Entity<InputState>>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .id(SharedString::from(format!("column-{}", idx)))
            .w_full()
            .bg(if is_selected {
                theme.selection
            } else if idx % 2 == 0 {
                theme.table_even
            } else {
                theme.table
            })
            .border_l_1()
            .border_r_1()
            .border_b_1()
            .border_color(theme.border)
            .on_click(cx.listener(move |this, _, _window, cx| {
                this.selected_column_index = Some(idx);
                cx.notify();
            }))
            // Name column
            .child(
                div()
                    .w(px(180.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        name_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| {
                                Input::new(&self.table_name_input).xsmall().w_full()
                            }),
                    ),
            )
            // Type column - dropdown selector
            .child(
                div()
                    .w(px(140.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .when_some(type_select, |el, ts| {
                        el.child(Select::new(&ts).xsmall().placeholder("Type..."))
                    }),
            )
            // Length column
            .child(
                div()
                    .w(px(100.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        length_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| {
                                Input::new(&self.table_name_input).xsmall().w_full()
                            }),
                    ),
            )
            // Not Null checkbox
            .child(
                div()
                    .w(px(50.0))
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        Checkbox::new(SharedString::from(format!("notnull-{}", idx)))
                            .checked(!nullable)
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                if let Some(col) = this.design.columns.get_mut(idx) {
                                    col.nullable = !col.nullable;
                                    this.mark_dirty(cx);
                                }
                            })),
                    ),
            )
            // Primary Key checkbox
            .child(
                div()
                    .w(px(50.0))
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        Checkbox::new(SharedString::from(format!("pk-{}", idx)))
                            .checked(is_primary_key)
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                if let Some(col) = this.design.columns.get_mut(idx) {
                                    col.is_primary_key = !col.is_primary_key;
                                    this.mark_dirty(cx);
                                }
                            })),
                    ),
            )
            // Unique checkbox
            .child(
                div()
                    .w(px(50.0))
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        Checkbox::new(SharedString::from(format!("unique-{}", idx)))
                            .checked(is_unique)
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                if let Some(col) = this.design.columns.get_mut(idx) {
                                    col.is_unique = !col.is_unique;
                                    this.mark_dirty(cx);
                                }
                            })),
                    ),
            )
            // Default column
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        default_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| {
                                Input::new(&self.table_name_input).xsmall().w_full()
                            }),
                    ),
            )
            // Comment column
            .child(
                div().w(px(180.0)).px_2().py_1().child(
                    comment_input
                        .map(|input| Input::new(&input).xsmall().w_full())
                        .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                ),
            )
    }

    /// Render the indexes tab content (delegates to ui/indexes_tab.rs)
    fn render_indexes_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::indexes_tab::render_indexes_tab(self, cx)
    }

    /// Render index header row
    fn render_index_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .bg(theme.table_head)
            .border_1()
            .border_color(theme.border)
            .text_xs()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Name"),
            )
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Columns"),
            )
            .child(
                div()
                    .w(px(80.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Type"),
            )
            .child(
                div()
                    .w(px(60.0))
                    .px_2()
                    .py_1()
                    .text_center()
                    .child("Unique"),
            )
    }

    /// Build a single index row element
    fn build_index_row_element(
        &self,
        idx: usize,
        is_selected: bool,
        name: String,
        columns: String,
        index_type: String,
        is_unique: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .id(SharedString::from(format!("index-{}", idx)))
            .w_full()
            .bg(if is_selected {
                theme.selection
            } else if idx % 2 == 0 {
                theme.table_even
            } else {
                theme.table
            })
            .border_l_1()
            .border_r_1()
            .border_b_1()
            .border_color(theme.border)
            .on_click(cx.listener(move |this, _, _window, cx| {
                this.selected_index_index = Some(idx);
                cx.notify();
            }))
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(if name.is_empty() {
                        SharedString::from("(unnamed)")
                    } else {
                        SharedString::from(name)
                    }),
            )
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child(columns),
            )
            .child(
                div()
                    .w(px(80.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(index_type),
            )
            .child(
                div().w(px(60.0)).py_1().flex().justify_center().child(
                    Checkbox::new(SharedString::from(format!("idx-unique-{}", idx)))
                        .checked(is_unique)
                        .on_click(cx.listener(move |this, _checked, _window, cx| {
                            if let Some(index) = this.design.indexes.get_mut(idx) {
                                index.is_unique = !index.is_unique;
                                this.mark_dirty(cx);
                            }
                        })),
                ),
            )
    }

    /// Render the foreign keys tab content (delegates to ui/foreign_keys_tab.rs)
    fn render_foreign_keys_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::foreign_keys_tab::render_foreign_keys_tab(self, cx)
    }

    /// Render foreign key header row
    fn render_fk_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .bg(theme.table_head)
            .border_1()
            .border_color(theme.border)
            .text_xs()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Name"),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Column(s)"),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("References Table"),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("References Column(s)"),
            )
            .child(
                div()
                    .w(px(100.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("On Delete"),
            )
            .child(div().w(px(100.0)).px_2().py_1().child("On Update"))
    }

    /// Render a single foreign key row (inner implementation)
    fn render_fk_row_inner(
        &self,
        idx: usize,
        is_selected: bool,
        name: String,
        columns: String,
        referenced_table: String,
        referenced_columns: String,
        on_delete: &'static str,
        on_update: &'static str,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .id(SharedString::from(format!("fk-{}", idx)))
            .w_full()
            .bg(if is_selected {
                theme.selection
            } else if idx % 2 == 0 {
                theme.table_even
            } else {
                theme.table
            })
            .border_l_1()
            .border_r_1()
            .border_b_1()
            .border_color(theme.border)
            .on_click(cx.listener(move |this, _, _window, cx| {
                this.selected_fk_index = Some(idx);
                cx.notify();
            }))
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(name),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(columns),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(referenced_table),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_sm()
                    .child(referenced_columns),
            )
            .child(
                div()
                    .w(px(100.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .text_xs()
                    .child(on_delete),
            )
            .child(div().w(px(100.0)).px_2().py_1().text_xs().child(on_update))
    }

    /// Render the options tab content (delegates to ui/options_tab.rs)
    fn render_options_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::options_tab::render_options_tab(self, cx)
    }

    /// Render SQLite-specific options
    fn render_sqlite_options(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        Checkbox::new("without-rowid")
                            .checked(self.design.options.without_rowid)
                            .on_click(cx.listener(|this, _checked, _window, cx| {
                                this.design.options.without_rowid =
                                    !this.design.options.without_rowid;
                                this.mark_dirty(cx);
                            })),
                    )
                    .child(div().text_sm().child("WITHOUT ROWID")),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        Checkbox::new("strict")
                            .checked(self.design.options.strict)
                            .on_click(cx.listener(|this, _checked, _window, cx| {
                                this.design.options.strict = !this.design.options.strict;
                                this.mark_dirty(cx);
                            })),
                    )
                    .child(div().text_sm().child("STRICT")),
            )
    }

    /// Render MySQL-specific options
    fn render_mysql_options(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted_fg = theme.muted_foreground;

        // Extract values to avoid lifetime issues with self escaping the method
        let engine = self
            .design
            .options
            .engine
            .clone()
            .unwrap_or_else(|| "InnoDB".to_string());
        let charset = self
            .design
            .options
            .charset
            .clone()
            .unwrap_or_else(|| "utf8mb4".to_string());
        let collation = self
            .design
            .options
            .collation
            .clone()
            .unwrap_or_else(|| "utf8mb4_unicode_ci".to_string());

        v_flex()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Engine:"))
                    .child(div().text_sm().text_color(muted_fg).child(engine)),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Charset:"))
                    .child(div().text_sm().text_color(muted_fg).child(charset)),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Collation:"))
                    .child(div().text_sm().text_color(muted_fg).child(collation)),
            )
    }

    /// Render PostgreSQL-specific options
    fn render_postgres_options(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex().gap_2().child(
            h_flex()
                .gap_2()
                .items_center()
                .child(
                    Checkbox::new("unlogged")
                        .checked(self.design.options.unlogged)
                        .on_click(cx.listener(|this, _checked, _window, cx| {
                            this.design.options.unlogged = !this.design.options.unlogged;
                            this.mark_dirty(cx);
                        })),
                )
                .child(div().text_sm().child("UNLOGGED")),
        )
    }

    /// Render the SQL preview tab content (delegates to ui/sql_preview_tab.rs)
    fn render_sql_preview_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::sql_preview_tab::render_sql_preview_tab(self, cx)
    }

    /// Render the footer with save/cancel buttons
    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        // Validate by checking actual input values, not self.design
        let table_name = self.table_name_input.read(cx).value();
        let table_name_valid = !table_name.is_empty();

        let columns_valid = !self.design.columns.is_empty()
            && self.design.columns.iter().enumerate().all(|(i, col)| {
                let name_valid = self
                    .column_name_inputs
                    .get(i)
                    .map(|input| !input.read(cx).value().is_empty())
                    .unwrap_or(false);
                let type_valid = !col.data_type.is_empty();
                name_valid && type_valid
            });

        let is_valid = table_name_valid && columns_valid;

        h_flex()
            .w_full()
            .justify_between()
            .p_3()
            .border_t_1()
            .border_color(theme.border)
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} columns", self.design.columns.len())),
                    )
                    .when(self.is_dirty, |this| {
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
                                "Create Table"
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

impl Render for TableDesignerPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Extract theme colors before any mutable borrows
        let theme = cx.theme();
        let bg_color = theme.background;
        let border_color = theme.border;
        let active_tab = self.active_tab;

        // Render tab content based on active tab
        let tab_content = match active_tab {
            DesignerTab::Fields => self.render_fields_tab(cx).into_any_element(),
            DesignerTab::Indexes => self.render_indexes_tab(cx).into_any_element(),
            DesignerTab::ForeignKeys => self.render_foreign_keys_tab(cx).into_any_element(),
            DesignerTab::Options => self.render_options_tab(cx).into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview_tab(cx).into_any_element(),
        };

        let tab_bar = self.render_tab_bar(cx).into_any_element();
        let footer = self.render_footer(cx).into_any_element();

        v_flex()
            .id("table-designer-panel")
            .key_context("TableDesignerPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(bg_color)
            .child(
                // Table name header
                h_flex()
                    .w_full()
                    .p_3()
                    .gap_2()
                    .items_center()
                    .border_b_1()
                    .border_color(border_color)
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Table:"),
                    )
                    .child(Input::new(&self.table_name_input).small().w(px(200.0))),
            )
            .child(tab_bar)
            .child(div().flex_1().overflow_hidden().child(tab_content))
            .child(footer)
    }
}

impl Focusable for TableDesignerPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for TableDesignerPanel {}
impl EventEmitter<TableDesignerEvent> for TableDesignerPanel {}

impl Panel for TableDesignerPanel {
    fn panel_name(&self) -> &'static str {
        "TableDesignerPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.design.is_new {
            SharedString::from("New Table")
        } else {
            SharedString::from(self.design.table_name.clone())
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }
}
