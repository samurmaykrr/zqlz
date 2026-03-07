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
    menu::{ContextMenuExt, PopupMenuItem},
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

/// Default column widths for the Fields tab: [Name, Type, Length, NN, PK, UQ, Default, Comment]
const DEFAULT_FIELD_COL_WIDTHS: [f32; 8] = [180.0, 140.0, 100.0, 50.0, 50.0, 50.0, 0.0, 180.0];
const MIN_COL_WIDTH: f32 = 30.0;

#[derive(Clone)]
struct ColumnResizeDrag {
    col_index: usize,
}

impl Render for ColumnResizeDrag {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        Empty
    }
}

actions!(
    table_designer,
    [
        /// Save the table design
        SaveDesign,
        /// Undo last change
        Undo,
        /// Redo last undone change
        Redo,
        /// Add a new column
        AddColumn,
        /// Remove the selected column/index/FK
        RemoveSelected,
        /// Move selected column up
        MoveColumnUp,
        /// Move selected column down
        MoveColumnDown,
        /// Duplicate selected column
        DuplicateColumn,
        /// Add UUID primary key template
        AddTemplateUuidPk,
        /// Add audit columns (created_at, updated_at)
        AddTemplateAuditColumns,
        /// Add soft delete column (deleted_at)
        AddTemplateSoftDelete,
        /// Select previous row
        SelectPreviousRow,
        /// Select next row
        SelectNextRow,
    ]
);

pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("cmd-s", SaveDesign, Some("TableDesigner")),
        KeyBinding::new("cmd-z", Undo, Some("TableDesigner")),
        KeyBinding::new("cmd-shift-z", Redo, Some("TableDesigner")),
        KeyBinding::new("up", SelectPreviousRow, Some("TableDesigner")),
        KeyBinding::new("down", SelectNextRow, Some("TableDesigner")),
    ]);
}

/// Active tab in the table designer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DesignerTab {
    #[default]
    Fields,
    Indexes,
    ForeignKeys,
    CheckConstraints,
    Triggers,
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

    /// Input states for generated expression (indexed by column ordinal)
    column_generated_inputs: Vec<Entity<InputState>>,

    /// Input states for column scale (indexed by column ordinal)
    column_scale_inputs: Vec<Entity<InputState>>,

    /// Input states for check constraint names
    check_name_inputs: Vec<Entity<InputState>>,

    /// Input states for check constraint expressions
    check_expression_inputs: Vec<Entity<InputState>>,

    /// Input states for index names
    index_name_inputs: Vec<Entity<InputState>>,

    /// Input states for index columns (comma-separated)
    index_columns_inputs: Vec<Entity<InputState>>,

    /// Input states for index type (e.g., BTREE, HASH)
    index_type_inputs: Vec<Entity<InputState>>,

    /// Input states for index WHERE clause (partial indexes)
    index_where_inputs: Vec<Entity<InputState>>,

    /// Input states for index INCLUDE columns (covering indexes)
    index_include_inputs: Vec<Entity<InputState>>,

    /// Input states for FK names
    fk_name_inputs: Vec<Entity<InputState>>,

    /// Input states for FK local columns (comma-separated)
    fk_columns_inputs: Vec<Entity<InputState>>,

    /// Input states for FK referenced table
    fk_ref_table_inputs: Vec<Entity<InputState>>,

    /// Input states for FK referenced columns (comma-separated)
    fk_ref_columns_inputs: Vec<Entity<InputState>>,

    /// Input state for schema name (non-SQLite dialects)
    schema_input: Entity<InputState>,

    /// Input state for table comment
    table_comment_input: Entity<InputState>,

    /// Input state for MySQL engine option
    mysql_engine_input: Entity<InputState>,

    /// Input state for MySQL charset option
    mysql_charset_input: Entity<InputState>,

    /// Input state for MySQL collation option
    mysql_collation_input: Entity<InputState>,

    /// Input state for PostgreSQL tablespace option
    pg_tablespace_input: Entity<InputState>,

    /// Undo stack (snapshots of previous design states)
    undo_stack: Vec<TableDesign>,

    /// Redo stack (snapshots of undone design states)
    redo_stack: Vec<TableDesign>,

    /// Column widths for the Fields tab
    field_col_widths: [f32; 8],

    /// Selected check constraint index
    selected_check_index: Option<usize>,

    /// Available data types for the current dialect
    data_types: Vec<DataTypeInfo>,

    /// Generated DDL preview (cached)
    ddl_preview: Option<String>,

    /// Whether the design has been modified
    is_dirty: bool,

    /// Validation errors from the last save attempt
    validation_errors: Vec<crate::models::ValidationError>,

    /// Cached result of checking all column name inputs are non-empty and all data types are set.
    /// Updated eagerly inside `mark_dirty` so render never needs to loop over entities.
    all_columns_valid: bool,

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

        let schema_input = cx.new(|cx| InputState::new(window, cx).placeholder("Schema (optional)"));
        let table_comment_input = cx.new(|cx| InputState::new(window, cx).placeholder("Table comment"));
        let mysql_engine_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("InnoDB");
            state.set_value("InnoDB", window, cx);
            state
        });
        let mysql_charset_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("utf8mb4");
            state.set_value("utf8mb4", window, cx);
            state
        });
        let mysql_collation_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("utf8mb4_unicode_ci");
            state.set_value("utf8mb4_unicode_ci", window, cx);
            state
        });
        let pg_tablespace_input = cx.new(|cx| InputState::new(window, cx).placeholder("Tablespace (optional)"));

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
            column_generated_inputs: Vec::new(),
            column_scale_inputs: Vec::new(),
            check_name_inputs: Vec::new(),
            check_expression_inputs: Vec::new(),
            index_name_inputs: Vec::new(),
            index_columns_inputs: Vec::new(),
            index_type_inputs: Vec::new(),
            index_where_inputs: Vec::new(),
            index_include_inputs: Vec::new(),
            fk_name_inputs: Vec::new(),
            fk_columns_inputs: Vec::new(),
            fk_ref_table_inputs: Vec::new(),
            fk_ref_columns_inputs: Vec::new(),
            schema_input,
            table_comment_input,
            mysql_engine_input,
            mysql_charset_input,
            mysql_collation_input,
            pg_tablespace_input,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            field_col_widths: DEFAULT_FIELD_COL_WIDTHS,
            selected_check_index: None,
            data_types,
            ddl_preview: None,
            is_dirty: false,
            validation_errors: Vec::new(),
            // No columns yet for a new table, so the footer Save button starts disabled.
            all_columns_valid: false,
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
        let mut column_generated_inputs = Vec::with_capacity(design.columns.len());

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

            let gen_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Expression");
                if let Some(ref expr) = col.generated_expression {
                    state.set_value(expr, window, cx);
                }
                state
            });
            column_generated_inputs.push(gen_input);

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
        for input in &column_generated_inputs {
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

        // Create scale inputs for existing columns
        let mut column_scale_inputs = Vec::with_capacity(design.columns.len());
        for col in &design.columns {
            let scale_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Scale");
                if let Some(scale) = col.scale {
                    state.set_value(&scale.to_string(), window, cx);
                }
                state
            });
            subscriptions.push(cx.subscribe(&scale_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            column_scale_inputs.push(scale_input);
        }

        let schema_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Schema (optional)");
            if let Some(ref schema) = design.schema {
                state.set_value(schema, window, cx);
            }
            state
        });
        let table_comment_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Table comment");
            if let Some(ref comment) = design.comment {
                state.set_value(comment, window, cx);
            }
            state
        });

        let engine_val = design.options.engine.clone().unwrap_or_else(|| "InnoDB".to_string());
        let charset_val = design.options.charset.clone().unwrap_or_else(|| "utf8mb4".to_string());
        let collation_val = design.options.collation.clone().unwrap_or_else(|| "utf8mb4_unicode_ci".to_string());
        let tablespace_val = design.options.tablespace.clone().unwrap_or_default();

        let mysql_engine_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("InnoDB");
            state.set_value(&engine_val, window, cx);
            state
        });
        let mysql_charset_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("utf8mb4");
            state.set_value(&charset_val, window, cx);
            state
        });
        let mysql_collation_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("utf8mb4_unicode_ci");
            state.set_value(&collation_val, window, cx);
            state
        });
        let pg_tablespace_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Tablespace (optional)");
            if !tablespace_val.is_empty() {
                state.set_value(&tablespace_val, window, cx);
            }
            state
        });

        // Create check constraint inputs
        let mut check_name_inputs = Vec::new();
        let mut check_expression_inputs = Vec::new();
        for cc in &design.check_constraints {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Constraint name");
                if let Some(ref name) = cc.name {
                    state.set_value(name, window, cx);
                }
                state
            });
            subscriptions.push(cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            check_name_inputs.push(name_input);

            let expr_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("CHECK expression");
                state.set_value(&cc.expression, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&expr_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            check_expression_inputs.push(expr_input);
        }

        // Create index inputs for existing indexes
        let mut index_name_inputs = Vec::new();
        let mut index_columns_inputs = Vec::new();
        let mut index_type_inputs = Vec::new();
        let mut index_where_inputs = Vec::new();
        let mut index_include_inputs = Vec::new();
        for index in &design.indexes {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Index name");
                state.set_value(&index.name, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            index_name_inputs.push(name_input);

            let cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&index.columns.join(", "), window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            index_columns_inputs.push(cols_input);

            let type_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("BTREE");
                state.set_value(&index.index_type, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&type_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            index_type_inputs.push(type_input);

            let where_clause_val = index.where_clause.clone().unwrap_or_default();
            let where_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("WHERE clause");
                if !where_clause_val.is_empty() {
                    state.set_value(&where_clause_val, window, cx);
                }
                state
            });
            subscriptions.push(cx.subscribe(&where_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            index_where_inputs.push(where_input);

            let include_cols_val = index.include_columns.join(", ");
            let include_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                if !include_cols_val.is_empty() {
                    state.set_value(&include_cols_val, window, cx);
                }
                state
            });
            subscriptions.push(cx.subscribe(&include_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            index_include_inputs.push(include_input);
        }

        // Create FK inputs for existing foreign keys
        let mut fk_name_inputs = Vec::new();
        let mut fk_columns_inputs = Vec::new();
        let mut fk_ref_table_inputs = Vec::new();
        let mut fk_ref_columns_inputs = Vec::new();
        for fk in &design.foreign_keys {
            let fk_name_val = fk.name.clone().unwrap_or_default();
            let fk_cols_val = fk.columns.join(", ");
            let fk_ref_table_val = fk.referenced_table.clone();
            let fk_ref_cols_val = fk.referenced_columns.join(", ");

            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("FK name");
                state.set_value(&fk_name_val, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            fk_name_inputs.push(name_input);

            let cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&fk_cols_val, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }));
            fk_columns_inputs.push(cols_input);

            let ref_table_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Referenced table");
                state.set_value(&fk_ref_table_val, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(
                &ref_table_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            fk_ref_table_inputs.push(ref_table_input);

            let ref_cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&fk_ref_cols_val, window, cx);
                state
            });
            subscriptions.push(cx.subscribe(
                &ref_cols_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            fk_ref_columns_inputs.push(ref_cols_input);
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
            column_generated_inputs,
            column_scale_inputs,
            check_name_inputs,
            check_expression_inputs,
            index_name_inputs,
            index_columns_inputs,
            index_type_inputs,
            index_where_inputs,
            index_include_inputs,
            fk_name_inputs,
            fk_columns_inputs,
            fk_ref_table_inputs,
            fk_ref_columns_inputs,
            schema_input,
            table_comment_input,
            mysql_engine_input,
            mysql_charset_input,
            mysql_collation_input,
            pg_tablespace_input,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            field_col_widths: DEFAULT_FIELD_COL_WIDTHS,
            selected_check_index: None,
            data_types,
            ddl_preview: None,
            is_dirty: false,
            validation_errors: Vec::new(),
            // Existing columns were pre-populated from the loaded design so they start valid.
            all_columns_valid: true,
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

    const MAX_UNDO_DEPTH: usize = 50;

    /// Save the current design state onto the undo stack before a mutation.
    fn push_undo_snapshot(&mut self) {
        self.undo_stack.push(self.design.clone());
        if self.undo_stack.len() > Self::MAX_UNDO_DEPTH {
            self.undo_stack.remove(0);
        }
        self.redo_stack.clear();
    }

    /// Undo the last design change.
    fn handle_undo(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(snapshot) = self.undo_stack.pop() {
            self.redo_stack.push(self.design.clone());
            self.design = snapshot;
            self.rebuild_column_inputs_from_design(window, cx);
            self.is_dirty = true;
            self.ddl_preview = None;
            self.recompute_columns_valid(cx);
            cx.notify();
        }
    }

    /// Redo the last undone change.
    fn handle_redo(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(snapshot) = self.redo_stack.pop() {
            self.undo_stack.push(self.design.clone());
            self.design = snapshot;
            self.rebuild_column_inputs_from_design(window, cx);
            self.is_dirty = true;
            self.ddl_preview = None;
            self.recompute_columns_valid(cx);
            cx.notify();
        }
    }

    /// Rebuild all column input/select states from the current design.
    /// Used after undo/redo to sync UI widgets with the restored design state.
    fn rebuild_column_inputs_from_design(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.column_name_inputs.clear();
        self.column_default_inputs.clear();
        self.column_length_inputs.clear();
        self.column_type_selects.clear();
        self.column_comment_inputs.clear();
        self.column_generated_inputs.clear();
        self.column_scale_inputs.clear();

        for (col_idx, col) in self.design.columns.iter().enumerate() {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Column name");
                state.set_value(&col.name, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.column_name_inputs.push(name_input);

            let default_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Default");
                if let Some(ref default) = col.default_value {
                    state.set_value(default, window, cx);
                }
                state
            });
            self._subscriptions.push(cx.subscribe(
                &default_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            self.column_default_inputs.push(default_input);

            let length_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Length");
                if let Some(length) = col.length {
                    state.set_value(&length.to_string(), window, cx);
                }
                state
            });
            self._subscriptions.push(
                cx.subscribe(&length_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.column_length_inputs.push(length_input);

            let scale_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Scale");
                if let Some(scale) = col.scale {
                    state.set_value(&scale.to_string(), window, cx);
                }
                state
            });
            self._subscriptions.push(
                cx.subscribe(&scale_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.column_scale_inputs.push(scale_input);

            let comment_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Comment");
                if let Some(ref comment) = col.comment {
                    state.set_value(comment, window, cx);
                }
                state
            });
            self._subscriptions.push(cx.subscribe(
                &comment_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            self.column_comment_inputs.push(comment_input);

            let gen_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Expression");
                if let Some(ref expr) = col.generated_expression {
                    state.set_value(expr, window, cx);
                }
                state
            });
            self._subscriptions.push(cx.subscribe(
                &gen_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            self.column_generated_inputs.push(gen_input);

            let data_types_clone = self.data_types.clone();
            let selected_index = data_types_clone
                .iter()
                .position(|dt| dt.name.eq_ignore_ascii_case(&col.data_type))
                .map(|i| zqlz_ui::widgets::IndexPath::default().row(i));
            let type_select = cx.new(|cx| {
                SelectState::new(data_types_clone, selected_index, window, cx).searchable(true)
            });
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
        }

        // Rebuild check constraint inputs
        self.check_name_inputs.clear();
        self.check_expression_inputs.clear();
        for cc in &self.design.check_constraints {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Constraint name");
                if let Some(ref name) = cc.name {
                    state.set_value(name, window, cx);
                }
                state
            });
            self._subscriptions.push(
                cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.check_name_inputs.push(name_input);

            let expr_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("CHECK expression");
                state.set_value(&cc.expression, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&expr_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.check_expression_inputs.push(expr_input);
        }

        // Rebuild index inputs
        self.index_name_inputs.clear();
        self.index_columns_inputs.clear();
        self.index_type_inputs.clear();
        self.index_where_inputs.clear();
        self.index_include_inputs.clear();
        let index_values: Vec<_> = self.design.indexes.iter().map(|index| {
            (
                index.name.clone(),
                index.columns.join(", "),
                index.index_type.clone(),
                index.where_clause.clone().unwrap_or_default(),
                index.include_columns.join(", "),
            )
        }).collect();
        for (name_val, cols_val, type_val, where_val, include_val) in index_values {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Index name");
                state.set_value(&name_val, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.index_name_inputs.push(name_input);

            let cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&cols_val, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.index_columns_inputs.push(cols_input);

            let type_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("BTREE");
                state.set_value(&type_val, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&type_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.index_type_inputs.push(type_input);

            let where_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("WHERE clause");
                if !where_val.is_empty() {
                    state.set_value(&where_val, window, cx);
                }
                state
            });
            self._subscriptions.push(
                cx.subscribe(&where_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.index_where_inputs.push(where_input);

            let include_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                if !include_val.is_empty() {
                    state.set_value(&include_val, window, cx);
                }
                state
            });
            self._subscriptions.push(
                cx.subscribe(&include_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.index_include_inputs.push(include_input);
        }

        // Rebuild FK inputs - clone values to avoid self borrow issues
        self.fk_name_inputs.clear();
        self.fk_columns_inputs.clear();
        self.fk_ref_table_inputs.clear();
        self.fk_ref_columns_inputs.clear();
        let fk_values: Vec<_> = self.design.foreign_keys.iter().map(|fk| {
            (
                fk.name.clone().unwrap_or_default(),
                fk.columns.join(", "),
                fk.referenced_table.clone(),
                fk.referenced_columns.join(", "),
            )
        }).collect();
        for (fk_name_val, fk_cols_val, fk_ref_table_val, fk_ref_cols_val) in fk_values {
            let name_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("FK name");
                state.set_value(&fk_name_val, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.fk_name_inputs.push(name_input);

            let cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&fk_cols_val, window, cx);
                state
            });
            self._subscriptions.push(
                cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                }),
            );
            self.fk_columns_inputs.push(cols_input);

            let ref_table_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Referenced table");
                state.set_value(&fk_ref_table_val, window, cx);
                state
            });
            self._subscriptions.push(cx.subscribe(
                &ref_table_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            self.fk_ref_table_inputs.push(ref_table_input);

            let ref_cols_input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("col1, col2, ...");
                state.set_value(&fk_ref_cols_val, window, cx);
                state
            });
            self._subscriptions.push(cx.subscribe(
                &ref_cols_input,
                |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        this.mark_dirty(cx);
                    }
                },
            ));
            self.fk_ref_columns_inputs.push(ref_cols_input);
        }

        // Adjust selections
        if self.selected_column_index.map_or(false, |i| i >= self.design.columns.len()) {
            self.selected_column_index = self.design.columns.len().checked_sub(1);
        }
        if self.selected_check_index.map_or(false, |i| i >= self.design.check_constraints.len()) {
            self.selected_check_index = self.design.check_constraints.len().checked_sub(1);
        }
        if self.selected_index_index.map_or(false, |i| i >= self.design.indexes.len()) {
            self.selected_index_index = self.design.indexes.len().checked_sub(1);
        }
        if self.selected_fk_index.map_or(false, |i| i >= self.design.foreign_keys.len()) {
            self.selected_fk_index = self.design.foreign_keys.len().checked_sub(1);
        }
    }

    /// Mark the design as dirty (modified)
    fn mark_dirty(&mut self, cx: &mut Context<Self>) {
        self.is_dirty = true;
        self.ddl_preview = None;
        self.recompute_columns_valid(cx);
        if self.active_tab == DesignerTab::SqlPreview {
            self.generate_ddl_preview(cx);
        }
        cx.notify();
    }

    /// Recompute and cache whether every column has a non-empty name input and a data type set.
    ///
    /// Called from `mark_dirty` so the cached value stays current without needing entity reads
    /// inside the render hot-path.
    fn recompute_columns_valid(&mut self, cx: &Context<Self>) {
        self.all_columns_valid = !self.design.columns.is_empty()
            && self.design.columns.iter().enumerate().all(|(i, col)| {
                let name_valid = self
                    .column_name_inputs
                    .get(i)
                    .map(|input| !input.read(cx).value().is_empty())
                    .unwrap_or(false);
                name_valid && !col.data_type.is_empty()
            });
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
        self.push_undo_snapshot();
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

        let gen_input = cx.new(|cx| InputState::new(window, cx).placeholder("Expression"));
        self._subscriptions.push(cx.subscribe(
            &gen_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            },
        ));
        self.column_generated_inputs.push(gen_input);

        self.selected_column_index = Some(ordinal);
        self.mark_dirty(cx);
    }

    /// Remove selected column
    fn remove_column(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_column_index {
            if idx < self.design.columns.len() {
                self.push_undo_snapshot();
                self.design.columns.remove(idx);
                self.column_name_inputs.remove(idx);
                self.column_default_inputs.remove(idx);
                self.column_length_inputs.remove(idx);
                self.column_type_selects.remove(idx);
                self.column_comment_inputs.remove(idx);
                if idx < self.column_generated_inputs.len() {
                    self.column_generated_inputs.remove(idx);
                }

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
                self.push_undo_snapshot();
                self.design.columns.swap(idx, idx - 1);
                self.column_name_inputs.swap(idx, idx - 1);
                self.column_default_inputs.swap(idx, idx - 1);
                self.column_length_inputs.swap(idx, idx - 1);
                self.column_type_selects.swap(idx, idx - 1);
                self.column_comment_inputs.swap(idx, idx - 1);
                if idx < self.column_generated_inputs.len() && idx - 1 < self.column_generated_inputs.len() {
                    self.column_generated_inputs.swap(idx, idx - 1);
                }

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
                self.push_undo_snapshot();
                self.design.columns.swap(idx, idx + 1);
                self.column_name_inputs.swap(idx, idx + 1);
                self.column_default_inputs.swap(idx, idx + 1);
                self.column_length_inputs.swap(idx, idx + 1);
                self.column_type_selects.swap(idx, idx + 1);
                self.column_comment_inputs.swap(idx, idx + 1);
                if idx < self.column_generated_inputs.len() && idx + 1 < self.column_generated_inputs.len() {
                    self.column_generated_inputs.swap(idx, idx + 1);
                }

                // Update ordinals
                self.design.columns[idx].ordinal = idx;
                self.design.columns[idx + 1].ordinal = idx + 1;

                self.selected_column_index = Some(idx + 1);
                self.mark_dirty(cx);
            }
        }
    }

    /// Select previous row in the active tab
    fn select_previous_row(&mut self, cx: &mut Context<Self>) {
        match self.active_tab {
            DesignerTab::Fields => {
                if let Some(idx) = self.selected_column_index {
                    if idx > 0 {
                        self.selected_column_index = Some(idx - 1);
                        cx.notify();
                    }
                } else if !self.design.columns.is_empty() {
                    self.selected_column_index = Some(0);
                    cx.notify();
                }
            }
            DesignerTab::Indexes => {
                if let Some(idx) = self.selected_index_index {
                    if idx > 0 {
                        self.selected_index_index = Some(idx - 1);
                        cx.notify();
                    }
                } else if !self.design.indexes.is_empty() {
                    self.selected_index_index = Some(0);
                    cx.notify();
                }
            }
            DesignerTab::ForeignKeys => {
                if let Some(idx) = self.selected_fk_index {
                    if idx > 0 {
                        self.selected_fk_index = Some(idx - 1);
                        cx.notify();
                    }
                } else if !self.design.foreign_keys.is_empty() {
                    self.selected_fk_index = Some(0);
                    cx.notify();
                }
            }
            _ => {}
        }
    }

    /// Select next row in the active tab
    fn select_next_row(&mut self, cx: &mut Context<Self>) {
        match self.active_tab {
            DesignerTab::Fields => {
                let max = self.design.columns.len().saturating_sub(1);
                if let Some(idx) = self.selected_column_index {
                    if idx < max {
                        self.selected_column_index = Some(idx + 1);
                        cx.notify();
                    }
                } else if !self.design.columns.is_empty() {
                    self.selected_column_index = Some(0);
                    cx.notify();
                }
            }
            DesignerTab::Indexes => {
                let max = self.design.indexes.len().saturating_sub(1);
                if let Some(idx) = self.selected_index_index {
                    if idx < max {
                        self.selected_index_index = Some(idx + 1);
                        cx.notify();
                    }
                } else if !self.design.indexes.is_empty() {
                    self.selected_index_index = Some(0);
                    cx.notify();
                }
            }
            DesignerTab::ForeignKeys => {
                let max = self.design.foreign_keys.len().saturating_sub(1);
                if let Some(idx) = self.selected_fk_index {
                    if idx < max {
                        self.selected_fk_index = Some(idx + 1);
                        cx.notify();
                    }
                } else if !self.design.foreign_keys.is_empty() {
                    self.selected_fk_index = Some(0);
                    cx.notify();
                }
            }
            _ => {}
        }
    }

    /// Add a new index
    fn add_index(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        self.design.indexes.push(IndexDesign::new());

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Index name"));
        self._subscriptions.push(
            cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.index_name_inputs.push(name_input);

        let cols_input = cx.new(|cx| InputState::new(window, cx).placeholder("col1, col2, ..."));
        self._subscriptions.push(
            cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.index_columns_inputs.push(cols_input);

        let type_input = cx.new(|cx| InputState::new(window, cx).placeholder("BTREE"));
        self._subscriptions.push(
            cx.subscribe(&type_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.index_type_inputs.push(type_input);

        let where_input = cx.new(|cx| InputState::new(window, cx).placeholder("WHERE clause"));
        self._subscriptions.push(
            cx.subscribe(&where_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.index_where_inputs.push(where_input);

        let include_input = cx.new(|cx| InputState::new(window, cx).placeholder("col1, col2, ..."));
        self._subscriptions.push(
            cx.subscribe(&include_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.index_include_inputs.push(include_input);

        self.selected_index_index = Some(self.design.indexes.len() - 1);
        self.mark_dirty(cx);
    }

    /// Remove selected index
    fn remove_index(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_index_index {
            if idx < self.design.indexes.len() {
                self.push_undo_snapshot();
                self.design.indexes.remove(idx);
                if idx < self.index_name_inputs.len() {
                    self.index_name_inputs.remove(idx);
                }
                if idx < self.index_columns_inputs.len() {
                    self.index_columns_inputs.remove(idx);
                }
                if idx < self.index_type_inputs.len() {
                    self.index_type_inputs.remove(idx);
                }
                if idx < self.index_where_inputs.len() {
                    self.index_where_inputs.remove(idx);
                }
                if idx < self.index_include_inputs.len() {
                    self.index_include_inputs.remove(idx);
                }

                if self.design.indexes.is_empty() {
                    self.selected_index_index = None;
                } else if idx >= self.design.indexes.len() {
                    self.selected_index_index = Some(self.design.indexes.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Auto-name all indexes that have empty names (used during sync)
    fn auto_name_indexes_in_design(&mut self) {
        let table_name = self.design.table_name.clone();
        if table_name.is_empty() {
            return;
        }
        for index in self.design.indexes.iter_mut() {
            if index.name.is_empty() && !index.columns.is_empty() {
                index.name = index.auto_name(&table_name);
            }
        }
    }

    /// Add a new foreign key
    fn add_foreign_key(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        self.design.foreign_keys.push(ForeignKeyDesign::new());

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("FK name"));
        self._subscriptions.push(
            cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.fk_name_inputs.push(name_input);

        let cols_input = cx.new(|cx| InputState::new(window, cx).placeholder("col1, col2, ..."));
        self._subscriptions.push(
            cx.subscribe(&cols_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.fk_columns_inputs.push(cols_input);

        let ref_table_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Referenced table"));
        self._subscriptions.push(cx.subscribe(
            &ref_table_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            },
        ));
        self.fk_ref_table_inputs.push(ref_table_input);

        let ref_cols_input = cx.new(|cx| InputState::new(window, cx).placeholder("col1, col2, ..."));
        self._subscriptions.push(cx.subscribe(
            &ref_cols_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            },
        ));
        self.fk_ref_columns_inputs.push(ref_cols_input);

        self.selected_fk_index = Some(self.design.foreign_keys.len() - 1);
        self.mark_dirty(cx);
    }

    /// Remove selected foreign key
    fn remove_foreign_key(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_fk_index {
            if idx < self.design.foreign_keys.len() {
                self.push_undo_snapshot();
                self.design.foreign_keys.remove(idx);
                if idx < self.fk_name_inputs.len() {
                    self.fk_name_inputs.remove(idx);
                }
                if idx < self.fk_columns_inputs.len() {
                    self.fk_columns_inputs.remove(idx);
                }
                if idx < self.fk_ref_table_inputs.len() {
                    self.fk_ref_table_inputs.remove(idx);
                }
                if idx < self.fk_ref_columns_inputs.len() {
                    self.fk_ref_columns_inputs.remove(idx);
                }

                if self.design.foreign_keys.is_empty() {
                    self.selected_fk_index = None;
                } else if idx >= self.design.foreign_keys.len() {
                    self.selected_fk_index = Some(self.design.foreign_keys.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Duplicate the selected column
    fn handle_duplicate_column(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_column_index {
            if let Some(col) = self.design.columns.get(idx).cloned() {
                self.push_undo_snapshot();
                let mut new_col = col;
                new_col.column_id = uuid::Uuid::new_v4();
                new_col.name = format!("{}_copy", new_col.name);
                new_col.ordinal = idx + 1;
                self.design.columns.insert(idx + 1, new_col.clone());

                // Update ordinals for subsequent columns
                for i in (idx + 2)..self.design.columns.len() {
                    self.design.columns[i].ordinal = i;
                }

                // Create input states for the duplicated column
                let name_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Column name");
                    state.set_value(&new_col.name, window, cx);
                    state
                });
                self._subscriptions.push(
                    cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.mark_dirty(cx);
                        }
                    }),
                );
                self.column_name_inputs.insert(idx + 1, name_input);

                let default_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Default");
                    if let Some(ref default) = new_col.default_value {
                        state.set_value(default, window, cx);
                    }
                    state
                });
                self._subscriptions.push(cx.subscribe(
                    &default_input,
                    |this, _, event: &InputEvent, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.mark_dirty(cx);
                        }
                    },
                ));
                self.column_default_inputs.insert(idx + 1, default_input);

                let length_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Length");
                    if let Some(length) = new_col.length {
                        state.set_value(&length.to_string(), window, cx);
                    }
                    state
                });
                self._subscriptions.push(
                    cx.subscribe(&length_input, |this, _, event: &InputEvent, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.mark_dirty(cx);
                        }
                    }),
                );
                self.column_length_inputs.insert(idx + 1, length_input);

                let comment_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Comment");
                    if let Some(ref comment) = new_col.comment {
                        state.set_value(comment, window, cx);
                    }
                    state
                });
                self._subscriptions.push(cx.subscribe(
                    &comment_input,
                    |this, _, event: &InputEvent, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.mark_dirty(cx);
                        }
                    },
                ));
                self.column_comment_inputs.insert(idx + 1, comment_input);

                let gen_input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder("Expression");
                    if let Some(ref expr) = new_col.generated_expression {
                        state.set_value(expr, window, cx);
                    }
                    state
                });
                self._subscriptions.push(cx.subscribe(
                    &gen_input,
                    |this, _, event: &InputEvent, cx| {
                        if matches!(event, InputEvent::Change) {
                            this.mark_dirty(cx);
                        }
                    },
                ));
                self.column_generated_inputs.insert(idx + 1, gen_input);

                let data_types_clone = self.data_types.clone();
                let selected_index = data_types_clone
                    .iter()
                    .position(|dt| dt.name.eq_ignore_ascii_case(&new_col.data_type))
                    .map(|i| zqlz_ui::widgets::IndexPath::default().row(i));
                let type_select = cx.new(|cx| {
                    SelectState::new(data_types_clone, selected_index, window, cx).searchable(true)
                });
                let col_idx = idx + 1;
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
                self.column_type_selects.insert(idx + 1, type_select);

                self.selected_column_index = Some(idx + 1);
                self.mark_dirty(cx);
            }
        }
    }

    /// Add a UUID primary key column template
    fn add_template_uuid_pk(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        let (data_type, default_val) = match self.design.dialect {
            DatabaseDialect::Postgres => ("UUID", Some("gen_random_uuid()")),
            DatabaseDialect::Mysql => ("CHAR", Some("(UUID())")),
            DatabaseDialect::Sqlite => ("TEXT", Some("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))))")),
        };
        let mut col = ColumnDesign::new(0);
        col.name = "id".to_string();
        col.data_type = data_type.to_string();
        col.is_primary_key = true;
        col.nullable = false;
        col.default_value = default_val.map(|s| s.to_string());
        if matches!(self.design.dialect, DatabaseDialect::Mysql) {
            col.length = Some(36);
        }

        // Insert at position 0
        col.ordinal = 0;
        self.design.columns.insert(0, col);
        for i in 1..self.design.columns.len() {
            self.design.columns[i].ordinal = i;
        }

        // Rebuild inputs to match
        self.rebuild_column_inputs_from_design(window, cx);
        self.selected_column_index = Some(0);
        self.mark_dirty(cx);
    }

    /// Add audit columns (created_at, updated_at)
    fn add_template_audit_columns(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        let (dt, default_now) = match self.design.dialect {
            DatabaseDialect::Postgres => ("TIMESTAMPTZ", "NOW()"),
            DatabaseDialect::Mysql => ("TIMESTAMP", "CURRENT_TIMESTAMP"),
            DatabaseDialect::Sqlite => ("TEXT", "CURRENT_TIMESTAMP"),
        };

        let ordinal = self.design.columns.len();
        let mut created = ColumnDesign::new(ordinal);
        created.name = "created_at".to_string();
        created.data_type = dt.to_string();
        created.nullable = false;
        created.default_value = Some(default_now.to_string());
        self.design.columns.push(created);

        let mut updated = ColumnDesign::new(ordinal + 1);
        updated.name = "updated_at".to_string();
        updated.data_type = dt.to_string();
        updated.nullable = false;
        updated.default_value = Some(default_now.to_string());
        self.design.columns.push(updated);

        self.rebuild_column_inputs_from_design(window, cx);
        self.selected_column_index = Some(ordinal);
        self.mark_dirty(cx);
    }

    /// Add soft delete column (deleted_at)
    fn add_template_soft_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        let dt = match self.design.dialect {
            DatabaseDialect::Postgres => "TIMESTAMPTZ",
            DatabaseDialect::Mysql => "TIMESTAMP",
            DatabaseDialect::Sqlite => "TEXT",
        };

        let ordinal = self.design.columns.len();
        let mut col = ColumnDesign::new(ordinal);
        col.name = "deleted_at".to_string();
        col.data_type = dt.to_string();
        col.nullable = true;
        col.default_value = None;
        self.design.columns.push(col);

        self.rebuild_column_inputs_from_design(window, cx);
        self.selected_column_index = Some(ordinal);
        self.mark_dirty(cx);
    }

    /// Add a check constraint
    fn add_check_constraint(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.push_undo_snapshot();
        self.design
            .check_constraints
            .push(crate::models::CheckConstraintDesign::new());
        let idx = self.design.check_constraints.len() - 1;

        let name_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Constraint name"));
        self._subscriptions.push(
            cx.subscribe(&name_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.check_name_inputs.push(name_input);

        let expr_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("CHECK expression"));
        self._subscriptions.push(
            cx.subscribe(&expr_input, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    this.mark_dirty(cx);
                }
            }),
        );
        self.check_expression_inputs.push(expr_input);

        self.selected_check_index = Some(idx);
        self.mark_dirty(cx);
    }

    /// Remove selected check constraint
    fn remove_check_constraint(&mut self, cx: &mut Context<Self>) {
        if let Some(idx) = self.selected_check_index {
            if idx < self.design.check_constraints.len() {
                self.push_undo_snapshot();
                self.design.check_constraints.remove(idx);
                self.check_name_inputs.remove(idx);
                self.check_expression_inputs.remove(idx);

                if self.design.check_constraints.is_empty() {
                    self.selected_check_index = None;
                } else if idx >= self.design.check_constraints.len() {
                    self.selected_check_index =
                        Some(self.design.check_constraints.len() - 1);
                }

                self.mark_dirty(cx);
            }
        }
    }

    /// Sync check constraints from inputs to design
    fn sync_check_constraints_from_inputs(&mut self, cx: &Context<Self>) {
        for (i, cc) in self.design.check_constraints.iter_mut().enumerate() {
            if let Some(input) = self.check_name_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                cc.name = if val.is_empty() { None } else { Some(val) };
            }
            if let Some(input) = self.check_expression_inputs.get(i) {
                cc.expression = input.read(cx).value().to_string();
            }
        }
    }

    fn sync_indexes_from_inputs(&mut self, cx: &Context<Self>) {
        for (i, index) in self.design.indexes.iter_mut().enumerate() {
            if let Some(input) = self.index_name_inputs.get(i) {
                index.name = input.read(cx).value().to_string();
            }
            if let Some(input) = self.index_columns_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                index.columns = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            if let Some(input) = self.index_type_inputs.get(i) {
                index.index_type = input.read(cx).value().to_string();
            }
            if let Some(input) = self.index_where_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                index.where_clause = if val.is_empty() { None } else { Some(val) };
            }
            if let Some(input) = self.index_include_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                index.include_columns = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
    }

    fn sync_foreign_keys_from_inputs(&mut self, cx: &Context<Self>) {
        for (i, fk) in self.design.foreign_keys.iter_mut().enumerate() {
            if let Some(input) = self.fk_name_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                fk.name = if val.is_empty() { None } else { Some(val) };
            }
            if let Some(input) = self.fk_columns_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                fk.columns = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            if let Some(input) = self.fk_ref_table_inputs.get(i) {
                fk.referenced_table = input.read(cx).value().to_string();
            }
            if let Some(input) = self.fk_ref_columns_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                fk.referenced_columns = val
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
    }

    fn sync_options_from_inputs(&mut self, cx: &Context<Self>) {
        let engine = self.mysql_engine_input.read(cx).value().to_string();
        self.design.options.engine = if engine.is_empty() { None } else { Some(engine) };
        let charset = self.mysql_charset_input.read(cx).value().to_string();
        self.design.options.charset = if charset.is_empty() { None } else { Some(charset) };
        let collation = self.mysql_collation_input.read(cx).value().to_string();
        self.design.options.collation = if collation.is_empty() { None } else { Some(collation) };
        let tablespace = self.pg_tablespace_input.read(cx).value().to_string();
        self.design.options.tablespace = if tablespace.is_empty() { None } else { Some(tablespace) };

        let schema = self.schema_input.read(cx).value().to_string();
        self.design.schema = if schema.is_empty() { None } else { Some(schema) };
        let comment = self.table_comment_input.read(cx).value().to_string();
        self.design.comment = if comment.is_empty() { None } else { Some(comment) };
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
            if let Some(input) = self.column_scale_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.scale = val.parse().ok();
            }
            if let Some(input) = self.column_comment_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.comment = if val.is_empty() { None } else { Some(val) };
            }
            if let Some(input) = self.column_generated_inputs.get(i) {
                let val = input.read(cx).value().to_string();
                col.generated_expression = if val.is_empty() { None } else { Some(val) };
            }
        }
    }

    /// Generate DDL preview
    fn generate_ddl_preview(&mut self, cx: &mut Context<Self>) {
        // Sync all data from inputs
        self.sync_table_name(cx);
        self.sync_columns_from_inputs(cx);
        self.sync_check_constraints_from_inputs(cx);
        self.sync_indexes_from_inputs(cx);
        self.auto_name_indexes_in_design();
        self.sync_foreign_keys_from_inputs(cx);
        self.sync_options_from_inputs(cx);

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
        self.sync_check_constraints_from_inputs(cx);
        self.sync_indexes_from_inputs(cx);
        self.auto_name_indexes_in_design();
        self.sync_foreign_keys_from_inputs(cx);
        self.sync_options_from_inputs(cx);

        // Validate
        let errors = self.design.validate();
        if !errors.is_empty() {
            self.validation_errors = errors;
            cx.notify();
            return;
        }
        self.validation_errors.clear();

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
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Name"),
            )
            .child(
                div()
                    .w(px(140.0))
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Type"),
            )
            .child(
                div()
                    .w(px(100.0))
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Length"),
            )
            .child(
                div()
                    .w(px(70.0))
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Scale"),
            )
            .child(
                div()
                    .w(px(50.0))
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
                    .py_2()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("UQ"),
            )
            .child(
                div()
                    .w(px(50.0))
                    .py_2()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("AI"),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Generated As"),
            )
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Default"),
            )
            .child(div().w(px(180.0)).px_2().py_2().child("Comment"))
    }

    /// Build a single column row element
    fn build_column_row_element(
        &self,
        idx: usize,
        is_selected: bool,
        nullable: bool,
        is_primary_key: bool,
        is_unique: bool,
        is_auto_increment: bool,
        name_input: Option<Entity<InputState>>,
        length_input: Option<Entity<InputState>>,
        scale_input: Option<Entity<InputState>>,
        default_input: Option<Entity<InputState>>,
        type_select: Option<Entity<SelectState<Vec<DataTypeInfo>>>>,
        comment_input: Option<Entity<InputState>>,
        generated_input: Option<Entity<InputState>>,
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
            // Scale column
            .child(
                div()
                    .w(px(70.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        scale_input
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
            // Auto-Increment checkbox
            .child(
                div()
                    .w(px(50.0))
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        Checkbox::new(SharedString::from(format!("ai-{}", idx)))
                            .checked(is_auto_increment)
                            .on_click(cx.listener(move |this, _checked, _window, cx| {
                                if let Some(col) = this.design.columns.get_mut(idx) {
                                    col.is_auto_increment = !col.is_auto_increment;
                                    this.mark_dirty(cx);
                                }
                            })),
                    ),
            )
            // Generated expression column
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        generated_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| {
                                Input::new(&self.table_name_input).xsmall().w_full()
                            }),
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
            .context_menu({
                let view = cx.entity().clone();
                move |menu, window, _cx| {
                    menu.item(
                        PopupMenuItem::new("Duplicate").on_click({
                            let view = view.clone();
                            window.listener_for(&view, move |this: &mut TableDesignerPanel, _, window, cx| {
                                this.selected_column_index = Some(idx);
                                this.handle_duplicate_column(window, cx);
                            })
                        }),
                    )
                    .separator()
                    .item(
                        PopupMenuItem::new("Move Up").disabled(idx == 0).on_click({
                            let view = view.clone();
                            window.listener_for(&view, move |this: &mut TableDesignerPanel, _, _, cx| {
                                this.selected_column_index = Some(idx);
                                this.move_column_up(cx);
                            })
                        }),
                    )
                    .item(
                        PopupMenuItem::new("Move Down").on_click({
                            let view = view.clone();
                            window.listener_for(&view, move |this: &mut TableDesignerPanel, _, _, cx| {
                                this.selected_column_index = Some(idx);
                                this.move_column_down(cx);
                            })
                        }),
                    )
                    .separator()
                    .item(
                        PopupMenuItem::new("Delete").on_click({
                            let view = view.clone();
                            window.listener_for(&view, move |this: &mut TableDesignerPanel, _, _, cx| {
                                this.selected_column_index = Some(idx);
                                this.remove_column(cx);
                            })
                        }),
                    )
                }
            })
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
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Unique"),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("WHERE"),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .child("INCLUDE"),
            )
    }

    /// Build a single index row element
    fn build_index_row_element(
        &self,
        idx: usize,
        is_selected: bool,
        _name: String,
        _columns: String,
        _index_type: String,
        is_unique: bool,
        name_input: Option<Entity<InputState>>,
        columns_input: Option<Entity<InputState>>,
        type_input: Option<Entity<InputState>>,
        where_input: Option<Entity<InputState>>,
        include_input: Option<Entity<InputState>>,
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
                    .child(
                        name_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        columns_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .w(px(80.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        type_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .w(px(60.0))
                    .py_1()
                    .flex()
                    .justify_center()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
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
            // WHERE clause input (partial index)
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        where_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            // INCLUDE columns input (covering index)
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .child(
                        include_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
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
        _name: String,
        _columns: String,
        _referenced_table: String,
        _referenced_columns: String,
        on_delete: &'static str,
        on_update: &'static str,
        name_input: Option<Entity<InputState>>,
        columns_input: Option<Entity<InputState>>,
        ref_table_input: Option<Entity<InputState>>,
        ref_columns_input: Option<Entity<InputState>>,
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
                    .child(
                        name_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        columns_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .w(px(150.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        ref_table_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
            )
            .child(
                div()
                    .w(px(120.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .child(
                        ref_columns_input
                            .map(|input| Input::new(&input).xsmall().w_full())
                            .unwrap_or_else(|| Input::new(&self.table_name_input).xsmall().w_full()),
                    ),
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

    /// Render the check constraints tab (delegates to ui/check_constraints_tab.rs)
    fn render_check_constraints_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::check_constraints_tab::render_check_constraints_tab(self, cx)
    }

    /// Render check constraint header row
    fn render_check_header(&self, cx: &Context<Self>) -> impl IntoElement {
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
                    .w(px(200.0))
                    .px_3()
                    .py_2()
                    .border_r_1()
                    .border_color(theme.border)
                    .child("Name"),
            )
            .child(div().flex_1().px_3().py_2().child("Expression"))
    }

    /// Build a single check constraint row element
    fn build_check_row_element(
        &self,
        idx: usize,
        is_selected: bool,
        name_input: Option<Entity<InputState>>,
        expr_input: Option<Entity<InputState>>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .id(SharedString::from(format!("check-{}", idx)))
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
                this.selected_check_index = Some(idx);
                cx.notify();
            }))
            .child(
                div()
                    .w(px(200.0))
                    .px_2()
                    .py_1()
                    .border_r_1()
                    .border_color(theme.border)
                    .when_some(name_input, |el, input| {
                        el.child(Input::new(&input).xsmall().w_full())
                    }),
            )
            .child(
                div()
                    .flex_1()
                    .px_2()
                    .py_1()
                    .when_some(expr_input, |el, input| {
                        el.child(Input::new(&input).xsmall().w_full())
                    }),
            )
    }

    /// Render the triggers tab (placeholder — view-only list)
    fn render_triggers_tab(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .p_4()
            .child(
                div()
                    .w_full()
                    .py_8()
                    .text_center()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("Triggers are managed via SQL. Use the query editor to create or modify triggers."),
            )
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
    fn render_mysql_options(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Engine:"))
                    .child(Input::new(&self.mysql_engine_input).small().w(px(200.0))),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Charset:"))
                    .child(Input::new(&self.mysql_charset_input).small().w(px(200.0))),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Collation:"))
                    .child(Input::new(&self.mysql_collation_input).small().w(px(200.0))),
            )
    }

    /// Render PostgreSQL-specific options
    fn render_postgres_options(&self, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .gap_2()
            .child(
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
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(div().w(px(120.0)).text_sm().child("Tablespace:"))
                    .child(Input::new(&self.pg_tablespace_input).small().w(px(200.0))),
            )
    }

    /// Render the SQL preview tab content (delegates to ui/sql_preview_tab.rs)
    fn render_sql_preview_tab(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        ui::sql_preview_tab::render_sql_preview_tab(self, cx)
    }

    /// Render the footer with save/cancel buttons
    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let table_name = self.table_name_input.read(cx).value();
        let table_name_valid = !table_name.is_empty();

        let is_valid = table_name_valid && self.all_columns_valid;

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
        let bg_color;
        let border_color;
        let danger_color;
        {
            let theme = cx.theme();
            bg_color = theme.background;
            border_color = theme.border;
            danger_color = theme.danger;
        }
        let active_tab = self.active_tab;
        let show_schema = !matches!(self.design.dialect, DatabaseDialect::Sqlite);
        let has_validation_errors = !self.validation_errors.is_empty();
        let validation_error_text = if has_validation_errors {
            self.validation_errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join(" · ")
        } else {
            String::new()
        };

        // Render tab content based on active tab
        let tab_content = match active_tab {
            DesignerTab::Fields => self.render_fields_tab(cx).into_any_element(),
            DesignerTab::Indexes => self.render_indexes_tab(cx).into_any_element(),
            DesignerTab::ForeignKeys => self.render_foreign_keys_tab(cx).into_any_element(),
            DesignerTab::CheckConstraints => {
                self.render_check_constraints_tab(cx).into_any_element()
            }
            DesignerTab::Options => self.render_options_tab(cx).into_any_element(),
            DesignerTab::SqlPreview => self.render_sql_preview_tab(cx).into_any_element(),
            DesignerTab::Triggers => self.render_triggers_tab(cx).into_any_element(),
        };

        let tab_bar = self.render_tab_bar(cx).into_any_element();
        let footer = self.render_footer(cx).into_any_element();

        v_flex()
            .id("table-designer-panel")
            .key_context("TableDesigner")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(bg_color)
            .on_action(cx.listener(|this, _: &SaveDesign, _window, cx| {
                this.handle_save(cx);
            }))
            .on_action(cx.listener(|this, _: &Undo, window, cx| {
                this.handle_undo(window, cx);
            }))
            .on_action(cx.listener(|this, _: &Redo, window, cx| {
                this.handle_redo(window, cx);
            }))
            .on_action(cx.listener(|this, _: &AddColumn, window, cx| {
                this.add_column(window, cx);
            }))
            .on_action(cx.listener(|this, _: &RemoveSelected, _window, cx| {
                match this.active_tab {
                    DesignerTab::Fields => this.remove_column(cx),
                    DesignerTab::Indexes => this.remove_index(cx),
                    DesignerTab::ForeignKeys => this.remove_foreign_key(cx),
                    _ => {}
                }
            }))
            .on_action(cx.listener(|this, _: &MoveColumnUp, _window, cx| {
                this.move_column_up(cx);
            }))
            .on_action(cx.listener(|this, _: &MoveColumnDown, _window, cx| {
                this.move_column_down(cx);
            }))
            .on_action(cx.listener(|this, _: &DuplicateColumn, window, cx| {
                this.handle_duplicate_column(window, cx);
            }))
            .on_action(cx.listener(|this, _: &AddTemplateUuidPk, window, cx| {
                this.add_template_uuid_pk(window, cx);
            }))
            .on_action(cx.listener(|this, _: &AddTemplateAuditColumns, window, cx| {
                this.add_template_audit_columns(window, cx);
            }))
            .on_action(cx.listener(|this, _: &AddTemplateSoftDelete, window, cx| {
                this.add_template_soft_delete(window, cx);
            }))
            .on_action(cx.listener(|this, _: &SelectPreviousRow, _window, cx| {
                this.select_previous_row(cx);
            }))
            .on_action(cx.listener(|this, _: &SelectNextRow, _window, cx| {
                this.select_next_row(cx);
            }))
            .child(
                // Table name header with schema and comment
                v_flex()
                    .w_full()
                    .p_3()
                    .gap_2()
                    .border_b_1()
                    .border_color(border_color)
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Table:"),
                            )
                            .child(Input::new(&self.table_name_input).small().w(px(200.0)))
                            .when(show_schema, |this| {
                                this.child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::MEDIUM)
                                        .child("Schema:"),
                                )
                                .child(
                                    Input::new(&self.schema_input).small().w(px(150.0)),
                                )
                            })
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Comment:"),
                            )
                            .child(
                                Input::new(&self.table_comment_input)
                                    .small()
                                    .w(px(200.0)),
                            ),
                    ),
            )
            // Validation error banner
            .when(has_validation_errors, |this| {
                this.child(
                    h_flex()
                        .w_full()
                        .px_3()
                        .py_2()
                        .bg(danger_color.opacity(0.1))
                        .border_b_1()
                        .border_color(danger_color.opacity(0.3))
                        .gap_2()
                        .items_center()
                        .child(
                            div()
                                .text_xs()
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(danger_color)
                                .child("Validation:"),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(danger_color)
                                .child(validation_error_text),
                        ),
                )
            })
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
        let name = if self.design.is_new {
            "New Table".to_string()
        } else {
            self.design.table_name.clone()
        };
        if self.is_dirty {
            SharedString::from(format!("● {}", name))
        } else {
            SharedString::from(name)
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }

    fn has_unsaved_changes(&self, _cx: &App) -> bool {
        self.is_dirty
    }
}
