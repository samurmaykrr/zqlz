//! Export Wizard Widget
//!
//! A multi-step wizard for exporting database tables to files.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use zqlz_core::Connection;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Root, Sizable,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    scroll::{ScrollableElement, Scrollbar, ScrollbarShow},
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    title_bar::TitleBar,
    tooltip::Tooltip,
    v_flex,
};

const EXPORT_LIST_SCROLLBAR_WIDTH: f32 = 16.0;
const EXPORT_TABLE_ROW_HEIGHT: f32 = 38.0;
const EXPORT_TABLE_CHECKBOX_WIDTH: f32 = 44.0;
const EXPORT_TABLE_NAME_WIDTH: f32 = 260.0;
const EXPORT_COLUMN_ROW_HEIGHT: f32 = 34.0;
const EXPORT_LOG_ROW_HEIGHT: f32 = 22.0;
const EXPORT_PROGRESS_TICK_MS: u64 = 200;

use super::types::*;
use crate::{
    CsvExportProgress, CsvExporter,
    exporter::{ExportOptions, ExportProgress, Exporter, GenericExporter, helpers as udif_helpers},
};

/// Events emitted by the export wizard
#[derive(Clone, Debug)]
pub enum ExportWizardEvent {
    /// User requested to close the wizard
    Close,
    /// User requested to start the export
    StartExport,
    /// Export completed
    ExportComplete,
    /// Export failed with error message
    ExportFailed(String),
    /// User wants to open the output folder
    OpenFolder(PathBuf),
    /// User wants to save a profile
    SaveProfile(ExportProfile),
}

/// Select item for encoding dropdown
#[derive(Clone, Debug)]
struct EncodingItem {
    value: FileEncoding,
    label: SharedString,
}

impl SelectItem for EncodingItem {
    type Value = FileEncoding;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Select item for timestamp format dropdown
#[derive(Clone, Debug)]
struct TimestampItem {
    value: TimestampFormat,
    label: SharedString,
}

impl SelectItem for TimestampItem {
    type Value = TimestampFormat;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

#[derive(Clone, Debug)]
struct DateOrderItem {
    value: DateOrder,
    label: SharedString,
}

impl SelectItem for DateOrderItem {
    type Value = DateOrder;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Select item for table dropdown (field selection step)
#[derive(Clone, Debug)]
struct TableItem {
    index: usize,
    name: SharedString,
}

impl SelectItem for TableItem {
    type Value = usize;

    fn title(&self) -> SharedString {
        self.name.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.index
    }
}

/// Select item for delimiter dropdowns
#[derive(Clone, Debug)]
struct DelimiterItem<T: Clone> {
    value: T,
    label: SharedString,
}

impl SelectItem for DelimiterItem<RecordDelimiter> {
    type Value = RecordDelimiter;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl SelectItem for DelimiterItem<FieldDelimiter> {
    type Value = FieldDelimiter;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl SelectItem for DelimiterItem<TextQualifier> {
    type Value = TextQualifier;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &Self::Value {
        &self.value
    }
}

impl SelectItem for DelimiterItem<BinaryEncoding> {
    type Value = BinaryEncoding;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Select item for export format dropdown
#[derive(Clone, Debug)]
struct FormatItem {
    value: ExportFormat,
    label: SharedString,
}

impl SelectItem for FormatItem {
    type Value = ExportFormat;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

#[derive(Clone, Debug, Default)]
struct ExportProgressSnapshot {
    current_object: String,
    total_objects: usize,
    completed_objects: usize,
    rows_exported: u64,
    total_rows: Option<u64>,
    message: Option<String>,
    log_level: LogLevel,
}

fn sanitize_profile_filename(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let trimmed = sanitized.trim_matches('_');
    if trimmed.is_empty() {
        "export_profile".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Export Wizard Panel
pub struct ExportWizard {
    focus_handle: FocusHandle,
    state: ExportWizardState,

    /// Database connection for executing export
    connection: Option<Arc<dyn Connection>>,

    // Step 1: Table Selection
    format_select_state: Entity<SelectState<SearchableVec<FormatItem>>>,
    encoding_state: Entity<SelectState<SearchableVec<EncodingItem>>>,
    timestamp_state: Entity<SelectState<SearchableVec<TimestampItem>>>,
    folder_input_state: Entity<InputState>,

    /// Index of the table row that was right-clicked (for context menu)
    context_menu_row: Option<usize>,

    // Step 2: Field Selection
    table_select_state: Entity<SelectState<SearchableVec<TableItem>>>,

    // Step 3: Format Options
    record_delimiter_state: Entity<SelectState<SearchableVec<DelimiterItem<RecordDelimiter>>>>,
    field_delimiter_state: Entity<SelectState<SearchableVec<DelimiterItem<FieldDelimiter>>>>,
    text_qualifier_state: Entity<SelectState<SearchableVec<DelimiterItem<TextQualifier>>>>,
    binary_encoding_state: Entity<SelectState<SearchableVec<DelimiterItem<BinaryEncoding>>>>,
    date_order_state: Entity<SelectState<SearchableVec<DateOrderItem>>>,
    decimal_input_state: Entity<InputState>,
    date_delimiter_input_state: Entity<InputState>,
    time_delimiter_input_state: Entity<InputState>,

    // Virtualized list scroll handles
    table_scroll_handle: UniformListScrollHandle,
    column_scroll_handle: UniformListScrollHandle,
    log_scroll_handle: UniformListScrollHandle,

    /// Export start time for elapsed calculation
    export_start_time: Option<Instant>,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ExportWizardEvent> for ExportWizard {}

pub struct ExportWizardHandle {
    pub wizard: Entity<ExportWizard>,
    pub window: AnyWindowHandle,
}

impl ExportWizard {
    pub fn new(
        initial_state: ExportWizardState,
        connection: Option<Arc<dyn Connection>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut subscriptions = Vec::new();

        // Build format items
        let format_items: Vec<FormatItem> = ExportFormat::all()
            .iter()
            .map(|f| FormatItem {
                value: *f,
                label: f.display_name().into(),
            })
            .collect();
        let format_index = format_items
            .iter()
            .position(|i| i.value == initial_state.export_format);

        let format_select_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(format_items),
                format_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build encoding items
        let encoding_items: Vec<EncodingItem> = FileEncoding::all()
            .iter()
            .map(|e| EncodingItem {
                value: *e,
                label: e.display_name().into(),
            })
            .collect();
        let encoding_index = encoding_items
            .iter()
            .position(|i| i.value == initial_state.encoding);

        let encoding_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(encoding_items),
                encoding_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build timestamp items
        let timestamp_items: Vec<TimestampItem> = TimestampFormat::all()
            .iter()
            .map(|t| TimestampItem {
                value: *t,
                label: t.display_name().into(),
            })
            .collect();
        let timestamp_index = timestamp_items
            .iter()
            .position(|i| i.value == initial_state.timestamp_format);

        let timestamp_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(timestamp_items),
                timestamp_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Folder input
        let folder_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.output_folder.display().to_string())
        });

        // Table select (will be populated based on tables)
        let table_items: Vec<TableItem> = initial_state
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.selected)
            .map(|(i, t)| TableItem {
                index: i,
                name: t.table_name.clone().into(),
            })
            .collect();

        let table_select_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(table_items),
                Some(zqlz_ui::widgets::IndexPath::default().row(0)),
                window,
                cx,
            )
        });

        // Format option selects
        let record_delimiter_items: Vec<DelimiterItem<RecordDelimiter>> = RecordDelimiter::all()
            .iter()
            .map(|d| DelimiterItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let record_delimiter_index = record_delimiter_items
            .iter()
            .position(|i| i.value == initial_state.csv_options.record_delimiter);

        let record_delimiter_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(record_delimiter_items),
                record_delimiter_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let field_delimiter_items: Vec<DelimiterItem<FieldDelimiter>> = FieldDelimiter::all()
            .iter()
            .map(|d| DelimiterItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let field_delimiter_index = field_delimiter_items
            .iter()
            .position(|i| i.value == initial_state.csv_options.field_delimiter);

        let field_delimiter_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(field_delimiter_items),
                field_delimiter_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let text_qualifier_items: Vec<DelimiterItem<TextQualifier>> = TextQualifier::all()
            .iter()
            .map(|d| DelimiterItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let text_qualifier_index = text_qualifier_items
            .iter()
            .position(|i| i.value == initial_state.csv_options.text_qualifier);

        let text_qualifier_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(text_qualifier_items),
                text_qualifier_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let binary_encoding_items: Vec<DelimiterItem<BinaryEncoding>> = BinaryEncoding::all()
            .iter()
            .map(|d| DelimiterItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let binary_encoding_index = binary_encoding_items
            .iter()
            .position(|i| i.value == initial_state.csv_options.binary_encoding);

        let binary_encoding_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(binary_encoding_items),
                binary_encoding_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let date_order_items: Vec<DateOrderItem> = DateOrder::all()
            .iter()
            .map(|date_order| DateOrderItem {
                value: *date_order,
                label: date_order.display_name().into(),
            })
            .collect();
        let date_order_index = date_order_items
            .iter()
            .position(|item| item.value == initial_state.csv_options.date_order);
        let date_order_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(date_order_items),
                date_order_index.map(|i| zqlz_ui::widgets::IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        let decimal_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.csv_options.decimal_symbol.clone())
        });
        let date_delimiter_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.csv_options.date_delimiter.clone())
        });
        let time_delimiter_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.csv_options.time_delimiter.clone())
        });

        // Subscribe to select changes
        subscriptions.push(cx.subscribe(
            &format_select_state,
            |this, _, event: &SelectEvent<SearchableVec<FormatItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.export_format = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &encoding_state,
            |this, _, event: &SelectEvent<SearchableVec<EncodingItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.encoding = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &timestamp_state,
            |this, _, event: &SelectEvent<SearchableVec<TimestampItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.timestamp_format = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &table_select_state,
            |this, _, event: &SelectEvent<SearchableVec<TableItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.selected_table_index = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &record_delimiter_state,
            |this, _, event: &SelectEvent<SearchableVec<DelimiterItem<RecordDelimiter>>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.csv_options.record_delimiter = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &field_delimiter_state,
            |this, _, event: &SelectEvent<SearchableVec<DelimiterItem<FieldDelimiter>>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.csv_options.field_delimiter = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &text_qualifier_state,
            |this, _, event: &SelectEvent<SearchableVec<DelimiterItem<TextQualifier>>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.csv_options.text_qualifier = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &binary_encoding_state,
            |this, _, event: &SelectEvent<SearchableVec<DelimiterItem<BinaryEncoding>>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.csv_options.binary_encoding = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &date_order_state,
            |this, _, event: &SelectEvent<SearchableVec<DateOrderItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.csv_options.date_order = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &folder_input_state,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.output_folder = PathBuf::from(value.to_string());
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &decimal_input_state,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.csv_options.decimal_symbol = value.to_string();
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &date_delimiter_input_state,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.csv_options.date_delimiter = value.to_string();
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &time_delimiter_input_state,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.csv_options.time_delimiter = value.to_string();
                    cx.notify();
                }
            },
        ));

        Self {
            focus_handle: cx.focus_handle(),
            state: initial_state,
            connection,
            format_select_state,
            encoding_state,
            timestamp_state,
            folder_input_state,
            context_menu_row: None,
            table_select_state,
            record_delimiter_state,
            field_delimiter_state,
            text_qualifier_state,
            binary_encoding_state,
            date_order_state,
            decimal_input_state,
            date_delimiter_input_state,
            time_delimiter_input_state,
            table_scroll_handle: UniformListScrollHandle::new(),
            column_scroll_handle: UniformListScrollHandle::new(),
            log_scroll_handle: UniformListScrollHandle::new(),
            export_start_time: None,
            _subscriptions: subscriptions,
        }
    }

    /// Open the export wizard in a new window
    pub fn open(
        initial_state: ExportWizardState,
        connection: Option<Arc<dyn Connection>>,
        cx: &mut App,
    ) -> Task<anyhow::Result<ExportWizardHandle>> {
        let window_options = WindowOptions {
            titlebar: Some(TitleBar::title_bar_options()),
            window_bounds: Some(WindowBounds::centered(size(px(800.0), px(600.0)), cx)),
            window_min_size: Some(size(px(600.0), px(450.0))),
            kind: WindowKind::Normal,
            focus: true,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            let mut wizard_entity = None;
            let window_handle = cx.open_window(window_options, |window, cx| {
                window.activate_window();
                window.set_window_title("Export Wizard");

                let wizard = cx.new(|cx| ExportWizard::new(initial_state, connection, window, cx));
                wizard_entity = Some(wizard.clone());

                cx.new(|cx| Root::new(wizard, window, cx))
            })?;

            let wizard = wizard_entity
                .ok_or_else(|| anyhow::anyhow!("Export wizard window did not open"))?;

            Ok(ExportWizardHandle {
                wizard,
                window: window_handle.into(),
            })
        })
    }

    /// Get the current wizard state
    pub fn state(&self) -> &ExportWizardState {
        &self.state
    }

    /// Get mutable access to the wizard state
    pub fn state_mut(&mut self) -> &mut ExportWizardState {
        &mut self.state
    }

    /// Update tables list (called when schema is loaded)
    pub fn set_tables(
        &mut self,
        tables: Vec<TableExportConfig>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.state.tables = tables;
        self.state.tables_loading = false;
        self.state.tables_load_error = None;
        self.state.table_selection_validation_error = None;
        self.state.field_selection_validation_error = None;
        self.update_table_select(window, cx);
        cx.notify();
    }

    pub fn set_tables_load_error(&mut self, message: impl Into<String>, cx: &mut Context<Self>) {
        self.state.tables_loading = false;
        self.state.tables_load_error = Some(message.into());
        cx.notify();
    }

    /// Add a log message
    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>, cx: &mut Context<Self>) {
        self.state.add_log(level, message);
        cx.notify();
    }

    /// Update progress
    pub fn set_progress(&mut self, progress: f32, cx: &mut Context<Self>) {
        self.state.progress = progress;
        cx.notify();
    }

    /// Set export complete
    pub fn set_complete(&mut self, cx: &mut Context<Self>) {
        self.state.is_complete = true;
        self.state.is_exporting = false;
        cx.emit(ExportWizardEvent::ExportComplete);
        cx.notify();
    }

    fn update_table_select(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let table_items: Vec<TableItem> = self
            .state
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.selected)
            .map(|(i, t)| TableItem {
                index: i,
                name: t.table_name.clone().into(),
            })
            .collect();

        if !self
            .state
            .tables
            .get(self.state.selected_table_index)
            .is_some_and(|table| table.selected)
        {
            self.state.selected_table_index =
                table_items.first().map(|item| item.index).unwrap_or(0);
        }

        self.table_select_state.update(cx, |state, cx| {
            state.set_items(SearchableVec::new(table_items), window, cx);
        });
    }

    fn go_next(&mut self, cx: &mut Context<Self>) {
        if let Some(next) = self.state.current_step.next() {
            if !self.can_navigate_to_step(next) {
                cx.notify();
                return;
            }

            self.state.current_step = next;
            cx.notify();
        }
    }

    fn go_back(&mut self, cx: &mut Context<Self>) {
        if let Some(prev) = self.state.current_step.previous() {
            self.state.current_step = prev;
            cx.notify();
        }
    }

    fn go_to_step(&mut self, step: ExportWizardStep, cx: &mut Context<Self>) {
        if self.state.is_exporting {
            return;
        }

        if !self.can_navigate_to_step(step) {
            cx.notify();
            return;
        }

        self.state.current_step = step;
        cx.notify();
    }

    fn can_navigate_to_step(&mut self, target_step: ExportWizardStep) -> bool {
        if self.state.tables_loading {
            self.state.table_selection_validation_error =
                Some("Wait for table metadata to finish loading.".to_string());
            return false;
        }

        let steps = ExportWizardStep::all();
        let Some(current_position) = steps
            .iter()
            .position(|step| *step == self.state.current_step)
        else {
            return false;
        };
        let Some(target_position) = steps.iter().position(|step| *step == target_step) else {
            return false;
        };

        if target_position <= current_position {
            return true;
        }

        for step in &steps[current_position..target_position] {
            match step {
                ExportWizardStep::TableSelection => {
                    if !self.state.validate_table_selection() {
                        return false;
                    }
                }
                ExportWizardStep::FieldSelection => {
                    if !self.state.validate_field_selection() {
                        return false;
                    }
                }
                ExportWizardStep::FormatOptions | ExportWizardStep::Progress => {}
            }
        }

        true
    }

    fn blocked_navigation_tooltip(&self, target_step: ExportWizardStep) -> Option<SharedString> {
        if self.state.is_exporting {
            return Some("Step navigation is disabled while export is running".into());
        }

        if self.state.tables_loading {
            return Some("Table metadata is still loading".into());
        }

        let steps = ExportWizardStep::all();
        let current_step = self.state.current_step;

        let current_position = steps.iter().position(|step| *step == current_step)?;
        let target_position = steps.iter().position(|step| *step == target_step)?;

        if target_position <= current_position {
            return None;
        }

        for step in &steps[current_position..target_position] {
            match step {
                ExportWizardStep::TableSelection => {
                    if !self.state.tables.iter().any(|table| table.selected) {
                        return Some("Select at least one table before continuing".into());
                    }
                }
                ExportWizardStep::FieldSelection => {
                    let has_selected_column = self
                        .state
                        .current_table()
                        .map(|table| table.columns.iter().any(|column| column.selected))
                        .unwrap_or(false);

                    if !has_selected_column {
                        return Some("Select at least one field before continuing".into());
                    }
                }
                ExportWizardStep::FormatOptions | ExportWizardStep::Progress => {}
            }
        }

        None
    }

    /// Toggle selection for a specific table
    fn toggle_table_selection(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(table) = self.state.tables.get_mut(index) {
            table.selected = !table.selected;
            self.state.table_selection_validation_error = None;
            self.update_table_select(window, cx);
            cx.notify();
        }
    }

    /// Select all tables
    fn select_all_tables(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        for table in &mut self.state.tables {
            table.selected = true;
        }
        self.state.table_selection_validation_error = None;
        self.update_table_select(window, cx);
        cx.notify();
    }

    /// Deselect all tables
    fn deselect_all_tables(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        for table in &mut self.state.tables {
            table.selected = false;
        }
        self.state.table_selection_validation_error = None;
        self.update_table_select(window, cx);
        cx.notify();
    }

    /// Select only the specified table (deselect all others)
    fn select_only_table(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        for (i, table) in self.state.tables.iter_mut().enumerate() {
            table.selected = i == index;
        }
        self.state.table_selection_validation_error = None;
        self.update_table_select(window, cx);
        cx.notify();
    }

    /// Toggle column selection
    fn toggle_column_selection(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut()
            && let Some(col) = table.columns.get_mut(index)
        {
            col.selected = !col.selected;
        }
        self.state.field_selection_validation_error = None;
        cx.notify();
    }

    /// Select all columns in current table
    fn select_all_columns(&mut self, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            table.select_all_fields();
        }
        self.state.field_selection_validation_error = None;
        cx.notify();
    }

    /// Deselect all columns in current table
    fn deselect_all_columns(&mut self, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            table.deselect_all_fields();
        }
        self.state.field_selection_validation_error = None;
        cx.notify();
    }

    /// Toggle all columns (if all selected, deselect all; otherwise select all)
    fn toggle_all_columns(&mut self, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            if table.all_columns_selected() {
                table.deselect_all_fields();
            } else {
                table.select_all_fields();
            }
        }
        self.state.field_selection_validation_error = None;
        cx.notify();
    }

    /// Toggle append option
    fn toggle_append(&mut self, cx: &mut Context<Self>) {
        self.state.csv_options.append = !self.state.csv_options.append;
        cx.notify();
    }

    /// Toggle continue on error option
    fn toggle_continue_on_error(&mut self, cx: &mut Context<Self>) {
        self.state.csv_options.continue_on_error = !self.state.csv_options.continue_on_error;
        cx.notify();
    }

    /// Toggle include headers option
    fn toggle_include_headers(&mut self, cx: &mut Context<Self>) {
        self.state.csv_options.include_headers = !self.state.csv_options.include_headers;
        cx.notify();
    }

    /// Toggle blank if zero option
    fn toggle_blank_if_zero(&mut self, cx: &mut Context<Self>) {
        self.state.csv_options.blank_if_zero = !self.state.csv_options.blank_if_zero;
        cx.notify();
    }

    fn toggle_zero_padding_date(&mut self, cx: &mut Context<Self>) {
        self.state.csv_options.zero_padding_date = !self.state.csv_options.zero_padding_date;
        cx.notify();
    }

    /// Toggle schema-only export (excludes all data rows, exports DDL only)
    fn toggle_include_data(&mut self, cx: &mut Context<Self>) {
        self.state.include_data = !self.state.include_data;
        cx.notify();
    }

    fn toggle_include_sequences(&mut self, cx: &mut Context<Self>) {
        self.state.include_sequences = !self.state.include_sequences;
        cx.notify();
    }

    /// Returns paths of any output files that already exist on disk, so the
    /// caller can prompt before overwriting them.
    fn existing_output_paths(&self) -> Vec<PathBuf> {
        match self.state.export_format {
            ExportFormat::Udif | ExportFormat::UdifCompressed => {
                let path = self.state.output_path();
                if path.exists() { vec![path] } else { vec![] }
            }
            ExportFormat::Csv => self
                .state
                .selected_tables()
                .into_iter()
                .map(|t| self.state.output_folder.join(&t.output_filename))
                .filter(|p| p.exists())
                .collect(),
        }
    }

    fn start_export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(connection) = self.connection.clone() else {
            self.state
                .add_log(LogLevel::Error, "No database connection available");
            cx.emit(ExportWizardEvent::ExportFailed(
                "No database connection".to_string(),
            ));
            cx.notify();
            return;
        };

        // Create output folder if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&self.state.output_folder) {
            self.state.add_log(
                LogLevel::Error,
                format!("Failed to create output folder: {}", e),
            );
            cx.emit(ExportWizardEvent::ExportFailed(e.to_string()));
            cx.notify();
            return;
        }

        let existing = self.existing_output_paths();

        if existing.is_empty() {
            self.run_export(connection, cx);
        } else {
            // Ask for confirmation before overwriting existing files.
            let message = if existing.len() == 1 {
                format!(
                    "The file \"{}\" already exists. Overwrite it?",
                    existing[0].display()
                )
            } else {
                format!(
                    "{} output file(s) already exist. Overwrite them all?",
                    existing.len()
                )
            };

            let receiver = window.prompt(
                PromptLevel::Warning,
                &message,
                None,
                &["Overwrite", "Cancel"],
                cx,
            );

            let window_handle = window.window_handle();
            cx.spawn(async move |this, cx| {
                if let Ok(answer) = receiver.await {
                    // Index 0 → "Overwrite" was clicked; anything else means Cancel.
                    if answer == 0 {
                        _ = window_handle.update(cx, |_, _window, cx| {
                            _ = this.update(cx, |this, cx| {
                                this.run_export(connection, cx);
                            });
                        });
                    }
                }
                anyhow::Ok(())
            })
            .detach();
        }
    }

    /// Initiates the actual export after any overwrite confirmation has been
    /// obtained.  Separated from `start_export` so the prompt callback can
    /// call it without needing a `Window` reference.
    fn run_export(&mut self, connection: Arc<dyn Connection>, cx: &mut Context<Self>) {
        self.state.is_exporting = true;
        self.state.is_complete = false;
        self.state.progress = 0.0;
        self.state.log_messages.clear();
        self.state.stats = ExportStats::default();
        self.state.last_export_error = None;
        self.export_start_time = Some(Instant::now());

        let format_name = self.state.export_format.display_name();
        self.state.add_log(LogLevel::Info, "Export start");
        self.state
            .add_log(LogLevel::Info, format!("Export Format - {}", format_name));
        cx.emit(ExportWizardEvent::StartExport);
        cx.notify();

        let progress_snapshot = Arc::new(Mutex::new(ExportProgressSnapshot {
            total_objects: self.state.selected_tables().len(),
            ..Default::default()
        }));
        self.start_progress_ticker(progress_snapshot.clone(), cx);

        match self.state.export_format {
            ExportFormat::Udif | ExportFormat::UdifCompressed => {
                self.start_udif_export(connection, progress_snapshot, cx);
            }
            ExportFormat::Csv => {
                self.start_csv_export(connection, progress_snapshot, cx);
            }
        }
    }

    fn start_progress_ticker(
        &mut self,
        progress_snapshot: Arc<Mutex<ExportProgressSnapshot>>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            let mut last_message: Option<String> = None;

            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(EXPORT_PROGRESS_TICK_MS))
                    .await;

                let snapshot = match progress_snapshot.lock() {
                    Ok(snapshot) => snapshot.clone(),
                    Err(error) => {
                        tracing::warn!("Export progress snapshot lock poisoned: {}", error);
                        ExportProgressSnapshot::default()
                    }
                };

                let keep_running = this.update(cx, |this, cx| {
                    if !this.state.is_exporting {
                        return false;
                    }

                    if let Some(start) = this.export_start_time {
                        this.state.stats.elapsed_seconds = start.elapsed().as_secs_f64();
                    }

                    if !snapshot.current_object.is_empty() {
                        this.state.stats.current_object = snapshot.current_object.clone();
                    }

                    if snapshot.total_rows.is_some() {
                        this.state.stats.total_rows = snapshot.total_rows.unwrap_or(0);
                    } else if snapshot.total_objects > 0 {
                        this.state.stats.total_rows = snapshot.total_objects as u64;
                    }

                    this.state.stats.processed_rows = if snapshot.total_rows.is_some() {
                        snapshot.rows_exported
                    } else {
                        snapshot.completed_objects as u64
                    };

                    if let Some(message) = &snapshot.message
                        && last_message.as_ref() != Some(message)
                    {
                        this.state.add_log(snapshot.log_level, message.clone());
                        last_message = Some(message.clone());
                    }

                    let elapsed_progress = this
                        .export_start_time
                        .map(|start| (start.elapsed().as_secs_f32() / 12.0).min(0.90))
                        .unwrap_or(0.0);
                    let object_progress = if snapshot.total_objects > 0 {
                        let row_progress = snapshot
                            .total_rows
                            .filter(|total_rows| *total_rows > 0)
                            .map(|total_rows| {
                                (snapshot.rows_exported as f32 / total_rows as f32).clamp(0.0, 1.0)
                            })
                            .unwrap_or(0.0);
                        ((snapshot.completed_objects as f32 + row_progress)
                            / snapshot.total_objects as f32)
                            .clamp(0.0, 0.96)
                    } else {
                        0.0
                    };

                    this.state.progress = this
                        .state
                        .progress
                        .max(object_progress)
                        .max(elapsed_progress);
                    cx.notify();
                    true
                });

                if !matches!(keep_running, Ok(true)) {
                    break;
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn start_udif_export(
        &mut self,
        connection: Arc<dyn Connection>,
        progress_snapshot: Arc<Mutex<ExportProgressSnapshot>>,
        cx: &mut Context<Self>,
    ) {
        let export_state = self.state.clone();
        let driver_name = connection.driver_name().to_string();

        // Build ExportOptions from wizard state
        let mut options = ExportOptions {
            include_schema: export_state.include_schema,
            include_data: export_state.include_data,
            include_indexes: export_state.include_indexes,
            include_foreign_keys: export_state.include_foreign_keys,
            include_sequences: export_state.include_sequences,
            continue_on_error: export_state.csv_options.continue_on_error,
            include_tables: export_state
                .tables
                .iter()
                .filter(|t| t.selected)
                .map(|t| t.table_name.clone())
                .collect(),
            ..Default::default()
        };

        // Build include_columns map from selected columns
        for table in &export_state.tables {
            if table.selected {
                let selected_cols: Vec<String> = table
                    .columns
                    .iter()
                    .filter(|c| c.selected)
                    .map(|c| c.name.clone())
                    .collect();
                if !selected_cols.is_empty() && selected_cols.len() < table.columns.len() {
                    options
                        .include_columns
                        .insert(table.table_name.clone(), selected_cols);
                }
            }
        }

        let output_folder = export_state.output_folder.clone();
        let output_filename = export_state.output_filename.clone();
        let export_format = export_state.export_format;

        cx.spawn(async move |this, cx| {
            let exporter = GenericExporter::new(connection, &driver_name);

            // Create progress callback
            let progress_callback: crate::exporter::ExportProgressCallback =
                Box::new(move |progress: ExportProgress| {
                    if let Ok(mut snapshot) = progress_snapshot.lock() {
                        snapshot.current_object = progress.current_table.unwrap_or_default();
                        snapshot.total_objects = progress.total_tables;
                        snapshot.completed_objects = progress.tables_completed;
                        snapshot.rows_exported = progress.rows_exported;
                        snapshot.total_rows = progress.total_rows;
                        snapshot.message = progress.message;
                        snapshot.log_level = LogLevel::Info;
                    }
                });

            let result = exporter
                .export_database_with_progress(&options, progress_callback)
                .await;

            match result {
                Ok(doc) => {
                    // Determine output path
                    let extension = export_format.file_extension();
                    let output_path =
                        output_folder.join(format!("{}{}", output_filename, extension));

                    // Write the document to file
                    let write_result = match export_format {
                        ExportFormat::Udif => udif_helpers::to_json(&doc)
                            .map_err(|e| e.to_string())
                            .and_then(|json| {
                                std::fs::write(&output_path, json).map_err(|e| e.to_string())
                            }),
                        ExportFormat::UdifCompressed => udif_helpers::to_json_compressed(&doc)
                            .map_err(|e| e.to_string())
                            .and_then(|data| {
                                std::fs::write(&output_path, data).map_err(|e| e.to_string())
                            }),
                        ExportFormat::Csv => {
                            // Should not reach here, but handle gracefully
                            Err("CSV format should use start_csv_export".to_string())
                        }
                    };

                    match write_result {
                        Ok(()) => {
                            let total_rows = doc.total_rows();
                            _ = this.update(cx, |this, cx| {
                                this.state.add_log(
                                    LogLevel::Success,
                                    format!("Created: {}", output_path.display()),
                                );
                                this.state.is_exporting = false;
                                this.state.is_complete = true;
                                this.state.progress = 1.0;
                                this.state.output_file_path = Some(output_path);

                                if let Some(start) = this.export_start_time {
                                    this.state.stats.elapsed_seconds =
                                        start.elapsed().as_secs_f64();
                                }

                                this.state.stats.processed_rows = total_rows as u64;
                                this.state.add_log(
                                    LogLevel::Success,
                                    format!("Export complete. {} rows exported.", total_rows),
                                );

                                let selected_tables: Vec<String> = this
                                    .state
                                    .tables
                                    .iter()
                                    .filter(|t| t.selected)
                                    .map(|t| t.table_name.clone())
                                    .collect();
                                let source_label = if selected_tables.is_empty() {
                                    "all_tables".to_string()
                                } else {
                                    selected_tables.join("_")
                                };
                                let target_label = this.state.output_filename.clone();
                                if let Ok(path) = this.state.write_log_file(
                                    &driver_name,
                                    &source_label,
                                    &target_label,
                                ) {
                                    this.state.log_file_path = Some(path);
                                }

                                cx.emit(ExportWizardEvent::ExportComplete);
                                cx.notify();
                            });
                        }
                        Err(e) => {
                            let error = e.clone();
                            _ = this.update(cx, |this, cx| {
                                this.state.is_exporting = false;
                                this.state.last_export_error = Some(error.clone());
                                this.state.add_log(
                                    LogLevel::Error,
                                    format!("Failed to write file: {}", error),
                                );
                                cx.emit(ExportWizardEvent::ExportFailed(error));
                                cx.notify();
                            });
                        }
                    }
                }
                Err(e) => {
                    let error = e.to_string();
                    _ = this.update(cx, |this, cx| {
                        this.state.is_exporting = false;
                        this.state.last_export_error = Some(error.clone());
                        this.state
                            .add_log(LogLevel::Error, format!("Export failed: {}", error));
                        cx.emit(ExportWizardEvent::ExportFailed(error));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn start_csv_export(
        &mut self,
        connection: Arc<dyn Connection>,
        progress_snapshot: Arc<Mutex<ExportProgressSnapshot>>,
        cx: &mut Context<Self>,
    ) {
        let export_state = self.state.clone();
        let driver_name = connection.driver_name().to_string();

        // Use shared atomic counters for progress tracking from the callback
        let rows_exported = Arc::new(AtomicU64::new(0));
        let rows_exported_clone = rows_exported.clone();

        cx.spawn(async move |this, cx| {
            // Create progress callback that updates shared atomics
            let progress_callback: Box<dyn Fn(CsvExportProgress) + Send + Sync> =
                Box::new(move |progress: CsvExportProgress| {
                    rows_exported_clone.store(progress.rows_exported, Ordering::SeqCst);
                    if let Ok(mut snapshot) = progress_snapshot.lock() {
                        snapshot.current_object = progress.current_table;
                        snapshot.total_objects = progress.total_tables;
                        snapshot.completed_objects = progress.table_index.saturating_sub(1);
                        snapshot.rows_exported = progress.rows_exported;
                        snapshot.total_rows = progress.total_rows;
                        snapshot.message = Some(progress.message);
                        snapshot.log_level = progress.log_level;
                    }
                });

            let exporter = CsvExporter::new(connection, export_state)
                .with_progress_callback(progress_callback);

            match exporter.export().await {
                Ok(files) => {
                    _ = this.update(cx, |this, cx| {
                        for file in &files {
                            this.state
                                .add_log(LogLevel::Success, format!("Created: {}", file.display()));
                        }

                        this.state.is_exporting = false;
                        this.state.is_complete = true;
                        this.state.progress = 1.0;

                        if let Some(start) = this.export_start_time {
                            this.state.stats.elapsed_seconds = start.elapsed().as_secs_f64();
                        }

                        this.state.stats.processed_rows = rows_exported.load(Ordering::SeqCst);
                        this.state.add_log(
                            LogLevel::Success,
                            format!("Export complete. {} file(s) created.", files.len()),
                        );

                        let selected_tables: Vec<String> = this
                            .state
                            .tables
                            .iter()
                            .filter(|t| t.selected)
                            .map(|t| t.table_name.clone())
                            .collect();
                        let source_label = if selected_tables.is_empty() {
                            "all_tables".to_string()
                        } else {
                            selected_tables.join("_")
                        };
                        let target_label = this.state.output_filename.clone();
                        if let Ok(path) =
                            this.state
                                .write_log_file(&driver_name, &source_label, &target_label)
                        {
                            this.state.log_file_path = Some(path);
                        }

                        cx.emit(ExportWizardEvent::ExportComplete);
                        cx.notify();
                    });
                }
                Err(e) => {
                    let error = e.to_string();
                    _ = this.update(cx, |this, cx| {
                        this.state.is_exporting = false;
                        this.state.last_export_error = Some(error.clone());
                        this.state
                            .add_log(LogLevel::Error, format!("Export failed: {}", error));
                        cx.emit(ExportWizardEvent::ExportFailed(error));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn show_help(&self, window: &mut Window, cx: &mut Context<Self>) {
        let message = "Export wizard steps:\n\n1. Select tables and output folder.\n2. Choose fields for selected tables only.\n3. Set UDIF or CSV options.\n4. Start export and watch live progress.\n\nProfiles:\n- Save updates the currently opened/saved profile.\n- Save As... writes a new profile JSON.\n- Open... loads profile settings into the wizard.\n\nFallbacks:\n- Continue on error keeps exporting remaining tables when one table fails.\n- Retry No Sequences skips sequence current-value lookup.\n- Retry Data Only skips schema, indexes, foreign keys, and sequences.\n\nCSV options:\n- Delimiters control row, field, date, time, and decimal formatting.\n- Text qualifier wraps values and escapes matching quotes.\n- Zero padding date controls date sample style.\n\nProgress uses driver callbacks when row counts are known and estimated progress while long database operations are running.";
        let receiver = window.prompt(PromptLevel::Info, message, None, &["OK"], cx);
        cx.spawn(async move |_this, _cx| {
            let _ = receiver.await;
            anyhow::Ok(())
        })
        .detach();
    }

    fn copy_last_error(&mut self, cx: &mut Context<Self>) {
        if let Some(error) = self.state.last_export_error.clone() {
            cx.write_to_clipboard(ClipboardItem::new_string(error));
            self.state
                .add_log(LogLevel::Info, "Copied export error to clipboard");
            cx.notify();
        }
    }

    fn copy_log_messages(&mut self, cx: &mut Context<Self>) {
        let log = self
            .state
            .log_messages
            .iter()
            .map(ExportLogMessage::format)
            .collect::<Vec<_>>()
            .join("\n");
        if !log.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(log));
            self.state
                .add_log(LogLevel::Info, "Copied export log to clipboard");
            cx.notify();
        }
    }

    fn profile_dir() -> Option<PathBuf> {
        dirs::config_dir().map(|config_dir| config_dir.join("zqlz").join("export_profiles"))
    }

    fn retry_without_sequences(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.state.include_sequences = false;
        self.state.add_log(
            LogLevel::Warning,
            "Retrying without sequence current values",
        );
        self.start_export(window, cx);
    }

    fn retry_data_only(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.state.include_schema = false;
        self.state.include_indexes = false;
        self.state.include_foreign_keys = false;
        self.state.include_sequences = false;
        self.state.include_data = true;
        self.state
            .add_log(LogLevel::Warning, "Retrying as data-only export");
        self.start_export(window, cx);
    }

    fn default_profile_path(&self) -> Option<PathBuf> {
        let profile_name = self.state.current_profile_name.clone().unwrap_or_else(|| {
            let now = chrono::Local::now();
            format!("Export Profile {}", now.format("%Y-%m-%d %H:%M:%S"))
        });
        let filename = format!("{}.json", sanitize_profile_filename(&profile_name));
        Self::profile_dir().map(|profile_dir| profile_dir.join(filename))
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(profile_path) = self.state.current_profile_path.clone() {
            self.write_profile(profile_path, cx);
        } else {
            self.save_profile_as(window, cx);
        }
    }

    fn save_profile_as(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(default_path) = self.default_profile_path() else {
            self.state
                .add_log(LogLevel::Error, "Could not resolve config directory");
            cx.notify();
            return;
        };

        let receiver = cx.prompt_for_new_path(&default_path, None);
        cx.spawn(async move |this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => return anyhow::Ok(()),
            };

            this.update(cx, |this, cx| {
                this.write_profile(path, cx);
            })?;

            anyhow::Ok(())
        })
        .detach();
    }

    fn write_profile(&mut self, profile_path: PathBuf, cx: &mut Context<Self>) {
        let profile_name = profile_path
            .file_stem()
            .and_then(|name| name.to_str())
            .map(|name| name.replace('_', " "))
            .unwrap_or_else(|| "Export Profile".to_string());
        let profile = ExportProfile::from_state(profile_name.clone(), &self.state);

        let save_result = profile_path
            .parent()
            .ok_or_else(|| "Profile path has no parent directory".to_string())
            .and_then(|profile_dir| {
                std::fs::create_dir_all(profile_dir).map_err(|error| error.to_string())
            })
            .map_err(|error| error.to_string())
            .and_then(|()| {
                serde_json::to_string_pretty(&profile).map_err(|error| error.to_string())
            })
            .and_then(|json| {
                std::fs::write(&profile_path, json).map_err(|error| error.to_string())
            });

        match save_result {
            Ok(()) => {
                self.state.current_profile_path = Some(profile_path.clone());
                self.state.current_profile_name = Some(profile_name);
                self.state.add_log(
                    LogLevel::Success,
                    format!("Saved profile: {}", profile_path.display()),
                );
                cx.emit(ExportWizardEvent::SaveProfile(profile));
                cx.notify();
            }
            Err(error) => {
                self.state.add_log(
                    LogLevel::Error,
                    format!("Failed to save profile: {}", error),
                );
                cx.notify();
            }
        }
    }

    fn open_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let receiver = cx.prompt_for_paths(gpui::PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Open Export Profile".into()),
        });
        let window_handle = window.window_handle();

        cx.spawn(async move |this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(paths))) => match paths.first() {
                    Some(path) => path.clone(),
                    None => return anyhow::Ok(()),
                },
                _ => return anyhow::Ok(()),
            };

            let json = match std::fs::read_to_string(&path) {
                Ok(json) => json,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.state.add_log(
                            LogLevel::Error,
                            format!("Failed to read profile: {}", error),
                        );
                        cx.notify();
                    })?;
                    return anyhow::Ok(());
                }
            };

            let profile = match serde_json::from_str::<ExportProfile>(&json) {
                Ok(profile) => profile,
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.state.add_log(
                            LogLevel::Error,
                            format!("Failed to parse profile: {}", error),
                        );
                        cx.notify();
                    })?;
                    return anyhow::Ok(());
                }
            };

            window_handle.update(cx, |_, window, cx| {
                this.update(cx, |this, cx| {
                    this.apply_profile(profile, path, window, cx);
                })
            })??;

            anyhow::Ok(())
        })
        .detach();
    }

    fn apply_profile(
        &mut self,
        profile: ExportProfile,
        path: PathBuf,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.state.encoding = profile.encoding;
        self.state.add_timestamp = profile.add_timestamp;
        self.state.timestamp_format = profile.timestamp_format;
        self.state.custom_timestamp_format = profile.custom_timestamp_format;
        self.state.csv_options = profile.csv_options;
        self.state.current_profile_path = Some(path);
        self.state.current_profile_name = Some(profile.name.clone());

        self.encoding_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.encoding, window, cx);
        });
        self.timestamp_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.timestamp_format, window, cx);
        });
        self.record_delimiter_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.csv_options.record_delimiter, window, cx);
        });
        self.field_delimiter_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.csv_options.field_delimiter, window, cx);
        });
        self.text_qualifier_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.csv_options.text_qualifier, window, cx);
        });
        self.binary_encoding_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.csv_options.binary_encoding, window, cx);
        });
        self.date_order_state.update(cx, |state, cx| {
            state.set_selected_value(&self.state.csv_options.date_order, window, cx);
        });
        self.decimal_input_state.update(cx, |input, cx| {
            input.set_value(self.state.csv_options.decimal_symbol.clone(), window, cx);
        });
        self.date_delimiter_input_state.update(cx, |input, cx| {
            input.set_value(self.state.csv_options.date_delimiter.clone(), window, cx);
        });
        self.time_delimiter_input_state.update(cx, |input, cx| {
            input.set_value(self.state.csv_options.time_delimiter.clone(), window, cx);
        });

        self.state.add_log(
            LogLevel::Success,
            format!("Opened profile: {}", profile.name),
        );
        cx.notify();
    }

    fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        cx.emit(ExportWizardEvent::Close);
        window.remove_window();
    }

    // =========================================================================
    // Step Renderers
    // =========================================================================

    fn render_step_indicator(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let current = self.state.current_step;
        let can_switch_steps = !self.state.is_exporting;

        h_flex()
            .w_full()
            .gap_0()
            .pl_4()
            .border_b_1()
            .border_color(theme.border)
            .children(
                ExportWizardStep::all()
                    .iter()
                    .enumerate()
                    .map(|(visual_idx, step)| {
                        let is_current = *step == current;
                        let is_done = visual_idx < current.index();
                        let step = *step;
                        let blocked_reason = self.blocked_navigation_tooltip(step);

                        div()
                            .id(SharedString::from(format!(
                                "export-step-tab-{}",
                                step.index()
                            )))
                            .text_sm()
                            .px_3()
                            .py_2()
                            .border_b_2()
                            .when(can_switch_steps && blocked_reason.is_none(), |this| {
                                this.cursor_pointer().on_mouse_down(
                                    gpui::MouseButton::Left,
                                    cx.listener(move |this, _: &MouseDownEvent, _, cx| {
                                        this.go_to_step(step, cx);
                                    }),
                                )
                            })
                            .when(!can_switch_steps || blocked_reason.is_some(), |this| {
                                this.opacity(0.7)
                            })
                            .when_some(blocked_reason, |this, reason| {
                                this.tooltip(move |window, cx| {
                                    Tooltip::new(reason.clone()).build(window, cx)
                                })
                            })
                            .when(is_current, |s| {
                                s.border_color(theme.accent)
                                    .text_color(theme.accent)
                                    .font_weight(FontWeight::SEMIBOLD)
                            })
                            .when(is_done, |s| {
                                s.border_color(transparent_black())
                                    .text_color(theme.foreground)
                            })
                            .when(!is_current && !is_done, |s| {
                                s.border_color(transparent_black())
                                    .text_color(theme.muted_foreground)
                            })
                            .child(step.display_name())
                    }),
            )
    }

    fn render_step_1_table_selection(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let table_selection_validation_error = self.state.table_selection_validation_error.clone();
        let tables_load_error = self.state.tables_load_error.clone();
        let tables_loading = self.state.tables_loading;

        // Clone states needed for handlers
        let folder_input = self.folder_input_state.clone();
        let view = cx.entity().clone();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            // Description
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child("You can select the export file and define some additional options."),
            )
            // Folder row
            .child(
                h_flex()
                    .w_full()
                    .gap_3()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child("Default Folder:"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .child(Input::new(&self.folder_input_state).small()),
                    )
                    .child({
                        let folder_input = folder_input.clone();
                        let view = view.clone();
                        Button::new("change-folder")
                            .child("Change...")
                            .primary()
                            .small()
                            .on_click(move |_, window, cx| {
                                let folder_input = folder_input.clone();
                                let window_handle = window.window_handle();
                                let view = view.clone();
                                let receiver = cx.prompt_for_paths(gpui::PathPromptOptions {
                                    files: false,
                                    directories: true,
                                    multiple: false,
                                    prompt: Some("Select Export Folder".into()),
                                });

                                cx.spawn(async move |cx| {
                                    if let Ok(Ok(Some(paths))) = receiver.await
                                        && let Some(path) = paths.first()
                                    {
                                        let path_str = path.to_string_lossy().to_string();
                                        let path_buf = path.clone();
                                        _ = window_handle.update(cx, |_, window, cx| {
                                            folder_input.update(cx, |input, cx| {
                                                input.set_value(path_str, window, cx);
                                            });
                                        });
                                        // Also update the state
                                        view.update(cx, |this, cx| {
                                            this.state.output_folder = path_buf;
                                            cx.notify();
                                        });
                                    }
                                    anyhow::Ok(())
                                })
                                .detach();
                            })
                    }),
            )
            // Table list description
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("Select source tables and review generated export file names."),
            )
            // Table list
            .child(
                v_flex()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .bg(theme.background)
                    .overflow_hidden()
                    .child(
                        h_flex()
                            .w_full()
                            .h(px(30.0))
                            .px_2()
                            .gap_3()
                            .items_center()
                            .border_b_1()
                            .border_color(theme.border)
                            .bg(theme.secondary)
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(
                                div()
                                    .w(px(EXPORT_TABLE_CHECKBOX_WIDTH))
                                    .flex_shrink_0()
                                    .child("Use"),
                            )
                            .child(
                                div()
                                    .w(px(EXPORT_TABLE_NAME_WIDTH))
                                    .flex_shrink_0()
                                    .child("Table"),
                            )
                            .child(div().flex_1().child("Output file")),
                    )
                    .child(
                        div()
                            .flex_1()
                            .w_full()
                            .overflow_hidden()
                            .child(self.render_table_list(cx)),
                    ),
            )
            // Format row
            .child(
                h_flex()
                    .w_full()
                    .gap_3()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .w(px(80.0))
                            .flex_shrink_0()
                            .whitespace_nowrap()
                            .child("Format:"),
                    )
                    .child(Select::new(&self.format_select_state).small().w(px(280.0))),
            )
            // Encoding row (only shown for CSV format)
            .when(self.state.export_format == ExportFormat::Csv, |this| {
                this.child(
                    h_flex()
                        .w_full()
                        .gap_3()
                        .items_center()
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.foreground)
                                .w(px(80.0))
                                .flex_shrink_0()
                                .whitespace_nowrap()
                                .child("Encoding:"),
                        )
                        .child(Select::new(&self.encoding_state).small().w(px(280.0))),
                )
            })
            // Timestamp row
            .child(
                h_flex()
                    .w_full()
                    .gap_3()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .w(px(80.0))
                            .flex_shrink_0()
                            .whitespace_nowrap()
                            .child("Timestamp:"),
                    )
                    .child(Select::new(&self.timestamp_state).small().w(px(280.0))),
            )
            .when_some(table_selection_validation_error, |this, error| {
                this.child(
                    h_flex()
                        .gap_2()
                        .items_center()
                        .child(div().text_sm().text_color(theme.danger).child(error)),
                )
            })
            .when(tables_loading, |this| {
                this.child(
                    h_flex().gap_2().items_center().child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Loading table metadata..."),
                    ),
                )
            })
            .when_some(tables_load_error, |this, error| {
                this.child(
                    h_flex()
                        .gap_2()
                        .items_center()
                        .child(div().text_sm().text_color(theme.danger).child(error)),
                )
            })
    }

    fn render_table_list(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tables = Arc::new(self.state.tables.clone());
        let table_count = tables.len();
        let tables_loading = self.state.tables_loading;
        let tables_load_error = self.state.tables_load_error.clone();
        let view = cx.entity().clone();
        div()
            .w_full()
            .h_full()
            .overflow_hidden()
            // Single context menu on container
            .context_menu({
                let view_for_container_menu = view.clone();
                move |menu, window, _cx| {
                    let row_idx = view_for_container_menu.read(_cx).context_menu_row;

                    menu.when_some(row_idx, |menu, idx| {
                        menu.item(PopupMenuItem::new("Select Only This").on_click({
                            let view = view_for_container_menu.clone();
                            window.listener_for(&view, move |this, _, window, cx| {
                                this.select_only_table(idx, window, cx);
                            })
                        }))
                        .separator()
                    })
                    .item(PopupMenuItem::new("Select All").on_click({
                        let view = view_for_container_menu.clone();
                        window.listener_for(&view, |this, _, window, cx| {
                            this.select_all_tables(window, cx);
                        })
                    }))
                    .item(PopupMenuItem::new("Deselect All").on_click({
                        let view = view_for_container_menu.clone();
                        window.listener_for(&view, |this, _, window, cx| {
                            this.deselect_all_tables(window, cx);
                        })
                    }))
                }
            })
            .child(
                div()
                    .size_full()
                    .relative()
                    .overflow_hidden()
                    .when(table_count > 0, |this| {
                        let tables = tables.clone();
                        let view = view.clone();
                        this.child(
                            uniform_list(
                                "export-table-rows",
                                table_count,
                                cx.processor(
                                    move |_wizard,
                                          visible_range: Range<usize>,
                                          _window,
                                          cx| {
                                        let total_rows = tables.len();
                                        let start = visible_range.start.min(total_rows);
                                        let end = visible_range.end.min(total_rows);

                                        if visible_range.end > total_rows {
                                            tracing::debug!(
                                                ?visible_range,
                                                total_rows,
                                                "Export table list visible range exceeded available rows"
                                            );
                                        }

                                        let theme = cx.theme();

                                        (start..end)
                                            .filter_map(|index| {
                                                let table = tables.get(index)?;
                                                let table_name = table.table_name.clone();
                                                let output_filename = table.output_filename.clone();
                                                let selected = table.selected;
                                                let view_for_click = view.clone();
                                                let view_for_mouse = view.clone();

                                                Some(
                                                    h_flex()
                                                        .id(ElementId::Name(
                                                            format!("table-row-{}", index).into(),
                                                        ))
                                                        .w_full()
                                                        .h(px(EXPORT_TABLE_ROW_HEIGHT))
                                                        .px_2()
                                                        .gap_3()
                                                        .items_center()
                                                        .cursor_pointer()
                                                        .border_b_1()
                                                        .border_color(theme.border)
                                                        .hover(|style| style.bg(theme.list_active))
                                                        .on_mouse_down(
                                                            gpui::MouseButton::Right,
                                                            move |_, _, cx| {
                                                                view_for_mouse.update(cx, |this, cx| {
                                                                    this.context_menu_row = Some(index);
                                                                    cx.notify();
                                                                });
                                                            },
                                                        )
                                                        .on_click(move |_, window, cx| {
                                                            view_for_click.update(cx, |this, cx| {
                                                                this.toggle_table_selection(index, window, cx);
                                                            });
                                                        })
                                                        .child(
                                                            div()
                                                                .w(px(EXPORT_TABLE_CHECKBOX_WIDTH))
                                                                .flex_shrink_0()
                                                                .child(
                                                                    Checkbox::new(format!("table-{}", index))
                                                                        .checked(selected),
                                                                ),
                                                        )
                                                        .child(
                                                            div()
                                                                .w(px(EXPORT_TABLE_NAME_WIDTH))
                                                                .flex_shrink_0()
                                                                .overflow_hidden()
                                                                .whitespace_nowrap()
                                                                .text_ellipsis()
                                                                .text_sm()
                                                                .text_color(theme.foreground)
                                                                .child(table_name),
                                                        )
                                                        .child(
                                                            div()
                                                                .flex_1()
                                                                .min_w_0()
                                                                .overflow_hidden()
                                                                .whitespace_nowrap()
                                                                .text_ellipsis()
                                                                .text_sm()
                                                                .text_color(theme.muted_foreground)
                                                                .child(output_filename),
                                                        )
                                                        .into_any_element(),
                                                )
                                            })
                                            .collect::<Vec<_>>()
                                    },
                                ),
                            )
                            .flex_grow()
                            .size_full()
                            .pr(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                            .track_scroll(&self.table_scroll_handle)
                            .with_sizing_behavior(ListSizingBehavior::Auto)
                            .into_any_element(),
                        )
                    })
                    .when(table_count == 0, |this| {
                        this.child(
                            v_flex()
                                .size_full()
                                .justify_center()
                                .items_center()
                                .gap_2()
                                .child({
                                    let message = if tables_loading {
                                        "Loading table metadata..."
                                    } else if tables_load_error.is_some() {
                                        "Could not load table metadata"
                                    } else {
                                        "No tables available"
                                    };

                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child(message)
                                })
                                .when_some(tables_load_error, |this, error| {
                                    this.child(
                                        div()
                                            .max_w(px(520.0))
                                            .text_sm()
                                            .text_color(theme.danger)
                                            .child(error),
                                    )
                                }),
                        )
                    })
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .right_0()
                            .bottom_0()
                            .w(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                            .when(table_count > 0, |this| {
                                this.child(
                                    Scrollbar::vertical(&self.table_scroll_handle)
                                        .scrollbar_show(ScrollbarShow::Always),
                                )
                            }),
                    ),
            )
    }

    fn render_step_2_field_selection(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let field_selection_validation_error = self.state.field_selection_validation_error.clone();

        v_flex()
            .w_full()
            .h_full()
            .overflow_hidden()
            .gap_3()
            .p_4()
            // Description
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child("You can select the fields to export."),
            )
            // Source table dropdown
            .child(
                h_flex()
                    .w_full()
                    .gap_3()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .w(px(100.0))
                            .child("Source Table:"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .child(Select::new(&self.table_select_state).small()),
                    ),
            )
            // Column list
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .bg(theme.background)
                    .overflow_hidden()
                    .child(self.render_column_list(cx)),
            )
            // Select all / Deselect all buttons
            .child(
                h_flex()
                    .w_full()
                    .gap_3()
                    .items_center()
                    .child(
                        Button::new("select-all")
                            .child("Select All")
                            .ghost()
                            .small()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.select_all_columns(cx);
                            })),
                    )
                    .child(
                        Button::new("deselect-all")
                            .child("Deselect All")
                            .ghost()
                            .small()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.deselect_all_columns(cx);
                            })),
                    )
                    .child(
                        Checkbox::new("all-fields")
                            .checked(
                                self.state
                                    .current_table()
                                    .map(|t| t.all_columns_selected())
                                    .unwrap_or(true),
                            )
                            .label("All fields")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_all_columns(cx);
                            })),
                    ),
            )
            .when_some(field_selection_validation_error, |this, error| {
                this.child(
                    h_flex()
                        .gap_2()
                        .items_center()
                        .child(div().text_sm().text_color(theme.danger).child(error)),
                )
            })
    }

    fn render_column_list(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let columns = Arc::new(
            self.state
                .current_table()
                .map(|table| table.columns.clone())
                .unwrap_or_default(),
        );
        let column_count = columns.len();
        let view = cx.entity().clone();

        div()
            .size_full()
            .relative()
            .overflow_hidden()
            .when(column_count > 0, |this| {
                let columns = columns.clone();
                let view = view.clone();
                this.child(
                    uniform_list(
                        "export-column-rows",
                        column_count,
                        cx.processor(move |_wizard, visible_range: Range<usize>, _window, cx| {
                            let total_rows = columns.len();
                            let start = visible_range.start.min(total_rows);
                            let end = visible_range.end.min(total_rows);

                            if visible_range.end > total_rows {
                                tracing::debug!(
                                    ?visible_range,
                                    total_rows,
                                    "Export column list visible range exceeded available rows"
                                );
                            }

                            let theme = cx.theme();

                            (start..end)
                                .filter_map(|index| {
                                    let column = columns.get(index)?;
                                    let column_name = column.name.clone();
                                    let selected = column.selected;
                                    let view_for_click = view.clone();

                                    Some(
                                        h_flex()
                                            .id(ElementId::Name(
                                                format!("col-row-{}", index).into(),
                                            ))
                                            .w_full()
                                            .h(px(EXPORT_COLUMN_ROW_HEIGHT))
                                            .px_2()
                                            .gap_3()
                                            .items_center()
                                            .hover(|style| style.bg(theme.list_active))
                                            .cursor_pointer()
                                            .on_click(move |_, _, cx| {
                                                view_for_click.update(cx, |this, cx| {
                                                    this.toggle_column_selection(index, cx);
                                                });
                                            })
                                            .child(
                                                Checkbox::new(format!("col-{}", index))
                                                    .checked(selected),
                                            )
                                            .child(
                                                div()
                                                    .flex_1()
                                                    .text_sm()
                                                    .text_color(theme.foreground)
                                                    .child(column_name),
                                            )
                                            .into_any_element(),
                                    )
                                })
                                .collect::<Vec<_>>()
                        }),
                    )
                    .flex_grow()
                    .size_full()
                    .pr(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                    .track_scroll(&self.column_scroll_handle)
                    .with_sizing_behavior(ListSizingBehavior::Auto)
                    .into_any_element(),
                )
            })
            .when(column_count == 0, |this| {
                this.child(
                    v_flex().size_full().justify_center().items_center().child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("No columns available"),
                    ),
                )
            })
            .child(
                div()
                    .absolute()
                    .top_0()
                    .right_0()
                    .bottom_0()
                    .w(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                    .when(column_count > 0, |this| {
                        this.child(
                            Scrollbar::vertical(&self.column_scroll_handle)
                                .scrollbar_show(ScrollbarShow::Always),
                        )
                    }),
            )
    }

    fn render_step_3_csv_options(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let format_opts = &self.state.csv_options;
        let sample = self.csv_format_sample();

        v_flex()
            .w_full()
            .h_full()
            .overflow_y_scrollbar()
            .p_4()
            .child(
                v_flex()
                    .w_full()
                    .max_w(px(720.0))
                    .mx_auto()
                    .gap_4()
                    // Description
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child("You can define some additional options."),
                    )
                    // UDIF-only options: shown when the format is not CSV
                    .when(self.state.export_format != ExportFormat::Csv, |this| {
                        this.child(self.render_section_header("UDIF Options", cx))
                            .child(
                                v_flex()
                                    .w_full()
                                    .gap_2()
                                    .child(
                                        Checkbox::new("schema-only")
                                            // Checkbox is checked when data is *excluded* (schema-only)
                                            .checked(!self.state.include_data)
                                            .label("Schema only (no data)")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.toggle_include_data(cx);
                                            })),
                                    )
                                    .child(
                                        Checkbox::new("include-sequences")
                                            .checked(self.state.include_sequences)
                                            .label("Include sequence current values")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.toggle_include_sequences(cx);
                                            })),
                                    ),
                            )
                    })
                    // Append & Continue on error (CSV only)
                    .when(self.state.export_format == ExportFormat::Csv, |this| {
                        this.child(
                            v_flex()
                                .w_full()
                                .gap_2()
                                .child(
                                    Checkbox::new("append")
                                        .checked(format_opts.append)
                                        .label("Append")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.toggle_append(cx);
                                        })),
                                )
                                .child(
                                    Checkbox::new("continue-on-error")
                                        .checked(format_opts.continue_on_error)
                                        .label("Continue on error")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.toggle_continue_on_error(cx);
                                        })),
                                ),
                        )
                    })
                    // File Formats section
                    // File Formats section (CSV only)
                    .when(self.state.export_format == ExportFormat::Csv, |this| {
                        this.child(self.render_section_header("File Formats", cx))
                            .child(
                                v_flex()
                                    .w_full()
                                    .gap_2()
                                    .child(
                                        Checkbox::new("include-headers")
                                            .checked(format_opts.include_headers)
                                            .label("Include column titles")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.toggle_include_headers(cx);
                                            })),
                                    )
                                    .child(
                                        self.render_format_row(
                                            "Record Delimiter:",
                                            Select::new(&self.record_delimiter_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    )
                                    .child(
                                        self.render_format_row(
                                            "Field Delimiter:",
                                            Select::new(&self.field_delimiter_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    )
                                    .child(
                                        self.render_format_row(
                                            "Text Qualifier:",
                                            Select::new(&self.text_qualifier_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    ),
                            )
                            // Data Formats section
                            .child(self.render_section_header("Data Formats", cx))
                            .child(
                                v_flex()
                                    .w_full()
                                    .gap_2()
                                    .child(
                                        Checkbox::new("blank-if-zero")
                                            .checked(format_opts.blank_if_zero)
                                            .label("Blank if zero")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.toggle_blank_if_zero(cx);
                                            })),
                                    )
                                    .child(
                                        Checkbox::new("zero-padding-date")
                                            .checked(format_opts.zero_padding_date)
                                            .label("Zero padding date")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.toggle_zero_padding_date(cx);
                                            })),
                                    )
                                    .child(self.render_format_row(
                                        "Date Order:",
                                        Select::new(&self.date_order_state).small().w(px(180.0)),
                                        cx,
                                    ))
                                    .child(
                                        self.render_format_row(
                                            "Date Delimiter:",
                                            Input::new(&self.date_delimiter_input_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    )
                                    .child(
                                        self.render_format_row(
                                            "Time Delimiter:",
                                            Input::new(&self.time_delimiter_input_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    )
                                    .child(self.render_format_row(
                                        "Decimal Symbol:",
                                        Input::new(&self.decimal_input_state).small().w(px(180.0)),
                                        cx,
                                    ))
                                    .child(
                                        self.render_format_row(
                                            "Binary Data Encoding:",
                                            Select::new(&self.binary_encoding_state)
                                                .small()
                                                .w(px(180.0)),
                                            cx,
                                        ),
                                    )
                                    .child(
                                        div()
                                            .mt_2()
                                            .p_2()
                                            .border_1()
                                            .border_color(theme.border)
                                            .rounded_md()
                                            .bg(theme.secondary)
                                            .text_xs()
                                            .font_family(theme.mono_font_family.clone())
                                            .text_color(theme.foreground)
                                            .child(sample),
                                    ),
                            )
                    }),
            )
    }

    fn csv_format_sample(&self) -> String {
        let options = &self.state.csv_options;
        let date = match (options.date_order, options.zero_padding_date) {
            (DateOrder::Dmy, true) => format!(
                "24{}08{}2026",
                options.date_delimiter, options.date_delimiter
            ),
            (DateOrder::Dmy, false) => format!(
                "24{}8{}2026",
                options.date_delimiter, options.date_delimiter
            ),
            (DateOrder::Mdy, true) => format!(
                "08{}24{}2026",
                options.date_delimiter, options.date_delimiter
            ),
            (DateOrder::Mdy, false) => format!(
                "8{}24{}2026",
                options.date_delimiter, options.date_delimiter
            ),
            (DateOrder::Ymd, true) => format!(
                "2026{}08{}24",
                options.date_delimiter, options.date_delimiter
            ),
            (DateOrder::Ymd, false) => format!(
                "2026{}8{}24",
                options.date_delimiter, options.date_delimiter
            ),
        };
        let delimiter = options.field_delimiter.value();
        format!(
            "Sample: id{delimiter}name{delimiter}created_at\n1{delimiter}Ada{delimiter}{date} 14{}05{}09",
            options.time_delimiter, options.time_delimiter
        )
    }

    fn render_section_header(&self, title: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        div()
            .w_full()
            .py_2()
            .text_sm()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.foreground)
            .child(title.to_string())
    }

    fn render_format_row(
        &self,
        label: &str,
        control: impl IntoElement,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .gap_3()
            .items_center()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .w(px(130.0))
                    .flex_shrink_0()
                    .whitespace_nowrap()
                    .child(label.to_string()),
            )
            .child(control)
    }

    fn render_step_4_progress(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let stats = &self.state.stats;
        let messages = Arc::new(self.state.log_messages.clone());
        let message_count = messages.len();
        let last_error = self.state.last_export_error.clone();

        v_flex()
            .w_full()
            .h_full()
            .overflow_hidden()
            .gap_3()
            .p_4()
            // Description
            .child(
                div()
                    .text_sm()
                    .text_color(if last_error.is_some() {
                        theme.danger
                    } else {
                        theme.foreground
                    })
                    .child(if self.state.is_complete {
                        "Export completed successfully."
                    } else if last_error.is_some() {
                        "Export failed. Copy the error or retry with a fallback below."
                    } else if self.state.is_exporting {
                        "Exporting data..."
                    } else {
                        "We have gathered all information the wizard needs to export your data. Click the Start button to begin exporting."
                    }),
            )
            .when_some(last_error, |this, error| {
                this.child(
                    div()
                        .w_full()
                        .p_2()
                        .border_1()
                        .border_color(theme.danger)
                        .rounded_md()
                        .text_sm()
                        .text_color(theme.danger)
                        .child(error),
                )
            })
            // Statistics
            .child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .child(self.render_stat_row("Source Object:", &stats.current_object, cx))
                    .child(self.render_stat_row("Total:", &stats.total_rows.to_string(), cx))
                    .child(self.render_stat_row("Processed:", &stats.processed_rows.to_string(), cx))
                    .child(self.render_stat_row("Time:", &stats.elapsed_display(), cx)),
            )
            // Log area
            .child(
                v_flex()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .bg(theme.background)
                    .overflow_hidden()
                    .child(
                        div()
                            .size_full()
                            .relative()
                            .overflow_hidden()
                            .when(message_count > 0, |this| {
                                let messages = messages.clone();
                                this.child(
                                    uniform_list(
                                        "export-progress-log-rows",
                                        message_count,
                                        cx.processor(
                                            move |_wizard,
                                                  visible_range: Range<usize>,
                                                  _window,
                                                  cx| {
                                                let total_rows = messages.len();
                                                let start = visible_range.start.min(total_rows);
                                                let end = visible_range.end.min(total_rows);

                                                if visible_range.end > total_rows {
                                                    tracing::debug!(
                                                        ?visible_range,
                                                        total_rows,
                                                        "Export progress log visible range exceeded available rows"
                                                    );
                                                }

                                                let theme = cx.theme();

                                                (start..end)
                                                    .filter_map(|index| {
                                                        let message = messages.get(index)?;
                                                        let color = match message.level {
                                                            LogLevel::Error => theme.danger,
                                                            LogLevel::Warning => theme.warning,
                                                            LogLevel::Success => theme.success,
                                                            LogLevel::Info => theme.foreground,
                                                        };

                                                        Some(
                                                            h_flex()
                                                                .id(index)
                                                                .w_full()
                                                                .h(px(EXPORT_LOG_ROW_HEIGHT))
                                                                .px_2()
                                                                .items_center()
                                                                .child(
                                                                    div()
                                                                        .w_full()
                                                                        .text_xs()
                                                                        .font_family(
                                                                            theme
                                                                                .mono_font_family
                                                                                .clone(),
                                                                        )
                                                                        .text_color(color)
                                                                        .child(message.format()),
                                                                )
                                                                .into_any_element(),
                                                        )
                                                    })
                                                    .collect::<Vec<_>>()
                                            },
                                        ),
                                    )
                                    .flex_grow()
                                    .size_full()
                                    .pr(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                                    .track_scroll(&self.log_scroll_handle)
                                    .with_sizing_behavior(ListSizingBehavior::Auto)
                                    .into_any_element(),
                                )
                            })
                            .when(message_count == 0, |this| {
                                this.child(
                                    v_flex()
                                        .size_full()
                                        .justify_center()
                                        .items_center()
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.muted_foreground)
                                                .child("No log messages yet"),
                                        ),
                                )
                            })
                            .child(
                                div()
                                    .absolute()
                                    .top_0()
                                    .right_0()
                                    .bottom_0()
                                    .w(px(EXPORT_LIST_SCROLLBAR_WIDTH))
                                    .when(message_count > 0, |this| {
                                        this.child(
                                            Scrollbar::vertical(&self.log_scroll_handle)
                                                .scrollbar_show(ScrollbarShow::Always),
                                        )
                                    }),
                            ),
                    ),
            )
            // Progress bar
            .child(self.render_progress_bar(cx))
    }

    fn render_stat_row(&self, label: &str, value: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .gap_3()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .w(px(100.0))
                    .text_right()
                    .child(label.to_string()),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child(value.to_string()),
            )
    }

    fn render_progress_bar(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let progress = self.state.progress;

        div()
            .w_full()
            .h(px(8.0))
            .bg(theme.muted)
            .rounded_full()
            .overflow_hidden()
            .child(
                div()
                    .h_full()
                    .w(relative(progress))
                    .bg(theme.primary)
                    .rounded_full(),
            )
    }
}

impl Render for ExportWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let step = self.state.current_step;
        let is_exporting = self.state.is_exporting;
        let is_complete = self.state.is_complete;
        let tables_loading = self.state.tables_loading;

        // Render step content first (before borrowing theme)
        // This allows mutable borrows for step renderers that need cx.listener
        let step_content = match step {
            ExportWizardStep::TableSelection => {
                self.render_step_1_table_selection(cx).into_any_element()
            }
            ExportWizardStep::FieldSelection => {
                self.render_step_2_field_selection(cx).into_any_element()
            }
            ExportWizardStep::FormatOptions => {
                self.render_step_3_csv_options(cx).into_any_element()
            }
            ExportWizardStep::Progress => self.render_step_4_progress(cx).into_any_element(),
        };

        // Now get theme for the rest
        let theme = cx.theme();

        // Button handlers
        let back_handler = cx.listener(|this, _: &ClickEvent, _, cx| {
            this.go_back(cx);
        });

        let next_handler = cx.listener(|this, _: &ClickEvent, _, cx| {
            this.go_next(cx);
        });

        let start_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.start_export(window, cx);
        });

        let help_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.show_help(window, cx);
        });

        let save_profile_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.save_profile(window, cx);
        });

        let save_profile_as_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.save_profile_as(window, cx);
        });

        let open_profile_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.open_profile(window, cx);
        });

        let copy_error_handler = cx.listener(|this, _: &ClickEvent, _, cx| {
            this.copy_last_error(cx);
        });

        let copy_log_handler = cx.listener(|this, _: &ClickEvent, _, cx| {
            this.copy_log_messages(cx);
        });

        let retry_without_sequences_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.retry_without_sequences(window, cx);
        });

        let retry_data_only_handler = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.retry_data_only(window, cx);
        });

        // Separate handlers needed for conditional buttons (can't clone listeners)
        let close_handler_for_finish = cx.listener(|this, _: &ClickEvent, window, cx| {
            this.close(window, cx);
        });

        let open_folder_handler = {
            let output_folder = self.state.output_folder.clone();
            cx.listener(move |_: &mut Self, _: &ClickEvent, _, cx| {
                cx.emit(ExportWizardEvent::OpenFolder(output_folder.clone()));
            })
        };

        v_flex()
            .w_full()
            .h_full()
            .bg(theme.background)
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &menu::Cancel, window, cx| {
                this.close(window, cx);
            }))
            // Title bar reserves space for the macOS traffic light buttons and provides a drag region
            .child(TitleBar::new())
            // Step indicator at the top
            .child(self.render_step_indicator(cx))
            // Main content area
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(step_content),
            )
            // Footer with buttons
            .child(
                h_flex()
                    .w_full()
                    .px_4()
                    .py_3()
                    .gap_3()
                    .items_center()
                    .justify_between()
                    .border_t_1()
                    .border_color(theme.border)
                    // Left side: Help and Save Profile
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new("help")
                                    .child("?")
                                    .ghost()
                                    .small()
                                    .on_click(help_handler),
                            )
                            .child(
                                Button::new("save-profile")
                                    .child("Save")
                                    .ghost()
                                    .small()
                                    .disabled(is_exporting)
                                    .on_click(save_profile_handler),
                            )
                            .child(
                                Button::new("save-profile-as")
                                    .child("Save As...")
                                    .ghost()
                                    .small()
                                    .disabled(is_exporting)
                                    .on_click(save_profile_as_handler),
                            )
                            .child(
                                Button::new("open-profile")
                                    .child("Open...")
                                    .ghost()
                                    .small()
                                    .disabled(is_exporting)
                                    .on_click(open_profile_handler),
                            )
                            .when_some(self.state.current_profile_name.clone(), |this, name| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .max_w(px(180.0))
                                        .overflow_hidden()
                                        .whitespace_nowrap()
                                        .text_ellipsis()
                                        .child(name),
                                )
                            }),
                    )
                    // Right side: Navigation buttons
                    .child(
                        h_flex()
                            .gap_2()
                            .when(
                                step == ExportWizardStep::Progress
                                    && self.state.last_export_error.is_some(),
                                |this| {
                                    this.child(
                                        Button::new("copy-error")
                                            .child("Copy Error")
                                            .ghost()
                                            .small()
                                            .on_click(copy_error_handler),
                                    )
                                    .child(
                                        Button::new("copy-log")
                                            .child("Copy Log")
                                            .ghost()
                                            .small()
                                            .on_click(copy_log_handler),
                                    )
                                    .child(
                                        Button::new("retry-no-sequences")
                                            .child("Retry No Sequences")
                                            .ghost()
                                            .small()
                                            .on_click(retry_without_sequences_handler),
                                    )
                                    .child(
                                        Button::new("retry-data-only")
                                            .child("Retry Data Only")
                                            .ghost()
                                            .small()
                                            .on_click(retry_data_only_handler),
                                    )
                                },
                            )
                            .when(step == ExportWizardStep::Progress && is_complete, |this| {
                                this.child(
                                    Button::new("open-folder")
                                        .child("Open")
                                        .ghost()
                                        .small()
                                        .on_click(open_folder_handler),
                                )
                            })
                            .when(step == ExportWizardStep::Progress && is_complete, |this| {
                                let log_path = self.state.log_file_path.clone();
                                this.child(
                                    Button::new("view-log")
                                        .child("View Log")
                                        .ghost()
                                        .small()
                                        .when_some(log_path, |button, path| {
                                            button.on_click(cx.listener(
                                                move |_this, _: &ClickEvent, _, cx| {
                                                    cx.open_url(&format!(
                                                        "file://{}",
                                                        path.display()
                                                    ));
                                                },
                                            ))
                                        }),
                                )
                            })
                            .child(
                                Button::new("back")
                                    .child("Back")
                                    .ghost()
                                    .small()
                                    .disabled(!step.can_go_back() || is_exporting)
                                    .on_click(back_handler),
                            )
                            .child(
                                Button::new("next")
                                    .child("Next")
                                    .primary()
                                    .small()
                                    .disabled(!step.can_go_next() || is_exporting || tables_loading)
                                    .on_click(next_handler),
                            )
                            .when(step == ExportWizardStep::Progress, |this| {
                                if is_complete {
                                    this.child(
                                        Button::new("close-btn")
                                            .child("Close")
                                            .small()
                                            .primary()
                                            .on_click(close_handler_for_finish),
                                    )
                                } else {
                                    this.child(
                                        Button::new("start")
                                            .child("Start")
                                            .small()
                                            .primary()
                                            .disabled(is_exporting || tables_loading)
                                            .on_click(start_handler),
                                    )
                                }
                            })
                            .when(step != ExportWizardStep::Progress, |this| {
                                this.child(
                                    Button::new("start-disabled")
                                        .child("Start")
                                        .small()
                                        .primary()
                                        .disabled(true),
                                )
                            }),
                    ),
            )
    }
}

impl Focusable for ExportWizard {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

// Action for closing
mod menu {
    use gpui::actions;
    actions!(export_wizard, [Cancel]);
}
