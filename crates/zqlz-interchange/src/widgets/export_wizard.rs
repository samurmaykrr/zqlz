//! Export Wizard Widget
//!
//! A multi-step wizard for exporting database tables to files.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use zqlz_core::Connection;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Root, Sizable,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{ContextMenuExt, PopupMenuItem},
    scroll::ScrollableElement,
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    title_bar::TitleBar,
    v_flex,
};

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
    decimal_input_state: Entity<InputState>,

    // Scroll state for table list and log
    scroll_handle: ScrollHandle,
    log_scroll_handle: ScrollHandle,

    /// Export start time for elapsed calculation
    export_start_time: Option<Instant>,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ExportWizardEvent> for ExportWizard {}

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

        let decimal_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.csv_options.decimal_symbol.clone())
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
            decimal_input_state,
            scroll_handle: ScrollHandle::new(),
            log_scroll_handle: ScrollHandle::new(),
            export_start_time: None,
            _subscriptions: subscriptions,
        }
    }

    /// Open the export wizard in a new window
    pub fn open(
        initial_state: ExportWizardState,
        connection: Option<Arc<dyn Connection>>,
        cx: &mut App,
    ) {
        let window_options = WindowOptions {
            titlebar: Some(TitleBar::title_bar_options()),
            window_bounds: Some(WindowBounds::centered(size(px(800.0), px(600.0)), cx)),
            window_min_size: Some(size(px(600.0), px(450.0))),
            kind: WindowKind::Normal,
            focus: true,
            ..Default::default()
        };

        cx.spawn(async move |cx| {
            cx.open_window(window_options, |window, cx| {
                window.activate_window();
                window.set_window_title("Export Wizard");

                let wizard = cx.new(|cx| {
                    ExportWizard::new(initial_state, connection, window, cx)
                });

                cx.new(|cx| Root::new(wizard, window, cx))
            })?;

            Ok::<_, anyhow::Error>(())
        })
        .detach();
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
        self.update_table_select(window, cx);
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

        self.table_select_state.update(cx, |state, cx| {
            state.set_items(SearchableVec::new(table_items), window, cx);
        });
    }

    fn go_next(&mut self, cx: &mut Context<Self>) {
        if let Some(next) = self.state.current_step.next() {
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

    /// Toggle selection for a specific table
    fn toggle_table_selection(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(table) = self.state.tables.get_mut(index) {
            table.selected = !table.selected;
            cx.notify();
        }
    }

    /// Select all tables
    fn select_all_tables(&mut self, cx: &mut Context<Self>) {
        for table in &mut self.state.tables {
            table.selected = true;
        }
        cx.notify();
    }

    /// Deselect all tables
    fn deselect_all_tables(&mut self, cx: &mut Context<Self>) {
        for table in &mut self.state.tables {
            table.selected = false;
        }
        cx.notify();
    }

    /// Select only the specified table (deselect all others)
    fn select_only_table(&mut self, index: usize, cx: &mut Context<Self>) {
        for (i, table) in self.state.tables.iter_mut().enumerate() {
            table.selected = i == index;
        }
        cx.notify();
    }

    /// Toggle column selection
    fn toggle_column_selection(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            if let Some(col) = table.columns.get_mut(index) {
                col.selected = !col.selected;
            }
        }
        cx.notify();
    }

    /// Select all columns in current table
    fn select_all_columns(&mut self, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            table.select_all_fields();
        }
        cx.notify();
    }

    /// Deselect all columns in current table
    fn deselect_all_columns(&mut self, cx: &mut Context<Self>) {
        if let Some(table) = self.state.current_table_mut() {
            table.deselect_all_fields();
        }
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

    fn start_export(&mut self, cx: &mut Context<Self>) {
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

        self.state.is_exporting = true;
        self.state.is_complete = false;
        self.state.progress = 0.0;
        self.state.log_messages.clear();
        self.export_start_time = Some(Instant::now());

        let format_name = self.state.export_format.display_name();
        self.state.add_log(LogLevel::Info, "Export start");
        self.state
            .add_log(LogLevel::Info, format!("Export Format - {}", format_name));
        cx.emit(ExportWizardEvent::StartExport);
        cx.notify();

        match self.state.export_format {
            ExportFormat::Udif | ExportFormat::UdifCompressed => {
                self.start_udif_export(connection, cx);
            }
            ExportFormat::Csv => {
                self.start_csv_export(connection, cx);
            }
        }
    }

    fn start_udif_export(&mut self, connection: Arc<dyn Connection>, cx: &mut Context<Self>) {
        let export_state = self.state.clone();
        let driver_name = connection.driver_name().to_string();

        // Build ExportOptions from wizard state
        let mut options = ExportOptions::default();
        options.include_schema = export_state.include_schema;
        options.include_indexes = export_state.include_indexes;
        options.include_foreign_keys = export_state.include_foreign_keys;

        // Add selected tables
        options.include_tables = export_state
            .tables
            .iter()
            .filter(|t| t.selected)
            .map(|t| t.table_name.clone())
            .collect();

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

        // Use shared atomic for progress tracking
        let rows_exported = Arc::new(AtomicU64::new(0));
        let rows_exported_clone = rows_exported.clone();

        cx.spawn(async move |this, cx| {
            let exporter = GenericExporter::new(connection, &driver_name);

            // Create progress callback
            let progress_callback: crate::exporter::ExportProgressCallback =
                Box::new(move |progress: ExportProgress| {
                    rows_exported_clone.store(progress.rows_exported, Ordering::SeqCst);
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

                                cx.emit(ExportWizardEvent::ExportComplete);
                                cx.notify();
                            });
                        }
                        Err(e) => {
                            _ = this.update(cx, |this, cx| {
                                this.state.is_exporting = false;
                                this.state.add_log(
                                    LogLevel::Error,
                                    format!("Failed to write file: {}", e),
                                );
                                cx.emit(ExportWizardEvent::ExportFailed(e));
                                cx.notify();
                            });
                        }
                    }
                }
                Err(e) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_exporting = false;
                        this.state
                            .add_log(LogLevel::Error, format!("Export failed: {}", e));
                        cx.emit(ExportWizardEvent::ExportFailed(e.to_string()));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn start_csv_export(&mut self, connection: Arc<dyn Connection>, cx: &mut Context<Self>) {
        let export_state = self.state.clone();

        // Use shared atomic counters for progress tracking from the callback
        let rows_exported = Arc::new(AtomicU64::new(0));
        let current_table_idx = Arc::new(AtomicU64::new(0));
        let rows_exported_clone = rows_exported.clone();
        let current_table_idx_clone = current_table_idx.clone();

        cx.spawn(async move |this, cx| {
            // Create progress callback that updates shared atomics
            let progress_callback: Box<dyn Fn(CsvExportProgress) + Send + Sync> =
                Box::new(move |progress: CsvExportProgress| {
                    rows_exported_clone.store(progress.rows_exported, Ordering::SeqCst);
                    current_table_idx_clone.store(progress.table_index as u64, Ordering::SeqCst);
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

                        cx.emit(ExportWizardEvent::ExportComplete);
                        cx.notify();
                    });
                }
                Err(e) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_exporting = false;
                        this.state
                            .add_log(LogLevel::Error, format!("Export failed: {}", e));
                        cx.emit(ExportWizardEvent::ExportFailed(e.to_string()));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        cx.emit(ExportWizardEvent::Close);
        window.remove_window();
    }

    // =========================================================================
    // Step Renderers
    // =========================================================================

    fn render_step_1_table_selection(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

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
                                    if let Ok(Ok(Some(paths))) = receiver.await {
                                        if let Some(path) = paths.first() {
                                            let path_str = path.to_string_lossy().to_string();
                                            let path_buf = path.clone();
                                            _ = window_handle.update(cx, |_, window, cx| {
                                                folder_input.update(cx, |input, cx| {
                                                    input.set_value(path_str, window, cx);
                                                });
                                            });
                                            // Also update the state
                                            _ = view.update(cx, |this, cx| {
                                                this.state.output_folder = path_buf;
                                                cx.notify();
                                            });
                                        }
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
                    .child("You can specify the export file(s) name."),
            )
            // Table list
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .bg(theme.background)
                    .overflow_hidden()
                    .child(self.render_table_list(cx)),
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
                    .child(
                        Select::new(&self.format_select_state)
                            .small()
                            .w(px(280.0))
                            .menu_width(px(280.0)),
                    ),
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
                        .child(
                            Select::new(&self.encoding_state)
                                .small()
                                .w(px(280.0))
                                .menu_width(px(280.0)),
                        ),
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
                    .child(
                        Select::new(&self.timestamp_state)
                            .small()
                            .w(px(280.0))
                            .menu_width(px(280.0)),
                    ),
            )
    }

    fn render_table_list(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tables = self.state.tables.clone();
        let view = cx.entity().clone();
        let context_menu_row = self.context_menu_row;

        // Single context menu on container that uses the tracked row index
        let view_for_container_menu = view.clone();

        div()
            .w_full()
            .h_full()
            .overflow_y_scrollbar()
            // Single context menu on container
            .context_menu({
                move |menu, window, _cx| {
                    let row_idx = context_menu_row;

                    menu.when_some(row_idx, |menu, idx| {
                        menu.item(
                            PopupMenuItem::new("Select Only This").on_click({
                                let view = view_for_container_menu.clone();
                                window.listener_for(&view, move |this, _, _, cx| {
                                    this.select_only_table(idx, cx);
                                })
                            }),
                        )
                        .separator()
                    })
                    .item(
                        PopupMenuItem::new("Select All").on_click({
                            let view = view_for_container_menu.clone();
                            window.listener_for(&view, |this, _, _, cx| {
                                this.select_all_tables(cx);
                            })
                        }),
                    )
                    .item(
                        PopupMenuItem::new("Deselect All").on_click({
                            let view = view_for_container_menu.clone();
                            window.listener_for(&view, |this, _, _, cx| {
                                this.deselect_all_tables(cx);
                            })
                        }),
                    )
                }
            })
            .child(
                v_flex()
                    .w_full()
                    .children(tables.iter().enumerate().map(|(idx, table)| {
                        let table_name = table.table_name.clone();
                        let output_filename = table.output_filename.clone();
                        let selected = table.selected;
                        let view_for_click = view.clone();
                        let view_for_mouse = view.clone();

                        h_flex()
                            .id(ElementId::Name(format!("table-row-{}", idx).into()))
                            .w_full()
                            .px_2()
                            .py_1()
                            .gap_3()
                            .items_center()
                            .cursor_pointer()
                            .hover(|s| s.bg(theme.list_active))
                            // Track which row was right-clicked
                            .on_mouse_down(gpui::MouseButton::Right, move |_, _, cx| {
                                view_for_mouse.update(cx, |this, cx| {
                                    this.context_menu_row = Some(idx);
                                    cx.notify();
                                });
                            })
                            // Click on row toggles selection
                            .on_click(move |_, _, cx| {
                                view_for_click.update(cx, |this, cx| {
                                    this.toggle_table_selection(idx, cx);
                                });
                            })
                            .child(Checkbox::new(format!("table-{}", idx)).checked(selected))
                            .child(
                                div()
                                    .w(px(180.0))
                                    .text_sm()
                                    .text_color(theme.foreground)
                                    .child(table_name),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .text_sm()
                                    .text_color(theme.muted_foreground)
                                    .child(output_filename),
                            )
                    })),
            )
    }

    fn render_step_2_field_selection(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

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
                            .small()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.select_all_columns(cx);
                            })),
                    )
                    .child(
                        Button::new("deselect-all")
                            .child("Deselect All")
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
    }

    fn render_column_list(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let columns = self
            .state
            .current_table()
            .map(|t| t.columns.clone())
            .unwrap_or_default();
        let view = cx.entity().clone();

        v_flex()
            .w_full()
            .children(columns.iter().enumerate().map(|(idx, col)| {
                let col_name = col.name.clone();
                let selected = col.selected;
                let view_for_click = view.clone();

                h_flex()
                    .id(ElementId::Name(format!("col-row-{}", idx).into()))
                    .w_full()
                    .px_2()
                    .py_1()
                    .gap_3()
                    .items_center()
                    .hover(|s| s.bg(theme.list_active))
                    .cursor_pointer()
                    .on_click(move |_, _, cx| {
                        view_for_click.update(cx, |this, cx| {
                            this.toggle_column_selection(idx, cx);
                        });
                    })
                    .child(Checkbox::new(format!("col-{}", idx)).checked(selected))
                    .child(
                        div()
                            .flex_1()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(col_name),
                    )
            }))
    }

    fn render_step_3_csv_options(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let format_opts = &self.state.csv_options;

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
                    .child("You can define some additional options."),
            )
            // Append & Continue on error
            .child(
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
            // File Formats section
            .child(self.render_section_header("File Formats", cx))
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
                    .child(self.render_format_row(
                        "Record Delimiter:",
                        Select::new(&self.record_delimiter_state)
                            .small()
                            .w(px(180.0))
                            .menu_width(px(180.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Field Delimiter:",
                        Select::new(&self.field_delimiter_state)
                            .small()
                            .w(px(180.0))
                            .menu_width(px(180.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Text Qualifier:",
                        Select::new(&self.text_qualifier_state)
                            .small()
                            .w(px(180.0))
                            .menu_width(px(180.0)),
                        cx,
                    )),
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
                    .child(self.render_format_row(
                        "Decimal Symbol:",
                        Input::new(&self.decimal_input_state).small().w(px(80.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Binary Data Encoding:",
                        Select::new(&self.binary_encoding_state)
                            .small()
                            .w(px(180.0))
                            .menu_width(px(180.0)),
                        cx,
                    )),
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
                    .child(if self.state.is_complete {
                        "Export completed successfully."
                    } else if self.state.is_exporting {
                        "Exporting data..."
                    } else {
                        "We have gathered all information the wizard needs to export your data. Click the Start button to begin exporting."
                    }),
            )
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
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .bg(theme.background)
                    .overflow_hidden()
                    .child(self.render_log_area(cx)),
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

    fn render_log_area(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let messages = self.state.log_messages.clone();

        div().w_full().h_full().p_2().overflow_y_scrollbar().child(
            v_flex()
                .w_full()
                .gap_0p5()
                .children(messages.iter().map(|msg| {
                    div()
                        .w_full()
                        .text_xs()
                        .font_family("monospace")
                        .text_color(match msg.level {
                            LogLevel::Error => theme.danger,
                            LogLevel::Warning => theme.warning,
                            LogLevel::Success => theme.success,
                            LogLevel::Info => theme.foreground,
                        })
                        .child(msg.format())
                })),
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

        let start_handler = cx.listener(|this, _: &ClickEvent, _, cx| {
            this.start_export(cx);
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
                    .bg(theme.title_bar)
                    // Left side: Help and Save Profile
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Button::new("help").child("?").small())
                            .child(Button::new("save-profile").child("Save Profile").small()),
                    )
                    // Right side: Navigation buttons
                    .child(
                        h_flex()
                            .gap_2()
                            .when(step == ExportWizardStep::Progress && is_complete, |this| {
                                this.child(
                                    Button::new("open-folder")
                                        .child("Open")
                                        .small()
                                        .on_click(open_folder_handler),
                                )
                            })
                            .child(
                                Button::new("back")
                                    .child("Back")
                                    .small()
                                    .disabled(!step.can_go_back() || is_exporting)
                                    .on_click(back_handler),
                            )
                            .child(
                                Button::new("next")
                                    .child("Next")
                                    .small()
                                    .disabled(!step.can_go_next() || is_exporting)
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
                                            .disabled(is_exporting)
                                            .on_click(start_handler),
                                    )
                                }
                            })
                            .when(step != ExportWizardStep::Progress, |this| {
                                this.child(
                                    Button::new("start-disabled")
                                        .child("Start")
                                        .small()
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
