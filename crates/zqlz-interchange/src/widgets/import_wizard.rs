//! Import Wizard Widget
//!
//! A multi-step wizard for importing data from files into database tables.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use zqlz_core::Connection;
use zqlz_ui::widgets::{
    ActiveTheme, IndexPath, Sizable,
    button::{Button, ButtonVariants},
    checkbox::Checkbox,
    h_flex,
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement,
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
    v_flex,
};

use super::types::*;
use crate::{
    CsvImporter,
    importer::{GenericImporter, Importer, helpers as udif_helpers},
};

/// Events emitted by the import wizard
#[derive(Clone, Debug)]
pub enum ImportWizardEvent {
    /// User requested to close the wizard
    Close,
    /// User requested to start the import
    StartImport,
    /// Import completed
    ImportComplete,
    /// Import failed with error message
    ImportFailed(String),
    /// User wants to add files
    AddFiles,
    /// User wants to add URL
    AddUrl(String),
    /// User wants to view the log file
    ViewLog(PathBuf),
    /// User wants to save a profile
    SaveProfile(ImportProfile),
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

/// Select item for date order dropdown
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

/// Select item for date time order dropdown
#[derive(Clone, Debug)]
struct DateTimeOrderItem {
    value: DateTimeOrder,
    label: SharedString,
}

impl SelectItem for DateTimeOrderItem {
    type Value = DateTimeOrder;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Select item for source file dropdown
#[derive(Clone, Debug)]
struct SourceItem {
    index: usize,
    name: SharedString,
}

impl SelectItem for SourceItem {
    type Value = usize;

    fn title(&self) -> SharedString {
        self.name.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.index
    }
}

/// Select item for import mode dropdown
#[derive(Clone, Debug)]
struct ImportModeItem {
    value: ImportMode,
    label: SharedString,
}

impl SelectItem for ImportModeItem {
    type Value = ImportMode;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Select item for binary encoding dropdown
#[derive(Clone, Debug)]
struct BinaryEncodingItem {
    value: BinaryEncoding,
    label: SharedString,
}

impl SelectItem for BinaryEncodingItem {
    type Value = BinaryEncoding;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }
}

/// Import Wizard Panel
pub struct ImportWizard {
    focus_handle: FocusHandle,
    state: ImportWizardState,

    /// Database connection for executing import
    connection: Option<Arc<dyn Connection>>,

    // Step 1: File Source
    encoding_state: Entity<SelectState<SearchableVec<EncodingItem>>>,

    // Step 2: Source Format
    date_order_state: Entity<SelectState<SearchableVec<DateOrderItem>>>,
    date_time_order_state: Entity<SelectState<SearchableVec<DateTimeOrderItem>>>,
    binary_encoding_state: Entity<SelectState<SearchableVec<BinaryEncodingItem>>>,
    field_name_row_input: Entity<InputState>,
    data_row_start_input: Entity<InputState>,
    data_row_end_input: Entity<InputState>,
    date_delimiter_input: Entity<InputState>,
    time_delimiter_input: Entity<InputState>,
    decimal_input_state: Entity<InputState>,

    // Step 3: Target Table
    source_select_state: Entity<SelectState<SearchableVec<SourceItem>>>,

    // Step 5: Import Mode
    import_mode_state: Entity<SelectState<SearchableVec<ImportModeItem>>>,

    // Scroll handles
    scroll_handle: ScrollHandle,
    log_scroll_handle: ScrollHandle,

    /// Import start time for elapsed calculation
    import_start_time: Option<Instant>,

    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ImportWizardEvent> for ImportWizard {}

impl ImportWizard {
    pub fn new(
        initial_state: ImportWizardState,
        connection: Option<Arc<dyn Connection>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut subscriptions = Vec::new();

        // Build encoding items
        let encoding_items: Vec<EncodingItem> = FileEncoding::all()
            .iter()
            .map(|e| EncodingItem {
                value: *e,
                label: e.display_name().into(),
            })
            .collect();
        let encoding_idx = encoding_items
            .iter()
            .position(|i| i.value == initial_state.encoding);
        let encoding_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(encoding_items),
                encoding_idx.map(|i| IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build date order items
        let date_order_items: Vec<DateOrderItem> = DateOrder::all()
            .iter()
            .map(|d| DateOrderItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let date_order_idx = date_order_items
            .iter()
            .position(|i| i.value == initial_state.source_format.date_order);
        let date_order_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(date_order_items),
                date_order_idx.map(|i| IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build date time order items
        let date_time_order_items: Vec<DateTimeOrderItem> = DateTimeOrder::all()
            .iter()
            .map(|d| DateTimeOrderItem {
                value: *d,
                label: d.display_name().into(),
            })
            .collect();
        let date_time_order_idx = date_time_order_items
            .iter()
            .position(|i| i.value == initial_state.source_format.date_time_order);
        let date_time_order_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(date_time_order_items),
                date_time_order_idx.map(|i| IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build binary encoding items
        let binary_items: Vec<BinaryEncodingItem> = BinaryEncoding::all()
            .iter()
            .map(|b| BinaryEncodingItem {
                value: *b,
                label: b.display_name().into(),
            })
            .collect();
        let binary_idx = binary_items
            .iter()
            .position(|i| i.value == initial_state.source_format.binary_encoding);
        let binary_encoding_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(binary_items),
                binary_idx.map(|i| IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Build source select items
        let source_items: Vec<SourceItem> = initial_state
            .sources
            .iter()
            .enumerate()
            .map(|(idx, s)| SourceItem {
                index: idx,
                name: s.source_name.clone().into(),
            })
            .collect();
        let source_idx = if source_items.is_empty() {
            None
        } else {
            Some(IndexPath::default().row(0))
        };
        let source_select_state =
            cx.new(|cx| SelectState::new(SearchableVec::new(source_items), source_idx, window, cx));

        // Build import mode items
        let mode_items: Vec<ImportModeItem> = ImportMode::all()
            .iter()
            .map(|m| ImportModeItem {
                value: *m,
                label: m.short_name().into(),
            })
            .collect();
        let mode_idx = mode_items
            .iter()
            .position(|i| i.value == initial_state.import_mode);
        let import_mode_state = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(mode_items),
                mode_idx.map(|i| IndexPath::default().row(i)),
                window,
                cx,
            )
        });

        // Input states
        let field_name_row_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.source_format.field_name_row.to_string())
        });
        let data_row_start_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.source_format.data_row_start.to_string())
        });
        let data_row_end_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(
                    initial_state
                        .source_format
                        .data_row_end
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                )
                .placeholder("End of File")
        });
        let date_delimiter_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.source_format.date_delimiter.clone())
        });
        let time_delimiter_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.source_format.time_delimiter.clone())
        });
        let decimal_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(initial_state.source_format.decimal_symbol.clone())
        });

        // Subscribe to select events
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
            &date_order_state,
            |this, _, event: &SelectEvent<SearchableVec<DateOrderItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.source_format.date_order = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &date_time_order_state,
            |this, _, event: &SelectEvent<SearchableVec<DateTimeOrderItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.source_format.date_time_order = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &binary_encoding_state,
            |this, _, event: &SelectEvent<SearchableVec<BinaryEncodingItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.source_format.binary_encoding = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &source_select_state,
            |this, _, event: &SelectEvent<SearchableVec<SourceItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.selected_mapping_index = *value;
                    cx.notify();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &import_mode_state,
            |this, _, event: &SelectEvent<SearchableVec<ImportModeItem>>, cx| {
                if let SelectEvent::Confirm(Some(value)) = event {
                    this.state.import_mode = *value;
                    cx.notify();
                }
            },
        ));

        // Subscribe to input events
        subscriptions.push(cx.subscribe(
            &field_name_row_input,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    if let Ok(row) = value.parse::<usize>() {
                        this.state.source_format.field_name_row = row;
                    }
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &data_row_start_input,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    if let Ok(row) = value.parse::<usize>() {
                        this.state.source_format.data_row_start = row;
                    }
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &data_row_end_input,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.source_format.data_row_end = value.parse::<usize>().ok();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &date_delimiter_input,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.source_format.date_delimiter = value.to_string();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &time_delimiter_input,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.source_format.time_delimiter = value.to_string();
                }
            },
        ));

        subscriptions.push(cx.subscribe(
            &decimal_input_state,
            |this, state, event: &InputEvent, cx| {
                if let InputEvent::Change = event {
                    let value = state.read(cx).value();
                    this.state.source_format.decimal_symbol = value.to_string();
                }
            },
        ));

        Self {
            focus_handle: cx.focus_handle(),
            state: initial_state,
            connection,
            encoding_state,
            date_order_state,
            date_time_order_state,
            binary_encoding_state,
            field_name_row_input,
            data_row_start_input,
            data_row_end_input,
            date_delimiter_input,
            time_delimiter_input,
            decimal_input_state,
            source_select_state,
            import_mode_state,
            scroll_handle: ScrollHandle::new(),
            log_scroll_handle: ScrollHandle::new(),
            import_start_time: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn state(&self) -> &ImportWizardState {
        &self.state
    }

    pub fn set_sources(
        &mut self,
        sources: Vec<ImportSource>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.state.sources = sources;

        // Detect format from first file
        if let Some(source) = self.state.sources.first() {
            if let Some(path) = source.path() {
                self.state.detected_format = Some(ImportFormat::from_path(path));
            }
        }

        self.update_source_select(window, cx);

        // If this is a UDIF file, load preview
        if self.state.is_udif_import() {
            self.load_udif_preview(cx);
        }
    }

    /// Add a single file to the import sources
    pub fn add_file(&mut self, path: PathBuf, window: &mut Window, cx: &mut Context<Self>) {
        self.state.add_file(path);
        self.update_source_select(window, cx);

        // If this is a UDIF file, load preview
        if self.state.is_udif_import() {
            self.load_udif_preview(cx);
        }

        cx.notify();
    }

    fn update_source_select(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let source_items: Vec<SourceItem> = self
            .state
            .sources
            .iter()
            .enumerate()
            .map(|(idx, s)| SourceItem {
                index: idx,
                name: s.source_name.clone().into(),
            })
            .collect();

        self.source_select_state.update(cx, |state, cx| {
            state.set_items(SearchableVec::new(source_items), window, cx);
        });
    }

    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>, cx: &mut Context<Self>) {
        self.state.add_log(level, message);
        cx.notify();
    }

    pub fn set_progress(&mut self, progress: f32, cx: &mut Context<Self>) {
        self.state.progress = progress;
        cx.notify();
    }

    pub fn set_complete(&mut self, cx: &mut Context<Self>) {
        self.state.is_complete = true;
        self.state.is_importing = false;
        cx.notify();
    }

    /// Load UDIF document for preview (called when UDIF file is added)
    pub fn load_udif_preview(&mut self, cx: &mut Context<Self>) {
        if !self.state.is_udif_import() {
            return;
        }

        // Get the first source file
        let source_path = match self.state.sources.first() {
            Some(source) => match &source.source_type {
                ImportSourceType::File(path) => path.clone(),
                ImportSourceType::Url(_) => return, // Can't preview URLs yet
            },
            None => return,
        };

        let is_compressed = matches!(
            self.state.detected_format,
            Some(ImportFormat::UdifCompressed)
        );

        // Load in background to avoid blocking UI
        cx.spawn({
            let source_path = source_path.clone();
            async move |this, cx| {
                let doc_result = if is_compressed {
                    match std::fs::read(&source_path) {
                        Ok(data) => udif_helpers::from_json_compressed(&data).ok(),
                        Err(_) => None,
                    }
                } else {
                    match std::fs::read_to_string(&source_path) {
                        Ok(json) => udif_helpers::from_json(&json).ok(),
                        Err(_) => None,
                    }
                };

                _ = this.update(cx, |this, cx| {
                    this.state.udif_document = doc_result;
                    cx.notify();
                });

                anyhow::Ok(())
            }
        })
        .detach();
    }

    fn go_next(&mut self, cx: &mut Context<Self>) {
        let is_udif = self.state.is_udif_import();
        if let Some(next) = self.state.current_step.next_for_format(is_udif) {
            self.state.current_step = next;
            cx.notify();
        }
    }

    fn go_back(&mut self, cx: &mut Context<Self>) {
        let is_udif = self.state.is_udif_import();
        if let Some(prev) = self.state.current_step.previous_for_format(is_udif) {
            self.state.current_step = prev;
            cx.notify();
        }
    }

    fn start_import(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.connection.clone() else {
            self.state
                .add_log(LogLevel::Error, "No database connection available");
            cx.emit(ImportWizardEvent::ImportFailed(
                "No database connection".to_string(),
            ));
            cx.notify();
            return;
        };

        if self.state.sources.is_empty() {
            self.state
                .add_log(LogLevel::Error, "No files selected for import");
            cx.emit(ImportWizardEvent::ImportFailed(
                "No files selected".to_string(),
            ));
            cx.notify();
            return;
        }

        self.state.is_importing = true;
        self.state.is_complete = false;
        self.state.progress = 0.0;
        self.state.log_messages.clear();
        self.import_start_time = Some(Instant::now());

        let format_name = match self.state.detected_format {
            Some(ImportFormat::Udif) | Some(ImportFormat::UdifCompressed) => "UDIF",
            Some(ImportFormat::Csv) | Some(ImportFormat::Unknown) | None => "CSV",
        };
        self.state.add_log(LogLevel::Info, "Import started");
        self.state
            .add_log(LogLevel::Info, format!("Format: {}", format_name));
        cx.emit(ImportWizardEvent::StartImport);
        cx.notify();

        match self.state.detected_format {
            Some(ImportFormat::Udif) | Some(ImportFormat::UdifCompressed) => {
                self.start_udif_import(connection, cx);
            }
            Some(ImportFormat::Csv) | Some(ImportFormat::Unknown) | None => {
                self.start_csv_import(connection, cx);
            }
        }
    }

    fn start_udif_import(&mut self, connection: Arc<dyn Connection>, cx: &mut Context<Self>) {
        let import_state = self.state.clone();

        cx.spawn(async move |this, cx| {
            // Get the first source file path
            let source_path = match import_state.sources.first() {
                Some(source) => match &source.source_type {
                    ImportSourceType::File(path) => path.clone(),
                    ImportSourceType::Url(_) => {
                        _ = this.update(cx, |this, cx| {
                            this.state.is_importing = false;
                            this.state
                                .add_log(LogLevel::Error, "UDIF import from URL not yet supported");
                            cx.emit(ImportWizardEvent::ImportFailed(
                                "UDIF import from URL not yet supported".to_string(),
                            ));
                            cx.notify();
                        });
                        return anyhow::Ok(());
                    }
                },
                None => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state
                            .add_log(LogLevel::Error, "No source file specified");
                        cx.emit(ImportWizardEvent::ImportFailed(
                            "No source file".to_string(),
                        ));
                        cx.notify();
                    });
                    return anyhow::Ok(());
                }
            };

            // Load the UDIF document from file
            let doc_result = match import_state.detected_format {
                Some(ImportFormat::UdifCompressed) => match std::fs::read(&source_path) {
                    Ok(data) => udif_helpers::from_json_compressed(&data),
                    Err(e) => Err(crate::importer::ImportError::DecodingError(e.to_string())),
                },
                _ => match std::fs::read_to_string(&source_path) {
                    Ok(json) => udif_helpers::from_json(&json),
                    Err(e) => Err(crate::importer::ImportError::DecodingError(e.to_string())),
                },
            };

            let doc = match doc_result {
                Ok(doc) => doc,
                Err(e) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state
                            .add_log(LogLevel::Error, format!("Failed to parse UDIF file: {}", e));
                        cx.emit(ImportWizardEvent::ImportFailed(e.to_string()));
                        cx.notify();
                    });
                    return anyhow::Ok(());
                }
            };

            // Build import options from wizard state (uses to_import_options which includes UDIF settings)
            let options = import_state.to_import_options();

            // Perform the import
            let importer = GenericImporter::new(connection);
            let result = importer.import(&doc, &options).await;

            match result {
                Ok(result) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state.is_complete = true;
                        this.state.progress = 1.0;

                        if let Some(start) = this.import_start_time {
                            this.state.stats.elapsed_seconds = start.elapsed().as_secs_f64();
                        }

                        let total_rows = result.total_rows();
                        this.state.stats.processed = total_rows;
                        this.state.stats.added = total_rows;
                        this.state.stats.errors = result.errors.len() as u64;

                        if result.has_errors() {
                            this.state.add_log(
                                LogLevel::Warning,
                                format!(
                                    "Import complete with {} errors. {} rows imported.",
                                    result.errors.len(),
                                    total_rows
                                ),
                            );
                            for err in result.errors.iter().take(10) {
                                this.state.add_log(LogLevel::Error, err.clone());
                            }
                            if result.errors.len() > 10 {
                                this.state.add_log(
                                    LogLevel::Warning,
                                    format!("... and {} more errors", result.errors.len() - 10),
                                );
                            }
                        } else {
                            this.state.add_log(
                                LogLevel::Success,
                                format!(
                                    "Import complete. {} tables created, {} rows imported.",
                                    result.tables_created, total_rows
                                ),
                            );
                        }

                        // Log any warnings
                        for warning in &result.warnings {
                            this.state
                                .add_log(LogLevel::Warning, warning.message.clone());
                        }

                        cx.emit(ImportWizardEvent::ImportComplete);
                        cx.notify();
                    });
                }
                Err(e) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state
                            .add_log(LogLevel::Error, format!("Import failed: {}", e));
                        cx.emit(ImportWizardEvent::ImportFailed(e.to_string()));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn start_csv_import(&mut self, connection: Arc<dyn Connection>, cx: &mut Context<Self>) {
        let import_state = self.state.clone();

        cx.spawn(async move |this, cx| {
            let importer = CsvImporter::new(connection, import_state);

            match importer.import().await {
                Ok(result) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state.is_complete = true;
                        this.state.progress = 1.0;

                        if let Some(start) = this.import_start_time {
                            this.state.stats.elapsed_seconds = start.elapsed().as_secs_f64();
                        }

                        this.state.stats.processed = result.rows_processed;
                        this.state.stats.added = result.rows_added;
                        this.state.stats.updated = result.rows_updated;
                        this.state.stats.deleted = result.rows_deleted;
                        this.state.stats.errors = result.error_count;

                        if result.error_count > 0 {
                            this.state.add_log(
                                LogLevel::Warning,
                                format!(
                                    "Import complete with {} errors. {} added, {} updated.",
                                    result.error_count, result.rows_added, result.rows_updated
                                ),
                            );
                            for err in result.errors.iter().take(10) {
                                this.state.add_log(LogLevel::Error, err.clone());
                            }
                            if result.errors.len() > 10 {
                                this.state.add_log(
                                    LogLevel::Warning,
                                    format!("... and {} more errors", result.errors.len() - 10),
                                );
                            }
                        } else {
                            this.state.add_log(
                                LogLevel::Success,
                                format!(
                                    "Import complete. {} added, {} updated.",
                                    result.rows_added, result.rows_updated
                                ),
                            );
                        }

                        cx.emit(ImportWizardEvent::ImportComplete);
                        cx.notify();
                    });
                }
                Err(e) => {
                    _ = this.update(cx, |this, cx| {
                        this.state.is_importing = false;
                        this.state
                            .add_log(LogLevel::Error, format!("Import failed: {}", e));
                        cx.emit(ImportWizardEvent::ImportFailed(e.to_string()));
                        cx.notify();
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
    }

    fn close(&mut self, cx: &mut Context<Self>) {
        cx.emit(ImportWizardEvent::Close);
    }

    fn render_step_indicator(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let current = self.state.current_step;
        let is_udif = self.state.is_udif_import();
        let steps = ImportWizardStep::all_for_format(is_udif);

        h_flex()
            .w_full()
            .px_4()
            .py_2()
            .gap_2()
            .border_b_1()
            .border_color(theme.border)
            .children(steps.iter().enumerate().map(|(visual_idx, step)| {
                let is_current = *step == current;
                // For determining "done", we need to check position in the steps list
                let current_pos = steps.iter().position(|s| *s == current).unwrap_or(0);
                let is_done = visual_idx < current_pos;

                div()
                    .text_xs()
                    .px_2()
                    .py_1()
                    .rounded_sm()
                    .when(is_current, |s| {
                        s.bg(theme.primary).text_color(theme.primary_foreground)
                    })
                    .when(is_done, |s| s.text_color(theme.success))
                    .when(!is_current && !is_done, |s| {
                        s.text_color(theme.muted_foreground)
                    })
                    .child(step.display_name())
            }))
    }

    fn render_section_header(&self, title: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        div()
            .text_sm()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.foreground)
            .py_2()
            .child(title.to_string())
    }

    fn render_format_row(
        &self,
        label: &str,
        control: impl IntoElement,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let label = label.to_string();
        h_flex()
            .w_full()
            .gap_3()
            .items_center()
            .child(
                div()
                    .w(px(140.0))
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child(label),
            )
            .child(div().w(px(120.0)).child(control))
    }

    /// Render UDIF document preview showing source info, tables, and statistics
    fn render_udif_preview(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        match &self.state.udif_document {
            Some(doc) => {
                let source = &doc.source;
                let total_tables = doc.schema.tables.len();
                let total_rows = doc.total_rows();
                let total_indexes: usize =
                    doc.schema.tables.values().map(|t| t.indexes.len()).sum();
                let total_fks: usize = doc
                    .schema
                    .tables
                    .values()
                    .map(|t| t.foreign_keys.len())
                    .sum();

                // Sort tables by name for consistent display
                let mut tables: Vec<_> = doc.schema.tables.values().collect();
                tables.sort_by(|a, b| a.name.cmp(&b.name));

                v_flex()
                    .w_full()
                    .gap_2()
                    // Header
                    .child(
                        h_flex()
                            .w_full()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.foreground)
                                    .child("UDIF Document Preview"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(format!("v{}", doc.version)),
                            ),
                    )
                    // Source info
                    .child(
                        div().w_full().p_2().bg(theme.secondary).rounded_md().child(
                            h_flex()
                                .w_full()
                                .gap_4()
                                .child(
                                    v_flex()
                                        .gap_0p5()
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child("Source Driver"),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.foreground)
                                                .child(source.driver.clone()),
                                        ),
                                )
                                .when_some(source.database.clone(), |this, db| {
                                    this.child(
                                        v_flex()
                                            .gap_0p5()
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child("Database"),
                                            )
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(theme.foreground)
                                                    .child(db),
                                            ),
                                    )
                                })
                                .child(
                                    v_flex()
                                        .gap_0p5()
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child("Exported"),
                                        )
                                        .child(div().text_sm().text_color(theme.foreground).child(
                                            doc.exported_at.format("%Y-%m-%d %H:%M").to_string(),
                                        )),
                                ),
                        ),
                    )
                    // Statistics row
                    .child(
                        h_flex()
                            .w_full()
                            .gap_4()
                            .child(self.render_stat_badge(
                                "Tables",
                                total_tables,
                                theme.primary,
                                cx,
                            ))
                            .child(self.render_stat_badge("Rows", total_rows, theme.success, cx))
                            .child(self.render_stat_badge(
                                "Indexes",
                                total_indexes,
                                theme.warning,
                                cx,
                            ))
                            .child(self.render_stat_badge(
                                "Foreign Keys",
                                total_fks,
                                theme.info,
                                cx,
                            )),
                    )
                    // Table list
                    .child(
                        div()
                            .w_full()
                            .max_h(px(200.0))
                            .border_1()
                            .border_color(theme.border)
                            .rounded_md()
                            .overflow_y_scrollbar()
                            .child(
                                v_flex()
                                    .w_full()
                                    // Header row
                                    .child(
                                        h_flex()
                                            .w_full()
                                            .px_2()
                                            .py_1()
                                            .bg(theme.secondary)
                                            .child(
                                                div()
                                                    .w(px(180.0))
                                                    .text_xs()
                                                    .font_weight(FontWeight::SEMIBOLD)
                                                    .text_color(theme.muted_foreground)
                                                    .child("Table"),
                                            )
                                            .child(
                                                div()
                                                    .w(px(80.0))
                                                    .text_xs()
                                                    .font_weight(FontWeight::SEMIBOLD)
                                                    .text_color(theme.muted_foreground)
                                                    .child("Columns"),
                                            )
                                            .child(
                                                div()
                                                    .w(px(80.0))
                                                    .text_xs()
                                                    .font_weight(FontWeight::SEMIBOLD)
                                                    .text_color(theme.muted_foreground)
                                                    .child("Rows"),
                                            )
                                            .child(
                                                div()
                                                    .flex_1()
                                                    .text_xs()
                                                    .font_weight(FontWeight::SEMIBOLD)
                                                    .text_color(theme.muted_foreground)
                                                    .child("Primary Key"),
                                            ),
                                    )
                                    // Table rows
                                    .children(tables.iter().map(|table| {
                                        let row_count = doc
                                            .data
                                            .get(&table.name)
                                            .map(|d| d.rows.len())
                                            .unwrap_or(0);
                                        let pk_cols = table
                                            .primary_key
                                            .as_ref()
                                            .map(|pk| pk.columns.join(", "))
                                            .unwrap_or_else(|| "-".to_string());

                                        h_flex()
                                            .w_full()
                                            .px_2()
                                            .py_1()
                                            .hover(|s| s.bg(theme.list_active))
                                            .child(
                                                div()
                                                    .w(px(180.0))
                                                    .text_sm()
                                                    .text_color(theme.foreground)
                                                    .child(table.name.clone()),
                                            )
                                            .child(
                                                div()
                                                    .w(px(80.0))
                                                    .text_sm()
                                                    .text_color(theme.muted_foreground)
                                                    .child(table.columns.len().to_string()),
                                            )
                                            .child(
                                                div()
                                                    .w(px(80.0))
                                                    .text_sm()
                                                    .text_color(theme.muted_foreground)
                                                    .child(row_count.to_string()),
                                            )
                                            .child(
                                                div()
                                                    .flex_1()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(pk_cols),
                                            )
                                    })),
                            ),
                    )
                    .into_any_element()
            }
            None => {
                // Show loading or info message
                div()
                    .w_full()
                    .p_3()
                    .bg(theme.secondary)
                    .rounded_md()
                    .child(
                        v_flex()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(theme.foreground)
                                    .child("UDIF Import"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("UDIF files contain embedded schema information. Table structure, column types, indexes, and foreign keys will be automatically imported."),
                            ),
                    )
                    .into_any_element()
            }
        }
    }

    fn render_stat_badge(
        &self,
        label: &str,
        value: usize,
        color: gpui::Hsla,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .gap_1()
            .items_center()
            .child(div().size(px(8.0)).rounded_full().bg(color))
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("{}: ", label)),
            )
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child(value.to_string()),
            )
    }

    fn render_step_1_file_source(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let sources = self.state.sources.clone();
        let detected_format = self.state.detected_format;
        let is_udif = self.state.is_udif_import();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .child(div().text_sm().text_color(theme.foreground).child(
                "Select the files or URLs to import. Click Add File or Add URL to add sources.",
            ))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("add-file")
                            .child("Add File...")
                            .small()
                            .on_click(cx.listener(|_this, _: &ClickEvent, _, cx| {
                                cx.emit(ImportWizardEvent::AddFiles);
                            })),
                    )
                    .child(Button::new("add-url").child("Add URL...").small()),
            )
            // Show detected format
            .when_some(detected_format, |this, format| {
                this.child(
                    h_flex()
                        .w_full()
                        .gap_3()
                        .items_center()
                        .child(
                            div()
                                .w(px(140.0))
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("Detected Format:"),
                        )
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(if is_udif {
                                    theme.success
                                } else {
                                    theme.foreground
                                })
                                .child(format.display_name()),
                        ),
                )
            })
            // Only show encoding option for CSV imports
            .when(!is_udif, |this| {
                this.child(self.render_format_row(
                    "Encoding:",
                    Select::new(&self.encoding_state).small(),
                    cx,
                ))
            })
            // Show UDIF preview (includes info box when document not loaded)
            .when(is_udif, |this| this.child(self.render_udif_preview(cx)))
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .overflow_y_scrollbar()
                    .child(v_flex().w_full().children(sources.iter().enumerate().map(
                        |(idx, source)| {
                            let source_name = source.source_name.clone();
                            let source_display = source.source_type.short_display();
                            let selected = source.selected;

                            h_flex()
                                .w_full()
                                .px_2()
                                .py_1()
                                .gap_3()
                                .items_center()
                                .hover(|s| s.bg(theme.list_active))
                                .child(Checkbox::new(format!("source-{}", idx)).checked(selected))
                                .child(
                                    v_flex()
                                        .flex_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(theme.foreground)
                                                .child(source_name),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child(source_display),
                                        ),
                                )
                                .child(
                                    Button::new(format!("remove-{}", idx))
                                        .child("Remove")
                                        .small()
                                        .ghost(),
                                )
                        },
                    ))),
            )
    }

    fn render_step_2_source_format(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let examples = self.state.date_time_example();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .overflow_y_scrollbar()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child("Configure how the source file should be parsed."),
            )
            .child(self.render_section_header("Row Settings", cx))
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(
                        Checkbox::new("has-field-name")
                            .checked(self.state.source_format.has_field_name_row)
                            .label("File has field name row"),
                    )
                    .child(self.render_format_row(
                        "Field Name Row:",
                        Input::new(&self.field_name_row_input).small().w(px(80.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Data Row Start:",
                        Input::new(&self.data_row_start_input).small().w(px(80.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Data Row End:",
                        Input::new(&self.data_row_end_input).small().w(px(80.0)),
                        cx,
                    )),
            )
            .child(self.render_section_header("Date/Time Settings", cx))
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(self.render_format_row(
                        "Date Order:",
                        Select::new(&self.date_order_state).small(),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Date Time Order:",
                        Select::new(&self.date_time_order_state).small(),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Date Delimiter:",
                        Input::new(&self.date_delimiter_input).small().w(px(60.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Time Delimiter:",
                        Input::new(&self.time_delimiter_input).small().w(px(60.0)),
                        cx,
                    )),
            )
            .child(self.render_section_header("Date/Time Examples", cx))
            .child(div().w_full().p_2().bg(theme.secondary).rounded_md().child(
                v_flex().gap_1().children(examples.iter().map(|ex| {
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(ex.clone())
                })),
            ))
            .child(self.render_section_header("Data Settings", cx))
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(self.render_format_row(
                        "Decimal Symbol:",
                        Input::new(&self.decimal_input_state).small().w(px(60.0)),
                        cx,
                    ))
                    .child(self.render_format_row(
                        "Binary Encoding:",
                        Select::new(&self.binary_encoding_state).small(),
                        cx,
                    )),
            )
    }

    fn render_step_3_target_table(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let configs = self.state.target_configs.clone();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child("Map source files to target tables. You can create new tables or use existing ones."),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .overflow_y_scrollbar()
                    .child(
                        v_flex()
                            .w_full()
                            .child(
                                h_flex()
                                    .w_full()
                                    .px_2()
                                    .py_1()
                                    .bg(theme.secondary)
                                    .child(
                                        div()
                                            .w(px(200.0))
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Source"),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Target Table"),
                                    )
                                    .child(
                                        div()
                                            .w(px(100.0))
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Create New"),
                                    ),
                            )
                            .children(configs.iter().enumerate().map(|(idx, config)| {
                                h_flex()
                                    .w_full()
                                    .px_2()
                                    .py_1()
                                    .gap_2()
                                    .items_center()
                                    .hover(|s| s.bg(theme.list_active))
                                    .child(
                                        div()
                                            .w(px(200.0))
                                            .text_sm()
                                            .text_color(theme.foreground)
                                            .child(config.source_name.clone()),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .text_sm()
                                            .text_color(theme.foreground)
                                            .child(config.target_table.clone()),
                                    )
                                    .child(
                                        div()
                                            .w(px(100.0))
                                            .child(
                                                Checkbox::new(format!("create-{}", idx))
                                                    .checked(config.create_new_table),
                                            ),
                                    )
                            })),
                    ),
            )
    }

    fn render_step_4_field_mapping(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let mappings = self
            .state
            .field_mappings
            .get(&self.state.selected_mapping_index)
            .cloned()
            .unwrap_or_default();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child("Map source fields to target table columns."),
            )
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Source:"),
                    )
                    .child(
                        div()
                            .w(px(200.0))
                            .child(Select::new(&self.source_select_state).small()),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .overflow_y_scrollbar()
                    .child(
                        v_flex()
                            .w_full()
                            .child(
                                h_flex()
                                    .w_full()
                                    .px_2()
                                    .py_1()
                                    .bg(theme.secondary)
                                    .child(
                                        div()
                                            .w(px(180.0))
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Source Field"),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Target Field"),
                                    )
                                    .child(
                                        div()
                                            .w(px(60.0))
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("PK"),
                                    )
                                    .child(
                                        div()
                                            .w(px(60.0))
                                            .text_xs()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(theme.muted_foreground)
                                            .child("Skip"),
                                    ),
                            )
                            .children(mappings.iter().enumerate().map(|(idx, mapping)| {
                                h_flex()
                                    .w_full()
                                    .px_2()
                                    .py_1()
                                    .gap_2()
                                    .items_center()
                                    .hover(|s| s.bg(theme.list_active))
                                    .child(
                                        div()
                                            .w(px(180.0))
                                            .text_sm()
                                            .text_color(theme.foreground)
                                            .child(mapping.source_field.clone()),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .text_sm()
                                            .text_color(theme.foreground)
                                            .child(mapping.target_field.clone()),
                                    )
                                    .child(
                                        div().w(px(60.0)).child(
                                            Checkbox::new(format!("pk-{}", idx))
                                                .checked(mapping.is_primary_key),
                                        ),
                                    )
                                    .child(
                                        div().w(px(60.0)).child(
                                            Checkbox::new(format!("skip-{}", idx))
                                                .checked(mapping.skip),
                                        ),
                                    )
                            })),
                    ),
            )
    }

    fn render_step_5_import_mode(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let advanced = &self.state.advanced_settings;
        let is_udif = self.state.is_udif_import();

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .overflow_y_scrollbar()
            // UDIF-specific options
            .when(is_udif, |this| {
                this.child(
                    div()
                        .text_sm()
                        .text_color(theme.foreground)
                        .child("Select what to import from the UDIF file."),
                )
                .child(self.render_section_header("Import Components", cx))
                .child(
                    div().w_full().p_3().bg(theme.secondary).rounded_md().child(
                        v_flex()
                            .w_full()
                            .gap_3()
                            .child(self.render_udif_component_option(
                                "udif-schema",
                                "Create Tables (Schema)",
                                "Create table structures with columns, types, and primary keys",
                                self.state.udif_import_schema,
                                |this| {
                                    this.state.udif_import_schema = !this.state.udif_import_schema;
                                },
                                cx,
                            ))
                            .child(self.render_udif_component_option(
                                "udif-data",
                                "Import Data",
                                "Import all row data from the UDIF file",
                                self.state.udif_import_data,
                                |this| {
                                    this.state.udif_import_data = !this.state.udif_import_data;
                                },
                                cx,
                            ))
                            .child(self.render_udif_component_option(
                                "udif-indexes",
                                "Create Indexes",
                                "Create indexes defined in the UDIF schema",
                                self.state.udif_import_indexes,
                                |this| {
                                    this.state.udif_import_indexes =
                                        !this.state.udif_import_indexes;
                                },
                                cx,
                            ))
                            .child(self.render_udif_component_option(
                                "udif-fk",
                                "Create Foreign Keys",
                                "Create foreign key relationships between tables",
                                !advanced.ignore_foreign_key,
                                |this| {
                                    this.state.advanced_settings.ignore_foreign_key =
                                        !this.state.advanced_settings.ignore_foreign_key;
                                },
                                cx,
                            )),
                    ),
                )
                .child(self.render_section_header("Table Handling", cx))
                .child(
                    v_flex()
                        .w_full()
                        .gap_1()
                        .child(self.render_udif_table_handling_option(
                            "replace",
                            "Replace existing tables",
                            "Drop and recreate tables if they already exist",
                            self.state.import_mode == ImportMode::Copy,
                            cx,
                        ))
                        .child(self.render_udif_table_handling_option(
                            "append",
                            "Append to existing tables",
                            "Add data to existing tables without dropping them",
                            self.state.import_mode == ImportMode::Append,
                            cx,
                        )),
                )
            })
            // CSV-specific options (existing import mode UI)
            .when(!is_udif, |this| {
                this.child(
                    div()
                        .text_sm()
                        .text_color(theme.foreground)
                        .child("Select how records should be imported."),
                )
                .child(self.render_section_header("Import Mode", cx))
                .child(v_flex().w_full().gap_1().children(
                    ImportMode::all().iter().map(|mode| {
                        let is_selected = self.state.import_mode == *mode;
                        h_flex()
                            .w_full()
                            .px_2()
                            .py_1()
                            .gap_2()
                            .items_center()
                            .rounded_sm()
                            .when(is_selected, |s| s.bg(theme.list_active))
                            .child(
                                div()
                                    .size_4()
                                    .rounded_full()
                                    .border_1()
                                    .border_color(if is_selected {
                                        theme.primary
                                    } else {
                                        theme.border
                                    })
                                    .when(is_selected, |s| {
                                        s.child(
                                            div()
                                                .size_2()
                                                .m(px(2.0))
                                                .rounded_full()
                                                .bg(theme.primary),
                                        )
                                    }),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .text_sm()
                                    .text_color(theme.foreground)
                                    .child(mode.display_name()),
                            )
                    }),
                ))
            })
            .child(self.render_section_header("Advanced Settings", cx))
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    // Extended INSERT is CSV-specific
                    .when(!is_udif, |this| {
                        this.child(
                            Checkbox::new("extended-insert")
                                .checked(advanced.use_extended_insert)
                                .label("Use extended INSERT statements"),
                        )
                    })
                    // Empty string as NULL is CSV-specific
                    .when(!is_udif, |this| {
                        this.child(
                            Checkbox::new("empty-null")
                                .checked(advanced.empty_string_as_null)
                                .label("Treat empty string as NULL"),
                        )
                    })
                    // Ignore FK is shown for CSV only (UDIF has it above)
                    .when(!is_udif, |this| {
                        this.child(
                            Checkbox::new("ignore-fk")
                                .checked(advanced.ignore_foreign_key)
                                .label("Ignore foreign key constraints"),
                        )
                    })
                    .child(
                        Checkbox::new("continue-error")
                            .checked(advanced.continue_on_error)
                            .label("Continue on error"),
                    ),
            )
    }

    fn render_udif_component_option(
        &self,
        id: &str,
        title: &str,
        description: &str,
        is_checked: bool,
        on_toggle: impl Fn(&mut Self) + 'static,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let title = title.to_string();
        let description = description.to_string();

        div()
            .id(SharedString::from(id.to_string()))
            .w_full()
            .flex()
            .flex_row()
            .gap_3()
            .items_center()
            .py_1()
            .px_2()
            .rounded_sm()
            .cursor_pointer()
            .hover(|s| s.bg(theme.list_active))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                on_toggle(this);
                cx.notify();
            }))
            .child(Checkbox::new(format!("cb-{}", id)).checked(is_checked))
            .child(
                v_flex()
                    .gap_0p5()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child(title),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(description),
                    ),
            )
    }

    fn render_udif_table_handling_option(
        &self,
        id: &str,
        title: &str,
        description: &str,
        is_selected: bool,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let id_clone = id.to_string();
        let title = title.to_string();
        let description = description.to_string();

        div()
            .id(SharedString::from(id.to_string()))
            .w_full()
            .flex()
            .flex_row()
            .px_2()
            .py_1()
            .gap_2()
            .items_center()
            .rounded_sm()
            .cursor_pointer()
            .when(is_selected, |s| s.bg(theme.list_active))
            .hover(|s| s.bg(theme.list_active))
            .on_click(cx.listener(move |this, _: &ClickEvent, _, cx| {
                if id_clone == "replace" {
                    this.state.import_mode = ImportMode::Copy;
                } else {
                    this.state.import_mode = ImportMode::Append;
                }
                cx.notify();
            }))
            .child(
                div()
                    .size_4()
                    .rounded_full()
                    .border_1()
                    .border_color(if is_selected {
                        theme.primary
                    } else {
                        theme.border
                    })
                    .when(is_selected, |s| {
                        s.child(div().size_2().m(px(2.0)).rounded_full().bg(theme.primary))
                    }),
            )
            .child(
                v_flex()
                    .gap_0p5()
                    .child(div().text_sm().text_color(theme.foreground).child(title))
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(description),
                    ),
            )
    }

    fn render_step_6_progress(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let stats = &self.state.stats;

        v_flex()
            .w_full()
            .h_full()
            .gap_3()
            .p_4()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child(if self.state.is_complete {
                        "Import completed successfully."
                    } else if self.state.is_importing {
                        "Importing data..."
                    } else {
                        "We have gathered all information the wizard needs. Click the Start button to begin importing."
                    }),
            )
            .child(
                v_flex()
                    .w_full()
                    .gap_1()
                    .child(self.render_stat_row("Table:", &stats.current_table, cx))
                    .child(self.render_stat_row("Processed:", &stats.processed.to_string(), cx))
                    .child(self.render_stat_row("Added:", &stats.added.to_string(), cx))
                    .child(self.render_stat_row("Updated:", &stats.updated.to_string(), cx))
                    .child(self.render_stat_row("Deleted:", &stats.deleted.to_string(), cx))
                    .child(self.render_stat_row("Errors:", &stats.errors.to_string(), cx))
                    .child(self.render_stat_row("Time:", &stats.elapsed_display(), cx)),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .p_2()
                    .overflow_y_scrollbar()
                    .child(self.render_log_messages(cx)),
            )
            .child(self.render_progress_bar(cx))
    }

    fn render_stat_row(&self, label: &str, value: &str, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let label = label.to_string();
        let value = value.to_string();
        h_flex()
            .w_full()
            .gap_2()
            .child(
                div()
                    .w(px(100.0))
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child(label),
            )
            .child(
                div()
                    .flex_1()
                    .text_sm()
                    .text_color(theme.foreground)
                    .child(value),
            )
    }

    fn render_log_messages(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let messages = self.state.log_messages.clone();

        v_flex()
            .w_full()
            .gap_px()
            .children(messages.iter().map(|msg| {
                let color = match msg.level {
                    LogLevel::Error => theme.danger,
                    LogLevel::Warning => theme.warning,
                    LogLevel::Success => theme.success,
                    LogLevel::Info => theme.foreground,
                };

                div()
                    .w_full()
                    .text_xs()
                    .text_color(color)
                    .child(msg.format())
            }))
    }

    fn render_progress_bar(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let progress = self.state.progress;

        div()
            .w_full()
            .h(px(8.0))
            .rounded_full()
            .bg(theme.secondary)
            .child(
                div()
                    .h_full()
                    .rounded_full()
                    .bg(theme.primary)
                    .w(relative(progress)),
            )
    }

    fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let step = self.state.current_step;
        let is_importing = self.state.is_importing;
        let is_complete = self.state.is_complete;
        let is_udif = self.state.is_udif_import();

        // Check if we can navigate based on format
        let can_go_back = step.previous_for_format(is_udif).is_some();
        let can_go_next = step.next_for_format(is_udif).is_some();

        h_flex()
            .w_full()
            .px_4()
            .py_3()
            .gap_2()
            .justify_end()
            .border_t_1()
            .border_color(theme.border)
            .child(
                Button::new("close")
                    .child("Close")
                    .small()
                    .on_click(cx.listener(|this, _: &ClickEvent, _, cx| {
                        this.close(cx);
                    })),
            )
            .when(can_go_back && !is_importing, |s| {
                s.child(
                    Button::new("back")
                        .child("Back")
                        .small()
                        .on_click(cx.listener(|this, _: &ClickEvent, _, cx| {
                            this.go_back(cx);
                        })),
                )
            })
            .when(can_go_next, |s| {
                s.child(
                    Button::new("next")
                        .child("Next")
                        .small()
                        .primary()
                        .on_click(cx.listener(|this, _: &ClickEvent, _, cx| {
                            this.go_next(cx);
                        })),
                )
            })
            .when(
                matches!(step, ImportWizardStep::Progress) && !is_importing && !is_complete,
                |s| {
                    s.child(
                        Button::new("start")
                            .child("Start")
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _: &ClickEvent, _, cx| {
                                this.start_import(cx);
                            })),
                    )
                },
            )
            .when(is_complete, |s| {
                s.child(Button::new("view-log").child("View Log").small().when_some(
                    self.state.log_file_path.clone(),
                    |button, path| {
                        button.on_click(cx.listener(move |_this, _: &ClickEvent, _, cx| {
                            cx.emit(ImportWizardEvent::ViewLog(path.clone()));
                        }))
                    },
                ))
            })
    }
}

impl Focusable for ImportWizard {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ImportWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(self.render_step_indicator(cx))
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(match self.state.current_step {
                        ImportWizardStep::FileSource => {
                            self.render_step_1_file_source(cx).into_any_element()
                        }
                        ImportWizardStep::SourceFormat => {
                            self.render_step_2_source_format(cx).into_any_element()
                        }
                        ImportWizardStep::TargetTable => {
                            self.render_step_3_target_table(cx).into_any_element()
                        }
                        ImportWizardStep::FieldMapping => {
                            self.render_step_4_field_mapping(cx).into_any_element()
                        }
                        ImportWizardStep::ImportMode => {
                            self.render_step_5_import_mode(cx).into_any_element()
                        }
                        ImportWizardStep::Progress => {
                            self.render_step_6_progress(cx).into_any_element()
                        }
                    }),
            )
            .child(self.render_footer(cx))
    }
}
