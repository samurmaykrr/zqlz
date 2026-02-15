//! Export Wizard Types and State Models
//!
//! This module defines the data structures for the export wizard UI.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// =============================================================================
// Shared Types
// =============================================================================

/// Log level for wizard log messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    #[default]
    Info,
    Success,
    Warning,
    Error,
}

/// File encoding options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FileEncoding {
    #[default]
    Utf8,
    Utf16Le,
    Utf16Be,
    Latin1,
    Ascii,
    Windows1252,
}

impl FileEncoding {
    pub fn all() -> &'static [Self] {
        &[
            Self::Utf8,
            Self::Utf16Le,
            Self::Utf16Be,
            Self::Latin1,
            Self::Ascii,
            Self::Windows1252,
        ]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Utf16Le => "UTF-16 LE",
            Self::Utf16Be => "UTF-16 BE",
            Self::Latin1 => "ISO-8859-1 (Latin-1)",
            Self::Ascii => "ASCII",
            Self::Windows1252 => "Windows-1252",
        }
    }
}

/// Binary data encoding options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BinaryEncoding {
    #[default]
    Hex,
    Base64,
}

impl BinaryEncoding {
    pub fn all() -> &'static [Self] {
        &[Self::Hex, Self::Base64]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Hex => "Hexadecimal",
            Self::Base64 => "Base64",
        }
    }
}

/// Export format options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ExportFormat {
    /// UDIF (Universal Data Interchange Format) - preserves full schema and types
    #[default]
    Udif,
    /// UDIF compressed with gzip
    UdifCompressed,
    /// CSV/Text file format - lossy but human-readable
    Csv,
}

impl ExportFormat {
    pub fn all() -> &'static [Self] {
        &[Self::Udif, Self::UdifCompressed, Self::Csv]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Udif => "UDIF (Universal Data Interchange Format)",
            Self::UdifCompressed => "UDIF Compressed (.udif.json.gz)",
            Self::Csv => "CSV (Comma-Separated Values)",
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            Self::Udif => ".udif.json",
            Self::UdifCompressed => ".udif.json.gz",
            Self::Csv => ".csv",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::Udif => {
                "Preserves full schema, types, constraints, and data. Best for database-to-database transfers."
            }
            Self::UdifCompressed => {
                "Same as UDIF but compressed. Smaller file size for large datasets."
            }
            Self::Csv => "Human-readable text format. Lossy - does not preserve types or schema.",
        }
    }

    /// Whether this format preserves schema information
    pub fn preserves_schema(&self) -> bool {
        matches!(self, Self::Udif | Self::UdifCompressed)
    }
}

/// Import format detection result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportFormat {
    /// UDIF JSON format
    Udif,
    /// UDIF compressed format
    UdifCompressed,
    /// CSV/Text file format
    Csv,
    /// Unknown format
    Unknown,
}

impl ImportFormat {
    /// Detect format from file extension
    pub fn from_path(path: &std::path::Path) -> Self {
        let path_str = path.to_string_lossy().to_lowercase();
        if path_str.ends_with(".udif.json.gz") {
            Self::UdifCompressed
        } else if path_str.ends_with(".udif.json") {
            Self::Udif
        } else if path_str.ends_with(".csv")
            || path_str.ends_with(".txt")
            || path_str.ends_with(".tsv")
        {
            Self::Csv
        } else {
            Self::Unknown
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Udif => "UDIF (Universal Data Interchange Format)",
            Self::UdifCompressed => "UDIF Compressed",
            Self::Csv => "CSV/Text File",
            Self::Unknown => "Unknown Format",
        }
    }
}

// =============================================================================
// Export Wizard Types
// =============================================================================

/// Current step in the export wizard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportWizardStep {
    #[default]
    TableSelection,
    FieldSelection,
    FormatOptions,
    Progress,
}

impl ExportWizardStep {
    pub fn index(&self) -> usize {
        match self {
            Self::TableSelection => 0,
            Self::FieldSelection => 1,
            Self::FormatOptions => 2,
            Self::Progress => 3,
        }
    }

    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::TableSelection),
            1 => Some(Self::FieldSelection),
            2 => Some(Self::FormatOptions),
            3 => Some(Self::Progress),
            _ => None,
        }
    }

    pub fn can_go_back(&self) -> bool {
        !matches!(self, Self::TableSelection)
    }

    pub fn can_go_next(&self) -> bool {
        !matches!(self, Self::Progress)
    }

    pub fn next(&self) -> Option<Self> {
        Self::from_index(self.index() + 1)
    }

    pub fn previous(&self) -> Option<Self> {
        if self.index() == 0 {
            None
        } else {
            Self::from_index(self.index() - 1)
        }
    }
}

/// Record delimiter for export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RecordDelimiter {
    #[default]
    CrLf,
    Cr,
    Lf,
}

impl RecordDelimiter {
    pub fn all() -> &'static [Self] {
        &[Self::CrLf, Self::Cr, Self::Lf]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::CrLf => "CR+LF (\\r\\n)",
            Self::Cr => "CR (\\r)",
            Self::Lf => "LF (\\n)",
        }
    }

    pub fn value(&self) -> &'static str {
        match self {
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
            Self::Lf => "\n",
        }
    }
}

/// Field delimiter for export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FieldDelimiter {
    #[default]
    Comma,
    Tab,
    Semicolon,
    Space,
    Pipe,
}

impl FieldDelimiter {
    pub fn all() -> &'static [Self] {
        &[
            Self::Comma,
            Self::Tab,
            Self::Semicolon,
            Self::Space,
            Self::Pipe,
        ]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Comma => "Comma (,)",
            Self::Tab => "Tab (\\t)",
            Self::Semicolon => "Semicolon (;)",
            Self::Space => "Space",
            Self::Pipe => "Pipe (|)",
        }
    }

    pub fn value(&self) -> &'static str {
        match self {
            Self::Comma => ",",
            Self::Tab => "\t",
            Self::Semicolon => ";",
            Self::Space => " ",
            Self::Pipe => "|",
        }
    }
}

/// Text qualifier for export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TextQualifier {
    #[default]
    DoubleQuote,
    SingleQuote,
    None,
}

impl TextQualifier {
    pub fn all() -> &'static [Self] {
        &[Self::DoubleQuote, Self::SingleQuote, Self::None]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DoubleQuote => "Double Quote (\")",
            Self::SingleQuote => "Single Quote (')",
            Self::None => "None",
        }
    }

    pub fn value(&self) -> Option<char> {
        match self {
            Self::DoubleQuote => Some('"'),
            Self::SingleQuote => Some('\''),
            Self::None => None,
        }
    }
}

/// Timestamp format for export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimestampFormat {
    #[default]
    Iso8601,
    Unix,
    Custom,
}

impl TimestampFormat {
    pub fn all() -> &'static [Self] {
        &[Self::Iso8601, Self::Unix, Self::Custom]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Iso8601 => "ISO 8601 (YYYY-MM-DD HH:MM:SS)",
            Self::Unix => "Unix Timestamp",
            Self::Custom => "Custom Format",
        }
    }
}

/// Export column configuration
#[derive(Debug, Clone)]
pub struct ExportColumn {
    /// Column name
    pub name: String,
    /// Whether this column is selected for export
    pub selected: bool,
}

impl ExportColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            selected: true,
        }
    }
}

/// Configuration for exporting a single table
#[derive(Debug, Clone)]
pub struct TableExportConfig {
    /// Table name
    pub table_name: String,
    /// Output filename (without path)
    pub output_filename: String,
    /// Whether this table is selected for export
    pub selected: bool,
    /// Column configurations
    pub columns: Vec<ExportColumn>,
}

impl TableExportConfig {
    pub fn new(table_name: impl Into<String>, field_names: Vec<String>) -> Self {
        let name = table_name.into();
        let output_filename = format!("{}.csv", &name);
        let columns = field_names.into_iter().map(ExportColumn::new).collect();
        Self {
            table_name: name,
            output_filename,
            selected: true,
            columns,
        }
    }

    pub fn select_all_fields(&mut self) {
        for col in &mut self.columns {
            col.selected = true;
        }
    }

    pub fn deselect_all_fields(&mut self) {
        for col in &mut self.columns {
            col.selected = false;
        }
    }

    pub fn all_columns_selected(&self) -> bool {
        self.columns.iter().all(|c| c.selected)
    }

    pub fn selected_columns(&self) -> Vec<&ExportColumn> {
        self.columns.iter().filter(|c| c.selected).collect()
    }
}

/// Export log message
#[derive(Debug, Clone)]
pub struct ExportLogMessage {
    pub level: LogLevel,
    pub message: String,
    pub timestamp: chrono::DateTime<chrono::Local>,
}

impl ExportLogMessage {
    pub fn format(&self) -> String {
        format!("[EXP] {}", self.message)
    }
}

/// Export statistics
#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    /// Current object being exported
    pub current_object: String,
    /// Total rows to process
    pub total_rows: u64,
    /// Rows processed
    pub processed_rows: u64,
    /// Time elapsed in seconds
    pub elapsed_seconds: f64,
}

impl ExportStats {
    pub fn elapsed_display(&self) -> String {
        format!("{:.2} s", self.elapsed_seconds)
    }
}

/// Export format options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFormatOptions {
    /// Append to existing file
    pub append: bool,
    /// Continue on error
    pub continue_on_error: bool,
    /// Include column headers
    pub include_headers: bool,
    /// Display blank instead of zero
    pub blank_if_zero: bool,
    /// Record delimiter
    pub record_delimiter: RecordDelimiter,
    /// Field delimiter
    pub field_delimiter: FieldDelimiter,
    /// Text qualifier
    pub text_qualifier: TextQualifier,
    /// Binary encoding
    pub binary_encoding: BinaryEncoding,
    /// Decimal symbol
    pub decimal_symbol: String,
}

impl Default for ExportFormatOptions {
    fn default() -> Self {
        Self {
            append: false,
            continue_on_error: true,
            include_headers: true,
            blank_if_zero: false,
            record_delimiter: RecordDelimiter::default(),
            field_delimiter: FieldDelimiter::default(),
            text_qualifier: TextQualifier::default(),
            binary_encoding: BinaryEncoding::default(),
            decimal_symbol: ".".to_string(),
        }
    }
}

/// Complete export wizard state
#[derive(Debug, Clone)]
pub struct ExportWizardState {
    /// Current step
    pub current_step: ExportWizardStep,
    /// Export format (UDIF or CSV)
    pub export_format: ExportFormat,
    /// File encoding (for CSV)
    pub encoding: FileEncoding,
    /// Output folder
    pub output_folder: PathBuf,
    /// Output filename (without extension, extension added based on format)
    pub output_filename: String,
    /// Table configurations
    pub tables: Vec<TableExportConfig>,
    /// Currently selected table index for field selection
    pub selected_table_index: usize,
    /// Add timestamp to filename
    pub add_timestamp: bool,
    /// Timestamp format
    pub timestamp_format: TimestampFormat,
    /// Custom timestamp format string
    pub custom_timestamp_format: String,
    /// CSV format options (only used when export_format is Csv)
    pub csv_options: ExportFormatOptions,
    /// Whether to include schema in export (for UDIF)
    pub include_schema: bool,
    /// Whether to include indexes in export (for UDIF)
    pub include_indexes: bool,
    /// Whether to include foreign keys in export (for UDIF)
    pub include_foreign_keys: bool,
    /// Whether export is running
    pub is_exporting: bool,
    /// Whether export is complete
    pub is_complete: bool,
    /// Export progress (0.0 to 1.0)
    pub progress: f32,
    /// Export statistics
    pub stats: ExportStats,
    /// Log messages
    pub log_messages: Vec<ExportLogMessage>,
    /// Output file path (for opening after export)
    pub output_file_path: Option<PathBuf>,
}

impl Default for ExportWizardState {
    fn default() -> Self {
        Self {
            current_step: ExportWizardStep::default(),
            export_format: ExportFormat::default(),
            encoding: FileEncoding::default(),
            output_folder: dirs::document_dir().unwrap_or_else(|| PathBuf::from(".")),
            output_filename: "export".to_string(),
            tables: Vec::new(),
            selected_table_index: 0,
            add_timestamp: false,
            timestamp_format: TimestampFormat::default(),
            custom_timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
            csv_options: ExportFormatOptions::default(),
            include_schema: true,
            include_indexes: true,
            include_foreign_keys: true,
            is_exporting: false,
            is_complete: false,
            progress: 0.0,
            stats: ExportStats::default(),
            log_messages: Vec::new(),
            output_file_path: None,
        }
    }
}

impl ExportWizardState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: TableExportConfig) {
        self.tables.push(table);
    }

    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>) {
        self.log_messages.push(ExportLogMessage {
            level,
            message: message.into(),
            timestamp: chrono::Local::now(),
        });
    }

    pub fn selected_tables(&self) -> Vec<&TableExportConfig> {
        self.tables.iter().filter(|t| t.selected).collect()
    }

    pub fn current_table(&self) -> Option<&TableExportConfig> {
        self.tables.get(self.selected_table_index)
    }

    pub fn current_table_mut(&mut self) -> Option<&mut TableExportConfig> {
        self.tables.get_mut(self.selected_table_index)
    }

    /// Get the full output file path based on format and settings
    pub fn output_path(&self) -> PathBuf {
        let mut filename = self.output_filename.clone();

        // Add timestamp if requested
        if self.add_timestamp {
            let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
            filename = format!("{}_{}", filename, timestamp);
        }

        // Add extension based on format
        filename.push_str(self.export_format.file_extension());

        self.output_folder.join(filename)
    }

    /// Build ExportOptions for the GenericExporter (UDIF export)
    pub fn to_export_options(&self) -> crate::ExportOptions {
        let include_tables: Vec<String> = self
            .tables
            .iter()
            .filter(|t| t.selected)
            .map(|t| t.table_name.clone())
            .collect();

        crate::ExportOptions {
            include_schema: self.include_schema,
            include_data: true,
            include_indexes: self.include_indexes,
            include_foreign_keys: self.include_foreign_keys,
            include_tables,
            ..Default::default()
        }
    }
}

/// Export profile for saving/loading wizard configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportProfile {
    pub name: String,
    pub encoding: FileEncoding,
    pub add_timestamp: bool,
    pub timestamp_format: TimestampFormat,
    pub custom_timestamp_format: String,
    pub csv_options: ExportFormatOptions,
}

impl ExportProfile {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            encoding: FileEncoding::default(),
            add_timestamp: false,
            timestamp_format: TimestampFormat::default(),
            custom_timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
            csv_options: ExportFormatOptions::default(),
        }
    }

    pub fn from_state(name: impl Into<String>, state: &ExportWizardState) -> Self {
        Self {
            name: name.into(),
            encoding: state.encoding,
            add_timestamp: state.add_timestamp,
            timestamp_format: state.timestamp_format,
            custom_timestamp_format: state.custom_timestamp_format.clone(),
            csv_options: state.csv_options.clone(),
        }
    }
}

// =============================================================================
// Import Wizard Types
// =============================================================================

/// Current step in the import wizard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImportWizardStep {
    #[default]
    FileSource,
    SourceFormat,
    TargetTable,
    FieldMapping,
    ImportMode,
    Progress,
}

impl ImportWizardStep {
    pub fn index(&self) -> usize {
        match self {
            Self::FileSource => 0,
            Self::SourceFormat => 1,
            Self::TargetTable => 2,
            Self::FieldMapping => 3,
            Self::ImportMode => 4,
            Self::Progress => 5,
        }
    }

    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::FileSource),
            1 => Some(Self::SourceFormat),
            2 => Some(Self::TargetTable),
            3 => Some(Self::FieldMapping),
            4 => Some(Self::ImportMode),
            5 => Some(Self::Progress),
            _ => None,
        }
    }

    pub fn can_go_back(&self) -> bool {
        !matches!(self, Self::FileSource)
    }

    pub fn can_go_next(&self) -> bool {
        !matches!(self, Self::Progress)
    }

    pub fn next(&self) -> Option<Self> {
        Self::from_index(self.index() + 1)
    }

    pub fn previous(&self) -> Option<Self> {
        if self.index() == 0 {
            None
        } else {
            Self::from_index(self.index() - 1)
        }
    }

    /// Get the next step, skipping CSV-specific steps for UDIF imports
    pub fn next_for_format(&self, is_udif: bool) -> Option<Self> {
        if is_udif {
            // For UDIF: FileSource -> ImportMode -> Progress
            // Skip SourceFormat, TargetTable, FieldMapping as UDIF has embedded schema
            match self {
                Self::FileSource => Some(Self::ImportMode),
                Self::ImportMode => Some(Self::Progress),
                _ => None,
            }
        } else {
            self.next()
        }
    }

    /// Get the previous step, skipping CSV-specific steps for UDIF imports
    pub fn previous_for_format(&self, is_udif: bool) -> Option<Self> {
        if is_udif {
            // For UDIF: Progress -> ImportMode -> FileSource
            match self {
                Self::Progress => Some(Self::ImportMode),
                Self::ImportMode => Some(Self::FileSource),
                _ => None,
            }
        } else {
            self.previous()
        }
    }

    /// Check if this step applies to CSV imports only
    pub fn is_csv_only(&self) -> bool {
        matches!(
            self,
            Self::SourceFormat | Self::TargetTable | Self::FieldMapping
        )
    }

    /// Get the display name for the step
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::FileSource => "1. File Source",
            Self::SourceFormat => "2. Source Format",
            Self::TargetTable => "3. Target Table",
            Self::FieldMapping => "4. Field Mapping",
            Self::ImportMode => "5. Import Mode",
            Self::Progress => "6. Import",
        }
    }

    /// Get all steps relevant for the given format
    pub fn all_for_format(is_udif: bool) -> Vec<Self> {
        if is_udif {
            vec![Self::FileSource, Self::ImportMode, Self::Progress]
        } else {
            vec![
                Self::FileSource,
                Self::SourceFormat,
                Self::TargetTable,
                Self::FieldMapping,
                Self::ImportMode,
                Self::Progress,
            ]
        }
    }
}

/// Source type for import
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportSourceType {
    File(PathBuf),
    Url(String),
}

impl ImportSourceType {
    pub fn display(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Url(url) => url.clone(),
        }
    }

    pub fn short_display(&self) -> String {
        match self {
            Self::File(path) => {
                // Truncate long paths with ellipsis
                let display = path.display().to_string();
                if display.len() > 30 {
                    format!("{}...{}", &display[..15], &display[display.len() - 12..])
                } else {
                    display
                }
            }
            Self::Url(url) => {
                if url.len() > 30 {
                    format!("{}...", &url[..27])
                } else {
                    url.clone()
                }
            }
        }
    }
}

/// An import source with parsed metadata
#[derive(Debug, Clone)]
pub struct ImportSource {
    /// The source (file or URL)
    pub source_type: ImportSourceType,
    /// Detected source name (filename or inferred from URL)
    pub source_name: String,
    /// Whether this source is selected for import
    pub selected: bool,
    /// Preview of first few rows (populated after parsing)
    pub preview_rows: Vec<Vec<String>>,
    /// Detected column headers
    pub detected_columns: Vec<String>,
}

impl ImportSource {
    pub fn from_file(path: PathBuf) -> Self {
        let source_name = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        Self {
            source_type: ImportSourceType::File(path),
            source_name,
            selected: true,
            preview_rows: Vec::new(),
            detected_columns: Vec::new(),
        }
    }

    pub fn from_url(url: String) -> Self {
        // Try to extract name from URL
        let source_name = url
            .split('/')
            .last()
            .and_then(|s| s.split('.').next())
            .unwrap_or("imported")
            .to_string();

        Self {
            source_type: ImportSourceType::Url(url),
            source_name,
            selected: true,
            preview_rows: Vec::new(),
            detected_columns: Vec::new(),
        }
    }

    /// Get the file path if this is a file source
    pub fn path(&self) -> Option<&PathBuf> {
        match &self.source_type {
            ImportSourceType::File(path) => Some(path),
            ImportSourceType::Url(_) => None,
        }
    }

    /// Get the URL if this is a URL source
    pub fn url(&self) -> Option<&str> {
        match &self.source_type {
            ImportSourceType::File(_) => None,
            ImportSourceType::Url(url) => Some(url),
        }
    }

    /// Check if this is a file source
    pub fn is_file(&self) -> bool {
        matches!(self.source_type, ImportSourceType::File(_))
    }

    /// Check if this is a URL source
    pub fn is_url(&self) -> bool {
        matches!(self.source_type, ImportSourceType::Url(_))
    }
}

/// Date order format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DateOrder {
    #[default]
    DMY, // Day/Month/Year
    MDY, // Month/Day/Year
    YMD, // Year/Month/Day
}

impl DateOrder {
    pub fn all() -> &'static [Self] {
        &[Self::DMY, Self::MDY, Self::YMD]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DMY => "DMY",
            Self::MDY => "MDY",
            Self::YMD => "YMD",
        }
    }
}

/// Date time order format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DateTimeOrder {
    #[default]
    DateTime, // Date then Time
    TimeDate, // Time then Date
    DateOnly, // Date only
    TimeOnly, // Time only
}

impl DateTimeOrder {
    pub fn all() -> &'static [Self] {
        &[
            Self::DateTime,
            Self::TimeDate,
            Self::DateOnly,
            Self::TimeOnly,
        ]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DateTime => "Date Time",
            Self::TimeDate => "Time Date",
            Self::DateOnly => "Date Only",
            Self::TimeOnly => "Time Only",
        }
    }
}

/// Source format options (Step 2)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFormatOptions {
    /// Whether file has field name row
    pub has_field_name_row: bool,
    /// Row number containing field names (1-based)
    pub field_name_row: usize,
    /// Starting data row (1-based)
    pub data_row_start: usize,
    /// Ending data row (None = End of File)
    pub data_row_end: Option<usize>,
    /// Date order format
    pub date_order: DateOrder,
    /// Date time order format
    pub date_time_order: DateTimeOrder,
    /// Date delimiter (e.g., "/", "-", ".")
    pub date_delimiter: String,
    /// Whether to use year delimiter
    pub use_year_delimiter: bool,
    /// Year delimiter
    pub year_delimiter: String,
    /// Time delimiter (e.g., ":")
    pub time_delimiter: String,
    /// Decimal symbol
    pub decimal_symbol: String,
    /// Binary data encoding
    pub binary_encoding: BinaryEncoding,
}

impl Default for SourceFormatOptions {
    fn default() -> Self {
        Self {
            has_field_name_row: true,
            field_name_row: 1,
            data_row_start: 2,
            data_row_end: None,
            date_order: DateOrder::default(),
            date_time_order: DateTimeOrder::default(),
            date_delimiter: "/".to_string(),
            use_year_delimiter: false,
            year_delimiter: "/".to_string(),
            time_delimiter: ":".to_string(),
            decimal_symbol: ".".to_string(),
            binary_encoding: BinaryEncoding::default(),
        }
    }
}

/// Target table configuration (Step 3)
#[derive(Debug, Clone)]
pub struct TargetTableConfig {
    /// Index of the source file
    pub source_index: usize,
    /// Source name (from file)
    pub source_name: String,
    /// Target table name
    pub target_table: String,
    /// Whether to create a new table
    pub create_new_table: bool,
}

/// Field mapping configuration (Step 4)
#[derive(Debug, Clone)]
pub struct FieldMapping {
    /// Source field name
    pub source_field: String,
    /// Target field name
    pub target_field: String,
    /// Whether this field is a primary key
    pub is_primary_key: bool,
    /// Whether to skip this field
    pub skip: bool,
}

/// Import mode (Step 5)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ImportMode {
    /// Add records to the destination table
    #[default]
    Append,
    /// Update records in the destination with matching records from source
    Update,
    /// If records exist in destination, update it. Otherwise, add it
    AppendUpdate,
    /// If records exist in destination, skip it. Otherwise, add it
    AppendWithoutUpdate,
    /// Delete records in destination that match records in source
    Delete,
    /// Delete all records in destination, repopulate from the source
    Copy,
}

impl ImportMode {
    pub fn all() -> &'static [Self] {
        &[
            Self::Append,
            Self::Update,
            Self::AppendUpdate,
            Self::AppendWithoutUpdate,
            Self::Delete,
            Self::Copy,
        ]
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Append => "Append: add records to the destination table",
            Self::Update => {
                "Update: update records in the destination with matching records from source"
            }
            Self::AppendUpdate => {
                "Append/Update: if records exist in destination, update it. Otherwise, add it"
            }
            Self::AppendWithoutUpdate => {
                "Append without update: if records exist in destination, skip it. Otherwise, add it"
            }
            Self::Delete => "Delete: delete records in destination that match records in source",
            Self::Copy => "Copy: delete all records in destination, repopulate from the source",
        }
    }

    pub fn short_name(&self) -> &'static str {
        match self {
            Self::Append => "Append",
            Self::Update => "Update",
            Self::AppendUpdate => "Append/Update",
            Self::AppendWithoutUpdate => "Append without update",
            Self::Delete => "Delete",
            Self::Copy => "Copy",
        }
    }
}

/// Advanced import settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportAdvancedSettings {
    /// Use extended insert statements (batch multiple rows per INSERT)
    pub use_extended_insert: bool,
    /// Maximum statement size in KB
    pub max_statement_size_kb: usize,
    /// Treat empty string as NULL
    pub empty_string_as_null: bool,
    /// Ignore foreign key constraints during import
    pub ignore_foreign_key: bool,
    /// Continue on error
    pub continue_on_error: bool,
}

impl Default for ImportAdvancedSettings {
    fn default() -> Self {
        Self {
            use_extended_insert: true,
            max_statement_size_kb: 1024,
            empty_string_as_null: false,
            ignore_foreign_key: false,
            continue_on_error: true,
        }
    }
}

/// Import statistics (Step 6)
#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    /// Current table being imported (e.g., "1/3")
    pub current_table: String,
    /// Rows processed
    pub processed: u64,
    /// Rows added
    pub added: u64,
    /// Rows updated
    pub updated: u64,
    /// Rows deleted
    pub deleted: u64,
    /// Error count
    pub errors: u64,
    /// Time elapsed in seconds
    pub elapsed_seconds: f64,
}

impl ImportStats {
    pub fn elapsed_display(&self) -> String {
        format!("{:.2} s", self.elapsed_seconds)
    }
}

/// Complete import wizard state
#[derive(Debug, Clone)]
pub struct ImportWizardState {
    /// Current step
    pub current_step: ImportWizardStep,
    /// Detected import format (auto-detected from first source file)
    pub detected_format: Option<ImportFormat>,
    /// File encoding (for CSV)
    pub encoding: FileEncoding,
    /// Import sources (files/URLs)
    pub sources: Vec<ImportSource>,
    /// Source format options (for CSV)
    pub source_format: SourceFormatOptions,
    /// Target table configurations
    pub target_configs: Vec<TargetTableConfig>,
    /// Field mappings per source (source_index -> mappings)
    pub field_mappings: HashMap<usize, Vec<FieldMapping>>,
    /// Currently selected source index for field mapping
    pub selected_mapping_index: usize,
    /// Import mode
    pub import_mode: ImportMode,
    /// Advanced settings
    pub advanced_settings: ImportAdvancedSettings,

    // UDIF-specific import options
    /// Whether to create tables/import schema (UDIF only)
    pub udif_import_schema: bool,
    /// Whether to import data (UDIF only)
    pub udif_import_data: bool,
    /// Whether to create indexes (UDIF only)
    pub udif_import_indexes: bool,
    /// Whether to create foreign keys (UDIF only) - uses advanced_settings.ignore_foreign_key inverse
    /// This field is kept in sync with !advanced_settings.ignore_foreign_key

    /// Whether import is running
    pub is_importing: bool,
    /// Whether import is complete
    pub is_complete: bool,
    /// Import progress (0.0 to 1.0)
    pub progress: f32,
    /// Import statistics
    pub stats: ImportStats,
    /// Log messages
    pub log_messages: Vec<ImportLogMessage>,
    /// Path to log file (for View Log button)
    pub log_file_path: Option<PathBuf>,
    /// Loaded UDIF document (if importing UDIF format)
    pub udif_document: Option<crate::UdifDocument>,
}

impl Default for ImportWizardState {
    fn default() -> Self {
        Self {
            current_step: ImportWizardStep::default(),
            detected_format: None,
            encoding: FileEncoding::default(),
            sources: Vec::new(),
            source_format: SourceFormatOptions::default(),
            target_configs: Vec::new(),
            field_mappings: HashMap::new(),
            selected_mapping_index: 0,
            import_mode: ImportMode::default(),
            advanced_settings: ImportAdvancedSettings::default(),
            // UDIF options default to true (import everything)
            udif_import_schema: true,
            udif_import_data: true,
            udif_import_indexes: true,
            is_importing: false,
            is_complete: false,
            progress: 0.0,
            stats: ImportStats::default(),
            log_messages: Vec::new(),
            log_file_path: None,
            udif_document: None,
        }
    }
}

impl ImportWizardState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file(&mut self, path: PathBuf) {
        // Detect format from first file added
        if self.detected_format.is_none() {
            self.detected_format = Some(ImportFormat::from_path(&path));
        }
        self.sources.push(ImportSource::from_file(path));
    }

    pub fn add_url(&mut self, url: String) {
        self.sources.push(ImportSource::from_url(url));
    }

    pub fn remove_source(&mut self, index: usize) {
        if index < self.sources.len() {
            self.sources.remove(index);
            // Re-detect format if we removed the first source
            if index == 0 {
                self.detected_format = self
                    .sources
                    .first()
                    .and_then(|s| s.path().map(|p| ImportFormat::from_path(p)));
            }
        }
    }

    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>) {
        self.log_messages.push(ImportLogMessage {
            level,
            message: message.into(),
            timestamp: chrono::Local::now(),
        });
    }

    /// Check if importing UDIF format
    pub fn is_udif_import(&self) -> bool {
        matches!(
            self.detected_format,
            Some(ImportFormat::Udif) | Some(ImportFormat::UdifCompressed)
        )
    }

    /// Check if importing CSV format
    pub fn is_csv_import(&self) -> bool {
        matches!(self.detected_format, Some(ImportFormat::Csv))
    }

    /// Build ImportOptions for the GenericImporter (UDIF import)
    pub fn to_import_options(&self) -> crate::ImportOptions {
        let if_exists = match self.import_mode {
            ImportMode::Append => crate::IfTableExists::Append,
            ImportMode::Update => crate::IfTableExists::Append,
            ImportMode::AppendUpdate => crate::IfTableExists::Append,
            ImportMode::AppendWithoutUpdate => crate::IfTableExists::Append,
            ImportMode::Delete => crate::IfTableExists::Append,
            ImportMode::Copy => crate::IfTableExists::Replace,
        };

        crate::ImportOptions {
            if_exists,
            create_tables: self.udif_import_schema,
            import_data: self.udif_import_data,
            create_indexes: self.udif_import_indexes,
            create_foreign_keys: !self.advanced_settings.ignore_foreign_key,
            continue_on_error: self.advanced_settings.continue_on_error,
            ..Default::default()
        }
    }

    /// Generate date time example based on current format settings
    pub fn date_time_example(&self) -> Vec<String> {
        let fmt = &self.source_format;
        let d = fmt.date_delimiter.as_str();

        // Generate example date parts based on date order
        let (d1, d2, d3) = match fmt.date_order {
            DateOrder::DMY => ("24", "8", "23"),
            DateOrder::MDY => ("8", "24", "23"),
            DateOrder::YMD => ("23", "8", "24"),
        };

        let date_short = format!("{}{d}{}{d}{}", d1, d2, d3);

        let (d1_long, d2_long, d3_long) = match fmt.date_order {
            DateOrder::DMY => ("24", "8", "2023"),
            DateOrder::MDY => ("8", "24", "2023"),
            DateOrder::YMD => ("2023", "8", "24"),
        };
        let date_long = format!("{}{d}{}{d}{}", d1_long, d2_long, d3_long);

        let t = fmt.time_delimiter.as_str();
        let time = format!("15{t}30{t}38");

        let examples = vec![
            format!("{} {}", date_short, time),
            format!("{} {}", date_long, time),
            format!("{}/Aug/{} {}", d1, d3, time),
            format!("{}/August/{} {}", d1, d3, time),
        ];

        examples
    }
}

/// Import log message
#[derive(Debug, Clone)]
pub struct ImportLogMessage {
    pub level: LogLevel,
    pub message: String,
    pub timestamp: chrono::DateTime<chrono::Local>,
}

impl ImportLogMessage {
    pub fn format(&self) -> String {
        format!("[IMP] {}", self.message)
    }
}

/// Import profile for saving/loading wizard configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportProfile {
    pub name: String,
    pub encoding: FileEncoding,
    pub source_format: SourceFormatOptions,
    pub import_mode: ImportMode,
    pub advanced_settings: ImportAdvancedSettings,
}

impl ImportProfile {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            encoding: FileEncoding::default(),
            source_format: SourceFormatOptions::default(),
            import_mode: ImportMode::default(),
            advanced_settings: ImportAdvancedSettings::default(),
        }
    }

    pub fn from_state(name: impl Into<String>, state: &ImportWizardState) -> Self {
        Self {
            name: name.into(),
            encoding: state.encoding,
            source_format: state.source_format.clone(),
            import_mode: state.import_mode,
            advanced_settings: state.advanced_settings.clone(),
        }
    }
}
