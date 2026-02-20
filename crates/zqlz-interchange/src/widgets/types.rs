//! Export Wizard Types and State Models
//!
//! This module defines the data structures for the export wizard UI.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::importer::DegradationWarning;

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
    /// Detect format from file extension alone (sync, no I/O).
    ///
    /// `.udif.json` and `.udif.json.gz` are unambiguous by extension and never require
    /// content sniffing; this function handles them authoritatively.  For plain `.json`
    /// files use [`ImportFormat::detect`] instead, which reads the first 4 KB to
    /// distinguish UDIF from arbitrary JSON.
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

    /// Detect format from the file path, falling back to content sniffing for `.json` files.
    ///
    /// Extension-unambiguous cases (`.udif.json`, `.udif.json.gz`, `.csv`, `.tsv`, `.txt`) are
    /// resolved without any file I/O.  Only plain `.json` files (where the extension is
    /// ambiguous between UDIF and arbitrary JSON) trigger a read of the first 4 KB to look for
    /// the `"version"` and `"schema"` top-level keys that uniquely identify a UDIF document.
    ///
    /// The file is not held open after this function returns.  If the file cannot be read or the
    /// JSON is malformed, `Unknown` is returned rather than propagating an I/O error — the caller
    /// can surface a more helpful message when it subsequently tries to parse the file.
    pub async fn detect(path: &std::path::Path) -> Self {
        let by_extension = Self::from_path(path);

        // Only plain .json files are ambiguous; all other extensions are conclusive.
        if by_extension != Self::Unknown {
            return by_extension;
        }

        let path_lower = path.to_string_lossy().to_lowercase();
        if !path_lower.ends_with(".json") {
            return Self::Unknown;
        }

        // Read first 4 KB to look for UDIF marker keys.
        Self::sniff_json_file(path).await
    }

    /// Read the first 4 KB of a `.json` file and return `Udif` if both `"version"` and
    /// `"schema"` top-level keys are present, otherwise `Unknown`.
    ///
    /// Using a byte-level search rather than full JSON parsing keeps this fast and avoids
    /// pulling in a dependency on async JSON streaming.  The search is conservative: if the
    /// file is valid UDIF the keys will appear near the top within the 4 KB window.
    async fn sniff_json_file(path: &std::path::Path) -> Self {
        use tokio::io::AsyncReadExt;

        let file = match tokio::fs::File::open(path).await {
            Ok(f) => f,
            Err(_) => return Self::Unknown,
        };

        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = vec![0u8; 4096];
        let bytes_read = match reader.read(&mut buffer).await {
            Ok(n) => n,
            Err(_) => return Self::Unknown,
        };
        // File is dropped here when `reader` goes out of scope.

        let snippet = match std::str::from_utf8(&buffer[..bytes_read]) {
            Ok(s) => s,
            Err(_) => return Self::Unknown,
        };

        // Both keys must be present as top-level JSON keys to distinguish UDIF from arbitrary
        // JSON that might coincidentally contain one of them.
        let has_version = snippet.contains("\"version\"");
        let has_schema = snippet.contains("\"schema\"");

        if has_version && has_schema {
            Self::Udif
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

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::TableSelection => "1. Select Tables",
            Self::FieldSelection => "2. Select Fields",
            Self::FormatOptions => "3. Format Options",
            Self::Progress => "4. Export",
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::TableSelection,
            Self::FieldSelection,
            Self::FormatOptions,
            Self::Progress,
        ]
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
    /// Whether to include data rows in export (for UDIF); false produces a schema-only DDL export
    pub include_data: bool,
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
    /// Path to log file saved after export completes (for View Log button)
    pub log_file_path: Option<PathBuf>,
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
            include_data: true,
            include_indexes: true,
            include_foreign_keys: true,
            is_exporting: false,
            is_complete: false,
            progress: 0.0,
            stats: ExportStats::default(),
            log_messages: Vec::new(),
            output_file_path: None,
            log_file_path: None,
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

    /// Writes the accumulated log messages to a timestamped file in the wizard
    /// log directory and returns the path to that file.
    ///
    /// `driver_name` is the database driver (e.g. `"sqlite"`), `source_label`
    /// describes what was exported (e.g. table names or the connection name),
    /// and `target_label` is the output filename. These are embedded in the
    /// filename for easy identification without opening the file.
    pub fn write_log_file(
        &self,
        driver_name: &str,
        source_label: &str,
        target_label: &str,
    ) -> std::io::Result<std::path::PathBuf> {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y%m%d_%H%M%S");
        let source_part = sanitize_for_filename(&format!("{}_{}", driver_name, source_label));
        let target_part = sanitize_for_filename(target_label);
        let filename = format!("export_{}_{}_{}.log", timestamp, source_part, target_part);

        let log_dir = wizard_log_dir();
        std::fs::create_dir_all(&log_dir)?;
        let path = log_dir.join(&filename);

        let format_name = self.export_format.display_name();

        let mut content = format!(
            "=== zqlz Export Log ===\nStarted:  {}\nFormat:   {}\nSource:   {} → {}\nTarget:   {}\nDuration: {}\n\n--- Log ---\n",
            now.format("%Y-%m-%d %H:%M:%S"),
            format_name,
            driver_name,
            source_label,
            target_label,
            self.stats.elapsed_display(),
        );
        for msg in &self.log_messages {
            content.push_str(&format!(
                "{} [{}]  {}\n",
                msg.timestamp.format("%H:%M:%S"),
                log_level_label(msg.level),
                msg.message,
            ));
        }

        std::fs::write(&path, content)?;
        Ok(path)
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
            include_data: self.include_data,
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
    /// Shown after a successful import to present the degradation report.
    Summary,
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
            Self::Summary => 6,
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
            6 => Some(Self::Summary),
            _ => None,
        }
    }

    pub fn can_go_back(&self) -> bool {
        !matches!(self, Self::FileSource)
    }

    pub fn can_go_next(&self) -> bool {
        !matches!(self, Self::Progress | Self::Summary)
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
            // Summary is entered programmatically after import completion, not via Next
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
            // Summary has no Back — the user can only close or review
            match self {
                Self::Progress => Some(Self::ImportMode),
                Self::ImportMode => Some(Self::FileSource),
                _ => None,
            }
        } else {
            // Summary has no Back for CSV either
            if matches!(self, Self::Summary) {
                None
            } else {
                self.previous()
            }
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
            Self::Summary => "7. Summary",
        }
    }

    /// Get all steps relevant for the given format
    pub fn all_for_format(is_udif: bool) -> Vec<Self> {
        if is_udif {
            vec![
                Self::FileSource,
                Self::ImportMode,
                Self::Progress,
                Self::Summary,
            ]
        } else {
            vec![
                Self::FileSource,
                Self::SourceFormat,
                Self::TargetTable,
                Self::FieldMapping,
                Self::ImportMode,
                Self::Progress,
                Self::Summary,
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
    /// Field delimiter character used to split columns.
    ///
    /// Defaults to `','` for CSV. Auto-populated to `'\t'` when the source
    /// file has a `.tsv` extension so TSV files work without manual configuration.
    #[serde(default = "SourceFormatOptions::default_field_delimiter")]
    pub field_delimiter: char,
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
            field_delimiter: ',',
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

impl SourceFormatOptions {
    fn default_field_delimiter() -> char {
        ','
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
    /// Whether this field is auto-generated by the database (serial / AUTO_INCREMENT).
    ///
    /// When true the column must be omitted from INSERT statements so the database
    /// assigns the value automatically.  Attempting to INSERT into a serial or
    /// AUTO_INCREMENT column causes duplicate-key errors once the sequence catches up.
    pub is_auto_increment: bool,
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

    /// Whether this mode has a complete backend implementation.
    ///
    /// Update, AppendUpdate, AppendWithoutUpdate, and Delete all require UPSERT/DELETE logic
    /// that does not yet exist in GenericImporter or CsvImporter. Returning false here lets
    /// callers surface a user-visible error rather than silently doing the wrong thing.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Append | Self::Copy)
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

// =============================================================================
// Log file helpers (shared by import and export wizards)
// =============================================================================

/// Returns the directory where wizard log files are stored.
///
/// Mirrors the path used by `zqlz-app`'s logging module so all logs live
/// together under `{data_local_dir}/zqlz/logs/`.
fn wizard_log_dir() -> std::path::PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("zqlz")
        .join("logs")
}

/// Strips characters that are unsafe in file names, replacing runs of them
/// with a single underscore.
fn sanitize_for_filename(s: &str) -> String {
    let sanitized: String = s
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    // Collapse consecutive underscores for readability.
    let mut result = String::with_capacity(sanitized.len());
    let mut last_was_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !last_was_underscore {
                result.push(c);
            }
            last_was_underscore = true;
        } else {
            result.push(c);
            last_was_underscore = false;
        }
    }
    result.trim_matches('_').to_string()
}

fn log_level_label(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Info => "INFO ",
        LogLevel::Success => "OK   ",
        LogLevel::Warning => "WARN ",
        LogLevel::Error => "ERROR",
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
    /// Inline error shown below the file list in Step 1 (e.g. format conflict on add_file)
    pub add_file_error: Option<String>,
    /// Inline error shown below the target-table grid in Step 3 when any table name is blank
    pub target_table_validation_error: Option<String>,
    /// Inline error shown below the field mapping grid in Step 4 when all columns are skipped
    pub field_mapping_validation_error: Option<String>,
    /// Degradation warnings collected from the last successful UDIF import, displayed in the Summary step.
    pub degradation_warnings: Vec<DegradationWarning>,
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
            add_file_error: None,
            target_table_validation_error: None,
            field_mapping_validation_error: None,
            degradation_warnings: Vec::new(),
        }
    }
}

impl ImportWizardState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a file to the import sources, rejecting it if its format conflicts with
    /// files already in the list.
    ///
    /// Mixing formats (e.g. adding a CSV after a UDIF file) would cause an opaque
    /// failure deep in the importer, so we surface the problem here with a clear
    /// human-readable message instead.
    ///
    /// For plain `.json` files the format is determined by content-sniffing the first 4 KB
    /// (looking for UDIF marker keys) rather than the extension alone.
    ///
    /// When the first file with a `.tsv` extension is added, `source_format.field_delimiter`
    /// is automatically set to `'\t'` so TSV files parse correctly without manual configuration.
    pub async fn add_file(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let detected = ImportFormat::detect(&path).await;
        self.add_file_with_format(path, detected)
    }

    /// Core add-file logic given an already-detected format.
    ///
    /// Separated from `add_file` so that the GPUI wizard can spawn the async `detect` call
    /// and then apply the result synchronously on the foreground thread without holding a
    /// mutable borrow across an await point.
    pub fn add_file_with_format(&mut self, path: PathBuf, new_format: ImportFormat) -> anyhow::Result<()> {

        if let Some(existing) = self.detected_format {
            // Unknown-format files can always be added alongside any other format;
            // the user may be intentionally mixing in supplementary files.
            let conflict = existing != new_format
                && new_format != ImportFormat::Unknown
                && existing != ImportFormat::Unknown;

            if conflict {
                anyhow::bail!(
                    "Cannot mix {} and {} files in the same import. \
                     Remove the existing files first, or start a new import.",
                    existing.display_name(),
                    new_format.display_name()
                );
            }
        }

        // Lock in the detected format on the first file with a known format.
        if self.detected_format.is_none() || self.detected_format == Some(ImportFormat::Unknown) {
            if new_format != ImportFormat::Unknown {
                self.detected_format = Some(new_format);
            } else if self.detected_format.is_none() {
                self.detected_format = Some(ImportFormat::Unknown);
            }
        }

        // Auto-set tab delimiter for TSV files on the first file added so the user
        // does not have to configure it manually. Only applies to the first source
        // so that a subsequent CSV added to an already-tab-delimited session does not
        // silently revert the delimiter the user may have manually set.
        if self.sources.is_empty() {
            let path_lower = path.to_string_lossy().to_lowercase();
            if path_lower.ends_with(".tsv") {
                self.source_format.field_delimiter = '\t';
            }
        }

        self.sources.push(ImportSource::from_file(path));
        Ok(())
    }

    pub fn add_url(&mut self, url: String) {
        self.sources.push(ImportSource::from_url(url));
    }

    pub fn remove_source(&mut self, index: usize) {
        if index < self.sources.len() {
            self.sources.remove(index);
            // Always re-derive the format from the remaining sources so that removing
            // the only UDIF file from a mixed (Unknown + UDIF) list resets correctly.
            self.detected_format = self
                .sources
                .iter()
                .find_map(|s| s.path().map(|p| ImportFormat::from_path(p)))
                .or(if self.sources.is_empty() {
                    None
                } else {
                    Some(ImportFormat::Unknown)
                });
            // A removal clears any outstanding add-file error so the user can try again.
            self.add_file_error = None;
        }
    }

    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>) {
        self.log_messages.push(ImportLogMessage {
            level,
            message: message.into(),
            timestamp: chrono::Local::now(),
        });
    }

    /// Writes the accumulated log messages to a timestamped file in the wizard
    /// log directory and returns the path to that file.
    ///
    /// `source_label` should be the source file stem or URL, `driver_name` the
    /// database driver (e.g. `"sqlite"`), and `target_label` the target table
    /// name. These are embedded in the filename so logs are identifiable at a
    /// glance without opening them.
    pub fn write_log_file(
        &self,
        source_label: &str,
        driver_name: &str,
        target_label: &str,
    ) -> std::io::Result<std::path::PathBuf> {
        let now = chrono::Local::now();
        let timestamp = now.format("%Y%m%d_%H%M%S");
        let source_part = sanitize_for_filename(source_label);
        let target_part = sanitize_for_filename(&format!("{}_{}", driver_name, target_label));
        let filename = format!("import_{}_{}_{}.log", timestamp, source_part, target_part);

        let log_dir = wizard_log_dir();
        std::fs::create_dir_all(&log_dir)?;
        let path = log_dir.join(&filename);

        let format_name = match self.detected_format {
            Some(ImportFormat::Udif) | Some(ImportFormat::UdifCompressed) => "UDIF",
            Some(ImportFormat::Csv) | Some(ImportFormat::Unknown) | None => "CSV",
        };

        let mut content = format!(
            "=== zqlz Import Log ===\nStarted:  {}\nFormat:   {}\nSource:   {}\nTarget:   {} → {}\nDuration: {}\n\n--- Log ---\n",
            now.format("%Y-%m-%d %H:%M:%S"),
            format_name,
            source_label,
            driver_name,
            target_label,
            self.stats.elapsed_display(),
        );
        for msg in &self.log_messages {
            content.push_str(&format!(
                "{} [{}]  {}\n",
                msg.timestamp.format("%H:%M:%S"),
                log_level_label(msg.level),
                msg.message,
            ));
        }

        std::fs::write(&path, content)?;
        Ok(path)
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

    /// Validate that every target table config has a non-empty table name.
    ///
    /// Sets `target_table_validation_error` when any name is blank so the UI can
    /// display it inline, and clears it when all names are valid. Returns `true`
    /// if all names are non-empty and the wizard may advance.
    pub fn validate_target_tables(&mut self) -> bool {
        let all_valid = self
            .target_configs
            .iter()
            .all(|c| !c.target_table.trim().is_empty());
        if all_valid {
            self.target_table_validation_error = None;
        } else {
            self.target_table_validation_error =
                Some("All target table names must be filled in before continuing.".to_string());
        }
        all_valid
    }

    /// Validate that the field mappings for the currently selected source have at least
    /// one non-skipped column.
    ///
    /// Skipping every column would produce an import with no columns, which is always an
    /// error. The user must keep at least one column active.
    pub fn validate_field_mappings(&mut self) -> bool {
        let mappings = self
            .field_mappings
            .get(&self.selected_mapping_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        // When there are no mappings at all (e.g. UDIF import) there is nothing to validate.
        if mappings.is_empty() {
            self.field_mapping_validation_error = None;
            return true;
        }

        let has_active_column = mappings.iter().any(|m| !m.skip && !m.is_auto_increment);
        if has_active_column {
            self.field_mapping_validation_error = None;
        } else {
            self.field_mapping_validation_error =
                Some("At least one column must not be skipped before continuing.".to_string());
        }
        has_active_column
    }

    /// Build ImportOptions for the GenericImporter (UDIF import).
    ///
    /// Returns an error for import modes that have no backend implementation yet
    /// (Update, AppendUpdate, AppendWithoutUpdate, Delete). Returning an error here
    /// surfaces a meaningful message to the user instead of silently doing a plain
    /// append and duplicating rows.
    pub fn to_import_options(&self) -> anyhow::Result<crate::ImportOptions> {
        let if_exists = match self.import_mode {
            ImportMode::Append => crate::IfTableExists::Append,
            ImportMode::Copy => crate::IfTableExists::Replace,
            ImportMode::Update
            | ImportMode::AppendUpdate
            | ImportMode::AppendWithoutUpdate
            | ImportMode::Delete => {
                anyhow::bail!(
                    "Import mode '{}' is not yet supported. Please choose Append or Copy.",
                    self.import_mode.short_name()
                );
            }
        };

        Ok(crate::ImportOptions {
            if_exists,
            create_tables: self.udif_import_schema,
            import_data: self.udif_import_data,
            create_indexes: self.udif_import_indexes,
            create_foreign_keys: !self.advanced_settings.ignore_foreign_key,
            continue_on_error: self.advanced_settings.continue_on_error,
            ..Default::default()
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn default_state_with_mode(mode: ImportMode) -> ImportWizardState {
        let mut state = ImportWizardState::default();
        state.import_mode = mode;
        state
    }

    // -------------------------------------------------------------------------
    // add_file / format conflict tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn add_udif_then_csv_returns_err() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.udif.json"))
            .await
            .expect("first file should be accepted");
        let result = state.add_file(PathBuf::from("/tmp/data.csv")).await;
        assert!(
            result.is_err(),
            "adding a CSV after a UDIF file must return Err"
        );
        assert_eq!(state.sources.len(), 1, "rejected file must not be appended");
    }

    #[tokio::test]
    async fn add_two_csv_files_both_accepted() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/a.csv"))
            .await
            .expect("first CSV accepted");
        state
            .add_file(PathBuf::from("/tmp/b.csv"))
            .await
            .expect("second CSV of same format must be accepted");
        assert_eq!(state.sources.len(), 2);
        assert_eq!(state.detected_format, Some(ImportFormat::Csv));
    }

    #[tokio::test]
    async fn add_csv_then_udif_returns_err() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.csv"))
            .await
            .expect("first file should be accepted");
        let result = state.add_file(PathBuf::from("/tmp/data.udif.json")).await;
        assert!(result.is_err(), "adding UDIF after CSV must return Err");
        assert_eq!(state.sources.len(), 1);
    }

    #[tokio::test]
    async fn add_unknown_extension_file_accepted_alongside_csv() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/a.csv"))
            .await
            .expect("CSV accepted");
        // An unknown-format file should not trigger a conflict error.
        state
            .add_file(PathBuf::from("/tmp/supplementary.dat"))
            .await
            .expect("unknown-format file alongside CSV must be accepted");
        assert_eq!(state.sources.len(), 2);
        // detected_format stays Csv because the .dat file is Unknown format.
        assert_eq!(state.detected_format, Some(ImportFormat::Csv));
    }

    #[tokio::test]
    async fn remove_source_resets_format_when_all_files_removed() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.csv"))
            .await
            .expect("first CSV accepted");
        state.remove_source(0);
        assert_eq!(
            state.detected_format, None,
            "format must reset after removing the only file"
        );
        assert!(state.sources.is_empty());
    }

    #[tokio::test]
    async fn remove_source_clears_add_file_error() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.udif.json"))
            .await
            .expect("first file accepted");
        // Trigger a conflict error — we assert it is an error but don't need the message.
        assert!(
            state.add_file(PathBuf::from("/tmp/data.csv")).await.is_err(),
            "conflicting format must be rejected"
        );
        assert!(
            state.add_file_error.is_none(),
            "add_file_error is stored on the wizard, not on state"
        );
        // But after removing the UDIF file, re-adding CSV should work.
        state.remove_source(0);
        state
            .add_file(PathBuf::from("/tmp/data.csv"))
            .await
            .expect("CSV must be accepted after UDIF removed");
        assert_eq!(state.detected_format, Some(ImportFormat::Csv));
    }

    #[tokio::test]
    async fn remove_first_of_two_files_leaves_second() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/first.csv"))
            .await
            .expect("first CSV accepted");
        state
            .add_file(PathBuf::from("/tmp/second.csv"))
            .await
            .expect("second CSV accepted");

        state.remove_source(0);

        assert_eq!(state.sources.len(), 1);
        // source_name is the file stem (without extension).
        assert_eq!(state.sources[0].source_name, "second");
        assert_eq!(state.detected_format, Some(ImportFormat::Csv));
    }

    #[tokio::test]
    async fn remove_udif_then_add_csv_detects_csv_format() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/export.udif.json"))
            .await
            .expect("UDIF file accepted");

        state.remove_source(0);
        assert_eq!(
            state.detected_format, None,
            "format must reset after removing the only UDIF file"
        );

        // Adding a CSV after the UDIF is removed must succeed and detect Csv.
        state
            .add_file(PathBuf::from("/tmp/data.csv"))
            .await
            .expect("CSV must be accepted after UDIF removed");
        assert_eq!(
            state.detected_format,
            Some(ImportFormat::Csv),
            "detected_format must not be stuck as Udif after removal and re-add"
        );
    }

    // -------------------------------------------------------------------------
    // to_import_options / ImportMode tests
    // -------------------------------------------------------------------------

    #[test]
    fn to_import_options_append_maps_to_append() {
        let state = default_state_with_mode(ImportMode::Append);
        let options = state
            .to_import_options()
            .expect("Append should be supported");
        assert!(matches!(options.if_exists, crate::IfTableExists::Append));
    }

    #[test]
    fn to_import_options_copy_maps_to_replace() {
        let state = default_state_with_mode(ImportMode::Copy);
        let options = state.to_import_options().expect("Copy should be supported");
        assert!(matches!(options.if_exists, crate::IfTableExists::Replace));
    }

    #[test]
    fn to_import_options_update_returns_err() {
        let state = default_state_with_mode(ImportMode::Update);
        assert!(
            state.to_import_options().is_err(),
            "Update should return Err because it is not implemented"
        );
    }

    #[test]
    fn to_import_options_append_update_returns_err() {
        let state = default_state_with_mode(ImportMode::AppendUpdate);
        assert!(
            state.to_import_options().is_err(),
            "AppendUpdate should return Err because it is not implemented"
        );
    }

    #[test]
    fn to_import_options_append_without_update_returns_err() {
        let state = default_state_with_mode(ImportMode::AppendWithoutUpdate);
        assert!(
            state.to_import_options().is_err(),
            "AppendWithoutUpdate should return Err because it is not implemented"
        );
    }

    #[test]
    fn to_import_options_delete_returns_err() {
        let state = default_state_with_mode(ImportMode::Delete);
        assert!(
            state.to_import_options().is_err(),
            "Delete should return Err because it is not implemented"
        );
    }

    #[test]
    fn is_supported_only_for_append_and_copy() {
        assert!(ImportMode::Append.is_supported());
        assert!(ImportMode::Copy.is_supported());
        assert!(!ImportMode::Update.is_supported());
        assert!(!ImportMode::AppendUpdate.is_supported());
        assert!(!ImportMode::AppendWithoutUpdate.is_supported());
        assert!(!ImportMode::Delete.is_supported());
    }

    // -------------------------------------------------------------------------
    // ExportWizardState::to_export_options — include_data field tests
    // -------------------------------------------------------------------------

    #[test]
    fn export_wizard_state_defaults_include_data_true() {
        let state = ExportWizardState::default();
        assert!(
            state.include_data,
            "default state must include data (schema-only must be opt-in)"
        );
    }

    #[test]
    fn to_export_options_include_data_true_by_default() {
        let state = ExportWizardState::default();
        let opts = state.to_export_options();
        assert!(
            opts.include_data,
            "default to_export_options must include data"
        );
    }

    #[test]
    fn to_export_options_schema_only_when_include_data_false() {
        let mut state = ExportWizardState::default();
        state.include_data = false;
        let opts = state.to_export_options();
        assert!(
            !opts.include_data,
            "to_export_options must propagate include_data=false (schema-only mode)"
        );
    }

    #[test]
    fn to_export_options_include_schema_propagated() {
        // Disabling include_schema (data-only) must also be reflected
        let mut state = ExportWizardState::default();
        state.include_schema = false;
        let opts = state.to_export_options();
        assert!(!opts.include_schema);
        // include_data is still true (unrelated flag)
        assert!(opts.include_data);
    }

    // -------------------------------------------------------------------------
    // TargetTableConfig / validate_target_tables tests
    // -------------------------------------------------------------------------

    #[test]
    fn target_table_config_create_new_table_defaults_false() {
        let config = TargetTableConfig {
            source_index: 0,
            source_name: "my_table".to_string(),
            target_table: "my_table".to_string(),
            create_new_table: false,
        };
        assert!(!config.create_new_table);
    }

    #[test]
    fn validate_target_tables_passes_when_all_names_nonempty() {
        let mut state = ImportWizardState::default();
        state.target_configs.push(TargetTableConfig {
            source_index: 0,
            source_name: "a".to_string(),
            target_table: "table_a".to_string(),
            create_new_table: false,
        });
        state.target_configs.push(TargetTableConfig {
            source_index: 1,
            source_name: "b".to_string(),
            target_table: "table_b".to_string(),
            create_new_table: false,
        });
        assert!(
            state.validate_target_tables(),
            "should be valid when all names are non-empty"
        );
        assert!(state.target_table_validation_error.is_none());
    }

    #[test]
    fn validate_target_tables_fails_and_sets_error_when_name_empty() {
        let mut state = ImportWizardState::default();
        state.target_configs.push(TargetTableConfig {
            source_index: 0,
            source_name: "a".to_string(),
            target_table: "table_a".to_string(),
            create_new_table: false,
        });
        state.target_configs.push(TargetTableConfig {
            source_index: 1,
            source_name: "b".to_string(),
            target_table: "".to_string(), // blank — should fail
            create_new_table: false,
        });
        assert!(
            !state.validate_target_tables(),
            "should be invalid when any name is empty"
        );
        assert!(
            state.target_table_validation_error.is_some(),
            "error message must be set when validation fails"
        );
    }

    #[test]
    fn validate_target_tables_clears_error_after_fix() {
        let mut state = ImportWizardState::default();
        state.target_configs.push(TargetTableConfig {
            source_index: 0,
            source_name: "a".to_string(),
            target_table: "".to_string(),
            create_new_table: false,
        });
        assert!(!state.validate_target_tables());
        assert!(state.target_table_validation_error.is_some());

        // Fix the blank name and re-validate
        state.target_configs[0].target_table = "fixed_table".to_string();
        assert!(
            state.validate_target_tables(),
            "must pass after name is filled in"
        );
        assert!(
            state.target_table_validation_error.is_none(),
            "error must be cleared once all names are valid"
        );
    }

    #[test]
    fn validate_target_tables_passes_when_no_configs() {
        let mut state = ImportWizardState::default();
        // An import with no source files has no target configs; validation must not block
        assert!(state.validate_target_tables());
        assert!(state.target_table_validation_error.is_none());
    }

    // -------------------------------------------------------------------------
    // validate_field_mappings tests
    // -------------------------------------------------------------------------

    fn make_mapping(source_field: &str, skip: bool, is_auto_increment: bool) -> FieldMapping {
        FieldMapping {
            source_field: source_field.to_string(),
            target_field: source_field.to_string(),
            is_primary_key: false,
            skip,
            is_auto_increment,
        }
    }

    #[test]
    fn validate_field_mappings_passes_with_active_column() {
        let mut state = ImportWizardState::default();
        state.field_mappings.insert(
            0,
            vec![
                make_mapping("id", false, false),
                make_mapping("name", false, false),
            ],
        );
        assert!(state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_none());
    }

    #[test]
    fn validate_field_mappings_fails_when_all_skipped() {
        let mut state = ImportWizardState::default();
        state.field_mappings.insert(
            0,
            vec![
                make_mapping("id", true, false),
                make_mapping("name", true, false),
            ],
        );
        assert!(!state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_some());
    }

    #[test]
    fn validate_field_mappings_fails_when_all_auto_increment() {
        let mut state = ImportWizardState::default();
        // auto-increment columns are excluded from INSERT just like skipped ones;
        // if every column is auto-increment there is nothing to import.
        state
            .field_mappings
            .insert(0, vec![make_mapping("id", false, true)]);
        assert!(!state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_some());
    }

    #[test]
    fn validate_field_mappings_passes_when_no_mappings() {
        // UDIF imports may have no explicit field_mappings; validation must not block.
        let mut state = ImportWizardState::default();
        assert!(state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_none());
    }

    #[test]
    fn validate_field_mappings_clears_error_after_fix() {
        let mut state = ImportWizardState::default();
        state
            .field_mappings
            .insert(0, vec![make_mapping("id", true, false)]);
        assert!(!state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_some());

        // Un-skip the column
        state.field_mappings.get_mut(&0).unwrap()[0].skip = false;
        assert!(state.validate_field_mappings());
        assert!(state.field_mapping_validation_error.is_none());
    }

    // -------------------------------------------------------------------------
    // field_delimiter / TSV auto-detection tests  (ic-011)
    // -------------------------------------------------------------------------

    #[test]
    fn source_format_defaults_to_comma_delimiter() {
        let options = SourceFormatOptions::default();
        assert_eq!(
            options.field_delimiter, ',',
            "default delimiter must be comma for standard CSV"
        );
    }

    #[tokio::test]
    async fn add_tsv_file_auto_sets_tab_delimiter() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.tsv"))
            .await
            .expect("TSV file must be accepted");
        assert_eq!(
            state.source_format.field_delimiter, '\t',
            "adding a .tsv file must auto-set the delimiter to tab"
        );
    }

    #[tokio::test]
    async fn add_csv_file_leaves_comma_delimiter() {
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/data.csv"))
            .await
            .expect("CSV file must be accepted");
        assert_eq!(
            state.source_format.field_delimiter, ',',
            "adding a .csv file must not change the default comma delimiter"
        );
    }

    #[tokio::test]
    async fn add_second_tsv_does_not_override_manually_set_delimiter() {
        // Only the first file triggers auto-detection; subsequent files leave the
        // delimiter unchanged so a user-manual override is preserved.
        let mut state = ImportWizardState::default();
        state
            .add_file(PathBuf::from("/tmp/first.tsv"))
            .await
            .expect("first TSV accepted");
        assert_eq!(state.source_format.field_delimiter, '\t');

        // Simulate the user overriding the delimiter to pipe.
        state.source_format.field_delimiter = '|';

        state
            .add_file(PathBuf::from("/tmp/second.tsv"))
            .await
            .expect("second TSV accepted");
        assert_eq!(
            state.source_format.field_delimiter, '|',
            "delimiter set by the user must not be overwritten by subsequent file additions"
        );
    }

    #[test]
    fn custom_pipe_delimiter_roundtrips_through_serde() {
        // Verifies the #[serde(default)] attribute keeps existing documents
        // (which lack the field) deserialising to comma, while a document that
        // explicitly stores a pipe delimiter roundtrips correctly.
        let mut options = SourceFormatOptions::default();
        options.field_delimiter = '|';

        let json = serde_json::to_string(&options).expect("serialise");
        let restored: SourceFormatOptions = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(restored.field_delimiter, '|');
    }

    #[test]
    fn legacy_document_without_field_delimiter_deserialises_to_comma() {
        // A UDIF document created before ic-011 will not have the field_delimiter
        // key.  The #[serde(default)] annotation must supply ',' in that case.
        let json = r#"{"has_field_name_row":true,"field_name_row":1,"data_row_start":2,"data_row_end":null,"date_order":"DMY","date_time_order":"DateTime","date_delimiter":"/","use_year_delimiter":false,"year_delimiter":"/","time_delimiter":":","decimal_symbol":".","binary_encoding":"Hex"}"#;
        let options: SourceFormatOptions =
            serde_json::from_str(json).expect("deserialise legacy doc");
        assert_eq!(
            options.field_delimiter, ',',
            "legacy documents without field_delimiter must default to comma"
        );
    }

    // -------------------------------------------------------------------------
    // ExportWizardState::output_path — used by the overwrite-check (ic-022)
    // -------------------------------------------------------------------------

    #[test]
    fn output_path_udif_uses_correct_extension() {
        let mut state = ExportWizardState::default();
        state.output_folder = std::path::PathBuf::from("/tmp");
        state.output_filename = "my_export".to_string();
        state.export_format = ExportFormat::Udif;
        state.add_timestamp = false;

        let path = state.output_path();
        assert_eq!(path, std::path::PathBuf::from("/tmp/my_export.udif.json"));
    }

    #[test]
    fn output_path_udif_compressed_uses_correct_extension() {
        let mut state = ExportWizardState::default();
        state.output_folder = std::path::PathBuf::from("/tmp");
        state.output_filename = "db_backup".to_string();
        state.export_format = ExportFormat::UdifCompressed;
        state.add_timestamp = false;

        let path = state.output_path();
        assert!(
            path.to_string_lossy().ends_with(".udif.json.gz"),
            "compressed UDIF must use .udif.json.gz extension, got: {}",
            path.display()
        );
    }

    #[test]
    fn output_path_with_timestamp_contains_datetime_component() {
        let mut state = ExportWizardState::default();
        state.output_folder = std::path::PathBuf::from("/tmp");
        state.output_filename = "snap".to_string();
        state.export_format = ExportFormat::Udif;
        state.add_timestamp = true;

        let path = state.output_path();
        let name = path.file_name().unwrap().to_string_lossy();
        // Timestamp format is `%Y%m%d_%H%M%S` — always 15 extra chars after the base name.
        assert!(
            name.starts_with("snap_"),
            "timestamped path must start with base filename, got: {}",
            name
        );
        assert!(
            name.len() > "snap_.udif.json".len(),
            "timestamped path must be longer than non-timestamped, got: {}",
            name
        );
    }

    // -------------------------------------------------------------------------
    // ExportWizardStep::display_name tests
    // -------------------------------------------------------------------------

    #[test]
    fn export_wizard_step_display_name_non_empty() {
        for step in ExportWizardStep::all() {
            let name = step.display_name();
            assert!(
                !name.is_empty(),
                "display_name for {:?} must not be empty",
                step
            );
        }
    }

    #[test]
    fn export_wizard_step_all_covers_every_variant() {
        // Exhaustive match ensures this test breaks at compile time if a new variant is added
        // without updating all() or display_name().
        let steps = ExportWizardStep::all();
        assert_eq!(
            steps.len(),
            4,
            "all() must return all 4 ExportWizardStep variants"
        );
        for (i, step) in steps.iter().enumerate() {
            assert_eq!(
                step.index(),
                i,
                "all() must be ordered by index, mismatch at position {}",
                i
            );
        }
    }

    // -------------------------------------------------------------------------
    // ImportFormat::detect — content-sniff tests  (ic-020)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn detect_udif_json_by_extension_without_opening_file() {
        // .udif.json is unambiguous by extension — detect() must return Udif without
        // attempting to open the file (the path does not exist on disk).
        let format = ImportFormat::detect(std::path::Path::new("/nonexistent/export.udif.json")).await;
        assert_eq!(format, ImportFormat::Udif);
    }

    #[tokio::test]
    async fn detect_plain_json_with_udif_content_returns_udif() {
        // A .json file whose content contains the UDIF marker keys must be detected as Udif.
        use std::io::Write;
        let mut temp = tempfile::NamedTempFile::new().expect("temp file");
        // Minimal UDIF-shaped JSON: top-level "version" and "schema" keys.
        write!(
            temp,
            r#"{{"version": 1, "schema": {{"tables": {{}}}}, "data": {{}}}}"#
        )
        .expect("write temp");
        let path = temp.path().to_path_buf();
        // Rename to .json so detect() treats it as a plain JSON candidate for sniffing.
        let json_path = path.with_extension("json");
        std::fs::copy(&path, &json_path).expect("copy to .json");

        let format = ImportFormat::detect(&json_path).await;
        std::fs::remove_file(&json_path).ok();

        assert_eq!(format, ImportFormat::Udif);
    }

    #[tokio::test]
    async fn detect_plain_json_with_non_udif_content_returns_unknown() {
        // A .json file whose content is a plain JSON array (no UDIF marker keys) must return Unknown.
        use std::io::Write;
        let mut temp = tempfile::NamedTempFile::new().expect("temp file");
        write!(temp, r#"[{{"id": 1, "name": "Alice"}}, {{"id": 2, "name": "Bob"}}]"#)
            .expect("write temp");
        let path = temp.path().to_path_buf();
        let json_path = path.with_extension("json");
        std::fs::copy(&path, &json_path).expect("copy to .json");

        let format = ImportFormat::detect(&json_path).await;
        std::fs::remove_file(&json_path).ok();

        assert_eq!(format, ImportFormat::Unknown);
    }
}
