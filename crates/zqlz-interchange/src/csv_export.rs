//! CSV Export functionality
//!
//! This module provides CSV export functionality that integrates with the Export Wizard UI.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use zqlz_core::Connection;

use crate::widgets::{
    ExportWizardState, FieldDelimiter, LogLevel, RecordDelimiter, TableExportConfig, TextQualifier,
};

/// Errors during CSV export
#[derive(Debug, Error)]
pub enum CsvExportError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("No tables selected for export")]
    NoTablesSelected,

    #[error("Export cancelled")]
    Cancelled,
}

/// Progress callback for export operations
pub type CsvExportProgressCallback = Box<dyn Fn(CsvExportProgress) + Send + Sync>;

/// Export progress information
#[derive(Debug, Clone)]
pub struct CsvExportProgress {
    /// Current table being exported
    pub current_table: String,
    /// Current table index (1-based)
    pub table_index: usize,
    /// Total number of tables
    pub total_tables: usize,
    /// Rows exported for current table
    pub rows_exported: u64,
    /// Total rows in current table (if known)
    pub total_rows: Option<u64>,
    /// Log level for this message
    pub log_level: LogLevel,
    /// Log message
    pub message: String,
}

/// CSV Exporter that works with ExportWizardState
pub struct CsvExporter {
    connection: Arc<dyn Connection>,
    state: ExportWizardState,
    progress_callback: Option<CsvExportProgressCallback>,
}

impl CsvExporter {
    /// Create a new CSV exporter
    pub fn new(connection: Arc<dyn Connection>, state: ExportWizardState) -> Self {
        Self {
            connection,
            state,
            progress_callback: None,
        }
    }

    /// Set progress callback
    pub fn with_progress_callback(mut self, callback: CsvExportProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    fn report_progress(&self, progress: CsvExportProgress) {
        if let Some(ref callback) = self.progress_callback {
            callback(progress);
        }
    }

    /// Execute the export
    pub async fn export(&self) -> Result<Vec<PathBuf>, CsvExportError> {
        let selected_tables: Vec<&TableExportConfig> = self.state.selected_tables();

        if selected_tables.is_empty() {
            return Err(CsvExportError::NoTablesSelected);
        }

        let mut output_files = Vec::new();
        let total_tables = selected_tables.len();

        for (idx, table_config) in selected_tables.iter().enumerate() {
            let table_index = idx + 1;

            self.report_progress(CsvExportProgress {
                current_table: table_config.table_name.clone(),
                table_index,
                total_tables,
                rows_exported: 0,
                total_rows: None,
                log_level: LogLevel::Info,
                message: format!("Exporting table: {}", table_config.table_name),
            });

            let output_path = self
                .export_table(table_config, table_index, total_tables)
                .await?;
            output_files.push(output_path);

            self.report_progress(CsvExportProgress {
                current_table: table_config.table_name.clone(),
                table_index,
                total_tables,
                rows_exported: 0,
                total_rows: None,
                log_level: LogLevel::Success,
                message: format!("Completed: {}", table_config.table_name),
            });
        }

        self.report_progress(CsvExportProgress {
            current_table: String::new(),
            table_index: total_tables,
            total_tables,
            rows_exported: 0,
            total_rows: None,
            log_level: LogLevel::Success,
            message: format!("Export complete. {} file(s) created.", output_files.len()),
        });

        Ok(output_files)
    }

    async fn export_table(
        &self,
        table_config: &TableExportConfig,
        table_index: usize,
        total_tables: usize,
    ) -> Result<PathBuf, CsvExportError> {
        // Build output path
        let output_path = self.state.output_folder.join(&table_config.output_filename);

        // Get selected columns
        let selected_columns: Vec<&str> = table_config
            .columns
            .iter()
            .filter(|c| c.selected)
            .map(|c| c.name.as_str())
            .collect();

        // Build query
        let column_list = if selected_columns.is_empty() {
            "*".to_string()
        } else {
            selected_columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let sql = format!(
            "SELECT {} FROM \"{}\"",
            column_list, table_config.table_name
        );

        // Execute query
        let result = self
            .connection
            .query(&sql, &[])
            .await
            .map_err(|e| CsvExportError::QueryError(e.to_string()))?;

        // Write to file
        let file = std::fs::File::create(&output_path)?;
        let mut writer = std::io::BufWriter::new(file);

        let format = &self.state.csv_options;
        let field_delim = format.field_delimiter.as_char();
        let record_delim = format.record_delimiter.as_str();
        let qualifier = format.text_qualifier.as_char();

        // Write header if configured
        if format.include_headers {
            let header_line = result
                .columns
                .iter()
                .map(|c| self.qualify_value(&c.name, qualifier))
                .collect::<Vec<_>>()
                .join(&field_delim.to_string());
            writer.write_all(header_line.as_bytes())?;
            writer.write_all(record_delim.as_bytes())?;
        }

        // Write data rows
        let total_rows = result.rows.len();
        for (row_idx, row) in result.rows.iter().enumerate() {
            let row_line = row
                .values
                .iter()
                .map(|v| {
                    if v.is_null() {
                        String::new()
                    } else {
                        let s = v.to_string();
                        if format.blank_if_zero && s == "0" {
                            String::new()
                        } else {
                            self.qualify_value(&s, qualifier)
                        }
                    }
                })
                .collect::<Vec<_>>()
                .join(&field_delim.to_string());

            writer.write_all(row_line.as_bytes())?;
            writer.write_all(record_delim.as_bytes())?;

            // Report progress every 1000 rows
            if (row_idx + 1) % 1000 == 0 {
                self.report_progress(CsvExportProgress {
                    current_table: table_config.table_name.clone(),
                    table_index,
                    total_tables,
                    rows_exported: (row_idx + 1) as u64,
                    total_rows: Some(total_rows as u64),
                    log_level: LogLevel::Info,
                    message: format!(
                        "Exported {}/{} rows from {}",
                        row_idx + 1,
                        total_rows,
                        table_config.table_name
                    ),
                });
            }
        }

        writer.flush()?;

        self.report_progress(CsvExportProgress {
            current_table: table_config.table_name.clone(),
            table_index,
            total_tables,
            rows_exported: total_rows as u64,
            total_rows: Some(total_rows as u64),
            log_level: LogLevel::Info,
            message: format!("Exported {} rows to {}", total_rows, output_path.display()),
        });

        Ok(output_path)
    }

    fn qualify_value(&self, value: &str, qualifier: Option<char>) -> String {
        match qualifier {
            Some(q) => {
                // Escape the qualifier character if it appears in the value
                let escaped = value.replace(q, &format!("{}{}", q, q));
                format!("{}{}{}", q, escaped, q)
            }
            None => value.to_string(),
        }
    }
}

// Helper implementations for format types
impl FieldDelimiter {
    pub fn as_char(&self) -> char {
        match self {
            FieldDelimiter::Comma => ',',
            FieldDelimiter::Tab => '\t',
            FieldDelimiter::Semicolon => ';',
            FieldDelimiter::Space => ' ',
            FieldDelimiter::Pipe => '|',
        }
    }
}

impl RecordDelimiter {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordDelimiter::CrLf => "\r\n",
            RecordDelimiter::Cr => "\r",
            RecordDelimiter::Lf => "\n",
        }
    }
}

impl TextQualifier {
    pub fn as_char(&self) -> Option<char> {
        match self {
            TextQualifier::DoubleQuote => Some('"'),
            TextQualifier::SingleQuote => Some('\''),
            TextQualifier::None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_delimiter() {
        assert_eq!(FieldDelimiter::Comma.as_char(), ',');
        assert_eq!(FieldDelimiter::Tab.as_char(), '\t');
        assert_eq!(FieldDelimiter::Semicolon.as_char(), ';');
    }

    #[test]
    fn test_record_delimiter() {
        assert_eq!(RecordDelimiter::CrLf.as_str(), "\r\n");
        assert_eq!(RecordDelimiter::Lf.as_str(), "\n");
    }

    #[test]
    fn test_text_qualifier() {
        assert_eq!(TextQualifier::DoubleQuote.as_char(), Some('"'));
        assert_eq!(TextQualifier::None.as_char(), None);
    }
}
