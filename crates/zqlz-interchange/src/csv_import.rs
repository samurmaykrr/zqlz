//! CSV Import functionality
//!
//! This module provides CSV import functionality that integrates with the Import Wizard UI.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use zqlz_core::Connection;

use crate::widgets::{FieldMapping, ImportAdvancedSettings, ImportWizardState, LogLevel};

/// Errors during CSV import
#[derive(Debug, Error)]
pub enum CsvImportError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Parse error at line {line}: {message}")]
    ParseError { line: usize, message: String },

    #[error("No sources configured for import")]
    NoSourcesConfigured,

    #[error("Source file not found: {0}")]
    SourceNotFound(String),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Import cancelled")]
    Cancelled,

    #[error("Validation error: {0}")]
    ValidationError(String),
}

/// Progress callback for import operations
pub type CsvImportProgressCallback = Box<dyn Fn(CsvImportProgress) + Send + Sync>;

/// Import progress information
#[derive(Debug, Clone)]
pub struct CsvImportProgress {
    /// Current source being imported
    pub current_source: String,
    /// Current source index (1-based)
    pub source_index: usize,
    /// Total number of sources
    pub total_sources: usize,
    /// Rows processed
    pub rows_processed: u64,
    /// Rows added
    pub rows_added: u64,
    /// Rows updated
    pub rows_updated: u64,
    /// Rows deleted
    pub rows_deleted: u64,
    /// Error count
    pub error_count: u64,
    /// Log level for this message
    pub log_level: LogLevel,
    /// Log message
    pub message: String,
}

/// Result of a single import operation
#[derive(Debug, Clone, Default)]
pub struct CsvImportResult {
    /// Number of rows processed
    pub rows_processed: u64,
    /// Number of rows added
    pub rows_added: u64,
    /// Number of rows updated
    pub rows_updated: u64,
    /// Number of rows deleted
    pub rows_deleted: u64,
    /// Number of errors
    pub error_count: u64,
    /// Error messages (limited to first 100)
    pub errors: Vec<String>,
}

impl CsvImportResult {
    fn add_error(&mut self, error: String) {
        self.error_count += 1;
        if self.errors.len() < 100 {
            self.errors.push(error);
        }
    }
}

/// CSV Importer that works with ImportWizardState
pub struct CsvImporter {
    connection: Arc<dyn Connection>,
    state: ImportWizardState,
    progress_callback: Option<CsvImportProgressCallback>,
}

impl CsvImporter {
    /// Create a new CSV importer
    pub fn new(connection: Arc<dyn Connection>, state: ImportWizardState) -> Self {
        Self {
            connection,
            state,
            progress_callback: None,
        }
    }

    /// Set progress callback
    pub fn with_progress_callback(mut self, callback: CsvImportProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    fn report_progress(&self, progress: CsvImportProgress) {
        if let Some(ref callback) = self.progress_callback {
            callback(progress);
        }
    }

    /// Execute the import
    pub async fn import(&self) -> Result<CsvImportResult, CsvImportError> {
        if self.state.sources.is_empty() {
            return Err(CsvImportError::NoSourcesConfigured);
        }

        let total_sources = self.state.sources.len();
        let mut total_result = CsvImportResult::default();

        for (idx, source) in self.state.sources.iter().enumerate() {
            let source_index = idx + 1;

            self.report_progress(CsvImportProgress {
                current_source: source.source_name.clone(),
                source_index,
                total_sources,
                rows_processed: 0,
                rows_added: 0,
                rows_updated: 0,
                rows_deleted: 0,
                error_count: 0,
                log_level: LogLevel::Info,
                message: format!("Importing from: {}", source.source_name),
            });

            // Get target table config for this source
            let target_config = self
                .state
                .target_configs
                .iter()
                .find(|c| c.source_index == idx);

            let target_table = match target_config {
                Some(config) => config.target_table.clone(),
                None => {
                    // Use source name as table name if no config
                    source
                        .source_name
                        .trim_end_matches(".csv")
                        .trim_end_matches(".txt")
                        .to_string()
                }
            };

            // Get field mappings for this source
            let field_mappings = self
                .state
                .field_mappings
                .get(&idx)
                .cloned()
                .unwrap_or_default();

            // Import based on source type
            let result = match source.path() {
                Some(path) => {
                    self.import_file(
                        path,
                        &target_table,
                        &field_mappings,
                        source_index,
                        total_sources,
                    )
                    .await?
                }
                None => {
                    // URL source - not implemented yet
                    self.report_progress(CsvImportProgress {
                        current_source: source.source_name.clone(),
                        source_index,
                        total_sources,
                        rows_processed: 0,
                        rows_added: 0,
                        rows_updated: 0,
                        rows_deleted: 0,
                        error_count: 0,
                        log_level: LogLevel::Warning,
                        message: "URL import not yet implemented".to_string(),
                    });
                    CsvImportResult::default()
                }
            };

            // Accumulate results
            total_result.rows_processed += result.rows_processed;
            total_result.rows_added += result.rows_added;
            total_result.rows_updated += result.rows_updated;
            total_result.rows_deleted += result.rows_deleted;
            total_result.error_count += result.error_count;
            total_result.errors.extend(result.errors);

            self.report_progress(CsvImportProgress {
                current_source: source.source_name.clone(),
                source_index,
                total_sources,
                rows_processed: result.rows_processed,
                rows_added: result.rows_added,
                rows_updated: result.rows_updated,
                rows_deleted: result.rows_deleted,
                error_count: result.error_count,
                log_level: LogLevel::Success,
                message: format!(
                    "Completed: {} ({} added, {} updated, {} errors)",
                    source.source_name, result.rows_added, result.rows_updated, result.error_count
                ),
            });
        }

        self.report_progress(CsvImportProgress {
            current_source: String::new(),
            source_index: total_sources,
            total_sources,
            rows_processed: total_result.rows_processed,
            rows_added: total_result.rows_added,
            rows_updated: total_result.rows_updated,
            rows_deleted: total_result.rows_deleted,
            error_count: total_result.error_count,
            log_level: if total_result.error_count > 0 {
                LogLevel::Warning
            } else {
                LogLevel::Success
            },
            message: format!(
                "Import complete. {} processed, {} added, {} updated, {} errors",
                total_result.rows_processed,
                total_result.rows_added,
                total_result.rows_updated,
                total_result.error_count
            ),
        });

        Ok(total_result)
    }

    async fn import_file(
        &self,
        path: &PathBuf,
        target_table: &str,
        field_mappings: &[FieldMapping],
        source_index: usize,
        total_sources: usize,
    ) -> Result<CsvImportResult, CsvImportError> {
        let file = std::fs::File::open(path)
            .map_err(|e| CsvImportError::SourceNotFound(format!("{}: {}", path.display(), e)))?;
        let reader = BufReader::new(file);

        let format = &self.state.source_format;
        let advanced = &self.state.advanced_settings;

        // Determine field delimiter (default to comma for CSV)
        let delimiter = ','; // TODO: Could be configurable in source_format

        let mut result = CsvImportResult::default();
        let mut lines = reader.lines();
        let mut line_number = 0;
        let mut headers: Vec<String> = Vec::new();

        // Skip to field name row and read headers
        let field_name_row = if format.has_field_name_row {
            format.field_name_row
        } else {
            0 // No header row, will skip header processing
        };
        let data_start_row = format.data_row_start;
        let data_end_row = format.data_row_end;

        while let Some(line_result) = lines.next() {
            line_number += 1;
            let line = line_result?;

            if line_number == field_name_row {
                headers = self.parse_csv_line(&line, delimiter);
                continue;
            }

            if line_number < data_start_row {
                continue;
            }

            if let Some(end) = data_end_row {
                if line_number > end {
                    break;
                }
            }

            // Parse data row
            let values = self.parse_csv_line(&line, delimiter);

            if values.is_empty() || (values.len() == 1 && values[0].is_empty()) {
                continue; // Skip empty lines
            }

            // Build INSERT statement
            match self
                .insert_row(target_table, &headers, &values, field_mappings, advanced)
                .await
            {
                Ok(inserted) => {
                    result.rows_processed += 1;
                    if inserted {
                        result.rows_added += 1;
                    }
                }
                Err(e) => {
                    result.rows_processed += 1;
                    result.add_error(format!("Line {}: {}", line_number, e));

                    if !advanced.continue_on_error {
                        return Err(CsvImportError::QueryError(e));
                    }
                }
            }

            // Report progress every 100 rows
            if result.rows_processed % 100 == 0 {
                self.report_progress(CsvImportProgress {
                    current_source: path.display().to_string(),
                    source_index,
                    total_sources,
                    rows_processed: result.rows_processed,
                    rows_added: result.rows_added,
                    rows_updated: result.rows_updated,
                    rows_deleted: result.rows_deleted,
                    error_count: result.error_count,
                    log_level: LogLevel::Info,
                    message: format!("Processed {} rows...", result.rows_processed),
                });
            }
        }

        Ok(result)
    }

    fn parse_csv_line(&self, line: &str, delimiter: char) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if in_quotes {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        // Escaped quote
                        chars.next();
                        current.push('"');
                    } else {
                        // End of quoted field
                        in_quotes = false;
                    }
                } else {
                    current.push(c);
                }
            } else if c == '"' {
                in_quotes = true;
            } else if c == delimiter {
                result.push(current.trim().to_string());
                current = String::new();
            } else {
                current.push(c);
            }
        }

        result.push(current.trim().to_string());
        result
    }

    async fn insert_row(
        &self,
        table_name: &str,
        headers: &[String],
        values: &[String],
        field_mappings: &[FieldMapping],
        advanced: &ImportAdvancedSettings,
    ) -> Result<bool, String> {
        // Build column names and values based on mappings
        let mut column_names = Vec::new();
        let mut value_strings = Vec::new();

        for (idx, header) in headers.iter().enumerate() {
            // Check if this field should be skipped
            let mapping = field_mappings.iter().find(|m| &m.source_field == header);

            if let Some(m) = mapping {
                if m.skip {
                    continue;
                }
            }

            // Get target column name
            let target_column = mapping
                .map(|m| m.target_field.clone())
                .unwrap_or_else(|| header.clone());

            column_names.push(format!("\"{}\"", target_column));

            // Get value
            let value = values.get(idx).map(|s| s.as_str()).unwrap_or("");

            // Format value for SQL
            let sql_value = if value.is_empty() {
                if advanced.empty_string_as_null {
                    "NULL".to_string()
                } else {
                    "''".to_string()
                }
            } else {
                // Escape single quotes
                format!("'{}'", value.replace('\'', "''"))
            };

            value_strings.push(sql_value);
        }

        if column_names.is_empty() {
            return Ok(false);
        }

        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            value_strings.join(", ")
        );

        self.connection
            .execute(&sql, &[])
            .await
            .map_err(|e| e.to_string())?;

        Ok(true)
    }
}

/// Parse a file to preview its contents
pub fn preview_csv_file(path: &PathBuf, max_rows: usize) -> Result<CsvPreview, CsvImportError> {
    let file = std::fs::File::open(path)
        .map_err(|e| CsvImportError::SourceNotFound(format!("{}: {}", path.display(), e)))?;
    let reader = BufReader::new(file);

    let mut headers = Vec::new();
    let mut rows = Vec::new();
    let delimiter = ',';

    for (idx, line_result) in reader.lines().enumerate() {
        let line = line_result?;

        let values = parse_simple_csv_line(&line, delimiter);

        if idx == 0 {
            headers = values;
        } else if idx <= max_rows {
            rows.push(values);
        } else {
            break;
        }
    }

    Ok(CsvPreview { headers, rows })
}

fn parse_simple_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == delimiter {
            result.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
    }

    result.push(current.trim().to_string());
    result
}

/// Preview of a CSV file
#[derive(Debug, Clone)]
pub struct CsvPreview {
    /// Column headers
    pub headers: Vec<String>,
    /// First N rows of data
    pub rows: Vec<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_csv() {
        let line = "hello,world,test";
        let result = parse_simple_csv_line(line, ',');
        assert_eq!(result, vec!["hello", "world", "test"]);
    }

    #[test]
    fn test_parse_quoted_csv() {
        let line = r#""hello, world","test",value"#;
        let result = parse_simple_csv_line(line, ',');
        assert_eq!(result, vec!["hello, world", "test", "value"]);
    }

    #[test]
    fn test_parse_escaped_quotes() {
        let line = r#""hello ""world""","test""#;
        let result = parse_simple_csv_line(line, ',');
        assert_eq!(result, vec![r#"hello "world""#, "test"]);
    }
}
