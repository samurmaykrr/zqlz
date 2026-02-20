//! CSV Import functionality
//!
//! This module provides CSV import functionality that integrates with the Import Wizard UI.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use zqlz_core::Connection;

use crate::widgets::{FieldMapping, ImportAdvancedSettings, ImportMode, ImportWizardState, LogLevel};

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

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Import mode not supported: {0}")]
    UnsupportedMode(String),

    #[error("URL import sources are not yet supported: '{0}'")]
    UrlSourceNotSupported(String),
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

    /// Quote an identifier using the driver's quoting style.
    ///
    /// MySQL uses backticks; all other supported drivers (PostgreSQL, SQLite) use
    /// double-quotes. This keeps generated DDL valid without pulling in the full
    /// GenericImporter machinery.
    fn quote_identifier(&self, name: &str) -> String {
        if self.connection.driver_name() == "mysql" {
            format!("`{}`", name.replace('`', "``"))
        } else {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }

    /// Returns true if a table with the given name exists on the target connection.
    async fn table_exists(&self, table_name: &str) -> Result<bool, CsvImportError> {
        let schema = self
            .connection
            .as_schema_introspection()
            .ok_or_else(|| CsvImportError::SchemaError("Schema introspection not supported".into()))?;
        let tables = schema
            .list_tables(None)
            .await
            .map_err(|e| CsvImportError::SchemaError(e.to_string()))?;
        Ok(tables.iter().any(|t| t.name == table_name))
    }

    /// Returns true if the table exists and contains at least one row.
    ///
    /// Used for the `Skip` import mode: a non-empty table is left untouched.
    async fn table_has_rows(&self, table_name: &str) -> Result<bool, CsvImportError> {
        let quoted = self.quote_identifier(table_name);
        let sql = format!("SELECT 1 FROM {} LIMIT 1", quoted);
        let rows = self
            .connection
            .query(&sql, &[])
            .await
            .map_err(|e| CsvImportError::QueryError(e.to_string()))?;
        Ok(!rows.rows.is_empty())
    }

    /// Applies the pre-import table preparation dictated by `state.import_mode`.
    ///
    /// - `Copy`  (Replace): clears the table via `DELETE FROM` so inserts start from a clean
    ///   state. We use `DELETE FROM` rather than `TRUNCATE TABLE` for maximum driver
    ///   compatibility (SQLite has no TRUNCATE).  The table must already exist; if it does
    ///   not, the method is a no-op (a subsequent CREATE TABLE or the insert itself will
    ///   surface the error with a clearer message).
    /// - `Append`: no-op; rows will be appended to whatever is already in the table.
    /// - `Skip`:   returns `true` to tell the caller to skip this source entirely when the
    ///   table already contains rows.
    /// - Unsupported modes return `Err(UnsupportedMode)` immediately so the error surfaces
    ///   to the user rather than silently doing the wrong thing.
    ///
    /// Returns `Ok(true)` if the source should be skipped, `Ok(false)` to proceed normally.
    async fn prepare_target_table(
        &self,
        table_name: &str,
    ) -> Result<bool, CsvImportError> {
        match self.state.import_mode {
            ImportMode::Append => Ok(false),
            ImportMode::Copy => {
                if self.table_exists(table_name).await? {
                    let sql = format!("DELETE FROM {}", self.quote_identifier(table_name));
                    self.connection
                        .execute(&sql, &[])
                        .await
                        .map_err(|e| CsvImportError::QueryError(e.to_string()))?;
                }
                Ok(false)
            }
            ImportMode::AppendWithoutUpdate => {
                // Skip source if the table already has rows; append if it is empty.
                if self.table_exists(table_name).await? && self.table_has_rows(table_name).await? {
                    return Ok(true);
                }
                Ok(false)
            }
            ImportMode::Update
            | ImportMode::AppendUpdate
            | ImportMode::Delete => {
                Err(CsvImportError::UnsupportedMode(format!(
                    "Import mode '{}' is not yet supported for CSV imports. \
                     Please choose Append or Copy.",
                    self.state.import_mode.short_name()
                )))
            }
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

            // If the user requested table creation, generate and execute a CREATE TABLE
            // before inserting any rows. The table DDL is built from the CSV header row
            // so we need to peek at the file independently of the main import loop.
            let wants_create = target_config.map(|c| c.create_new_table).unwrap_or(false);
            if wants_create {
                if let Some(path) = source.path() {
                    let headers = self.read_csv_headers(path)?;
                    if !headers.is_empty() {
                        let create_sql =
                            generate_create_table_sql(&target_table, &headers, &field_mappings);
                        self.connection
                            .execute(&create_sql, &[])
                            .await
                            .map_err(|e| CsvImportError::QueryError(e.to_string()))?;
                    }
                }
            }

            // Apply the import mode before writing any rows.  For Copy this clears the
            // existing rows; for AppendWithoutUpdate this skips the source entirely when
            // the table already has data; unsupported modes return an early error.
            // We run this after CREATE TABLE so that clearing an existing table and then
            // re-creating it does not accidentally error on a missing table.
            let skip_source = self.prepare_target_table(&target_table).await?;

            // Import based on source type
            let result = if skip_source {
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
                    message: format!(
                        "Skipped '{}': table '{}' already contains rows",
                        source.source_name, target_table
                    ),
                });
                CsvImportResult::default()
            } else {
                match source.path() {
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
                    // URL sources require an HTTP download step that is not yet implemented.
                    // Returning an error here ensures the failure is visible to the caller
                    // rather than silently producing an empty result.
                    let url = source
                        .url()
                        .unwrap_or("<unknown>")
                        .to_string();
                    return Err(CsvImportError::UrlSourceNotSupported(url));
                }
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

        let delimiter = format.field_delimiter;

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

    /// Read only the header row from a CSV file, returning column names.
    ///
    /// Used before `import_file` when `create_new_table` is true so that we can build
    /// the CREATE TABLE DDL without duplicating the full parse loop.
    fn read_csv_headers(&self, path: &PathBuf) -> Result<Vec<String>, CsvImportError> {
        let file = std::fs::File::open(path)
            .map_err(|e| CsvImportError::SourceNotFound(format!("{}: {}", path.display(), e)))?;
        let reader = BufReader::new(file);
        let format = &self.state.source_format;

        if !format.has_field_name_row {
            return Ok(Vec::new());
        }

        let field_name_row = format.field_name_row;
        let delimiter = format.field_delimiter;
        for (line_number, line_result) in reader.lines().enumerate() {
            let line = line_result?;
            if line_number + 1 == field_name_row {
                return Ok(self.parse_csv_line(&line, delimiter));
            }
        }

        Ok(Vec::new())
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
                // Skip columns the user explicitly excluded, and skip auto-increment
                // columns so the database can assign the value via its own sequence.
                // Supplying an explicit value for a serial/AUTO_INCREMENT column causes
                // duplicate-key errors once the sequence counter catches up.
                if m.skip || m.is_auto_increment {
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

/// Build a `CREATE TABLE IF NOT EXISTS` SQL statement from CSV headers and field mappings.
///
/// All columns are created as `TEXT` — CSV has no type information, so TEXT is the safest
/// universal choice. Columns whose `FieldMapping::is_primary_key` is true are declared with
/// `PRIMARY KEY`. When multiple columns are marked as primary key a composite PK constraint
/// is used instead of per-column declarations so the DDL is valid SQL.
///
/// Columns whose `FieldMapping::skip` or `FieldMapping::is_auto_increment` is true are
/// excluded from the table definition, matching the INSERT behaviour.
pub fn generate_create_table_sql(
    table_name: &str,
    headers: &[String],
    field_mappings: &[FieldMapping],
) -> String {
    // Determine which columns to include and whether each is a PK column.
    let active_columns: Vec<(&str, bool)> = headers
        .iter()
        .filter_map(|header| {
            let mapping = field_mappings.iter().find(|m| &m.source_field == header);
            let skip = mapping.map(|m| m.skip || m.is_auto_increment).unwrap_or(false);
            if skip {
                return None;
            }
            let target = mapping
                .map(|m| m.target_field.as_str())
                .unwrap_or(header.as_str());
            let is_pk = mapping.map(|m| m.is_primary_key).unwrap_or(false);
            Some((target, is_pk))
        })
        .collect();

    let pk_columns: Vec<&str> = active_columns
        .iter()
        .filter_map(|(col, is_pk)| if *is_pk { Some(*col) } else { None })
        .collect();

    let mut column_defs: Vec<String> = active_columns
        .iter()
        .map(|(col, is_pk)| {
            // Use inline PRIMARY KEY only when there is exactly one PK column;
            // otherwise the composite constraint below is used.
            if *is_pk && pk_columns.len() == 1 {
                format!("    \"{}\" TEXT PRIMARY KEY", col)
            } else {
                format!("    \"{}\" TEXT", col)
            }
        })
        .collect();

    if pk_columns.len() > 1 {
        let pk_list = pk_columns
            .iter()
            .map(|col| format!("\"{}\"", col))
            .collect::<Vec<_>>()
            .join(", ");
        column_defs.push(format!("    PRIMARY KEY ({})", pk_list));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}\n)",
        table_name,
        column_defs.join(",\n")
    )
}

/// Parse a file to preview its contents
pub fn preview_csv_file(
    path: &PathBuf,
    max_rows: usize,
    field_delimiter: char,
) -> Result<CsvPreview, CsvImportError> {
    let file = std::fs::File::open(path)
        .map_err(|e| CsvImportError::SourceNotFound(format!("{}: {}", path.display(), e)))?;
    let reader = BufReader::new(file);

    let mut headers = Vec::new();
    let mut rows = Vec::new();

    for (idx, line_result) in reader.lines().enumerate() {
        let line = line_result?;

        let values = parse_simple_csv_line(&line, field_delimiter);

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

    // -------------------------------------------------------------------------
    // generate_create_table_sql tests
    // -------------------------------------------------------------------------

    fn make_mapping(source: &str, target: &str, is_pk: bool, skip: bool, auto_inc: bool) -> FieldMapping {
        FieldMapping {
            source_field: source.to_string(),
            target_field: target.to_string(),
            is_primary_key: is_pk,
            skip,
            is_auto_increment: auto_inc,
        }
    }

    #[test]
    fn generate_create_table_sql_no_pk() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let mappings: Vec<FieldMapping> = Vec::new();
        let sql = generate_create_table_sql("users", &headers, &mappings);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS \"users\""));
        assert!(sql.contains("\"id\" TEXT"));
        assert!(sql.contains("\"name\" TEXT"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn generate_create_table_sql_single_pk() {
        let headers = vec!["id".to_string(), "name".to_string()];
        let mappings = vec![make_mapping("id", "id", true, false, false)];
        let sql = generate_create_table_sql("users", &headers, &mappings);
        assert!(sql.contains("\"id\" TEXT PRIMARY KEY"));
        assert!(sql.contains("\"name\" TEXT"));
        // No composite constraint needed for a single PK.
        assert!(!sql.contains("PRIMARY KEY ("));
    }

    #[test]
    fn generate_create_table_sql_composite_pk() {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mappings = vec![
            make_mapping("a", "a", true, false, false),
            make_mapping("b", "b", true, false, false),
        ];
        let sql = generate_create_table_sql("composite", &headers, &mappings);
        // Composite PK: no inline PRIMARY KEY on individual columns.
        assert!(!sql.contains("\"a\" TEXT PRIMARY KEY"));
        assert!(!sql.contains("\"b\" TEXT PRIMARY KEY"));
        assert!(sql.contains("PRIMARY KEY (\"a\", \"b\")"));
        assert!(sql.contains("\"c\" TEXT"));
    }

    #[test]
    fn generate_create_table_sql_skipped_columns_excluded() {
        let headers = vec!["id".to_string(), "secret".to_string(), "name".to_string()];
        let mappings = vec![make_mapping("secret", "secret", false, true, false)];
        let sql = generate_create_table_sql("t", &headers, &mappings);
        assert!(sql.contains("\"id\""));
        assert!(!sql.contains("\"secret\""));
        assert!(sql.contains("\"name\""));
    }

    #[test]
    fn generate_create_table_sql_auto_increment_excluded() {
        let headers = vec!["row_id".to_string(), "value".to_string()];
        let mappings = vec![make_mapping("row_id", "row_id", false, false, true)];
        let sql = generate_create_table_sql("t", &headers, &mappings);
        assert!(!sql.contains("\"row_id\""));
        assert!(sql.contains("\"value\""));
    }

    #[test]
    fn generate_create_table_sql_target_field_rename() {
        // When a mapping renames the column the DDL should use the target name.
        let headers = vec!["src_col".to_string()];
        let mappings = vec![make_mapping("src_col", "dst_col", false, false, false)];
        let sql = generate_create_table_sql("t", &headers, &mappings);
        assert!(sql.contains("\"dst_col\""));
        assert!(!sql.contains("\"src_col\""));
    }

    // -------------------------------------------------------------------------
    // quote_identifier tests (no DB interaction needed)
    // -------------------------------------------------------------------------

    use async_trait::async_trait;
    use std::sync::Mutex;
    use zqlz_core::{
        QueryResult, Result as ZqlzResult, SchemaIntrospection, StatementResult, TableInfo,
        TableType, Transaction, ZqlzError,
    };

    /// Minimal mock connection whose driver name and response behaviour are
    /// configurable.  All `execute` calls are recorded for assertion.
    struct TrackingConnection {
        driver: &'static str,
        /// SQL strings captured by `execute`.
        executed: Mutex<Vec<String>>,
        /// Table names to report as existing via `as_schema_introspection`.
        existing_tables: Vec<String>,
        /// Whether `query` should return a non-empty result (simulates a
        /// non-empty table for `table_has_rows`).
        query_returns_rows: bool,
    }

    impl TrackingConnection {
        fn new(driver: &'static str) -> Self {
            Self {
                driver,
                executed: Mutex::new(Vec::new()),
                existing_tables: Vec::new(),
                query_returns_rows: false,
            }
        }

        fn with_existing_tables(mut self, tables: Vec<&str>) -> Self {
            self.existing_tables = tables.iter().map(|s| s.to_string()).collect();
            self
        }

        fn with_rows_in_table(mut self) -> Self {
            self.query_returns_rows = true;
            self
        }

        fn executed_sql(&self) -> Vec<String> {
            self.executed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl zqlz_core::Connection for TrackingConnection {
        fn driver_name(&self) -> &str {
            self.driver
        }

        async fn execute(&self, sql: &str, _params: &[zqlz_core::Value]) -> ZqlzResult<StatementResult> {
            self.executed.lock().unwrap().push(sql.to_string());
            Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 0,
                error: None,
            })
        }

        async fn query(&self, _sql: &str, _params: &[zqlz_core::Value]) -> ZqlzResult<QueryResult> {
            if self.query_returns_rows {
                // Return a single-row result to signal the table has data.
                let mut result = QueryResult::empty();
                result.rows.push(zqlz_core::Row::new(
                    Vec::new(),
                    vec![zqlz_core::Value::Int64(1)],
                ));
                Ok(result)
            } else {
                Ok(QueryResult::empty())
            }
        }

        async fn begin_transaction(&self) -> ZqlzResult<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn close(&self) -> ZqlzResult<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }

        fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
            Some(self)
        }
    }

    #[async_trait]
    impl SchemaIntrospection for TrackingConnection {
        async fn list_databases(&self) -> ZqlzResult<Vec<zqlz_core::DatabaseInfo>> {
            Ok(Vec::new())
        }

        async fn list_schemas(&self) -> ZqlzResult<Vec<zqlz_core::SchemaInfo>> {
            Ok(Vec::new())
        }

        async fn list_tables(&self, _schema: Option<&str>) -> ZqlzResult<Vec<TableInfo>> {
            Ok(self
                .existing_tables
                .iter()
                .map(|name| TableInfo {
                    schema: None,
                    name: name.clone(),
                    table_type: TableType::Table,
                    owner: None,
                    row_count: None,
                    size_bytes: None,
                    comment: None,
                    index_count: None,
                    trigger_count: None,
                    key_value_info: None,
                })
                .collect())
        }

        async fn list_views(&self, _schema: Option<&str>) -> ZqlzResult<Vec<zqlz_core::ViewInfo>> {
            Ok(Vec::new())
        }

        async fn get_table(&self, _schema: Option<&str>, _name: &str) -> ZqlzResult<zqlz_core::TableDetails> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn get_columns(&self, _schema: Option<&str>, _table: &str) -> ZqlzResult<Vec<zqlz_core::ColumnInfo>> {
            Ok(Vec::new())
        }

        async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> ZqlzResult<Vec<zqlz_core::IndexInfo>> {
            Ok(Vec::new())
        }

        async fn get_foreign_keys(&self, _schema: Option<&str>, _table: &str) -> ZqlzResult<Vec<zqlz_core::ForeignKeyInfo>> {
            Ok(Vec::new())
        }

        async fn get_primary_key(&self, _schema: Option<&str>, _table: &str) -> ZqlzResult<Option<zqlz_core::PrimaryKeyInfo>> {
            Ok(None)
        }

        async fn get_constraints(&self, _schema: Option<&str>, _table: &str) -> ZqlzResult<Vec<zqlz_core::ConstraintInfo>> {
            Ok(Vec::new())
        }

        async fn list_functions(&self, _schema: Option<&str>) -> ZqlzResult<Vec<zqlz_core::FunctionInfo>> {
            Ok(Vec::new())
        }

        async fn list_procedures(&self, _schema: Option<&str>) -> ZqlzResult<Vec<zqlz_core::ProcedureInfo>> {
            Ok(Vec::new())
        }

        async fn list_triggers(&self, _schema: Option<&str>, _table: Option<&str>) -> ZqlzResult<Vec<zqlz_core::TriggerInfo>> {
            Ok(Vec::new())
        }

        async fn list_sequences(&self, _schema: Option<&str>) -> ZqlzResult<Vec<zqlz_core::SequenceInfo>> {
            Ok(Vec::new())
        }

        async fn list_types(&self, _schema: Option<&str>) -> ZqlzResult<Vec<zqlz_core::TypeInfo>> {
            Ok(Vec::new())
        }

        async fn generate_ddl(&self, _object: &zqlz_core::DatabaseObject) -> ZqlzResult<String> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn get_dependencies(&self, _object: &zqlz_core::DatabaseObject) -> ZqlzResult<Vec<zqlz_core::Dependency>> {
            Ok(Vec::new())
        }
    }

    fn make_importer_with_mode(connection: Arc<TrackingConnection>, mode: ImportMode) -> CsvImporter {
        let mut state = ImportWizardState::default();
        state.import_mode = mode;
        CsvImporter {
            connection,
            state,
            progress_callback: None,
        }
    }

    #[test]
    fn quote_identifier_uses_double_quotes_for_postgres() {
        let conn = Arc::new(TrackingConnection::new("postgresql"));
        let importer = make_importer_with_mode(conn, ImportMode::Append);
        assert_eq!(importer.quote_identifier("my_table"), "\"my_table\"");
    }

    #[test]
    fn quote_identifier_uses_backticks_for_mysql() {
        let conn = Arc::new(TrackingConnection::new("mysql"));
        let importer = make_importer_with_mode(conn, ImportMode::Append);
        assert_eq!(importer.quote_identifier("my_table"), "`my_table`");
    }

    #[tokio::test]
    async fn prepare_target_table_append_is_noop() {
        let conn = Arc::new(TrackingConnection::new("postgresql").with_existing_tables(vec!["users"]));
        let importer = make_importer_with_mode(conn.clone(), ImportMode::Append);
        let skip = importer.prepare_target_table("users").await.unwrap();
        assert!(!skip, "Append should not skip the source");
        assert!(
            conn.executed_sql().is_empty(),
            "Append must not execute any SQL"
        );
    }

    #[tokio::test]
    async fn prepare_target_table_copy_issues_delete_when_table_exists() {
        let conn = Arc::new(TrackingConnection::new("postgresql").with_existing_tables(vec!["users"]));
        let importer = make_importer_with_mode(conn.clone(), ImportMode::Copy);
        let skip = importer.prepare_target_table("users").await.unwrap();
        assert!(!skip, "Copy should not skip the source");
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 1);
        assert!(
            sqls[0].starts_with("DELETE FROM"),
            "Expected DELETE FROM, got: {}",
            sqls[0]
        );
        assert!(sqls[0].contains("\"users\""), "Table name must be quoted");
    }

    #[tokio::test]
    async fn prepare_target_table_copy_is_noop_when_table_missing() {
        // When the table does not exist yet, Copy should not attempt to clear it.
        let conn = Arc::new(TrackingConnection::new("postgresql"));
        let importer = make_importer_with_mode(conn.clone(), ImportMode::Copy);
        let skip = importer.prepare_target_table("users").await.unwrap();
        assert!(!skip);
        assert!(conn.executed_sql().is_empty());
    }

    #[tokio::test]
    async fn prepare_target_table_append_without_update_skips_nonempty_table() {
        let conn = Arc::new(
            TrackingConnection::new("postgresql")
                .with_existing_tables(vec!["users"])
                .with_rows_in_table(),
        );
        let importer = make_importer_with_mode(conn.clone(), ImportMode::AppendWithoutUpdate);
        let skip = importer.prepare_target_table("users").await.unwrap();
        assert!(skip, "AppendWithoutUpdate should skip a non-empty table");
        assert!(
            conn.executed_sql().is_empty(),
            "No mutation SQL should be issued when skipping"
        );
    }

    #[tokio::test]
    async fn prepare_target_table_append_without_update_proceeds_on_empty_table() {
        // Table exists but is empty → should not skip.
        let conn = Arc::new(
            TrackingConnection::new("postgresql").with_existing_tables(vec!["users"]),
        );
        let importer = make_importer_with_mode(conn.clone(), ImportMode::AppendWithoutUpdate);
        let skip = importer.prepare_target_table("users").await.unwrap();
        assert!(!skip, "Empty table should be populated");
    }

    #[tokio::test]
    async fn prepare_target_table_unsupported_mode_returns_error() {
        for mode in [ImportMode::Update, ImportMode::AppendUpdate, ImportMode::Delete] {
            let conn = Arc::new(TrackingConnection::new("postgresql"));
            let importer = make_importer_with_mode(conn, mode);
            let result = importer.prepare_target_table("users").await;
            assert!(
                result.is_err(),
                "Expected error for unsupported mode {:?}",
                mode
            );
            matches!(result.unwrap_err(), CsvImportError::UnsupportedMode(_));
        }
    }
}
