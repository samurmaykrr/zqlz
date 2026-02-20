//! Row duplication operations
//!
//! Provides functions for duplicating rows in the table viewer, with support
//! for clearing auto-increment columns.

use zqlz_core::{ColumnMeta, Value};

/// Options for row duplication
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct DuplicateOptions {
    /// Whether to clear auto-increment columns (set to NULL)
    pub clear_auto_increment: bool,
    /// Whether to clear primary key columns (set to NULL)
    pub clear_primary_key: bool,
    /// Column indices to always clear (set to NULL)
    pub columns_to_clear: Vec<usize>,
}

impl DuplicateOptions {
    /// Create new options with default settings (clear auto-increment only)
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            clear_auto_increment: true,
            clear_primary_key: false,
            columns_to_clear: Vec::new(),
        }
    }

    /// Set whether to clear auto-increment columns
    #[allow(dead_code)]
    pub fn with_clear_auto_increment(mut self, clear: bool) -> Self {
        self.clear_auto_increment = clear;
        self
    }

    /// Set whether to clear primary key columns
    #[allow(dead_code)]
    pub fn with_clear_primary_key(mut self, clear: bool) -> Self {
        self.clear_primary_key = clear;
        self
    }

    /// Set specific columns to clear by index
    #[allow(dead_code)]
    pub fn with_columns_to_clear(mut self, columns: Vec<usize>) -> Self {
        self.columns_to_clear = columns;
        self
    }
}

/// A row that was duplicated, containing the new values
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DuplicatedRow {
    /// The new row values (with appropriate columns cleared)
    pub values: Vec<Value>,
    /// Indices of columns that were cleared
    pub cleared_columns: Vec<usize>,
}

impl DuplicatedRow {
    /// Create a new duplicated row
    pub fn new(values: Vec<Value>, cleared_columns: Vec<usize>) -> Self {
        Self {
            values,
            cleared_columns,
        }
    }
}

/// Duplicate a single row, optionally clearing auto-increment and other columns
#[allow(dead_code)]
pub fn duplicate_row(
    row: &[Value],
    columns: &[ColumnMeta],
    options: &DuplicateOptions,
) -> DuplicatedRow {
    let mut values = row.to_vec();
    let mut cleared_columns = Vec::new();

    for (i, column) in columns.iter().enumerate() {
        if i >= values.len() {
            break;
        }

        let should_clear = (options.clear_auto_increment && column.auto_increment)
            || options.columns_to_clear.contains(&i);

        if should_clear {
            values[i] = Value::Null;
            cleared_columns.push(i);
        }
    }

    DuplicatedRow::new(values, cleared_columns)
}

/// Duplicate multiple rows, optionally clearing auto-increment and other columns
#[allow(dead_code)]
pub fn duplicate_rows(
    rows: &[Vec<Value>],
    columns: &[ColumnMeta],
    options: &DuplicateOptions,
) -> Vec<DuplicatedRow> {
    rows.iter()
        .map(|row| duplicate_row(row, columns, options))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, auto_increment: bool) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: "INTEGER".to_string(),
            nullable: true,
            ordinal: 0,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment,
            default_value: None,
            comment: None,
            enum_values: None,
        }
    }

    #[test]
    fn test_duplicate_single_row() {
        let columns = vec![
            make_column("id", true),
            make_column("name", false),
            make_column("email", false),
        ];

        let row = vec![
            Value::Int64(1),
            Value::String("Alice".to_string()),
            Value::String("alice@example.com".to_string()),
        ];

        let options = DuplicateOptions::new();
        let duplicated = duplicate_row(&row, &columns, &options);

        // Auto-increment column should be cleared
        assert_eq!(duplicated.values[0], Value::Null);
        // Other columns should be preserved
        assert_eq!(duplicated.values[1], Value::String("Alice".to_string()));
        assert_eq!(
            duplicated.values[2],
            Value::String("alice@example.com".to_string())
        );
        // Only column 0 should be in cleared_columns
        assert_eq!(duplicated.cleared_columns, vec![0]);
    }

    #[test]
    fn test_duplicate_multiple_rows() {
        let columns = vec![make_column("id", true), make_column("name", false)];

        let rows = vec![
            vec![Value::Int64(1), Value::String("Alice".to_string())],
            vec![Value::Int64(2), Value::String("Bob".to_string())],
            vec![Value::Int64(3), Value::String("Charlie".to_string())],
        ];

        let options = DuplicateOptions::new();
        let duplicated = duplicate_rows(&rows, &columns, &options);

        assert_eq!(duplicated.len(), 3);

        // All auto-increment columns should be cleared
        for dup in &duplicated {
            assert_eq!(dup.values[0], Value::Null);
            assert_eq!(dup.cleared_columns, vec![0]);
        }

        // Names should be preserved
        assert_eq!(duplicated[0].values[1], Value::String("Alice".to_string()));
        assert_eq!(duplicated[1].values[1], Value::String("Bob".to_string()));
        assert_eq!(
            duplicated[2].values[1],
            Value::String("Charlie".to_string())
        );
    }

    #[test]
    fn test_auto_increment_columns_cleared() {
        let columns = vec![
            make_column("id", true),
            make_column("secondary_id", true),
            make_column("name", false),
            make_column("counter", false),
        ];

        let row = vec![
            Value::Int64(100),
            Value::Int64(200),
            Value::String("Test".to_string()),
            Value::Int64(42),
        ];

        // With auto-increment clearing enabled (default)
        let options = DuplicateOptions::new();
        let duplicated = duplicate_row(&row, &columns, &options);

        // Both auto-increment columns should be cleared
        assert_eq!(duplicated.values[0], Value::Null);
        assert_eq!(duplicated.values[1], Value::Null);
        // Non-auto-increment columns should be preserved
        assert_eq!(duplicated.values[2], Value::String("Test".to_string()));
        assert_eq!(duplicated.values[3], Value::Int64(42));
        // Both auto-increment columns should be in cleared_columns
        assert_eq!(duplicated.cleared_columns, vec![0, 1]);

        // With auto-increment clearing disabled
        let options = DuplicateOptions::new().with_clear_auto_increment(false);
        let duplicated = duplicate_row(&row, &columns, &options);

        // All values should be preserved
        assert_eq!(duplicated.values[0], Value::Int64(100));
        assert_eq!(duplicated.values[1], Value::Int64(200));
        assert_eq!(duplicated.values[2], Value::String("Test".to_string()));
        assert_eq!(duplicated.values[3], Value::Int64(42));
        // No columns should be cleared
        assert!(duplicated.cleared_columns.is_empty());
    }
}
