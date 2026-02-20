//! Multi-row operations
//!
//! Provides bulk operations for multiple rows in the table viewer, including
//! set value, delete, and duplicate operations with SQL generation support.

use zqlz_core::{ColumnMeta, Value};

use super::duplicate::{duplicate_row, DuplicateOptions, DuplicatedRow};

/// An operation to perform on multiple rows
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Operation {
    /// Set a specific column to a value for all selected rows
    SetValue(usize, Value),
    /// Delete all selected rows
    Delete,
    /// Duplicate all selected rows
    Duplicate,
}

impl Operation {
    /// Create a SetValue operation
    #[allow(dead_code)]
    pub fn set_value(column_index: usize, value: Value) -> Self {
        Operation::SetValue(column_index, value)
    }

    /// Create a Delete operation
    #[allow(dead_code)]
    pub fn delete() -> Self {
        Operation::Delete
    }

    /// Create a Duplicate operation
    #[allow(dead_code)]
    pub fn duplicate() -> Self {
        Operation::Duplicate
    }
}

/// Result of a multi-row operation
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum OperationResult {
    /// Rows that were modified with new values
    Modified(Vec<Vec<Value>>),
    /// Rows marked for deletion (original row indices)
    Deleted(Vec<usize>),
    /// Duplicated rows ready for insertion
    Duplicated(Vec<DuplicatedRow>),
}

impl OperationResult {
    /// Returns the number of affected rows
    #[allow(dead_code)]
    pub fn affected_count(&self) -> usize {
        match self {
            OperationResult::Modified(rows) => rows.len(),
            OperationResult::Deleted(indices) => indices.len(),
            OperationResult::Duplicated(rows) => rows.len(),
        }
    }

    /// Returns true if no rows were affected
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.affected_count() == 0
    }
}

/// Executor for multi-row operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MultiRowOperation {
    /// The operation to perform
    operation: Operation,
    /// Options for duplicate operations
    duplicate_options: DuplicateOptions,
}

impl MultiRowOperation {
    /// Create a new multi-row operation
    #[allow(dead_code)]
    pub fn new(operation: Operation) -> Self {
        Self {
            operation,
            duplicate_options: DuplicateOptions::new(),
        }
    }

    /// Set duplicate options (only used for Duplicate operation)
    #[allow(dead_code)]
    pub fn with_duplicate_options(mut self, options: DuplicateOptions) -> Self {
        self.duplicate_options = options;
        self
    }

    /// Execute the operation on the given rows
    #[allow(dead_code)]
    pub fn execute(
        &self,
        rows: &[Vec<Value>],
        row_indices: &[usize],
        columns: &[ColumnMeta],
    ) -> OperationResult {
        match &self.operation {
            Operation::SetValue(column_index, value) => {
                let modified = rows
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| row_indices.contains(i))
                    .map(|(_, row)| {
                        let mut new_row = row.clone();
                        if *column_index < new_row.len() {
                            new_row[*column_index] = value.clone();
                        }
                        new_row
                    })
                    .collect();
                OperationResult::Modified(modified)
            }
            Operation::Delete => OperationResult::Deleted(row_indices.to_vec()),
            Operation::Duplicate => {
                let duplicated = rows
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| row_indices.contains(i))
                    .map(|(_, row)| duplicate_row(row, columns, &self.duplicate_options))
                    .collect();
                OperationResult::Duplicated(duplicated)
            }
        }
    }
}

/// Generate a bulk UPDATE SQL statement for setting a column value
#[allow(dead_code)]
pub fn generate_bulk_update_sql(
    table_name: &str,
    column_name: &str,
    primary_key_column: &str,
    primary_key_values: &[Value],
    new_value: &Value,
    driver_name: &str,
) -> String {
    if primary_key_values.is_empty() {
        return String::new();
    }

    let pk_list = primary_key_values
        .iter()
        .map(|v| format_value(v))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "UPDATE {} SET {} = {} WHERE {} IN ({})",
        quote_identifier(table_name, driver_name),
        quote_identifier(column_name, driver_name),
        format_value(new_value),
        quote_identifier(primary_key_column, driver_name),
        pk_list
    )
}

/// Generate a bulk DELETE SQL statement
#[allow(dead_code)]
pub fn generate_bulk_delete_sql(
    table_name: &str,
    primary_key_column: &str,
    primary_key_values: &[Value],
    driver_name: &str,
) -> String {
    if primary_key_values.is_empty() {
        return String::new();
    }

    let pk_list = primary_key_values
        .iter()
        .map(|v| format_value(v))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "DELETE FROM {} WHERE {} IN ({})",
        quote_identifier(table_name, driver_name),
        quote_identifier(primary_key_column, driver_name),
        pk_list
    )
}

/// Quote a SQL identifier using the appropriate style for the target database.
#[allow(dead_code)]
fn quote_identifier(name: &str, driver_name: &str) -> String {
    match driver_name {
        "mysql" => format!("`{}`", name.replace('`', "``")),
        "mssql" => format!("[{}]", name.replace(']', "]]")),
        _ => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

/// Format a value for SQL
#[allow(dead_code)]
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(n) => n.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float32(n) => n.to_string(),
        Value::Float64(n) => n.to_string(),
        Value::Decimal(s) => s.clone(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!("X'{}'", hex_encode(b)),
        Value::Uuid(u) => format!("'{}'", u),
        Value::Date(d) => format!("'{}'", d),
        Value::Time(t) => format!("'{}'", t),
        Value::DateTime(dt) => format!("'{}'", dt),
        Value::DateTimeUtc(dt) => format!("'{}'", dt),
        Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(|v| format_value(v)).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
    }
}

/// Encode bytes as hex string
#[allow(dead_code)]
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect()
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
    fn test_multi_row_delete() {
        let rows = vec![
            vec![Value::Int64(1), Value::String("Alice".to_string())],
            vec![Value::Int64(2), Value::String("Bob".to_string())],
            vec![Value::Int64(3), Value::String("Charlie".to_string())],
        ];

        let columns = vec![make_column("id", true), make_column("name", false)];

        let op = MultiRowOperation::new(Operation::delete());
        let result = op.execute(&rows, &[0, 2], &columns);

        match result {
            OperationResult::Deleted(indices) => {
                assert_eq!(indices, vec![0, 2]);
            }
            _ => panic!("Expected Deleted result"),
        }
    }

    #[test]
    fn test_multi_row_set_value() {
        let rows = vec![
            vec![Value::Int64(1), Value::String("Alice".to_string())],
            vec![Value::Int64(2), Value::String("Bob".to_string())],
            vec![Value::Int64(3), Value::String("Charlie".to_string())],
        ];

        let columns = vec![make_column("id", true), make_column("name", false)];

        // Set column 1 (name) to "Updated" for rows 0 and 2
        let op = MultiRowOperation::new(Operation::set_value(
            1,
            Value::String("Updated".to_string()),
        ));
        let result = op.execute(&rows, &[0, 2], &columns);

        match result {
            OperationResult::Modified(modified) => {
                assert_eq!(modified.len(), 2);
                // First modified row (was row 0)
                assert_eq!(modified[0][0], Value::Int64(1));
                assert_eq!(modified[0][1], Value::String("Updated".to_string()));
                // Second modified row (was row 2)
                assert_eq!(modified[1][0], Value::Int64(3));
                assert_eq!(modified[1][1], Value::String("Updated".to_string()));
            }
            _ => panic!("Expected Modified result"),
        }
    }

    #[test]
    fn test_bulk_update_sql_generation() {
        let sql = generate_bulk_update_sql(
            "users",
            "status",
            "id",
            &[Value::Int64(1), Value::Int64(3), Value::Int64(5)],
            &Value::String("active".to_string()),
            "postgresql",
        );

        assert_eq!(
            sql,
            r#"UPDATE "users" SET "status" = 'active' WHERE "id" IN (1, 3, 5)"#
        );
    }

    #[test]
    fn test_bulk_update_sql_generation_mysql() {
        let sql = generate_bulk_update_sql(
            "users",
            "status",
            "id",
            &[Value::Int64(1), Value::Int64(3)],
            &Value::String("active".to_string()),
            "mysql",
        );

        assert_eq!(
            sql,
            "UPDATE `users` SET `status` = 'active' WHERE `id` IN (1, 3)"
        );
    }

    #[test]
    fn test_bulk_delete_sql_generation() {
        let sql = generate_bulk_delete_sql(
            "users",
            "id",
            &[Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            "postgresql",
        );

        assert_eq!(sql, r#"DELETE FROM "users" WHERE "id" IN (1, 2, 3)"#);
    }

    #[test]
    fn test_bulk_delete_sql_generation_mysql() {
        let sql =
            generate_bulk_delete_sql("users", "id", &[Value::Int64(1), Value::Int64(2)], "mysql");

        assert_eq!(sql, "DELETE FROM `users` WHERE `id` IN (1, 2)");
    }

    #[test]
    fn test_multi_row_duplicate() {
        let rows = vec![
            vec![Value::Int64(1), Value::String("Alice".to_string())],
            vec![Value::Int64(2), Value::String("Bob".to_string())],
        ];

        let columns = vec![make_column("id", true), make_column("name", false)];

        let op = MultiRowOperation::new(Operation::duplicate());
        let result = op.execute(&rows, &[0, 1], &columns);

        match result {
            OperationResult::Duplicated(duplicated) => {
                assert_eq!(duplicated.len(), 2);
                // Auto-increment columns should be cleared
                assert_eq!(duplicated[0].values[0], Value::Null);
                assert_eq!(duplicated[0].values[1], Value::String("Alice".to_string()));
                assert_eq!(duplicated[1].values[0], Value::Null);
                assert_eq!(duplicated[1].values[1], Value::String("Bob".to_string()));
            }
            _ => panic!("Expected Duplicated result"),
        }
    }

    #[test]
    fn test_operation_result_affected_count() {
        let modified = OperationResult::Modified(vec![vec![], vec![], vec![]]);
        assert_eq!(modified.affected_count(), 3);
        assert!(!modified.is_empty());

        let deleted = OperationResult::Deleted(vec![0, 1]);
        assert_eq!(deleted.affected_count(), 2);

        let empty = OperationResult::Deleted(vec![]);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_empty_primary_keys_returns_empty_sql() {
        let sql =
            generate_bulk_update_sql("users", "status", "id", &[], &Value::Null, "postgresql");
        assert!(sql.is_empty());

        let sql = generate_bulk_delete_sql("users", "id", &[], "postgresql");
        assert!(sql.is_empty());
    }
}
