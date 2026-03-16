use super::*;

#[allow(dead_code)]
impl TableViewerDelegate {
    pub fn is_boolean_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                t == "boolean"
                    || t == "bool"
                    || t == "bit"
                    || t == "tinyint(1)"
                    || t.starts_with("bool")
            })
            .unwrap_or(false)
    }

    pub fn is_integer_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                let base = base_type(&t);
                matches!(
                    base,
                    "int2"
                        | "int4"
                        | "int8"
                        | "smallint"
                        | "integer"
                        | "bigint"
                        | "int"
                        | "mediumint"
                        | "tinyint"
                        | "serial"
                        | "bigserial"
                        | "smallserial"
                ) && !self.is_boolean_column(data_col_ix)
            })
            .unwrap_or(false)
    }

    pub fn is_float_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                let base = base_type(&t);
                matches!(
                    base,
                    "float4"
                        | "float8"
                        | "real"
                        | "double precision"
                        | "double"
                        | "float"
                        | "numeric"
                        | "decimal"
                        | "money"
                )
            })
            .unwrap_or(false)
    }

    pub fn is_string_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                let base = base_type(&t);
                matches!(
                    base,
                    "text"
                        | "varchar"
                        | "char"
                        | "bpchar"
                        | "name"
                        | "citext"
                        | "character varying"
                        | "character"
                        | "nvarchar"
                        | "nchar"
                        | "longtext"
                        | "mediumtext"
                        | "tinytext"
                )
            })
            .unwrap_or(false)
    }

    /// Validate a cell value against the column's data type.
    /// Accepts a user-entered string (from inline edit or paste).
    /// Returns `Ok(())` if valid, or `Err(message)` describing the problem.
    pub fn validate_cell_value(&self, data_col: usize, value: &str) -> Result<(), String> {
        if value.is_empty() || value.eq_ignore_ascii_case("null") {
            return Ok(());
        }

        if self.is_integer_column(data_col) && value.parse::<i64>().is_err() {
            let col_name = self
                .column_meta
                .get(data_col)
                .map(|c| c.name.as_str())
                .unwrap_or("column");
            return Err(format!(
                "'{}' is not a valid integer for column '{}'",
                value, col_name
            ));
        }

        if self.is_float_column(data_col) && value.parse::<f64>().is_err() {
            let col_name = self
                .column_meta
                .get(data_col)
                .map(|c| c.name.as_str())
                .unwrap_or("column");
            return Err(format!(
                "'{}' is not a valid number for column '{}'",
                value, col_name
            ));
        }

        if self.is_string_column(data_col)
            && let Some(max_length) = self.column_meta.get(data_col).and_then(|c| c.max_length)
            && max_length > 0
            && value.len() > max_length as usize
        {
            let col_name = self
                .column_meta
                .get(data_col)
                .map(|c| c.name.as_str())
                .unwrap_or("column");
            return Err(format!(
                "Value exceeds max length {} for column '{}' ({} chars)",
                max_length,
                col_name,
                value.len()
            ));
        }

        Ok(())
    }

    pub(super) fn parse_boolean_value(&self, value: &Value) -> Option<bool> {
        match value {
            Value::Null => None,
            Value::Bool(b) => Some(*b),
            Value::Int8(v) => Some(*v != 0),
            Value::Int16(v) => Some(*v != 0),
            Value::Int32(v) => Some(*v != 0),
            Value::Int64(v) => Some(*v != 0),
            Value::String(s) => {
                if s.is_empty() || s.eq_ignore_ascii_case("null") {
                    return None;
                }
                match s.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" | "on" => Some(true),
                    "false" | "f" | "0" | "no" | "n" | "off" => Some(false),
                    _ => None,
                }
            }
            _ => {
                let s = value.display_for_table();
                match s.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" | "on" => Some(true),
                    "false" | "f" | "0" | "no" | "n" | "off" => Some(false),
                    _ => None,
                }
            }
        }
    }

    fn format_boolean_value(&self, value: Option<bool>) -> Value {
        match value {
            Some(b) => Value::Bool(b),
            None => Value::Null,
        }
    }

    pub fn toggle_boolean_cell(
        &mut self,
        row: usize,
        col: usize,
        cx: &mut Context<TableState<Self>>,
    ) {
        let data_col = col - 1;

        let current_value = self
            .rows
            .get(row)
            .and_then(|r| r.get(data_col))
            .cloned()
            .unwrap_or_default();

        let current_bool = self.parse_boolean_value(&current_value);

        let new_bool = match current_bool {
            Some(false) => Some(true),
            Some(true) => Some(false),
            None => Some(false),
        };

        let new_value = self.format_boolean_value(new_bool);

        self.push_undo(UndoEntry {
            edits: vec![UndoCellEdit {
                row,
                data_col,
                old_value: current_value.clone(),
                new_value: new_value.clone(),
            }],
        });

        let total_rows = self.rows.len();
        let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

        if let Some(new_row_idx) = new_row_idx {
            self.apply_value_locally(row, data_col, new_value.clone());
            self.pending_changes
                .update_new_row_cell(new_row_idx, data_col, new_value);
        } else if self.auto_commit_mode {
            self.save_existing_cell_or_queue(row, data_col, new_value, &current_value, cx);
        } else {
            self.store_pending_cell_change(row, data_col, new_value, &current_value);
        }

        cx.notify();
    }

    pub fn is_date_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                if t == "date" {
                    return true;
                }
                if t == "text" || t == "dynamic" {
                    let name = col.name.to_lowercase();
                    if name.ends_with("_date")
                        || name == "date"
                        || name == "birthdate"
                        || name == "dob"
                    {
                        return true;
                    }
                }
                false
            })
            .unwrap_or(false)
    }

    pub fn is_time_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                t == "time"
                    || t.starts_with("time without")
                    || t.starts_with("time with")
                    || t.starts_with("time(")
            })
            .unwrap_or(false)
    }

    pub fn is_datetime_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                if t == "datetime"
                    || t == "datetime2"
                    || t == "smalldatetime"
                    || t == "datetimeoffset"
                    || t == "timestamp"
                    || t == "timestamptz"
                    || t.starts_with("timestamp without")
                    || t.starts_with("timestamp with")
                    || t.starts_with("timestamp(")
                    || t.starts_with("datetime(")
                {
                    return true;
                }
                if t == "text" || t == "dynamic" {
                    let name = col.name.to_lowercase();
                    if name.ends_with("_at")
                        || name.ends_with("_time")
                        || name == "timestamp"
                        || name == "datetime"
                        || name == "created"
                        || name == "updated"
                        || name == "deleted"
                    {
                        return true;
                    }
                }
                false
            })
            .unwrap_or(false)
    }

    pub fn is_date_time_column(&self, data_col_ix: usize) -> bool {
        self.is_date_column(data_col_ix)
            || self.is_time_column(data_col_ix)
            || self.is_datetime_column(data_col_ix)
    }

    pub fn is_enum_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                if col.enum_values.as_ref().is_some_and(|v| !v.is_empty()) {
                    return true;
                }
                let t = col.data_type.to_lowercase();
                t.starts_with("enum") || t.starts_with("set(")
            })
            .unwrap_or(false)
    }

    pub fn get_enum_values(&self, data_col_ix: usize) -> Option<&Vec<String>> {
        self.column_meta
            .get(data_col_ix)
            .and_then(|col| col.enum_values.as_ref())
            .filter(|v| !v.is_empty())
    }

    /// Detect binary/blob columns that store raw byte data
    pub fn is_binary_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| {
                let t = col.data_type.to_lowercase();
                t == "blob"
                    || t == "mediumblob"
                    || t == "longblob"
                    || t == "tinyblob"
                    || t == "bytea"
                    || t == "binary"
                    || t == "varbinary"
                    || t.starts_with("binary(")
                    || t.starts_with("varbinary(")
                    || t.starts_with("bit(")
                    || t == "image"
                    || t == "raw"
                    || t.starts_with("raw(")
            })
            .unwrap_or(false)
    }

    /// Check if a cell value is binary data
    pub fn is_bytes_value(value: &Value) -> bool {
        matches!(value, Value::Bytes(_))
    }
}

/// Extract the base type name from a possibly-parameterized SQL type
/// (e.g. "varchar(255)" -> "varchar", "decimal(10,2)" -> "decimal").
fn base_type(type_string: &str) -> &str {
    match type_string.find('(') {
        Some(idx) => &type_string[..idx],
        None => type_string,
    }
}
