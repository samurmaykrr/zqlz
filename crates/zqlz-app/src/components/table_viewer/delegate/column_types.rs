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

    pub(super) fn parse_boolean_value(&self, value: &str) -> Option<bool> {
        if value.is_empty() || value.eq_ignore_ascii_case("null") {
            return None;
        }
        match value.to_lowercase().as_str() {
            "true" | "t" | "1" | "yes" | "y" | "on" => Some(true),
            "false" | "f" | "0" | "no" | "n" | "off" => Some(false),
            _ => None,
        }
    }

    fn format_boolean_value(&self, value: Option<bool>) -> String {
        match value {
            Some(true) => "true".to_string(),
            Some(false) => "false".to_string(),
            None => String::new(),
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

        let total_rows = self.rows.len();
        let new_row_idx = self.pending_changes.get_new_row_index(row, total_rows);

        if let Some(new_row_idx) = new_row_idx {
            if let Some(row_data) = self.rows.get_mut(row) {
                if let Some(cell) = row_data.get_mut(data_col) {
                    *cell = new_value.clone();
                }
            }
            self.pending_changes
                .update_new_row_cell(new_row_idx, data_col, new_value);
        } else if self.auto_commit_mode {
            let table_name = self.table_name.clone();
            let connection_id = self.connection_id;
            let all_row_values = self.rows.get(row).cloned().unwrap_or_default();
            let all_column_names: Vec<String> =
                self.column_meta.iter().map(|c| c.name.clone()).collect();
            let all_column_types: Vec<String> = self
                .column_meta
                .iter()
                .map(|c| c.data_type.clone())
                .collect();
            let column_name = self
                .column_meta
                .get(data_col)
                .map(|c| c.name.clone())
                .unwrap_or_default();

            if let Some(row_data) = self.rows.get_mut(row) {
                if let Some(cell) = row_data.get_mut(data_col) {
                    *cell = new_value.clone();
                }
            }

            let viewer_panel = self.viewer_panel.clone();
            cx.defer(move |cx| {
                _ = viewer_panel.update(cx, |_panel, cx| {
                    cx.emit(TableViewerEvent::SaveCell {
                        table_name,
                        connection_id,
                        row,
                        col: data_col,
                        column_name,
                        new_value,
                        original_value: current_value,
                        all_row_values,
                        all_column_names,
                        all_column_types,
                    });
                });
            });
        } else {
            if let Some(row_data) = self.rows.get_mut(row) {
                if let Some(cell) = row_data.get_mut(data_col) {
                    self.pending_changes.modified_cells.insert(
                        (row, data_col),
                        PendingCellChange {
                            original_value: current_value,
                            new_value: new_value.clone(),
                        },
                    );
                    *cell = new_value;
                }
            }
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
                if col.enum_values.is_some() && !col.enum_values.as_ref().unwrap().is_empty() {
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

    /// Check if a cell value looks like a binary placeholder
    pub fn is_bytes_placeholder(value: &str) -> bool {
        value == "BLOB" || (value.starts_with('<') && value.ends_with("bytes>"))
    }
}
