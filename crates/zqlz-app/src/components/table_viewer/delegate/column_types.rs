use super::*;
use zqlz_core::{SqlNumericKind, SqlTemporalKind, SqlTypeFamily, SqlTypeInfo};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnTypePresentation {
    pub(crate) label: String,
    pub(crate) tooltip: String,
    pub(crate) family: SqlTypeFamily,
}

#[allow(dead_code)]
impl TableViewerDelegate {
    pub(crate) fn type_presentation_for_meta(meta: &ColumnMeta) -> ColumnTypePresentation {
        let label = SqlTypeInfo::display_label_for_column(meta);
        let tooltip = if label == meta.data_type {
            meta.data_type.clone()
        } else {
            format!("{} ({})", label, meta.data_type)
        };

        ColumnTypePresentation {
            label,
            tooltip,
            family: SqlTypeInfo::from_column_meta(meta).family,
        }
    }

    pub fn is_boolean_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .is_some_and(|col| SqlTypeInfo::from_column_meta(col).family == SqlTypeFamily::Boolean)
    }

    fn numeric_kind_for_column(&self, data_col_ix: usize) -> Option<SqlNumericKind> {
        self.column_meta
            .get(data_col_ix)
            .and_then(|column| SqlTypeInfo::from_column_meta(column).numeric_kind)
    }

    fn scalar_temporal_kind_for_column(&self, data_col_ix: usize) -> Option<SqlTemporalKind> {
        self.column_meta
            .get(data_col_ix)
            .and_then(|column| {
                let info = SqlTypeInfo::from_column_meta(column);
                info.is_scalar_temporal()
                    .then_some(info.temporal_kind)
                    .flatten()
            })
            .or_else(|| {
                self.column_meta
                    .get(data_col_ix)
                    .and_then(classify_text_column_temporal_hint)
            })
    }

    fn column_type_family(&self, data_col_ix: usize) -> SqlTypeFamily {
        self.column_meta
            .get(data_col_ix)
            .map(SqlTypeInfo::from_column_meta)
            .map(|info| info.family)
            .unwrap_or(SqlTypeFamily::Unknown)
    }

    pub fn is_integer_column(&self, data_col_ix: usize) -> bool {
        self.numeric_kind_for_column(data_col_ix) == Some(SqlNumericKind::Integer)
            && !self.is_boolean_column(data_col_ix)
    }

    pub fn is_float_column(&self, data_col_ix: usize) -> bool {
        matches!(
            self.numeric_kind_for_column(data_col_ix),
            Some(SqlNumericKind::Float | SqlNumericKind::Decimal)
        )
    }

    pub fn is_string_column(&self, data_col_ix: usize) -> bool {
        self.column_type_family(data_col_ix) == SqlTypeFamily::Text
    }

    pub fn can_generate_uuid_for_column(&self, data_col_ix: usize) -> bool {
        self.column_meta
            .get(data_col_ix)
            .map(|col| is_uuid_generation_compatible_data_type(&col.data_type))
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
        self.scalar_temporal_kind_for_column(data_col_ix) == Some(SqlTemporalKind::Date)
    }

    pub fn is_time_column(&self, data_col_ix: usize) -> bool {
        self.scalar_temporal_kind_for_column(data_col_ix) == Some(SqlTemporalKind::Time)
    }

    pub fn is_datetime_column(&self, data_col_ix: usize) -> bool {
        self.scalar_temporal_kind_for_column(data_col_ix) == Some(SqlTemporalKind::DateTime)
    }

    pub fn is_date_time_column(&self, data_col_ix: usize) -> bool {
        self.scalar_temporal_kind_for_column(data_col_ix).is_some()
    }

    pub fn is_enum_column(&self, data_col_ix: usize) -> bool {
        self.column_type_family(data_col_ix) == SqlTypeFamily::Enum
    }

    pub fn get_enum_values(&self, data_col_ix: usize) -> Option<&Vec<String>> {
        self.column_meta
            .get(data_col_ix)
            .and_then(|col| col.enum_values.as_ref())
            .filter(|v| !v.is_empty())
    }

    /// Detect binary/blob columns that store raw byte data
    pub fn is_binary_column(&self, data_col_ix: usize) -> bool {
        self.column_type_family(data_col_ix) == SqlTypeFamily::Binary
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

fn classify_text_column_temporal_hint(meta: &ColumnMeta) -> Option<SqlTemporalKind> {
    let normalized = meta.data_type.trim().to_ascii_lowercase();
    let base = base_type(&normalized).trim();
    if !matches!(base, "text" | "dynamic") {
        return None;
    }

    let name = meta.name.to_ascii_lowercase();
    if name.ends_with("_date") || matches!(name.as_str(), "date" | "birthdate" | "dob") {
        return Some(SqlTemporalKind::Date);
    }

    if name.ends_with("_at")
        || name.ends_with("_time")
        || matches!(
            name.as_str(),
            "timestamp" | "datetime" | "created" | "updated" | "deleted"
        )
    {
        return Some(SqlTemporalKind::DateTime);
    }

    None
}

fn is_uuid_generation_compatible_data_type(data_type: &str) -> bool {
    matches!(
        SqlTypeInfo::from_data_type(data_type).family,
        SqlTypeFamily::Uuid | SqlTypeFamily::Text
    )
}

#[cfg(test)]
mod tests {
    use super::{
        TableViewerDelegate, classify_text_column_temporal_hint,
        is_uuid_generation_compatible_data_type,
    };
    use zqlz_core::{ColumnMeta, SqlTemporalKind, SqlTypeFamily};

    fn meta(data_type: &str) -> ColumnMeta {
        ColumnMeta {
            name: "value".to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            ordinal: 0,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        }
    }

    #[::core::prelude::v1::test]
    fn allows_uuid_and_string_like_types() {
        assert!(is_uuid_generation_compatible_data_type("uuid"));
        assert!(is_uuid_generation_compatible_data_type("UNIQUEIDENTIFIER"));
        assert!(is_uuid_generation_compatible_data_type("varchar(255)"));
        assert!(is_uuid_generation_compatible_data_type("TEXT"));
        assert!(is_uuid_generation_compatible_data_type(
            "character varying(64)"
        ));
    }

    #[::core::prelude::v1::test]
    fn rejects_non_uuid_non_string_like_types() {
        assert!(!is_uuid_generation_compatible_data_type("int"));
        assert!(!is_uuid_generation_compatible_data_type("boolean"));
        assert!(!is_uuid_generation_compatible_data_type("jsonb"));
        assert!(!is_uuid_generation_compatible_data_type("date"));
    }

    #[::core::prelude::v1::test]
    fn type_presentation_classifies_driver_types() {
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("uuid")).family,
            SqlTypeFamily::Uuid
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("jsonb")).family,
            SqlTypeFamily::Json
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("numeric(18,4)")).family,
            SqlTypeFamily::Number
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("timestamp with time zone"))
                .family,
            SqlTypeFamily::Temporal
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("daterange")).family,
            SqlTypeFamily::Temporal
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("TINYINT(1)")).family,
            SqlTypeFamily::Boolean
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("varbinary(max)")).family,
            SqlTypeFamily::Binary
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("String")).family,
            SqlTypeFamily::Text
        );
    }

    #[::core::prelude::v1::test]
    fn range_types_are_not_scalar_date_picker_types() {
        for data_type in ["daterange", "tsrange", "tstzrange"] {
            let info = zqlz_core::SqlTypeInfo::from_data_type(data_type);
            assert_eq!(info.family, SqlTypeFamily::Temporal);
            assert!(info.is_range);
            assert!(!info.is_scalar_temporal());
        }
    }

    #[::core::prelude::v1::test]
    fn text_temporal_hints_only_apply_to_weak_text_types() {
        let mut text_date = meta("text");
        text_date.name = "shipped_date".to_string();
        assert_eq!(
            classify_text_column_temporal_hint(&text_date),
            Some(SqlTemporalKind::Date)
        );

        let mut typed_date = meta("varchar");
        typed_date.name = "shipped_date".to_string();
        assert_eq!(classify_text_column_temporal_hint(&typed_date), None);
    }

    #[::core::prelude::v1::test]
    fn type_presentation_preserves_driver_native_label() {
        let mut numeric = meta("numeric");
        numeric.precision = Some(18);
        numeric.scale = Some(4);
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&numeric).label,
            "numeric(18,4)"
        );

        let mut varchar = meta("varchar");
        varchar.max_length = Some(255);
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&varchar).label,
            "varchar(255)"
        );

        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("jsonb")).label,
            "jsonb"
        );
        assert_eq!(
            TableViewerDelegate::type_presentation_for_meta(&meta("nvarchar(max)")).label,
            "nvarchar(max)"
        );
    }
}
