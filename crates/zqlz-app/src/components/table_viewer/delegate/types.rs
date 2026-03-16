use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use zqlz_core::Value;
use zqlz_ui::widgets::select::SelectItem;

/// Item for FK dropdown with value and display label
#[derive(Clone, Debug)]
pub struct FkSelectItem {
    /// The actual FK value (stored in database)
    pub value: String,
    /// Display label (e.g., "123 - John Smith" for a user FK)
    pub label: String,
}

impl SelectItem for FkSelectItem {
    type Value = String;

    fn title(&self) -> SharedString {
        SharedString::from(self.label.clone())
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }

    fn matches(&self, query: &str) -> bool {
        let query_lower = query.to_lowercase();
        self.label.to_lowercase().contains(&query_lower)
            || self.value.to_lowercase().contains(&query_lower)
    }

    fn render(&self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        use zqlz_ui::widgets::ActiveTheme;
        let theme = cx.theme();

        // Render FK item with value highlighted and label secondary
        div()
            .flex()
            .items_center()
            .gap_1()
            .child(div().text_color(theme.foreground).child(self.value.clone()))
            .when(self.label != self.value, |this| {
                this.child(
                    div()
                        .text_color(theme.muted_foreground)
                        .text_xs()
                        .child(format!(
                            "({})",
                            self.label.replace(&format!("{} - ", self.value), "")
                        )),
                )
            })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CellValue {
    Null,
    Value(Value),
}

impl CellValue {
    pub fn from_value(value: &Value) -> Self {
        if value.is_null() {
            Self::Null
        } else {
            Self::Value(value.clone())
        }
    }

    pub fn as_option_string(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Value(value) => Some(value.display_for_editor()),
        }
    }

    pub fn display_for_table(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Value(value) => value.display_for_table(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Value(value) => value.clone(),
        }
    }

    pub fn to_value(&self, data_type: &str) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Value(value) => match value {
                Value::String(text) => Value::parse_from_string(text, data_type),
                other => other.clone(),
            },
        }
    }
}

/// Represents a pending cell change (not yet committed to database)
#[derive(Clone, Debug)]
pub struct PendingCellChange {
    /// Original persisted value before editing.
    pub original_value: CellValue,
    /// New persisted value after editing.
    pub new_value: CellValue,
}

#[derive(Clone, Debug)]
pub(crate) struct SaveCellRequest {
    pub table_name: String,
    pub connection_id: Uuid,
    pub row: usize,
    pub data_col: usize,
    pub column_name: String,
    pub new_value: CellValue,
    pub original_value: CellValue,
    pub all_row_values: Vec<Value>,
    pub all_column_names: Vec<String>,
    pub all_column_types: Vec<String>,
}

/// A single undoable cell edit, storing enough info to reverse the change.
#[derive(Clone, Debug)]
pub struct UndoCellEdit {
    pub row: usize,
    pub data_col: usize,
    pub old_value: Value,
    pub new_value: Value,
}

/// An undo entry groups one or more cell edits that should be undone/redone atomically.
/// For example, a bulk edit or paste produces a single entry with many cell edits.
#[derive(Clone, Debug)]
pub struct UndoEntry {
    pub edits: Vec<UndoCellEdit>,
}

/// Tracks all pending changes in the table (not yet committed to database)
#[derive(Clone, Debug, Default)]
pub struct PendingChanges {
    /// Modified cells: (row_index, col_index) -> change details
    pub modified_cells: HashMap<(usize, usize), PendingCellChange>,
    /// Rows marked for deletion (by row index)
    pub deleted_rows: HashSet<usize>,
    /// New rows to be inserted (each Vec<Value> is a row of values)
    pub new_rows: Vec<Vec<Value>>,
}

impl PendingChanges {
    /// Check if there are any pending changes
    pub fn is_empty(&self) -> bool {
        self.modified_cells.is_empty() && self.deleted_rows.is_empty() && self.new_rows.is_empty()
    }

    /// Clear all pending changes
    pub fn clear(&mut self) {
        self.modified_cells.clear();
        self.deleted_rows.clear();
        self.new_rows.clear();
    }

    /// Get total count of pending changes
    pub fn change_count(&self) -> usize {
        self.modified_cells.len() + self.deleted_rows.len() + self.new_rows.len()
    }

    /// Check if a specific cell has pending changes
    pub fn is_cell_modified(&self, row: usize, col: usize) -> bool {
        self.modified_cells.contains_key(&(row, col))
    }

    /// Get the pending change for a cell, if any
    pub fn get_cell_change(&self, row: usize, col: usize) -> Option<&PendingCellChange> {
        self.modified_cells.get(&(row, col))
    }

    /// Check if a row is marked for deletion
    pub fn is_row_deleted(&self, row: usize) -> bool {
        self.deleted_rows.contains(&row)
    }

    /// Get the number of new rows pending
    pub fn new_row_count(&self) -> usize {
        self.new_rows.len()
    }

    /// Check if a row index corresponds to a new (unsaved) row
    /// Returns Some(new_row_index) if it's a new row, None otherwise
    pub fn get_new_row_index(&self, row_index: usize, total_rows: usize) -> Option<usize> {
        let original_row_count = total_rows.checked_sub(self.new_rows.len())?;
        if row_index >= original_row_count {
            Some(row_index - original_row_count)
        } else {
            None
        }
    }

    /// Update a cell value in a new row (not yet saved to database)
    pub fn update_new_row_cell(
        &mut self,
        new_row_index: usize,
        col_index: usize,
        value: impl Into<Value>,
    ) {
        let value = value.into();
        if let Some(row) = self.new_rows.get_mut(new_row_index)
            && let Some(cell) = row.get_mut(col_index)
        {
            *cell = value;
        }
    }
}
