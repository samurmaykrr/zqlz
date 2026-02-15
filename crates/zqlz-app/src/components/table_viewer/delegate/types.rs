use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::{HashMap, HashSet};
use zqlz_core::ForeignKeyInfo;
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

/// Represents a pending cell change (not yet committed to database)
#[derive(Clone, Debug)]
pub struct PendingCellChange {
    /// Original value before editing
    pub original_value: String,
    /// New value after editing
    pub new_value: String,
}

/// Tracks all pending changes in the table (not yet committed to database)
#[derive(Clone, Debug, Default)]
pub struct PendingChanges {
    /// Modified cells: (row_index, col_index) -> change details
    pub modified_cells: HashMap<(usize, usize), PendingCellChange>,
    /// Rows marked for deletion (by row index)
    pub deleted_rows: HashSet<usize>,
    /// New rows to be inserted (each Vec<String> is a row of values)
    pub new_rows: Vec<Vec<String>>,
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
        let original_row_count = total_rows - self.new_rows.len();
        if row_index >= original_row_count {
            Some(row_index - original_row_count)
        } else {
            None
        }
    }

    /// Update a cell value in a new row (not yet saved to database)
    pub fn update_new_row_cell(&mut self, new_row_index: usize, col_index: usize, value: String) {
        if let Some(row) = self.new_rows.get_mut(new_row_index) {
            if let Some(cell) = row.get_mut(col_index) {
                *cell = value;
            }
        }
    }
}
