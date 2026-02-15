use super::*;
use crate::components::table_viewer::filter_types::{
    FilterCondition, FilterOperator, LogicalOperator,
};

impl TableViewerDelegate {
    pub fn set_search_filter(&mut self, filter: Option<String>) {
        self.search_filter = filter.clone();

        if let Some(ref search_text) = filter {
            self.is_filtering = true;
            let original_row_count = self.rows.len() - self.pending_changes.new_rows.len();

            self.filtered_row_indices = self
                .rows
                .iter()
                .enumerate()
                .filter(|(idx, row)| {
                    if *idx >= original_row_count {
                        return true;
                    }
                    row.iter()
                        .any(|cell| cell.to_lowercase().contains(search_text))
                })
                .map(|(idx, _)| idx)
                .collect();
        } else {
            self.is_filtering = false;
            self.filtered_row_indices.clear();
        }
    }

    /// Apply structured filter conditions against in-memory rows.
    /// Used for KeyValue/Document drivers where data is already loaded.
    pub fn apply_advanced_filters(&mut self, filters: &[FilterCondition], search_text: &str) {
        let enabled_filters: Vec<&FilterCondition> = filters
            .iter()
            .filter(|f| f.enabled && f.is_valid())
            .collect();

        let has_filters = !enabled_filters.is_empty();
        let has_search = !search_text.is_empty();

        if !has_filters && !has_search {
            self.is_filtering = false;
            self.filtered_row_indices.clear();
            return;
        }

        self.is_filtering = true;
        let search_lower = search_text.to_lowercase();

        self.filtered_row_indices = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                // Check advanced filters
                if has_filters && !row_matches_filters(row, &enabled_filters, &self.column_meta) {
                    return false;
                }
                // Check search text (matches any column)
                if has_search
                    && !row
                        .iter()
                        .any(|cell| cell.to_lowercase().contains(&search_lower))
                {
                    return false;
                }
                true
            })
            .map(|(idx, _)| idx)
            .collect();
    }

    pub fn get_search_match_count(&self) -> usize {
        if self.is_filtering {
            self.filtered_row_indices.len()
        } else {
            self.rows.len()
        }
    }

    pub(super) fn cell_matches_search(&self, value: &str) -> bool {
        if let Some(ref search_text) = self.search_filter {
            value.to_lowercase().contains(search_text)
        } else {
            false
        }
    }

    pub fn get_actual_row_index(&self, display_row: usize) -> usize {
        if self.is_filtering {
            self.filtered_row_indices
                .get(display_row)
                .copied()
                .unwrap_or(display_row)
        } else {
            display_row
        }
    }
}

/// Check if a row matches all filter conditions, respecting AND/OR logical operators.
fn row_matches_filters(
    row: &[String],
    filters: &[&FilterCondition],
    column_meta: &[ColumnMeta],
) -> bool {
    if filters.is_empty() {
        return true;
    }

    // Evaluate each filter and combine with logical operators.
    // The logical_operator on filter[i] connects filter[i] to filter[i+1].
    let mut result = evaluate_filter(filters[0], row, column_meta);

    for window in filters.windows(2) {
        let previous = window[0];
        let current = window[1];
        let current_result = evaluate_filter(current, row, column_meta);

        match previous.logical_operator {
            LogicalOperator::And => result = result && current_result,
            LogicalOperator::Or => result = result || current_result,
        }
    }

    result
}

/// Evaluate a single filter condition against a row.
fn evaluate_filter(filter: &FilterCondition, row: &[String], column_meta: &[ColumnMeta]) -> bool {
    // Custom SQL filters can't be evaluated client-side — skip them
    if filter.operator.is_custom() || filter.custom_sql.is_some() {
        return true;
    }

    let column_name = match &filter.column {
        Some(name) if !name.is_empty() => name,
        _ => return true,
    };

    let col_index = column_meta.iter().position(|m| &m.name == column_name);

    let col_index = match col_index {
        Some(idx) => idx,
        None => return true,
    };

    let cell_value = row.get(col_index).map(|s| s.as_str()).unwrap_or("");

    evaluate_operator(
        filter.operator,
        cell_value,
        &filter.value,
        filter.value2.as_deref(),
    )
}

/// Evaluate a filter operator against a cell value.
fn evaluate_operator(
    operator: FilterOperator,
    cell_value: &str,
    filter_value: &str,
    filter_value2: Option<&str>,
) -> bool {
    let cell_lower = cell_value.to_lowercase();
    let filter_lower = filter_value.to_lowercase();

    match operator {
        FilterOperator::Equal => cell_lower == filter_lower,
        FilterOperator::NotEqual => cell_lower != filter_lower,

        FilterOperator::LessThan => numeric_or_string_cmp(cell_value, filter_value).is_lt(),
        FilterOperator::LessThanOrEqual => numeric_or_string_cmp(cell_value, filter_value).is_le(),
        FilterOperator::GreaterThan => numeric_or_string_cmp(cell_value, filter_value).is_gt(),
        FilterOperator::GreaterThanOrEqual => {
            numeric_or_string_cmp(cell_value, filter_value).is_ge()
        }

        FilterOperator::Contains => cell_lower.contains(&filter_lower),
        FilterOperator::DoesNotContain => !cell_lower.contains(&filter_lower),
        FilterOperator::BeginsWith => cell_lower.starts_with(&filter_lower),
        FilterOperator::DoesNotBeginWith => !cell_lower.starts_with(&filter_lower),
        FilterOperator::EndsWith => cell_lower.ends_with(&filter_lower),
        FilterOperator::DoesNotEndWith => !cell_lower.ends_with(&filter_lower),

        FilterOperator::IsNull => cell_value.is_empty() || cell_value.eq_ignore_ascii_case("null"),
        FilterOperator::IsNotNull => {
            !cell_value.is_empty() && !cell_value.eq_ignore_ascii_case("null")
        }
        FilterOperator::IsEmpty => cell_value.is_empty(),
        FilterOperator::IsNotEmpty => !cell_value.is_empty(),

        FilterOperator::IsBetween => {
            let val2 = filter_value2.unwrap_or(filter_value);
            numeric_or_string_cmp(cell_value, filter_value).is_ge()
                && numeric_or_string_cmp(cell_value, val2).is_le()
        }
        FilterOperator::IsNotBetween => {
            let val2 = filter_value2.unwrap_or(filter_value);
            numeric_or_string_cmp(cell_value, filter_value).is_lt()
                || numeric_or_string_cmp(cell_value, val2).is_gt()
        }

        FilterOperator::IsInList => {
            let items: Vec<String> = filter_value
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .collect();
            items.contains(&cell_lower)
        }
        FilterOperator::IsNotInList => {
            let items: Vec<String> = filter_value
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .collect();
            !items.contains(&cell_lower)
        }

        FilterOperator::Custom => true,
    }
}

/// Compare two values, preferring numeric comparison when both parse as numbers.
fn numeric_or_string_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.to_lowercase().cmp(&b.to_lowercase())
    }
}
