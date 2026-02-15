//! Multi-column sorting for table viewer
//!
//! Provides multi-column sorting with null handling and SQL ORDER BY clause generation.

use std::cmp::Ordering;

use zqlz_core::Value;

use crate::components::table_viewer::{SortCriterion, SortDirection};

/// A column to sort by with its index and direction
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column index in the row
    pub column_index: usize,
    /// Sort direction
    pub direction: SortDirection,
}

impl SortColumn {
    /// Create a new sort column
    pub fn new(column_index: usize, direction: SortDirection) -> Self {
        Self {
            column_index,
            direction,
        }
    }

    /// Create ascending sort column
    pub fn ascending(column_index: usize) -> Self {
        Self::new(column_index, SortDirection::Ascending)
    }

    /// Create descending sort column
    pub fn descending(column_index: usize) -> Self {
        Self::new(column_index, SortDirection::Descending)
    }
}

/// Configuration for null value handling in sorting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullPosition {
    /// NULL values appear first
    First,
    /// NULL values appear last (default, matches SQL behavior)
    #[default]
    Last,
}

impl NullPosition {
    /// Get display label
    pub fn label(&self) -> &'static str {
        match self {
            Self::First => "NULLS FIRST",
            Self::Last => "NULLS LAST",
        }
    }
}

/// Multi-column sort configuration
#[derive(Debug, Clone, Default)]
pub struct MultiColumnSort {
    /// Columns to sort by, in priority order (first = highest priority)
    columns: Vec<SortColumn>,
    /// How to handle NULL values
    null_position: NullPosition,
}

impl MultiColumnSort {
    /// Create a new empty multi-column sort
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with null position configuration
    pub fn with_null_position(null_position: NullPosition) -> Self {
        Self {
            columns: Vec::new(),
            null_position,
        }
    }

    /// Add a sort column
    pub fn add_column(&mut self, column: SortColumn) {
        self.columns.push(column);
    }

    /// Add an ascending sort column by index
    pub fn add_ascending(&mut self, column_index: usize) {
        self.columns.push(SortColumn::ascending(column_index));
    }

    /// Add a descending sort column by index
    pub fn add_descending(&mut self, column_index: usize) {
        self.columns.push(SortColumn::descending(column_index));
    }

    /// Clear all sort columns
    pub fn clear(&mut self) {
        self.columns.clear();
    }

    /// Check if any sort columns are configured
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get the number of sort columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the sort columns
    pub fn columns(&self) -> &[SortColumn] {
        &self.columns
    }

    /// Get null position configuration
    pub fn null_position(&self) -> NullPosition {
        self.null_position
    }

    /// Set null position configuration
    pub fn set_null_position(&mut self, position: NullPosition) {
        self.null_position = position;
    }

    /// Compare two values with null handling
    fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        let a_is_null = a.is_null();
        let b_is_null = b.is_null();

        // Handle NULL values according to null_position
        match (a_is_null, b_is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => match self.null_position {
                NullPosition::First => Ordering::Less,
                NullPosition::Last => Ordering::Greater,
            },
            (false, true) => match self.null_position {
                NullPosition::First => Ordering::Greater,
                NullPosition::Last => Ordering::Less,
            },
            (false, false) => compare_non_null_values(a, b),
        }
    }

    /// Compare two rows using all configured sort columns
    pub fn compare_rows(&self, row_a: &[Value], row_b: &[Value]) -> Ordering {
        for sort_col in &self.columns {
            let a = row_a.get(sort_col.column_index);
            let b = row_b.get(sort_col.column_index);

            let ordering = match (a, b) {
                (Some(va), Some(vb)) => self.compare_values(va, vb),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };

            if ordering != Ordering::Equal {
                return match sort_col.direction {
                    SortDirection::Ascending => ordering,
                    SortDirection::Descending => ordering.reverse(),
                };
            }
        }
        Ordering::Equal
    }

    /// Sort a vector of rows in place
    pub fn sort_rows(&self, rows: &mut [Vec<Value>]) {
        if self.is_empty() {
            return;
        }
        rows.sort_by(|a, b| self.compare_rows(a, b));
    }

    /// Create from a list of SortCriterion and column names
    pub fn from_criteria(criteria: &[SortCriterion], column_names: &[String]) -> Self {
        let mut sort = Self::new();
        for criterion in criteria {
            if let Some(idx) = column_names.iter().position(|c| c == &criterion.column) {
                sort.add_column(SortColumn::new(idx, criterion.direction));
            }
        }
        sort
    }

    /// Generate SQL ORDER BY clause fragment (without the "ORDER BY" keyword)
    pub fn to_sql(&self, column_names: &[String]) -> Option<String> {
        if self.is_empty() {
            return None;
        }

        let parts: Vec<String> = self
            .columns
            .iter()
            .filter_map(|col| {
                column_names.get(col.column_index).map(|name| {
                    let escaped_col = format!("\"{}\"", name.replace("\"", "\"\""));
                    let nulls = match self.null_position {
                        NullPosition::First => " NULLS FIRST",
                        NullPosition::Last => " NULLS LAST",
                    };
                    format!("{} {}{}", escaped_col, col.direction.label(), nulls)
                })
            })
            .collect();

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(", "))
        }
    }
}

/// Compare two non-null values
fn compare_non_null_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        // Boolean comparison
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),

        // Integer comparisons
        (Value::Int8(a), Value::Int8(b)) => a.cmp(b),
        (Value::Int16(a), Value::Int16(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),

        // Float comparisons (handle NaN)
        (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),

        // String comparisons
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),

        // UUID comparison
        (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),

        // Date/Time comparisons
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (Value::DateTimeUtc(a), Value::DateTimeUtc(b)) => a.cmp(b),

        // Bytes comparison
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),

        // Array comparison (by length, then lexicographically)
        (Value::Array(a), Value::Array(b)) => match a.len().cmp(&b.len()) {
            Ordering::Equal => {
                for (va, vb) in a.iter().zip(b.iter()) {
                    let cmp = compare_non_null_values(va, vb);
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                Ordering::Equal
            }
            other => other,
        },

        // JSON comparison (convert to string for comparison)
        (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),

        // Cross-type integer comparison (promote to i64)
        (a, b) if a.as_i64().is_some() && b.as_i64().is_some() => {
            a.as_i64().unwrap().cmp(&b.as_i64().unwrap())
        }

        // Cross-type float comparison (promote to f64)
        (a, b) if a.as_f64().is_some() && b.as_f64().is_some() => a
            .as_f64()
            .partial_cmp(&b.as_f64())
            .unwrap_or(Ordering::Equal),

        // Fallback: compare string representations
        _ => a.to_string().cmp(&b.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_column_sort_ascending() {
        let mut rows = vec![
            vec![Value::Int32(3), Value::String("c".to_string())],
            vec![Value::Int32(1), Value::String("a".to_string())],
            vec![Value::Int32(2), Value::String("b".to_string())],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[1][0], Value::Int32(2));
        assert_eq!(rows[2][0], Value::Int32(3));
    }

    #[test]
    fn test_single_column_sort_descending() {
        let mut rows = vec![
            vec![Value::Int32(1), Value::String("a".to_string())],
            vec![Value::Int32(3), Value::String("c".to_string())],
            vec![Value::Int32(2), Value::String("b".to_string())],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_descending(0);
        sort.sort_rows(&mut rows);

        assert_eq!(rows[0][0], Value::Int32(3));
        assert_eq!(rows[1][0], Value::Int32(2));
        assert_eq!(rows[2][0], Value::Int32(1));
    }

    #[test]
    fn test_multi_column_sort() {
        let mut rows = vec![
            vec![Value::String("A".to_string()), Value::Int32(2)],
            vec![Value::String("B".to_string()), Value::Int32(1)],
            vec![Value::String("A".to_string()), Value::Int32(1)],
            vec![Value::String("B".to_string()), Value::Int32(2)],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_ascending(0); // First by first column ascending
        sort.add_descending(1); // Then by second column descending

        sort.sort_rows(&mut rows);

        // A's first, sorted by second column descending
        assert_eq!(rows[0][0], Value::String("A".to_string()));
        assert_eq!(rows[0][1], Value::Int32(2));
        assert_eq!(rows[1][0], Value::String("A".to_string()));
        assert_eq!(rows[1][1], Value::Int32(1));
        // B's next, sorted by second column descending
        assert_eq!(rows[2][0], Value::String("B".to_string()));
        assert_eq!(rows[2][1], Value::Int32(2));
        assert_eq!(rows[3][0], Value::String("B".to_string()));
        assert_eq!(rows[3][1], Value::Int32(1));
    }

    #[test]
    fn test_null_values_sorted_last() {
        let mut rows = vec![
            vec![Value::Null],
            vec![Value::Int32(2)],
            vec![Value::Int32(1)],
            vec![Value::Null],
            vec![Value::Int32(3)],
        ];

        let mut sort = MultiColumnSort::with_null_position(NullPosition::Last);
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        // Non-null values first, sorted ascending
        assert_eq!(rows[0], vec![Value::Int32(1)]);
        assert_eq!(rows[1], vec![Value::Int32(2)]);
        assert_eq!(rows[2], vec![Value::Int32(3)]);
        // Nulls last
        assert_eq!(rows[3], vec![Value::Null]);
        assert_eq!(rows[4], vec![Value::Null]);
    }

    #[test]
    fn test_null_values_sorted_first() {
        let mut rows = vec![
            vec![Value::Int32(2)],
            vec![Value::Null],
            vec![Value::Int32(1)],
            vec![Value::Null],
        ];

        let mut sort = MultiColumnSort::with_null_position(NullPosition::First);
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        // Nulls first
        assert_eq!(rows[0], vec![Value::Null]);
        assert_eq!(rows[1], vec![Value::Null]);
        // Non-null values, sorted ascending
        assert_eq!(rows[2], vec![Value::Int32(1)]);
        assert_eq!(rows[3], vec![Value::Int32(2)]);
    }

    #[test]
    fn test_string_sort() {
        let mut rows = vec![
            vec![Value::String("banana".to_string())],
            vec![Value::String("apple".to_string())],
            vec![Value::String("cherry".to_string())],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        assert_eq!(rows[0][0], Value::String("apple".to_string()));
        assert_eq!(rows[1][0], Value::String("banana".to_string()));
        assert_eq!(rows[2][0], Value::String("cherry".to_string()));
    }

    #[test]
    fn test_to_sql() {
        let mut sort = MultiColumnSort::with_null_position(NullPosition::Last);
        sort.add_ascending(0);
        sort.add_descending(2);

        let column_names = vec![
            "name".to_string(),
            "age".to_string(),
            "created_at".to_string(),
        ];

        let sql = sort.to_sql(&column_names);
        assert_eq!(
            sql,
            Some("\"name\" ASC NULLS LAST, \"created_at\" DESC NULLS LAST".to_string())
        );
    }

    #[test]
    fn test_to_sql_nulls_first() {
        let mut sort = MultiColumnSort::with_null_position(NullPosition::First);
        sort.add_ascending(0);

        let column_names = vec!["name".to_string()];

        let sql = sort.to_sql(&column_names);
        assert_eq!(sql, Some("\"name\" ASC NULLS FIRST".to_string()));
    }

    #[test]
    fn test_empty_sort() {
        let sort = MultiColumnSort::new();
        assert!(sort.is_empty());
        assert_eq!(sort.column_count(), 0);
        assert_eq!(sort.to_sql(&["col".to_string()]), None);
    }

    #[test]
    fn test_from_criteria() {
        let criteria = vec![
            SortCriterion::new(1, "name".to_string()),
            SortCriterion {
                id: 2,
                column: "age".to_string(),
                direction: SortDirection::Descending,
            },
        ];

        let column_names = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "email".to_string(),
        ];

        let sort = MultiColumnSort::from_criteria(&criteria, &column_names);

        assert_eq!(sort.column_count(), 2);
        assert_eq!(sort.columns()[0].column_index, 1); // name
        assert_eq!(sort.columns()[0].direction, SortDirection::Ascending);
        assert_eq!(sort.columns()[1].column_index, 2); // age
        assert_eq!(sort.columns()[1].direction, SortDirection::Descending);
    }

    #[test]
    fn test_float_sort() {
        let mut rows = vec![
            vec![Value::Float64(3.14)],
            vec![Value::Float64(1.0)],
            vec![Value::Float64(2.5)],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        assert_eq!(rows[0][0], Value::Float64(1.0));
        assert_eq!(rows[1][0], Value::Float64(2.5));
        assert_eq!(rows[2][0], Value::Float64(3.14));
    }

    #[test]
    fn test_bool_sort() {
        let mut rows = vec![
            vec![Value::Bool(true)],
            vec![Value::Bool(false)],
            vec![Value::Bool(true)],
            vec![Value::Bool(false)],
        ];

        let mut sort = MultiColumnSort::new();
        sort.add_ascending(0);
        sort.sort_rows(&mut rows);

        // false (0) comes before true (1)
        assert_eq!(rows[0][0], Value::Bool(false));
        assert_eq!(rows[1][0], Value::Bool(false));
        assert_eq!(rows[2][0], Value::Bool(true));
        assert_eq!(rows[3][0], Value::Bool(true));
    }
}
