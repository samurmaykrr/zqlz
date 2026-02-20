//! Filter and sort types for table viewer
//!
//! Shared data structures for filtering, sorting, column visibility, and profile management.

use gpui::SharedString;

/// Filter operators for WHERE clause generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterOperator {
    // Equality operators
    #[default]
    Equal,
    NotEqual,

    // Comparison operators
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // String operators
    Contains,
    DoesNotContain,
    BeginsWith,
    DoesNotBeginWith,
    EndsWith,
    DoesNotEndWith,

    // NULL/Empty operators
    IsNull,
    IsNotNull,
    IsEmpty,
    IsNotEmpty,

    // Range operators
    IsBetween,
    IsNotBetween,

    // List operators
    IsInList,
    IsNotInList,

    // Custom SQL expression
    Custom,
}

impl FilterOperator {
    /// Get the display label for the operator
    pub fn label(&self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Contains => "contains",
            Self::DoesNotContain => "does not contain",
            Self::BeginsWith => "begins with",
            Self::DoesNotBeginWith => "does not begin with",
            Self::EndsWith => "ends with",
            Self::DoesNotEndWith => "does not end with",
            Self::IsNull => "is null",
            Self::IsNotNull => "is not null",
            Self::IsEmpty => "is empty",
            Self::IsNotEmpty => "is not empty",
            Self::IsBetween => "is between",
            Self::IsNotBetween => "is not between",
            Self::IsInList => "is in list",
            Self::IsNotInList => "is not in list",
            Self::Custom => "[Custom]",
        }
    }

    /// Returns true if this operator requires a value input
    pub fn requires_value(&self) -> bool {
        !matches!(
            self,
            Self::IsNull | Self::IsNotNull | Self::IsEmpty | Self::IsNotEmpty
        )
    }

    /// Returns true if this operator requires two values (for BETWEEN)
    pub fn requires_two_values(&self) -> bool {
        matches!(self, Self::IsBetween | Self::IsNotBetween)
    }

    /// Returns true if this operator is a custom SQL expression
    pub fn is_custom(&self) -> bool {
        matches!(self, Self::Custom)
    }

    /// Get all available operators in display order
    pub fn all() -> &'static [FilterOperator] {
        &[
            Self::Equal,
            Self::NotEqual,
            Self::LessThan,
            Self::LessThanOrEqual,
            Self::GreaterThan,
            Self::GreaterThanOrEqual,
            Self::Contains,
            Self::DoesNotContain,
            Self::BeginsWith,
            Self::DoesNotBeginWith,
            Self::EndsWith,
            Self::DoesNotEndWith,
            Self::IsNull,
            Self::IsNotNull,
            Self::IsEmpty,
            Self::IsNotEmpty,
            Self::IsBetween,
            Self::IsNotBetween,
            Self::IsInList,
            Self::IsNotInList,
            Self::Custom,
        ]
    }

    /// Convert operator to SQL fragment
    /// Returns (sql_fragment, needs_value, is_pattern)
    /// If needs_value is true, the value should be bound as a parameter
    /// If is_pattern is true, the value should be wrapped with % for LIKE
    pub fn to_sql_fragment(&self, column: &str, value: &str, value2: Option<&str>) -> String {
        let escaped_col = format!("\"{}\"", column.replace("\"", "\"\""));
        let escaped_val = escape_sql_value(value);

        match self {
            Self::Equal => format!("{} = {}", escaped_col, escaped_val),
            Self::NotEqual => format!("{} != {}", escaped_col, escaped_val),
            Self::LessThan => format!("{} < {}", escaped_col, escaped_val),
            Self::LessThanOrEqual => format!("{} <= {}", escaped_col, escaped_val),
            Self::GreaterThan => format!("{} > {}", escaped_col, escaped_val),
            Self::GreaterThanOrEqual => format!("{} >= {}", escaped_col, escaped_val),
            Self::Contains => format!("{} LIKE '%{}%'", escaped_col, escape_like_value(value)),
            Self::DoesNotContain => {
                format!("{} NOT LIKE '%{}%'", escaped_col, escape_like_value(value))
            }
            Self::BeginsWith => format!("{} LIKE '{}%'", escaped_col, escape_like_value(value)),
            Self::DoesNotBeginWith => {
                format!("{} NOT LIKE '{}%'", escaped_col, escape_like_value(value))
            }
            Self::EndsWith => format!("{} LIKE '%{}'", escaped_col, escape_like_value(value)),
            Self::DoesNotEndWith => {
                format!("{} NOT LIKE '%{}'", escaped_col, escape_like_value(value))
            }
            Self::IsNull => format!("{} IS NULL", escaped_col),
            Self::IsNotNull => format!("{} IS NOT NULL", escaped_col),
            Self::IsEmpty => format!("{} = ''", escaped_col),
            Self::IsNotEmpty => format!("{} != ''", escaped_col),
            Self::IsBetween => {
                let val2 = value2
                    .map(escape_sql_value)
                    .unwrap_or_else(|| escaped_val.clone());
                format!("{} BETWEEN {} AND {}", escaped_col, escaped_val, val2)
            }
            Self::IsNotBetween => {
                let val2 = value2
                    .map(escape_sql_value)
                    .unwrap_or_else(|| escaped_val.clone());
                format!("{} NOT BETWEEN {} AND {}", escaped_col, escaped_val, val2)
            }
            Self::IsInList => {
                let items: Vec<String> = value
                    .split(',')
                    .map(|s| escape_sql_value(s.trim()))
                    .collect();
                format!("{} IN ({})", escaped_col, items.join(", "))
            }
            Self::IsNotInList => {
                let items: Vec<String> = value
                    .split(',')
                    .map(|s| escape_sql_value(s.trim()))
                    .collect();
                format!("{} NOT IN ({})", escaped_col, items.join(", "))
            }
            Self::Custom => value.to_string(),
        }
    }
}

/// Escape a value for SQL (wraps in quotes and escapes internal quotes)
fn escape_sql_value(value: &str) -> String {
    // Try to parse as number first
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        return value.to_string();
    }
    // Handle NULL
    if value.eq_ignore_ascii_case("null") {
        return "NULL".to_string();
    }
    // Handle booleans
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return value.to_uppercase();
    }
    // String value - escape single quotes
    format!("'{}'", value.replace("'", "''"))
}

/// Escape special characters for LIKE pattern
fn escape_like_value(value: &str) -> String {
    value
        .replace("'", "''")
        .replace("%", "\\%")
        .replace("_", "\\_")
}

/// A single filter condition
#[derive(Debug, Clone)]
pub struct FilterCondition {
    /// Unique ID for this filter row
    pub id: usize,
    /// Whether this filter is enabled (checkbox)
    pub enabled: bool,
    /// Column name (None means "[Custom]" is selected)
    pub column: Option<String>,
    /// Filter operator
    pub operator: FilterOperator,
    /// Primary value
    pub value: String,
    /// Secondary value (for BETWEEN operators)
    pub value2: Option<String>,
    /// Custom SQL expression (when operator is Custom or column is Custom)
    pub custom_sql: Option<String>,
    /// Logical operator to use AFTER this filter (AND/OR) - applies between this filter and the next
    pub logical_operator: LogicalOperator,
}

impl FilterCondition {
    /// Create a new empty filter condition
    pub fn new(id: usize) -> Self {
        Self {
            id,
            enabled: true,
            column: None,
            operator: FilterOperator::Equal,
            value: String::new(),
            value2: None,
            custom_sql: None,
            logical_operator: LogicalOperator::And,
        }
    }

    /// Convert this condition to SQL WHERE clause fragment
    pub fn to_sql(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }

        // If custom SQL is set, use it directly
        if let Some(ref custom) = self.custom_sql {
            if !custom.trim().is_empty() {
                return Some(custom.clone());
            }
        }

        // For Custom operator, use the value as raw SQL
        if self.operator.is_custom() {
            if !self.value.trim().is_empty() {
                return Some(self.value.clone());
            }
            return None;
        }

        // Need a column for non-custom operators
        let column = self.column.as_ref()?;
        if column.is_empty() {
            return None;
        }

        // Operators that don't require a value
        if !self.operator.requires_value() {
            return Some(self.operator.to_sql_fragment(column, "", None));
        }

        // Need a value for value-based operators
        if self.value.is_empty() && !self.operator.is_custom() {
            return None;
        }

        Some(
            self.operator
                .to_sql_fragment(column, &self.value, self.value2.as_deref()),
        )
    }

    /// Check if this is a valid, complete filter
    pub fn is_valid(&self) -> bool {
        if !self.enabled {
            return true; // Disabled filters are always "valid"
        }

        // Custom SQL case
        if self.custom_sql.is_some() || self.operator.is_custom() {
            return !self.value.trim().is_empty()
                || self
                    .custom_sql
                    .as_ref()
                    .map(|s| !s.trim().is_empty())
                    .unwrap_or(false);
        }

        // Need a column
        if self.column.is_none() {
            return false;
        }

        // NULL/Empty operators don't need a value
        if !self.operator.requires_value() {
            return true;
        }

        // Need a value
        !self.value.is_empty()
    }
}

impl Default for FilterCondition {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Logical operator for combining filters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogicalOperator {
    #[default]
    And,
    Or,
}

impl LogicalOperator {
    pub fn label(&self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
        }
    }

    pub fn sql(&self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
        }
    }

    pub fn toggle(&self) -> Self {
        match self {
            Self::And => Self::Or,
            Self::Or => Self::And,
        }
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

impl SortDirection {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        }
    }

    #[allow(dead_code)]
    pub fn icon_name(&self) -> &'static str {
        match self {
            Self::Ascending => "arrow-up",
            Self::Descending => "arrow-down",
        }
    }

    pub fn toggle(&self) -> Self {
        match self {
            Self::Ascending => Self::Descending,
            Self::Descending => Self::Ascending,
        }
    }
}

/// A single sort criterion
#[derive(Debug, Clone)]
pub struct SortCriterion {
    /// Unique ID for this sort item
    pub id: usize,
    /// Column name to sort by
    pub column: String,
    /// Sort direction
    pub direction: SortDirection,
}

impl SortCriterion {
    pub fn new(id: usize, column: String) -> Self {
        Self {
            id,
            column,
            direction: SortDirection::Ascending,
        }
    }

    /// Convert to SQL ORDER BY fragment
    pub fn to_sql(&self) -> String {
        let escaped_col = format!("\"{}\"", self.column.replace("\"", "\"\""));
        format!("{} {}", escaped_col, self.direction.label())
    }
}

/// Column visibility state
#[derive(Debug, Clone)]
pub struct ColumnVisibility {
    /// Column name
    pub name: String,
    /// Column data type (for display)
    pub data_type: String,
    /// Whether the column is visible
    pub visible: bool,
}

impl ColumnVisibility {
    pub fn new(name: String, data_type: String) -> Self {
        Self {
            name,
            data_type,
            visible: true,
        }
    }
}

/// A saved filter/sort profile
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FilterProfile {
    /// Profile name
    pub name: String,
    /// Profile description
    pub description: Option<String>,
    /// Filter conditions
    pub filters: Vec<FilterCondition>,
    /// Sort criteria
    pub sorts: Vec<SortCriterion>,
    /// Column visibility settings
    pub column_visibility: Vec<ColumnVisibility>,
    /// Whether this is a default profile
    pub is_default: bool,
    /// Table name this profile is for
    pub table_name: String,
    /// Connection ID this profile is for (optional - if None, applies to any connection)
    pub connection_id: Option<String>,
}

#[allow(dead_code)]
impl FilterProfile {
    pub fn new(name: String, table_name: String) -> Self {
        Self {
            name,
            description: None,
            filters: Vec::new(),
            sorts: Vec::new(),
            column_visibility: Vec::new(),
            is_default: false,
            table_name,
            connection_id: None,
        }
    }
}

/// Column item for the searchable dropdown
#[derive(Debug, Clone)]
pub struct ColumnSelectItem {
    pub name: SharedString,
    pub data_type: SharedString,
    pub is_custom: bool,
}

#[allow(dead_code)]
impl ColumnSelectItem {
    pub fn new(name: impl Into<SharedString>, data_type: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            is_custom: false,
        }
    }

    pub fn custom() -> Self {
        Self {
            name: "[Custom]".into(),
            data_type: "SQL".into(),
            is_custom: true,
        }
    }
}

/// Operator item for the dropdown
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OperatorSelectItem {
    pub operator: FilterOperator,
    pub label: SharedString,
}

impl OperatorSelectItem {
    #[allow(dead_code)]
    pub fn new(operator: FilterOperator) -> Self {
        Self {
            operator,
            label: operator.label().into(),
        }
    }

    #[allow(dead_code)]
    pub fn all() -> Vec<Self> {
        FilterOperator::all()
            .iter()
            .map(|op| Self::new(*op))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_operator_to_sql() {
        assert_eq!(
            FilterOperator::Equal.to_sql_fragment("name", "John", None),
            "\"name\" = 'John'"
        );
        assert_eq!(
            FilterOperator::Equal.to_sql_fragment("age", "25", None),
            "\"age\" = 25"
        );
        assert_eq!(
            FilterOperator::Contains.to_sql_fragment("name", "test", None),
            "\"name\" LIKE '%test%'"
        );
        assert_eq!(
            FilterOperator::IsNull.to_sql_fragment("name", "", None),
            "\"name\" IS NULL"
        );
        assert_eq!(
            FilterOperator::IsBetween.to_sql_fragment("age", "18", Some("65")),
            "\"age\" BETWEEN 18 AND 65"
        );
        assert_eq!(
            FilterOperator::IsInList.to_sql_fragment("status", "active, pending, done", None),
            "\"status\" IN ('active', 'pending', 'done')"
        );
    }

    #[test]
    fn test_escape_sql_value() {
        assert_eq!(escape_sql_value("hello"), "'hello'");
        assert_eq!(escape_sql_value("it's"), "'it''s'");
        assert_eq!(escape_sql_value("123"), "123");
        assert_eq!(escape_sql_value("12.5"), "12.5");
        assert_eq!(escape_sql_value("null"), "NULL");
        assert_eq!(escape_sql_value("true"), "TRUE");
    }

    #[test]
    fn test_filter_condition_to_sql() {
        let mut condition = FilterCondition::new(1);
        condition.column = Some("name".to_string());
        condition.operator = FilterOperator::Equal;
        condition.value = "John".to_string();

        assert_eq!(condition.to_sql(), Some("\"name\" = 'John'".to_string()));

        // Disabled filter returns None
        condition.enabled = false;
        assert_eq!(condition.to_sql(), None);
    }

    #[test]
    fn test_sort_criterion_to_sql() {
        let sort = SortCriterion::new(1, "name".to_string());
        assert_eq!(sort.to_sql(), "\"name\" ASC");

        let mut sort_desc = SortCriterion::new(2, "created_at".to_string());
        sort_desc.direction = SortDirection::Descending;
        assert_eq!(sort_desc.to_sql(), "\"created_at\" DESC");
    }
}
