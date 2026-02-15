//! Table design model

use std::collections::HashSet;
use zqlz_core::TableDetails;

use super::{
    ColumnDesign, DatabaseDialect, ForeignKeyDesign, IndexDesign, TableOptions, ValidationError,
};

/// Table design model for creating/editing tables
#[derive(Debug, Clone)]
pub struct TableDesign {
    /// Table name
    pub table_name: String,
    /// Schema name (optional, not used by SQLite)
    pub schema: Option<String>,
    /// Database dialect
    pub dialect: DatabaseDialect,
    /// Columns
    pub columns: Vec<ColumnDesign>,
    /// Indexes
    pub indexes: Vec<IndexDesign>,
    /// Foreign keys
    pub foreign_keys: Vec<ForeignKeyDesign>,
    /// Table-level options (driver-specific)
    pub options: TableOptions,
    /// Comment/description
    pub comment: Option<String>,
    /// Whether this is a new table (vs editing existing)
    pub is_new: bool,
}

impl TableDesign {
    /// Create a new empty table design with a name
    pub fn new(table_name: impl Into<String>, dialect: DatabaseDialect) -> Self {
        Self {
            table_name: table_name.into(),
            schema: None,
            dialect,
            columns: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            options: TableOptions::default(),
            comment: None,
            is_new: true,
        }
    }

    /// Create an empty table design (no name)
    pub fn empty(dialect: DatabaseDialect) -> Self {
        Self::new("", dialect)
    }

    /// Create from existing table details
    pub fn from_table_details(details: TableDetails, dialect: DatabaseDialect) -> Self {
        let columns = details
            .columns
            .iter()
            .map(|c| ColumnDesign::from_column_info(c))
            .collect();

        let indexes = details
            .indexes
            .iter()
            .map(|i| IndexDesign::from_index_info(i))
            .collect();

        let foreign_keys = details
            .foreign_keys
            .iter()
            .map(|fk| ForeignKeyDesign::from_foreign_key_info(fk))
            .collect();

        Self {
            table_name: details.info.name,
            schema: details.info.schema,
            dialect,
            columns,
            indexes,
            foreign_keys,
            options: TableOptions::default(),
            comment: details.info.comment,
            is_new: false,
        }
    }

    /// Builder: add a column
    pub fn with_column(mut self, column: ColumnDesign) -> Self {
        let ordinal = self.columns.len();
        let mut col = column;
        col.ordinal = ordinal;
        self.columns.push(col);
        self
    }

    /// Builder: add an index
    pub fn with_index(mut self, index: IndexDesign) -> Self {
        self.indexes.push(index);
        self
    }

    /// Builder: add a foreign key
    pub fn with_foreign_key(mut self, fk: ForeignKeyDesign) -> Self {
        self.foreign_keys.push(fk);
        self
    }

    /// Builder: set schema
    pub fn in_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Add a new column and return mutable reference
    pub fn add_column(&mut self) -> &mut ColumnDesign {
        let ordinal = self.columns.len();
        self.columns.push(ColumnDesign::new(ordinal));
        self.columns.last_mut().unwrap()
    }

    /// Remove a column by index
    pub fn remove_column(&mut self, index: usize) {
        if index < self.columns.len() {
            self.columns.remove(index);
            for (i, col) in self.columns.iter_mut().enumerate() {
                col.ordinal = i;
            }
        }
    }

    /// Move a column up
    pub fn move_column_up(&mut self, index: usize) {
        if index > 0 && index < self.columns.len() {
            self.columns.swap(index, index - 1);
            self.columns[index].ordinal = index;
            self.columns[index - 1].ordinal = index - 1;
        }
    }

    /// Move a column down
    pub fn move_column_down(&mut self, index: usize) {
        if index < self.columns.len().saturating_sub(1) {
            self.columns.swap(index, index + 1);
            self.columns[index].ordinal = index;
            self.columns[index + 1].ordinal = index + 1;
        }
    }

    /// Add a new index and return mutable reference
    pub fn add_index(&mut self) -> &mut IndexDesign {
        self.indexes.push(IndexDesign::new());
        self.indexes.last_mut().unwrap()
    }

    /// Remove an index by index
    pub fn remove_index(&mut self, index: usize) {
        if index < self.indexes.len() {
            self.indexes.remove(index);
        }
    }

    /// Add a new foreign key and return mutable reference
    pub fn add_foreign_key(&mut self) -> &mut ForeignKeyDesign {
        self.foreign_keys.push(ForeignKeyDesign::new());
        self.foreign_keys.last_mut().unwrap()
    }

    /// Remove a foreign key by index
    pub fn remove_foreign_key(&mut self, index: usize) {
        if index < self.foreign_keys.len() {
            self.foreign_keys.remove(index);
        }
    }

    /// Check if the design has any validation errors
    pub fn validate(&self) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if self.table_name.is_empty() {
            errors.push(ValidationError::new("table_name", "Table name is required"));
        }

        if self.columns.is_empty() {
            errors.push(ValidationError::new(
                "columns",
                "At least one column is required",
            ));
        }

        for (i, col) in self.columns.iter().enumerate() {
            if col.name.is_empty() {
                errors.push(ValidationError::new(
                    format!("columns[{}].name", i),
                    format!("Column {} name is required", i + 1),
                ));
            }
            if col.data_type.is_empty() {
                errors.push(ValidationError::new(
                    format!("columns[{}].data_type", i),
                    format!("Column {} data type is required", i + 1),
                ));
            }
        }

        // Check for duplicate column names
        let mut seen_names = HashSet::new();
        for col in &self.columns {
            if !col.name.is_empty() && !seen_names.insert(col.name.to_lowercase()) {
                errors.push(ValidationError::new(
                    "columns",
                    format!("Duplicate column name: {}", col.name),
                ));
            }
        }

        // Validate index columns exist
        let column_names: HashSet<_> = self.columns.iter().map(|c| c.name.to_lowercase()).collect();
        for (i, idx) in self.indexes.iter().enumerate() {
            for col in &idx.columns {
                if !column_names.contains(&col.to_lowercase()) {
                    errors.push(ValidationError::new(
                        format!("indexes[{}].columns", i),
                        format!("Index column '{}' does not exist in table", col),
                    ));
                }
            }
        }

        // Validate foreign key columns exist
        for (i, fk) in self.foreign_keys.iter().enumerate() {
            for col in &fk.columns {
                if !column_names.contains(&col.to_lowercase()) {
                    errors.push(ValidationError::new(
                        format!("foreign_keys[{}].columns", i),
                        format!("Foreign key column '{}' does not exist in table", col),
                    ));
                }
            }
        }

        errors
    }

    /// Get column names for use in dropdowns
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnDesign> {
        self.columns.iter().filter(|c| c.is_primary_key).collect()
    }
}
