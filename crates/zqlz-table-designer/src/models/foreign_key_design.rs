//! Foreign key design model

use zqlz_core::{ForeignKeyAction, ForeignKeyInfo};

/// Foreign key design model
#[derive(Debug, Clone)]
pub struct ForeignKeyDesign {
    /// Constraint name (optional)
    pub name: Option<String>,
    /// Local columns
    pub columns: Vec<String>,
    /// Referenced table name
    pub referenced_table: String,
    /// Referenced schema (optional)
    pub referenced_schema: Option<String>,
    /// Referenced columns
    pub referenced_columns: Vec<String>,
    /// Action on update
    pub on_update: ForeignKeyAction,
    /// Action on delete
    pub on_delete: ForeignKeyAction,
}

impl ForeignKeyDesign {
    /// Create a new empty foreign key design
    pub fn new() -> Self {
        Self {
            name: None,
            columns: Vec::new(),
            referenced_table: String::new(),
            referenced_schema: None,
            referenced_columns: Vec::new(),
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::NoAction,
        }
    }

    /// Create from existing foreign key info
    pub fn from_foreign_key_info(info: &ForeignKeyInfo) -> Self {
        Self {
            name: Some(info.name.clone()),
            columns: info.columns.clone(),
            referenced_table: info.referenced_table.clone(),
            referenced_schema: info.referenced_schema.clone(),
            referenced_columns: info.referenced_columns.clone(),
            on_update: info.on_update,
            on_delete: info.on_delete,
        }
    }

    /// Builder: set constraint name
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Builder: add local column
    pub fn column(mut self, name: impl Into<String>) -> Self {
        self.columns.push(name.into());
        self
    }

    /// Builder: set referenced table
    pub fn references(mut self, table: impl Into<String>) -> Self {
        self.referenced_table = table.into();
        self
    }

    /// Builder: add referenced column
    pub fn referenced_column(mut self, name: impl Into<String>) -> Self {
        self.referenced_columns.push(name.into());
        self
    }

    /// Builder: set on delete action
    pub fn on_delete(mut self, action: ForeignKeyAction) -> Self {
        self.on_delete = action;
        self
    }

    /// Builder: set on update action
    pub fn on_update(mut self, action: ForeignKeyAction) -> Self {
        self.on_update = action;
        self
    }
}

impl Default for ForeignKeyDesign {
    fn default() -> Self {
        Self::new()
    }
}
