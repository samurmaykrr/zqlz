//! Check constraint design model

use zqlz_core::ConstraintInfo;

/// Check constraint design model
#[derive(Debug, Clone)]
pub struct CheckConstraintDesign {
    /// Constraint name (optional)
    pub name: Option<String>,
    /// CHECK expression (the SQL condition)
    pub expression: String,
    /// NO INHERIT (PostgreSQL: prevent child tables from inheriting this constraint)
    pub no_inherit: bool,
}

impl CheckConstraintDesign {
    /// Create a new empty check constraint
    pub fn new() -> Self {
        Self {
            name: None,
            expression: String::new(),
            no_inherit: false,
        }
    }

    /// Create from existing constraint info
    pub fn from_constraint_info(info: &ConstraintInfo) -> Self {
        Self {
            name: Some(info.name.clone()),
            expression: info.definition.clone().unwrap_or_default(),
            no_inherit: false,
        }
    }

    /// Builder: set constraint name
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Builder: set expression
    pub fn expression(mut self, expr: impl Into<String>) -> Self {
        self.expression = expr.into();
        self
    }

    /// Auto-generate a constraint name from table name and a sequence number
    pub fn auto_name(&mut self, table_name: &str, index: usize) {
        self.name = Some(format!("chk_{}_{}", table_name, index + 1));
    }
}

impl Default for CheckConstraintDesign {
    fn default() -> Self {
        Self::new()
    }
}
