//! Index design model

use zqlz_core::IndexInfo;

/// Index design model
#[derive(Debug, Clone)]
pub struct IndexDesign {
    /// Index name
    pub name: String,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Is this a unique index?
    pub is_unique: bool,
    /// Is this the primary key index?
    pub is_primary: bool,
    /// Index type (BTREE, HASH, etc.)
    pub index_type: String,
    /// Comment/description
    pub comment: Option<String>,
}

impl IndexDesign {
    /// Create a new empty index design
    pub fn new() -> Self {
        Self {
            name: String::new(),
            columns: Vec::new(),
            is_unique: false,
            is_primary: false,
            index_type: "BTREE".to_string(),
            comment: None,
        }
    }

    /// Create an index with a name
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::new()
        }
    }

    /// Create from existing index info
    pub fn from_index_info(info: &IndexInfo) -> Self {
        Self {
            name: info.name.clone(),
            columns: info.columns.clone(),
            is_unique: info.is_unique,
            is_primary: info.is_primary,
            index_type: info.index_type.clone(),
            comment: info.comment.clone(),
        }
    }

    /// Builder: add a column
    pub fn column(mut self, name: impl Into<String>) -> Self {
        self.columns.push(name.into());
        self
    }

    /// Builder: set as unique
    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }
}

impl Default for IndexDesign {
    fn default() -> Self {
        Self::new()
    }
}
