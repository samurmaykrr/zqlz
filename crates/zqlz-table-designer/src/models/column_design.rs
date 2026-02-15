//! Column design model

use zqlz_core::ColumnInfo;

/// Column design model for table designer
#[derive(Debug, Clone)]
pub struct ColumnDesign {
    /// Column name
    pub name: String,
    /// Data type (e.g., "INTEGER", "VARCHAR", "TEXT")
    pub data_type: String,
    /// Length for types that support it (e.g., VARCHAR(255))
    pub length: Option<u32>,
    /// Scale for DECIMAL types
    pub scale: Option<u32>,
    /// Whether NULL values are allowed
    pub nullable: bool,
    /// Default value expression
    pub default_value: Option<String>,
    /// Is this column part of the primary key?
    pub is_primary_key: bool,
    /// Is this part of a composite primary key?
    pub is_part_of_composite_pk: bool,
    /// Is this column auto-incrementing?
    pub is_auto_increment: bool,
    /// Is this column unique?
    pub is_unique: bool,
    /// Column ordinal position
    pub ordinal: usize,
    /// Comment/description
    pub comment: Option<String>,
    /// Virtual/computed column expression (for SQLite GENERATED columns)
    pub generated_expression: Option<String>,
    /// Whether the generated column is STORED or VIRTUAL
    pub generated_stored: bool,
}

impl ColumnDesign {
    /// Create a new empty column design
    pub fn new(ordinal: usize) -> Self {
        Self {
            name: String::new(),
            data_type: "TEXT".to_string(),
            length: None,
            scale: None,
            nullable: true,
            default_value: None,
            is_primary_key: false,
            is_part_of_composite_pk: false,
            is_auto_increment: false,
            is_unique: false,
            ordinal,
            comment: None,
            generated_expression: None,
            generated_stored: false,
        }
    }

    /// Create a column with a specific name
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::new(0)
        }
    }

    /// Create from existing column info
    pub fn from_column_info(info: &ColumnInfo) -> Self {
        Self {
            name: info.name.clone(),
            data_type: info.data_type.clone(),
            length: info.max_length.map(|l| l as u32),
            scale: info.scale.map(|s| s as u32),
            nullable: info.nullable,
            default_value: info.default_value.clone(),
            is_primary_key: info.is_primary_key,
            is_part_of_composite_pk: false,
            is_auto_increment: info.is_auto_increment,
            is_unique: info.is_unique,
            ordinal: info.ordinal,
            comment: info.comment.clone(),
            generated_expression: None,
            generated_stored: false,
        }
    }

    /// Builder: set data type
    pub fn data_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = data_type.into();
        self
    }

    /// Builder: set as integer type
    pub fn integer(mut self) -> Self {
        self.data_type = "INTEGER".to_string();
        self
    }

    /// Builder: set as text type
    pub fn text(mut self) -> Self {
        self.data_type = "TEXT".to_string();
        self
    }

    /// Builder: set as primary key
    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.nullable = false;
        self
    }

    /// Builder: set as not null
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Builder: set as unique
    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    /// Builder: set as auto increment
    pub fn auto_increment(mut self) -> Self {
        self.is_auto_increment = true;
        self
    }

    /// Builder: set default value
    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }

    /// Builder: set length
    pub fn with_length(mut self, length: u32) -> Self {
        self.length = Some(length);
        self
    }
}
