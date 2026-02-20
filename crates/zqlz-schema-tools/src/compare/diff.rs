//! Schema diff data structures
//!
//! This module defines the data structures used to represent differences
//! between database schemas, tables, columns, indexes, and other objects.

use serde::{Deserialize, Serialize};
use zqlz_core::{
    ColumnInfo, ConstraintInfo, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo,
    PrimaryKeyInfo, ProcedureInfo, SequenceInfo, TableInfo, TriggerInfo, TypeInfo, ViewInfo,
};

/// Represents the complete diff between two database schemas
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Tables that exist only in the source schema
    pub added_tables: Vec<TableInfo>,
    /// Tables that exist only in the target schema
    pub removed_tables: Vec<TableInfo>,
    /// Tables that exist in both but have differences
    pub modified_tables: Vec<TableDiff>,
    /// Views that exist only in the source schema
    pub added_views: Vec<ViewInfo>,
    /// Views that exist only in the target schema
    pub removed_views: Vec<ViewInfo>,
    /// Views that have different definitions
    pub modified_views: Vec<ViewDiff>,
    /// Functions that exist only in the source
    pub added_functions: Vec<FunctionInfo>,
    /// Functions that exist only in the target
    pub removed_functions: Vec<FunctionInfo>,
    /// Functions that have different definitions
    pub modified_functions: Vec<FunctionDiff>,
    /// Procedures that exist only in the source
    pub added_procedures: Vec<ProcedureInfo>,
    /// Procedures that exist only in the target
    pub removed_procedures: Vec<ProcedureInfo>,
    /// Procedures that have different definitions
    pub modified_procedures: Vec<ProcedureDiff>,
    /// Triggers that exist only in the source
    pub added_triggers: Vec<TriggerInfo>,
    /// Triggers that exist only in the target
    pub removed_triggers: Vec<TriggerInfo>,
    /// Triggers that have different definitions
    pub modified_triggers: Vec<TriggerDiff>,
    /// Sequences that exist only in the source
    pub added_sequences: Vec<SequenceInfo>,
    /// Sequences that exist only in the target
    pub removed_sequences: Vec<SequenceInfo>,
    /// Sequences with different configurations
    pub modified_sequences: Vec<SequenceDiff>,
    /// Custom types that exist only in the source
    pub added_types: Vec<TypeInfo>,
    /// Custom types that exist only in the target
    pub removed_types: Vec<TypeInfo>,
    /// Custom types with different definitions
    pub modified_types: Vec<TypeDiff>,
}

impl SchemaDiff {
    /// Creates a new empty schema diff
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if there are no differences
    pub fn is_empty(&self) -> bool {
        self.added_tables.is_empty()
            && self.removed_tables.is_empty()
            && self.modified_tables.is_empty()
            && self.added_views.is_empty()
            && self.removed_views.is_empty()
            && self.modified_views.is_empty()
            && self.added_functions.is_empty()
            && self.removed_functions.is_empty()
            && self.modified_functions.is_empty()
            && self.added_procedures.is_empty()
            && self.removed_procedures.is_empty()
            && self.modified_procedures.is_empty()
            && self.added_triggers.is_empty()
            && self.removed_triggers.is_empty()
            && self.modified_triggers.is_empty()
            && self.added_sequences.is_empty()
            && self.removed_sequences.is_empty()
            && self.modified_sequences.is_empty()
            && self.added_types.is_empty()
            && self.removed_types.is_empty()
            && self.modified_types.is_empty()
    }

    /// Returns the total number of changes
    pub fn change_count(&self) -> usize {
        self.added_tables.len()
            + self.removed_tables.len()
            + self.modified_tables.len()
            + self.added_views.len()
            + self.removed_views.len()
            + self.modified_views.len()
            + self.added_functions.len()
            + self.removed_functions.len()
            + self.modified_functions.len()
            + self.added_procedures.len()
            + self.removed_procedures.len()
            + self.modified_procedures.len()
            + self.added_triggers.len()
            + self.removed_triggers.len()
            + self.modified_triggers.len()
            + self.added_sequences.len()
            + self.removed_sequences.len()
            + self.modified_sequences.len()
            + self.added_types.len()
            + self.removed_types.len()
            + self.modified_types.len()
    }

    /// Returns true if there are any breaking changes (removals or modifications)
    pub fn has_breaking_changes(&self) -> bool {
        !self.removed_tables.is_empty()
            || !self.modified_tables.iter().all(|t| t.is_safe())
            || !self.removed_views.is_empty()
            || !self.removed_functions.is_empty()
            || !self.removed_procedures.is_empty()
            || !self.removed_triggers.is_empty()
            || !self.removed_sequences.is_empty()
            || !self.removed_types.is_empty()
    }
}

/// Represents differences in a single table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDiff {
    /// The table being compared
    pub table_name: String,
    /// Schema of the table
    pub schema: Option<String>,
    /// Columns that exist only in the source
    pub added_columns: Vec<ColumnInfo>,
    /// Columns that exist only in the target
    pub removed_columns: Vec<ColumnInfo>,
    /// Columns that exist in both but have differences
    pub modified_columns: Vec<ColumnDiff>,
    /// Indexes that exist only in the source
    pub added_indexes: Vec<IndexInfo>,
    /// Indexes that exist only in the target
    pub removed_indexes: Vec<IndexInfo>,
    /// Indexes that have different definitions
    pub modified_indexes: Vec<IndexDiff>,
    /// Foreign keys that exist only in the source
    pub added_foreign_keys: Vec<ForeignKeyInfo>,
    /// Foreign keys that exist only in the target
    pub removed_foreign_keys: Vec<ForeignKeyInfo>,
    /// Foreign keys that have different definitions
    pub modified_foreign_keys: Vec<ForeignKeyDiff>,
    /// Constraints that exist only in the source
    pub added_constraints: Vec<ConstraintInfo>,
    /// Constraints that exist only in the target
    pub removed_constraints: Vec<ConstraintInfo>,
    /// Constraints that have different definitions
    pub modified_constraints: Vec<ConstraintDiff>,
    /// Primary key changes (None if unchanged)
    pub primary_key_change: Option<PrimaryKeyChange>,
}

impl TableDiff {
    /// Creates a new table diff
    pub fn new(table_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            table_name: table_name.into(),
            schema,
            added_columns: Vec::new(),
            removed_columns: Vec::new(),
            modified_columns: Vec::new(),
            added_indexes: Vec::new(),
            removed_indexes: Vec::new(),
            modified_indexes: Vec::new(),
            added_foreign_keys: Vec::new(),
            removed_foreign_keys: Vec::new(),
            modified_foreign_keys: Vec::new(),
            added_constraints: Vec::new(),
            removed_constraints: Vec::new(),
            modified_constraints: Vec::new(),
            primary_key_change: None,
        }
    }

    /// Returns the qualified table name (schema.table or just table)
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.table_name),
            None => self.table_name.clone(),
        }
    }

    /// Returns true if there are no changes in this table
    pub fn is_empty(&self) -> bool {
        self.added_columns.is_empty()
            && self.removed_columns.is_empty()
            && self.modified_columns.is_empty()
            && self.added_indexes.is_empty()
            && self.removed_indexes.is_empty()
            && self.modified_indexes.is_empty()
            && self.added_foreign_keys.is_empty()
            && self.removed_foreign_keys.is_empty()
            && self.modified_foreign_keys.is_empty()
            && self.added_constraints.is_empty()
            && self.removed_constraints.is_empty()
            && self.modified_constraints.is_empty()
            && self.primary_key_change.is_none()
    }

    /// Returns true if all changes are safe (additions only, no breaking changes)
    pub fn is_safe(&self) -> bool {
        self.removed_columns.is_empty()
            && self.modified_columns.iter().all(|c| c.is_safe())
            && self.removed_indexes.is_empty()
            && self.removed_foreign_keys.is_empty()
            && self.removed_constraints.is_empty()
            && self
                .primary_key_change
                .as_ref()
                .map(|c| c.is_safe())
                .unwrap_or(true)
    }
}

/// Represents changes to a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDiff {
    /// Column name
    pub column_name: String,
    /// Data type change (old, new)
    pub type_change: Option<(String, String)>,
    /// Nullable change (old, new)
    pub nullable_change: Option<(bool, bool)>,
    /// Default value change (old, new)
    pub default_change: Option<(Option<String>, Option<String>)>,
    /// Max length change (old, new)
    pub max_length_change: Option<(Option<i64>, Option<i64>)>,
    /// Precision change (old, new)
    pub precision_change: Option<(Option<i32>, Option<i32>)>,
    /// Scale change (old, new)
    pub scale_change: Option<(Option<i32>, Option<i32>)>,
    /// Comment change (old, new)
    pub comment_change: Option<(Option<String>, Option<String>)>,
}

impl ColumnDiff {
    /// Creates a new column diff
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            type_change: None,
            nullable_change: None,
            default_change: None,
            max_length_change: None,
            precision_change: None,
            scale_change: None,
            comment_change: None,
        }
    }

    /// Returns true if there are no changes
    pub fn is_empty(&self) -> bool {
        self.type_change.is_none()
            && self.nullable_change.is_none()
            && self.default_change.is_none()
            && self.max_length_change.is_none()
            && self.precision_change.is_none()
            && self.scale_change.is_none()
            && self.comment_change.is_none()
    }

    /// Returns true if all changes are safe (no data loss risk)
    pub fn is_safe(&self) -> bool {
        if let Some((old_nullable, new_nullable)) = self.nullable_change
            && old_nullable && !new_nullable
        {
            return false;
        }
        true
    }
}

/// Represents changes to an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDiff {
    /// Index name
    pub index_name: String,
    /// Old index definition
    pub old: IndexInfo,
    /// New index definition
    pub new: IndexInfo,
}

impl IndexDiff {
    /// Creates a new index diff
    pub fn new(index_name: impl Into<String>, old: IndexInfo, new: IndexInfo) -> Self {
        Self {
            index_name: index_name.into(),
            old,
            new,
        }
    }
}

/// Represents changes to a foreign key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDiff {
    /// Foreign key name
    pub fk_name: String,
    /// Changes to on_update action
    pub on_update_change: Option<(ForeignKeyAction, ForeignKeyAction)>,
    /// Changes to on_delete action
    pub on_delete_change: Option<(ForeignKeyAction, ForeignKeyAction)>,
    /// Changes to referenced table
    pub referenced_table_change: Option<(String, String)>,
    /// Changes to columns
    pub columns_change: Option<(Vec<String>, Vec<String>)>,
}

impl ForeignKeyDiff {
    /// Creates a new foreign key diff
    pub fn new(fk_name: impl Into<String>) -> Self {
        Self {
            fk_name: fk_name.into(),
            on_update_change: None,
            on_delete_change: None,
            referenced_table_change: None,
            columns_change: None,
        }
    }
}

/// Represents changes to a constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDiff {
    /// Constraint name
    pub constraint_name: String,
    /// Old constraint definition
    pub old: ConstraintInfo,
    /// New constraint definition
    pub new: ConstraintInfo,
}

impl ConstraintDiff {
    /// Creates a new constraint diff
    pub fn new(
        constraint_name: impl Into<String>,
        old: ConstraintInfo,
        new: ConstraintInfo,
    ) -> Self {
        Self {
            constraint_name: constraint_name.into(),
            old,
            new,
        }
    }
}

/// Represents changes to the primary key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrimaryKeyChange {
    /// Primary key was added
    Added(PrimaryKeyInfo),
    /// Primary key was removed
    Removed(PrimaryKeyInfo),
    /// Primary key columns changed
    Modified {
        old: PrimaryKeyInfo,
        new: PrimaryKeyInfo,
    },
}

impl PrimaryKeyChange {
    /// Returns true if this is a safe change (addition only)
    pub fn is_safe(&self) -> bool {
        matches!(self, PrimaryKeyChange::Added(_))
    }
}

/// Represents changes to a view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDiff {
    /// View name
    pub view_name: String,
    /// Schema of the view
    pub schema: Option<String>,
    /// Definition change (old, new)
    pub definition_change: Option<(Option<String>, Option<String>)>,
    /// Materialized flag change
    pub materialized_change: Option<(bool, bool)>,
}

impl ViewDiff {
    /// Creates a new view diff
    pub fn new(view_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            view_name: view_name.into(),
            schema,
            definition_change: None,
            materialized_change: None,
        }
    }

    /// Returns the qualified view name
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.view_name),
            None => self.view_name.clone(),
        }
    }
}

/// Represents changes to a function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDiff {
    /// Function name
    pub function_name: String,
    /// Schema of the function
    pub schema: Option<String>,
    /// Return type change
    pub return_type_change: Option<(String, String)>,
    /// Language change
    pub language_change: Option<(String, String)>,
    /// Definition change
    pub definition_change: Option<(Option<String>, Option<String>)>,
}

impl FunctionDiff {
    /// Creates a new function diff
    pub fn new(function_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            function_name: function_name.into(),
            schema,
            return_type_change: None,
            language_change: None,
            definition_change: None,
        }
    }
}

/// Represents changes to a procedure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureDiff {
    /// Procedure name
    pub procedure_name: String,
    /// Schema of the procedure
    pub schema: Option<String>,
    /// Language change
    pub language_change: Option<(String, String)>,
    /// Definition change
    pub definition_change: Option<(Option<String>, Option<String>)>,
}

impl ProcedureDiff {
    /// Creates a new procedure diff
    pub fn new(procedure_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            procedure_name: procedure_name.into(),
            schema,
            language_change: None,
            definition_change: None,
        }
    }
}

/// Represents changes to a trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDiff {
    /// Trigger name
    pub trigger_name: String,
    /// Schema of the trigger
    pub schema: Option<String>,
    /// Table name the trigger is on
    pub table_name: String,
    /// Definition change
    pub definition_change: Option<(Option<String>, Option<String>)>,
    /// Enabled status change
    pub enabled_change: Option<(bool, bool)>,
}

impl TriggerDiff {
    /// Creates a new trigger diff
    pub fn new(
        trigger_name: impl Into<String>,
        table_name: impl Into<String>,
        schema: Option<String>,
    ) -> Self {
        Self {
            trigger_name: trigger_name.into(),
            schema,
            table_name: table_name.into(),
            definition_change: None,
            enabled_change: None,
        }
    }
}

/// Represents changes to a sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDiff {
    /// Sequence name
    pub sequence_name: String,
    /// Schema of the sequence
    pub schema: Option<String>,
    /// Start value change
    pub start_value_change: Option<(i64, i64)>,
    /// Increment change
    pub increment_change: Option<(i64, i64)>,
    /// Min value change
    pub min_value_change: Option<(i64, i64)>,
    /// Max value change
    pub max_value_change: Option<(i64, i64)>,
}

impl SequenceDiff {
    /// Creates a new sequence diff
    pub fn new(sequence_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            sequence_name: sequence_name.into(),
            schema,
            start_value_change: None,
            increment_change: None,
            min_value_change: None,
            max_value_change: None,
        }
    }
}

/// Represents changes to a custom type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDiff {
    /// Type name
    pub type_name: String,
    /// Schema of the type
    pub schema: Option<String>,
    /// Values change (for enums)
    #[allow(clippy::type_complexity)]
    pub values_change: Option<(Option<Vec<String>>, Option<Vec<String>>)>,
    /// Definition change
    pub definition_change: Option<(Option<String>, Option<String>)>,
}

impl TypeDiff {
    /// Creates a new type diff
    pub fn new(type_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            type_name: type_name.into(),
            schema,
            values_change: None,
            definition_change: None,
        }
    }
}
