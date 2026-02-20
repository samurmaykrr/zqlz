//! Schema comparator implementation
//!
//! Provides functionality to compare two database schemas and generate diffs.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zqlz_core::{
    ColumnInfo, ConstraintInfo, ForeignKeyInfo, FunctionInfo, IndexInfo, PrimaryKeyInfo,
    ProcedureInfo, SequenceInfo, TableDetails, TableInfo, TriggerInfo, TypeInfo, ViewInfo,
};

use super::diff::{
    ColumnDiff, ConstraintDiff, ForeignKeyDiff, FunctionDiff, IndexDiff, PrimaryKeyChange,
    ProcedureDiff, SchemaDiff, SequenceDiff, TableDiff, TriggerDiff, TypeDiff, ViewDiff,
};

/// Errors that can occur during schema comparison
#[derive(Debug, Error)]
pub enum CompareError {
    /// Schema source is empty or invalid
    #[error("source schema is empty")]
    EmptySource,
    /// Schema target is empty or invalid
    #[error("target schema is empty")]
    EmptyTarget,
    /// Table details not found
    #[error("table details not found for '{0}'")]
    TableDetailsNotFound(String),
}

/// Result type for comparison operations
pub type CompareResult<T> = Result<T, CompareError>;

/// Configuration for schema comparison
#[derive(Debug, Clone)]
pub struct CompareConfig {
    /// Whether to compare table comments
    pub compare_comments: bool,
    /// Whether to compare indexes
    pub compare_indexes: bool,
    /// Whether to compare foreign keys
    pub compare_foreign_keys: bool,
    /// Whether to compare constraints
    pub compare_constraints: bool,
    /// Whether to compare triggers
    pub compare_triggers: bool,
    /// Whether to ignore column order
    pub ignore_column_order: bool,
    /// Whether to treat case differences as changes
    pub case_sensitive: bool,
}

impl Default for CompareConfig {
    fn default() -> Self {
        Self {
            compare_comments: true,
            compare_indexes: true,
            compare_foreign_keys: true,
            compare_constraints: true,
            compare_triggers: true,
            ignore_column_order: false,
            case_sensitive: true,
        }
    }
}

impl CompareConfig {
    /// Creates a new config with all options enabled
    pub fn new() -> Self {
        Self::default()
    }

    /// Disables comment comparison
    pub fn without_comments(mut self) -> Self {
        self.compare_comments = false;
        self
    }

    /// Disables index comparison
    pub fn without_indexes(mut self) -> Self {
        self.compare_indexes = false;
        self
    }

    /// Disables foreign key comparison
    pub fn without_foreign_keys(mut self) -> Self {
        self.compare_foreign_keys = false;
        self
    }

    /// Disables constraint comparison
    pub fn without_constraints(mut self) -> Self {
        self.compare_constraints = false;
        self
    }

    /// Disables trigger comparison
    pub fn without_triggers(mut self) -> Self {
        self.compare_triggers = false;
        self
    }

    /// Ignores column order when comparing tables
    pub fn ignore_column_order(mut self) -> Self {
        self.ignore_column_order = true;
        self
    }

    /// Makes comparison case insensitive
    pub fn case_insensitive(mut self) -> Self {
        self.case_sensitive = false;
        self
    }
}

/// Schema comparator for comparing database schemas
#[derive(Debug)]
pub struct SchemaComparator {
    config: CompareConfig,
}

impl Default for SchemaComparator {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaComparator {
    /// Creates a new schema comparator with default configuration
    pub fn new() -> Self {
        Self {
            config: CompareConfig::default(),
        }
    }

    /// Creates a new schema comparator with the given configuration
    pub fn with_config(config: CompareConfig) -> Self {
        Self { config }
    }

    /// Returns the current configuration
    pub fn config(&self) -> &CompareConfig {
        &self.config
    }

    /// Normalizes a name for comparison based on case sensitivity setting
    fn normalize_name(&self, name: &str) -> String {
        if self.config.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    }

    /// Compares two sets of tables and returns the diff
    pub fn compare_tables(
        &self,
        source_tables: &[TableInfo],
        target_tables: &[TableInfo],
        source_details: &HashMap<String, TableDetails>,
        target_details: &HashMap<String, TableDetails>,
    ) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_names: HashSet<String> = source_tables
            .iter()
            .map(|t| self.normalize_name(&t.name))
            .collect();
        let target_names: HashSet<String> = target_tables
            .iter()
            .map(|t| self.normalize_name(&t.name))
            .collect();

        for table in source_tables {
            let normalized = self.normalize_name(&table.name);
            if !target_names.contains(&normalized) {
                diff.added_tables.push(table.clone());
            } else if let (Some(source_detail), Some(target_detail)) = (
                source_details.get(&table.name),
                target_details.get(&table.name),
            )
                && let Some(table_diff) = self.compare_table_details(source_detail, target_detail)
            {
                if !table_diff.is_empty() {
                    diff.modified_tables.push(table_diff);
                }
            }
        }

        for table in target_tables {
            let normalized = self.normalize_name(&table.name);
            if !source_names.contains(&normalized) {
                diff.removed_tables.push(table.clone());
            }
        }

        diff
    }

    /// Compares two table details and returns the diff
    pub fn compare_table_details(
        &self,
        source: &TableDetails,
        target: &TableDetails,
    ) -> Option<TableDiff> {
        let mut table_diff = TableDiff::new(&source.info.name, source.info.schema.clone());

        self.compare_columns(&source.columns, &target.columns, &mut table_diff);

        if self.config.compare_indexes {
            self.compare_indexes(&source.indexes, &target.indexes, &mut table_diff);
        }

        if self.config.compare_foreign_keys {
            self.compare_foreign_keys(&source.foreign_keys, &target.foreign_keys, &mut table_diff);
        }

        if self.config.compare_constraints {
            self.compare_constraints(&source.constraints, &target.constraints, &mut table_diff);
        }

        self.compare_primary_keys(&source.primary_key, &target.primary_key, &mut table_diff);

        if table_diff.is_empty() {
            None
        } else {
            Some(table_diff)
        }
    }

    /// Compares columns between source and target
    fn compare_columns(&self, source: &[ColumnInfo], target: &[ColumnInfo], diff: &mut TableDiff) {
        let source_map: HashMap<String, &ColumnInfo> = source
            .iter()
            .map(|c| (self.normalize_name(&c.name), c))
            .collect();
        let target_map: HashMap<String, &ColumnInfo> = target
            .iter()
            .map(|c| (self.normalize_name(&c.name), c))
            .collect();

        for col in source {
            let normalized = self.normalize_name(&col.name);
            if !target_map.contains_key(&normalized) {
                diff.added_columns.push(col.clone());
            } else if let Some(target_col) = target_map.get(&normalized)
                && let Some(col_diff) = self.compare_column(col, target_col)
            {
                if !col_diff.is_empty() {
                    diff.modified_columns.push(col_diff);
                }
            }
        }

        for col in target {
            let normalized = self.normalize_name(&col.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_columns.push(col.clone());
            }
        }
    }

    /// Compares two columns and returns the diff
    fn compare_column(&self, source: &ColumnInfo, target: &ColumnInfo) -> Option<ColumnDiff> {
        let mut col_diff = ColumnDiff::new(&source.name);

        if self.normalize_name(&source.data_type) != self.normalize_name(&target.data_type) {
            col_diff.type_change = Some((source.data_type.clone(), target.data_type.clone()));
        }

        if source.nullable != target.nullable {
            col_diff.nullable_change = Some((source.nullable, target.nullable));
        }

        if source.default_value != target.default_value {
            col_diff.default_change =
                Some((source.default_value.clone(), target.default_value.clone()));
        }

        if source.max_length != target.max_length {
            col_diff.max_length_change = Some((source.max_length, target.max_length));
        }

        if source.precision != target.precision {
            col_diff.precision_change = Some((source.precision, target.precision));
        }

        if source.scale != target.scale {
            col_diff.scale_change = Some((source.scale, target.scale));
        }

        if self.config.compare_comments && source.comment != target.comment {
            col_diff.comment_change = Some((source.comment.clone(), target.comment.clone()));
        }

        if col_diff.is_empty() {
            None
        } else {
            Some(col_diff)
        }
    }

    /// Compares indexes between source and target
    fn compare_indexes(&self, source: &[IndexInfo], target: &[IndexInfo], diff: &mut TableDiff) {
        let source_map: HashMap<String, &IndexInfo> = source
            .iter()
            .map(|i| (self.normalize_name(&i.name), i))
            .collect();
        let target_map: HashMap<String, &IndexInfo> = target
            .iter()
            .map(|i| (self.normalize_name(&i.name), i))
            .collect();

        for idx in source {
            let normalized = self.normalize_name(&idx.name);
            if !target_map.contains_key(&normalized) {
                diff.added_indexes.push(idx.clone());
            } else if let Some(target_idx) = target_map.get(&normalized)
                && !self.indexes_equal(idx, target_idx)
            {
                diff.modified_indexes.push(IndexDiff::new(
                    &idx.name,
                    (*target_idx).clone(),
                    idx.clone(),
                ));
            }
        }

        for idx in target {
            let normalized = self.normalize_name(&idx.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_indexes.push(idx.clone());
            }
        }
    }

    /// Checks if two indexes are equal
    fn indexes_equal(&self, a: &IndexInfo, b: &IndexInfo) -> bool {
        a.columns == b.columns
            && a.is_unique == b.is_unique
            && a.is_primary == b.is_primary
            && self.normalize_name(&a.index_type) == self.normalize_name(&b.index_type)
    }

    /// Compares foreign keys between source and target
    fn compare_foreign_keys(
        &self,
        source: &[ForeignKeyInfo],
        target: &[ForeignKeyInfo],
        diff: &mut TableDiff,
    ) {
        let source_map: HashMap<String, &ForeignKeyInfo> = source
            .iter()
            .map(|fk| (self.normalize_name(&fk.name), fk))
            .collect();
        let target_map: HashMap<String, &ForeignKeyInfo> = target
            .iter()
            .map(|fk| (self.normalize_name(&fk.name), fk))
            .collect();

        for fk in source {
            let normalized = self.normalize_name(&fk.name);
            if !target_map.contains_key(&normalized) {
                diff.added_foreign_keys.push(fk.clone());
            } else if let Some(target_fk) = target_map.get(&normalized)
                && let Some(fk_diff) = self.compare_foreign_key(fk, target_fk)
            {
                diff.modified_foreign_keys.push(fk_diff);
            }
        }

        for fk in target {
            let normalized = self.normalize_name(&fk.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_foreign_keys.push(fk.clone());
            }
        }
    }

    /// Compares two foreign keys
    fn compare_foreign_key(
        &self,
        source: &ForeignKeyInfo,
        target: &ForeignKeyInfo,
    ) -> Option<ForeignKeyDiff> {
        let mut fk_diff = ForeignKeyDiff::new(&source.name);

        if source.on_update != target.on_update {
            fk_diff.on_update_change = Some((source.on_update, target.on_update));
        }

        if source.on_delete != target.on_delete {
            fk_diff.on_delete_change = Some((source.on_delete, target.on_delete));
        }

        if self.normalize_name(&source.referenced_table)
            != self.normalize_name(&target.referenced_table)
        {
            fk_diff.referenced_table_change = Some((
                source.referenced_table.clone(),
                target.referenced_table.clone(),
            ));
        }

        if source.columns != target.columns {
            fk_diff.columns_change = Some((source.columns.clone(), target.columns.clone()));
        }

        if fk_diff.on_update_change.is_none()
            && fk_diff.on_delete_change.is_none()
            && fk_diff.referenced_table_change.is_none()
            && fk_diff.columns_change.is_none()
        {
            None
        } else {
            Some(fk_diff)
        }
    }

    /// Compares constraints between source and target
    fn compare_constraints(
        &self,
        source: &[ConstraintInfo],
        target: &[ConstraintInfo],
        diff: &mut TableDiff,
    ) {
        let source_map: HashMap<String, &ConstraintInfo> = source
            .iter()
            .map(|c| (self.normalize_name(&c.name), c))
            .collect();
        let target_map: HashMap<String, &ConstraintInfo> = target
            .iter()
            .map(|c| (self.normalize_name(&c.name), c))
            .collect();

        for constraint in source {
            let normalized = self.normalize_name(&constraint.name);
            if !target_map.contains_key(&normalized) {
                diff.added_constraints.push(constraint.clone());
            } else if let Some(target_constraint) = target_map.get(&normalized)
                && !self.constraints_equal(constraint, target_constraint)
            {
                diff.modified_constraints.push(ConstraintDiff::new(
                    &constraint.name,
                    (*target_constraint).clone(),
                    constraint.clone(),
                ));
            }
        }

        for constraint in target {
            let normalized = self.normalize_name(&constraint.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_constraints.push(constraint.clone());
            }
        }
    }

    /// Checks if two constraints are equal
    fn constraints_equal(&self, a: &ConstraintInfo, b: &ConstraintInfo) -> bool {
        a.constraint_type == b.constraint_type
            && a.columns == b.columns
            && a.definition == b.definition
    }

    /// Compares primary keys
    fn compare_primary_keys(
        &self,
        source: &Option<PrimaryKeyInfo>,
        target: &Option<PrimaryKeyInfo>,
        diff: &mut TableDiff,
    ) {
        match (source, target) {
            (Some(s), None) => {
                diff.primary_key_change = Some(PrimaryKeyChange::Added(s.clone()));
            }
            (None, Some(t)) => {
                diff.primary_key_change = Some(PrimaryKeyChange::Removed(t.clone()));
            }
            (Some(s), Some(t)) if s.columns != t.columns => {
                diff.primary_key_change = Some(PrimaryKeyChange::Modified {
                    old: t.clone(),
                    new: s.clone(),
                });
            }
            _ => {}
        }
    }

    /// Compares views between source and target
    pub fn compare_views(&self, source: &[ViewInfo], target: &[ViewInfo]) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &ViewInfo> = source
            .iter()
            .map(|v| (self.normalize_name(&v.name), v))
            .collect();
        let target_map: HashMap<String, &ViewInfo> = target
            .iter()
            .map(|v| (self.normalize_name(&v.name), v))
            .collect();

        for view in source {
            let normalized = self.normalize_name(&view.name);
            if !target_map.contains_key(&normalized) {
                diff.added_views.push(view.clone());
            } else if let Some(target_view) = target_map.get(&normalized)
                && let Some(view_diff) = self.compare_view(view, target_view)
            {
                diff.modified_views.push(view_diff);
            }
        }

        for view in target {
            let normalized = self.normalize_name(&view.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_views.push(view.clone());
            }
        }

        diff
    }

    /// Compares two views
    fn compare_view(&self, source: &ViewInfo, target: &ViewInfo) -> Option<ViewDiff> {
        let mut view_diff = ViewDiff::new(&source.name, source.schema.clone());

        if source.definition != target.definition {
            view_diff.definition_change =
                Some((source.definition.clone(), target.definition.clone()));
        }

        if source.is_materialized != target.is_materialized {
            view_diff.materialized_change = Some((source.is_materialized, target.is_materialized));
        }

        if view_diff.definition_change.is_none() && view_diff.materialized_change.is_none() {
            None
        } else {
            Some(view_diff)
        }
    }

    /// Compares functions between source and target
    pub fn compare_functions(
        &self,
        source: &[FunctionInfo],
        target: &[FunctionInfo],
    ) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &FunctionInfo> = source
            .iter()
            .map(|f| (self.normalize_name(&f.name), f))
            .collect();
        let target_map: HashMap<String, &FunctionInfo> = target
            .iter()
            .map(|f| (self.normalize_name(&f.name), f))
            .collect();

        for func in source {
            let normalized = self.normalize_name(&func.name);
            if !target_map.contains_key(&normalized) {
                diff.added_functions.push(func.clone());
            } else if let Some(target_func) = target_map.get(&normalized)
                && let Some(func_diff) = self.compare_function(func, target_func)
            {
                diff.modified_functions.push(func_diff);
            }
        }

        for func in target {
            let normalized = self.normalize_name(&func.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_functions.push(func.clone());
            }
        }

        diff
    }

    /// Compares two functions
    fn compare_function(
        &self,
        source: &FunctionInfo,
        target: &FunctionInfo,
    ) -> Option<FunctionDiff> {
        let mut func_diff = FunctionDiff::new(&source.name, source.schema.clone());

        if self.normalize_name(&source.return_type) != self.normalize_name(&target.return_type) {
            func_diff.return_type_change =
                Some((source.return_type.clone(), target.return_type.clone()));
        }

        if self.normalize_name(&source.language) != self.normalize_name(&target.language) {
            func_diff.language_change = Some((source.language.clone(), target.language.clone()));
        }

        if source.definition != target.definition {
            func_diff.definition_change =
                Some((source.definition.clone(), target.definition.clone()));
        }

        if func_diff.return_type_change.is_none()
            && func_diff.language_change.is_none()
            && func_diff.definition_change.is_none()
        {
            None
        } else {
            Some(func_diff)
        }
    }

    /// Compares procedures between source and target
    pub fn compare_procedures(
        &self,
        source: &[ProcedureInfo],
        target: &[ProcedureInfo],
    ) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &ProcedureInfo> = source
            .iter()
            .map(|p| (self.normalize_name(&p.name), p))
            .collect();
        let target_map: HashMap<String, &ProcedureInfo> = target
            .iter()
            .map(|p| (self.normalize_name(&p.name), p))
            .collect();

        for proc in source {
            let normalized = self.normalize_name(&proc.name);
            if !target_map.contains_key(&normalized) {
                diff.added_procedures.push(proc.clone());
            } else if let Some(target_proc) = target_map.get(&normalized)
                && let Some(proc_diff) = self.compare_procedure(proc, target_proc)
            {
                diff.modified_procedures.push(proc_diff);
            }
        }

        for proc in target {
            let normalized = self.normalize_name(&proc.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_procedures.push(proc.clone());
            }
        }

        diff
    }

    /// Compares two procedures
    fn compare_procedure(
        &self,
        source: &ProcedureInfo,
        target: &ProcedureInfo,
    ) -> Option<ProcedureDiff> {
        let mut proc_diff = ProcedureDiff::new(&source.name, source.schema.clone());

        if self.normalize_name(&source.language) != self.normalize_name(&target.language) {
            proc_diff.language_change = Some((source.language.clone(), target.language.clone()));
        }

        if source.definition != target.definition {
            proc_diff.definition_change =
                Some((source.definition.clone(), target.definition.clone()));
        }

        if proc_diff.language_change.is_none() && proc_diff.definition_change.is_none() {
            None
        } else {
            Some(proc_diff)
        }
    }

    /// Compares triggers between source and target
    pub fn compare_triggers(&self, source: &[TriggerInfo], target: &[TriggerInfo]) -> SchemaDiff {
        if !self.config.compare_triggers {
            return SchemaDiff::new();
        }

        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &TriggerInfo> = source
            .iter()
            .map(|t| (self.normalize_name(&t.name), t))
            .collect();
        let target_map: HashMap<String, &TriggerInfo> = target
            .iter()
            .map(|t| (self.normalize_name(&t.name), t))
            .collect();

        for trigger in source {
            let normalized = self.normalize_name(&trigger.name);
            if !target_map.contains_key(&normalized) {
                diff.added_triggers.push(trigger.clone());
            } else if let Some(target_trigger) = target_map.get(&normalized)
                && let Some(trigger_diff) = self.compare_trigger(trigger, target_trigger)
            {
                diff.modified_triggers.push(trigger_diff);
            }
        }

        for trigger in target {
            let normalized = self.normalize_name(&trigger.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_triggers.push(trigger.clone());
            }
        }

        diff
    }

    /// Compares two triggers
    fn compare_trigger(&self, source: &TriggerInfo, target: &TriggerInfo) -> Option<TriggerDiff> {
        let mut trigger_diff =
            TriggerDiff::new(&source.name, &source.table_name, source.schema.clone());

        if source.definition != target.definition {
            trigger_diff.definition_change =
                Some((source.definition.clone(), target.definition.clone()));
        }

        if source.enabled != target.enabled {
            trigger_diff.enabled_change = Some((source.enabled, target.enabled));
        }

        if trigger_diff.definition_change.is_none() && trigger_diff.enabled_change.is_none() {
            None
        } else {
            Some(trigger_diff)
        }
    }

    /// Compares sequences between source and target
    pub fn compare_sequences(
        &self,
        source: &[SequenceInfo],
        target: &[SequenceInfo],
    ) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &SequenceInfo> = source
            .iter()
            .map(|s| (self.normalize_name(&s.name), s))
            .collect();
        let target_map: HashMap<String, &SequenceInfo> = target
            .iter()
            .map(|s| (self.normalize_name(&s.name), s))
            .collect();

        for seq in source {
            let normalized = self.normalize_name(&seq.name);
            if !target_map.contains_key(&normalized) {
                diff.added_sequences.push(seq.clone());
            } else if let Some(target_seq) = target_map.get(&normalized)
                && let Some(seq_diff) = self.compare_sequence(seq, target_seq)
            {
                diff.modified_sequences.push(seq_diff);
            }
        }

        for seq in target {
            let normalized = self.normalize_name(&seq.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_sequences.push(seq.clone());
            }
        }

        diff
    }

    /// Compares two sequences
    fn compare_sequence(
        &self,
        source: &SequenceInfo,
        target: &SequenceInfo,
    ) -> Option<SequenceDiff> {
        let mut seq_diff = SequenceDiff::new(&source.name, source.schema.clone());

        if source.start_value != target.start_value {
            seq_diff.start_value_change = Some((source.start_value, target.start_value));
        }

        if source.increment_by != target.increment_by {
            seq_diff.increment_change = Some((source.increment_by, target.increment_by));
        }

        if source.min_value != target.min_value {
            seq_diff.min_value_change = Some((source.min_value, target.min_value));
        }

        if source.max_value != target.max_value {
            seq_diff.max_value_change = Some((source.max_value, target.max_value));
        }

        if seq_diff.start_value_change.is_none()
            && seq_diff.increment_change.is_none()
            && seq_diff.min_value_change.is_none()
            && seq_diff.max_value_change.is_none()
        {
            None
        } else {
            Some(seq_diff)
        }
    }

    /// Compares custom types between source and target
    pub fn compare_types(&self, source: &[TypeInfo], target: &[TypeInfo]) -> SchemaDiff {
        let mut diff = SchemaDiff::new();

        let source_map: HashMap<String, &TypeInfo> = source
            .iter()
            .map(|t| (self.normalize_name(&t.name), t))
            .collect();
        let target_map: HashMap<String, &TypeInfo> = target
            .iter()
            .map(|t| (self.normalize_name(&t.name), t))
            .collect();

        for type_info in source {
            let normalized = self.normalize_name(&type_info.name);
            if !target_map.contains_key(&normalized) {
                diff.added_types.push(type_info.clone());
            } else if let Some(target_type) = target_map.get(&normalized)
                && let Some(type_diff) = self.compare_type(type_info, target_type)
            {
                diff.modified_types.push(type_diff);
            }
        }

        for type_info in target {
            let normalized = self.normalize_name(&type_info.name);
            if !source_map.contains_key(&normalized) {
                diff.removed_types.push(type_info.clone());
            }
        }

        diff
    }

    /// Compares two custom types
    fn compare_type(&self, source: &TypeInfo, target: &TypeInfo) -> Option<TypeDiff> {
        let mut type_diff = TypeDiff::new(&source.name, source.schema.clone());

        if source.values != target.values {
            type_diff.values_change = Some((source.values.clone(), target.values.clone()));
        }

        if source.definition != target.definition {
            type_diff.definition_change =
                Some((source.definition.clone(), target.definition.clone()));
        }

        if type_diff.values_change.is_none() && type_diff.definition_change.is_none() {
            None
        } else {
            Some(type_diff)
        }
    }

    /// Merges multiple SchemaDiff instances into one
    pub fn merge_diffs(&self, diffs: Vec<SchemaDiff>) -> SchemaDiff {
        let mut merged = SchemaDiff::new();

        for diff in diffs {
            merged.added_tables.extend(diff.added_tables);
            merged.removed_tables.extend(diff.removed_tables);
            merged.modified_tables.extend(diff.modified_tables);
            merged.added_views.extend(diff.added_views);
            merged.removed_views.extend(diff.removed_views);
            merged.modified_views.extend(diff.modified_views);
            merged.added_functions.extend(diff.added_functions);
            merged.removed_functions.extend(diff.removed_functions);
            merged.modified_functions.extend(diff.modified_functions);
            merged.added_procedures.extend(diff.added_procedures);
            merged.removed_procedures.extend(diff.removed_procedures);
            merged.modified_procedures.extend(diff.modified_procedures);
            merged.added_triggers.extend(diff.added_triggers);
            merged.removed_triggers.extend(diff.removed_triggers);
            merged.modified_triggers.extend(diff.modified_triggers);
            merged.added_sequences.extend(diff.added_sequences);
            merged.removed_sequences.extend(diff.removed_sequences);
            merged.modified_sequences.extend(diff.modified_sequences);
            merged.added_types.extend(diff.added_types);
            merged.removed_types.extend(diff.removed_types);
            merged.modified_types.extend(diff.modified_types);
        }

        merged
    }
}
