//! How a grid row is addressed when it is updated or deleted.
//!
//! A declared primary key is the best case, but plenty of real tables have
//! none. Those rows are still addressable: a unique key works just as well, and
//! failing that a row can be matched on all of its column values as long as the
//! driver narrows the statement to a single row (see
//! [`SingleRowDmlScope`](crate::SingleRowDmlScope)), so duplicate rows are not
//! written together.

use serde::{Deserialize, Serialize};

use crate::{ConstraintInfo, ConstraintType, IndexInfo, TableType};

/// The part of a column that decides whether it can identify a row.
///
/// Taking this rather than a concrete column type lets the service layer pass
/// its own UI-facing column model without converting it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowIdentityColumn<'a> {
    pub name: &'a str,
    pub nullable: bool,
}

/// Reason shown when a relation exposes no way to address one of its rows.
pub const VIEW_ROW_IDENTITY_REASON: &str =
    "Editing a view requires a primary key on the view itself";
pub const NO_COLUMNS_ROW_IDENTITY_REASON: &str = "Editing requires a relation with columns";

/// The columns a row is matched on for updates and deletes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowIdentity {
    /// The declared primary key.
    PrimaryKey(Vec<String>),
    /// A unique constraint or unique index that covers every row.
    UniqueKey { name: String, columns: Vec<String> },
    /// No key at all: rows are matched on every column value, and the driver
    /// limits the statement to a single row. This needs no metadata, so it is
    /// also what the viewer assumes before a schema load finishes.
    #[default]
    AllColumns,
    /// Rows cannot be addressed; the string explains why to the user.
    Unavailable(String),
}

impl RowIdentity {
    /// Identity to assume before a relation's schema has loaded.
    ///
    /// Matching on all columns needs no metadata, so ordinary tables stay
    /// editable while introspection is still in flight. Views only become
    /// editable once a primary key is actually found on them.
    pub fn assumed_for(table_type: TableType) -> Self {
        if Self::needs_declared_key(table_type) {
            Self::Unavailable(VIEW_ROW_IDENTITY_REASON.to_string())
        } else {
            Self::AllColumns
        }
    }

    /// Whether existing rows can be updated or deleted.
    pub fn can_mutate_rows(&self) -> bool {
        !matches!(self, Self::Unavailable(_))
    }

    /// Why row mutations are unavailable, if they are.
    pub fn unavailable_reason(&self) -> Option<&str> {
        match self {
            Self::Unavailable(reason) => Some(reason.as_str()),
            _ => None,
        }
    }

    /// The key columns rows are matched on, empty when matching on all columns.
    pub fn key_columns(&self) -> &[String] {
        match self {
            Self::PrimaryKey(columns) => columns,
            Self::UniqueKey { columns, .. } => columns,
            Self::AllColumns | Self::Unavailable(_) => &[],
        }
    }

    /// Relations whose rows can only be reached through a declared key, because
    /// there is no physical row behind them for a keyless match to hit.
    fn needs_declared_key(table_type: TableType) -> bool {
        matches!(table_type, TableType::View | TableType::MaterializedView)
    }
}

/// Pick the columns that identify a row in this relation.
///
/// Preference order is primary key, then the narrowest unique key that applies
/// to every row, then all columns.
pub fn resolve_row_identity(
    table_type: TableType,
    columns: &[RowIdentityColumn<'_>],
    primary_key_columns: &[String],
    constraints: &[ConstraintInfo],
    indexes: &[IndexInfo],
) -> RowIdentity {
    if !primary_key_columns.is_empty() {
        return RowIdentity::PrimaryKey(primary_key_columns.to_vec());
    }

    if let Some(unique_key) = narrowest_unique_key(columns, constraints, indexes) {
        return unique_key;
    }

    if RowIdentity::needs_declared_key(table_type) {
        return RowIdentity::Unavailable(VIEW_ROW_IDENTITY_REASON.to_string());
    }

    if columns.is_empty() {
        return RowIdentity::Unavailable(NO_COLUMNS_ROW_IDENTITY_REASON.to_string());
    }

    RowIdentity::AllColumns
}

/// Unique keys that could identify a row, narrowest first.
///
/// Partial indexes are left out because they skip rows, so a row can match no
/// index entry at all. Callers still have to establish that the key is never
/// NULL for the row at hand — NULL compares unequal to itself in every SQL
/// dialect, so a NULL key matches nothing.
pub fn unique_key_candidates(
    constraints: &[ConstraintInfo],
    indexes: &[IndexInfo],
) -> Vec<(String, Vec<String>)> {
    let mut candidates: Vec<(String, Vec<String>)> = constraints
        .iter()
        .filter(|constraint| constraint.constraint_type == ConstraintType::Unique)
        .map(|constraint| (constraint.name.clone(), constraint.columns.clone()))
        .chain(
            indexes
                .iter()
                .filter(|index| index.is_unique && index.where_clause.is_none())
                .map(|index| (index.name.clone(), index.columns.clone())),
        )
        .filter(|(_, key_columns)| !key_columns.is_empty())
        .collect();

    candidates.sort_by(|(left_name, left_columns), (right_name, right_columns)| {
        left_columns
            .len()
            .cmp(&right_columns.len())
            .then_with(|| left_name.cmp(right_name))
    });
    candidates.dedup_by(|left, right| left.1 == right.1);
    candidates
}

/// Unique keys that identify every row of the relation, narrowest first.
pub fn usable_unique_keys(
    columns: &[RowIdentityColumn<'_>],
    constraints: &[ConstraintInfo],
    indexes: &[IndexInfo],
) -> Vec<(String, Vec<String>)> {
    unique_key_candidates(constraints, indexes)
        .into_iter()
        .filter(|(_, key_columns)| {
            key_columns
                .iter()
                .all(|key_column| is_non_nullable_column(columns, key_column))
        })
        .collect()
}

fn narrowest_unique_key(
    columns: &[RowIdentityColumn<'_>],
    constraints: &[ConstraintInfo],
    indexes: &[IndexInfo],
) -> Option<RowIdentity> {
    usable_unique_keys(columns, constraints, indexes)
        .into_iter()
        .next()
        .map(|(name, columns)| RowIdentity::UniqueKey { name, columns })
}

/// A key column missing from the column list is treated as nullable: without
/// metadata we cannot promise it identifies a row.
fn is_non_nullable_column(columns: &[RowIdentityColumn<'_>], column_name: &str) -> bool {
    columns
        .iter()
        .find(|column| column.name == column_name)
        .is_some_and(|column| !column.nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, nullable: bool) -> RowIdentityColumn<'_> {
        RowIdentityColumn { name, nullable }
    }

    fn unique_index(name: &str, columns: &[&str]) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            columns: columns.iter().map(|column| column.to_string()).collect(),
            is_unique: true,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            where_clause: None,
            ..Default::default()
        }
    }

    #[test]
    fn primary_key_wins_over_every_other_candidate() {
        let identity = resolve_row_identity(
            TableType::Table,
            &[column("id", false), column("email", false)],
            &["id".to_string()],
            &[],
            &[unique_index("uq_email", &["email"])],
        );

        assert_eq!(identity, RowIdentity::PrimaryKey(vec!["id".to_string()]));
    }

    #[test]
    fn narrowest_not_null_unique_index_is_used_without_a_primary_key() {
        let identity = resolve_row_identity(
            TableType::Table,
            &[
                column("tenant", false),
                column("email", false),
                column("slug", false),
            ],
            &[],
            &[],
            &[
                unique_index("uq_tenant_email", &["tenant", "email"]),
                unique_index("uq_slug", &["slug"]),
            ],
        );

        assert_eq!(
            identity,
            RowIdentity::UniqueKey {
                name: "uq_slug".to_string(),
                columns: vec!["slug".to_string()],
            }
        );
    }

    #[test]
    fn nullable_and_partial_unique_indexes_are_not_row_identities() {
        let partial = IndexInfo {
            where_clause: Some("deleted_at IS NULL".to_string()),
            ..unique_index("uq_partial", &["slug"])
        };

        let identity = resolve_row_identity(
            TableType::Table,
            &[column("email", true), column("slug", false)],
            &[],
            &[],
            &[unique_index("uq_email", &["email"]), partial],
        );

        assert_eq!(identity, RowIdentity::AllColumns);
    }

    #[test]
    fn keyless_table_matches_on_all_columns() {
        let identity = resolve_row_identity(
            TableType::Table,
            &[column("workflow_id", true), column("workflow_jinja", true)],
            &[],
            &[],
            &[],
        );

        assert_eq!(identity, RowIdentity::AllColumns);
        assert!(identity.can_mutate_rows());
    }

    #[test]
    fn keyless_view_cannot_be_edited() {
        let identity = resolve_row_identity(
            TableType::View,
            &[column("workflow_id", true)],
            &[],
            &[],
            &[],
        );

        assert!(!identity.can_mutate_rows());
        assert_eq!(
            identity.unavailable_reason(),
            Some(VIEW_ROW_IDENTITY_REASON)
        );
    }

    #[test]
    fn view_with_a_primary_key_can_be_edited() {
        let identity = resolve_row_identity(
            TableType::View,
            &[column("id", false)],
            &["id".to_string()],
            &[],
            &[],
        );

        assert_eq!(identity, RowIdentity::PrimaryKey(vec!["id".to_string()]));
    }

    #[test]
    fn assumed_identity_keeps_tables_editable_while_schema_loads() {
        assert_eq!(
            RowIdentity::assumed_for(TableType::Table),
            RowIdentity::AllColumns
        );
        assert!(!RowIdentity::assumed_for(TableType::View).can_mutate_rows());
    }
}
