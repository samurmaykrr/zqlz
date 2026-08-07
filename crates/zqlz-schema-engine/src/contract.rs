//! Driver-agnostic schema-introspection contract.
//!
//! [`run_schema_contract`] exercises a [`SchemaIntrospection`] implementation
//! through its public interface and asserts the invariants every driver must
//! satisfy. The same suite runs against a real engine (SQLite in-memory) and
//! against [`crate::RecordedCatalog`] fixtures, so the engine's normalization
//! and composition are covered on every `cargo test`.

use std::collections::HashSet;

use zqlz_core::{DatabaseObject, ObjectType, SchemaIntrospection};

/// What a given database is expected to contain, so the contract can target
/// real objects without hard-coding driver specifics.
#[derive(Debug, Clone, Default)]
pub struct ContractExpectations {
    /// Schema to introspect (None uses the driver default).
    pub schema: Option<String>,
    /// Tables that must be present and fully introspectable.
    pub tables: Vec<String>,
    /// Whether `generate_ddl` is expected to succeed for the tables.
    pub expect_table_ddl: bool,
}

/// Run the contract. Returns `Err(description)` on the first violation.
pub async fn run_schema_contract(
    introspection: &dyn SchemaIntrospection,
    expectations: &ContractExpectations,
) -> Result<(), String> {
    let schema = expectations.schema.as_deref();

    check_table_listing(introspection, expectations, schema).await?;
    for table in &expectations.tables {
        check_table_details(introspection, schema, table).await?;
        check_composition(introspection, schema, table).await?;
        if expectations.expect_table_ddl {
            check_table_ddl(introspection, schema, table).await?;
        }
    }
    check_bulk_matches_per_table(introspection, expectations, schema).await?;
    check_panel_consistency(introspection, schema).await?;

    Ok(())
}

/// The schema-wide fetches must agree with the per-table ones.
///
/// Completions warm a whole schema through `list_all_columns` / `list_all_foreign_keys`
/// to avoid one query per table, so any divergence from the per-table path shows up as
/// wrong or missing completions rather than as a failure. Drivers without a bulk form
/// return `None` and are skipped.
async fn check_bulk_matches_per_table(
    introspection: &dyn SchemaIntrospection,
    expectations: &ContractExpectations,
    schema: Option<&str>,
) -> Result<(), String> {
    let bulk_columns = introspection
        .list_all_columns(schema)
        .await
        .map_err(|error| format!("list_all_columns failed: {error}"))?;

    if let Some(bulk_columns) = bulk_columns {
        for table in &expectations.tables {
            let expected = introspection
                .get_columns(schema, table)
                .await
                .map_err(|error| format!("get_columns('{table}') failed: {error}"))?;
            let actual = bulk_columns.get(table).ok_or_else(|| {
                format!("list_all_columns omitted '{table}'; got {:?}", {
                    let mut names: Vec<&str> =
                        bulk_columns.keys().map(String::as_str).collect();
                    names.sort_unstable();
                    names
                })
            })?;

            let expected_names: Vec<&str> =
                expected.iter().map(|column| column.name.as_str()).collect();
            let actual_names: Vec<&str> =
                actual.iter().map(|column| column.name.as_str()).collect();
            if expected_names != actual_names {
                return Err(format!(
                    "list_all_columns('{table}') returned {actual_names:?}, per-table returned {expected_names:?}"
                ));
            }

            for (bulk, single) in actual.iter().zip(expected.iter()) {
                if bulk.data_type != single.data_type || bulk.nullable != single.nullable {
                    return Err(format!(
                        "list_all_columns('{table}'.{}) disagrees with get_columns: {:?}/{} vs {:?}/{}",
                        bulk.name, bulk.data_type, bulk.nullable, single.data_type, single.nullable
                    ));
                }
            }
        }
    }

    let bulk_foreign_keys = introspection
        .list_all_foreign_keys(schema)
        .await
        .map_err(|error| format!("list_all_foreign_keys failed: {error}"))?;

    if let Some(bulk_foreign_keys) = bulk_foreign_keys {
        for table in &expectations.tables {
            let expected = introspection
                .get_foreign_keys(schema, table)
                .await
                .map_err(|error| format!("get_foreign_keys('{table}') failed: {error}"))?;
            let actual = bulk_foreign_keys.get(table).map_or(&[][..], Vec::as_slice);

            let mut expected_refs: Vec<(&str, &str)> = expected
                .iter()
                .map(|key| {
                    (
                        key.referenced_table.as_str(),
                        key.columns.first().map_or("", String::as_str),
                    )
                })
                .collect();
            let mut actual_refs: Vec<(&str, &str)> = actual
                .iter()
                .map(|key| {
                    (
                        key.referenced_table.as_str(),
                        key.columns.first().map_or("", String::as_str),
                    )
                })
                .collect();
            expected_refs.sort_unstable();
            actual_refs.sort_unstable();

            if expected_refs != actual_refs {
                return Err(format!(
                    "list_all_foreign_keys('{table}') returned {actual_refs:?}, per-table returned {expected_refs:?}"
                ));
            }
        }
    }

    Ok(())
}

async fn check_table_listing(
    introspection: &dyn SchemaIntrospection,
    expectations: &ContractExpectations,
    schema: Option<&str>,
) -> Result<(), String> {
    let tables = introspection
        .list_tables(schema)
        .await
        .map_err(|error| format!("list_tables failed: {error}"))?;
    let names: HashSet<&str> = tables.iter().map(|table| table.name.as_str()).collect();
    for expected in &expectations.tables {
        if !names.contains(expected.as_str()) {
            return Err(format!("expected table '{expected}' missing from list_tables"));
        }
    }
    Ok(())
}

async fn check_table_details(
    introspection: &dyn SchemaIntrospection,
    schema: Option<&str>,
    table: &str,
) -> Result<(), String> {
    let details = introspection
        .get_table(schema, table)
        .await
        .map_err(|error| format!("get_table('{table}') failed: {error}"))?;

    if details.columns.is_empty() {
        return Err(format!("table '{table}' has no columns"));
    }

    let mut seen_ordinals = HashSet::new();
    for column in &details.columns {
        if column.name.is_empty() {
            return Err(format!("table '{table}' has a column with an empty name"));
        }
        if !seen_ordinals.insert(column.ordinal) {
            return Err(format!(
                "table '{table}' has duplicate column ordinal {}",
                column.ordinal
            ));
        }
    }

    if let Some(primary_key) = &details.primary_key {
        for pk_column in &primary_key.columns {
            let column = details
                .columns
                .iter()
                .find(|column| &column.name == pk_column)
                .ok_or_else(|| {
                    format!("table '{table}' primary key references unknown column '{pk_column}'")
                })?;
            if !column.is_primary_key {
                return Err(format!(
                    "table '{table}' column '{pk_column}' is in the primary key but not flagged is_primary_key"
                ));
            }
        }
    }

    for foreign_key in &details.foreign_keys {
        if foreign_key.columns.is_empty() {
            return Err(format!("table '{table}' has a foreign key with no columns"));
        }
        if foreign_key.columns.len() != foreign_key.referenced_columns.len() {
            return Err(format!(
                "table '{table}' foreign key '{}' has mismatched column arity ({} local vs {} referenced)",
                foreign_key.name,
                foreign_key.columns.len(),
                foreign_key.referenced_columns.len()
            ));
        }
        if foreign_key.referenced_table.is_empty() {
            return Err(format!(
                "table '{table}' foreign key '{}' references an empty table name",
                foreign_key.name
            ));
        }
    }

    Ok(())
}

async fn check_composition(
    introspection: &dyn SchemaIntrospection,
    schema: Option<&str>,
    table: &str,
) -> Result<(), String> {
    let details = introspection
        .get_table(schema, table)
        .await
        .map_err(|error| format!("get_table('{table}') failed: {error}"))?;

    let columns = introspection
        .get_columns(schema, table)
        .await
        .map_err(|error| format!("get_columns('{table}') failed: {error}"))?;
    let detail_column_names: Vec<&str> = details.columns.iter().map(|c| c.name.as_str()).collect();
    let standalone_column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    if detail_column_names != standalone_column_names {
        return Err(format!(
            "table '{table}': get_columns disagrees with get_table columns ({standalone_column_names:?} vs {detail_column_names:?})"
        ));
    }

    let indexes = introspection
        .get_indexes(schema, table)
        .await
        .map_err(|error| format!("get_indexes('{table}') failed: {error}"))?;
    let detail_index_names: HashSet<&str> = details.indexes.iter().map(|i| i.name.as_str()).collect();
    let standalone_index_names: HashSet<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
    if detail_index_names != standalone_index_names {
        return Err(format!(
            "table '{table}': get_indexes disagrees with get_table indexes"
        ));
    }

    let primary_key = introspection
        .get_primary_key(schema, table)
        .await
        .map_err(|error| format!("get_primary_key('{table}') failed: {error}"))?;
    let detail_pk = details.primary_key.as_ref().map(|pk| &pk.columns);
    let standalone_pk = primary_key.as_ref().map(|pk| &pk.columns);
    if detail_pk != standalone_pk {
        return Err(format!(
            "table '{table}': get_primary_key disagrees with get_table primary key ({standalone_pk:?} vs {detail_pk:?})"
        ));
    }

    Ok(())
}

async fn check_table_ddl(
    introspection: &dyn SchemaIntrospection,
    schema: Option<&str>,
    table: &str,
) -> Result<(), String> {
    let object = DatabaseObject {
        object_type: ObjectType::Table,
        schema: schema.map(ToString::to_string),
        name: table.to_string(),
        signature: None,
    };
    let ddl = introspection
        .generate_ddl(&object)
        .await
        .map_err(|error| format!("generate_ddl('{table}') failed: {error}"))?;
    if ddl.trim().is_empty() {
        return Err(format!("generate_ddl('{table}') returned empty DDL"));
    }
    Ok(())
}

async fn check_panel_consistency(
    introspection: &dyn SchemaIntrospection,
    schema: Option<&str>,
) -> Result<(), String> {
    let manifest = introspection
        .list_objects_panel_manifest(schema)
        .await
        .map_err(|error| format!("list_objects_panel_manifest failed: {error}"))?;
    manifest
        .validate()
        .map_err(|error| format!("objects panel manifest invalid: {error}"))?;

    let kind_ids: HashSet<&str> = manifest.object_kinds.iter().map(|kind| kind.id.as_str()).collect();
    let data = introspection
        .list_tables_extended(schema)
        .await
        .map_err(|error| format!("list_tables_extended failed: {error}"))?;
    for row in &data.rows {
        if !kind_ids.contains(row.object_kind_id()) {
            return Err(format!(
                "panel row '{}' has kind '{}' not present in the manifest",
                row.name,
                row.object_kind_id()
            ));
        }
    }

    Ok(())
}
