//! Schema introspection traits and types

mod catalog;
mod core_types;
mod driver_category;
mod key_value;
mod object_forms;
mod objects_panel;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{Result, ZqlzError};

pub use catalog::*;
pub use core_types::*;
pub use driver_category::*;
pub use key_value::*;
pub use object_forms::*;
pub use objects_panel::*;

/// Schema introspection interface
#[async_trait]
pub trait SchemaIntrospection: Send + Sync {
    /// List all databases
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>>;

    /// List all schemas in the current database
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>>;

    /// List all tables in a schema
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;

    /// List all views in a schema
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>>;

    /// List materialized views in a schema.
    /// Default returns an empty list since not all databases support them.
    async fn list_materialized_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        Ok(Vec::new())
    }

    /// Get detailed table information
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails>;

    /// Get columns for a table
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>>;

    /// Get columns for every table in a schema, keyed by bare table name.
    ///
    /// Backends whose catalog exposes columns as a queryable relation answer this in
    /// one round-trip, which is what makes warming a whole schema for completions
    /// affordable. `Ok(None)` means the backend has no bulk form and the caller
    /// should fall back to per-table [`Self::get_columns`]; `Ok(Some(empty))` means
    /// the schema genuinely has no columns.
    async fn list_all_columns(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ColumnInfo>>>> {
        Ok(None)
    }

    /// Get foreign keys for every table in a schema, keyed by bare table name.
    ///
    /// Same contract as [`Self::list_all_columns`].
    async fn list_all_foreign_keys(
        &self,
        _schema: Option<&str>,
    ) -> Result<Option<HashMap<String, Vec<ForeignKeyInfo>>>> {
        Ok(None)
    }

    /// Get indexes for a table
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>>;

    /// Get foreign keys for a table
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>>;

    /// Get primary key for a table
    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>>;

    /// Get constraints for a table
    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>>;

    /// List all functions in a schema
    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>>;

    /// List all procedures in a schema
    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>>;

    /// List all triggers in a schema (optionally filtered by table)
    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>>;

    /// List all sequences in a schema
    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>>;

    /// List all custom types/enums in a schema
    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>>;

    /// Generate DDL for a database object
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String>;

    /// Get object dependencies
    async fn get_dependencies(&self, object: &DatabaseObject) -> Result<Vec<Dependency>>;

    /// Extended table listing for the objects panel.
    ///
    /// Returns driver-specific column definitions and row data so each database
    /// engine can surface its own metadata (e.g. PostgreSQL shows OID, Owner, ACL
    /// while SQLite shows simple counts). Drivers that don't override this get a
    /// reasonable default built from `list_tables()`.
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let tables = self.list_tables(schema).await?;
        Ok(ObjectsPanelData::from_table_infos(tables))
    }

    /// Kind-scoped objects panel data.
    ///
    /// Drivers can override this to avoid loading every object kind when the UI
    /// only needs the active Objects Panel kind.
    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        Ok(self
            .list_tables_extended(schema)
            .await?
            .for_kind_and_scope(kind_id, None))
    }

    /// Declarative manifest for Objects Panel behavior.
    ///
    /// Drivers can override this to describe which object kinds exist, their
    /// available actions, and UI metadata. The default derives a conservative
    /// manifest from `list_tables_extended` so existing drivers remain compatible.
    async fn list_objects_panel_manifest(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        let data = self.list_tables_extended(schema).await?;
        Ok(ObjectsPanelManifest::from_data(&data))
    }

    /// Driver-defined form for creating, editing, or dropping an object kind.
    async fn object_form_spec(
        &self,
        _request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(None)
    }

    /// Generate executable DDL from a driver-defined object form payload.
    async fn generate_object_form_ddl(
        &self,
        _request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        Err(ZqlzError::NotSupported(
            "Object form DDL generation is not supported by this driver".to_string(),
        ))
    }
}
