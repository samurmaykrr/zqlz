use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_services::{SchemaService, TableColumnSummary, TableDetails};

use crate::{
    CompletionCache, ContextAnalyzer, DatabaseObject, FuzzyMatcher, SchemaCache, SchemaValidator,
    SqlDiagnostics, SqlDialect, SqlLsp, TableInfo, schema_fetch,
};

impl SqlLsp {
    #[allow(dead_code)]
    pub fn new(schema_service: Arc<SchemaService>) -> Self {
        Self {
            connection_id: None,
            connection: None,
            driver_type: "generic".to_string(),
            dialect: SqlDialect::Generic,
            schema_cache: SchemaCache::default(),
            active_database: None,
            active_schema: None,
            schema_service,
            context_analyzer: ContextAnalyzer::new().expect("context analyzer initializes"),
            schema_validator: SchemaValidator::new(),
            sql_diagnostics: SqlDiagnostics::new(),
            fuzzy_matcher: FuzzyMatcher::new(false),
            completion_cache: CompletionCache::default(),
            schema_loading: false,
            fetch_epoch: 0,
        }
    }

    pub fn with_connection(
        connection_id: Uuid,
        connection: Arc<dyn Connection>,
        driver_type: String,
        schema_service: Arc<SchemaService>,
    ) -> Self {
        let dialect = SqlDialect::from_driver(&driver_type);
        Self {
            connection_id: Some(connection_id),
            connection: Some(connection),
            driver_type,
            dialect,
            schema_cache: SchemaCache::default(),
            active_database: None,
            active_schema: None,
            schema_service,
            context_analyzer: ContextAnalyzer::new().expect("context analyzer initializes"),
            schema_validator: SchemaValidator::new(),
            sql_diagnostics: SqlDiagnostics::new(),
            fuzzy_matcher: FuzzyMatcher::new(false),
            completion_cache: CompletionCache::default(),
            schema_loading: false,
            fetch_epoch: 0,
        }
    }

    /// Update the connection for this LSP instance.
    pub fn set_connection(
        &mut self,
        connection_id: Option<Uuid>,
        connection: Option<Arc<dyn Connection>>,
        driver_type: Option<String>,
    ) {
        self.connection_id = connection_id;
        self.connection = connection;

        if let Some(driver) = driver_type {
            tracing::info!("Updating SQL LSP dialect to: {}", driver);
            self.driver_type = driver.clone();
            self.dialect = SqlDialect::from_driver(&driver);
            tracing::info!("SQL LSP now using dialect: {:?}", self.dialect);
        }

        self.schema_cache = SchemaCache::default();
        self.schema_loading = true;
        self.active_schema = None;
    }

    /// Set the active logical database for schema introspection.
    pub fn set_active_database(&mut self, database: Option<String>) {
        self.active_database = database;
    }

    /// Returns the active logical database selected for this LSP instance.
    pub fn active_database(&self) -> Option<String> {
        self.active_database.clone()
    }

    /// Set the active schema for schema-scoped introspection.
    pub fn set_active_schema(&mut self, schema: Option<String>) {
        self.active_schema = schema;
    }

    /// Returns the active schema selected for this LSP instance.
    pub fn active_schema(&self) -> Option<String> {
        self.active_schema.clone()
    }

    /// Fetches schema data from the database as a pure I/O operation, returning
    /// a populated `SchemaCache` without touching `self`.
    pub async fn fetch_schema_cache(
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        active_database: Option<String>,
        active_schema: Option<String>,
        schema_service: &SchemaService,
    ) -> Result<SchemaCache> {
        schema_fetch::fetch_schema_cache(
            connection,
            connection_id,
            active_database,
            active_schema,
            schema_service,
        )
        .await
    }

    /// Applies a pre-fetched schema cache, replacing the current one.
    pub fn apply_schema_cache(&mut self, cache: SchemaCache) {
        self.schema_cache = cache;
        self.schema_loading = false;
    }

    /// Applies the cache only if `epoch` matches the current fetch epoch.
    pub fn apply_schema_cache_if_current(&mut self, cache: SchemaCache, epoch: u64) {
        if self.fetch_epoch == epoch {
            self.apply_schema_cache(cache);
        }
    }

    /// Increments the fetch epoch and returns the new value.
    pub fn next_fetch_epoch(&mut self) -> u64 {
        self.fetch_epoch += 1;
        self.fetch_epoch
    }

    /// Seeds the schema cache with bare table names.
    pub fn pre_populate_tables(&mut self, table_names: &[String]) {
        for name in table_names {
            self.schema_cache
                .tables
                .entry(name.clone())
                .or_insert_with(|| TableInfo {
                    name: name.clone(),
                    schema: None,
                    comment: None,
                    row_count: None,
                    table_type: zqlz_core::TableType::Table,
                });
        }
    }

    /// Merges one table's columns and foreign keys into the cache without a full refetch.
    ///
    /// Called when something else in the app (the sidebar, the table viewer) has just
    /// loaded a table's details, so `alias.` completions for that table work immediately
    /// instead of waiting for the next whole-schema refresh. Unlike `pre_populate_tables`
    /// this is not gated on `schema_loading` — the data is authoritative either way.
    pub fn merge_table_columns(&mut self, table_name: &str, details: &TableDetails) {
        self.drop_cached_columns(std::slice::from_ref(&table_name.to_string()));
        self.insert_table_details(table_name, details);
    }

    /// Merges many tables' details in one pass.
    ///
    /// Equivalent to calling [`Self::merge_table_columns`] per table, but the stale-object
    /// sweep runs once for the whole batch instead of once per table — the per-table form
    /// is O(tables × objects), which is a visible stall on the UI thread for a schema with
    /// dozens of tables.
    pub fn merge_table_details_batch(&mut self, details: &HashMap<String, TableDetails>) {
        if details.is_empty() {
            return;
        }

        let table_names: Vec<String> = details.keys().cloned().collect();
        self.drop_cached_columns(&table_names);

        for (table_name, table_details) in details {
            self.insert_table_details(table_name, table_details);
        }
    }

    /// Bulk form for the connect-time warm-up, which fetches only what completions
    /// need rather than full table details.
    pub fn merge_table_columns_batch(&mut self, summaries: &HashMap<String, TableColumnSummary>) {
        if summaries.is_empty() {
            return;
        }

        let table_names: Vec<String> = summaries.keys().cloned().collect();
        self.drop_cached_columns(&table_names);

        for (table_name, summary) in summaries {
            self.insert_table_columns(table_name, &summary.columns, &summary.foreign_keys);
        }
    }

    /// Removes the cached column state for `table_names` so a merge replaces rather
    /// than appends — otherwise a re-merge after `ALTER TABLE` leaves dropped columns
    /// completing forever.
    fn drop_cached_columns(&mut self, table_names: &[String]) {
        self.pre_populate_tables(table_names);

        let canonical: Vec<String> = table_names
            .iter()
            .map(|table_name| self.canonical_table_name(table_name))
            .collect();
        let is_stale = |name: &str| {
            canonical
                .iter()
                .any(|table_name| table_name.eq_ignore_ascii_case(name))
        };

        self.schema_cache.objects.retain(|object| match object {
            DatabaseObject::Column(column) => !is_stale(&column.table_name),
            _ => true,
        });
        for reverse in self.schema_cache.reverse_foreign_keys.values_mut() {
            reverse.retain(|(source_table, _)| !is_stale(source_table));
        }
    }

    fn insert_table_details(&mut self, table_name: &str, details: &TableDetails) {
        self.insert_table_columns(table_name, &details.columns, &details.foreign_keys);
    }

    fn insert_table_columns(
        &mut self,
        table_name: &str,
        columns: &[zqlz_services::ColumnInfo],
        foreign_keys: &[zqlz_core::ForeignKeyInfo],
    ) {
        let canonical = self.canonical_table_name(table_name);
        let column_infos = schema_fetch::columns_from_parts(&canonical, columns, foreign_keys);

        for column in &column_infos {
            self.schema_cache
                .objects
                .push(DatabaseObject::Column(column.clone()));
        }
        self.schema_cache
            .columns_by_table
            .insert(canonical.clone(), column_infos);

        if !foreign_keys.is_empty() {
            self.schema_cache
                .foreign_keys_by_table
                .insert(canonical.clone(), foreign_keys.to_vec());

            for foreign_key in foreign_keys {
                self.schema_cache
                    .reverse_foreign_keys
                    .entry(foreign_key.referenced_table.clone())
                    .or_default()
                    .push((canonical.clone(), foreign_key.clone()));
            }
        }
    }

    /// Returns the cache's existing spelling of `table_name`, so merging `USERS`
    /// updates the `users` entry instead of creating a second one.
    fn canonical_table_name(&self, table_name: &str) -> String {
        self.schema_cache
            .columns_by_table
            .keys()
            .chain(self.schema_cache.tables.keys())
            .find(|name| name.eq_ignore_ascii_case(table_name))
            .cloned()
            .unwrap_or_else(|| table_name.to_string())
    }

    /// Returns the active connection ID, if any.
    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    /// Returns a clone of the schema service handle.
    pub fn schema_service(&self) -> Arc<SchemaService> {
        self.schema_service.clone()
    }

    /// Returns a clone of the active connection, if any.
    pub fn connection(&self) -> Option<Arc<dyn Connection>> {
        self.connection.clone()
    }

    /// Refresh schema cache from the database using SchemaService.
    pub async fn refresh_schema(&mut self) -> Result<()> {
        tracing::info!("refresh_schema called - using SchemaService");

        let Some(connection) = self.connection.clone() else {
            tracing::warn!("refresh_schema: No connection available");
            return Ok(());
        };

        let Some(connection_id) = self.connection_id else {
            tracing::warn!("refresh_schema: No connection ID available");
            return Ok(());
        };

        let cache = Self::fetch_schema_cache(
            connection,
            connection_id,
            self.active_database.clone(),
            self.active_schema.clone(),
            &self.schema_service,
        )
        .await?;
        self.apply_schema_cache(cache);
        Ok(())
    }
}
