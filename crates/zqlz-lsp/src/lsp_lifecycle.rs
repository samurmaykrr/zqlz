use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_services::SchemaService;

use crate::{
    CompletionCache, ContextAnalyzer, FuzzyMatcher, SchemaCache, SchemaValidator, SqlDiagnostics,
    SqlDialect, SqlLsp, TableInfo, schema_fetch,
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
