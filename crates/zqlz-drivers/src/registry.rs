//! Driver registry for managing available database drivers

use std::collections::HashMap;
use std::sync::Arc;
use zqlz_core::{DatabaseDriver, DialectBundle, DialectInfo};

/// Registry of available database drivers
pub struct DriverRegistry {
    drivers: HashMap<String, Arc<dyn DatabaseDriver>>,
}

impl DriverRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            drivers: HashMap::new(),
        }
    }

    /// Create a registry with all built-in drivers registered
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();

        // SQL Databases
        #[cfg(feature = "sqlite")]
        registry.register(Arc::new(crate::sqlite::SqliteDriver::new()));
        #[cfg(feature = "postgres")]
        registry.register(Arc::new(crate::postgres::PostgresDriver::new()));
        #[cfg(feature = "mysql")]
        registry.register(Arc::new(crate::mysql::MySqlDriver::new()));
        #[cfg(feature = "mssql")]
        registry.register(Arc::new(crate::mssql::MssqlDriver::new()));
        #[cfg(feature = "duckdb")]
        registry.register(Arc::new(crate::duckdb::DuckDbDriver::new()));

        // NoSQL Databases
        #[cfg(feature = "redis")]
        registry.register(Arc::new(crate::redis::RedisDriver::new()));
        #[cfg(feature = "mongodb")]
        registry.register(Arc::new(crate::mongodb::MongoDbDriver::new()));
        #[cfg(feature = "clickhouse")]
        registry.register(Arc::new(crate::clickhouse::ClickHouseDriver::new()));

        registry
    }

    /// Register a new driver
    pub fn register(&mut self, driver: Arc<dyn DatabaseDriver>) {
        let name = driver.name().to_string();
        tracing::info!(driver = %name, "registering database driver");
        self.drivers.insert(name, driver);
    }

    /// Get a driver by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn DatabaseDriver>> {
        let driver = self.drivers.get(name).cloned();
        if driver.is_none() {
            tracing::warn!(driver = %name, "driver not found in registry");
        }
        driver
    }

    /// List all registered driver names
    pub fn list(&self) -> Vec<&str> {
        self.drivers.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a driver is registered
    pub fn has(&self, name: &str) -> bool {
        self.drivers.contains_key(name)
    }

    /// Get dialect info for a driver by name
    pub fn dialect_info(&self, name: &str) -> Option<DialectInfo> {
        self.drivers.get(name).map(|driver| driver.dialect_info())
    }

    /// Get dialect bundle for a driver by name
    pub fn dialect_bundle(&self, name: &str) -> Option<&'static DialectBundle> {
        self.drivers
            .get(name)
            .and_then(|driver| driver.dialect_bundle())
    }
}

/// Get dialect info for a driver by name without needing a registry instance.
/// This is a convenience function that creates a temporary driver instance.
/// For repeated lookups, prefer using a cached `DriverRegistry` instance.
pub fn get_dialect_info(driver_name: &str) -> DialectInfo {
    // For drivers that support the new bundle system, use the bundle
    // Otherwise fall back to the legacy dialect functions
    if let Some(bundle) = get_dialect_bundle(driver_name) {
        return bundle.into();
    }

    // Legacy fallback for drivers not yet migrated to bundle system
    match driver_name.to_lowercase().as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => crate::sqlite::sqlite_dialect(),
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => crate::postgres::postgres_dialect(),
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => crate::mysql::mysql_dialect(),
        #[cfg(feature = "mssql")]
        "mssql" | "sqlserver" => crate::mssql::mssql_dialect(),
        #[cfg(feature = "duckdb")]
        "duckdb" => crate::duckdb::duckdb_dialect(),
        #[cfg(feature = "mongodb")]
        "mongodb" => crate::mongodb::mongodb_dialect(),
        #[cfg(feature = "clickhouse")]
        "clickhouse" => crate::clickhouse::clickhouse_dialect(),
        _ => DialectInfo::default(),
    }
}

/// Get the dialect bundle for a driver by name.
///
/// Returns the full DialectBundle containing the config, completions, and highlights.
/// This is available for drivers that have migrated to the declarative dialect system.
///
/// Returns None for drivers that still use the legacy hardcoded dialect functions.
pub fn get_dialect_bundle(driver_name: &str) -> Option<&'static DialectBundle> {
    match driver_name.to_lowercase().as_str() {
        #[cfg(feature = "redis")]
        "redis" => {
            // Redis uses the new bundle system
            let driver = crate::redis::RedisDriver::new();
            driver.dialect_bundle()
        }
        // Other drivers will be migrated over time
        _ => None,
    }
}

impl Default for DriverRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
