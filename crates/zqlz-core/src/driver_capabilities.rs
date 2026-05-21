use crate::{Connection, DriverCategory};

/// Resolve a broad driver category from a driver identifier.
pub fn driver_category_from_driver_name(driver_name: &str) -> DriverCategory {
    match driver_name.to_ascii_lowercase().as_str() {
        "redis" | "memcached" | "valkey" | "keydb" | "dragonfly" | "etcd" => {
            DriverCategory::KeyValue
        }
        "mongodb" | "couchdb" | "couchbase" | "dynamodb" | "cassandra" | "scylladb" => {
            DriverCategory::Document
        }
        "influxdb" | "timescaledb" | "questdb" => DriverCategory::TimeSeries,
        "neo4j" | "arangodb" | "janusgraph" => DriverCategory::Graph,
        "elasticsearch" | "opensearch" | "meilisearch" => DriverCategory::Search,
        _ => DriverCategory::Relational,
    }
}

/// Default database label for result metadata when a driver does not expose
/// an explicit database/catalog parameter.
pub fn default_database_label_for_driver(driver_name: &str) -> Option<&'static str> {
    match driver_name.to_ascii_lowercase().as_str() {
        "sqlite" | "sqlite3" => Some("main"),
        _ => None,
    }
}

/// Returns true when a file extension can be opened as a database connection.
pub fn extension_supports_database_file_open(extension: &str) -> bool {
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "db" | "sqlite" | "sqlite3" | "duckdb"
    )
}

/// Returns true when a dialect id represents PostgreSQL.
pub fn dialect_is_postgres(dialect_id: Option<&str>) -> bool {
    dialect_id.is_some_and(|dialect| {
        dialect.eq_ignore_ascii_case("postgres") || dialect.eq_ignore_ascii_case("postgresql")
    })
}

/// Returns true when a dialect id represents MySQL or a compatible dialect.
pub fn dialect_is_mysql_compatible(dialect_id: Option<&str>) -> bool {
    dialect_id.is_some_and(|dialect| {
        dialect.eq_ignore_ascii_case("mysql") || dialect.eq_ignore_ascii_case("mariadb")
    })
}

/// Returns true when `CREATE OR REPLACE VIEW` is supported for the dialect.
pub fn dialect_supports_create_or_replace_view(dialect_id: Option<&str>) -> bool {
    dialect_id.is_some_and(|dialect| {
        dialect.eq_ignore_ascii_case("postgres")
            || dialect.eq_ignore_ascii_case("postgresql")
            || dialect.eq_ignore_ascii_case("mysql")
            || dialect.eq_ignore_ascii_case("mariadb")
    })
}

/// Returns true when the connection dialect represents PostgreSQL.
pub fn connection_is_postgres(connection: &dyn Connection) -> bool {
    dialect_is_postgres(connection.dialect_id())
        || dialect_is_postgres(Some(connection.driver_name()))
}

/// Returns true when the connection dialect represents MySQL or a compatible dialect.
pub fn connection_is_mysql_compatible(connection: &dyn Connection) -> bool {
    dialect_is_mysql_compatible(connection.dialect_id())
        || dialect_is_mysql_compatible(Some(connection.driver_name()))
}

/// Returns true when `CREATE OR REPLACE VIEW` is supported for this connection.
pub fn connection_supports_create_or_replace_view(connection: &dyn Connection) -> bool {
    dialect_supports_create_or_replace_view(connection.dialect_id())
}

/// Resolve the schema qualifier for SQL operations from a dialect identifier
/// and optional database target.
///
/// For drivers that model database/catalog as the SQL qualification scope
/// (MySQL, MariaDB, ClickHouse, SQL Server), this returns the provided
/// `database_name`. For dialects with separate schema semantics (PostgreSQL) or
/// no schema support (SQLite/Redis), this returns `None`.
pub fn resolve_schema_qualifier_for_dialect(
    dialect_id: Option<&str>,
    database_name: Option<&str>,
) -> Option<String> {
    match dialect_id {
        Some("mysql") | Some("mariadb") | Some("clickhouse") | Some("mssql")
        | Some("sqlserver") => database_name.map(ToOwned::to_owned),
        _ => None,
    }
}

/// Resolve the schema qualifier for SQL operations from a connection and
/// optional database target.
pub fn resolve_schema_qualifier_for_connection(
    connection: &dyn Connection,
    database_name: Option<&str>,
) -> Option<String> {
    resolve_schema_qualifier_for_dialect(connection.dialect_id(), database_name)
}

/// Resolve the metadata/introspection namespace used for schema-scoped objects.
///
/// Some dialects expose sequence and object metadata under the selected
/// database/catalog, while others expose it under the selected schema. Keep that
/// mapping in core driver capabilities so editor/LSP layers do not branch on
/// concrete driver names.
pub fn resolve_metadata_scope_for_dialect(
    dialect_id: Option<&str>,
    active_database: Option<&str>,
    active_schema: Option<&str>,
    default_database: Option<&str>,
    default_schema: Option<&str>,
) -> Option<String> {
    if matches!(
        dialect_id,
        Some("mysql") | Some("mariadb") | Some("clickhouse") | Some("mssql") | Some("sqlserver")
    ) {
        active_database.or(default_database).map(ToOwned::to_owned)
    } else {
        active_schema.or(default_schema).map(ToOwned::to_owned)
    }
}

/// Resolve the metadata/introspection namespace from a concrete connection.
pub fn resolve_metadata_scope_for_connection(
    connection: &dyn Connection,
    active_database: Option<&str>,
    active_schema: Option<&str>,
    default_database: Option<&str>,
    default_schema: Option<&str>,
) -> Option<String> {
    resolve_metadata_scope_for_dialect(
        connection.dialect_id(),
        active_database,
        active_schema,
        default_database,
        default_schema,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        default_database_label_for_driver, dialect_is_mysql_compatible, dialect_is_postgres,
        dialect_supports_create_or_replace_view, driver_category_from_driver_name,
        extension_supports_database_file_open, resolve_metadata_scope_for_dialect,
        resolve_schema_qualifier_for_dialect,
    };
    use crate::DriverCategory;

    #[test]
    fn driver_category_mapping_covers_known_non_relational_families() {
        assert_eq!(
            driver_category_from_driver_name("redis"),
            DriverCategory::KeyValue
        );
        assert_eq!(
            driver_category_from_driver_name("mongodb"),
            DriverCategory::Document
        );
        assert_eq!(
            driver_category_from_driver_name("timescaledb"),
            DriverCategory::TimeSeries
        );
        assert_eq!(
            driver_category_from_driver_name("neo4j"),
            DriverCategory::Graph
        );
        assert_eq!(
            driver_category_from_driver_name("opensearch"),
            DriverCategory::Search
        );
        assert_eq!(
            driver_category_from_driver_name("postgres"),
            DriverCategory::Relational
        );
    }

    #[test]
    fn default_database_label_mapping_covers_sqlite_main_database() {
        assert_eq!(default_database_label_for_driver("sqlite"), Some("main"));
        assert_eq!(default_database_label_for_driver("sqlite3"), Some("main"));
        assert_eq!(default_database_label_for_driver("postgres"), None);
    }

    #[test]
    fn database_file_open_extension_mapping_is_case_insensitive() {
        assert!(extension_supports_database_file_open("db"));
        assert!(extension_supports_database_file_open("SQLITE"));
        assert!(extension_supports_database_file_open("sqlite3"));
        assert!(extension_supports_database_file_open("duckdb"));
        assert!(!extension_supports_database_file_open("sql"));
        assert!(!extension_supports_database_file_open("csv"));
    }

    #[test]
    fn postgres_dialect_detection_is_case_insensitive() {
        assert!(dialect_is_postgres(Some("postgres")));
        assert!(dialect_is_postgres(Some("PostgreSQL")));
        assert!(!dialect_is_postgres(Some("mysql")));
        assert!(!dialect_is_postgres(None));
    }

    #[test]
    fn mysql_compatible_dialect_detection_is_case_insensitive() {
        assert!(dialect_is_mysql_compatible(Some("mysql")));
        assert!(dialect_is_mysql_compatible(Some("MariaDB")));
        assert!(!dialect_is_mysql_compatible(Some("postgres")));
        assert!(!dialect_is_mysql_compatible(None));
    }

    #[test]
    fn create_or_replace_view_support_is_dialect_driven() {
        assert!(dialect_supports_create_or_replace_view(Some("postgres")));
        assert!(dialect_supports_create_or_replace_view(Some("postgresql")));
        assert!(dialect_supports_create_or_replace_view(Some("mysql")));
        assert!(dialect_supports_create_or_replace_view(Some("mariadb")));
        assert!(!dialect_supports_create_or_replace_view(Some("sqlite")));
        assert!(!dialect_supports_create_or_replace_view(None));
    }

    #[test]
    fn schema_qualifier_resolution_matches_database_scoped_dialects() {
        assert_eq!(
            resolve_schema_qualifier_for_dialect(Some("mysql"), Some("sakila")),
            Some("sakila".to_string())
        );
        assert_eq!(
            resolve_schema_qualifier_for_dialect(Some("clickhouse"), Some("analytics")),
            Some("analytics".to_string())
        );
        assert_eq!(
            resolve_schema_qualifier_for_dialect(Some("postgres"), Some("public")),
            None
        );
        assert_eq!(
            resolve_schema_qualifier_for_dialect(Some("sqlite"), Some("main")),
            None
        );
    }

    #[test]
    fn metadata_scope_resolution_uses_dialect_semantics() {
        assert_eq!(
            resolve_metadata_scope_for_dialect(
                Some("mysql"),
                Some("app"),
                Some("public"),
                Some("fallback_db"),
                Some("fallback_schema")
            ),
            Some("app".to_string())
        );
        assert_eq!(
            resolve_metadata_scope_for_dialect(
                Some("postgres"),
                Some("app"),
                Some("public"),
                Some("fallback_db"),
                Some("fallback_schema")
            ),
            Some("public".to_string())
        );
        assert_eq!(
            resolve_metadata_scope_for_dialect(
                Some("mssql"),
                None,
                Some("dbo"),
                Some("fallback_db"),
                Some("fallback_schema")
            ),
            Some("fallback_db".to_string())
        );
    }
}
