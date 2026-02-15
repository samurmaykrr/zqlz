//! Database icon and logo helpers
//!
//! Maps database types to their corresponding themed icons and colored logos.

use crate::widgets::sidebar::ConnectionSidebar;
use zqlz_ui::widgets::{DatabaseLogo, ZqlzIcon};

impl ConnectionSidebar {
    /// Get database icon based on database type.
    ///
    /// Returns a themed monochrome icon that adapts to the current theme.
    /// Used as a fallback when a colored logo isn't available or in contexts
    /// where monochrome icons are preferred (e.g., disabled states).
    ///
    /// # Examples
    ///
    /// ```
    /// let icon = sidebar.get_db_icon("postgresql"); // ZqlzIcon::PostgreSQL
    /// let icon = sidebar.get_db_icon("unknown");     // ZqlzIcon::Database (fallback)
    /// ```
    pub(super) fn get_db_icon(&self, db_type: &str) -> ZqlzIcon {
        match db_type.to_lowercase().as_str() {
            "sqlite" => ZqlzIcon::SQLite,
            "postgresql" | "postgres" => ZqlzIcon::PostgreSQL,
            "mysql" => ZqlzIcon::MySQL,
            "mariadb" => ZqlzIcon::MariaDB,
            "redis" => ZqlzIcon::Redis,
            "mongodb" => ZqlzIcon::MongoDB,
            "clickhouse" => ZqlzIcon::ClickHouse,
            "duckdb" => ZqlzIcon::DuckDB,
            "mssql" | "sqlserver" => ZqlzIcon::MsSql,
            _ => ZqlzIcon::Database,
        }
    }

    /// Get colored database logo based on database type.
    ///
    /// Returns the official colored logo for known database types.
    /// These logos maintain their brand colors regardless of theme.
    ///
    /// # Returns
    ///
    /// - `Some(logo)` for recognized database types with official branding
    /// - `None` for unknown types (fallback to monochrome icon via `get_db_icon`)
    ///
    /// # Examples
    ///
    /// ```
    /// if let Some(logo) = sidebar.get_db_logo("postgresql") {
    ///     // Render colored PostgreSQL elephant logo
    /// } else {
    ///     // Fall back to monochrome database icon
    /// }
    /// ```
    pub(super) fn get_db_logo(&self, db_type: &str) -> Option<DatabaseLogo> {
        match db_type.to_lowercase().as_str() {
            "sqlite" => Some(DatabaseLogo::SQLite),
            "postgresql" | "postgres" => Some(DatabaseLogo::PostgreSQL),
            "mysql" => Some(DatabaseLogo::MySQL),
            "mariadb" => Some(DatabaseLogo::MariaDB),
            "redis" => Some(DatabaseLogo::Redis),
            "mongodb" => Some(DatabaseLogo::MongoDB),
            "clickhouse" => Some(DatabaseLogo::ClickHouse),
            "duckdb" => Some(DatabaseLogo::DuckDB),
            "mssql" | "sqlserver" => Some(DatabaseLogo::MsSql),
            _ => None,
        }
    }
}
