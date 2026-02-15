//! Table-level options (driver-specific)

/// Table-level options that vary by database dialect
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    // SQLite options
    /// WITHOUT ROWID tables (SQLite only)
    pub without_rowid: bool,
    /// STRICT tables (SQLite 3.37+)
    pub strict: bool,

    // MySQL options
    /// Storage engine (InnoDB, MyISAM, etc.)
    pub engine: Option<String>,
    /// Character set
    pub charset: Option<String>,
    /// Collation
    pub collation: Option<String>,
    /// AUTO_INCREMENT starting value
    pub auto_increment_start: Option<u64>,
    /// Row format (DYNAMIC, COMPACT, etc.)
    pub row_format: Option<String>,

    // PostgreSQL options
    /// Tablespace name
    pub tablespace: Option<String>,
    /// UNLOGGED table (not crash-safe but faster)
    pub unlogged: bool,
}

impl TableOptions {
    /// Check if any options are set that would require adding to the DDL
    pub fn has_options(&self) -> bool {
        self.without_rowid
            || self.strict
            || self.engine.is_some()
            || self.charset.is_some()
            || self.collation.is_some()
            || self.auto_increment_start.is_some()
            || self.row_format.is_some()
            || self.tablespace.is_some()
            || self.unlogged
    }

    /// Create SQLite-specific options
    pub fn sqlite() -> Self {
        Self::default()
    }

    /// Create MySQL-specific options with defaults
    pub fn mysql() -> Self {
        Self {
            engine: Some("InnoDB".to_string()),
            charset: Some("utf8mb4".to_string()),
            collation: Some("utf8mb4_unicode_ci".to_string()),
            ..Default::default()
        }
    }

    /// Create PostgreSQL-specific options
    pub fn postgres() -> Self {
        Self::default()
    }
}
