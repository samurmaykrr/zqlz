//! T-SQL dialect implementation for MS SQL Server
//!
//! This module provides the `MssqlDialect` struct with utility methods
//! for generating T-SQL-specific SQL syntax, including identifier quoting
//! and pagination clauses.

use zqlz_core::DialectInfo;

use crate::driver::mssql_dialect;

/// T-SQL dialect implementation for MS SQL Server
///
/// Provides methods for generating T-SQL-specific SQL syntax.
///
/// # Example
///
/// ```
/// use zqlz_driver_mssql::MssqlDialect;
///
/// let dialect = MssqlDialect::new();
///
/// // Quote an identifier using square brackets
/// assert_eq!(dialect.quote_identifier("table"), "[table]");
///
/// // Generate a TOP clause for simple limits
/// assert_eq!(dialect.limit_clause(10, None), "TOP 10");
///
/// // Generate OFFSET FETCH for pagination
/// assert_eq!(dialect.limit_clause(10, Some(20)), "OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
/// ```
#[derive(Debug, Clone, Default)]
pub struct MssqlDialect;

impl MssqlDialect {
    /// Create a new MS SQL Server dialect instance
    pub fn new() -> Self {
        Self
    }

    /// Get the complete dialect information for T-SQL
    ///
    /// Returns the full `DialectInfo` struct containing keywords, functions,
    /// data types, and other dialect-specific metadata.
    pub fn dialect_info(&self) -> DialectInfo {
        mssql_dialect()
    }

    /// Quote an identifier using SQL Server's square bracket syntax
    ///
    /// SQL Server uses `[` and `]` as identifier delimiters. This method
    /// handles identifiers that contain the closing bracket by doubling them.
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// assert_eq!(dialect.quote_identifier("users"), "[users]");
    /// assert_eq!(dialect.quote_identifier("user[data]"), "[user[data]]]");
    /// ```
    pub fn quote_identifier(&self, ident: &str) -> String {
        // Escape any closing brackets by doubling them
        let escaped = ident.replace(']', "]]");
        format!("[{}]", escaped)
    }

    /// Quote a string literal using single quotes
    ///
    /// Escapes single quotes within the string by doubling them.
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// assert_eq!(dialect.quote_string("hello"), "'hello'");
    /// assert_eq!(dialect.quote_string("it's"), "'it''s'");
    /// ```
    pub fn quote_string(&self, s: &str) -> String {
        let escaped = s.replace('\'', "''");
        format!("'{}'", escaped)
    }

    /// Generate a LIMIT clause for SQL Server (T-SQL)
    ///
    /// SQL Server has different syntax depending on whether an offset is needed:
    ///
    /// - **Without offset**: Uses `TOP n` clause (placed after SELECT)
    /// - **With offset**: Uses `OFFSET m ROWS FETCH NEXT n ROWS ONLY` (requires ORDER BY)
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Optional number of rows to skip
    ///
    /// # Returns
    ///
    /// A string containing the appropriate T-SQL pagination clause.
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    ///
    /// // Simple limit - returns TOP clause
    /// assert_eq!(dialect.limit_clause(10, None), "TOP 10");
    ///
    /// // With offset - returns OFFSET FETCH clause
    /// assert_eq!(dialect.limit_clause(10, Some(20)), "OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
    ///
    /// // Zero offset is treated as no offset
    /// assert_eq!(dialect.limit_clause(5, Some(0)), "TOP 5");
    /// ```
    ///
    /// # Note
    ///
    /// When using OFFSET FETCH, the query **must** include an ORDER BY clause.
    /// The TOP clause can be used without ORDER BY.
    pub fn limit_clause(&self, limit: u64, offset: Option<u64>) -> String {
        match offset {
            Some(off) if off > 0 => {
                // OFFSET FETCH syntax (SQL Server 2012+)
                // Requires ORDER BY in the query
                format!("OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", off, limit)
            }
            _ => {
                // TOP syntax (all SQL Server versions)
                format!("TOP {}", limit)
            }
        }
    }

    /// Generate a row-limiting clause suitable for a subquery
    ///
    /// When using pagination in a subquery, SQL Server requires the TOP clause
    /// unless the subquery has its own ORDER BY with OFFSET FETCH.
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// assert_eq!(dialect.limit_clause_for_subquery(100), "TOP 100");
    /// ```
    pub fn limit_clause_for_subquery(&self, limit: u64) -> String {
        format!("TOP {}", limit)
    }

    /// Generate an ORDER BY clause with OFFSET FETCH for pagination
    ///
    /// This is the preferred pagination method for SQL Server 2012+ when
    /// you need both ordering and pagination.
    ///
    /// # Arguments
    ///
    /// * `order_by` - The column(s) to order by (e.g., "id ASC")
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// let clause = dialect.order_by_with_pagination("created_at DESC", 10, 20);
    /// assert_eq!(clause, "ORDER BY created_at DESC OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
    /// ```
    pub fn order_by_with_pagination(&self, order_by: &str, limit: u64, offset: u64) -> String {
        format!(
            "ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            order_by, offset, limit
        )
    }

    /// Check if an identifier needs quoting
    ///
    /// Returns true if the identifier is a reserved keyword, contains
    /// special characters, or starts with a digit.
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// assert!(dialect.needs_quoting("select"));  // reserved keyword
    /// assert!(dialect.needs_quoting("my table")); // contains space
    /// assert!(dialect.needs_quoting("123col"));   // starts with digit
    /// assert!(!dialect.needs_quoting("users"));   // simple identifier
    /// ```
    pub fn needs_quoting(&self, ident: &str) -> bool {
        if ident.is_empty() {
            return true;
        }

        // Starts with a digit
        if ident.chars().next().map_or(false, |c| c.is_ascii_digit()) {
            return true;
        }

        // Contains non-alphanumeric characters (except underscore)
        if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return true;
        }

        // Check if it's a reserved keyword (case-insensitive)
        let upper = ident.to_uppercase();
        RESERVED_KEYWORDS.contains(&upper.as_str())
    }

    /// Quote an identifier only if necessary
    ///
    /// # Example
    ///
    /// ```
    /// use zqlz_driver_mssql::MssqlDialect;
    ///
    /// let dialect = MssqlDialect::new();
    /// assert_eq!(dialect.quote_identifier_if_needed("users"), "users");
    /// assert_eq!(dialect.quote_identifier_if_needed("select"), "[select]");
    /// ```
    pub fn quote_identifier_if_needed(&self, ident: &str) -> String {
        if self.needs_quoting(ident) {
            self.quote_identifier(ident)
        } else {
            ident.to_string()
        }
    }
}

/// T-SQL reserved keywords that require quoting when used as identifiers
static RESERVED_KEYWORDS: &[&str] = &[
    // DQL
    "SELECT",
    "FROM",
    "WHERE",
    "ORDER",
    "GROUP",
    "BY",
    "HAVING",
    "DISTINCT",
    "ALL",
    "TOP",
    "OFFSET",
    "FETCH",
    "NEXT",
    "ROWS",
    "ONLY",
    // DML
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "INTO",
    "VALUES",
    "SET",
    "OUTPUT",
    // DDL
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "TABLE",
    "VIEW",
    "INDEX",
    "DATABASE",
    "SCHEMA",
    "PROCEDURE",
    "FUNCTION",
    "TRIGGER",
    // Joins
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "OUTER",
    "FULL",
    "CROSS",
    "ON",
    // Operators
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "IS",
    "NULL",
    "EXISTS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    // Transaction
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "TRANSACTION",
    "SAVE",
    "TRAN",
    // DCL
    "GRANT",
    "REVOKE",
    "DENY",
    // T-SQL specific
    "GO",
    "USE",
    "EXEC",
    "EXECUTE",
    "DECLARE",
    "PRINT",
    "RETURN",
    "IF",
    "WHILE",
    "BREAK",
    "CONTINUE",
    "GOTO",
    "TRY",
    "CATCH",
    "THROW",
    // Clauses
    "AS",
    "WITH",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "OVER",
    "PARTITION",
    "ASC",
    "DESC",
    // Constraints
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "CONSTRAINT",
    "IDENTITY",
    // Types
    "INT",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BIT",
    "DECIMAL",
    "NUMERIC",
    "FLOAT",
    "REAL",
    "CHAR",
    "VARCHAR",
    "NCHAR",
    "NVARCHAR",
    "TEXT",
    "NTEXT",
    "DATE",
    "TIME",
    "DATETIME",
    "DATETIME2",
    "BINARY",
    "VARBINARY",
    "IMAGE",
    "XML",
];
