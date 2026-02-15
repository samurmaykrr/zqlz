//! SQLite dialect information
//!
//! Provides comprehensive metadata about the SQLite SQL dialect.

use std::borrow::Cow;
use zqlz_core::{
    AutoIncrementInfo, AutoIncrementStyle, CommentStyles, DataTypeCategory, DataTypeInfo,
    DialectInfo, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo, SqlFunctionInfo,
    TableOptionDef, TableOptionType,
};

/// Build the complete SQLite dialect info
pub fn sqlite_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("sqlite"),
        display_name: Cow::Borrowed("SQLite"),
        keywords: sqlite_keywords(),
        functions: sqlite_functions(),
        data_types: sqlite_data_types(),
        table_options: sqlite_table_options(),
        auto_increment: Some(AutoIncrementInfo {
            keyword: Cow::Borrowed("AUTOINCREMENT"),
            style: AutoIncrementStyle::Suffix,
            description: Some(Cow::Borrowed(
                "Only valid for INTEGER PRIMARY KEY. Ensures unique rowid even after deletion.",
            )),
        }),
        identifier_quote: '"',
        string_quote: '\'',
        case_sensitive_identifiers: false,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: ExplainConfig::sqlite(),
    }
}

fn sqlite_keywords() -> Vec<KeywordInfo> {
    vec![
        // DQL
        KeywordInfo::with_desc("SELECT", KeywordCategory::Dql, "Retrieve data from tables"),
        KeywordInfo::with_desc("FROM", KeywordCategory::Dql, "Specify source tables"),
        KeywordInfo::with_desc("WHERE", KeywordCategory::Dql, "Filter rows"),
        KeywordInfo::with_desc("ORDER BY", KeywordCategory::Dql, "Sort results"),
        KeywordInfo::with_desc("GROUP BY", KeywordCategory::Dql, "Group rows"),
        KeywordInfo::with_desc("HAVING", KeywordCategory::Dql, "Filter groups"),
        KeywordInfo::with_desc("LIMIT", KeywordCategory::Dql, "Limit result count"),
        KeywordInfo::with_desc("OFFSET", KeywordCategory::Dql, "Skip rows"),
        KeywordInfo::with_desc("DISTINCT", KeywordCategory::Dql, "Remove duplicates"),
        // DML
        KeywordInfo::with_desc("INSERT", KeywordCategory::Dml, "Insert rows"),
        KeywordInfo::with_desc("UPDATE", KeywordCategory::Dml, "Update rows"),
        KeywordInfo::with_desc("DELETE", KeywordCategory::Dml, "Delete rows"),
        KeywordInfo::with_desc("REPLACE", KeywordCategory::Dml, "Insert or replace rows"),
        // DDL
        KeywordInfo::with_desc("CREATE", KeywordCategory::Ddl, "Create database objects"),
        KeywordInfo::with_desc("ALTER", KeywordCategory::Ddl, "Modify database objects"),
        KeywordInfo::with_desc("DROP", KeywordCategory::Ddl, "Remove database objects"),
        KeywordInfo::with_desc("TABLE", KeywordCategory::Ddl, "Table object type"),
        KeywordInfo::with_desc("INDEX", KeywordCategory::Ddl, "Index object type"),
        KeywordInfo::with_desc("VIEW", KeywordCategory::Ddl, "View object type"),
        KeywordInfo::with_desc("TRIGGER", KeywordCategory::Ddl, "Trigger object type"),
        // Transaction
        KeywordInfo::with_desc("BEGIN", KeywordCategory::Transaction, "Start transaction"),
        KeywordInfo::with_desc("COMMIT", KeywordCategory::Transaction, "Commit transaction"),
        KeywordInfo::with_desc(
            "ROLLBACK",
            KeywordCategory::Transaction,
            "Rollback transaction",
        ),
        KeywordInfo::with_desc(
            "SAVEPOINT",
            KeywordCategory::Transaction,
            "Create savepoint",
        ),
        // Clauses
        KeywordInfo::with_desc("JOIN", KeywordCategory::Clause, "Join tables"),
        KeywordInfo::with_desc("INNER JOIN", KeywordCategory::Clause, "Inner join"),
        KeywordInfo::with_desc("LEFT JOIN", KeywordCategory::Clause, "Left outer join"),
        KeywordInfo::with_desc("CROSS JOIN", KeywordCategory::Clause, "Cross join"),
        KeywordInfo::with_desc("ON", KeywordCategory::Clause, "Join condition"),
        KeywordInfo::with_desc("USING", KeywordCategory::Clause, "Join using columns"),
        KeywordInfo::with_desc("AS", KeywordCategory::Clause, "Alias"),
        KeywordInfo::with_desc("UNION", KeywordCategory::Clause, "Combine results"),
        KeywordInfo::with_desc("EXCEPT", KeywordCategory::Clause, "Subtract results"),
        KeywordInfo::with_desc("INTERSECT", KeywordCategory::Clause, "Intersect results"),
        KeywordInfo::with_desc("WITH", KeywordCategory::Clause, "Common Table Expression"),
        KeywordInfo::with_desc("RECURSIVE", KeywordCategory::Clause, "Recursive CTE"),
        KeywordInfo::with_desc("VALUES", KeywordCategory::Clause, "Values clause"),
        KeywordInfo::with_desc("RETURNING", KeywordCategory::Clause, "Return affected rows"),
        // Operators
        KeywordInfo::with_desc("AND", KeywordCategory::Operator, "Logical AND"),
        KeywordInfo::with_desc("OR", KeywordCategory::Operator, "Logical OR"),
        KeywordInfo::with_desc("NOT", KeywordCategory::Operator, "Logical NOT"),
        KeywordInfo::with_desc("IN", KeywordCategory::Operator, "In list/subquery"),
        KeywordInfo::with_desc("LIKE", KeywordCategory::Operator, "Pattern matching"),
        KeywordInfo::with_desc(
            "GLOB",
            KeywordCategory::Operator,
            "Unix-style pattern matching",
        ),
        KeywordInfo::with_desc("BETWEEN", KeywordCategory::Operator, "Range check"),
        KeywordInfo::with_desc("IS", KeywordCategory::Operator, "Identity comparison"),
        KeywordInfo::with_desc("NULL", KeywordCategory::Operator, "Null value"),
        KeywordInfo::with_desc("EXISTS", KeywordCategory::Operator, "Subquery existence"),
        KeywordInfo::with_desc("CASE", KeywordCategory::Operator, "Conditional expression"),
        KeywordInfo::with_desc("WHEN", KeywordCategory::Operator, "Case condition"),
        KeywordInfo::with_desc("THEN", KeywordCategory::Operator, "Case result"),
        KeywordInfo::with_desc("ELSE", KeywordCategory::Operator, "Case default"),
        KeywordInfo::with_desc("END", KeywordCategory::Operator, "End case/block"),
        // SQLite-specific
        KeywordInfo::with_desc(
            "PRAGMA",
            KeywordCategory::DatabaseSpecific,
            "SQLite configuration",
        ),
        KeywordInfo::with_desc(
            "ATTACH",
            KeywordCategory::DatabaseSpecific,
            "Attach database",
        ),
        KeywordInfo::with_desc(
            "DETACH",
            KeywordCategory::DatabaseSpecific,
            "Detach database",
        ),
        KeywordInfo::with_desc(
            "VACUUM",
            KeywordCategory::DatabaseSpecific,
            "Rebuild database",
        ),
        KeywordInfo::with_desc(
            "ANALYZE",
            KeywordCategory::DatabaseSpecific,
            "Update statistics",
        ),
        KeywordInfo::with_desc(
            "REINDEX",
            KeywordCategory::DatabaseSpecific,
            "Rebuild indexes",
        ),
        KeywordInfo::with_desc("EXPLAIN", KeywordCategory::DatabaseSpecific, "Query plan"),
        KeywordInfo::with_desc(
            "EXPLAIN QUERY PLAN",
            KeywordCategory::DatabaseSpecific,
            "Detailed query plan",
        ),
        // Constraints
        KeywordInfo::with_desc(
            "PRIMARY KEY",
            KeywordCategory::Ddl,
            "Primary key constraint",
        ),
        KeywordInfo::with_desc(
            "FOREIGN KEY",
            KeywordCategory::Ddl,
            "Foreign key constraint",
        ),
        KeywordInfo::with_desc("REFERENCES", KeywordCategory::Ddl, "Foreign key reference"),
        KeywordInfo::with_desc("UNIQUE", KeywordCategory::Ddl, "Unique constraint"),
        KeywordInfo::with_desc("CHECK", KeywordCategory::Ddl, "Check constraint"),
        KeywordInfo::with_desc("DEFAULT", KeywordCategory::Ddl, "Default value"),
        KeywordInfo::with_desc("AUTOINCREMENT", KeywordCategory::Ddl, "Auto-increment"),
        KeywordInfo::with_desc(
            "WITHOUT ROWID",
            KeywordCategory::DatabaseSpecific,
            "Clustered table",
        ),
        KeywordInfo::with_desc("STRICT", KeywordCategory::DatabaseSpecific, "Strict typing"),
        KeywordInfo::with_desc(
            "ON CONFLICT",
            KeywordCategory::Dml,
            "Conflict resolution clause",
        ),
    ]
}

fn sqlite_functions() -> Vec<SqlFunctionInfo> {
    vec![
        // Aggregate functions
        SqlFunctionInfo::new("COUNT", FunctionCategory::Aggregate)
            .with_signature("COUNT(*) or COUNT(expression)"),
        SqlFunctionInfo::new("SUM", FunctionCategory::Aggregate).with_signature("SUM(expression)"),
        SqlFunctionInfo::new("AVG", FunctionCategory::Aggregate).with_signature("AVG(expression)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Aggregate).with_signature("MIN(expression)"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Aggregate).with_signature("MAX(expression)"),
        SqlFunctionInfo::new("GROUP_CONCAT", FunctionCategory::Aggregate)
            .with_signature("GROUP_CONCAT(expression, separator)"),
        SqlFunctionInfo::new("TOTAL", FunctionCategory::Aggregate)
            .with_signature("TOTAL(expression)"),
        // String functions
        SqlFunctionInfo::new("LENGTH", FunctionCategory::String).with_signature("LENGTH(string)"),
        SqlFunctionInfo::new("SUBSTR", FunctionCategory::String)
            .with_signature("SUBSTR(string, start, length)"),
        SqlFunctionInfo::new("UPPER", FunctionCategory::String).with_signature("UPPER(string)"),
        SqlFunctionInfo::new("LOWER", FunctionCategory::String).with_signature("LOWER(string)"),
        SqlFunctionInfo::new("TRIM", FunctionCategory::String).with_signature("TRIM(string)"),
        SqlFunctionInfo::new("LTRIM", FunctionCategory::String).with_signature("LTRIM(string)"),
        SqlFunctionInfo::new("RTRIM", FunctionCategory::String).with_signature("RTRIM(string)"),
        SqlFunctionInfo::new("REPLACE", FunctionCategory::String)
            .with_signature("REPLACE(string, from, to)"),
        SqlFunctionInfo::new("INSTR", FunctionCategory::String)
            .with_signature("INSTR(string, substring)"),
        SqlFunctionInfo::new("PRINTF", FunctionCategory::String)
            .with_signature("PRINTF(format, args...)"),
        SqlFunctionInfo::new("CONCAT", FunctionCategory::String)
            .with_signature("CONCAT(value1, value2, ...)"),
        // Numeric functions
        SqlFunctionInfo::new("ABS", FunctionCategory::Numeric).with_signature("ABS(number)"),
        SqlFunctionInfo::new("ROUND", FunctionCategory::Numeric)
            .with_signature("ROUND(number, decimals)"),
        SqlFunctionInfo::new("RANDOM", FunctionCategory::Numeric).with_signature("RANDOM()"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Numeric)
            .with_signature("MAX(value1, value2, ...)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Numeric)
            .with_signature("MIN(value1, value2, ...)"),
        // Date/Time functions
        SqlFunctionInfo::new("DATE", FunctionCategory::DateTime)
            .with_signature("DATE(timestring, modifier...)"),
        SqlFunctionInfo::new("TIME", FunctionCategory::DateTime)
            .with_signature("TIME(timestring, modifier...)"),
        SqlFunctionInfo::new("DATETIME", FunctionCategory::DateTime)
            .with_signature("DATETIME(timestring, modifier...)"),
        SqlFunctionInfo::new("JULIANDAY", FunctionCategory::DateTime)
            .with_signature("JULIANDAY(timestring)"),
        SqlFunctionInfo::new("STRFTIME", FunctionCategory::DateTime)
            .with_signature("STRFTIME(format, timestring)"),
        SqlFunctionInfo::new("UNIXEPOCH", FunctionCategory::DateTime)
            .with_signature("UNIXEPOCH(timestring)"),
        // Conditional
        SqlFunctionInfo::new("COALESCE", FunctionCategory::Conditional)
            .with_signature("COALESCE(value1, value2, ...)"),
        SqlFunctionInfo::new("NULLIF", FunctionCategory::Conditional)
            .with_signature("NULLIF(value1, value2)"),
        SqlFunctionInfo::new("IIF", FunctionCategory::Conditional)
            .with_signature("IIF(condition, true_result, false_result)"),
        SqlFunctionInfo::new("IFNULL", FunctionCategory::Conditional)
            .with_signature("IFNULL(value, default)"),
        // Type conversion
        SqlFunctionInfo::new("CAST", FunctionCategory::Conversion)
            .with_signature("CAST(expression AS type)"),
        SqlFunctionInfo::new("TYPEOF", FunctionCategory::Conversion)
            .with_signature("TYPEOF(expression)"),
        // JSON functions
        SqlFunctionInfo::new("JSON", FunctionCategory::Json).with_signature("JSON(json_string)"),
        SqlFunctionInfo::new("JSON_EXTRACT", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT(json, path)"),
        SqlFunctionInfo::new("JSON_OBJECT", FunctionCategory::Json)
            .with_signature("JSON_OBJECT(key1, value1, ...)"),
        SqlFunctionInfo::new("JSON_ARRAY", FunctionCategory::Json)
            .with_signature("JSON_ARRAY(value1, value2, ...)"),
        SqlFunctionInfo::new("JSON_TYPE", FunctionCategory::Json)
            .with_signature("JSON_TYPE(json, path)"),
        // Other
        SqlFunctionInfo::new("HEX", FunctionCategory::Other).with_signature("HEX(blob)"),
        SqlFunctionInfo::new("ZEROBLOB", FunctionCategory::Other).with_signature("ZEROBLOB(n)"),
        SqlFunctionInfo::new("QUOTE", FunctionCategory::Other).with_signature("QUOTE(value)"),
        SqlFunctionInfo::new("LIKELIHOOD", FunctionCategory::Other)
            .with_signature("LIKELIHOOD(value, probability)"),
        SqlFunctionInfo::new("LIKELY", FunctionCategory::Other).with_signature("LIKELY(value)"),
        SqlFunctionInfo::new("UNLIKELY", FunctionCategory::Other).with_signature("UNLIKELY(value)"),
        // Window functions
        SqlFunctionInfo::new("ROW_NUMBER", FunctionCategory::Window)
            .with_signature("ROW_NUMBER() OVER(...)"),
        SqlFunctionInfo::new("RANK", FunctionCategory::Window).with_signature("RANK() OVER(...)"),
        SqlFunctionInfo::new("DENSE_RANK", FunctionCategory::Window)
            .with_signature("DENSE_RANK() OVER(...)"),
        SqlFunctionInfo::new("NTILE", FunctionCategory::Window)
            .with_signature("NTILE(n) OVER(...)"),
        SqlFunctionInfo::new("LAG", FunctionCategory::Window)
            .with_signature("LAG(expression, offset, default) OVER(...)"),
        SqlFunctionInfo::new("LEAD", FunctionCategory::Window)
            .with_signature("LEAD(expression, offset, default) OVER(...)"),
        SqlFunctionInfo::new("FIRST_VALUE", FunctionCategory::Window)
            .with_signature("FIRST_VALUE(expression) OVER(...)"),
        SqlFunctionInfo::new("LAST_VALUE", FunctionCategory::Window)
            .with_signature("LAST_VALUE(expression) OVER(...)"),
        SqlFunctionInfo::new("NTH_VALUE", FunctionCategory::Window)
            .with_signature("NTH_VALUE(expression, n) OVER(...)"),
    ]
}

fn sqlite_data_types() -> Vec<DataTypeInfo> {
    vec![
        // SQLite has only 5 storage classes - these are the real types
        DataTypeInfo::new("INTEGER", DataTypeCategory::Integer),
        DataTypeInfo::new("REAL", DataTypeCategory::Float),
        DataTypeInfo::new("TEXT", DataTypeCategory::String),
        DataTypeInfo::new("BLOB", DataTypeCategory::Binary),
        DataTypeInfo::new("NUMERIC", DataTypeCategory::Decimal),
    ]
}

fn sqlite_table_options() -> Vec<TableOptionDef> {
    vec![
        TableOptionDef {
            key: Cow::Borrowed("without_rowid"),
            label: Cow::Borrowed("WITHOUT ROWID"),
            option_type: TableOptionType::Boolean,
            default_value: Some(Cow::Borrowed("false")),
            description: Some(Cow::Borrowed(
                "Create a clustered table without the implicit rowid column",
            )),
            choices: Vec::new(),
        },
        TableOptionDef {
            key: Cow::Borrowed("strict"),
            label: Cow::Borrowed("STRICT"),
            option_type: TableOptionType::Boolean,
            default_value: Some(Cow::Borrowed("false")),
            description: Some(Cow::Borrowed("Enforce strict type checking (SQLite 3.37+)")),
            choices: Vec::new(),
        },
    ]
}
