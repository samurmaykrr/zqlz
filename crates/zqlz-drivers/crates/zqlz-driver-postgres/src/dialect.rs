//! PostgreSQL dialect information
//!
//! Provides comprehensive metadata about the PostgreSQL SQL dialect.

use std::borrow::Cow;
use zqlz_core::{
    AutoIncrementInfo, AutoIncrementStyle, CommentStyles, DataTypeCategory, DataTypeInfo,
    DialectInfo, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo, SqlFunctionInfo,
    TableOptionDef, TableOptionType,
};

/// Build the complete PostgreSQL dialect info
pub fn postgres_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("postgresql"),
        display_name: Cow::Borrowed("PostgreSQL"),
        keywords: postgres_keywords(),
        functions: postgres_functions(),
        data_types: postgres_data_types(),
        table_options: postgres_table_options(),
        auto_increment: Some(AutoIncrementInfo {
            keyword: Cow::Borrowed("SERIAL"),
            style: AutoIncrementStyle::TypeName,
            description: Some(Cow::Borrowed(
                "Use SERIAL, BIGSERIAL, or SMALLSERIAL types for auto-increment",
            )),
        }),
        identifier_quote: '"',
        string_quote: '\'',
        case_sensitive_identifiers: false,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: ExplainConfig::postgresql(),
    }
}

fn postgres_keywords() -> Vec<KeywordInfo> {
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
        KeywordInfo::with_desc("FETCH", KeywordCategory::Dql, "SQL standard row limiting"),
        KeywordInfo::with_desc("DISTINCT", KeywordCategory::Dql, "Remove duplicates"),
        KeywordInfo::with_desc(
            "DISTINCT ON",
            KeywordCategory::Dql,
            "Remove duplicates by columns",
        ),
        KeywordInfo::with_desc("FOR UPDATE", KeywordCategory::Dql, "Lock selected rows"),
        KeywordInfo::with_desc(
            "FOR SHARE",
            KeywordCategory::Dql,
            "Share lock selected rows",
        ),
        // DML
        KeywordInfo::with_desc("INSERT", KeywordCategory::Dml, "Insert rows"),
        KeywordInfo::with_desc("UPDATE", KeywordCategory::Dml, "Update rows"),
        KeywordInfo::with_desc("DELETE", KeywordCategory::Dml, "Delete rows"),
        KeywordInfo::with_desc("UPSERT", KeywordCategory::Dml, "Insert or update rows"),
        KeywordInfo::with_desc("TRUNCATE", KeywordCategory::Dml, "Remove all rows quickly"),
        KeywordInfo::with_desc("COPY", KeywordCategory::Dml, "Bulk data import/export"),
        // DDL
        KeywordInfo::with_desc("CREATE", KeywordCategory::Ddl, "Create database objects"),
        KeywordInfo::with_desc("ALTER", KeywordCategory::Ddl, "Modify database objects"),
        KeywordInfo::with_desc("DROP", KeywordCategory::Ddl, "Remove database objects"),
        KeywordInfo::with_desc("TABLE", KeywordCategory::Ddl, "Table object type"),
        KeywordInfo::with_desc("INDEX", KeywordCategory::Ddl, "Index object type"),
        KeywordInfo::with_desc("VIEW", KeywordCategory::Ddl, "View object type"),
        KeywordInfo::with_desc(
            "MATERIALIZED VIEW",
            KeywordCategory::Ddl,
            "Materialized view",
        ),
        KeywordInfo::with_desc("SEQUENCE", KeywordCategory::Ddl, "Sequence object type"),
        KeywordInfo::with_desc("SCHEMA", KeywordCategory::Ddl, "Schema namespace"),
        KeywordInfo::with_desc("DATABASE", KeywordCategory::Ddl, "Database object type"),
        KeywordInfo::with_desc("TYPE", KeywordCategory::Ddl, "Custom type"),
        KeywordInfo::with_desc("ENUM", KeywordCategory::Ddl, "Enumeration type"),
        KeywordInfo::with_desc("DOMAIN", KeywordCategory::Ddl, "Domain type"),
        KeywordInfo::with_desc("EXTENSION", KeywordCategory::Ddl, "Extension"),
        KeywordInfo::with_desc("FUNCTION", KeywordCategory::Ddl, "Function"),
        KeywordInfo::with_desc("PROCEDURE", KeywordCategory::Ddl, "Procedure"),
        KeywordInfo::with_desc("TRIGGER", KeywordCategory::Ddl, "Trigger"),
        // DCL
        KeywordInfo::with_desc("GRANT", KeywordCategory::Dcl, "Grant privileges"),
        KeywordInfo::with_desc("REVOKE", KeywordCategory::Dcl, "Revoke privileges"),
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
        KeywordInfo::with_desc("RELEASE", KeywordCategory::Transaction, "Release savepoint"),
        // Clauses
        KeywordInfo::with_desc("JOIN", KeywordCategory::Clause, "Join tables"),
        KeywordInfo::with_desc("INNER JOIN", KeywordCategory::Clause, "Inner join"),
        KeywordInfo::with_desc("LEFT JOIN", KeywordCategory::Clause, "Left outer join"),
        KeywordInfo::with_desc("RIGHT JOIN", KeywordCategory::Clause, "Right outer join"),
        KeywordInfo::with_desc("FULL JOIN", KeywordCategory::Clause, "Full outer join"),
        KeywordInfo::with_desc("CROSS JOIN", KeywordCategory::Clause, "Cross join"),
        KeywordInfo::with_desc("LATERAL", KeywordCategory::Clause, "Lateral join"),
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
        KeywordInfo::with_desc(
            "ON CONFLICT",
            KeywordCategory::Clause,
            "Upsert conflict handling",
        ),
        KeywordInfo::with_desc("DO UPDATE", KeywordCategory::Clause, "Upsert update action"),
        KeywordInfo::with_desc(
            "DO NOTHING",
            KeywordCategory::Clause,
            "Upsert ignore action",
        ),
        KeywordInfo::with_desc("WINDOW", KeywordCategory::Clause, "Window definition"),
        // Operators
        KeywordInfo::with_desc("AND", KeywordCategory::Operator, "Logical AND"),
        KeywordInfo::with_desc("OR", KeywordCategory::Operator, "Logical OR"),
        KeywordInfo::with_desc("NOT", KeywordCategory::Operator, "Logical NOT"),
        KeywordInfo::with_desc("IN", KeywordCategory::Operator, "In list/subquery"),
        KeywordInfo::with_desc("LIKE", KeywordCategory::Operator, "Pattern matching"),
        KeywordInfo::with_desc("ILIKE", KeywordCategory::Operator, "Case-insensitive LIKE"),
        KeywordInfo::with_desc(
            "SIMILAR TO",
            KeywordCategory::Operator,
            "Regex-like pattern",
        ),
        KeywordInfo::with_desc("BETWEEN", KeywordCategory::Operator, "Range check"),
        KeywordInfo::with_desc("IS", KeywordCategory::Operator, "Identity comparison"),
        KeywordInfo::with_desc(
            "IS DISTINCT FROM",
            KeywordCategory::Operator,
            "Null-safe comparison",
        ),
        KeywordInfo::with_desc("NULL", KeywordCategory::Operator, "Null value"),
        KeywordInfo::with_desc("EXISTS", KeywordCategory::Operator, "Subquery existence"),
        KeywordInfo::with_desc("ANY", KeywordCategory::Operator, "Array/subquery any"),
        KeywordInfo::with_desc("ALL", KeywordCategory::Operator, "Array/subquery all"),
        KeywordInfo::with_desc("SOME", KeywordCategory::Operator, "Alias for ANY"),
        KeywordInfo::with_desc("CASE", KeywordCategory::Operator, "Conditional expression"),
        KeywordInfo::with_desc("WHEN", KeywordCategory::Operator, "Case condition"),
        KeywordInfo::with_desc("THEN", KeywordCategory::Operator, "Case result"),
        KeywordInfo::with_desc("ELSE", KeywordCategory::Operator, "Case default"),
        KeywordInfo::with_desc("END", KeywordCategory::Operator, "End case/block"),
        // PostgreSQL-specific
        KeywordInfo::with_desc("EXPLAIN", KeywordCategory::DatabaseSpecific, "Query plan"),
        KeywordInfo::with_desc(
            "EXPLAIN ANALYZE",
            KeywordCategory::DatabaseSpecific,
            "Execute and show plan",
        ),
        KeywordInfo::with_desc(
            "ANALYZE",
            KeywordCategory::DatabaseSpecific,
            "Update statistics",
        ),
        KeywordInfo::with_desc(
            "VACUUM",
            KeywordCategory::DatabaseSpecific,
            "Reclaim storage",
        ),
        KeywordInfo::with_desc(
            "REINDEX",
            KeywordCategory::DatabaseSpecific,
            "Rebuild indexes",
        ),
        KeywordInfo::with_desc(
            "CLUSTER",
            KeywordCategory::DatabaseSpecific,
            "Cluster table",
        ),
        KeywordInfo::with_desc(
            "REFRESH",
            KeywordCategory::DatabaseSpecific,
            "Refresh materialized view",
        ),
        KeywordInfo::with_desc(
            "LISTEN",
            KeywordCategory::DatabaseSpecific,
            "Listen for notifications",
        ),
        KeywordInfo::with_desc(
            "NOTIFY",
            KeywordCategory::DatabaseSpecific,
            "Send notification",
        ),
        KeywordInfo::with_desc(
            "SET",
            KeywordCategory::DatabaseSpecific,
            "Set session variable",
        ),
        KeywordInfo::with_desc(
            "SHOW",
            KeywordCategory::DatabaseSpecific,
            "Show setting value",
        ),
        KeywordInfo::with_desc("RESET", KeywordCategory::DatabaseSpecific, "Reset setting"),
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
        KeywordInfo::with_desc("NOT NULL", KeywordCategory::Ddl, "Not null constraint"),
        KeywordInfo::with_desc("EXCLUDE", KeywordCategory::Ddl, "Exclusion constraint"),
        KeywordInfo::with_desc("DEFERRABLE", KeywordCategory::Ddl, "Deferrable constraint"),
        KeywordInfo::with_desc("GENERATED", KeywordCategory::Ddl, "Generated column"),
        KeywordInfo::with_desc("IDENTITY", KeywordCategory::Ddl, "Identity column"),
        // Table options
        KeywordInfo::with_desc(
            "UNLOGGED",
            KeywordCategory::DatabaseSpecific,
            "Unlogged table",
        ),
        KeywordInfo::with_desc(
            "TEMPORARY",
            KeywordCategory::DatabaseSpecific,
            "Temporary table",
        ),
        KeywordInfo::with_desc("TEMP", KeywordCategory::DatabaseSpecific, "Temporary table"),
        KeywordInfo::with_desc("IF EXISTS", KeywordCategory::Ddl, "Conditional drop"),
        KeywordInfo::with_desc("IF NOT EXISTS", KeywordCategory::Ddl, "Conditional create"),
        KeywordInfo::with_desc("CASCADE", KeywordCategory::Ddl, "Drop dependents"),
        KeywordInfo::with_desc("RESTRICT", KeywordCategory::Ddl, "Prevent if dependents"),
    ]
}

fn postgres_functions() -> Vec<SqlFunctionInfo> {
    vec![
        // Aggregate functions
        SqlFunctionInfo::new("COUNT", FunctionCategory::Aggregate)
            .with_signature("COUNT(*) or COUNT(expression)"),
        SqlFunctionInfo::new("SUM", FunctionCategory::Aggregate).with_signature("SUM(expression)"),
        SqlFunctionInfo::new("AVG", FunctionCategory::Aggregate).with_signature("AVG(expression)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Aggregate).with_signature("MIN(expression)"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Aggregate).with_signature("MAX(expression)"),
        SqlFunctionInfo::new("ARRAY_AGG", FunctionCategory::Aggregate)
            .with_signature("ARRAY_AGG(expression ORDER BY ...)"),
        SqlFunctionInfo::new("STRING_AGG", FunctionCategory::Aggregate)
            .with_signature("STRING_AGG(expression, separator ORDER BY ...)"),
        SqlFunctionInfo::new("BOOL_AND", FunctionCategory::Aggregate)
            .with_signature("BOOL_AND(expression)"),
        SqlFunctionInfo::new("BOOL_OR", FunctionCategory::Aggregate)
            .with_signature("BOOL_OR(expression)"),
        SqlFunctionInfo::new("BIT_AND", FunctionCategory::Aggregate)
            .with_signature("BIT_AND(expression)"),
        SqlFunctionInfo::new("BIT_OR", FunctionCategory::Aggregate)
            .with_signature("BIT_OR(expression)"),
        SqlFunctionInfo::new("JSONB_AGG", FunctionCategory::Aggregate)
            .with_signature("JSONB_AGG(expression)"),
        SqlFunctionInfo::new("JSONB_OBJECT_AGG", FunctionCategory::Aggregate)
            .with_signature("JSONB_OBJECT_AGG(key, value)"),
        // String functions
        SqlFunctionInfo::new("LENGTH", FunctionCategory::String).with_signature("LENGTH(string)"),
        SqlFunctionInfo::new("CHAR_LENGTH", FunctionCategory::String)
            .with_signature("CHAR_LENGTH(string)"),
        SqlFunctionInfo::new("SUBSTRING", FunctionCategory::String)
            .with_signature("SUBSTRING(string FROM start FOR length)"),
        SqlFunctionInfo::new("UPPER", FunctionCategory::String).with_signature("UPPER(string)"),
        SqlFunctionInfo::new("LOWER", FunctionCategory::String).with_signature("LOWER(string)"),
        SqlFunctionInfo::new("INITCAP", FunctionCategory::String).with_signature("INITCAP(string)"),
        SqlFunctionInfo::new("TRIM", FunctionCategory::String)
            .with_signature("TRIM([LEADING|TRAILING|BOTH] chars FROM string)"),
        SqlFunctionInfo::new("LTRIM", FunctionCategory::String)
            .with_signature("LTRIM(string, chars)"),
        SqlFunctionInfo::new("RTRIM", FunctionCategory::String)
            .with_signature("RTRIM(string, chars)"),
        SqlFunctionInfo::new("REPLACE", FunctionCategory::String)
            .with_signature("REPLACE(string, from, to)"),
        SqlFunctionInfo::new("TRANSLATE", FunctionCategory::String)
            .with_signature("TRANSLATE(string, from, to)"),
        SqlFunctionInfo::new("POSITION", FunctionCategory::String)
            .with_signature("POSITION(substring IN string)"),
        SqlFunctionInfo::new("STRPOS", FunctionCategory::String)
            .with_signature("STRPOS(string, substring)"),
        SqlFunctionInfo::new("CONCAT", FunctionCategory::String)
            .with_signature("CONCAT(value1, value2, ...)"),
        SqlFunctionInfo::new("CONCAT_WS", FunctionCategory::String)
            .with_signature("CONCAT_WS(separator, value1, value2, ...)"),
        SqlFunctionInfo::new("FORMAT", FunctionCategory::String)
            .with_signature("FORMAT(formatstr, args...)"),
        SqlFunctionInfo::new("LEFT", FunctionCategory::String).with_signature("LEFT(string, n)"),
        SqlFunctionInfo::new("RIGHT", FunctionCategory::String).with_signature("RIGHT(string, n)"),
        SqlFunctionInfo::new("LPAD", FunctionCategory::String)
            .with_signature("LPAD(string, length, fill)"),
        SqlFunctionInfo::new("RPAD", FunctionCategory::String)
            .with_signature("RPAD(string, length, fill)"),
        SqlFunctionInfo::new("REPEAT", FunctionCategory::String)
            .with_signature("REPEAT(string, n)"),
        SqlFunctionInfo::new("REVERSE", FunctionCategory::String).with_signature("REVERSE(string)"),
        SqlFunctionInfo::new("SPLIT_PART", FunctionCategory::String)
            .with_signature("SPLIT_PART(string, delimiter, n)"),
        SqlFunctionInfo::new("REGEXP_REPLACE", FunctionCategory::String)
            .with_signature("REGEXP_REPLACE(string, pattern, replacement, flags)"),
        SqlFunctionInfo::new("REGEXP_MATCHES", FunctionCategory::String)
            .with_signature("REGEXP_MATCHES(string, pattern, flags)"),
        // Numeric functions
        SqlFunctionInfo::new("ABS", FunctionCategory::Numeric).with_signature("ABS(number)"),
        SqlFunctionInfo::new("ROUND", FunctionCategory::Numeric)
            .with_signature("ROUND(number, decimals)"),
        SqlFunctionInfo::new("TRUNC", FunctionCategory::Numeric)
            .with_signature("TRUNC(number, decimals)"),
        SqlFunctionInfo::new("CEIL", FunctionCategory::Numeric).with_signature("CEIL(number)"),
        SqlFunctionInfo::new("FLOOR", FunctionCategory::Numeric).with_signature("FLOOR(number)"),
        SqlFunctionInfo::new("MOD", FunctionCategory::Numeric).with_signature("MOD(a, b)"),
        SqlFunctionInfo::new("POWER", FunctionCategory::Numeric).with_signature("POWER(a, b)"),
        SqlFunctionInfo::new("SQRT", FunctionCategory::Numeric).with_signature("SQRT(number)"),
        SqlFunctionInfo::new("EXP", FunctionCategory::Numeric).with_signature("EXP(number)"),
        SqlFunctionInfo::new("LN", FunctionCategory::Numeric).with_signature("LN(number)"),
        SqlFunctionInfo::new("LOG", FunctionCategory::Numeric).with_signature("LOG(base, number)"),
        SqlFunctionInfo::new("RANDOM", FunctionCategory::Numeric).with_signature("RANDOM()"),
        SqlFunctionInfo::new("GREATEST", FunctionCategory::Numeric)
            .with_signature("GREATEST(value1, value2, ...)"),
        SqlFunctionInfo::new("LEAST", FunctionCategory::Numeric)
            .with_signature("LEAST(value1, value2, ...)"),
        // Date/Time functions
        SqlFunctionInfo::new("NOW", FunctionCategory::DateTime).with_signature("NOW()"),
        SqlFunctionInfo::new("CURRENT_DATE", FunctionCategory::DateTime)
            .with_signature("CURRENT_DATE"),
        SqlFunctionInfo::new("CURRENT_TIME", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIME"),
        SqlFunctionInfo::new("CURRENT_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIMESTAMP"),
        SqlFunctionInfo::new("LOCALTIMESTAMP", FunctionCategory::DateTime)
            .with_signature("LOCALTIMESTAMP"),
        SqlFunctionInfo::new("DATE_TRUNC", FunctionCategory::DateTime)
            .with_signature("DATE_TRUNC(field, source)"),
        SqlFunctionInfo::new("DATE_PART", FunctionCategory::DateTime)
            .with_signature("DATE_PART(field, source)"),
        SqlFunctionInfo::new("EXTRACT", FunctionCategory::DateTime)
            .with_signature("EXTRACT(field FROM source)"),
        SqlFunctionInfo::new("AGE", FunctionCategory::DateTime)
            .with_signature("AGE(timestamp1, timestamp2)"),
        SqlFunctionInfo::new("TO_CHAR", FunctionCategory::DateTime)
            .with_signature("TO_CHAR(timestamp, format)"),
        SqlFunctionInfo::new("TO_DATE", FunctionCategory::DateTime)
            .with_signature("TO_DATE(string, format)"),
        SqlFunctionInfo::new("TO_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("TO_TIMESTAMP(string, format)"),
        SqlFunctionInfo::new("MAKE_DATE", FunctionCategory::DateTime)
            .with_signature("MAKE_DATE(year, month, day)"),
        SqlFunctionInfo::new("MAKE_TIME", FunctionCategory::DateTime)
            .with_signature("MAKE_TIME(hour, min, sec)"),
        SqlFunctionInfo::new("MAKE_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("MAKE_TIMESTAMP(year, month, day, hour, min, sec)"),
        // Conditional
        SqlFunctionInfo::new("COALESCE", FunctionCategory::Conditional)
            .with_signature("COALESCE(value1, value2, ...)"),
        SqlFunctionInfo::new("NULLIF", FunctionCategory::Conditional)
            .with_signature("NULLIF(value1, value2)"),
        // Type conversion
        SqlFunctionInfo::new("CAST", FunctionCategory::Conversion)
            .with_signature("CAST(expression AS type)"),
        SqlFunctionInfo::new("TO_NUMBER", FunctionCategory::Conversion)
            .with_signature("TO_NUMBER(string, format)"),
        // JSON functions
        SqlFunctionInfo::new("JSON_BUILD_OBJECT", FunctionCategory::Json)
            .with_signature("JSON_BUILD_OBJECT(key1, value1, ...)"),
        SqlFunctionInfo::new("JSON_BUILD_ARRAY", FunctionCategory::Json)
            .with_signature("JSON_BUILD_ARRAY(value1, value2, ...)"),
        SqlFunctionInfo::new("JSON_EXTRACT_PATH", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT_PATH(json, path...)"),
        SqlFunctionInfo::new("JSON_EXTRACT_PATH_TEXT", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT_PATH_TEXT(json, path...)"),
        SqlFunctionInfo::new("JSONB_SET", FunctionCategory::Json)
            .with_signature("JSONB_SET(target, path, new_value, create_missing)"),
        SqlFunctionInfo::new("JSONB_INSERT", FunctionCategory::Json)
            .with_signature("JSONB_INSERT(target, path, new_value, insert_after)"),
        SqlFunctionInfo::new("JSONB_PRETTY", FunctionCategory::Json)
            .with_signature("JSONB_PRETTY(jsonb)"),
        SqlFunctionInfo::new("TO_JSON", FunctionCategory::Json).with_signature("TO_JSON(value)"),
        SqlFunctionInfo::new("TO_JSONB", FunctionCategory::Json).with_signature("TO_JSONB(value)"),
        // Array functions
        SqlFunctionInfo::new("ARRAY_LENGTH", FunctionCategory::Array)
            .with_signature("ARRAY_LENGTH(array, dimension)"),
        SqlFunctionInfo::new("ARRAY_APPEND", FunctionCategory::Array)
            .with_signature("ARRAY_APPEND(array, element)"),
        SqlFunctionInfo::new("ARRAY_PREPEND", FunctionCategory::Array)
            .with_signature("ARRAY_PREPEND(element, array)"),
        SqlFunctionInfo::new("ARRAY_CAT", FunctionCategory::Array)
            .with_signature("ARRAY_CAT(array1, array2)"),
        SqlFunctionInfo::new("ARRAY_REMOVE", FunctionCategory::Array)
            .with_signature("ARRAY_REMOVE(array, element)"),
        SqlFunctionInfo::new("ARRAY_POSITION", FunctionCategory::Array)
            .with_signature("ARRAY_POSITION(array, element)"),
        SqlFunctionInfo::new("UNNEST", FunctionCategory::Array).with_signature("UNNEST(array)"),
        // Window functions
        SqlFunctionInfo::new("ROW_NUMBER", FunctionCategory::Window)
            .with_signature("ROW_NUMBER() OVER(...)"),
        SqlFunctionInfo::new("RANK", FunctionCategory::Window).with_signature("RANK() OVER(...)"),
        SqlFunctionInfo::new("DENSE_RANK", FunctionCategory::Window)
            .with_signature("DENSE_RANK() OVER(...)"),
        SqlFunctionInfo::new("PERCENT_RANK", FunctionCategory::Window)
            .with_signature("PERCENT_RANK() OVER(...)"),
        SqlFunctionInfo::new("CUME_DIST", FunctionCategory::Window)
            .with_signature("CUME_DIST() OVER(...)"),
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
        // System functions
        SqlFunctionInfo::new("CURRENT_USER", FunctionCategory::Other)
            .with_signature("CURRENT_USER"),
        SqlFunctionInfo::new("CURRENT_SCHEMA", FunctionCategory::Other)
            .with_signature("CURRENT_SCHEMA"),
        SqlFunctionInfo::new("CURRENT_DATABASE", FunctionCategory::Other)
            .with_signature("CURRENT_DATABASE()"),
        SqlFunctionInfo::new("VERSION", FunctionCategory::Other).with_signature("VERSION()"),
        SqlFunctionInfo::new("PG_TYPEOF", FunctionCategory::Other)
            .with_signature("PG_TYPEOF(expression)"),
        SqlFunctionInfo::new("GEN_RANDOM_UUID", FunctionCategory::Other)
            .with_signature("GEN_RANDOM_UUID()"),
    ]
}

fn postgres_data_types() -> Vec<DataTypeInfo> {
    vec![
        // Integer types
        DataTypeInfo::new("SMALLINT", DataTypeCategory::Integer),
        DataTypeInfo::new("INTEGER", DataTypeCategory::Integer),
        DataTypeInfo::new("INT", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGINT", DataTypeCategory::Integer),
        DataTypeInfo::new("SMALLSERIAL", DataTypeCategory::Integer),
        DataTypeInfo::new("SERIAL", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGSERIAL", DataTypeCategory::Integer),
        // Floating point
        DataTypeInfo::new("REAL", DataTypeCategory::Float),
        DataTypeInfo::new("DOUBLE PRECISION", DataTypeCategory::Float),
        DataTypeInfo::new("FLOAT", DataTypeCategory::Float),
        // Fixed precision
        DataTypeInfo::new("NUMERIC", DataTypeCategory::Decimal).with_length(None, None),
        DataTypeInfo::new("DECIMAL", DataTypeCategory::Decimal).with_length(None, None),
        DataTypeInfo::new("MONEY", DataTypeCategory::Decimal),
        // String types
        DataTypeInfo::new("VARCHAR", DataTypeCategory::String)
            .with_length(Some(255), Some(10485760)),
        DataTypeInfo::new("CHARACTER VARYING", DataTypeCategory::String)
            .with_length(Some(255), Some(10485760)),
        DataTypeInfo::new("CHAR", DataTypeCategory::String).with_length(Some(1), Some(10485760)),
        DataTypeInfo::new("CHARACTER", DataTypeCategory::String)
            .with_length(Some(1), Some(10485760)),
        DataTypeInfo::new("TEXT", DataTypeCategory::String),
        DataTypeInfo::new("NAME", DataTypeCategory::String),
        // Binary
        DataTypeInfo::new("BYTEA", DataTypeCategory::Binary),
        // Boolean
        DataTypeInfo::new("BOOLEAN", DataTypeCategory::Boolean),
        DataTypeInfo::new("BOOL", DataTypeCategory::Boolean),
        // Date/Time
        DataTypeInfo::new("DATE", DataTypeCategory::Date),
        DataTypeInfo::new("TIME", DataTypeCategory::Time),
        DataTypeInfo::new("TIME WITH TIME ZONE", DataTypeCategory::Time),
        DataTypeInfo::new("TIMETZ", DataTypeCategory::Time),
        DataTypeInfo::new("TIMESTAMP", DataTypeCategory::DateTime),
        DataTypeInfo::new("TIMESTAMP WITH TIME ZONE", DataTypeCategory::DateTime),
        DataTypeInfo::new("TIMESTAMPTZ", DataTypeCategory::DateTime),
        DataTypeInfo::new("INTERVAL", DataTypeCategory::Interval),
        // JSON
        DataTypeInfo::new("JSON", DataTypeCategory::Json),
        DataTypeInfo::new("JSONB", DataTypeCategory::Json),
        // UUID
        DataTypeInfo::new("UUID", DataTypeCategory::Uuid),
        // Network
        DataTypeInfo::new("INET", DataTypeCategory::Network),
        DataTypeInfo::new("CIDR", DataTypeCategory::Network),
        DataTypeInfo::new("MACADDR", DataTypeCategory::Network),
        DataTypeInfo::new("MACADDR8", DataTypeCategory::Network),
        // Arrays (use type[] syntax)
        DataTypeInfo::new("ARRAY", DataTypeCategory::Array),
        // Other PostgreSQL types
        DataTypeInfo::new("XML", DataTypeCategory::Other),
        DataTypeInfo::new("TSQUERY", DataTypeCategory::Other),
        DataTypeInfo::new("TSVECTOR", DataTypeCategory::Other),
        DataTypeInfo::new("POINT", DataTypeCategory::Geometry),
        DataTypeInfo::new("LINE", DataTypeCategory::Geometry),
        DataTypeInfo::new("LSEG", DataTypeCategory::Geometry),
        DataTypeInfo::new("BOX", DataTypeCategory::Geometry),
        DataTypeInfo::new("PATH", DataTypeCategory::Geometry),
        DataTypeInfo::new("POLYGON", DataTypeCategory::Geometry),
        DataTypeInfo::new("CIRCLE", DataTypeCategory::Geometry),
        DataTypeInfo::new("INT4RANGE", DataTypeCategory::Other),
        DataTypeInfo::new("INT8RANGE", DataTypeCategory::Other),
        DataTypeInfo::new("NUMRANGE", DataTypeCategory::Other),
        DataTypeInfo::new("TSRANGE", DataTypeCategory::Other),
        DataTypeInfo::new("TSTZRANGE", DataTypeCategory::Other),
        DataTypeInfo::new("DATERANGE", DataTypeCategory::Other),
    ]
}

fn postgres_table_options() -> Vec<TableOptionDef> {
    vec![
        TableOptionDef {
            key: Cow::Borrowed("unlogged"),
            label: Cow::Borrowed("UNLOGGED"),
            option_type: TableOptionType::Boolean,
            default_value: Some(Cow::Borrowed("false")),
            description: Some(Cow::Borrowed(
                "Create an unlogged table (faster writes, no WAL, not crash-safe)",
            )),
            choices: Vec::new(),
        },
        TableOptionDef {
            key: Cow::Borrowed("tablespace"),
            label: Cow::Borrowed("Tablespace"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Tablespace to store the table in")),
            choices: Vec::new(),
        },
    ]
}
