//! MySQL dialect information
//!
//! Provides comprehensive metadata about the MySQL SQL dialect.

use std::borrow::Cow;
use zqlz_core::{
    AutoIncrementInfo, AutoIncrementStyle, CommentStyles, DataTypeCategory, DataTypeInfo,
    DialectInfo, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo, SqlFunctionInfo,
    TableOptionDef, TableOptionType,
};

/// Build the complete MySQL dialect info
pub fn mysql_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("mysql"),
        display_name: Cow::Borrowed("MySQL"),
        keywords: mysql_keywords(),
        functions: mysql_functions(),
        data_types: mysql_data_types(),
        table_options: mysql_table_options(),
        auto_increment: Some(AutoIncrementInfo {
            keyword: Cow::Borrowed("AUTO_INCREMENT"),
            style: AutoIncrementStyle::Suffix,
            description: Some(Cow::Borrowed(
                "Add AUTO_INCREMENT after column definition for auto-incrementing values",
            )),
        }),
        identifier_quote: '`',
        string_quote: '\'',
        case_sensitive_identifiers: false,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: ExplainConfig::mysql(),
    }
}

fn mysql_keywords() -> Vec<KeywordInfo> {
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
        KeywordInfo::with_desc("FOR UPDATE", KeywordCategory::Dql, "Lock selected rows"),
        KeywordInfo::with_desc(
            "FOR SHARE",
            KeywordCategory::Dql,
            "Share lock selected rows",
        ),
        KeywordInfo::with_desc(
            "LOCK IN SHARE MODE",
            KeywordCategory::Dql,
            "Legacy share lock",
        ),
        // DML
        KeywordInfo::with_desc("INSERT", KeywordCategory::Dml, "Insert rows"),
        KeywordInfo::with_desc(
            "INSERT IGNORE",
            KeywordCategory::Dml,
            "Insert ignoring errors",
        ),
        KeywordInfo::with_desc("UPDATE", KeywordCategory::Dml, "Update rows"),
        KeywordInfo::with_desc("DELETE", KeywordCategory::Dml, "Delete rows"),
        KeywordInfo::with_desc("REPLACE", KeywordCategory::Dml, "Replace rows"),
        KeywordInfo::with_desc("TRUNCATE", KeywordCategory::Dml, "Remove all rows quickly"),
        KeywordInfo::with_desc("LOAD DATA", KeywordCategory::Dml, "Bulk data import"),
        // DDL
        KeywordInfo::with_desc("CREATE", KeywordCategory::Ddl, "Create database objects"),
        KeywordInfo::with_desc("ALTER", KeywordCategory::Ddl, "Modify database objects"),
        KeywordInfo::with_desc("DROP", KeywordCategory::Ddl, "Remove database objects"),
        KeywordInfo::with_desc("TABLE", KeywordCategory::Ddl, "Table object type"),
        KeywordInfo::with_desc("INDEX", KeywordCategory::Ddl, "Index object type"),
        KeywordInfo::with_desc("VIEW", KeywordCategory::Ddl, "View object type"),
        KeywordInfo::with_desc("DATABASE", KeywordCategory::Ddl, "Database object type"),
        KeywordInfo::with_desc(
            "SCHEMA",
            KeywordCategory::Ddl,
            "Schema (alias for DATABASE)",
        ),
        KeywordInfo::with_desc("FUNCTION", KeywordCategory::Ddl, "Function"),
        KeywordInfo::with_desc("PROCEDURE", KeywordCategory::Ddl, "Stored procedure"),
        KeywordInfo::with_desc("TRIGGER", KeywordCategory::Ddl, "Trigger"),
        KeywordInfo::with_desc("EVENT", KeywordCategory::Ddl, "Scheduled event"),
        // DCL
        KeywordInfo::with_desc("GRANT", KeywordCategory::Dcl, "Grant privileges"),
        KeywordInfo::with_desc("REVOKE", KeywordCategory::Dcl, "Revoke privileges"),
        KeywordInfo::with_desc("FLUSH", KeywordCategory::Dcl, "Flush privileges/caches"),
        // Transaction
        KeywordInfo::with_desc(
            "START TRANSACTION",
            KeywordCategory::Transaction,
            "Start transaction",
        ),
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
        KeywordInfo::with_desc(
            "RELEASE SAVEPOINT",
            KeywordCategory::Transaction,
            "Release savepoint",
        ),
        KeywordInfo::with_desc(
            "ROLLBACK TO SAVEPOINT",
            KeywordCategory::Transaction,
            "Rollback to savepoint",
        ),
        // Clauses
        KeywordInfo::with_desc("JOIN", KeywordCategory::Clause, "Join tables"),
        KeywordInfo::with_desc("INNER JOIN", KeywordCategory::Clause, "Inner join"),
        KeywordInfo::with_desc("LEFT JOIN", KeywordCategory::Clause, "Left outer join"),
        KeywordInfo::with_desc("RIGHT JOIN", KeywordCategory::Clause, "Right outer join"),
        KeywordInfo::with_desc("CROSS JOIN", KeywordCategory::Clause, "Cross join"),
        KeywordInfo::with_desc("NATURAL JOIN", KeywordCategory::Clause, "Natural join"),
        KeywordInfo::with_desc("STRAIGHT_JOIN", KeywordCategory::Clause, "Force join order"),
        KeywordInfo::with_desc("ON", KeywordCategory::Clause, "Join condition"),
        KeywordInfo::with_desc("USING", KeywordCategory::Clause, "Join using columns"),
        KeywordInfo::with_desc("AS", KeywordCategory::Clause, "Alias"),
        KeywordInfo::with_desc("UNION", KeywordCategory::Clause, "Combine results"),
        KeywordInfo::with_desc("UNION ALL", KeywordCategory::Clause, "Combine all results"),
        KeywordInfo::with_desc("EXCEPT", KeywordCategory::Clause, "Subtract results"),
        KeywordInfo::with_desc("INTERSECT", KeywordCategory::Clause, "Intersect results"),
        KeywordInfo::with_desc("WITH", KeywordCategory::Clause, "Common Table Expression"),
        KeywordInfo::with_desc("RECURSIVE", KeywordCategory::Clause, "Recursive CTE"),
        KeywordInfo::with_desc("VALUES", KeywordCategory::Clause, "Values clause"),
        KeywordInfo::with_desc(
            "ON DUPLICATE KEY UPDATE",
            KeywordCategory::Clause,
            "Upsert on duplicate",
        ),
        KeywordInfo::with_desc("WINDOW", KeywordCategory::Clause, "Window definition"),
        // Operators
        KeywordInfo::with_desc("AND", KeywordCategory::Operator, "Logical AND"),
        KeywordInfo::with_desc("OR", KeywordCategory::Operator, "Logical OR"),
        KeywordInfo::with_desc("NOT", KeywordCategory::Operator, "Logical NOT"),
        KeywordInfo::with_desc("XOR", KeywordCategory::Operator, "Logical XOR"),
        KeywordInfo::with_desc("IN", KeywordCategory::Operator, "In list/subquery"),
        KeywordInfo::with_desc("LIKE", KeywordCategory::Operator, "Pattern matching"),
        KeywordInfo::with_desc("RLIKE", KeywordCategory::Operator, "Regex pattern matching"),
        KeywordInfo::with_desc(
            "REGEXP",
            KeywordCategory::Operator,
            "Regex pattern matching",
        ),
        KeywordInfo::with_desc("BETWEEN", KeywordCategory::Operator, "Range check"),
        KeywordInfo::with_desc("IS", KeywordCategory::Operator, "Identity comparison"),
        KeywordInfo::with_desc("IS NULL", KeywordCategory::Operator, "Null check"),
        KeywordInfo::with_desc("IS NOT NULL", KeywordCategory::Operator, "Not null check"),
        KeywordInfo::with_desc("NULL", KeywordCategory::Operator, "Null value"),
        KeywordInfo::with_desc("EXISTS", KeywordCategory::Operator, "Subquery existence"),
        KeywordInfo::with_desc("ANY", KeywordCategory::Operator, "Subquery any"),
        KeywordInfo::with_desc("ALL", KeywordCategory::Operator, "Subquery all"),
        KeywordInfo::with_desc("SOME", KeywordCategory::Operator, "Alias for ANY"),
        KeywordInfo::with_desc("CASE", KeywordCategory::Operator, "Conditional expression"),
        KeywordInfo::with_desc("WHEN", KeywordCategory::Operator, "Case condition"),
        KeywordInfo::with_desc("THEN", KeywordCategory::Operator, "Case result"),
        KeywordInfo::with_desc("ELSE", KeywordCategory::Operator, "Case default"),
        KeywordInfo::with_desc("END", KeywordCategory::Operator, "End case/block"),
        KeywordInfo::with_desc("DIV", KeywordCategory::Operator, "Integer division"),
        KeywordInfo::with_desc("MOD", KeywordCategory::Operator, "Modulo"),
        // MySQL-specific
        KeywordInfo::with_desc(
            "SHOW",
            KeywordCategory::DatabaseSpecific,
            "Show database info",
        ),
        KeywordInfo::with_desc(
            "SHOW DATABASES",
            KeywordCategory::DatabaseSpecific,
            "List databases",
        ),
        KeywordInfo::with_desc(
            "SHOW TABLES",
            KeywordCategory::DatabaseSpecific,
            "List tables",
        ),
        KeywordInfo::with_desc(
            "SHOW COLUMNS",
            KeywordCategory::DatabaseSpecific,
            "Show table columns",
        ),
        KeywordInfo::with_desc(
            "SHOW INDEX",
            KeywordCategory::DatabaseSpecific,
            "Show table indexes",
        ),
        KeywordInfo::with_desc(
            "SHOW CREATE TABLE",
            KeywordCategory::DatabaseSpecific,
            "Show table DDL",
        ),
        KeywordInfo::with_desc(
            "SHOW PROCESSLIST",
            KeywordCategory::DatabaseSpecific,
            "Show running queries",
        ),
        KeywordInfo::with_desc(
            "SHOW STATUS",
            KeywordCategory::DatabaseSpecific,
            "Show server status",
        ),
        KeywordInfo::with_desc(
            "SHOW VARIABLES",
            KeywordCategory::DatabaseSpecific,
            "Show server variables",
        ),
        KeywordInfo::with_desc(
            "DESCRIBE",
            KeywordCategory::DatabaseSpecific,
            "Describe table structure",
        ),
        KeywordInfo::with_desc("DESC", KeywordCategory::DatabaseSpecific, "Describe table"),
        KeywordInfo::with_desc(
            "EXPLAIN",
            KeywordCategory::DatabaseSpecific,
            "Show query plan",
        ),
        KeywordInfo::with_desc(
            "EXPLAIN ANALYZE",
            KeywordCategory::DatabaseSpecific,
            "Execute and show plan",
        ),
        KeywordInfo::with_desc("USE", KeywordCategory::DatabaseSpecific, "Select database"),
        KeywordInfo::with_desc(
            "SET",
            KeywordCategory::DatabaseSpecific,
            "Set session variable",
        ),
        KeywordInfo::with_desc(
            "ANALYZE TABLE",
            KeywordCategory::DatabaseSpecific,
            "Update statistics",
        ),
        KeywordInfo::with_desc(
            "OPTIMIZE TABLE",
            KeywordCategory::DatabaseSpecific,
            "Optimize table storage",
        ),
        KeywordInfo::with_desc(
            "REPAIR TABLE",
            KeywordCategory::DatabaseSpecific,
            "Repair corrupted table",
        ),
        KeywordInfo::with_desc(
            "CHECK TABLE",
            KeywordCategory::DatabaseSpecific,
            "Check table integrity",
        ),
        KeywordInfo::with_desc(
            "KILL",
            KeywordCategory::DatabaseSpecific,
            "Kill a connection",
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
        KeywordInfo::with_desc("NOT NULL", KeywordCategory::Ddl, "Not null constraint"),
        KeywordInfo::with_desc(
            "AUTO_INCREMENT",
            KeywordCategory::Ddl,
            "Auto-increment column",
        ),
        KeywordInfo::with_desc("GENERATED", KeywordCategory::Ddl, "Generated column"),
        KeywordInfo::with_desc(
            "ON DELETE",
            KeywordCategory::Ddl,
            "Foreign key delete action",
        ),
        KeywordInfo::with_desc(
            "ON UPDATE",
            KeywordCategory::Ddl,
            "Foreign key update action",
        ),
        KeywordInfo::with_desc("CASCADE", KeywordCategory::Ddl, "Cascade action"),
        KeywordInfo::with_desc("SET NULL", KeywordCategory::Ddl, "Set null action"),
        KeywordInfo::with_desc("RESTRICT", KeywordCategory::Ddl, "Restrict action"),
        KeywordInfo::with_desc("NO ACTION", KeywordCategory::Ddl, "No action"),
        // Table options
        KeywordInfo::with_desc(
            "ENGINE",
            KeywordCategory::DatabaseSpecific,
            "Storage engine",
        ),
        KeywordInfo::with_desc(
            "CHARSET",
            KeywordCategory::DatabaseSpecific,
            "Character set",
        ),
        KeywordInfo::with_desc("COLLATE", KeywordCategory::DatabaseSpecific, "Collation"),
        KeywordInfo::with_desc(
            "COMMENT",
            KeywordCategory::DatabaseSpecific,
            "Table/column comment",
        ),
        KeywordInfo::with_desc(
            "TEMPORARY",
            KeywordCategory::DatabaseSpecific,
            "Temporary table",
        ),
        KeywordInfo::with_desc("IF EXISTS", KeywordCategory::Ddl, "Conditional drop"),
        KeywordInfo::with_desc("IF NOT EXISTS", KeywordCategory::Ddl, "Conditional create"),
    ]
}

fn mysql_functions() -> Vec<SqlFunctionInfo> {
    vec![
        // Aggregate functions
        SqlFunctionInfo::new("COUNT", FunctionCategory::Aggregate)
            .with_signature("COUNT(*) or COUNT(expression)"),
        SqlFunctionInfo::new("SUM", FunctionCategory::Aggregate).with_signature("SUM(expression)"),
        SqlFunctionInfo::new("AVG", FunctionCategory::Aggregate).with_signature("AVG(expression)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Aggregate).with_signature("MIN(expression)"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Aggregate).with_signature("MAX(expression)"),
        SqlFunctionInfo::new("GROUP_CONCAT", FunctionCategory::Aggregate)
            .with_signature("GROUP_CONCAT(expression ORDER BY ... SEPARATOR ',')"),
        SqlFunctionInfo::new("JSON_ARRAYAGG", FunctionCategory::Aggregate)
            .with_signature("JSON_ARRAYAGG(expression)"),
        SqlFunctionInfo::new("JSON_OBJECTAGG", FunctionCategory::Aggregate)
            .with_signature("JSON_OBJECTAGG(key, value)"),
        SqlFunctionInfo::new("BIT_AND", FunctionCategory::Aggregate)
            .with_signature("BIT_AND(expression)"),
        SqlFunctionInfo::new("BIT_OR", FunctionCategory::Aggregate)
            .with_signature("BIT_OR(expression)"),
        SqlFunctionInfo::new("BIT_XOR", FunctionCategory::Aggregate)
            .with_signature("BIT_XOR(expression)"),
        SqlFunctionInfo::new("STD", FunctionCategory::Aggregate).with_signature("STD(expression)"),
        SqlFunctionInfo::new("STDDEV", FunctionCategory::Aggregate)
            .with_signature("STDDEV(expression)"),
        SqlFunctionInfo::new("VARIANCE", FunctionCategory::Aggregate)
            .with_signature("VARIANCE(expression)"),
        // String functions
        SqlFunctionInfo::new("LENGTH", FunctionCategory::String).with_signature("LENGTH(string)"),
        SqlFunctionInfo::new("CHAR_LENGTH", FunctionCategory::String)
            .with_signature("CHAR_LENGTH(string)"),
        SqlFunctionInfo::new("CHARACTER_LENGTH", FunctionCategory::String)
            .with_signature("CHARACTER_LENGTH(string)"),
        SqlFunctionInfo::new("SUBSTRING", FunctionCategory::String)
            .with_signature("SUBSTRING(string, pos, len)"),
        SqlFunctionInfo::new("SUBSTR", FunctionCategory::String)
            .with_signature("SUBSTR(string, pos, len)"),
        SqlFunctionInfo::new("UPPER", FunctionCategory::String).with_signature("UPPER(string)"),
        SqlFunctionInfo::new("LOWER", FunctionCategory::String).with_signature("LOWER(string)"),
        SqlFunctionInfo::new("UCASE", FunctionCategory::String).with_signature("UCASE(string)"),
        SqlFunctionInfo::new("LCASE", FunctionCategory::String).with_signature("LCASE(string)"),
        SqlFunctionInfo::new("TRIM", FunctionCategory::String)
            .with_signature("TRIM([LEADING|TRAILING|BOTH] chars FROM string)"),
        SqlFunctionInfo::new("LTRIM", FunctionCategory::String).with_signature("LTRIM(string)"),
        SqlFunctionInfo::new("RTRIM", FunctionCategory::String).with_signature("RTRIM(string)"),
        SqlFunctionInfo::new("REPLACE", FunctionCategory::String)
            .with_signature("REPLACE(string, from, to)"),
        SqlFunctionInfo::new("LOCATE", FunctionCategory::String)
            .with_signature("LOCATE(substring, string, start)"),
        SqlFunctionInfo::new("INSTR", FunctionCategory::String)
            .with_signature("INSTR(string, substring)"),
        SqlFunctionInfo::new("POSITION", FunctionCategory::String)
            .with_signature("POSITION(substring IN string)"),
        SqlFunctionInfo::new("CONCAT", FunctionCategory::String)
            .with_signature("CONCAT(value1, value2, ...)"),
        SqlFunctionInfo::new("CONCAT_WS", FunctionCategory::String)
            .with_signature("CONCAT_WS(separator, value1, value2, ...)"),
        SqlFunctionInfo::new("FORMAT", FunctionCategory::String)
            .with_signature("FORMAT(number, decimals, locale)"),
        SqlFunctionInfo::new("LEFT", FunctionCategory::String).with_signature("LEFT(string, n)"),
        SqlFunctionInfo::new("RIGHT", FunctionCategory::String).with_signature("RIGHT(string, n)"),
        SqlFunctionInfo::new("LPAD", FunctionCategory::String)
            .with_signature("LPAD(string, length, pad)"),
        SqlFunctionInfo::new("RPAD", FunctionCategory::String)
            .with_signature("RPAD(string, length, pad)"),
        SqlFunctionInfo::new("REPEAT", FunctionCategory::String)
            .with_signature("REPEAT(string, count)"),
        SqlFunctionInfo::new("REVERSE", FunctionCategory::String).with_signature("REVERSE(string)"),
        SqlFunctionInfo::new("SPACE", FunctionCategory::String).with_signature("SPACE(n)"),
        SqlFunctionInfo::new("SUBSTRING_INDEX", FunctionCategory::String)
            .with_signature("SUBSTRING_INDEX(string, delimiter, count)"),
        SqlFunctionInfo::new("REGEXP_REPLACE", FunctionCategory::String)
            .with_signature("REGEXP_REPLACE(string, pattern, replacement)"),
        SqlFunctionInfo::new("REGEXP_SUBSTR", FunctionCategory::String)
            .with_signature("REGEXP_SUBSTR(string, pattern)"),
        SqlFunctionInfo::new("REGEXP_INSTR", FunctionCategory::String)
            .with_signature("REGEXP_INSTR(string, pattern)"),
        SqlFunctionInfo::new("REGEXP_LIKE", FunctionCategory::String)
            .with_signature("REGEXP_LIKE(string, pattern)"),
        SqlFunctionInfo::new("FIELD", FunctionCategory::String)
            .with_signature("FIELD(value, val1, val2, ...)"),
        SqlFunctionInfo::new("FIND_IN_SET", FunctionCategory::String)
            .with_signature("FIND_IN_SET(string, string_list)"),
        SqlFunctionInfo::new("ELT", FunctionCategory::String)
            .with_signature("ELT(n, str1, str2, ...)"),
        SqlFunctionInfo::new("ASCII", FunctionCategory::String).with_signature("ASCII(string)"),
        SqlFunctionInfo::new("CHAR", FunctionCategory::String).with_signature("CHAR(n1, n2, ...)"),
        SqlFunctionInfo::new("ORD", FunctionCategory::String).with_signature("ORD(string)"),
        SqlFunctionInfo::new("HEX", FunctionCategory::String).with_signature("HEX(value)"),
        SqlFunctionInfo::new("UNHEX", FunctionCategory::String).with_signature("UNHEX(string)"),
        // Numeric functions
        SqlFunctionInfo::new("ABS", FunctionCategory::Numeric).with_signature("ABS(number)"),
        SqlFunctionInfo::new("ROUND", FunctionCategory::Numeric)
            .with_signature("ROUND(number, decimals)"),
        SqlFunctionInfo::new("TRUNCATE", FunctionCategory::Numeric)
            .with_signature("TRUNCATE(number, decimals)"),
        SqlFunctionInfo::new("CEIL", FunctionCategory::Numeric).with_signature("CEIL(number)"),
        SqlFunctionInfo::new("CEILING", FunctionCategory::Numeric)
            .with_signature("CEILING(number)"),
        SqlFunctionInfo::new("FLOOR", FunctionCategory::Numeric).with_signature("FLOOR(number)"),
        SqlFunctionInfo::new("MOD", FunctionCategory::Numeric).with_signature("MOD(a, b)"),
        SqlFunctionInfo::new("POW", FunctionCategory::Numeric).with_signature("POW(base, exp)"),
        SqlFunctionInfo::new("POWER", FunctionCategory::Numeric).with_signature("POWER(base, exp)"),
        SqlFunctionInfo::new("SQRT", FunctionCategory::Numeric).with_signature("SQRT(number)"),
        SqlFunctionInfo::new("EXP", FunctionCategory::Numeric).with_signature("EXP(number)"),
        SqlFunctionInfo::new("LN", FunctionCategory::Numeric).with_signature("LN(number)"),
        SqlFunctionInfo::new("LOG", FunctionCategory::Numeric).with_signature("LOG(base, number)"),
        SqlFunctionInfo::new("LOG10", FunctionCategory::Numeric).with_signature("LOG10(number)"),
        SqlFunctionInfo::new("LOG2", FunctionCategory::Numeric).with_signature("LOG2(number)"),
        SqlFunctionInfo::new("RAND", FunctionCategory::Numeric).with_signature("RAND(seed)"),
        SqlFunctionInfo::new("SIGN", FunctionCategory::Numeric).with_signature("SIGN(number)"),
        SqlFunctionInfo::new("GREATEST", FunctionCategory::Numeric)
            .with_signature("GREATEST(value1, value2, ...)"),
        SqlFunctionInfo::new("LEAST", FunctionCategory::Numeric)
            .with_signature("LEAST(value1, value2, ...)"),
        SqlFunctionInfo::new("PI", FunctionCategory::Numeric).with_signature("PI()"),
        SqlFunctionInfo::new("SIN", FunctionCategory::Numeric).with_signature("SIN(number)"),
        SqlFunctionInfo::new("COS", FunctionCategory::Numeric).with_signature("COS(number)"),
        SqlFunctionInfo::new("TAN", FunctionCategory::Numeric).with_signature("TAN(number)"),
        SqlFunctionInfo::new("ASIN", FunctionCategory::Numeric).with_signature("ASIN(number)"),
        SqlFunctionInfo::new("ACOS", FunctionCategory::Numeric).with_signature("ACOS(number)"),
        SqlFunctionInfo::new("ATAN", FunctionCategory::Numeric).with_signature("ATAN(number)"),
        SqlFunctionInfo::new("ATAN2", FunctionCategory::Numeric).with_signature("ATAN2(y, x)"),
        SqlFunctionInfo::new("DEGREES", FunctionCategory::Numeric)
            .with_signature("DEGREES(radians)"),
        SqlFunctionInfo::new("RADIANS", FunctionCategory::Numeric)
            .with_signature("RADIANS(degrees)"),
        // Date/Time functions
        SqlFunctionInfo::new("NOW", FunctionCategory::DateTime).with_signature("NOW()"),
        SqlFunctionInfo::new("CURDATE", FunctionCategory::DateTime).with_signature("CURDATE()"),
        SqlFunctionInfo::new("CURRENT_DATE", FunctionCategory::DateTime)
            .with_signature("CURRENT_DATE()"),
        SqlFunctionInfo::new("CURTIME", FunctionCategory::DateTime).with_signature("CURTIME()"),
        SqlFunctionInfo::new("CURRENT_TIME", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIME()"),
        SqlFunctionInfo::new("CURRENT_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("CURRENT_TIMESTAMP()"),
        SqlFunctionInfo::new("SYSDATE", FunctionCategory::DateTime).with_signature("SYSDATE()"),
        SqlFunctionInfo::new("UTC_DATE", FunctionCategory::DateTime).with_signature("UTC_DATE()"),
        SqlFunctionInfo::new("UTC_TIME", FunctionCategory::DateTime).with_signature("UTC_TIME()"),
        SqlFunctionInfo::new("UTC_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("UTC_TIMESTAMP()"),
        SqlFunctionInfo::new("DATE", FunctionCategory::DateTime).with_signature("DATE(datetime)"),
        SqlFunctionInfo::new("TIME", FunctionCategory::DateTime).with_signature("TIME(datetime)"),
        SqlFunctionInfo::new("YEAR", FunctionCategory::DateTime).with_signature("YEAR(date)"),
        SqlFunctionInfo::new("MONTH", FunctionCategory::DateTime).with_signature("MONTH(date)"),
        SqlFunctionInfo::new("DAY", FunctionCategory::DateTime).with_signature("DAY(date)"),
        SqlFunctionInfo::new("DAYOFMONTH", FunctionCategory::DateTime)
            .with_signature("DAYOFMONTH(date)"),
        SqlFunctionInfo::new("DAYOFWEEK", FunctionCategory::DateTime)
            .with_signature("DAYOFWEEK(date)"),
        SqlFunctionInfo::new("DAYOFYEAR", FunctionCategory::DateTime)
            .with_signature("DAYOFYEAR(date)"),
        SqlFunctionInfo::new("DAYNAME", FunctionCategory::DateTime).with_signature("DAYNAME(date)"),
        SqlFunctionInfo::new("MONTHNAME", FunctionCategory::DateTime)
            .with_signature("MONTHNAME(date)"),
        SqlFunctionInfo::new("HOUR", FunctionCategory::DateTime).with_signature("HOUR(time)"),
        SqlFunctionInfo::new("MINUTE", FunctionCategory::DateTime).with_signature("MINUTE(time)"),
        SqlFunctionInfo::new("SECOND", FunctionCategory::DateTime).with_signature("SECOND(time)"),
        SqlFunctionInfo::new("MICROSECOND", FunctionCategory::DateTime)
            .with_signature("MICROSECOND(time)"),
        SqlFunctionInfo::new("QUARTER", FunctionCategory::DateTime).with_signature("QUARTER(date)"),
        SqlFunctionInfo::new("WEEK", FunctionCategory::DateTime).with_signature("WEEK(date, mode)"),
        SqlFunctionInfo::new("WEEKDAY", FunctionCategory::DateTime).with_signature("WEEKDAY(date)"),
        SqlFunctionInfo::new("WEEKOFYEAR", FunctionCategory::DateTime)
            .with_signature("WEEKOFYEAR(date)"),
        SqlFunctionInfo::new("EXTRACT", FunctionCategory::DateTime)
            .with_signature("EXTRACT(unit FROM datetime)"),
        SqlFunctionInfo::new("DATE_FORMAT", FunctionCategory::DateTime)
            .with_signature("DATE_FORMAT(date, format)"),
        SqlFunctionInfo::new("TIME_FORMAT", FunctionCategory::DateTime)
            .with_signature("TIME_FORMAT(time, format)"),
        SqlFunctionInfo::new("STR_TO_DATE", FunctionCategory::DateTime)
            .with_signature("STR_TO_DATE(string, format)"),
        SqlFunctionInfo::new("DATE_ADD", FunctionCategory::DateTime)
            .with_signature("DATE_ADD(date, INTERVAL expr unit)"),
        SqlFunctionInfo::new("DATE_SUB", FunctionCategory::DateTime)
            .with_signature("DATE_SUB(date, INTERVAL expr unit)"),
        SqlFunctionInfo::new("ADDDATE", FunctionCategory::DateTime)
            .with_signature("ADDDATE(date, INTERVAL expr unit)"),
        SqlFunctionInfo::new("SUBDATE", FunctionCategory::DateTime)
            .with_signature("SUBDATE(date, INTERVAL expr unit)"),
        SqlFunctionInfo::new("DATEDIFF", FunctionCategory::DateTime)
            .with_signature("DATEDIFF(date1, date2)"),
        SqlFunctionInfo::new("TIMEDIFF", FunctionCategory::DateTime)
            .with_signature("TIMEDIFF(time1, time2)"),
        SqlFunctionInfo::new("TIMESTAMPDIFF", FunctionCategory::DateTime)
            .with_signature("TIMESTAMPDIFF(unit, datetime1, datetime2)"),
        SqlFunctionInfo::new("TIMESTAMPADD", FunctionCategory::DateTime)
            .with_signature("TIMESTAMPADD(unit, interval, datetime)"),
        SqlFunctionInfo::new("FROM_UNIXTIME", FunctionCategory::DateTime)
            .with_signature("FROM_UNIXTIME(unix_timestamp, format)"),
        SqlFunctionInfo::new("UNIX_TIMESTAMP", FunctionCategory::DateTime)
            .with_signature("UNIX_TIMESTAMP(date)"),
        SqlFunctionInfo::new("MAKEDATE", FunctionCategory::DateTime)
            .with_signature("MAKEDATE(year, dayofyear)"),
        SqlFunctionInfo::new("MAKETIME", FunctionCategory::DateTime)
            .with_signature("MAKETIME(hour, minute, second)"),
        SqlFunctionInfo::new("LAST_DAY", FunctionCategory::DateTime)
            .with_signature("LAST_DAY(date)"),
        // Conditional functions
        SqlFunctionInfo::new("IF", FunctionCategory::Conditional)
            .with_signature("IF(condition, true_value, false_value)"),
        SqlFunctionInfo::new("IFNULL", FunctionCategory::Conditional)
            .with_signature("IFNULL(expr1, expr2)"),
        SqlFunctionInfo::new("NULLIF", FunctionCategory::Conditional)
            .with_signature("NULLIF(expr1, expr2)"),
        SqlFunctionInfo::new("COALESCE", FunctionCategory::Conditional)
            .with_signature("COALESCE(value1, value2, ...)"),
        SqlFunctionInfo::new("ISNULL", FunctionCategory::Conditional)
            .with_signature("ISNULL(expr)"),
        // Type conversion
        SqlFunctionInfo::new("CAST", FunctionCategory::Conversion)
            .with_signature("CAST(expression AS type)"),
        SqlFunctionInfo::new("CONVERT", FunctionCategory::Conversion)
            .with_signature("CONVERT(expression, type)"),
        // JSON functions
        SqlFunctionInfo::new("JSON_OBJECT", FunctionCategory::Json)
            .with_signature("JSON_OBJECT(key1, value1, ...)"),
        SqlFunctionInfo::new("JSON_ARRAY", FunctionCategory::Json)
            .with_signature("JSON_ARRAY(value1, value2, ...)"),
        SqlFunctionInfo::new("JSON_EXTRACT", FunctionCategory::Json)
            .with_signature("JSON_EXTRACT(json, path...)"),
        SqlFunctionInfo::new("JSON_SET", FunctionCategory::Json)
            .with_signature("JSON_SET(json, path, value, ...)"),
        SqlFunctionInfo::new("JSON_INSERT", FunctionCategory::Json)
            .with_signature("JSON_INSERT(json, path, value, ...)"),
        SqlFunctionInfo::new("JSON_REPLACE", FunctionCategory::Json)
            .with_signature("JSON_REPLACE(json, path, value, ...)"),
        SqlFunctionInfo::new("JSON_REMOVE", FunctionCategory::Json)
            .with_signature("JSON_REMOVE(json, path...)"),
        SqlFunctionInfo::new("JSON_CONTAINS", FunctionCategory::Json)
            .with_signature("JSON_CONTAINS(json, candidate, path)"),
        SqlFunctionInfo::new("JSON_CONTAINS_PATH", FunctionCategory::Json)
            .with_signature("JSON_CONTAINS_PATH(json, 'one'|'all', path...)"),
        SqlFunctionInfo::new("JSON_KEYS", FunctionCategory::Json)
            .with_signature("JSON_KEYS(json, path)"),
        SqlFunctionInfo::new("JSON_LENGTH", FunctionCategory::Json)
            .with_signature("JSON_LENGTH(json, path)"),
        SqlFunctionInfo::new("JSON_DEPTH", FunctionCategory::Json)
            .with_signature("JSON_DEPTH(json)"),
        SqlFunctionInfo::new("JSON_TYPE", FunctionCategory::Json).with_signature("JSON_TYPE(json)"),
        SqlFunctionInfo::new("JSON_VALID", FunctionCategory::Json)
            .with_signature("JSON_VALID(string)"),
        SqlFunctionInfo::new("JSON_PRETTY", FunctionCategory::Json)
            .with_signature("JSON_PRETTY(json)"),
        SqlFunctionInfo::new("JSON_UNQUOTE", FunctionCategory::Json)
            .with_signature("JSON_UNQUOTE(json)"),
        SqlFunctionInfo::new("JSON_QUOTE", FunctionCategory::Json)
            .with_signature("JSON_QUOTE(string)"),
        SqlFunctionInfo::new("JSON_MERGE_PATCH", FunctionCategory::Json)
            .with_signature("JSON_MERGE_PATCH(json1, json2, ...)"),
        SqlFunctionInfo::new("JSON_MERGE_PRESERVE", FunctionCategory::Json)
            .with_signature("JSON_MERGE_PRESERVE(json1, json2, ...)"),
        SqlFunctionInfo::new("JSON_TABLE", FunctionCategory::Json)
            .with_signature("JSON_TABLE(json, path COLUMNS(...))"),
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
        // Encryption/Hash functions
        SqlFunctionInfo::new("MD5", FunctionCategory::Other).with_signature("MD5(string)"),
        SqlFunctionInfo::new("SHA1", FunctionCategory::Other).with_signature("SHA1(string)"),
        SqlFunctionInfo::new("SHA2", FunctionCategory::Other)
            .with_signature("SHA2(string, hash_length)"),
        SqlFunctionInfo::new("AES_ENCRYPT", FunctionCategory::Other)
            .with_signature("AES_ENCRYPT(str, key_str)"),
        SqlFunctionInfo::new("AES_DECRYPT", FunctionCategory::Other)
            .with_signature("AES_DECRYPT(crypt_str, key_str)"),
        SqlFunctionInfo::new("PASSWORD", FunctionCategory::Other)
            .with_signature("PASSWORD(string)"),
        // System functions
        SqlFunctionInfo::new("DATABASE", FunctionCategory::Other).with_signature("DATABASE()"),
        SqlFunctionInfo::new("SCHEMA", FunctionCategory::Other).with_signature("SCHEMA()"),
        SqlFunctionInfo::new("USER", FunctionCategory::Other).with_signature("USER()"),
        SqlFunctionInfo::new("CURRENT_USER", FunctionCategory::Other)
            .with_signature("CURRENT_USER()"),
        SqlFunctionInfo::new("SESSION_USER", FunctionCategory::Other)
            .with_signature("SESSION_USER()"),
        SqlFunctionInfo::new("SYSTEM_USER", FunctionCategory::Other)
            .with_signature("SYSTEM_USER()"),
        SqlFunctionInfo::new("VERSION", FunctionCategory::Other).with_signature("VERSION()"),
        SqlFunctionInfo::new("CONNECTION_ID", FunctionCategory::Other)
            .with_signature("CONNECTION_ID()"),
        SqlFunctionInfo::new("LAST_INSERT_ID", FunctionCategory::Other)
            .with_signature("LAST_INSERT_ID()"),
        SqlFunctionInfo::new("ROW_COUNT", FunctionCategory::Other).with_signature("ROW_COUNT()"),
        SqlFunctionInfo::new("FOUND_ROWS", FunctionCategory::Other).with_signature("FOUND_ROWS()"),
        SqlFunctionInfo::new("UUID", FunctionCategory::Other).with_signature("UUID()"),
        SqlFunctionInfo::new("UUID_SHORT", FunctionCategory::Other).with_signature("UUID_SHORT()"),
        SqlFunctionInfo::new("SLEEP", FunctionCategory::Other).with_signature("SLEEP(duration)"),
        SqlFunctionInfo::new("BENCHMARK", FunctionCategory::Other)
            .with_signature("BENCHMARK(count, expr)"),
    ]
}

fn mysql_data_types() -> Vec<DataTypeInfo> {
    vec![
        // Integer types
        DataTypeInfo::new("TINYINT", DataTypeCategory::Integer),
        DataTypeInfo::new("SMALLINT", DataTypeCategory::Integer),
        DataTypeInfo::new("MEDIUMINT", DataTypeCategory::Integer),
        DataTypeInfo::new("INT", DataTypeCategory::Integer),
        DataTypeInfo::new("INTEGER", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGINT", DataTypeCategory::Integer),
        // Unsigned variants
        DataTypeInfo::new("TINYINT UNSIGNED", DataTypeCategory::Integer),
        DataTypeInfo::new("SMALLINT UNSIGNED", DataTypeCategory::Integer),
        DataTypeInfo::new("MEDIUMINT UNSIGNED", DataTypeCategory::Integer),
        DataTypeInfo::new("INT UNSIGNED", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGINT UNSIGNED", DataTypeCategory::Integer),
        // Floating point
        DataTypeInfo::new("FLOAT", DataTypeCategory::Float),
        DataTypeInfo::new("DOUBLE", DataTypeCategory::Float),
        DataTypeInfo::new("DOUBLE PRECISION", DataTypeCategory::Float),
        DataTypeInfo::new("REAL", DataTypeCategory::Float),
        // Fixed precision
        DataTypeInfo::new("DECIMAL", DataTypeCategory::Decimal).with_length(None, None),
        DataTypeInfo::new("NUMERIC", DataTypeCategory::Decimal).with_length(None, None),
        DataTypeInfo::new("DEC", DataTypeCategory::Decimal).with_length(None, None),
        DataTypeInfo::new("FIXED", DataTypeCategory::Decimal).with_length(None, None),
        // Bit
        DataTypeInfo::new("BIT", DataTypeCategory::Integer).with_length(Some(1), Some(64)),
        // Boolean
        DataTypeInfo::new("BOOLEAN", DataTypeCategory::Boolean),
        DataTypeInfo::new("BOOL", DataTypeCategory::Boolean),
        // String types
        DataTypeInfo::new("CHAR", DataTypeCategory::String).with_length(Some(1), Some(255)),
        DataTypeInfo::new("VARCHAR", DataTypeCategory::String).with_length(Some(1), Some(65535)),
        DataTypeInfo::new("TINYTEXT", DataTypeCategory::String),
        DataTypeInfo::new("TEXT", DataTypeCategory::String),
        DataTypeInfo::new("MEDIUMTEXT", DataTypeCategory::String),
        DataTypeInfo::new("LONGTEXT", DataTypeCategory::String),
        // Binary types
        DataTypeInfo::new("BINARY", DataTypeCategory::Binary).with_length(Some(1), Some(255)),
        DataTypeInfo::new("VARBINARY", DataTypeCategory::Binary).with_length(Some(1), Some(65535)),
        DataTypeInfo::new("TINYBLOB", DataTypeCategory::Binary),
        DataTypeInfo::new("BLOB", DataTypeCategory::Binary),
        DataTypeInfo::new("MEDIUMBLOB", DataTypeCategory::Binary),
        DataTypeInfo::new("LONGBLOB", DataTypeCategory::Binary),
        // Enum and Set
        DataTypeInfo::new("ENUM", DataTypeCategory::Other),
        DataTypeInfo::new("SET", DataTypeCategory::Other),
        // Date/Time
        DataTypeInfo::new("DATE", DataTypeCategory::Date),
        DataTypeInfo::new("TIME", DataTypeCategory::Time),
        DataTypeInfo::new("DATETIME", DataTypeCategory::DateTime),
        DataTypeInfo::new("TIMESTAMP", DataTypeCategory::DateTime),
        DataTypeInfo::new("YEAR", DataTypeCategory::Date),
        // JSON
        DataTypeInfo::new("JSON", DataTypeCategory::Json),
        // Spatial types
        DataTypeInfo::new("GEOMETRY", DataTypeCategory::Geometry),
        DataTypeInfo::new("POINT", DataTypeCategory::Geometry),
        DataTypeInfo::new("LINESTRING", DataTypeCategory::Geometry),
        DataTypeInfo::new("POLYGON", DataTypeCategory::Geometry),
        DataTypeInfo::new("MULTIPOINT", DataTypeCategory::Geometry),
        DataTypeInfo::new("MULTILINESTRING", DataTypeCategory::Geometry),
        DataTypeInfo::new("MULTIPOLYGON", DataTypeCategory::Geometry),
        DataTypeInfo::new("GEOMETRYCOLLECTION", DataTypeCategory::Geometry),
    ]
}

fn mysql_table_options() -> Vec<TableOptionDef> {
    vec![
        TableOptionDef {
            key: Cow::Borrowed("engine"),
            label: Cow::Borrowed("ENGINE"),
            option_type: TableOptionType::Choice,
            default_value: Some(Cow::Borrowed("InnoDB")),
            description: Some(Cow::Borrowed("Storage engine for the table")),
            choices: vec![
                Cow::Borrowed("InnoDB"),
                Cow::Borrowed("MyISAM"),
                Cow::Borrowed("MEMORY"),
                Cow::Borrowed("CSV"),
                Cow::Borrowed("ARCHIVE"),
                Cow::Borrowed("BLACKHOLE"),
                Cow::Borrowed("MERGE"),
                Cow::Borrowed("FEDERATED"),
                Cow::Borrowed("NDB"),
            ],
        },
        TableOptionDef {
            key: Cow::Borrowed("charset"),
            label: Cow::Borrowed("CHARACTER SET"),
            option_type: TableOptionType::Choice,
            default_value: Some(Cow::Borrowed("utf8mb4")),
            description: Some(Cow::Borrowed("Default character set for the table")),
            choices: vec![
                Cow::Borrowed("utf8mb4"),
                Cow::Borrowed("utf8mb3"),
                Cow::Borrowed("utf8"),
                Cow::Borrowed("latin1"),
                Cow::Borrowed("ascii"),
                Cow::Borrowed("binary"),
            ],
        },
        TableOptionDef {
            key: Cow::Borrowed("collate"),
            label: Cow::Borrowed("COLLATE"),
            option_type: TableOptionType::Choice,
            default_value: Some(Cow::Borrowed("utf8mb4_unicode_ci")),
            description: Some(Cow::Borrowed("Default collation for the table")),
            choices: vec![
                Cow::Borrowed("utf8mb4_unicode_ci"),
                Cow::Borrowed("utf8mb4_general_ci"),
                Cow::Borrowed("utf8mb4_bin"),
                Cow::Borrowed("utf8_unicode_ci"),
                Cow::Borrowed("utf8_general_ci"),
                Cow::Borrowed("latin1_swedish_ci"),
            ],
        },
        TableOptionDef {
            key: Cow::Borrowed("auto_increment"),
            label: Cow::Borrowed("AUTO_INCREMENT"),
            option_type: TableOptionType::Number,
            default_value: Some(Cow::Borrowed("1")),
            description: Some(Cow::Borrowed("Starting value for AUTO_INCREMENT")),
            choices: Vec::new(),
        },
        TableOptionDef {
            key: Cow::Borrowed("comment"),
            label: Cow::Borrowed("COMMENT"),
            option_type: TableOptionType::Text,
            default_value: None,
            description: Some(Cow::Borrowed("Table comment/description")),
            choices: Vec::new(),
        },
        TableOptionDef {
            key: Cow::Borrowed("row_format"),
            label: Cow::Borrowed("ROW_FORMAT"),
            option_type: TableOptionType::Choice,
            default_value: Some(Cow::Borrowed("DYNAMIC")),
            description: Some(Cow::Borrowed("Row storage format")),
            choices: vec![
                Cow::Borrowed("DEFAULT"),
                Cow::Borrowed("DYNAMIC"),
                Cow::Borrowed("FIXED"),
                Cow::Borrowed("COMPRESSED"),
                Cow::Borrowed("REDUNDANT"),
                Cow::Borrowed("COMPACT"),
            ],
        },
    ]
}
