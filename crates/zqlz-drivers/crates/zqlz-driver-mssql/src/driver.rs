//! MS SQL Server driver implementation

use crate::connection::MssqlConnection;
use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    AutoIncrementInfo, AutoIncrementStyle, CommentStyles, Connection, ConnectionConfig,
    ConnectionField, ConnectionFieldSchema, DataTypeCategory, DataTypeInfo, DatabaseDriver,
    DialectInfo, DriverCapabilities, ExplainConfig, FunctionCategory, KeywordCategory, KeywordInfo,
    Result, SqlFunctionInfo, ZqlzError,
};

/// MS SQL Server database driver
pub struct MssqlDriver;

impl MssqlDriver {
    /// Create a new MS SQL Server driver instance
    pub fn new() -> Self {
        tracing::debug!("MS SQL Server driver initialized");
        Self
    }
}

impl Default for MssqlDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for MssqlDriver {
    fn id(&self) -> &'static str {
        "mssql"
    }

    fn name(&self) -> &'static str {
        "mssql"
    }

    fn display_name(&self) -> &'static str {
        "MS SQL Server"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn default_port(&self) -> Option<u16> {
        Some(1433)
    }

    fn icon_name(&self) -> &'static str {
        "mssql"
    }

    fn dialect_info(&self) -> DialectInfo {
        mssql_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: true, // OUTPUT clause
            supports_upsert: true,    // MERGE statement
            supports_window_functions: true,
            supports_cte: true,
            supports_json: true, // SQL Server 2016+
            supports_full_text_search: true,
            supports_stored_procedures: true,
            supports_schemas: true,
            supports_multiple_databases: true,
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true, // SET SHOWPLAN_*
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: Some(128),
            max_parameters: Some(2100), // SQL Server limit
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref(), database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        tracing::debug!("connecting to MS SQL Server");
        let connection = MssqlConnection::from_config(config)
            .await
            .map_err(|e| ZqlzError::Driver(e.to_string()))?;
        Ok(Arc::new(connection))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing MS SQL Server connection");
        let _conn = self.connect(config).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 1433 };
        let database = config.get_string("database");
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));

        let mut conn_str = format!("Server={},{}", host, port);

        if let Some(db) = database {
            conn_str.push_str(&format!(";Database={}", db));
        }

        if let Some(u) = user {
            conn_str.push_str(&format!(";User Id={}", u));
            if let Some(p) = config.get_string("password") {
                conn_str.push_str(&format!(";Password={}", p));
            }
        } else {
            conn_str.push_str(";Trusted_Connection=True");
        }

        conn_str
    }

    fn connection_string_help(&self) -> &'static str {
        "Server=host,port;Database=dbname;User Id=user;Password=pass"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        ConnectionFieldSchema {
            title: Cow::Borrowed("MS SQL Server Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("1433")
                    .default_value("1433")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database")
                    .placeholder("master")
                    .default_value("master"),
                ConnectionField::text("user", "Username")
                    .placeholder("sa")
                    .default_value("sa")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .required()
                    .width(0.5)
                    .row_group(2),
                ConnectionField::boolean("trust_certificate", "Trust Server Certificate")
                    .help_text("Trust the server certificate without validation (for development)"),
            ],
        }
    }
}

/// Create MS SQL Server (T-SQL) dialect information
pub fn mssql_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("mssql"),
        display_name: Cow::Borrowed("T-SQL"),
        keywords: mssql_keywords(),
        functions: mssql_functions(),
        data_types: mssql_data_types(),
        table_options: vec![],
        auto_increment: Some(AutoIncrementInfo {
            keyword: Cow::Borrowed("IDENTITY"),
            style: AutoIncrementStyle::Suffix,
            description: Some(Cow::Borrowed(
                "Auto-incrementing column using IDENTITY(1,1)",
            )),
        }),
        identifier_quote: '[', // Also supports ] for closing, but [ is the opening char
        string_quote: '\'',
        case_sensitive_identifiers: false,
        statement_terminator: ';',
        comment_styles: CommentStyles::sql_standard(),
        explain_config: mssql_explain_config(),
    }
}

fn mssql_explain_config() -> ExplainConfig {
    ExplainConfig {
        explain_format: Cow::Borrowed("SET SHOWPLAN_TEXT ON; {sql}; SET SHOWPLAN_TEXT OFF"),
        query_plan_format: Some(Cow::Borrowed(
            "SET SHOWPLAN_ALL ON; {sql}; SET SHOWPLAN_ALL OFF",
        )),
        analyze_format: Some(Cow::Borrowed(
            "SET STATISTICS IO ON; SET STATISTICS TIME ON; {sql}; SET STATISTICS IO OFF; SET STATISTICS TIME OFF",
        )),
        explain_description: Cow::Borrowed("Shows text-based execution plan"),
        query_plan_description: Some(Cow::Borrowed(
            "Shows detailed execution plan with cost estimates",
        )),
        analyze_is_safe: false,
    }
}

fn mssql_keywords() -> Vec<KeywordInfo> {
    vec![
        KeywordInfo::new("SELECT", KeywordCategory::Dql),
        KeywordInfo::new("FROM", KeywordCategory::Dql),
        KeywordInfo::new("WHERE", KeywordCategory::Dql),
        KeywordInfo::new("INSERT", KeywordCategory::Dml),
        KeywordInfo::new("UPDATE", KeywordCategory::Dml),
        KeywordInfo::new("DELETE", KeywordCategory::Dml),
        KeywordInfo::new("CREATE", KeywordCategory::Ddl),
        KeywordInfo::new("ALTER", KeywordCategory::Ddl),
        KeywordInfo::new("DROP", KeywordCategory::Ddl),
        KeywordInfo::new("TRUNCATE", KeywordCategory::Ddl),
        KeywordInfo::new("BEGIN", KeywordCategory::Transaction),
        KeywordInfo::new("COMMIT", KeywordCategory::Transaction),
        KeywordInfo::new("ROLLBACK", KeywordCategory::Transaction),
        KeywordInfo::new("SAVE", KeywordCategory::Transaction),
        KeywordInfo::new("TRANSACTION", KeywordCategory::Transaction),
        KeywordInfo::new("JOIN", KeywordCategory::Clause),
        KeywordInfo::new("INNER", KeywordCategory::Clause),
        KeywordInfo::new("LEFT", KeywordCategory::Clause),
        KeywordInfo::new("RIGHT", KeywordCategory::Clause),
        KeywordInfo::new("OUTER", KeywordCategory::Clause),
        KeywordInfo::new("CROSS", KeywordCategory::Clause),
        KeywordInfo::new("ON", KeywordCategory::Clause),
        KeywordInfo::new("GROUP", KeywordCategory::Clause),
        KeywordInfo::new("BY", KeywordCategory::Clause),
        KeywordInfo::new("HAVING", KeywordCategory::Clause),
        KeywordInfo::new("ORDER", KeywordCategory::Clause),
        KeywordInfo::new("TOP", KeywordCategory::Clause),
        KeywordInfo::new("OFFSET", KeywordCategory::Clause),
        KeywordInfo::new("FETCH", KeywordCategory::Clause),
        KeywordInfo::new("AND", KeywordCategory::Operator),
        KeywordInfo::new("OR", KeywordCategory::Operator),
        KeywordInfo::new("NOT", KeywordCategory::Operator),
        KeywordInfo::new("IN", KeywordCategory::Operator),
        KeywordInfo::new("LIKE", KeywordCategory::Operator),
        KeywordInfo::new("BETWEEN", KeywordCategory::Operator),
        KeywordInfo::new("EXISTS", KeywordCategory::Operator),
        KeywordInfo::new("IS", KeywordCategory::Operator),
        KeywordInfo::new("NULL", KeywordCategory::Operator),
        KeywordInfo::with_desc("GO", KeywordCategory::DatabaseSpecific, "Batch separator"),
        KeywordInfo::with_desc("USE", KeywordCategory::DatabaseSpecific, "Switch database"),
        KeywordInfo::with_desc(
            "EXEC",
            KeywordCategory::DatabaseSpecific,
            "Execute stored procedure",
        ),
        KeywordInfo::with_desc(
            "EXECUTE",
            KeywordCategory::DatabaseSpecific,
            "Execute stored procedure",
        ),
        KeywordInfo::with_desc(
            "DECLARE",
            KeywordCategory::DatabaseSpecific,
            "Declare variable",
        ),
        KeywordInfo::with_desc(
            "SET",
            KeywordCategory::DatabaseSpecific,
            "Set variable or option",
        ),
        KeywordInfo::with_desc("PRINT", KeywordCategory::DatabaseSpecific, "Print message"),
        KeywordInfo::with_desc("MERGE", KeywordCategory::Dml, "Upsert operation"),
        KeywordInfo::with_desc("OUTPUT", KeywordCategory::Clause, "Return affected rows"),
        KeywordInfo::with_desc(
            "INSERTED",
            KeywordCategory::DatabaseSpecific,
            "Inserted rows in trigger/OUTPUT",
        ),
        KeywordInfo::with_desc(
            "DELETED",
            KeywordCategory::DatabaseSpecific,
            "Deleted rows in trigger/OUTPUT",
        ),
        KeywordInfo::new("WITH", KeywordCategory::Clause),
        KeywordInfo::new("AS", KeywordCategory::Clause),
        KeywordInfo::new("OVER", KeywordCategory::Clause),
        KeywordInfo::new("PARTITION", KeywordCategory::Clause),
        KeywordInfo::new("GRANT", KeywordCategory::Dcl),
        KeywordInfo::new("REVOKE", KeywordCategory::Dcl),
        KeywordInfo::new("DENY", KeywordCategory::Dcl),
    ]
}

fn mssql_functions() -> Vec<SqlFunctionInfo> {
    vec![
        SqlFunctionInfo::new("COUNT", FunctionCategory::Aggregate)
            .with_signature("COUNT(expression)"),
        SqlFunctionInfo::new("SUM", FunctionCategory::Aggregate).with_signature("SUM(expression)"),
        SqlFunctionInfo::new("AVG", FunctionCategory::Aggregate).with_signature("AVG(expression)"),
        SqlFunctionInfo::new("MIN", FunctionCategory::Aggregate).with_signature("MIN(expression)"),
        SqlFunctionInfo::new("MAX", FunctionCategory::Aggregate).with_signature("MAX(expression)"),
        SqlFunctionInfo::new("STRING_AGG", FunctionCategory::Aggregate)
            .with_signature("STRING_AGG(expression, separator)"),
        SqlFunctionInfo::new("ROW_NUMBER", FunctionCategory::Window)
            .with_signature("ROW_NUMBER() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("RANK", FunctionCategory::Window)
            .with_signature("RANK() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("DENSE_RANK", FunctionCategory::Window)
            .with_signature("DENSE_RANK() OVER (ORDER BY column)"),
        SqlFunctionInfo::new("NTILE", FunctionCategory::Window)
            .with_signature("NTILE(n) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("LAG", FunctionCategory::Window)
            .with_signature("LAG(expression, offset, default) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("LEAD", FunctionCategory::Window)
            .with_signature("LEAD(expression, offset, default) OVER (ORDER BY column)"),
        SqlFunctionInfo::new("LEN", FunctionCategory::String).with_signature("LEN(string)"),
        SqlFunctionInfo::new("DATALENGTH", FunctionCategory::String)
            .with_signature("DATALENGTH(expression)"),
        SqlFunctionInfo::new("LEFT", FunctionCategory::String)
            .with_signature("LEFT(string, length)"),
        SqlFunctionInfo::new("RIGHT", FunctionCategory::String)
            .with_signature("RIGHT(string, length)"),
        SqlFunctionInfo::new("SUBSTRING", FunctionCategory::String)
            .with_signature("SUBSTRING(string, start, length)"),
        SqlFunctionInfo::new("CHARINDEX", FunctionCategory::String)
            .with_signature("CHARINDEX(substring, string, start)"),
        SqlFunctionInfo::new("REPLACE", FunctionCategory::String)
            .with_signature("REPLACE(string, old, new)"),
        SqlFunctionInfo::new("CONCAT", FunctionCategory::String)
            .with_signature("CONCAT(string1, string2, ...)"),
        SqlFunctionInfo::new("UPPER", FunctionCategory::String).with_signature("UPPER(string)"),
        SqlFunctionInfo::new("LOWER", FunctionCategory::String).with_signature("LOWER(string)"),
        SqlFunctionInfo::new("LTRIM", FunctionCategory::String).with_signature("LTRIM(string)"),
        SqlFunctionInfo::new("RTRIM", FunctionCategory::String).with_signature("RTRIM(string)"),
        SqlFunctionInfo::new("TRIM", FunctionCategory::String).with_signature("TRIM(string)"),
        SqlFunctionInfo::new("ABS", FunctionCategory::Numeric).with_signature("ABS(number)"),
        SqlFunctionInfo::new("CEILING", FunctionCategory::Numeric)
            .with_signature("CEILING(number)"),
        SqlFunctionInfo::new("FLOOR", FunctionCategory::Numeric).with_signature("FLOOR(number)"),
        SqlFunctionInfo::new("ROUND", FunctionCategory::Numeric)
            .with_signature("ROUND(number, precision)"),
        SqlFunctionInfo::new("POWER", FunctionCategory::Numeric)
            .with_signature("POWER(base, exponent)"),
        SqlFunctionInfo::new("SQRT", FunctionCategory::Numeric).with_signature("SQRT(number)"),
        SqlFunctionInfo::new("GETDATE", FunctionCategory::DateTime).with_signature("GETDATE()"),
        SqlFunctionInfo::new("GETUTCDATE", FunctionCategory::DateTime)
            .with_signature("GETUTCDATE()"),
        SqlFunctionInfo::new("SYSDATETIME", FunctionCategory::DateTime)
            .with_signature("SYSDATETIME()"),
        SqlFunctionInfo::new("DATEADD", FunctionCategory::DateTime)
            .with_signature("DATEADD(datepart, number, date)"),
        SqlFunctionInfo::new("DATEDIFF", FunctionCategory::DateTime)
            .with_signature("DATEDIFF(datepart, startdate, enddate)"),
        SqlFunctionInfo::new("DATEPART", FunctionCategory::DateTime)
            .with_signature("DATEPART(datepart, date)"),
        SqlFunctionInfo::new("DATENAME", FunctionCategory::DateTime)
            .with_signature("DATENAME(datepart, date)"),
        SqlFunctionInfo::new("YEAR", FunctionCategory::DateTime).with_signature("YEAR(date)"),
        SqlFunctionInfo::new("MONTH", FunctionCategory::DateTime).with_signature("MONTH(date)"),
        SqlFunctionInfo::new("DAY", FunctionCategory::DateTime).with_signature("DAY(date)"),
        SqlFunctionInfo::new("FORMAT", FunctionCategory::DateTime)
            .with_signature("FORMAT(value, format)"),
        SqlFunctionInfo::new("CAST", FunctionCategory::Conversion)
            .with_signature("CAST(expression AS datatype)"),
        SqlFunctionInfo::new("CONVERT", FunctionCategory::Conversion)
            .with_signature("CONVERT(datatype, expression, style)"),
        SqlFunctionInfo::new("TRY_CAST", FunctionCategory::Conversion)
            .with_signature("TRY_CAST(expression AS datatype)"),
        SqlFunctionInfo::new("TRY_CONVERT", FunctionCategory::Conversion)
            .with_signature("TRY_CONVERT(datatype, expression, style)"),
        SqlFunctionInfo::new("PARSE", FunctionCategory::Conversion)
            .with_signature("PARSE(string AS datatype USING culture)"),
        SqlFunctionInfo::new("CASE", FunctionCategory::Conditional)
            .with_signature("CASE WHEN condition THEN result ELSE default END"),
        SqlFunctionInfo::new("COALESCE", FunctionCategory::Conditional)
            .with_signature("COALESCE(expression1, expression2, ...)"),
        SqlFunctionInfo::new("NULLIF", FunctionCategory::Conditional)
            .with_signature("NULLIF(expression1, expression2)"),
        SqlFunctionInfo::new("IIF", FunctionCategory::Conditional)
            .with_signature("IIF(condition, true_value, false_value)"),
        SqlFunctionInfo::new("ISNULL", FunctionCategory::Conditional)
            .with_signature("ISNULL(expression, replacement)"),
        SqlFunctionInfo::new("JSON_VALUE", FunctionCategory::Json)
            .with_signature("JSON_VALUE(expression, path)"),
        SqlFunctionInfo::new("JSON_QUERY", FunctionCategory::Json)
            .with_signature("JSON_QUERY(expression, path)"),
        SqlFunctionInfo::new("JSON_MODIFY", FunctionCategory::Json)
            .with_signature("JSON_MODIFY(expression, path, newValue)"),
        SqlFunctionInfo::new("ISJSON", FunctionCategory::Json).with_signature("ISJSON(expression)"),
        SqlFunctionInfo::new("OPENJSON", FunctionCategory::Json)
            .with_signature("OPENJSON(expression, path)"),
        SqlFunctionInfo::new("NEWID", FunctionCategory::Other).with_signature("NEWID()"),
        SqlFunctionInfo::new("NEWSEQUENTIALID", FunctionCategory::Other)
            .with_signature("NEWSEQUENTIALID()"),
        SqlFunctionInfo::new("SCOPE_IDENTITY", FunctionCategory::Other)
            .with_signature("SCOPE_IDENTITY()"),
        SqlFunctionInfo::new("@@IDENTITY", FunctionCategory::Other).with_signature("@@IDENTITY"),
        SqlFunctionInfo::new("@@ROWCOUNT", FunctionCategory::Other).with_signature("@@ROWCOUNT"),
    ]
}

fn mssql_data_types() -> Vec<DataTypeInfo> {
    vec![
        DataTypeInfo::new("BIT", DataTypeCategory::Boolean),
        DataTypeInfo::new("TINYINT", DataTypeCategory::Integer),
        DataTypeInfo::new("SMALLINT", DataTypeCategory::Integer),
        DataTypeInfo::new("INT", DataTypeCategory::Integer),
        DataTypeInfo::new("BIGINT", DataTypeCategory::Integer),
        DataTypeInfo::new("DECIMAL", DataTypeCategory::Decimal).with_length(Some(18), Some(38)),
        DataTypeInfo::new("NUMERIC", DataTypeCategory::Decimal).with_length(Some(18), Some(38)),
        DataTypeInfo::new("MONEY", DataTypeCategory::Decimal),
        DataTypeInfo::new("SMALLMONEY", DataTypeCategory::Decimal),
        DataTypeInfo::new("FLOAT", DataTypeCategory::Float).with_length(Some(53), Some(53)),
        DataTypeInfo::new("REAL", DataTypeCategory::Float),
        DataTypeInfo::new("CHAR", DataTypeCategory::String).with_length(Some(1), Some(8000)),
        DataTypeInfo::new("VARCHAR", DataTypeCategory::String).with_length(Some(1), Some(8000)),
        DataTypeInfo::new("NCHAR", DataTypeCategory::String).with_length(Some(1), Some(4000)),
        DataTypeInfo::new("NVARCHAR", DataTypeCategory::String).with_length(Some(1), Some(4000)),
        DataTypeInfo::new("TEXT", DataTypeCategory::String),
        DataTypeInfo::new("NTEXT", DataTypeCategory::String),
        DataTypeInfo::new("BINARY", DataTypeCategory::Binary).with_length(Some(1), Some(8000)),
        DataTypeInfo::new("VARBINARY", DataTypeCategory::Binary).with_length(Some(1), Some(8000)),
        DataTypeInfo::new("IMAGE", DataTypeCategory::Binary),
        DataTypeInfo::new("DATE", DataTypeCategory::Date),
        DataTypeInfo::new("TIME", DataTypeCategory::Time).with_length(Some(7), Some(7)),
        DataTypeInfo::new("DATETIME", DataTypeCategory::DateTime),
        DataTypeInfo::new("DATETIME2", DataTypeCategory::DateTime).with_length(Some(7), Some(7)),
        DataTypeInfo::new("SMALLDATETIME", DataTypeCategory::DateTime),
        DataTypeInfo::new("DATETIMEOFFSET", DataTypeCategory::DateTime)
            .with_length(Some(7), Some(7)),
        DataTypeInfo::new("UNIQUEIDENTIFIER", DataTypeCategory::Uuid),
        DataTypeInfo::new("XML", DataTypeCategory::Other),
        DataTypeInfo::new("SQL_VARIANT", DataTypeCategory::Other),
        DataTypeInfo::new("GEOGRAPHY", DataTypeCategory::Geometry),
        DataTypeInfo::new("GEOMETRY", DataTypeCategory::Geometry),
        DataTypeInfo::new("HIERARCHYID", DataTypeCategory::Other),
    ]
}
