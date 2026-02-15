//! MS SQL Server connection implementation using tiberius

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row as TiberiusRow};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, Connection, QueryResult, Result, Row, StatementResult, Transaction, Value,
    ZqlzError,
};

/// MS SQL Server connection errors
#[derive(Debug, thiserror::Error)]
pub enum MssqlConnectionError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Connection is closed")]
    ConnectionClosed,

    #[error("Tiberius error: {0}")]
    Tiberius(#[from] tiberius::error::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<MssqlConnectionError> for ZqlzError {
    fn from(err: MssqlConnectionError) -> Self {
        ZqlzError::Driver(err.to_string())
    }
}

/// MS SQL Server connection using tiberius
pub struct MssqlConnection {
    client: Mutex<Client<Compat<TcpStream>>>,
    closed: AtomicBool,
    database: Option<String>,
}

impl MssqlConnection {
    /// Create a new MS SQL Server connection
    ///
    /// # Arguments
    /// * `host` - Server hostname
    /// * `port` - Server port (default 1433)
    /// * `database` - Database name (optional)
    /// * `username` - Username (None for Windows auth)
    /// * `password` - Password
    /// * `trust_cert` - Whether to trust server certificate (for dev/testing)
    #[tracing::instrument(skip(password))]
    pub async fn connect(
        host: &str,
        port: u16,
        database: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
        trust_cert: bool,
    ) -> std::result::Result<Self, MssqlConnectionError> {
        tracing::debug!("connecting to MS SQL Server at {}:{}", host, port);

        let mut config = Config::new();
        config.host(host);
        config.port(port);

        if let Some(db) = database {
            config.database(db);
        }

        if trust_cert {
            config.trust_cert();
        }

        config.encryption(EncryptionLevel::Required);

        match (username, password) {
            (Some(user), Some(pass)) => {
                config.authentication(AuthMethod::sql_server(user, pass));
            }
            (Some(user), None) => {
                config.authentication(AuthMethod::sql_server(user, ""));
            }
            (None, _) => {
                #[cfg(windows)]
                {
                    config.authentication(AuthMethod::Integrated);
                }
                #[cfg(not(windows))]
                {
                    return Err(MssqlConnectionError::AuthenticationFailed(
                        "Windows authentication is only supported on Windows".to_string(),
                    ));
                }
            }
        }

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| MssqlConnectionError::ConnectionFailed(e.to_string()))?;

        tcp.set_nodelay(true)?;
        let compat_stream = tcp.compat_write();

        let client = Client::connect(config, compat_stream)
            .await
            .map_err(|e| MssqlConnectionError::ConnectionFailed(e.to_string()))?;

        tracing::debug!("successfully connected to MS SQL Server");

        Ok(Self {
            client: Mutex::new(client),
            closed: AtomicBool::new(false),
            database: database.map(String::from),
        })
    }

    /// Create connection from config with standard keys
    pub async fn from_config(
        config: &zqlz_core::ConnectionConfig,
    ) -> std::result::Result<Self, MssqlConnectionError> {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 1433 };
        let database = config.get_string("database");
        let username = config
            .get_string("user")
            .or_else(|| config.get_string("username"));
        let password = config.get_string("password");
        let trust_cert = config
            .params
            .get("trust_cert")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        Self::connect(
            &host,
            port,
            database.as_deref(),
            username.as_deref(),
            password.as_deref(),
            trust_cert,
        )
        .await
    }

    fn ensure_not_closed(&self) -> std::result::Result<(), MssqlConnectionError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(MssqlConnectionError::ConnectionClosed);
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for MssqlConnection {
    fn driver_name(&self) -> &str {
        "mssql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mssql")
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut client = self.client.lock().await;

        let result = if params.is_empty() {
            client.execute(sql, &[]).await
        } else {
            let tiberius_params = values_to_tiberius_params(params)?;
            let param_refs: Vec<&dyn tiberius::ToSql> = tiberius_params
                .iter()
                .map(|p| p.as_ref() as &dyn tiberius::ToSql)
                .collect();
            client.execute(sql, &param_refs[..]).await
        };

        match result {
            Ok(exec_result) => {
                let affected_rows = exec_result.rows_affected().iter().sum::<u64>();
                tracing::debug!(
                    affected_rows = affected_rows,
                    duration_ms = start.elapsed().as_millis() as u64,
                    "execute completed"
                );

                Ok(StatementResult {
                    is_query: false,
                    result: None,
                    affected_rows,
                    error: None,
                })
            }
            Err(e) => {
                tracing::error!(error = %e, "execute failed");
                Err(ZqlzError::Driver(e.to_string()))
            }
        }
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut client = self.client.lock().await;

        let stream = if params.is_empty() {
            client.query(sql, &[]).await
        } else {
            let tiberius_params = values_to_tiberius_params(params)?;
            let param_refs: Vec<&dyn tiberius::ToSql> = tiberius_params
                .iter()
                .map(|p| p.as_ref() as &dyn tiberius::ToSql)
                .collect();
            client.query(sql, &param_refs[..]).await
        };

        match stream {
            Ok(query_stream) => {
                let mut columns: Vec<ColumnMeta> = Vec::new();

                let tib_rows = query_stream
                    .into_first_result()
                    .await
                    .map_err(|e| ZqlzError::Driver(e.to_string()))?;

                if let Some(first_row) = tib_rows.first() {
                    columns = first_row
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| tiberius_column_to_meta(col, idx))
                        .collect();
                }

                let mut rows: Vec<Row> = Vec::new();
                let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

                for tib_row in tib_rows {
                    let values = tiberius_row_to_values(tib_row)?;
                    rows.push(Row::new(column_names.clone(), values));
                }

                let execution_time_ms = start.elapsed().as_millis() as u64;
                tracing::debug!(
                    row_count = rows.len(),
                    duration_ms = execution_time_ms,
                    "query completed"
                );

                Ok(QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    rows,
                    total_rows: None,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms,
                    warnings: Vec::new(),
                })
            }
            Err(e) => {
                tracing::error!(error = %e, "query failed");
                Err(ZqlzError::Driver(e.to_string()))
            }
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.ensure_not_closed()?;
        Err(ZqlzError::NotImplemented(
            "Transactions for MS SQL Server will be implemented in a future update".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        tracing::debug!("MS SQL Server connection closed");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Convert a tiberius column to ColumnMeta
fn tiberius_column_to_meta(col: &tiberius::Column, ordinal: usize) -> ColumnMeta {
    let data_type = format!("{:?}", col.column_type());

    ColumnMeta {
        name: col.name().to_string(),
        data_type,
        nullable: true,
        ordinal,
        max_length: None,
        precision: None,
        scale: None,
        auto_increment: false,
        default_value: None,
        comment: None,
        enum_values: None,
    }
}

/// Convert a tiberius row to a vector of Values by consuming the row
fn tiberius_row_to_values(row: TiberiusRow) -> Result<Vec<Value>> {
    let mut values = Vec::new();

    for col_data in row.into_iter() {
        let value = column_data_to_value(col_data)?;
        values.push(value);
    }

    Ok(values)
}

/// Convert tiberius ColumnData to zqlz Value
pub(crate) fn column_data_to_value(col_data: ColumnData<'static>) -> Result<Value> {
    match col_data {
        ColumnData::Bit(None) => Ok(Value::Null),
        ColumnData::Bit(Some(v)) => Ok(Value::Bool(v)),
        ColumnData::U8(None) => Ok(Value::Null),
        ColumnData::U8(Some(v)) => Ok(Value::Int32(v as i32)),
        ColumnData::I16(None) => Ok(Value::Null),
        ColumnData::I16(Some(v)) => Ok(Value::Int16(v)),
        ColumnData::I32(None) => Ok(Value::Null),
        ColumnData::I32(Some(v)) => Ok(Value::Int32(v)),
        ColumnData::I64(None) => Ok(Value::Null),
        ColumnData::I64(Some(v)) => Ok(Value::Int64(v)),
        ColumnData::F32(None) => Ok(Value::Null),
        ColumnData::F32(Some(v)) => Ok(Value::Float32(v)),
        ColumnData::F64(None) => Ok(Value::Null),
        ColumnData::F64(Some(v)) => Ok(Value::Float64(v)),
        ColumnData::String(None) => Ok(Value::Null),
        ColumnData::String(Some(v)) => Ok(Value::String(v.into_owned())),
        ColumnData::Guid(None) => Ok(Value::Null),
        ColumnData::Guid(Some(v)) => Ok(Value::Uuid(v)),
        ColumnData::Binary(None) => Ok(Value::Null),
        ColumnData::Binary(Some(v)) => Ok(Value::Bytes(v.into_owned())),
        ColumnData::Numeric(None) => Ok(Value::Null),
        ColumnData::Numeric(Some(v)) => Ok(Value::Decimal(v.to_string())),
        ColumnData::DateTime(None) => Ok(Value::Null),
        ColumnData::DateTime(Some(v)) => {
            let dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
                    + chrono::Duration::days(v.days() as i64),
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (v.seconds_fragments() as f64 / 300.0) as u32,
                    0,
                )
                .unwrap_or_default(),
            );
            Ok(Value::DateTime(dt))
        }
        ColumnData::SmallDateTime(None) => Ok(Value::Null),
        ColumnData::SmallDateTime(Some(v)) => {
            let dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
                    + chrono::Duration::days(v.days() as i64),
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (v.seconds_fragments() as u32) * 60,
                    0,
                )
                .unwrap_or_default(),
            );
            Ok(Value::DateTime(dt))
        }
        ColumnData::DateTime2(None) => Ok(Value::Null),
        ColumnData::DateTime2(Some(v)) => {
            let date = v.date();
            let time = v.time();
            let dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap()
                    + chrono::Duration::days(date.days() as i64),
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (time.increments() / 10_000_000) as u32,
                    ((time.increments() % 10_000_000) * 100) as u32,
                )
                .unwrap_or_default(),
            );
            Ok(Value::DateTime(dt))
        }
        ColumnData::DateTimeOffset(None) => Ok(Value::Null),
        ColumnData::DateTimeOffset(Some(v)) => {
            let dt2 = v.datetime2();
            let date = dt2.date();
            let time = dt2.time();
            let naive = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap()
                    + chrono::Duration::days(date.days() as i64),
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (time.increments() / 10_000_000) as u32,
                    ((time.increments() % 10_000_000) * 100) as u32,
                )
                .unwrap_or_default(),
            );
            let utc =
                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc);
            Ok(Value::DateTimeUtc(utc))
        }
        ColumnData::Date(None) => Ok(Value::Null),
        ColumnData::Date(Some(v)) => {
            let date = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap()
                + chrono::Duration::days(v.days() as i64);
            Ok(Value::Date(date))
        }
        ColumnData::Time(None) => Ok(Value::Null),
        ColumnData::Time(Some(v)) => {
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                (v.increments() / 10_000_000) as u32,
                ((v.increments() % 10_000_000) * 100) as u32,
            )
            .unwrap_or_default();
            Ok(Value::Time(time))
        }
        ColumnData::Xml(None) => Ok(Value::Null),
        ColumnData::Xml(Some(v)) => Ok(Value::String(v.into_owned().into_string())),
    }
}

/// Container for tiberius parameter values
#[derive(Debug)]
pub(crate) enum TiberiusParam {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
}

impl tiberius::ToSql for TiberiusParam {
    fn to_sql(&self) -> ColumnData<'_> {
        match self {
            TiberiusParam::Null => ColumnData::I32(None),
            TiberiusParam::Bool(v) => ColumnData::Bit(Some(*v)),
            TiberiusParam::I16(v) => ColumnData::I16(Some(*v)),
            TiberiusParam::I32(v) => ColumnData::I32(Some(*v)),
            TiberiusParam::I64(v) => ColumnData::I64(Some(*v)),
            TiberiusParam::F32(v) => ColumnData::F32(Some(*v)),
            TiberiusParam::F64(v) => ColumnData::F64(Some(*v)),
            TiberiusParam::String(v) => {
                ColumnData::String(Some(std::borrow::Cow::Borrowed(v.as_str())))
            }
            TiberiusParam::Bytes(v) => {
                ColumnData::Binary(Some(std::borrow::Cow::Borrowed(v.as_slice())))
            }
            TiberiusParam::Uuid(v) => ColumnData::Guid(Some(*v)),
        }
    }
}

/// Convert zqlz Values to tiberius parameters
pub(crate) fn values_to_tiberius_params(values: &[Value]) -> Result<Vec<Box<TiberiusParam>>> {
    values
        .iter()
        .map(|v| {
            let param = match v {
                Value::Null => TiberiusParam::Null,
                Value::Bool(b) => TiberiusParam::Bool(*b),
                Value::Int8(i) => TiberiusParam::I16(*i as i16),
                Value::Int16(i) => TiberiusParam::I16(*i),
                Value::Int32(i) => TiberiusParam::I32(*i),
                Value::Int64(i) => TiberiusParam::I64(*i),
                Value::Float32(f) => TiberiusParam::F32(*f),
                Value::Float64(f) => TiberiusParam::F64(*f),
                Value::Decimal(d) => TiberiusParam::String(d.clone()),
                Value::String(s) => TiberiusParam::String(s.clone()),
                Value::Bytes(b) => TiberiusParam::Bytes(b.clone()),
                Value::Uuid(u) => TiberiusParam::Uuid(*u),
                Value::Date(d) => TiberiusParam::String(d.to_string()),
                Value::Time(t) => TiberiusParam::String(t.to_string()),
                Value::DateTime(dt) => TiberiusParam::String(dt.to_string()),
                Value::DateTimeUtc(dt) => TiberiusParam::String(dt.to_string()),
                Value::Json(j) => TiberiusParam::String(j.to_string()),
                Value::Array(arr) => {
                    let json = serde_json::to_string(arr).unwrap_or_default();
                    TiberiusParam::String(json)
                }
            };
            Ok(Box::new(param))
        })
        .collect()
}

impl std::fmt::Debug for MssqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlConnection")
            .field("database", &self.database)
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}
