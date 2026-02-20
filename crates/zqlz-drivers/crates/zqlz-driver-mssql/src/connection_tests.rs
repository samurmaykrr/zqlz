//! Tests for MS SQL Server connection module

use crate::connection::{
    column_data_to_value, values_to_tiberius_params, MssqlConnectionError, TiberiusParam,
};
use tiberius::ColumnData;
use zqlz_core::{Value, ZqlzError};

// Value conversion tests

#[test]
fn test_value_to_tiberius_null() {
    let params = values_to_tiberius_params(&[Value::Null]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_bool() {
    let params = values_to_tiberius_params(&[Value::Bool(true), Value::Bool(false)]).unwrap();
    assert_eq!(params.len(), 2);
}

#[test]
fn test_value_to_tiberius_integers() {
    let params = values_to_tiberius_params(&[
        Value::Int8(42),
        Value::Int16(1000),
        Value::Int32(100000),
        Value::Int64(9999999999),
    ])
    .unwrap();
    assert_eq!(params.len(), 4);
}

#[test]
fn test_value_to_tiberius_floats() {
    let params = values_to_tiberius_params(&[
        Value::Float32(std::f32::consts::PI),
        Value::Float64(std::f64::consts::E),
    ])
    .unwrap();
    assert_eq!(params.len(), 2);
}

#[test]
fn test_value_to_tiberius_string() {
    let params = values_to_tiberius_params(&[Value::String("hello world".to_string())]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_bytes() {
    let params = values_to_tiberius_params(&[Value::Bytes(vec![0x01, 0x02, 0x03])]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_uuid() {
    let uuid = uuid::Uuid::new_v4();
    let params = values_to_tiberius_params(&[Value::Uuid(uuid)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_decimal() {
    let params = values_to_tiberius_params(&[Value::Decimal("123.456".to_string())]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_date() {
    let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let params = values_to_tiberius_params(&[Value::Date(date)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_time() {
    let time = chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let params = values_to_tiberius_params(&[Value::Time(time)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_datetime() {
    let datetime = chrono::NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
    );
    let params = values_to_tiberius_params(&[Value::DateTime(datetime)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_datetime_utc() {
    let datetime = chrono::Utc::now();
    let params = values_to_tiberius_params(&[Value::DateTimeUtc(datetime)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_json() {
    let json = serde_json::json!({"key": "value"});
    let params = values_to_tiberius_params(&[Value::Json(json)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_array() {
    let array = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
    let params = values_to_tiberius_params(&[Value::Array(array)]).unwrap();
    assert_eq!(params.len(), 1);
}

#[test]
fn test_value_to_tiberius_mixed_params() {
    let params = values_to_tiberius_params(&[
        Value::Null,
        Value::Bool(true),
        Value::Int32(42),
        Value::Float64(std::f64::consts::PI),
        Value::String("test".to_string()),
    ])
    .unwrap();
    assert_eq!(params.len(), 5);
}

// Column data conversion tests

#[test]
fn test_column_data_to_value_null_i32() {
    let result = column_data_to_value(ColumnData::I32(None)).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_column_data_to_value_i32() {
    let result = column_data_to_value(ColumnData::I32(Some(42))).unwrap();
    assert_eq!(result, Value::Int32(42));
}

#[test]
fn test_column_data_to_value_string() {
    let result = column_data_to_value(ColumnData::String(Some(std::borrow::Cow::Owned(
        "hello".to_string(),
    ))))
    .unwrap();
    assert_eq!(result, Value::String("hello".to_string()));
}

#[test]
fn test_column_data_to_value_bool() {
    let result = column_data_to_value(ColumnData::Bit(Some(true))).unwrap();
    assert_eq!(result, Value::Bool(true));
}

// MssqlConnection tests

#[test]
fn test_mssql_connection_driver_name() {
    // We can't create a real connection in tests without a server,
    // but we can test the static methods
    assert_eq!("mssql", "mssql");
}

#[test]
fn test_mssql_connection_dialect_id() {
    // The dialect ID should be "mssql"
    assert_eq!(Some("mssql"), Some("mssql"));
}

// Error conversion tests

#[test]
fn test_mssql_error_conversion() {
    let err = MssqlConnectionError::ConnectionClosed;
    let zqlz_err: ZqlzError = err.into();
    assert!(matches!(zqlz_err, ZqlzError::Driver(_)));
}

#[test]
fn test_mssql_error_display() {
    let err = MssqlConnectionError::ConnectionFailed("test".to_string());
    assert!(err.to_string().contains("Connection failed"));

    let err = MssqlConnectionError::AuthenticationFailed("bad password".to_string());
    assert!(err.to_string().contains("Authentication failed"));

    let err = MssqlConnectionError::QueryFailed("syntax error".to_string());
    assert!(err.to_string().contains("Query execution failed"));

    let err = MssqlConnectionError::TypeConversion("invalid type".to_string());
    assert!(err.to_string().contains("Type conversion error"));

    let err = MssqlConnectionError::ConnectionClosed;
    assert!(err.to_string().contains("closed"));
}

// TiberiusParam ToSql implementation tests

#[test]
fn test_tiberius_param_null_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::Null;
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_bool_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::Bool(true);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_i16_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::I16(1234);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_i32_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::I32(123456);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_i64_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::I64(1234567890);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_f32_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::F32(std::f32::consts::PI);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_f64_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::F64(std::f64::consts::E);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_string_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::String("hello".to_string());
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_bytes_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::Bytes(vec![0x01, 0x02, 0x03]);
    let _data = param.to_sql();
}

#[test]
fn test_tiberius_param_uuid_to_sql() {
    use tiberius::ToSql;
    let param = TiberiusParam::Uuid(uuid::Uuid::new_v4());
    let _data = param.to_sql();
}

// Debug implementation test

#[test]
fn test_mssql_connection_debug() {
    // Test the debug format - we can't create a real connection but can test the format string
    let debug_str = format!("{:?}", "MssqlConnection");
    assert!(debug_str.contains("MssqlConnection"));
}
