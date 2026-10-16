//! Conversion of Redis wire-protocol values into ZQLZ query result rows

use super::parsing::bytes_to_zqlz_value;
use zqlz_core::{ColumnMeta, Row, Value};

/// Convert Redis value to query result rows
pub(super) fn redis_value_to_rows(value: &redis::Value) -> (Vec<ColumnMeta>, Vec<Row>) {
    let columns = vec![
        ColumnMeta {
            name: "key".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal: 0,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        },
        ColumnMeta {
            name: "value".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal: 1,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        },
    ];

    let column_names = vec!["key".to_string(), "value".to_string()];

    let rows = match value {
        redis::Value::Nil => vec![],
        redis::Value::Int(n) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Int64(*n)],
            )]
        }
        redis::Value::BulkString(data) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, bytes_to_zqlz_value(data)],
            )]
        }
        redis::Value::Array(arr) => {
            // Flatten nested arrays for commands like SCAN that return [cursor, [keys...]]
            let mut rows = Vec::new();
            for (i, v) in arr.iter().enumerate() {
                match v {
                    // If the element is itself an array, flatten it into individual rows
                    redis::Value::Array(inner_arr) => {
                        for inner_v in inner_arr {
                            let val = redis_value_to_zqlz_value(inner_v);
                            rows.push(Row::new(column_names.clone(), vec![Value::Null, val]));
                        }
                    }
                    // Otherwise, treat as a normal element with index as key
                    _ => {
                        let key = Value::String(i.to_string());
                        let val = redis_value_to_zqlz_value(v);
                        rows.push(Row::new(column_names.clone(), vec![key, val]));
                    }
                }
            }
            rows
        }
        redis::Value::Okay => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String("OK".to_string())],
            )]
        }
        redis::Value::SimpleString(s) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(s.clone())],
            )]
        }
        redis::Value::Map(map) => map
            .iter()
            .map(|(k, v)| {
                let key = redis_value_to_zqlz_value(k);
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
        redis::Value::Set(set) => set
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let key = Value::String(i.to_string());
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
        redis::Value::Double(d) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Float64(*d)],
            )]
        }
        redis::Value::Boolean(b) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::Bool(*b)],
            )]
        }
        redis::Value::BigNumber(bn) => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(format!("{:?}", bn))],
            )]
        }
        redis::Value::VerbatimString { format: _, text } => {
            vec![Row::new(
                column_names.clone(),
                vec![Value::Null, Value::String(text.clone())],
            )]
        }
        redis::Value::ServerError(err) => {
            vec![Row::new(
                column_names.clone(),
                vec![
                    Value::String("error".to_string()),
                    Value::String(format!("{:?}", err)),
                ],
            )]
        }
        redis::Value::Attribute {
            data,
            attributes: _,
        } => redis_value_to_rows(data).1,
        redis::Value::Push { kind: _, data } => data
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let key = Value::String(i.to_string());
                let val = redis_value_to_zqlz_value(v);
                Row::new(column_names.clone(), vec![key, val])
            })
            .collect(),
    };

    (columns, rows)
}

/// Convert a single Redis value to ZQLZ Value
pub(super) fn redis_value_to_zqlz_value(value: &redis::Value) -> Value {
    match value {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(n) => Value::Int64(*n),
        redis::Value::BulkString(data) => bytes_to_zqlz_value(data),
        redis::Value::Okay => Value::String("OK".to_string()),
        redis::Value::SimpleString(s) => Value::String(s.clone()),
        redis::Value::Double(d) => Value::Float64(*d),
        redis::Value::Boolean(b) => Value::Bool(*b),
        redis::Value::Array(arr) => {
            let json_arr: Vec<serde_json::Value> = arr
                .iter()
                .map(|v| match redis_value_to_zqlz_value(v) {
                    Value::String(s) => serde_json::Value::String(s),
                    Value::Int64(n) => serde_json::Value::Number(n.into()),
                    Value::Float64(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Value::Bool(b) => serde_json::Value::Bool(b),
                    Value::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                })
                .collect();
            Value::String(serde_json::to_string(&json_arr).unwrap_or_default())
        }
        redis::Value::Map(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                let key = match redis_value_to_zqlz_value(k) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                let val = match redis_value_to_zqlz_value(v) {
                    Value::String(s) => serde_json::Value::String(s),
                    Value::Int64(n) => serde_json::Value::Number(n.into()),
                    Value::Float64(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Value::Bool(b) => serde_json::Value::Bool(b),
                    Value::Null => serde_json::Value::Null,
                    _ => serde_json::Value::Null,
                };
                json_map.insert(key, val);
            }
            Value::String(serde_json::to_string(&json_map).unwrap_or_default())
        }
        redis::Value::BigNumber(bn) => Value::String(format!("{:?}", bn)),
        redis::Value::VerbatimString { format: _, text } => Value::String(text.clone()),
        redis::Value::ServerError(err) => Value::String(format!("ERROR: {:?}", err)),
        _ => Value::Null,
    }
}

