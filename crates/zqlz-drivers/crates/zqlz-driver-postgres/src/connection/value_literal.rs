//! SQL literal formatting for `Value`s (used for cell-update `SET` clauses
//! and default-value DDL rendering).

use zqlz_core::Value;

fn value_to_pg_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::String(v) => format!("'{}'", v.replace("'", "''")),
        Value::Bytes(v) => {
            // Convert bytes to hex string for PostgreSQL bytea
            let hex: String = v.iter().map(|b| format!("{:02x}", b)).collect();
            format!("E'\\\\x{}'", hex)
        }
        Value::Uuid(v) => format!("'{}'", v),
        Value::Json(v) => format!("'{}'", v.to_string().replace("'", "''")),
        Value::DateTimeUtc(v) => format!("'{}'", v.to_rfc3339()),
        Value::Date(v) => format!("'{}'", v),
        Value::Time(v) => format!("'{}'", v),
        Value::DateTime(v) => format!("'{}'", v),
        // Emitted unquoted to preserve precision. A non-numeric payload would be
        // injected as SQL, so quote it instead and let Postgres reject the type.
        Value::Decimal(v) => {
            if Value::is_sql_numeric_literal(v) {
                v.to_string()
            } else {
                format!("'{}'", v.replace('\'', "''"))
            }
        }
        Value::Array(arr) => {
            // Format as PostgreSQL array literal: ARRAY[val1, val2, ...]
            let values: Vec<String> = arr.iter().map(value_to_pg_literal).collect();
            format!("ARRAY[{}]", values.join(", "))
        }
    }
}

fn pg_array_cast_type(column_type: Option<&str>, value: &Value) -> Option<String> {
    if let Some(column_type) = column_type {
        let normalized = Value::normalize_data_type(column_type);
        if let Some(element_type) = Value::array_element_type(&normalized) {
            return Some(format!("{}[]", pg_array_element_cast_type(&element_type)));
        }
    }

    let Value::Array(values) = value else {
        return None;
    };

    values
        .iter()
        .find(|value| !value.is_null())
        .map(pg_array_element_cast_type_for_value)
        .map(|element_type| format!("{}[]", element_type))
}

fn pg_array_element_cast_type(element_type: &str) -> String {
    match element_type {
        "text" | "string" => "text",
        "bool" | "boolean" => "bool",
        "int2" | "smallint" => "int2",
        "int4" | "int" | "integer" => "int4",
        "int8" | "bigint" => "int8",
        "float4" | "real" | "float" => "float4",
        "float8" | "double" | "double precision" => "float8",
        "numeric" | "decimal" => "numeric",
        "money" => "money",
        "bytea" | "binary" | "varbinary" => "bytea",
        "uuid" => "uuid",
        "date" => "date",
        "time" => "time",
        "timetz" | "time with time zone" => "timetz",
        "timestamp" | "datetime" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "json" => "json",
        "jsonb" => "jsonb",
        other => other,
    }
    .to_string()
}

fn pg_array_element_cast_type_for_value(value: &Value) -> String {
    match value {
        Value::Bool(_) => "bool",
        Value::Int8(_) | Value::Int16(_) => "int2",
        Value::Int32(_) => "int4",
        Value::Int64(_) => "int8",
        Value::Float32(_) => "float4",
        Value::Float64(_) => "float8",
        Value::Decimal(_) => "numeric",
        Value::Bytes(_) => "bytea",
        Value::Uuid(_) => "uuid",
        Value::Date(_) => "date",
        Value::Time(_) => "time",
        Value::DateTime(_) => "timestamp",
        Value::DateTimeUtc(_) => "timestamptz",
        Value::Json(_) => "jsonb",
        _ => "text",
    }
    .to_string()
}

fn pg_scalar_cast_type(column_type: Option<&str>) -> Option<String> {
    let column_type = column_type?.trim();
    let normalized = Value::normalize_data_type(column_type);

    if Value::is_array_data_type(&normalized) || Value::is_json_data_type(&normalized) {
        return None;
    }

    let base_type = normalized
        .split_once('(')
        .map_or(normalized.as_str(), |(base, _)| base);
    let is_builtin_without_cast = matches!(
        base_type,
        "text"
            | "varchar"
            | "char"
            | "bpchar"
            | "name"
            | "citext"
            | "character varying"
            | "character"
            | "bool"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "uuid"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"
            | "bytea"
    );

    if is_builtin_without_cast {
        return None;
    }

    Some(column_type.to_string())
}

pub(crate) fn value_to_pg_literal_for_type(value: &Value, column_type: Option<&str>) -> String {
    match value {
        Value::Array(values) => {
            let values: Vec<String> = values.iter().map(value_to_pg_literal).collect();
            let array_literal = format!("ARRAY[{}]", values.join(", "));
            match pg_array_cast_type(column_type, value) {
                Some(array_type) => format!("{}::{}", array_literal, array_type),
                None => array_literal,
            }
        }
        Value::String(_) => match pg_scalar_cast_type(column_type) {
            Some(column_type) => format!("{}::{}", value_to_pg_literal(value), column_type),
            None => value_to_pg_literal(value),
        },
        _ => value_to_pg_literal(value),
    }
}

pub(crate) fn row_column_type<'a>(column_types: &'a [(String, String)], column_name: &str) -> Option<&'a str> {
    column_types
        .iter()
        .find(|(name, _)| name == column_name)
        .map(|(_, data_type)| data_type.as_str())
}
