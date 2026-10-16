//! Row-value decoding (`tokio-postgres` `Row` -> `zqlz_core::Value`) and
//! identifier/table-name escaping.

use std::collections::{BTreeSet, HashMap};

use tokio_postgres::{Client, Row as PgRow};
use zqlz_core::{ColumnMeta, Result, Row, Value};

use super::pg_value::{
    PgArrayValue, PgBinaryDisplayString, PgCompositeValue, PgFallbackBytes, PgFallbackString,
    PgInetString, PgNumericString, PgTemporal, PgTstzRangeString, PgTsvectorString,
};
use super::money::{MONEY_PROBE_SQL, MoneyFormat, MoneyFormatCache};
use super::value_format::{is_postgres_composite_array_type, is_postgres_composite_type};

fn may_contain_money(ty: &postgres_types::Type) -> bool {
    use postgres_types::Kind;
    ty.name() == "money"
        || match ty.kind() {
            Kind::Array(inner) | Kind::Domain(inner) => may_contain_money(inner),
            Kind::Composite(fields) => fields.iter().any(|field| may_contain_money(field.type_())),
            _ => false,
        }
}

/// The session's `money` layout, probed from the server on first use and then
/// cached on the connection. Only results that can hold money pay for the probe;
/// a failed probe falls back to the C-locale form without being cached.
pub(crate) async fn money_format_for(
    client: &Client,
    cache: &MoneyFormatCache,
    columns: &[tokio_postgres::Column],
) -> MoneyFormat {
    if !columns.iter().any(|column| may_contain_money(column.type_())) {
        return MoneyFormat::default();
    }
    if let Some(format) = cache.lock().unwrap_or_else(|e| e.into_inner()).clone() {
        return format;
    }

    let probed = match client.query_one(MONEY_PROBE_SQL, &[]).await {
        Ok(row) => match (
            row.try_get::<_, String>(0),
            row.try_get::<_, String>(1),
            row.try_get::<_, String>(2),
        ) {
            (Ok(large), Ok(negative), Ok(small)) => {
                MoneyFormat::parse_probe(&large, &negative, &small)
            }
            _ => None,
        },
        Err(error) => {
            tracing::debug!(%error, "money format probe failed");
            None
        }
    };
    match probed {
        Some(format) => {
            *cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(format.clone());
            format
        }
        None => MoneyFormat::default(),
    }
}

/// Convert PostgreSQL row value to our Value type
///
/// A value that fails to decode is never shown as NULL: it is logged and kept
/// as its raw bytes so the cell still tells the user something is there.
pub(crate) fn postgres_to_value(row: &PgRow, idx: usize) -> Result<Value> {
    match decode_value(row, idx) {
        Ok(value) => Ok(value),
        Err(error) => {
            let column = &row.columns()[idx];
            tracing::warn!(
                column = column.name(),
                pg_type = column.type_().name(),
                %error,
                "failed to decode PostgreSQL value; showing raw bytes"
            );
            Ok(row
                .try_get::<_, Option<PgFallbackBytes>>(idx)
                .ok()
                .flatten()
                .map(|value| Value::Bytes(value.0))
                .unwrap_or(Value::Null))
        }
    }
}

fn decode_value(row: &PgRow, idx: usize) -> std::result::Result<Value, tokio_postgres::Error> {
    let col = &row.columns()[idx];
    let type_name = col.type_().name();

    // Composite types have user-chosen names, so they are matched by kind and
    // must be checked before the name-based arms.
    if is_postgres_composite_type(col.type_()) || is_postgres_composite_array_type(col.type_()) {
        return Ok(row
            .try_get::<_, Option<PgCompositeValue>>(idx)?
            .map(|value| value.0)
            .unwrap_or(Value::Null));
    }

    // Try to extract value based on type
    let value = match type_name {
        "bool" => row
            .try_get::<_, Option<bool>>(idx)?
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" | "smallint" => row
            .try_get::<_, Option<i16>>(idx)?
            .map(Value::Int16)
            .unwrap_or(Value::Null),
        "int4" | "int" | "integer" => row
            .try_get::<_, Option<i32>>(idx)?
            .map(Value::Int32)
            .unwrap_or(Value::Null),
        "int8" | "bigint" => row
            .try_get::<_, Option<i64>>(idx)?
            .map(Value::Int64)
            .unwrap_or(Value::Null),
        "float4" | "real" => row
            .try_get::<_, Option<f32>>(idx)?
            .map(Value::Float32)
            .unwrap_or(Value::Null),
        "float8" | "double precision" => row
            .try_get::<_, Option<f64>>(idx)?
            .map(Value::Float64)
            .unwrap_or(Value::Null),
        // `"char"` is a single byte on the wire, which String refuses to decode.
        "char" => row
            .try_get::<_, Option<i8>>(idx)?
            .map(|byte| Value::String(char::from(byte as u8).to_string()))
            .unwrap_or(Value::Null),
        "text" | "varchar" | "bpchar" | "name" => row
            .try_get::<_, Option<String>>(idx)?
            .map(Value::String)
            .unwrap_or(Value::Null),
        "bytea" => row
            .try_get::<_, Option<Vec<u8>>>(idx)?
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        "uuid" => row
            .try_get::<_, Option<uuid::Uuid>>(idx)?
            .map(Value::Uuid)
            .unwrap_or(Value::Null),
        "json" | "jsonb" => row
            .try_get::<_, Option<serde_json::Value>>(idx)?
            .map(Value::Json)
            .unwrap_or(Value::Null),
        "date" | "timestamp" | "timestamptz" => row
            .try_get::<_, Option<PgTemporal>>(idx)?
            .map(|value| value.0)
            .unwrap_or(Value::Null),
        "time" => row
            .try_get::<_, Option<chrono::NaiveTime>>(idx)?
            .map(Value::Time)
            .unwrap_or(Value::Null),
        "numeric" | "decimal" => row
            .try_get::<_, Option<PgNumericString>>(idx)?
            .map(|value| Value::Decimal(value.0))
            .unwrap_or(Value::Null),
        "inet" | "cidr" => row
            .try_get::<_, Option<PgInetString>>(idx)?
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "tsvector" => row
            .try_get::<_, Option<PgTsvectorString>>(idx)?
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "tstzrange" => row
            .try_get::<_, Option<PgTstzRangeString>>(idx)?
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "hstore" | "macaddr" | "macaddr8" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" | "int4range" | "int8range" | "numrange" | "tsrange"
        | "daterange" | "money" | "interval" | "timetz" | "bit" | "varbit" | "oid" | "xid"
        | "cid" | "regclass" | "regtype" | "regproc" | "regprocedure" | "regoper"
        | "regoperator" | "regnamespace" | "regrole" | "pg_lsn" | "vector" | "halfvec"
        | "sparsevec" | "int4multirange" | "int8multirange" | "nummultirange"
        | "tsmultirange" | "tstzmultirange" | "datemultirange" | "tsquery" | "txid_snapshot"
        | "pg_snapshot" => row
            .try_get::<_, Option<PgBinaryDisplayString>>(idx)?
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        // Array types: one decoder handles NULL elements and any dimensionality.
        _ if matches!(col.type_().kind(), postgres_types::Kind::Array(_)) => row
            .try_get::<_, Option<PgArrayValue>>(idx)?
            .map(|value| value.0)
            .unwrap_or(Value::Null),
        _ => {
            // Fallback for custom PostgreSQL types: enums and text-like domains decode as
            // UTF-8. Types with a non-UTF-8 binary payload are preserved as raw bytes so
            // real values never silently collapse to NULL.
            match row.try_get::<_, Option<PgFallbackString>>(idx) {
                Ok(value) => value.map(|value| Value::String(value.0)).unwrap_or(Value::Null),
                Err(_) => row
                    .try_get::<_, Option<PgFallbackBytes>>(idx)
                    .ok()
                    .flatten()
                    .map(|value| Value::Bytes(value.0))
                    .unwrap_or(Value::Null),
            }
        }
    };

    Ok(value)
}

/// OID alias types that psql shows by name but the binary protocol sends as a bare OID.
const OID_ALIAS_TYPES: &[&str] = &[
    "regclass",
    "regtype",
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regnamespace",
    "regrole",
];

/// Replace the OIDs decoded for `regclass`-style columns with the names the
/// server prints for them (`19306` -> `zqlz_lab.orders_2025`), one lookup per
/// such column. Values stay as OIDs if the lookup fails.
pub(crate) async fn resolve_oid_alias_names(
    client: &Client,
    columns: &[ColumnMeta],
    rows: &mut [Row],
) {
    for (idx, column) in columns.iter().enumerate() {
        let Some(type_name) = OID_ALIAS_TYPES
            .iter()
            .find(|name| **name == column.data_type)
        else {
            continue;
        };
        let oids: Vec<u32> = rows
            .iter()
            .filter_map(|row| match row.values.get(idx) {
                Some(Value::String(oid)) => oid.parse().ok(),
                _ => None,
            })
            .collect::<BTreeSet<u32>>()
            .into_iter()
            .collect();
        if oids.is_empty() {
            continue;
        }
        // Casting an OID to an alias type never errors (unknown OIDs print as
        // numbers), so this can't abort an open transaction.
        let sql = format!("SELECT o, o::{type_name}::text FROM unnest($1::oid[]) AS o");
        let Ok(name_rows) = client.query(&sql, &[&oids]).await else {
            continue;
        };
        let names: HashMap<u32, String> = name_rows
            .iter()
            .filter_map(|row| Some((row.try_get(0).ok()?, row.try_get(1).ok()?)))
            .collect();
        for row in rows.iter_mut() {
            if let Some(Value::String(value)) = row.values.get_mut(idx)
                && let Some(name) = value.parse().ok().and_then(|oid: u32| names.get(&oid))
            {
                *value = name.clone();
            }
        }
    }
}

/// Escape a PostgreSQL identifier (column name, etc.)
pub(crate) fn escape_identifier_pg(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

/// Escape a table name which may include schema (e.g., "schema.table")
pub(crate) fn escape_table_name_pg(table_name: &str) -> String {
    if table_name.contains('.') {
        // Handle schema.table format
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            format!(
                "{}.{}",
                escape_identifier_pg(parts[0]),
                escape_identifier_pg(parts[1])
            )
        } else {
            escape_identifier_pg(table_name)
        }
    } else {
        escape_identifier_pg(table_name)
    }
}

