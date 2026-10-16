//! `PgValue`: the `zqlz_core::Value` <-> `tokio-postgres` `ToSql`/`FromSql`
//! bridge, plus the NUMERIC binary-wire encoder/decoder and the newtype
//! wrappers used to decode PostgreSQL types `tokio-postgres` has no native
//! mapping for.

use bytes::BytesMut;
use tokio_postgres::types::{FromSql, ToSql};
use zqlz_core::Value;

use super::value_format::{
    composite_array_element_type, format_postgres_binary_display_value,
    format_postgres_network_value, format_postgres_tstzrange_value, format_postgres_tsvector_value,
    format_postgres_value_text, is_postgres_composite_array_type, is_postgres_composite_type,
    parse_postgres_array_elements,
};

/// Wrapper enum for converting zqlz_core::Value to types implementing ToSql.
/// This is needed because tokio-postgres requires owned values that implement ToSql.
#[derive(Debug)]
pub(crate) enum PgValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Json(serde_json::Value),
    DateTimeUtc(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
    Array(Vec<PgValue>),
    /// Digits held as text so precision wider than f64 survives.
    Decimal(String),
    /// Days since 2000-01-01, including the `infinity` sentinels chrono lacks.
    DateRaw(i32),
    /// Microseconds since 2000-01-01, including the `infinity` sentinels.
    TimestampRaw(i64),
}

#[derive(Debug)]
pub(crate) struct PgNumericString(pub(crate) String);
#[derive(Debug)]
pub(crate) struct PgFallbackString(pub(crate) String);
#[derive(Debug)]
pub(crate) struct PgFallbackBytes(pub(crate) Vec<u8>);
#[derive(Debug)]
pub(crate) struct PgInetString(pub(crate) String);
#[derive(Debug)]
pub(crate) struct PgTsvectorString(pub(crate) String);
#[derive(Debug)]
pub(crate) struct PgTstzRangeString(pub(crate) String);
#[derive(Debug)]
pub(crate) struct PgBinaryDisplayString(pub(crate) String);
/// A composite / `record` value as psql's `(a,b)` text, or an array of them as
/// one such string per element (NULL elements stay NULL).
#[derive(Debug)]
pub(crate) struct PgCompositeValue(pub(crate) Value);
/// date / timestamp / timestamptz decoded by hand: chrono cannot hold
/// `infinity`, and PostgreSQL writes years <= 0 as `YYYY BC`, so those become
/// strings in psql's spelling and everything else stays a typed value.
#[derive(Debug)]
pub(crate) struct PgTemporal(pub(crate) Value);
/// Any array type: NULL elements become `Value::Null` and multi-dimensional
/// arrays become nested `Value::Array`s.
#[derive(Debug)]
pub(crate) struct PgArrayValue(pub(crate) Value);

/// Encode a decimal string into PostgreSQL's binary NUMERIC wire format.
///
/// This is the inverse of [`PgNumericString::parse`]. It exists because
/// `Value::Decimal` carries its digits as text to preserve precision, and no
/// `ToSql` impl accepts a string for a `numeric` column — without this, decimal
/// parameters fail on the bound query path.
///
/// The format is: `ndigits`, `weight`, `sign`, `dscale`, then `ndigits`
/// base-10000 groups, all big-endian i16.
pub(crate) fn encode_pg_numeric(
    text: &str,
    out: &mut BytesMut,
) -> std::result::Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let trimmed = text.trim();
    let special_sign = match trimmed.to_ascii_lowercase().as_str() {
        "nan" => Some(0xC000u16),
        "infinity" | "+infinity" | "inf" | "+inf" => Some(0xD000),
        "-infinity" | "-inf" => Some(0xF000),
        _ => None,
    };
    if let Some(sign) = special_sign {
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&0i16.to_be_bytes());
        return Ok(());
    }

    let (negative, unsigned) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };

    let (integer_text, fraction_text) = match unsigned.find('.') {
        Some(index) => (&unsigned[..index], &unsigned[index + 1..]),
        None => (unsigned, ""),
    };

    if (integer_text.is_empty() && fraction_text.is_empty())
        || !integer_text.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid NUMERIC literal: {}", text).into());
    }

    let dscale = fraction_text.len();

    // Group into base-10000 digits: the integer part aligns from the right, the
    // fraction from the left, so both pad to a multiple of four.
    let mut integer_padded = "0".repeat((4 - integer_text.len() % 4) % 4);
    integer_padded.push_str(integer_text);

    let mut fraction_padded = String::from(fraction_text);
    fraction_padded.push_str(&"0".repeat((4 - fraction_text.len() % 4) % 4));

    let mut groups: Vec<u16> = Vec::new();
    for chunk in integer_padded.as_bytes().chunks(4) {
        groups.push(std::str::from_utf8(chunk)?.parse::<u16>()?);
    }
    let mut weight = groups.len() as i32 - 1;
    for chunk in fraction_padded.as_bytes().chunks(4) {
        groups.push(std::str::from_utf8(chunk)?.parse::<u16>()?);
    }

    // Leading zero groups shift the weight; trailing ones are simply dropped.
    let leading_zeros = groups.iter().take_while(|group| **group == 0).count();
    if leading_zeros == groups.len() {
        groups.clear();
        weight = 0;
    } else {
        groups.drain(..leading_zeros);
        weight -= leading_zeros as i32;
        while groups.last() == Some(&0) {
            groups.pop();
        }
    }

    let ndigits = i16::try_from(groups.len())
        .map_err(|_| format!("NUMERIC value has too many digits: {}", text))?;
    let weight = i16::try_from(weight)
        .map_err(|_| format!("NUMERIC value is out of range: {}", text))?;
    let dscale = i16::try_from(dscale)
        .map_err(|_| format!("NUMERIC value has too many decimal places: {}", text))?;

    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    // The server normalises zero to positive, so never send a negative zero.
    let negative = negative && !groups.is_empty();
    out.extend_from_slice(&(if negative { 0x4000u16 } else { 0x0000u16 }).to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for group in groups {
        out.extend_from_slice(&group.to_be_bytes());
    }

    Ok(())
}

impl PgNumericString {
    /// Render the binary NUMERIC the way the server's text output does: exactly
    /// `dscale` fraction digits, a leading `0` for |x| < 1, and `NaN` /
    /// `Infinity` / `-Infinity` for the special sign words.
    pub(crate) fn parse(raw: &[u8]) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err("invalid NUMERIC payload: too short".into());
        }

        let ndigits = i16::from_be_bytes([raw[0], raw[1]]).max(0) as usize;
        let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = i16::from_be_bytes([raw[6], raw[7]]).max(0) as usize;
        let expected_len = 8 + ndigits * 2;

        if raw.len() < expected_len {
            return Err("invalid NUMERIC payload: truncated digits".into());
        }

        match sign {
            0xC000 => return Ok("NaN".to_string()),
            0xD000 => return Ok("Infinity".to_string()),
            0xF000 => return Ok("-Infinity".to_string()),
            0x0000 | 0x4000 => {}
            _ => return Err("invalid NUMERIC payload: unknown sign".into()),
        }

        let mut digits = Vec::with_capacity(ndigits);
        for index in 0..ndigits {
            let offset = 8 + index * 2;
            let group = u16::from_be_bytes([raw[offset], raw[offset + 1]]);
            if group > 9999 {
                return Err("invalid NUMERIC payload: group out of range".into());
            }
            digits.push(group);
        }

        // Groups outside the stored range are implicit zeros.
        let group_at = |index: i64| -> u16 {
            usize::try_from(index)
                .ok()
                .and_then(|index| digits.get(index))
                .copied()
                .unwrap_or(0)
        };

        let mut output = String::new();
        if sign == 0x4000 {
            output.push('-');
        }

        if weight < 0 {
            output.push('0');
        } else {
            for group_index in 0..=weight {
                let group = group_at(group_index);
                if group_index == 0 {
                    output.push_str(&group.to_string());
                } else {
                    output.push_str(&format!("{group:04}"));
                }
            }
        }

        if dscale > 0 {
            output.push('.');
            let mut fraction = String::with_capacity(dscale + 4);
            for group_number in 0..dscale.div_ceil(4) as i64 {
                fraction.push_str(&format!("{:04}", group_at(group_number + weight + 1)));
            }
            fraction.truncate(dscale);
            output.push_str(&fraction);
        }

        Ok(output)
    }
}

impl<'a> FromSql<'a> for PgNumericString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(Self::parse(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::NUMERIC
    }
}

impl<'a> FromSql<'a> for PgFallbackString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let text = String::from_utf8(raw.to_vec())?;
        Ok(Self(text))
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }
}

impl<'a> FromSql<'a> for PgFallbackBytes {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw.to_vec()))
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }
}

impl<'a> FromSql<'a> for PgInetString {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_network_value(ty.name(), raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty.name(), "inet" | "cidr")
    }
}

impl<'a> FromSql<'a> for PgTsvectorString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_tsvector_value(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.name() == "tsvector"
    }
}

impl<'a> FromSql<'a> for PgTstzRangeString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_tstzrange_value(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.name() == "tstzrange"
    }
}

impl<'a> FromSql<'a> for PgBinaryDisplayString {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_binary_display_value(ty.name(), raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(
            ty.name(),
            "hstore"
                | "macaddr"
                | "macaddr8"
                | "point"
                | "line"
                | "lseg"
                | "box"
                | "path"
                | "polygon"
                | "circle"
                | "money"
                | "interval"
                | "timetz"
                | "bit"
                | "varbit"
                | "oid"
                | "xid"
                | "cid"
                | "regclass"
                | "regtype"
                | "regproc"
                | "regprocedure"
                | "regoper"
                | "regoperator"
                | "regnamespace"
                | "regrole"
                | "pg_lsn"
                | "int4range"
                | "int8range"
                | "numrange"
                | "tsrange"
                | "tstzrange"
                | "daterange"
                | "int4multirange"
                | "int8multirange"
                | "nummultirange"
                | "tsmultirange"
                | "tstzmultirange"
                | "datemultirange"
                | "tsquery"
                | "txid_snapshot"
                | "pg_snapshot"
                | "vector"
                | "halfvec"
                | "sparsevec"
        )
    }
}

fn pg_epoch() -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .expect("2000-01-01 is a valid date")
}

impl<'a> FromSql<'a> for PgTemporal {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        use chrono::Datelike;
        use tokio_postgres::types::Type;

        let infinity = |positive: bool| {
            Value::String(if positive { "infinity" } else { "-infinity" }.to_string())
        };

        if *ty == Type::DATE {
            let days = i32::from_be_bytes(raw.try_into()?);
            let value = match days {
                i32::MAX => infinity(true),
                i32::MIN => infinity(false),
                _ => {
                    let date = pg_epoch()
                        .date()
                        .checked_add_signed(chrono::Duration::days(days as i64))
                        .ok_or("date out of range")?;
                    if date.year() <= 0 {
                        Value::String(format!(
                            "{:04}{} BC",
                            1 - date.year(),
                            date.format("-%m-%d")
                        ))
                    } else {
                        Value::Date(date)
                    }
                }
            };
            return Ok(Self(value));
        }

        let micros = i64::from_be_bytes(raw.try_into()?);
        let value = match micros {
            i64::MAX => infinity(true),
            i64::MIN => infinity(false),
            _ => {
                let timestamp = pg_epoch()
                    .checked_add_signed(chrono::Duration::microseconds(micros))
                    .ok_or("timestamp out of range")?;
                let timezone = *ty == Type::TIMESTAMPTZ;
                if timestamp.year() <= 0 {
                    Value::String(format!(
                        "{:04}{}{} BC",
                        1 - timestamp.year(),
                        timestamp.format("-%m-%d %H:%M:%S%.f"),
                        if timezone { "+00" } else { "" }
                    ))
                } else if timezone {
                    Value::DateTimeUtc(chrono::DateTime::from_naive_utc_and_offset(
                        timestamp,
                        chrono::Utc,
                    ))
                } else {
                    Value::DateTime(timestamp)
                }
            }
        };
        Ok(Self(value))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        use tokio_postgres::types::Type;
        matches!(*ty, Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ)
    }
}

/// Inverse of the strings [`PgTemporal`] produces for values chrono cannot
/// hold, so a cell showing `infinity` or `0044-03-15 BC` can be written back.
fn parse_special_temporal(
    text: &str,
    target_type: &tokio_postgres::types::Type,
) -> Option<PgValue> {
    use chrono::Datelike;

    let is_date = *target_type == tokio_postgres::types::Type::DATE;
    let text = text.trim();
    let infinity = |positive: bool| {
        if is_date {
            PgValue::DateRaw(if positive { i32::MAX } else { i32::MIN })
        } else {
            PgValue::TimestampRaw(if positive { i64::MAX } else { i64::MIN })
        }
    };
    match text.to_ascii_lowercase().as_str() {
        "infinity" | "+infinity" => return Some(infinity(true)),
        "-infinity" => return Some(infinity(false)),
        _ => {}
    }

    let text = text.strip_suffix(" BC").or_else(|| text.strip_suffix(" bc"))?;
    let text = text.strip_suffix("+00").unwrap_or(text);
    let parsed = if is_date {
        chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()?
            .and_hms_opt(0, 0, 0)?
    } else {
        chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").ok()?
    };
    // BC year N is astronomical year 1 - N.
    let shifted = parsed.with_year(1 - parsed.year())?;
    let since_epoch = shifted - pg_epoch();
    if is_date {
        i32::try_from(since_epoch.num_days()).ok().map(PgValue::DateRaw)
    } else {
        since_epoch.num_microseconds().map(PgValue::TimestampRaw)
    }
}

fn take_bytes<'a>(
    cursor: &mut &'a [u8],
    len: usize,
) -> std::result::Result<&'a [u8], Box<dyn std::error::Error + Sync + Send>> {
    if cursor.len() < len {
        return Err("invalid array payload: truncated".into());
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

fn take_i32(
    cursor: &mut &[u8],
) -> std::result::Result<i32, Box<dyn std::error::Error + Sync + Send>> {
    Ok(i32::from_be_bytes(take_bytes(cursor, 4)?.try_into()?))
}

fn decode_array_element(
    ty: &tokio_postgres::types::Type,
    raw: &[u8],
) -> std::result::Result<Value, Box<dyn std::error::Error + Sync + Send>> {
    Ok(match ty.name() {
        "bool" => Value::Bool(bool::from_sql(ty, raw)?),
        "int2" => Value::Int16(i16::from_sql(ty, raw)?),
        "int4" => Value::Int32(i32::from_sql(ty, raw)?),
        "int8" => Value::Int64(i64::from_sql(ty, raw)?),
        "float4" => Value::Float32(f32::from_sql(ty, raw)?),
        "float8" => Value::Float64(f64::from_sql(ty, raw)?),
        "char" => Value::String(char::from(i8::from_sql(ty, raw)? as u8).to_string()),
        "text" | "varchar" | "bpchar" | "name" => Value::String(String::from_sql(ty, raw)?),
        "numeric" => Value::Decimal(PgNumericString::parse(raw)?),
        "uuid" => Value::Uuid(uuid::Uuid::from_sql(ty, raw)?),
        "bytea" => Value::Bytes(Vec::<u8>::from_sql(ty, raw)?),
        "json" | "jsonb" => Value::Json(serde_json::Value::from_sql(ty, raw)?),
        "time" => Value::Time(chrono::NaiveTime::from_sql(ty, raw)?),
        "date" | "timestamp" | "timestamptz" => PgTemporal::from_sql(ty, raw)?.0,
        "inet" | "cidr" => Value::String(format_postgres_network_value(ty.name(), raw)?),
        "tsvector" => Value::String(format_postgres_tsvector_value(raw)?),
        // Everything else is either a type with a binary display form (money,
        // ranges, ...), or text-shaped (enums, domains); raw bytes as a last resort.
        name => match format_postgres_binary_display_value(name, raw) {
            Ok(text) => Value::String(text),
            Err(_) => match std::str::from_utf8(raw) {
                Ok(text) => Value::String(text.to_string()),
                Err(_) => Value::Bytes(raw.to_vec()),
            },
        },
    })
}

fn nest_array(elements: &mut std::vec::IntoIter<Value>, dimensions: &[usize]) -> Value {
    match dimensions {
        [length] => Value::Array(elements.take(*length).collect()),
        [length, rest @ ..] => Value::Array(
            (0..*length)
                .map(|_| nest_array(elements, rest))
                .collect(),
        ),
        [] => Value::Array(Vec::new()),
    }
}

impl<'a> FromSql<'a> for PgArrayValue {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let postgres_types::Kind::Array(element_type) = ty.kind() else {
            return Err("not an array type".into());
        };

        let mut cursor = raw;
        let dimension_count = take_i32(&mut cursor)?;
        let _has_null = take_i32(&mut cursor)?;
        let _element_oid = take_i32(&mut cursor)?;
        if dimension_count < 0 {
            return Err("invalid array payload: negative dimension count".into());
        }

        // Lower bounds only matter for psql's `[0:1]={...}` prefix; ignored.
        let mut dimensions = Vec::new();
        for _ in 0..dimension_count {
            let length = take_i32(&mut cursor)?;
            let _lower_bound = take_i32(&mut cursor)?;
            let length = usize::try_from(length)
                .map_err(|_| "invalid array payload: negative dimension length")?;
            dimensions.push(length);
        }

        let element_count = dimensions
            .iter()
            .try_fold(1usize, |count, length| count.checked_mul(*length))
            .ok_or("invalid array payload: dimension overflow")?;
        // Every element carries at least a 4-byte length word.
        if !dimensions.is_empty() && element_count > cursor.len() / 4 {
            return Err("invalid array payload: truncated".into());
        }

        let mut elements = Vec::with_capacity(if dimensions.is_empty() { 0 } else { element_count });
        if !dimensions.is_empty() {
            for _ in 0..element_count {
                let length = take_i32(&mut cursor)?;
                elements.push(match usize::try_from(length) {
                    Ok(length) => decode_array_element(element_type, take_bytes(&mut cursor, length)?)?,
                    Err(_) => Value::Null,
                });
            }
        }

        Ok(Self(nest_array(&mut elements.into_iter(), &dimensions)))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty.kind(), postgres_types::Kind::Array(_))
    }
}

impl<'a> FromSql<'a> for PgCompositeValue {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if let Some(element_type) = composite_array_element_type(ty) {
            // Multi-dimensional arrays of composites are flattened in storage order.
            let (_, elements) = parse_postgres_array_elements(&element_type, raw)?;
            return Ok(Self(Value::Array(
                elements
                    .into_iter()
                    .map(|element| element.map(Value::String).unwrap_or(Value::Null))
                    .collect(),
            )));
        }
        Ok(Self(Value::String(format_postgres_value_text(ty, raw)?)))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        is_postgres_composite_type(ty) || is_postgres_composite_array_type(ty)
    }
}

impl PgValue {
    /// Convert a zqlz_core::Value into a PgValue that matches the target
    /// PostgreSQL column type. This ensures tokio-postgres writes the correct
    /// binary width (e.g. 4 bytes for INT4, not 8 bytes from an i64).
    pub(crate) fn from_value_for_type(value: &Value, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;

        match value {
            Value::Null => PgValue::Null,
            Value::Bool(v) => PgValue::Bool(*v),

            Value::Int8(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int16(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int32(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int64(v) => Self::coerce_int(*v, target_type),

            Value::Float32(v) => match *target_type {
                Type::FLOAT8 => PgValue::Float64(*v as f64),
                _ => PgValue::Float32(*v),
            },
            Value::Float64(v) => match *target_type {
                Type::FLOAT4 => PgValue::Float32(*v as f32),
                _ => PgValue::Float64(*v),
            },

            Value::Decimal(v) => PgValue::Decimal(v.clone()),
            Value::String(v) => Self::coerce_string(v, target_type),
            Value::Bytes(v) => PgValue::Bytes(v.clone()),
            Value::Uuid(v) => PgValue::Uuid(*v),
            Value::Json(v) => PgValue::Json(v.clone()),
            Value::DateTimeUtc(v) => PgValue::DateTimeUtc(*v),
            Value::Date(v) => PgValue::Date(*v),
            Value::Time(v) => PgValue::Time(*v),
            Value::DateTime(v) => PgValue::DateTime(*v),
            Value::Array(items) => PgValue::Array(items.iter().map(PgValue::from_value).collect()),
        }
    }

    /// Pick the PgValue integer variant that matches the target column type
    /// so tokio-postgres writes the correct number of bytes.
    fn coerce_int(value: i64, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;
        match *target_type {
            Type::INT2 => PgValue::Int16(value as i16),
            Type::INT4 => PgValue::Int32(value as i32),
            Type::INT8 => PgValue::Int64(value),
            _ => PgValue::Int64(value),
        }
    }

    /// Coerce string literals into strongly typed PostgreSQL parameter values
    /// when the prepared statement provides a concrete target type.
    fn coerce_string(value: &str, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;

        if matches!(*target_type, Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ)
            && let Some(raw) = parse_special_temporal(value, target_type)
        {
            return raw;
        }

        match *target_type {
            Type::JSON | Type::JSONB => serde_json::from_str::<serde_json::Value>(value)
                .map(PgValue::Json)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::DATE => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(PgValue::Date)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::TIME => chrono::NaiveTime::parse_from_str(value, "%H:%M:%S")
                .or_else(|_| chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f"))
                .map(PgValue::Time)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::TIMESTAMP => {
                let parsed = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").ok()
                    })
                    .or_else(|| {
                        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                            .ok()
                            .and_then(|date| {
                                chrono::NaiveTime::from_hms_opt(0, 0, 0)
                                    .map(|time| date.and_time(time))
                            })
                    });
                parsed
                    .map(PgValue::DateTime)
                    .unwrap_or_else(|| PgValue::String(value.to_string()))
            }
            Type::TIMESTAMPTZ => {
                let parsed = chrono::DateTime::parse_from_rfc3339(value)
                    .ok()
                    .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                            .ok()
                            .or_else(|| {
                                chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                                    .ok()
                            })
                            .map(|timestamp| {
                                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                    timestamp,
                                    chrono::Utc,
                                )
                            })
                    })
                    .or_else(|| {
                        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                            .ok()
                            .and_then(|date| {
                                chrono::NaiveTime::from_hms_opt(0, 0, 0).map(|time| {
                                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                        date.and_time(time),
                                        chrono::Utc,
                                    )
                                })
                            })
                    });
                parsed
                    .map(PgValue::DateTimeUtc)
                    .unwrap_or_else(|| PgValue::String(value.to_string()))
            }
            _ => PgValue::String(value.to_string()),
        }
    }

    /// Fallback used when we don't know the target column type (e.g. raw queries).
    pub(crate) fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => PgValue::Null,
            Value::Bool(v) => PgValue::Bool(*v),
            Value::Int8(v) => PgValue::Int16(*v as i16),
            Value::Int16(v) => PgValue::Int16(*v),
            Value::Int32(v) => PgValue::Int32(*v),
            Value::Int64(v) => PgValue::Int64(*v),
            Value::Float32(v) => PgValue::Float32(*v),
            Value::Float64(v) => PgValue::Float64(*v),
            Value::Decimal(v) => PgValue::Decimal(v.clone()),
            Value::String(v) => PgValue::String(v.clone()),
            Value::Bytes(v) => PgValue::Bytes(v.clone()),
            Value::Uuid(v) => PgValue::Uuid(*v),
            Value::Json(v) => PgValue::Json(v.clone()),
            Value::DateTimeUtc(v) => PgValue::DateTimeUtc(*v),
            Value::Date(v) => PgValue::Date(*v),
            Value::Time(v) => PgValue::Time(*v),
            Value::DateTime(v) => PgValue::DateTime(*v),
            Value::Array(items) => PgValue::Array(items.iter().map(PgValue::from_value).collect()),
        }
    }
}

impl ToSql for PgValue {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> std::result::Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgValue::Null => Ok(postgres_types::IsNull::Yes),
            PgValue::Bool(v) => v.to_sql(ty, out),
            PgValue::Int16(v) => v.to_sql(ty, out),
            PgValue::Int32(v) => v.to_sql(ty, out),
            PgValue::Int64(v) => v.to_sql(ty, out),
            PgValue::Float32(v) => v.to_sql(ty, out),
            PgValue::Float64(v) => v.to_sql(ty, out),
            PgValue::String(v) => v.to_sql(ty, out),
            PgValue::Bytes(v) => v.to_sql(ty, out),
            PgValue::Uuid(v) => v.to_sql(ty, out),
            PgValue::Json(v) => v.to_sql(ty, out),
            PgValue::DateTimeUtc(v) => v.to_sql(ty, out),
            PgValue::Date(v) => v.to_sql(ty, out),
            PgValue::Time(v) => v.to_sql(ty, out),
            PgValue::DateTime(v) => v.to_sql(ty, out),
            PgValue::DateRaw(v) => {
                out.extend_from_slice(&v.to_be_bytes());
                Ok(postgres_types::IsNull::No)
            }
            PgValue::TimestampRaw(v) => {
                out.extend_from_slice(&v.to_be_bytes());
                Ok(postgres_types::IsNull::No)
            }
            // A numeric column needs the binary NUMERIC encoding; anywhere else
            // (text, varchar) the digits travel as a plain string.
            PgValue::Decimal(v) => {
                if *ty == tokio_postgres::types::Type::NUMERIC {
                    encode_pg_numeric(v, out)?;
                    Ok(postgres_types::IsNull::No)
                } else {
                    v.to_sql(ty, out)
                }
            }
            // Encode against the array's element type so each member is written
            // with the right binary format rather than stringified.
            PgValue::Array(values) => match ty.kind() {
                postgres_types::Kind::Array(element_type) => {
                    values.as_slice().to_sql(ty, out).or_else(|_| {
                        Err(format!(
                            "failed to encode array of {} for {}",
                            element_type, ty
                        )
                        .into())
                    })
                }
                _ => Err(format!("cannot send an array value to non-array type {}", ty).into()),
            },
        }
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }

    postgres_types::to_sql_checked!();
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::Type;

    fn numeric_payload(weight: i16, sign: u16, dscale: i16, digits: &[u16]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        raw.extend_from_slice(&weight.to_be_bytes());
        raw.extend_from_slice(&sign.to_be_bytes());
        raw.extend_from_slice(&dscale.to_be_bytes());
        for digit in digits {
            raw.extend_from_slice(&digit.to_be_bytes());
        }
        raw
    }

    #[test]
    fn numeric_decodes_like_psql_text() {
        let cases: &[(Vec<u8>, &str)] = &[
            (numeric_payload(-1, 0x4000, 1, &[5000]), "-0.5"),
            (numeric_payload(-2, 0x0000, 7, &[10]), "0.0000001"),
            (numeric_payload(-1, 0x4000, 6, &[1]), "-0.000100"),
            (numeric_payload(0, 0x0000, 2, &[1, 5000]), "1.50"),
            (numeric_payload(0, 0x0000, 2, &[]), "0.00"),
            (numeric_payload(0, 0x0000, 0, &[]), "0"),
            (numeric_payload(0, 0x0000, 3, &[1]), "1.000"),
            (numeric_payload(1, 0x0000, 0, &[100]), "1000000"),
            (numeric_payload(0, 0x4000, 0, &[42]), "-42"),
            (numeric_payload(0, 0xC000, 0, &[]), "NaN"),
            (numeric_payload(0, 0xD000, 0, &[]), "Infinity"),
            (numeric_payload(0, 0xF000, 0, &[]), "-Infinity"),
            (
                numeric_payload(4, 0x0000, 10, &[1234, 5678, 9012, 3456, 7890, 123, 4567, 8900]),
                "12345678901234567890.0123456789",
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(&PgNumericString::parse(raw).unwrap(), expected);
        }
    }

    #[test]
    fn numeric_rejects_bad_payloads() {
        assert!(PgNumericString::parse(&[0; 4]).is_err());
        assert!(PgNumericString::parse(&numeric_payload(0, 0x0000, 0, &[10000])).is_err());
        assert!(PgNumericString::parse(&numeric_payload(0, 0x1234, 0, &[])).is_err());
    }

    #[test]
    fn numeric_encode_then_decode_is_exact() {
        for text in [
            "0", "0.00", "1", "-1", "1.50", "-0.5", "0.0000001", "-0.000100", "1.000", "100",
            "1000000", "0.1", "12345.6789", "-6.0000", "99999999999999999999.99999999999",
            "123456789012345678901234567890.123456789", "NaN", "Infinity", "-Infinity",
            "0.00000000000000000001",
        ] {
            let mut buffer = BytesMut::new();
            encode_pg_numeric(text, &mut buffer).unwrap();
            assert_eq!(PgNumericString::parse(&buffer).unwrap(), text);
        }
    }

    fn array_payload(element_oid: u32, dimensions: &[i32], elements: &[Option<&[u8]>]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&(dimensions.len() as i32).to_be_bytes());
        raw.extend_from_slice(&(elements.iter().any(Option::is_none) as i32).to_be_bytes());
        raw.extend_from_slice(&element_oid.to_be_bytes());
        for length in dimensions {
            raw.extend_from_slice(&length.to_be_bytes());
            raw.extend_from_slice(&1i32.to_be_bytes());
        }
        for element in elements {
            match element {
                Some(bytes) => {
                    raw.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    raw.extend_from_slice(bytes);
                }
                None => raw.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        raw
    }

    fn int_array(dimensions: &[i32], values: &[Option<i32>]) -> Value {
        let encoded: Vec<[u8; 4]> = values.iter().map(|v| v.unwrap_or(0).to_be_bytes()).collect();
        let elements: Vec<Option<&[u8]>> = values
            .iter()
            .zip(&encoded)
            .map(|(value, bytes)| value.map(|_| bytes.as_slice()))
            .collect();
        PgArrayValue::from_sql(&Type::INT4_ARRAY, &array_payload(23, dimensions, &elements))
            .unwrap()
            .0
    }

    #[test]
    fn array_with_null_element_keeps_the_null() {
        assert_eq!(
            int_array(&[3], &[Some(1), None, Some(3)]),
            Value::Array(vec![Value::Int32(1), Value::Null, Value::Int32(3)])
        );
    }

    #[test]
    fn multidimensional_array_nests() {
        let row = |a, b| Value::Array(vec![Value::Int32(a), Value::Int32(b)]);
        assert_eq!(
            int_array(&[2, 2], &[Some(1), Some(2), Some(3), Some(4)]),
            Value::Array(vec![row(1, 2), row(3, 4)])
        );
    }

    #[test]
    fn empty_array_is_empty_not_null() {
        let raw = array_payload(23, &[], &[]);
        assert_eq!(
            PgArrayValue::from_sql(&Type::INT4_ARRAY, &raw).unwrap().0,
            Value::Array(vec![])
        );
    }

    #[test]
    fn text_numeric_and_date_arrays_decode_elements() {
        let text = array_payload(25, &[2], &[Some(b"a"), None]);
        assert_eq!(
            PgArrayValue::from_sql(&Type::TEXT_ARRAY, &text).unwrap().0,
            Value::Array(vec![Value::String("a".into()), Value::Null])
        );

        let numeric = numeric_payload(0, 0x0000, 2, &[1, 1000]);
        let raw = array_payload(1700, &[1], &[Some(&numeric)]);
        assert_eq!(
            PgArrayValue::from_sql(&Type::NUMERIC_ARRAY, &raw).unwrap().0,
            Value::Array(vec![Value::Decimal("1.10".into())])
        );

        let raw = array_payload(1082, &[2], &[Some(&i32::MAX.to_be_bytes()), Some(&0i32.to_be_bytes())]);
        assert_eq!(
            PgArrayValue::from_sql(&Type::DATE_ARRAY, &raw).unwrap().0,
            Value::Array(vec![
                Value::String("infinity".into()),
                Value::Date(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
            ])
        );
    }

    #[test]
    fn truncated_array_errors_without_panicking() {
        let mut raw = array_payload(23, &[3], &[Some(&1i32.to_be_bytes()), Some(&2i32.to_be_bytes())]);
        assert!(PgArrayValue::from_sql(&Type::INT4_ARRAY, &raw).is_err());
        raw.truncate(5);
        assert!(PgArrayValue::from_sql(&Type::INT4_ARRAY, &raw).is_err());
    }

    fn decode_temporal(ty: &Type, raw: &[u8]) -> Value {
        PgTemporal::from_sql(ty, raw).unwrap().0
    }

    #[test]
    fn temporal_infinity_and_bc_render_like_psql() {
        assert_eq!(
            decode_temporal(&Type::DATE, &i32::MAX.to_be_bytes()),
            Value::String("infinity".into())
        );
        assert_eq!(
            decode_temporal(&Type::TIMESTAMP, &i64::MIN.to_be_bytes()),
            Value::String("-infinity".into())
        );
        assert_eq!(
            decode_temporal(&Type::TIMESTAMPTZ, &i64::MAX.to_be_bytes()),
            Value::String("infinity".into())
        );

        let days = (chrono::NaiveDate::from_ymd_opt(-43, 3, 15).unwrap() - pg_epoch().date())
            .num_days() as i32;
        assert_eq!(
            decode_temporal(&Type::DATE, &days.to_be_bytes()),
            Value::String("0044-03-15 BC".into())
        );

        let micros = (chrono::NaiveDate::from_ymd_opt(-43, 3, 15)
            .unwrap()
            .and_hms_micro_opt(10, 0, 0, 500_000)
            .unwrap()
            - pg_epoch())
        .num_microseconds()
        .unwrap();
        assert_eq!(
            decode_temporal(&Type::TIMESTAMP, &micros.to_be_bytes()),
            Value::String("0044-03-15 10:00:00.500 BC".into())
        );
        assert_eq!(
            decode_temporal(&Type::TIMESTAMPTZ, &micros.to_be_bytes()),
            Value::String("0044-03-15 10:00:00.500+00 BC".into())
        );
    }

    #[test]
    fn temporal_ordinary_values_stay_typed() {
        assert_eq!(
            decode_temporal(&Type::DATE, &9190i32.to_be_bytes()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 2, 28).unwrap())
        );
        assert!(matches!(
            decode_temporal(&Type::TIMESTAMP, &0i64.to_be_bytes()),
            Value::DateTime(_)
        ));
        assert!(matches!(
            decode_temporal(&Type::TIMESTAMPTZ, &0i64.to_be_bytes()),
            Value::DateTimeUtc(_)
        ));
    }

    #[test]
    fn special_temporal_text_round_trips_through_the_wire_form() {
        for (ty, text, raw) in [
            (Type::DATE, "infinity", i32::MAX.to_be_bytes().to_vec()),
            (Type::DATE, "-infinity", i32::MIN.to_be_bytes().to_vec()),
            (Type::TIMESTAMP, "-infinity", i64::MIN.to_be_bytes().to_vec()),
        ] {
            let parsed = parse_special_temporal(text, &ty).unwrap();
            let mut out = BytesMut::new();
            parsed.to_sql(&ty, &mut out).unwrap();
            assert_eq!(out.to_vec(), raw);
        }

        for (ty, text) in [
            (Type::DATE, "0044-03-15 BC"),
            (Type::TIMESTAMP, "0044-03-15 10:00:00.500 BC"),
            (Type::TIMESTAMPTZ, "0044-03-15 10:00:00+00 BC"),
        ] {
            let mut out = BytesMut::new();
            parse_special_temporal(text, &ty).unwrap().to_sql(&ty, &mut out).unwrap();
            assert_eq!(decode_temporal(&ty, &out), Value::String(text.into()));
        }

        assert!(parse_special_temporal("2025-01-01", &Type::DATE).is_none());
    }
}
