//! Binary-protocol value formatting/parsing for PostgreSQL types that
//! `tokio-postgres` does not decode natively (network, geometric, range,
//! interval, bit-string, and similar types).

use tokio_postgres::types::{Kind, Type};

use super::pg_value::PgNumericString;

pub(crate) fn format_postgres_network_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid network payload: too short".into());
    }

    let family = raw[0];
    let prefix_bits = raw[1];
    let address_length = raw[3] as usize;
    let address = raw
        .get(4..4 + address_length)
        .ok_or("invalid network payload: truncated address")?;

    let (address, full_prefix_bits) = match family {
        2 if address_length == 4 => {
            let octets: [u8; 4] = address.try_into()?;
            (std::net::Ipv4Addr::from(octets).to_string(), 32)
        }
        3 if address_length == 16 => {
            let octets: [u8; 16] = address.try_into()?;
            (std::net::Ipv6Addr::from(octets).to_string(), 128)
        }
        _ => return Err("invalid network payload: unsupported address family".into()),
    };

    if type_name == "cidr" || prefix_bits != full_prefix_bits {
        Ok(format!("{address}/{prefix_bits}"))
    } else {
        Ok(address)
    }
}

pub(crate) fn format_postgres_tsvector_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid tsvector payload: too short".into());
    }

    let lexeme_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if lexeme_count < 0 {
        return Err("invalid tsvector payload: negative lexeme count".into());
    }

    let mut offset = 4;
    let mut lexemes = Vec::with_capacity(lexeme_count as usize);
    for _ in 0..lexeme_count {
        let lexeme_end = raw
            .get(offset..)
            .and_then(|bytes| bytes.iter().position(|byte| *byte == 0))
            .map(|position| offset + position)
            .ok_or("invalid tsvector payload: unterminated lexeme")?;
        let lexeme = std::str::from_utf8(&raw[offset..lexeme_end])?;
        offset = lexeme_end + 1;

        let position_count_bytes = raw
            .get(offset..offset + 2)
            .ok_or("invalid tsvector payload: missing position count")?;
        let position_count = i16::from_be_bytes(position_count_bytes.try_into()?);
        if position_count < 0 {
            return Err("invalid tsvector payload: negative position count".into());
        }
        offset += 2;

        let mut positions = Vec::with_capacity(position_count as usize);
        for _ in 0..position_count {
            let position_bytes = raw
                .get(offset..offset + 2)
                .ok_or("invalid tsvector payload: truncated position")?;
            let encoded = u16::from_be_bytes(position_bytes.try_into()?);
            let position = encoded & 0x3fff;
            let weight = match encoded >> 14 {
                0 => "",
                1 => "C",
                2 => "B",
                3 => "A",
                _ => "",
            };
            positions.push(format!("{position}{weight}"));
            offset += 2;
        }

        let escaped = lexeme.replace('\'', "''");
        if positions.is_empty() {
            lexemes.push(format!("'{escaped}'"));
        } else {
            lexemes.push(format!("'{escaped}':{}", positions.join(",")));
        }
    }

    Ok(lexemes.join(" "))
}

pub(crate) fn format_postgres_tstzrange_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    format_postgres_range_value("tstzrange", raw)
}

pub(crate) fn format_postgres_binary_display_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    match type_name {
        "hstore" => format_postgres_hstore_value(raw),
        "macaddr" => format_postgres_macaddr_value(raw, 6),
        "macaddr8" => format_postgres_macaddr_value(raw, 8),
        "point" => format_postgres_point_value(raw),
        "line" => format_postgres_line_value(raw),
        "lseg" => format_postgres_lseg_value(raw),
        "box" => format_postgres_box_value(raw),
        "path" => format_postgres_path_value(raw),
        "polygon" => format_postgres_polygon_value(raw),
        "circle" => format_postgres_circle_value(raw),
        "money" => format_postgres_money_value(raw),
        "interval" => format_postgres_interval_value(raw),
        "timetz" => format_postgres_timetz_value(raw),
        "bit" | "varbit" => format_postgres_bit_value(raw),
        "oid" | "xid" | "cid" | "regclass" | "regtype" | "regproc" | "regprocedure" | "regoper"
        | "regoperator" | "regnamespace" | "regrole" => format_postgres_u32_value(raw),
        "pg_lsn" => format_postgres_lsn_value(raw),
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            format_postgres_range_value(type_name, raw)
        }
        "int4multirange" | "int8multirange" | "nummultirange" | "tsmultirange"
        | "tstzmultirange" | "datemultirange" => format_postgres_multirange_value(type_name, raw),
        "tsquery" => format_postgres_tsquery_value(raw),
        "txid_snapshot" | "pg_snapshot" => format_postgres_snapshot_value(raw),
        "vector" => format_postgres_vector_value(raw),
        "halfvec" => format_postgres_halfvec_value(raw),
        "sparsevec" => format_postgres_sparsevec_value(raw),
        _ => Err("unsupported PostgreSQL binary display type".into()),
    }
}

fn format_postgres_known_binary_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    match type_name {
        "inet" | "cidr" => format_postgres_network_value(type_name, raw),
        "tsvector" => format_postgres_tsvector_value(raw),
        _ => format_postgres_binary_display_value(type_name, raw),
    }
}

fn format_postgres_hstore_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid hstore payload: too short".into());
    }

    let pair_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if pair_count < 0 {
        return Err("invalid hstore payload: negative pair count".into());
    }

    let mut offset = 4;
    let mut pairs = Vec::with_capacity(pair_count as usize);
    for _ in 0..pair_count {
        let (key, next_offset) = parse_postgres_length_prefixed_text(raw, offset, "hstore key")?;
        offset = next_offset;

        let value_length_bytes = raw
            .get(offset..offset + 4)
            .ok_or("invalid hstore payload: missing value length")?;
        let value_length = i32::from_be_bytes(value_length_bytes.try_into()?);
        offset += 4;

        let key = quote_postgres_hstore_part(&key);
        if value_length < 0 {
            pairs.push(format!("{key}=>NULL"));
        } else {
            let value_length = value_length as usize;
            let value_bytes = raw
                .get(offset..offset + value_length)
                .ok_or("invalid hstore payload: truncated value")?;
            let value = std::str::from_utf8(value_bytes)?;
            offset += value_length;
            pairs.push(format!("{key}=>{}", quote_postgres_hstore_part(value)));
        }
    }

    Ok(pairs.join(", "))
}

fn parse_postgres_length_prefixed_text(
    raw: &[u8],
    offset: usize,
    label: &str,
) -> std::result::Result<(String, usize), Box<dyn std::error::Error + Sync + Send>> {
    let length_bytes = raw
        .get(offset..offset + 4)
        .ok_or_else(|| format!("invalid payload: missing {label} length"))?;
    let length = i32::from_be_bytes(length_bytes.try_into()?);
    if length < 0 {
        return Err(format!("invalid payload: negative {label} length").into());
    }
    let text_offset = offset + 4;
    let length = length as usize;
    let text_bytes = raw
        .get(text_offset..text_offset + length)
        .ok_or_else(|| format!("invalid payload: truncated {label}"))?;
    Ok((
        std::str::from_utf8(text_bytes)?.to_string(),
        text_offset + length,
    ))
}

fn quote_postgres_hstore_part(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn format_postgres_macaddr_value(
    raw: &[u8],
    expected_length: usize,
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != expected_length {
        return Err("invalid macaddr payload length".into());
    }

    Ok(raw
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":"))
}

fn format_postgres_point_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let (x, y) = parse_postgres_point(raw, 0)?;
    Ok(format!("({},{})", format_float(x), format_float(y)))
}

fn format_postgres_line_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 3, "line")?;
    Ok(format!(
        "{{{},{},{}}}",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2])
    ))
}

fn format_postgres_lseg_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 4, "lseg")?;
    Ok(format!(
        "[({},{}),({},{})]",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2]),
        format_float(values[3])
    ))
}

fn format_postgres_box_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 4, "box")?;
    Ok(format!(
        "({},{}),({},{})",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2]),
        format_float(values[3])
    ))
}

fn format_postgres_path_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 5 {
        return Err("invalid path payload: too short".into());
    }
    let closed = raw[0] != 0;
    let point_count = i32::from_be_bytes(raw[1..5].try_into()?);
    if point_count < 0 {
        return Err("invalid path payload: negative point count".into());
    }
    let points = parse_postgres_points(raw, 5, point_count as usize)?;
    let joined = points.join(",");
    if closed {
        Ok(format!("({joined})"))
    } else {
        Ok(format!("[{joined}]"))
    }
}

fn format_postgres_polygon_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid polygon payload: too short".into());
    }
    let point_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if point_count < 0 {
        return Err("invalid polygon payload: negative point count".into());
    }
    Ok(format!(
        "({})",
        parse_postgres_points(raw, 4, point_count as usize)?.join(",")
    ))
}

fn format_postgres_circle_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 3, "circle")?;
    Ok(format!(
        "<({},{}),{}>",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2])
    ))
}

fn format_postgres_money_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid money payload: expected 8 bytes".into());
    }

    Ok(super::money::format_active_money(i64::from_be_bytes(raw.try_into()?)))
}

fn format_postgres_interval_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 16 {
        return Err("invalid interval payload: expected 16 bytes".into());
    }

    let micros = i64::from_be_bytes(raw[0..8].try_into()?);
    let days = i32::from_be_bytes(raw[8..12].try_into()?);
    let months = i32::from_be_bytes(raw[12..16].try_into()?);
    let mut parts = Vec::new();
    // Mirrors EncodeInterval (IntervalStyle=postgres): each nonzero field makes the
    // next positive one print an explicit '+', and the sign of every field is its own.
    let mut is_before = false;
    for (value, unit) in [(months / 12, "year"), (months % 12, "mon"), (days, "day")] {
        if value == 0 {
            continue;
        }
        let plus = if is_before && value > 0 { "+" } else { "" };
        let plural = if value != 1 { "s" } else { "" };
        parts.push(format!("{plus}{value} {unit}{plural}"));
        is_before = value < 0;
    }
    if micros != 0 || parts.is_empty() {
        let time = format_postgres_duration_micros(micros);
        parts.push(if is_before && micros > 0 {
            format!("+{time}")
        } else {
            time
        });
    }

    Ok(parts.join(" "))
}

fn format_postgres_timetz_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 12 {
        return Err("invalid timetz payload: expected 12 bytes".into());
    }

    let micros = i64::from_be_bytes(raw[0..8].try_into()?);
    let zone_seconds_west = i32::from_be_bytes(raw[8..12].try_into()?);
    let time = format_postgres_time_micros(micros);
    let offset_seconds = -zone_seconds_west;
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let absolute = offset_seconds.abs();
    Ok(format!(
        "{time}{sign}{:02}:{:02}",
        absolute / 3600,
        (absolute % 3600) / 60
    ))
}

fn format_postgres_bit_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid bit payload: too short".into());
    }

    let bit_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if bit_count < 0 {
        return Err("invalid bit payload: negative bit count".into());
    }

    let bit_count = bit_count as usize;
    let bytes = raw
        .get(4..)
        .ok_or("invalid bit payload: missing bit bytes")?;
    if bytes.len() * 8 < bit_count {
        return Err("invalid bit payload: truncated bit bytes".into());
    }

    let mut output = String::with_capacity(bit_count);
    for index in 0..bit_count {
        let byte = bytes[index / 8];
        let mask = 1 << (7 - (index % 8));
        output.push(if byte & mask == 0 { '0' } else { '1' });
    }
    Ok(output)
}

fn format_postgres_u32_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 4 {
        return Err("invalid oid-like payload: expected 4 bytes".into());
    }
    Ok(u32::from_be_bytes(raw.try_into()?).to_string())
}

fn format_postgres_lsn_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid pg_lsn payload: expected 8 bytes".into());
    }
    let value = u64::from_be_bytes(raw.try_into()?);
    Ok(format!("{:X}/{:X}", value >> 32, value & 0xffff_ffff))
}

fn format_postgres_duration_micros(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let absolute = micros.unsigned_abs();
    let total_seconds = absolute / 1_000_000;
    let fractional_micros = absolute % 1_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if fractional_micros == 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{fractional_micros:06}")
            .trim_end_matches('0')
            .to_string()
    }
}

fn format_postgres_time_micros(micros: i64) -> String {
    format_postgres_duration_micros(micros)
}

fn parse_postgres_points(
    raw: &[u8],
    offset: usize,
    point_count: usize,
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error + Sync + Send>> {
    let expected_length = offset + point_count * 16;
    if raw.len() < expected_length {
        return Err("invalid geometry payload: truncated points".into());
    }

    let mut points = Vec::with_capacity(point_count);
    for index in 0..point_count {
        let (x, y) = parse_postgres_point(raw, offset + index * 16)?;
        points.push(format!("({},{})", format_float(x), format_float(y)));
    }
    Ok(points)
}

fn parse_postgres_point(
    raw: &[u8],
    offset: usize,
) -> std::result::Result<(f64, f64), Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(
        raw.get(offset..).ok_or("invalid point payload: offset")?,
        2,
        "point",
    )?;
    Ok((values[0], values[1]))
}

fn parse_postgres_f64_values(
    raw: &[u8],
    count: usize,
    label: &str,
) -> std::result::Result<Vec<f64>, Box<dyn std::error::Error + Sync + Send>> {
    let expected_length = count * 8;
    if raw.len() < expected_length {
        return Err(format!("invalid {label} payload: too short").into());
    }

    let mut values = Vec::with_capacity(count);
    for chunk in raw[..expected_length].chunks_exact(8) {
        values.push(f64::from_be_bytes(chunk.try_into()?));
    }
    Ok(values)
}

/// float8out: shortest round-trip digits, scientific form below 1e-4 and from 1e15.
pub(crate) fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    shortest_float_text(&format!("{value:e}"), value.to_string(), 15)
}

/// float4out counterpart of [`format_float`].
pub(crate) fn format_float32(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    shortest_float_text(&format!("{value:e}"), value.to_string(), 6)
}

/// `scientific` is Rust's `{:e}` form (`1.5e-7`), `fixed` its plain `Display` form;
/// values with a decimal exponent of `max_fixed_exponent` or more go scientific
/// (the digit count of the type: 15 for float8, 6 for float4).
fn shortest_float_text(scientific: &str, fixed: String, max_fixed_exponent: i32) -> String {
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return fixed;
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return fixed;
    };
    if (-4..max_fixed_exponent).contains(&exponent) {
        fixed
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

fn format_postgres_range_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let flags = *raw.first().ok_or("invalid range payload: too short")?;
    if flags & 0x01 != 0 {
        return Ok("empty".to_string());
    }

    let mut offset = 1;
    let lower_infinite = flags & 0x08 != 0;
    let upper_infinite = flags & 0x10 != 0;

    let lower = if lower_infinite {
        String::new()
    } else {
        let (bound, next_offset) = parse_postgres_range_bound(type_name, raw, offset)?;
        offset = next_offset;
        bound
    };

    let upper = if upper_infinite {
        String::new()
    } else {
        let (bound, _next_offset) = parse_postgres_range_bound(type_name, raw, offset)?;
        bound
    };

    let lower_bracket = if flags & 0x02 != 0 { "[" } else { "(" };
    let upper_bracket = if flags & 0x04 != 0 { "]" } else { ")" };
    Ok(format!(
        "{lower_bracket}{},{}{upper_bracket}",
        quote_postgres_range_bound(&lower),
        quote_postgres_range_bound(&upper)
    ))
}

/// range_out quotes a bound holding a delimiter, bracket, quote, backslash or whitespace.
fn quote_postgres_range_bound(bound: &str) -> String {
    let needs_quotes = bound
        .chars()
        .any(|ch| matches!(ch, '"' | '\\' | '(' | ')' | '[' | ']' | ',') || ch.is_ascii_whitespace());
    if needs_quotes {
        format!("\"{}\"", bound.replace('\\', "\\\\").replace('"', "\"\""))
    } else {
        bound.to_string()
    }
}

fn parse_postgres_range_bound(
    type_name: &str,
    raw: &[u8],
    offset: usize,
) -> std::result::Result<(String, usize), Box<dyn std::error::Error + Sync + Send>> {
    let length_bytes = raw
        .get(offset..offset + 4)
        .ok_or("invalid range payload: missing bound length")?;
    let length = i32::from_be_bytes(length_bytes.try_into()?);
    if length < 0 {
        return Err("invalid range payload: negative bound length".into());
    }
    let value_offset = offset + 4;
    let length = length as usize;
    let value = raw
        .get(value_offset..value_offset + length)
        .ok_or("invalid range payload: truncated bound")?;

    let formatted = match type_name {
        "int4range" if length == 4 => i32::from_be_bytes(value.try_into()?).to_string(),
        "int8range" if length == 8 => i64::from_be_bytes(value.try_into()?).to_string(),
        "numrange" => PgNumericString::parse(value)?,
        "tsrange" => parse_postgres_timestamp_value(value)?,
        "tstzrange" => parse_postgres_timestamptz_value(value)?,
        "daterange" => parse_postgres_date_value(value)?,
        _ => return Err("unsupported range subtype payload".into()),
    };

    Ok((formatted, value_offset + length))
}

fn parse_postgres_timestamp_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid timestamp payload: expected 8 bytes".into());
    }
    format_postgres_timestamp_micros(i64::from_be_bytes(raw.try_into()?), false)
}

fn parse_postgres_timestamptz_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid timestamptz payload: expected 8 bytes".into());
    }
    format_postgres_timestamp_micros(i64::from_be_bytes(raw.try_into()?), true)
}

fn format_postgres_timestamp_micros(
    micros: i64,
    utc: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    match micros {
        i64::MAX => return Ok("infinity".to_string()),
        i64::MIN => return Ok("-infinity".to_string()),
        _ => {}
    }
    let (days, time_micros) = (
        micros.div_euclid(86_400_000_000),
        micros.rem_euclid(86_400_000_000),
    );
    let days = i32::try_from(days).map_err(|_| "invalid timestamp payload: out of range")?;
    let (date, era) = format_postgres_date_days(days)?;
    let fraction = time_micros % 1_000_000;
    let mut time = format!(
        "{:02}:{:02}:{:02}",
        time_micros / 3_600_000_000,
        time_micros / 60_000_000 % 60,
        time_micros / 1_000_000 % 60
    );
    if fraction != 0 {
        time.push_str(format!(".{fraction:06}").trim_end_matches('0'));
    }
    let zone = if utc { "+00" } else { "" };
    Ok(format!("{date} {time}{zone}{era}"))
}

/// Days since 2000-01-01 as `YYYY-MM-DD` plus the " BC" suffix when the year is before 1 AD.
fn format_postgres_date_days(
    days: i32,
) -> std::result::Result<(String, &'static str), Box<dyn std::error::Error + Sync + Send>> {
    use chrono::Datelike;

    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).ok_or("invalid postgres date epoch")?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .ok_or("invalid date payload: date out of range")?;
    let (year, era) = if date.year() <= 0 {
        (1 - date.year(), " BC")
    } else {
        (date.year(), "")
    };
    Ok((format!("{year:04}-{:02}-{:02}", date.month(), date.day()), era))
}

fn parse_postgres_date_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 4 {
        return Err("invalid date payload: expected 4 bytes".into());
    }
    match i32::from_be_bytes(raw.try_into()?) {
        i32::MAX => Ok("infinity".to_string()),
        i32::MIN => Ok("-infinity".to_string()),
        days => {
            let (date, era) = format_postgres_date_days(days)?;
            Ok(format!("{date}{era}"))
        }
    }
}

fn format_postgres_vector_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let elements = parse_postgres_vector_header(raw, 4, "vector")?;
    let mut parts = Vec::with_capacity(elements.len() / 4);
    for chunk in elements.chunks_exact(4) {
        parts.push(f32::from_be_bytes(chunk.try_into()?).to_string());
    }
    Ok(format!("[{}]", parts.join(",")))
}

fn format_postgres_halfvec_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let elements = parse_postgres_vector_header(raw, 2, "halfvec")?;
    let mut parts = Vec::with_capacity(elements.len() / 2);
    for chunk in elements.chunks_exact(2) {
        parts.push(f32_from_f16_bits(u16::from_be_bytes(chunk.try_into()?)).to_string());
    }
    Ok(format!("[{}]", parts.join(",")))
}

/// Validate a pgvector `vector`/`halfvec` payload (`i16 dim, i16 unused,
/// dim × element`) and return the element bytes.
fn parse_postgres_vector_header<'a>(
    raw: &'a [u8],
    element_size: usize,
    label: &str,
) -> std::result::Result<&'a [u8], Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err(format!("invalid {label} payload: too short").into());
    }
    let dimension_count = i16::from_be_bytes(raw[0..2].try_into()?);
    if dimension_count < 0 {
        return Err(format!("invalid {label} payload: negative dimension count").into());
    }
    let element_length = dimension_count as usize * element_size;
    raw.get(4..4 + element_length)
        .ok_or_else(|| format!("invalid {label} payload: truncated elements").into())
}

fn format_postgres_sparsevec_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 12 {
        return Err("invalid sparsevec payload: too short".into());
    }
    let dimension_count = i32::from_be_bytes(raw[0..4].try_into()?);
    let nonzero_count = i32::from_be_bytes(raw[4..8].try_into()?);
    if dimension_count < 0 || nonzero_count < 0 {
        return Err("invalid sparsevec payload: negative count".into());
    }
    let nonzero_count = nonzero_count as usize;
    let indices_offset = 12;
    let values_offset = indices_offset + nonzero_count * 4;
    if raw.len() < values_offset + nonzero_count * 4 {
        return Err("invalid sparsevec payload: truncated entries".into());
    }

    let mut entries = Vec::with_capacity(nonzero_count);
    for position in 0..nonzero_count {
        let index_start = indices_offset + position * 4;
        let value_start = values_offset + position * 4;
        let index = i32::from_be_bytes(raw[index_start..index_start + 4].try_into()?);
        let value = f32::from_be_bytes(raw[value_start..value_start + 4].try_into()?);
        // Indices are 0-based on the wire but 1-based in pgvector's text form.
        entries.push(format!("{}:{}", i64::from(index) + 1, value));
    }
    Ok(format!("{{{}}}/{dimension_count}", entries.join(",")))
}

fn f32_from_f16_bits(bits: u16) -> f32 {
    let sign = u32::from(bits >> 15) << 31;
    let exponent = u32::from((bits >> 10) & 0x1f);
    let mantissa = u32::from(bits & 0x3ff);

    let magnitude = match (exponent, mantissa) {
        (0, 0) => 0,
        (0, _) => {
            // Subnormal half: renormalize into the f32 exponent range.
            let leading_zeros = mantissa.leading_zeros();
            let exponent = 134 - leading_zeros;
            let mantissa = (mantissa << (leading_zeros - 8)) & 0x007f_ffff;
            (exponent << 23) | mantissa
        }
        (0x1f, 0) => 0x7f80_0000,
        (0x1f, _) => 0x7f80_0000 | (mantissa << 13),
        _ => ((exponent + 127 - 15) << 23) | (mantissa << 13),
    };
    f32::from_bits(sign | magnitude)
}

type FormatResult = std::result::Result<String, Box<dyn std::error::Error + Sync + Send>>;

fn read_be<const N: usize>(
    raw: &[u8],
    offset: usize,
) -> std::result::Result<[u8; N], Box<dyn std::error::Error + Sync + Send>> {
    Ok(raw
        .get(offset..offset + N)
        .ok_or("invalid payload: truncated")?
        .try_into()?)
}

/// `{[1,3),[5,7)}`: a count, then each range as a length-prefixed range payload.
fn format_postgres_multirange_value(type_name: &str, raw: &[u8]) -> FormatResult {
    let range_type = format!(
        "{}range",
        type_name
            .strip_suffix("multirange")
            .ok_or("unsupported multirange type")?
    );
    let count = i32::from_be_bytes(read_be(raw, 0)?);
    if count < 0 {
        return Err("invalid multirange payload: negative range count".into());
    }
    let mut offset = 4;
    let mut ranges = Vec::new();
    for _ in 0..count {
        let length = i32::from_be_bytes(read_be(raw, offset)?);
        offset += 4;
        let length = usize::try_from(length).map_err(|_| "invalid multirange payload: negative length")?;
        let range = raw
            .get(offset..offset + length)
            .ok_or("invalid multirange payload: truncated range")?;
        ranges.push(format_postgres_range_value(&range_type, range)?);
        offset += length;
    }
    Ok(format!("{{{}}}", ranges.join(",")))
}

/// `xmin:xmax:xip1,xip2` for `txid_snapshot` / `pg_snapshot`.
fn format_postgres_snapshot_value(raw: &[u8]) -> FormatResult {
    let count = i32::from_be_bytes(read_be(raw, 0)?);
    let count = usize::try_from(count).map_err(|_| "invalid snapshot payload: negative count")?;
    let xmin = u64::from_be_bytes(read_be(raw, 4)?);
    let xmax = u64::from_be_bytes(read_be(raw, 12)?);
    let mut in_progress = Vec::new();
    for index in 0..count {
        in_progress.push(u64::from_be_bytes(read_be(raw, 20 + index * 8)?).to_string());
    }
    Ok(format!("{xmin}:{xmax}:{}", in_progress.join(",")))
}

const TSQUERY_MAX_DEPTH: usize = 1000;

/// Binary tsquery is a prefix-order item list (an operator is followed by its
/// right operand, then its left); this rebuilds tsquery's infix text.
pub(crate) fn format_postgres_tsquery_value(raw: &[u8]) -> FormatResult {
    let item_count = i32::from_be_bytes(read_be(raw, 0)?);
    if item_count < 0 {
        return Err("invalid tsquery payload: negative item count".into());
    }
    if item_count == 0 {
        return Ok(String::new());
    }
    let mut offset = 4;
    format_postgres_tsquery_node(raw, &mut offset, 0, 0)
}

/// `min_priority` is the weakest operator allowed to appear unparenthesised here.
fn format_postgres_tsquery_node(
    raw: &[u8],
    offset: &mut usize,
    min_priority: u8,
    depth: usize,
) -> FormatResult {
    const OP_NOT: u8 = 1;
    const OP_AND: u8 = 2;
    const OP_OR: u8 = 3;
    const OP_PHRASE: u8 = 4;

    if depth > TSQUERY_MAX_DEPTH {
        return Err("invalid tsquery payload: nested too deeply".into());
    }
    let [item_type] = read_be::<1>(raw, *offset)?;
    *offset += 1;
    match item_type {
        1 => {
            let [weight, prefix] = read_be::<2>(raw, *offset)?;
            *offset += 2;
            let length = raw[*offset..]
                .iter()
                .position(|byte| *byte == 0)
                .ok_or("invalid tsquery payload: unterminated operand")?;
            let operand = std::str::from_utf8(&raw[*offset..*offset + length])?;
            *offset += length + 1;
            let mut text = format!("'{}'", operand.replace('\'', "''").replace('\\', "\\\\"));
            if weight != 0 || prefix != 0 {
                text.push(':');
                if prefix != 0 {
                    text.push('*');
                }
                for (bit, label) in [(8, 'A'), (4, 'B'), (2, 'C'), (1, 'D')] {
                    if weight & bit != 0 {
                        text.push(label);
                    }
                }
            }
            Ok(text)
        }
        2 => {
            let [operator] = read_be::<1>(raw, *offset)?;
            *offset += 1;
            let (priority, symbol) = match operator {
                OP_NOT => (4, String::new()),
                OP_AND => (2, "&".to_string()),
                OP_OR => (1, "|".to_string()),
                OP_PHRASE => {
                    let distance = u16::from_be_bytes(read_be(raw, *offset)?);
                    *offset += 2;
                    (3, if distance == 1 { "<->".to_string() } else { format!("<{distance}>") })
                }
                _ => return Err("invalid tsquery payload: unknown operator".into()),
            };
            let text = if operator == OP_NOT {
                format!("!{}", format_postgres_tsquery_node(raw, offset, priority, depth + 1)?)
            } else {
                // Phrase is order-sensitive, so a phrase on its right keeps its parentheses.
                let right_min = if operator == OP_PHRASE { priority + 1 } else { priority };
                let right = format_postgres_tsquery_node(raw, offset, right_min, depth + 1)?;
                let left = format_postgres_tsquery_node(raw, offset, priority, depth + 1)?;
                format!("{left} {symbol} {right}")
            };
            Ok(if priority < min_priority {
                format!("( {text} )")
            } else {
                text
            })
        }
        _ => Err("invalid tsquery payload: unknown item type".into()),
    }
}

/// True for composite/`record` columns, which have no fixed type name to match on.
pub(crate) fn is_postgres_composite_type(ty: &Type) -> bool {
    *ty == Type::RECORD || matches!(ty.kind(), Kind::Composite(_))
}

pub(crate) fn is_postgres_composite_array_type(ty: &Type) -> bool {
    composite_array_element_type(ty).is_some()
}

/// Element type of an array of composites. `record[]` is a pseudo-type in
/// tokio-postgres, so its element type is not reachable through `Type::kind()`.
pub(crate) fn composite_array_element_type(ty: &Type) -> Option<Type> {
    if *ty == Type::RECORD_ARRAY {
        return Some(Type::RECORD);
    }
    match ty.kind() {
        Kind::Array(inner) if is_postgres_composite_type(inner) => Some(inner.clone()),
        _ => None,
    }
}

fn postgres_hex_escape(raw: &[u8]) -> String {
    let mut text = String::with_capacity(2 + raw.len() * 2);
    text.push_str("\\x");
    for byte in raw {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// Text form of a binary value of any type we can name, as psql would print it.
/// Types without a known renderer error out so callers can pick their own fallback.
pub(crate) fn format_postgres_value_text(ty: &Type, raw: &[u8]) -> FormatResult {
    match ty.kind() {
        Kind::Composite(fields) => {
            return format_postgres_record_value(raw, |index, oid| {
                fields
                    .get(index)
                    .map(|field| field.type_().clone())
                    .or_else(|| Type::from_oid(oid))
            });
        }
        Kind::Array(inner) => {
            let (dimensions, elements) = parse_postgres_array_elements(inner, raw)?;
            return Ok(format_postgres_array_text(inner, &dimensions, &elements));
        }
        Kind::Domain(inner) => return format_postgres_value_text(inner, raw),
        Kind::Enum(_) => return Ok(std::str::from_utf8(raw)?.to_string()),
        _ => {}
    }

    match ty.name() {
        "record" => format_postgres_record_value(raw, |_, oid| Type::from_oid(oid)),
        "bool" => Ok(if *raw.first().ok_or("invalid bool payload")? != 0 { "t" } else { "f" }.to_string()),
        "int2" => Ok(i16::from_be_bytes(read_be(raw, 0)?).to_string()),
        "int4" => Ok(i32::from_be_bytes(read_be(raw, 0)?).to_string()),
        "int8" => Ok(i64::from_be_bytes(read_be(raw, 0)?).to_string()),
        "float4" => Ok(format_float32(f32::from_be_bytes(read_be(raw, 0)?))),
        "float8" => Ok(format_float(f64::from_be_bytes(read_be(raw, 0)?))),
        "text" | "varchar" | "bpchar" | "name" | "unknown" | "citext" | "xml" | "json" => {
            Ok(std::str::from_utf8(raw)?.to_string())
        }
        // jsonb travels as a version byte followed by the text form.
        "jsonb" => Ok(std::str::from_utf8(raw.get(1..).ok_or("invalid jsonb payload")?)?.to_string()),
        "char" => Ok(raw.first().map(|byte| char::from(*byte).to_string()).unwrap_or_default()),
        "bytea" => Ok(postgres_hex_escape(raw)),
        "uuid" => Ok(uuid::Uuid::from_slice(raw)?.to_string()),
        "numeric" => PgNumericString::parse(raw),
        "date" => parse_postgres_date_value(raw),
        "timestamp" => parse_postgres_timestamp_value(raw),
        "timestamptz" => parse_postgres_timestamptz_value(raw),
        "time" => Ok(format_postgres_time_micros(i64::from_be_bytes(read_be(raw, 0)?))),
        name => format_postgres_known_binary_value(name, raw),
    }
}

/// record_out: `(a,"b c",)` with NULL fields left empty and empty strings quoted.
/// `field_type` maps (field index, wire OID) to the type used to render that field.
fn format_postgres_record_value(
    raw: &[u8],
    field_type: impl Fn(usize, u32) -> Option<Type>,
) -> FormatResult {
    let count = i32::from_be_bytes(read_be(raw, 0)?);
    let count = usize::try_from(count).map_err(|_| "invalid record payload: negative field count")?;
    let mut offset = 4;
    let mut fields = Vec::new();
    for index in 0..count {
        let oid = u32::from_be_bytes(read_be(raw, offset)?);
        let length = i32::from_be_bytes(read_be(raw, offset + 4)?);
        offset += 8;
        let Ok(length) = usize::try_from(length) else {
            fields.push(String::new());
            continue;
        };
        let bytes = raw
            .get(offset..offset + length)
            .ok_or("invalid record payload: truncated field")?;
        offset += length;
        let text = field_type(index, oid)
            .and_then(|ty| format_postgres_value_text(&ty, bytes).ok())
            .unwrap_or_else(|| postgres_hex_escape(bytes));
        fields.push(quote_postgres_record_field(&text));
    }
    Ok(format!("({})", fields.join(",")))
}

/// record_out quoting: empty strings and text holding parentheses, commas, quotes,
/// backslashes or whitespace go in double quotes, with `"` and `\` doubled.
fn quote_postgres_record_field(text: &str) -> String {
    let needs_quotes = text.is_empty()
        || text.chars().any(|ch| {
            matches!(ch, '(' | ')' | ',' | '"' | '\\') || ch.is_ascii_whitespace()
        });
    if needs_quotes {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\"\""))
    } else {
        text.to_string()
    }
}

/// Elements of a binary array in storage order (`None` = NULL) rendered with the
/// element type, plus each dimension's `(length, lower_bound)`.
pub(crate) fn parse_postgres_array_elements(
    element_type: &Type,
    raw: &[u8],
) -> std::result::Result<(Vec<(usize, i32)>, Vec<Option<String>>), Box<dyn std::error::Error + Sync + Send>> {
    let dimension_count = i32::from_be_bytes(read_be(raw, 0)?);
    let dimension_count =
        usize::try_from(dimension_count).map_err(|_| "invalid array payload: negative dimensions")?;
    let mut offset = 12;
    let mut dimensions = Vec::new();
    let mut element_count = if dimension_count == 0 { 0 } else { 1usize };
    for _ in 0..dimension_count {
        let length = i32::from_be_bytes(read_be(raw, offset)?);
        let lower_bound = i32::from_be_bytes(read_be(raw, offset + 4)?);
        offset += 8;
        let length = usize::try_from(length).map_err(|_| "invalid array payload: negative length")?;
        element_count = element_count
            .checked_mul(length)
            .ok_or("invalid array payload: dimension overflow")?;
        dimensions.push((length, lower_bound));
    }

    let mut elements = Vec::new();
    for _ in 0..element_count {
        let length = i32::from_be_bytes(read_be(raw, offset)?);
        offset += 4;
        let Ok(length) = usize::try_from(length) else {
            elements.push(None);
            continue;
        };
        let bytes = raw
            .get(offset..offset + length)
            .ok_or("invalid array payload: truncated element")?;
        offset += length;
        elements.push(Some(
            format_postgres_value_text(element_type, bytes)
                .unwrap_or_else(|_| postgres_hex_escape(bytes)),
        ));
    }
    Ok((dimensions, elements))
}

/// array_out: `{a,"b c",NULL}`, nested braces per dimension, `[lo:hi]=` prefix for non-1 bounds.
fn format_postgres_array_text(
    element_type: &Type,
    dimensions: &[(usize, i32)],
    elements: &[Option<String>],
) -> String {
    if dimensions.is_empty() {
        return "{}".to_string();
    }
    // box is the one built-in type whose array delimiter is not a comma.
    let delimiter = if element_type.name() == "box" { ';' } else { ',' };

    fn emit(
        dimensions: &[(usize, i32)],
        elements: &mut std::slice::Iter<'_, Option<String>>,
        delimiter: char,
        out: &mut String,
    ) {
        let Some(((length, _), rest)) = dimensions.split_first() else {
            return;
        };
        out.push('{');
        for index in 0..*length {
            if index > 0 {
                out.push(delimiter);
            }
            if rest.is_empty() {
                match elements.next() {
                    Some(Some(text)) => out.push_str(&quote_postgres_array_element(text, delimiter)),
                    _ => out.push_str("NULL"),
                }
            } else {
                emit(rest, elements, delimiter, out);
            }
        }
        out.push('}');
    }

    let mut out = String::new();
    if dimensions.iter().any(|(_, lower_bound)| *lower_bound != 1) {
        for (length, lower_bound) in dimensions {
            out.push_str(&format!("[{lower_bound}:{}]", i64::from(*lower_bound) + *length as i64 - 1));
        }
        out.push('=');
    }
    emit(dimensions, &mut elements.iter(), delimiter, &mut out);
    out
}

fn quote_postgres_array_element(text: &str, delimiter: char) -> String {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("null")
        || text.chars().any(|ch| {
            matches!(ch, '{' | '}' | '"' | '\\') || ch == delimiter || ch.is_ascii_whitespace()
        });
    if needs_quotes {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector_payload(values: &[f32]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&(values.len() as i16).to_be_bytes());
        raw.extend_from_slice(&0i16.to_be_bytes());
        for value in values {
            raw.extend_from_slice(&value.to_be_bytes());
        }
        raw
    }

    #[test]
    fn vector_decodes_to_pgvector_text_form() {
        let raw = vector_payload(&[0.1, 0.2, 0.3]);
        assert_eq!(
            format_postgres_vector_value(&raw).unwrap(),
            "[0.1,0.2,0.3]"
        );
    }

    #[test]
    fn empty_vector_decodes() {
        assert_eq!(format_postgres_vector_value(&vector_payload(&[])).unwrap(), "[]");
    }

    #[test]
    fn truncated_vector_errors_without_panicking() {
        let mut raw = vector_payload(&[1.0, 2.0]);
        raw.truncate(raw.len() - 3);
        assert!(format_postgres_vector_value(&raw).is_err());
        assert!(format_postgres_vector_value(&[0x00]).is_err());
    }

    #[test]
    fn halfvec_decodes_to_pgvector_text_form() {
        // 1.0 = 0x3C00, -2.0 = 0xC000, 0.5 = 0x3800 in IEEE half precision.
        let raw: Vec<u8> = [0x00, 0x03, 0x00, 0x00, 0x3C, 0x00, 0xC0, 0x00, 0x38, 0x00].into();
        assert_eq!(format_postgres_halfvec_value(&raw).unwrap(), "[1,-2,0.5]");
    }

    #[test]
    fn half_precision_conversion_covers_edge_cases() {
        assert_eq!(f32_from_f16_bits(0x0000), 0.0);
        assert_eq!(f32_from_f16_bits(0x8000), -0.0);
        assert_eq!(f32_from_f16_bits(0x3C00), 1.0);
        assert_eq!(f32_from_f16_bits(0x0001), 2.0f32.powi(-24)); // smallest subnormal
        assert_eq!(f32_from_f16_bits(0x7BFF), 65504.0); // largest finite half
        assert_eq!(f32_from_f16_bits(0x7C00), f32::INFINITY);
        assert_eq!(f32_from_f16_bits(0xFC00), f32::NEG_INFINITY);
        assert!(f32_from_f16_bits(0x7E00).is_nan());
    }

    #[test]
    fn sparsevec_decodes_with_one_based_indices() {
        // dim=5, nnz=2, entries {1:1.5, 4:-2} (0-based indices 0 and 3 on the wire).
        let mut raw = Vec::new();
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.extend_from_slice(&2i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        raw.extend_from_slice(&3i32.to_be_bytes());
        raw.extend_from_slice(&1.5f32.to_be_bytes());
        raw.extend_from_slice(&(-2.0f32).to_be_bytes());
        assert_eq!(
            format_postgres_sparsevec_value(&raw).unwrap(),
            "{1:1.5,4:-2}/5"
        );
    }

    #[test]
    fn truncated_sparsevec_errors_without_panicking() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&5i32.to_be_bytes());
        raw.extend_from_slice(&2i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        assert!(format_postgres_sparsevec_value(&raw).is_err());
    }

    fn record_payload(fields: &[(u32, Option<&[u8]>)]) -> Vec<u8> {
        let mut raw = (fields.len() as i32).to_be_bytes().to_vec();
        for (oid, value) in fields {
            raw.extend_from_slice(&oid.to_be_bytes());
            match value {
                Some(bytes) => {
                    raw.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    raw.extend_from_slice(bytes);
                }
                None => raw.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        raw
    }

    #[test]
    fn anonymous_record_decodes_like_record_out() {
        let raw = record_payload(&[(23, Some(&1i32.to_be_bytes())), (25, Some(b"a"))]);
        assert_eq!(format_postgres_value_text(&Type::RECORD, &raw).unwrap(), "(1,a)");
    }

    #[test]
    fn record_quotes_special_fields_and_leaves_nulls_empty() {
        let raw = record_payload(&[
            (25, None),
            (25, Some(b"")),
            (25, Some(b"a,b")),
            (25, Some(b"c\"d")),
            (25, Some(b"e\\f")),
            (25, Some(b"g h")),
            (25, Some(b"(x)")),
            (16, Some(&[1])),
        ]);
        assert_eq!(
            format_postgres_value_text(&Type::RECORD, &raw).unwrap(),
            r#"(,"","a,b","c""d","e\\f","g h","(x)",t)"#
        );
        assert_eq!(
            format_postgres_value_text(&Type::RECORD, &record_payload(&[(23, None)])).unwrap(),
            "()"
        );
    }

    #[test]
    fn record_renders_unknown_field_types_as_hex() {
        // OID 4294967000 is not a built-in type.
        let raw = record_payload(&[(4_294_967_000, Some(&[0xde, 0xad]))]);
        assert_eq!(
            format_postgres_value_text(&Type::RECORD, &raw).unwrap(),
            r#"("\\xdead")"#
        );
    }

    fn composite_type() -> Type {
        use tokio_postgres::types::Field;
        Type::new(
            "currency_amount".to_string(),
            90_001,
            Kind::Composite(vec![
                Field::new("currency".to_string(), Type::TEXT),
                Field::new("amount".to_string(), Type::NUMERIC),
            ]),
            "zqlz_lab".to_string(),
        )
    }

    fn numeric_payload(text: &str) -> Vec<u8> {
        let mut out = bytes::BytesMut::new();
        crate::connection::pg_value::encode_pg_numeric(text, &mut out).unwrap();
        out.to_vec()
    }

    #[test]
    fn declared_composite_uses_its_field_types() {
        // The wire OIDs are ignored in favour of the declared field types.
        let amount = numeric_payload("12.34");
        let raw = record_payload(&[(25, Some(b"USD")), (1700, Some(&amount))]);
        assert_eq!(
            format_postgres_value_text(&composite_type(), &raw).unwrap(),
            "(USD,12.34)"
        );
    }

    #[test]
    fn array_of_composites_yields_one_text_per_element() {
        let amount = numeric_payload("1.5");
        let element = record_payload(&[(25, Some(b"EUR")), (1700, Some(&amount))]);
        let array_type = Type::new(
            "_currency_amount".to_string(),
            90_002,
            Kind::Array(composite_type()),
            "zqlz_lab".to_string(),
        );
        assert!(is_postgres_composite_array_type(&array_type));

        let mut raw = Vec::new();
        for value in [1i32, 0, 90_001, 2, 1] {
            raw.extend_from_slice(&value.to_be_bytes());
        }
        raw.extend_from_slice(&(element.len() as i32).to_be_bytes());
        raw.extend_from_slice(&element);
        raw.extend_from_slice(&(-1i32).to_be_bytes());

        let (_, elements) = parse_postgres_array_elements(&composite_type(), &raw).unwrap();
        assert_eq!(elements, vec![Some("(EUR,1.5)".to_string()), None]);
        assert_eq!(
            format_postgres_value_text(&array_type, &raw).unwrap(),
            r#"{"(EUR,1.5)",NULL}"#
        );
    }

    #[test]
    fn array_text_quotes_like_array_out() {
        let element = Type::TEXT;
        let elements = ["a", "b c", "NULL", "q\"r", ""].map(|text| Some(text.to_string()));
        assert_eq!(
            format_postgres_array_text(&element, &[(5, 1)], &elements),
            r#"{a,"b c","NULL","q\"r",""}"#
        );
        let numbers = [1, 2, 3, 4].map(|n| Some(n.to_string()));
        assert_eq!(
            format_postgres_array_text(&Type::INT4, &[(2, 1), (2, 1)], &numbers),
            "{{1,2},{3,4}}"
        );
        assert_eq!(
            format_postgres_array_text(&Type::INT4, &[(2, 0)], &numbers[..2]),
            "[0:1]={1,2}"
        );
    }

    fn tsquery_operand(weight: u8, prefix: u8, text: &str) -> Vec<u8> {
        let mut raw = vec![1, weight, prefix];
        raw.extend_from_slice(text.as_bytes());
        raw.push(0);
        raw
    }

    fn tsquery_payload(items: &[Vec<u8>]) -> Vec<u8> {
        let mut raw = (items.len() as i32).to_be_bytes().to_vec();
        raw.extend(items.iter().flatten());
        raw
    }

    #[test]
    fn tsquery_decodes_to_infix_text() {
        // Captured from the server for 'quick & fox': operator, right operand, left operand.
        let raw = tsquery_payload(&[
            vec![2, 2],
            tsquery_operand(0, 0, "fox"),
            tsquery_operand(0, 0, "quick"),
        ]);
        assert_eq!(format_postgres_tsquery_value(&raw).unwrap(), "'quick' & 'fox'");
    }

    #[test]
    fn tsquery_parenthesises_by_priority_and_phrase_order() {
        // 'a' | 'b' & 'c' needs no parentheses, ('a' | 'b') & 'c' does.
        let and_or = tsquery_payload(&[
            vec![2, 2],
            tsquery_operand(0, 0, "c"),
            vec![2, 3],
            tsquery_operand(0, 0, "b"),
            tsquery_operand(0, 0, "a"),
        ]);
        assert_eq!(
            format_postgres_tsquery_value(&and_or).unwrap(),
            "( 'a' | 'b' ) & 'c'"
        );
        let or_and = tsquery_payload(&[
            vec![2, 3],
            vec![2, 2],
            tsquery_operand(0, 0, "c"),
            tsquery_operand(0, 0, "b"),
            tsquery_operand(0, 0, "a"),
        ]);
        assert_eq!(format_postgres_tsquery_value(&or_and).unwrap(), "'a' | 'b' & 'c'");
        // 'a' <-> ('b' <-> 'c'): phrase on the right keeps its parentheses.
        let phrase = |distance: u8| vec![2, 4, 0, distance];
        let right_nested = tsquery_payload(&[
            phrase(1),
            phrase(1),
            tsquery_operand(0, 0, "c"),
            tsquery_operand(0, 0, "b"),
            tsquery_operand(0, 0, "a"),
        ]);
        assert_eq!(
            format_postgres_tsquery_value(&right_nested).unwrap(),
            "'a' <-> ( 'b' <-> 'c' )"
        );
        let distance = tsquery_payload(&[
            phrase(2),
            tsquery_operand(0, 0, "b"),
            tsquery_operand(0, 0, "a"),
        ]);
        assert_eq!(format_postgres_tsquery_value(&distance).unwrap(), "'a' <2> 'b'");
    }

    #[test]
    fn tsquery_handles_not_weights_prefix_and_escapes() {
        let negated = tsquery_payload(&[vec![2, 1], tsquery_operand(0, 0, "it's")]);
        assert_eq!(format_postgres_tsquery_value(&negated).unwrap(), "!'it''s'");
        let weighted = tsquery_payload(&[tsquery_operand(8 | 4, 1, "a")]);
        assert_eq!(format_postgres_tsquery_value(&weighted).unwrap(), "'a':*AB");
        let negated_group = tsquery_payload(&[
            vec![2, 1],
            vec![2, 2],
            tsquery_operand(0, 0, "b"),
            tsquery_operand(0, 0, "a"),
        ]);
        assert_eq!(
            format_postgres_tsquery_value(&negated_group).unwrap(),
            "!( 'a' & 'b' )"
        );
        assert_eq!(format_postgres_tsquery_value(&tsquery_payload(&[])).unwrap(), "");
        assert!(format_postgres_tsquery_value(&tsquery_payload(&[vec![2, 2]])).is_err());
    }

    fn range_payload(flags: u8, bounds: &[&[u8]]) -> Vec<u8> {
        let mut raw = vec![flags];
        for bound in bounds {
            raw.extend_from_slice(&(bound.len() as i32).to_be_bytes());
            raw.extend_from_slice(bound);
        }
        raw
    }

    #[test]
    fn multirange_decodes_each_range() {
        let first = range_payload(0x02, &[&1i32.to_be_bytes(), &3i32.to_be_bytes()]);
        let second = range_payload(0x02, &[&5i32.to_be_bytes(), &7i32.to_be_bytes()]);
        let mut raw = 2i32.to_be_bytes().to_vec();
        for range in [&first, &second] {
            raw.extend_from_slice(&(range.len() as i32).to_be_bytes());
            raw.extend_from_slice(range);
        }
        assert_eq!(
            format_postgres_binary_display_value("int4multirange", &raw).unwrap(),
            "{[1,3),[5,7)}"
        );
        assert_eq!(
            format_postgres_binary_display_value("int4multirange", &0i32.to_be_bytes()).unwrap(),
            "{}"
        );
        raw.truncate(raw.len() - 2);
        assert!(format_postgres_binary_display_value("int4multirange", &raw).is_err());
    }

    #[test]
    fn timestamp_range_bounds_are_quoted_and_psql_formatted() {
        // 2025-01-01 10:00:00 and 12:00:00.5 as microseconds since 2000-01-01.
        let start = 789_040_800_000_000i64;
        let end = start + 7_200_500_000;
        let flags = 0x02;
        let tsrange = range_payload(flags, &[&start.to_be_bytes(), &end.to_be_bytes()]);
        assert_eq!(
            format_postgres_range_value("tsrange", &tsrange).unwrap(),
            r#"["2025-01-01 10:00:00","2025-01-01 12:00:00.5")"#
        );
        assert_eq!(
            format_postgres_range_value("tstzrange", &tsrange).unwrap(),
            r#"["2025-01-01 10:00:00+00","2025-01-01 12:00:00.5+00")"#
        );
        // [1999-01-01, infinity)
        let daterange = range_payload(0x02, &[&(-365i32).to_be_bytes(), &i32::MAX.to_be_bytes()]);
        assert_eq!(
            format_postgres_range_value("daterange", &daterange).unwrap(),
            "[1999-01-01,infinity)"
        );
        // Unbounded sides stay empty rather than becoming quoted empty strings.
        let unbounded = range_payload(0x08 | 0x04, &[&5i32.to_be_bytes()]);
        assert_eq!(
            format_postgres_range_value("int4range", &unbounded).unwrap(),
            "(,5]"
        );
    }

    #[test]
    fn snapshot_decodes_to_xmin_xmax_xip_list() {
        let mut raw = 3i32.to_be_bytes().to_vec();
        for value in [10u64, 20, 10, 14, 15] {
            raw.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(
            format_postgres_binary_display_value("txid_snapshot", &raw).unwrap(),
            "10:20:10,14,15"
        );
        assert_eq!(
            format_postgres_binary_display_value("pg_snapshot", &raw).unwrap(),
            "10:20:10,14,15"
        );
        let mut empty = 0i32.to_be_bytes().to_vec();
        for value in [5u64, 5] {
            empty.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(format_postgres_snapshot_value(&empty).unwrap(), "5:5:");
        raw.truncate(raw.len() - 1);
        assert!(format_postgres_snapshot_value(&raw).is_err());
    }

    fn interval_payload(micros: i64, days: i32, months: i32) -> Vec<u8> {
        let mut raw = micros.to_be_bytes().to_vec();
        raw.extend_from_slice(&days.to_be_bytes());
        raw.extend_from_slice(&months.to_be_bytes());
        raw
    }

    #[test]
    fn interval_matches_postgres_interval_style() {
        let cases = [
            (interval_payload(14_706_700_000, 3, 14), "1 year 2 mons 3 days 04:05:06.7"),
            (interval_payload(-1_000_000, -3, 0), "-3 days -00:00:01"),
            (interval_payload(0, 0, -14), "-1 years -2 mons"),
            (interval_payload(3_600_000_000, -1, 1), "1 mon -1 days +01:00:00"),
            (interval_payload(0, 0, 0), "00:00:00"),
            (interval_payload(0, 1, 12), "1 year 1 day"),
            (interval_payload(360_000_000_000, 0, 0), "100:00:00"),
        ];
        for (raw, expected) in cases {
            assert_eq!(format_postgres_interval_value(&raw).unwrap(), expected);
        }
    }

    #[test]
    fn geometry_matches_psql_spacing() {
        let doubles = |values: &[f64]| values.iter().flat_map(|v| v.to_be_bytes()).collect::<Vec<u8>>();
        let four = doubles(&[1.0, 1.0, 0.0, 0.0]);
        assert_eq!(format_postgres_lseg_value(&four).unwrap(), "[(1,1),(0,0)]");
        assert_eq!(format_postgres_box_value(&four).unwrap(), "(1,1),(0,0)");
        assert_eq!(
            format_postgres_circle_value(&doubles(&[0.0, 0.0, 2.0])).unwrap(),
            "<(0,0),2>"
        );
    }

    #[test]
    fn floats_switch_to_scientific_like_float8out() {
        assert_eq!(format_float(1e14), "100000000000000");
        assert_eq!(format_float(1e15), "1e+15");
        assert_eq!(format_float(0.0001), "0.0001");
        assert_eq!(format_float(0.00001), "1e-05");
        assert_eq!(format_float(1.5e-7), "1.5e-07");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(format_float32(16_777_216.0), "1.6777216e+07");
        assert_eq!(format_float32(123_456.7), "123456.7");
        assert_eq!(format_float32(1e6), "1e+06");
        assert_eq!(format_float32(0.1), "0.1");
        assert_eq!(format_float(2e20), "2e+20");
    }


    #[test]
    fn record_array_is_detected_despite_being_a_pseudo_type() {
        assert!(is_postgres_composite_array_type(&Type::RECORD_ARRAY));
        assert!(!is_postgres_composite_array_type(&Type::TEXT_ARRAY));
        assert_eq!(composite_array_element_type(&Type::RECORD_ARRAY), Some(Type::RECORD));
    }

    #[test]
    fn vector_types_dispatch_through_binary_display() {
        let raw = vector_payload(&[1.0]);
        assert_eq!(
            format_postgres_binary_display_value("vector", &raw).unwrap(),
            "[1]"
        );
    }
}
