//! Redis command parsing and value/preview formatting helpers

use crate::RedisConnection;
use zqlz_core::{Result, Value, ZqlzError, command::{CommandTokenizer, parse_commands}};

const HYPERLOGLOG_MAGIC: &[u8; 4] = b"HYLL";

pub(super) type StreamEntryFields = Vec<(String, String)>;
pub(super) type StreamEntries = Vec<(String, StreamEntryFields)>;

pub(super) fn parse_single_redis_command(sql: &str) -> Result<(String, Vec<String>)> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(ZqlzError::Driver("Empty command".to_string()));
    }
    if has_unclosed_quote(trimmed) {
        return Err(ZqlzError::Driver(
            "Redis command contains an unterminated quoted string".to_string(),
        ));
    }

    let mut tokenizer = CommandTokenizer::new(trimmed, false);
    let tokens = tokenizer.tokenize();
    let commands = parse_commands(&tokens);

    match commands.as_slice() {
        [] => Err(ZqlzError::Driver("Empty command".to_string())),
        [command] => Ok((command.command.clone(), command.args.clone())),
        _ => Err(ZqlzError::Driver(
            "Redis driver executes one command at a time".to_string(),
        )),
    }
}

fn has_unclosed_quote(input: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;

    for character in input.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        match quote {
            Some(open_quote) if character == open_quote => quote = None,
            None if character == '"' || character == '\'' => quote = Some(character),
            _ => {}
        }
    }

    quote.is_some()
}

pub(super) fn truncate_text(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        value.to_string()
    } else {
        let take_len = max_len.saturating_sub(3);
        let prefix = value.chars().take(take_len).collect::<String>();
        format!("{}...", prefix)
    }
}

pub(super) fn bytes_to_display_string(bytes: &[u8]) -> String {
    if is_hyperloglog_payload(bytes) {
        return format!("HyperLogLog payload ({} B)", bytes.len());
    }

    if let Ok(value) = std::str::from_utf8(bytes)
        && value.chars().all(is_text_display_character)
    {
        return value.to_string();
    }

    let hex = bytes
        .iter()
        .take(32)
        .map(|byte| format!("{:02x}", byte))
        .collect::<Vec<_>>()
        .join(" ");
    let suffix = if bytes.len() > 32 { " ..." } else { "" };
    format!("Binary data ({} B): {}{}", bytes.len(), hex, suffix)
}

pub(super) fn bytes_to_zqlz_value(bytes: &[u8]) -> Value {
    if let Ok(value) = std::str::from_utf8(bytes)
        && value.chars().all(is_text_display_character)
    {
        return Value::String(value.to_string());
    }

    Value::Bytes(bytes.to_vec())
}

fn is_text_display_character(character: char) -> bool {
    !character.is_control() || matches!(character, '\n' | '\r' | '\t')
}

fn is_hyperloglog_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(HYPERLOGLOG_MAGIC)
}

pub(super) fn string_key_preview(key: &str, value: &redis::Value) -> String {
    match value {
        redis::Value::BulkString(bytes) if is_hyperloglog_payload(bytes) => {
            format!("HyperLogLog key ({} B). Run PFCOUNT {}", bytes.len(), key)
        }
        _ => RedisConnection::value_to_string(value),
    }
}

pub(super) fn array_preview(value: redis::Value, open: &str, close: &str) -> String {
    let items = match value {
        redis::Value::Array(items) => items
            .iter()
            .map(RedisConnection::value_to_string)
            .map(|value| truncate_text(&value, 24))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    format!("{}{}{}", open, items.join(", "), close)
}

pub(super) fn scan_items_preview(value: redis::Value) -> Option<Vec<String>> {
    let redis::Value::Array(items) = value else {
        return None;
    };
    let redis::Value::Array(values) = items.get(1)? else {
        return None;
    };
    Some(
        values
            .iter()
            .map(RedisConnection::value_to_string)
            .map(|value| truncate_text(&value, 24))
            .collect(),
    )
}

pub(super) fn hash_scan_preview(value: redis::Value) -> Option<String> {
    let values = scan_items_preview(value)?;
    let pairs = values
        .chunks(2)
        .filter(|chunk| chunk.len() == 2)
        .map(|chunk| format!("{}: {}", chunk[0], chunk[1]))
        .collect::<Vec<_>>();
    Some(pairs.join(", "))
}

pub(super) fn zset_preview(value: redis::Value) -> String {
    let redis::Value::Array(items) = value else {
        return String::new();
    };
    items
        .chunks(2)
        .filter(|chunk| chunk.len() == 2)
        .map(|chunk| {
            format!(
                "{}({})",
                truncate_text(&RedisConnection::value_to_string(&chunk[0]), 24),
                RedisConnection::value_to_string(&chunk[1])
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn stream_preview(value: redis::Value, length: i64) -> String {
    let redis::Value::Array(entries) = value else {
        return format!("{} entries", length);
    };
    let Some(redis::Value::Array(parts)) = entries.first() else {
        return format!("{} entries", length);
    };
    let Some(id) = parts.first().map(RedisConnection::value_to_string) else {
        return format!("{} entries", length);
    };
    format!("{} entries [{}]", length, truncate_text(&id, 32))
}

pub(super) fn stream_entries_to_json(value: &redis::Value) -> Result<String> {
    let redis::Value::Array(entries) = value else {
        return Ok("[]".to_string());
    };

    let mut serialized_entries = Vec::new();
    for entry in entries {
        let redis::Value::Array(parts) = entry else {
            continue;
        };
        if parts.len() != 2 {
            continue;
        }

        let id = RedisConnection::value_to_string(&parts[0]);
        let redis::Value::Array(field_values) = &parts[1] else {
            continue;
        };

        let mut fields = serde_json::Map::new();
        for chunk in field_values.chunks(2) {
            if chunk.len() != 2 {
                continue;
            }
            fields.insert(
                RedisConnection::value_to_string(&chunk[0]),
                serde_json::Value::String(RedisConnection::value_to_string(&chunk[1])),
            );
        }

        let mut entry_object = serde_json::Map::new();
        entry_object.insert("id".to_string(), serde_json::Value::String(id));
        entry_object.insert("fields".to_string(), serde_json::Value::Object(fields));
        serialized_entries.push(serde_json::Value::Object(entry_object));
    }

    serde_json::to_string(&serialized_entries)
        .map_err(|error| ZqlzError::Driver(format!("Invalid stream payload JSON: {}", error)))
}

pub(super) fn parse_stream_entries(value: &str) -> Result<StreamEntries> {
    let json = serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| ZqlzError::Driver(format!("Invalid stream payload JSON: {}", error)))?;
    let entries = json
        .as_array()
        .ok_or_else(|| ZqlzError::Driver("Stream payload must be a JSON array".to_string()))?;

    entries
        .iter()
        .map(|entry| {
            let object = entry.as_object().ok_or_else(|| {
                ZqlzError::Driver("Stream entries must be JSON objects".to_string())
            })?;
            let id = object
                .get("id")
                .and_then(|value| value.as_str())
                .filter(|id| !id.trim().is_empty())
                .unwrap_or("*")
                .to_string();
            let fields = object
                .get("fields")
                .and_then(|value| value.as_object())
                .ok_or_else(|| {
                    ZqlzError::Driver("Stream entry missing fields object".to_string())
                })?;
            let fields = fields
                .iter()
                .filter(|(field, _)| !field.trim().is_empty())
                .map(|(field, value)| {
                    let value = match value {
                        serde_json::Value::String(value) => value.clone(),
                        other => other.to_string(),
                    };
                    (field.clone(), value)
                })
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err(ZqlzError::Driver(
                    "Stream entries must contain at least one field".to_string(),
                ));
            }
            Ok((id, fields))
        })
        .collect()
}

#[cfg(test)]
mod display_tests {
    use super::*;

    #[test]
    fn bytes_to_display_string_keeps_plain_text() {
        assert_eq!(bytes_to_display_string(b"enabled"), "enabled");
    }

    #[test]
    fn bytes_to_display_string_formats_binary_payloads() {
        let value = bytes_to_display_string(&[0, 159, 146, 150]);
        assert_eq!(value, "Binary data (4 B): 00 9f 92 96");
    }

    #[test]
    fn string_key_preview_identifies_hyperloglog_payloads() {
        let value = redis::Value::BulkString(vec![b'H', b'Y', b'L', b'L', 1, 0, 0, 0]);
        assert_eq!(
            string_key_preview("zqlz:hll:test", &value),
            "HyperLogLog key (8 B). Run PFCOUNT zqlz:hll:test"
        );
    }

    #[test]
    fn bytes_to_zqlz_value_preserves_binary_bytes() {
        assert_eq!(
            bytes_to_zqlz_value(&[0, 159, 146, 150]),
            Value::Bytes(vec![0, 159, 146, 150])
        );
    }
}

#[cfg(test)]
mod zset_payload_tests {
    use super::*;

    #[test]
    fn parse_zset_items_accepts_member_score_object() {
        let mut items = RedisConnection::parse_zset_items(r#"{"gru":34800,"dfw":92783}"#).unwrap();

        // A JSON object has no inherent member order, and `serde_json::Map` iterates
        // in insertion order or sorted order depending on whether some other crate in
        // the build turned on `preserve_order` — gpui does. The only consumer feeds
        // ZADD, where pair order does not matter, so compare without depending on it.
        items.sort_by(|left, right| left.1.cmp(&right.1));
        assert_eq!(
            items,
            vec![(92783.0, "dfw".to_string()), (34800.0, "gru".to_string())]
        );
    }

    #[test]
    fn parse_zset_items_accepts_score_member_objects() {
        let items =
            RedisConnection::parse_zset_items(r#"[{"score":34800,"member":"gru"}]"#).unwrap();
        assert_eq!(items, vec![(34800.0, "gru".to_string())]);
    }

    #[test]
    fn parse_zset_items_accepts_score_member_pairs() {
        let items = RedisConnection::parse_zset_items(r#"[34800,"gru",92783,"dfw"]"#).unwrap();
        assert_eq!(
            items,
            vec![(34800.0, "gru".to_string()), (92783.0, "dfw".to_string())]
        );
    }

    #[test]
    fn parse_stream_entries_accepts_entry_objects() {
        let items = RedisConnection::parse_stream_entries(
            r#"[{"id":"1778802803694-1","fields":{"player":"ada","severity":"low"}}]"#,
        )
        .unwrap();

        assert_eq!(
            items,
            vec![(
                "1778802803694-1".to_string(),
                vec![
                    ("player".to_string(), "ada".to_string()),
                    ("severity".to_string(), "low".to_string())
                ]
            )]
        );
    }
}
