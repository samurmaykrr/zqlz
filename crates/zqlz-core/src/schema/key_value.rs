use serde::{Deserialize, Serialize};

/// Key-Value store specific metadata (for Redis, Memcached, Valkey, etc.)
///
/// This extends `TableInfo` for key-value databases where each "table" is actually
/// a key with its value, type, size, and TTL information.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeyValueInfo {
    /// The type of the key (string, hash, list, set, zset, stream, etc.)
    pub key_type: String,
    /// Preview of the value (truncated for display)
    pub value_preview: Option<String>,
    /// Size in bytes (if available)
    pub size_bytes: Option<i64>,
    /// Time-to-live in seconds (-1 for no expiry, -2 for key not found)
    pub ttl_seconds: Option<i64>,
}

impl KeyValueInfo {
    /// Create new key-value info
    pub fn new(key_type: impl Into<String>) -> Self {
        Self {
            key_type: key_type.into(),
            value_preview: None,
            size_bytes: None,
            ttl_seconds: None,
        }
    }

    /// Set value preview
    pub fn with_value_preview(mut self, preview: impl Into<String>) -> Self {
        self.value_preview = Some(preview.into());
        self
    }

    /// Set size in bytes
    pub fn with_size(mut self, size: i64) -> Self {
        self.size_bytes = Some(size);
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl_seconds = Some(ttl);
        self
    }

    /// Format TTL for display
    pub fn format_ttl(&self) -> String {
        match self.ttl_seconds {
            None => "Unknown".to_string(),
            Some(-1) => "No TTL".to_string(),
            Some(-2) => "Key not found".to_string(),
            Some(ttl) if ttl < 60 => format!("{}s", ttl),
            Some(ttl) if ttl < 3600 => format!("{}m {}s", ttl / 60, ttl % 60),
            Some(ttl) if ttl < 86400 => format!("{}h {}m", ttl / 3600, (ttl % 3600) / 60),
            Some(ttl) => format!("{}d {}h", ttl / 86400, (ttl % 86400) / 3600),
        }
    }

    /// Format size for display
    pub fn format_size(&self) -> String {
        match self.size_bytes {
            None => "-".to_string(),
            Some(size) if size < 1024 => format!("{} B", size),
            Some(size) if size < 1024 * 1024 => format!("{:.1} KB", size as f64 / 1024.0),
            Some(size) => format!("{:.1} MB", size as f64 / (1024.0 * 1024.0)),
        }
    }
}
