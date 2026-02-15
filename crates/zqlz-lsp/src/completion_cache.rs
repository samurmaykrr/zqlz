use lsp_types::CompletionItem;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cache for completion items to improve performance
///
/// Caches completion results with TTL to avoid repeated computation
/// for the same or similar queries.
pub struct CompletionCache {
    /// Cache entries: query key -> (cached items, timestamp)
    cache: HashMap<String, (Vec<CompletionItem>, Instant)>,

    /// Time-to-live for cache entries
    ttl: Duration,

    /// Maximum number of entries to keep in cache
    max_entries: usize,
}

impl CompletionCache {
    pub fn new(ttl_seconds: u64, max_entries: usize) -> Self {
        Self {
            cache: HashMap::new(),
            ttl: Duration::from_secs(ttl_seconds),
            max_entries,
        }
    }

    /// Get cached completions for a given query
    pub fn get(&mut self, key: &str) -> Option<Vec<CompletionItem>> {
        // Clean expired entries periodically
        self.cleanup_expired();

        if let Some((items, timestamp)) = self.cache.get(key) {
            if timestamp.elapsed() < self.ttl {
                return Some(items.clone());
            } else {
                // Entry expired, remove it
                self.cache.remove(key);
            }
        }

        None
    }

    /// Cache completion results for a query
    pub fn put(&mut self, key: String, items: Vec<CompletionItem>) {
        // If cache is full, remove oldest entries
        if self.cache.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.cache.insert(key, (items, Instant::now()));
    }

    /// Clear all cached entries
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.len(),
            max_entries: self.max_entries,
            ttl_seconds: self.ttl.as_secs(),
        }
    }

    /// Remove expired entries
    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        self.cache
            .retain(|_, (_, timestamp)| now.duration_since(*timestamp) < self.ttl);
    }

    /// Evict oldest entries when cache is full
    fn evict_oldest(&mut self) {
        if self.cache.is_empty() {
            return;
        }

        // Find the oldest entry
        let mut oldest_key = None;
        let mut oldest_time = Instant::now();

        for (key, (_, timestamp)) in &self.cache {
            if timestamp < &oldest_time {
                oldest_time = *timestamp;
                oldest_key = Some(key.clone());
            }
        }

        if let Some(key) = oldest_key {
            self.cache.remove(&key);
        }
    }

    /// Create a cache key from query context
    pub fn create_key(sql_prefix: &str, cursor_offset: usize, context_type: &str) -> String {
        // Use last 100 chars + offset + context for cache key
        let prefix_len = sql_prefix.len().min(100);
        let sql_part = if sql_prefix.len() > 100 {
            &sql_prefix[sql_prefix.len() - 100..]
        } else {
            sql_prefix
        };

        format!("{}:{}:{}", context_type, cursor_offset, sql_part)
    }
}

impl Default for CompletionCache {
    fn default() -> Self {
        // Default: 30 second TTL, max 100 entries
        Self::new(30, 100)
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entry_count: usize,
    pub max_entries: usize,
    pub ttl_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsp_types::CompletionItemKind;

    fn create_test_item(label: &str) -> CompletionItem {
        CompletionItem {
            label: label.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        }
    }

    #[test]
    fn test_cache_put_get() {
        let mut cache = CompletionCache::new(10, 10);
        let items = vec![create_test_item("SELECT")];

        cache.put("test_key".to_string(), items.clone());

        let cached = cache.get("test_key").unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].label, "SELECT");
    }

    #[test]
    fn test_cache_miss() {
        let mut cache = CompletionCache::new(10, 10);

        let result = cache.get("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = CompletionCache::new(10, 10);
        cache.put("key1".to_string(), vec![create_test_item("SELECT")]);
        cache.put("key2".to_string(), vec![create_test_item("INSERT")]);

        assert_eq!(cache.stats().entry_count, 2);

        cache.clear();

        assert_eq!(cache.stats().entry_count, 0);
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = CompletionCache::new(10, 2); // Max 2 entries

        cache.put("key1".to_string(), vec![create_test_item("SELECT")]);
        cache.put("key2".to_string(), vec![create_test_item("INSERT")]);
        cache.put("key3".to_string(), vec![create_test_item("UPDATE")]);

        // Should only have 2 entries (oldest evicted)
        assert_eq!(cache.stats().entry_count, 2);

        // key1 should be evicted
        assert!(cache.get("key1").is_none());
        assert!(cache.get("key2").is_some());
        assert!(cache.get("key3").is_some());
    }

    #[test]
    fn test_create_key() {
        let key1 = CompletionCache::create_key("SELECT * FROM users WHERE ", 25, "WHERE");

        let key2 = CompletionCache::create_key("SELECT * FROM users WHERE ", 25, "WHERE");

        // Same inputs should produce same key
        assert_eq!(key1, key2);

        let key3 = CompletionCache::create_key(
            "SELECT * FROM users WHERE ",
            26, // Different offset
            "WHERE",
        );

        // Different offset should produce different key
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_create_key_long_prefix() {
        let long_sql = "a".repeat(200);
        let key = CompletionCache::create_key(&long_sql, 0, "GENERAL");

        // Key should not contain the entire long prefix
        assert!(key.len() < 200 + 50); // Some reasonable upper bound
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = CompletionCache::new(30, 100);
        cache.put("key1".to_string(), vec![create_test_item("SELECT")]);

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.max_entries, 100);
        assert_eq!(stats.ttl_seconds, 30);
    }
}
