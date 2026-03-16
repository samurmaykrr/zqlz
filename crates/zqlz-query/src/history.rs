//! Query history management

use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::sync::Arc;
use uuid::Uuid;

fn normalize_history_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn preview_history_text(text: &str, max_length: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated: String = normalized.chars().take(max_length).collect();

    if normalized.chars().count() <= max_length {
        normalized
    } else {
        format!("{}...", truncated)
    }
}

/// A single query history entry
#[derive(Clone, Debug)]
pub struct QueryHistoryEntry {
    /// Unique identifier
    pub id: Uuid,

    /// The SQL query
    pub sql: String,

    /// Connection ID this was run against
    pub connection_id: Option<Uuid>,

    /// When the query was executed
    pub executed_at: DateTime<Utc>,

    /// Execution duration in milliseconds
    pub duration_ms: u64,

    /// Number of rows returned/affected
    pub row_count: Option<u64>,

    /// Error message if failed
    pub error: Option<String>,

    /// Whether the query succeeded
    pub success: bool,
}

impl QueryHistoryEntry {
    /// Create a successful history entry
    pub fn success(
        sql: String,
        connection_id: Option<Uuid>,
        duration_ms: u64,
        row_count: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sql,
            connection_id,
            executed_at: Utc::now(),
            duration_ms,
            row_count: Some(row_count),
            error: None,
            success: true,
        }
    }

    /// Create a failed history entry
    pub fn failure(
        sql: String,
        connection_id: Option<Uuid>,
        duration_ms: u64,
        error: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sql,
            connection_id,
            executed_at: Utc::now(),
            duration_ms,
            row_count: None,
            error: Some(error),
            success: false,
        }
    }

    /// Return a single-line preview of the SQL for compact UI rendering.
    pub fn sql_preview(&self, max_length: usize) -> String {
        preview_history_text(&self.sql, max_length)
    }

    /// Return a single-line preview of the error, if present.
    pub fn error_preview(&self, max_length: usize) -> Option<String> {
        self.error
            .as_ref()
            .map(|error| preview_history_text(error, max_length))
    }

    /// Whether this entry matches a user search query.
    pub fn matches_search(&self, query: &str) -> bool {
        let normalized_query = normalize_history_text(query);
        if normalized_query.is_empty() {
            return true;
        }

        normalize_history_text(&self.sql).contains(&normalized_query)
            || self
                .error
                .as_ref()
                .is_some_and(|error| normalize_history_text(error).contains(&normalized_query))
    }
}

/// Persistence backend for query history.
///
/// Implementors are responsible for durably storing and clearing entries.
/// This trait keeps `QueryHistory` decoupled from any specific storage format.
pub trait HistoryPersistence: Send + Sync {
    /// Write a newly added entry to durable storage.
    fn persist_entry(&self, entry: &QueryHistoryEntry);

    /// Remove all entries from durable storage.
    fn clear_all(&self);
}

/// Query history manager
pub struct QueryHistory {
    /// History entries (most recent first)
    entries: VecDeque<QueryHistoryEntry>,

    /// Maximum entries to keep
    max_entries: usize,

    /// Optional backend for durable storage of entries
    persistence: Option<Arc<dyn HistoryPersistence>>,
}

impl QueryHistory {
    /// Create a new query history
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
            persistence: None,
        }
    }

    /// Attach a persistence backend.
    ///
    /// After this call every new entry added via [`add`] is forwarded to the
    /// backend, and [`clear`] will also wipe it from persistent storage.
    pub fn set_persistence(&mut self, store: Arc<dyn HistoryPersistence>) {
        self.persistence = Some(store);
    }

    /// Load a previously persisted entry at startup.
    ///
    /// Unlike [`add`], this skips the consecutive-duplicate check and does not
    /// forward the entry to the persistence backend (it is already on disk).
    /// Entries must be supplied in **oldest-first** order so that the most
    /// recent one ends up at the front of the deque after all inserts.
    pub fn load_entry(&mut self, entry: QueryHistoryEntry) {
        self.entries.push_front(entry);
        // Trim in case the stored count somehow exceeds max_entries.
        while self.entries.len() > self.max_entries {
            self.entries.pop_back();
        }
    }

    /// Add an entry to history, skipping consecutive duplicates (same SQL run back-to-back).
    pub fn add(&mut self, entry: QueryHistoryEntry) {
        // Don't record the same SQL twice in a row — only consecutive deduplication,
        // so the same query appearing later after other queries is still recorded.
        if self
            .entries
            .front()
            .is_some_and(|last| last.sql.trim() == entry.sql.trim())
        {
            return;
        }

        tracing::debug!(
            query_id = %entry.id,
            success = entry.success,
            duration_ms = entry.duration_ms,
            "adding query to history"
        );

        if let Some(store) = &self.persistence {
            store.persist_entry(&entry);
        }

        self.entries.push_front(entry);
        while self.entries.len() > self.max_entries {
            self.entries.pop_back();
        }
    }

    /// Get all entries
    pub fn entries(&self) -> impl Iterator<Item = &QueryHistoryEntry> {
        self.entries.iter()
    }

    /// Get entries for a specific connection
    pub fn for_connection(&self, connection_id: Uuid) -> impl Iterator<Item = &QueryHistoryEntry> {
        self.entries
            .iter()
            .filter(move |e| e.connection_id == Some(connection_id))
    }

    /// Search history by SQL content
    pub fn search(&self, query: &str) -> impl Iterator<Item = &QueryHistoryEntry> {
        self.entries
            .iter()
            .filter(move |entry| entry.matches_search(query))
    }

    /// Clear all history
    pub fn clear(&mut self) {
        let count = self.entries.len();
        tracing::info!(entries_cleared = count, "clearing query history");
        if let Some(store) = &self.persistence {
            store.clear_all();
        }
        self.entries.clear();
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if history is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for QueryHistory {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::QueryHistoryEntry;

    #[test]
    fn search_matches_sql_across_newlines() {
        let entry = QueryHistoryEntry::success(
            "SELECT *\nFROM users\nWHERE id = 1".to_string(),
            None,
            12,
            1,
        );

        assert!(entry.matches_search("select * from users"));
        assert!(entry.matches_search("where id = 1"));
    }

    #[test]
    fn search_matches_error_text() {
        let entry = QueryHistoryEntry::failure(
            "SELECT * FROM users".to_string(),
            None,
            8,
            "syntax error near FROM".to_string(),
        );

        assert!(entry.matches_search("syntax error"));
    }

    #[test]
    fn preview_flattens_newlines() {
        let entry = QueryHistoryEntry::success("SELECT\n*\nFROM users".to_string(), None, 5, 1);

        assert_eq!(entry.sql_preview(100), "SELECT * FROM users");
    }
}
