//! Query history management

use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use uuid::Uuid;

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
}

/// Query history manager
pub struct QueryHistory {
    /// History entries (most recent first)
    entries: VecDeque<QueryHistoryEntry>,

    /// Maximum entries to keep
    max_entries: usize,
}

impl QueryHistory {
    /// Create a new query history
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    /// Add an entry to history
    pub fn add(&mut self, entry: QueryHistoryEntry) {
        tracing::debug!(
            query_id = %entry.id,
            success = entry.success,
            duration_ms = entry.duration_ms,
            "adding query to history"
        );
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
        let query_lower = query.to_lowercase();
        self.entries
            .iter()
            .filter(move |e| e.sql.to_lowercase().contains(&query_lower))
    }

    /// Clear all history
    pub fn clear(&mut self) {
        let count = self.entries.len();
        tracing::info!(entries_cleared = count, "clearing query history");
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
