//! Saved connection configuration

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A saved database connection configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedConnection {
    /// Unique identifier
    pub id: Uuid,

    /// Display name
    pub name: String,

    /// Driver type (sqlite, postgres, mysql, etc.)
    pub driver: String,

    /// Connection parameters (host, port, database, etc.)
    /// Sensitive values like passwords should be stored separately
    pub params: std::collections::HashMap<String, String>,

    /// Optional folder/group for organization
    pub folder: Option<String>,

    /// Optional color tag
    pub color: Option<String>,

    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Last modified timestamp
    pub modified_at: chrono::DateTime<chrono::Utc>,

    /// Last connected timestamp
    pub last_connected: Option<chrono::DateTime<chrono::Utc>>,
}

impl SavedConnection {
    /// Create a new saved connection
    pub fn new(name: String, driver: String) -> Self {
        tracing::debug!(name = %name, driver = %driver, "creating new saved connection");
        let now = chrono::Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            driver,
            params: std::collections::HashMap::new(),
            folder: None,
            color: None,
            created_at: now,
            modified_at: now,
            last_connected: None,
        }
    }

    /// Set a connection parameter
    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.params.insert(key.to_string(), value.to_string());
        self
    }
}
