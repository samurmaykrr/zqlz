//! Database object types for version control
//!
//! Defines the types of database objects that can be version controlled.

use serde::{Deserialize, Serialize};

/// Types of database objects that can be versioned
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatabaseObjectType {
    /// Stored procedure
    Procedure,
    /// User-defined function
    Function,
    /// Database view
    View,
    /// Materialized view
    MaterializedView,
    /// Database trigger
    Trigger,
    /// Check constraint or other constraint
    Constraint,
    /// Database index
    Index,
    /// Custom type or domain
    Type,
    /// Sequence
    Sequence,
    /// Event or scheduled job
    Event,
    /// Policy (row-level security)
    Policy,
    /// Other/unknown object type
    Other,
}

impl DatabaseObjectType {
    /// Convert to string representation for storage
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Procedure => "procedure",
            Self::Function => "function",
            Self::View => "view",
            Self::MaterializedView => "materialized_view",
            Self::Trigger => "trigger",
            Self::Constraint => "constraint",
            Self::Index => "index",
            Self::Type => "type",
            Self::Sequence => "sequence",
            Self::Event => "event",
            Self::Policy => "policy",
            Self::Other => "other",
        }
    }

    /// Parse from string representation
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "procedure" | "stored_procedure" => Self::Procedure,
            "function" => Self::Function,
            "view" => Self::View,
            "materialized_view" | "matview" => Self::MaterializedView,
            "trigger" => Self::Trigger,
            "constraint" | "check" | "foreign_key" | "primary_key" | "unique" => Self::Constraint,
            "index" => Self::Index,
            "type" | "domain" | "enum" => Self::Type,
            "sequence" => Self::Sequence,
            "event" | "job" | "scheduled_job" => Self::Event,
            "policy" | "rls_policy" => Self::Policy,
            _ => Self::Other,
        }
    }

    /// Human-readable display name
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Procedure => "Procedure",
            Self::Function => "Function",
            Self::View => "View",
            Self::MaterializedView => "Materialized View",
            Self::Trigger => "Trigger",
            Self::Constraint => "Constraint",
            Self::Index => "Index",
            Self::Type => "Type",
            Self::Sequence => "Sequence",
            Self::Event => "Event",
            Self::Policy => "Policy",
            Self::Other => "Other",
        }
    }

    /// Icon name for UI display (matches icon assets)
    pub fn icon_name(&self) -> &'static str {
        match self {
            Self::Procedure => "function",
            Self::Function => "function",
            Self::View => "table",
            Self::MaterializedView => "table",
            Self::Trigger => "zap",
            Self::Constraint => "lock",
            Self::Index => "list",
            Self::Type => "type",
            Self::Sequence => "hash",
            Self::Event => "clock",
            Self::Policy => "shield",
            Self::Other => "file",
        }
    }

    /// Whether this object type typically contains executable code
    pub fn is_executable(&self) -> bool {
        matches!(
            self,
            Self::Procedure | Self::Function | Self::Trigger | Self::Event
        )
    }

    /// Whether this object type can be "applied" back to the database
    pub fn is_applyable(&self) -> bool {
        matches!(
            self,
            Self::Procedure
                | Self::Function
                | Self::View
                | Self::MaterializedView
                | Self::Trigger
                | Self::Type
                | Self::Policy
        )
    }
}

impl Default for DatabaseObjectType {
    fn default() -> Self {
        Self::Other
    }
}

impl std::fmt::Display for DatabaseObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let types = [
            DatabaseObjectType::Procedure,
            DatabaseObjectType::Function,
            DatabaseObjectType::View,
            DatabaseObjectType::MaterializedView,
            DatabaseObjectType::Trigger,
        ];

        for t in types {
            let s = t.as_str();
            let parsed = DatabaseObjectType::from_str(s);
            assert_eq!(t, parsed, "Failed roundtrip for {:?}", t);
        }
    }

    #[test]
    fn test_from_str_variants() {
        assert_eq!(
            DatabaseObjectType::from_str("stored_procedure"),
            DatabaseObjectType::Procedure
        );
        assert_eq!(
            DatabaseObjectType::from_str("FUNCTION"),
            DatabaseObjectType::Function
        );
        assert_eq!(
            DatabaseObjectType::from_str("matview"),
            DatabaseObjectType::MaterializedView
        );
    }
}
