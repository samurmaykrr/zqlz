//! Text diffing engine

use similar::{ChangeTag, TextDiff};

/// Diff engine for comparing text
pub struct DiffEngine;

impl DiffEngine {
    /// Create a unified diff between two texts
    pub fn unified_diff(old: &str, new: &str, context_lines: usize) -> String {
        let diff = TextDiff::from_lines(old, new);
        diff.unified_diff()
            .context_radius(context_lines)
            .to_string()
    }

    /// Get individual changes
    pub fn changes(old: &str, new: &str) -> Vec<Change> {
        let diff = TextDiff::from_lines(old, new);
        diff.iter_all_changes()
            .map(|change| {
                let tag = match change.tag() {
                    ChangeTag::Delete => ChangeType::Delete,
                    ChangeTag::Insert => ChangeType::Insert,
                    ChangeTag::Equal => ChangeType::Equal,
                };
                Change {
                    tag,
                    value: change.value().to_string(),
                    old_index: change.old_index(),
                    new_index: change.new_index(),
                }
            })
            .collect()
    }

    /// Check if two texts are identical
    pub fn is_identical(old: &str, new: &str) -> bool {
        old == new
    }
}

/// Type of change
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeType {
    Delete,
    Insert,
    Equal,
}

/// A single change in the diff
#[derive(Clone, Debug)]
pub struct Change {
    pub tag: ChangeType,
    pub value: String,
    pub old_index: Option<usize>,
    pub new_index: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_changes() {
        let old = "line1\nline2\nline3";
        let new = "line1\nline2 modified\nline3";

        let changes = DiffEngine::changes(old, new);
        assert!(!changes.is_empty());
    }

    #[test]
    fn test_identical() {
        let text = "hello world";
        assert!(DiffEngine::is_identical(text, text));
        assert!(!DiffEngine::is_identical(text, "hello"));
    }
}
