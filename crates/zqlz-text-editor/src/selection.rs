//! Selection state and operations.
//!
//! This module handles text selection in the editor. A selection is defined by:
//! - **Anchor**: The fixed point where the selection started
//! - **Head**: The moving point (usually the cursor position)
//!
//! The selection range is from min(anchor, head) to max(anchor, head).
//! This allows selecting both forwards and backwards.
//!
//! ## Selection Modes
//!
//! Currently supports:
//! - Character mode: Select individual characters
//!
//! Future phases will add:
//! - Line mode: Select entire lines (triple-click, Shift+Down/Up)
//! - Block mode: Select rectangular regions (Alt+Shift+arrows)
//! - Multiple selections: Multiple cursors with independent selections

use crate::buffer::{Position, Range};

/// The mode of selection (character, line, or block/column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SelectionMode {
    /// Standard character selection (single contiguous range).
    #[default]
    Character,
    /// Block/column (rectangular) selection. The anchor and head define
    /// opposite corners of the rectangle.
    Block,
}

/// Selection state for the text editor.
///
/// A selection is defined by an anchor (where selection started) and a head
/// (where the cursor is now). The actual selection range is always from
/// min(anchor, head) to max(anchor, head).
///
/// When there's no selection, anchor == head (both at cursor position).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    /// The anchor point (where selection started)
    anchor: Position,

    /// The head point (where the cursor is)
    head: Position,

    /// Selection mode (character or block).
    mode: SelectionMode,
}

impl Selection {
    /// Create a new selection with no range (anchor == head)
    pub fn new() -> Self {
        Self {
            anchor: Position::zero(),
            head: Position::zero(),
            mode: SelectionMode::Character,
        }
    }

    /// Create a selection at a specific position with no range
    pub fn at(position: Position) -> Self {
        Self {
            anchor: position,
            head: position,
            mode: SelectionMode::Character,
        }
    }

    /// Create a selection from anchor to head
    pub fn from_anchor_head(anchor: Position, head: Position) -> Self {
        Self { anchor, head, mode: SelectionMode::Character }
    }

    /// Get the anchor position
    pub fn anchor(&self) -> Position {
        self.anchor
    }

    /// Get the head position (cursor position)
    pub fn head(&self) -> Position {
        self.head
    }

    /// Set both anchor and head to the same position (clear selection)
    pub fn set_position(&mut self, position: Position) {
        self.anchor = position;
        self.head = position;
    }

    /// Move the head while keeping the anchor fixed (extend selection)
    pub fn set_head(&mut self, head: Position) {
        self.head = head;
    }

    /// Move both anchor and head to the same position (move cursor without selecting)
    pub fn move_to(&mut self, position: Position) {
        self.anchor = position;
        self.head = position;
    }

    /// Start a new selection at the given position
    pub fn start_selection(&mut self, anchor: Position) {
        self.anchor = anchor;
        self.head = anchor;
    }

    /// Extend the selection to the given position
    pub fn extend_to(&mut self, head: Position) {
        self.head = head;
    }

    /// Check if there is an active selection (anchor != head)
    pub fn has_selection(&self) -> bool {
        self.anchor != self.head
    }

    /// Get the selection range (normalized to start <= end)
    pub fn range(&self) -> Range {
        if self.anchor <= self.head {
            Range::new(self.anchor, self.head)
        } else {
            Range::new(self.head, self.anchor)
        }
    }

    /// Clear the selection (set anchor to head)
    pub fn clear(&mut self) {
        self.anchor = self.head;
    }

    /// Select all text in the buffer
    pub fn select_all(&mut self, buffer_end: Position) {
        self.anchor = Position::zero();
        self.head = buffer_end;
    }

    /// Get the start position of the selection (min of anchor and head)
    pub fn start(&self) -> Position {
        if self.anchor <= self.head {
            self.anchor
        } else {
            self.head
        }
    }

    /// Get the end position of the selection (max of anchor and head)
    pub fn end(&self) -> Position {
        if self.anchor <= self.head {
            self.head
        } else {
            self.anchor
        }
    }

    /// Check if the selection is reversed (head < anchor)
    pub fn is_reversed(&self) -> bool {
        self.head < self.anchor
    }

    /// Get the current selection mode.
    pub fn mode(&self) -> SelectionMode {
        self.mode
    }

    /// Set the selection mode.
    pub fn set_mode(&mut self, mode: SelectionMode) {
        self.mode = mode;
    }

    /// Returns true if this is a block (column/rectangular) selection.
    pub fn is_block(&self) -> bool {
        self.mode == SelectionMode::Block
    }

    /// For block selections, returns the per-line column ranges forming the rectangle.
    /// Each entry is `(line, start_col, end_col)`.
    pub fn block_ranges(&self) -> Vec<(usize, usize, usize)> {
        if !self.is_block() || !self.has_selection() {
            return Vec::new();
        }
        let top_line = self.anchor.line.min(self.head.line);
        let bottom_line = self.anchor.line.max(self.head.line);
        let left_col = self.anchor.column.min(self.head.column);
        let right_col = self.anchor.column.max(self.head.column);

        (top_line..=bottom_line)
            .map(|line| (line, left_col, right_col))
            .collect()
    }
}

impl Default for Selection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_selection() {
        let sel = Selection::new();
        assert_eq!(sel.anchor(), Position::zero());
        assert_eq!(sel.head(), Position::zero());
        assert!(!sel.has_selection());
    }

    #[test]
    fn test_selection_at_position() {
        let pos = Position::new(5, 10);
        let sel = Selection::at(pos);
        assert_eq!(sel.anchor(), pos);
        assert_eq!(sel.head(), pos);
        assert!(!sel.has_selection());
    }

    #[test]
    fn test_selection_range_forward() {
        let anchor = Position::new(1, 5);
        let head = Position::new(3, 10);
        let sel = Selection::from_anchor_head(anchor, head);

        assert!(sel.has_selection());
        assert_eq!(sel.range().start, anchor);
        assert_eq!(sel.range().end, head);
        assert_eq!(sel.start(), anchor);
        assert_eq!(sel.end(), head);
        assert!(!sel.is_reversed());
    }

    #[test]
    fn test_selection_range_backward() {
        let anchor = Position::new(3, 10);
        let head = Position::new(1, 5);
        let sel = Selection::from_anchor_head(anchor, head);

        assert!(sel.has_selection());
        assert_eq!(sel.range().start, head);
        assert_eq!(sel.range().end, anchor);
        assert_eq!(sel.start(), head);
        assert_eq!(sel.end(), anchor);
        assert!(sel.is_reversed());
    }

    #[test]
    fn test_extend_selection() {
        let mut sel = Selection::at(Position::new(2, 5));
        assert!(!sel.has_selection());

        sel.extend_to(Position::new(4, 10));
        assert!(sel.has_selection());
        assert_eq!(sel.anchor(), Position::new(2, 5));
        assert_eq!(sel.head(), Position::new(4, 10));
    }

    #[test]
    fn test_clear_selection() {
        let mut sel = Selection::from_anchor_head(Position::new(1, 0), Position::new(3, 5));
        assert!(sel.has_selection());

        sel.clear();
        assert!(!sel.has_selection());
        assert_eq!(sel.anchor(), sel.head());
    }

    #[test]
    fn test_move_to() {
        let mut sel = Selection::from_anchor_head(Position::new(1, 0), Position::new(3, 5));

        let new_pos = Position::new(5, 8);
        sel.move_to(new_pos);

        assert!(!sel.has_selection());
        assert_eq!(sel.anchor(), new_pos);
        assert_eq!(sel.head(), new_pos);
    }

    #[test]
    fn test_select_all() {
        let mut sel = Selection::new();
        let end = Position::new(100, 50);

        sel.select_all(end);
        assert!(sel.has_selection());
        assert_eq!(sel.start(), Position::zero());
        assert_eq!(sel.end(), end);
    }

    #[test]
    fn test_start_selection_then_extend() {
        let mut sel = Selection::new();
        let start_pos = Position::new(2, 3);

        sel.start_selection(start_pos);
        assert_eq!(sel.anchor(), start_pos);
        assert_eq!(sel.head(), start_pos);
        assert!(!sel.has_selection());

        sel.extend_to(Position::new(4, 7));
        assert!(sel.has_selection());
        assert_eq!(sel.anchor(), start_pos);
        assert_eq!(sel.head(), Position::new(4, 7));
    }

    #[test]
    fn test_set_head() {
        let mut sel = Selection::at(Position::new(1, 0));
        let new_head = Position::new(2, 5);

        sel.set_head(new_head);
        assert!(sel.has_selection());
        assert_eq!(sel.anchor(), Position::new(1, 0));
        assert_eq!(sel.head(), new_head);
    }

    #[test]
    fn test_set_position() {
        let mut sel = Selection::from_anchor_head(Position::new(1, 0), Position::new(3, 5));

        let new_pos = Position::new(5, 2);
        sel.set_position(new_pos);

        assert!(!sel.has_selection());
        assert_eq!(sel.anchor(), new_pos);
        assert_eq!(sel.head(), new_pos);
    }
}
