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

use crate::buffer::{AnchoredRange, Position, Range};
use crate::cursor::Cursor;

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
        Self {
            anchor,
            head,
            mode: SelectionMode::Character,
        }
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

    pub fn from_anchored_range(range: AnchoredRange, buffer_range: Range) -> Self {
        if range.start.offset() <= range.end.offset() {
            Self::from_anchor_head(buffer_range.start, buffer_range.end)
        } else {
            Self::from_anchor_head(buffer_range.end, buffer_range.start)
        }
    }
}

impl Default for Selection {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionEntry {
    pub cursor: Cursor,
    pub selection: Selection,
}

pub type CursorSelectionPair = (Cursor, Selection);
pub type PrimaryAndExtraSelections = (Cursor, Selection, Vec<CursorSelectionPair>);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SelectionsCollection {
    entries: Vec<SelectionEntry>,
    primary_index: usize,
    newest_index: usize,
}

impl SelectionsCollection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn single(cursor: Cursor, selection: Selection) -> Self {
        Self {
            entries: vec![SelectionEntry { cursor, selection }],
            primary_index: 0,
            newest_index: 0,
        }
    }

    pub fn from_primary_and_extras(
        cursor: Cursor,
        selection: Selection,
        extra_cursors: Vec<(Cursor, Selection)>,
    ) -> Self {
        let mut entries = vec![SelectionEntry { cursor, selection }];
        entries.extend(
            extra_cursors
                .into_iter()
                .map(|(cursor, selection)| SelectionEntry { cursor, selection }),
        );
        let newest_index = entries.len().saturating_sub(1);
        Self {
            entries,
            primary_index: 0,
            newest_index,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn primary(&self) -> Option<&SelectionEntry> {
        self.entries.get(self.primary_index)
    }

    pub fn primary_mut(&mut self) -> Option<&mut SelectionEntry> {
        self.entries.get_mut(self.primary_index)
    }

    pub fn newest(&self) -> Option<&SelectionEntry> {
        self.entries.get(self.newest_index)
    }

    pub fn all(&self) -> &[SelectionEntry] {
        &self.entries
    }

    pub fn all_mut(&mut self) -> &mut [SelectionEntry] {
        &mut self.entries
    }

    pub fn primary_index(&self) -> usize {
        self.primary_index
    }

    pub fn newest_index(&self) -> usize {
        self.newest_index
    }

    pub fn push(&mut self, cursor: Cursor, selection: Selection) {
        self.entries.push(SelectionEntry { cursor, selection });
        self.newest_index = self.entries.len().saturating_sub(1);
    }

    pub fn extra_entries(&self) -> &[SelectionEntry] {
        if self.entries.len() <= 1 {
            &[]
        } else {
            &self.entries[1..]
        }
    }

    pub fn extra_entries_mut(&mut self) -> &mut [SelectionEntry] {
        if self.entries.len() <= 1 {
            &mut []
        } else {
            &mut self.entries[1..]
        }
    }

    pub fn primary_and_extras(&self) -> Option<PrimaryAndExtraSelections> {
        let primary = self.primary()?.clone();
        let extras = self
            .extra_entries()
            .iter()
            .map(|entry| (entry.cursor.clone(), entry.selection.clone()))
            .collect();
        Some((primary.cursor, primary.selection, extras))
    }

    pub fn from_entries(
        entries: Vec<SelectionEntry>,
        primary_index: usize,
        newest_index: usize,
    ) -> Option<Self> {
        if entries.is_empty() {
            return None;
        }

        let last_index = entries.len().saturating_sub(1);
        Some(Self {
            entries,
            primary_index: primary_index.min(last_index),
            newest_index: newest_index.min(last_index),
        })
    }

    pub fn disjoint_ranges(&self) -> Vec<Range> {
        let mut ranges = self
            .entries
            .iter()
            .map(|entry| entry.selection.range())
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| (range.start, range.end));

        let mut merged_ranges = Vec::with_capacity(ranges.len());
        for range in ranges {
            let should_merge = merged_ranges
                .last()
                .map(|previous: &Range| ranges_overlap(previous, &range))
                .unwrap_or(false);
            if should_merge {
                if let Some(previous) = merged_ranges.last_mut() {
                    previous.end = previous.end.max(range.end);
                }
            } else {
                merged_ranges.push(range);
            }
        }
        merged_ranges
    }

    pub fn normalized(&self) -> Self {
        let mut indexed_entries = self.entries.iter().cloned().enumerate().collect::<Vec<_>>();
        indexed_entries.sort_by_key(|(index, entry)| {
            let range = entry.selection.range();
            (range.start, range.end, *index)
        });

        let mut normalized_entries = Vec::with_capacity(indexed_entries.len());
        let mut merged_original_indexes: Vec<Vec<usize>> =
            Vec::with_capacity(indexed_entries.len());

        for (original_index, entry) in indexed_entries {
            let should_merge = normalized_entries
                .last()
                .map(|existing: &SelectionEntry| {
                    let existing_range = existing.selection.range();
                    let next_range = entry.selection.range();
                    ranges_overlap(&existing_range, &next_range)
                })
                .unwrap_or(false);

            if should_merge {
                if let Some(existing) = normalized_entries.last_mut() {
                    let merged_range =
                        merge_ranges(existing.selection.range(), entry.selection.range());
                    existing.selection =
                        Selection::from_anchor_head(merged_range.start, merged_range.end);
                    existing.cursor = Cursor::at(merged_range.end);
                }
                if let Some(indexes) = merged_original_indexes.last_mut() {
                    indexes.push(original_index);
                }
            } else {
                normalized_entries.push(entry);
                merged_original_indexes.push(vec![original_index]);
            }
        }

        let primary_index = merged_original_indexes
            .iter()
            .position(|indexes| indexes.contains(&self.primary_index))
            .unwrap_or(0);
        let newest_index = merged_original_indexes
            .iter()
            .position(|indexes| indexes.contains(&self.newest_index))
            .unwrap_or(primary_index);

        Self {
            entries: normalized_entries,
            primary_index,
            newest_index,
        }
    }
}

fn ranges_overlap(left: &Range, right: &Range) -> bool {
    left.start <= right.end && right.start <= left.end
}

fn merge_ranges(left: Range, right: Range) -> Range {
    Range::new(left.start.min(right.start), left.end.max(right.end))
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

    #[test]
    fn test_from_anchored_range_preserves_forward_direction() {
        use crate::buffer::{Anchor, Bias};

        let anchored = AnchoredRange::new(
            Anchor::new(1, 0, Bias::Left),
            Anchor::new(4, 0, Bias::Right),
        );
        let selection = Selection::from_anchored_range(
            anchored,
            Range::new(Position::new(0, 1), Position::new(0, 4)),
        );

        assert_eq!(selection.anchor(), Position::new(0, 1));
        assert_eq!(selection.head(), Position::new(0, 4));
        assert!(!selection.is_reversed());
    }

    #[test]
    fn test_from_anchored_range_preserves_reversed_direction() {
        use crate::buffer::{Anchor, Bias};

        let anchored = AnchoredRange::new(
            Anchor::new(8, 0, Bias::Right),
            Anchor::new(3, 0, Bias::Left),
        );
        let selection = Selection::from_anchored_range(
            anchored,
            Range::new(Position::new(0, 3), Position::new(0, 8)),
        );

        assert_eq!(selection.anchor(), Position::new(0, 8));
        assert_eq!(selection.head(), Position::new(0, 3));
        assert!(selection.is_reversed());
    }

    #[test]
    fn test_collection_primary_and_extras_round_trips_entries() {
        let collection = SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(0, 1)),
            Selection::at(Position::new(0, 1)),
            vec![
                (
                    Cursor::at(Position::new(1, 2)),
                    Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
                ),
                (
                    Cursor::at(Position::new(2, 3)),
                    Selection::at(Position::new(2, 3)),
                ),
            ],
        );

        let (cursor, selection, extras) = collection
            .primary_and_extras()
            .expect("collection should have a primary entry");

        assert_eq!(cursor.position(), Position::new(0, 1));
        assert_eq!(selection.head(), Position::new(0, 1));
        assert_eq!(extras.len(), 2);
        assert_eq!(extras[0].0.position(), Position::new(1, 2));
        assert_eq!(extras[1].0.position(), Position::new(2, 3));
    }

    #[test]
    fn collection_normalization_sorts_and_merges_overlapping_entries() {
        let collection = SelectionsCollection::from_entries(
            vec![
                SelectionEntry {
                    cursor: Cursor::at(Position::new(0, 5)),
                    selection: Selection::from_anchor_head(
                        Position::new(0, 3),
                        Position::new(0, 5),
                    ),
                },
                SelectionEntry {
                    cursor: Cursor::at(Position::new(0, 2)),
                    selection: Selection::from_anchor_head(
                        Position::new(0, 1),
                        Position::new(0, 2),
                    ),
                },
                SelectionEntry {
                    cursor: Cursor::at(Position::new(0, 4)),
                    selection: Selection::from_anchor_head(
                        Position::new(0, 2),
                        Position::new(0, 4),
                    ),
                },
            ],
            0,
            2,
        )
        .expect("entries");

        let normalized = collection.normalized();

        assert_eq!(normalized.len(), 1);
        let primary = normalized.primary().expect("primary selection");
        assert_eq!(
            primary.selection.range(),
            Range::new(Position::new(0, 1), Position::new(0, 5))
        );
        assert_eq!(primary.cursor.position(), Position::new(0, 5));
    }

    #[test]
    fn collection_disjoint_ranges_merge_overlaps_deterministically() {
        let collection = SelectionsCollection::from_entries(
            vec![
                SelectionEntry {
                    cursor: Cursor::at(Position::new(0, 2)),
                    selection: Selection::from_anchor_head(
                        Position::new(0, 0),
                        Position::new(0, 2),
                    ),
                },
                SelectionEntry {
                    cursor: Cursor::at(Position::new(0, 4)),
                    selection: Selection::from_anchor_head(
                        Position::new(0, 1),
                        Position::new(0, 4),
                    ),
                },
                SelectionEntry {
                    cursor: Cursor::at(Position::new(1, 1)),
                    selection: Selection::from_anchor_head(
                        Position::new(1, 0),
                        Position::new(1, 1),
                    ),
                },
            ],
            0,
            2,
        )
        .expect("entries");

        assert_eq!(
            collection.disjoint_ranges(),
            vec![
                Range::new(Position::new(0, 0), Position::new(0, 4)),
                Range::new(Position::new(1, 0), Position::new(1, 1)),
            ]
        );
    }
}
