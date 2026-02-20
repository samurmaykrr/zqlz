//! Text buffer implementation using rope data structure.
//!
//! ## Why Rope?
//!
//! A rope is a tree-based data structure optimized for efficient insertion and deletion
//! at arbitrary positions in a large text document. Unlike a simple String:
//!
//! - **Insertion/Deletion**: O(log n) instead of O(n)
//! - **Line access**: O(log n) instead of O(n)
//! - **Memory efficiency**: Shares unchanged portions, good for undo/redo
//! - **Large files**: Can handle millions of lines without performance degradation
//!
//! We use the `ropey` crate, which is mature, well-tested, and used in production
//! text editors like Helix and Lapce.
//!
//! ## Position vs Offset
//!
//! Throughout this module, we use two ways to refer to positions in the text:
//!
//! - **Position**: A `(line, column)` tuple (0-indexed, column is UTF-8 byte offset within line)
//! - **Offset**: A single byte offset from the start of the buffer (0-indexed)
//!
//! The buffer provides efficient conversion between these representations.

use anyhow::{anyhow, Result};
use ropey::Rope;
use std::time::SystemTime;

/// A position in the text buffer.
///
/// Both line and column are 0-indexed. Column is measured in UTF-8 bytes,
/// not grapheme clusters or characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub line: usize,
    pub column: usize,
}

impl Position {
    pub fn new(line: usize, column: usize) -> Self {
        Self { line, column }
    }

    pub fn zero() -> Self {
        Self { line: 0, column: 0 }
    }
}

/// A range in the text buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

impl Range {
    pub fn new(start: Position, end: Position) -> Self {
        Self { start, end }
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// Represents a change made to the text buffer.
///
/// Changes track insertions and deletions for:
/// - Undo/redo history management
/// - LSP textDocument/didChange notifications
/// - Change event subscriptions
///
/// Each change includes:
/// - The byte range affected
/// - The old text (what was there before)
/// - The new text (what is there now)
/// - A timestamp for ordering
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// The byte offset where the change starts
    pub offset: usize,

    /// The text that was removed (empty for pure insertions)
    pub old_text: String,

    /// The text that was inserted (empty for pure deletions)
    pub new_text: String,

    /// When the change occurred
    pub timestamp: SystemTime,
}

impl Change {
    /// Creates a new change representing an insertion.
    pub fn insert(offset: usize, text: impl Into<String>) -> Self {
        Self {
            offset,
            old_text: String::new(),
            new_text: text.into(),
            timestamp: SystemTime::now(),
        }
    }

    /// Creates a new change representing a deletion.
    pub fn delete(offset: usize, text: impl Into<String>) -> Self {
        Self {
            offset,
            old_text: text.into(),
            new_text: String::new(),
            timestamp: SystemTime::now(),
        }
    }

    /// Creates a new change representing a replacement.
    pub fn replace(
        offset: usize,
        old_text: impl Into<String>,
        new_text: impl Into<String>,
    ) -> Self {
        Self {
            offset,
            old_text: old_text.into(),
            new_text: new_text.into(),
            timestamp: SystemTime::now(),
        }
    }

    /// Returns the inverse of this change (for undo).
    ///
    /// The inverse swaps old_text and new_text, allowing the change to be reverted.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::Change;
    ///
    /// let change = Change::insert(5, "Hello");
    /// let inverse = change.inverse();
    ///
    /// assert_eq!(inverse.offset, 5);
    /// assert_eq!(inverse.old_text, "Hello");
    /// assert_eq!(inverse.new_text, "");
    /// ```
    pub fn inverse(&self) -> Self {
        Self {
            offset: self.offset,
            old_text: self.new_text.clone(),
            new_text: self.old_text.clone(),
            timestamp: SystemTime::now(),
        }
    }

    /// Returns true if this change is an insertion (no text removed).
    pub fn is_insertion(&self) -> bool {
        self.old_text.is_empty() && !self.new_text.is_empty()
    }

    /// Returns true if this change is a deletion (no text added).
    pub fn is_deletion(&self) -> bool {
        !self.old_text.is_empty() && self.new_text.is_empty()
    }

    /// Returns true if this change is a replacement (both text removed and added).
    pub fn is_replacement(&self) -> bool {
        !self.old_text.is_empty() && !self.new_text.is_empty()
    }

    /// Returns the byte range that this change affects.
    pub fn range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.old_text.len()
    }

    /// Returns the byte range after this change is applied.
    pub fn new_range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.new_text.len()
    }
}

/// An efficient text buffer using a rope data structure.
///
/// The buffer stores text in a rope (tree of text chunks) which provides:
/// - O(log n) insertion and deletion at any position
/// - O(log n) line access
/// - O(log n) position/offset conversions
/// - Efficient memory usage for large files
///
/// The buffer also tracks all changes made to the text, which enables:
/// - Undo/redo functionality
/// - LSP textDocument/didChange notifications
/// - Change event subscriptions
///
/// # Examples
///
/// ```
/// use zqlz_text_editor::buffer::TextBuffer;
///
/// let mut buffer = TextBuffer::new("Hello\nWorld");
/// assert_eq!(buffer.line_count(), 2);
/// assert_eq!(buffer.line(0), Some("Hello\n".to_string()));
/// assert_eq!(buffer.line(1), Some("World".to_string()));
///
/// buffer.insert(5, " there").unwrap();
/// assert_eq!(buffer.text(), "Hello there\nWorld");
///
/// // Check that change was tracked
/// assert_eq!(buffer.changes().len(), 1);
/// assert!(buffer.changes()[0].is_insertion());
/// ```
#[derive(Debug, Clone)]
pub struct TextBuffer {
    rope: Rope,
    changes: Vec<Change>,
}

impl TextBuffer {
    /// Creates a new text buffer from a string.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let buffer = TextBuffer::new("Hello, world!");
    /// assert_eq!(buffer.text(), "Hello, world!");
    /// ```
    pub fn new(text: impl AsRef<str>) -> Self {
        Self {
            rope: Rope::from_str(text.as_ref()),
            changes: Vec::new(),
        }
    }

    /// Creates an empty text buffer.
    pub fn empty() -> Self {
        Self {
            rope: Rope::new(),
            changes: Vec::new(),
        }
    }

    /// Returns the entire text content as a `String`.
    ///
    /// Note: This allocates a new String. For large buffers, prefer iterating
    /// over lines or using `slice()` for specific ranges.
    ///
    /// # Performance
    ///
    /// O(n) where n is the length of the text.
    pub fn text(&self) -> String {
        self.rope.to_string()
    }

    /// Returns the length of the buffer in bytes.
    ///
    /// # Performance
    ///
    /// O(1) - rope tracks this internally.
    pub fn len(&self) -> usize {
        self.rope.len_bytes()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.rope.len_bytes() == 0
    }

    /// Returns the number of lines in the buffer.
    ///
    /// An empty buffer has 1 line. A buffer ending with a newline counts
    /// that last empty line.
    ///
    /// # Performance
    ///
    /// O(1) - rope tracks this internally.
    pub fn line_count(&self) -> usize {
        self.rope.len_lines()
    }

    /// Returns the text of a specific line, including its line ending.
    ///
    /// Returns `None` if the line index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let buffer = TextBuffer::new("Hello\nWorld\n");
    /// assert_eq!(buffer.line(0), Some("Hello\n".to_string()));
    /// assert_eq!(buffer.line(1), Some("World\n".to_string()));
    /// assert_eq!(buffer.line(2), Some("".to_string())); // Empty line at end
    /// assert_eq!(buffer.line(3), None); // Out of bounds
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of lines.
    pub fn line(&self, line_idx: usize) -> Option<String> {
        if line_idx >= self.rope.len_lines() {
            return None;
        }

        let line = self.rope.line(line_idx);
        Some(line.to_string())
    }

    /// Returns the byte offset of the start of a line.
    ///
    /// Returns `None` if the line index is out of bounds.
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of lines.
    pub fn line_to_byte(&self, line_idx: usize) -> Option<usize> {
        if line_idx >= self.rope.len_lines() {
            return None;
        }

        Some(self.rope.line_to_byte(line_idx))
    }

    /// Returns the line index containing the given byte offset.
    ///
    /// Returns `None` if the offset is out of bounds.
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of lines.
    pub fn byte_to_line(&self, offset: usize) -> Option<usize> {
        if offset > self.rope.len_bytes() {
            return None;
        }

        Some(self.rope.byte_to_line(offset))
    }

    /// Inserts text at the given byte offset.
    ///
    /// The change is tracked for undo/redo and LSP notifications.
    ///
    /// # Errors
    ///
    /// Returns an error if the offset is out of bounds (greater than buffer length).
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let mut buffer = TextBuffer::new("Hello World");
    /// buffer.insert(5, ",").unwrap();
    /// assert_eq!(buffer.text(), "Hello, World");
    ///
    /// // Change is tracked
    /// assert_eq!(buffer.changes().len(), 1);
    /// assert!(buffer.changes()[0].is_insertion());
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of text chunks in the rope.
    pub fn insert(&mut self, offset: usize, text: impl AsRef<str>) -> Result<()> {
        if offset > self.rope.len_bytes() {
            return Err(anyhow!(
                "Insert offset {} is out of bounds (buffer length: {})",
                offset,
                self.rope.len_bytes()
            ));
        }

        let text_str = text.as_ref();
        self.rope.insert(offset, text_str);

        // Track the change
        self.changes.push(Change::insert(offset, text_str));

        Ok(())
    }

    /// Deletes text in the given byte range.
    ///
    /// The change is tracked for undo/redo and LSP notifications.
    ///
    /// # Errors
    ///
    /// Returns an error if the range is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let mut buffer = TextBuffer::new("Hello, World");
    /// buffer.delete(5..6).unwrap(); // Remove comma
    /// assert_eq!(buffer.text(), "Hello World");
    ///
    /// // Change is tracked
    /// assert_eq!(buffer.changes().len(), 1);
    /// assert!(buffer.changes()[0].is_deletion());
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of text chunks in the rope.
    pub fn delete(&mut self, range: std::ops::Range<usize>) -> Result<()> {
        if range.end > self.rope.len_bytes() {
            return Err(anyhow!(
                "Delete range {:?} is out of bounds (buffer length: {})",
                range,
                self.rope.len_bytes()
            ));
        }

        if range.start > range.end {
            return Err(anyhow!("Delete range {:?} has start > end", range));
        }

        if range.start == range.end {
            // Empty range, nothing to delete
            return Ok(());
        }

        // Get the text being deleted before we delete it
        let deleted_text = self.rope.slice(range.clone()).to_string();

        self.rope.remove(range.clone());

        // Track the change
        self.changes.push(Change::delete(range.start, deleted_text));

        Ok(())
    }

    /// Returns the character at the given byte offset.
    ///
    /// Returns `None` if the offset is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let buffer = TextBuffer::new("Hello 🦀");
    /// assert_eq!(buffer.char_at(0), Some('H'));
    /// assert_eq!(buffer.char_at(6), Some('🦀'));
    /// assert_eq!(buffer.char_at(100), None);
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of text chunks in the rope.
    pub fn char_at(&self, offset: usize) -> Option<char> {
        if offset >= self.rope.len_bytes() {
            return None;
        }

        // Convert byte offset to char index, then get the char
        let char_idx = self.rope.byte_to_char(offset);
        self.rope.get_char(char_idx)
    }

    /// Converts a position (line, column) to a byte offset.
    ///
    /// The column is the UTF-8 byte offset within the line.
    ///
    /// # Errors
    ///
    /// Returns an error if the position is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::{TextBuffer, Position};
    ///
    /// let buffer = TextBuffer::new("Hello\nWorld");
    /// assert_eq!(buffer.position_to_offset(Position::new(0, 0)).unwrap(), 0);
    /// assert_eq!(buffer.position_to_offset(Position::new(0, 5)).unwrap(), 5); // Newline
    /// assert_eq!(buffer.position_to_offset(Position::new(1, 0)).unwrap(), 6);
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of lines.
    pub fn position_to_offset(&self, pos: Position) -> Result<usize> {
        if pos.line >= self.rope.len_lines() {
            return Err(anyhow!(
                "Position line {} is out of bounds (buffer has {} lines)",
                pos.line,
                self.rope.len_lines()
            ));
        }

        let line_start = self.rope.line_to_byte(pos.line);
        let line_end = if pos.line + 1 < self.rope.len_lines() {
            self.rope.line_to_byte(pos.line + 1)
        } else {
            self.rope.len_bytes()
        };

        let offset = line_start + pos.column;
        if offset > line_end {
            return Err(anyhow!(
                "Position column {} is out of bounds for line {} (line length: {})",
                pos.column,
                pos.line,
                line_end - line_start
            ));
        }

        Ok(offset)
    }

    /// Converts a byte offset to a position (line, column).
    ///
    /// The column is the UTF-8 byte offset within the line.
    ///
    /// # Errors
    ///
    /// Returns an error if the offset is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::{TextBuffer, Position};
    ///
    /// let buffer = TextBuffer::new("Hello\nWorld");
    /// assert_eq!(buffer.offset_to_position(0).unwrap(), Position::new(0, 0));
    /// assert_eq!(buffer.offset_to_position(5).unwrap(), Position::new(0, 5)); // Newline position
    /// assert_eq!(buffer.offset_to_position(6).unwrap(), Position::new(1, 0));
    /// ```
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of lines.
    pub fn offset_to_position(&self, offset: usize) -> Result<Position> {
        if offset > self.rope.len_bytes() {
            return Err(anyhow!(
                "Offset {} is out of bounds (buffer length: {})",
                offset,
                self.rope.len_bytes()
            ));
        }

        let line = self.rope.byte_to_line(offset);
        let line_start = self.rope.line_to_byte(line);
        let column = offset - line_start;

        Ok(Position::new(line, column))
    }

    /// Clamps a position to valid bounds within the buffer.
    ///
    /// If the line is out of bounds, clamps to the last line.
    /// If the column is out of bounds, clamps to the end of the line.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::{TextBuffer, Position};
    ///
    /// let buffer = TextBuffer::new("Hello\nWorld");
    /// assert_eq!(buffer.clamp_position(Position::new(0, 0)), Position::new(0, 0));
    /// assert_eq!(buffer.clamp_position(Position::new(0, 100)), Position::new(0, 6)); // End of first line (including \n)
    /// assert_eq!(buffer.clamp_position(Position::new(100, 0)), Position::new(1, 0)); // Last line
    /// ```
    pub fn clamp_position(&self, pos: Position) -> Position {
        let line = pos.line.min(self.rope.len_lines().saturating_sub(1));
        let line_start = self.rope.line_to_byte(line);
        let line_end = if line + 1 < self.rope.len_lines() {
            self.rope.line_to_byte(line + 1)
        } else {
            self.rope.len_bytes()
        };
        let line_len = line_end - line_start;
        let column = pos.column.min(line_len);

        Position::new(line, column)
    }

    /// Returns a slice of the text between two byte offsets.
    ///
    /// # Errors
    ///
    /// Returns an error if the range is out of bounds.
    ///
    /// # Performance
    ///
    /// O(m + log n) where m is the length of the slice and n is the number of text chunks.
    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<String> {
        if range.end > self.rope.len_bytes() {
            return Err(anyhow!(
                "Slice range {:?} is out of bounds (buffer length: {})",
                range,
                self.rope.len_bytes()
            ));
        }

        if range.start > range.end {
            return Err(anyhow!("Slice range {:?} has start > end", range));
        }

        Ok(self.rope.slice(range).to_string())
    }

    /// Returns the list of changes made to the buffer.
    ///
    /// Changes are returned in chronological order (oldest first).
    /// This is useful for:
    /// - Building undo/redo history
    /// - Sending LSP textDocument/didChange notifications
    /// - Subscribing to change events
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let mut buffer = TextBuffer::new("Hello");
    /// buffer.insert(5, " World").unwrap();
    /// buffer.delete(5..6).unwrap(); // Remove space
    ///
    /// assert_eq!(buffer.changes().len(), 2);
    /// assert!(buffer.changes()[0].is_insertion());
    /// assert!(buffer.changes()[1].is_deletion());
    /// ```
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Clears the change history.
    ///
    /// This is useful after:
    /// - Applying changes to undo/redo history
    /// - Sending changes to LSP server
    /// - Processing change event subscriptions
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let mut buffer = TextBuffer::new("Hello");
    /// buffer.insert(5, " World").unwrap();
    /// assert_eq!(buffer.changes().len(), 1);
    ///
    /// buffer.clear_changes();
    /// assert_eq!(buffer.changes().len(), 0);
    /// ```
    pub fn clear_changes(&mut self) {
        self.changes.clear();
    }

    /// Takes all pending changes, clearing the buffer's change history.
    ///
    /// This is a convenience method that returns the changes and clears them
    /// in one operation, which is common when processing changes.
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::TextBuffer;
    ///
    /// let mut buffer = TextBuffer::new("Hello");
    /// buffer.insert(5, " World").unwrap();
    ///
    /// let changes = buffer.take_changes();
    /// assert_eq!(changes.len(), 1);
    /// assert_eq!(buffer.changes().len(), 0); // Changes cleared
    /// ```
    pub fn take_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }

    /// Applies a change to the buffer.
    ///
    /// This is used to apply changes from:
    /// - Redo operations (applying a previously undone change)
    /// - LSP server responses (applying remote edits)
    /// - Synchronizing with other buffers
    ///
    /// Note: Applying a change does NOT add it to the change history.
    /// This prevents double-tracking when replaying changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the change cannot be applied (e.g., offset out of bounds).
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_text_editor::buffer::{TextBuffer, Change};
    ///
    /// let mut buffer = TextBuffer::new("Hello");
    /// let change = Change::insert(5, " World");
    ///
    /// buffer.apply_change(&change).unwrap();
    /// assert_eq!(buffer.text(), "Hello World");
    ///
    /// // Change was NOT added to history
    /// assert_eq!(buffer.changes().len(), 0);
    /// ```
    pub fn apply_change(&mut self, change: &Change) -> Result<()> {
        if !change.old_text.is_empty() {
            // Delete the old text
            let delete_range = change.range();
            if delete_range.end > self.rope.len_bytes() {
                return Err(anyhow!(
                    "Change delete range {:?} is out of bounds (buffer length: {})",
                    delete_range,
                    self.rope.len_bytes()
                ));
            }
            self.rope.remove(delete_range);
        }

        if !change.new_text.is_empty() {
            // Insert the new text
            if change.offset > self.rope.len_bytes() {
                return Err(anyhow!(
                    "Change insert offset {} is out of bounds (buffer length: {})",
                    change.offset,
                    self.rope.len_bytes()
                ));
            }
            self.rope.insert(change.offset, &change.new_text);
        }

        Ok(())
    }
}

impl Default for TextBuffer {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_buffer() {
        let buffer = TextBuffer::empty();
        assert_eq!(buffer.text(), "");
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.line_count(), 1); // Empty buffer has 1 line
    }

    #[test]
    fn test_single_line() {
        let buffer = TextBuffer::new("Hello, world!");
        assert_eq!(buffer.text(), "Hello, world!");
        assert_eq!(buffer.len(), 13);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.line_count(), 1);
        assert_eq!(buffer.line(0), Some("Hello, world!".to_string()));
        assert_eq!(buffer.line(1), None);
    }

    #[test]
    fn test_multiple_lines() {
        let buffer = TextBuffer::new("Line 1\nLine 2\nLine 3");
        assert_eq!(buffer.line_count(), 3);
        assert_eq!(buffer.line(0), Some("Line 1\n".to_string()));
        assert_eq!(buffer.line(1), Some("Line 2\n".to_string()));
        assert_eq!(buffer.line(2), Some("Line 3".to_string()));
    }

    #[test]
    fn test_trailing_newline() {
        let buffer = TextBuffer::new("Line 1\nLine 2\n");
        assert_eq!(buffer.line_count(), 3); // Last empty line counts
        assert_eq!(buffer.line(0), Some("Line 1\n".to_string()));
        assert_eq!(buffer.line(1), Some("Line 2\n".to_string()));
        assert_eq!(buffer.line(2), Some("".to_string()));
    }

    #[test]
    fn test_insert_at_start() {
        let mut buffer = TextBuffer::new("World");
        buffer.insert(0, "Hello ").unwrap();
        assert_eq!(buffer.text(), "Hello World");
    }

    #[test]
    fn test_insert_at_end() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.insert(5, " World").unwrap();
        assert_eq!(buffer.text(), "Hello World");
    }

    #[test]
    fn test_insert_in_middle() {
        let mut buffer = TextBuffer::new("HelloWorld");
        buffer.insert(5, " ").unwrap();
        assert_eq!(buffer.text(), "Hello World");
    }

    #[test]
    fn test_insert_out_of_bounds() {
        let mut buffer = TextBuffer::new("Hello");
        let result = buffer.insert(100, " World");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_range() {
        let mut buffer = TextBuffer::new("Hello, World");
        buffer.delete(5..7).unwrap(); // Remove ", "
        assert_eq!(buffer.text(), "HelloWorld");
    }

    #[test]
    fn test_delete_at_start() {
        let mut buffer = TextBuffer::new("Hello World");
        buffer.delete(0..6).unwrap();
        assert_eq!(buffer.text(), "World");
    }

    #[test]
    fn test_delete_at_end() {
        let mut buffer = TextBuffer::new("Hello World");
        buffer.delete(5..11).unwrap();
        assert_eq!(buffer.text(), "Hello");
    }

    #[test]
    fn test_delete_empty_range() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.delete(2..2).unwrap();
        assert_eq!(buffer.text(), "Hello");
    }

    #[test]
    fn test_delete_out_of_bounds() {
        let mut buffer = TextBuffer::new("Hello");
        let result = buffer.delete(0..100);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_invalid_range() {
        let mut buffer = TextBuffer::new("Hello");
        let result = buffer.delete(3..1);
        assert!(result.is_err());
    }

    #[test]
    fn test_char_at() {
        let buffer = TextBuffer::new("Hello");
        assert_eq!(buffer.char_at(0), Some('H'));
        assert_eq!(buffer.char_at(1), Some('e'));
        assert_eq!(buffer.char_at(4), Some('o'));
        assert_eq!(buffer.char_at(5), None);
    }

    #[test]
    fn test_char_at_unicode() {
        let buffer = TextBuffer::new("Hello 🦀");
        assert_eq!(buffer.char_at(0), Some('H'));
        assert_eq!(buffer.char_at(6), Some('🦀'));
    }

    #[test]
    fn test_position_to_offset() {
        let buffer = TextBuffer::new("Hello\nWorld");
        assert_eq!(buffer.position_to_offset(Position::new(0, 0)).unwrap(), 0);
        assert_eq!(buffer.position_to_offset(Position::new(0, 5)).unwrap(), 5);
        assert_eq!(buffer.position_to_offset(Position::new(1, 0)).unwrap(), 6);
        assert_eq!(buffer.position_to_offset(Position::new(1, 5)).unwrap(), 11);
    }

    #[test]
    fn test_offset_to_position() {
        let buffer = TextBuffer::new("Hello\nWorld");
        assert_eq!(buffer.offset_to_position(0).unwrap(), Position::new(0, 0));
        assert_eq!(buffer.offset_to_position(5).unwrap(), Position::new(0, 5));
        assert_eq!(buffer.offset_to_position(6).unwrap(), Position::new(1, 0));
        assert_eq!(buffer.offset_to_position(11).unwrap(), Position::new(1, 5));
    }

    #[test]
    fn test_clamp_position() {
        let buffer = TextBuffer::new("Hello\nWorld");
        assert_eq!(
            buffer.clamp_position(Position::new(0, 0)),
            Position::new(0, 0)
        );
        assert_eq!(
            buffer.clamp_position(Position::new(0, 100)),
            Position::new(0, 6)
        );
        assert_eq!(
            buffer.clamp_position(Position::new(100, 0)),
            Position::new(1, 0)
        );
        assert_eq!(
            buffer.clamp_position(Position::new(100, 100)),
            Position::new(1, 5)
        );
    }

    #[test]
    fn test_slice() {
        let buffer = TextBuffer::new("Hello World");
        assert_eq!(buffer.slice(0..5).unwrap(), "Hello");
        assert_eq!(buffer.slice(6..11).unwrap(), "World");
        assert_eq!(buffer.slice(0..11).unwrap(), "Hello World");
    }

    #[test]
    fn test_large_buffer() {
        // Test with a large buffer to ensure O(log n) performance
        let lines: Vec<String> = (0..10000).map(|i| format!("Line {}", i)).collect();
        let text = lines.join("\n");
        let buffer = TextBuffer::new(&text);
        assert_eq!(buffer.line_count(), 10000);
        assert_eq!(buffer.line(5000), Some("Line 5000\n".to_string()));
    }

    // Change tracking tests

    #[test]
    fn test_insert_tracks_change() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.insert(5, " World").unwrap();

        let changes = buffer.changes();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_insertion());
        assert_eq!(changes[0].offset, 5);
        assert_eq!(changes[0].new_text, " World");
        assert_eq!(changes[0].old_text, "");
    }

    #[test]
    fn test_delete_tracks_change() {
        let mut buffer = TextBuffer::new("Hello, World");
        buffer.delete(5..7).unwrap();

        let changes = buffer.changes();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_deletion());
        assert_eq!(changes[0].offset, 5);
        assert_eq!(changes[0].old_text, ", ");
        assert_eq!(changes[0].new_text, "");
    }

    #[test]
    fn test_multiple_changes_tracked() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.insert(5, " World").unwrap();
        buffer.delete(5..6).unwrap(); // Remove space
        buffer.insert(5, ", ").unwrap();

        let changes = buffer.changes();
        assert_eq!(changes.len(), 3);
        assert!(changes[0].is_insertion());
        assert!(changes[1].is_deletion());
        assert!(changes[2].is_insertion());
    }

    #[test]
    fn test_clear_changes() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.insert(5, " World").unwrap();
        assert_eq!(buffer.changes().len(), 1);

        buffer.clear_changes();
        assert_eq!(buffer.changes().len(), 0);
    }

    #[test]
    fn test_take_changes() {
        let mut buffer = TextBuffer::new("Hello");
        buffer.insert(5, " World").unwrap();
        buffer.delete(0..1).unwrap();

        let changes = buffer.take_changes();
        assert_eq!(changes.len(), 2);
        assert_eq!(buffer.changes().len(), 0); // Changes cleared
    }

    #[test]
    fn test_change_inverse() {
        let change = Change::insert(5, "Hello");
        let inverse = change.inverse();

        assert_eq!(inverse.offset, 5);
        assert_eq!(inverse.old_text, "Hello");
        assert_eq!(inverse.new_text, "");
    }

    #[test]
    fn test_change_is_insertion() {
        let change = Change::insert(5, "Hello");
        assert!(change.is_insertion());
        assert!(!change.is_deletion());
        assert!(!change.is_replacement());
    }

    #[test]
    fn test_change_is_deletion() {
        let change = Change::delete(5, "Hello");
        assert!(!change.is_insertion());
        assert!(change.is_deletion());
        assert!(!change.is_replacement());
    }

    #[test]
    fn test_change_is_replacement() {
        let change = Change::replace(5, "Hello", "World");
        assert!(!change.is_insertion());
        assert!(!change.is_deletion());
        assert!(change.is_replacement());
    }

    #[test]
    fn test_change_range() {
        let change = Change::delete(5, "Hello");
        let range = change.range();
        assert_eq!(range, 5..10);
    }

    #[test]
    fn test_change_new_range() {
        let change = Change::insert(5, "World");
        let range = change.new_range();
        assert_eq!(range, 5..10);
    }

    #[test]
    fn test_apply_change_insert() {
        let mut buffer = TextBuffer::new("Hello");
        let change = Change::insert(5, " World");

        buffer.apply_change(&change).unwrap();
        assert_eq!(buffer.text(), "Hello World");

        // Change not added to history
        assert_eq!(buffer.changes().len(), 0);
    }

    #[test]
    fn test_apply_change_delete() {
        let mut buffer = TextBuffer::new("Hello, World");
        let change = Change::delete(5, ", ");

        buffer.apply_change(&change).unwrap();
        assert_eq!(buffer.text(), "HelloWorld");

        // Change not added to history
        assert_eq!(buffer.changes().len(), 0);
    }

    #[test]
    fn test_apply_change_replace() {
        let mut buffer = TextBuffer::new("Hello World");
        let change = Change::replace(6, "World", "Rust");

        buffer.apply_change(&change).unwrap();
        assert_eq!(buffer.text(), "Hello Rust");

        // Change not added to history
        assert_eq!(buffer.changes().len(), 0);
    }

    #[test]
    fn test_apply_change_undo_redo() {
        let mut buffer = TextBuffer::new("Hello");

        // Make a change
        buffer.insert(5, " World").unwrap();
        assert_eq!(buffer.text(), "Hello World");

        // Get the change and create its inverse for undo
        let changes = buffer.take_changes();
        assert_eq!(changes.len(), 1);

        let undo_change = changes[0].inverse();
        buffer.apply_change(&undo_change).unwrap();
        assert_eq!(buffer.text(), "Hello");

        // Redo by applying the original change
        buffer.apply_change(&changes[0]).unwrap();
        assert_eq!(buffer.text(), "Hello World");
    }

    #[test]
    fn test_apply_change_out_of_bounds() {
        let mut buffer = TextBuffer::new("Hello");
        let change = Change::insert(100, " World");

        let result = buffer.apply_change(&change);
        assert!(result.is_err());
    }
}
