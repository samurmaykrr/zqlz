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

use anyhow::{Result, anyhow};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Bias {
    Left,
    Right,
}

/// Durable document coordinate that can be rebased through later edits.
///
/// Anchors keep the revision they were created against so the text layer can be
/// responsible for translating long-lived state into current offsets instead of
/// forcing every caller to manually track edits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Anchor {
    offset: usize,
    revision: usize,
    bias: Bias,
}

impl Anchor {
    pub fn new(offset: usize, revision: usize, bias: Bias) -> Self {
        Self {
            offset,
            revision,
            bias,
        }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn revision(&self) -> usize {
        self.revision
    }

    pub fn bias(&self) -> Bias {
        self.bias
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchoredRange {
    pub start: Anchor,
    pub end: Anchor,
}

impl AnchoredRange {
    pub fn new(start: Anchor, end: Anchor) -> Self {
        Self { start, end }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionEdit {
    pub start_revision: usize,
    pub end_revision: usize,
    pub change: Change,
}

impl RevisionEdit {
    fn rebase_offset(&self, offset: usize, bias: Bias) -> usize {
        let start = self.change.offset;
        let old_len = self.change.old_text.len();
        let new_len = self.change.new_text.len();
        let old_end = start + old_len;
        let new_end = start + new_len;

        if offset < start {
            offset
        } else if offset > old_end {
            offset - old_len + new_len
        } else if offset == start {
            match bias {
                Bias::Left => start,
                Bias::Right => new_end,
            }
        } else if offset == old_end {
            new_end
        } else {
            match bias {
                Bias::Left => start,
                Bias::Right => new_end,
            }
        }
    }
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
    edit_log: Vec<RevisionEdit>,
    revision: usize,
    next_transaction_id: u64,
    active_transaction: Option<TransactionId>,
}

/// Immutable snapshot of buffer contents and revision state.
///
/// Rope clones are shallow, so snapshots stay cheap while still preserving the
/// exact text and line mapping that were current when the snapshot was taken.
#[derive(Debug, Clone)]
pub struct BufferSnapshot {
    rope: Rope,
    revision: usize,
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
            edit_log: Vec::new(),
            revision: 0,
            next_transaction_id: 1,
            active_transaction: None,
        }
    }

    /// Creates an empty text buffer.
    pub fn empty() -> Self {
        Self {
            rope: Rope::new(),
            changes: Vec::new(),
            edit_log: Vec::new(),
            revision: 0,
            next_transaction_id: 1,
            active_transaction: None,
        }
    }

    pub fn start_transaction_at(&mut self) -> TransactionId {
        let id = TransactionId(self.next_transaction_id);
        self.next_transaction_id = self.next_transaction_id.saturating_add(1);
        self.active_transaction = Some(id);
        id
    }

    pub fn end_transaction_at(&mut self, id: TransactionId) {
        if self.active_transaction == Some(id) {
            self.active_transaction = None;
        }
    }

    pub fn active_transaction(&self) -> Option<TransactionId> {
        self.active_transaction
    }

    /// Returns the current monotonic revision for the buffer.
    pub fn revision(&self) -> usize {
        self.revision
    }

    /// Creates an immutable snapshot of the buffer's current text and revision.
    pub fn snapshot(&self) -> BufferSnapshot {
        BufferSnapshot {
            rope: self.rope.clone(),
            revision: self.revision,
        }
    }

    pub fn edits_since(&self, revision: usize) -> Result<&[RevisionEdit]> {
        if revision > self.revision {
            return Err(anyhow!(
                "Revision {} is newer than current buffer revision {}",
                revision,
                self.revision
            ));
        }

        Ok(&self.edit_log[revision..])
    }

    pub fn anchor_at(&self, offset: usize, bias: Bias) -> Result<Anchor> {
        if offset > self.len() {
            return Err(anyhow!(
                "Anchor offset {} is out of bounds (buffer length: {})",
                offset,
                self.len()
            ));
        }

        Ok(Anchor::new(offset, self.revision, bias))
    }

    pub fn anchor_before(&self, offset: usize) -> Result<Anchor> {
        self.anchor_at(offset, Bias::Left)
    }

    pub fn anchor_after(&self, offset: usize) -> Result<Anchor> {
        self.anchor_at(offset, Bias::Right)
    }

    pub fn anchor_for_position(&self, position: Position, bias: Bias) -> Result<Anchor> {
        let offset = self.position_to_offset(position)?;
        self.anchor_at(offset, bias)
    }

    pub fn anchored_range(
        &self,
        range: std::ops::Range<usize>,
        start_bias: Bias,
        end_bias: Bias,
    ) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.anchor_at(range.start, start_bias)?,
            self.anchor_at(range.end, end_bias)?,
        ))
    }

    pub fn anchored_position_range(
        &self,
        range: Range,
        start_bias: Bias,
        end_bias: Bias,
    ) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.anchor_for_position(range.start, start_bias)?,
            self.anchor_for_position(range.end, end_bias)?,
        ))
    }

    pub fn resolve_anchor_offset(&self, anchor: Anchor) -> Result<usize> {
        if anchor.revision > self.revision {
            return Err(anyhow!(
                "Anchor revision {} is newer than current buffer revision {}",
                anchor.revision,
                self.revision
            ));
        }

        let mut offset = anchor.offset;
        for edit in self.edits_since(anchor.revision)? {
            offset = edit.rebase_offset(offset, anchor.bias);
        }

        Ok(offset.min(self.len()))
    }

    pub fn resolve_anchor_position(&self, anchor: Anchor) -> Result<Position> {
        let offset = self.resolve_anchor_offset(anchor)?;
        self.offset_to_position(offset)
    }

    pub fn rebase_anchor(&self, anchor: Anchor) -> Result<Anchor> {
        let offset = self.resolve_anchor_offset(anchor)?;
        Ok(Anchor::new(offset, self.revision, anchor.bias()))
    }

    pub fn resolve_anchored_range(&self, range: AnchoredRange) -> Result<std::ops::Range<usize>> {
        Ok(self.resolve_anchor_offset(range.start)?..self.resolve_anchor_offset(range.end)?)
    }

    pub fn resolve_anchored_position_range(&self, range: AnchoredRange) -> Result<Range> {
        Ok(Range::new(
            self.resolve_anchor_position(range.start)?,
            self.resolve_anchor_position(range.end)?,
        ))
    }

    pub fn rebase_anchored_range(&self, range: AnchoredRange) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.rebase_anchor(range.start)?,
            self.rebase_anchor(range.end)?,
        ))
    }

    pub fn restore_snapshot(&mut self, snapshot: &BufferSnapshot) {
        self.rope = snapshot.rope.clone();
        self.revision = snapshot.revision;
        self.edit_log.truncate(snapshot.revision);
        self.changes.clear();
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

    /// Returns a UTF-8 string slice copy for a local byte range.
    ///
    /// This still allocates, but only for the requested span instead of the
    /// whole document, which keeps hot-path callers bounded to local work.
    pub fn text_for_range(&self, range: std::ops::Range<usize>) -> Result<String> {
        if range.end > self.rope.len_bytes() {
            return Err(anyhow!(
                "Range {:?} is out of bounds (buffer length: {})",
                range,
                self.rope.len_bytes()
            ));
        }

        if range.start > range.end {
            return Err(anyhow!("Range {:?} has start > end", range));
        }

        let char_start = self.rope.byte_to_char(range.start);
        let char_end = self.rope.byte_to_char(range.end);
        Ok(self.rope.slice(char_start..char_end).to_string())
    }

    /// Returns a local line window as a single string.
    pub fn text_for_line_range(&self, line_range: std::ops::Range<usize>) -> Result<String> {
        if line_range.start > line_range.end {
            return Err(anyhow!("Line range {:?} has start > end", line_range));
        }

        let start = self.line_to_byte(line_range.start).unwrap_or(self.len());
        let end = if line_range.end >= self.line_count() {
            self.len()
        } else {
            self.line_to_byte(line_range.end).unwrap_or(self.len())
        };

        self.text_for_range(start..end)
    }

    /// Returns a cheap clone of the underlying rope.
    ///
    /// Rope clones are shallow and share the same underlying chunks, making
    /// this suitable for read-only consumers that need rope APIs without
    /// materializing the entire buffer into a String.
    pub fn rope(&self) -> Rope {
        self.rope.clone()
    }

    /// Replace the internal rope with a snapshot, used for undo rollback.
    pub fn restore_rope(&mut self, rope: Rope) {
        self.rope = rope;
        self.revision = self.revision.saturating_add(1);
        self.edit_log.clear();
        self.changes.clear();
    }

    /// Returns the length of the buffer in bytes.
    ///
    /// # Performance
    ///
    /// O(1) - rope tracks this internally.
    pub fn len(&self) -> usize {
        self.rope.len_bytes()
    }

    /// Returns the previous UTF-8 character boundary strictly before `offset`.
    ///
    /// If `offset` is `0`, returns `0`.
    pub fn previous_char_boundary(&self, offset: usize) -> Result<usize> {
        if offset > self.rope.len_bytes() {
            return Err(anyhow!(
                "Offset {} is out of bounds (buffer length: {})",
                offset,
                self.rope.len_bytes()
            ));
        }

        if offset == 0 {
            return Ok(0);
        }

        let char_index = self.rope.byte_to_char(offset - 1);
        Ok(self.rope.char_to_byte(char_index))
    }

    /// Returns the next UTF-8 character boundary strictly after `offset`.
    ///
    /// If `offset` is at or beyond the end of the buffer, returns the buffer
    /// length.
    pub fn next_char_boundary(&self, offset: usize) -> Result<usize> {
        if offset > self.rope.len_bytes() {
            return Err(anyhow!(
                "Offset {} is out of bounds (buffer length: {})",
                offset,
                self.rope.len_bytes()
            ));
        }

        if offset >= self.rope.len_bytes() {
            return Ok(self.rope.len_bytes());
        }

        let char_index = self.rope.byte_to_char(offset);
        Ok(self.rope.char_to_byte(char_index + 1))
    }

    /// Returns the nearest valid UTF-8 boundary at or before `offset`.
    pub fn floor_char_boundary(&self, offset: usize) -> usize {
        let clamped_offset = offset.min(self.rope.len_bytes());
        if clamped_offset == self.rope.len_bytes() {
            return clamped_offset;
        }

        let char_index = self.rope.byte_to_char(clamped_offset);
        self.rope.char_to_byte(char_index)
    }

    /// Returns the nearest valid UTF-8 boundary at or after `offset`.
    pub fn ceil_char_boundary(&self, offset: usize) -> usize {
        let clamped_offset = offset.min(self.rope.len_bytes());
        if clamped_offset == self.rope.len_bytes() {
            return clamped_offset;
        }

        let floor = self.floor_char_boundary(clamped_offset);
        if floor == clamped_offset {
            floor
        } else {
            self.next_char_boundary(floor)
                .unwrap_or(self.rope.len_bytes())
        }
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
        let char_idx = self.rope.byte_to_char(offset);
        self.rope.insert(char_idx, text_str);
        let change = Change::insert(offset, text_str);
        self.record_edit(&change);
        self.changes.push(change);

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
        let char_start = self.rope.byte_to_char(range.start);
        let char_end = self.rope.byte_to_char(range.end);
        let deleted_text = self.rope.slice(char_start..char_end).to_string();

        self.rope.remove(char_start..char_end);
        let change = Change::delete(range.start, deleted_text);
        self.record_edit(&change);
        self.changes.push(change);

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

        let char_start = self.rope.byte_to_char(range.start);
        let char_end = self.rope.byte_to_char(range.end);
        Ok(self.rope.slice(char_start..char_end).to_string())
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
            let char_start = self.rope.byte_to_char(delete_range.start);
            let char_end = self.rope.byte_to_char(delete_range.end);
            let actual_text = self.rope.slice(char_start..char_end).to_string();
            if actual_text != change.old_text {
                return Err(anyhow!(
                    "Change old_text mismatch at offset {}: expected {:?}, found {:?}",
                    change.offset,
                    change.old_text,
                    actual_text
                ));
            }
            self.rope.remove(char_start..char_end);
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
            self.rope
                .insert(self.rope.byte_to_char(change.offset), &change.new_text);
        }

        self.record_edit(change);

        Ok(())
    }

    fn record_edit(&mut self, change: &Change) {
        let start_revision = self.revision;
        self.revision = self.revision.saturating_add(1);
        self.edit_log.push(RevisionEdit {
            start_revision,
            end_revision: self.revision,
            change: change.clone(),
        });
    }
}

impl BufferSnapshot {
    /// Returns the revision captured by this snapshot.
    pub fn revision(&self) -> usize {
        self.revision
    }

    pub fn anchor_at(&self, offset: usize, bias: Bias) -> Result<Anchor> {
        if offset > self.len() {
            return Err(anyhow!(
                "Anchor offset {} is out of bounds (buffer length: {})",
                offset,
                self.len()
            ));
        }

        Ok(Anchor::new(offset, self.revision, bias))
    }

    pub fn anchor_before(&self, offset: usize) -> Result<Anchor> {
        self.anchor_at(offset, Bias::Left)
    }

    pub fn anchor_after(&self, offset: usize) -> Result<Anchor> {
        self.anchor_at(offset, Bias::Right)
    }

    pub fn anchor_for_position(&self, position: Position, bias: Bias) -> Result<Anchor> {
        let offset = self.position_to_offset(position)?;
        self.anchor_at(offset, bias)
    }

    pub fn anchored_range(
        &self,
        range: std::ops::Range<usize>,
        start_bias: Bias,
        end_bias: Bias,
    ) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.anchor_at(range.start, start_bias)?,
            self.anchor_at(range.end, end_bias)?,
        ))
    }

    pub fn anchored_position_range(
        &self,
        range: Range,
        start_bias: Bias,
        end_bias: Bias,
    ) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.anchor_for_position(range.start, start_bias)?,
            self.anchor_for_position(range.end, end_bias)?,
        ))
    }

    pub fn resolve_anchor_offset(&self, anchor: Anchor) -> Result<usize> {
        if anchor.revision != self.revision {
            return Err(anyhow!(
                "Snapshot revision {} can only resolve anchors created at the same revision, got {}",
                self.revision,
                anchor.revision
            ));
        }

        Ok(anchor.offset.min(self.len()))
    }

    pub fn resolve_anchor_position(&self, anchor: Anchor) -> Result<Position> {
        let offset = self.resolve_anchor_offset(anchor)?;
        self.offset_to_position(offset)
    }

    pub fn rebase_anchor(&self, anchor: Anchor) -> Result<Anchor> {
        let offset = self.resolve_anchor_offset(anchor)?;
        Ok(Anchor::new(offset, self.revision, anchor.bias()))
    }

    pub fn resolve_anchored_range(&self, range: AnchoredRange) -> Result<std::ops::Range<usize>> {
        Ok(self.resolve_anchor_offset(range.start)?..self.resolve_anchor_offset(range.end)?)
    }

    pub fn resolve_anchored_position_range(&self, range: AnchoredRange) -> Result<Range> {
        Ok(Range::new(
            self.resolve_anchor_position(range.start)?,
            self.resolve_anchor_position(range.end)?,
        ))
    }

    pub fn rebase_anchored_range(&self, range: AnchoredRange) -> Result<AnchoredRange> {
        Ok(AnchoredRange::new(
            self.rebase_anchor(range.start)?,
            self.rebase_anchor(range.end)?,
        ))
    }

    /// Returns the length of the snapshot in bytes.
    pub fn len(&self) -> usize {
        self.rope.len_bytes()
    }

    /// Returns true if the snapshot has no bytes.
    pub fn is_empty(&self) -> bool {
        self.rope.len_bytes() == 0
    }

    /// Returns the number of lines in the snapshot.
    pub fn line_count(&self) -> usize {
        self.rope.len_lines()
    }

    /// Returns the text for a given line, including its trailing newline.
    pub fn line(&self, line_idx: usize) -> Option<String> {
        if line_idx >= self.rope.len_lines() {
            return None;
        }

        Some(self.rope.line(line_idx).to_string())
    }

    /// Returns the byte offset for the start of the given line.
    pub fn line_to_byte(&self, line_idx: usize) -> Option<usize> {
        if line_idx >= self.rope.len_lines() {
            return None;
        }

        Some(self.rope.line_to_byte(line_idx))
    }

    /// Returns the line containing the given byte offset.
    pub fn byte_to_line(&self, offset: usize) -> Option<usize> {
        if offset > self.rope.len_bytes() {
            return None;
        }

        Some(self.rope.byte_to_line(offset))
    }

    /// Converts a position to a byte offset within the snapshot.
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

    /// Converts a byte offset to a position within the snapshot.
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

    /// Clamps a position to valid snapshot bounds.
    pub fn clamp_position(&self, pos: Position) -> Position {
        let line = pos.line.min(self.rope.len_lines().saturating_sub(1));
        let line_start = self.rope.line_to_byte(line);
        let line_end = if line + 1 < self.rope.len_lines() {
            self.rope.line_to_byte(line + 1)
        } else {
            self.rope.len_bytes()
        };

        Position::new(line, pos.column.min(line_end - line_start))
    }

    /// Returns a slice of snapshot text between two byte offsets.
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

        let char_start = self.rope.byte_to_char(range.start);
        let char_end = self.rope.byte_to_char(range.end);
        Ok(self.rope.slice(char_start..char_end).to_string())
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
        let result = buffer.delete(std::ops::Range { start: 3, end: 1 });
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

    #[test]
    fn test_anchor_rebases_after_insertion() {
        let mut buffer = TextBuffer::new("abcd");
        let anchor = buffer.anchor_before(2).unwrap();

        buffer.insert(1, "XYZ").unwrap();

        assert_eq!(buffer.resolve_anchor_offset(anchor).unwrap(), 5);
        assert_eq!(
            buffer.resolve_anchor_position(anchor).unwrap(),
            Position::new(0, 5)
        );
    }

    #[test]
    fn test_anchor_bias_controls_insertion_boundary_resolution() {
        let mut buffer = TextBuffer::new("abcd");
        let left_anchor = buffer.anchor_before(2).unwrap();
        let right_anchor = buffer.anchor_after(2).unwrap();

        buffer.insert(2, "XYZ").unwrap();

        assert_eq!(buffer.resolve_anchor_offset(left_anchor).unwrap(), 2);
        assert_eq!(buffer.resolve_anchor_offset(right_anchor).unwrap(), 5);
    }

    #[test]
    fn test_snapshot_resolves_old_revision_anchor_deterministically() {
        let mut buffer = TextBuffer::new("hello");
        let snapshot = buffer.snapshot();
        let anchor = snapshot.anchor_after(5).unwrap();

        buffer.insert(0, "say ").unwrap();

        assert_eq!(snapshot.resolve_anchor_offset(anchor).unwrap(), 5);
        assert_eq!(buffer.resolve_anchor_offset(anchor).unwrap(), 9);
    }

    #[test]
    fn test_anchored_range_rebases_both_ends() {
        let mut buffer = TextBuffer::new("hello world");
        let range = buffer
            .anchored_range(6..11, Bias::Left, Bias::Right)
            .unwrap();

        buffer.insert(0, "wide ").unwrap();

        assert_eq!(buffer.resolve_anchored_range(range).unwrap(), 11..16);
    }

    #[test]
    fn test_rebase_anchor_updates_revision_without_changing_future_resolution() {
        let mut buffer = TextBuffer::new("abcdef");
        let original_anchor = buffer.anchor_after(4).unwrap();

        buffer.delete(1..3).unwrap();

        let rebased_anchor = buffer.rebase_anchor(original_anchor).unwrap();
        assert_eq!(rebased_anchor.offset(), 2);
        assert_eq!(rebased_anchor.revision(), buffer.revision());
        assert_eq!(rebased_anchor.bias(), Bias::Right);

        buffer.insert(0, "Z").unwrap();

        assert_eq!(buffer.resolve_anchor_offset(original_anchor).unwrap(), 3);
        assert_eq!(buffer.resolve_anchor_offset(rebased_anchor).unwrap(), 3);
    }

    #[test]
    fn test_rebase_anchored_range_updates_both_endpoints() {
        let mut buffer = TextBuffer::new("hello world");
        let original_range = buffer
            .anchored_range(6..11, Bias::Left, Bias::Right)
            .unwrap();

        buffer.insert(0, "wide ").unwrap();

        let rebased_range = buffer.rebase_anchored_range(original_range).unwrap();
        assert_eq!(rebased_range.start.revision(), buffer.revision());
        assert_eq!(rebased_range.end.revision(), buffer.revision());
        assert_eq!(
            buffer.resolve_anchored_range(rebased_range).unwrap(),
            11..16
        );

        buffer.insert(0, "very ").unwrap();

        assert_eq!(
            buffer.resolve_anchored_range(original_range).unwrap(),
            16..21
        );
        assert_eq!(
            buffer.resolve_anchored_range(rebased_range).unwrap(),
            16..21
        );
    }

    #[test]
    fn test_snapshot_rebase_anchor_normalizes_to_snapshot_revision() {
        let snapshot = TextBuffer::new("hello").snapshot();
        let anchor = snapshot.anchor_after(5).unwrap();
        let rebased_anchor = snapshot.rebase_anchor(anchor).unwrap();

        assert_eq!(rebased_anchor, anchor);
        assert_eq!(rebased_anchor.revision(), snapshot.revision());
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
    fn test_snapshot_preserves_original_text_after_mutation() {
        let mut buffer = TextBuffer::new("hello");
        let snapshot = buffer.snapshot();

        buffer.insert(5, " world").unwrap();

        assert_eq!(snapshot.slice(0..5).unwrap(), "hello");
        assert_eq!(snapshot.len(), 5);
        assert_eq!(buffer.text(), "hello world");
    }

    #[test]
    fn test_snapshot_preserves_original_line_mapping_after_mutation() {
        let mut buffer = TextBuffer::new("alpha\nbeta");
        let snapshot = buffer.snapshot();

        buffer.insert(0, "prefix\n").unwrap();

        assert_eq!(snapshot.line_count(), 2);
        assert_eq!(snapshot.offset_to_position(6).unwrap(), Position::new(1, 0));
        assert_eq!(snapshot.position_to_offset(Position::new(1, 2)).unwrap(), 8);
    }

    #[test]
    fn test_snapshot_revision_stays_stable_after_mutation() {
        let mut buffer = TextBuffer::new("hello");
        let snapshot = buffer.snapshot();
        let original_revision = snapshot.revision();

        buffer.insert(5, "!").unwrap();

        assert_eq!(snapshot.revision(), original_revision);
        assert!(buffer.revision() > original_revision);
    }

    #[test]
    fn test_transaction_start_and_end() {
        let mut buffer = TextBuffer::new("hello");
        let transaction = buffer.start_transaction_at();

        assert_eq!(buffer.active_transaction(), Some(transaction));

        buffer.end_transaction_at(transaction);
        assert_eq!(buffer.active_transaction(), None);
    }

    #[test]
    fn test_text_for_range_reads_local_slice() {
        let buffer = TextBuffer::new("alpha\nbeta\ngamma");

        let text = buffer.text_for_range(6..10).unwrap();
        assert_eq!(text, "beta");
    }

    #[test]
    fn test_text_for_line_range_reads_local_lines() {
        let buffer = TextBuffer::new("alpha\nbeta\ngamma");

        let text = buffer.text_for_line_range(1..3).unwrap();
        assert_eq!(text, "beta\ngamma");
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
