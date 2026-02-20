//! Cursor position and movement logic.
//!
//! ## Cursor State
//!
//! The cursor represents the insertion point in the text buffer. A text editor
//! can have multiple cursors (multi-cursor editing), but we start with single-cursor
//! support and add multi-cursor in Phase 6.
//!
//! ## Position vs Affinity
//!
//! When moving the cursor vertically (up/down), we want to maintain the cursor's
//! horizontal position when possible. For example:
//!
//! ```text
//! This is a long line with many characters
//! Short
//! This is another long line
//! ```
//!
//! If the cursor is at column 20 on line 0, pressing down moves to line 1.
//! Line 1 is only 5 characters, so the cursor moves to column 5 (end of line).
//! When pressing down again to line 2, we want to return to column 20, not stay at column 5.
//!
//! This is called "column affinity" - the cursor remembers its preferred column.

use crate::buffer::{Position, TextBuffer};

/// Cursor state including position and column affinity.
///
/// # Examples
///
/// ```
/// use zqlz_text_editor::buffer::{TextBuffer, Position};
/// use zqlz_text_editor::cursor::Cursor;
///
/// let buffer = TextBuffer::new("Hello\nWorld");
/// let mut cursor = Cursor::new();
///
/// assert_eq!(cursor.position(), Position::new(0, 0));
///
/// cursor.move_right(&buffer);
/// assert_eq!(cursor.position(), Position::new(0, 1));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// Current cursor position in the buffer
    position: Position,

    /// Preferred column for vertical movement.
    ///
    /// When moving up/down through lines of different lengths, we try to
    /// maintain this column position rather than the actual column.
    affinity: usize,
}

impl Cursor {
    /// Creates a new cursor at position (0, 0).
    pub fn new() -> Self {
        Self {
            position: Position::zero(),
            affinity: 0,
        }
    }

    /// Creates a cursor at the specified position.
    pub fn at(position: Position) -> Self {
        Self {
            position,
            affinity: position.column,
        }
    }

    /// Returns the current cursor position.
    pub fn position(&self) -> Position {
        self.position
    }

    /// Returns the preferred column for vertical movement.
    pub fn affinity(&self) -> usize {
        self.affinity
    }

    /// Returns the byte offset of the cursor position in the buffer.
    pub fn offset(&self, buffer: &TextBuffer) -> usize {
        buffer.position_to_offset(self.position).unwrap_or(0)
    }

    /// Sets the cursor position, updating affinity.
    pub fn set_position(&mut self, position: Position) {
        self.position = position;
        self.affinity = position.column;
    }

    /// Sets the cursor position without updating affinity.
    ///
    /// This is useful for vertical movement where we want to maintain
    /// the preferred column.
    fn set_position_keep_affinity(&mut self, position: Position) {
        self.position = position;
    }

    // ============================================================================
    // Horizontal Movement
    // ============================================================================

    /// Moves the cursor one character to the right.
    ///
    /// If at the end of a line (excluding newline), moves to the start of the next line.
    /// If at the end of the buffer, does nothing.
    pub fn move_right(&mut self, buffer: &TextBuffer) {
        let current = self.position;

        // Get the current line
        if let Some(line_text) = buffer.line(current.line) {
            // Line length without the trailing newline
            let line_len_without_newline = if line_text.ends_with('\n') {
                line_text.len().saturating_sub(1)
            } else {
                line_text.len()
            };

            if current.column < line_len_without_newline {
                // Move within the line (not at newline yet)
                // We need to handle multi-byte UTF-8 characters properly
                let new_column = self.next_char_boundary(&line_text, current.column);
                self.set_position(Position::new(current.line, new_column));
            } else if current.line + 1 < buffer.line_count() {
                // At or past the end of visible text, move to the start of the next line
                self.set_position(Position::new(current.line + 1, 0));
            }
            // If at end of last line, do nothing
        }
    }

    /// Moves the cursor one character to the left.
    ///
    /// If at the start of a line, moves to the end of the previous line.
    /// If at the start of the buffer, does nothing.
    pub fn move_left(&mut self, buffer: &TextBuffer) {
        let current = self.position;

        if current.column > 0 {
            // Move within the line
            if let Some(line_text) = buffer.line(current.line) {
                let new_column = self.prev_char_boundary(&line_text, current.column);
                self.set_position(Position::new(current.line, new_column));
            }
        } else if current.line > 0 {
            // Move to the end of the previous line
            if let Some(prev_line) = buffer.line(current.line - 1) {
                self.set_position(Position::new(current.line - 1, prev_line.len()));
            }
        }
        // If at start of first line, do nothing
    }

    /// Moves the cursor to the start of the current line (column 0).
    pub fn move_to_line_start(&mut self) {
        self.set_position(Position::new(self.position.line, 0));
    }

    /// Moves the cursor to the end of the current line.
    pub fn move_to_line_end(&mut self, buffer: &TextBuffer) {
        if let Some(line_text) = buffer.line(self.position.line) {
            self.set_position(Position::new(self.position.line, line_text.len()));
        }
    }

    // ============================================================================
    // Vertical Movement
    // ============================================================================

    /// Moves the cursor one line up.
    ///
    /// Maintains column affinity when possible. If the target line is shorter
    /// than the affinity column, moves to the end of that line.
    pub fn move_up(&mut self, buffer: &TextBuffer) {
        if self.position.line == 0 {
            // Already at first line
            return;
        }

        let target_line = self.position.line - 1;
        if let Some(line_text) = buffer.line(target_line) {
            let line_len = line_text.len();
            let new_column = self.affinity.min(line_len);
            self.set_position_keep_affinity(Position::new(target_line, new_column));
        }
    }

    /// Moves the cursor one line down.
    ///
    /// Maintains column affinity when possible. If the target line is shorter
    /// than the affinity column, moves to the end of that line.
    pub fn move_down(&mut self, buffer: &TextBuffer) {
        let target_line = self.position.line + 1;
        if target_line >= buffer.line_count() {
            // Already at last line
            return;
        }

        if let Some(line_text) = buffer.line(target_line) {
            let line_len = line_text.len();
            let new_column = self.affinity.min(line_len);
            self.set_position_keep_affinity(Position::new(target_line, new_column));
        }
    }

    // ============================================================================
    // Document Movement
    // ============================================================================

    /// Moves the cursor to the start of the document (0, 0).
    pub fn move_to_document_start(&mut self) {
        self.set_position(Position::zero());
    }

    /// Moves the cursor to the end of the document.
    pub fn move_to_document_end(&mut self, buffer: &TextBuffer) {
        if buffer.is_empty() {
            self.set_position(Position::zero());
            return;
        }

        let last_line = buffer.line_count().saturating_sub(1);
        if let Some(line_text) = buffer.line(last_line) {
            self.set_position(Position::new(last_line, line_text.len()));
        }
    }

    // ============================================================================
    // Word Movement
    // ============================================================================

    /// Moves the cursor to the start of the next word.
    ///
    /// A word is a sequence of alphanumeric characters or underscores.
    /// This mimics common editor behavior (Ctrl+Right).
    pub fn move_to_next_word_start(&mut self, buffer: &TextBuffer) {
        let current = self.position;

        if let Some(line_text) = buffer.line(current.line) {
            // If we find a word boundary in the current line, move there
            if let Some(new_column) = self.find_next_word_boundary(&line_text, current.column) {
                self.set_position(Position::new(current.line, new_column));
                return;
            }
        }

        // No word boundary found in current line, move to start of next line
        if current.line + 1 < buffer.line_count() {
            self.set_position(Position::new(current.line + 1, 0));
        }
    }

    /// Moves the cursor to the start of the previous word.
    ///
    /// This mimics common editor behavior (Ctrl+Left).
    pub fn move_to_prev_word_start(&mut self, buffer: &TextBuffer) {
        let current = self.position;

        if current.column > 0 {
            if let Some(line_text) = buffer.line(current.line) {
                // Find word boundary in current line
                if let Some(new_column) = self.find_prev_word_boundary(&line_text, current.column) {
                    self.set_position(Position::new(current.line, new_column));
                    return;
                }
            }
        }

        // No word boundary found, move to end of previous line
        if current.line > 0 {
            if let Some(prev_line) = buffer.line(current.line - 1) {
                self.set_position(Position::new(current.line - 1, prev_line.len()));
            }
        }
    }

    // ============================================================================
    // UTF-8 Helpers
    // ============================================================================

    /// Finds the next UTF-8 character boundary after the given byte offset.
    fn next_char_boundary(&self, s: &str, offset: usize) -> usize {
        if offset >= s.len() {
            return s.len();
        }

        // Find the next character boundary
        let bytes = s.as_bytes();
        let mut pos = offset + 1;
        while pos < s.len() && (bytes[pos] & 0b1100_0000) == 0b1000_0000 {
            pos += 1;
        }
        pos.min(s.len())
    }

    /// Finds the previous UTF-8 character boundary before the given byte offset.
    fn prev_char_boundary(&self, s: &str, offset: usize) -> usize {
        if offset == 0 {
            return 0;
        }

        // Find the previous character boundary
        let bytes = s.as_bytes();
        let mut pos = offset - 1;
        while pos > 0 && (bytes[pos] & 0b1100_0000) == 0b1000_0000 {
            pos -= 1;
        }
        pos
    }

    // ============================================================================
    // Word Boundary Helpers
    // ============================================================================

    /// Finds the next word boundary after the given position.
    ///
    /// Returns None if already at end of line.
    fn find_next_word_boundary(&self, line: &str, column: usize) -> Option<usize> {
        if column >= line.len() {
            return None;
        }

        let chars: Vec<char> = line[column..].chars().collect();
        if chars.is_empty() {
            return None;
        }

        // Skip current word characters
        let mut i = 0;
        let in_word = Self::is_word_char(chars[0]);

        while i < chars.len() && Self::is_word_char(chars[i]) == in_word {
            i += 1;
        }

        // Skip whitespace
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }

        if i == 0 {
            None
        } else {
            // Convert char count to byte offset
            let char_offset = &chars[..i];
            let byte_offset: usize = char_offset.iter().map(|c| c.len_utf8()).sum();
            Some(column + byte_offset)
        }
    }

    /// Finds the previous word boundary before the given position.
    ///
    /// Returns None if already at start of line.
    fn find_prev_word_boundary(&self, line: &str, column: usize) -> Option<usize> {
        if column == 0 {
            return None;
        }

        let before_cursor = &line[..column];
        let chars: Vec<char> = before_cursor.chars().collect();
        if chars.is_empty() {
            return None;
        }

        // Start from the end and work backwards
        let mut i = chars.len();

        // Skip trailing whitespace
        while i > 0 && chars[i - 1].is_whitespace() {
            i -= 1;
        }

        if i == 0 {
            return Some(0);
        }

        // Skip word characters
        let in_word = Self::is_word_char(chars[i - 1]);
        while i > 0 && Self::is_word_char(chars[i - 1]) == in_word {
            i -= 1;
        }

        // Convert char count to byte offset
        let char_offset = &chars[..i];
        let byte_offset: usize = char_offset.iter().map(|c| c.len_utf8()).sum();
        Some(byte_offset)
    }

    /// Returns true if the character is considered part of a word.
    ///
    /// Word characters are alphanumeric or underscore.
    fn is_word_char(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cursor() {
        let cursor = Cursor::new();
        assert_eq!(cursor.position(), Position::new(0, 0));
        assert_eq!(cursor.affinity(), 0);
    }

    #[test]
    fn test_cursor_at() {
        let cursor = Cursor::at(Position::new(2, 5));
        assert_eq!(cursor.position(), Position::new(2, 5));
        assert_eq!(cursor.affinity(), 5);
    }

    #[test]
    fn test_move_right_single_line() {
        let buffer = TextBuffer::new("Hello");
        let mut cursor = Cursor::new();

        cursor.move_right(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 1));

        cursor.move_right(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 2));
    }

    #[test]
    fn test_move_right_to_next_line() {
        let buffer = TextBuffer::new("Hi\nWorld");
        let mut cursor = Cursor::at(Position::new(0, 2)); // At newline

        cursor.move_right(&buffer);
        assert_eq!(cursor.position(), Position::new(1, 0));
    }

    #[test]
    fn test_move_right_at_end() {
        let buffer = TextBuffer::new("Hi");
        let mut cursor = Cursor::at(Position::new(0, 2));

        cursor.move_right(&buffer);
        // Should stay at end
        assert_eq!(cursor.position(), Position::new(0, 2));
    }

    #[test]
    fn test_move_left_single_line() {
        let buffer = TextBuffer::new("Hello");
        let mut cursor = Cursor::at(Position::new(0, 2));

        cursor.move_left(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 1));

        cursor.move_left(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 0));
    }

    #[test]
    fn test_move_left_to_prev_line() {
        let buffer = TextBuffer::new("Hi\nWorld");
        let mut cursor = Cursor::at(Position::new(1, 0));

        cursor.move_left(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 3)); // End of "Hi\n"
    }

    #[test]
    fn test_move_left_at_start() {
        let buffer = TextBuffer::new("Hello");
        let mut cursor = Cursor::new();

        cursor.move_left(&buffer);
        // Should stay at start
        assert_eq!(cursor.position(), Position::new(0, 0));
    }

    #[test]
    fn test_move_to_line_start() {
        let mut cursor = Cursor::at(Position::new(0, 3));

        cursor.move_to_line_start();
        assert_eq!(cursor.position(), Position::new(0, 0));
    }

    #[test]
    fn test_move_to_line_end() {
        let buffer = TextBuffer::new("Hello\nWorld");
        let mut cursor = Cursor::new();

        cursor.move_to_line_end(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 6)); // Including \n
    }

    #[test]
    fn test_move_up() {
        let buffer = TextBuffer::new("Hello\nWorld\nTest");
        let mut cursor = Cursor::at(Position::new(1, 2));

        cursor.move_up(&buffer);
        assert_eq!(cursor.position(), Position::new(0, 2));
        assert_eq!(cursor.affinity(), 2);
    }

    #[test]
    fn test_move_up_with_affinity() {
        let buffer = TextBuffer::new("This is a long line\nShort\nAnother long line");
        let mut cursor = Cursor::at(Position::new(0, 15));

        cursor.move_down(&buffer);
        // Short line is only 6 chars (including \n), cursor should be at end
        assert_eq!(cursor.position(), Position::new(1, 6));
        assert_eq!(cursor.affinity(), 15); // Still remembers column 15

        cursor.move_down(&buffer);
        // Should return to column 15 on the long line
        assert_eq!(cursor.position(), Position::new(2, 15));
    }

    #[test]
    fn test_move_down() {
        let buffer = TextBuffer::new("Hello\nWorld\nTest");
        let mut cursor = Cursor::at(Position::new(0, 2));

        cursor.move_down(&buffer);
        assert_eq!(cursor.position(), Position::new(1, 2));
        assert_eq!(cursor.affinity(), 2);
    }

    #[test]
    fn test_move_to_document_start() {
        let mut cursor = Cursor::at(Position::new(1, 3));

        cursor.move_to_document_start();
        assert_eq!(cursor.position(), Position::new(0, 0));
    }

    #[test]
    fn test_move_to_document_end() {
        let buffer = TextBuffer::new("Hello\nWorld");
        let mut cursor = Cursor::new();

        cursor.move_to_document_end(&buffer);
        assert_eq!(cursor.position(), Position::new(1, 5));
    }

    #[test]
    fn test_move_to_next_word() {
        let buffer = TextBuffer::new("hello world test");
        let mut cursor = Cursor::new();

        cursor.move_to_next_word_start(&buffer);
        assert_eq!(cursor.position().column, 6); // Start of "world"

        cursor.move_to_next_word_start(&buffer);
        assert_eq!(cursor.position().column, 12); // Start of "test"
    }

    #[test]
    fn test_move_to_prev_word() {
        let buffer = TextBuffer::new("hello world test");
        let mut cursor = Cursor::at(Position::new(0, 16)); // End of line

        cursor.move_to_prev_word_start(&buffer);
        assert_eq!(cursor.position().column, 12); // Start of "test"

        cursor.move_to_prev_word_start(&buffer);
        assert_eq!(cursor.position().column, 6); // Start of "world"
    }

    #[test]
    fn test_unicode_movement() {
        let buffer = TextBuffer::new("Hello 🦀 World");
        let mut cursor = Cursor::at(Position::new(0, 6)); // Before emoji

        cursor.move_right(&buffer);
        // Should skip the entire emoji (4 bytes)
        assert_eq!(cursor.position().column, 10); // After emoji
    }

    #[test]
    fn test_affinity_preserved() {
        let buffer = TextBuffer::new("Long line here\nX\nAnother long line");
        let mut cursor = Cursor::at(Position::new(0, 10));

        cursor.move_down(&buffer);
        assert_eq!(cursor.position(), Position::new(1, 2)); // End of "X\n"
        assert_eq!(cursor.affinity(), 10);

        cursor.move_down(&buffer);
        assert_eq!(cursor.position(), Position::new(2, 10)); // Back to column 10
    }
}
