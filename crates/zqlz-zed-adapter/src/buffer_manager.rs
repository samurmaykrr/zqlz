//! Manages Zed MultiBuffer lifecycle for SQL queries
//!
//! BufferManager provides helper functions for creating and managing
//! Zed's MultiBuffer instances with SQL language support.

use gpui::{App, AppContext as _, Entity};
use language::Buffer;
use multi_buffer::MultiBuffer;

/// Manages buffer creation, language assignment, and cleanup
///
/// This module provides helper functions for working with Zed's MultiBuffer.
/// All ZQLZ SQL editors use singleton buffers (one buffer per editor).
pub struct BufferManager;

impl BufferManager {
    /// Creates a new buffer with SQL language support
    ///
    /// Creates a singleton MultiBuffer (one underlying Buffer) suitable for
    /// editing SQL queries. The buffer is initialized with the provided text
    /// content.
    ///
    /// # Arguments
    /// * `initial_text` - The initial text content for the buffer (empty string for new buffers)
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// A new MultiBuffer entity ready for use with the Zed editor
    pub fn create_sql_buffer(initial_text: &str, cx: &mut App) -> Entity<MultiBuffer> {
        // Create a local buffer with the initial text
        // local() creates a non-file-backed buffer suitable for in-memory editing
        let buffer = cx.new(|cx| Buffer::local(initial_text, cx));

        // Wrap in MultiBuffer for editor compatibility
        // MultiBuffer::singleton creates a single-buffer view, which is what we need
        // for SQL query editing (as opposed to multi-file editing)
        cx.new(|cx| MultiBuffer::singleton(buffer, cx))
    }

    /// Loads text into an existing buffer
    ///
    /// Replaces the entire content of the buffer with the provided text.
    /// This is useful for loading saved queries or resetting the buffer state.
    ///
    /// # Arguments
    /// * `buffer` - The MultiBuffer to load text into
    /// * `text` - The text content to load
    /// * `cx` - The GPUI app context
    pub fn load_text_into_buffer(buffer: &Entity<MultiBuffer>, text: String, cx: &mut App) {
        buffer.update(cx, |multi_buffer, cx| {
            // Get the singleton buffer from the MultiBuffer
            if let Some(single_buffer) = multi_buffer.as_singleton() {
                single_buffer.update(cx, |buffer, cx| {
                    // Get the full range of the current buffer
                    let buffer_len = buffer.len();
                    let full_range = 0..buffer_len;

                    // Replace the entire buffer content with the new text
                    buffer.edit([(full_range, text)], None, cx);
                });
            }
        });
    }

    /// Extracts text from a buffer
    ///
    /// Reads the entire text content from the buffer as a String.
    /// This is useful for saving queries or executing SQL statements.
    ///
    /// # Arguments
    /// * `buffer` - The MultiBuffer to extract text from
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// The buffer's text content as a String
    pub fn get_buffer_text(buffer: &Entity<MultiBuffer>, cx: &App) -> String {
        // Read the MultiBuffer, then read its underlying Buffer to get the text
        // buffer.read(cx) gives us &MultiBuffer
        // .read(cx) on that gives us &Buffer
        // .text() extracts the full text content
        buffer.read(cx).read(cx).text()
    }
}
