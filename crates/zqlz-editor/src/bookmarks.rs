//! Query bookmarks module
//!
//! Provides bookmark storage and management for saved SQL queries.

mod manager;
mod storage;

pub use manager::{BookmarkFilter, BookmarkManager};
pub use storage::{Bookmark, BookmarkStorage};
