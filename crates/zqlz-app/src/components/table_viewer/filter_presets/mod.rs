//! Filter presets module for table viewer
//!
//! Provides persistent storage and management for filter presets.

mod manager;
mod storage;

pub use manager::FilterPresetManager;
pub use storage::FilterPresetStorage;
