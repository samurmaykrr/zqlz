//! Settings file utilities

use anyhow::{Context, Result};
use std::path::PathBuf;

pub fn config_dir() -> Result<PathBuf> {
    dirs::config_dir()
        .context("Could not determine config directory")
        .map(|p| p.join("zqlz"))
}

pub fn data_dir() -> Result<PathBuf> {
    dirs::data_dir()
        .context("Could not determine data directory")
        .map(|p| p.join("zqlz"))
}

pub fn themes_dir() -> Result<PathBuf> {
    config_dir().map(|p| p.join("themes"))
}

pub fn layouts_dir() -> Result<PathBuf> {
    data_dir().map(|p| p.join("layouts"))
}

pub fn connections_file() -> Result<PathBuf> {
    data_dir().map(|p| p.join("connections.json"))
}

pub fn history_file() -> Result<PathBuf> {
    data_dir().map(|p| p.join("query_history.db"))
}

pub fn ensure_directories() -> Result<()> {
    let dirs = [config_dir()?, data_dir()?, themes_dir()?, layouts_dir()?];
    for dir in dirs {
        if !dir.exists() {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create directory: {:?}", dir))?;
        }
    }
    Ok(())
}
