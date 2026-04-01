//! Settings file utilities

use anyhow::{Context, Result};
use std::path::PathBuf;

pub fn config_dir() -> Result<PathBuf> {
    Ok(zqlz_core::paths::config_dir()?)
}

pub fn data_dir() -> Result<PathBuf> {
    Ok(zqlz_core::paths::data_dir()?)
}

pub fn themes_dir() -> Result<PathBuf> {
    Ok(zqlz_core::paths::themes_dir()?)
}

pub fn layouts_dir() -> Result<PathBuf> {
    Ok(zqlz_core::paths::layouts_dir()?)
}

pub fn connections_file() -> Result<PathBuf> {
    Ok(zqlz_core::paths::connections_file()?)
}

pub fn history_file() -> Result<PathBuf> {
    Ok(zqlz_core::paths::query_history_file()?)
}

pub fn ensure_directories() -> Result<()> {
    zqlz_core::paths::ensure_directories().context("Failed to create ZQLZ directories")?;
    Ok(())
}
