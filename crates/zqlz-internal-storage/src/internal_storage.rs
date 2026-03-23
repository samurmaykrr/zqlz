use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

pub use rusqlite;

pub struct InternalStorage {
    path: PathBuf,
}

impl InternalStorage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory {}", parent.display()))?;
        }
        Ok(Self { path })
    }

    pub fn for_config_file(file_name: &str) -> Result<Self> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        let app_dir = config_dir.join("zqlz");
        Self::open(app_dir.join(file_name))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn connect(&self) -> Result<Connection> {
        Connection::open(&self.path)
            .with_context(|| format!("Failed to open database at {}", self.path.display()))
    }
}
