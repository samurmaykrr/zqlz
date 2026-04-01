//! Canonical filesystem paths for ZQLZ runtime data.
//!
//! This module centralizes every on-disk location used by the application so
//! all crates read/write from the same directory layout.

#[cfg(windows)]
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

/// Root directory name inside `~/.config`.
pub const APP_DIRECTORY_NAME: &str = "zqlz";

/// Main persisted settings file name.
pub const SETTINGS_FILE_NAME: &str = "settings.json";

/// Saved connections JSON file name.
pub const CONNECTIONS_FILE_NAME: &str = "connections.json";

/// SQLite query history database file name.
pub const QUERY_HISTORY_FILE_NAME: &str = "query_history.db";

/// Internal storage SQLite database file name.
pub const STORAGE_FILE_NAME: &str = "storage.db";

/// IPC socket file name used on Unix platforms.
pub const IPC_SOCKET_FILE_NAME: &str = "ipc.sock";

/// Resolve the canonical ZQLZ directory: `~/.config/zqlz`.
pub fn config_dir() -> Result<PathBuf> {
    let home_directory = dirs::home_dir().ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            "Could not determine home directory for ZQLZ config paths",
        )
    })?;

    Ok(home_directory.join(".config").join(APP_DIRECTORY_NAME))
}

/// Alias for [`config_dir`], kept for call-sites that conceptually read from a
/// runtime data directory.
pub fn data_dir() -> Result<PathBuf> {
    config_dir()
}

/// Directory containing user-installed themes.
pub fn themes_dir() -> Result<PathBuf> {
    config_dir().map(|path| path.join("themes"))
}

/// Directory containing saved workspace layouts.
pub fn layouts_dir() -> Result<PathBuf> {
    config_dir().map(|path| path.join("layouts"))
}

/// Directory containing ZQLZ logs.
pub fn logs_dir() -> Result<PathBuf> {
    config_dir().map(|path| path.join("logs"))
}

/// Directory containing schema cache files.
pub fn schema_cache_dir() -> Result<PathBuf> {
    config_dir().map(|path| path.join("schema_cache"))
}

/// Path to the main settings JSON file.
pub fn settings_file() -> Result<PathBuf> {
    config_dir().map(|path| path.join(SETTINGS_FILE_NAME))
}

/// Path to the saved connections JSON file.
pub fn connections_file() -> Result<PathBuf> {
    config_dir().map(|path| path.join(CONNECTIONS_FILE_NAME))
}

/// Path to the query history SQLite database.
pub fn query_history_file() -> Result<PathBuf> {
    config_dir().map(|path| path.join(QUERY_HISTORY_FILE_NAME))
}

/// Path to the internal application SQLite database.
pub fn storage_file() -> Result<PathBuf> {
    config_dir().map(|path| path.join(STORAGE_FILE_NAME))
}

/// Path to the IPC endpoint.
///
/// Unix: filesystem socket at `~/.config/zqlz/ipc.sock`.
/// Windows: per-user named pipe derived from the canonical config directory.
pub fn ipc_endpoint() -> Result<PathBuf> {
    #[cfg(unix)]
    {
        return config_dir().map(|path| path.join(IPC_SOCKET_FILE_NAME));
    }

    #[cfg(windows)]
    {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        config_dir()?
            .to_string_lossy()
            .to_lowercase()
            .hash(&mut hasher);
        let endpoint = format!(r"\\.\pipe\zqlz-ipc-{:016x}", hasher.finish());
        return Ok(PathBuf::from(endpoint));
    }

    #[allow(unreachable_code)]
    config_dir().map(|path| path.join(IPC_SOCKET_FILE_NAME))
}

/// Ensure all commonly used directories exist.
pub fn ensure_directories() -> Result<()> {
    let directories = [
        config_dir()?,
        themes_dir()?,
        layouts_dir()?,
        logs_dir()?,
        schema_cache_dir()?,
    ];

    for directory in directories {
        std::fs::create_dir_all(&directory)?;
    }

    Ok(())
}

/// Ensure a file's parent directory exists.
pub fn ensure_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent_directory) = path.parent() {
        std::fs::create_dir_all(parent_directory)?;
    }

    Ok(())
}
