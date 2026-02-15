//! Asset source for ZQLZ theme files
//!
//! This module provides an `AssetSource` implementation that loads ZQLZ's
//! theme JSON files for use with Zed's `ThemeRegistry`.

use std::borrow::Cow;

use anyhow::Result;
use gpui::{AssetSource, SharedString};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/../zqlz-app/assets/themes"]
#[include = "*.json"]
pub struct ZqlzThemeAssets;

impl AssetSource for ZqlzThemeAssets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        let path = path.strip_prefix("themes/").unwrap_or(path);
        tracing::debug!("ZqlzThemeAssets::load({})", path);
        match Self::get(path) {
            Some(file) => {
                tracing::debug!(
                    "ZqlzThemeAssets::load({}) -> {} bytes",
                    path,
                    file.data.len()
                );
                Ok(Some(Cow::Owned(file.data.into_owned())))
            }
            None => {
                tracing::warn!("ZqlzThemeAssets::load({}) -> NOT FOUND", path);
                Ok(None)
            }
        }
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        tracing::debug!("ZqlzThemeAssets::list({})", path);
        let prefix = path;
        let files: Vec<_> = Self::iter()
            .filter(|p| p.ends_with(".json"))
            .map(|p| {
                let full_path = format!("themes/{}", p.as_ref());
                SharedString::from(full_path)
            })
            .filter(|p| p.starts_with(prefix))
            .collect();
        tracing::debug!("ZqlzThemeAssets::list({}) -> {} files", path, files.len());
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_theme_assets_list() {
        let assets = ZqlzThemeAssets;
        let files = assets.list("themes/").expect("Failed to list themes");
        eprintln!("Found {} theme files: {:?}", files.len(), files);
        assert!(!files.is_empty(), "Should have theme files");
        assert!(
            files.iter().any(|f| f.contains("catppuccin")),
            "Should have catppuccin theme"
        );
    }

    #[test]
    fn test_theme_assets_load() {
        let assets = ZqlzThemeAssets;
        let result = assets
            .load("themes/catppuccin.json")
            .expect("Failed to load catppuccin");
        assert!(result.is_some(), "Should load catppuccin.json");
        let data = result.unwrap();
        eprintln!("catppuccin.json is {} bytes", data.len());
        assert!(data.len() > 1000, "catppuccin.json should have content");
    }
}
