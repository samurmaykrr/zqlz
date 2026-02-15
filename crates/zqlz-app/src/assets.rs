//! Combined asset source for ZQLZ.
//!
//! This provides ZQLZ-specific assets (icons, etc).

use anyhow::anyhow;
use gpui::{AssetSource, Result, SharedString};
use rust_embed::RustEmbed;
use std::borrow::Cow;

/// ZQLZ-specific assets (icons, etc)
#[derive(RustEmbed)]
#[folder = "assets"]
#[include = "icons/**/*.svg"]
#[include = "icons/**/*.png"]
pub struct ZqlzAssets;

/// Combined asset source.
pub struct CombinedAssets;

impl AssetSource for CombinedAssets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        if path.is_empty() {
            return Ok(None);
        }

        // First try ZQLZ assets
        if let Some(file) = ZqlzAssets::get(path) {
            return Ok(Some(file.data));
        }

        // Asset not found
        Err(anyhow!("could not find asset at path \"{path}\""))
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        // List from ZQLZ assets
        let result: Vec<SharedString> = ZqlzAssets::iter()
            .filter_map(|p| p.starts_with(path).then(|| p.into()))
            .collect();

        Ok(result)
    }
}
