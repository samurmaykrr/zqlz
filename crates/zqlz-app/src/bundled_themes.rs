//! Bundled themes that ship with ZQLZ
//!
//! These themes are embedded at compile time from the assets/themes directory.
//! Startup loading remains lazy so only active themes are parsed up front.

use anyhow::{Context as _, Result};
use gpui::App;
use rust_embed::RustEmbed;
use std::collections::HashSet;
use zqlz_settings::ZqlzSettings;
use zqlz_ui::widgets::{ThemeCatalogEntry, ThemeConfig, ThemeRegistry, ThemeSet};

#[derive(RustEmbed)]
#[folder = "assets/themes"]
#[include = "*.json"]
struct BundledThemeAssets;

#[derive(Clone, Copy)]
struct BundledThemeManifestEntry {
    theme_name: &'static str,
    file_name: &'static str,
    is_default: bool,
}

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bundled_theme_manifest.rs"));

#[cfg(rust_analyzer)]
const BUNDLED_THEME_MANIFEST: &[BundledThemeManifestEntry] = &[];

#[cfg(test)]
fn bundled_theme_files() -> Vec<String> {
    let mut files: Vec<String> = BundledThemeAssets::iter()
        .filter(|path| path.as_ref().ends_with(".json"))
        .map(|path| path.to_string())
        .collect();
    files.sort_unstable();
    files
}

fn bundled_theme_file_for_name(theme_name: &str) -> Option<&'static str> {
    BUNDLED_THEME_MANIFEST
        .iter()
        .find(|entry| entry.theme_name == theme_name)
        .map(|entry| entry.file_name)
}

fn bundled_theme_catalog_entries() -> Vec<ThemeCatalogEntry> {
    BUNDLED_THEME_MANIFEST
        .iter()
        .map(|entry| ThemeCatalogEntry {
            name: entry.theme_name.into(),
            is_default: entry.is_default,
        })
        .collect()
}

fn parse_bundled_theme_file(file_name: &str) -> Result<Vec<ThemeConfig>> {
    let file = BundledThemeAssets::get(file_name)
        .with_context(|| format!("missing embedded bundled theme file '{file_name}'"))?;

    let content = std::str::from_utf8(file.data.as_ref())
        .with_context(|| format!("failed to decode bundled theme file '{file_name}' as UTF-8"))?;

    let parsed = serde_json::from_str::<ThemeSet>(content)
        .with_context(|| format!("failed to parse bundled theme file '{file_name}'"))?;

    Ok(parsed.themes)
}

fn load_bundled_theme_by_name(theme_name: &str) -> Result<Vec<ThemeConfig>> {
    let Some(file_name) = bundled_theme_file_for_name(theme_name) else {
        return Ok(Vec::new());
    };

    parse_bundled_theme_file(file_name)
}

fn startup_theme_names(cx: &App) -> Vec<gpui::SharedString> {
    let mut startup_names = HashSet::new();

    if cx.has_global::<ZqlzSettings>() {
        let settings = ZqlzSettings::global(cx);
        startup_names.insert(settings.appearance.light_theme.clone());
        startup_names.insert(settings.appearance.dark_theme.clone());
    }

    let registry = ThemeRegistry::global(cx);
    startup_names.insert(registry.default_light_theme().name.clone());
    startup_names.insert(registry.default_dark_theme().name.clone());

    startup_names.into_iter().collect()
}

pub fn load_bundled_themes(cx: &mut App) {
    let catalog_entries = bundled_theme_catalog_entries();
    let startup_themes = startup_theme_names(cx);

    {
        let registry = ThemeRegistry::global_mut(cx);
        registry.register_theme_catalog(catalog_entries);
        registry.register_lazy_theme_loader(load_bundled_theme_by_name);
    }

    let mut ensured_theme_count = 0;
    for theme_name in &startup_themes {
        if ThemeRegistry::global_mut(cx).ensure_theme_loaded_by_name(theme_name) {
            ensured_theme_count += 1;
            continue;
        }

        tracing::warn!(
            theme = %theme_name,
            "Configured startup theme was not found in bundled catalog"
        );
    }

    tracing::info!(
        ensured_theme_count,
        catalog_theme_count = BUNDLED_THEME_MANIFEST.len(),
        "Registered bundled theme catalog and ensured startup themes are loaded"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_bundled_themes_parse_without_error() {
        let bundled_files = bundled_theme_files();
        assert!(
            !bundled_files.is_empty(),
            "No bundled theme files were found in embedded assets"
        );

        let mut total = 0;
        for file_name in bundled_files {
            let themes = parse_bundled_theme_file(&file_name)
                .unwrap_or_else(|error| panic!("Theme '{}' failed to parse: {}", file_name, error));

            assert!(
                !themes.is_empty(),
                "Theme file '{}' contains no theme entries",
                file_name
            );

            for theme in &themes {
                assert!(
                    !theme.name.is_empty(),
                    "A theme entry in '{}' has an empty name",
                    file_name
                );
                // All Zed themes must be parsed with a non-default background color.
                assert!(
                    theme.colors.background.is_some(),
                    "Theme '{}' in '{}' has no background color — style mapping may be broken",
                    theme.name,
                    file_name
                );
            }

            total += themes.len();
        }

        assert!(total > 0, "No themes were parsed from bundled files");
    }

    #[test]
    fn bundled_theme_manifest_points_to_embedded_assets() {
        assert!(
            !BUNDLED_THEME_MANIFEST.is_empty(),
            "Bundled theme manifest should not be empty"
        );

        for entry in BUNDLED_THEME_MANIFEST {
            assert!(
                BundledThemeAssets::get(entry.file_name).is_some(),
                "Bundled theme manifest references missing file '{}' for theme '{}'",
                entry.file_name,
                entry.theme_name
            );
        }
    }
}
