//! Bundled themes that ship with ZQLZ
//!
//! These themes are embedded at compile time from the assets/themes directory.

use gpui::App;
use rust_embed::RustEmbed;
use std::rc::Rc;
use zqlz_ui::widgets::{ThemeRegistry, ThemeSet};

#[derive(RustEmbed)]
#[folder = "assets/themes"]
#[include = "*.json"]
struct BundledThemeAssets;

fn bundled_theme_files() -> Vec<String> {
    let mut files: Vec<String> = BundledThemeAssets::iter()
        .filter(|path| path.as_ref().ends_with(".json"))
        .map(|path| path.to_string())
        .collect();
    files.sort_unstable();
    files
}

pub fn load_bundled_themes(cx: &mut App) {
    let registry = ThemeRegistry::global_mut(cx);
    let mut total_themes_loaded = 0;
    let mut total_theme_files_loaded = 0;

    for file_name in bundled_theme_files() {
        let Some(file) = BundledThemeAssets::get(&file_name) else {
            tracing::warn!("Missing embedded bundled theme file: {}", file_name);
            continue;
        };

        let content = match std::str::from_utf8(file.data.as_ref()) {
            Ok(content) => content,
            Err(err) => {
                tracing::warn!(
                    "Failed to decode bundled theme '{}' as UTF-8: {}",
                    file_name,
                    err
                );
                continue;
            }
        };

        match serde_json::from_str::<ThemeSet>(content) {
            Ok(theme_set) => {
                total_theme_files_loaded += 1;
                tracing::debug!(
                    "Parsed theme file '{}' with {} themes",
                    file_name,
                    theme_set.themes.len()
                );
                for theme in theme_set.themes {
                    let theme_name = theme.name.clone();
                    if !registry.themes().contains_key(&theme_name) {
                        registry
                            .themes_mut()
                            .insert(theme_name.clone(), Rc::new(theme));
                        total_themes_loaded += 1;
                        tracing::debug!("Loaded bundled theme: {}", theme_name);
                    }
                }
            }
            Err(err) => {
                tracing::warn!("Failed to parse bundled theme '{}': {}", file_name, err);
            }
        }
    }

    tracing::info!(
        "Loaded {} bundled themes from {} theme files",
        total_themes_loaded,
        total_theme_files_loaded
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
            let file = BundledThemeAssets::get(&file_name).unwrap_or_else(|| {
                panic!("Missing embedded theme file '{}': not found", file_name)
            });
            let content = std::str::from_utf8(file.data.as_ref())
                .unwrap_or_else(|err| panic!("Theme '{}' is not valid UTF-8: {}", file_name, err));
            let theme_set = serde_json::from_str::<ThemeSet>(content)
                .unwrap_or_else(|err| panic!("Theme '{}' failed to parse: {}", file_name, err));

            assert!(
                !theme_set.themes.is_empty(),
                "Theme file '{}' contains no theme entries",
                file_name
            );

            for theme in &theme_set.themes {
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

            total += theme_set.themes.len();
        }

        assert!(total > 0, "No themes were parsed from bundled files");
    }
}
