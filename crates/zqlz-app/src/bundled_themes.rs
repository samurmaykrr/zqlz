//! Bundled themes that ship with ZQLZ
//!
//! These themes are embedded at compile time from the assets/themes directory.

use gpui::App;
use std::rc::Rc;
use zqlz_ui::widgets::{ThemeRegistry, ThemeSet};

const ADVENTURE: &str = include_str!("../assets/themes/adventure.json");
const ALDUIN: &str = include_str!("../assets/themes/alduin.json");
const AYU: &str = include_str!("../assets/themes/ayu.json");
const CATPPUCCIN: &str = include_str!("../assets/themes/catppuccin.json");
const CYBERPUNK_SCARLET: &str = include_str!("../assets/themes/cyberpunk-scarlet.json");
const EVERFOREST: &str = include_str!("../assets/themes/everforest.json");
const FAHRENHEIT: &str = include_str!("../assets/themes/fahrenheit.json");
const FLEXOKI: &str = include_str!("../assets/themes/flexoki.json");
const GRUVBOX: &str = include_str!("../assets/themes/gruvbox.json");
const HARPER: &str = include_str!("../assets/themes/harper.json");
const HYBRID: &str = include_str!("../assets/themes/hybrid.json");
const JELLYBEANS: &str = include_str!("../assets/themes/jellybeans.json");
const KIBBLE: &str = include_str!("../assets/themes/kibble.json");
const MACOS_CLASSIC: &str = include_str!("../assets/themes/macos-classic.json");
const MATRIX: &str = include_str!("../assets/themes/matrix.json");
const MELLIFLUOUS: &str = include_str!("../assets/themes/mellifluous.json");
const MOLOKAI: &str = include_str!("../assets/themes/molokai.json");
const SOLARIZED: &str = include_str!("../assets/themes/solarized.json");
const SPACEDUCK: &str = include_str!("../assets/themes/spaceduck.json");
const TOKYONIGHT: &str = include_str!("../assets/themes/tokyonight.json");
const TWILIGHT: &str = include_str!("../assets/themes/twilight.json");

const BUNDLED_THEMES: &[(&str, &str)] = &[
    ("adventure", ADVENTURE),
    ("alduin", ALDUIN),
    ("ayu", AYU),
    ("catppuccin", CATPPUCCIN),
    ("cyberpunk-scarlet", CYBERPUNK_SCARLET),
    ("everforest", EVERFOREST),
    ("fahrenheit", FAHRENHEIT),
    ("flexoki", FLEXOKI),
    ("gruvbox", GRUVBOX),
    ("harper", HARPER),
    ("hybrid", HYBRID),
    ("jellybeans", JELLYBEANS),
    ("kibble", KIBBLE),
    ("macos-classic", MACOS_CLASSIC),
    ("matrix", MATRIX),
    ("mellifluous", MELLIFLUOUS),
    ("molokai", MOLOKAI),
    ("solarized", SOLARIZED),
    ("spaceduck", SPACEDUCK),
    ("tokyonight", TOKYONIGHT),
    ("twilight", TWILIGHT),
];

pub fn load_bundled_themes(cx: &mut App) {
    let registry = ThemeRegistry::global_mut(cx);
    let mut total_themes_loaded = 0;

    for (name, content) in BUNDLED_THEMES {
        match serde_json::from_str::<ThemeSet>(content) {
            Ok(theme_set) => {
                tracing::debug!(
                    "Parsed theme file '{}' with {} themes",
                    name,
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
                tracing::warn!("Failed to parse bundled theme '{}': {}", name, err);
            }
        }
    }

    tracing::info!(
        "Loaded {} bundled themes from {} theme files",
        total_themes_loaded,
        BUNDLED_THEMES.len()
    );
}
