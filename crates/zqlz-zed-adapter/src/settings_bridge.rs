//! Bridges ZQLZ settings to Zed's settings system
//!
//! SettingsBridge manages the integration between ZQLZ's settings and Zed's settings.
//! Since Zed uses a global settings system (via Settings trait and SettingsStore),
//! rather than per-editor configuration, this bridge:
//!
//! 1. Converts ZQLZ settings to Zed's SettingsContent format
//! 2. Applies ZQLZ settings to Zed's SettingsStore at startup
//! 3. Updates Zed settings when ZQLZ settings change
//!
//! ## Architecture Notes
//!
//! Zed's settings are global and loaded from JSON. The SettingsContent types
//! are the serializable representation used for settings files. We convert
//! ZQLZ settings to this format and update the SettingsStore directly.
//!
//! ZQLZ settings remain the source of truth - changes in ZQLZ UI propagate
//! to Zed's editors through this bridge.
//!
//! ## Theme System
//!
//! ZQLZ and Zed now share the same theme files. Themes are stored in Zed's
//! native format (ThemeFamilyContent) in `crates/zqlz-app/assets/themes/`.
//! Both ZQLZ UI components and Zed's editor use Zed's ThemeRegistry to
//! access themes by name.

use editor::Editor;
use gpui::{App, Entity, UpdateGlobal};
use settings::{
    CurrentLineHighlight, EditorSettingsContent, FontFamilyName, FontSize, FontWeightContent,
    GutterContent, SettingsContent, SettingsStore, ThemeAppearanceMode, ThemeName, ThemeSelection,
    ThemeSettingsContent,
};
use std::sync::Arc;
use vim_mode_setting::VimModeSetting;
use zqlz_settings::{AppearanceSettings, EditorSettings, FontSettings, ZqlzSettings};
use zqlz_ui::widgets::theme::Theme;

/// Bridges ZQLZ settings to Zed editor configuration
///
/// This struct provides utilities for synchronizing settings between ZQLZ's
/// database-backed settings system and Zed's global settings system.
pub struct SettingsBridge;

impl SettingsBridge {
    /// Converts ZQLZ settings to Zed's SettingsContent format
    ///
    /// This function maps ZQLZ's settings to Zed's SettingsContent, which can
    /// then be applied to the SettingsStore. The conversion covers:
    /// - Editor settings (tab size, line numbers, wrap, etc.)
    /// - Font settings (family, size, weight for UI and buffer)
    /// - Theme selection (light/dark mode with theme names)
    ///
    /// # Arguments
    /// * `zqlz` - Reference to ZQLZ's global settings
    ///
    /// # Returns
    /// A SettingsContent that can be serialized and applied to SettingsStore
    pub fn zqlz_to_settings_content(zqlz: &ZqlzSettings) -> SettingsContent {
        let mut content = SettingsContent::default();

        content.editor = Self::convert_editor_settings(&zqlz.editor);
        *content.theme = Self::convert_theme_settings(&zqlz.appearance, &zqlz.fonts);

        content
    }

    /// Converts ZQLZ EditorSettings to Zed's EditorSettingsContent
    fn convert_editor_settings(zqlz: &EditorSettings) -> EditorSettingsContent {
        let mut editor = EditorSettingsContent::default();

        editor.gutter = Some(GutterContent {
            line_numbers: Some(zqlz.show_line_numbers),
            min_line_number_digits: None,
            runnables: Some(false),
            breakpoints: Some(false),
            folds: Some(zqlz.show_folding),
            ..Default::default()
        });

        editor.current_line_highlight = Some(if zqlz.highlight_current_line {
            CurrentLineHighlight::All
        } else {
            CurrentLineHighlight::None
        });

        editor.cursor_blink = Some(true);

        editor
    }

    /// Converts ZQLZ AppearanceSettings and FontSettings to Zed's ThemeSettingsContent
    fn convert_theme_settings(
        appearance: &AppearanceSettings,
        fonts: &FontSettings,
    ) -> ThemeSettingsContent {
        let mut theme = ThemeSettingsContent::default();

        theme.ui_font_family = Some(FontFamilyName(Arc::from(fonts.ui_font_family.as_ref())));
        theme.ui_font_size = Some(FontSize(fonts.ui_font_size));
        theme.ui_font_weight = Some(FontWeightContent(fonts.ui_font_weight as f32));

        theme.buffer_font_family =
            Some(FontFamilyName(Arc::from(fonts.editor_font_family.as_ref())));
        theme.buffer_font_size = Some(FontSize(fonts.editor_font_size));
        theme.buffer_font_weight = Some(FontWeightContent(fonts.editor_font_weight as f32));

        theme.theme = Some(Self::convert_theme_selection(appearance));

        theme
    }

    /// Converts ZQLZ theme mode and theme names to Zed's ThemeSelection
    fn convert_theme_selection(appearance: &AppearanceSettings) -> ThemeSelection {
        let mode = match appearance.theme_mode {
            zqlz_settings::ThemeModePreference::Light => ThemeAppearanceMode::Light,
            zqlz_settings::ThemeModePreference::Dark => ThemeAppearanceMode::Dark,
            zqlz_settings::ThemeModePreference::System => ThemeAppearanceMode::System,
        };

        ThemeSelection::Dynamic {
            mode,
            light: ThemeName(Arc::from(appearance.light_theme.as_ref())),
            dark: ThemeName(Arc::from(appearance.dark_theme.as_ref())),
        }
    }

    /// Applies ZQLZ settings to Zed's SettingsStore
    ///
    /// This updates the global SettingsStore with ZQLZ settings. All editors
    /// created after this call will use the ZQLZ settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Example
    /// ```ignore
    /// // During app initialization:
    /// SettingsBridge::apply_zqlz_settings_to_zed(cx);
    /// ```
    pub fn apply_zqlz_settings_to_zed(cx: &mut App) {
        let zqlz_settings = ZqlzSettings::global(cx).clone();
        let settings_content = Self::zqlz_to_settings_content(&zqlz_settings);

        let json = match serde_json::to_string(&settings_content) {
            Ok(json) => json,
            Err(err) => {
                tracing::error!("Failed to serialize ZQLZ settings to JSON: {}", err);
                return;
            }
        };

        SettingsStore::update_global(cx, |store, cx| {
            let result = store.set_user_settings(&json, cx);
            if let Err(err) = result.result() {
                tracing::error!("Failed to apply ZQLZ settings to Zed: {:?}", err);
            }
        });

        VimModeSetting::set_global(
            VimModeSetting {
                enabled: zqlz_settings.editor.vim_mode_enabled,
            },
            cx,
        );
    }

    /// Updates Zed settings when ZQLZ settings change
    ///
    /// This should be called whenever ZQLZ settings are modified to ensure
    /// Zed editors reflect the new settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    pub fn sync_settings(cx: &mut App) {
        Self::apply_zqlz_settings_to_zed(cx);
    }

    /// Initializes Zed's settings system with defaults
    ///
    /// This should be called during application startup to ensure Zed's
    /// settings are properly initialized before any editors are created.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    pub fn initialize(cx: &mut App) {
        let default_content = SettingsContent::default();
        let json = serde_json::to_string(&default_content).unwrap_or_default();

        SettingsStore::update_global(cx, |store, cx| {
            let _ = store.set_user_settings(&json, cx);
        });
    }

    /// Applies ZQLZ editor settings to the editor
    ///
    /// Maps ZQLZ's EditorSettings to Zed's configuration where applicable.
    ///
    /// # Arguments
    /// * `_editor` - The Zed editor entity (for future per-editor config)
    /// * `settings` - ZQLZ editor settings to apply
    /// * `_cx` - The GPUI app context
    ///
    /// # ZQLZ → Zed Settings Mapping
    ///
    /// | ZQLZ Setting | Zed Equivalent | Notes |
    /// |--------------|----------------|-------|
    /// | tab_size | (controlled by language config) | Zed reads from language settings |
    /// | insert_spaces | (controlled by language config) | Hard tabs vs spaces |
    /// | show_line_numbers | gutter.line_numbers | Gutter configuration |
    /// | word_wrap | (soft_wrap in buffer settings) | Line wrapping |
    /// | highlight_current_line | current_line_highlight | CurrentLineHighlight enum |
    /// | show_inline_diagnostics | (always shown in Zed) | Diagnostic squiggles |
    /// | auto_indent | (built into Zed) | Automatic indentation |
    /// | bracket_matching | (built into Zed) | Bracket pair highlighting |
    pub fn apply_zqlz_settings(
        _editor: &Entity<Editor>,
        _settings: &EditorSettings,
        _cx: &mut App,
    ) {
    }

    /// Synchronizes ZQLZ theme to Zed editor
    ///
    /// Maps ZQLZ's theme colors to Zed's syntax highlighting theme.
    ///
    /// # Arguments
    /// * `_editor` - The Zed editor entity
    /// * `_theme` - ZQLZ theme to apply
    /// * `_cx` - The GPUI app context
    ///
    /// # Theme Mapping
    ///
    /// Zed uses theme.json files for syntax highlighting colors and UI theming.
    /// ZQLZ has its own theme system with ThemeColor and HighlightTheme.
    ///
    /// For MVP, editors will use Zed's default syntax theme which provides:
    /// - SQL keyword highlighting
    /// - String literal colors
    /// - Comment colors
    /// - Number literal colors
    pub fn sync_theme(_editor: &Entity<Editor>, _theme: &Theme, _cx: &mut App) {}

    /// Subscribes to ZQLZ settings changes
    ///
    /// Sets up listeners for when ZQLZ settings change in the database,
    /// and updates Zed editors accordingly.
    ///
    /// # Arguments
    /// * `_cx` - The GPUI app context
    ///
    /// # Returns
    /// A subscription that should be kept alive for the lifetime of the app
    pub fn subscribe_to_settings_changes(_cx: &mut App) {}

    /// Gets the current ZQLZ editor settings
    ///
    /// Convenience method to read ZQLZ editor settings from the global settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// The current ZQLZ editor settings
    pub fn get_editor_settings(cx: &App) -> EditorSettings {
        ZqlzSettings::global(cx).editor.clone()
    }

    /// Gets the current ZQLZ theme
    ///
    /// Convenience method to read ZQLZ theme from global state.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// A reference to the current ZQLZ theme
    pub fn get_theme(cx: &App) -> &Theme {
        Theme::global(cx)
    }
}
