//! Bridge between Zed's ThemeRegistry and ZQLZ's Theme system
//!
//! This module synchronizes theme data from Zed's `ThemeRegistry` (which loads
//! themes from JSON files in Zed's format) to ZQLZ's global `Theme` struct.
//!
//! ## Architecture
//!
//! 1. **Source of Truth**: Zed's `ThemeRegistry` - loads themes from JSON files
//! 2. **Consumer**: ZQLZ's `Theme` global - used by all ZQLZ UI components
//! 3. **Bridge**: This module - maps Zed theme colors to ZQLZ theme colors
//!
//! ## Color Mapping
//!
//! Zed's theme schema uses semantic names like `border`, `text`, `panel.background`.
//! ZQLZ uses similar but not identical naming. This bridge maps between them.

use gpui::App;
use theme::{ActiveTheme as ZedActiveTheme, Theme as ZedTheme, ThemeRegistry};
use zqlz_ui::widgets::theme::{Theme, ThemeColor, ThemeMode};

use crate::highlight_bridge::HighlightThemeBridge;

fn convert_theme_color(zed: &ZedTheme) -> ThemeColor {
    let colors = &zed.styles.colors;
    let status = &zed.styles.status;

    ThemeColor {
        accent: colors.ghost_element_hover,
        accent_foreground: colors.text_accent,
        accordion: colors.surface_background,
        accordion_hover: colors.ghost_element_hover,
        // Use editor_background so the ZQLZ UI surrounding the Zed editor
        // matches the editor's own background color (they differ in most themes).
        background: colors.editor_background,
        border: colors.border,
        group_box: colors.surface_background,
        group_box_foreground: colors.text,
        caret: colors.text,
        chart_1: status.created,
        chart_2: status.info,
        chart_3: status.warning,
        chart_4: status.hint,
        chart_5: status.modified,
        danger: status.error,
        danger_active: status.error,
        danger_foreground: colors.text,
        danger_hover: status.error,
        description_list_label: colors.surface_background,
        description_list_label_foreground: colors.text_muted,
        drag_border: colors.border_focused,
        drop_target: colors.drop_target_background,
        foreground: colors.text,
        info: status.info,
        info_active: status.info,
        info_foreground: colors.text,
        info_hover: status.info,
        input: colors.border,
        link: colors.text_accent,
        link_active: colors.text_accent,
        link_hover: colors.text_accent,
        list: colors.surface_background,
        list_active: colors.ghost_element_selected,
        list_active_border: colors.border_selected,
        list_even: colors.surface_background,
        list_head: colors.panel_background,
        list_hover: colors.ghost_element_hover,
        muted: colors.surface_background,
        muted_foreground: colors.text_muted,
        popover: colors.elevated_surface_background,
        popover_foreground: colors.text,
        primary: colors.element_active,
        primary_active: colors.element_active,
        primary_foreground: colors.text,
        primary_hover: colors.element_hover,
        progress_bar: colors.surface_background,
        ring: colors.border_focused,
        scrollbar: colors.scrollbar_track_background,
        scrollbar_thumb: colors.scrollbar_thumb_background,
        scrollbar_thumb_hover: colors.scrollbar_thumb_hover_background,
        secondary: colors.surface_background,
        secondary_active: colors.ghost_element_active,
        secondary_foreground: colors.text,
        secondary_hover: colors.ghost_element_hover,
        selection: colors.element_selection_background,
        sidebar: colors.panel_background,
        sidebar_accent: colors.ghost_element_hover,
        sidebar_accent_foreground: colors.text,
        sidebar_border: colors.border,
        sidebar_foreground: colors.text,
        sidebar_primary: colors.element_active,
        sidebar_primary_foreground: colors.text,
        skeleton: colors.surface_background,
        slider_bar: colors.surface_background,
        slider_thumb: colors.element_active,
        success: status.created,
        success_foreground: colors.text,
        success_hover: status.created,
        success_active: status.created,
        bullish: status.created,
        bearish: status.error,
        switch: colors.element_background,
        switch_thumb: colors.text,
        tab: colors.tab_inactive_background,
        tab_active: colors.tab_active_background,
        tab_active_foreground: colors.text,
        tab_bar: colors.tab_bar_background,
        tab_bar_segmented: colors.surface_background,
        tab_foreground: colors.text_muted,
        table: colors.surface_background,
        table_active: colors.ghost_element_selected,
        table_active_border: colors.border_selected,
        table_even: colors.surface_background,
        table_head: colors.panel_background,
        table_head_foreground: colors.text_muted,
        table_hover: colors.ghost_element_hover,
        table_row_border: colors.border,
        title_bar: colors.title_bar_background,
        title_bar_border: colors.border,
        tiles: colors.surface_background,
        warning: status.warning,
        warning_active: status.warning,
        warning_hover: status.warning,
        warning_foreground: colors.text,
        overlay: colors.elevated_surface_background,
        window_border: colors.border,
        red: status.error,
        red_light: status.error,
        green: status.created,
        green_light: status.created,
        blue: status.info,
        blue_light: status.info,
        yellow: status.warning,
        yellow_light: status.warning,
        magenta: status.hint,
        magenta_light: status.hint,
        cyan: status.hint,
        cyan_light: status.hint,
    }
}

pub struct ThemeBridge;

impl ThemeBridge {
    pub fn sync_zed_theme_to_zqlz(cx: &mut App) {
        let zed_theme = cx.theme();
        let settings = zqlz_settings::ZqlzSettings::global(cx);

        let mode = match settings.appearance.theme_mode {
            zqlz_settings::ThemeModePreference::Light => ThemeMode::Light,
            zqlz_settings::ThemeModePreference::Dark => ThemeMode::Dark,
            zqlz_settings::ThemeModePreference::System => {
                if zed_theme.appearance.is_light() {
                    ThemeMode::Light
                } else {
                    ThemeMode::Dark
                }
            }
        };

        let colors = convert_theme_color(zed_theme);
        let highlight_theme = HighlightThemeBridge::from_zed_theme(zed_theme);

        tracing::info!(
            theme_name = %zed_theme.name,
            editor_bg = ?zed_theme.styles.colors.editor_background,
            window_bg = ?zed_theme.styles.colors.background,
            mapped_bg = ?colors.background,
            "ThemeBridge: synced Zed theme to ZQLZ (background mapped from editor_background)"
        );

        Theme::update_from_zed(colors, highlight_theme, mode, cx);
    }

    pub fn sync_theme_by_name(theme_name: &str, cx: &mut App) -> anyhow::Result<()> {
        let registry = ThemeRegistry::global(cx);
        let zed_theme = registry.get(theme_name)?;

        let colors = convert_theme_color(&zed_theme);
        let highlight_theme = HighlightThemeBridge::from_zed_theme(&zed_theme);
        let mode = if zed_theme.appearance.is_light() {
            ThemeMode::Light
        } else {
            ThemeMode::Dark
        };

        Theme::update_from_zed(colors, highlight_theme, mode, cx);

        Ok(())
    }
}
