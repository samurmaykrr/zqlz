use std::{rc::Rc, sync::Arc};

use gpui::{px, SharedString};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use crate::widgets::{
    highlighter::{HighlightTheme, HighlightThemeStyle},
    try_parse_color, Colorize, Theme, ThemeColor, ThemeMode,
};

/// Represents a theme configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ThemeSet {
    /// The name of the theme set.
    pub name: SharedString,
    /// The author of the theme.
    pub author: Option<SharedString>,
    /// The URL of the theme.
    pub url: Option<SharedString>,
    /// The theme list of the theme set.
    #[serde(rename = "themes")]
    pub themes: Vec<ThemeConfig>,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct ThemeConfig {
    /// Whether this theme is the default theme.
    pub is_default: bool,
    /// The name of the theme.
    pub name: SharedString,
    /// The mode of the theme, default is light.
    pub mode: ThemeMode,

    /// The base font size, default is 16.
    #[serde(rename = "font.size")]
    pub font_size: Option<f32>,
    /// The base font family, default is system font: `.SystemUIFont`.
    #[serde(rename = "font.family")]
    pub font_family: Option<SharedString>,
    /// The monospace font family, default is platform specific:
    /// - macOS: `Menlo`
    /// - Windows: `Consolas`
    /// - Linux: `DejaVu Sans Mono`
    #[serde(rename = "mono_font.family")]
    pub mono_font_family: Option<SharedString>,
    /// The monospace font size, default is 13.
    #[serde(rename = "mono_font.size")]
    pub mono_font_size: Option<f32>,

    /// The border radius for general elements, default is 6.
    #[serde(rename = "radius")]
    pub radius: Option<usize>,
    /// The border radius for large elements like Dialogs and Notifications, default is 8.
    #[serde(rename = "radius.lg")]
    pub radius_lg: Option<usize>,
    /// Set shadows in the theme, for example the Input and Button, default is true.
    #[serde(rename = "shadow")]
    pub shadow: Option<bool>,

    /// The colors of the theme.
    pub colors: ThemeConfigColors,
    /// The highlight theme, this part is combilbility with `style` section in Zed theme.
    ///
    /// https://github.com/zed-industries/zed/blob/f50041779dcfd7a76c8aec293361c60c53f02d51/assets/themes/ayu/ayu.json#L9
    pub highlight: Option<HighlightThemeStyle>,
}

impl<'de> Deserialize<'de> for ThemeConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        deserialize_theme_config(value).map_err(serde::de::Error::custom)
    }
}

/// Deserializes a `ThemeConfig` from a raw JSON value, supporting both the internal
/// format (with `"mode"` + `"colors"`) and Zed's theme format (with `"appearance"` + `"style"`).
fn deserialize_theme_config(value: serde_json::Value) -> anyhow::Result<ThemeConfig> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected object"))?;

    // Zed format uses "appearance"; internal format uses "mode".
    if obj.contains_key("appearance") {
        deserialize_zed_theme(obj)
    } else {
        deserialize_internal_theme(value)
    }
}

/// Deserializes a theme entry using the internal format (used by default-theme.json).
fn deserialize_internal_theme(value: serde_json::Value) -> anyhow::Result<ThemeConfig> {
    // Helper struct that mirrors ThemeConfig with all serde renames, used only for internal format.
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Internal {
        is_default: bool,
        name: SharedString,
        mode: ThemeMode,
        #[serde(rename = "font.size")]
        font_size: Option<f32>,
        #[serde(rename = "font.family")]
        font_family: Option<SharedString>,
        #[serde(rename = "mono_font.family")]
        mono_font_family: Option<SharedString>,
        #[serde(rename = "mono_font.size")]
        mono_font_size: Option<f32>,
        #[serde(rename = "radius")]
        radius: Option<usize>,
        #[serde(rename = "radius.lg")]
        radius_lg: Option<usize>,
        shadow: Option<bool>,
        colors: ThemeConfigColors,
        highlight: Option<HighlightThemeStyle>,
    }

    let internal: Internal = serde_json::from_value(value)?;
    Ok(ThemeConfig {
        is_default: internal.is_default,
        name: internal.name,
        mode: internal.mode,
        font_size: internal.font_size,
        font_family: internal.font_family,
        mono_font_family: internal.mono_font_family,
        mono_font_size: internal.mono_font_size,
        radius: internal.radius,
        radius_lg: internal.radius_lg,
        shadow: internal.shadow,
        colors: internal.colors,
        highlight: internal.highlight,
    })
}

/// Extracts an optional string from a Zed style object by key.
#[inline]
fn style_str(
    style: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<SharedString> {
    style
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| SharedString::from(s.to_string()))
}

/// Deserializes a Zed-format theme entry, remapping Zed's `appearance` and `style`
/// fields to the internal `ThemeConfig` structure.
fn deserialize_zed_theme(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<ThemeConfig> {
    let name: SharedString = obj
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
        .into();

    let mode = match obj.get("appearance").and_then(|v| v.as_str()) {
        Some("dark") => ThemeMode::Dark,
        _ => ThemeMode::Light,
    };

    let style = obj
        .get("style")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("Zed theme missing 'style' object"))?;

    let colors = ThemeConfigColors {
        background: style_str(style, "background"),
        foreground: style_str(style, "text"),
        muted_foreground: style_str(style, "text.muted"),
        accent_foreground: style_str(style, "text.accent"),
        border: style_str(style, "border"),
        ring: style_str(style, "border.focused")
            .or_else(|| style_str(style, "panel.focused_border")),
        tab_bar: style_str(style, "tab_bar.background"),
        tab_active: style_str(style, "tab.active_background"),
        tab: style_str(style, "tab.inactive_background"),
        tab_foreground: style_str(style, "tab.text"),
        tab_active_foreground: style_str(style, "tab.active_text"),
        title_bar: style_str(style, "title_bar.background"),
        accent: style_str(style, "icon.accent"),
        secondary: style_str(style, "element.background")
            .or_else(|| style_str(style, "surface.background"))
            .or_else(|| style_str(style, "panel.background")),
        secondary_hover: style_str(style, "element.hover"),
        popover: style_str(style, "elevated_surface.background"),
        list_active: style_str(style, "element.selected")
            .or_else(|| style_str(style, "ghost_element.selected")),
        list_hover: style_str(style, "ghost_element.hover"),
        drop_target: style_str(style, "drop_target.background"),
        link_hover: style_str(style, "link_text.hover"),
        scrollbar: style_str(style, "scrollbar.track.background"),
        scrollbar_thumb: style_str(style, "scrollbar.thumb.background"),
        scrollbar_thumb_hover: style_str(style, "scrollbar.thumb.hover_background"),
        danger: style_str(style, "error"),
        info: style_str(style, "info"),
        success: style_str(style, "success"),
        warning: style_str(style, "warning"),
        // Fields with no direct Zed mapping fall back to None (apply_config handles fallbacks).
        ..ThemeConfigColors::default()
    };

    // The `style` object in Zed themes contains both UI colors and highlight/editor colors.
    // We deserialize the whole style object into HighlightThemeStyle which uses matching key names.
    let highlight: Option<HighlightThemeStyle> =
        serde_json::from_value(serde_json::Value::Object(style.clone())).ok();

    Ok(ThemeConfig {
        is_default: false,
        name,
        mode,
        font_size: None,
        font_family: None,
        mono_font_family: None,
        mono_font_size: None,
        radius: None,
        radius_lg: None,
        shadow: None,
        colors,
        highlight,
    })
}

#[derive(Debug, Default, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ThemeConfigColors {
    /// Used for accents such as hover background on MenuItem, ListItem, etc.
    #[serde(rename = "accent.background")]
    pub accent: Option<SharedString>,
    /// Used for accent text color.
    #[serde(rename = "accent.foreground")]
    pub accent_foreground: Option<SharedString>,
    /// Accordion background color.
    #[serde(rename = "accordion.background")]
    pub accordion: Option<SharedString>,
    /// Accordion hover background color.
    #[serde(rename = "accordion.hover.background")]
    pub accordion_hover: Option<SharedString>,
    /// Default background color.
    #[serde(rename = "background")]
    pub background: Option<SharedString>,
    /// Default border color
    #[serde(rename = "border")]
    pub border: Option<SharedString>,
    /// Background color for GroupBox.
    #[serde(rename = "group_box.background")]
    pub group_box: Option<SharedString>,
    /// Text color for GroupBox.
    #[serde(rename = "group_box.foreground")]
    pub group_box_foreground: Option<SharedString>,
    /// Title text color for GroupBox.
    #[serde(rename = "group_box.title.foreground")]
    pub group_box_title_foreground: Option<SharedString>,
    /// Input caret color (Blinking cursor).
    #[serde(rename = "caret")]
    pub caret: Option<SharedString>,
    /// Chart 1 color.
    #[serde(rename = "chart.1")]
    pub chart_1: Option<SharedString>,
    /// Chart 2 color.
    #[serde(rename = "chart.2")]
    pub chart_2: Option<SharedString>,
    /// Chart 3 color.
    #[serde(rename = "chart.3")]
    pub chart_3: Option<SharedString>,
    /// Chart 4 color.
    #[serde(rename = "chart.4")]
    pub chart_4: Option<SharedString>,
    /// Chart 5 color.
    #[serde(rename = "chart.5")]
    pub chart_5: Option<SharedString>,
    /// Danger background color.
    #[serde(rename = "danger.background")]
    pub danger: Option<SharedString>,
    /// Danger active background color.
    #[serde(rename = "danger.active.background")]
    pub danger_active: Option<SharedString>,
    /// Danger text color.
    #[serde(rename = "danger.foreground")]
    pub danger_foreground: Option<SharedString>,
    /// Danger hover background color.
    #[serde(rename = "danger.hover.background")]
    pub danger_hover: Option<SharedString>,
    /// Description List label background color.
    #[serde(rename = "description_list.label.background")]
    pub description_list_label: Option<SharedString>,
    /// Description List label foreground color.
    #[serde(rename = "description_list.label.foreground")]
    pub description_list_label_foreground: Option<SharedString>,
    /// Drag border color.
    #[serde(rename = "drag.border")]
    pub drag_border: Option<SharedString>,
    /// Drop target background color.
    #[serde(rename = "drop_target.background")]
    pub drop_target: Option<SharedString>,
    /// Default text color.
    #[serde(rename = "foreground")]
    pub foreground: Option<SharedString>,
    /// Info background color.
    #[serde(rename = "info.background")]
    pub info: Option<SharedString>,
    /// Info active background color.
    #[serde(rename = "info.active.background")]
    pub info_active: Option<SharedString>,
    /// Info text color.
    #[serde(rename = "info.foreground")]
    pub info_foreground: Option<SharedString>,
    /// Info hover background color.
    #[serde(rename = "info.hover.background")]
    pub info_hover: Option<SharedString>,
    /// Border color for inputs such as Input, Select, etc.
    #[serde(rename = "input.border")]
    pub input: Option<SharedString>,
    /// Link text color.
    #[serde(rename = "link")]
    pub link: Option<SharedString>,
    /// Active link text color.
    #[serde(rename = "link.active")]
    pub link_active: Option<SharedString>,
    /// Hover link text color.
    #[serde(rename = "link.hover")]
    pub link_hover: Option<SharedString>,
    /// Background color for List and ListItem.
    #[serde(rename = "list.background")]
    pub list: Option<SharedString>,
    /// Background color for active ListItem.
    #[serde(rename = "list.active.background")]
    pub list_active: Option<SharedString>,
    /// Border color for active ListItem.
    #[serde(rename = "list.active.border")]
    pub list_active_border: Option<SharedString>,
    /// Stripe background color for even ListItem.
    #[serde(rename = "list.even.background")]
    pub list_even: Option<SharedString>,
    /// Background color for List header.
    #[serde(rename = "list.head.background")]
    pub list_head: Option<SharedString>,
    /// Hover background color for ListItem.
    #[serde(rename = "list.hover.background")]
    pub list_hover: Option<SharedString>,
    /// Muted backgrounds such as Skeleton and Switch.
    #[serde(rename = "muted.background")]
    pub muted: Option<SharedString>,
    /// Muted text color, as used in disabled text.
    #[serde(rename = "muted.foreground")]
    pub muted_foreground: Option<SharedString>,
    /// Background color for Popover.
    #[serde(rename = "popover.background")]
    pub popover: Option<SharedString>,
    /// Text color for Popover.
    #[serde(rename = "popover.foreground")]
    pub popover_foreground: Option<SharedString>,
    /// Primary background color.
    #[serde(rename = "primary.background")]
    pub primary: Option<SharedString>,
    /// Active primary background color.
    #[serde(rename = "primary.active.background")]
    pub primary_active: Option<SharedString>,
    /// Primary text color.
    #[serde(rename = "primary.foreground")]
    pub primary_foreground: Option<SharedString>,
    /// Hover primary background color.
    #[serde(rename = "primary.hover.background")]
    pub primary_hover: Option<SharedString>,
    /// Progress bar background color.
    #[serde(rename = "progress.bar.background")]
    pub progress_bar: Option<SharedString>,
    /// Used for focus ring.
    #[serde(rename = "ring")]
    pub ring: Option<SharedString>,
    /// Scrollbar background color.
    #[serde(rename = "scrollbar.background")]
    pub scrollbar: Option<SharedString>,
    /// Scrollbar thumb background color.
    #[serde(rename = "scrollbar.thumb.background")]
    pub scrollbar_thumb: Option<SharedString>,
    /// Scrollbar thumb hover background color.
    #[serde(rename = "scrollbar.thumb.hover.background")]
    pub scrollbar_thumb_hover: Option<SharedString>,
    /// Secondary background color.
    #[serde(rename = "secondary.background")]
    pub secondary: Option<SharedString>,
    /// Active secondary background color.
    #[serde(rename = "secondary.active.background")]
    pub secondary_active: Option<SharedString>,
    /// Secondary text color, used for secondary Button text color or secondary text.
    #[serde(rename = "secondary.foreground")]
    pub secondary_foreground: Option<SharedString>,
    /// Hover secondary background color.
    #[serde(rename = "secondary.hover.background")]
    pub secondary_hover: Option<SharedString>,
    /// Input selection background color.
    #[serde(rename = "selection.background")]
    pub selection: Option<SharedString>,
    /// Sidebar background color.
    #[serde(rename = "sidebar.background")]
    pub sidebar: Option<SharedString>,
    /// Sidebar accent background color.
    #[serde(rename = "sidebar.accent.background")]
    pub sidebar_accent: Option<SharedString>,
    /// Sidebar accent text color.
    #[serde(rename = "sidebar.accent.foreground")]
    pub sidebar_accent_foreground: Option<SharedString>,
    /// Sidebar border color.
    #[serde(rename = "sidebar.border")]
    pub sidebar_border: Option<SharedString>,
    /// Sidebar text color.
    #[serde(rename = "sidebar.foreground")]
    pub sidebar_foreground: Option<SharedString>,
    /// Sidebar primary background color.
    #[serde(rename = "sidebar.primary.background")]
    pub sidebar_primary: Option<SharedString>,
    /// Sidebar primary text color.
    #[serde(rename = "sidebar.primary.foreground")]
    pub sidebar_primary_foreground: Option<SharedString>,
    /// Skeleton background color.
    #[serde(rename = "skeleton.background")]
    pub skeleton: Option<SharedString>,
    /// Slider bar background color.
    #[serde(rename = "slider.background")]
    pub slider_bar: Option<SharedString>,
    /// Slider thumb background color.
    #[serde(rename = "slider.thumb.background")]
    pub slider_thumb: Option<SharedString>,
    /// Success background color.
    #[serde(rename = "success.background")]
    pub success: Option<SharedString>,
    /// Success text color.
    #[serde(rename = "success.foreground")]
    pub success_foreground: Option<SharedString>,
    /// Success hover background color.
    #[serde(rename = "success.hover.background")]
    pub success_hover: Option<SharedString>,
    /// Success active background color.
    #[serde(rename = "success.active.background")]
    pub success_active: Option<SharedString>,
    /// Bullish color for candlestick charts (upward price movement).
    #[serde(rename = "bullish.background")]
    pub bullish: Option<SharedString>,
    /// Bearish color for candlestick charts (downward price movement).
    #[serde(rename = "bearish.background")]
    pub bearish: Option<SharedString>,
    /// Switch background color.
    #[serde(rename = "switch.background")]
    pub switch: Option<SharedString>,
    /// Switch thumb background color.
    #[serde(rename = "switch.thumb.background")]
    pub switch_thumb: Option<SharedString>,
    /// Tab background color.
    #[serde(rename = "tab.background")]
    pub tab: Option<SharedString>,
    /// Tab active background color.
    #[serde(rename = "tab.active.background")]
    pub tab_active: Option<SharedString>,
    /// Tab active text color.
    #[serde(rename = "tab.active.foreground")]
    pub tab_active_foreground: Option<SharedString>,
    /// TabBar background color.
    #[serde(rename = "tab_bar.background")]
    pub tab_bar: Option<SharedString>,
    /// TabBar segmented background color.
    #[serde(rename = "tab_bar.segmented.background")]
    pub tab_bar_segmented: Option<SharedString>,
    /// Tab text color.
    #[serde(rename = "tab.foreground")]
    pub tab_foreground: Option<SharedString>,
    /// Table background color.
    #[serde(rename = "table.background")]
    pub table: Option<SharedString>,
    /// Table active item background color.
    #[serde(rename = "table.active.background")]
    pub table_active: Option<SharedString>,
    /// Table active item border color.
    #[serde(rename = "table.active.border")]
    pub table_active_border: Option<SharedString>,
    /// Stripe background color for even TableRow.
    #[serde(rename = "table.even.background")]
    pub table_even: Option<SharedString>,
    /// Table head background color.
    #[serde(rename = "table.head.background")]
    pub table_head: Option<SharedString>,
    /// Table head text color.
    #[serde(rename = "table.head.foreground")]
    pub table_head_foreground: Option<SharedString>,
    /// Table item hover background color.
    #[serde(rename = "table.hover.background")]
    pub table_hover: Option<SharedString>,
    /// Table row border color.
    #[serde(rename = "table.row.border")]
    pub table_row_border: Option<SharedString>,
    /// TitleBar background color, use for Window title bar.
    #[serde(rename = "title_bar.background")]
    pub title_bar: Option<SharedString>,
    /// TitleBar border color.
    #[serde(rename = "title_bar.border")]
    pub title_bar_border: Option<SharedString>,
    /// Background color for Tiles.
    #[serde(rename = "tiles.background")]
    pub tiles: Option<SharedString>,
    /// Warning background color.
    #[serde(rename = "warning.background")]
    pub warning: Option<SharedString>,
    /// Warning active background color.
    #[serde(rename = "warning.active.background")]
    pub warning_active: Option<SharedString>,
    /// Warning hover background color.
    #[serde(rename = "warning.hover.background")]
    pub warning_hover: Option<SharedString>,
    /// Warning foreground color.
    #[serde(rename = "warning.foreground")]
    pub warning_foreground: Option<SharedString>,
    /// Overlay background color.
    #[serde(rename = "overlay")]
    pub overlay: Option<SharedString>,
    /// Window border color.
    ///
    /// # Platform specific:
    ///
    /// This is only works on Linux, other platforms we can't change the window border color.
    #[serde(rename = "window.border")]
    pub window_border: Option<SharedString>,

    /// Base blue color.
    #[serde(rename = "base.blue")]
    blue: Option<String>,
    /// Base light blue color.
    #[serde(rename = "base.blue.light")]
    blue_light: Option<String>,
    /// Base cyan color.
    #[serde(rename = "base.cyan")]
    cyan: Option<String>,
    /// Base light cyan color.
    #[serde(rename = "base.cyan.light")]
    cyan_light: Option<String>,
    /// Base green color.
    #[serde(rename = "base.green")]
    green: Option<String>,
    /// Base light green color.
    #[serde(rename = "base.green.light")]
    green_light: Option<String>,
    /// Base magenta color.
    #[serde(rename = "base.magenta")]
    magenta: Option<String>,
    #[serde(rename = "base.magenta.light")]
    magenta_light: Option<String>,
    /// Base red color.
    #[serde(rename = "base.red")]
    red: Option<String>,
    /// Base light red color.
    #[serde(rename = "base.red.light")]
    red_light: Option<String>,
    /// Base yellow color.
    #[serde(rename = "base.yellow")]
    yellow: Option<String>,
    /// Base light yellow color.
    #[serde(rename = "base.yellow.light")]
    yellow_light: Option<String>,
}

impl ThemeColor {
    /// Create a new `ThemeColor` from a `ThemeConfig`.
    pub(crate) fn apply_config(&mut self, config: &ThemeConfig, default_theme: &ThemeColor) {
        let colors = config.colors.clone();

        macro_rules! apply_color {
            ($config_field:ident) => {
                if let Some(value) = colors.$config_field {
                    if let Ok(color) = try_parse_color(&value) {
                        self.$config_field = color;
                    } else {
                        self.$config_field = default_theme.$config_field;
                    }
                } else {
                    self.$config_field = default_theme.$config_field;
                }
            };
            // With fallback
            ($config_field:ident, fallback = $fallback:expr) => {
                if let Some(value) = colors.$config_field {
                    if let Ok(color) = try_parse_color(&value) {
                        self.$config_field = color;
                    }
                } else {
                    self.$config_field = $fallback;
                }
            };
        }

        apply_color!(background);

        // Base colors for fallback
        apply_color!(red);
        apply_color!(
            red_light,
            fallback = self.background.blend(self.red.opacity(0.8))
        );
        apply_color!(green);
        apply_color!(
            green_light,
            fallback = self.background.blend(self.green.opacity(0.8))
        );
        apply_color!(blue);
        apply_color!(
            blue_light,
            fallback = self.background.blend(self.blue.opacity(0.8))
        );
        apply_color!(magenta);
        apply_color!(
            magenta_light,
            fallback = self.background.blend(self.magenta.opacity(0.8))
        );
        apply_color!(yellow);
        apply_color!(
            yellow_light,
            fallback = self.background.blend(self.yellow.opacity(0.8))
        );
        apply_color!(cyan);
        apply_color!(
            cyan_light,
            fallback = self.background.blend(self.cyan.opacity(0.8))
        );

        apply_color!(border);
        apply_color!(foreground);
        apply_color!(muted);
        apply_color!(
            muted_foreground,
            fallback = self.muted.blend(self.foreground.opacity(0.7))
        );

        // Button colors
        let active_darken = if config.mode.is_dark() { 0.2 } else { 0.1 };
        let hover_opacity = 0.9;
        apply_color!(primary);
        apply_color!(primary_foreground, fallback = self.foreground);
        apply_color!(
            primary_hover,
            fallback = self.background.blend(self.primary.opacity(hover_opacity))
        );
        apply_color!(
            primary_active,
            fallback = self.primary.darken(active_darken)
        );
        apply_color!(secondary);
        apply_color!(secondary_foreground, fallback = self.foreground);
        apply_color!(
            secondary_hover,
            fallback = self.background.blend(self.secondary.opacity(hover_opacity))
        );
        apply_color!(
            secondary_active,
            fallback = self.secondary.darken(active_darken)
        );
        apply_color!(success, fallback = self.green);
        apply_color!(success_foreground, fallback = self.primary_foreground);
        apply_color!(
            success_hover,
            fallback = self.background.blend(self.success.opacity(hover_opacity))
        );
        apply_color!(
            success_active,
            fallback = self.success.darken(active_darken)
        );
        apply_color!(bullish, fallback = self.green);
        apply_color!(bearish, fallback = self.red);
        apply_color!(info, fallback = self.cyan);
        apply_color!(info_foreground, fallback = self.primary_foreground);
        apply_color!(
            info_hover,
            fallback = self.background.blend(self.info.opacity(hover_opacity))
        );
        apply_color!(info_active, fallback = self.info.darken(active_darken));
        apply_color!(warning, fallback = self.yellow);
        apply_color!(warning_foreground, fallback = self.primary_foreground);
        apply_color!(
            warning_hover,
            fallback = self.background.blend(self.warning.opacity(0.9))
        );
        apply_color!(
            warning_active,
            fallback = self.background.blend(self.warning.darken(active_darken))
        );

        // Other colors
        apply_color!(accent, fallback = self.secondary);
        apply_color!(accent_foreground, fallback = self.foreground);
        apply_color!(accordion, fallback = self.background);
        apply_color!(accordion_hover, fallback = self.accent.opacity(0.8));
        apply_color!(
            group_box,
            fallback = self
                .background
                .blend(
                    self.secondary
                        .opacity(if config.mode.is_dark() { 0.3 } else { 0.4 })
                )
        );
        apply_color!(group_box_foreground, fallback = self.foreground);
        apply_color!(
            group_box_title_foreground,
            fallback = self.group_box_foreground
        );
        apply_color!(caret, fallback = self.primary);
        apply_color!(chart_1, fallback = self.blue.lighten(0.4));
        apply_color!(chart_2, fallback = self.blue.lighten(0.2));
        apply_color!(chart_3, fallback = self.blue);
        apply_color!(chart_4, fallback = self.blue.darken(0.2));
        apply_color!(chart_5, fallback = self.blue.darken(0.4));
        apply_color!(danger, fallback = self.red);
        apply_color!(danger_active, fallback = self.danger.darken(active_darken));
        apply_color!(danger_foreground, fallback = self.primary_foreground);
        apply_color!(
            danger_hover,
            fallback = self.background.blend(self.danger.opacity(0.9))
        );
        apply_color!(
            description_list_label,
            fallback = self.background.blend(self.border.opacity(0.2))
        );
        apply_color!(
            description_list_label_foreground,
            fallback = self.muted_foreground
        );
        apply_color!(drag_border, fallback = self.primary.opacity(0.65));
        apply_color!(drop_target, fallback = self.primary.opacity(0.2));
        apply_color!(input, fallback = self.border);
        apply_color!(link, fallback = self.primary);
        apply_color!(link_active, fallback = self.link);
        apply_color!(link_hover, fallback = self.link);
        apply_color!(list, fallback = self.background);
        apply_color!(
            list_active,
            fallback = self.background.blend(self.primary.opacity(0.1))
        );
        apply_color!(
            list_active_border,
            fallback = self.background.blend(self.primary.opacity(0.6))
        );
        apply_color!(list_even, fallback = self.list);
        apply_color!(list_head, fallback = self.list);
        apply_color!(list_hover, fallback = self.secondary_hover);
        apply_color!(popover, fallback = self.background);
        apply_color!(popover_foreground, fallback = self.foreground);
        apply_color!(progress_bar, fallback = self.primary);
        apply_color!(ring, fallback = self.blue);
        apply_color!(scrollbar, fallback = self.background);
        apply_color!(scrollbar_thumb, fallback = self.accent);
        apply_color!(scrollbar_thumb_hover, fallback = self.scrollbar_thumb);
        apply_color!(selection, fallback = self.primary);
        apply_color!(sidebar, fallback = self.background);
        apply_color!(sidebar_accent, fallback = self.accent);
        apply_color!(sidebar_accent_foreground, fallback = self.accent_foreground);
        apply_color!(sidebar_border, fallback = self.border);
        apply_color!(sidebar_foreground, fallback = self.foreground);
        apply_color!(sidebar_primary, fallback = self.primary);
        apply_color!(
            sidebar_primary_foreground,
            fallback = self.primary_foreground
        );
        apply_color!(skeleton, fallback = self.secondary);
        apply_color!(slider_bar, fallback = self.primary);
        apply_color!(slider_thumb, fallback = self.primary_foreground);
        apply_color!(switch, fallback = self.secondary);
        apply_color!(switch_thumb, fallback = self.background);
        apply_color!(tab, fallback = self.background);
        apply_color!(tab_active, fallback = self.background);
        apply_color!(tab_active_foreground, fallback = self.foreground);
        apply_color!(tab_bar, fallback = self.background);
        apply_color!(tab_bar_segmented, fallback = self.secondary);
        apply_color!(tab_foreground, fallback = self.foreground);
        apply_color!(table, fallback = self.list);
        apply_color!(table_active, fallback = self.list_active);
        apply_color!(table_active_border, fallback = self.list_active_border);
        apply_color!(table_even, fallback = self.list_even);
        apply_color!(table_head, fallback = self.list_head);
        apply_color!(table_head_foreground, fallback = self.muted_foreground);
        apply_color!(table_hover, fallback = self.list_hover);
        apply_color!(table_row_border, fallback = self.border);
        apply_color!(title_bar, fallback = self.background);
        apply_color!(title_bar_border, fallback = self.border);
        apply_color!(tiles, fallback = self.background);
        apply_color!(overlay);
        apply_color!(window_border, fallback = self.border);

        // TODO: Apply default fallback colors to highlight.

        // Ensure opacity for list_active, table_active
        self.list_active = self.list_active.alpha(self.list_active.a.min(0.2));
        self.table_active = self.table_active.alpha(self.table_active.a.min(0.2));
        self.selection = self.selection.alpha(self.selection.a.min(0.3));
    }
}

impl Theme {
    /// Apply the given theme configuration to the current theme.
    pub fn apply_config(&mut self, config: &Rc<ThemeConfig>) {
        if config.mode.is_dark() {
            self.dark_theme = config.clone();
        } else {
            self.light_theme = config.clone();
        }
        if let Some(style) = &config.highlight {
            let highlight_theme = Arc::new(HighlightTheme {
                name: config.name.to_string(),
                appearance: config.mode,
                style: style.clone(),
            });
            self.highlight_theme = highlight_theme.clone();
        }

        let default_theme = if config.mode.is_dark() {
            Self::from(ThemeColor::dark().as_ref())
        } else {
            Self::from(ThemeColor::light().as_ref())
        };

        if let Some(font_size) = config.font_size {
            self.font_size = px(font_size);
        } else {
            self.font_size = default_theme.font_size;
        }
        if let Some(font_family) = &config.font_family {
            self.font_family = font_family.clone();
        } else {
            self.font_family = default_theme.font_family.clone();
        }
        if let Some(mono_font_family) = &config.mono_font_family {
            self.mono_font_family = mono_font_family.clone();
        } else {
            self.mono_font_family = default_theme.mono_font_family.clone();
        }
        if let Some(mono_font_size) = config.mono_font_size {
            self.mono_font_size = px(mono_font_size);
        } else {
            self.mono_font_size = default_theme.mono_font_size;
        }
        if let Some(radius) = config.radius {
            self.radius = px(radius as f32);
        } else {
            self.radius = default_theme.radius;
        }
        if let Some(radius_lg) = config.radius_lg {
            self.radius_lg = px(radius_lg as f32);
        } else {
            self.radius_lg = default_theme.radius_lg;
        }
        if let Some(shadow) = config.shadow {
            self.shadow = shadow;
        } else {
            self.shadow = default_theme.shadow;
        }

        self.colors.apply_config(&config, &default_theme.colors);
        self.mode = config.mode;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn str_val(opt: &Option<SharedString>) -> Option<&str> {
        opt.as_ref().map(|s| s.as_ref())
    }

    // A minimal Zed-format theme set to validate parsing.
    const ZED_THEME_JSON: &str = r##"
    {
        "name": "Test Zed",
        "author": "Test",
        "themes": [
            {
                "name": "Test Dark",
                "appearance": "dark",
                "style": {
                    "background": "#1a1b26",
                    "text": "#c0caf5",
                    "text.muted": "#565f89",
                    "border": "#292e42",
                    "border.focused": "#7aa2f7",
                    "tab_bar.background": "#161720",
                    "tab.active_background": "#1a1b26",
                    "tab.inactive_background": "#292e42",
                    "tab.text": "#565f89",
                    "tab.active_text": "#c0caf5",
                    "title_bar.background": "#161720",
                    "elevated_surface.background": "#1a1b26",
                    "element.background": "#292e42",
                    "element.hover": "#31374f",
                    "element.selected": "#7aa2f722",
                    "ghost_element.hover": "#7aa2f711",
                    "icon.accent": "#7aa2f7",
                    "scrollbar.track.background": "#1a1b2600",
                    "scrollbar.thumb.background": "#414868",
                    "scrollbar.thumb.hover_background": "#7aa2f7",
                    "drop_target.background": "#7aa2f722",
                    "link_text.hover": "#7aa2f7",
                    "error": "#f7768e",
                    "info": "#7aa2f7",
                    "success": "#9ece6a",
                    "warning": "#e0af68",
                    "syntax": {
                        "keyword": { "color": "#f7768e" },
                        "string": { "color": "#9ece6a" },
                        "function": { "color": "#7aa2f7" }
                    }
                }
            },
            {
                "name": "Test Light",
                "appearance": "light",
                "style": {
                    "background": "#ffffff",
                    "text": "#000000",
                    "border": "#cccccc"
                }
            }
        ]
    }
    "##;

    // A minimal internal-format theme set to validate that existing format still works.
    const INTERNAL_THEME_JSON: &str = r##"
    {
        "name": "Test Internal",
        "themes": [
            {
                "name": "Test Internal Dark",
                "mode": "dark",
                "colors": {
                    "background": "#1a1b26",
                    "foreground": "#c0caf5",
                    "border": "#292e42"
                }
            }
        ]
    }
    "##;

    #[test]
    fn test_zed_format_parses_appearance_as_mode() {
        let theme_set: ThemeSet =
            serde_json::from_str(ZED_THEME_JSON).expect("Zed format should parse");
        assert_eq!(theme_set.themes.len(), 2);

        let dark = &theme_set.themes[0];
        assert_eq!(dark.name.as_ref(), "Test Dark");
        assert_eq!(
            dark.mode,
            ThemeMode::Dark,
            "appearance=dark must map to ThemeMode::Dark"
        );

        let light = &theme_set.themes[1];
        assert_eq!(light.name.as_ref(), "Test Light");
        assert_eq!(
            light.mode,
            ThemeMode::Light,
            "appearance=light must map to ThemeMode::Light"
        );
    }

    #[test]
    fn test_zed_format_parses_style_colors() {
        let theme_set: ThemeSet =
            serde_json::from_str(ZED_THEME_JSON).expect("Zed format should parse");
        let dark = &theme_set.themes[0];
        let colors = &dark.colors;

        assert!(colors.background.is_some(), "background should be parsed");
        assert_eq!(str_val(&colors.background), Some("#1a1b26"));
        assert_eq!(
            str_val(&colors.foreground),
            Some("#c0caf5"),
            "text → foreground"
        );
        assert_eq!(
            str_val(&colors.muted_foreground),
            Some("#565f89"),
            "text.muted → muted_foreground"
        );
        assert_eq!(str_val(&colors.border), Some("#292e42"));
        assert_eq!(
            str_val(&colors.ring),
            Some("#7aa2f7"),
            "border.focused → ring"
        );
        assert_eq!(
            str_val(&colors.tab_bar),
            Some("#161720"),
            "tab_bar.background"
        );
        assert_eq!(
            str_val(&colors.tab_active),
            Some("#1a1b26"),
            "tab.active_background"
        );
        assert_eq!(
            str_val(&colors.tab),
            Some("#292e42"),
            "tab.inactive_background"
        );
        assert_eq!(str_val(&colors.tab_foreground), Some("#565f89"), "tab.text");
        assert_eq!(
            str_val(&colors.tab_active_foreground),
            Some("#c0caf5"),
            "tab.active_text"
        );
        assert_eq!(
            str_val(&colors.title_bar),
            Some("#161720"),
            "title_bar.background"
        );
        assert_eq!(
            str_val(&colors.accent),
            Some("#7aa2f7"),
            "icon.accent → accent"
        );
        assert_eq!(
            str_val(&colors.secondary),
            Some("#292e42"),
            "element.background → secondary"
        );
        assert_eq!(
            str_val(&colors.secondary_hover),
            Some("#31374f"),
            "element.hover → secondary_hover"
        );
        assert_eq!(
            str_val(&colors.popover),
            Some("#1a1b26"),
            "elevated_surface.background → popover"
        );
        assert_eq!(
            str_val(&colors.scrollbar_thumb),
            Some("#414868"),
            "scrollbar.thumb.background"
        );
        assert_eq!(
            str_val(&colors.scrollbar_thumb_hover),
            Some("#7aa2f7"),
            "scrollbar.thumb.hover_background"
        );
        assert_eq!(str_val(&colors.danger), Some("#f7768e"), "error → danger");
        assert_eq!(str_val(&colors.info), Some("#7aa2f7"), "info");
        assert_eq!(str_val(&colors.success), Some("#9ece6a"), "success");
        assert_eq!(str_val(&colors.warning), Some("#e0af68"), "warning");
    }

    #[test]
    fn test_zed_format_parses_syntax_highlight() {
        let theme_set: ThemeSet =
            serde_json::from_str(ZED_THEME_JSON).expect("Zed format should parse");
        let dark = &theme_set.themes[0];
        let highlight = dark.highlight.as_ref().expect("highlight should be Some");
        assert!(
            highlight.syntax.keyword.is_some(),
            "syntax.keyword should be parsed"
        );
        assert!(
            highlight.syntax.string.is_some(),
            "syntax.string should be parsed"
        );
        assert!(
            highlight.syntax.function.is_some(),
            "syntax.function should be parsed"
        );
    }

    #[test]
    fn test_internal_format_still_works() {
        let theme_set: ThemeSet =
            serde_json::from_str(INTERNAL_THEME_JSON).expect("internal format should parse");
        assert_eq!(theme_set.themes.len(), 1);

        let dark = &theme_set.themes[0];
        assert_eq!(dark.name.as_ref(), "Test Internal Dark");
        assert_eq!(
            dark.mode,
            ThemeMode::Dark,
            "mode field should be read directly"
        );
        assert_eq!(str_val(&dark.colors.background), Some("#1a1b26"));
        assert_eq!(str_val(&dark.colors.foreground), Some("#c0caf5"));
    }

    #[test]
    fn test_default_theme_json_parses() {
        const DEFAULT_THEME: &str = include_str!("./default-theme.json");
        let theme_set: ThemeSet =
            serde_json::from_str(DEFAULT_THEME).expect("default-theme.json should parse");
        assert!(
            !theme_set.themes.is_empty(),
            "default-theme.json must contain at least one theme"
        );
        for theme in &theme_set.themes {
            assert!(
                !theme.name.is_empty(),
                "default theme entry has an empty name"
            );
            assert!(
                theme.colors.background.is_some(),
                "default theme '{}' is missing a background color",
                theme.name
            );
        }
    }
}
