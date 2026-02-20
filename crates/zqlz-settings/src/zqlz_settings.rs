//! ZQLZ Settings System
//!
//! Provides application settings with persistence, including:
//! - Theme settings (light/dark mode, theme selection)
//! - Font settings (UI font, editor font, sizes)
//! - Editor settings (tab size, line numbers, etc.)
//! - Connection defaults
//! - Layout persistence

use anyhow::{Context, Result};
use gpui::{px, App, Global, SharedString};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use zqlz_ui::widgets::{Theme, ThemeMode, ThemeRegistry};

mod layout;
mod settings_file;
pub mod widgets;

pub use layout::*;
pub use settings_file::*;
pub use widgets::{SettingsPanel, SettingsPanelEvent};

pub fn init(cx: &mut App) {
    init_with_bundled_themes(cx, |_| {});
}

pub fn init_with_bundled_themes<F>(cx: &mut App, load_bundled_themes: F)
where
    F: Fn(&mut App) + 'static + Clone,
{
    // Ensure config directories exist
    if let Err(err) = ensure_directories() {
        tracing::warn!("Failed to create config directories: {}", err);
    }

    // Load bundled themes first (before watch_dir which may trigger reload)
    load_bundled_themes.clone()(cx);

    // Setup theme registry to watch the user themes directory for custom themes
    if let Ok(themes_path) = themes_dir() {
        if let Err(err) = ThemeRegistry::watch_dir(themes_path, cx, move |cx| {
            // Re-load bundled themes after reload (since reload() clears all themes)
            load_bundled_themes(cx);

            // Re-apply settings when themes are reloaded
            if cx.has_global::<ZqlzSettings>() {
                let settings = ZqlzSettings::global(cx).clone();
                settings.appearance.apply_with_fonts(&settings.fonts, cx);
            }
        }) {
            tracing::warn!("Failed to watch themes directory: {}", err);
        }
    }

    let settings = ZqlzSettings::load().unwrap_or_default();
    cx.set_global(settings.clone());

    // Apply settings - this uses apply_with_fonts internally to ensure fonts are preserved
    settings.apply(cx);

    cx.observe_global::<ZqlzSettings>(|cx| {
        let settings = ZqlzSettings::global(cx).clone();
        if let Err(err) = settings.save() {
            tracing::error!("Failed to save settings: {}", err);
        }
    })
    .detach();
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ZqlzSettings {
    pub appearance: AppearanceSettings,
    pub fonts: FontSettings,
    pub editor: EditorSettings,
    pub connections: ConnectionSettings,
}

impl Global for ZqlzSettings {}

impl ZqlzSettings {
    pub fn global(cx: &App) -> &Self {
        cx.global::<Self>()
    }

    pub fn global_mut(cx: &mut App) -> &mut Self {
        cx.global_mut::<Self>()
    }

    pub fn load() -> Result<Self> {
        let path = Self::settings_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read settings from {:?}", path))?;
        serde_json::from_str(&content).with_context(|| "Failed to parse settings JSON")
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::settings_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    pub fn settings_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Could not find config directory")?;
        Ok(config_dir.join("zqlz").join("settings.json"))
    }

    pub fn apply(&self, cx: &mut App) {
        // Use apply_with_fonts to ensure fonts are preserved after theme change
        self.appearance.apply_with_fonts(&self.fonts, cx);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppearanceSettings {
    pub theme_mode: ThemeModePreference,
    pub light_theme: SharedString,
    pub dark_theme: SharedString,
    pub show_scrollbars: ScrollbarVisibility,
}

impl Default for AppearanceSettings {
    fn default() -> Self {
        Self {
            theme_mode: ThemeModePreference::System,
            light_theme: "Catppuccin Latte".into(),
            dark_theme: "Catppuccin Mocha".into(),
            show_scrollbars: ScrollbarVisibility::Auto,
        }
    }
}

impl AppearanceSettings {
    /// Apply appearance settings (mode, theme selection, scrollbar visibility).
    ///
    /// Looks up the configured light/dark theme names in `ThemeRegistry`, assigns
    /// the found configs to `Theme`, then calls `Theme::change()` which applies
    /// the active config and triggers a repaint.
    pub fn apply(&self, cx: &mut App) {
        let mode = match self.theme_mode {
            ThemeModePreference::Light => ThemeMode::Light,
            ThemeModePreference::Dark => ThemeMode::Dark,
            ThemeModePreference::System => {
                if cx.window_appearance().is_dark() {
                    ThemeMode::Dark
                } else {
                    ThemeMode::Light
                }
            }
        };

        // Resolve theme names and update the Theme global before calling change(),
        // so the correct configs are active when apply_config() runs inside it.
        if let Some(light) = ThemeRegistry::global(cx)
            .themes()
            .get(&self.light_theme)
            .cloned()
        {
            Theme::global_mut(cx).light_theme = light;
        }
        if let Some(dark) = ThemeRegistry::global(cx)
            .themes()
            .get(&self.dark_theme)
            .cloned()
        {
            Theme::global_mut(cx).dark_theme = dark;
        }

        Theme::change(mode, None, cx);

        // Re-apply scrollbar preference after Theme::change() in case apply_config reset it.
        Theme::global_mut(cx).scrollbar_show = self.show_scrollbars.into();

        // Theme::change() only calls window.refresh() when a Window is provided.
        // When called without one (as here, from the settings panel), we must
        // explicitly ask every open window to redraw so the new theme is visible.
        cx.refresh_windows();
    }

    /// Apply appearance settings and then reapply font settings.
    pub fn apply_with_fonts(&self, fonts: &FontSettings, cx: &mut App) {
        self.apply(cx);
        fonts.apply(cx);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThemeModePreference {
    Light,
    Dark,
    #[default]
    System,
}

impl ThemeModePreference {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Light => "Light",
            Self::Dark => "Dark",
            Self::System => "System",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::System, Self::Light, Self::Dark]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScrollbarVisibility {
    Always,
    #[default]
    Auto,
    Never,
}

impl ScrollbarVisibility {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Always => "Always",
            Self::Auto => "Auto",
            Self::Never => "Never",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::Auto, Self::Always, Self::Never]
    }
}

impl From<ScrollbarVisibility> for zqlz_ui::widgets::scroll::ScrollbarShow {
    fn from(visibility: ScrollbarVisibility) -> Self {
        match visibility {
            ScrollbarVisibility::Always => zqlz_ui::widgets::scroll::ScrollbarShow::Always,
            ScrollbarVisibility::Auto => zqlz_ui::widgets::scroll::ScrollbarShow::Scrolling,
            ScrollbarVisibility::Never => zqlz_ui::widgets::scroll::ScrollbarShow::Hover,
        }
    }
}

/// SQL dialect for syntax highlighting and parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialect {
    /// Standard SQL (SQL-92 compatible)
    Standard,
    /// PostgreSQL dialect
    Postgres,
    /// MySQL dialect
    Mysql,
    /// SQLite dialect
    Sqlite,
    /// MariaDB dialect
    Mariadb,
}

impl Default for SqlDialect {
    fn default() -> Self {
        Self::Standard
    }
}

impl SqlDialect {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Standard => "Standard SQL",
            Self::Postgres => "PostgreSQL",
            Self::Mysql => "MySQL",
            Self::Sqlite => "SQLite",
            Self::Mariadb => "MariaDB",
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::Standard,
            Self::Postgres,
            Self::Mysql,
            Self::Sqlite,
            Self::Mariadb,
        ]
    }
}

/// Inline suggestion provider
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InlineSuggestionProvider {
    /// Use LSP completions only
    LspOnly,
    /// Use AI completions only
    AiOnly,
    /// Use both LSP and AI, show LSP first
    Both,
}

impl Default for InlineSuggestionProvider {
    fn default() -> Self {
        Self::LspOnly
    }
}

impl InlineSuggestionProvider {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::LspOnly => "LSP Only",
            Self::AiOnly => "AI Only",
            Self::Both => "Both (LSP first)",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::LspOnly, Self::AiOnly, Self::Both]
    }
}

/// AI provider for inline suggestions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiProvider {
    /// OpenAI API
    OpenAi,
    /// Anthropic API
    Anthropic,
    /// Local/model running locally
    Local,
    /// No AI provider (disabled)
    None,
}

impl Default for AiProvider {
    fn default() -> Self {
        Self::OpenAi
    }
}

impl AiProvider {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::OpenAi => "OpenAI",
            Self::Anthropic => "Anthropic",
            Self::Local => "Local",
            Self::None => "None",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::OpenAi, Self::Anthropic, Self::Local, Self::None]
    }

    /// Returns the default model for this provider.
    pub fn default_model(&self) -> &'static str {
        match self {
            Self::OpenAi => "gpt-4",
            Self::Anthropic => "claude-3-opus-20240229",
            Self::Local => "codellama-7b",
            Self::None => "",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FontSettings {
    pub ui_font_family: SharedString,
    pub ui_font_size: f32,
    pub ui_font_weight: u16,
    pub editor_font_family: SharedString,
    pub editor_font_size: f32,
    pub editor_font_weight: u16,
    pub mono_font_family: SharedString,
    pub mono_font_size: f32,
}

impl Default for FontSettings {
    fn default() -> Self {
        Self {
            ui_font_family: "Inter".into(),
            ui_font_size: 16.0,
            ui_font_weight: 400,
            editor_font_family: "JetBrains Mono".into(),
            editor_font_size: 15.0,
            editor_font_weight: 400,
            mono_font_family: "JetBrains Mono".into(),
            mono_font_size: 15.0,
        }
    }
}

impl FontSettings {
    pub fn apply(&self, cx: &mut App) {
        let theme = Theme::global_mut(cx);
        theme.font_family = self.ui_font_family.clone();
        theme.font_size = px(self.ui_font_size);
        theme.font_weight = self.ui_font_weight;
        theme.mono_font_family = self.mono_font_family.clone();
        theme.mono_font_size = px(self.mono_font_size);
        theme.mono_font_weight = self.editor_font_weight;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorBlink {
    On,
    Off,
    System,
}

impl Default for CursorBlink {
    fn default() -> Self {
        Self::On
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorShape {
    Block,
    Line,
    Underline,
}

impl Default for CursorShape {
    fn default() -> Self {
        Self::Line
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrollBeyondLastLine {
    Disabled,
    Enabled,
    HorizontalScrollbar,
}

impl Default for ScrollBeyondLastLine {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchWrap {
    Disabled,
    Enabled,
    NoWrap,
}

impl Default for SearchWrap {
    fn default() -> Self {
        Self::Enabled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EditorSettings {
    // Basic editing
    pub tab_size: u32,
    pub insert_spaces: bool,
    pub show_line_numbers: bool,
    pub word_wrap: bool,
    pub highlight_current_line: bool,
    pub show_inline_diagnostics: bool,
    pub auto_indent: bool,
    pub bracket_matching: bool,
    pub vim_mode_enabled: bool,
    pub highlight_enabled: bool,
    pub sql_dialect: SqlDialect,
    // Cursor and selection
    pub cursor_blink: CursorBlink,
    pub cursor_shape: CursorShape,
    pub selection_highlight: bool,
    pub rounded_selection: bool,
    // Line numbers
    pub relative_line_numbers: bool,
    // Scroll behavior
    pub scroll_beyond_last_line: ScrollBeyondLastLine,
    pub vertical_scroll_margin: u32,
    pub horizontal_scroll_margin: u32,
    pub scroll_sensitivity: f32,
    pub autoscroll_on_clicks: bool,
    // Search behavior
    pub search_wrap: SearchWrap,
    pub use_smartcase_search: bool,
    // LSP capability settings
    pub lsp_enabled: bool,
    pub lsp_completions_enabled: bool,
    pub lsp_hover_enabled: bool,
    pub lsp_diagnostics_enabled: bool,
    pub lsp_code_actions_enabled: bool,
    pub lsp_rename_enabled: bool,
    // Inline suggestion settings
    pub inline_suggestions_enabled: bool,
    pub inline_suggestions_provider: InlineSuggestionProvider,
    pub inline_suggestions_delay_ms: u32,
    // AI completion settings
    pub ai_provider: AiProvider,
    pub ai_api_key: Option<SharedString>,
    pub ai_model: SharedString,
    pub ai_temperature: f32,
    // Gutter settings
    pub show_gutter_diagnostics: bool,
    pub show_folding: bool,
}

impl Default for EditorSettings {
    fn default() -> Self {
        Self {
            tab_size: 4,
            insert_spaces: true,
            show_line_numbers: true,
            word_wrap: false,
            highlight_current_line: true,
            show_inline_diagnostics: true,
            auto_indent: true,
            bracket_matching: true,
            vim_mode_enabled: false,
            highlight_enabled: true,
            sql_dialect: SqlDialect::Standard,
            // Cursor and selection
            cursor_blink: CursorBlink::On,
            cursor_shape: CursorShape::Line,
            selection_highlight: true,
            rounded_selection: true,
            // Line numbers
            relative_line_numbers: false,
            // Scroll behavior
            scroll_beyond_last_line: ScrollBeyondLastLine::Disabled,
            vertical_scroll_margin: 3,
            horizontal_scroll_margin: 3,
            scroll_sensitivity: 1.0,
            autoscroll_on_clicks: true,
            // Search behavior
            search_wrap: SearchWrap::Enabled,
            use_smartcase_search: true,
            // LSP settings - all enabled by default for full IDE experience
            lsp_enabled: true,
            lsp_completions_enabled: true,
            lsp_hover_enabled: true,
            lsp_diagnostics_enabled: true,
            lsp_code_actions_enabled: true,
            lsp_rename_enabled: true,
            // Inline suggestion settings
            inline_suggestions_enabled: true,
            inline_suggestions_provider: InlineSuggestionProvider::LspOnly,
            inline_suggestions_delay_ms: 200,
            // AI completion settings
            ai_provider: AiProvider::OpenAi,
            ai_api_key: None,
            ai_model: AiProvider::OpenAi.default_model().into(),
            ai_temperature: 0.7,
            // Gutter settings
            show_gutter_diagnostics: true,
            show_folding: true,
        }
    }
}

/// Pagination display mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PaginationMode {
    /// Traditional page-based pagination with page numbers
    #[default]
    PageBased,
    /// Infinite scroll - loads more data as user scrolls
    InfiniteScroll,
}

impl PaginationMode {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::PageBased => "Page Based",
            Self::InfiniteScroll => "Infinite Scroll",
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::PageBased, Self::InfiniteScroll]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionSettings {
    pub query_timeout_seconds: u64,
    pub max_rows_per_query: usize,
    pub auto_commit: bool,
    pub fetch_schema_on_connect: bool,
    /// Pagination mode: page-based or infinite scroll
    pub pagination_mode: PaginationMode,
    /// Available page size options for the dropdown
    pub available_page_sizes: Vec<usize>,
    /// Whether to show total row count (requires additional COUNT query)
    pub show_total_row_count: bool,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            query_timeout_seconds: 30,
            max_rows_per_query: 1000,
            auto_commit: true,
            fetch_schema_on_connect: true,
            pagination_mode: PaginationMode::PageBased,
            available_page_sizes: vec![100, 500, 1000, 5000, 10000],
            show_total_row_count: true,
        }
    }
}

trait WindowAppearanceExt {
    fn is_dark(&self) -> bool;
}

impl WindowAppearanceExt for gpui::WindowAppearance {
    fn is_dark(&self) -> bool {
        matches!(
            self,
            gpui::WindowAppearance::Dark | gpui::WindowAppearance::VibrantDark
        )
    }
}

/// EditorConfig provides a direct configuration interface for Zed editors.
///
/// This struct allows Zed editors to query settings directly without going through
/// Zed's SettingsStore. ZQLZ settings remain the source of truth, and this struct
/// provides a simplified API for editor consumption.
///
/// # Usage
///
/// ```ignore
/// use zqlz_settings::EditorConfig;
///
/// fn configure_editor(cx: &App) -> EditorConfig {
///     EditorConfig::new(cx)
/// }
/// ```
#[derive(Clone)]
pub struct EditorConfig {
    editor: EditorSettings,
}

impl EditorConfig {
    /// Creates a new EditorConfig from the global ZQLZ settings.
    pub fn new(cx: &App) -> Self {
        let editor = ZqlzSettings::global(cx).editor.clone();
        Self { editor }
    }

    /// Creates a new EditorConfig with the given EditorSettings.
    pub fn from_settings(editor: EditorSettings) -> Self {
        Self { editor }
    }

    /// Returns the tab size for indentation.
    pub fn tab_size(&self) -> u32 {
        self.editor.tab_size
    }

    /// Returns whether to insert spaces instead of tabs.
    pub fn insert_spaces(&self) -> bool {
        self.editor.insert_spaces
    }

    /// Returns whether to show line numbers in the gutter.
    pub fn show_line_numbers(&self) -> bool {
        self.editor.show_line_numbers
    }

    /// Returns whether to wrap lines at the viewport edge.
    pub fn word_wrap(&self) -> bool {
        self.editor.word_wrap
    }

    /// Returns whether to highlight the current line.
    pub fn highlight_current_line(&self) -> bool {
        self.editor.highlight_current_line
    }

    /// Returns whether to show inline diagnostics.
    pub fn show_inline_diagnostics(&self) -> bool {
        self.editor.show_inline_diagnostics
    }

    /// Returns whether to automatically indent new lines.
    pub fn auto_indent(&self) -> bool {
        self.editor.auto_indent
    }

    /// Returns whether to highlight matching brackets.
    pub fn bracket_matching(&self) -> bool {
        self.editor.bracket_matching
    }

    /// Returns whether vim mode is enabled.
    pub fn vim_mode_enabled(&self) -> bool {
        self.editor.vim_mode_enabled
    }

    /// Returns whether syntax highlighting is enabled.
    pub fn highlight_enabled(&self) -> bool {
        self.editor.highlight_enabled
    }

    /// Returns the cursor blink setting.
    pub fn cursor_blink(&self) -> CursorBlink {
        self.editor.cursor_blink
    }

    /// Returns the cursor shape setting.
    pub fn cursor_shape(&self) -> CursorShape {
        self.editor.cursor_shape
    }

    /// Returns whether to highlight selections in other parts of the document.
    pub fn selection_highlight(&self) -> bool {
        self.editor.selection_highlight
    }

    /// Returns whether selections should be rounded.
    pub fn rounded_selection(&self) -> bool {
        self.editor.rounded_selection
    }

    /// Returns whether to show relative line numbers.
    pub fn relative_line_numbers(&self) -> bool {
        self.editor.relative_line_numbers
    }

    /// Returns the scroll behavior past the last line.
    pub fn scroll_beyond_last_line(&self) -> ScrollBeyondLastLine {
        self.editor.scroll_beyond_last_line
    }

    /// Returns the vertical scroll margin (number of lines to keep above/below cursor).
    pub fn vertical_scroll_margin(&self) -> u32 {
        self.editor.vertical_scroll_margin
    }

    /// Returns the horizontal scroll margin.
    pub fn horizontal_scroll_margin(&self) -> u32 {
        self.editor.horizontal_scroll_margin
    }

    /// Returns the scroll sensitivity factor.
    pub fn scroll_sensitivity(&self) -> f32 {
        self.editor.scroll_sensitivity
    }

    /// Returns whether to automatically scroll when clicking.
    pub fn autoscroll_on_clicks(&self) -> bool {
        self.editor.autoscroll_on_clicks
    }

    /// Returns the search wrap setting.
    pub fn search_wrap(&self) -> SearchWrap {
        self.editor.search_wrap
    }

    /// Returns whether to use smartcase in search.
    pub fn use_smartcase_search(&self) -> bool {
        self.editor.use_smartcase_search
    }

    /// Returns whether LSP is enabled.
    pub fn lsp_enabled(&self) -> bool {
        self.editor.lsp_enabled
    }

    /// Returns whether LSP completions are enabled.
    pub fn lsp_completions_enabled(&self) -> bool {
        self.editor.lsp_completions_enabled
    }

    /// Returns whether LSP hover is enabled.
    pub fn lsp_hover_enabled(&self) -> bool {
        self.editor.lsp_hover_enabled
    }

    /// Returns whether LSP diagnostics are enabled.
    pub fn lsp_diagnostics_enabled(&self) -> bool {
        self.editor.lsp_diagnostics_enabled
    }

    /// Returns whether LSP code actions are enabled.
    pub fn lsp_code_actions_enabled(&self) -> bool {
        self.editor.lsp_code_actions_enabled
    }

    /// Returns whether LSP rename is enabled.
    pub fn lsp_rename_enabled(&self) -> bool {
        self.editor.lsp_rename_enabled
    }

    /// Returns whether inline suggestions are enabled.
    pub fn inline_suggestions_enabled(&self) -> bool {
        self.editor.inline_suggestions_enabled
    }

    /// Returns whether to show gutter diagnostics.
    pub fn show_gutter_diagnostics(&self) -> bool {
        self.editor.show_gutter_diagnostics
    }

    /// Returns whether to show code folding controls.
    pub fn show_folding(&self) -> bool {
        self.editor.show_folding
    }

    /// Returns a reference to the underlying EditorSettings.
    pub fn as_settings(&self) -> &EditorSettings {
        &self.editor
    }
}

impl Default for EditorConfig {
    fn default() -> Self {
        Self {
            editor: EditorSettings::default(),
        }
    }
}
