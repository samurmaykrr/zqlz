//! Adapter layer for integrating Zed Editor into ZQLZ
//!
//! This crate provides a minimal adapter layer that wraps Zed's Editor component
//! for use within ZQLZ's Panel system. It is the ONLY place where Zed Editor is
//! directly used - all other ZQLZ code interacts with the adapter types defined here.
//!
//! # Architecture
//!
//! - **EditorWrapper**: Wraps Zed's Editor with ZQLZ-specific APIs for text operations
//!   and diagnostics management
//! - **BufferManager**: Manages Zed MultiBuffer lifecycle for SQL queries
//! - **SettingsBridge**: Bridges ZQLZ editor settings to Zed's settings system
//! - **ThemeBridge**: Syncs Zed's ThemeRegistry to ZQLZ's Theme global
//! - **LspBridge**: Translates between zqlz-lsp and Zed's editor diagnostic format
//! - **actions**: Re-exports Zed editor actions for ZQLZ's keybinding system
//! - **vim_support**: Optional vim mode initialization (requires `vim-mode` feature)
//!
//! # Design Principles
//!
//! 1. Minimal surface area - only expose what ZQLZ needs
//! 2. Hide Zed-specific details behind simple APIs
//! 3. No direct Zed imports outside this crate
//! 4. Preserve ZQLZ's existing Panel/Dock architecture
//!
//! # Features
//!
//! - `vim-mode`: Enables Zed's vim mode support. Call `vim_support::init(cx)` during
//!   app initialization to enable vim mode globally.

// Module declarations
pub mod actions;
pub mod buffer_manager;
pub mod completion_menu;
pub mod editor_wrapper;
pub mod highlight_bridge;
pub mod language_init;
pub mod lsp_bridge;
pub mod settings_bridge;
pub mod theme_assets;
pub mod theme_bridge;
pub mod vim_support;

pub use crate::completion_menu::{
    CompletionMenu, CompletionMenuEditor, Confirm, SelectDown, SelectUp,
};

// Re-exports for convenience
pub use actions::{list_common_editor_actions, register_editor_actions};
pub use buffer_manager::BufferManager;
pub use editor_wrapper::EditorWrapper;
pub use lsp_bridge::LspBridge;
pub use settings_bridge::SettingsBridge;
pub use theme_bridge::ThemeBridge;

use gpui::App;
use zqlz_settings::ZqlzSettings;

use crate::theme_assets::ZqlzThemeAssets;
use theme::ThemeRegistry;

/// Initializes Zed's editor subsystem
///
/// This function must be called during application startup before creating
/// any EditorWrapper instances. It initializes:
/// - Zed's SettingsStore with Zed's built-in default settings
/// - Zed's theme system with ZQLZ bundled themes (Catppuccin, Gruvbox, etc.)
/// - Zed's language system with SQL support
/// - Zed's editor subsystem with actions and keybindings
/// - EditorConfig store for .editorconfig file support
///
/// # Initialization Order
/// The initialization must follow this order:
/// 1. settings::init() - Required by theme, language, and editor
/// 2. theme::init() - Required by editor for styling
/// 3. language_init::init() - Registers SQL language for syntax highlighting
/// 4. editor::init() - Registers editor actions and keybindings
///
/// Note: ThemeBridge::sync_zed_theme_to_zqlz() should be called AFTER
/// ZqlzSettings is initialized to sync themes correctly.
///
/// # Panics
/// This function will panic if called more than once or if the SettingsStore,
/// theme system, language system, or editor subsystem is already initialized.
pub fn init(cx: &mut App) {
    // Initialize settings first (required by theme system, language, and editor)
    settings::init(cx);

    // Initialize theme system with ZQLZ bundled themes
    // We use our custom AssetSource to load themes from zqlz-app/assets/themes/
    // These themes (Catppuccin, Gruvbox, etc.) are in Zed's native format
    theme::init(theme::LoadThemes::All(Box::new(ZqlzThemeAssets)), cx);

    // Debug: List loaded themes
    let registry = ThemeRegistry::global(cx);
    let theme_names = registry.list_names();
    eprintln!(
        "[ZQLZ] Zed ThemeRegistry loaded {} themes: {:?}",
        theme_names.len(),
        theme_names
    );

    // Initialize language system with SQL support
    // This must happen before creating editors that use SQL language
    language_init::init(cx);

    // Initialize editor subsystem to register actions and keybindings
    // This enables all standard editor operations (undo, redo, navigation, selection, etc.)
    editor::init(cx);

    // Subscribe to ZQLZ settings changes and sync to Zed's SettingsStore
    // This ensures editor settings update live when changed in ZQLZ settings UI
    cx.observe_global::<ZqlzSettings>(|cx| {
        tracing::debug!("ZQLZ settings changed, syncing to Zed SettingsStore");
        SettingsBridge::apply_zqlz_settings_to_zed(cx);
        // Also sync theme after settings change
        ThemeBridge::sync_zed_theme_to_zqlz(cx);
    })
    .detach();
}
