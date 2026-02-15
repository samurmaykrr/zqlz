//! Font Loading and Branding Constants
//!
//! Handles embedding and registering custom fonts with GPUI for consistent typography
//! across the application. Fonts are embedded at compile time for reliable distribution.
//!
//! # Branding Typography System
//!
//! ZQLZ uses a consistent typography system:
//! - **UI Font**: Inter - Used for all UI text (labels, buttons, menus)
//! - **Mono Font**: JetBrains Mono - Used for SQL, code, and data display
//!
//! ## Usage
//!
//! Always use theme values for font families - never hard-code font names:
//!
//! ```rust
//! // ✅ CORRECT: Use theme values
//! .font_family(cx.theme().font_family.clone())     // UI text
//! .font_family(cx.theme().mono_font_family.clone()) // Code/SQL text
//!
//! // ✅ CORRECT: Use typography helpers
//! body("Some text")           // Uses theme.font_family
//! code("SELECT * FROM")       // Uses theme.mono_font_family
//!
//! // ❌ WRONG: Hard-coded fonts
//! .font_family("monospace")
//! .font_family("JetBrains Mono")
//! ```

use gpui::*;

/// ZQLZ brand font for UI elements (labels, buttons, menus)
pub const BRAND_UI_FONT: &str = "Inter";

/// ZQLZ brand font for code/SQL/data display
pub const BRAND_MONO_FONT: &str = "JetBrains Mono";

/// System font identifier (fallback for UI)
pub const SYSTEM_UI_FONT: &str = ".SystemUIFont";

/// Platform-specific system mono font (fallback for code)
pub const SYSTEM_MONO_FONT_MACOS: &str = "Menlo";
pub const SYSTEM_MONO_FONT_WINDOWS: &str = "Consolas";
pub const SYSTEM_MONO_FONT_LINUX: &str = "DejaVu Sans Mono";

/// Standard font sizes used across the application
///
/// Use these functions for consistent sizing across the UI.
/// These correspond to the TextVariant sizes in typography.rs.
pub mod sizes {
    use gpui::{Pixels, px};

    /// Extra small text (12px) - captions, badges
    pub fn xs() -> Pixels {
        px(12.0)
    }
    /// Small text (13px) - secondary content  
    pub fn sm() -> Pixels {
        px(13.0)
    }
    /// Base text (14px) - default body text
    pub fn base() -> Pixels {
        px(14.0)
    }
    /// Large text (16px) - emphasis, larger body
    pub fn lg() -> Pixels {
        px(16.0)
    }
    /// Extra large text (18px) - small headings
    pub fn xl() -> Pixels {
        px(18.0)
    }
    /// 2XL text (20px) - headings
    pub fn xxl() -> Pixels {
        px(20.0)
    }
}

// Embed font files at compile time
const INTER_REGULAR: &[u8] = include_bytes!("../assets/fonts/Inter-Regular.ttf");
const INTER_MEDIUM: &[u8] = include_bytes!("../assets/fonts/Inter-Medium.ttf");
const INTER_SEMIBOLD: &[u8] = include_bytes!("../assets/fonts/Inter-SemiBold.ttf");
const INTER_BOLD: &[u8] = include_bytes!("../assets/fonts/Inter-Bold.ttf");

// Monospace
const JETBRAINS_MONO_REGULAR: &[u8] = include_bytes!("../assets/fonts/JetBrainsMono-Regular.ttf");
const JETBRAINS_MONO_BOLD: &[u8] = include_bytes!("../assets/fonts/JetBrainsMono-Bold.ttf");

/// Register all embedded fonts with GPUI
///
/// This should be called during application initialization before any UI is rendered.
/// Note: These fonts are registered but not used by default. The theme uses system fonts
/// for maximum compatibility. To use custom fonts, configure the theme accordingly.
pub fn register_fonts(cx: &mut App) {
    tracing::info!("Starting font registration...");

    // Register Inter family (UI font)
    tracing::info!("Registering Inter fonts (4 variants)...");
    match cx.text_system().add_fonts(vec![
        INTER_REGULAR.into(),
        INTER_MEDIUM.into(),
        INTER_SEMIBOLD.into(),
        INTER_BOLD.into(),
    ]) {
        Ok(_) => tracing::info!("✓ Inter fonts registered successfully"),
        Err(e) => {
            tracing::error!("✗ Failed to load Inter fonts: {}", e);
            tracing::warn!("Continuing with system fonts...");
        }
    }

    // Register JetBrains Mono family (monospace font)
    tracing::info!("Registering JetBrains Mono fonts (2 variants)...");
    match cx.text_system().add_fonts(vec![
        JETBRAINS_MONO_REGULAR.into(),
        JETBRAINS_MONO_BOLD.into(),
    ]) {
        Ok(_) => tracing::info!("✓ JetBrains Mono fonts registered successfully"),
        Err(e) => {
            tracing::error!("✗ Failed to load JetBrains Mono fonts: {}", e);
            tracing::warn!("Continuing with system fonts...");
        }
    }

    tracing::info!("Font registration complete!");
}

/// Get the brand UI font family (Inter)
///
/// This is the recommended font for all UI elements.
pub fn brand_ui_font() -> SharedString {
    BRAND_UI_FONT.into()
}

/// Get the brand monospace font family (JetBrains Mono)
///
/// This is the recommended font for code, SQL, and data display.
pub fn brand_mono_font() -> SharedString {
    BRAND_MONO_FONT.into()
}

/// Get the fallback UI font family (system font)
///
/// Use this only as a fallback when brand fonts are unavailable.
pub fn system_ui_font() -> SharedString {
    SYSTEM_UI_FONT.into()
}

/// Get the fallback monospace font family for the current platform
///
/// Use this only as a fallback when brand fonts are unavailable.
pub fn system_mono_font() -> SharedString {
    #[cfg(target_os = "macos")]
    {
        SYSTEM_MONO_FONT_MACOS.into()
    }
    #[cfg(target_os = "windows")]
    {
        SYSTEM_MONO_FONT_WINDOWS.into()
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        SYSTEM_MONO_FONT_LINUX.into()
    }
}
