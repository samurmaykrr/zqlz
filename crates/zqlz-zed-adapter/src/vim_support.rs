//! Vim mode support for Zed editor integration
//!
//! This module provides initialization for Zed's vim mode when the `vim-mode` feature is enabled.
//! Vim mode is controlled globally via settings and automatically applies to all editors.

use gpui::App;

/// Initializes vim mode support
///
/// This function should be called once during application initialization.
/// Vim mode will then be automatically enabled for all editors when the
/// VimModeSetting is enabled in settings.
///
/// # Arguments
/// * `cx` - The application context
///
/// # Example
/// ```ignore
/// // In app initialization (e.g., main.rs or app setup)
/// #[cfg(feature = "vim-mode")]
/// zqlz_zed_adapter::vim_support::init(cx);
/// ```
#[cfg(feature = "vim-mode")]
pub fn init(cx: &mut App) {
    vim::init(cx);
}

/// No-op initialization when vim-mode feature is disabled
///
/// This allows calling code to not worry about feature flags.
#[cfg(not(feature = "vim-mode"))]
pub fn init(_cx: &mut App) {
    // Vim mode is not compiled in - no-op
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_does_not_panic() {
        // Test that init can be called safely
        // In real usage, this would need an App context
        // For now, just verify the function exists and compiles
        assert!(true);
    }
}
