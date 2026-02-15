//! ZQLZ UI - User interface widgets library
//!
//! This crate contains reusable UI widgets built with GPUI.

pub mod fonts;
pub mod widgets;

use gpui::App;

// Initialize internationalization
rust_i18n::i18n!("locales", fallback = "en");

/// Initialize the UI widget system
pub fn init(cx: &mut App) {
    tracing::info!("Initializing zqlz-ui...");

    // Register fonts first - critical for text rendering
    tracing::info!("Step 1: Registering fonts...");
    fonts::register_fonts(cx);

    // Initialize our widget system
    tracing::info!("Step 2: Initializing widget system...");
    widgets::init(cx);

    tracing::info!("zqlz-ui initialization complete!");
}
