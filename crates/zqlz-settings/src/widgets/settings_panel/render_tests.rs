//! Window-level tests for the settings panel.
//!
//! These open a real GPUI window so they exercise the actual sidebar wiring
//! between clicking a page and the corresponding section painting — not just
//! the `selected_page` field in isolation.
//!
//! Deliberately avoids `use super::*` here: `settings_panel.rs` does
//! `use gpui::*`, which re-exports `gpui_macros::test` as `test`. A glob
//! import would shadow the built-in `#[test]` attribute with it, and
//! `#[gpui::test]`'s own generated wrapper emits an unqualified `#[test]`
//! internally — under that shadow it re-resolves to `gpui::test` again,
//! recursively, until the compiler's parser stack overflows.

use gpui::{Entity, TestAppContext, VisualTestContext};

use super::{SettingsPageKey, SettingsPanel};

fn init_test(cx: &mut TestAppContext) {
    cx.update(zqlz_ui::init);
    cx.update(crate::init);
}

/// Open the settings panel in a real window and paint it.
fn open_settings_panel(cx: &mut TestAppContext) -> (Entity<SettingsPanel>, VisualTestContext) {
    zqlz_test_support::open_in_window(cx, (900.0, 700.0), SettingsPanel::new)
}

#[gpui::test]
fn settings_panel_opens_on_the_appearance_page_by_default(cx: &mut TestAppContext) {
    init_test(cx);
    let (panel, mut cx) = open_settings_panel(cx);

    panel.read_with(&cx, |panel, _cx| {
        assert_eq!(
            panel.selected_page,
            SettingsPageKey::Appearance,
            "the panel should default to the Appearance page"
        );
    });

    assert!(
        cx.debug_bounds("settings-section:Appearance").is_some(),
        "the Appearance section should paint by default"
    );
    assert!(
        cx.debug_bounds("settings-section:Fonts").is_none(),
        "only the active page's section should paint"
    );
    assert!(cx.debug_bounds("settings-page:Appearance").is_some());
    assert!(cx.debug_bounds("settings-page:Fonts").is_some());
}

#[gpui::test]
fn clicking_a_sidebar_page_switches_the_active_section(cx: &mut TestAppContext) {
    init_test(cx);
    let (panel, mut cx) = open_settings_panel(cx);

    zqlz_test_support::click(&mut cx, "settings-page:Fonts");

    panel.read_with(&cx, |panel, _cx| {
        assert_eq!(
            panel.selected_page,
            SettingsPageKey::Fonts,
            "clicking the Fonts sidebar item should switch the active page"
        );
    });

    assert!(
        cx.debug_bounds("settings-section:Fonts").is_some(),
        "the Fonts section should paint after switching"
    );
    assert!(
        cx.debug_bounds("settings-section:Appearance").is_none(),
        "the previous page's section should no longer paint"
    );
    assert!(
        cx.debug_bounds("setting-row:UI Font").is_some(),
        "a Fonts-specific setting row should be visible"
    );
}
