//! Application initialization
//!
//! Registers panels for the application.
//! Keybindings are now loaded from JSON files via the `keymaps` module.

use gpui::{App, AppContext};
use zqlz_ui::widgets::dock::register_panel;

use crate::app::AppState;
use crate::components::{
    ConnectionSidebar, QueryTabsPanel, ResultsPanel, SchemaDetailsPanel, SettingsPanel,
};

/// Register all ZQLZ panels with the PanelRegistry
pub fn register_panels(cx: &mut App) {
    // Register ConnectionSidebar (left dock)
    register_panel(
        cx,
        "ConnectionSidebar",
        |_dock_area, _state, _info, _window, cx| Box::new(cx.new(|cx| ConnectionSidebar::new(cx))),
    );

    // Register QueryTabsPanel (center)
    register_panel(
        cx,
        "QueryTabsPanel",
        |_dock_area, _state, _info, _window, cx| {
            Box::new(cx.new(|cx| {
                let mut panel = QueryTabsPanel::new(cx);
                // Set schema service from AppState
                if let Some(app_state) = cx.try_global::<AppState>() {
                    panel.set_schema_service(app_state.schema_service.clone());
                }
                panel
            }))
        },
    );

    // Register ResultsPanel (bottom dock)
    register_panel(
        cx,
        "ResultsPanel",
        |_dock_area, _state, _info, _window, cx| Box::new(cx.new(|cx| ResultsPanel::new(cx))),
    );

    // Register SchemaDetailsPanel (right dock)
    register_panel(
        cx,
        "SchemaDetailsPanel",
        |_dock_area, _state, _info, _window, cx| Box::new(cx.new(|cx| SchemaDetailsPanel::new(cx))),
    );

    // Register SettingsPanel
    register_panel(
        cx,
        "SettingsPanel",
        |_dock_area, _state, _info, window, cx| {
            Box::new(cx.new(|cx| SettingsPanel::new(window, cx)))
        },
    );
}
