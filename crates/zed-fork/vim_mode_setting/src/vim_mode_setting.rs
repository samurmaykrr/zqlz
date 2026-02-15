// Stub for Zed's vim_mode_setting crate
//
// This integrates with ZQLZ's settings system. The vim_mode_enabled field
// in EditorSettings controls whether vim mode is active.

use gpui::{App, Global};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VimModeSetting {
    pub enabled: bool,
}

impl Global for VimModeSetting {}

impl VimModeSetting {
    pub fn get_global(cx: &App) -> Self {
        cx.try_global::<Self>().cloned().unwrap_or_default()
    }

    pub fn set_global(value: Self, cx: &mut App) {
        cx.set_global(value);
    }

    pub fn is_enabled(cx: &App) -> bool {
        Self::get_global(cx).enabled
    }
}
