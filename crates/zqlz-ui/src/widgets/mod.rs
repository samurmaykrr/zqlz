//! ZQLZ UI Widgets
//!
//! Core UI components and widgets copied from gpui-component for independent customization.

use gpui::{App, SharedString};

mod anchored;
mod element_ext;
mod event;
mod geometry;
mod global_state;
mod icon;
mod index_path;
mod root;
mod styled;
mod virtual_list;
mod window_ext;
mod zqlz_icons;
mod status_dot;

pub(crate) mod actions;

pub mod animation;
pub mod badge;
pub mod button;
pub mod checkbox;
pub mod clipboard;
pub mod collapsible;
pub mod date_picker;
pub mod dialog;
pub mod divider;
pub mod dock;
pub mod highlighter;
pub mod history;
pub mod hover_card;
pub mod input;
pub mod kbd;
pub mod label;
pub mod link;
pub mod list;
pub mod menu;
pub mod notification;
pub mod popover;
pub mod radio;
pub mod resizable;
pub mod scroll;
pub mod select;
pub mod sheet;
pub mod skeleton;
pub mod slider;
pub mod spinner;
pub mod switch;
pub mod tab;
pub mod table;
pub mod text;
pub mod theme;
pub mod title_bar;
pub mod tooltip;
pub mod tree;
pub mod typography;
pub mod window_border;

pub use anchored::anchored;
pub use crate::widgets::styled::Disableable;
pub use element_ext::ElementExt;
pub use event::InteractiveElementExt;
pub use geometry::*;
pub use icon::*;
pub use index_path::IndexPath;
pub use input::{Rope, RopeExt, RopeLines};
pub use root::Root;
pub use styled::*;
pub use theme::*;
pub use title_bar::{TITLE_BAR_HEIGHT, TitleBar};
pub use typography::{
    Text, TextVariant, body, body_large, body_small, caption, code, code_small, h1, h2, h3, h4, h5,
    h6, label, label_small, muted, muted_small,
};
pub use virtual_list::{VirtualList, VirtualListScrollHandle, h_virtual_list, v_virtual_list};
pub use window_border::{WindowBorder, window_border, window_paddings};
pub use window_ext::WindowExt;
pub use zqlz_icons::{DatabaseLogo, ZqlzIcon};
pub use status_dot::{ConnectionStatus, StatusDot};

/// Initialize the widget system
pub fn init(cx: &mut App) {
    // Note: Custom fonts (Inter, JetBrains Mono) are registered in fonts::register_fonts()
    // but the theme defaults to .SystemUIFont for maximum compatibility.
    // This ensures text always renders, even if custom fonts fail to load.

    theme::init(cx);
    global_state::init(cx);
    root::init(cx);
    dock::init(cx);
    select::init(cx);
    input::init(cx);
    list::init(cx);
    dialog::init(cx);
    popover::init(cx);
    menu::init(cx);
    table::init(cx);
    tree::init(cx);
    date_picker::init(cx);
}

#[inline]
pub(crate) fn measure_enable() -> bool {
    std::env::var("ZED_MEASUREMENTS").is_ok() || std::env::var("GPUI_MEASUREMENTS").is_ok()
}

/// Measures the execution time of a function and logs it if `if_` is true.
///
/// And need env `GPUI_MEASUREMENTS=1`
#[inline]
#[track_caller]
pub fn measure_if(name: impl Into<SharedString>, if_: bool, f: impl FnOnce()) {
    if if_ && measure_enable() {
        let measure = Measure::new(name);
        f();
        measure.end();
    } else {
        f();
    }
}

/// Measures the execution time.
#[inline]
#[track_caller]
pub fn measure(name: impl Into<SharedString>, f: impl FnOnce()) {
    measure_if(name, true, f);
}

pub struct Measure {
    name: SharedString,
    start: std::time::Instant,
}

impl Measure {
    #[track_caller]
    pub fn new(name: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            start: std::time::Instant::now(),
        }
    }

    #[track_caller]
    pub fn end(self) {
        let duration = self.start.elapsed();
        tracing::trace!("{} in {:?}", self.name, duration);
    }
}
