//! Shared scaffolding for windowed GPUI interaction tests.
//!
//! Extracted from the `open_*`/`click`/`bounds` helpers that were
//! independently duplicated across `render_tests.rs` modules in
//! `zqlz-connection`, `zqlz-command-palette`, `zqlz-settings`, and others.
//! See `crates/zqlz-connection/src/widgets/sidebar/render_tests.rs` for the
//! house style this generalizes.

use gpui::{
    AppContext, Bounds, Context, Entity, Modifiers, Pixels, Render, TestAppContext,
    VisualTestContext, Window, px, size,
};
use zqlz_ui::widgets::Root;

/// Build `V` as the content of a `ui::Root`-wrapped window and paint it.
///
/// Wrapping in `Root` is required by any widget that reads theme/focus state
/// off it while painting (anything using `Input`/`Select`, for example) — a
/// bare view root panics as soon as such a widget paints. Wrapping
/// unconditionally is harmless for views that don't need it.
pub fn open_in_window<V: Render>(
    cx: &mut TestAppContext,
    window_size: (f32, f32),
    build: impl FnOnce(&mut Window, &mut Context<V>) -> V + 'static,
) -> (Entity<V>, VisualTestContext) {
    let mut entity_slot = None;
    let window = cx.add_window(|window, cx| {
        let entity = cx.new(|cx| build(window, cx));
        entity_slot = Some(entity.clone());
        Root::new(entity, window, cx)
    });
    let entity = entity_slot.expect("entity should have been created by `build`");
    let visual_cx = VisualTestContext::from_window(window.into(), cx);
    visual_cx.simulate_resize(size(px(window_size.0), px(window_size.1)));
    visual_cx.run_until_parked();
    (entity, visual_cx)
}

/// Bounds of a painted element, found by the `debug_selector` its renderer
/// set (e.g. `.debug_selector(|| format!("row:{id}"))`).
pub fn bounds(cx: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
    cx.debug_bounds(selector)
        .unwrap_or_else(|| panic!("{selector:?} should be painted"))
}

/// Click the center of a painted element, by its `debug_selector`, and let
/// the resulting update settle.
pub fn click(cx: &mut VisualTestContext, selector: &'static str) {
    let target = bounds(cx, selector);
    cx.simulate_click(target.center(), Modifiers::none());
    cx.run_until_parked();
}
