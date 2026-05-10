use crate::widgets::{ActiveTheme, Sizable, Size, StyledExt};
use gpui::div;
use gpui::prelude::FluentBuilder as _;
use gpui::{
    Animation, AnimationExt as _, AnyElement, App, ElementId, Hsla, InteractiveElement as _,
    IntoElement, ParentElement, RenderOnce, StyleRefinement, Styled, Window, ease_in_out, relative,
};
use std::time::Duration;

use super::ProgressState;

/// A circular progress indicator element.
#[derive(IntoElement)]
pub struct ProgressCircle {
    id: ElementId,
    style: StyleRefinement,
    color: Option<Hsla>,
    value: f32,
    size: Size,
    children: Vec<AnyElement>,
    loading: bool,
}

impl ProgressCircle {
    /// Create a new circular progress indicator.
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            value: Default::default(),
            color: None,
            style: StyleRefinement::default(),
            size: Size::default(),
            children: Vec::new(),
            loading: false,
        }
    }

    /// Enable indeterminate loading animation.
    ///
    /// When `loading` is `true`, the `value` is ignored and an infinite
    /// rotating arc animation is shown instead.
    pub fn loading(mut self, loading: bool) -> Self {
        self.loading = loading;
        self
    }

    /// Set the color of the progress circle.
    pub fn color(mut self, color: impl Into<Hsla>) -> Self {
        self.color = Some(color.into());
        self
    }

    /// Set the percentage value of the progress circle.
    ///
    /// The value should be between 0.0 and 100.0.
    pub fn value(mut self, value: f32) -> Self {
        self.value = value.clamp(0., 100.);
        self
    }

    fn render_circle(start_value: f32, end_value: f32, color: Hsla) -> impl IntoElement {
        let opacity = ((end_value - start_value).abs() / 100.).clamp(0.08, 1.);

        div()
            .absolute()
            .size_full()
            .rounded_full()
            .border_2()
            .border_color(color.opacity(0.18))
            .child(
                div()
                    .absolute()
                    .size_full()
                    .rounded_full()
                    .border_2()
                    .border_color(color.opacity(opacity)),
            )
    }
}

impl Styled for ProgressCircle {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl Sizable for ProgressCircle {
    fn with_size(mut self, size: impl Into<Size>) -> Self {
        self.size = size.into();
        self
    }
}

impl ParentElement for ProgressCircle {
    fn extend(&mut self, elements: impl IntoIterator<Item = AnyElement>) {
        self.children.extend(elements);
    }
}

impl RenderOnce for ProgressCircle {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let value = self.value;
        let loading = self.loading;
        let state = window.use_keyed_state(self.id.clone(), cx, |_, _| ProgressState::new(value));
        let prev_target = state.read(cx).target();
        let has_changed = prev_target != value;

        let color = self.color.unwrap_or(cx.theme().progress_bar);

        div()
            .id(self.id.clone())
            .flex()
            .items_center()
            .justify_center()
            .line_height(relative(1.))
            .map(|this| match self.size {
                Size::XSmall => this.size_2(),
                Size::Small => this.size_3(),
                Size::Medium => this.size_4(),
                Size::Large => this.size_5(),
                Size::Custom(s) => this.size(s * 0.75),
            })
            .refine_style(&self.style)
            .children(self.children)
            .map(|this| {
                if has_changed {
                    let from = prev_target;
                    state.read(cx).set_target(value);

                    let duration = Duration::from_secs_f64(0.15);
                    cx.spawn({
                        let state = state.clone();
                        async move |cx| {
                            cx.background_executor().timer(duration).await;
                            state.update(cx, |this, _| {
                                this.value = this.target();
                            });
                        }
                    })
                    .detach();

                    this.with_animation(
                        format!("progress-circle-{}", from),
                        Animation::new(duration),
                        move |this, delta| {
                            let v = from + (value - from) * delta;
                            this.child(Self::render_circle(0., v, color))
                        },
                    )
                    .into_any_element()
                } else if loading {
                    this.with_animation(
                        "progress-circle-loading",
                        Animation::new(Duration::from_secs(1)).repeat(),
                        move |this, delta| {
                            let end = ease_in_out(delta) * 100.;
                            let start = ease_in_out(((delta - 0.5) / 0.5).clamp(0., 1.)) * 100.;
                            this.child(Self::render_circle(start, end, color))
                        },
                    )
                    .into_any_element()
                } else {
                    this.child(Self::render_circle(0., value, color))
                        .into_any_element()
                }
            })
    }
}
