#![allow(clippy::type_complexity)]

use crate::widgets::{
    ActiveTheme, Collapsible, Side, StyledExt, h_flex, scroll::ScrollableElement, v_flex,
};
use gpui::{
    AnyElement, App, EdgesRefinement, ElementId, InteractiveElement as _, IntoElement,
    ListAlignment, ListState, ParentElement, Pixels, RenderOnce, SharedString, StyleRefinement,
    Styled, Window, div, list, prelude::FluentBuilder, px,
};

mod menu;
pub use menu::*;

const DEFAULT_WIDTH: Pixels = px(255.);
const COLLAPSED_WIDTH: Pixels = px(48.);

pub trait SidebarItem: Collapsible + Clone {
    fn render(
        self,
        id: impl Into<ElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> impl IntoElement;
}

/// A Sidebar element that can contain collapsible child elements.
#[derive(IntoElement)]
pub struct Sidebar<E: SidebarItem + 'static> {
    id: ElementId,
    style: StyleRefinement,
    content: Vec<E>,
    /// header view
    header: Option<AnyElement>,
    /// footer view
    footer: Option<AnyElement>,
    /// The side of the sidebar
    side: Side,
    collapsed: bool,
}

impl<E: SidebarItem> Sidebar<E> {
    /// Create a new Sidebar with the given ID.
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            style: StyleRefinement::default(),
            content: vec![],
            header: None,
            footer: None,
            side: Side::Left,
            collapsed: false,
        }
    }

    /// Set the side of the sidebar.
    ///
    /// Default is `Side::Left`.
    pub fn side(mut self, side: Side) -> Self {
        self.side = side;
        self
    }

    /// Set the sidebar to be collapsed
    pub fn collapsed(mut self, collapsed: bool) -> Self {
        self.collapsed = collapsed;
        self
    }

    /// Set the header of the sidebar.
    pub fn header(mut self, header: impl IntoElement) -> Self {
        self.header = Some(header.into_any_element());
        self
    }

    /// Set the footer of the sidebar.
    pub fn footer(mut self, footer: impl IntoElement) -> Self {
        self.footer = Some(footer.into_any_element());
        self
    }

    /// Add a child element to the sidebar, the child must implement `Collapsible`
    pub fn child(mut self, child: E) -> Self {
        self.content.push(child);
        self
    }

    /// Add multiple children to the sidebar, the children must implement `Collapsible`
    pub fn children(mut self, children: impl IntoIterator<Item = E>) -> Self {
        self.content.extend(children);
        self
    }
}

impl<E: SidebarItem> Styled for Sidebar<E> {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

impl<E: SidebarItem> RenderOnce for Sidebar<E> {
    fn render(mut self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        self.style.padding = EdgesRefinement::default();

        let id = self.id;
        let content_len = self.content.len();
        let overdraw = px(window.viewport_size().height.as_f32() * 0.3);
        let list_state = window
            .use_keyed_state(
                SharedString::from(format!("{}-list-state", id)),
                cx,
                |_, _| ListState::new(content_len, ListAlignment::Top, overdraw),
            )
            .read(cx)
            .clone();
        if list_state.item_count() != content_len {
            list_state.reset(content_len);
        }

        let sidebar = v_flex()
            .id(id.clone())
            .flex_shrink_0()
            .h_full()
            .overflow_hidden()
            .relative()
            .bg(cx.theme().sidebar)
            .text_color(cx.theme().sidebar_foreground)
            .border_color(cx.theme().sidebar_border)
            .map(|this| match self.side {
                Side::Left => this.border_r_1(),
                Side::Right => this.border_l_1(),
            })
            .when(self.style.size.width.is_none(), |this| {
                this.w(DEFAULT_WIDTH)
            })
            .refine_style(&self.style)
            .when(self.collapsed, |this| this.w(COLLAPSED_WIDTH).gap_2())
            .when_some(self.header.take(), |this, header| {
                this.child(
                    h_flex()
                        .id("header")
                        .pt_3()
                        .px_3()
                        .gap_2()
                        .when(self.collapsed, |this| this.pt_2().px_2())
                        .child(header),
                )
            })
            .child(
                v_flex().id("content").flex_1().min_h_0().child(
                    v_flex()
                        .id("inner")
                        .size_full()
                        .px_3()
                        .gap_y_3()
                        .when(self.collapsed, |this| this.p_2())
                        .child(
                            list(list_state.clone(), {
                                move |ix, window, cx| {
                                    let group = self.content[ix].clone();
                                    div()
                                        .id(ix)
                                        .child(
                                            group
                                                .collapsed(self.collapsed)
                                                .render(ix, window, cx)
                                                .into_any_element(),
                                        )
                                        .when(ix == 0, |this| this.pt_3())
                                        .when(ix + 1 == content_len, |this| this.pb_3())
                                        .into_any_element()
                                }
                            })
                            .size_full(),
                        )
                        .vertical_scrollbar(&list_state),
                ),
            )
            .when_some(self.footer.take(), |this, footer| {
                this.child(
                    h_flex()
                        .id("footer")
                        .pb_3()
                        .px_3()
                        .gap_2()
                        .when(self.collapsed, |this| this.pt_2().px_2())
                        .child(footer),
                )
            });

        sidebar.into_any_element()
    }
}
