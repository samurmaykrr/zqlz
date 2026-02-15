mod code_action_menu;
mod completion_menu;
mod context_menu;
mod diagnostic_popover;
mod hover_popover;

pub(crate) use code_action_menu::*;
pub use completion_menu::*;
pub(crate) use context_menu::*;
pub(crate) use diagnostic_popover::*;
pub(crate) use hover_popover::*;

use gpui::{
    App, Div, ElementId, Entity, InteractiveElement as _, IntoElement, Stateful, Styled as _, div,
};

use crate::widgets::StyledExt as _;
use super::InputState;

pub(crate) enum ContextMenu {
    Completion(Entity<CompletionMenu<InputState>>),
    CodeAction(Entity<CodeActionMenu>),
    MouseContext(Entity<MouseContextMenu>),
}

impl ContextMenu {
    pub(crate) fn is_open(&self, cx: &App) -> bool {
        match self {
            ContextMenu::Completion(menu) => menu.read(cx).is_open(),
            ContextMenu::CodeAction(menu) => menu.read(cx).is_open(),
            ContextMenu::MouseContext(menu) => menu.read(cx).is_open(),
        }
    }

    pub(crate) fn render(&self) -> impl IntoElement {
        match self {
            ContextMenu::Completion(menu) => menu.clone().into_any_element(),
            ContextMenu::CodeAction(menu) => menu.clone().into_any_element(),
            ContextMenu::MouseContext(menu) => menu.clone().into_any_element(),
        }
    }
}

pub(super) fn editor_popover(id: impl Into<ElementId>, cx: &App) -> Stateful<Div> {
    div()
        .id(id)
        .flex_none()
        .occlude()
        .popover_style(cx)
        .shadow_md()
        .text_xs()
        .p_1()
}
