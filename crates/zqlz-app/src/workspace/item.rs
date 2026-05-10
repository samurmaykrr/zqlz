use gpui::{App, Entity, FocusHandle, SharedString};
use std::sync::Arc;
use zqlz_query::widgets::QueryEditor;
use zqlz_ui::widgets::{
    ZqlzIcon,
    dock::{Panel, PanelView},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosePolicy {
    Close,
    PromptToSave,
}

#[derive(Clone)]
pub struct WorkspaceItemMetadata {
    pub title: SharedString,
    #[allow(dead_code)]
    pub icon: Option<ZqlzIcon>,
    pub tooltip: Option<SharedString>,
    pub focus_handle: FocusHandle,
    pub dirty: bool,
    pub closable: bool,
    pub pinned: bool,
    pub preview: bool,
    #[allow(dead_code)]
    pub can_split: bool,
    pub can_move_to_new_window: bool,
}

pub trait WorkspaceItemView {
    fn tab_title(&self, cx: &App) -> SharedString;
    fn tab_icon(&self, cx: &App) -> Option<ZqlzIcon>;
    fn tab_tooltip(&self, cx: &App) -> Option<SharedString>;
    fn focus_handle(&self, cx: &App) -> FocusHandle;
    fn is_dirty(&self, cx: &App) -> bool;
    fn close_policy(&self, cx: &App) -> ClosePolicy;
    fn can_split(&self, cx: &App) -> bool;

    fn metadata(&self, cx: &App) -> WorkspaceItemMetadata {
        WorkspaceItemMetadata {
            title: self.tab_title(cx),
            icon: self.tab_icon(cx),
            tooltip: self.tab_tooltip(cx),
            focus_handle: self.focus_handle(cx),
            dirty: self.is_dirty(cx),
            closable: true,
            pinned: false,
            preview: false,
            can_split: self.can_split(cx),
            can_move_to_new_window: false,
        }
    }
}

#[derive(Clone)]
pub struct WorkspaceItemHandle {
    panel: Arc<dyn PanelView>,
}

impl WorkspaceItemHandle {
    pub fn new(panel: Arc<dyn PanelView>) -> Self {
        Self { panel }
    }

    pub fn metadata(&self, cx: &App) -> WorkspaceItemMetadata {
        metadata_for_panel(self.panel.as_ref(), cx)
    }
}

impl WorkspaceItemView for Entity<QueryEditor> {
    fn tab_title(&self, cx: &App) -> SharedString {
        self.read_with(cx, |editor, _cx| editor.name().into())
    }

    fn tab_icon(&self, _cx: &App) -> Option<ZqlzIcon> {
        Some(ZqlzIcon::FileSql)
    }

    fn tab_tooltip(&self, cx: &App) -> Option<SharedString> {
        self.read_with(cx, |editor, cx| editor.tab_tooltip(cx))
    }

    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.read(cx).editor_focus_handle(cx)
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.read(cx).is_dirty(cx)
    }

    fn close_policy(&self, cx: &App) -> ClosePolicy {
        if self.is_dirty(cx) {
            ClosePolicy::PromptToSave
        } else {
            ClosePolicy::Close
        }
    }

    fn can_split(&self, _cx: &App) -> bool {
        true
    }
}

pub fn metadata_for_panel(panel: &dyn PanelView, cx: &App) -> WorkspaceItemMetadata {
    if let Ok(editor) = panel.view().downcast::<QueryEditor>() {
        let mut metadata = editor.metadata(cx);
        metadata.can_move_to_new_window = panel.can_move_to_new_window(cx);
        return metadata;
    }

    let title = panel
        .tab_name(cx)
        .unwrap_or_else(|| panel.panel_name(cx).into());

    WorkspaceItemMetadata {
        title,
        icon: panel.tab_icon(cx),
        tooltip: panel.tab_tooltip(cx),
        focus_handle: panel.focus_handle(cx),
        dirty: panel.has_unsaved_changes(cx),
        closable: panel.closable(cx),
        pinned: false,
        preview: false,
        can_split: panel.can_split(cx),
        can_move_to_new_window: panel.can_move_to_new_window(cx),
    }
}
