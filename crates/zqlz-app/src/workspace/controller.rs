use gpui::{
    App, Bounds, Context, Entity, EntityId, FocusHandle, Pixels, SharedString, WeakEntity, Window,
};
use std::sync::Arc;

use zqlz_query::widgets::QueryEditor;
use zqlz_settings::{WorkspaceId, save_layout};
use zqlz_ui::widgets::{
    WindowExt,
    dock::{DockArea, DockPlacement, PanelView, TabCommand, TabPanel},
    menu::PopupMenu,
    notification::Notification,
};

use super::{
    WorkspaceItemCloseIntent, WorkspaceItemHandle, WorkspaceItemMetadata, close_intent_for_panel,
};

pub struct WorkspaceController {
    dock_area: WeakEntity<DockArea>,
    workspace_id: WorkspaceId,
    base_title: SharedString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceWindowCloseIntent {
    CloseNow,
    ConfirmDiscard { dirty_count: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceWindowClosePrompt {
    pub title: &'static str,
    pub message: String,
    pub discard_button: &'static str,
    pub cancel_button: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceCloseResult {
    Closed,
    NotClosed,
}

#[derive(Clone)]
pub struct DetachedWorkspaceItem {
    pub panel: Arc<dyn PanelView>,
    pub was_pinned: bool,
    pub was_preview: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkspaceBulkCloseMode {
    Others { keep_index: usize },
    Right { from_index: usize },
    Left { from_index: usize },
    Clean,
    All,
}

#[derive(Default)]
struct WorkspaceBulkCloseTargets {
    panels: Vec<Arc<dyn PanelView>>,
    skipped_dirty_count: usize,
}

impl WorkspaceController {
    pub fn new(dock_area: WeakEntity<DockArea>, workspace_id: WorkspaceId) -> Self {
        Self {
            dock_area,
            workspace_id,
            base_title: SharedString::from("ZQLZ"),
        }
    }

    pub fn active_center_item_close_intent(&self, cx: &App) -> WorkspaceItemCloseIntent {
        let Some(active_panel) = self.active_center_panel(cx) else {
            return WorkspaceItemCloseIntent::CloseNow;
        };

        close_intent_for_panel(active_panel.as_ref(), cx)
    }

    pub fn window_title(&self, cx: &App) -> SharedString {
        workspace_window_title(
            &self.base_title,
            self.active_center_item_metadata(cx).as_ref(),
        )
    }

    pub fn refresh_window_title(&self, window: &mut Window, cx: &App) {
        crate::window_manager::set_main_window_title(window, self.window_title(cx));
    }

    pub fn open_new_window(
        &self,
        cx: &mut App,
        build_root: impl FnOnce(&mut Window, &mut App) -> Entity<zqlz_ui::widgets::Root> + 'static,
    ) -> anyhow::Result<()> {
        crate::window_manager::open_main_window(cx, build_root)
    }

    pub fn close_window_now(&self, window: &mut Window, cx: &mut App) {
        crate::window_manager::close_main_window(window, cx);
    }

    pub fn minimize_window(&self, window: &mut Window) {
        crate::window_manager::minimize_main_window(window);
    }

    pub fn zoom_window(&self, window: &mut Window) {
        crate::window_manager::zoom_main_window(window);
    }

    pub fn save_layout(&self, cx: &App) {
        let Some(dock_area) = self.dock_area.upgrade() else {
            return;
        };

        let state = dock_area.read(cx).dump(cx);
        if let Err(error) = save_layout(&self.workspace_id, &state) {
            tracing::error!(%error, "Failed to save dock layout");
        } else {
            tracing::debug!("Dock layout saved successfully");
        }
    }

    pub fn active_center_item_focus_handle(&self, cx: &App) -> Option<FocusHandle> {
        self.active_center_item_metadata(cx)
            .map(|metadata| metadata.focus_handle)
    }

    pub fn active_center_item_metadata(&self, cx: &App) -> Option<WorkspaceItemMetadata> {
        let active_item = self.active_center_item(cx)?;
        let mut metadata = active_item.metadata(cx);

        if let Some(dock_area) = self.dock_area.upgrade() {
            let dock_area = dock_area.read(cx);
            metadata.pinned = dock_area.active_tab_is_pinned(cx);
            metadata.preview = dock_area.active_tab_is_preview(cx);
        }

        Some(metadata)
    }

    pub fn active_center_item(&self, cx: &App) -> Option<WorkspaceItemHandle> {
        self.active_center_panel(cx).map(WorkspaceItemHandle::new)
    }

    pub fn active_center_panel_name(&self, cx: &App) -> Option<&'static str> {
        self.active_center_panel(cx)
            .as_ref()
            .map(|panel| panel.panel_name(cx))
    }

    pub fn active_center_panel(
        &self,
        cx: &App,
    ) -> Option<std::sync::Arc<dyn zqlz_ui::widgets::dock::PanelView>> {
        self.dock_area
            .upgrade()
            .and_then(|dock_area| dock_area.read(cx).active_panel(cx))
    }

    pub fn active_query_editor(&self, cx: &App) -> Option<Entity<QueryEditor>> {
        self.active_center_view(cx)
    }

    pub fn active_center_view<T: 'static>(&self, cx: &App) -> Option<Entity<T>> {
        self.active_center_panel(cx)
            .and_then(|panel| panel.view().downcast::<T>().ok())
    }

    pub fn center_tab_panel(&self, cx: &App) -> Option<Entity<TabPanel>> {
        self.dock_area
            .upgrade()
            .and_then(|dock_area| dock_area.read(cx).center_tab_panel())
    }

    pub fn active_center_tab_index(&self, cx: &App) -> Option<usize> {
        self.center_tab_panel(cx)
            .and_then(|tab_panel| tab_panel.read(cx).active_index())
    }

    pub fn tab_context_menu(
        &self,
        tab_index: usize,
        focus_handle: FocusHandle,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<Entity<PopupMenu>> {
        let center_panel = self.center_tab_panel(cx)?;
        Some(TabPanel::context_menu_for_tab(
            center_panel.downgrade(),
            tab_index,
            focus_handle,
            window,
            cx,
        ))
    }

    pub fn center_panels(&self, cx: &App) -> Vec<Arc<dyn PanelView>> {
        self.dock_area
            .upgrade()
            .map(|dock_area| dock_area.read(cx).center_panels(cx))
            .unwrap_or_default()
    }

    pub fn center_items(&self, cx: &App) -> Vec<WorkspaceItemHandle> {
        self.center_panels(cx)
            .into_iter()
            .map(WorkspaceItemHandle::new)
            .collect()
    }

    pub fn center_tab_metadata(&self, cx: &App) -> Vec<WorkspaceItemMetadata> {
        let items = self.center_items(cx);
        let tab_metadata = self
            .dock_area
            .upgrade()
            .map(|dock_area| dock_area.read(cx).center_tab_metadata(cx))
            .unwrap_or_default();

        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let mut metadata = item.metadata(cx);
                if let Some(tab_metadata) = tab_metadata.get(index) {
                    metadata.pinned = tab_metadata.pinned;
                    metadata.preview = tab_metadata.preview;
                }
                metadata
            })
            .collect()
    }

    pub fn dirty_center_item_count(&self, cx: &App) -> usize {
        self.center_tab_metadata(cx)
            .iter()
            .filter(|metadata| metadata.dirty)
            .count()
    }

    pub fn window_close_intent(&self, cx: &App) -> WorkspaceWindowCloseIntent {
        window_close_intent_for_dirty_count(self.dirty_center_item_count(cx))
    }

    pub fn activate_next_tab(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.activate_next_tab(window, cx);
        });
    }

    pub fn activate_prev_tab(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.activate_prev_tab(window, cx);
        });
    }

    pub fn activate_tab_by_number(
        &self,
        number: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.activate_tab_by_number(number, window, cx);
        });
    }

    pub fn activate_tab_index_and_close_intent(
        &self,
        tab_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> WorkspaceItemCloseIntent {
        self.activate_tab_by_number(tab_index + 1, window, cx);
        self.active_center_item_close_intent(cx)
    }

    pub fn activate_panel_by_id(
        &self,
        panel_id: EntityId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.update_dock_with(cx, |dock_area, cx| {
            dock_area.activate_panel_by_id(panel_id, window, cx)
        })
        .unwrap_or(false)
    }

    pub fn activate_panel(
        &self,
        panel_name: &str,
        placement: DockPlacement,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.update_dock_with(cx, |dock_area, cx| {
            dock_area.activate_panel(panel_name, placement, window, cx)
        })
        .unwrap_or(false)
    }

    pub fn reveal_panel(
        &self,
        panel_name: &str,
        placement: DockPlacement,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.set_dock_open(placement, true, window, cx);
        self.activate_panel(panel_name, placement, window, cx)
    }

    pub fn add_panel(
        &self,
        panel: Arc<dyn PanelView>,
        placement: DockPlacement,
        bounds: Option<Bounds<Pixels>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.add_panel(panel, placement, bounds, window, cx);
        });
    }

    pub fn add_center_item(
        &self,
        panel: Arc<dyn PanelView>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.add_panel(panel, DockPlacement::Center, None, window, cx);
    }

    pub fn remove_panel(
        &self,
        panel: Arc<dyn PanelView>,
        placement: DockPlacement,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.remove_panel(panel, placement, window, cx);
        });
    }

    pub fn remove_center_item(
        &self,
        panel: Arc<dyn PanelView>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.remove_panel(panel, DockPlacement::Center, window, cx);
    }

    pub fn detach_center_item_at(
        &self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<DetachedWorkspaceItem> {
        let tab_metadata = self.center_tab_metadata(cx);
        let metadata = tab_metadata.get(index)?;
        let was_pinned = metadata.pinned;
        let was_preview = metadata.preview;

        let panel = self.update_dock_with(cx, |dock_area, cx| {
            dock_area.take_center_tab_at(index, window, cx)
        })??;

        Some(DetachedWorkspaceItem {
            panel,
            was_pinned,
            was_preview,
        })
    }

    pub fn add_detached_center_item(
        &self,
        item: DetachedWorkspaceItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.add_center_item(item.panel, window, cx);
        if item.was_pinned {
            self.pin_active_tab(cx);
        }
        if item.was_preview {
            self.mark_active_tab_as_preview(cx);
        }
        self.refresh_window_title(window, cx);
    }

    pub fn remove_panels_from_all_docks(
        &self,
        panels: Vec<Arc<dyn PanelView>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            for panel in panels {
                dock_area.remove_panel_from_all_docks(panel, window, cx);
            }
        });
    }

    pub fn set_dock_open(
        &self,
        placement: DockPlacement,
        open: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.set_dock_open(placement, open, window, cx);
        });
    }

    pub fn toggle_dock(
        &self,
        placement: DockPlacement,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.toggle_dock(placement, window, cx);
        });
    }

    pub fn toggle_all_docks(&self, window: &mut Window, cx: &mut Context<Self>) {
        let placements = [
            DockPlacement::Left,
            DockPlacement::Right,
            DockPlacement::Bottom,
        ];
        let open = self
            .dock_area
            .upgrade()
            .map(|dock_area| {
                let dock_area = dock_area.read(cx);
                !placements
                    .iter()
                    .any(|placement| dock_area.is_dock_open(*placement, cx))
            })
            .unwrap_or(false);

        self.update_dock(cx, |dock_area, cx| {
            for placement in placements {
                if dock_area.has_dock(placement) {
                    dock_area.set_dock_open(placement, open, window, cx);
                }
            }
        });
    }

    pub fn activate_last_tab(&self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(dock_area) = self.dock_area.upgrade() {
            let count = dock_area.read(cx).tab_count(cx);
            if count > 0 {
                dock_area.update(cx, |dock_area, cx| {
                    dock_area.activate_tab_by_number(count, window, cx);
                });
            }
        }
    }

    pub fn navigate_tab_back(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.navigate_tab_back(window, cx);
        });
    }

    pub fn navigate_tab_forward(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.navigate_tab_forward(window, cx);
        });
    }

    pub fn close_active_tab(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> WorkspaceCloseResult {
        self.update_dock_with(cx, |dock_area, cx| dock_area.close_active_tab(window, cx))
            .map(workspace_close_result)
            .unwrap_or(WorkspaceCloseResult::NotClosed)
    }

    pub fn force_close_active_tab(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> WorkspaceCloseResult {
        self.update_dock_with(cx, |dock_area, cx| {
            dock_area.force_close_active_tab(window, cx)
        })
        .map(workspace_close_result)
        .unwrap_or(WorkspaceCloseResult::NotClosed)
    }

    pub fn close_other_tabs(&self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(active_index) = self.active_center_tab_index(cx) {
            self.close_other_tabs_at(active_index, window, cx);
        }
    }

    pub fn close_other_tabs_at(
        &self,
        keep_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_bulk_center_tabs(WorkspaceBulkCloseMode::Others { keep_index }, window, cx);
    }

    pub fn handle_tab_command(
        &self,
        command: TabCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match command {
            TabCommand::CloseOthers { keep_index } => {
                self.close_other_tabs_at(keep_index, window, cx);
            }
            TabCommand::CloseRight { from_index } => {
                self.close_tabs_to_right_at(from_index, window, cx);
            }
            TabCommand::CloseLeft { from_index } => {
                self.close_tabs_to_left_at(from_index, window, cx);
            }
            TabCommand::CloseClean => {
                self.close_clean_tabs(window, cx);
            }
            TabCommand::CloseAll => {
                self.close_all_tabs(window, cx);
            }
            TabCommand::TogglePin { index } => {
                self.toggle_pin_tab_at(index, cx);
            }
            TabCommand::SetPreview { index } => {
                self.set_preview_tab_at(index, cx);
            }
            TabCommand::MoveToNewWindow { .. } => {}
        }
    }

    pub fn close_tabs_to_right(&self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(active_index) = self.active_center_tab_index(cx) {
            self.close_tabs_to_right_at(active_index, window, cx);
        }
    }

    pub fn close_tabs_to_right_at(
        &self,
        from_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_bulk_center_tabs(WorkspaceBulkCloseMode::Right { from_index }, window, cx);
    }

    pub fn close_tabs_to_left(&self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(active_index) = self.active_center_tab_index(cx) {
            self.close_tabs_to_left_at(active_index, window, cx);
        }
    }

    pub fn close_tabs_to_left_at(
        &self,
        from_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close_bulk_center_tabs(WorkspaceBulkCloseMode::Left { from_index }, window, cx);
    }

    pub fn close_clean_tabs(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_bulk_center_tabs(WorkspaceBulkCloseMode::Clean, window, cx);
    }

    pub fn close_all_tabs(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_bulk_center_tabs(WorkspaceBulkCloseMode::All, window, cx);
    }

    pub fn toggle_pin_active_tab(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.toggle_pin_active_tab(cx);
        });
    }

    pub fn toggle_pin_tab_at(&self, index: usize, cx: &mut Context<Self>) {
        self.update_center_tab_panel(cx, |tab_panel, cx| {
            tab_panel.toggle_pin_tab(index, cx);
        });
    }

    pub fn pin_active_tab(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.set_pin_active_tab(true, cx);
        });
    }

    pub fn unpin_active_tab(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.set_pin_active_tab(false, cx);
        });
    }

    pub fn mark_active_tab_as_preview(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.mark_active_tab_as_preview(cx);
        });
    }

    pub fn set_preview_tab_at(&self, index: Option<usize>, cx: &mut Context<Self>) {
        self.update_center_tab_panel(cx, |tab_panel, cx| {
            tab_panel.set_preview_tab(index, cx);
        });
    }

    pub fn clear_active_tab_preview(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.clear_active_tab_preview(cx);
        });
    }

    pub fn pin_active_preview_tab(&self, cx: &mut Context<Self>) {
        self.update_dock(cx, |dock_area, cx| {
            dock_area.pin_active_preview_tab(cx);
        });
    }

    pub fn replace_active_preview_tab(
        &self,
        panel: Arc<dyn PanelView>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.update_dock_with(cx, |dock_area, cx| {
            dock_area.replace_active_preview_tab(panel, window, cx)
        })
        .unwrap_or(false)
    }

    fn update_dock(
        &self,
        cx: &mut Context<Self>,
        update: impl FnOnce(&mut DockArea, &mut Context<DockArea>),
    ) {
        if let Some(dock_area) = self.dock_area.upgrade() {
            dock_area.update(cx, update);
        }
    }

    fn update_dock_with<T>(
        &self,
        cx: &mut Context<Self>,
        update: impl FnOnce(&mut DockArea, &mut Context<DockArea>) -> T,
    ) -> Option<T> {
        self.dock_area
            .upgrade()
            .map(|dock_area| dock_area.update(cx, update))
    }

    fn update_center_tab_panel(
        &self,
        cx: &mut Context<Self>,
        update: impl FnOnce(&mut TabPanel, &mut Context<TabPanel>),
    ) {
        if let Some(tab_panel) = self.center_tab_panel(cx) {
            tab_panel.update(cx, update);
        }
    }

    fn close_bulk_center_tabs(
        &self,
        mode: WorkspaceBulkCloseMode,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let targets = self.clean_bulk_close_targets(mode, cx);
        let skipped_dirty_count = targets.skipped_dirty_count;

        self.update_dock(cx, |dock_area, cx| {
            for panel in targets.panels {
                dock_area.remove_panel(panel, DockPlacement::Center, window, cx);
            }
        });

        if skipped_dirty_count > 0 {
            window.push_notification(
                Notification::warning(skipped_dirty_tabs_message(skipped_dirty_count)),
                cx,
            );
        }
    }

    fn clean_bulk_close_targets(
        &self,
        mode: WorkspaceBulkCloseMode,
        cx: &App,
    ) -> WorkspaceBulkCloseTargets {
        let mut targets = WorkspaceBulkCloseTargets::default();
        let panels = self.center_panels(cx);

        for (index, metadata) in self.center_tab_metadata(cx).into_iter().enumerate() {
            if should_count_skipped_dirty_for_workspace_bulk_mode(
                mode,
                index,
                metadata.closable,
                metadata.dirty,
                metadata.pinned,
            ) {
                targets.skipped_dirty_count += 1;
            }

            if should_close_for_workspace_bulk_mode(
                mode,
                index,
                metadata.closable,
                metadata.dirty,
                metadata.pinned,
            ) && let Some(panel) = panels.get(index)
            {
                targets.panels.push(panel.clone());
            }
        }

        targets
    }
}

fn window_close_intent_for_dirty_count(dirty_count: usize) -> WorkspaceWindowCloseIntent {
    if dirty_count > 0 {
        WorkspaceWindowCloseIntent::ConfirmDiscard { dirty_count }
    } else {
        WorkspaceWindowCloseIntent::CloseNow
    }
}

fn workspace_close_result(closed: bool) -> WorkspaceCloseResult {
    if closed {
        WorkspaceCloseResult::Closed
    } else {
        WorkspaceCloseResult::NotClosed
    }
}

fn should_close_for_workspace_bulk_mode(
    mode: WorkspaceBulkCloseMode,
    index: usize,
    closable: bool,
    dirty: bool,
    pinned: bool,
) -> bool {
    if !closable || dirty || pinned {
        return false;
    }

    index_matches_workspace_bulk_mode(mode, index)
}

fn should_count_skipped_dirty_for_workspace_bulk_mode(
    mode: WorkspaceBulkCloseMode,
    index: usize,
    closable: bool,
    dirty: bool,
    pinned: bool,
) -> bool {
    if mode == WorkspaceBulkCloseMode::Clean {
        return false;
    }

    closable && dirty && !pinned && index_matches_workspace_bulk_mode(mode, index)
}

fn index_matches_workspace_bulk_mode(mode: WorkspaceBulkCloseMode, index: usize) -> bool {
    match mode {
        WorkspaceBulkCloseMode::Others { keep_index } => index != keep_index,
        WorkspaceBulkCloseMode::Right { from_index } => index > from_index,
        WorkspaceBulkCloseMode::Left { from_index } => index < from_index,
        WorkspaceBulkCloseMode::Clean | WorkspaceBulkCloseMode::All => true,
    }
}

fn skipped_dirty_tabs_message(count: usize) -> String {
    format!(
        "Skipped {} tab{} with unsaved changes.",
        count,
        if count == 1 { "" } else { "s" }
    )
}

pub fn window_close_discard_message(dirty_count: usize) -> String {
    format!(
        "Close this window and discard unsaved changes in {} tab{}?",
        dirty_count,
        if dirty_count == 1 { "" } else { "s" }
    )
}

pub fn window_close_discard_prompt(dirty_count: usize) -> WorkspaceWindowClosePrompt {
    WorkspaceWindowClosePrompt {
        title: "Close Window?",
        message: window_close_discard_message(dirty_count),
        discard_button: "Discard & Close",
        cancel_button: "Cancel",
    }
}

fn workspace_window_title(
    base_title: &SharedString,
    metadata: Option<&WorkspaceItemMetadata>,
) -> SharedString {
    let Some(metadata) = metadata else {
        return base_title.clone();
    };

    format_workspace_window_title(
        base_title,
        Some(&metadata.title),
        metadata.tooltip.as_ref(),
        metadata.dirty,
    )
}

fn format_workspace_window_title(
    base_title: &SharedString,
    title: Option<&SharedString>,
    tooltip: Option<&SharedString>,
    dirty: bool,
) -> SharedString {
    let Some(title) = title else {
        return base_title.clone();
    };

    let title = if title.is_empty() {
        tooltip.unwrap_or(base_title)
    } else {
        title
    };
    let dirty_marker = if dirty { "*" } else { "" };
    format!("{title}{dirty_marker} - {base_title}").into()
}

pub fn force_close_active_tab(
    workspace_controller: &Entity<WorkspaceController>,
    window: &mut Window,
    cx: &mut App,
) -> WorkspaceCloseResult {
    workspace_controller.update(cx, |workspace, cx| {
        workspace.force_close_active_tab(window, cx)
    })
}

pub fn force_close_active_tab_and_dialog(
    workspace_controller: &Entity<WorkspaceController>,
    window: &mut Window,
    cx: &mut App,
) -> WorkspaceCloseResult {
    let close_result = force_close_active_tab(workspace_controller, window, cx);
    if close_result == WorkspaceCloseResult::Closed {
        window.close_dialog(cx);
    }
    close_result
}

#[cfg(test)]
mod tests {
    use gpui::SharedString;

    use super::{
        WorkspaceBulkCloseMode, WorkspaceCloseResult, WorkspaceWindowCloseIntent,
        format_workspace_window_title, should_close_for_workspace_bulk_mode,
        should_count_skipped_dirty_for_workspace_bulk_mode, skipped_dirty_tabs_message,
        window_close_discard_message, window_close_discard_prompt,
        window_close_intent_for_dirty_count, workspace_close_result,
    };

    #[test]
    fn window_close_without_dirty_items_closes_now() {
        assert_eq!(
            window_close_intent_for_dirty_count(0),
            WorkspaceWindowCloseIntent::CloseNow
        );
    }

    #[test]
    fn window_close_with_dirty_items_requires_discard_confirmation() {
        assert_eq!(
            window_close_intent_for_dirty_count(3),
            WorkspaceWindowCloseIntent::ConfirmDiscard { dirty_count: 3 }
        );
    }

    #[test]
    fn workspace_close_result_tracks_dock_close_status() {
        assert_eq!(workspace_close_result(true), WorkspaceCloseResult::Closed);
        assert_eq!(
            workspace_close_result(false),
            WorkspaceCloseResult::NotClosed
        );
    }

    #[test]
    fn workspace_bulk_close_skips_dirty_pinned_and_blocked_items() {
        assert!(should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            false,
            false
        ));
        assert!(!should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            false,
            false,
            false
        ));
        assert!(!should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            true,
            false
        ));
        assert!(!should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            false,
            true
        ));
    }

    #[test]
    fn workspace_bulk_close_respects_relative_modes() {
        assert!(!should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Others { keep_index: 1 },
            1,
            true,
            false,
            false
        ));
        assert!(should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Others { keep_index: 1 },
            2,
            true,
            false,
            false
        ));
        assert!(should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Right { from_index: 1 },
            2,
            true,
            false,
            false
        ));
        assert!(!should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Right { from_index: 1 },
            1,
            true,
            false,
            false
        ));
        assert!(should_close_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Left { from_index: 2 },
            1,
            true,
            false,
            false
        ));
    }

    #[test]
    fn workspace_bulk_close_counts_skipped_dirty_targets() {
        assert!(should_count_skipped_dirty_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            true,
            false
        ));
        assert!(!should_count_skipped_dirty_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            false,
            false
        ));
        assert!(!should_count_skipped_dirty_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::All,
            0,
            true,
            true,
            true
        ));
        assert!(!should_count_skipped_dirty_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Right { from_index: 1 },
            1,
            true,
            true,
            false
        ));
        assert!(!should_count_skipped_dirty_for_workspace_bulk_mode(
            WorkspaceBulkCloseMode::Clean,
            0,
            true,
            true,
            false
        ));
    }

    #[test]
    fn skipped_dirty_tabs_message_handles_plural_tabs() {
        assert_eq!(
            skipped_dirty_tabs_message(1),
            "Skipped 1 tab with unsaved changes."
        );
        assert_eq!(
            skipped_dirty_tabs_message(2),
            "Skipped 2 tabs with unsaved changes."
        );
    }

    #[test]
    fn window_close_discard_message_handles_plural_tabs() {
        assert_eq!(
            window_close_discard_message(1),
            "Close this window and discard unsaved changes in 1 tab?"
        );
        assert_eq!(
            window_close_discard_message(3),
            "Close this window and discard unsaved changes in 3 tabs?"
        );
    }

    #[test]
    fn window_close_discard_prompt_describes_destructive_action() {
        let prompt = window_close_discard_prompt(2);

        assert_eq!(prompt.title, "Close Window?");
        assert_eq!(
            prompt.message,
            "Close this window and discard unsaved changes in 2 tabs?"
        );
        assert_eq!(prompt.discard_button, "Discard & Close");
        assert_eq!(prompt.cancel_button, "Cancel");
    }

    #[test]
    fn window_title_without_active_item_uses_base_title() {
        let base_title = SharedString::from("ZQLZ");

        assert_eq!(
            format_workspace_window_title(&base_title, None, None, false),
            SharedString::from("ZQLZ")
        );
    }

    #[test]
    fn window_title_marks_dirty_active_item() {
        let base_title = SharedString::from("ZQLZ");
        let title = SharedString::from("scratch.sql");

        assert_eq!(
            format_workspace_window_title(&base_title, Some(&title), None, true),
            SharedString::from("scratch.sql* - ZQLZ")
        );
    }

    #[test]
    fn window_title_uses_tooltip_when_active_item_title_empty() {
        let base_title = SharedString::from("ZQLZ");
        let title = SharedString::from("");
        let tooltip = SharedString::from("postgres://local");

        assert_eq!(
            format_workspace_window_title(&base_title, Some(&title), Some(&tooltip), false),
            SharedString::from("postgres://local - ZQLZ")
        );
    }
}
