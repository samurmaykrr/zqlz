//! Main-window helpers and actions.

use gpui::{
    AnyWindowHandle, App, Entity, Global, Window, WindowBounds, WindowId, WindowKind,
    WindowOptions, px, size,
};
use std::collections::HashMap;
use zqlz_ui::widgets::{Root, TitleBar};

use crate::workspace::WorkspaceController;

pub struct WindowManager {
    opened_main_windows: usize,
    close_requested_main_windows: usize,
    workspaces_by_window: HashMap<WindowId, Entity<WorkspaceController>>,
    active_main_window: Option<WindowId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WindowManagerSnapshot {
    pub opened_main_windows: usize,
    pub close_requested_main_windows: usize,
    pub registered_main_windows: usize,
    pub active_main_window: Option<u64>,
}

impl WindowManager {
    fn new() -> Self {
        Self {
            opened_main_windows: 0,
            close_requested_main_windows: 0,
            workspaces_by_window: HashMap::new(),
            active_main_window: None,
        }
    }

    fn record_main_window_opened(&mut self) {
        self.opened_main_windows += 1;
    }

    fn record_main_window_close_requested(&mut self) {
        self.close_requested_main_windows += 1;
    }

    fn register_workspace(
        &mut self,
        window: AnyWindowHandle,
        workspace: Entity<WorkspaceController>,
    ) {
        let window_id = window.window_id();
        self.active_main_window = Some(window_id);
        self.workspaces_by_window.insert(window_id, workspace);
    }

    fn unregister_workspace(&mut self, window: AnyWindowHandle) {
        let window_id = window.window_id();
        self.workspaces_by_window.remove(&window_id);
        self.active_main_window = active_window_after_unregister(
            self.active_main_window,
            window_id,
            self.workspaces_by_window.keys().copied(),
        );
    }

    fn mark_active_workspace(&mut self, window: AnyWindowHandle) {
        let window_id = window.window_id();
        if self.workspaces_by_window.contains_key(&window_id) {
            self.active_main_window = Some(window_id);
        }
    }

    #[allow(dead_code)]
    fn active_workspace(&self) -> Option<Entity<WorkspaceController>> {
        let window_id = self.active_main_window?;
        self.workspaces_by_window.get(&window_id).cloned()
    }

    #[allow(dead_code)]
    fn workspace_for_window(&self, window: AnyWindowHandle) -> Option<Entity<WorkspaceController>> {
        self.workspaces_by_window.get(&window.window_id()).cloned()
    }

    pub fn snapshot(&self) -> WindowManagerSnapshot {
        WindowManagerSnapshot {
            opened_main_windows: self.opened_main_windows,
            close_requested_main_windows: self.close_requested_main_windows,
            registered_main_windows: self.workspaces_by_window.len(),
            active_main_window: self.active_main_window.map(|window_id| window_id.as_u64()),
        }
    }
}

impl Global for WindowManager {}

fn active_window_after_unregister(
    active_window: Option<WindowId>,
    removed_window: WindowId,
    remaining_windows: impl IntoIterator<Item = WindowId>,
) -> Option<WindowId> {
    if active_window == Some(removed_window) {
        remaining_windows.into_iter().next()
    } else {
        active_window
    }
}

pub fn init(cx: &mut App) {
    cx.set_global(WindowManager::new());
}

pub fn main_window_title() -> &'static str {
    if cfg!(debug_assertions) {
        "ZQLZ - Database IDE [DEBUG BUILD]"
    } else {
        "ZQLZ - Database IDE"
    }
}

pub fn set_main_window_title(window: &mut Window, title: impl AsRef<str>) {
    window.set_window_title(title.as_ref());
}

pub fn register_main_workspace(
    window: &mut Window,
    workspace: Entity<WorkspaceController>,
    cx: &mut App,
) {
    let manager = cx.global_mut::<WindowManager>();
    manager.register_workspace(window.window_handle(), workspace);
}

pub fn mark_active_main_workspace(window: &mut Window, cx: &mut App) {
    let manager = cx.global_mut::<WindowManager>();
    manager.mark_active_workspace(window.window_handle());
}

#[allow(dead_code)]
pub fn active_main_workspace(cx: &App) -> Option<Entity<WorkspaceController>> {
    cx.try_global::<WindowManager>()
        .and_then(WindowManager::active_workspace)
}

#[allow(dead_code)]
pub fn workspace_for_main_window(
    window: &mut Window,
    cx: &App,
) -> Option<Entity<WorkspaceController>> {
    cx.try_global::<WindowManager>()
        .and_then(|manager| manager.workspace_for_window(window.window_handle()))
}

pub fn main_window_options(cx: &mut App) -> WindowOptions {
    let initial_window_size = if cfg!(target_os = "windows") {
        size(px(1100.0), px(720.0))
    } else {
        size(px(1280.0), px(800.0))
    };

    WindowOptions {
        titlebar: Some(TitleBar::title_bar_options()),
        window_bounds: Some(WindowBounds::centered(initial_window_size, cx)),
        window_min_size: Some(size(px(800.0), px(600.0))),
        kind: WindowKind::Normal,
        ..Default::default()
    }
}

pub fn open_main_window(
    cx: &mut App,
    build_root: impl FnOnce(&mut Window, &mut App) -> Entity<Root> + 'static,
) -> anyhow::Result<()> {
    let window_options = main_window_options(cx);

    cx.spawn(async move |cx| {
        cx.open_window(window_options, |window, cx| {
            window.activate_window();
            set_main_window_title(window, main_window_title());
            let manager = cx.global_mut::<WindowManager>();
            manager.record_main_window_opened();
            let snapshot = manager.snapshot();
            tracing::debug!(
                opened_main_windows = snapshot.opened_main_windows,
                close_requested_main_windows = snapshot.close_requested_main_windows,
                "Main window state updated"
            );
            build_root(window, cx)
        })?;

        anyhow::Ok(())
    })
    .detach_and_log_err(cx);

    tracing::info!("Main window opened successfully");

    Ok(())
}

pub fn close_main_window(window: &mut Window, cx: &mut App) {
    let manager = cx.global_mut::<WindowManager>();
    manager.record_main_window_close_requested();
    manager.unregister_workspace(window.window_handle());
    let snapshot = manager.snapshot();
    tracing::debug!(
        opened_main_windows = snapshot.opened_main_windows,
        close_requested_main_windows = snapshot.close_requested_main_windows,
        "Main window close requested"
    );
    window.remove_window();
}

pub fn minimize_main_window(window: &mut Window) {
    tracing::debug!("Minimizing main window");
    window.minimize_window();
}

pub fn zoom_main_window(window: &mut Window) {
    tracing::debug!("Zooming main window");
    window.zoom_window();
}

#[cfg(test)]
mod tests {
    use super::{WindowManager, WindowManagerSnapshot, active_window_after_unregister};
    use gpui::WindowId;

    #[test]
    fn snapshot_reports_window_counts() {
        let mut manager = WindowManager::new();
        manager.record_main_window_opened();
        manager.record_main_window_opened();
        manager.record_main_window_close_requested();

        assert_eq!(
            manager.snapshot(),
            WindowManagerSnapshot {
                opened_main_windows: 2,
                close_requested_main_windows: 1,
                registered_main_windows: 0,
                active_main_window: None,
            }
        );
    }

    #[test]
    fn unregister_active_workspace_selects_remaining_window() {
        let first = WindowId::from(1);
        let second = WindowId::from(2);

        assert_eq!(
            active_window_after_unregister(Some(second), second, [first]),
            Some(first)
        );
        assert_eq!(
            active_window_after_unregister(Some(first), second, [first]),
            Some(first)
        );
        assert_eq!(
            active_window_after_unregister(Some(second), second, []),
            None
        );
    }
}
