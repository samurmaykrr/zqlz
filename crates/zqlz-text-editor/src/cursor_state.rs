use gpui::Task;
use std::time::{Duration, Instant};

pub(crate) struct EditorCursorState {
    last_activity: Instant,
    visible: bool,
    blink_task: Task<anyhow::Result<()>>,
    blink_running: bool,
}

impl Default for EditorCursorState {
    fn default() -> Self {
        Self {
            last_activity: Instant::now(),
            visible: true,
            blink_task: Task::ready(Ok(())),
            blink_running: false,
        }
    }
}

impl EditorCursorState {
    pub(crate) fn visible(&self) -> bool {
        self.visible
    }

    pub(crate) fn record_activity(&mut self) {
        self.last_activity = Instant::now();
        self.visible = true;
    }

    pub(crate) fn blink_running(&self) -> bool {
        self.blink_running
    }

    pub(crate) fn mark_blink_running(&mut self) {
        self.blink_running = true;
    }

    pub(crate) fn store_blink_task(&mut self, task: Task<anyhow::Result<()>>) {
        self.blink_task = task;
    }

    pub(crate) fn stop_blinking(&mut self) {
        self.visible = true;
        self.blink_running = false;
    }

    pub(crate) fn activity_within(&self, duration: Duration) -> bool {
        self.last_activity.elapsed() < duration
    }

    pub(crate) fn ensure_visible(&mut self) -> bool {
        if self.visible {
            return false;
        }

        self.visible = true;
        true
    }

    pub(crate) fn toggle_visible(&mut self) {
        self.visible = !self.visible;
    }
}

#[cfg(test)]
mod tests {
    use super::EditorCursorState;
    use std::time::Duration;

    #[test]
    fn cursor_activity_forces_visible_and_counts_as_recent() {
        let mut state = EditorCursorState::default();
        state.toggle_visible();

        state.record_activity();

        assert!(state.visible());
        assert!(state.activity_within(Duration::from_secs(1)));
    }

    #[test]
    fn blink_lifecycle_tracks_running_and_visibility() {
        let mut state = EditorCursorState::default();

        state.mark_blink_running();
        assert!(state.blink_running());

        state.toggle_visible();
        assert!(!state.visible());
        assert!(state.ensure_visible());
        assert!(state.visible());
        assert!(!state.ensure_visible());

        state.stop_blinking();
        assert!(state.visible());
        assert!(!state.blink_running());
    }
}
