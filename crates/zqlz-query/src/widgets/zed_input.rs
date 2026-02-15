//! ZedInput - Single-line editor-backed input component
//!
//! A single-line SQL/code input that uses the Zed editor under the hood.
//! Provides syntax highlighting and LSP support while maintaining single-line behavior.
//!
//! This component is designed to replace legacy Input widgets for code/query usage.

use gpui::{
    div, px, AppContext as _, Context, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement as _, IntoElement, ParentElement as _, RenderOnce, SharedString, Styled,
    Window,
};
use std::sync::Arc;
use zqlz_lsp::SqlLsp;
use zqlz_zed_adapter::editor_wrapper::EditorWrapper;

/// Events that ZedInput can emit
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZedInputEvent {
    /// The input value changed
    Change,
    /// Enter was pressed (submit)
    Submit {
        /// Whether secondary enter was pressed (e.g., Shift+Enter)
        secondary: bool,
    },
    /// Focus was gained
    Focus,
    /// Focus was lost
    Blur,
}

/// State for ZedInput - manages the EditorWrapper and value
pub struct ZedInputState {
    /// The underlying editor wrapper
    pub editor: Entity<EditorWrapper>,
    /// Current value
    value: SharedString,
    /// Placeholder text
    placeholder: SharedString,
    /// Whether the input is disabled
    disabled: bool,
    /// Focus handle for the editor
    focus_handle: FocusHandle,
    /// SQL LSP for completions
    sql_lsp: Option<Arc<parking_lot::RwLock<SqlLsp>>>,
}

impl ZedInputState {
    /// Create a new ZedInputState
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editor = cx.new(|cx| EditorWrapper::new(window, cx));
        let focus_handle = cx.focus_handle();

        Self {
            editor,
            value: SharedString::from(""),
            placeholder: SharedString::from(""),
            disabled: false,
            focus_handle,
            sql_lsp: None,
        }
    }

    /// Create with initial value
    pub fn with_value(mut self, value: impl Into<SharedString>) -> Self {
        let val: SharedString = value.into();
        self.set_value(val.clone());
        self
    }

    /// Create with placeholder
    pub fn with_placeholder(mut self, placeholder: impl Into<SharedString>) -> Self {
        self.placeholder = placeholder.into();
        self
    }

    /// Set the SQL LSP for completions
    pub fn set_sql_lsp(&mut self, lsp: Arc<parking_lot::RwLock<SqlLsp>>, cx: &mut Context<Self>) {
        self.sql_lsp = Some(lsp.clone());
        self.editor.update(cx, |editor, _cx| {
            editor.set_sql_lsp(lsp);
        });
    }

    /// Get the current value
    pub fn value(&self) -> SharedString {
        self.value.clone()
    }

    /// Set the value
    pub fn set_value(&mut self, value: SharedString) {
        self.value = value;
    }

    /// Get the placeholder text
    pub fn placeholder(&self) -> &str {
        &self.placeholder
    }

    /// Check if disabled
    pub fn is_disabled(&self) -> bool {
        self.disabled
    }

    /// Set disabled state
    pub fn set_disabled(&mut self, disabled: bool) {
        self.disabled = disabled;
    }

    /// Focus this input
    pub fn focus(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window, cx);
    }

    /// Get the focus handle
    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }
}

impl EventEmitter<ZedInputEvent> for ZedInputState {}

/// ZedInput - Single-line editor-backed input component
///
/// Provides SQL syntax highlighting and LSP support in a single-line format.
/// Designed to replace legacy Input widgets for code/query content.
#[derive(IntoElement)]
pub struct ZedInput {
    state: Entity<ZedInputState>,
}

impl ZedInput {
    /// Create a new ZedInput bound to the given state
    pub fn new(state: &Entity<ZedInputState>) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl RenderOnce for ZedInput {
    fn render(self, window: &mut Window, cx: &mut gpui::App) -> impl IntoElement {
        let state = self.state.read(cx);
        let editor_entity = state.editor.read(cx).editor();

        // Get editor content to sync value
        let current_text = state.editor.read(cx).get_text(cx);
        if current_text.as_str() != state.value.as_str() {
            // Update value from editor
            drop(state);
            self.state.update(cx, |s, _cx| {
                s.value = current_text.into();
            });
        }

        // Use a simple container with border - without border radius for simplicity
        div()
            .relative()
            .h(px(32.0))
            .border_1()
            .bg(gpui::hsla(0.0, 0.0, 1.0, 1.0))
            .border_color(gpui::hsla(0.0, 0.0, 0.0, 0.1))
            .child(editor_entity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[gpui::test]
    fn test_zed_input_creation() {
        let mut cx = gpui::TestAppContext::new();
        cx.update(|window, cx| {
            let state = ZedInputState::new(window, cx);
            assert_eq!(state.value(), "");
            assert_eq!(state.placeholder(), "");
            assert!(!state.is_disabled());
        });
    }

    #[gpui::test]
    fn test_zed_input_with_value() {
        let mut cx = gpui::TestAppContext::new();
        cx.update(|window, cx| {
            let state = ZedInputState::new(window, cx)
                .with_value("SELECT * FROM users")
                .with_placeholder("Enter SQL...");
            assert_eq!(state.value(), "SELECT * FROM users");
            assert_eq!(state.placeholder(), "Enter SQL...");
        });
    }

    #[gpui::test]
    fn test_zed_input_disabled() {
        let mut cx = gpui::TestAppContext::new();
        cx.update(|window, cx| {
            let mut state = ZedInputState::new(window, cx);
            state.set_disabled(true);
            assert!(state.is_disabled());

            state.set_disabled(false);
            assert!(!state.is_disabled());
        });
    }
}
