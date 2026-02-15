//! Completion menu actions for Zed Editor in ZQLZ
//!
//! The unified `CompletionMenu<E>` lives in `zqlz-ui`. This module only defines
//! the action types used by `QueryEditor` for keyboard navigation and re-exports
//! the unified menu type.

use gpui::actions;

actions!(zed_completion, [Confirm, Cancel, SelectUp, SelectDown]);

pub use zqlz_ui::widgets::input::popovers::{CompletionMenu, CompletionMenuEditor};
