//! Table operation modules.
//!
//! This module contains operations that can be performed on database tables:
//! - Creating new tables
//! - Opening tables
//! - Designing table structure
//! - Deleting tables
//! - Emptying tables (truncate)
//! - Duplicating tables
//! - Renaming tables
//! - Importing and exporting data

mod create;
mod delete;
pub(in crate::main_view) mod design;
mod duplicate;
mod empty;
mod import_export;
mod open;
mod rename;

use gpui::{AnyWindowHandle, AppContext as _, AsyncApp, Window};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

/// Surface a table-operation failure to the user from a detached background task.
///
/// These operations run inside dialog `on_ok` callbacks, where the only handle
/// back to the UI is the window handle captured before the spawn.
pub(in crate::main_view) fn notify_table_operation_error(
    cx: &AsyncApp,
    window_handle: AnyWindowHandle,
    message: String,
) {
    let mut cx = cx.clone();
    if let Err(error) = cx.update_window(window_handle, |_, window: &mut Window, cx| {
        window.push_notification(Notification::error(message), cx);
    }) {
        tracing::warn!(%error, "Window no longer available while reporting table operation failure");
    }
}
