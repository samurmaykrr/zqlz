//! Versioning facade for app-shell entry points.
//!
//! This keeps version-history UI event call sites stable while app-only dock and
//! window concerns remain in `main_view`.

use gpui::*;
use uuid::Uuid;
use zqlz_versioning::DatabaseObjectType;

use crate::main_view::MainView;

impl MainView {
    /// Routes version-history panel orchestration through the versioning app boundary.
    pub(super) fn versioning_facade_show_version_history(
        &mut self,
        connection_id: Uuid,
        object_id: String,
        object_schema: Option<String>,
        object_type: DatabaseObjectType,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.show_version_history(
            connection_id,
            object_id,
            object_schema,
            object_type,
            window,
            cx,
        );
    }
}
