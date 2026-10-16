//! Server-side sorting from a column header click.

use gpui::*;
use uuid::Uuid;

use crate::components::TableViewerPanel;

/// The viewer reloads with the new ORDER BY rather than sorting client-side.
pub(in crate::main_view) fn handle_sort_column_event(
    _connection_id: Uuid,
    table_name: &str,
    column_name: &str,
    direction: crate::components::SortDirection,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "SortColumn event: table={}, column={}, direction={:?}",
        table_name,
        column_name,
        direction
    );

    viewer_entity.update(cx, |panel, cx| {
        panel.apply_sort(column_name.to_string(), direction, window, cx);
    });
}
