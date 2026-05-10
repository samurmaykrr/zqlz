use std::collections::HashSet;
use zqlz_core::{DataEditingFeatureSet, FeatureAvailability};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum TableContextMenuAction {
    EditCells,
    DeleteRows,
    InsertRows,
}

pub(super) fn table_context_menu_action_availability(
    features: &DataEditingFeatureSet,
    action: TableContextMenuAction,
) -> FeatureAvailability {
    match action {
        TableContextMenuAction::EditCells => features.edit_cells.clone(),
        TableContextMenuAction::DeleteRows => features.delete_rows.clone(),
        TableContextMenuAction::InsertRows => features.insert_rows.clone(),
    }
}

pub(super) fn ordered_unique_actual_rows_from_display_rows<F>(
    selected_display_rows: &[usize],
    mut map_display_to_actual: F,
    total_rows: usize,
) -> Vec<usize>
where
    F: FnMut(usize) -> usize,
{
    let mut seen_actual_rows = HashSet::new();

    selected_display_rows
        .iter()
        .filter_map(|display_row| {
            let actual_row = map_display_to_actual(*display_row);
            if actual_row >= total_rows || !seen_actual_rows.insert(actual_row) {
                None
            } else {
                Some(actual_row)
            }
        })
        .collect()
}

pub(super) fn pasted_text_for_selection_index(
    clipboard_lines: &[&str],
    full_clipboard_text: &str,
    index: usize,
) -> Option<String> {
    if clipboard_lines.len() <= 1 {
        Some(full_clipboard_text.to_string())
    } else {
        clipboard_lines.get(index).map(|line| (*line).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::FeatureAvailability;

    fn data_editing_features(
        edit_cells: FeatureAvailability,
        insert_rows: FeatureAvailability,
        delete_rows: FeatureAvailability,
    ) -> DataEditingFeatureSet {
        DataEditingFeatureSet {
            browse_rows: FeatureAvailability::available(),
            edit_cells,
            insert_rows,
            delete_rows,
        }
    }

    #[test]
    fn table_context_menu_action_availability_maps_to_data_editing_features() {
        let features = data_editing_features(
            FeatureAvailability::unavailable("edit blocked"),
            FeatureAvailability::available(),
            FeatureAvailability::unavailable("delete blocked"),
        );

        assert_eq!(
            table_context_menu_action_availability(&features, TableContextMenuAction::EditCells),
            FeatureAvailability::unavailable("edit blocked")
        );
        assert_eq!(
            table_context_menu_action_availability(&features, TableContextMenuAction::InsertRows),
            FeatureAvailability::available()
        );
        assert_eq!(
            table_context_menu_action_availability(&features, TableContextMenuAction::DeleteRows),
            FeatureAvailability::unavailable("delete blocked")
        );
    }

    #[test]
    fn ordered_unique_rows_preserve_display_order() {
        let selected_display_rows = vec![0, 1, 2, 3];
        let actual_by_display = [7, 3, 7, 9];

        let ordered_actual_rows = ordered_unique_actual_rows_from_display_rows(
            &selected_display_rows,
            |display_row| actual_by_display[display_row],
            16,
        );

        assert_eq!(ordered_actual_rows, vec![7, 3, 9]);
    }

    #[test]
    fn ordered_unique_rows_skip_out_of_bounds() {
        let selected_display_rows = vec![0, 1, 2];
        let actual_by_display = [1, 100, 2];

        let ordered_actual_rows = ordered_unique_actual_rows_from_display_rows(
            &selected_display_rows,
            |display_row| actual_by_display[display_row],
            3,
        );

        assert_eq!(ordered_actual_rows, vec![1, 2]);
    }

    #[test]
    fn pasted_text_reuses_single_line_for_all_rows() {
        let clipboard = "same value";
        let lines: Vec<&str> = clipboard.lines().collect();

        assert_eq!(
            pasted_text_for_selection_index(&lines, clipboard, 0),
            Some("same value".to_string())
        );
        assert_eq!(
            pasted_text_for_selection_index(&lines, clipboard, 4),
            Some("same value".to_string())
        );
    }

    #[test]
    fn pasted_text_maps_multiline_by_index() {
        let clipboard = "first\nsecond";
        let lines: Vec<&str> = clipboard.lines().collect();

        assert_eq!(
            pasted_text_for_selection_index(&lines, clipboard, 0),
            Some("first".to_string())
        );
        assert_eq!(
            pasted_text_for_selection_index(&lines, clipboard, 1),
            Some("second".to_string())
        );
        assert_eq!(pasted_text_for_selection_index(&lines, clipboard, 2), None);
    }
}
