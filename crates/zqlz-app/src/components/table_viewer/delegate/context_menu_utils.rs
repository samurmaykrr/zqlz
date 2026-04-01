use std::collections::HashSet;

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
