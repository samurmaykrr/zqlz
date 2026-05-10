#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorShapeStyle {
    Block,
    Line,
    Underline,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EditorAppearance {
    #[default]
    Default,
    QueryConsole,
}

pub(crate) struct EditorRenderSettings {
    appearance: EditorAppearance,
    show_line_numbers: bool,
    highlight_current_line: bool,
    show_inline_diagnostics: bool,
    show_folding: bool,
    highlight_enabled: bool,
    bracket_matching_enabled: bool,
    relative_line_numbers: bool,
    show_gutter_diagnostics: bool,
    cursor_shape: CursorShapeStyle,
    cursor_blink_enabled: bool,
    rounded_selection: bool,
    selection_highlight_enabled: bool,
}

impl Default for EditorRenderSettings {
    fn default() -> Self {
        Self {
            appearance: EditorAppearance::Default,
            show_line_numbers: true,
            highlight_current_line: true,
            show_inline_diagnostics: true,
            show_folding: true,
            highlight_enabled: true,
            bracket_matching_enabled: true,
            relative_line_numbers: false,
            show_gutter_diagnostics: true,
            cursor_shape: CursorShapeStyle::Line,
            cursor_blink_enabled: true,
            rounded_selection: true,
            selection_highlight_enabled: true,
        }
    }
}

impl EditorRenderSettings {
    pub(crate) fn appearance(&self) -> EditorAppearance {
        self.appearance
    }

    pub(crate) fn show_line_numbers(&self) -> bool {
        self.show_line_numbers
    }

    pub(crate) fn highlight_current_line(&self) -> bool {
        self.highlight_current_line
    }

    pub(crate) fn show_inline_diagnostics(&self) -> bool {
        self.show_inline_diagnostics
    }

    pub(crate) fn show_folding(&self) -> bool {
        self.show_folding
    }

    pub(crate) fn highlight_enabled(&self) -> bool {
        self.highlight_enabled
    }

    pub(crate) fn bracket_matching_enabled(&self) -> bool {
        self.bracket_matching_enabled
    }

    pub(crate) fn relative_line_numbers(&self) -> bool {
        self.relative_line_numbers
    }

    pub(crate) fn show_gutter_diagnostics(&self) -> bool {
        self.show_gutter_diagnostics
    }

    pub(crate) fn cursor_shape(&self) -> CursorShapeStyle {
        self.cursor_shape
    }

    pub(crate) fn cursor_blink_enabled(&self) -> bool {
        self.cursor_blink_enabled
    }

    pub(crate) fn rounded_selection(&self) -> bool {
        self.rounded_selection
    }

    pub(crate) fn selection_highlight_enabled(&self) -> bool {
        self.selection_highlight_enabled
    }

    pub(crate) fn set_appearance(&mut self, appearance: EditorAppearance) -> bool {
        if self.appearance == appearance {
            return false;
        }
        self.appearance = appearance;
        true
    }

    pub(crate) fn set_show_line_numbers(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.show_line_numbers, enabled)
    }

    pub(crate) fn set_highlight_current_line(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.highlight_current_line, enabled)
    }

    pub(crate) fn set_show_inline_diagnostics(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.show_inline_diagnostics, enabled)
    }

    pub(crate) fn set_show_folding(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.show_folding, enabled)
    }

    pub(crate) fn set_highlight_enabled(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.highlight_enabled, enabled)
    }

    pub(crate) fn set_bracket_matching_enabled(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.bracket_matching_enabled, enabled)
    }

    pub(crate) fn set_relative_line_numbers(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.relative_line_numbers, enabled)
    }

    pub(crate) fn set_show_gutter_diagnostics(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.show_gutter_diagnostics, enabled)
    }

    pub(crate) fn set_cursor_shape(&mut self, shape: CursorShapeStyle) -> bool {
        if self.cursor_shape == shape {
            return false;
        }
        self.cursor_shape = shape;
        true
    }

    pub(crate) fn set_cursor_blink_enabled(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.cursor_blink_enabled, enabled)
    }

    pub(crate) fn set_selection_highlight_enabled(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.selection_highlight_enabled, enabled)
    }

    pub(crate) fn set_rounded_selection(&mut self, enabled: bool) -> bool {
        Self::set_bool(&mut self.rounded_selection, enabled)
    }

    fn set_bool(slot: &mut bool, enabled: bool) -> bool {
        if *slot == enabled {
            return false;
        }
        *slot = enabled;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::EditorRenderSettings;
    use crate::{CursorShapeStyle, EditorAppearance};

    #[test]
    fn render_setting_helpers_report_changed_state() {
        let mut settings = EditorRenderSettings::default();

        assert!(!settings.set_show_line_numbers(true));
        assert!(settings.show_line_numbers());

        assert!(settings.set_show_line_numbers(false));
        assert!(!settings.show_line_numbers());
    }

    #[test]
    fn appearance_and_cursor_helpers_preserve_noop_contract() {
        let mut settings = EditorRenderSettings::default();

        assert!(!settings.set_appearance(EditorAppearance::Default));
        assert!(settings.set_cursor_shape(CursorShapeStyle::Block));
        assert_eq!(settings.cursor_shape(), CursorShapeStyle::Block);
        assert!(!settings.set_cursor_shape(CursorShapeStyle::Block));
    }

    #[test]
    fn read_helpers_preserve_default_contract() {
        let settings = EditorRenderSettings::default();

        assert_eq!(settings.appearance(), EditorAppearance::Default);
        assert!(settings.show_line_numbers());
        assert!(settings.highlight_current_line());
        assert!(settings.show_inline_diagnostics());
        assert!(settings.show_folding());
        assert!(settings.highlight_enabled());
        assert!(settings.bracket_matching_enabled());
        assert!(!settings.relative_line_numbers());
        assert!(settings.show_gutter_diagnostics());
        assert!(settings.cursor_blink_enabled());
        assert!(settings.rounded_selection());
        assert!(settings.selection_highlight_enabled());
    }
}
