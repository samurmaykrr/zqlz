use crate::{LargeFilePolicyConfig, ResolvedLargeFilePolicy};

#[derive(Default)]
pub(crate) struct EditorBehaviorState {
    autofocus_on_open: bool,
    soft_wrap: bool,
    auto_indent_enabled: bool,
    large_file_policy: LargeFilePolicyConfig,
}

impl EditorBehaviorState {
    pub(crate) fn new() -> Self {
        Self {
            autofocus_on_open: false,
            soft_wrap: false,
            auto_indent_enabled: true,
            large_file_policy: LargeFilePolicyConfig::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn autofocus_on_open(&self) -> bool {
        self.autofocus_on_open
    }

    pub(crate) fn set_autofocus_on_open(&mut self, autofocus_on_open: bool) {
        self.autofocus_on_open = autofocus_on_open;
    }

    pub(crate) fn take_autofocus_on_open(&mut self) -> bool {
        let autofocus_on_open = self.autofocus_on_open;
        self.autofocus_on_open = false;
        autofocus_on_open
    }

    pub(crate) fn soft_wrap(&self) -> bool {
        self.soft_wrap
    }

    pub(crate) fn set_soft_wrap(&mut self, enabled: bool) -> bool {
        if self.soft_wrap == enabled {
            return false;
        }

        self.soft_wrap = enabled;
        true
    }

    pub(crate) fn auto_indent_enabled(&self) -> bool {
        self.auto_indent_enabled
    }

    pub(crate) fn set_auto_indent_enabled(&mut self, enabled: bool) -> bool {
        if self.auto_indent_enabled == enabled {
            return false;
        }

        self.auto_indent_enabled = enabled;
        true
    }

    pub(crate) fn set_large_file_thresholds(
        &mut self,
        line_threshold: usize,
        byte_threshold: usize,
    ) {
        self.large_file_policy = LargeFilePolicyConfig {
            reduced_semantic_line_threshold: line_threshold,
            reduced_semantic_byte_threshold: byte_threshold,
            plain_text_line_threshold: line_threshold.saturating_mul(5).max(line_threshold),
            plain_text_byte_threshold: byte_threshold.saturating_mul(4).max(byte_threshold),
        };
    }

    pub(crate) fn set_large_file_policy_thresholds(
        &mut self,
        reduced_semantic_line_threshold: usize,
        reduced_semantic_byte_threshold: usize,
        plain_text_line_threshold: usize,
        plain_text_byte_threshold: usize,
    ) {
        self.large_file_policy = LargeFilePolicyConfig {
            reduced_semantic_line_threshold,
            reduced_semantic_byte_threshold,
            plain_text_line_threshold: plain_text_line_threshold
                .max(reduced_semantic_line_threshold),
            plain_text_byte_threshold: plain_text_byte_threshold
                .max(reduced_semantic_byte_threshold),
        };
    }

    pub(crate) fn large_file_policy(
        &self,
        line_count: usize,
        byte_count: usize,
    ) -> ResolvedLargeFilePolicy {
        self.large_file_policy.resolve(line_count, byte_count)
    }
}

#[cfg(test)]
mod tests {
    use super::EditorBehaviorState;

    #[test]
    fn autofocus_is_one_shot() {
        let mut state = EditorBehaviorState::new();

        state.set_autofocus_on_open(true);

        assert!(state.autofocus_on_open());
        assert!(state.take_autofocus_on_open());
        assert!(!state.autofocus_on_open());
        assert!(!state.take_autofocus_on_open());
    }

    #[test]
    fn setters_report_changed_state() {
        let mut state = EditorBehaviorState::new();

        assert!(!state.set_soft_wrap(false));
        assert!(state.set_soft_wrap(true));
        assert!(state.soft_wrap());

        assert!(!state.set_auto_indent_enabled(true));
        assert!(state.set_auto_indent_enabled(false));
        assert!(!state.auto_indent_enabled());
    }

    #[test]
    fn large_file_threshold_helpers_preserve_policy_ordering() {
        let mut state = EditorBehaviorState::new();

        state.set_large_file_thresholds(10, 100);
        assert_eq!(
            state.large_file_policy(10, 0).tier,
            crate::LargeFilePolicyTier::ReducedSemantic
        );
        assert_eq!(
            state.large_file_policy(50, 0).tier,
            crate::LargeFilePolicyTier::PlainText
        );

        state.set_large_file_policy_thresholds(20, 200, 5, 50);
        assert_eq!(
            state.large_file_policy(20, 0).tier,
            crate::LargeFilePolicyTier::PlainText
        );
    }
}
