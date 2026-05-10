use crate::SyntaxRefreshStrategy;

const DEFAULT_REDUCED_SEMANTIC_LINE_THRESHOLD: usize = 20_000;
const DEFAULT_REDUCED_SEMANTIC_BYTE_THRESHOLD: usize = 2 * 1024 * 1024;
const DEFAULT_PLAIN_TEXT_LINE_THRESHOLD: usize = 100_000;
const DEFAULT_PLAIN_TEXT_BYTE_THRESHOLD: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LargeFilePolicyTier {
    Full,
    ReducedSemantic,
    PlainText,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LargeFilePolicyConfig {
    pub(crate) reduced_semantic_line_threshold: usize,
    pub(crate) reduced_semantic_byte_threshold: usize,
    pub(crate) plain_text_line_threshold: usize,
    pub(crate) plain_text_byte_threshold: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedLargeFilePolicy {
    pub tier: LargeFilePolicyTier,
    pub syntax_highlighting_enabled: bool,
    pub folding_enabled: bool,
    pub diagnostics_enabled: bool,
    pub completions_enabled: bool,
    pub hover_enabled: bool,
    pub reference_highlights_enabled: bool,
    pub triggered_by_lines: bool,
    pub triggered_by_bytes: bool,
}

impl Default for LargeFilePolicyConfig {
    fn default() -> Self {
        Self {
            reduced_semantic_line_threshold: DEFAULT_REDUCED_SEMANTIC_LINE_THRESHOLD,
            reduced_semantic_byte_threshold: DEFAULT_REDUCED_SEMANTIC_BYTE_THRESHOLD,
            plain_text_line_threshold: DEFAULT_PLAIN_TEXT_LINE_THRESHOLD,
            plain_text_byte_threshold: DEFAULT_PLAIN_TEXT_BYTE_THRESHOLD,
        }
    }
}

impl LargeFilePolicyConfig {
    pub(crate) fn resolve(&self, line_count: usize, byte_count: usize) -> ResolvedLargeFilePolicy {
        let plain_text_triggered_by_lines = line_count >= self.plain_text_line_threshold;
        let plain_text_triggered_by_bytes = byte_count >= self.plain_text_byte_threshold;
        if plain_text_triggered_by_lines || plain_text_triggered_by_bytes {
            return ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::PlainText,
                syntax_highlighting_enabled: false,
                folding_enabled: false,
                diagnostics_enabled: false,
                completions_enabled: false,
                hover_enabled: false,
                reference_highlights_enabled: false,
                triggered_by_lines: plain_text_triggered_by_lines,
                triggered_by_bytes: plain_text_triggered_by_bytes,
            };
        }

        let reduced_triggered_by_lines = line_count >= self.reduced_semantic_line_threshold;
        let reduced_triggered_by_bytes = byte_count >= self.reduced_semantic_byte_threshold;
        if reduced_triggered_by_lines || reduced_triggered_by_bytes {
            return ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::ReducedSemantic,
                syntax_highlighting_enabled: true,
                folding_enabled: true,
                diagnostics_enabled: false,
                completions_enabled: true,
                hover_enabled: true,
                reference_highlights_enabled: false,
                triggered_by_lines: reduced_triggered_by_lines,
                triggered_by_bytes: reduced_triggered_by_bytes,
            };
        }

        ResolvedLargeFilePolicy {
            tier: LargeFilePolicyTier::Full,
            syntax_highlighting_enabled: true,
            folding_enabled: true,
            diagnostics_enabled: true,
            completions_enabled: true,
            hover_enabled: true,
            reference_highlights_enabled: true,
            triggered_by_lines: false,
            triggered_by_bytes: false,
        }
    }
}

impl ResolvedLargeFilePolicy {
    pub fn full() -> Self {
        LargeFilePolicyConfig::default().resolve(0, 0)
    }

    pub(crate) fn allow_async_provider_requests(&self) -> bool {
        self.tier == LargeFilePolicyTier::Full
    }
}

pub(crate) fn syntax_refresh_strategy_for_policy(
    policy: ResolvedLargeFilePolicy,
    visible_byte_range: Option<std::ops::Range<usize>>,
) -> SyntaxRefreshStrategy {
    if !policy.syntax_highlighting_enabled {
        return SyntaxRefreshStrategy::Disabled;
    }

    match policy.tier {
        LargeFilePolicyTier::PlainText => SyntaxRefreshStrategy::Disabled,
        LargeFilePolicyTier::ReducedSemantic => visible_byte_range
            .map(SyntaxRefreshStrategy::VisibleRange)
            .unwrap_or(SyntaxRefreshStrategy::FullDocument),
        LargeFilePolicyTier::Full => SyntaxRefreshStrategy::FullDocument,
    }
}
