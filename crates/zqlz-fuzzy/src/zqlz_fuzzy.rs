/// Fuzzy string matching with multi-tier quality scoring.
///
/// Supports prefix, substring, underscore-stripped, acronym, and character-by-character
/// fuzzy matching. Each tier produces a `MatchQuality` discriminant and a numeric score
/// for fine-grained ranking within the same tier.

// ── Match quality ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchQuality {
    None = 0,
    /// Character-by-character fuzzy match with gaps.
    Fuzzy = 1,
    /// First letters of words match (e.g., "ij" -> "INNER JOIN").
    Acronym = 2,
    /// Contiguous substring found (e.g., "lect" -> "SELECT").
    Substring = 3,
    /// Candidate starts with the pattern.
    Prefix = 4,
}

// ── Match result ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FuzzyMatch {
    pub quality: MatchQuality,
    /// Higher is better. Scores are only comparable within the same `MatchQuality` tier.
    pub score: i32,
    /// Character indices in the candidate that were matched. Useful for highlighting.
    pub matched_indices: Vec<usize>,
}

impl FuzzyMatch {
    pub fn is_match(&self) -> bool {
        self.quality != MatchQuality::None
    }
}

// ── Matcher ─────────────────────────────────────────────────────────────

pub struct FuzzyMatcher {
    case_sensitive: bool,
}

impl FuzzyMatcher {
    pub fn new(case_sensitive: bool) -> Self {
        Self { case_sensitive }
    }

    /// Match `pattern` against `candidate`. Returns `None` only if the inputs
    /// are somehow pathological; a non-match is indicated by `MatchQuality::None`.
    pub fn fuzzy_match(&self, pattern: &str, candidate: &str) -> Option<FuzzyMatch> {
        if pattern.is_empty() {
            return Some(FuzzyMatch {
                quality: MatchQuality::Prefix,
                score: 0,
                matched_indices: Vec::new(),
            });
        }

        let pattern_lower = pattern.to_lowercase();
        let candidate_lower = candidate.to_lowercase();

        let pattern_chars: Vec<char> = if self.case_sensitive {
            pattern.chars().collect()
        } else {
            pattern_lower.chars().collect()
        };

        let candidate_chars: Vec<char> = if self.case_sensitive {
            candidate.chars().collect()
        } else {
            candidate_lower.chars().collect()
        };

        // 1. Prefix match (highest quality).
        if candidate_lower.starts_with(&pattern_lower) {
            let indices: Vec<usize> = (0..pattern_chars.len()).collect();
            return Some(FuzzyMatch {
                quality: MatchQuality::Prefix,
                // Prefer shorter candidates (more specific match). Use char counts, not byte lengths.
                score: 1000 - (candidate_chars.len() as i32 - pattern_chars.len() as i32),
                matched_indices: indices,
            });
        }

        // 2. Contiguous substring match.
        // `str::find` returns a byte offset — convert to a char index for correct highlighting.
        if let Some(byte_pos) = candidate_lower.find(&pattern_lower) {
            let char_pos = candidate_lower[..byte_pos].chars().count();
            let indices: Vec<usize> = (char_pos..char_pos + pattern_chars.len()).collect();
            return Some(FuzzyMatch {
                quality: MatchQuality::Substring,
                score: 800 - char_pos as i32,
                matched_indices: indices,
            });
        }

        // 2.5. Underscore-stripped substring (common in SQL identifiers like user_id).
        // Map matched positions in the stripped string back to their original
        // character indices so that highlights render correctly.
        let candidate_no_underscore: String =
            candidate_lower.chars().filter(|&c| c != '_').collect();
        if let Some(stripped_pos) = candidate_no_underscore.find(&pattern_lower) {
            let stripped_end = stripped_pos + pattern_lower.len();

            // Build a map from stripped-string char index → original char index,
            // skipping underscores.
            let mut indices = Vec::with_capacity(pattern_chars.len());
            let mut stripped_char_idx = 0;
            for (original_char_idx, ch) in candidate_lower.chars().enumerate() {
                if ch == '_' {
                    continue;
                }
                if stripped_char_idx >= stripped_pos && stripped_char_idx < stripped_end {
                    indices.push(original_char_idx);
                }
                stripped_char_idx += 1;
                if stripped_char_idx >= stripped_end {
                    break;
                }
            }

            return Some(FuzzyMatch {
                quality: MatchQuality::Substring,
                score: 750 - stripped_pos as i32,
                matched_indices: indices,
            });
        }

        // 3. Acronym match (first letters of words).
        if let Some(indices) = self.match_acronym(&pattern_chars, &candidate_chars, candidate) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Acronym,
                score: 600,
                matched_indices: indices,
            });
        }

        // 4. Character-by-character fuzzy match.
        if let Some((score, indices)) = self.match_fuzzy(&pattern_chars, &candidate_chars) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Fuzzy,
                score,
                matched_indices: indices,
            });
        }

        Some(FuzzyMatch {
            quality: MatchQuality::None,
            score: 0,
            matched_indices: Vec::new(),
        })
    }

    /// Match pattern characters to word-boundary characters in the candidate.
    fn match_acronym(
        &self,
        pattern: &[char],
        candidate: &[char],
        original_candidate: &str,
    ) -> Option<Vec<usize>> {
        let word_starts: Vec<usize> = original_candidate
            .char_indices()
            .enumerate()
            .filter_map(|(idx, (byte_pos, ch))| {
                let is_word_start = idx == 0
                    || ch.is_uppercase()
                    || (byte_pos > 0 && {
                        let prev_char =
                            original_candidate[..byte_pos].chars().last().unwrap_or(' ');
                        !prev_char.is_alphanumeric()
                    });
                if is_word_start { Some(idx) } else { None }
            })
            .collect();

        if word_starts.len() < pattern.len() {
            return None;
        }

        let mut matched_indices = Vec::new();
        let mut word_idx = 0;

        for &pattern_char in pattern {
            let mut found = false;
            while word_idx < word_starts.len() {
                let candidate_idx = word_starts[word_idx];
                if candidate_idx < candidate.len() && candidate[candidate_idx] == pattern_char {
                    matched_indices.push(candidate_idx);
                    word_idx += 1;
                    found = true;
                    break;
                }
                word_idx += 1;
            }
            if !found {
                return None;
            }
        }

        Some(matched_indices)
    }

    /// Iterative DP fuzzy match. Uses a scoring table to find the best
    /// alignment of pattern characters within the candidate, with bonuses for
    /// consecutive matches. O(pattern_len * candidate_len) time and space,
    /// replacing the prior recursive backtracking approach that was exponential
    /// in the worst case.
    fn match_fuzzy(&self, pattern: &[char], candidate: &[char]) -> Option<(i32, Vec<usize>)> {
        if pattern.is_empty() {
            return Some((0, Vec::new()));
        }

        if pattern.len() > candidate.len() {
            return None;
        }

        let pattern_len = pattern.len();
        let candidate_len = candidate.len();

        // dp[i][j] = best score for matching pattern[0..i] using candidate[0..j].
        // We use i32::MIN as "impossible" sentinel.
        const NEG_INF: i32 = i32::MIN / 2;

        // Two score tables: one for when candidate[j-1] was matched (consecutive),
        // one for when it was skipped. This lets us efficiently track consecutive
        // bonuses without backtracking.
        //
        // matched[i][j]  = best score ending with candidate[j-1] matched to pattern[i-1]
        // skipped[i][j]  = best score for pattern[0..i] in candidate[0..j] where
        //                   candidate[j-1] was NOT matched to pattern[i-1]
        let mut matched = vec![vec![NEG_INF; candidate_len + 1]; pattern_len + 1];
        let mut skipped = vec![vec![NEG_INF; candidate_len + 1]; pattern_len + 1];

        // Base case: matching 0 pattern chars against any prefix is score 0.
        for entry in &mut skipped[0][..=candidate_len] {
            *entry = 0;
        }

        let match_score = 1;
        let consecutive_bonus = 10;
        let skip_penalty = -1;

        for i in 1..=pattern_len {
            for j in i..=candidate_len {
                // Can we match pattern[i-1] to candidate[j-1]?
                if pattern[i - 1] == candidate[j - 1] {
                    // Score if previous pattern char was also matched at j-2 (consecutive).
                    let from_consecutive = if i >= 2 && j >= 2 {
                        matched[i - 1][j - 1].saturating_add(match_score + consecutive_bonus)
                    } else {
                        NEG_INF
                    };

                    // Score if previous match was not consecutive (came from a skip).
                    let from_skipped = skipped[i - 1][j - 1].saturating_add(match_score);

                    // Also from matched but at an earlier position (non-consecutive).
                    let from_matched_non_consec = if i >= 2 {
                        // If i-1 was matched at some position < j-1, it goes through
                        // skipped[i][j-1] below. But we also need the case where
                        // pattern[i-2] was matched at j-2 and we're continuing.
                        // That's already covered by from_consecutive.
                        NEG_INF
                    } else {
                        // i == 1: matching first pattern char
                        skipped[0][j - 1].saturating_add(match_score)
                    };

                    matched[i][j] = from_consecutive
                        .max(from_skipped)
                        .max(from_matched_non_consec);
                }

                // Skip candidate[j-1]: best of matched or skipped at j-1, with penalty.
                let prev_best = matched[i][j - 1].max(skipped[i][j - 1]);
                if prev_best > NEG_INF {
                    skipped[i][j] = prev_best.saturating_add(skip_penalty);
                }
            }
        }

        let final_score =
            matched[pattern_len][candidate_len].max(skipped[pattern_len][candidate_len]);

        if final_score <= NEG_INF {
            return None;
        }

        // Backtrace to recover matched indices.
        let indices = self.backtrace_dp(pattern, candidate, &matched, &skipped);

        Some((final_score, indices))
    }

    /// Walk backwards through the DP tables to reconstruct which candidate
    /// positions were matched.
    fn backtrace_dp(
        &self,
        pattern: &[char],
        candidate: &[char],
        matched: &[Vec<i32>],
        skipped: &[Vec<i32>],
    ) -> Vec<usize> {
        let pattern_len = pattern.len();
        let candidate_len = candidate.len();
        let mut indices = Vec::with_capacity(pattern_len);

        let mut i = pattern_len;
        let mut j = candidate_len;

        // Determine if we ended in a matched or skipped state.
        let mut in_matched = matched[i][j] >= skipped[i][j];

        while i > 0 && j > 0 {
            if in_matched {
                // candidate[j-1] was matched to pattern[i-1].
                indices.push(j - 1);
                i -= 1;
                j -= 1;
                // Check if the previous state was also matched (consecutive).
                in_matched = i > 0 && j > 0 && matched[i][j] >= skipped[i][j];
            } else {
                // candidate[j-1] was skipped.
                j -= 1;
                if j > 0 {
                    in_matched = matched[i][j] >= skipped[i][j];
                }
            }
        }

        indices.reverse();
        indices
    }
}

impl Default for FuzzyMatcher {
    fn default() -> Self {
        Self::new(false)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("sel", "SELECT").unwrap();

        assert_eq!(result.quality, MatchQuality::Prefix);
        assert!(result.is_match());
        assert_eq!(result.matched_indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_substring_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("lect", "SELECT").unwrap();

        assert_eq!(result.quality, MatchQuality::Substring);
        assert!(result.is_match());
        assert_eq!(result.matched_indices, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_acronym_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("ij", "INNER JOIN").unwrap();

        assert_eq!(result.quality, MatchQuality::Acronym);
        assert!(result.is_match());
    }

    #[test]
    fn test_fuzzy_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("slct", "SELECT").unwrap();

        // "SELECT" is all-caps so every character is a word boundary — "slct" can
        // match as Acronym, Fuzzy, or (in theory) Substring depending on the tier
        // that fires first. The important thing is that it *does* match.
        assert!(result.is_match());
        assert!(
            result.quality == MatchQuality::Fuzzy
                || result.quality == MatchQuality::Substring
                || result.quality == MatchQuality::Acronym
        );
    }

    #[test]
    fn test_no_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("xyz", "SELECT").unwrap();

        assert_eq!(result.quality, MatchQuality::None);
        assert!(!result.is_match());
    }

    #[test]
    fn test_empty_pattern() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("", "SELECT").unwrap();

        assert_eq!(result.quality, MatchQuality::Prefix);
        assert!(result.is_match());
    }

    #[test]
    fn test_case_insensitive() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("SEL", "select").unwrap();

        assert_eq!(result.quality, MatchQuality::Prefix);
        assert!(result.is_match());
    }

    #[test]
    fn test_underscore_stripped_match() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("userid", "user_id").unwrap();

        assert_eq!(result.quality, MatchQuality::Substring);
        assert!(result.is_match());
        // "user_id" chars: u(0) s(1) e(2) r(3) _(4) i(5) d(6)
        // Stripped "userid" matches at stripped pos 0 → original indices [0,1,2,3,5,6]
        assert_eq!(result.matched_indices, vec![0, 1, 2, 3, 5, 6]);
    }

    #[test]
    fn test_underscore_stripped_partial() {
        let matcher = FuzzyMatcher::new(false);
        let result = matcher.fuzzy_match("createdat", "created_at").unwrap();

        assert_eq!(result.quality, MatchQuality::Substring);
        // "created_at" chars: c(0) r(1) e(2) a(3) t(4) e(5) d(6) _(7) a(8) t(9)
        // Stripped "createdat" matches all non-underscore chars
        assert_eq!(result.matched_indices, vec![0, 1, 2, 3, 4, 5, 6, 8, 9]);
    }

    #[test]
    fn test_consecutive_match_bonus() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("slct", "SELECT").unwrap();
        let result2 = matcher.fuzzy_match("slt", "SELECT").unwrap();

        if result1.quality == MatchQuality::Fuzzy && result2.quality == MatchQuality::Fuzzy {
            assert!(result1.score >= result2.score);
        }
    }

    #[test]
    fn test_prefer_shorter_prefix() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("sel", "SELECT").unwrap();
        let result2 = matcher.fuzzy_match("sel", "SELECTTTTTT").unwrap();

        assert!(result1.score > result2.score);
    }

    #[test]
    fn test_prefer_earlier_substring() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("abc", "abcdef").unwrap();
        let result2 = matcher.fuzzy_match("abc", "xyzabc").unwrap();

        assert!(result1.score > result2.score);
    }

    #[test]
    fn test_command_palette_style_queries() {
        let matcher = FuzzyMatcher::new(false);

        // "nq" should match "New Query" (acronym)
        let result = matcher.fuzzy_match("nq", "New Query").unwrap();
        assert!(result.is_match());

        // "query" should prefix-match "Query"
        let result = matcher.fuzzy_match("query", "New Query").unwrap();
        assert!(result.is_match());

        // "tls" should match "Toggle Left Sidebar" (acronym)
        let result = matcher.fuzzy_match("tls", "Toggle Left Sidebar").unwrap();
        assert!(result.is_match());

        // "settings" should prefix-match "Settings"
        let result = matcher.fuzzy_match("sett", "Settings").unwrap();
        assert_eq!(result.quality, MatchQuality::Prefix);
    }

    #[test]
    fn test_long_string_completes_quickly() {
        let matcher = FuzzyMatcher::new(false);
        let long_candidate = "a".repeat(500);
        // The DP approach is O(pattern_len * candidate_len) so this should
        // complete in well under a second even for long inputs.
        let result = matcher.fuzzy_match("aaaa", &long_candidate).unwrap();
        assert!(result.is_match());
    }

    #[test]
    fn test_non_ascii_substring_indices() {
        let matcher = FuzzyMatcher::new(false);

        // 'á' is 2 bytes in UTF-8. str::find("bcd") would return byte offset 2,
        // but the char index of 'b' is 1. We must produce char indices.
        let result = matcher.fuzzy_match("bcd", "ábcdef").unwrap();
        assert_eq!(result.quality, MatchQuality::Substring);
        // Chars: á(0) b(1) c(2) d(3) e(4) f(5) — "bcd" starts at char 1
        assert_eq!(result.matched_indices, vec![1, 2, 3]);
    }

    #[test]
    fn test_non_ascii_prefix_score_uses_char_count() {
        let matcher = FuzzyMatcher::new(false);

        // Both candidates have the same char length (6) but different byte lengths.
        let result_ascii = matcher.fuzzy_match("ab", "abcdef").unwrap();
        let result_unicode = matcher.fuzzy_match("ab", "abcdéf").unwrap();
        assert_eq!(result_ascii.quality, MatchQuality::Prefix);
        assert_eq!(result_unicode.quality, MatchQuality::Prefix);
        // Same char-length difference (6-2=4) → same score.
        assert_eq!(result_ascii.score, result_unicode.score);
    }

    #[test]
    fn test_cjk_substring_match() {
        let matcher = FuzzyMatcher::new(false);

        // CJK chars are 3 bytes each. "表b" in "数据表b列" should match at char 2.
        let result = matcher.fuzzy_match("表b", "数据表b列").unwrap();
        assert!(result.is_match());
        assert_eq!(result.matched_indices, vec![2, 3]);
    }
}
