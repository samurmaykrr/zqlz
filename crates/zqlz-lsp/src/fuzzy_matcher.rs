/// Fuzzy string matching for SQL completions
///
/// Implements a lightweight fuzzy matcher similar to what's used in modern IDEs.
/// Supports:
/// - Prefix matching (highest priority)
/// - Substring matching
/// - Acronym matching (e.g., "sel" matches "SELECT")
/// - Character-by-character fuzzy matching with penalty

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchQuality {
    /// No match at all
    None = 0,
    /// Fuzzy match with gaps (lowest quality)
    Fuzzy = 1,
    /// Acronym match (e.g., "sel" -> "SELECT")
    Acronym = 2,
    /// Substring match (e.g., "lect" -> "SELECT")
    Substring = 3,
    /// Prefix match (highest quality)
    Prefix = 4,
}

#[derive(Debug, Clone)]
pub struct FuzzyMatch {
    pub quality: MatchQuality,
    pub score: i32,
    pub matched_indices: Vec<usize>,
}

impl FuzzyMatch {
    /// Check if this is a valid match
    pub fn is_match(&self) -> bool {
        self.quality != MatchQuality::None
    }
}

pub struct FuzzyMatcher {
    case_sensitive: bool,
}

struct FuzzySearchFrame {
    pattern_idx: usize,
    candidate_idx: usize,
    current_score: i32,
    current_indices: Vec<usize>,
}

struct FuzzySearchState {
    best_score: Option<i32>,
    best_indices: Vec<usize>,
}

impl FuzzyMatcher {
    pub fn new(case_sensitive: bool) -> Self {
        Self { case_sensitive }
    }

    /// Match a pattern against a candidate string
    /// Returns None if no match, otherwise returns match quality and score
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

        // 1. Check for prefix match (highest priority)
        if candidate_lower.starts_with(&pattern_lower) {
            let indices: Vec<usize> = (0..pattern_chars.len()).collect();
            return Some(FuzzyMatch {
                quality: MatchQuality::Prefix,
                score: 1000 - (candidate.len() - pattern.len()) as i32,
                matched_indices: indices,
            });
        }

        // 2. Check for substring match
        if let Some(pos) = candidate_lower.find(&pattern_lower) {
            let indices: Vec<usize> = (pos..pos + pattern_chars.len()).collect();
            let score = 800 - pos as i32; // Prefer earlier matches
            return Some(FuzzyMatch {
                quality: MatchQuality::Substring,
                score,
                matched_indices: indices,
            });
        }

        // 2.5. Check for underscore-stripped match (e.g., "userid" matches "user_id")
        // This is common in SQL where identifiers use underscores
        let candidate_no_underscore = candidate_lower.replace('_', "");
        if candidate_no_underscore.contains(&pattern_lower) {
            // Try to find the match position in the original string
            if let Some(pos) = candidate_no_underscore.find(&pattern_lower) {
                let score = 750 - pos as i32; // Slightly lower than exact substring match
                return Some(FuzzyMatch {
                    quality: MatchQuality::Substring,
                    score,
                    matched_indices: Vec::new(), // Indices would be complex to calculate
                });
            }
        }

        // 3. Check for acronym match (first letters of words)
        if let Some(indices) = self.match_acronym(&pattern_chars, &candidate_chars, candidate) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Acronym,
                score: 600,
                matched_indices: indices,
            });
        }

        // 4. Try fuzzy character-by-character matching
        if let Some((score, indices)) = self.match_fuzzy(&pattern_chars, &candidate_chars) {
            return Some(FuzzyMatch {
                quality: MatchQuality::Fuzzy,
                score,
                matched_indices: indices,
            });
        }

        // No match found
        Some(FuzzyMatch {
            quality: MatchQuality::None,
            score: 0,
            matched_indices: Vec::new(),
        })
    }

    /// Match acronym style (first letters of words)
    fn match_acronym(
        &self,
        pattern: &[char],
        candidate: &[char],
        original_candidate: &str,
    ) -> Option<Vec<usize>> {
        let original_chars: Vec<char> = original_candidate.chars().collect();
        let word_starts: Vec<usize> = original_chars
            .iter()
            .enumerate()
            .filter_map(|(idx, ch)| {
                if idx == 0 {
                    return Some(idx);
                }

                let prev_char = original_chars[idx - 1];
                let starts_after_separator = !prev_char.is_alphanumeric();
                let starts_camel_case = ch.is_uppercase() && !prev_char.is_uppercase();

                if starts_after_separator || starts_camel_case {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        if word_starts.len() < pattern.len() {
            return None;
        }

        // Try to match pattern characters to word starts
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

    /// Fuzzy character-by-character matching with scoring
    fn match_fuzzy(&self, pattern: &[char], candidate: &[char]) -> Option<(i32, Vec<usize>)> {
        if pattern.is_empty() {
            return Some((0, Vec::new()));
        }

        if pattern.len() > candidate.len() {
            return None;
        }

        let mut state = FuzzySearchState {
            best_score: None,
            best_indices: Vec::new(),
        };

        // Try to find the best fuzzy match
        self.fuzzy_match_recursive(
            pattern,
            candidate,
            FuzzySearchFrame {
                pattern_idx: 0,
                candidate_idx: 0,
                current_score: 0,
                current_indices: Vec::new(),
            },
            &mut state,
        );

        state.best_score.map(|score| (score, state.best_indices))
    }

    fn fuzzy_match_recursive(
        &self,
        pattern: &[char],
        candidate: &[char],
        frame: FuzzySearchFrame,
        state: &mut FuzzySearchState,
    ) {
        // All pattern characters matched
        if frame.pattern_idx >= pattern.len() {
            if state
                .best_score
                .is_none_or(|score| frame.current_score > score)
            {
                state.best_score = Some(frame.current_score);
                state.best_indices = frame.current_indices;
            }
            return;
        }

        // Ran out of candidate characters
        if frame.candidate_idx >= candidate.len() {
            return;
        }

        let pattern_char = pattern[frame.pattern_idx];

        // Try matching current candidate character
        if candidate[frame.candidate_idx] == pattern_char {
            let mut new_indices = frame.current_indices.clone();
            new_indices.push(frame.candidate_idx);

            // Calculate score based on consecutive matches
            let score_bonus = if frame.pattern_idx > 0
                && frame.candidate_idx > 0
                && frame.current_indices.last() == Some(&(frame.candidate_idx - 1))
            {
                10 // Bonus for consecutive matches
            } else {
                0
            };

            self.fuzzy_match_recursive(
                pattern,
                candidate,
                FuzzySearchFrame {
                    pattern_idx: frame.pattern_idx + 1,
                    candidate_idx: frame.candidate_idx + 1,
                    current_score: frame.current_score + score_bonus + 1,
                    current_indices: new_indices,
                },
                state,
            );
        }

        // Try skipping current candidate character (with penalty)
        if frame.candidate_idx < candidate.len() - 1 {
            self.fuzzy_match_recursive(
                pattern,
                candidate,
                FuzzySearchFrame {
                    pattern_idx: frame.pattern_idx,
                    candidate_idx: frame.candidate_idx + 1,
                    current_score: frame.current_score - 1,
                    current_indices: frame.current_indices,
                },
                state,
            );
        }
    }
}

impl Default for FuzzyMatcher {
    fn default() -> Self {
        Self::new(false)
    }
}

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

        assert!(result.quality == MatchQuality::Fuzzy || result.quality == MatchQuality::Substring);
        assert!(result.is_match());
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
    fn test_consecutive_match_bonus() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("slct", "SELECT").unwrap();
        let result2 = matcher.fuzzy_match("slt", "SELECT").unwrap();

        // More consecutive matches should have higher score
        if result1.quality == MatchQuality::Fuzzy && result2.quality == MatchQuality::Fuzzy {
            assert!(result1.score >= result2.score);
        }
    }

    #[test]
    fn test_prefer_shorter_prefix() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("sel", "SELECT").unwrap();
        let result2 = matcher.fuzzy_match("sel", "SELECTTTTTT").unwrap();

        // Shorter match should have higher score
        assert!(result1.score > result2.score);
    }

    #[test]
    fn test_prefer_earlier_substring() {
        let matcher = FuzzyMatcher::new(false);
        let result1 = matcher.fuzzy_match("abc", "abcdef").unwrap();
        let result2 = matcher.fuzzy_match("abc", "xyzabc").unwrap();

        // Earlier substring should have higher score
        assert!(result1.score > result2.score);
    }
}
