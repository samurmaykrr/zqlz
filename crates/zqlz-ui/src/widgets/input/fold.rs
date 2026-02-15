use ropey::Rope;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

/// Represents a foldable region in the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldRegion {
    /// Start line (0-indexed)
    pub start_line: usize,
    /// End line (0-indexed, inclusive)
    pub end_line: usize,
    /// Nesting level of this fold region
    pub level: usize,
}

impl FoldRegion {
    pub fn new(start_line: usize, end_line: usize, level: usize) -> Self {
        Self {
            start_line,
            end_line,
            level,
        }
    }

    pub fn contains_line(&self, line: usize) -> bool {
        line >= self.start_line && line <= self.end_line
    }

    pub fn line_range(&self) -> Range<usize> {
        self.start_line..self.end_line + 1
    }
}

/// Manages the fold state for a text editor.
#[derive(Debug, Clone)]
pub struct FoldState {
    /// All detected fold regions in the text
    regions: Vec<FoldRegion>,
    /// Set of currently folded regions (by start line)
    folded: BTreeSet<usize>,
    /// Maps start line to fold region index
    region_by_line: BTreeMap<usize, usize>,
}

impl FoldState {
    pub fn new() -> Self {
        Self {
            regions: Vec::new(),
            folded: BTreeSet::new(),
            region_by_line: BTreeMap::new(),
        }
    }

    /// Detect fold regions in SQL code based on keywords and indentation.
    pub fn detect_sql_regions(rope: &Rope) -> Self {
        let mut regions = Vec::new();
        let mut region_by_line = BTreeMap::new();
        let mut stack: Vec<(usize, usize)> = Vec::new(); // (start_line, indent_level)

        use ropey::LineType;
        let line_count = rope.len_lines(LineType::LF);

        for line_idx in 0..line_count {
            let line = rope.line(line_idx, LineType::LF);
            let line_str = line.to_string();
            let trimmed = line_str.trim_start();

            if trimmed.is_empty() {
                continue;
            }

            let indent = line_str.len() - trimmed.len();
            let trimmed_upper = trimmed.to_uppercase();

            // Check for SQL block keywords
            let is_block_start = trimmed_upper.starts_with("SELECT")
                || trimmed_upper.starts_with("INSERT")
                || trimmed_upper.starts_with("UPDATE")
                || trimmed_upper.starts_with("DELETE")
                || trimmed_upper.starts_with("CREATE")
                || trimmed_upper.starts_with("ALTER")
                || trimmed_upper.starts_with("WITH")
                || trimmed_upper.starts_with("CASE")
                || trimmed_upper.starts_with("BEGIN")
                || (trimmed_upper.starts_with("(") && trimmed_upper.len() > 1);

            // Pop stack if we've decreased indentation (but not for same level)
            while let Some(&(_, prev_indent)) = stack.last() {
                if indent < prev_indent {
                    if let Some((start_line, _)) = stack.pop() {
                        let end_line = line_idx - 1;
                        if end_line > start_line {
                            let region_idx = regions.len();
                            let level = stack.len();
                            regions.push(FoldRegion::new(start_line, end_line, level));
                            region_by_line.insert(start_line, region_idx);
                        }
                    }
                } else {
                    break;
                }
            }

            // Check for ending keywords
            let is_block_end = trimmed_upper.starts_with("END")
                || trimmed_upper.starts_with(";")
                || trimmed_upper == ")";

            if is_block_end {
                if let Some((start_line, _)) = stack.pop() {
                    let end_line = line_idx;
                    if end_line > start_line {
                        let region_idx = regions.len();
                        let level = stack.len();
                        regions.push(FoldRegion::new(start_line, end_line, level));
                        region_by_line.insert(start_line, region_idx);
                    }
                }
            } else if is_block_start {
                stack.push((line_idx, indent));
            }
        }

        // Close any remaining open regions
        while let Some((start_line, _)) = stack.pop() {
            let end_line = line_count - 1;
            if end_line > start_line {
                let region_idx = regions.len();
                let level = stack.len();
                regions.push(FoldRegion::new(start_line, end_line, level));
                region_by_line.insert(start_line, region_idx);
            }
        }

        Self {
            regions,
            folded: BTreeSet::new(),
            region_by_line,
        }
    }

    /// Get all fold regions.
    pub fn regions(&self) -> &[FoldRegion] {
        &self.regions
    }

    /// Get the fold region that starts at the given line, if any.
    pub fn region_at_line(&self, line: usize) -> Option<&FoldRegion> {
        self.region_by_line
            .get(&line)
            .and_then(|&idx| self.regions.get(idx))
    }

    /// Check if a line is folded (hidden).
    pub fn is_line_folded(&self, line: usize) -> bool {
        for &start_line in self.folded.iter() {
            if let Some(&idx) = self.region_by_line.get(&start_line) {
                if let Some(region) = self.regions.get(idx) {
                    if line > region.start_line && line <= region.end_line {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if a region starting at the given line is currently folded.
    pub fn is_folded(&self, line: usize) -> bool {
        self.folded.contains(&line)
    }

    /// Toggle fold state for a region starting at the given line.
    pub fn toggle(&mut self, line: usize) {
        if self.folded.contains(&line) {
            self.folded.remove(&line);
        } else if self.region_by_line.contains_key(&line) {
            self.folded.insert(line);
        }
    }

    /// Fold a region starting at the given line.
    pub fn fold(&mut self, line: usize) {
        if self.region_by_line.contains_key(&line) {
            self.folded.insert(line);
        }
    }

    /// Unfold a region starting at the given line.
    pub fn unfold(&mut self, line: usize) {
        self.folded.remove(&line);
    }

    /// Unfold all regions.
    pub fn unfold_all(&mut self) {
        self.folded.clear();
    }

    /// Fold all regions.
    pub fn fold_all(&mut self) {
        for &start_line in self.region_by_line.keys() {
            self.folded.insert(start_line);
        }
    }

    /// Get the set of visible lines (excluding folded lines).
    pub fn visible_lines(&self, total_lines: usize) -> Vec<usize> {
        (0..total_lines)
            .filter(|&line| !self.is_line_folded(line))
            .collect()
    }

    /// Get the next visible line after the given line.
    pub fn next_visible_line(&self, line: usize, total_lines: usize) -> Option<usize> {
        for next_line in (line + 1)..total_lines {
            if !self.is_line_folded(next_line) {
                return Some(next_line);
            }
        }
        None
    }

    /// Get the previous visible line before the given line.
    pub fn prev_visible_line(&self, line: usize) -> Option<usize> {
        for prev_line in (0..line).rev() {
            if !self.is_line_folded(prev_line) {
                return Some(prev_line);
            }
        }
        None
    }
}

impl Default for FoldState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fold_region() {
        let region = FoldRegion::new(5, 10, 0);
        assert_eq!(region.start_line, 5);
        assert_eq!(region.end_line, 10);
        assert_eq!(region.level, 0);
        assert!(region.contains_line(5));
        assert!(region.contains_line(7));
        assert!(region.contains_line(10));
        assert!(!region.contains_line(4));
        assert!(!region.contains_line(11));
    }

    #[test]
    fn test_fold_state_basic() {
        let mut state = FoldState::new();
        assert_eq!(state.regions().len(), 0);
        assert!(!state.is_folded(0));
    }

    #[test]
    fn test_detect_sql_regions() {
        let sql = "SELECT id, name\nFROM users\nWHERE active = true\n;";
        let rope = Rope::from_str(sql);
        let state = FoldState::detect_sql_regions(&rope);

        // Should detect at least one fold region starting at SELECT
        assert!(
            state.regions().len() > 0,
            "Expected at least one fold region"
        );
    }

    #[test]
    fn test_fold_toggle() {
        let sql = "SELECT id\nFROM users;";
        let rope = Rope::from_str(sql);
        let mut state = FoldState::detect_sql_regions(&rope);

        if let Some(region) = state.regions().first() {
            let start_line = region.start_line;
            assert!(!state.is_folded(start_line));

            state.toggle(start_line);
            assert!(state.is_folded(start_line));

            state.toggle(start_line);
            assert!(!state.is_folded(start_line));
        }
    }

    #[test]
    fn test_fold_all_unfold_all() {
        let sql = "SELECT id\nFROM users;\n\nSELECT *\nFROM orders;";
        let rope = Rope::from_str(sql);
        let mut state = FoldState::detect_sql_regions(&rope);

        state.fold_all();
        for region in state.regions() {
            assert!(state.is_folded(region.start_line));
        }

        state.unfold_all();
        for region in state.regions() {
            assert!(!state.is_folded(region.start_line));
        }
    }

    #[test]
    fn test_is_line_folded() {
        let sql = "SELECT id\nFROM users\nWHERE active = true;";
        let rope = Rope::from_str(sql);
        let mut state = FoldState::detect_sql_regions(&rope);

        if let Some(region) = state.regions().first() {
            let start_line = region.start_line;
            let end_line = region.end_line;
            state.fold(start_line);

            // Lines within the folded region should be hidden
            for line in (start_line + 1)..=end_line {
                assert!(state.is_line_folded(line), "Line {} should be folded", line);
            }

            // The start line itself should not be hidden
            assert!(!state.is_line_folded(start_line));
        }
    }
}
