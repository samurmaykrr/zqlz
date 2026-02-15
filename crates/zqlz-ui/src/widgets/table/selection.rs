//! High-performance cell selection for tables.
//!
//! This module provides optimized data structures for tracking cell selections
//! in large tables with support for:
//! - Rectangular region selection (drag, shift+click)
//! - Sparse multi-cell selection (cmd/ctrl+click)
//! - O(1) selection lookups using bit arrays
//! - Visible range caching for minimal per-frame computation

use std::collections::HashSet;
use std::ops::Range;

use smallvec::SmallVec;

/// Represents a cell position in the table
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct CellPosition {
    pub row: usize,
    pub col: usize,
}

impl CellPosition {
    #[inline]
    pub fn new(row: usize, col: usize) -> Self {
        Self { row, col }
    }
}

/// A rectangular region of cells
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellRegion {
    pub start_row: usize,
    pub end_row: usize,
    pub start_col: usize,
    pub end_col: usize,
}

impl CellRegion {
    /// Create a new cell region from two corner positions
    #[inline]
    pub fn new(anchor: CellPosition, end: CellPosition) -> Self {
        Self {
            start_row: anchor.row.min(end.row),
            end_row: anchor.row.max(end.row),
            start_col: anchor.col.min(end.col),
            end_col: anchor.col.max(end.col),
        }
    }

    /// Create a single-cell region
    #[inline]
    pub fn single(row: usize, col: usize) -> Self {
        Self {
            start_row: row,
            end_row: row,
            start_col: col,
            end_col: col,
        }
    }

    /// Check if a cell is within this region - O(1)
    #[inline]
    pub fn contains(&self, row: usize, col: usize) -> bool {
        row >= self.start_row && row <= self.end_row && col >= self.start_col && col <= self.end_col
    }

    /// Check if a row intersects this region - O(1)
    #[inline]
    pub fn intersects_row(&self, row: usize) -> bool {
        row >= self.start_row && row <= self.end_row
    }

    /// Get number of rows in selection
    #[inline]
    pub fn row_count(&self) -> usize {
        self.end_row - self.start_row + 1
    }

    /// Get number of columns in selection
    #[inline]
    pub fn col_count(&self) -> usize {
        self.end_col - self.start_col + 1
    }

    /// Get total number of cells in selection
    #[inline]
    pub fn cell_count(&self) -> usize {
        self.row_count() * self.col_count()
    }

    /// Iterate over all cells in region (row-major order)
    pub fn iter(&self) -> impl Iterator<Item = CellPosition> + '_ {
        (self.start_row..=self.end_row).flat_map(move |row| {
            (self.start_col..=self.end_col).map(move |col| CellPosition { row, col })
        })
    }

    /// Get the bounds as a tuple (min_row, max_row, min_col, max_col)
    #[inline]
    pub fn bounds(&self) -> (usize, usize, usize, usize) {
        (self.start_row, self.end_row, self.start_col, self.end_col)
    }
}

/// Represents a rectangular cell range selection with anchor and end points.
/// This preserves the direction of selection for proper anchor tracking.
#[derive(Clone, Debug, PartialEq)]
pub struct CellRange {
    /// Anchor cell where selection started
    pub anchor: CellPosition,
    /// Current end cell of selection
    pub end: CellPosition,
}

impl CellRange {
    /// Create a new cell range
    #[inline]
    pub fn new(anchor: CellPosition, end: CellPosition) -> Self {
        Self { anchor, end }
    }

    /// Create a single-cell range
    #[inline]
    pub fn single(row: usize, col: usize) -> Self {
        let pos = CellPosition { row, col };
        Self {
            anchor: pos,
            end: pos,
        }
    }

    /// Get the normalized region (sorted bounds)
    #[inline]
    pub fn to_region(&self) -> CellRegion {
        CellRegion::new(self.anchor, self.end)
    }

    /// Get normalized bounds (min/max row/col)
    #[inline]
    pub fn bounds(&self) -> (usize, usize, usize, usize) {
        self.to_region().bounds()
    }

    /// Check if a cell is within this range
    #[inline]
    pub fn contains(&self, row: usize, col: usize) -> bool {
        self.to_region().contains(row, col)
    }

    /// Get number of rows in selection
    #[inline]
    pub fn row_count(&self) -> usize {
        self.to_region().row_count()
    }

    /// Get number of columns in selection
    #[inline]
    pub fn col_count(&self) -> usize {
        self.to_region().col_count()
    }

    /// Get total number of cells in selection
    #[inline]
    pub fn cell_count(&self) -> usize {
        self.to_region().cell_count()
    }

    /// Iterate over all cells in range (row-major order)
    pub fn iter(&self) -> impl Iterator<Item = CellPosition> {
        let (start_row, end_row, start_col, end_col) = self.bounds();
        (start_row..=end_row)
            .flat_map(move |row| (start_col..=end_col).map(move |col| CellPosition { row, col }))
    }
}

/// Cache for visible range selection state.
/// Pre-computed once per frame for O(1) lookups during rendering.
#[derive(Clone, Debug, Default)]
pub struct VisibleSelectionCache {
    /// The visible row range this cache was computed for
    pub visible_rows: Range<usize>,
    /// The visible column range this cache was computed for
    pub visible_cols: Range<usize>,
    /// Packed bits for visible cells selection state.
    /// Layout: selected_bits[local_row * cols_in_range + local_col]
    /// Each bit represents whether that cell is selected.
    selected_bits: Vec<u64>,
    /// Number of visible columns (for indexing)
    cols_count: usize,
    /// Anchor position if it's in the visible range (for special styling)
    pub anchor_position: Option<CellPosition>,
    /// Whether the cache is valid
    valid: bool,
}

impl VisibleSelectionCache {
    /// Create an invalid/empty cache
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a cell (in absolute coordinates) is selected - O(1)
    #[inline]
    pub fn is_selected(&self, row: usize, col: usize) -> bool {
        if !self.valid {
            return false;
        }
        if !self.visible_rows.contains(&row) || !self.visible_cols.contains(&col) {
            return false;
        }
        let local_row = row - self.visible_rows.start;
        let local_col = col - self.visible_cols.start;
        let bit_index = local_row * self.cols_count + local_col;
        let word_index = bit_index / 64;
        let bit_offset = bit_index % 64;

        self.selected_bits
            .get(word_index)
            .map(|word| (word >> bit_offset) & 1 == 1)
            .unwrap_or(false)
    }

    /// Check if the given cell is the anchor
    #[inline]
    pub fn is_anchor(&self, row: usize, col: usize) -> bool {
        self.anchor_position == Some(CellPosition { row, col })
    }

    /// Invalidate the cache
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Check if cache is valid for the given visible range
    #[inline]
    pub fn is_valid_for(&self, visible_rows: &Range<usize>, visible_cols: &Range<usize>) -> bool {
        self.valid && self.visible_rows == *visible_rows && self.visible_cols == *visible_cols
    }
}

/// High-performance cell selection state.
///
/// Optimized for:
/// - Fast O(1) selection checks via cached bit arrays
/// - Efficient rectangular selections (most common case)
/// - Support for sparse multi-cell selections (cmd+click)
/// - Minimal memory allocation during selection operations
#[derive(Clone, Debug, Default)]
pub struct FastCellSelection {
    /// Primary rectangular selection range (anchor + end for direction)
    pub range: Option<CellRange>,

    /// Additional non-contiguous selected cells (Cmd+Click).
    /// Using HashSet for O(1) membership tests.
    /// This is typically small (< 100 cells) so HashSet is fine.
    pub additional_cells: HashSet<CellPosition>,

    /// Whether a drag selection is in progress (mouse moved to different cell while button held)
    pub is_selecting: bool,

    /// Cell where mouse button was pressed down (for detecting drag vs click).
    /// Set on mouse down, cleared on mouse up.
    /// Drag selection only starts when mouse moves to a DIFFERENT cell.
    pub drag_start_cell: Option<CellPosition>,

    /// Cache for visible range - computed once per frame
    visible_cache: VisibleSelectionCache,

    /// Dirty flag - set when selection changes, cleared when cache is rebuilt
    cache_dirty: bool,
}

impl FastCellSelection {
    /// Create a new empty selection
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a cell is selected.
    /// Uses cached lookup when available, falls back to direct check.
    #[inline]
    pub fn is_selected(&self, row: usize, col: usize) -> bool {
        // Fast path: check cache first if valid
        if self.visible_cache.valid
            && self.visible_cache.visible_rows.contains(&row)
            && self.visible_cache.visible_cols.contains(&col)
        {
            return self.visible_cache.is_selected(row, col);
        }

        // Slow path: direct check
        self.is_selected_direct(row, col)
    }

    /// Direct selection check without cache - O(1) for range, O(1) amortized for additional cells
    #[inline]
    pub fn is_selected_direct(&self, row: usize, col: usize) -> bool {
        if let Some(ref range) = self.range {
            if range.contains(row, col) {
                return true;
            }
        }
        self.additional_cells.contains(&CellPosition { row, col })
    }

    /// Get the anchor cell (where selection started)
    #[inline]
    pub fn anchor(&self) -> Option<CellPosition> {
        self.range.as_ref().map(|r| r.anchor)
    }

    /// Get all selected cell positions (for copy, export, etc.)
    pub fn selected_cells(&self) -> Vec<CellPosition> {
        let mut cells: HashSet<CellPosition> = self.additional_cells.clone();
        if let Some(ref range) = self.range {
            cells.extend(range.iter());
        }
        let mut result: Vec<_> = cells.into_iter().collect();
        // Sort by row then col for predictable order
        result.sort_by(|a, b| a.row.cmp(&b.row).then(a.col.cmp(&b.col)));
        result
    }

    /// Get total number of selected cells
    pub fn cell_count(&self) -> usize {
        let mut count = self.additional_cells.len();
        if let Some(ref range) = self.range {
            count += range.cell_count();
        }
        count
    }

    /// Check if selection is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.range.is_none() && self.additional_cells.is_empty()
    }

    /// Clear all selection
    pub fn clear(&mut self) {
        self.range = None;
        self.additional_cells.clear();
        self.is_selecting = false;
        self.drag_start_cell = None;
        self.cache_dirty = true;
        self.visible_cache.invalidate();
    }

    /// Start tracking a potential drag selection (called on mouse down).
    /// This doesn't start the actual selection - it only records where the mouse went down.
    /// The actual drag selection starts when mouse moves to a different cell.
    pub fn begin_potential_drag(&mut self, row: usize, col: usize) {
        self.drag_start_cell = Some(CellPosition::new(row, col));
        self.is_selecting = false; // Not selecting yet, just tracking potential drag
    }

    /// End potential drag tracking (called on mouse up).
    /// Clears drag_start_cell and is_selecting.
    pub fn end_drag(&mut self) {
        self.drag_start_cell = None;
        self.is_selecting = false;
    }

    /// Check if we should start/continue drag selection based on current cell.
    /// Returns true if drag selection should be active (mouse moved to different cell).
    pub fn should_drag_select(&mut self, row: usize, col: usize) -> bool {
        if let Some(start_cell) = self.drag_start_cell {
            // Only start drag if we've moved to a different cell
            if start_cell.row != row || start_cell.col != col {
                // On first transition to drag mode, initialize selection with drag_start_cell as anchor
                // This ensures the selection starts from where the user clicked, not where they dragged to
                if !self.is_selecting {
                    // Clear any additional cells from Cmd+click
                    self.additional_cells.clear();
                    // Set range with drag_start_cell as anchor and current cell as end
                    self.range = Some(CellRange::new(start_cell, CellPosition::new(row, col)));
                    self.cache_dirty = true;
                }
                self.is_selecting = true;
                return true;
            }
        }
        false
    }

    /// Start a new selection at the given position (clears previous)
    pub fn start_selection(&mut self, row: usize, col: usize) {
        self.additional_cells.clear();
        self.range = Some(CellRange::single(row, col));
        self.cache_dirty = true;
    }

    /// Extend selection to include the given cell (for shift+click or drag)
    pub fn extend_selection(&mut self, row: usize, col: usize) {
        if let Some(ref mut range) = self.range {
            range.end = CellPosition { row, col };
        } else {
            // No anchor, start new selection
            self.range = Some(CellRange::single(row, col));
        }
        self.cache_dirty = true;
    }

    /// Toggle a cell in the selection (for cmd+click)
    pub fn toggle_cell(&mut self, row: usize, col: usize) {
        let pos = CellPosition { row, col };

        // Check if cell is in the main range
        let in_range = self
            .range
            .as_ref()
            .map(|r| r.contains(row, col))
            .unwrap_or(false);

        if in_range {
            // Cell is in range - we can't easily "punch a hole" in a range,
            // so for simplicity, we just add it to additional_cells as an exclusion
            // Note: A more sophisticated implementation could convert the range
            // to multiple smaller ranges, but that adds complexity.
            // For now, we handle this by checking additional_cells as "exceptions"

            // Actually, let's convert range to explicit cells and toggle
            // This is only triggered on cmd+click which is relatively rare
            if let Some(range) = self.range.take() {
                // Convert range to explicit cells
                for cell in range.iter() {
                    if cell != pos {
                        self.additional_cells.insert(cell);
                    }
                }
            }
        } else if self.additional_cells.contains(&pos) {
            // Cell is in additional_cells - remove it
            self.additional_cells.remove(&pos);
        } else {
            // Cell is not selected - add it
            self.additional_cells.insert(pos);
        }
        self.cache_dirty = true;
    }

    /// Select all cells in the given range
    pub fn select_all(&mut self, row_count: usize, col_count: usize) {
        if row_count > 0 && col_count > 0 {
            self.additional_cells.clear();
            self.range = Some(CellRange::new(
                CellPosition { row: 0, col: 0 },
                CellPosition {
                    row: row_count - 1,
                    col: col_count - 1,
                },
            ));
            self.cache_dirty = true;
        }
    }

    /// Compute/update the visible selection cache for the given visible range.
    /// Call this once per frame before rendering.
    pub fn update_visible_cache(&mut self, visible_rows: Range<usize>, visible_cols: Range<usize>) {
        // Skip if cache is still valid
        if !self.cache_dirty
            && self
                .visible_cache
                .is_valid_for(&visible_rows, &visible_cols)
        {
            return;
        }

        let rows_count = visible_rows.len();
        let cols_count = visible_cols.len();
        let total_cells = rows_count * cols_count;
        let words_needed = (total_cells + 63) / 64;

        // Resize and clear bits
        self.visible_cache.selected_bits.clear();
        self.visible_cache.selected_bits.resize(words_needed, 0u64);
        self.visible_cache.cols_count = cols_count;
        self.visible_cache.visible_rows = visible_rows.clone();
        self.visible_cache.visible_cols = visible_cols.clone();
        self.visible_cache.anchor_position = None;

        // Populate selection bits
        if let Some(ref range) = self.range {
            let region = range.to_region();

            // Set anchor if in visible range
            if visible_rows.contains(&range.anchor.row) && visible_cols.contains(&range.anchor.col)
            {
                self.visible_cache.anchor_position = Some(range.anchor);
            }

            // Fill bits for intersection of range and visible area
            let row_start = region.start_row.max(visible_rows.start);
            let row_end = region.end_row.min(visible_rows.end.saturating_sub(1));
            let col_start = region.start_col.max(visible_cols.start);
            let col_end = region.end_col.min(visible_cols.end.saturating_sub(1));

            if row_start <= row_end && col_start <= col_end {
                for row in row_start..=row_end {
                    let local_row = row - visible_rows.start;
                    for col in col_start..=col_end {
                        let local_col = col - visible_cols.start;
                        let bit_index = local_row * cols_count + local_col;
                        let word_index = bit_index / 64;
                        let bit_offset = bit_index % 64;
                        if word_index < self.visible_cache.selected_bits.len() {
                            self.visible_cache.selected_bits[word_index] |= 1u64 << bit_offset;
                        }
                    }
                }
            }
        }

        // Add additional cells
        for cell in &self.additional_cells {
            if visible_rows.contains(&cell.row) && visible_cols.contains(&cell.col) {
                let local_row = cell.row - visible_rows.start;
                let local_col = cell.col - visible_cols.start;
                let bit_index = local_row * cols_count + local_col;
                let word_index = bit_index / 64;
                let bit_offset = bit_index % 64;
                if word_index < self.visible_cache.selected_bits.len() {
                    self.visible_cache.selected_bits[word_index] |= 1u64 << bit_offset;
                }
            }
        }

        self.visible_cache.valid = true;
        self.cache_dirty = false;
    }

    /// Get rows that are affected by the current selection (for dirty tracking)
    pub fn affected_rows(&self) -> SmallVec<[Range<usize>; 4]> {
        let mut ranges = SmallVec::new();

        if let Some(ref range) = self.range {
            let region = range.to_region();
            ranges.push(region.start_row..region.end_row + 1);
        }

        // Group additional cells by contiguous row ranges
        if !self.additional_cells.is_empty() {
            let mut rows: Vec<usize> = self.additional_cells.iter().map(|c| c.row).collect();
            rows.sort();
            rows.dedup();

            let mut start = rows[0];
            let mut end = rows[0];
            for &row in &rows[1..] {
                if row == end + 1 {
                    end = row;
                } else {
                    ranges.push(start..end + 1);
                    start = row;
                    end = row;
                }
            }
            ranges.push(start..end + 1);
        }

        ranges
    }

    /// Invalidate the visible cache (call when scrolling)
    pub fn invalidate_cache(&mut self) {
        self.visible_cache.invalidate();
    }

    /// Get reference to the visible cache
    pub fn visible_cache(&self) -> &VisibleSelectionCache {
        &self.visible_cache
    }
}

// Backward compatibility: provide type aliases and conversion
/// Legacy type alias for backward compatibility
pub type CellSelection = FastCellSelection;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_region_contains() {
        let region = CellRegion::new(CellPosition::new(1, 1), CellPosition::new(3, 3));
        assert!(region.contains(1, 1));
        assert!(region.contains(2, 2));
        assert!(region.contains(3, 3));
        assert!(!region.contains(0, 0));
        assert!(!region.contains(4, 4));
    }

    #[test]
    fn test_cell_range_direction() {
        // Selection from bottom-right to top-left
        let range = CellRange::new(CellPosition::new(5, 5), CellPosition::new(2, 2));
        assert_eq!(range.anchor, CellPosition::new(5, 5));
        assert_eq!(range.bounds(), (2, 5, 2, 5));
        assert!(range.contains(3, 3));
    }

    #[test]
    fn test_fast_selection_cache() {
        let mut sel = FastCellSelection::new();
        sel.start_selection(5, 5);
        sel.extend_selection(10, 10);

        // Update cache for visible range
        sel.update_visible_cache(0..20, 0..15);

        // Check cached lookups
        assert!(sel.is_selected(5, 5));
        assert!(sel.is_selected(7, 7));
        assert!(sel.is_selected(10, 10));
        assert!(!sel.is_selected(4, 4));
        assert!(!sel.is_selected(11, 11));
    }

    #[test]
    fn test_toggle_cell() {
        let mut sel = FastCellSelection::new();
        sel.start_selection(0, 0);
        sel.extend_selection(2, 2);

        // Toggle a cell in the range - should create explicit cells
        sel.toggle_cell(1, 1);

        // After toggling, the original range is converted to explicit cells minus (1,1)
        assert!(sel.is_selected_direct(0, 0));
        assert!(!sel.is_selected_direct(1, 1)); // This was toggled off
        assert!(sel.is_selected_direct(2, 2));
    }
}
